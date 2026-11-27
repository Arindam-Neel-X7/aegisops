"""Versioned incident publication contract, wire serialization and validation.

Implements Task 4.12-C1 against approved Task 4.12-A1-R1 without starting
Kafka, PostgreSQL, Neo4j, or the runtime coordinator.

Distinguishes four independent contract versions:
- Transport envelope (TelemetryEvent.schema_version): "1.0"
- Incident aggregate (IncidentAggregate.schema_version): "1.2"
- Receiving handoff (Phase5InvestigationHandoff.schema_version):
  "phase5-investigation-handoff/1.0"
- Publication payload (payload.publication_contract_version):
  "incident-publication/1.0"

Structural decoding verifies wire bounds, exact headers, duplicate-key and
non-finite rejection, envelope/payload/header/key mirrors, deterministic
event_id and publication_fingerprint derivation, and nested structural handoff
consistency. Semantic consumer validation additionally delegates the embedded
handoff to frozen ``validate_phase5_handoff`` against independently pinned
topology and policy authorities.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
import json
import math
from typing import Annotated, Any, Literal, cast
from uuid import NAMESPACE_DNS, UUID, uuid5

from aiokafka.record.default_records import DefaultRecordBatchBuilder
from pydantic import (
    AwareDatetime,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.correlation.blast_radius import (
    BlastRadiusLimitError,
    BlastRadiusPolicy,
    BlastRadiusTopologyMismatchError,
    Fingerprint,
    _canonical_bytes,
    _Contract,
    _fresh,
)
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
    serialize_correlation_config,
)
from app.correlation.errors import CorrelationError
from app.correlation.handoff import (
    Phase5HandoffContractError,
    Phase5InvestigationHandoff,
    build_phase5_handoff,
    decode_phase5_handoff_structural,
    serialize_phase5_handoff,
    validate_phase5_handoff,
)
from app.correlation.models import (
    MAX_INCIDENT_NUMBER_LEN,
    MAX_POLICY_LEN,
    MAX_REVISION,
    MIN_REVISION,
    SUPPORTED_INCIDENT_SCHEMA_VERSION,
    IncidentAggregate,
    TopologySnapshotReference,
)
from app.correlation.persistence_repository import PersistedPublicationIntent
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    serialize_incident_aggregate,
)
from app.correlation.topology import TopologySnapshot
from app.telemetry.schemas import EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic
from app.telemetry.transport.consumer import TelemetryEnvelope, parse_kafka_headers
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    deserialize_event,
    serialize_event,
)

# ============================================================================
# FROZEN CONTRACT VERSIONS, IDENTITY NAMESPACE AND WIRE BOUNDS
# ============================================================================

ENVELOPE_SCHEMA_VERSION: Literal["1.0"] = "1.0"
INCIDENT_SCHEMA_VERSION: Literal["1.2"] = "1.2"
HANDOFF_SCHEMA_VERSION: Literal["phase5-investigation-handoff/1.0"] = (
    "phase5-investigation-handoff/1.0"
)
BLAST_RADIUS_CONTEXT_SCHEMA_VERSION: Literal["blast-radius-context/1.0"] = (
    "blast-radius-context/1.0"
)
BLAST_RADIUS_POLICY_VERSION: Literal["blast-radius-policy/1.0"] = (
    "blast-radius-policy/1.0"
)
HANDOFF_PROVENANCE_PRODUCER_VERSION: Literal["phase4-blast-handoff/1.0"] = (
    "phase4-blast-handoff/1.0"
)
PUBLICATION_CONTRACT_VERSION: Literal["incident-publication/1.0"] = (
    "incident-publication/1.0"
)
DEFAULT_PUBLICATION_PRODUCER_VERSION: str = "phase4-incident-publisher/1.0"

INCIDENT_PUBLICATION_EVENT_NAMESPACE_NAME: str = (
    "aegisops.phase4.incident-publication-event/v1"
)
INCIDENT_PUBLICATION_EVENT_NAMESPACE: UUID = uuid5(
    NAMESPACE_DNS, INCIDENT_PUBLICATION_EVENT_NAMESPACE_NAME
)

INCIDENT_PUBLICATION_EVENT_TYPE: EventType = EventType.INCIDENT
INCIDENT_PUBLICATION_TOPIC: KafkaTopic = EVENT_TYPE_TO_TOPIC[EventType.INCIDENT]

MAX_KAFKA_KEY_BYTES: int = 512
MAX_KAFKA_HEADER_VALUE_BYTES: int = 256
MAX_KAFKA_HEADERS_TOTAL_BYTES: int = 1_024
MAX_KAFKA_VALUE_BYTES: int = 983_040
MAX_KAFKA_RECORD_ESTIMATED_BYTES: int = 1_048_576
MAX_JSON_DEPTH: int = 64

MAX_RUN_ID_HEADER_BYTES: int = 36
MAX_SCENARIO_ID_BYTES: int = 128
MAX_SCENARIO_VERSION_BYTES: int = 64
MAX_REPRODUCIBILITY_KEY_BYTES: int = 256
MAX_SEED_HEADER_BYTES: int = 20
MAX_PRODUCER_VERSION_BYTES: int = 128
MAX_CLAIMED_BY_BYTES: int = 128
MAX_INT64: int = 9_223_372_036_854_775_807

REQUIRED_PUBLICATION_HEADER_NAMES: tuple[str, ...] = (
    "run_id",
    "scenario_id",
    "scenario_version",
    "reproducibility_key",
    "seed",
)
OPTIONAL_PUBLICATION_HEADER_NAME: str = "producer_version"
ORDERED_PUBLICATION_HEADER_NAMES: tuple[str, ...] = (
    *REQUIRED_PUBLICATION_HEADER_NAMES,
    OPTIONAL_PUBLICATION_HEADER_NAME,
)
HEADER_FIELD_MAX_BYTES: Mapping[str, int] = {
    "run_id": MAX_RUN_ID_HEADER_BYTES,
    "scenario_id": MAX_SCENARIO_ID_BYTES,
    "scenario_version": MAX_SCENARIO_VERSION_BYTES,
    "reproducibility_key": MAX_REPRODUCIBILITY_KEY_BYTES,
    "seed": MAX_SEED_HEADER_BYTES,
    "producer_version": MAX_PRODUCER_VERSION_BYTES,
}

ENVELOPE_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "schema_version",
        "event_id",
        "event_time",
        "tenant_id",
        "environment",
        "service",
        "event_type",
        "severity",
        "trace_id",
        "payload",
    }
)

PAYLOAD_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "correlation_config_fingerprint",
    "correlation_policy_version",
    "event_id",
    "execution_lineage",
    "handoff",
    "incident_id",
    "incident_number",
    "intent_time",
    "producer_version",
    "publication_contract_version",
    "publication_fingerprint",
    "revision",
    "revision_fingerprint",
    "tenant_id",
)

EXECUTION_LINEAGE_KEYS: tuple[str, ...] = (
    "reproducibility_key",
    "run_id",
    "scenario_id",
    "scenario_version",
    "seed",
)

PUBLICATION_FINGERPRINT_INPUT_KEYS: tuple[str, ...] = (
    "correlation_config_fingerprint",
    "correlation_policy_version",
    "envelope_schema_version",
    "environment",
    "event_id",
    "event_time",
    "event_type",
    "execution_lineage",
    "handoff_fingerprint",
    "handoff_id",
    "incident_id",
    "incident_number",
    "intent_time",
    "membership_fingerprint",
    "producer_version",
    "publication_contract_version",
    "revision",
    "revision_fingerprint",
    "service",
    "severity",
    "tenant_id",
    "topology_id",
    "topology_snapshot_fingerprint",
    "topology_version",
    "trace_id",
)

TRANSPORT_FIELDS_EXCLUDED_FROM_SEMANTIC_EQUALITY: tuple[str, ...] = (
    "partition",
    "offset",
    "broker_timestamp_ms",
    "kafka_timestamp_ms",
    "latency_ms",
    "consumed_at",
    "attempt_count",
    "claim_token",
    "claimed_by",
    "claimed_at",
    "acknowledged_at",
)

_HEX_CHARS = frozenset("0123456789abcdef")
_UTC_ZERO = timedelta(0)

BoundedText = Annotated[str, StringConstraints(strict=True, min_length=1)]


class _UnsetType:
    """Sentinel distinguishing an omitted optional expectation from ``None``."""


_UNSET = _UnsetType()


# ============================================================================
# TYPED ERROR CONTRACTS
# ============================================================================


class IncidentPublicationError(CorrelationError):
    """Base exception for Task 4.12 incident publication contract errors."""

    default_error_code: str = "INCIDENT_PUBLICATION_ERROR"

    def __init__(self, message: str, *, error_code: str | None = None) -> None:
        self.error_code: str = error_code or self.default_error_code
        super().__init__(message)


class IncidentPublicationContractError(IncidentPublicationError):
    """Raised on malformed, inconsistent, or non-canonical publication contracts."""

    default_error_code = "INCIDENT_PUBLICATION_CONTRACT_ERROR"


class IncidentPublicationVersionError(IncidentPublicationContractError):
    """Raised when an envelope, payload, handoff, or incident version is unsupported."""

    default_error_code = "INCIDENT_PUBLICATION_VERSION_ERROR"


class IncidentPublicationHeaderError(IncidentPublicationContractError):
    """Raised when Kafka execution headers are missing, duplicated, misordered, or invalid."""

    default_error_code = "INCIDENT_PUBLICATION_HEADER_ERROR"


class IncidentPublicationSizeLimitError(IncidentPublicationContractError):
    """Raised when key, header, payload value, or estimated Kafka record size exceeds caps."""

    default_error_code = "INCIDENT_PUBLICATION_SIZE_LIMIT_ERROR"


class IncidentPublicationValidationError(IncidentPublicationContractError):
    """Raised when topology, policy, config, lineage, or semantic handoff validation fails."""

    default_error_code = "INCIDENT_PUBLICATION_VALIDATION_ERROR"


class IncidentPublicationValidationOutcome(StrEnum):
    """Typed non-raising classification for contract build/decode/validation results."""

    VALID = "valid"
    INVALID_CONTRACT = "invalid_contract"
    UNSUPPORTED_VERSION = "unsupported_version"
    INVALID_HEADERS = "invalid_headers"
    SIZE_LIMIT_EXCEEDED = "size_limit_exceeded"
    SEMANTIC_MISMATCH = "semantic_mismatch"


# ============================================================================
# INTERNAL STRICT VALIDATION & CANONICAL ENCODING HELPERS
# ============================================================================


def _has_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in value)


def _is_strict_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _encode_utf8_strict(
    value: str,
    *,
    field_name: str,
    error_cls: type[IncidentPublicationContractError] = (
        IncidentPublicationContractError
    ),
) -> bytes:
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise error_cls(f"{field_name} contains invalid Unicode") from exc


def _validate_exact_trimmed_utf8(
    value: Any,
    *,
    field_name: str,
    max_bytes: int,
    size_error_cls: type[IncidentPublicationContractError] = (
        IncidentPublicationSizeLimitError
    ),
    format_error_cls: type[IncidentPublicationContractError] = (
        IncidentPublicationContractError
    ),
) -> str:
    if not isinstance(value, str):
        raise format_error_cls(f"{field_name} must be a string")
    if not value or value != value.strip():
        raise format_error_cls(f"{field_name} must be trimmed non-empty text")
    if _has_control_chars(value):
        raise format_error_cls(f"{field_name} must not contain control characters")
    encoded_len = len(
        _encode_utf8_strict(value, field_name=field_name, error_cls=format_error_cls)
    )
    if encoded_len > max_bytes:
        raise size_error_cls(f"{field_name} exceeds maximum UTF-8 byte limit")
    return value


def _validate_hex_fingerprint(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise IncidentPublicationContractError(
            f"{field_name} must be a 64-character lowercase hex string"
        )
    if any(ch not in _HEX_CHARS for ch in value):
        raise IncidentPublicationContractError(
            f"{field_name} must be a 64-character lowercase hex string"
        )
    return value


def _validate_canonical_uuid_str(
    value: Any,
    *,
    field_name: str,
    error_cls: type[IncidentPublicationContractError] = (
        IncidentPublicationContractError
    ),
) -> UUID:
    if not isinstance(value, str):
        raise error_cls(f"{field_name} must be a canonical lowercase UUID string")
    encoded_len = len(
        _encode_utf8_strict(value, field_name=field_name, error_cls=error_cls)
    )
    if encoded_len > MAX_RUN_ID_HEADER_BYTES:
        raise IncidentPublicationSizeLimitError(
            f"{field_name} exceeds maximum UTF-8 byte limit"
        )
    if value != value.strip() or _has_control_chars(value):
        raise error_cls(f"{field_name} must be a canonical lowercase UUID string")
    try:
        parsed = UUID(value)
    except (ValueError, TypeError) as exc:
        raise error_cls(
            f"{field_name} must be a canonical lowercase UUID string"
        ) from exc
    if str(parsed) != value:
        raise error_cls(f"{field_name} must be a canonical lowercase UUID string")
    return parsed


def _validate_utc_datetime(value: Any, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise IncidentPublicationContractError(f"{field_name} must be a UTC datetime")
    if value.tzinfo is None or value.utcoffset() != _UTC_ZERO:
        raise IncidentPublicationContractError(f"{field_name} must be UTC")
    return value.astimezone(timezone.utc)


def _format_utc_iso_z(value: datetime) -> str:
    utc_value = _validate_utc_datetime(value, field_name="timestamp")
    return utc_value.isoformat().replace("+00:00", "Z")


def _canonical_json_bytes_bounded(payload: Any, limit: int) -> bytes:
    """Serialize compact sorted-key UTF-8 JSON with incremental byte counting."""
    try:
        return _canonical_bytes(payload, limit)
    except BlastRadiusLimitError as exc:
        raise IncidentPublicationSizeLimitError(
            "Canonical JSON payload exceeds byte limit"
        ) from exc
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to encode canonical JSON payload"
        ) from exc


def _check_json_depth_and_unicode(value: Any, max_depth: int = MAX_JSON_DEPTH) -> None:
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            raise IncidentPublicationContractError(
                "JSON payload exceeds maximum nesting depth"
            )
        if isinstance(current, str):
            _encode_utf8_strict(current, field_name="json_string")
        elif isinstance(current, dict):
            for key, child in current.items():
                if isinstance(key, str):
                    _encode_utf8_strict(key, field_name="json_key")
                if isinstance(child, (dict, list, str)):
                    stack.append((child, depth + 1))
        elif isinstance(current, list):
            for child in current:
                if isinstance(child, (dict, list, str)):
                    stack.append((child, depth + 1))


def _decode_strict_json_object(raw: bytes, limit: int) -> dict[str, Any]:
    if len(raw) == 0:
        raise IncidentPublicationContractError("Publication payload bytes are empty")
    if len(raw) > limit:
        raise IncidentPublicationSizeLimitError(
            "Publication payload exceeds maximum UTF-8 byte limit"
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IncidentPublicationContractError(
            "Publication payload is not valid UTF-8"
        ) from exc

    def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        obj: dict[str, Any] = {}
        for key, val in pairs:
            if key in obj:
                raise ValueError("Duplicate JSON key")
            obj[key] = val
        return obj

    def _reject_nonfinite(token: str) -> None:
        raise ValueError(f"Non-finite JSON constant: {token}")

    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_nonfinite,
        )
    except (json.JSONDecodeError, ValueError, TypeError, RecursionError) as exc:
        raise IncidentPublicationContractError(
            "Malformed or non-finite JSON publication payload"
        ) from exc

    if not isinstance(parsed, dict):
        raise IncidentPublicationContractError(
            "Publication payload JSON root must be an object"
        )
    _check_json_depth_and_unicode(parsed, MAX_JSON_DEPTH)
    return parsed


def _estimate_kafka_record_bytes(
    key: bytes,
    value: bytes,
    headers: Sequence[tuple[str, bytes]],
) -> int:
    try:
        return int(
            DefaultRecordBatchBuilder.estimate_size_in_bytes(key, value, list(headers))
        )
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to estimate Kafka record size"
        ) from exc


def _revalidate_execution_context(
    context: TelemetryExecutionContext,
) -> PublicationExecutionLineage:
    if not isinstance(context, TelemetryExecutionContext):
        raise IncidentPublicationContractError(
            "execution_context must be a TelemetryExecutionContext"
        )
    fields = TelemetryExecutionContext.model_fields
    if set(context.__dict__) != set(fields) or context.__pydantic_extra__:
        raise IncidentPublicationContractError(
            "execution_context contains missing or undeclared fields"
        )
    raw_run_id = context.__dict__["run_id"]
    raw_scenario_id = context.__dict__["scenario_id"]
    raw_scenario_version = context.__dict__["scenario_version"]
    raw_repro_key = context.__dict__["reproducibility_key"]
    raw_seed = context.__dict__["seed"]

    if not isinstance(raw_run_id, UUID):
        raise IncidentPublicationHeaderError("run_id must be a UUID")
    scenario_id = _validate_exact_trimmed_utf8(
        raw_scenario_id,
        field_name="scenario_id",
        max_bytes=MAX_SCENARIO_ID_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )
    scenario_version = _validate_exact_trimmed_utf8(
        raw_scenario_version,
        field_name="scenario_version",
        max_bytes=MAX_SCENARIO_VERSION_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )
    reproducibility_key = _validate_exact_trimmed_utf8(
        raw_repro_key,
        field_name="reproducibility_key",
        max_bytes=MAX_REPRODUCIBILITY_KEY_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )
    if not _is_strict_int(raw_seed) or not (0 <= raw_seed <= MAX_UINT64):
        raise IncidentPublicationHeaderError("seed must be an integer in [0, 2^64 - 1]")

    return PublicationExecutionLineage(
        run_id=raw_run_id,
        scenario_id=scenario_id,
        scenario_version=scenario_version,
        reproducibility_key=reproducibility_key,
        seed=raw_seed,
    )


def _revalidate_correlation_config(
    config: CorrelationConfig,
) -> tuple[CorrelationConfig, str]:
    if not isinstance(config, CorrelationConfig):
        raise IncidentPublicationValidationError(
            "correlation_config must be a CorrelationConfig"
        )
    try:
        fresh = _fresh(config, CorrelationConfig)
        # Re-serialize through frozen serializer to verify canonical parity.
        if serialize_correlation_config(fresh) != serialize_correlation_config(config):
            raise ValueError("Noncanonical correlation config")
        fingerprint = compute_correlation_config_fingerprint(fresh)
    except Exception as exc:
        raise IncidentPublicationValidationError(
            "Invalid correlation configuration"
        ) from exc
    return fresh, fingerprint


def _classify_exception(
    exc: IncidentPublicationError,
) -> IncidentPublicationValidationOutcome:
    if isinstance(exc, IncidentPublicationVersionError):
        return IncidentPublicationValidationOutcome.UNSUPPORTED_VERSION
    if isinstance(exc, IncidentPublicationSizeLimitError):
        return IncidentPublicationValidationOutcome.SIZE_LIMIT_EXCEEDED
    if isinstance(exc, IncidentPublicationHeaderError):
        return IncidentPublicationValidationOutcome.INVALID_HEADERS
    if isinstance(exc, IncidentPublicationValidationError):
        return IncidentPublicationValidationOutcome.SEMANTIC_MISMATCH
    return IncidentPublicationValidationOutcome.INVALID_CONTRACT


def _extract_cause_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__
    return chain


# ============================================================================
# DETERMINISTIC IDENTITY & FINGERPRINT DERIVATION
# ============================================================================


def derive_incident_publication_event_id(
    *,
    tenant_id: UUID,
    incident_id: UUID,
    revision: int,
    revision_fingerprint: str,
    publication_contract_version: str = PUBLICATION_CONTRACT_VERSION,
) -> UUID:
    """Derive the deterministic UUIDv5 event_id for a committed incident revision."""
    if not isinstance(publication_contract_version, str):
        raise IncidentPublicationVersionError(
            "Unsupported publication_contract_version"
        )
    if publication_contract_version != PUBLICATION_CONTRACT_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported publication_contract_version: {publication_contract_version!r}"
        )
    if not isinstance(tenant_id, UUID):
        raise IncidentPublicationContractError("tenant_id must be a UUID")
    if not isinstance(incident_id, UUID):
        raise IncidentPublicationContractError("incident_id must be a UUID")
    if not _is_strict_int(revision) or not (MIN_REVISION <= revision <= MAX_REVISION):
        raise IncidentPublicationContractError(
            "revision must be an integer in [1, 2^64 - 1]"
        )
    fp = _validate_hex_fingerprint(
        revision_fingerprint, field_name="revision_fingerprint"
    )
    seed_input = "|".join(
        [
            publication_contract_version,
            str(tenant_id),
            str(incident_id),
            str(revision),
            fp,
        ]
    )
    return uuid5(INCIDENT_PUBLICATION_EVENT_NAMESPACE, seed_input)


def build_publication_fingerprint_projection(
    *,
    event_id: UUID,
    handoff: Phase5InvestigationHandoff,
    correlation_policy_version: str,
    correlation_config_fingerprint: str,
    execution_lineage: PublicationExecutionLineage,
    producer_version: str | None,
    intent_time: datetime,
) -> dict[str, Any]:
    """Construct the exact 25-field semantic projection hashed for publication_fingerprint."""
    incident = handoff.incident
    topo_ref = handoff.topology_context.topology_snapshot
    dt_str = _format_utc_iso_z(intent_time)
    return {
        "correlation_config_fingerprint": correlation_config_fingerprint,
        "correlation_policy_version": correlation_policy_version,
        "envelope_schema_version": ENVELOPE_SCHEMA_VERSION,
        "environment": incident.environment,
        "event_id": str(event_id),
        "event_time": dt_str,
        "event_type": INCIDENT_PUBLICATION_EVENT_TYPE.value,
        "execution_lineage": {
            "reproducibility_key": execution_lineage.reproducibility_key,
            "run_id": str(execution_lineage.run_id),
            "scenario_id": execution_lineage.scenario_id,
            "scenario_version": execution_lineage.scenario_version,
            "seed": execution_lineage.seed,
        },
        "handoff_fingerprint": handoff.handoff_fingerprint,
        "handoff_id": str(handoff.handoff_id),
        "incident_id": str(incident.incident_id),
        "incident_number": incident.incident_number,
        "intent_time": dt_str,
        "membership_fingerprint": handoff.membership_fingerprint,
        "producer_version": producer_version,
        "publication_contract_version": PUBLICATION_CONTRACT_VERSION,
        "revision": incident.revision,
        "revision_fingerprint": incident.revision_fingerprint,
        "service": incident.primary_service,
        "severity": incident.severity.value,
        "tenant_id": str(incident.tenant_id),
        "topology_id": str(topo_ref.topology_id),
        "topology_snapshot_fingerprint": topo_ref.snapshot_fingerprint,
        "topology_version": topo_ref.topology_version,
        "trace_id": None,
    }


def compute_incident_publication_fingerprint(projection: Mapping[str, Any]) -> str:
    """Compute the lowercase 64-hex SHA-256 semantic publication fingerprint."""
    if not isinstance(projection, Mapping):
        raise IncidentPublicationContractError(
            "Fingerprint projection must be a mapping"
        )
    expected_keys = set(PUBLICATION_FINGERPRINT_INPUT_KEYS)
    actual_keys = set(projection.keys())
    if actual_keys != expected_keys:
        raise IncidentPublicationContractError(
            "Fingerprint projection must contain the exact 25 approved semantic fields"
        )
    canonical_bytes = _canonical_json_bytes_bounded(
        dict(projection), MAX_KAFKA_VALUE_BYTES
    )
    return hashlib.sha256(canonical_bytes).hexdigest()


# ============================================================================
# STRICT CONTRACT MODELS
# ============================================================================


class PublicationExecutionLineage(_Contract):
    """Strict, bounded mirror of TelemetryExecutionContext embedded in payload."""

    run_id: UUID
    scenario_id: BoundedText
    scenario_version: BoundedText
    reproducibility_key: BoundedText
    seed: int = Field(ge=0, le=MAX_UINT64)

    @field_validator("scenario_id")
    @classmethod
    def _check_scenario_id(cls, value: str) -> str:
        return _validate_exact_trimmed_utf8(
            value,
            field_name="scenario_id",
            max_bytes=MAX_SCENARIO_ID_BYTES,
            size_error_cls=IncidentPublicationSizeLimitError,
            format_error_cls=IncidentPublicationHeaderError,
        )

    @field_validator("scenario_version")
    @classmethod
    def _check_scenario_version(cls, value: str) -> str:
        return _validate_exact_trimmed_utf8(
            value,
            field_name="scenario_version",
            max_bytes=MAX_SCENARIO_VERSION_BYTES,
            size_error_cls=IncidentPublicationSizeLimitError,
            format_error_cls=IncidentPublicationHeaderError,
        )

    @field_validator("reproducibility_key")
    @classmethod
    def _check_reproducibility_key(cls, value: str) -> str:
        return _validate_exact_trimmed_utf8(
            value,
            field_name="reproducibility_key",
            max_bytes=MAX_REPRODUCIBILITY_KEY_BYTES,
            size_error_cls=IncidentPublicationSizeLimitError,
            format_error_cls=IncidentPublicationHeaderError,
        )

    @field_validator("seed", mode="before")
    @classmethod
    def _check_seed_not_bool(cls, value: Any) -> Any:
        if not _is_strict_int(value):
            raise ValueError("seed must be an integer")
        return value

    def to_execution_context(self) -> TelemetryExecutionContext:
        return TelemetryExecutionContext(
            run_id=self.run_id,
            scenario_id=self.scenario_id,
            scenario_version=self.scenario_version,
            reproducibility_key=self.reproducibility_key,
            seed=self.seed,
        )

    @classmethod
    def from_execution_context(
        cls, context: TelemetryExecutionContext
    ) -> PublicationExecutionLineage:
        return _revalidate_execution_context(context)


class IncidentPublicationPayload(_Contract):
    """Strict 14-key payload carried inside canonical TelemetryEvent.payload."""

    publication_contract_version: Literal["incident-publication/1.0"] = (
        PUBLICATION_CONTRACT_VERSION
    )
    event_id: UUID
    publication_fingerprint: Fingerprint
    tenant_id: UUID
    incident_id: UUID
    incident_number: BoundedText
    revision: int = Field(ge=MIN_REVISION, le=MAX_REVISION)
    revision_fingerprint: Fingerprint
    intent_time: AwareDatetime
    correlation_policy_version: BoundedText
    correlation_config_fingerprint: Fingerprint
    execution_lineage: PublicationExecutionLineage
    producer_version: str | None
    handoff: Phase5InvestigationHandoff

    @field_validator("revision", mode="before")
    @classmethod
    def _check_revision_not_bool(cls, value: Any) -> Any:
        if not _is_strict_int(value):
            raise ValueError("revision must be an integer")
        return value

    @field_validator("incident_number")
    @classmethod
    def _check_incident_number(cls, value: str) -> str:
        return _validate_exact_trimmed_utf8(
            value,
            field_name="incident_number",
            max_bytes=MAX_INCIDENT_NUMBER_LEN,
        )

    @field_validator("correlation_policy_version")
    @classmethod
    def _check_policy_version(cls, value: str) -> str:
        return _validate_exact_trimmed_utf8(
            value,
            field_name="correlation_policy_version",
            max_bytes=MAX_POLICY_LEN,
        )

    @field_validator("producer_version")
    @classmethod
    def _check_producer_version(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_exact_trimmed_utf8(
            value,
            field_name="producer_version",
            max_bytes=MAX_PRODUCER_VERSION_BYTES,
            size_error_cls=IncidentPublicationSizeLimitError,
            format_error_cls=IncidentPublicationHeaderError,
        )

    @field_validator("intent_time")
    @classmethod
    def _check_intent_time_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != _UTC_ZERO:
            raise ValueError("intent_time must be UTC")
        return value.astimezone(timezone.utc)

    @field_validator("handoff", mode="before")
    @classmethod
    def _check_handoff(cls, value: Any, info: ValidationInfo) -> Any:
        if isinstance(value, Phase5InvestigationHandoff):
            return decode_phase5_handoff_structural(serialize_phase5_handoff(value))
        if isinstance(value, dict):
            if info.mode == "python":
                return Phase5InvestigationHandoff.model_validate(value)
            return decode_phase5_handoff_structural(
                _canonical_json_bytes_bounded(value, MAX_KAFKA_VALUE_BYTES)
            )
        return value

    @model_validator(mode="after")
    def _reconcile(self) -> IncidentPublicationPayload:
        handoff = decode_phase5_handoff_structural(
            serialize_phase5_handoff(self.handoff)
        )
        object.__setattr__(self, "handoff", handoff)
        incident = handoff.incident

        if self.tenant_id != incident.tenant_id:
            raise ValueError("Payload tenant_id does not match incident")
        if self.incident_id != incident.incident_id:
            raise ValueError("Payload incident_id does not match incident")
        if self.incident_number != incident.incident_number:
            raise ValueError("Payload incident_number does not match incident")
        if self.revision != incident.revision:
            raise ValueError("Payload revision does not match incident")
        if self.revision_fingerprint != incident.revision_fingerprint:
            raise ValueError("Payload revision_fingerprint does not match incident")
        if self.intent_time != incident.updated_at:
            raise ValueError("Payload intent_time must equal incident.updated_at")
        if self.intent_time != handoff.generated_at:
            raise ValueError("Payload intent_time must equal handoff.generated_at")

        expected_event_id = derive_incident_publication_event_id(
            tenant_id=self.tenant_id,
            incident_id=self.incident_id,
            revision=self.revision,
            revision_fingerprint=self.revision_fingerprint,
            publication_contract_version=self.publication_contract_version,
        )
        if self.event_id != expected_event_id:
            raise ValueError("Payload event_id does not match deterministic derivation")

        projection = build_publication_fingerprint_projection(
            event_id=self.event_id,
            handoff=handoff,
            correlation_policy_version=self.correlation_policy_version,
            correlation_config_fingerprint=self.correlation_config_fingerprint,
            execution_lineage=self.execution_lineage,
            producer_version=self.producer_version,
            intent_time=self.intent_time,
        )
        expected_fp = compute_incident_publication_fingerprint(projection)
        if self.publication_fingerprint != expected_fp:
            raise ValueError(
                "Payload publication_fingerprint does not match canonical projection"
            )
        return self


class PublicationTransportMetadata(_Contract):
    """Observed Kafka/dispatch transport metadata explicitly excluded from semantic equality."""

    partition: int | None = Field(default=None, ge=0)
    offset: int | None = Field(default=None, ge=0, le=MAX_INT64)
    broker_timestamp_ms: int | None = None
    kafka_timestamp_ms: int | None = None
    latency_ms: float | None = Field(default=None, ge=0.0)
    consumed_at: AwareDatetime | None = None
    attempt_count: int | None = Field(default=None, ge=0)
    claim_token: UUID | None = None
    claimed_by: str | None = None
    claimed_at: AwareDatetime | None = None
    acknowledged_at: AwareDatetime | None = None

    @field_validator(
        "partition",
        "offset",
        "broker_timestamp_ms",
        "kafka_timestamp_ms",
        "attempt_count",
        mode="before",
    )
    @classmethod
    def _check_optional_strict_int(cls, value: Any) -> Any:
        if value is None:
            return None
        if not _is_strict_int(value):
            raise ValueError("Transport integer field must be a strict int")
        return value

    @field_validator("latency_ms", mode="before")
    @classmethod
    def _check_latency_ms(cls, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("latency_ms must be a finite non-negative number")
        fval = float(value)
        if not math.isfinite(fval) or fval < 0.0:
            raise ValueError("latency_ms must be a finite non-negative number")
        return fval

    @field_validator("claimed_by")
    @classmethod
    def _check_claimed_by(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_exact_trimmed_utf8(
            value,
            field_name="claimed_by",
            max_bytes=MAX_CLAIMED_BY_BYTES,
        )

    @field_validator("consumed_at", "claimed_at", "acknowledged_at")
    @classmethod
    def _check_optional_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() != _UTC_ZERO:
            raise ValueError("Transport timestamp must be UTC")
        return value.astimezone(timezone.utc)


class IncidentPublicationIdentity(_Contract):
    """Immutable semantic identity of one committed incident revision publication."""

    publication_contract_version: Literal["incident-publication/1.0"] = (
        PUBLICATION_CONTRACT_VERSION
    )
    envelope_schema_version: Literal["1.0"] = ENVELOPE_SCHEMA_VERSION
    handoff_schema_version: Literal["phase5-investigation-handoff/1.0"] = (
        HANDOFF_SCHEMA_VERSION
    )
    tenant_id: UUID
    incident_id: UUID
    revision: int = Field(ge=MIN_REVISION, le=MAX_REVISION)
    revision_fingerprint: Fingerprint
    event_id: UUID
    publication_fingerprint: Fingerprint
    handoff_id: UUID
    handoff_fingerprint: Fingerprint
    membership_fingerprint: Fingerprint
    topology_id: UUID
    topology_version: BoundedText
    topology_snapshot_fingerprint: Fingerprint
    correlation_policy_version: BoundedText
    correlation_config_fingerprint: Fingerprint
    intent_time: AwareDatetime


@dataclass(frozen=True)
class IncidentPublicationRecord:
    """Immutable validated wire publication artifact for a committed incident revision.

    ``transport_metadata`` is explicitly excluded from equality and hashing so
    broker offsets, partitions, timestamps, latencies, and dispatch bookkeeping
    observations never affect semantic equality.
    """

    topic: KafkaTopic
    event_id: UUID
    publication_fingerprint: str
    payload: IncidentPublicationPayload
    execution_context: TelemetryExecutionContext
    producer_version: str | None
    kafka_key: bytes
    kafka_value: bytes
    kafka_headers: tuple[tuple[str, bytes], ...]
    kafka_headers_total_bytes: int
    estimated_record_bytes: int
    transport_metadata: PublicationTransportMetadata | None = field(
        default=None, compare=False, hash=False
    )

    def __post_init__(self) -> None:
        _verify_publication_record_integrity(self)

    @property
    def identity(self) -> IncidentPublicationIdentity:
        handoff = self.payload.handoff
        topo_ref = handoff.topology_context.topology_snapshot
        return IncidentPublicationIdentity(
            publication_contract_version=self.payload.publication_contract_version,
            envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
            handoff_schema_version=handoff.schema_version,
            tenant_id=self.payload.tenant_id,
            incident_id=self.payload.incident_id,
            revision=self.payload.revision,
            revision_fingerprint=self.payload.revision_fingerprint,
            event_id=self.event_id,
            publication_fingerprint=self.publication_fingerprint,
            handoff_id=handoff.handoff_id,
            handoff_fingerprint=handoff.handoff_fingerprint,
            membership_fingerprint=handoff.membership_fingerprint,
            topology_id=topo_ref.topology_id,
            topology_version=topo_ref.topology_version,
            topology_snapshot_fingerprint=topo_ref.snapshot_fingerprint,
            correlation_policy_version=self.payload.correlation_policy_version,
            correlation_config_fingerprint=self.payload.correlation_config_fingerprint,
            intent_time=self.payload.intent_time,
        )

    @property
    def handoff(self) -> Phase5InvestigationHandoff:
        return self.payload.handoff

    @property
    def incident(self) -> IncidentAggregate:
        return self.payload.handoff.incident

    @property
    def value_sha256(self) -> str:
        return hashlib.sha256(self.kafka_value).hexdigest()

    @property
    def key_sha256(self) -> str:
        return hashlib.sha256(self.kafka_key).hexdigest()

    @property
    def kafka_headers_json(self) -> list[list[str]]:
        return [[name, value.decode("utf-8")] for name, value in self.kafka_headers]

    def to_telemetry_event(self) -> TelemetryEvent:
        """Return a fresh verified canonical TelemetryEvent copy preserving sorted payload keys."""
        return _verify_publication_record_integrity(self)

    @property
    def event(self) -> TelemetryEvent:
        """Fresh canonical TelemetryEvent copy (safe against caller mutation)."""
        return self.to_telemetry_event()

    def with_transport_metadata(
        self, metadata: PublicationTransportMetadata | None
    ) -> IncidentPublicationRecord:
        """Return a new record with validated transport metadata attached."""
        validated = (
            _revalidate_transport_metadata(metadata) if metadata is not None else None
        )
        return IncidentPublicationRecord(
            topic=self.topic,
            event_id=self.event_id,
            publication_fingerprint=self.publication_fingerprint,
            payload=self.payload,
            execution_context=self.execution_context,
            producer_version=self.producer_version,
            kafka_key=self.kafka_key,
            kafka_value=self.kafka_value,
            kafka_headers=self.kafka_headers,
            kafka_headers_total_bytes=self.kafka_headers_total_bytes,
            estimated_record_bytes=self.estimated_record_bytes,
            transport_metadata=validated,
        )

    def semantic_equals(self, other: object) -> bool:
        """Compare complete semantic and wire identity, excluding transport metadata."""
        if not isinstance(other, IncidentPublicationRecord):
            return False
        return (
            self.topic == other.topic
            and self.event_id == other.event_id
            and self.publication_fingerprint == other.publication_fingerprint
            and self.payload == other.payload
            and self.execution_context == other.execution_context
            and self.producer_version == other.producer_version
            and self.kafka_key == other.kafka_key
            and self.kafka_value == other.kafka_value
            and self.kafka_headers == other.kafka_headers
            and self.kafka_headers_total_bytes == other.kafka_headers_total_bytes
            and self.estimated_record_bytes == other.estimated_record_bytes
        )


@dataclass(frozen=True)
class IncidentPublicationContractResult:
    """Typed non-raising outcome for publication build, decode, and validation."""

    ok: bool
    outcome: IncidentPublicationValidationOutcome
    record: IncidentPublicationRecord | None = None
    error_code: str | None = None
    error_type: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.ok:
            if (
                self.outcome != IncidentPublicationValidationOutcome.VALID
                or self.record is None
                or self.error_code is not None
                or self.error_type is not None
                or self.error_message is not None
            ):
                raise ValueError("Invalid successful IncidentPublicationContractResult")
        else:
            if (
                self.outcome == IncidentPublicationValidationOutcome.VALID
                or self.record is not None
                or not self.error_code
                or not self.error_type
                or not self.error_message
            ):
                raise ValueError("Invalid failed IncidentPublicationContractResult")


def publications_semantically_equal(
    left: IncidentPublicationRecord,
    right: IncidentPublicationRecord,
) -> bool:
    """Return True iff two publication records are semantically and wire-byte identical.

    Transport observations (partition, offset, timestamps, latency, consumed_at,
    and dispatch bookkeeping metadata) are explicitly excluded.
    """
    if not isinstance(left, IncidentPublicationRecord) or not isinstance(
        right, IncidentPublicationRecord
    ):
        raise IncidentPublicationContractError(
            "Both arguments must be IncidentPublicationRecord instances"
        )
    return left.semantic_equals(right)


def _revalidate_transport_metadata(
    metadata: PublicationTransportMetadata,
) -> PublicationTransportMetadata:
    if not isinstance(metadata, PublicationTransportMetadata):
        raise IncidentPublicationContractError(
            "transport_metadata must be a PublicationTransportMetadata instance"
        )
    try:
        return _fresh(metadata, PublicationTransportMetadata)
    except Exception as exc:
        raise IncidentPublicationContractError("Invalid transport_metadata") from exc


# ============================================================================
# BOUNDED HEADER & KEY VALIDATION AND CONSTRUCTION
# ============================================================================


def validate_publication_execution_context(
    context: TelemetryExecutionContext,
) -> PublicationExecutionLineage:
    """Strictly validate TelemetryExecutionContext against A1-R1 header bounds."""
    return _revalidate_execution_context(context)


def validate_publication_producer_version(
    producer_version: str | None,
) -> str | None:
    """Strictly validate optional producer_version against A1-R1 header bounds."""
    if producer_version is None:
        return None
    return _validate_exact_trimmed_utf8(
        producer_version,
        field_name="producer_version",
        max_bytes=MAX_PRODUCER_VERSION_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )


def build_publication_kafka_headers(
    context: TelemetryExecutionContext,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
) -> tuple[tuple[str, bytes], ...]:
    """Construct exact ordered, bounded Kafka headers via frozen transport helper."""
    lineage = validate_publication_execution_context(context)
    validated_pv = validate_publication_producer_version(producer_version)
    clean_context = lineage.to_execution_context()
    raw_headers = construct_kafka_headers(clean_context, validated_pv)
    _, _, validated_headers, _ = parse_and_validate_publication_headers(raw_headers)
    return validated_headers


def parse_and_validate_publication_headers(
    headers: Sequence[tuple[str, bytes]] | None,
) -> tuple[
    TelemetryExecutionContext,
    str | None,
    tuple[tuple[str, bytes], ...],
    int,
]:
    """Parse and strictly validate ordered, bounded incident publication headers."""
    if headers is None:
        raise IncidentPublicationHeaderError("Kafka headers are missing")
    if isinstance(headers, (str, bytes, bytearray, memoryview)) or not isinstance(
        headers, Sequence
    ):
        raise IncidentPublicationHeaderError(
            "Kafka headers must be an ordered sequence of (name, bytes) pairs"
        )

    normalized_pairs: list[tuple[str, bytes]] = []
    seen_names: set[str] = set()
    total_bytes = 0

    for item in headers:
        if (
            not isinstance(item, tuple)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], (bytes, bytearray, memoryview))
        ):
            raise IncidentPublicationHeaderError(
                "Each Kafka header must be a (str, bytes) tuple"
            )
        name = item[0]
        raw_val = bytes(item[1])
        name_bytes_len = len(
            _encode_utf8_strict(
                name,
                field_name="header_name",
                error_cls=IncidentPublicationHeaderError,
            )
        )
        val_bytes_len = len(raw_val)

        total_bytes += name_bytes_len + val_bytes_len
        if total_bytes > MAX_KAFKA_HEADERS_TOTAL_BYTES:
            raise IncidentPublicationSizeLimitError(
                "Total Kafka headers byte size exceeds maximum limit (1024 bytes)"
            )
        if val_bytes_len > MAX_KAFKA_HEADER_VALUE_BYTES:
            raise IncidentPublicationSizeLimitError(
                f"Kafka header '{name}' exceeds single-header byte limit (256 bytes)"
            )
        if name in seen_names:
            raise IncidentPublicationHeaderError(f"Duplicate Kafka header: '{name}'")
        seen_names.add(name)
        normalized_pairs.append((name, raw_val))

    names_order = tuple(name for name, _ in normalized_pairs)
    expected_5 = REQUIRED_PUBLICATION_HEADER_NAMES
    expected_6 = ORDERED_PUBLICATION_HEADER_NAMES
    if names_order not in (expected_5, expected_6):
        missing = [req for req in expected_5 if req not in seen_names]
        if missing:
            raise IncidentPublicationHeaderError(
                f"Missing required Kafka header(s): {', '.join(missing)}"
            )
        unknown = [n for n in names_order if n not in expected_6]
        if unknown:
            raise IncidentPublicationHeaderError(
                f"Unknown Kafka header(s): {', '.join(unknown)}"
            )
        raise IncidentPublicationHeaderError(
            "Kafka headers must match exact approved order"
        )

    decoded: dict[str, str] = {}
    for name, raw_val in normalized_pairs:
        max_len = HEADER_FIELD_MAX_BYTES[name]
        if len(raw_val) > max_len:
            raise IncidentPublicationSizeLimitError(
                f"Kafka header '{name}' exceeds maximum byte limit ({max_len} bytes)"
            )
        if len(raw_val) == 0:
            raise IncidentPublicationHeaderError(
                f"Kafka header '{name}' must not be empty"
            )
        try:
            text_val = raw_val.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IncidentPublicationHeaderError(
                f"Kafka header '{name}' contains invalid UTF-8"
            ) from exc
        decoded[name] = text_val

    run_id = _validate_canonical_uuid_str(
        decoded["run_id"],
        field_name="run_id",
        error_cls=IncidentPublicationHeaderError,
    )
    scenario_id = _validate_exact_trimmed_utf8(
        decoded["scenario_id"],
        field_name="scenario_id",
        max_bytes=MAX_SCENARIO_ID_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )
    scenario_version = _validate_exact_trimmed_utf8(
        decoded["scenario_version"],
        field_name="scenario_version",
        max_bytes=MAX_SCENARIO_VERSION_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )
    reproducibility_key = _validate_exact_trimmed_utf8(
        decoded["reproducibility_key"],
        field_name="reproducibility_key",
        max_bytes=MAX_REPRODUCIBILITY_KEY_BYTES,
        size_error_cls=IncidentPublicationSizeLimitError,
        format_error_cls=IncidentPublicationHeaderError,
    )

    seed_str = decoded["seed"]
    if (
        not seed_str
        or not seed_str.isascii()
        or not seed_str.isdigit()
        or (len(seed_str) > 1 and seed_str.startswith("0"))
    ):
        raise IncidentPublicationHeaderError(
            "Kafka header 'seed' must be a canonical base-10 integer"
        )
    seed_val = int(seed_str)
    if not (0 <= seed_val <= MAX_UINT64) or str(seed_val) != seed_str:
        raise IncidentPublicationHeaderError(
            "Kafka header 'seed' must be a canonical base-10 integer in [0, 2^64 - 1]"
        )

    producer_version: str | None = None
    if OPTIONAL_PUBLICATION_HEADER_NAME in decoded:
        producer_version = _validate_exact_trimmed_utf8(
            decoded[OPTIONAL_PUBLICATION_HEADER_NAME],
            field_name="producer_version",
            max_bytes=MAX_PRODUCER_VERSION_BYTES,
            size_error_cls=IncidentPublicationSizeLimitError,
            format_error_cls=IncidentPublicationHeaderError,
        )

    # Reuse frozen transport parser to guarantee transport compatibility.
    try:
        parsed_context, parsed_pv = parse_kafka_headers(normalized_pairs)
    except Exception as exc:
        raise IncidentPublicationHeaderError(
            "Frozen transport header validation failed"
        ) from exc

    expected_context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=scenario_id,
        scenario_version=scenario_version,
        reproducibility_key=reproducibility_key,
        seed=seed_val,
    )
    if parsed_context != expected_context or parsed_pv != producer_version:
        raise IncidentPublicationHeaderError(
            "Header normalization mismatch with frozen transport parser"
        )

    return expected_context, producer_version, tuple(normalized_pairs), total_bytes


def build_publication_kafka_key(event: TelemetryEvent) -> bytes:
    """Construct and validate bounded Kafka partition routing key via frozen helper."""
    if not isinstance(event, TelemetryEvent):
        raise IncidentPublicationContractError("event must be a TelemetryEvent")
    try:
        key = construct_kafka_key(event)
    except UnicodeEncodeError as exc:
        raise IncidentPublicationContractError(
            "Kafka routing key contains invalid Unicode"
        ) from exc
    if len(key) == 0:
        raise IncidentPublicationContractError("Kafka routing key must not be empty")
    if len(key) > MAX_KAFKA_KEY_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Kafka routing key exceeds maximum byte limit (512 bytes)"
        )
    return key


def _build_canonical_event_from_payload(
    *,
    payload: IncidentPublicationPayload,
    handoff_bytes: bytes,
) -> TelemetryEvent:
    projection = build_publication_fingerprint_projection(
        event_id=payload.event_id,
        handoff=payload.handoff,
        correlation_policy_version=payload.correlation_policy_version,
        correlation_config_fingerprint=payload.correlation_config_fingerprint,
        execution_lineage=payload.execution_lineage,
        producer_version=payload.producer_version,
        intent_time=payload.intent_time,
    )
    payload_dict: dict[str, Any] = {
        key: projection[key] for key in PAYLOAD_TOP_LEVEL_KEYS if key in projection
    }
    payload_dict["handoff"] = json.loads(handoff_bytes.decode("utf-8"))
    payload_dict["publication_fingerprint"] = payload.publication_fingerprint
    canonical_payload_bytes = _canonical_json_bytes_bounded(
        payload_dict, MAX_KAFKA_VALUE_BYTES
    )
    sorted_payload_obj = cast(dict[str, Any], json.loads(canonical_payload_bytes))
    incident = payload.handoff.incident
    return TelemetryEvent(
        schema_version=ENVELOPE_SCHEMA_VERSION,
        event_id=payload.event_id,
        event_time=payload.intent_time,
        tenant_id=incident.tenant_id,
        environment=incident.environment,
        service=incident.primary_service,
        event_type=INCIDENT_PUBLICATION_EVENT_TYPE,
        severity=incident.severity,
        trace_id=None,
        payload=sorted_payload_obj,
    )


def _verify_publication_record_integrity(
    record: IncidentPublicationRecord,
) -> TelemetryEvent:
    """Verify full canonical wire, header, key, topic and event_type integrity."""
    if not isinstance(record, IncidentPublicationRecord):
        raise IncidentPublicationContractError(
            "record must be an IncidentPublicationRecord instance"
        )
    if (
        not isinstance(record.topic, KafkaTopic)
        or record.topic != INCIDENT_PUBLICATION_TOPIC
    ):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord topic must be KafkaTopic.INCIDENTS"
        )
    if not isinstance(record.kafka_key, bytes):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_key must be bytes"
        )
    if not isinstance(record.kafka_value, bytes):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_value must be bytes"
        )
    if not isinstance(record.kafka_headers, tuple):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_headers must be a tuple"
        )
    if len(record.kafka_key) == 0:
        raise IncidentPublicationContractError("Kafka routing key must not be empty")
    if len(record.kafka_key) > MAX_KAFKA_KEY_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Kafka routing key exceeds maximum byte limit (512 bytes)"
        )
    if len(record.kafka_value) == 0:
        raise IncidentPublicationContractError("Kafka value must not be empty")
    if len(record.kafka_value) > MAX_KAFKA_VALUE_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Serialized Kafka value exceeds maximum limit (983040 bytes)"
        )
    if not isinstance(record.payload, IncidentPublicationPayload):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord payload must be an IncidentPublicationPayload"
        )
    try:
        fresh_payload = _fresh(record.payload, IncidentPublicationPayload)
    except IncidentPublicationError:
        raise
    except Exception as exc:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord payload failed integrity revalidation"
        ) from exc

    if record.event_id != fresh_payload.event_id:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord event_id does not match payload"
        )
    if record.publication_fingerprint != fresh_payload.publication_fingerprint:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord publication_fingerprint does not match payload"
        )
    if record.producer_version != fresh_payload.producer_version:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord producer_version does not match payload"
        )

    lineage = validate_publication_execution_context(record.execution_context)
    if lineage != fresh_payload.execution_lineage:
        raise IncidentPublicationValidationError(
            "IncidentPublicationRecord execution_context does not match payload"
        )

    (
        hdr_context,
        hdr_pv,
        normalized_headers,
        headers_total_bytes,
    ) = parse_and_validate_publication_headers(record.kafka_headers)
    if hdr_context != record.execution_context or hdr_pv != record.producer_version:
        raise IncidentPublicationValidationError(
            "IncidentPublicationRecord kafka_headers do not match execution_context or producer_version"
        )
    if normalized_headers != record.kafka_headers:
        raise IncidentPublicationHeaderError(
            "IncidentPublicationRecord kafka_headers are not normalized"
        )
    if (
        not _is_strict_int(record.kafka_headers_total_bytes)
        or record.kafka_headers_total_bytes != headers_total_bytes
    ):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_headers_total_bytes mismatch"
        )

    env_obj = _decode_strict_json_object(record.kafka_value, MAX_KAFKA_VALUE_BYTES)
    if set(env_obj.keys()) != ENVELOPE_TOP_LEVEL_KEYS:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_value keys do not match TelemetryEvent contract"
        )
    _check_declared_versions(env_obj)
    if env_obj.get("event_type") != INCIDENT_PUBLICATION_EVENT_TYPE.value:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_value event_type must be 'incident'"
        )
    if env_obj.get("trace_id") is not None:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_value trace_id must be null"
        )

    try:
        event = deserialize_event(record.kafka_value)
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to deserialize IncidentPublicationRecord kafka_value"
        ) from exc

    if event.schema_version != ENVELOPE_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported envelope schema_version: {event.schema_version!r}"
        )
    if event.event_type != INCIDENT_PUBLICATION_EVENT_TYPE:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord event_type must be EventType.INCIDENT"
        )
    if EVENT_TYPE_TO_TOPIC[event.event_type] != INCIDENT_PUBLICATION_TOPIC:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord event_type does not route to aegis.incidents"
        )

    handoff_bytes = serialize_phase5_handoff(fresh_payload.handoff)
    expected_event = _build_canonical_event_from_payload(
        payload=fresh_payload, handoff_bytes=handoff_bytes
    )
    expected_key = build_publication_kafka_key(expected_event)
    if record.kafka_key != expected_key:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_key does not match canonical routing key"
        )
    try:
        expected_value = serialize_event(expected_event)
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to serialize canonical TelemetryEvent for integrity verification"
        ) from exc
    if record.kafka_value != expected_value:
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord kafka_value does not match canonical serialized event"
        )

    estimated = _estimate_kafka_record_bytes(
        record.kafka_key, record.kafka_value, record.kafka_headers
    )
    if estimated > MAX_KAFKA_RECORD_ESTIMATED_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Estimated Kafka record size exceeds maximum limit (1048576 bytes)"
        )
    if (
        not _is_strict_int(record.estimated_record_bytes)
        or record.estimated_record_bytes != estimated
    ):
        raise IncidentPublicationContractError(
            "IncidentPublicationRecord estimated_record_bytes mismatch"
        )

    if record.transport_metadata is not None:
        _revalidate_transport_metadata(record.transport_metadata)

    return event


# ============================================================================
# INCIDENT / HANDOFF -> TELEMETRY EVENT & WIRE PUBLICATION CONVERSION
# ============================================================================


def _build_wire_record_from_validated_handoff(
    *,
    handoff: Phase5InvestigationHandoff,
    handoff_bytes: bytes,
    correlation_policy_version: str,
    correlation_config_fingerprint: str,
    lineage: PublicationExecutionLineage,
    producer_version: str | None,
    intent_time: datetime,
    transport_metadata: PublicationTransportMetadata | None = None,
) -> IncidentPublicationRecord:
    if len(handoff_bytes) > MAX_KAFKA_VALUE_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Serialized handoff exceeds Kafka publication value limit (983040 bytes)"
        )

    incident = handoff.incident
    if incident.schema_version != INCIDENT_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported incident schema_version: {incident.schema_version!r}"
        )
    if handoff.schema_version != HANDOFF_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported handoff schema_version: {handoff.schema_version!r}"
        )
    if intent_time != incident.updated_at or intent_time != handoff.generated_at:
        raise IncidentPublicationValidationError(
            "intent_time must match incident.updated_at and handoff.generated_at"
        )

    event_id = derive_incident_publication_event_id(
        tenant_id=incident.tenant_id,
        incident_id=incident.incident_id,
        revision=incident.revision,
        revision_fingerprint=incident.revision_fingerprint,
        publication_contract_version=PUBLICATION_CONTRACT_VERSION,
    )
    projection = build_publication_fingerprint_projection(
        event_id=event_id,
        handoff=handoff,
        correlation_policy_version=correlation_policy_version,
        correlation_config_fingerprint=correlation_config_fingerprint,
        execution_lineage=lineage,
        producer_version=producer_version,
        intent_time=intent_time,
    )
    publication_fingerprint = compute_incident_publication_fingerprint(projection)

    payload_dict: dict[str, Any] = {
        key: projection[key] for key in PAYLOAD_TOP_LEVEL_KEYS if key in projection
    }
    payload_dict["handoff"] = json.loads(handoff_bytes.decode("utf-8"))
    payload_dict["publication_fingerprint"] = publication_fingerprint

    canonical_payload_bytes = _canonical_json_bytes_bounded(
        payload_dict, MAX_KAFKA_VALUE_BYTES
    )
    try:
        payload_model = IncidentPublicationPayload.model_validate_json(
            canonical_payload_bytes
        )
    except IncidentPublicationError:
        raise
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Incident publication payload validation failed"
        ) from exc

    sorted_payload_obj = cast(dict[str, Any], json.loads(canonical_payload_bytes))
    event = TelemetryEvent(
        schema_version=ENVELOPE_SCHEMA_VERSION,
        event_id=event_id,
        event_time=intent_time,
        tenant_id=incident.tenant_id,
        environment=incident.environment,
        service=incident.primary_service,
        event_type=INCIDENT_PUBLICATION_EVENT_TYPE,
        severity=incident.severity,
        trace_id=None,
        payload=sorted_payload_obj,
    )

    execution_context = lineage.to_execution_context()
    kafka_headers = build_publication_kafka_headers(execution_context, producer_version)
    headers_total_bytes = sum(
        len(name.encode("utf-8")) + len(val) for name, val in kafka_headers
    )
    kafka_key = build_publication_kafka_key(event)

    try:
        kafka_value = serialize_event(event)
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to serialize canonical TelemetryEvent"
        ) from exc

    if len(kafka_value) > MAX_KAFKA_VALUE_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Serialized Kafka value exceeds maximum limit (983040 bytes)"
        )

    estimated_record_bytes = _estimate_kafka_record_bytes(
        kafka_key, kafka_value, kafka_headers
    )
    if estimated_record_bytes > MAX_KAFKA_RECORD_ESTIMATED_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Estimated Kafka record size exceeds maximum limit (1048576 bytes)"
        )

    validated_transport = (
        _revalidate_transport_metadata(transport_metadata)
        if transport_metadata is not None
        else None
    )

    return IncidentPublicationRecord(
        topic=INCIDENT_PUBLICATION_TOPIC,
        event_id=event_id,
        publication_fingerprint=publication_fingerprint,
        payload=payload_model,
        execution_context=execution_context,
        producer_version=producer_version,
        kafka_key=kafka_key,
        kafka_value=kafka_value,
        kafka_headers=kafka_headers,
        kafka_headers_total_bytes=headers_total_bytes,
        estimated_record_bytes=estimated_record_bytes,
        transport_metadata=validated_transport,
    )


def build_incident_publication_from_handoff(
    *,
    handoff: Phase5InvestigationHandoff,
    correlation_config: CorrelationConfig,
    execution_context: TelemetryExecutionContext,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
    intent_time: datetime | None = None,
    expected_topology: TopologySnapshotReference | None = None,
    snapshot: TopologySnapshot | None = None,
    expected_blast_radius_policy: BlastRadiusPolicy | None = None,
) -> IncidentPublicationRecord:
    """Convert an approved Phase5InvestigationHandoff into a bounded Kafka publication record."""
    if not isinstance(handoff, Phase5InvestigationHandoff):
        raise IncidentPublicationContractError(
            "handoff must be a Phase5InvestigationHandoff instance"
        )
    try:
        raw_handoff = serialize_phase5_handoff(handoff)
        validated_handoff = decode_phase5_handoff_structural(raw_handoff)
    except Phase5HandoffContractError as exc:
        if any(isinstance(c, BlastRadiusLimitError) for c in _extract_cause_chain(exc)):
            raise IncidentPublicationSizeLimitError(
                "Handoff exceeds size limit"
            ) from exc
        raise IncidentPublicationContractError(
            "Invalid Phase5InvestigationHandoff"
        ) from exc

    if expected_topology is not None:
        if not isinstance(expected_topology, TopologySnapshotReference):
            raise IncidentPublicationValidationError(
                "expected_topology must be a TopologySnapshotReference"
            )
        try:
            fresh_expected_topo = _fresh(expected_topology, TopologySnapshotReference)
        except Exception as exc:
            raise IncidentPublicationValidationError(
                "Invalid expected_topology reference"
            ) from exc
        actual_topo = validated_handoff.topology_context.topology_snapshot
        if (
            actual_topo.topology_id,
            actual_topo.topology_version,
            actual_topo.snapshot_fingerprint,
            actual_topo.captured_at,
        ) != (
            fresh_expected_topo.topology_id,
            fresh_expected_topo.topology_version,
            fresh_expected_topo.snapshot_fingerprint,
            fresh_expected_topo.captured_at,
        ):
            raise IncidentPublicationValidationError(
                "Handoff topology pin does not match expected_topology"
            )
        if snapshot is not None or expected_blast_radius_policy is not None:
            if snapshot is None or expected_blast_radius_policy is None:
                raise IncidentPublicationValidationError(
                    "Both snapshot and expected_blast_radius_policy are required for semantic handoff validation"
                )
            try:
                validated_handoff = validate_phase5_handoff(
                    raw=raw_handoff,
                    snapshot=snapshot,
                    expected_topology=fresh_expected_topo,
                    expected_policy=expected_blast_radius_policy,
                )
            except Phase5HandoffContractError as exc:
                raise IncidentPublicationValidationError(
                    "Semantic handoff validation failed"
                ) from exc
    elif snapshot is not None or expected_blast_radius_policy is not None:
        raise IncidentPublicationValidationError(
            "expected_topology is required when snapshot or expected_blast_radius_policy is supplied"
        )

    fresh_config, config_fingerprint = _revalidate_correlation_config(
        correlation_config
    )
    lineage = validate_publication_execution_context(execution_context)
    validated_pv = validate_publication_producer_version(producer_version)

    effective_intent_time = (
        _validate_utc_datetime(intent_time, field_name="intent_time")
        if intent_time is not None
        else _validate_utc_datetime(
            validated_handoff.incident.updated_at, field_name="intent_time"
        )
    )

    return _build_wire_record_from_validated_handoff(
        handoff=validated_handoff,
        handoff_bytes=raw_handoff,
        correlation_policy_version=fresh_config.policy_version,
        correlation_config_fingerprint=config_fingerprint,
        lineage=lineage,
        producer_version=validated_pv,
        intent_time=effective_intent_time,
    )


def build_incident_publication(
    *,
    incident: IncidentAggregate,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    blast_radius_policy: BlastRadiusPolicy,
    correlation_config: CorrelationConfig,
    execution_context: TelemetryExecutionContext,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
    intent_time: datetime | None = None,
) -> IncidentPublicationRecord:
    """Assemble and serialize a committed IncidentAggregate into a Kafka publication record."""
    if not isinstance(incident, IncidentAggregate):
        raise IncidentPublicationContractError(
            "incident must be an IncidentAggregate instance"
        )
    if not isinstance(snapshot, TopologySnapshot):
        raise IncidentPublicationValidationError(
            "snapshot must be a TopologySnapshot instance"
        )
    if not isinstance(expected_topology, TopologySnapshotReference):
        raise IncidentPublicationValidationError(
            "expected_topology must be a TopologySnapshotReference instance"
        )
    if not isinstance(blast_radius_policy, BlastRadiusPolicy):
        raise IncidentPublicationValidationError(
            "blast_radius_policy must be a BlastRadiusPolicy instance"
        )

    raw_schema_version = incident.__dict__.get("schema_version")
    if raw_schema_version != INCIDENT_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported incident schema_version: {raw_schema_version!r}"
        )

    try:
        fresh_incident = _fresh(incident, IncidentAggregate)
        source_bytes = serialize_incident_aggregate(incident)
        canonical_incident = deserialize_incident_aggregate(
            serialize_incident_aggregate(fresh_incident)
        )
        if serialize_incident_aggregate(canonical_incident) != source_bytes:
            raise ValueError("Incident aggregate is not canonical")
    except IncidentPublicationError:
        raise
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Invalid or non-canonical IncidentAggregate"
        ) from exc

    if len(source_bytes) > MAX_KAFKA_VALUE_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Serialized incident aggregate exceeds Kafka publication value limit (983040 bytes)"
        )

    effective_intent_time = (
        _validate_utc_datetime(intent_time, field_name="intent_time")
        if intent_time is not None
        else _validate_utc_datetime(
            canonical_incident.updated_at, field_name="intent_time"
        )
    )
    if effective_intent_time != canonical_incident.updated_at:
        raise IncidentPublicationValidationError(
            "intent_time must equal incident.updated_at"
        )

    fresh_config, _ = _revalidate_correlation_config(correlation_config)
    validate_publication_execution_context(execution_context)
    validated_pv = validate_publication_producer_version(producer_version)

    try:
        handoff = build_phase5_handoff(
            incident=canonical_incident,
            snapshot=snapshot,
            expected_topology=expected_topology,
            policy=blast_radius_policy,
            as_of=effective_intent_time,
        )
    except Phase5HandoffContractError as exc:
        causes = _extract_cause_chain(exc)
        if any(isinstance(c, BlastRadiusLimitError) for c in causes):
            raise IncidentPublicationSizeLimitError(
                "Blast-radius or handoff limit exceeded"
            ) from exc
        if any(isinstance(c, BlastRadiusTopologyMismatchError) for c in causes):
            raise IncidentPublicationValidationError(
                "Topology snapshot or age policy mismatch"
            ) from exc
        raise IncidentPublicationValidationError(
            "Failed to assemble topology-pinned Phase 5 handoff"
        ) from exc

    return build_incident_publication_from_handoff(
        handoff=handoff,
        correlation_config=fresh_config,
        execution_context=execution_context,
        producer_version=validated_pv,
        intent_time=effective_intent_time,
        expected_topology=expected_topology,
    )


def build_incident_publication_from_intent(
    *,
    intent: PersistedPublicationIntent,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    blast_radius_policy: BlastRadiusPolicy,
    correlation_config: CorrelationConfig,
    execution_context: TelemetryExecutionContext,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
) -> IncidentPublicationRecord:
    """Convert an immutable PersistedPublicationIntent into a Kafka publication record."""
    if not isinstance(intent, PersistedPublicationIntent):
        raise IncidentPublicationContractError(
            "intent must be a PersistedPublicationIntent instance"
        )
    if intent.schema_version != SUPPORTED_INCIDENT_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported intent schema_version: {intent.schema_version!r}"
        )
    if not isinstance(intent.aggregate, IncidentAggregate):
        raise IncidentPublicationContractError(
            "intent.aggregate must be an IncidentAggregate instance"
        )
    if not _is_strict_int(intent.revision):
        raise IncidentPublicationContractError(
            "PersistedPublicationIntent revision must be an integer"
        )
    agg = intent.aggregate
    if (
        intent.tenant_id != agg.tenant_id
        or intent.incident_id != agg.incident_id
        or intent.revision != agg.revision
        or intent.revision_fingerprint != agg.revision_fingerprint
    ):
        raise IncidentPublicationValidationError(
            "PersistedPublicationIntent metadata does not match embedded aggregate"
        )
    intent_time = _validate_utc_datetime(intent.intent_time, field_name="intent_time")
    if intent_time != agg.updated_at:
        raise IncidentPublicationValidationError(
            "PersistedPublicationIntent intent_time does not match aggregate.updated_at"
        )

    return build_incident_publication(
        incident=agg,
        snapshot=snapshot,
        expected_topology=expected_topology,
        blast_radius_policy=blast_radius_policy,
        correlation_config=correlation_config,
        execution_context=execution_context,
        producer_version=producer_version,
        intent_time=intent_time,
    )


def incident_publication_to_telemetry_event(
    record: IncidentPublicationRecord,
) -> TelemetryEvent:
    """Return a fresh canonical TelemetryEvent from a validated publication record."""
    if not isinstance(record, IncidentPublicationRecord):
        raise IncidentPublicationContractError(
            "record must be an IncidentPublicationRecord instance"
        )
    return record.to_telemetry_event()


# ============================================================================
# STRUCTURAL DECODING & SEMANTIC CONSUMER VALIDATION
# ============================================================================


def _check_declared_versions(envelope_obj: Mapping[str, Any]) -> None:
    if "schema_version" not in envelope_obj:
        raise IncidentPublicationVersionError(
            "Missing required envelope schema_version"
        )
    if envelope_obj["schema_version"] != ENVELOPE_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported envelope schema_version: {envelope_obj['schema_version']!r}"
        )

    payload_obj = envelope_obj.get("payload")
    if not isinstance(payload_obj, dict):
        raise IncidentPublicationContractError("Envelope payload must be a JSON object")

    if "publication_contract_version" not in payload_obj:
        raise IncidentPublicationVersionError(
            "Missing required publication_contract_version"
        )
    if payload_obj["publication_contract_version"] != PUBLICATION_CONTRACT_VERSION:
        raise IncidentPublicationVersionError(
            "Unsupported publication_contract_version: "
            f"{payload_obj['publication_contract_version']!r}"
        )

    handoff_obj = payload_obj.get("handoff")
    if not isinstance(handoff_obj, dict):
        raise IncidentPublicationContractError("Payload handoff must be a JSON object")
    if handoff_obj.get("schema_version") != HANDOFF_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported handoff schema_version: {handoff_obj.get('schema_version')!r}"
        )

    incident_obj = handoff_obj.get("incident")
    if not isinstance(incident_obj, dict):
        raise IncidentPublicationContractError("Handoff incident must be a JSON object")
    if incident_obj.get("schema_version") != INCIDENT_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            "Unsupported incident schema_version: "
            f"{incident_obj.get('schema_version')!r}"
        )

    topo_ctx_obj = handoff_obj.get("topology_context")
    if not isinstance(topo_ctx_obj, dict):
        raise IncidentPublicationContractError(
            "Handoff topology_context must be a JSON object"
        )
    if topo_ctx_obj.get("schema_version") != BLAST_RADIUS_CONTEXT_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            "Unsupported blast-radius context schema_version: "
            f"{topo_ctx_obj.get('schema_version')!r}"
        )

    policy_obj = topo_ctx_obj.get("policy")
    if not isinstance(policy_obj, dict):
        raise IncidentPublicationContractError(
            "Handoff topology_context.policy must be a JSON object"
        )
    if policy_obj.get("policy_version") != BLAST_RADIUS_POLICY_VERSION:
        raise IncidentPublicationVersionError(
            "Unsupported blast-radius policy_version: "
            f"{policy_obj.get('policy_version')!r}"
        )

    provenance_obj = handoff_obj.get("provenance")
    if not isinstance(provenance_obj, dict):
        raise IncidentPublicationContractError(
            "Handoff provenance must be a JSON object"
        )
    if provenance_obj.get("producer_version") != HANDOFF_PROVENANCE_PRODUCER_VERSION:
        raise IncidentPublicationVersionError(
            "Unsupported handoff provenance producer_version: "
            f"{provenance_obj.get('producer_version')!r}"
        )


def _check_canonical_json_uuids(envelope_obj: Mapping[str, Any]) -> None:
    _validate_canonical_uuid_str(
        envelope_obj.get("event_id"), field_name="envelope.event_id"
    )
    _validate_canonical_uuid_str(
        envelope_obj.get("tenant_id"), field_name="envelope.tenant_id"
    )
    payload_obj = envelope_obj["payload"]
    _validate_canonical_uuid_str(
        payload_obj.get("event_id"), field_name="payload.event_id"
    )
    _validate_canonical_uuid_str(
        payload_obj.get("tenant_id"), field_name="payload.tenant_id"
    )
    _validate_canonical_uuid_str(
        payload_obj.get("incident_id"), field_name="payload.incident_id"
    )
    lineage_obj = payload_obj.get("execution_lineage")
    if not isinstance(lineage_obj, dict):
        raise IncidentPublicationContractError(
            "payload.execution_lineage must be a JSON object"
        )
    if set(lineage_obj.keys()) != set(EXECUTION_LINEAGE_KEYS):
        raise IncidentPublicationContractError(
            "payload.execution_lineage keys do not match approved allowlist"
        )
    _validate_canonical_uuid_str(
        lineage_obj.get("run_id"),
        field_name="payload.execution_lineage.run_id",
        error_cls=IncidentPublicationContractError,
    )


def decode_incident_publication_structural(
    *,
    raw_value: bytes | bytearray | memoryview,
    raw_key: bytes | bytearray | memoryview,
    headers: Sequence[tuple[str, bytes]] | None,
    topic: str | KafkaTopic = INCIDENT_PUBLICATION_TOPIC,
    transport_metadata: PublicationTransportMetadata | None = None,
    require_canonical_bytes: bool = False,
) -> IncidentPublicationRecord:
    """Decode and structurally verify a Kafka incident publication record.

    Does not authenticate topology graph reachability against an external
    snapshot; consumers use ``validate_incident_publication`` for full
    semantic verification.
    """
    topic_str = topic.value if isinstance(topic, KafkaTopic) else topic
    if not isinstance(topic_str, str) or topic_str != INCIDENT_PUBLICATION_TOPIC.value:
        raise IncidentPublicationContractError(
            f"Invalid incident publication topic: {topic_str!r}"
        )

    if not isinstance(raw_value, (bytes, bytearray, memoryview)):
        raise IncidentPublicationContractError("raw_value must be bytes-like")
    if not isinstance(raw_key, (bytes, bytearray, memoryview)):
        raise IncidentPublicationContractError("raw_key must be bytes-like")

    raw_value_bytes = bytes(raw_value)
    raw_key_bytes = bytes(raw_key)

    if len(raw_value_bytes) == 0:
        raise IncidentPublicationContractError("raw_value must not be empty")
    if len(raw_value_bytes) > MAX_KAFKA_VALUE_BYTES:
        raise IncidentPublicationSizeLimitError(
            "raw_value exceeds maximum Kafka value byte limit (983040 bytes)"
        )
    if len(raw_key_bytes) == 0:
        raise IncidentPublicationContractError("raw_key must not be empty")
    if len(raw_key_bytes) > MAX_KAFKA_KEY_BYTES:
        raise IncidentPublicationSizeLimitError(
            "raw_key exceeds maximum Kafka key byte limit (512 bytes)"
        )

    (
        header_context,
        header_producer_version,
        normalized_headers,
        _,
    ) = parse_and_validate_publication_headers(headers)

    raw_estimated_bytes = _estimate_kafka_record_bytes(
        raw_key_bytes, raw_value_bytes, normalized_headers
    )
    if raw_estimated_bytes > MAX_KAFKA_RECORD_ESTIMATED_BYTES:
        raise IncidentPublicationSizeLimitError(
            "Estimated Kafka record size exceeds maximum limit (1048576 bytes)"
        )

    envelope_obj = _decode_strict_json_object(raw_value_bytes, MAX_KAFKA_VALUE_BYTES)

    if set(envelope_obj.keys()) != ENVELOPE_TOP_LEVEL_KEYS:
        if "schema_version" not in envelope_obj:
            raise IncidentPublicationVersionError(
                "Missing required envelope schema_version"
            )
        raise IncidentPublicationContractError(
            "Envelope top-level keys do not match canonical TelemetryEvent contract"
        )

    _check_declared_versions(envelope_obj)

    if envelope_obj.get("event_type") != INCIDENT_PUBLICATION_EVENT_TYPE.value:
        raise IncidentPublicationContractError("Envelope event_type must be 'incident'")
    if envelope_obj.get("trace_id") is not None:
        raise IncidentPublicationContractError(
            "Incident publication envelope trace_id must be null"
        )
    _validate_exact_trimmed_utf8(
        envelope_obj.get("environment"),
        field_name="envelope.environment",
        max_bytes=128,
    )
    _validate_exact_trimmed_utf8(
        envelope_obj.get("service"),
        field_name="envelope.service",
        max_bytes=128,
    )
    if not isinstance(envelope_obj.get("event_time"), str):
        raise IncidentPublicationContractError(
            "envelope.event_time must be an ISO-8601 UTC string"
        )
    _check_canonical_json_uuids(envelope_obj)

    # Delegate envelope parsing to frozen transport deserializer.
    try:
        envelope_event = deserialize_event(raw_value_bytes)
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to deserialize canonical TelemetryEvent"
        ) from exc

    if envelope_event.schema_version != ENVELOPE_SCHEMA_VERSION:
        raise IncidentPublicationVersionError(
            f"Unsupported envelope schema_version: {envelope_event.schema_version!r}"
        )
    if envelope_event.event_type != INCIDENT_PUBLICATION_EVENT_TYPE:
        raise IncidentPublicationContractError("Envelope event_type must be 'incident'")
    if envelope_event.trace_id is not None:
        raise IncidentPublicationContractError("Envelope trace_id must be None")
    envelope_event_time = _validate_utc_datetime(
        envelope_event.event_time, field_name="envelope.event_time"
    )

    payload_obj = envelope_obj["payload"]
    if set(payload_obj.keys()) != set(PAYLOAD_TOP_LEVEL_KEYS):
        raise IncidentPublicationContractError(
            "Payload top-level keys do not match approved 14-key allowlist"
        )

    canonical_payload_bytes = _canonical_json_bytes_bounded(
        payload_obj, MAX_KAFKA_VALUE_BYTES
    )
    try:
        payload = IncidentPublicationPayload.model_validate_json(
            canonical_payload_bytes
        )
    except IncidentPublicationError:
        raise
    except Exception as exc:
        raise IncidentPublicationContractError(
            "IncidentPublicationPayload structural validation failed"
        ) from exc

    incident = payload.handoff.incident
    if (
        envelope_event.event_id != payload.event_id
        or envelope_event_time != payload.intent_time
        or envelope_event.tenant_id != payload.tenant_id
        or envelope_event.environment != incident.environment
        or envelope_event.service != incident.primary_service
        or envelope_event.severity != incident.severity
    ):
        raise IncidentPublicationContractError(
            "Envelope fields do not match embedded publication payload"
        )

    if payload.execution_lineage.to_execution_context() != header_context:
        raise IncidentPublicationValidationError(
            "Kafka execution headers do not match payload execution_lineage"
        )
    if payload.producer_version != header_producer_version:
        raise IncidentPublicationValidationError(
            "Kafka producer_version header does not match payload producer_version"
        )

    expected_key = build_publication_kafka_key(envelope_event)
    if raw_key_bytes != expected_key:
        raise IncidentPublicationContractError(
            "Kafka routing key does not match canonical tenant:environment:service key"
        )

    handoff_bytes = serialize_phase5_handoff(payload.handoff)
    record = _build_wire_record_from_validated_handoff(
        handoff=payload.handoff,
        handoff_bytes=handoff_bytes,
        correlation_policy_version=payload.correlation_policy_version,
        correlation_config_fingerprint=payload.correlation_config_fingerprint,
        lineage=payload.execution_lineage,
        producer_version=payload.producer_version,
        intent_time=payload.intent_time,
        transport_metadata=transport_metadata,
    )

    if require_canonical_bytes and raw_value_bytes != record.kafka_value:
        raise IncidentPublicationContractError(
            "Raw Kafka value bytes are not in canonical serialized form"
        )
    return record


def validate_incident_publication(
    *,
    raw_value: bytes | bytearray | memoryview,
    raw_key: bytes | bytearray | memoryview,
    headers: Sequence[tuple[str, bytes]] | None,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    expected_blast_radius_policy: BlastRadiusPolicy,
    expected_correlation_config: CorrelationConfig | None = None,
    expected_execution_context: TelemetryExecutionContext | None = None,
    expected_producer_version: str | None | _UnsetType = _UNSET,
    expected_tenant_id: UUID | None = None,
    topic: str | KafkaTopic = INCIDENT_PUBLICATION_TOPIC,
    transport_metadata: PublicationTransportMetadata | None = None,
    require_canonical_bytes: bool = False,
) -> IncidentPublicationRecord:
    """Full consumer trust boundary: structural decode + semantic handoff & pin validation."""
    record = decode_incident_publication_structural(
        raw_value=raw_value,
        raw_key=raw_key,
        headers=headers,
        topic=topic,
        transport_metadata=transport_metadata,
        require_canonical_bytes=require_canonical_bytes,
    )

    if not isinstance(snapshot, TopologySnapshot):
        raise IncidentPublicationValidationError(
            "snapshot must be a TopologySnapshot instance"
        )
    if not isinstance(expected_topology, TopologySnapshotReference):
        raise IncidentPublicationValidationError(
            "expected_topology must be a TopologySnapshotReference instance"
        )
    if not isinstance(expected_blast_radius_policy, BlastRadiusPolicy):
        raise IncidentPublicationValidationError(
            "expected_blast_radius_policy must be a BlastRadiusPolicy instance"
        )

    handoff_bytes = serialize_phase5_handoff(record.handoff)
    try:
        validated_handoff = validate_phase5_handoff(
            raw=handoff_bytes,
            snapshot=snapshot,
            expected_topology=expected_topology,
            expected_policy=expected_blast_radius_policy,
        )
    except Phase5HandoffContractError as exc:
        raise IncidentPublicationValidationError(
            "Embedded Phase 5 handoff failed semantic topology/policy validation"
        ) from exc

    if serialize_phase5_handoff(validated_handoff) != handoff_bytes:
        raise IncidentPublicationValidationError(
            "Semantically validated handoff bytes differ from decoded handoff"
        )

    if expected_correlation_config is not None:
        fresh_cfg, expected_cfg_fp = _revalidate_correlation_config(
            expected_correlation_config
        )
        if (
            record.payload.correlation_policy_version != fresh_cfg.policy_version
            or record.payload.correlation_config_fingerprint != expected_cfg_fp
        ):
            raise IncidentPublicationValidationError(
                "Publication correlation policy/config does not match expected_correlation_config"
            )

    if expected_execution_context is not None:
        expected_lineage = validate_publication_execution_context(
            expected_execution_context
        )
        if record.payload.execution_lineage != expected_lineage:
            raise IncidentPublicationValidationError(
                "Publication execution lineage does not match expected_execution_context"
            )

    if not isinstance(expected_producer_version, _UnsetType):
        expected_pv = validate_publication_producer_version(expected_producer_version)
        if record.producer_version != expected_pv:
            raise IncidentPublicationValidationError(
                "Publication producer_version does not match expected_producer_version"
            )

    if expected_tenant_id is not None:
        if not isinstance(expected_tenant_id, UUID):
            raise IncidentPublicationValidationError(
                "expected_tenant_id must be a UUID"
            )
        if record.payload.tenant_id != expected_tenant_id:
            raise IncidentPublicationValidationError(
                "Publication tenant_id does not match expected_tenant_id"
            )

    return record


def _extract_verified_envelope_inputs(
    *,
    envelope: TelemetryEnvelope,
    raw_value: bytes | bytearray | memoryview | None,
    raw_headers: Sequence[tuple[str, bytes]] | None,
) -> tuple[bytes, bytes, Sequence[tuple[str, bytes]], PublicationTransportMetadata]:
    if not isinstance(envelope, TelemetryEnvelope):
        raise IncidentPublicationContractError(
            "envelope must be a TelemetryEnvelope instance"
        )
    if envelope.key is None:
        raise IncidentPublicationContractError(
            "Incident publication TelemetryEnvelope requires a non-null Kafka key"
        )
    if raw_value is None or raw_headers is None:
        raise IncidentPublicationContractError(
            "Original wire raw_value and raw_headers are required; "
            "TelemetryEnvelope does not retain raw wire bytes or original headers"
        )
    if not isinstance(raw_value, (bytes, bytearray, memoryview)):
        raise IncidentPublicationContractError("raw_value must be bytes-like")
    try:
        transport_metadata = PublicationTransportMetadata(
            partition=envelope.partition,
            offset=envelope.offset,
            kafka_timestamp_ms=envelope.kafka_timestamp_ms,
            consumed_at=envelope.consumed_at,
        )
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Invalid transport metadata on TelemetryEnvelope"
        ) from exc
    return bytes(raw_value), envelope.key, raw_headers, transport_metadata


def _verify_envelope_matches_record(
    *,
    envelope: TelemetryEnvelope,
    record: IncidentPublicationRecord,
    require_canonical_bytes: bool,
) -> None:
    if envelope.context != record.execution_context:
        raise IncidentPublicationValidationError(
            "TelemetryEnvelope context does not match validated raw headers"
        )
    if envelope.producer_version != record.producer_version:
        raise IncidentPublicationValidationError(
            "TelemetryEnvelope producer_version does not match validated raw headers"
        )
    if (
        envelope.event.schema_version != ENVELOPE_SCHEMA_VERSION
        or envelope.event.event_type != INCIDENT_PUBLICATION_EVENT_TYPE
    ):
        raise IncidentPublicationContractError(
            "TelemetryEnvelope event does not match incident publication envelope contract"
        )
    try:
        envelope_dump = envelope.event.model_dump(mode="json")
        record_dump = record.event.model_dump(mode="json")
    except Exception as exc:
        raise IncidentPublicationContractError(
            "Failed to compare TelemetryEnvelope event with decoded publication record"
        ) from exc
    if envelope_dump != record_dump:
        raise IncidentPublicationValidationError(
            "TelemetryEnvelope event does not match validated raw_value"
        )
    if require_canonical_bytes:
        try:
            envelope_bytes = serialize_event(envelope.event)
        except Exception as exc:
            raise IncidentPublicationContractError(
                "Failed to serialize TelemetryEnvelope event"
            ) from exc
        if envelope_bytes != record.kafka_value:
            raise IncidentPublicationContractError(
                "TelemetryEnvelope event is not in canonical serialized key order"
            )


def decode_incident_publication_from_envelope(
    envelope: TelemetryEnvelope,
    *,
    raw_value: bytes | bytearray | memoryview | None = None,
    raw_headers: Sequence[tuple[str, bytes]] | None = None,
    require_canonical_bytes: bool = False,
) -> IncidentPublicationRecord:
    """Decode an IncidentPublicationRecord from a TelemetryEnvelope with original wire evidence.

    Because ``TelemetryEnvelope`` discards original wire JSON bytes and raw Kafka
    header ordering/formatting, callers must supply ``raw_value`` and
    ``raw_headers``; omitting either fails closed. Transport observations
    (partition, offset, kafka_timestamp_ms, consumed_at) are extracted into
    ``transport_metadata`` and excluded from semantic equality.
    """
    value_bytes, key_bytes, headers_seq, transport_metadata = (
        _extract_verified_envelope_inputs(
            envelope=envelope,
            raw_value=raw_value,
            raw_headers=raw_headers,
        )
    )
    record = decode_incident_publication_structural(
        raw_value=value_bytes,
        raw_key=key_bytes,
        headers=headers_seq,
        topic=envelope.topic,
        transport_metadata=transport_metadata,
        require_canonical_bytes=require_canonical_bytes,
    )
    _verify_envelope_matches_record(
        envelope=envelope,
        record=record,
        require_canonical_bytes=require_canonical_bytes,
    )
    return record


def validate_incident_publication_from_envelope(
    envelope: TelemetryEnvelope,
    *,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    expected_blast_radius_policy: BlastRadiusPolicy,
    raw_value: bytes | bytearray | memoryview | None = None,
    raw_headers: Sequence[tuple[str, bytes]] | None = None,
    expected_correlation_config: CorrelationConfig | None = None,
    expected_execution_context: TelemetryExecutionContext | None = None,
    expected_producer_version: str | None | _UnsetType = _UNSET,
    expected_tenant_id: UUID | None = None,
    require_canonical_bytes: bool = False,
) -> IncidentPublicationRecord:
    """Semantically validate an IncidentPublicationRecord from a TelemetryEnvelope.

    Requires original ``raw_value`` and ``raw_headers`` so noncanonical headers,
    unknown/reordered headers, duplicate keys, or non-canonical JSON cannot be
    laundered through a parsed ``TelemetryEnvelope``.
    """
    value_bytes, key_bytes, headers_seq, transport_metadata = (
        _extract_verified_envelope_inputs(
            envelope=envelope,
            raw_value=raw_value,
            raw_headers=raw_headers,
        )
    )
    record = validate_incident_publication(
        raw_value=value_bytes,
        raw_key=key_bytes,
        headers=headers_seq,
        snapshot=snapshot,
        expected_topology=expected_topology,
        expected_blast_radius_policy=expected_blast_radius_policy,
        expected_correlation_config=expected_correlation_config,
        expected_execution_context=expected_execution_context,
        expected_producer_version=expected_producer_version,
        expected_tenant_id=expected_tenant_id,
        topic=envelope.topic,
        transport_metadata=transport_metadata,
        require_canonical_bytes=require_canonical_bytes,
    )
    _verify_envelope_matches_record(
        envelope=envelope,
        record=record,
        require_canonical_bytes=require_canonical_bytes,
    )
    return record


# ============================================================================
# NON-RAISING TYPED CONTRACT RESULT WRAPPERS
# ============================================================================


def try_build_incident_publication(
    *,
    incident: IncidentAggregate,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    blast_radius_policy: BlastRadiusPolicy,
    correlation_config: CorrelationConfig,
    execution_context: TelemetryExecutionContext,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
    intent_time: datetime | None = None,
) -> IncidentPublicationContractResult:
    """Build an incident publication record returning a typed non-raising result."""
    try:
        record = build_incident_publication(
            incident=incident,
            snapshot=snapshot,
            expected_topology=expected_topology,
            blast_radius_policy=blast_radius_policy,
            correlation_config=correlation_config,
            execution_context=execution_context,
            producer_version=producer_version,
            intent_time=intent_time,
        )
        return IncidentPublicationContractResult(
            ok=True,
            outcome=IncidentPublicationValidationOutcome.VALID,
            record=record,
        )
    except IncidentPublicationError as exc:
        return IncidentPublicationContractResult(
            ok=False,
            outcome=_classify_exception(exc),
            error_code=exc.error_code,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


def try_decode_incident_publication(
    *,
    raw_value: bytes | bytearray | memoryview,
    raw_key: bytes | bytearray | memoryview,
    headers: Sequence[tuple[str, bytes]] | None,
    topic: str | KafkaTopic = INCIDENT_PUBLICATION_TOPIC,
    transport_metadata: PublicationTransportMetadata | None = None,
    require_canonical_bytes: bool = False,
) -> IncidentPublicationContractResult:
    """Structurally decode an incident publication returning a typed non-raising result."""
    try:
        record = decode_incident_publication_structural(
            raw_value=raw_value,
            raw_key=raw_key,
            headers=headers,
            topic=topic,
            transport_metadata=transport_metadata,
            require_canonical_bytes=require_canonical_bytes,
        )
        return IncidentPublicationContractResult(
            ok=True,
            outcome=IncidentPublicationValidationOutcome.VALID,
            record=record,
        )
    except IncidentPublicationError as exc:
        return IncidentPublicationContractResult(
            ok=False,
            outcome=_classify_exception(exc),
            error_code=exc.error_code,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


def try_validate_incident_publication(
    *,
    raw_value: bytes | bytearray | memoryview,
    raw_key: bytes | bytearray | memoryview,
    headers: Sequence[tuple[str, bytes]] | None,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    expected_blast_radius_policy: BlastRadiusPolicy,
    expected_correlation_config: CorrelationConfig | None = None,
    expected_execution_context: TelemetryExecutionContext | None = None,
    expected_producer_version: str | None | _UnsetType = _UNSET,
    expected_tenant_id: UUID | None = None,
    topic: str | KafkaTopic = INCIDENT_PUBLICATION_TOPIC,
    transport_metadata: PublicationTransportMetadata | None = None,
    require_canonical_bytes: bool = False,
) -> IncidentPublicationContractResult:
    """Semantically validate an incident publication returning a typed non-raising result."""
    try:
        record = validate_incident_publication(
            raw_value=raw_value,
            raw_key=raw_key,
            headers=headers,
            snapshot=snapshot,
            expected_topology=expected_topology,
            expected_blast_radius_policy=expected_blast_radius_policy,
            expected_correlation_config=expected_correlation_config,
            expected_execution_context=expected_execution_context,
            expected_producer_version=expected_producer_version,
            expected_tenant_id=expected_tenant_id,
            topic=topic,
            transport_metadata=transport_metadata,
            require_canonical_bytes=require_canonical_bytes,
        )
        return IncidentPublicationContractResult(
            ok=True,
            outcome=IncidentPublicationValidationOutcome.VALID,
            record=record,
        )
    except IncidentPublicationError as exc:
        return IncidentPublicationContractResult(
            ok=False,
            outcome=_classify_exception(exc),
            error_code=exc.error_code,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


__all__ = [
    "BLAST_RADIUS_CONTEXT_SCHEMA_VERSION",
    "BLAST_RADIUS_POLICY_VERSION",
    "DEFAULT_PUBLICATION_PRODUCER_VERSION",
    "ENVELOPE_SCHEMA_VERSION",
    "ENVELOPE_TOP_LEVEL_KEYS",
    "EXECUTION_LINEAGE_KEYS",
    "HANDOFF_PROVENANCE_PRODUCER_VERSION",
    "HANDOFF_SCHEMA_VERSION",
    "HEADER_FIELD_MAX_BYTES",
    "INCIDENT_PUBLICATION_EVENT_NAMESPACE",
    "INCIDENT_PUBLICATION_EVENT_NAMESPACE_NAME",
    "INCIDENT_PUBLICATION_EVENT_TYPE",
    "INCIDENT_PUBLICATION_TOPIC",
    "INCIDENT_SCHEMA_VERSION",
    "MAX_JSON_DEPTH",
    "MAX_KAFKA_HEADERS_TOTAL_BYTES",
    "MAX_KAFKA_HEADER_VALUE_BYTES",
    "MAX_KAFKA_KEY_BYTES",
    "MAX_KAFKA_RECORD_ESTIMATED_BYTES",
    "MAX_KAFKA_VALUE_BYTES",
    "OPTIONAL_PUBLICATION_HEADER_NAME",
    "ORDERED_PUBLICATION_HEADER_NAMES",
    "PAYLOAD_TOP_LEVEL_KEYS",
    "PUBLICATION_CONTRACT_VERSION",
    "PUBLICATION_FINGERPRINT_INPUT_KEYS",
    "REQUIRED_PUBLICATION_HEADER_NAMES",
    "TRANSPORT_FIELDS_EXCLUDED_FROM_SEMANTIC_EQUALITY",
    "IncidentPublicationContractError",
    "IncidentPublicationContractResult",
    "IncidentPublicationError",
    "IncidentPublicationHeaderError",
    "IncidentPublicationIdentity",
    "IncidentPublicationPayload",
    "IncidentPublicationRecord",
    "IncidentPublicationSizeLimitError",
    "IncidentPublicationValidationError",
    "IncidentPublicationValidationOutcome",
    "IncidentPublicationVersionError",
    "PublicationExecutionLineage",
    "PublicationTransportMetadata",
    "build_incident_publication",
    "build_incident_publication_from_handoff",
    "build_incident_publication_from_intent",
    "build_publication_fingerprint_projection",
    "build_publication_kafka_headers",
    "build_publication_kafka_key",
    "compute_incident_publication_fingerprint",
    "decode_incident_publication_from_envelope",
    "decode_incident_publication_structural",
    "derive_incident_publication_event_id",
    "incident_publication_to_telemetry_event",
    "parse_and_validate_publication_headers",
    "publications_semantically_equal",
    "try_build_incident_publication",
    "try_decode_incident_publication",
    "try_validate_incident_publication",
    "validate_incident_publication",
    "validate_incident_publication_from_envelope",
    "validate_publication_execution_context",
    "validate_publication_producer_version",
]
