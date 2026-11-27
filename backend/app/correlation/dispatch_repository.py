"""Tenant-scoped async repository for durable incident publication dispatch.

Implements Task 4.12-D1 storage operations over PostgreSQL against approved
Task 4.12-A1-R1, C1-R1, and P1 contracts:
1. Caller-session-owned (never begins, commits, or rolls back transactions).
2. Enforces non-bypass runtime role, READ COMMITTED isolation, and bound
   transaction-local tenant context (``app.current_tenant_id``).
3. Reads committed immutable ``incident_publication_intents`` and preserves
   exact canonical aggregate bytes.
4. Persists frozen wire records in ``incident_publication_dispatches`` without
   modifying any of Task 4.9's five tables.
5. Executes atomic SQL conditional claims with server-clock lease/backoff
   checks, strict predecessor-intent acknowledgement gating, and
   ``claim_token`` fencing on completion bookkeeping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
import json
import re
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.correlation.errors import (
    CorrelationError,
    IncidentPersistenceTenantError,
)
from app.correlation.identity import compute_revision_fingerprint
from app.correlation.persistence_repository import PersistedPublicationIntent
from app.correlation.publication import (
    ENVELOPE_SCHEMA_VERSION,
    HANDOFF_SCHEMA_VERSION,
    INCIDENT_PUBLICATION_TOPIC,
    MAX_CLAIMED_BY_BYTES,
    MAX_KAFKA_KEY_BYTES,
    MAX_KAFKA_RECORD_ESTIMATED_BYTES,
    MAX_KAFKA_VALUE_BYTES,
    PUBLICATION_CONTRACT_VERSION,
    IncidentPublicationError,
    IncidentPublicationRecord,
    decode_incident_publication_structural,
)
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    serialize_incident_aggregate,
)
from app.models.incident import (
    IncidentPublicationDispatchRecord,
    IncidentPublicationIntentRecord,
)
from app.telemetry.transport.producer import PublishResult

MAX_REVISION: int = 18446744073709551615
MAX_DISPATCH_ATTEMPTS_CEILING: int = 10
MAX_ERROR_CODE_LEN: int = 64
MAX_ERROR_MESSAGE_LEN: int = 512
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_ZERO = timedelta(0)

MUTABLE_DISPATCH_COLUMNS: tuple[str, ...] = (
    "dispatch_state",
    "attempt_count",
    "claim_token",
    "claimed_by",
    "claimed_at",
    "claim_expires_at",
    "last_attempt_at",
    "next_retry_at",
    "last_error_code",
    "last_error_message",
    "broker_topic",
    "broker_partition",
    "broker_offset",
    "broker_timestamp_ms",
    "acknowledged_at",
    "updated_at",
)

IMMUTABLE_DISPATCH_COLUMNS: tuple[str, ...] = (
    "tenant_id",
    "incident_id",
    "revision",
    "revision_fingerprint",
    "publication_contract_version",
    "envelope_schema_version",
    "handoff_schema_version",
    "event_id",
    "publication_fingerprint",
    "handoff_id",
    "handoff_fingerprint",
    "topology_id",
    "topology_version",
    "topology_snapshot_fingerprint",
    "correlation_policy_version",
    "correlation_config_fingerprint",
    "intent_time",
    "kafka_topic",
    "kafka_key",
    "kafka_value",
    "kafka_headers_json",
    "wire_record_bytes",
    "created_at",
)


# ============================================================================
# TYPED ERROR HIERARCHY & STATES
# ============================================================================


class IncidentDispatchError(CorrelationError):
    """Base exception for Task 4.12-D1 durable dispatch errors."""


class DispatchConfigurationError(IncidentDispatchError):
    """Raised when dispatch timing or operational bounds are invalid."""


class DispatchInputError(IncidentDispatchError):
    """Raised when dispatch inputs or parameters violate contract preconditions."""


class DispatchClaimLostError(IncidentDispatchError):
    """Raised when a stale or expired claim token cannot fence a send or update."""


class DispatchCorruptionError(IncidentDispatchError):
    """Raised when persisted intent or dispatch bytes violate canonical invariants."""


class DispatchStoreError(IncidentDispatchError):
    """Raised on sanitized database or transaction failures during dispatch."""


class DispatchState(StrEnum):
    """Approved durable dispatch states for ``incident_publication_dispatches``."""

    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    RETRY_SCHEDULED = "retry_scheduled"
    AMBIGUOUS_OUTCOME = "ambiguous_outcome"
    ACKNOWLEDGED = "acknowledged"
    EXHAUSTED = "exhausted"
    REJECTED_UNSENDABLE = "rejected_unsendable"


TERMINAL_DISPATCH_STATES: frozenset[DispatchState] = frozenset(
    {
        DispatchState.ACKNOWLEDGED,
        DispatchState.EXHAUSTED,
        DispatchState.REJECTED_UNSENDABLE,
    }
)


# ============================================================================
# IMMUTABLE DOMAIN MODELS
# ============================================================================


@dataclass(frozen=True)
class PersistedPublicationDispatch:
    """Immutable snapshot of one row in ``incident_publication_dispatches``."""

    tenant_id: UUID
    incident_id: UUID
    revision: int
    revision_fingerprint: str
    publication_contract_version: str
    envelope_schema_version: str
    handoff_schema_version: str
    event_id: UUID
    publication_fingerprint: str
    handoff_id: UUID
    handoff_fingerprint: str
    topology_id: UUID
    topology_version: str
    topology_snapshot_fingerprint: str
    correlation_policy_version: str
    correlation_config_fingerprint: str
    intent_time: datetime
    kafka_topic: str
    kafka_key: bytes
    kafka_value: bytes | None
    kafka_headers_json: tuple[tuple[str, str], ...]
    wire_record_bytes: int
    created_at: datetime
    dispatch_state: DispatchState
    attempt_count: int
    claim_token: UUID | None
    claimed_by: str | None
    claimed_at: datetime | None
    claim_expires_at: datetime | None
    last_attempt_at: datetime | None
    next_retry_at: datetime | None
    last_error_code: str | None
    last_error_message: str | None
    broker_topic: str | None
    broker_partition: int | None
    broker_offset: int | None
    broker_timestamp_ms: int | None
    acknowledged_at: datetime | None
    updated_at: datetime

    @property
    def is_terminal(self) -> bool:
        return self.dispatch_state in TERMINAL_DISPATCH_STATES

    @property
    def input_offset_committable(self) -> bool:
        return self.dispatch_state is DispatchState.ACKNOWLEDGED


@dataclass(frozen=True)
class UnsendableDispatchDraft:
    """Immutable metadata for staging a deterministic ``rejected_unsendable`` row."""

    tenant_id: UUID
    incident_id: UUID
    revision: int
    revision_fingerprint: str
    event_id: UUID
    publication_fingerprint: str
    handoff_id: UUID
    handoff_fingerprint: str
    topology_id: UUID
    topology_version: str
    topology_snapshot_fingerprint: str
    correlation_policy_version: str
    correlation_config_fingerprint: str
    intent_time: datetime
    kafka_topic: str
    kafka_key: bytes
    kafka_headers_json: tuple[tuple[str, str], ...]
    wire_record_bytes: int
    error_code: str
    error_message: str


@dataclass(frozen=True)
class StageDispatchRecordResult:
    """Result of an idempotent stage insert into ``incident_publication_dispatches``."""

    dispatch: PersistedPublicationDispatch
    created: bool


@dataclass(frozen=True)
class PredecessorCheckResult:
    """Observed committed predecessor intent and acknowledged dispatch counts."""

    expected_predecessors: int
    committed_predecessor_intents: int
    acknowledged_predecessor_dispatches: int

    @property
    def satisfied(self) -> bool:
        return (
            self.committed_predecessor_intents == self.expected_predecessors
            and self.acknowledged_predecessor_dispatches == self.expected_predecessors
        )


# ============================================================================
# REPOSITORY PROTOCOL
# ============================================================================


@runtime_checkable
class IncidentDispatchRepository(Protocol):
    """Protocol defining the tenant-bound durable dispatch repository contract."""

    @property
    def tenant_id(self) -> UUID: ...

    async def bind_tenant_context(self) -> None: ...

    async def get_server_utc_time(self) -> datetime: ...

    async def load_publication_intent(
        self, incident_id: UUID, revision: int
    ) -> PersistedPublicationIntent | None: ...

    async def list_incident_publication_intents(
        self, incident_id: UUID
    ) -> tuple[PersistedPublicationIntent, ...]: ...

    async def list_unstaged_publication_intents(
        self, *, limit: int, incident_id: UUID | None = None
    ) -> tuple[PersistedPublicationIntent, ...]: ...

    async def load_dispatch(
        self, incident_id: UUID, revision: int, *, verify_wire: bool = True
    ) -> PersistedPublicationDispatch | None: ...

    async def list_incident_dispatches(
        self, incident_id: UUID
    ) -> tuple[PersistedPublicationDispatch, ...]: ...

    async def inspect_predecessors(
        self, incident_id: UUID, revision: int
    ) -> PredecessorCheckResult: ...

    async def stage_pending_dispatch(
        self, record: IncidentPublicationRecord
    ) -> StageDispatchRecordResult: ...

    async def stage_rejected_unsendable_dispatch(
        self, draft: UnsendableDispatchDraft
    ) -> StageDispatchRecordResult: ...

    async def claim_dispatch(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        claimed_by: str,
        claim_lease_seconds: int,
        max_dispatch_attempts: int,
    ) -> PersistedPublicationDispatch | None: ...

    async def verify_active_claim(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
    ) -> bool: ...

    async def load_active_claimed_dispatch(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
    ) -> PersistedPublicationDispatch | None: ...

    async def mark_expired_in_flight_exhausted(
        self,
        *,
        incident_id: UUID,
        revision: int,
        max_dispatch_attempts: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch | None: ...

    async def record_acknowledged(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        publish_result: PublishResult,
        expected_record: IncidentPublicationRecord | None = None,
    ) -> PersistedPublicationDispatch: ...

    async def record_retry_scheduled(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        max_dispatch_attempts: int,
        backoff_ms: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch: ...

    async def record_ambiguous_outcome(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        max_dispatch_attempts: int,
        backoff_ms: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch: ...

    async def record_exhausted(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch: ...

    async def list_recoverable_dispatches(
        self, *, limit: int, incident_id: UUID | None = None
    ) -> tuple[PersistedPublicationDispatch, ...]: ...


# ============================================================================
# VALIDATION, SANITIZATION & FROZEN WIRE RECONSTRUCTION HELPERS
# ============================================================================


def _is_strict_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _has_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in value)


def _validate_revision_int(value: Any, name: str = "revision") -> int:
    if not _is_strict_int(value) or not (1 <= value <= MAX_REVISION):
        raise DispatchInputError(
            f"Invalid {name}: must be an integer between 1 and {MAX_REVISION}"
        )
    return int(value)


def _decimal_to_revision(val: Any) -> int:
    if (
        isinstance(val, Decimal)
        and not isinstance(val, bool)
        and val.is_finite()
        and val == val.to_integral_value()
    ):
        int_val = int(val)
        if 1 <= int_val <= MAX_REVISION:
            return int_val
    raise DispatchCorruptionError(
        "Stored dispatch or intent revision is not a valid positive integer"
    )


def _validate_fingerprint(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _FINGERPRINT_RE.fullmatch(value):
        raise DispatchInputError(f"Invalid {name}: must be 64 lowercase hex characters")
    return value


def _validate_utc_datetime(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise DispatchInputError(f"{name} must be a timezone-aware UTC datetime")
    if value.utcoffset() != _UTC_ZERO:
        raise DispatchInputError(f"{name} must have UTC offset (+00:00)")
    return value.astimezone(timezone.utc)


def _validate_claimed_by(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise DispatchInputError("claimed_by must be trimmed non-empty text")
    if _has_control_chars(value):
        raise DispatchInputError("claimed_by must not contain control characters")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise DispatchInputError("claimed_by contains invalid Unicode") from exc
    if len(encoded) > MAX_CLAIMED_BY_BYTES:
        raise DispatchInputError(
            f"claimed_by exceeds maximum {MAX_CLAIMED_BY_BYTES} UTF-8 bytes"
        )
    return value


def _sanitize_error_code(code: str) -> str:
    if not isinstance(code, str) or not code.strip():
        raise DispatchInputError("error_code must be non-empty text")
    cleaned = "".join(ch for ch in code.strip() if 32 <= ord(ch) < 127)
    if not cleaned:
        raise DispatchInputError("error_code must contain printable ASCII characters")
    return cleaned[:MAX_ERROR_CODE_LEN]


def _sanitize_error_message(message: str) -> str:
    if not isinstance(message, str) or not message.strip():
        raise DispatchInputError("error_message must be non-empty text")
    cleaned = " ".join(
        "".join(ch for ch in message if ord(ch) >= 32 and ord(ch) != 127).split()
    )
    if not cleaned:
        raise DispatchInputError("error_message must contain printable text")
    return cleaned[:MAX_ERROR_MESSAGE_LEN]


def _extract_sqlstate(exc: BaseException | None) -> str | None:
    curr = exc
    visited: set[int] = set()
    while curr is not None and id(curr) not in visited:
        visited.add(id(curr))
        orig = getattr(curr, "orig", None)
        if orig is not None:
            code = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
            if code:
                return str(code)
        code = getattr(curr, "pgcode", None) or getattr(curr, "sqlstate", None)
        if code:
            return str(code)
        curr = curr.__cause__ or curr.__context__
    return None


def _translate_dispatch_db_error(exc: Exception) -> Exception:
    if isinstance(exc, (IncidentDispatchError, IncidentPersistenceTenantError)):
        return exc

    sqlstate = _extract_sqlstate(exc)
    if sqlstate == "42501":
        err: Exception = IncidentPersistenceTenantError(
            "Database tenant permission or isolation violation during incident dispatch"
        )
        err.__cause__ = exc
        return err

    if isinstance(exc, IntegrityError):
        if sqlstate in ("23503", "23514", "23502"):
            input_err = DispatchInputError(
                "Database constraint violation during incident dispatch"
            )
            input_err.__cause__ = exc
            return input_err
        store_err = DispatchStoreError(
            "Database integrity failure during incident dispatch"
        )
        store_err.__cause__ = exc
        return store_err

    if isinstance(exc, (DBAPIError, SQLAlchemyError)):
        store_err = DispatchStoreError(
            "Database operation failed during incident dispatch"
        )
        store_err.__cause__ = exc
        return store_err

    unexpected = DispatchStoreError(
        "Unexpected database error during incident dispatch"
    )
    unexpected.__cause__ = exc
    return unexpected


async def verify_runtime_role_and_isolation(session: AsyncSession) -> None:
    """Enforce READ COMMITTED isolation and non-superuser, non-bypassrls runtime role."""
    try:
        iso_res = await session.execute(text("SHOW transaction_isolation"))
        iso = iso_res.scalar_one_or_none()
        if iso is None or str(iso).lower() != "read committed":
            raise DispatchStoreError(
                "Expected transaction isolation 'read committed' for durable dispatch"
            )

        role_res = await session.execute(
            text(
                "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
        )
        role_row = role_res.first()
        if role_row is None:
            raise IncidentPersistenceTenantError(
                "Failed to inspect runtime role privileges for incident dispatch"
            )
        rolsuper, rolbypassrls = bool(role_row[0]), bool(role_row[1])
        if rolsuper or rolbypassrls:
            raise IncidentPersistenceTenantError(
                "Runtime dispatch role cannot have superuser or bypassrls privileges"
            )
    except (IncidentDispatchError, IncidentPersistenceTenantError):
        raise
    except Exception as exc:
        raise _translate_dispatch_db_error(exc) from exc


def _parse_headers_json(raw: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(raw, list):
        raise DispatchCorruptionError("Stored kafka_headers_json must be a JSON array")
    parsed: list[tuple[str, str]] = []
    for item in raw:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], str)
        ):
            raise DispatchCorruptionError(
                "Stored kafka_headers_json entries must be [name, value] string pairs"
            )
        parsed.append((item[0], item[1]))
    return tuple(parsed)


def decode_dispatch_publication_record(
    dispatch: PersistedPublicationDispatch,
) -> IncidentPublicationRecord:
    """Reconstruct and verify the exact frozen C1 wire record stored in a dispatch row."""
    if not isinstance(dispatch, PersistedPublicationDispatch):
        raise DispatchInputError(
            "dispatch must be a PersistedPublicationDispatch instance"
        )
    if (
        dispatch.dispatch_state is DispatchState.REJECTED_UNSENDABLE
        or dispatch.kafka_value is None
    ):
        raise DispatchInputError(
            "Rejected unsendable dispatch row carries no sendable Kafka payload"
        )

    try:
        raw_headers = tuple(
            (name, value.encode("utf-8")) for name, value in dispatch.kafka_headers_json
        )
    except UnicodeEncodeError as exc:
        raise DispatchCorruptionError(
            "Stored dispatch headers contain invalid UTF-8"
        ) from exc

    try:
        record = decode_incident_publication_structural(
            raw_value=dispatch.kafka_value,
            raw_key=dispatch.kafka_key,
            headers=raw_headers,
            require_canonical_bytes=True,
        )
    except IncidentPublicationError as exc:
        raise DispatchCorruptionError(
            "Stored frozen dispatch wire bytes failed canonical C1 decoding"
        ) from exc

    ident = record.identity
    expected_headers = tuple((pair[0], pair[1]) for pair in record.kafka_headers_json)
    if (
        record.topic.value != dispatch.kafka_topic
        or record.event_id != dispatch.event_id
        or record.publication_fingerprint != dispatch.publication_fingerprint
        or ident.tenant_id != dispatch.tenant_id
        or ident.incident_id != dispatch.incident_id
        or ident.revision != dispatch.revision
        or ident.revision_fingerprint != dispatch.revision_fingerprint
        or ident.publication_contract_version != dispatch.publication_contract_version
        or ident.envelope_schema_version != dispatch.envelope_schema_version
        or ident.handoff_schema_version != dispatch.handoff_schema_version
        or ident.handoff_id != dispatch.handoff_id
        or ident.handoff_fingerprint != dispatch.handoff_fingerprint
        or ident.topology_id != dispatch.topology_id
        or ident.topology_version != dispatch.topology_version
        or ident.topology_snapshot_fingerprint != dispatch.topology_snapshot_fingerprint
        or ident.correlation_policy_version != dispatch.correlation_policy_version
        or ident.correlation_config_fingerprint
        != dispatch.correlation_config_fingerprint
        or ident.intent_time != dispatch.intent_time
        or record.kafka_key != dispatch.kafka_key
        or record.kafka_value != dispatch.kafka_value
        or expected_headers != dispatch.kafka_headers_json
        or record.estimated_record_bytes != dispatch.wire_record_bytes
    ):
        raise DispatchCorruptionError(
            "Stored dispatch columns do not match frozen wire record identity"
        )

    return record


def _normalize_optional_utc(value: datetime | None, name: str) -> datetime | None:
    if value is None:
        return None
    return _validate_utc_datetime(value, name)


def _row_to_persisted_dispatch(
    row: Any, expected_tenant_id: UUID, *, verify_wire: bool = True
) -> PersistedPublicationDispatch:
    """Convert an ORM or mapping row into a validated ``PersistedPublicationDispatch``."""
    tenant_id = row.tenant_id
    if tenant_id != expected_tenant_id:
        raise DispatchCorruptionError("Loaded dispatch row tenant_id mismatch")
    rev = _decimal_to_revision(row.revision)
    try:
        state = DispatchState(row.dispatch_state)
    except ValueError as exc:
        raise DispatchCorruptionError("Unknown stored dispatch_state value") from exc

    raw_key = bytes(row.kafka_key)
    raw_value = None if row.kafka_value is None else bytes(row.kafka_value)
    headers_tuple = _parse_headers_json(row.kafka_headers_json)

    dispatch = PersistedPublicationDispatch(
        tenant_id=tenant_id,
        incident_id=row.incident_id,
        revision=rev,
        revision_fingerprint=str(row.revision_fingerprint),
        publication_contract_version=str(row.publication_contract_version),
        envelope_schema_version=str(row.envelope_schema_version),
        handoff_schema_version=str(row.handoff_schema_version),
        event_id=row.event_id,
        publication_fingerprint=str(row.publication_fingerprint),
        handoff_id=row.handoff_id,
        handoff_fingerprint=str(row.handoff_fingerprint),
        topology_id=row.topology_id,
        topology_version=str(row.topology_version),
        topology_snapshot_fingerprint=str(row.topology_snapshot_fingerprint),
        correlation_policy_version=str(row.correlation_policy_version),
        correlation_config_fingerprint=str(row.correlation_config_fingerprint),
        intent_time=_validate_utc_datetime(row.intent_time, "intent_time"),
        kafka_topic=str(row.kafka_topic),
        kafka_key=raw_key,
        kafka_value=raw_value,
        kafka_headers_json=headers_tuple,
        wire_record_bytes=int(row.wire_record_bytes),
        created_at=_validate_utc_datetime(row.created_at, "created_at"),
        dispatch_state=state,
        attempt_count=int(row.attempt_count),
        claim_token=row.claim_token,
        claimed_by=row.claimed_by,
        claimed_at=_normalize_optional_utc(row.claimed_at, "claimed_at"),
        claim_expires_at=_normalize_optional_utc(
            row.claim_expires_at, "claim_expires_at"
        ),
        last_attempt_at=_normalize_optional_utc(row.last_attempt_at, "last_attempt_at"),
        next_retry_at=_normalize_optional_utc(row.next_retry_at, "next_retry_at"),
        last_error_code=row.last_error_code,
        last_error_message=row.last_error_message,
        broker_topic=row.broker_topic,
        broker_partition=row.broker_partition,
        broker_offset=row.broker_offset,
        broker_timestamp_ms=row.broker_timestamp_ms,
        acknowledged_at=_normalize_optional_utc(row.acknowledged_at, "acknowledged_at"),
        updated_at=_validate_utc_datetime(row.updated_at, "updated_at"),
    )

    if state is DispatchState.REJECTED_UNSENDABLE:
        if dispatch.kafka_value is not None:
            raise DispatchCorruptionError(
                "rejected_unsendable dispatch row must have NULL kafka_value"
            )
    elif verify_wire:
        decode_dispatch_publication_record(dispatch)

    return dispatch


def _row_to_persisted_intent(
    row: IncidentPublicationIntentRecord, expected_tenant_id: UUID
) -> PersistedPublicationIntent:
    """Reconstruct and canonically verify a committed ``PersistedPublicationIntent``."""
    pi_rev = _decimal_to_revision(row.revision)
    if row.tenant_id != expected_tenant_id:
        raise DispatchCorruptionError("Loaded publication intent tenant_id mismatch")
    raw_bytes = bytes(row.aggregate_payload)
    try:
        agg = deserialize_incident_aggregate(raw_bytes)
    except Exception as exc:
        raise DispatchCorruptionError(
            "Failed to deserialize committed publication intent aggregate"
        ) from exc
    if serialize_incident_aggregate(agg) != raw_bytes:
        raise DispatchCorruptionError(
            "Committed publication intent payload is not canonically serialized"
        )
    if (
        agg.tenant_id != expected_tenant_id
        or agg.incident_id != row.incident_id
        or row.schema_version != agg.schema_version
        or pi_rev != agg.revision
        or row.revision_fingerprint != agg.revision_fingerprint
        or row.revision_fingerprint != compute_revision_fingerprint(agg)
    ):
        raise DispatchCorruptionError(
            "Committed publication intent metadata does not match aggregate bytes"
        )
    return PersistedPublicationIntent(
        tenant_id=row.tenant_id,
        incident_id=row.incident_id,
        revision=pi_rev,
        schema_version=row.schema_version,
        revision_fingerprint=row.revision_fingerprint,
        intent_time=_validate_utc_datetime(row.intent_time, "intent_time"),
        aggregate=agg,
    )


def _assert_existing_matches_staged_record(
    existing: PersistedPublicationDispatch,
    record: IncidentPublicationRecord,
) -> None:
    ident = record.identity
    expected_headers = tuple((p[0], p[1]) for p in record.kafka_headers_json)
    if (
        existing.dispatch_state is DispatchState.REJECTED_UNSENDABLE
        or existing.tenant_id != ident.tenant_id
        or existing.incident_id != ident.incident_id
        or existing.revision != ident.revision
        or existing.revision_fingerprint != ident.revision_fingerprint
        or existing.publication_contract_version != ident.publication_contract_version
        or existing.envelope_schema_version != ident.envelope_schema_version
        or existing.handoff_schema_version != ident.handoff_schema_version
        or existing.event_id != record.event_id
        or existing.publication_fingerprint != record.publication_fingerprint
        or existing.handoff_id != ident.handoff_id
        or existing.handoff_fingerprint != ident.handoff_fingerprint
        or existing.topology_id != ident.topology_id
        or existing.topology_version != ident.topology_version
        or existing.topology_snapshot_fingerprint != ident.topology_snapshot_fingerprint
        or existing.correlation_policy_version != ident.correlation_policy_version
        or existing.correlation_config_fingerprint
        != ident.correlation_config_fingerprint
        or existing.intent_time != ident.intent_time
        or existing.kafka_topic != record.topic.value
        or existing.kafka_key != record.kafka_key
        or existing.kafka_value != record.kafka_value
        or existing.kafka_headers_json != expected_headers
        or existing.wire_record_bytes != record.estimated_record_bytes
    ):
        raise DispatchCorruptionError(
            "Existing staged dispatch row conflicts with immutable publication identity"
        )


def _assert_existing_matches_unsendable_draft(
    existing: PersistedPublicationDispatch,
    draft: UnsendableDispatchDraft,
) -> None:
    if (
        existing.dispatch_state is not DispatchState.REJECTED_UNSENDABLE
        or existing.kafka_value is not None
        or existing.tenant_id != draft.tenant_id
        or existing.incident_id != draft.incident_id
        or existing.revision != draft.revision
        or existing.revision_fingerprint != draft.revision_fingerprint
        or existing.event_id != draft.event_id
        or existing.publication_fingerprint != draft.publication_fingerprint
        or existing.handoff_id != draft.handoff_id
        or existing.handoff_fingerprint != draft.handoff_fingerprint
        or existing.topology_id != draft.topology_id
        or existing.topology_version != draft.topology_version
        or existing.topology_snapshot_fingerprint != draft.topology_snapshot_fingerprint
        or existing.correlation_policy_version != draft.correlation_policy_version
        or existing.correlation_config_fingerprint
        != draft.correlation_config_fingerprint
        or existing.intent_time != draft.intent_time
        or existing.kafka_topic != draft.kafka_topic
        or existing.kafka_key != draft.kafka_key
        or existing.kafka_headers_json != draft.kafka_headers_json
        or existing.wire_record_bytes != draft.wire_record_bytes
    ):
        raise DispatchCorruptionError(
            "Existing staged dispatch row conflicts with unsendable revision identity"
        )


# ============================================================================
# CONCRETE SQLALCHEMY REPOSITORY IMPLEMENTATION
# ============================================================================


class SqlAlchemyIncidentDispatchRepository(IncidentDispatchRepository):
    """Caller-session-owned PostgreSQL repository for durable incident dispatch."""

    def __init__(self, session: AsyncSession, tenant_id: UUID) -> None:
        if not isinstance(session, AsyncSession):
            raise DispatchInputError("session must be an AsyncSession instance")
        if not isinstance(tenant_id, UUID):
            raise DispatchInputError("tenant_id must be a UUID instance")
        self._session = session
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    @property
    def session(self) -> AsyncSession:
        return self._session

    def _ensure_active_transaction(self) -> None:
        if not self._session.in_transaction():
            raise DispatchStoreError(
                "AsyncSession must be in an active transaction owned by the caller"
            )

    async def _verify_tenant_context(self) -> None:
        self._ensure_active_transaction()
        try:
            val = await self._session.scalar(
                select(func.current_setting("app.current_tenant_id", True))
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

        if val is None or val == "":
            raise IncidentPersistenceTenantError(
                "Transaction-local tenant context 'app.current_tenant_id' is not set"
            )
        try:
            ctx_uuid = UUID(str(val))
        except (ValueError, TypeError) as exc:
            raise IncidentPersistenceTenantError(
                "Invalid transaction-local tenant context value"
            ) from exc
        if ctx_uuid != self._tenant_id:
            raise IncidentPersistenceTenantError(
                "Active tenant context does not match dispatch repository tenant"
            )

    async def bind_tenant_context(self) -> None:
        self._ensure_active_transaction()
        try:
            await self._session.execute(
                select(
                    func.set_config(
                        "app.current_tenant_id",
                        str(self._tenant_id),
                        True,
                    )
                )
            )
            readback = await self._session.scalar(
                select(func.current_setting("app.current_tenant_id", True))
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

        if readback is None or UUID(str(readback)) != self._tenant_id:
            raise IncidentPersistenceTenantError(
                "Failed to verify transaction-local tenant context binding"
            )

    async def get_server_utc_time(self) -> datetime:
        self._ensure_active_transaction()
        await self._verify_tenant_context()
        try:
            ts = await self._session.scalar(text("SELECT clock_timestamp()"))
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc
        return _validate_utc_datetime(ts, "server_utc_time")

    async def load_publication_intent(
        self, incident_id: UUID, revision: int
    ) -> PersistedPublicationIntent | None:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = select(IncidentPublicationIntentRecord).where(
            IncidentPublicationIntentRecord.tenant_id == self._tenant_id,
            IncidentPublicationIntentRecord.incident_id == incident_id,
            IncidentPublicationIntentRecord.revision == Decimal(rev),
        )
        try:
            row = (await self._session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return None
            return _row_to_persisted_intent(row, self._tenant_id)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def list_incident_publication_intents(
        self, incident_id: UUID
    ) -> tuple[PersistedPublicationIntent, ...]:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = (
            select(IncidentPublicationIntentRecord)
            .where(
                IncidentPublicationIntentRecord.tenant_id == self._tenant_id,
                IncidentPublicationIntentRecord.incident_id == incident_id,
            )
            .order_by(IncidentPublicationIntentRecord.revision.asc())
        )
        try:
            rows = (await self._session.execute(stmt)).scalars().all()
            return tuple(_row_to_persisted_intent(r, self._tenant_id) for r in rows)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def list_unstaged_publication_intents(
        self, *, limit: int, incident_id: UUID | None = None
    ) -> tuple[PersistedPublicationIntent, ...]:
        if not _is_strict_int(limit) or not (1 <= limit <= 256):
            raise DispatchInputError("limit must be an integer between 1 and 256")
        if incident_id is not None and not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID when provided")
        self._ensure_active_transaction()
        await self._verify_tenant_context()

        disp = IncidentPublicationDispatchRecord
        intent = IncidentPublicationIntentRecord
        stmt = (
            select(intent)
            .outerjoin(
                disp,
                (disp.tenant_id == intent.tenant_id)
                & (disp.incident_id == intent.incident_id)
                & (disp.revision == intent.revision),
            )
            .where(
                intent.tenant_id == self._tenant_id,
                disp.tenant_id.is_(None),
            )
        )
        if incident_id is not None:
            stmt = stmt.where(intent.incident_id == incident_id)
        stmt = stmt.order_by(
            intent.incident_id.asc(),
            intent.revision.asc(),
        ).limit(limit)

        try:
            rows = (await self._session.execute(stmt)).scalars().all()
            return tuple(_row_to_persisted_intent(r, self._tenant_id) for r in rows)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def load_dispatch(
        self, incident_id: UUID, revision: int, *, verify_wire: bool = True
    ) -> PersistedPublicationDispatch | None:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = select(IncidentPublicationDispatchRecord).where(
            IncidentPublicationDispatchRecord.tenant_id == self._tenant_id,
            IncidentPublicationDispatchRecord.incident_id == incident_id,
            IncidentPublicationDispatchRecord.revision == Decimal(rev),
        )
        try:
            row = (await self._session.execute(stmt)).scalar_one_or_none()
            if row is None:
                return None
            return _row_to_persisted_dispatch(
                row, self._tenant_id, verify_wire=verify_wire
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def list_incident_dispatches(
        self, incident_id: UUID
    ) -> tuple[PersistedPublicationDispatch, ...]:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = (
            select(IncidentPublicationDispatchRecord)
            .where(
                IncidentPublicationDispatchRecord.tenant_id == self._tenant_id,
                IncidentPublicationDispatchRecord.incident_id == incident_id,
            )
            .order_by(IncidentPublicationDispatchRecord.revision.asc())
        )
        try:
            rows = (await self._session.execute(stmt)).scalars().all()
            return tuple(_row_to_persisted_dispatch(r, self._tenant_id) for r in rows)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def inspect_predecessors(
        self, incident_id: UUID, revision: int
    ) -> PredecessorCheckResult:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        self._ensure_active_transaction()
        await self._verify_tenant_context()

        expected = rev - 1
        if expected == 0:
            return PredecessorCheckResult(
                expected_predecessors=0,
                committed_predecessor_intents=0,
                acknowledged_predecessor_dispatches=0,
            )

        try:
            res = await self._session.execute(
                text(
                    """
                    SELECT
                        (
                            SELECT count(*)::bigint
                            FROM incident_publication_intents i
                            WHERE i.tenant_id = :tenant_id
                              AND i.incident_id = :incident_id
                              AND i.revision < :revision
                        ) AS intent_count,
                        (
                            SELECT count(*)::bigint
                            FROM incident_publication_dispatches d
                            WHERE d.tenant_id = :tenant_id
                              AND d.incident_id = :incident_id
                              AND d.revision < :revision
                              AND d.dispatch_state = 'acknowledged'
                        ) AS acked_count
                    """
                ),
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                },
            )
            row = res.one()
            return PredecessorCheckResult(
                expected_predecessors=expected,
                committed_predecessor_intents=int(row[0]),
                acknowledged_predecessor_dispatches=int(row[1]),
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def stage_pending_dispatch(
        self, record: IncidentPublicationRecord
    ) -> StageDispatchRecordResult:
        if not isinstance(record, IncidentPublicationRecord):
            raise DispatchInputError("record must be an IncidentPublicationRecord")
        # Re-verify canonical integrity before staging.
        record.to_telemetry_event()
        ident = record.identity
        if ident.tenant_id != self._tenant_id:
            raise IncidentPersistenceTenantError(
                "Publication record tenant_id does not match repository tenant"
            )
        if not (1 <= len(record.kafka_key) <= MAX_KAFKA_KEY_BYTES):
            raise DispatchInputError("Publication kafka_key exceeds byte bounds")
        if not (2 <= len(record.kafka_value) <= MAX_KAFKA_VALUE_BYTES):
            raise DispatchInputError("Publication kafka_value exceeds byte bounds")
        if not (0 < record.estimated_record_bytes <= MAX_KAFKA_RECORD_ESTIMATED_BYTES):
            raise DispatchInputError("Publication wire record size exceeds bounds")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        now_ts = await self.get_server_utc_time()
        values: dict[str, Any] = {
            "tenant_id": ident.tenant_id,
            "incident_id": ident.incident_id,
            "revision": Decimal(ident.revision),
            "revision_fingerprint": ident.revision_fingerprint,
            "publication_contract_version": ident.publication_contract_version,
            "envelope_schema_version": ident.envelope_schema_version,
            "handoff_schema_version": ident.handoff_schema_version,
            "event_id": record.event_id,
            "publication_fingerprint": record.publication_fingerprint,
            "handoff_id": ident.handoff_id,
            "handoff_fingerprint": ident.handoff_fingerprint,
            "topology_id": ident.topology_id,
            "topology_version": ident.topology_version,
            "topology_snapshot_fingerprint": ident.topology_snapshot_fingerprint,
            "correlation_policy_version": ident.correlation_policy_version,
            "correlation_config_fingerprint": ident.correlation_config_fingerprint,
            "intent_time": ident.intent_time,
            "kafka_topic": record.topic.value,
            "kafka_key": record.kafka_key,
            "kafka_value": record.kafka_value,
            "kafka_headers_json": record.kafka_headers_json,
            "wire_record_bytes": record.estimated_record_bytes,
            "created_at": now_ts,
            "dispatch_state": DispatchState.PENDING.value,
            "attempt_count": 0,
            "claim_token": None,
            "claimed_by": None,
            "claimed_at": None,
            "claim_expires_at": None,
            "last_attempt_at": None,
            "next_retry_at": None,
            "last_error_code": None,
            "last_error_message": None,
            "broker_topic": None,
            "broker_partition": None,
            "broker_offset": None,
            "broker_timestamp_ms": None,
            "acknowledged_at": None,
            "updated_at": now_ts,
        }

        stmt = (
            pg_insert(IncidentPublicationDispatchRecord)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=["tenant_id", "incident_id", "revision"]
            )
            .returning(IncidentPublicationDispatchRecord)
        )
        try:
            inserted = (await self._session.execute(stmt)).scalar_one_or_none()
            if inserted is not None:
                return StageDispatchRecordResult(
                    dispatch=_row_to_persisted_dispatch(
                        inserted, self._tenant_id, verify_wire=False
                    ),
                    created=True,
                )
            existing = await self.load_dispatch(ident.incident_id, ident.revision)
            if existing is None:
                raise DispatchStoreError(
                    "Dispatch row conflict occurred without readable existing row"
                )
            _assert_existing_matches_staged_record(existing, record)
            return StageDispatchRecordResult(dispatch=existing, created=False)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def stage_rejected_unsendable_dispatch(
        self, draft: UnsendableDispatchDraft
    ) -> StageDispatchRecordResult:
        if not isinstance(draft, UnsendableDispatchDraft):
            raise DispatchInputError("draft must be an UnsendableDispatchDraft")
        if draft.tenant_id != self._tenant_id:
            raise IncidentPersistenceTenantError(
                "Unsendable draft tenant_id does not match repository tenant"
            )
        _validate_revision_int(draft.revision)
        _validate_fingerprint(draft.revision_fingerprint, "revision_fingerprint")
        _validate_fingerprint(draft.publication_fingerprint, "publication_fingerprint")
        _validate_fingerprint(draft.handoff_fingerprint, "handoff_fingerprint")
        _validate_fingerprint(
            draft.topology_snapshot_fingerprint, "topology_snapshot_fingerprint"
        )
        _validate_fingerprint(
            draft.correlation_config_fingerprint, "correlation_config_fingerprint"
        )
        _validate_utc_datetime(draft.intent_time, "intent_time")
        if draft.kafka_topic != INCIDENT_PUBLICATION_TOPIC.value:
            raise DispatchInputError("Unsendable draft topic must be aegis.incidents")
        if not (1 <= len(draft.kafka_key) <= MAX_KAFKA_KEY_BYTES):
            raise DispatchInputError("Unsendable draft kafka_key exceeds byte bounds")
        if not _is_strict_int(draft.wire_record_bytes) or draft.wire_record_bytes <= 0:
            raise DispatchInputError("wire_record_bytes must be a positive integer")
        error_code = _sanitize_error_code(draft.error_code)
        error_message = _sanitize_error_message(draft.error_message)

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        now_ts = await self.get_server_utc_time()
        headers_json = [[k, v] for k, v in draft.kafka_headers_json]
        values: dict[str, Any] = {
            "tenant_id": draft.tenant_id,
            "incident_id": draft.incident_id,
            "revision": Decimal(draft.revision),
            "revision_fingerprint": draft.revision_fingerprint,
            "publication_contract_version": PUBLICATION_CONTRACT_VERSION,
            "envelope_schema_version": ENVELOPE_SCHEMA_VERSION,
            "handoff_schema_version": HANDOFF_SCHEMA_VERSION,
            "event_id": draft.event_id,
            "publication_fingerprint": draft.publication_fingerprint,
            "handoff_id": draft.handoff_id,
            "handoff_fingerprint": draft.handoff_fingerprint,
            "topology_id": draft.topology_id,
            "topology_version": draft.topology_version,
            "topology_snapshot_fingerprint": draft.topology_snapshot_fingerprint,
            "correlation_policy_version": draft.correlation_policy_version,
            "correlation_config_fingerprint": draft.correlation_config_fingerprint,
            "intent_time": draft.intent_time,
            "kafka_topic": draft.kafka_topic,
            "kafka_key": draft.kafka_key,
            "kafka_value": None,
            "kafka_headers_json": headers_json,
            "wire_record_bytes": draft.wire_record_bytes,
            "created_at": now_ts,
            "dispatch_state": DispatchState.REJECTED_UNSENDABLE.value,
            "attempt_count": 0,
            "claim_token": None,
            "claimed_by": None,
            "claimed_at": None,
            "claim_expires_at": None,
            "last_attempt_at": None,
            "next_retry_at": None,
            "last_error_code": error_code,
            "last_error_message": error_message,
            "broker_topic": None,
            "broker_partition": None,
            "broker_offset": None,
            "broker_timestamp_ms": None,
            "acknowledged_at": None,
            "updated_at": now_ts,
        }

        stmt = (
            pg_insert(IncidentPublicationDispatchRecord)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=["tenant_id", "incident_id", "revision"]
            )
            .returning(IncidentPublicationDispatchRecord)
        )
        try:
            inserted = (await self._session.execute(stmt)).scalar_one_or_none()
            if inserted is not None:
                return StageDispatchRecordResult(
                    dispatch=_row_to_persisted_dispatch(
                        inserted, self._tenant_id, verify_wire=False
                    ),
                    created=True,
                )
            existing = await self.load_dispatch(draft.incident_id, draft.revision)
            if existing is None:
                raise DispatchStoreError(
                    "Unsendable dispatch conflict occurred without readable existing row"
                )
            _assert_existing_matches_unsendable_draft(existing, draft)
            return StageDispatchRecordResult(dispatch=existing, created=False)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def claim_dispatch(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        claimed_by: str,
        claim_lease_seconds: int,
        max_dispatch_attempts: int,
    ) -> PersistedPublicationDispatch | None:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")
        worker = _validate_claimed_by(claimed_by)
        if not _is_strict_int(claim_lease_seconds) or not (
            5 <= claim_lease_seconds <= 600
        ):
            raise DispatchInputError("claim_lease_seconds must be an integer in 5..600")
        if not _is_strict_int(max_dispatch_attempts) or not (
            1 <= max_dispatch_attempts <= MAX_DISPATCH_ATTEMPTS_CEILING
        ):
            raise DispatchInputError(
                f"max_dispatch_attempts must be an integer in 1..{MAX_DISPATCH_ATTEMPTS_CEILING}"
            )

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            UPDATE incident_publication_dispatches AS d
            SET
                dispatch_state = 'in_flight',
                attempt_count = d.attempt_count + 1,
                claim_token = :claim_token,
                claimed_by = :claimed_by,
                claimed_at = ts.server_now,
                claim_expires_at = ts.server_now + make_interval(secs => :lease_secs),
                last_attempt_at = ts.server_now,
                next_retry_at = NULL,
                updated_at = GREATEST(ts.server_now, d.updated_at)
            FROM ts
            WHERE
                d.tenant_id = :tenant_id
                AND d.incident_id = :incident_id
                AND d.revision = :revision
                AND d.attempt_count < :max_attempts
                AND (
                    d.dispatch_state = 'pending'
                    OR (
                        d.dispatch_state = 'retry_scheduled'
                        AND d.next_retry_at IS NOT NULL
                        AND ts.server_now >= d.next_retry_at
                    )
                    OR (
                        d.dispatch_state = 'ambiguous_outcome'
                        AND d.claim_expires_at IS NOT NULL
                        AND ts.server_now >= d.claim_expires_at
                        AND (d.next_retry_at IS NULL OR ts.server_now >= d.next_retry_at)
                    )
                    OR (
                        d.dispatch_state = 'in_flight'
                        AND d.claim_expires_at IS NOT NULL
                        AND ts.server_now >= d.claim_expires_at
                        AND (d.next_retry_at IS NULL OR ts.server_now >= d.next_retry_at)
                    )
                )
                AND (
                    SELECT count(*)::bigint
                    FROM incident_publication_intents i
                    WHERE i.tenant_id = :tenant_id
                      AND i.incident_id = :incident_id
                      AND i.revision < :revision
                ) = :expected_predecessors
                AND (
                    SELECT count(*)::bigint
                    FROM incident_publication_dispatches d2
                    WHERE d2.tenant_id = :tenant_id
                      AND d2.incident_id = :incident_id
                      AND d2.revision < :revision
                      AND d2.dispatch_state = 'acknowledged'
                ) = :expected_predecessors
            RETURNING d.*
            """
        )

        try:
            res = await self._session.execute(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "expected_predecessors": rev - 1,
                    "claim_token": claim_token,
                    "claimed_by": worker,
                    "lease_secs": float(claim_lease_seconds),
                    "max_attempts": max_dispatch_attempts,
                },
            )
            row = res.mappings().one_or_none()
            if row is None:
                return None
            return _row_to_persisted_dispatch(
                _MappingRow(row), self._tenant_id, verify_wire=False
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def verify_active_claim(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
    ) -> bool:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            SELECT 1
            FROM incident_publication_dispatches d
            WHERE d.tenant_id = :tenant_id
              AND d.incident_id = :incident_id
              AND d.revision = :revision
              AND d.dispatch_state = 'in_flight'
              AND d.claim_token = :claim_token
              AND d.claim_expires_at IS NOT NULL
              AND d.claim_expires_at > clock_timestamp()
            """
        )
        try:
            found = await self._session.scalar(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "claim_token": claim_token,
                },
            )
            return found is not None
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def load_active_claimed_dispatch(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
    ) -> PersistedPublicationDispatch | None:
        """Load and validate the authoritative active in_flight row for a claim token."""
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            SELECT d.*
            FROM incident_publication_dispatches d
            WHERE d.tenant_id = :tenant_id
              AND d.incident_id = :incident_id
              AND d.revision = :revision
              AND d.dispatch_state = 'in_flight'
              AND d.claim_token = :claim_token
              AND d.claim_expires_at IS NOT NULL
              AND d.claim_expires_at > clock_timestamp()
            """
        )
        try:
            res = await self._session.execute(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "claim_token": claim_token,
                },
            )
            row = res.mappings().one_or_none()
            if row is None:
                return None
            return _row_to_persisted_dispatch(
                _MappingRow(row), self._tenant_id, verify_wire=False
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def mark_expired_in_flight_exhausted(
        self,
        *,
        incident_id: UUID,
        revision: int,
        max_dispatch_attempts: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch | None:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not _is_strict_int(max_dispatch_attempts) or not (
            1 <= max_dispatch_attempts <= MAX_DISPATCH_ATTEMPTS_CEILING
        ):
            raise DispatchInputError(
                f"max_dispatch_attempts must be an integer in 1..{MAX_DISPATCH_ATTEMPTS_CEILING}"
            )
        code = _sanitize_error_code(error_code)
        msg = _sanitize_error_message(error_message)

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            UPDATE incident_publication_dispatches AS d
            SET
                dispatch_state = 'exhausted',
                claim_token = NULL,
                next_retry_at = NULL,
                last_error_code = :error_code,
                last_error_message = :error_message,
                updated_at = GREATEST(ts.server_now, d.updated_at)
            FROM ts
            WHERE
                d.tenant_id = :tenant_id
                AND d.incident_id = :incident_id
                AND d.revision = :revision
                AND d.dispatch_state = 'in_flight'
                AND d.claim_expires_at IS NOT NULL
                AND ts.server_now >= d.claim_expires_at
                AND d.attempt_count >= :max_attempts
            RETURNING d.*
            """
        )
        try:
            res = await self._session.execute(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "max_attempts": max_dispatch_attempts,
                    "error_code": code,
                    "error_message": msg,
                },
            )
            row = res.mappings().one_or_none()
            if row is None:
                return None
            return _row_to_persisted_dispatch(_MappingRow(row), self._tenant_id)
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def record_acknowledged(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        publish_result: PublishResult,
        expected_record: IncidentPublicationRecord | None = None,
    ) -> PersistedPublicationDispatch:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")
        if not isinstance(publish_result, PublishResult):
            raise DispatchInputError("publish_result must be a PublishResult")
        if publish_result.topic != INCIDENT_PUBLICATION_TOPIC.value:
            raise DispatchInputError("publish_result topic must be aegis.incidents")
        if not _is_strict_int(publish_result.partition) or publish_result.partition < 0:
            raise DispatchInputError("publish_result partition must be non-negative")
        if not _is_strict_int(publish_result.offset) or publish_result.offset < 0:
            raise DispatchInputError("publish_result offset must be non-negative")
        if (
            not _is_strict_int(publish_result.serialized_bytes)
            or publish_result.serialized_bytes <= 0
        ):
            raise DispatchInputError(
                "publish_result serialized_bytes must be a positive integer"
            )

        extra_predicates = ""
        params: dict[str, Any] = {
            "tenant_id": self._tenant_id,
            "incident_id": incident_id,
            "revision": Decimal(rev),
            "claim_token": claim_token,
            "broker_topic": publish_result.topic,
            "broker_partition": publish_result.partition,
            "broker_offset": publish_result.offset,
            "broker_timestamp_ms": publish_result.timestamp_ms,
            "serialized_bytes": publish_result.serialized_bytes,
        }

        if expected_record is not None:
            if not isinstance(expected_record, IncidentPublicationRecord):
                raise DispatchInputError(
                    "expected_record must be an IncidentPublicationRecord"
                )
            ident = expected_record.identity
            if (
                ident.tenant_id != self._tenant_id
                or ident.incident_id != incident_id
                or ident.revision != rev
                or publish_result.serialized_bytes != len(expected_record.kafka_value)
            ):
                raise DispatchInputError(
                    "Expected publication record does not match target dispatch or receipt size"
                )
            extra_predicates = """
                AND d.event_id = :expected_event_id
                AND d.publication_fingerprint = :expected_publication_fingerprint
                AND d.revision_fingerprint = :expected_revision_fingerprint
                AND d.handoff_id = :expected_handoff_id
                AND d.handoff_fingerprint = :expected_handoff_fingerprint
                AND d.kafka_key = :expected_kafka_key
                AND d.kafka_value = :expected_kafka_value
                AND d.kafka_headers_json = CAST(:expected_kafka_headers_json AS jsonb)
                AND d.wire_record_bytes = :expected_wire_record_bytes
            """
            params.update(
                {
                    "expected_event_id": expected_record.event_id,
                    "expected_publication_fingerprint": (
                        expected_record.publication_fingerprint
                    ),
                    "expected_revision_fingerprint": ident.revision_fingerprint,
                    "expected_handoff_id": ident.handoff_id,
                    "expected_handoff_fingerprint": ident.handoff_fingerprint,
                    "expected_kafka_key": expected_record.kafka_key,
                    "expected_kafka_value": expected_record.kafka_value,
                    "expected_kafka_headers_json": json.dumps(
                        expected_record.kafka_headers_json,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                    "expected_wire_record_bytes": (
                        expected_record.estimated_record_bytes
                    ),
                }
            )

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            f"""
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            UPDATE incident_publication_dispatches AS d
            SET
                dispatch_state = 'acknowledged',
                claim_token = NULL,
                next_retry_at = NULL,
                last_error_code = NULL,
                last_error_message = NULL,
                broker_topic = :broker_topic,
                broker_partition = :broker_partition,
                broker_offset = :broker_offset,
                broker_timestamp_ms = :broker_timestamp_ms,
                acknowledged_at = ts.server_now,
                updated_at = GREATEST(ts.server_now, d.updated_at)
            FROM ts
            WHERE
                d.tenant_id = :tenant_id
                AND d.incident_id = :incident_id
                AND d.revision = :revision
                AND d.dispatch_state = 'in_flight'
                AND d.claim_token = :claim_token
                AND d.kafka_value IS NOT NULL
                AND octet_length(d.kafka_value) = :serialized_bytes
                {extra_predicates}
            RETURNING d.*
            """
        )
        try:
            res = await self._session.execute(stmt, params)
            row = res.mappings().one_or_none()
            if row is None:
                # Distinguish wire/receipt identity mismatch from lost/stale claim token.
                still_held = await self._session.scalar(
                    text(
                        """
                        SELECT 1
                        FROM incident_publication_dispatches d
                        WHERE d.tenant_id = :tenant_id
                          AND d.incident_id = :incident_id
                          AND d.revision = :revision
                          AND d.dispatch_state = 'in_flight'
                          AND d.claim_token = :claim_token
                        """
                    ),
                    {
                        "tenant_id": self._tenant_id,
                        "incident_id": incident_id,
                        "revision": Decimal(rev),
                        "claim_token": claim_token,
                    },
                )
                if still_held is not None:
                    raise DispatchInputError(
                        "Expected publication record or receipt does not match stored immutable dispatch wire identity"
                    )
                raise DispatchClaimLostError(
                    "Active dispatch claim was lost before acknowledgement bookkeeping"
                )
            return _row_to_persisted_dispatch(
                _MappingRow(row), self._tenant_id, verify_wire=False
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def record_retry_scheduled(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        max_dispatch_attempts: int,
        backoff_ms: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")
        if not _is_strict_int(max_dispatch_attempts) or not (
            1 <= max_dispatch_attempts <= MAX_DISPATCH_ATTEMPTS_CEILING
        ):
            raise DispatchInputError(
                f"max_dispatch_attempts must be an integer in 1..{MAX_DISPATCH_ATTEMPTS_CEILING}"
            )
        if not _is_strict_int(backoff_ms) or not (10 <= backoff_ms <= 300_000):
            raise DispatchInputError("backoff_ms must be an integer in 10..300000")
        code = _sanitize_error_code(error_code)
        msg = _sanitize_error_message(error_message)

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            UPDATE incident_publication_dispatches AS d
            SET
                dispatch_state = 'retry_scheduled',
                claim_token = NULL,
                claim_expires_at = NULL,
                next_retry_at = ts.server_now + make_interval(secs => :backoff_secs),
                last_error_code = :error_code,
                last_error_message = :error_message,
                updated_at = GREATEST(ts.server_now, d.updated_at)
            FROM ts
            WHERE
                d.tenant_id = :tenant_id
                AND d.incident_id = :incident_id
                AND d.revision = :revision
                AND d.dispatch_state = 'in_flight'
                AND d.claim_token = :claim_token
                AND d.attempt_count < :max_attempts
            RETURNING d.*
            """
        )
        try:
            res = await self._session.execute(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "claim_token": claim_token,
                    "max_attempts": max_dispatch_attempts,
                    "backoff_secs": backoff_ms / 1000.0,
                    "error_code": code,
                    "error_message": msg,
                },
            )
            row = res.mappings().one_or_none()
            if row is None:
                raise DispatchClaimLostError(
                    "Active dispatch claim was lost before retry scheduling"
                )
            return _row_to_persisted_dispatch(
                _MappingRow(row), self._tenant_id, verify_wire=False
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def record_ambiguous_outcome(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        max_dispatch_attempts: int,
        backoff_ms: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch:
        """Record ``ambiguous_outcome`` while retaining the prior ``claim_expires_at``."""
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")
        if not _is_strict_int(max_dispatch_attempts) or not (
            1 <= max_dispatch_attempts <= MAX_DISPATCH_ATTEMPTS_CEILING
        ):
            raise DispatchInputError(
                f"max_dispatch_attempts must be an integer in 1..{MAX_DISPATCH_ATTEMPTS_CEILING}"
            )
        if not _is_strict_int(backoff_ms) or not (10 <= backoff_ms <= 300_000):
            raise DispatchInputError("backoff_ms must be an integer in 10..300000")
        code = _sanitize_error_code(error_code)
        msg = _sanitize_error_message(error_message)

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            UPDATE incident_publication_dispatches AS d
            SET
                dispatch_state = 'ambiguous_outcome',
                claim_token = NULL,
                next_retry_at = ts.server_now + make_interval(secs => :backoff_secs),
                last_error_code = :error_code,
                last_error_message = :error_message,
                updated_at = GREATEST(ts.server_now, d.updated_at)
            FROM ts
            WHERE
                d.tenant_id = :tenant_id
                AND d.incident_id = :incident_id
                AND d.revision = :revision
                AND d.dispatch_state = 'in_flight'
                AND d.claim_token = :claim_token
                AND d.attempt_count < :max_attempts
            RETURNING d.*
            """
        )
        try:
            res = await self._session.execute(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "claim_token": claim_token,
                    "max_attempts": max_dispatch_attempts,
                    "backoff_secs": backoff_ms / 1000.0,
                    "error_code": code,
                    "error_message": msg,
                },
            )
            row = res.mappings().one_or_none()
            if row is None:
                raise DispatchClaimLostError(
                    "Active dispatch claim was lost before recording ambiguous outcome"
                )
            return _row_to_persisted_dispatch(
                _MappingRow(row), self._tenant_id, verify_wire=False
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def record_exhausted(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch:
        if not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID")
        rev = _validate_revision_int(revision)
        if not isinstance(claim_token, UUID):
            raise DispatchInputError("claim_token must be a UUID")
        code = _sanitize_error_code(error_code)
        msg = _sanitize_error_message(error_message)

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = text(
            """
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            UPDATE incident_publication_dispatches AS d
            SET
                dispatch_state = 'exhausted',
                claim_token = NULL,
                next_retry_at = NULL,
                last_error_code = :error_code,
                last_error_message = :error_message,
                updated_at = GREATEST(ts.server_now, d.updated_at)
            FROM ts
            WHERE
                d.tenant_id = :tenant_id
                AND d.incident_id = :incident_id
                AND d.revision = :revision
                AND d.dispatch_state = 'in_flight'
                AND d.claim_token = :claim_token
            RETURNING d.*
            """
        )
        try:
            res = await self._session.execute(
                stmt,
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "revision": Decimal(rev),
                    "claim_token": claim_token,
                    "error_code": code,
                    "error_message": msg,
                },
            )
            row = res.mappings().one_or_none()
            if row is None:
                raise DispatchClaimLostError(
                    "Active dispatch claim was lost before recording exhausted state"
                )
            return _row_to_persisted_dispatch(
                _MappingRow(row), self._tenant_id, verify_wire=False
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc

    async def list_recoverable_dispatches(
        self, *, limit: int, incident_id: UUID | None = None
    ) -> tuple[PersistedPublicationDispatch, ...]:
        if not _is_strict_int(limit) or not (1 <= limit <= 256):
            raise DispatchInputError("limit must be an integer between 1 and 256")
        if incident_id is not None and not isinstance(incident_id, UUID):
            raise DispatchInputError("incident_id must be a UUID when provided")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        incident_filter = "AND d.incident_id = :incident_id" if incident_id else ""
        stmt = text(
            f"""
            WITH ts AS (SELECT clock_timestamp() AS server_now)
            SELECT d.*
            FROM incident_publication_dispatches AS d, ts
            WHERE
                d.tenant_id = :tenant_id
                {incident_filter}
                AND (
                    d.dispatch_state = 'pending'
                    OR (
                        d.dispatch_state = 'retry_scheduled'
                        AND d.next_retry_at IS NOT NULL
                        AND ts.server_now >= d.next_retry_at
                    )
                    OR (
                        d.dispatch_state = 'ambiguous_outcome'
                        AND d.claim_expires_at IS NOT NULL
                        AND ts.server_now >= d.claim_expires_at
                        AND (d.next_retry_at IS NULL OR ts.server_now >= d.next_retry_at)
                    )
                    OR (
                        d.dispatch_state = 'in_flight'
                        AND d.claim_expires_at IS NOT NULL
                        AND ts.server_now >= d.claim_expires_at
                        AND (d.next_retry_at IS NULL OR ts.server_now >= d.next_retry_at)
                    )
                )
            ORDER BY d.incident_id ASC, d.revision ASC
            LIMIT :limit
            """
        )
        params: dict[str, Any] = {"tenant_id": self._tenant_id, "limit": limit}
        if incident_id is not None:
            params["incident_id"] = incident_id
        try:
            rows = (await self._session.execute(stmt, params)).mappings().all()
            return tuple(
                _row_to_persisted_dispatch(_MappingRow(r), self._tenant_id)
                for r in rows
            )
        except Exception as exc:
            raise _translate_dispatch_db_error(exc) from exc


class _MappingRow:
    """Thin attribute wrapper over SQLAlchemy RowMapping for unified row decoding."""

    def __init__(self, mapping: Any) -> None:
        self._mapping = mapping

    def __getattr__(self, name: str) -> Any:
        try:
            return self._mapping[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


__all__ = [
    "IMMUTABLE_DISPATCH_COLUMNS",
    "MAX_DISPATCH_ATTEMPTS_CEILING",
    "MAX_ERROR_CODE_LEN",
    "MAX_ERROR_MESSAGE_LEN",
    "MAX_REVISION",
    "MUTABLE_DISPATCH_COLUMNS",
    "TERMINAL_DISPATCH_STATES",
    "DispatchClaimLostError",
    "DispatchConfigurationError",
    "DispatchCorruptionError",
    "DispatchInputError",
    "DispatchState",
    "DispatchStoreError",
    "IncidentDispatchError",
    "IncidentDispatchRepository",
    "PersistedPublicationDispatch",
    "PredecessorCheckResult",
    "SqlAlchemyIncidentDispatchRepository",
    "StageDispatchRecordResult",
    "UnsendableDispatchDraft",
    "decode_dispatch_publication_record",
    "verify_runtime_role_and_isolation",
]
