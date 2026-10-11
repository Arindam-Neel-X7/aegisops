from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.telemetry.schemas import EventSeverity

SUPPORTED_INCIDENT_SCHEMA_VERSION = "1.0"

MAX_UINT64 = 18446744073709551615
MAX_ENV_SERVICE_LEN = 128
MAX_PROVIDER_LEN = 128
MAX_POLICY_LEN = 128
MAX_INCIDENT_NUMBER_LEN = 128
MAX_SUMMARY_LEN = 512
MAX_REASON_LEN = 512
MAX_SIGNAL_REFS = 1024
MAX_EVIDENCE_ENTRIES = 4096
MAX_AFFECTED_SERVICES = 512
MAX_PATH_SUMMARIES = 512
MAX_PATH_SUMMARY_LEN = 512
MAX_TOPOLOGY_VER_LEN = 128
MIN_REVISION = 1
MAX_REVISION = MAX_UINT64
MIN_HOPS = 0
MAX_HOPS = 32
FINGERPRINT_LEN = 64
PLACEHOLDER_FINGERPRINT = "0" * FINGERPRINT_LEN
_SKIP_FINGERPRINT = "_skip_revision_fingerprint_check"

_HEX_CHARS = frozenset("0123456789abcdef")
_UTC_OFFSET = timezone.utc.utcoffset(None)


def _validate_fingerprint(value: Any, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if len(value) != FINGERPRINT_LEN:
        raise ValueError(f"{field_name} must be exactly 64 characters")
    if any(c not in _HEX_CHARS for c in value):
        raise ValueError(f"{field_name} must be lowercase hexadecimal")
    return str(value)


def _validate_bounded_text(value: Any, max_len: int, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field_name} must not be empty or whitespace-only")
    if len(trimmed) > max_len:
        raise ValueError(f"{field_name} exceeds maximum length {max_len}")
    return str(trimmed)


def _validate_utc(value: Any, info: ValidationInfo) -> datetime:
    if isinstance(value, str):
        if info.mode == "python":
            raise ValueError("datetime object required in python validation mode")
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception as exc:
            raise ValueError("invalid ISO datetime string") from exc
        value = dt
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("timestamp must be timezone-aware")
        if value.tzinfo.utcoffset(value) != _UTC_OFFSET:
            raise ValueError("timestamp must be UTC")
        return value
    raise ValueError("timestamp must be a datetime or ISO string")


def _parse_utc_datetime(value: Any, info: ValidationInfo, field_name: str) -> datetime:
    if isinstance(value, str):
        if info.mode == "python":
            raise ValueError(
                f"{field_name} must be a datetime instance, not a string in Python mode"
            )
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception as exc:
            raise ValueError(
                f"{field_name} must be a valid ISO datetime string"
            ) from exc
        value = dt
    return _validate_utc(value, info)


def _validate_not_bool(value: Any) -> Any:
    if isinstance(value, bool):
        raise ValueError("boolean is not accepted where an integer is required")
    return value


def _validate_score(value: Any) -> Any:
    if isinstance(value, bool):
        raise ValueError("score must be a float, not boolean")
    if isinstance(value, int):
        raise ValueError("score must be a float, not integer")
    if not isinstance(value, float):
        raise ValueError("score must be a float")
    if not math.isfinite(value):
        raise ValueError("score must be finite")
    if value < 0.0 or value > 1.0:
        raise ValueError("score must be within [0.0, 1.0]")
    return value


def _canonical_model_key(model: object) -> str:
    return json.dumps(
        model.model_dump(mode="json"),  # type: ignore[attr-defined]
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _canonical_affected(value: Any) -> Any:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise ValueError("affected_services must be a list or tuple of strings")
    cleaned: list[str] = []
    for entry in list(value):
        cleaned.append(
            _validate_bounded_text(
                entry, MAX_ENV_SERVICE_LEN, "affected_services entry"
            )
        )
    if len(cleaned) < 1:
        raise ValueError("affected_services must contain at least one service")
    if len(cleaned) > MAX_AFFECTED_SERVICES:
        raise ValueError("affected_services exceeds maximum entries")
    if len(set(cleaned)) != len(cleaned):
        raise ValueError("affected_services must be unique")
    return tuple(sorted(cleaned))


def _canonical_paths(value: Any) -> Any:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise ValueError("path_summaries must be a list or tuple of strings")
    items = list(value)
    if len(items) > MAX_PATH_SUMMARIES:
        raise ValueError("path_summaries exceeds maximum entries")
    return tuple(
        _validate_bounded_text(entry, MAX_PATH_SUMMARY_LEN, "path summary")
        for entry in items
    )


class _FrozenStrictBase(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class IncidentStatus(StrEnum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    CLOSED = "closed"


class CorrelationWindowState(StrEnum):
    COLLECTING = "collecting"
    SETTLING = "settling"
    FINALIZED = "finalized"


class IncidentTimelineEventType(StrEnum):
    CREATED = "created"
    SIGNAL_ATTACHED = "signal_attached"
    SEVERITY_CHANGED = "severity_changed"
    TOPOLOGY_ENRICHED = "topology_enriched"
    CORRELATION_FINALIZED = "correlation_finalized"


class IncidentSignalReference(_FrozenStrictBase):
    signal_id: uuid.UUID
    semantic_fingerprint: str
    tenant_id: uuid.UUID
    environment: str
    service: str
    event_time: AwareDatetime
    severity: EventSeverity

    @field_validator("semantic_fingerprint", mode="after")
    @classmethod
    def _check_fingerprint(cls, v: str) -> str:
        return _validate_fingerprint(v, "semantic_fingerprint")

    @field_validator("environment", "service", mode="after")
    @classmethod
    def _check_text(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_ENV_SERVICE_LEN, "field")

    @field_validator("event_time", mode="before")
    @classmethod
    def _check_time(cls, v: Any, info: ValidationInfo) -> Any:
        return _parse_utc_datetime(v, info, "event_time")


class CorrelationEvidence(_FrozenStrictBase):
    left_signal_id: uuid.UUID
    right_signal_id: uuid.UUID
    temporal_score: float
    service_score: float
    topology_score: float
    symptom_score: float
    context_score: float
    weighted_score: float
    accepted: bool
    policy_version: str
    reason: str

    @model_validator(mode="before")
    @classmethod
    def _canonicalize_pair(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        left = data.get("left_signal_id")
        right = data.get("right_signal_id")
        if left is None or right is None:
            return data
        try:
            left_key = (
                left.bytes
                if isinstance(left, uuid.UUID)
                else uuid.UUID(str(left)).bytes
            )
            right_key = (
                right.bytes
                if isinstance(right, uuid.UUID)
                else uuid.UUID(str(right)).bytes
            )
        except Exception:
            return data
        if left_key > right_key:
            swapped = dict(data)
            swapped["left_signal_id"] = right
            swapped["right_signal_id"] = left
            return swapped
        return data

    @field_validator(
        "temporal_score",
        "service_score",
        "topology_score",
        "symptom_score",
        "context_score",
        "weighted_score",
        mode="before",
    )
    @classmethod
    def _check_scores(cls, v: Any) -> Any:
        return _validate_score(v)

    @field_validator("policy_version", mode="after")
    @classmethod
    def _check_policy(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_POLICY_LEN, "policy_version")

    @field_validator("reason", mode="after")
    @classmethod
    def _check_reason(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_REASON_LEN, "reason")


class TopologySnapshotReference(_FrozenStrictBase):
    topology_id: uuid.UUID
    topology_version: str
    snapshot_fingerprint: str
    captured_at: AwareDatetime
    provider: str

    @field_validator("topology_version", mode="after")
    @classmethod
    def _check_version(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_TOPOLOGY_VER_LEN, "topology_version")

    @field_validator("snapshot_fingerprint", mode="after")
    @classmethod
    def _check_fingerprint(cls, v: str) -> str:
        return _validate_fingerprint(v, "snapshot_fingerprint")

    @field_validator("captured_at", mode="after")
    @classmethod
    def _check_captured(cls, v: datetime, info: ValidationInfo) -> datetime:
        return _validate_utc(v, info)

    @field_validator("provider", mode="after")
    @classmethod
    def _check_provider(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_PROVIDER_LEN, "provider")


class BlastRadiusSummary(_FrozenStrictBase):
    primary_service: str
    affected_services: tuple[str, ...]
    max_hops: int = Field(ge=MIN_HOPS, le=MAX_HOPS)
    truncated: bool
    path_summaries: tuple[str, ...] = ()
    topology_snapshot: TopologySnapshotReference

    @field_validator("primary_service", mode="after")
    @classmethod
    def _check_primary(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_ENV_SERVICE_LEN, "primary_service")

    @field_validator("affected_services", mode="before")
    @classmethod
    def _check_affected(cls, v: Any) -> Any:
        return _canonical_affected(v)

    @field_validator("max_hops", mode="before")
    @classmethod
    def _check_hops(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("path_summaries", mode="before")
    @classmethod
    def _check_paths(cls, v: Any) -> Any:
        return _canonical_paths(v)

    @model_validator(mode="after")
    def _check_membership(self) -> BlastRadiusSummary:
        if self.primary_service not in self.affected_services:
            raise ValueError("primary_service must appear in affected_services")
        return self


class IncidentTimelineEvent(_FrozenStrictBase):
    schema_version: str = SUPPORTED_INCIDENT_SCHEMA_VERSION
    event_id: uuid.UUID
    incident_id: uuid.UUID
    revision: int = Field(ge=MIN_REVISION, le=MAX_REVISION)
    event_time: AwareDatetime
    event_type: IncidentTimelineEventType
    source_signal_id: uuid.UUID | None = None
    summary: str
    revision_fingerprint: str

    @field_validator("schema_version", mode="after")
    @classmethod
    def _check_schema(cls, v: str) -> str:
        if v != SUPPORTED_INCIDENT_SCHEMA_VERSION:
            raise ValueError("unsupported schema_version")
        return v

    @field_validator("revision", mode="before")
    @classmethod
    def _check_revision(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("event_time", mode="before")
    @classmethod
    def _check_time(cls, v: Any, info: ValidationInfo) -> Any:
        return _parse_utc_datetime(v, info, "event_time")

    @field_validator("summary", mode="after")
    @classmethod
    def _check_summary(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_SUMMARY_LEN, "summary")

    @field_validator("revision_fingerprint", mode="after")
    @classmethod
    def _check_fp(cls, v: str) -> str:
        return _validate_fingerprint(v, "revision_fingerprint")


class IncidentAggregate(_FrozenStrictBase):
    schema_version: str = SUPPORTED_INCIDENT_SCHEMA_VERSION
    incident_id: uuid.UUID
    incident_number: str
    tenant_id: uuid.UUID
    environment: str
    status: IncidentStatus
    correlation_state: CorrelationWindowState
    severity: EventSeverity
    started_at: AwareDatetime
    last_seen_at: AwareDatetime
    created_at: AwareDatetime
    updated_at: AwareDatetime
    revision: int = Field(ge=MIN_REVISION, le=MAX_REVISION)
    primary_service: str
    affected_services: tuple[str, ...]
    signal_references: tuple[IncidentSignalReference, ...]
    correlation_evidence: tuple[CorrelationEvidence, ...] = ()
    topology_snapshot: TopologySnapshotReference | None = None
    blast_radius: BlastRadiusSummary | None = None
    revision_fingerprint: str

    @field_validator("schema_version", mode="after")
    @classmethod
    def _check_schema(cls, v: str) -> str:
        if v != SUPPORTED_INCIDENT_SCHEMA_VERSION:
            raise ValueError("unsupported schema_version")
        return v

    @field_validator("incident_number", mode="after")
    @classmethod
    def _check_number(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_INCIDENT_NUMBER_LEN, "incident_number")

    @field_validator("environment", "primary_service", mode="after")
    @classmethod
    def _check_text(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_ENV_SERVICE_LEN, "field")

    @field_validator(
        "started_at", "last_seen_at", "created_at", "updated_at", mode="after"
    )
    @classmethod
    def _check_times(cls, v: datetime, info: ValidationInfo) -> datetime:
        return _validate_utc(v, info)

    @field_validator("revision", mode="before")
    @classmethod
    def _check_revision(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("revision_fingerprint", mode="after")
    @classmethod
    def _check_fp(cls, v: str) -> str:
        return _validate_fingerprint(v, "revision_fingerprint")

    @field_validator("affected_services", mode="before")
    @classmethod
    def _check_affected(cls, v: Any) -> Any:
        return _canonical_affected(v)

    @field_validator("signal_references", mode="before")
    @classmethod
    def _check_refs(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("signal_references must be a list or tuple")
        items = list(v)
        if len(items) < 1:
            raise ValueError("signal_references must contain at least one reference")
        if len(items) > MAX_SIGNAL_REFS:
            raise ValueError("signal_references exceeds maximum entries")
        return tuple(items)

    @field_validator("correlation_evidence", mode="before")
    @classmethod
    def _check_evidence(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("correlation_evidence must be a list or tuple")
        items = list(v)
        if len(items) > MAX_EVIDENCE_ENTRIES:
            raise ValueError("correlation_evidence exceeds maximum entries")
        return tuple(items)

    @model_validator(mode="after")
    def _check_stored_order(self) -> IncidentAggregate:
        object.__setattr__(
            self,
            "signal_references",
            tuple(sorted(self.signal_references, key=_canonical_model_key)),
        )
        object.__setattr__(
            self,
            "correlation_evidence",
            tuple(sorted(self.correlation_evidence, key=_canonical_model_key)),
        )
        return self

    @model_validator(mode="after")
    def _check_invariants(self, info: ValidationInfo) -> IncidentAggregate:
        if self.started_at > self.last_seen_at:
            raise ValueError("started_at must be <= last_seen_at")
        if self.created_at > self.updated_at:
            raise ValueError("created_at must be <= updated_at")
        if len(self.signal_references) < 1:
            raise ValueError("at least one signal reference required")
        sig_ids = [s.signal_id for s in self.signal_references]
        if len(set(sig_ids)) != len(sig_ids):
            raise ValueError("signal IDs must be unique")
        fps = [s.semantic_fingerprint for s in self.signal_references]
        if len(set(fps)) != len(fps):
            raise ValueError("semantic fingerprints must be unique")
        for s in self.signal_references:
            if s.tenant_id != self.tenant_id:
                raise ValueError("signal reference tenant must match aggregate tenant")
            if s.environment != self.environment:
                raise ValueError(
                    "signal reference environment must match aggregate environment"
                )
        if self.primary_service not in self.affected_services:
            raise ValueError("primary_service must appear in affected_services")
        member_ids = set(sig_ids)
        for ev in self.correlation_evidence:
            if (
                ev.left_signal_id not in member_ids
                or ev.right_signal_id not in member_ids
            ):
                raise ValueError("evidence must reference only member signals")
        if info.context and info.context.get(_SKIP_FINGERPRINT):
            return self
        from app.correlation.identity import compute_revision_fingerprint

        try:
            expected = compute_revision_fingerprint(self)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError("failed to compute revision fingerprint") from exc
        if expected != self.revision_fingerprint:
            raise ValueError(
                "revision_fingerprint does not match canonical semantic projection"
            )
        return self

    @classmethod
    def create(cls, **fields: Any) -> IncidentAggregate:
        """Construct an aggregate, computing the canonical revision fingerprint."""
        from app.correlation.identity import compute_revision_fingerprint

        provisional_fields = dict(fields)
        provisional_fields["revision_fingerprint"] = PLACEHOLDER_FINGERPRINT
        provisional = cls.model_validate(
            provisional_fields, context={_SKIP_FINGERPRINT: True}
        )
        fingerprint = compute_revision_fingerprint(provisional)
        final_fields = dict(fields)
        final_fields["revision_fingerprint"] = fingerprint
        return cls.model_validate(final_fields, context={_SKIP_FINGERPRINT: True})


class IncidentHandoff(_FrozenStrictBase):
    schema_version: str = SUPPORTED_INCIDENT_SCHEMA_VERSION
    handoff_id: uuid.UUID
    incident_id: uuid.UUID
    incident_revision: int = Field(ge=MIN_REVISION, le=MAX_REVISION)
    incident_revision_fingerprint: str
    tenant_id: uuid.UUID
    environment: str
    status: IncidentStatus
    severity: EventSeverity
    primary_service: str
    affected_services: tuple[str, ...]
    signal_references: tuple[IncidentSignalReference, ...]
    topology_snapshot: TopologySnapshotReference | None = None
    blast_radius: BlastRadiusSummary | None = None
    generated_at: AwareDatetime

    @field_validator("schema_version", mode="after")
    @classmethod
    def _check_schema(cls, v: str) -> str:
        if v != SUPPORTED_INCIDENT_SCHEMA_VERSION:
            raise ValueError("unsupported schema_version")
        return v

    @field_validator("incident_revision", mode="before")
    @classmethod
    def _check_rev(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("incident_revision_fingerprint", mode="after")
    @classmethod
    def _check_fp(cls, v: str) -> str:
        return _validate_fingerprint(v, "incident_revision_fingerprint")

    @field_validator("environment", "primary_service", mode="after")
    @classmethod
    def _check_text(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_ENV_SERVICE_LEN, "field")

    @field_validator("affected_services", mode="before")
    @classmethod
    def _check_affected(cls, v: Any) -> Any:
        return _canonical_affected(v)

    @field_validator("signal_references", mode="before")
    @classmethod
    def _check_refs(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("signal_references must be a list or tuple")
        items = list(v)
        if len(items) < 1:
            raise ValueError("signal_references must contain at least one reference")
        if len(items) > MAX_SIGNAL_REFS:
            raise ValueError("signal_references exceeds maximum entries")
        return tuple(items)

    @field_validator("generated_at", mode="after")
    @classmethod
    def _check_time(cls, v: datetime, info: ValidationInfo) -> datetime:
        return _validate_utc(v, info)

    @model_validator(mode="after")
    def _check_stored_order(self) -> IncidentHandoff:
        object.__setattr__(
            self,
            "signal_references",
            tuple(sorted(self.signal_references, key=_canonical_model_key)),
        )
        return self

    @model_validator(mode="after")
    def _check_membership(self) -> IncidentHandoff:
        sig_ids = [s.signal_id for s in self.signal_references]
        if len(set(sig_ids)) != len(sig_ids):
            raise ValueError("signal IDs must be unique")
        for s in self.signal_references:
            if s.tenant_id != self.tenant_id:
                raise ValueError("signal reference tenant must match handoff tenant")
            if s.environment != self.environment:
                raise ValueError(
                    "signal reference environment must match handoff environment"
                )
        if self.primary_service not in self.affected_services:
            raise ValueError("primary_service must appear in affected_services")
        return self


def aggregate_semantic_projection(aggregate: IncidentAggregate) -> dict[str, Any]:
    """Canonical semantic projection of an aggregate excluding its fingerprint."""
    return {
        "schema_version": aggregate.schema_version,
        "incident_id": str(aggregate.incident_id),
        "incident_number": aggregate.incident_number,
        "tenant_id": str(aggregate.tenant_id),
        "environment": aggregate.environment,
        "status": aggregate.status.value,
        "correlation_state": aggregate.correlation_state.value,
        "severity": aggregate.severity.value,
        "started_at": aggregate.started_at.isoformat(),
        "last_seen_at": aggregate.last_seen_at.isoformat(),
        "created_at": aggregate.created_at.isoformat(),
        "updated_at": aggregate.updated_at.isoformat(),
        "revision": aggregate.revision,
        "primary_service": aggregate.primary_service,
        "affected_services": list(aggregate.affected_services),
        "signal_references": [
            s.model_dump(mode="json") for s in aggregate.signal_references
        ],
        "correlation_evidence": [
            e.model_dump(mode="json") for e in aggregate.correlation_evidence
        ],
        "topology_snapshot": (
            aggregate.topology_snapshot.model_dump(mode="json")
            if aggregate.topology_snapshot is not None
            else None
        ),
        "blast_radius": (
            aggregate.blast_radius.model_dump(mode="json")
            if aggregate.blast_radius is not None
            else None
        ),
    }
