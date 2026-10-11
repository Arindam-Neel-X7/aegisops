from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.correlation.errors import IncidentIdentityError
from app.correlation.models import (
    SUPPORTED_INCIDENT_SCHEMA_VERSION,
    IncidentAggregate,
    IncidentTimelineEventType,
    aggregate_semantic_projection,
)

INCIDENT_NAMESPACE: uuid.UUID = uuid.uuid5(
    uuid.NAMESPACE_DNS, "aegisops.phase4.incident"
)
TIMELINE_EVENT_NAMESPACE: uuid.UUID = uuid.uuid5(
    uuid.NAMESPACE_DNS, "aegisops.phase4.timeline-event"
)
HANDOFF_NAMESPACE: uuid.UUID = uuid.uuid5(uuid.NAMESPACE_DNS, "aegisops.phase4.handoff")

_UTC_OFFSET = timezone.utc.utcoffset(None)


def _normalize_environment(environment: Any) -> str:
    if isinstance(environment, bool) or not isinstance(environment, str):
        raise ValueError("environment must be a string")
    trimmed = environment.strip()
    if not trimmed:
        raise ValueError("environment must be non-empty")
    return str(trimmed)


def _normalize_fingerprint(value: Any, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError(f"{field_name} must be 64 lowercase hex characters")
    return str(value)


def derive_incident_id(
    *,
    tenant_id: uuid.UUID,
    environment: str,
    anchor_signal_fingerprint: str,
    schema_version: str = SUPPORTED_INCIDENT_SCHEMA_VERSION,
) -> uuid.UUID:
    """Derive a deterministic UUIDv5 incident identity from semantic anchor inputs."""
    try:
        if isinstance(tenant_id, bool) or not isinstance(tenant_id, uuid.UUID):
            raise ValueError("tenant_id must be a UUID")
        env = _normalize_environment(environment)
        fp = _normalize_fingerprint(
            anchor_signal_fingerprint, "anchor_signal_fingerprint"
        )
        if schema_version != SUPPORTED_INCIDENT_SCHEMA_VERSION:
            raise ValueError("unsupported schema_version")
        canonical = "|".join(
            [SUPPORTED_INCIDENT_SCHEMA_VERSION, str(tenant_id), env, fp]
        )
        return uuid.uuid5(INCIDENT_NAMESPACE, canonical)
    except Exception as exc:
        raise IncidentIdentityError("Failed to derive incident id") from exc


def compute_revision_fingerprint(aggregate: Any) -> str:
    """Compute the canonical SHA-256 revision fingerprint of an incident aggregate."""
    try:
        if not isinstance(aggregate, IncidentAggregate):
            raise TypeError("aggregate must be an IncidentAggregate instance")
        projection = aggregate_semantic_projection(aggregate)
        canonical = json.dumps(
            projection,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    except Exception as exc:
        raise IncidentIdentityError("Failed to compute revision fingerprint") from exc


def derive_timeline_event_id(
    *,
    incident_id: uuid.UUID,
    revision: int,
    event_type: IncidentTimelineEventType,
    event_time: datetime,
    source_signal_id: uuid.UUID | None,
    revision_fingerprint: str,
) -> uuid.UUID:
    """Derive a deterministic UUIDv5 identity for an incident timeline event."""
    try:
        if isinstance(incident_id, bool) or not isinstance(incident_id, uuid.UUID):
            raise ValueError("incident_id must be a UUID")
        if isinstance(revision, bool) or not isinstance(revision, int):
            raise ValueError("revision must be an integer")
        if not isinstance(event_type, IncidentTimelineEventType):
            raise ValueError("event_type must be an IncidentTimelineEventType")
        if isinstance(event_time, bool) or not isinstance(event_time, datetime):
            raise ValueError("event_time must be a datetime")
        if (
            event_time.tzinfo is None
            or event_time.tzinfo.utcoffset(event_time) != _UTC_OFFSET
        ):
            raise ValueError("event_time must be timezone-aware UTC")
        if source_signal_id is not None and (
            isinstance(source_signal_id, bool)
            or not isinstance(source_signal_id, uuid.UUID)
        ):
            raise ValueError("source_signal_id must be a UUID or None")
        fp = _normalize_fingerprint(revision_fingerprint, "revision_fingerprint")
        canonical = "|".join(
            [
                str(incident_id),
                str(revision),
                event_type.value,
                event_time.isoformat(),
                str(source_signal_id) if source_signal_id is not None else "",
                fp,
            ]
        )
        return uuid.uuid5(TIMELINE_EVENT_NAMESPACE, canonical)
    except Exception as exc:
        raise IncidentIdentityError("Failed to derive timeline event id") from exc


def derive_handoff_id(
    *,
    incident_id: uuid.UUID,
    incident_revision: int,
    incident_revision_fingerprint: str,
) -> uuid.UUID:
    """Derive a deterministic UUIDv5 identity for an incident Phase 5 handoff."""
    try:
        if isinstance(incident_id, bool) or not isinstance(incident_id, uuid.UUID):
            raise ValueError("incident_id must be a UUID")
        if isinstance(incident_revision, bool) or not isinstance(
            incident_revision, int
        ):
            raise ValueError("incident_revision must be an integer")
        fp = _normalize_fingerprint(
            incident_revision_fingerprint, "incident_revision_fingerprint"
        )
        canonical = "|".join([str(incident_id), str(incident_revision), fp])
        return uuid.uuid5(HANDOFF_NAMESPACE, canonical)
    except Exception as exc:
        raise IncidentIdentityError("Failed to derive handoff id") from exc
