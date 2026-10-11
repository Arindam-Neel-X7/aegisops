from __future__ import annotations

import json

from pydantic import ValidationError

from app.correlation.errors import (
    IncidentContractError,
    IncidentDeserializationError,
    IncidentSerializationError,
)
from app.correlation.models import (
    IncidentAggregate,
    IncidentHandoff,
    IncidentTimelineEvent,
)


def _canonical_json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _to_canonical_payload(model: object) -> dict[str, object]:
    raw = model.model_dump(mode="json")  # type: ignore[attr-defined]
    if not isinstance(raw, dict):
        raise IncidentSerializationError("Invalid model payload")
    return raw


def _deserialize_with_validation(raw_bytes: bytes, model_cls: type) -> object:
    try:
        text = raw_bytes.decode("utf-8")
    except Exception as exc:
        raise IncidentSerializationError("Invalid UTF-8 payload") from exc
    if not text.strip().startswith("{"):
        raise IncidentSerializationError("Payload must be a JSON object")
    try:
        json.loads(text)
    except Exception as exc:
        raise IncidentSerializationError("Invalid JSON payload") from exc
    try:
        return model_cls.model_validate_json(text)  # type: ignore[attr-defined, no-any-return]
    except (IncidentContractError, IncidentDeserializationError):
        raise
    except ValidationError as exc:
        raise IncidentDeserializationError(
            "Incident contract validation failed"
        ) from exc
    except (Exception, ValueError, TypeError) as exc:
        if isinstance(exc, (IncidentContractError, IncidentDeserializationError)):
            raise
        if isinstance(exc, ValidationError):
            raise IncidentDeserializationError(
                "Incident contract validation failed"
            ) from exc
        raise IncidentDeserializationError(
            "Incident contract validation failed"
        ) from exc


def serialize_incident_aggregate(aggregate: IncidentAggregate) -> bytes:
    try:
        if not isinstance(aggregate, IncidentAggregate):
            raise TypeError("aggregate must be an IncidentAggregate")
        payload = _to_canonical_payload(aggregate)
        return _canonical_json_bytes(payload)
    except (IncidentContractError, IncidentSerializationError):
        raise
    except Exception as exc:
        raise IncidentSerializationError(
            "Failed to serialize IncidentAggregate"
        ) from exc


def deserialize_incident_aggregate(
    raw: bytes | bytearray | memoryview,
) -> IncidentAggregate:
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise IncidentSerializationError("Payload must be bytes")
    return _deserialize_with_validation(bytes(raw), IncidentAggregate)  # type: ignore[return-value]


def serialize_incident_timeline_event(event: IncidentTimelineEvent) -> bytes:
    try:
        if not isinstance(event, IncidentTimelineEvent):
            raise TypeError("event must be an IncidentTimelineEvent")
        payload = _to_canonical_payload(event)
        return _canonical_json_bytes(payload)
    except (IncidentContractError, IncidentSerializationError):
        raise
    except Exception as exc:
        raise IncidentSerializationError(
            "Failed to serialize IncidentTimelineEvent"
        ) from exc


def deserialize_incident_timeline_event(
    raw: bytes | bytearray | memoryview,
) -> IncidentTimelineEvent:
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise IncidentSerializationError("Payload must be bytes")
    return _deserialize_with_validation(bytes(raw), IncidentTimelineEvent)  # type: ignore[return-value]


def serialize_incident_handoff(handoff: IncidentHandoff) -> bytes:
    try:
        if not isinstance(handoff, IncidentHandoff):
            raise TypeError("handoff must be an IncidentHandoff")
        payload = _to_canonical_payload(handoff)
        return _canonical_json_bytes(payload)
    except (IncidentContractError, IncidentSerializationError):
        raise
    except Exception as exc:
        raise IncidentSerializationError("Failed to serialize IncidentHandoff") from exc


def deserialize_incident_handoff(
    raw: bytes | bytearray | memoryview,
) -> IncidentHandoff:
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise IncidentSerializationError("Payload must be bytes")
    return _deserialize_with_validation(bytes(raw), IncidentHandoff)  # type: ignore[return-value]
