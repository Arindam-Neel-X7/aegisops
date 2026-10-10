from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from app.anomaly.errors import AnomalyDeserializationError, AnomalySerializationError
from app.anomaly.models import SUPPORTED_ANOMALY_SCHEMA_VERSION, AnomalySignal
from app.telemetry.schemas import EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic

ANOMALY_EVENT_TYPE: EventType = EventType.ANOMALY
ANOMALY_KAFKA_TOPIC: KafkaTopic = KafkaTopic.ANOMALIES


def serialize_anomaly_signal(signal: AnomalySignal) -> bytes:
    """Serialize an AnomalySignal to UTF-8 encoded JSON bytes."""
    try:
        return signal.model_dump_json().encode("utf-8")
    except Exception as exc:
        raise AnomalySerializationError(
            f"Failed to serialize AnomalySignal {getattr(signal, 'signal_id', 'unknown')}: {exc}"
        ) from exc


def deserialize_anomaly_signal(raw: bytes | bytearray | memoryview) -> AnomalySignal:
    """Deserialize UTF-8 encoded JSON bytes into an AnomalySignal.

    Rejects invalid JSON, unsupported schema versions, and malformed fields.
    """
    try:
        raw_bytes = bytes(raw)
        data: Any = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(data, dict):
            raise AnomalyDeserializationError(
                "Deserialized JSON root must be an object"
            )

        if "schema_version" in data:
            declared_version = data["schema_version"]
            if declared_version != SUPPORTED_ANOMALY_SCHEMA_VERSION:
                raise AnomalyDeserializationError(
                    f"Unsupported schema_version: '{declared_version}'. "
                    f"Expected '{SUPPORTED_ANOMALY_SCHEMA_VERSION}'"
                )

        return AnomalySignal.model_validate(data)
    except AnomalyDeserializationError:
        raise
    except (
        json.JSONDecodeError,
        ValidationError,
        ValueError,
        TypeError,
        UnicodeDecodeError,
    ) as exc:
        raise AnomalyDeserializationError(
            f"Failed to deserialize AnomalySignal: {exc}"
        ) from exc


def construct_anomaly_kafka_key(signal: AnomalySignal) -> bytes:
    """Construct partition routing key from tenant_id:environment:service.

    Ensures anomaly signals route consistently to the same partition as the
    corresponding raw telemetry events.
    """
    return f"{signal.tenant_id}:{signal.environment}:{signal.service}".encode("utf-8")


def to_canonical_telemetry_event(signal: AnomalySignal) -> TelemetryEvent:
    """Package an AnomalySignal into a canonical TelemetryEvent envelope.

    Sets event_type=EventType.ANOMALY, maintaining compatibility with the
    canonical telemetry contract without modifying TelemetryEvent.
    """
    return TelemetryEvent(
        schema_version="1.0",
        event_id=signal.signal_id,
        event_time=signal.event_time,
        tenant_id=signal.tenant_id,
        environment=signal.environment,
        service=signal.service,
        event_type=EventType.ANOMALY,
        severity=signal.severity,
        trace_id=signal.trace_id,
        payload=signal.model_dump(mode="json"),
    )
