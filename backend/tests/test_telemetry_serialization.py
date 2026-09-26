from datetime import datetime, timedelta, timezone
import json
import uuid

import pytest

from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.transport.errors import (
    TelemetryDeserializationError,
    TelemetrySerializationError,
)
from app.telemetry.transport.serialization import (
    deserialize_event,
    serialize_event,
)


def test_metric_event_semantic_round_trip() -> None:
    tenant_id = uuid.uuid4()
    event_id = uuid.uuid4()
    now_utc = datetime.now(timezone.utc)
    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=now_utc,
        tenant_id=tenant_id,
        environment="production",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id="req-12345",
        payload={"metric_name": "http_requests_total", "value": 1, "status_code": 200},
    )

    raw_bytes = serialize_event(event)
    assert isinstance(raw_bytes, bytes)

    reconstructed = deserialize_event(raw_bytes)
    assert reconstructed.schema_version == "1.0"
    assert reconstructed.event_id == event_id
    assert reconstructed.event_time == now_utc
    assert reconstructed.tenant_id == tenant_id
    assert reconstructed.environment == "production"
    assert reconstructed.service == "order-service"
    assert reconstructed.event_type == EventType.METRIC
    assert reconstructed.severity == EventSeverity.INFO
    assert reconstructed.trace_id == "req-12345"
    assert reconstructed.payload == {"metric_name": "http_requests_total", "value": 1, "status_code": 200}


def test_log_event_semantic_round_trip() -> None:
    tenant_id = uuid.uuid4()
    event = TelemetryEvent(
        tenant_id=tenant_id,
        environment="staging",
        service="payment-service",
        event_type=EventType.LOG,
        severity=EventSeverity.ERROR,
        trace_id="trace-abc-999",
        payload={"message": "Payment gateway timeout", "latency_ms": 5002.5, "error": "TimeoutError"},
    )

    raw_bytes = serialize_event(event)
    reconstructed = deserialize_event(raw_bytes)

    assert reconstructed.event_id == event.event_id
    assert reconstructed.event_time == event.event_time
    assert reconstructed.event_type == EventType.LOG
    assert reconstructed.severity == EventSeverity.ERROR
    assert reconstructed.trace_id == "trace-abc-999"
    assert reconstructed.payload == event.payload


def test_system_event_semantic_round_trip() -> None:
    tenant_id = uuid.uuid4()
    event = TelemetryEvent(
        tenant_id=tenant_id,
        environment="simulation",
        service="api-gateway",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.WARNING,
        trace_id=None,
        payload={"marker": "fault_injected", "fault_type": "latency", "duration_seconds": 10.0},
    )

    raw_bytes = serialize_event(event)
    reconstructed = deserialize_event(raw_bytes)

    assert reconstructed.event_id == event.event_id
    assert reconstructed.event_time == event.event_time
    assert reconstructed.event_type == EventType.SYSTEM
    assert reconstructed.severity == EventSeverity.WARNING
    assert reconstructed.trace_id is None
    assert reconstructed.payload == event.payload


def test_trace_id_none_round_trip() -> None:
    event = TelemetryEvent(
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="inventory-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=None,
        payload={"metric_name": "simulated_cpu_utilization_pct", "value": 45.2},
    )

    raw_bytes = serialize_event(event)
    reconstructed = deserialize_event(raw_bytes)
    assert reconstructed.trace_id is None


def test_non_utc_timezone_aware_event_time_round_trip() -> None:
    tz_plus_5 = timezone(timedelta(hours=5))
    dt_offset = datetime(2026, 9, 26, 12, 0, 0, tzinfo=tz_plus_5)
    event = TelemetryEvent(
        tenant_id=uuid.uuid4(),
        environment="production",
        service="worker-service",
        event_type=EventType.LOG,
        severity=EventSeverity.INFO,
        event_time=dt_offset,
        payload={"msg": "job started"},
    )

    raw_bytes = serialize_event(event)
    reconstructed = deserialize_event(raw_bytes)

    # Must represent the exact same point in time (timestamp / UTC equivalence)
    assert reconstructed.event_time.timestamp() == dt_offset.timestamp()


def test_missing_schema_version_accepted_defaults_to_one() -> None:
    payload_dict = {
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-09-26T12:00:00Z",
        "tenant_id": str(uuid.uuid4()),
        "environment": "simulation",
        "service": "order-service",
        "event_type": "metric",
        "severity": "info",
        "payload": {"metric_name": "cpu", "value": 50},
    }
    raw_bytes = json.dumps(payload_dict).encode("utf-8")

    event = deserialize_event(raw_bytes)
    assert event.schema_version == "1.0"
    assert event.service == "order-service"


def test_explicit_unsupported_schema_version_rejected() -> None:
    payload_dict = {
        "schema_version": "2.0",
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-09-26T12:00:00Z",
        "tenant_id": str(uuid.uuid4()),
        "environment": "simulation",
        "service": "order-service",
        "event_type": "metric",
        "severity": "info",
        "payload": {},
    }
    raw_bytes = json.dumps(payload_dict).encode("utf-8")

    with pytest.raises(TelemetryDeserializationError) as exc_info:
        deserialize_event(raw_bytes)
    assert "Unsupported schema_version: 2.0" in str(exc_info.value)


def test_malformed_json_rejected() -> None:
    with pytest.raises(TelemetryDeserializationError):
        deserialize_event(b"not-a-valid-json-string{")


def test_non_dict_json_root_rejected() -> None:
    with pytest.raises(TelemetryDeserializationError):
        deserialize_event(b"[\"array\", \"not\", \"dict\"]")


def test_malformed_uuid_rejected() -> None:
    payload_dict = {
        "schema_version": "1.0",
        "event_id": "not-a-valid-uuid",
        "event_time": "2026-09-26T12:00:00Z",
        "tenant_id": str(uuid.uuid4()),
        "environment": "simulation",
        "service": "order-service",
        "event_type": "metric",
        "severity": "info",
        "payload": {},
    }
    raw_bytes = json.dumps(payload_dict).encode("utf-8")

    with pytest.raises(TelemetryDeserializationError):
        deserialize_event(raw_bytes)


def test_naive_datetime_rejected() -> None:
    payload_dict = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "event_time": "2026-09-26T12:00:00",  # naive, no timezone
        "tenant_id": str(uuid.uuid4()),
        "environment": "simulation",
        "service": "order-service",
        "event_type": "metric",
        "severity": "info",
        "payload": {},
    }
    raw_bytes = json.dumps(payload_dict).encode("utf-8")

    with pytest.raises(TelemetryDeserializationError):
        deserialize_event(raw_bytes)


def test_serialize_event_failure_wraps_in_serialization_error() -> None:
    class BadEvent:
        def model_dump_json(self) -> str:
            raise RuntimeError("JSON serialization exploded")

    with pytest.raises(TelemetrySerializationError):
        serialize_event(BadEvent())  # type: ignore[arg-type]
