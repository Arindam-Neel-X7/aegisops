import uuid
from datetime import datetime, timezone
import json
import pytest
from pydantic import ValidationError

from app.telemetry.schemas import (
    TelemetryEvent,
    EventType,
    EventSeverity,
)
from app.telemetry.topics import KafkaTopic, EVENT_TYPE_TO_TOPIC


def test_telemetry_event_valid_construction():
    tenant_id = uuid.uuid4()
    event = TelemetryEvent(
        tenant_id=tenant_id,
        environment="development",
        service="test-service",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.INFO,
    )
    
    assert event.schema_version == "1.0"
    assert isinstance(event.event_id, uuid.UUID)
    assert isinstance(event.event_time, datetime)
    assert event.event_time.tzinfo == timezone.utc
    assert event.tenant_id == tenant_id
    assert event.trace_id is None
    assert event.payload == {}
    assert event.environment == "development"
    assert event.service == "test-service"


def test_payload_default_isolation():
    tenant_id = uuid.uuid4()
    event1 = TelemetryEvent(
        tenant_id=tenant_id,
        environment="development",
        service="test-service",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.INFO,
    )
    event2 = TelemetryEvent(
        tenant_id=tenant_id,
        environment="development",
        service="test-service",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.INFO,
    )
    
    assert event1.payload is not event2.payload


def test_telemetry_event_serialization():
    tenant_id = uuid.uuid4()
    event = TelemetryEvent(
        tenant_id=tenant_id,
        environment="production",
        service="test-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.WARNING,
        payload={"key": "value", "count": 42},
    )
    
    serialized = event.model_dump_json()
    data = json.loads(serialized)
    
    assert data["schema_version"] == "1.0"
    assert data["tenant_id"] == str(tenant_id)
    assert data["event_type"] == "metric"
    assert data["severity"] == "warning"
    assert data["payload"] == {"key": "value", "count": 42}
    # Verify UUID serialization format (standard 8-4-4-4-12 string)
    assert isinstance(data["event_id"], str)


def test_canonical_topic_values():
    assert KafkaTopic.METRICS == "aegis.telemetry.metrics"
    assert KafkaTopic.LOGS == "aegis.telemetry.logs"
    assert KafkaTopic.SYSTEM_EVENTS == "aegis.system.events"
    assert KafkaTopic.ANOMALIES == "aegis.ml.anomalies"
    assert KafkaTopic.INCIDENTS == "aegis.incidents"
    assert KafkaTopic.AGENT_EVENTS == "aegis.agent.events"


def test_event_type_to_topic_mapping():
    assert EVENT_TYPE_TO_TOPIC[EventType.METRIC] == KafkaTopic.METRICS
    assert EVENT_TYPE_TO_TOPIC[EventType.LOG] == KafkaTopic.LOGS
    assert EVENT_TYPE_TO_TOPIC[EventType.SYSTEM] == KafkaTopic.SYSTEM_EVENTS
    assert EVENT_TYPE_TO_TOPIC[EventType.ANOMALY] == KafkaTopic.ANOMALIES
    assert EVENT_TYPE_TO_TOPIC[EventType.INCIDENT] == KafkaTopic.INCIDENTS
    assert EVENT_TYPE_TO_TOPIC[EventType.AGENT] == KafkaTopic.AGENT_EVENTS


def test_telemetry_event_validation_empty_service():
    with pytest.raises(ValidationError):
        TelemetryEvent(
            tenant_id=uuid.uuid4(),
            environment="production",
            service="",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.INFO,
        )


def test_telemetry_event_validation_empty_environment():
    with pytest.raises(ValidationError):
        TelemetryEvent(
            tenant_id=uuid.uuid4(),
            environment="",
            service="test-service",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.INFO,
        )

def test_telemetry_event_validation_empty_schema_version():
    with pytest.raises(ValidationError):
        TelemetryEvent(
            tenant_id=uuid.uuid4(),
            environment="production",
            service="test-service",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.INFO,
            schema_version=""
        )

def test_telemetry_event_validation_naive_datetime():
    with pytest.raises(ValidationError):
        TelemetryEvent(
            tenant_id=uuid.uuid4(),
            environment="production",
            service="test-service",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.INFO,
            event_time=datetime.now() # naive
        )
