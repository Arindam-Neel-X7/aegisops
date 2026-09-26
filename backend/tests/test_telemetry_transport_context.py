import uuid

from pydantic import ValidationError
import pytest

from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
)


def test_telemetry_execution_context_valid() -> None:
    run_id = uuid.uuid4()
    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0",
        reproducibility_key="rep-key-12345",
        seed=42,
    )

    assert context.run_id == run_id
    assert context.scenario_id == "cpu-saturation"
    assert context.scenario_version == "1.0"
    assert context.reproducibility_key == "rep-key-12345"
    assert context.seed == 42


def test_telemetry_execution_context_seed_bounds() -> None:
    run_id = uuid.uuid4()

    # Minimum seed 0
    ctx_min = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id="test",
        scenario_version="1.0",
        reproducibility_key="key",
        seed=0,
    )
    assert ctx_min.seed == 0

    # Maximum seed 2^64 - 1
    ctx_max = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id="test",
        scenario_version="1.0",
        reproducibility_key="key",
        seed=MAX_UINT64,
    )
    assert ctx_max.seed == MAX_UINT64

    # Negative seed rejected
    with pytest.raises(ValidationError):
        TelemetryExecutionContext(
            run_id=run_id,
            scenario_id="test",
            scenario_version="1.0",
            reproducibility_key="key",
            seed=-1,
        )

    # Seed > 2^64 - 1 rejected
    with pytest.raises(ValidationError):
        TelemetryExecutionContext(
            run_id=run_id,
            scenario_id="test",
            scenario_version="1.0",
            reproducibility_key="key",
            seed=MAX_UINT64 + 1,
        )


def test_telemetry_execution_context_empty_fields_rejected() -> None:
    run_id = uuid.uuid4()

    with pytest.raises(ValidationError):
        TelemetryExecutionContext(
            run_id=run_id,
            scenario_id="",
            scenario_version="1.0",
            reproducibility_key="key",
            seed=42,
        )

    with pytest.raises(ValidationError):
        TelemetryExecutionContext(
            run_id=run_id,
            scenario_id="test",
            scenario_version="",
            reproducibility_key="key",
            seed=42,
        )

    with pytest.raises(ValidationError):
        TelemetryExecutionContext(
            run_id=run_id,
            scenario_id="test",
            scenario_version="1.0",
            reproducibility_key="",
            seed=42,
        )


def test_telemetry_execution_context_immutability() -> None:
    context = TelemetryExecutionContext(
        run_id=uuid.uuid4(),
        scenario_id="test",
        scenario_version="1.0",
        reproducibility_key="key",
        seed=42,
    )
    with pytest.raises(ValidationError):
        context.seed = 99  # type: ignore[misc]


def test_construct_kafka_key() -> None:
    tenant_id = uuid.UUID("11111111-2222-3333-4444-555555555555")
    event = TelemetryEvent(
        tenant_id=tenant_id,
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
    )

    key_bytes = construct_kafka_key(event)
    assert isinstance(key_bytes, bytes)
    assert key_bytes == b"11111111-2222-3333-4444-555555555555:simulation:order-service"

    # Verify no run_id, event_id, or trace_id is present in the key
    assert str(event.event_id).encode("utf-8") not in key_bytes


def test_construct_kafka_headers_required_fields() -> None:
    run_id = uuid.UUID("99999999-8888-7777-6666-555555555555")
    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id="memory-exhaustion",
        scenario_version="1.0",
        reproducibility_key="rep-mem-001",
        seed=18446744073709551615,
    )

    headers = construct_kafka_headers(context)
    header_dict = dict(headers)

    assert len(headers) == 5
    assert header_dict["run_id"] == b"99999999-8888-7777-6666-555555555555"
    assert header_dict["scenario_id"] == b"memory-exhaustion"
    assert header_dict["scenario_version"] == b"1.0"
    assert header_dict["reproducibility_key"] == b"rep-mem-001"
    assert header_dict["seed"] == b"18446744073709551615"

    # Forbidden duplicate headers must not be present
    forbidden_keys = {"event_id", "event_type", "service", "tenant_id", "environment", "payload", "published_at"}
    assert forbidden_keys.isdisjoint(header_dict.keys())


def test_construct_kafka_headers_with_optional_producer_version() -> None:
    context = TelemetryExecutionContext(
        run_id=uuid.uuid4(),
        scenario_id="test",
        scenario_version="1.0",
        reproducibility_key="key",
        seed=42,
    )

    headers = construct_kafka_headers(context, producer_version="v1.0.0")
    header_dict = dict(headers)
    assert len(headers) == 6
    assert header_dict["producer_version"] == b"v1.0.0"


def test_topic_routing_all_six_event_types() -> None:
    assert EVENT_TYPE_TO_TOPIC[EventType.METRIC] == KafkaTopic.METRICS
    assert EVENT_TYPE_TO_TOPIC[EventType.LOG] == KafkaTopic.LOGS
    assert EVENT_TYPE_TO_TOPIC[EventType.SYSTEM] == KafkaTopic.SYSTEM_EVENTS
    assert EVENT_TYPE_TO_TOPIC[EventType.ANOMALY] == KafkaTopic.ANOMALIES
    assert EVENT_TYPE_TO_TOPIC[EventType.INCIDENT] == KafkaTopic.INCIDENTS
    assert EVENT_TYPE_TO_TOPIC[EventType.AGENT] == KafkaTopic.AGENT_EVENTS
