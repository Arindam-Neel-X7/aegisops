from datetime import datetime, timezone
import uuid

from aiokafka import AIOKafkaConsumer, TopicPartition
import pytest

from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    deserialize_event,
)


@pytest.mark.asyncio
async def test_real_kafka_producer_integration() -> None:
    """End-to-end integration test publishing to the real Step 2.3 Kafka broker on localhost:9092."""
    bootstrap_servers = "localhost:9092"

    # 1. Distinctive event_time (fixed historical instant)
    fixed_event_time = datetime(2026, 9, 26, 10, 0, 0, 0, tzinfo=timezone.utc)
    event_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    run_id = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=fixed_event_time,
        tenant_id=tenant_id,
        environment="simulation",
        service="api-gateway",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.INFO,
        trace_id="trace-step24-verify",
        payload={"marker": "step_2_4_producer_verification", "test_run": True},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id="step-2-4-verification",
        scenario_version="1.0",
        reproducibility_key="rep-step24-001",
        seed=123456789,
    )

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="step24-test-producer",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        publish_wall_clock_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        result = await producer.publish(event, context)

        # 2. Verify PublishResult
        assert result.topic == KafkaTopic.SYSTEM_EVENTS.value
        assert result.partition == 0
        assert result.offset >= 0
        assert result.timestamp_ms is not None
        assert result.latency_ms >= 0.0
        assert result.serialized_bytes > 0

        # Verify producer timestamp reflects publish wall-clock time, NOT simulation event_time
        assert abs(result.timestamp_ms - publish_wall_clock_ms) < 10000
        assert result.timestamp_ms != int(fixed_event_time.timestamp() * 1000)

        # 3. Consume the exact published message using test consumer
        tp = TopicPartition(result.topic, result.partition)
        consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            consumer.assign([tp])
            consumer.seek(tp, result.offset)

            msg = await consumer.getone()
            assert msg.offset == result.offset
            assert msg.topic == KafkaTopic.SYSTEM_EVENTS.value

            # 4. Verify Kafka Key
            expected_key = f"{tenant_id}:simulation:api-gateway".encode("utf-8")
            assert msg.key == expected_key

            # 5. Verify Deserialization & Event Identity Preservation
            reconstructed = deserialize_event(msg.value)
            assert reconstructed.event_id == event_id
            assert reconstructed.event_time == fixed_event_time
            assert reconstructed.tenant_id == tenant_id
            assert reconstructed.service == "api-gateway"
            assert reconstructed.event_type == EventType.SYSTEM
            assert reconstructed.severity == EventSeverity.INFO
            assert reconstructed.trace_id == "trace-step24-verify"
            assert reconstructed.payload == {"marker": "step_2_4_producer_verification", "test_run": True}

            # 6. Verify Headers
            assert msg.headers is not None
            header_dict = dict(msg.headers)
            assert header_dict["run_id"] == str(run_id).encode("utf-8")
            assert header_dict["scenario_id"] == b"step-2-4-verification"
            assert header_dict["scenario_version"] == b"1.0"
            assert header_dict["reproducibility_key"] == b"rep-step24-001"
            assert header_dict["seed"] == b"123456789"
            assert header_dict["producer_version"] == b"step2.4"

            # 7. Verify CreateTime timestamp semantics on broker
            # timestamp_type == 0 is CREATE_TIME in Kafka wire protocol
            assert msg.timestamp_type == 0
            assert msg.timestamp == result.timestamp_ms
            assert msg.timestamp != int(fixed_event_time.timestamp() * 1000)

        finally:
            await consumer.stop()

    finally:
        await producer.close()
