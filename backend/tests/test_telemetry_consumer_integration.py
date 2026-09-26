from datetime import datetime, timezone
import uuid

from aiokafka import TopicPartition
import pytest

from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    EvidenceConsumer,
    MetricsConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.errors import ConsumerHandlerError
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.serialization import TelemetryExecutionContext


class RecordingHandler:
    def __init__(self) -> None:
        self.envelopes: list[TelemetryEnvelope] = []

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        self.envelopes.append(envelope)


@pytest.mark.asyncio
async def test_real_metrics_consumer_integration() -> None:
    """Integration test verifying MetricsConsumer on real Kafka (localhost:9092)."""
    bootstrap_servers = "localhost:9092"
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step25-it-metrics-{test_id}"

    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    event_time = datetime(2026, 9, 26, 12, 0, 0, 0, tzinfo=timezone.utc)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=event_time,
        tenant_id=tenant_id,
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=f"trace-metrics-{test_id}",
        payload={"metric_name": "http_request_latency_ms", "value": 128.5},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-key-{test_id}",
        seed=987654321,
    )

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=f"producer-metrics-{test_id}",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        pub_result = await producer.publish(event, context)
        assert pub_result.topic == KafkaTopic.METRICS.value

        handler = RecordingHandler()
        consumer = MetricsConsumer(
            handler=handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-metrics-{test_id}",
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            # Poll for the published record
            envelope: TelemetryEnvelope | None = None
            for _ in range(50):
                env = await consumer.consume_one(timeout_ms=1000)
                if env is not None and env.event.event_id == event_id:
                    envelope = env
                    break

            assert envelope is not None, "Failed to consume published metric event"
            assert envelope.event.event_id == event_id
            assert envelope.event.event_time == event_time
            assert envelope.event.tenant_id == tenant_id
            assert envelope.event.service == "order-service"
            assert envelope.event.event_type == EventType.METRIC
            assert envelope.context.run_id == run_id
            assert envelope.context.scenario_id == f"scenario-{test_id}"
            assert envelope.context.seed == 987654321
            assert envelope.producer_version == "step2.4"
            assert envelope.kafka_timestamp_ms == pub_result.timestamp_ms
            assert envelope.topic == KafkaTopic.METRICS.value
            assert envelope.offset == pub_result.offset
            assert len(handler.envelopes) >= 1

            # Verify manual offset commit on broker
            tp = TopicPartition(pub_result.topic, pub_result.partition)
            committed_offset = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            assert committed_offset is not None
            assert committed_offset == pub_result.offset + 1

        finally:
            await consumer.stop()

    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_real_evidence_consumer_integration() -> None:
    """Integration test verifying EvidenceConsumer for LOG and SYSTEM events on real Kafka."""
    bootstrap_servers = "localhost:9092"
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step25-it-evidence-{test_id}"

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=f"producer-evidence-{test_id}",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        # 1. Publish LOG event
        log_event_id = uuid.uuid4()
        log_run_id = uuid.uuid4()
        log_tenant_id = uuid.uuid4()
        log_event_time = datetime(2026, 9, 26, 12, 10, 0, 0, tzinfo=timezone.utc)

        log_event = TelemetryEvent(
            schema_version="1.0",
            event_id=log_event_id,
            event_time=log_event_time,
            tenant_id=log_tenant_id,
            environment="simulation",
            service="payment-service",
            event_type=EventType.LOG,
            severity=EventSeverity.WARNING,
            trace_id=f"trace-log-{test_id}",
            payload={"message": "Payment processing took longer than expected"},
        )
        log_context = TelemetryExecutionContext(
            run_id=log_run_id,
            scenario_id=f"scenario-log-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rep-log-{test_id}",
            seed=111222333,
        )
        pub_log = await producer.publish(log_event, log_context)
        assert pub_log.topic == KafkaTopic.LOGS.value

        # 2. Publish SYSTEM event
        sys_event_id = uuid.uuid4()
        sys_run_id = uuid.uuid4()
        sys_tenant_id = uuid.uuid4()
        sys_event_time = datetime(2026, 9, 26, 12, 15, 0, 0, tzinfo=timezone.utc)

        sys_event = TelemetryEvent(
            schema_version="1.0",
            event_id=sys_event_id,
            event_time=sys_event_time,
            tenant_id=sys_tenant_id,
            environment="simulation",
            service="database",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.CRITICAL,
            trace_id=f"trace-sys-{test_id}",
            payload={"marker": "connection_pool_exhausted"},
        )
        sys_context = TelemetryExecutionContext(
            run_id=sys_run_id,
            scenario_id=f"scenario-sys-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rep-sys-{test_id}",
            seed=444555666,
        )
        pub_sys = await producer.publish(sys_event, sys_context)
        assert pub_sys.topic == KafkaTopic.SYSTEM_EVENTS.value

        handler = RecordingHandler()
        consumer = EvidenceConsumer(
            handler=handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-evidence-{test_id}",
            auto_offset_reset="earliest",
        )

        assert consumer.topics == [KafkaTopic.LOGS.value, KafkaTopic.SYSTEM_EVENTS.value]
        assert KafkaTopic.METRICS.value not in consumer.topics

        await consumer.start()
        try:
            consumed_log: TelemetryEnvelope | None = None
            consumed_sys: TelemetryEnvelope | None = None

            for _ in range(50):
                if consumed_log is not None and consumed_sys is not None:
                    break
                env = await consumer.consume_one(timeout_ms=1000)
                if env is not None:
                    if env.event.event_id == log_event_id:
                        consumed_log = env
                    elif env.event.event_id == sys_event_id:
                        consumed_sys = env

            assert consumed_log is not None, "Failed to consume LOG event"
            assert consumed_sys is not None, "Failed to consume SYSTEM event"

            assert consumed_log.event.event_id == log_event_id
            assert consumed_log.event.event_time == log_event_time
            assert consumed_log.context.run_id == log_run_id
            assert consumed_log.topic == KafkaTopic.LOGS.value

            assert consumed_sys.event.event_id == sys_event_id
            assert consumed_sys.event.event_time == sys_event_time
            assert consumed_sys.context.run_id == sys_run_id
            assert consumed_sys.topic == KafkaTopic.SYSTEM_EVENTS.value

            # Verify committed offsets for both partitions
            tp_log = TopicPartition(pub_log.topic, pub_log.partition)
            tp_sys = TopicPartition(pub_sys.topic, pub_sys.partition)

            committed_log = await consumer._consumer.committed(tp_log)  # type: ignore[union-attr]
            committed_sys = await consumer._consumer.committed(tp_sys)  # type: ignore[union-attr]

            assert committed_log is not None
            assert committed_log >= pub_log.offset + 1

            assert committed_sys is not None
            assert committed_sys >= pub_sys.offset + 1

        finally:
            await consumer.stop()

    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_real_handler_failure_leaves_offset_uncommitted() -> None:
    """Integration test verifying that handler failure prevents manual offset commit on real Kafka."""
    bootstrap_servers = "localhost:9092"
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step25-it-fail-{test_id}"

    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        tenant_id=tenant_id,
        environment="simulation",
        service="api-gateway",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=f"trace-fail-{test_id}",
        payload={"metric_name": "test_failure_metric", "value": 1.0},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-fail-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-fail-{test_id}",
        seed=777888999,
    )

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=f"producer-fail-{test_id}",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        pub_result = await producer.publish(event, context)

        async def failing_handler(env: TelemetryEnvelope) -> None:
            if env.event.event_id == event_id:
                raise RuntimeError("Intentional downstream failure during test")

        consumer = MetricsConsumer(
            handler=failing_handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-fail-{test_id}",
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            tp = TopicPartition(pub_result.topic, pub_result.partition)
            initial_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]

            with pytest.raises(ConsumerHandlerError, match="Downstream handler failed"):
                for _ in range(50):
                    await consumer.consume_one(timeout_ms=1000)

            # Verify that committed offset did NOT advance past pub_result.offset
            after_failure_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            if initial_committed is None:
                assert after_failure_committed is None or after_failure_committed <= pub_result.offset
            else:
                assert after_failure_committed == initial_committed

        finally:
            await consumer.stop()

    finally:
        await producer.close()
