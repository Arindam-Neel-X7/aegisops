import asyncio
from datetime import datetime, timezone
from typing import Any, Sequence
from unittest.mock import AsyncMock, MagicMock
import uuid

import aiokafka
from aiokafka import TopicPartition
from aiokafka.structs import ConsumerRecord
import pytest

from app.core.config import settings
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    EvidenceConsumer,
    MetricsConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.errors import (
    ConsumerHandlerError,
    ConsumerNotStartedError,
    ConsumerRecordValidationError,
    DuplicateHeaderError,
    InvalidHeaderError,
    MissingRequiredHeaderError,
    TopicEventTypeMismatchError,
)
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)


def _create_mock_record(
    topic: str,
    event: TelemetryEvent,
    context: TelemetryExecutionContext,
    partition: int = 0,
    offset: int = 10,
    timestamp: int = 1774612345678,
    producer_version: str | None = "v1",
    raw_value: bytes | None = None,
    raw_headers: Sequence[tuple[str, bytes]] | None = None,
) -> ConsumerRecord:
    value = raw_value if raw_value is not None else serialize_event(event)
    headers = (
        raw_headers
        if raw_headers is not None
        else construct_kafka_headers(context, producer_version=producer_version)
    )
    key = construct_kafka_key(event)
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        timestamp_type=0,
        key=key,
        value=value,
        checksum=None,
        serialized_key_size=len(key),
        serialized_value_size=len(value),
        headers=headers,
    )


def _create_sample_event(
    event_type: EventType = EventType.METRIC,
    service: str = "order-service",
    event_id: uuid.UUID | None = None,
) -> TelemetryEvent:
    return TelemetryEvent(
        schema_version="1.0",
        event_id=event_id or uuid.uuid4(),
        event_time=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service=service,
        event_type=event_type,
        severity=EventSeverity.INFO,
        trace_id="test-trace-123",
        payload={"metric_name": "http_requests", "value": 42.0},
    )


def _create_sample_context() -> TelemetryExecutionContext:
    return TelemetryExecutionContext(
        run_id=uuid.uuid4(),
        scenario_id="scenario-test",
        scenario_version="1.0",
        reproducibility_key="rk-test",
        seed=12345,
    )


# --- 1. Envelope & Routing Unit Tests ---


def test_telemetry_envelope_immutability() -> None:
    event = _create_sample_event()
    context = _create_sample_context()
    envelope = TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=123456789,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=5,
        key=b"test-key",
        producer_version="step2.4",
    )
    assert envelope.kafka_timestamp_ms == 123456789
    assert envelope.topic == KafkaTopic.METRICS.value
    assert envelope.partition == 0
    assert envelope.offset == 5
    assert envelope.event == event
    assert envelope.context == context
    assert envelope.producer_version == "step2.4"

    with pytest.raises(Exception):
        envelope.offset = 6  # type: ignore[misc]


@pytest.mark.asyncio
async def test_metric_routing_valid() -> None:
    event = _create_sample_event(EventType.METRIC)
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.METRICS.value, event, context, offset=42, timestamp=1770000000000)

    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.stop = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    envelope = await consumer.process_record(record)

    assert envelope.event.event_id == event.event_id
    assert envelope.event.event_type == EventType.METRIC
    assert envelope.context.run_id == context.run_id
    assert envelope.kafka_timestamp_ms == 1770000000000
    assert envelope.topic == KafkaTopic.METRICS.value
    assert envelope.offset == 42
    handler.assert_awaited_once_with(envelope)
    mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 43})


@pytest.mark.asyncio
async def test_log_and_system_routing_valid_on_evidence_consumer() -> None:
    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.stop = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = EvidenceConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    # 1. LOG on logs topic
    log_event = _create_sample_event(EventType.LOG)
    log_context = _create_sample_context()
    log_record = _create_mock_record(KafkaTopic.LOGS.value, log_event, log_context, offset=10)
    log_envelope = await consumer.process_record(log_record)
    assert log_envelope.topic == KafkaTopic.LOGS.value
    assert log_envelope.event.event_type == EventType.LOG
    mock_aiokafka.commit.assert_awaited_with({TopicPartition(KafkaTopic.LOGS.value, 0): 11})

    # 2. SYSTEM on system events topic
    sys_event = _create_sample_event(EventType.SYSTEM)
    sys_context = _create_sample_context()
    sys_record = _create_mock_record(KafkaTopic.SYSTEM_EVENTS.value, sys_event, sys_context, offset=20)
    sys_envelope = await consumer.process_record(sys_record)
    assert sys_envelope.topic == KafkaTopic.SYSTEM_EVENTS.value
    assert sys_envelope.event.event_type == EventType.SYSTEM
    mock_aiokafka.commit.assert_awaited_with({TopicPartition(KafkaTopic.SYSTEM_EVENTS.value, 0): 21})

    assert handler.await_count == 2


@pytest.mark.asyncio
async def test_metric_on_logs_topic_rejected() -> None:
    event = _create_sample_event(EventType.METRIC)
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.LOGS.value, event, context)

    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    with pytest.raises(TopicEventTypeMismatchError, match="does not match expected topic"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_log_on_system_topic_rejected() -> None:
    event = _create_sample_event(EventType.LOG)
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.SYSTEM_EVENTS.value, event, context)

    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = EvidenceConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    with pytest.raises(TopicEventTypeMismatchError, match="does not match expected topic"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_system_on_logs_topic_rejected() -> None:
    event = _create_sample_event(EventType.SYSTEM)
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.LOGS.value, event, context)

    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = EvidenceConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    with pytest.raises(TopicEventTypeMismatchError, match="does not match expected topic"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_anomaly_routed_into_metrics_consumer_rejected() -> None:
    event = _create_sample_event(EventType.ANOMALY)
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.ANOMALIES.value, event, context)

    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    with pytest.raises(TopicEventTypeMismatchError, match="not allowed for MetricsConsumer"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_incident_routed_into_evidence_consumer_rejected() -> None:
    event = _create_sample_event(EventType.INCIDENT)
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.INCIDENTS.value, event, context)

    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = EvidenceConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    with pytest.raises(TopicEventTypeMismatchError, match="not allowed for EvidenceConsumer"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


# --- 2. Consumer Lifecycle & Execution Tests ---


def test_no_network_in_constructor() -> None:
    consumer = MetricsConsumer(handler=AsyncMock())
    assert consumer._consumer is None
    assert consumer._started is False
    assert consumer._closed is False
    assert consumer.topics == [KafkaTopic.METRICS.value]
    assert consumer.group_id == settings.KAFKA_METRICS_CONSUMER_GROUP

    evidence = EvidenceConsumer(handler=AsyncMock())
    assert evidence._consumer is None
    assert evidence._started is False
    assert evidence.topics == [KafkaTopic.LOGS.value, KafkaTopic.SYSTEM_EVENTS.value]
    assert evidence.group_id == settings.KAFKA_EVIDENCE_CONSUMER_GROUP


@pytest.mark.asyncio
async def test_process_before_start_raises_error() -> None:
    consumer = MetricsConsumer(handler=AsyncMock())
    event = _create_sample_event()
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.METRICS.value, event, context)

    with pytest.raises(ConsumerNotStartedError, match="Consumer is not started"):
        await consumer.process_record(record)

    with pytest.raises(ConsumerNotStartedError, match="Consumer is not started"):
        await consumer.consume_one()

    with pytest.raises(ConsumerNotStartedError, match="Consumer is not started"):
        await consumer.run()


@pytest.mark.asyncio
async def test_start_configures_aiokafka_correctly() -> None:
    created_kwargs: dict[str, Any] = {}

    def mock_factory(*args: Any, **kwargs: Any) -> aiokafka.AIOKafkaConsumer:
        created_kwargs.update(kwargs)
        mock_c = MagicMock(spec=aiokafka.AIOKafkaConsumer)
        mock_c.start = AsyncMock()
        mock_c.stop = AsyncMock()
        return mock_c

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        consumer_factory=mock_factory,
    )
    await consumer.start()

    assert consumer._started is True
    assert consumer._closed is False
    assert created_kwargs["enable_auto_commit"] is False
    assert created_kwargs["auto_offset_reset"] == "earliest"
    assert created_kwargs["group_id"] == settings.KAFKA_METRICS_CONSUMER_GROUP
    assert created_kwargs["bootstrap_servers"] == settings.KAFKA_BOOTSTRAP_SERVERS


@pytest.mark.asyncio
async def test_stop_lifecycle_and_idempotency() -> None:
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.stop = AsyncMock()

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()
    assert consumer._started is True

    await consumer.stop()
    assert consumer._started is False
    assert consumer._closed is True
    assert consumer._consumer is None
    mock_aiokafka.stop.assert_awaited_once()

    # Repeated stop should be safe
    await consumer.stop()
    mock_aiokafka.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_context_manager() -> None:
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.stop = AsyncMock()

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    async with consumer as c:
        assert c._started is True
        mock_aiokafka.start.assert_awaited_once()

    assert consumer._started is False
    mock_aiokafka.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_handler_object_implementing_protocol() -> None:
    class RecordingHandler:
        def __init__(self) -> None:
            self.handled: list[TelemetryEnvelope] = []

        async def handle(self, envelope: TelemetryEnvelope) -> None:
            self.handled.append(envelope)

    handler = RecordingHandler()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.METRICS.value, event, context, offset=15)

    await consumer.process_record(record)
    assert len(handler.handled) == 1
    assert handler.handled[0].event.event_id == event.event_id
    mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 16})


# --- 3. Failure & No-Commit Flow Tests ---


@pytest.mark.asyncio
async def test_malformed_json_value_fails_and_does_not_commit() -> None:
    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    record = _create_mock_record(
        KafkaTopic.METRICS.value,
        event,
        context,
        raw_value=b"invalid-json-payload-{{{",
    )

    with pytest.raises(ConsumerRecordValidationError, match="Failed to deserialize record"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_unsupported_schema_version_fails_and_does_not_commit() -> None:
    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    raw_value = b'{"schema_version": "9.9", "event_id": "00000000-0000-0000-0000-000000000000"}'
    record = _create_mock_record(
        KafkaTopic.METRICS.value,
        event,
        context,
        raw_value=raw_value,
    )

    with pytest.raises(ConsumerRecordValidationError, match="Failed to deserialize record"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_missing_header_fails_and_does_not_commit() -> None:
    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    # Missing 'run_id'
    raw_headers = [
        ("scenario_id", b"sc-1"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b"100"),
    ]
    record = _create_mock_record(
        KafkaTopic.METRICS.value,
        event,
        context,
        raw_headers=raw_headers,
    )

    with pytest.raises(MissingRequiredHeaderError, match="Missing required execution header: 'run_id'"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_header_fails_and_does_not_commit() -> None:
    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    raw_headers = construct_kafka_headers(context)
    raw_headers.append(("seed", b"999"))  # duplicate seed

    record = _create_mock_record(
        KafkaTopic.METRICS.value,
        event,
        context,
        raw_headers=raw_headers,
    )

    with pytest.raises(DuplicateHeaderError, match="Duplicate required execution header: 'seed'"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_seed_header_fails_and_does_not_commit() -> None:
    handler = AsyncMock()
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    raw_headers = [
        ("run_id", str(uuid.uuid4()).encode("utf-8")),
        ("scenario_id", b"sc-1"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b"-50"),
    ]
    record = _create_mock_record(
        KafkaTopic.METRICS.value,
        event,
        context,
        raw_headers=raw_headers,
    )

    with pytest.raises(InvalidHeaderError, match="Header 'seed' must be in range"):
        await consumer.process_record(record)

    handler.assert_not_called()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_downstream_handler_failure_raises_consumer_handler_error_and_does_not_commit() -> None:
    failing_handler = AsyncMock(side_effect=RuntimeError("Storage connection failed!"))
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=failing_handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = _create_sample_event()
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.METRICS.value, event, context, offset=77)

    with pytest.raises(ConsumerHandlerError, match="Downstream handler failed for event") as exc_info:
        await consumer.process_record(record)

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) == "Storage connection failed!"
    failing_handler.assert_awaited_once()
    mock_aiokafka.commit.assert_not_called()


# --- 4. consume_one & run Loop Tests ---


@pytest.mark.asyncio
async def test_consume_one_processes_single_record() -> None:
    event = _create_sample_event()
    context = _create_sample_context()
    record = _create_mock_record(KafkaTopic.METRICS.value, event, context, offset=100)

    tp = TopicPartition(KafkaTopic.METRICS.value, 0)
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()
    mock_aiokafka.getmany = AsyncMock(return_value={tp: [record]})

    handler = AsyncMock()
    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    envelope = await consumer.consume_one(timeout_ms=500)
    assert envelope is not None
    assert envelope.offset == 100
    handler.assert_awaited_once_with(envelope)
    mock_aiokafka.commit.assert_awaited_once_with({tp: 101})


@pytest.mark.asyncio
async def test_consume_one_returns_none_when_empty() -> None:
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.getmany = AsyncMock(return_value={})

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    envelope = await consumer.consume_one(timeout_ms=200)
    assert envelope is None


@pytest.mark.asyncio
async def test_run_loop_bounded_execution() -> None:
    event1 = _create_sample_event()
    event2 = _create_sample_event()
    context = _create_sample_context()
    rec1 = _create_mock_record(KafkaTopic.METRICS.value, event1, context, offset=1)
    rec2 = _create_mock_record(KafkaTopic.METRICS.value, event2, context, offset=2)

    tp = TopicPartition(KafkaTopic.METRICS.value, 0)
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()
    mock_aiokafka.getmany = AsyncMock(return_value={tp: [rec1, rec2]})

    handler = AsyncMock()
    consumer = MetricsConsumer(
        handler=handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    processed_count = await consumer.run(max_records=2)
    assert processed_count == 2
    assert handler.await_count == 2
    assert mock_aiokafka.commit.await_count == 2


@pytest.mark.asyncio
async def test_run_loop_cancellation_safety() -> None:
    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.getmany = AsyncMock(side_effect=asyncio.CancelledError())

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    with pytest.raises(asyncio.CancelledError):
        await consumer.run()
