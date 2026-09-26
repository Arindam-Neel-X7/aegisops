from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest

from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.errors import (
    ProducerError,
    ProducerNotStartedError,
)
from app.telemetry.transport.producer import (
    KafkaTelemetryProducer,
    PublishResult,
)
from app.telemetry.transport.serialization import TelemetryExecutionContext


@pytest.fixture
def sample_event() -> TelemetryEvent:
    return TelemetryEvent(
        tenant_id=uuid.UUID("11111111-2222-3333-4444-555555555555"),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id="trace-123",
        payload={"metric_name": "http_requests_total", "value": 1},
    )


@pytest.fixture
def sample_context() -> TelemetryExecutionContext:
    return TelemetryExecutionContext(
        run_id=uuid.UUID("99999999-8888-7777-6666-555555555555"),
        scenario_id="cpu-saturation",
        scenario_version="1.0",
        reproducibility_key="rep-key-42",
        seed=42,
    )


def test_producer_constructor_no_network_connection() -> None:
    producer = KafkaTelemetryProducer(
        bootstrap_servers="localhost:9092",
        client_id="test-producer",
        request_timeout_ms=3000,
    )
    assert producer._producer is None
    assert producer._started is False
    assert producer._closed is False


@pytest.mark.asyncio
async def test_publish_before_start_raises_error(
    sample_event: TelemetryEvent,
    sample_context: TelemetryExecutionContext,
) -> None:
    producer = KafkaTelemetryProducer()
    with pytest.raises(ProducerNotStartedError):
        await producer.publish(sample_event, sample_context)


@pytest.mark.asyncio
async def test_publish_after_close_raises_error(
    sample_event: TelemetryEvent,
    sample_context: TelemetryExecutionContext,
) -> None:
    producer = KafkaTelemetryProducer()
    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.stop = AsyncMock()
        mock_aiokafka_cls.return_value = mock_instance

        await producer.start()
        await producer.close()

        with pytest.raises(ProducerNotStartedError):
            await producer.publish(sample_event, sample_context)


@pytest.mark.asyncio
async def test_producer_start_configures_idempotence() -> None:
    producer = KafkaTelemetryProducer(
        bootstrap_servers="kafka-broker:9092",
        client_id="custom-producer",
        request_timeout_ms=4000,
    )

    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_aiokafka_cls.return_value = mock_instance

        await producer.start()

        mock_aiokafka_cls.assert_called_once_with(
            bootstrap_servers="kafka-broker:9092",
            client_id="custom-producer",
            request_timeout_ms=4000,
            enable_idempotence=True,
            acks="all",
        )
        mock_instance.start.assert_awaited_once()
        assert producer._started is True


@pytest.mark.asyncio
async def test_publish_success(
    sample_event: TelemetryEvent,
    sample_context: TelemetryExecutionContext,
) -> None:
    producer = KafkaTelemetryProducer(producer_version="v1.0")

    mock_record_meta = MagicMock()
    mock_record_meta.topic = KafkaTopic.METRICS.value
    mock_record_meta.partition = 0
    mock_record_meta.offset = 42
    mock_record_meta.timestamp = 1790366000000

    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.send_and_wait = AsyncMock(return_value=mock_record_meta)
        mock_aiokafka_cls.return_value = mock_instance

        await producer.start()
        result = await producer.publish(sample_event, sample_context)

        assert isinstance(result, PublishResult)
        assert result.topic == KafkaTopic.METRICS.value
        assert result.partition == 0
        assert result.offset == 42
        assert result.timestamp_ms == 1790366000000
        assert result.latency_ms >= 0.0
        assert result.serialized_bytes > 0

        # Verify send_and_wait was called with exact arguments
        mock_instance.send_and_wait.assert_awaited_once()
        call_kwargs = mock_instance.send_and_wait.call_args.kwargs
        assert call_kwargs["topic"] == KafkaTopic.METRICS.value
        assert call_kwargs["key"] == b"11111111-2222-3333-4444-555555555555:simulation:order-service"
        assert isinstance(call_kwargs["value"], bytes)
        assert isinstance(call_kwargs["timestamp_ms"], int)
        assert isinstance(call_kwargs["headers"], list)


@pytest.mark.asyncio
async def test_publish_event_immutability(
    sample_event: TelemetryEvent,
    sample_context: TelemetryExecutionContext,
) -> None:
    producer = KafkaTelemetryProducer()

    mock_record_meta = MagicMock()
    mock_record_meta.topic = KafkaTopic.METRICS.value
    mock_record_meta.partition = 0
    mock_record_meta.offset = 1
    mock_record_meta.timestamp = 1790366000000

    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.send_and_wait = AsyncMock(return_value=mock_record_meta)
        mock_aiokafka_cls.return_value = mock_instance

        orig_event_id = sample_event.event_id
        orig_event_time = sample_event.event_time
        orig_payload = dict(sample_event.payload)

        await producer.start()
        await producer.publish(sample_event, sample_context)

        assert sample_event.event_id == orig_event_id
        assert sample_event.event_time == orig_event_time
        assert sample_event.payload == orig_payload


@pytest.mark.asyncio
async def test_publish_failure_wraps_in_producer_error(
    sample_event: TelemetryEvent,
    sample_context: TelemetryExecutionContext,
) -> None:
    producer = KafkaTelemetryProducer()

    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.send_and_wait = AsyncMock(side_effect=RuntimeError("Broker connection timeout"))
        mock_aiokafka_cls.return_value = mock_instance

        await producer.start()
        with pytest.raises(ProducerError) as exc_info:
            await producer.publish(sample_event, sample_context)

        assert "Failed to publish TelemetryEvent" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_publish_batch_sequential(
    sample_context: TelemetryExecutionContext,
) -> None:
    producer = KafkaTelemetryProducer()
    events = [
        TelemetryEvent(
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
        ),
        TelemetryEvent(
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="order-service",
            event_type=EventType.LOG,
            severity=EventSeverity.INFO,
        ),
    ]

    mock_record_meta = MagicMock()
    mock_record_meta.topic = "test-topic"
    mock_record_meta.partition = 0
    mock_record_meta.offset = 10
    mock_record_meta.timestamp = 1790366000000

    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.send_and_wait = AsyncMock(return_value=mock_record_meta)
        mock_aiokafka_cls.return_value = mock_instance

        await producer.start()
        results = await producer.publish_batch(events, sample_context)

        assert len(results) == 2
        assert mock_instance.send_and_wait.await_count == 2


@pytest.mark.asyncio
async def test_flush_and_close_delegate_correctly() -> None:
    producer = KafkaTelemetryProducer()

    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.flush = AsyncMock()
        mock_instance.stop = AsyncMock()
        mock_aiokafka_cls.return_value = mock_instance

        await producer.start()
        await producer.flush()
        mock_instance.flush.assert_awaited_once()

        await producer.close()
        mock_instance.stop.assert_awaited_once()
        assert producer._closed is True
        assert producer._producer is None


@pytest.mark.asyncio
async def test_async_context_manager() -> None:
    with patch("aiokafka.AIOKafkaProducer") as mock_aiokafka_cls:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.stop = AsyncMock()
        mock_aiokafka_cls.return_value = mock_instance

        async with KafkaTelemetryProducer() as producer:
            assert producer._started is True

        mock_instance.start.assert_awaited_once()
        mock_instance.stop.assert_awaited_once()
        assert producer._closed is True
