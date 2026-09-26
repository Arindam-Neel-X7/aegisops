from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import uuid

import aiokafka
from aiokafka import TopicPartition
from aiokafka.structs import ConsumerRecord
import pytest

from app.telemetry.reliability.errors import QuarantineWriteError
from app.telemetry.reliability.quarantine import (
    QuarantineRecord,
    decode_bytes,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    MetricsConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)


def _create_raw_record(
    topic: str,
    raw_value: bytes,
    raw_headers: list[tuple[str, bytes]] | None = None,
    partition: int = 0,
    offset: int = 10,
    raw_key: bytes | None = b"key-1",
    timestamp: int = 1770000000000,
) -> ConsumerRecord:
    headers = raw_headers if raw_headers is not None else []
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        timestamp_type=0,
        key=raw_key,
        value=raw_value,
        checksum=None,
        serialized_key_size=len(raw_key) if raw_key else 0,
        serialized_value_size=len(raw_value),
        headers=headers,
    )


@pytest.mark.asyncio
async def test_validation_failure_malformed_json_quarantines_and_commits() -> None:
    quarantined: list[QuarantineRecord] = []

    mock_sink = MagicMock()
    mock_sink.quarantine = AsyncMock(side_effect=lambda r: quarantined.append(r))

    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.stop = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    handler = AsyncMock()
    consumer = MetricsConsumer(
        handler=handler,
        quarantine_sink=mock_sink,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    raw_poison = b"non-json-corrupt-payload-{{{"
    record = _create_raw_record(KafkaTopic.METRICS.value, raw_poison, offset=42)

    result = await consumer.process_record(record)

    # Returns QuarantineRecord
    assert isinstance(result, QuarantineRecord)
    assert result.failure_stage == "deserialization"
    assert decode_bytes(result.raw_value_base64) == raw_poison

    # Handler not invoked
    handler.assert_not_called()

    # Quarantine sink called
    mock_sink.quarantine.assert_awaited_once_with(result)

    # Offset committed (+1)
    mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 43})


@pytest.mark.asyncio
async def test_validation_failure_missing_header_quarantines_and_commits() -> None:
    quarantined: list[QuarantineRecord] = []
    mock_sink = MagicMock()
    mock_sink.quarantine = AsyncMock(side_effect=lambda r: quarantined.append(r))

    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    handler = AsyncMock()
    consumer = MetricsConsumer(
        handler=handler,
        quarantine_sink=mock_sink,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    # Valid event JSON, but missing required run_id header
    event = TelemetryEvent(
        schema_version="1.0",
        event_id=uuid.uuid4(),
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": "cpu", "value": 50.0},
    )
    raw_val = serialize_event(event)
    headers = [("scenario_id", b"sc-1"), ("seed", b"42")]  # missing run_id, scenario_version, reproducibility_key

    record = _create_raw_record(KafkaTopic.METRICS.value, raw_val, raw_headers=headers, offset=77)

    result = await consumer.process_record(record)
    assert isinstance(result, QuarantineRecord)
    assert result.failure_stage == "header_validation"
    assert "Missing required execution header" in result.failure_message_sanitized

    mock_sink.quarantine.assert_awaited_once()
    mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 78})


@pytest.mark.asyncio
async def test_persistence_failure_quarantines_and_commits() -> None:
    quarantined: list[QuarantineRecord] = []
    mock_sink = MagicMock()
    mock_sink.quarantine = AsyncMock(side_effect=lambda r: quarantined.append(r))

    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    # Handler simulates storage 500 error
    failing_handler = AsyncMock(side_effect=RuntimeError("VictoriaMetrics connection refused"))

    consumer = MetricsConsumer(
        handler=failing_handler,
        quarantine_sink=mock_sink,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=uuid.uuid4(),
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": "cpu", "value": 50.0},
    )
    context = TelemetryExecutionContext(
        run_id=uuid.uuid4(),
        scenario_id="sc-1",
        scenario_version="1.0",
        reproducibility_key="rk-1",
        seed=100,
    )

    raw_val = serialize_event(event)
    headers = construct_kafka_headers(context)
    key = construct_kafka_key(event)

    record = _create_raw_record(
        KafkaTopic.METRICS.value,
        raw_val,
        raw_headers=headers,
        raw_key=key,
        offset=99,
    )

    result = await consumer.process_record(record)
    assert isinstance(result, QuarantineRecord)
    assert result.failure_stage == "persistence"
    assert result.event_id == event.event_id
    assert result.run_id == context.run_id

    mock_sink.quarantine.assert_awaited_once()
    mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 100})


@pytest.mark.asyncio
async def test_quarantine_sink_write_failure_does_not_commit() -> None:
    # Quarantine sink raises QuarantineWriteError
    mock_sink = MagicMock()
    mock_sink.quarantine = AsyncMock(side_effect=QuarantineWriteError("Disk full"))

    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        quarantine_sink=mock_sink,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    raw_poison = b"bad-bytes"
    record = _create_raw_record(KafkaTopic.METRICS.value, raw_poison, offset=50)

    with pytest.raises(QuarantineWriteError, match="Disk full"):
        await consumer.process_record(record)

    # Offset MUST NOT commit when quarantine write fails!
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_success_path_does_not_call_quarantine() -> None:
    mock_sink = MagicMock()
    mock_sink.quarantine = AsyncMock()

    mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.commit = AsyncMock()

    handler = AsyncMock()
    consumer = MetricsConsumer(
        handler=handler,
        quarantine_sink=mock_sink,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )
    await consumer.start()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=uuid.uuid4(),
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": "cpu", "value": 50.0},
    )
    context = TelemetryExecutionContext(
        run_id=uuid.uuid4(),
        scenario_id="sc-1",
        scenario_version="1.0",
        reproducibility_key="rk-1",
        seed=100,
    )
    raw_val = serialize_event(event)
    headers = construct_kafka_headers(context)
    key = construct_kafka_key(event)
    record = _create_raw_record(KafkaTopic.METRICS.value, raw_val, raw_headers=headers, raw_key=key, offset=12)

    result = await consumer.process_record(record)
    assert isinstance(result, TelemetryEnvelope)
    assert result.event.event_id == event.event_id

    mock_sink.quarantine.assert_not_called()
    mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 13})
