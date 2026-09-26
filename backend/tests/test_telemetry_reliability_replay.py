from unittest.mock import AsyncMock, MagicMock

import aiokafka
from aiokafka.structs import RecordMetadata
import pytest

from app.telemetry.reliability.errors import ReplayRecordError
from app.telemetry.reliability.quarantine import (
    QuarantineHeader,
    QuarantineRecord,
    encode_bytes,
)
from app.telemetry.reliability.replay import (
    REPLAY_MARKER_HEADER,
    QuarantineReplayService,
)
from app.telemetry.topics import KafkaTopic


@pytest.mark.asyncio
async def test_replay_record_republishes_exact_raw_bytes_and_adds_marker() -> None:
    raw_payload = b"\x00\x01\x02\x03\xff\xfe\xfd-exact-bytes"
    raw_key = b"tenant-1:sim:order-service"

    q_rec = QuarantineRecord(
        failure_stage="persistence",
        failure_type="HTTP500",
        failure_message_sanitized="Storage error",
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=100,
        kafka_timestamp_ms=1770000000000,
        raw_key_base64=encode_bytes(raw_key),
        raw_value_base64=encode_bytes(raw_payload) or "",
        headers=[
            QuarantineHeader(name="run_id", value_base64=encode_bytes(b"run-1") or ""),
            QuarantineHeader(name="seed", value_base64=encode_bytes(b"42") or ""),
        ],
    )

    mock_producer = MagicMock(spec=aiokafka.AIOKafkaProducer)
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(
        return_value=RecordMetadata(
            topic=KafkaTopic.METRICS.value,
            partition=0,
            topic_partition=None,
            offset=205,
            timestamp=1770000050000,
            timestamp_type=0,
            log_start_offset=0,
        )
    )

    replay_service = QuarantineReplayService(producer=mock_producer)
    result = await replay_service.replay_record(q_rec)

    assert result.success is True
    assert result.original_topic == KafkaTopic.METRICS.value
    assert result.original_offset == 100
    assert result.replay_offset == 205

    # Verify send_and_wait arguments
    mock_producer.send_and_wait.assert_awaited_once()
    _, kwargs = mock_producer.send_and_wait.call_args
    assert kwargs["topic"] == KafkaTopic.METRICS.value
    assert kwargs["value"] == raw_payload
    assert kwargs["key"] == raw_key

    # Verify replay marker header was added
    headers = kwargs["headers"]
    assert (REPLAY_MARKER_HEADER, b"1") in headers
    assert ("run_id", b"run-1") in headers
    assert ("seed", b"42") in headers


@pytest.mark.asyncio
async def test_replay_record_invalid_version_rejected() -> None:
    q_rec = QuarantineRecord(
        quarantine_version="9.9",  # unsupported
        failure_stage="deserialization",
        failure_type="Error",
        failure_message_sanitized="Fail",
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=1,
        kafka_timestamp_ms=1000,
        raw_value_base64="YQ==",
    )

    replay_service = QuarantineReplayService(producer=MagicMock(spec=aiokafka.AIOKafkaProducer))
    with pytest.raises(ReplayRecordError, match="Unsupported quarantine record version"):
        await replay_service.replay_record(q_rec)


@pytest.mark.asyncio
async def test_replay_record_unknown_topic_rejected() -> None:
    q_rec = QuarantineRecord(
        failure_stage="deserialization",
        failure_type="Error",
        failure_message_sanitized="Fail",
        topic="unknown.unregistered.topic",
        partition=0,
        offset=1,
        kafka_timestamp_ms=1000,
        raw_value_base64="YQ==",
    )

    replay_service = QuarantineReplayService(producer=MagicMock(spec=aiokafka.AIOKafkaProducer))
    with pytest.raises(ReplayRecordError, match="not a valid canonical application topic"):
        await replay_service.replay_record(q_rec)
