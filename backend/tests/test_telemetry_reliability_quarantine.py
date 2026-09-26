import asyncio
from pathlib import Path
import tempfile
import uuid

import pytest

from app.telemetry.reliability.errors import (
    QuarantineRecordValidationError,
    QuarantineWriteError,
)
from app.telemetry.reliability.quarantine import (
    FileQuarantineSink,
    QuarantineRecord,
    decode_bytes,
    decode_headers,
    encode_bytes,
    encode_headers,
    sanitize_failure_message,
)


def test_encode_decode_exact_bytes_round_trip() -> None:
    # 1. Arbitrary binary content with null bytes and invalid UTF-8 sequences
    raw_binary = b"\x00\xff\xfe\x80\x99\xaa\xbb\xcc\xdd\xee\x12\x34\x56"
    b64_str = encode_bytes(raw_binary)
    assert b64_str is not None
    decoded = decode_bytes(b64_str)
    assert decoded == raw_binary

    # 2. None encoding
    assert encode_bytes(None) is None
    assert decode_bytes(None) is None


def test_encode_decode_headers_preserves_order_and_duplicates() -> None:
    raw_headers = [
        ("run_id", b"run-123"),
        ("seed", b"42"),
        ("custom_header", b"\x01\x02"),
        ("duplicate_name", b"val1"),
        ("duplicate_name", b"val2"),
    ]

    header_models = encode_headers(raw_headers)
    assert len(header_models) == 5
    assert header_models[0].name == "run_id"
    assert header_models[3].name == "duplicate_name"
    assert header_models[4].name == "duplicate_name"

    decoded_headers = decode_headers(header_models)
    assert decoded_headers == raw_headers


def test_sanitize_failure_message() -> None:
    msg = "Error on line 1:\nFailed to parse token Bearer secret-token-123\r\n"
    sanitized = sanitize_failure_message(msg, max_length=50)
    assert "\n" not in sanitized
    assert "\r" not in sanitized
    assert sanitized.endswith(" [truncated]")


@pytest.mark.asyncio
async def test_file_quarantine_sink_creates_dir_and_appends_jsonl() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        q_path = Path(temp_dir) / "sub" / "quarantine.jsonl"
        sink = FileQuarantineSink(file_path=q_path)

        rec1 = QuarantineRecord(
            failure_stage="deserialization",
            failure_type="MalformedJSONError",
            failure_message_sanitized="Invalid JSON byte payload",
            topic="aegis.telemetry.metrics",
            partition=0,
            offset=10,
            kafka_timestamp_ms=1770000000000,
            raw_value_base64=encode_bytes(b"bad-json-1") or "",
        )

        rec2 = QuarantineRecord(
            failure_stage="persistence",
            failure_type="HTTP500Error",
            failure_message_sanitized="VictoriaMetrics 500 Internal Error",
            topic="aegis.telemetry.metrics",
            partition=0,
            offset=11,
            kafka_timestamp_ms=1770000001000,
            raw_value_base64=encode_bytes(b"valid-json-2") or "",
            event_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
        )

        await sink.quarantine(rec1)
        await sink.quarantine(rec2)

        # File exists and contains exactly two lines
        assert q_path.exists()
        with open(q_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        assert len(lines) == 2

        # Read back records
        read_records = await sink.read_records()
        assert len(read_records) == 2
        assert read_records[0].offset == 10
        assert decode_bytes(read_records[0].raw_value_base64) == b"bad-json-1"
        assert read_records[1].offset == 11
        assert decode_bytes(read_records[1].raw_value_base64) == b"valid-json-2"


@pytest.mark.asyncio
async def test_file_quarantine_sink_concurrent_writes_atomicity() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        q_path = Path(temp_dir) / "concurrent.jsonl"
        sink = FileQuarantineSink(file_path=q_path)

        records = [
            QuarantineRecord(
                failure_stage="routing_validation",
                failure_type="TopicMismatch",
                failure_message_sanitized=f"Mismatch {i}",
                topic="aegis.telemetry.logs",
                partition=0,
                offset=i,
                kafka_timestamp_ms=1770000000000 + i,
                raw_value_base64=encode_bytes(f"msg-{i}".encode("utf-8")) or "",
            )
            for i in range(20)
        ]

        await asyncio.gather(*(sink.quarantine(r) for r in records))

        read_records = await sink.read_records()
        assert len(read_records) == 20
        read_offsets = {r.offset for r in read_records}
        assert read_offsets == set(range(20))


@pytest.mark.asyncio
async def test_file_quarantine_sink_malformed_path_raises_quarantine_write_error() -> None:
    # Intentionally invalid path on Windows
    invalid_path = Path("Z:\\non_existent_drive_123\\quarantine.jsonl")
    sink = FileQuarantineSink(file_path=invalid_path)
    rec = QuarantineRecord(
        failure_stage="deserialization",
        failure_type="Error",
        failure_message_sanitized="Fail",
        topic="aegis.telemetry.metrics",
        partition=0,
        offset=1,
        kafka_timestamp_ms=1000,
        raw_value_base64="YQ==",
    )
    with pytest.raises(QuarantineWriteError):
        await sink.quarantine(rec)


@pytest.mark.asyncio
async def test_file_quarantine_sink_malformed_json_raises_validation_error() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        q_path = Path(temp_dir) / "corrupt.jsonl"
        with open(q_path, "w", encoding="utf-8") as f:
            f.write('{"quarantine_id": "invalid-json-content\n')

        sink = FileQuarantineSink(file_path=q_path)
        with pytest.raises(QuarantineRecordValidationError, match="Failed to parse quarantine record"):
            await sink.read_records()
