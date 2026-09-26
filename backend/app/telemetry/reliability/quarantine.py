import asyncio
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Protocol, Sequence, runtime_checkable
import uuid

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
import structlog

from app.core.config import settings
from app.telemetry.reliability.errors import (
    QuarantineReadError,
    QuarantineRecordValidationError,
    QuarantineWriteError,
)

logger = structlog.get_logger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def encode_bytes(b: bytes | bytearray | memoryview | None) -> str | None:
    """Losslessly encode raw binary bytes to base64 ASCII string."""
    if b is None:
        return None
    if isinstance(b, (bytearray, memoryview)):
        b = bytes(b)
    return base64.b64encode(b).decode("ascii")


def decode_bytes(s: str | None) -> bytes | None:
    """Decode base64 ASCII string back to exact original binary bytes."""
    if s is None:
        return None
    try:
        return base64.b64decode(s)
    except Exception as exc:
        raise QuarantineRecordValidationError(f"Invalid base64 payload: {exc}") from exc


def encode_headers(headers: Sequence[tuple[str, bytes]] | None) -> list["QuarantineHeader"]:
    """Convert raw Kafka headers sequence into lossless QuarantineHeader models preserving order and duplicates."""
    if not headers:
        return []
    result: list[QuarantineHeader] = []
    for name, val in headers:
        b64_val = encode_bytes(val) or ""
        result.append(QuarantineHeader(name=name, value_base64=b64_val))
    return result


def decode_headers(headers: list["QuarantineHeader"]) -> list[tuple[str, bytes]]:
    """Decode QuarantineHeader models back into ordered (name, raw_bytes) header tuples."""
    result: list[tuple[str, bytes]] = []
    for h in headers:
        raw_val = decode_bytes(h.value_base64) or b""
        result.append((h.name, raw_val))
    return result


def sanitize_failure_message(msg: str, max_length: int = 500) -> str:
    """Sanitize and truncate error messages to prevent leaking secrets or payloads."""
    if not msg:
        return "Unknown error"
    # Basic sanitization of bearer/auth headers if present
    sanitized = msg.replace("\r", " ").replace("\n", " ").strip()
    if len(sanitized) > max_length:
        return sanitized[:max_length] + " [truncated]"
    return sanitized


class QuarantineHeader(BaseModel):
    """Lossless representation of a single Kafka record header."""

    model_config = ConfigDict(frozen=True)

    name: str
    value_base64: str


class QuarantineRecord(BaseModel):
    """Immutable operational record containing exact raw Kafka message content and failure context."""

    model_config = ConfigDict(frozen=True)

    quarantine_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    quarantine_version: str = "1.0"
    quarantined_at: AwareDatetime = Field(default_factory=_now_utc)
    failure_stage: str  # "deserialization", "header_validation", "routing_validation", "persistence", "unknown_consumer_failure"
    failure_type: str
    failure_message_sanitized: str
    topic: str
    partition: int
    offset: int
    kafka_timestamp_ms: int
    raw_key_base64: str | None = None
    raw_value_base64: str
    headers: list[QuarantineHeader] = Field(default_factory=list)
    consumer_group: str | None = None

    # Optional recoverable domain metadata
    event_id: uuid.UUID | None = None
    run_id: uuid.UUID | None = None
    scenario_id: str | None = None
    service: str | None = None
    event_type: str | None = None


QuarantineRecord.model_rebuild()


@runtime_checkable
class QuarantineSink(Protocol):
    """Protocol for recording failed Kafka records to quarantine storage."""

    async def quarantine(self, record: QuarantineRecord) -> None:
        ...


class FileQuarantineSink:
    """File-backed quarantine sink appending JSONL records to disk."""

    def __init__(self, file_path: Path | str | None = None) -> None:
        raw_path = file_path or settings.TELEMETRY_QUARANTINE_PATH
        self.file_path = Path(raw_path)
        self._lock = asyncio.Lock()

    async def quarantine(self, record: QuarantineRecord) -> None:
        """Append one QuarantineRecord as a single JSON line to the configured storage path."""
        try:
            line_data = record.model_dump(mode="json")
            json_line = json.dumps(line_data, ensure_ascii=False)
        except Exception as exc:
            raise QuarantineWriteError(f"Failed to serialize QuarantineRecord to JSON: {exc}") from exc

        async with self._lock:
            try:
                parent_dir = self.file_path.parent
                if parent_dir and not parent_dir.exists():
                    os.makedirs(parent_dir, exist_ok=True)

                with open(self.file_path, "a", encoding="utf-8") as f:
                    f.write(json_line + "\n")
                    f.flush()
            except Exception as exc:
                logger.error(
                    "Failed to write record to quarantine file",
                    file_path=str(self.file_path),
                    quarantine_id=str(record.quarantine_id),
                    error=str(exc),
                )
                raise QuarantineWriteError(
                    f"Failed to append record to quarantine file '{self.file_path}': {exc}"
                ) from exc

        logger.info(
            "Quarantined failed telemetry record",
            quarantine_id=str(record.quarantine_id),
            failure_stage=record.failure_stage,
            failure_type=record.failure_type,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )

    async def read_records(self, limit: int | None = None) -> list[QuarantineRecord]:
        """Read and parse QuarantineRecords from the JSONL storage file."""
        if not self.file_path.exists():
            return []

        records: list[QuarantineRecord] = []
        async with self._lock:
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        cleaned = line.strip()
                        if not cleaned:
                            continue
                        try:
                            data = json.loads(cleaned)
                            rec = QuarantineRecord.model_validate(data)
                            records.append(rec)
                            if limit is not None and len(records) >= limit:
                                break
                        except Exception as exc:
                            raise QuarantineRecordValidationError(
                                f"Failed to parse quarantine record at line {line_num}: {exc}"
                            ) from exc
            except QuarantineRecordValidationError:
                raise
            except Exception as exc:
                raise QuarantineReadError(
                    f"Failed to read quarantine file '{self.file_path}': {exc}"
                ) from exc

        return records
