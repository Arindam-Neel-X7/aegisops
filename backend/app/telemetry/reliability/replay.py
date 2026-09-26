from collections.abc import Callable
from typing import Any
import uuid

import aiokafka
from pydantic import BaseModel, ConfigDict
import structlog

from app.core.config import settings
from app.telemetry.reliability.errors import (
    ReplayError,
    ReplayRecordError,
)
from app.telemetry.reliability.quarantine import (
    FileQuarantineSink,
    QuarantineRecord,
    decode_bytes,
    decode_headers,
)
from app.telemetry.topics import KafkaTopic

logger = structlog.get_logger(__name__)

REPLAY_MARKER_HEADER = "aegis_replayed"


class ReplayResult(BaseModel):
    """Immutable result metadata following republishing of a quarantined record."""

    model_config = ConfigDict(frozen=True)

    quarantine_id: uuid.UUID
    original_topic: str
    original_partition: int
    original_offset: int
    replay_topic: str
    replay_partition: int
    replay_offset: int
    replay_timestamp_ms: int
    success: bool


class QuarantineReplayService:
    """Service republishing quarantined Kafka records to original canonical application topics."""

    def __init__(
        self,
        sink: FileQuarantineSink | None = None,
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id: str = "aegisops-quarantine-replay",
        producer: aiokafka.AIOKafkaProducer | None = None,
    ) -> None:
        self.sink = sink or FileQuarantineSink()
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self._external_producer = producer
        self._producer: aiokafka.AIOKafkaProducer | None = producer
        self._started = False

    async def start(self) -> None:
        """Start the underlying aiokafka producer."""
        if self._started and self._producer is not None:
            return
        if self._producer is None:
            self._producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
            )
        await self._producer.start()
        self._started = True

    async def close(self) -> None:
        """Stop underlying producer."""
        if self._started and self._producer is not None and self._producer is not self._external_producer:
            await self._producer.stop()
            self._producer = None
            self._started = False

    async def __aenter__(self) -> "QuarantineReplayService":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def replay_record(self, record: QuarantineRecord) -> ReplayResult:
        """Replay a single QuarantineRecord to its canonical topic with exact raw bytes."""
        if not self._started or self._producer is None:
            await self.start()
            assert self._producer is not None

        if record.quarantine_version != "1.0":
            raise ReplayRecordError(
                f"Unsupported quarantine record version '{record.quarantine_version}'"
            )

        valid_topics = {t.value for t in KafkaTopic}
        if record.topic not in valid_topics:
            raise ReplayRecordError(
                f"Target topic '{record.topic}' is not a valid canonical application topic"
            )

        raw_value = decode_bytes(record.raw_value_base64)
        if raw_value is None:
            raise ReplayRecordError(f"Missing raw value bytes in quarantine record {record.quarantine_id}")

        raw_key = decode_bytes(record.raw_key_base64)

        # Decode headers and append replay marker header
        headers = decode_headers(record.headers)
        headers.append((REPLAY_MARKER_HEADER, b"1"))

        try:
            metadata = await self._producer.send_and_wait(
                topic=record.topic,
                value=raw_value,
                key=raw_key,
                headers=headers,
            )
        except Exception as exc:
            logger.error(
                "Failed to replay quarantined record",
                quarantine_id=str(record.quarantine_id),
                topic=record.topic,
                error=str(exc),
            )
            raise ReplayError(
                f"Failed to replay record {record.quarantine_id} to topic '{record.topic}': {exc}"
            ) from exc

        logger.info(
            "Replayed quarantined record to Kafka",
            quarantine_id=str(record.quarantine_id),
            topic=record.topic,
            replay_partition=metadata.partition,
            replay_offset=metadata.offset,
        )

        return ReplayResult(
            quarantine_id=record.quarantine_id,
            original_topic=record.topic,
            original_partition=record.partition,
            original_offset=record.offset,
            replay_topic=metadata.topic,
            replay_partition=metadata.partition,
            replay_offset=metadata.offset,
            replay_timestamp_ms=metadata.timestamp,
            success=True,
        )

    async def replay_from_file(
        self,
        predicate: Callable[[QuarantineRecord], bool] | None = None,
        limit: int | None = None,
    ) -> list[ReplayResult]:
        """Read and replay matching records from the configured quarantine storage file."""
        records = await self.sink.read_records()
        results: list[ReplayResult] = []

        for rec in records:
            if predicate is not None and not predicate(rec):
                continue
            res = await self.replay_record(rec)
            results.append(res)
            if limit is not None and len(results) >= limit:
                break

        return results
