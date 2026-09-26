from collections.abc import Sequence
from datetime import datetime, timezone
import time
from typing import Any

import aiokafka
from pydantic import BaseModel, ConfigDict
import structlog

from app.core.config import settings
from app.telemetry.schemas import TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC
from app.telemetry.transport.errors import (
    ProducerError,
    ProducerNotStartedError,
)
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)

logger = structlog.get_logger(__name__)


class PublishResult(BaseModel):
    """Immutable result metadata returned following confirmed broker acknowledgement."""

    model_config = ConfigDict(frozen=True)

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    latency_ms: float
    serialized_bytes: int


class KafkaTelemetryProducer:
    """Async telemetry producer publishing canonical TelemetryEvents to Kafka.

    Adheres strictly to the frozen Phase 2 transport contract:
    - Native aiokafka idempotent producer mode (enable_idempotence=True, acks="all").
    - Explicit producer CreateTime UTC timestamping.
    - Partition key constructed from tenant_id:environment:service.
    - Execution context propagated via Kafka record headers.
    - 1:1 topic routing enforced via EVENT_TYPE_TO_TOPIC.
    """

    def __init__(
        self,
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id: str = settings.KAFKA_CLIENT_ID,
        request_timeout_ms: int = settings.KAFKA_REQUEST_TIMEOUT_MS,
        producer_version: str | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.request_timeout_ms = request_timeout_ms
        self.producer_version = producer_version
        self._producer: aiokafka.AIOKafkaProducer | None = None
        self._started: bool = False
        self._closed: bool = False

    async def start(self) -> None:
        """Start the underlying aiokafka producer."""
        if self._started and not self._closed and self._producer is not None:
            return

        self._producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            request_timeout_ms=self.request_timeout_ms,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()
        self._started = True
        self._closed = False
        logger.info(
            "KafkaTelemetryProducer started",
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            idempotence=True,
        )

    async def publish(
        self,
        event: TelemetryEvent,
        context: TelemetryExecutionContext,
    ) -> PublishResult:
        """Publish a single canonical TelemetryEvent with its execution context."""
        if not self._started or self._closed or self._producer is None:
            raise ProducerNotStartedError("KafkaTelemetryProducer is not started or has been closed")

        topic = EVENT_TYPE_TO_TOPIC[event.event_type].value
        key = construct_kafka_key(event)
        value = serialize_event(event)
        headers = construct_kafka_headers(context, self.producer_version)
        timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        start_time = time.perf_counter()
        try:
            record_metadata = await self._producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                headers=headers,
                timestamp_ms=timestamp_ms,
            )
        except Exception as exc:
            raise ProducerError(
                f"Failed to publish TelemetryEvent {event.event_id} to topic {topic}: {exc}"
            ) from exc

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        logger.debug(
            "Published telemetry event",
            event_id=str(event.event_id),
            run_id=str(context.run_id),
            event_type=str(event.event_type),
            service=event.service,
            topic=record_metadata.topic,
            partition=record_metadata.partition,
            offset=record_metadata.offset,
            latency_ms=round(latency_ms, 3),
        )

        return PublishResult(
            topic=record_metadata.topic,
            partition=record_metadata.partition,
            offset=record_metadata.offset,
            timestamp_ms=record_metadata.timestamp,
            latency_ms=latency_ms,
            serialized_bytes=len(value),
        )

    async def publish_batch(
        self,
        events: Sequence[TelemetryEvent],
        context: TelemetryExecutionContext,
    ) -> list[PublishResult]:
        """Publish a sequence of TelemetryEvents sequentially, preserving ordering."""
        results: list[PublishResult] = []
        for event in events:
            res = await self.publish(event, context)
            results.append(res)
        return results

    async def flush(self) -> None:
        """Flush any pending messages."""
        if self._started and not self._closed and self._producer is not None:
            await self._producer.flush()

    async def close(self) -> None:
        """Stop and release producer resources."""
        if self._started and not self._closed and self._producer is not None:
            self._closed = True
            self._started = False
            await self._producer.stop()
            self._producer = None
            logger.info("KafkaTelemetryProducer closed")

    async def __aenter__(self) -> "KafkaTelemetryProducer":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()
