import asyncio
from datetime import datetime, timezone
import inspect
from typing import Any, Awaitable, Callable, Protocol, Sequence, runtime_checkable
import uuid

import aiokafka
from aiokafka import TopicPartition
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
import structlog

from app.core.config import settings
from app.telemetry.reliability.metrics import pipeline_metrics
from app.telemetry.reliability.quarantine import (
    QuarantineRecord,
    QuarantineSink,
    encode_bytes,
    encode_headers,
    sanitize_failure_message,
)
from app.telemetry.schemas import EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic
from app.telemetry.transport.errors import (
    ConsumerHandlerError,
    ConsumerNotStartedError,
    ConsumerRecordValidationError,
    DuplicateHeaderError,
    InvalidHeaderError,
    MissingRequiredHeaderError,
    TelemetryDeserializationError,
    TopicEventTypeMismatchError,
)
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
    deserialize_event,
)

logger = structlog.get_logger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_kafka_headers(
    headers: Sequence[tuple[str, bytes]] | None,
) -> tuple[TelemetryExecutionContext, str | None]:
    """Parse and validate required execution headers from a Kafka record.

    Required headers (exactly once):
    - run_id: valid UUID string
    - scenario_id: non-empty UTF-8 string
    - scenario_version: non-empty UTF-8 string
    - reproducibility_key: non-empty UTF-8 string
    - seed: decimal integer string in range [0, 2^64 - 1]

    Optional header:
    - producer_version: non-empty UTF-8 string if present
    """
    if headers is None:
        raise MissingRequiredHeaderError("Record headers are missing")

    header_counts: dict[str, int] = {}
    header_values: dict[str, bytes] = {}

    for k, v in headers:
        header_counts[k] = header_counts.get(k, 0) + 1
        header_values[k] = v

    required_keys = ("run_id", "scenario_id", "scenario_version", "reproducibility_key", "seed")
    for req in required_keys:
        count = header_counts.get(req, 0)
        if count == 0:
            raise MissingRequiredHeaderError(f"Missing required execution header: '{req}'")
        if count > 1:
            raise DuplicateHeaderError(f"Duplicate required execution header: '{req}'")

    if header_counts.get("producer_version", 0) > 1:
        raise DuplicateHeaderError("Duplicate execution header: 'producer_version'")

    decoded: dict[str, str] = {}
    for k, v in header_values.items():
        try:
            decoded[k] = v.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidHeaderError(f"Header '{k}' contains invalid UTF-8 bytes: {exc}") from exc

    for req in ("scenario_id", "scenario_version", "reproducibility_key"):
        val = decoded[req]
        if not val:
            raise InvalidHeaderError(f"Header '{req}' cannot be empty")

    run_id_str = decoded["run_id"]
    if not run_id_str:
        raise InvalidHeaderError("Header 'run_id' cannot be empty")
    try:
        run_id = uuid.UUID(run_id_str)
    except (ValueError, TypeError) as exc:
        raise InvalidHeaderError(f"Header 'run_id' is not a valid UUID: {exc}") from exc

    seed_str = decoded["seed"]
    if not seed_str:
        raise InvalidHeaderError("Header 'seed' cannot be empty")
    try:
        seed_val = int(seed_str)
    except (ValueError, TypeError) as exc:
        raise InvalidHeaderError(f"Header 'seed' is not a valid integer: {exc}") from exc

    if seed_val < 0 or seed_val > MAX_UINT64:
        raise InvalidHeaderError(
            f"Header 'seed' must be in range [0, 2^64 - 1], got {seed_val}"
        )

    producer_version = decoded.get("producer_version")
    if producer_version is not None and not producer_version:
        producer_version = None

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=decoded["scenario_id"],
        scenario_version=decoded["scenario_version"],
        reproducibility_key=decoded["reproducibility_key"],
        seed=seed_val,
    )
    return context, producer_version


class TelemetryEnvelope(BaseModel):
    """Immutable envelope encapsulating a consumed TelemetryEvent with transport context and Kafka metadata."""

    model_config = ConfigDict(frozen=True)

    event: TelemetryEvent
    context: TelemetryExecutionContext
    kafka_timestamp_ms: int
    topic: str
    partition: int
    offset: int
    key: bytes | None = None
    consumed_at: AwareDatetime = Field(default_factory=_now_utc)
    producer_version: str | None = None


@runtime_checkable
class TelemetryEnvelopeHandler(Protocol):
    """Protocol for downstream envelope processing boundary."""

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        ...


class TelemetryConsumer:
    """Base Kafka consumer for processing canonical TelemetryEvents."""

    def __init__(
        self,
        topics: Sequence[str],
        allowed_event_types: set[EventType],
        allowed_topics: set[str],
        handler: TelemetryEnvelopeHandler | Callable[[TelemetryEnvelope], Awaitable[None]],
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = "aegis-telemetry-consumer-group",
        request_timeout_ms: int = settings.KAFKA_REQUEST_TIMEOUT_MS,
        auto_offset_reset: str = "earliest",
        client_id: str | None = None,
        quarantine_sink: QuarantineSink | None = None,
        consumer_factory: Callable[..., aiokafka.AIOKafkaConsumer] | None = None,
    ) -> None:
        self.topics = list(topics)
        self.allowed_event_types = allowed_event_types
        self.allowed_topics = allowed_topics
        self._handler = handler
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.request_timeout_ms = request_timeout_ms
        self.auto_offset_reset = auto_offset_reset
        self.client_id = client_id
        self._quarantine_sink = quarantine_sink
        self._consumer_factory = consumer_factory

        self._consumer: aiokafka.AIOKafkaConsumer | None = None
        self._started: bool = False
        self._closed: bool = False

    async def start(self) -> None:
        """Start the underlying aiokafka consumer."""
        if self._started and not self._closed and self._consumer is not None:
            return

        if self._consumer_factory is not None:
            self._consumer = self._consumer_factory(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                client_id=self.client_id,
                request_timeout_ms=self.request_timeout_ms,
                enable_auto_commit=False,
                auto_offset_reset=self.auto_offset_reset,
            )
        else:
            self._consumer = aiokafka.AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                client_id=self.client_id,
                request_timeout_ms=self.request_timeout_ms,
                enable_auto_commit=False,
                auto_offset_reset=self.auto_offset_reset,
            )

        await self._consumer.start()
        self._started = True
        self._closed = False
        logger.info(
            "TelemetryConsumer started",
            consumer_class=self.__class__.__name__,
            topics=self.topics,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
        )

    async def stop(self) -> None:
        """Stop and release underlying consumer resources."""
        if self._started and not self._closed and self._consumer is not None:
            self._closed = True
            self._started = False
            await self._consumer.stop()
            self._consumer = None
            logger.info(
                "TelemetryConsumer stopped",
                consumer_class=self.__class__.__name__,
                group_id=self.group_id,
            )

    async def process_record(self, record: Any) -> Any:
        """Validate, deserialize, route, handle, and commit a single Kafka record.

        Failure contract:
        - Validation or persistence failure with configured QuarantineSink:
          quarantines exact original Kafka record, then commits offset + 1, and returns QuarantineRecord.
        - Quarantine failure: raises QuarantineWriteError and does NOT commit offset.
        - Without configured QuarantineSink: raises typed exception and does NOT commit offset.
        """
        if not self._started or self._closed or self._consumer is None:
            raise ConsumerNotStartedError("Consumer is not started or has been closed")

        raw_val_b64 = encode_bytes(record.value) or ""
        raw_key_b64 = encode_bytes(record.key)
        headers_models = encode_headers(record.headers)
        kafka_timestamp_ms = record.timestamp if record.timestamp is not None else 0

        # Phase 1: Validation & Envelope Construction
        validation_error: Exception | None = None
        failure_stage = "deserialization"
        event: TelemetryEvent | None = None
        context: TelemetryExecutionContext | None = None
        producer_version: str | None = None

        try:
            try:
                event = deserialize_event(record.value)
            except TelemetryDeserializationError as exc:
                failure_stage = "deserialization"
                raise ConsumerRecordValidationError(
                    f"Failed to deserialize record on topic '{record.topic}' offset {record.offset}: {exc}"
                ) from exc

            failure_stage = "header_validation"
            context, producer_version = parse_kafka_headers(record.headers)

            failure_stage = "routing_validation"
            expected_topic = EVENT_TYPE_TO_TOPIC[event.event_type].value
            if record.topic != expected_topic:
                raise TopicEventTypeMismatchError(
                    f"Record topic '{record.topic}' does not match expected topic '{expected_topic}' "
                    f"for event_type '{event.event_type}'"
                )

            if event.event_type not in self.allowed_event_types:
                raise TopicEventTypeMismatchError(
                    f"Event type '{event.event_type}' is not allowed for {self.__class__.__name__} "
                    f"(allowed: {[e.value for e in self.allowed_event_types]})"
                )

            if record.topic not in self.allowed_topics:
                raise TopicEventTypeMismatchError(
                    f"Topic '{record.topic}' is not allowed for {self.__class__.__name__} "
                    f"(allowed: {self.allowed_topics})"
                )

        except Exception as exc:
            validation_error = exc

        if validation_error is not None:
            if self._quarantine_sink is not None:
                q_record = QuarantineRecord(
                    failure_stage=failure_stage,
                    failure_type=validation_error.__class__.__name__,
                    failure_message_sanitized=sanitize_failure_message(str(validation_error)),
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    kafka_timestamp_ms=kafka_timestamp_ms,
                    raw_key_base64=raw_key_b64,
                    raw_value_base64=raw_val_b64,
                    headers=headers_models,
                    consumer_group=self.group_id,
                    event_id=event.event_id if event else None,
                    run_id=context.run_id if context else None,
                    scenario_id=context.scenario_id if context else None,
                    service=event.service if event else None,
                    event_type=event.event_type.value if event else None,
                )
                await self._quarantine_sink.quarantine(q_record)
                pipeline_metrics.record_quarantine(failure_stage)

                tp = TopicPartition(record.topic, record.partition)
                await self._consumer.commit({tp: record.offset + 1})
                return q_record

            pipeline_metrics.record_failure(failure_stage)
            raise validation_error

        assert event is not None and context is not None

        # Phase 2: Envelope Construction & Handler Execution
        envelope = TelemetryEnvelope(
            event=event,
            context=context,
            kafka_timestamp_ms=kafka_timestamp_ms,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            consumed_at=datetime.now(timezone.utc),
            producer_version=producer_version,
        )

        handler_error: Exception | None = None
        try:
            if hasattr(self._handler, "handle") and callable(getattr(self._handler, "handle")) and not callable(self._handler):
                handle_res: Any = self._handler.handle(envelope)
                if inspect.isawaitable(handle_res):
                    await handle_res
            elif callable(self._handler):
                call_res: Any = self._handler(envelope)
                if inspect.isawaitable(call_res):
                    await call_res
            elif hasattr(self._handler, "handle") and callable(getattr(self._handler, "handle")):
                obj_res: Any = self._handler.handle(envelope)
                if inspect.isawaitable(obj_res):
                    await obj_res
            else:
                raise ConsumerHandlerError(
                    f"Handler {self._handler} is neither callable nor implements handle()"
                )
        except ConsumerHandlerError as exc:
            handler_error = exc
        except Exception as exc:
            handler_error = ConsumerHandlerError(
                f"Downstream handler failed for event {event.event_id} at offset {record.offset}: {exc}"
            )
            handler_error.__cause__ = exc

        if handler_error is not None:
            if self._quarantine_sink is not None:
                q_record = QuarantineRecord(
                    failure_stage="persistence",
                    failure_type=handler_error.__class__.__name__,
                    failure_message_sanitized=sanitize_failure_message(str(handler_error)),
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    kafka_timestamp_ms=kafka_timestamp_ms,
                    raw_key_base64=raw_key_b64,
                    raw_value_base64=raw_val_b64,
                    headers=headers_models,
                    consumer_group=self.group_id,
                    event_id=event.event_id,
                    run_id=context.run_id,
                    scenario_id=context.scenario_id,
                    service=event.service,
                    event_type=event.event_type.value,
                )
                await self._quarantine_sink.quarantine(q_record)
                pipeline_metrics.record_quarantine("persistence")

                tp = TopicPartition(record.topic, record.partition)
                await self._consumer.commit({tp: record.offset + 1})
                return q_record

            pipeline_metrics.record_failure("persistence")
            raise handler_error

        # Phase 3: Success Commit & Observability
        tp = TopicPartition(record.topic, record.partition)
        await self._consumer.commit({tp: record.offset + 1})

        logger.debug(
            "Processed and committed telemetry record",
            consumer_class=self.__class__.__name__,
            event_id=str(event.event_id),
            run_id=str(context.run_id),
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            committed_offset=record.offset + 1,
        )

        return envelope

    async def consume_one(self, timeout_ms: int = 1000) -> Any:
        """Consume and process a single record within the given timeout in milliseconds."""
        if not self._started or self._closed or self._consumer is None:
            raise ConsumerNotStartedError("Consumer is not started or has been closed")

        records_dict = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=1)
        for records in records_dict.values():
            if records:
                return await self.process_record(records[0])
        return None

    async def run(self, max_records: int | None = None) -> int:
        """Consume and process records in a loop until stopped, cancelled, or max_records reached."""
        if not self._started or self._closed or self._consumer is None:
            raise ConsumerNotStartedError("Consumer is not started or has been closed")

        processed = 0
        try:
            while self._started and not self._closed:
                if max_records is not None and processed >= max_records:
                    break
                records_dict = await self._consumer.getmany(timeout_ms=1000, max_records=10)
                for records in records_dict.values():
                    for record in records:
                        await self.process_record(record)
                        processed += 1
                        if max_records is not None and processed >= max_records:
                            break
                    if max_records is not None and processed >= max_records:
                        break
        except asyncio.CancelledError:
            logger.info(
                "Consumer run loop cancelled",
                consumer_class=self.__class__.__name__,
                processed=processed,
            )
            raise
        return processed

    async def __aenter__(self) -> "TelemetryConsumer":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.stop()


class MetricsConsumer(TelemetryConsumer):
    """Kafka consumer dedicated to processing metric events from aegis.telemetry.metrics."""

    def __init__(
        self,
        handler: TelemetryEnvelopeHandler | Callable[[TelemetryEnvelope], Awaitable[None]],
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = settings.KAFKA_METRICS_CONSUMER_GROUP,
        request_timeout_ms: int = settings.KAFKA_REQUEST_TIMEOUT_MS,
        auto_offset_reset: str = "earliest",
        client_id: str | None = None,
        quarantine_sink: QuarantineSink | None = None,
        consumer_factory: Callable[..., aiokafka.AIOKafkaConsumer] | None = None,
    ) -> None:
        super().__init__(
            topics=[KafkaTopic.METRICS.value],
            allowed_event_types={EventType.METRIC},
            allowed_topics={KafkaTopic.METRICS.value},
            handler=handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            request_timeout_ms=request_timeout_ms,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            quarantine_sink=quarantine_sink,
            consumer_factory=consumer_factory,
        )


class EvidenceConsumer(TelemetryConsumer):
    """Kafka consumer dedicated to processing log and system events from aegis.telemetry.logs and aegis.system.events."""

    def __init__(
        self,
        handler: TelemetryEnvelopeHandler | Callable[[TelemetryEnvelope], Awaitable[None]],
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = settings.KAFKA_EVIDENCE_CONSUMER_GROUP,
        request_timeout_ms: int = settings.KAFKA_REQUEST_TIMEOUT_MS,
        auto_offset_reset: str = "earliest",
        client_id: str | None = None,
        quarantine_sink: QuarantineSink | None = None,
        consumer_factory: Callable[..., aiokafka.AIOKafkaConsumer] | None = None,
    ) -> None:
        super().__init__(
            topics=[KafkaTopic.LOGS.value, KafkaTopic.SYSTEM_EVENTS.value],
            allowed_event_types={EventType.LOG, EventType.SYSTEM},
            allowed_topics={KafkaTopic.LOGS.value, KafkaTopic.SYSTEM_EVENTS.value},
            handler=handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            request_timeout_ms=request_timeout_ms,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            quarantine_sink=quarantine_sink,
            consumer_factory=consumer_factory,
        )
