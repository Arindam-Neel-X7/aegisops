"""One-attempt incident publication through the existing telemetry producer.

The caller owns producer startup/shutdown, committed intents and recovery. An
unconfirmed send may still reach Kafka, including after timeout/cancellation;
neither a retryable outcome nor cancellation promises physical non-delivery.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
import math

from aiokafka.errors import (
    LeaderNotAvailableError,
    NotEnoughReplicasError,
    NotLeaderForPartitionError,
    UnknownTopicOrPartitionError,
)

from app.correlation.publication import (
    INCIDENT_PUBLICATION_TOPIC,
    IncidentPublicationError,
    IncidentPublicationRecord,
    incident_publication_to_telemetry_event,
)
from app.telemetry.schemas import TelemetryEvent
from app.telemetry.transport.errors import ProducerError, ProducerNotStartedError
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult
from app.telemetry.transport.serialization import TelemetryExecutionContext


class IncidentPublishStatus(StrEnum):
    ACKNOWLEDGED = "ACKNOWLEDGED"
    FAILED_CONTRACT = "FAILED_CONTRACT"
    FAILED_UNAVAILABLE = "FAILED_UNAVAILABLE"
    FAILED_RETRIABLE = "FAILED_RETRIABLE"
    FAILED_AMBIGUOUS = "FAILED_AMBIGUOUS"


class IncidentPublishError(IncidentPublicationError):
    """Safe public failure; raw transport diagnostics remain in __cause__."""


class IncidentPublishContractError(IncidentPublishError):
    pass


class IncidentPublishUnavailableError(IncidentPublishError):
    pass


class IncidentPublishRetriableError(IncidentPublishError):
    pass


class IncidentPublishAmbiguousError(IncidentPublishError):
    pass


@dataclass(frozen=True)
class IncidentPublishOutcome:
    status: IncidentPublishStatus
    result: PublishResult | None = None
    error: IncidentPublishError | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, IncidentPublishStatus):
            raise IncidentPublishContractError("Invalid publication outcome status.")
        if self.status is IncidentPublishStatus.ACKNOWLEDGED:
            valid = isinstance(self.result, PublishResult) and self.error is None
        else:
            error_type = _FAILURES[self.status][0]
            valid = self.result is None and isinstance(self.error, error_type)
        if not valid:
            raise IncidentPublishContractError("Inconsistent publication outcome.")


_FAILURES = {
    IncidentPublishStatus.FAILED_CONTRACT: (
        IncidentPublishContractError,
        "Incident publication contract validation failed.",
    ),
    IncidentPublishStatus.FAILED_UNAVAILABLE: (
        IncidentPublishUnavailableError,
        "Incident publication producer is unavailable.",
    ),
    IncidentPublishStatus.FAILED_RETRIABLE: (
        IncidentPublishRetriableError,
        "Incident publication received a retryable broker rejection.",
    ),
    IncidentPublishStatus.FAILED_AMBIGUOUS: (
        IncidentPublishAmbiguousError,
        "Incident publication acknowledgement is unconfirmed.",
    ),
}


def _failure(status: IncidentPublishStatus, cause: Exception) -> IncidentPublishOutcome:
    error_type, message = _FAILURES[status]
    error = error_type(message)
    error.__cause__ = cause
    return IncidentPublishOutcome(status, error=error)


def _validated_ack(result: PublishResult, expected_bytes: int) -> PublishResult:
    """Do not turn a malformed or misrouted receipt into confirmed delivery."""
    if not isinstance(result, PublishResult):
        raise ValueError("Invalid acknowledgement type.")
    fields = result.model_dump()
    if (
        set(fields) != set(PublishResult.model_fields)
        or set(vars(result)) != set(PublishResult.model_fields)
        or result.__pydantic_extra__
    ):
        raise ValueError("Invalid acknowledgement fields.")
    if (
        type(result.topic) is not str  # noqa: E721 -- Reject coercions in receipts.
        or result.topic != INCIDENT_PUBLICATION_TOPIC.value
    ):
        raise ValueError("Invalid acknowledgement topic.")
    for value, maximum in [(result.partition, 2**31 - 1), (result.offset, 2**63 - 1)]:
        if type(value) is not int or not 0 <= value <= maximum:  # noqa: E721
            raise ValueError("Invalid acknowledgement position.")
    if result.timestamp_ms is not None and (
        type(result.timestamp_ms) is not int  # noqa: E721
        or not -(2**63) <= result.timestamp_ms < 2**63
    ):
        raise ValueError("Invalid acknowledgement timestamp.")
    if (
        type(result.latency_ms) not in (int, float)
        or not math.isfinite(result.latency_ms)
        or result.latency_ms < 0
    ):
        raise ValueError("Invalid acknowledgement latency.")
    if (
        type(result.serialized_bytes) is not int  # noqa: E721
        or result.serialized_bytes != expected_bytes
    ):
        raise ValueError("Invalid acknowledgement byte count.")
    return PublishResult(**fields)


class IncidentPublisher:
    """Thin transport adapter, with no automatic retries or durable bookkeeping.

    The finite timeout bounds the asynchronous wait, not synchronous C1
    validation. Unsettled tasks are retained and their late exceptions consumed.
    The caller must keep its dispatch lease/recovery semantics conservative even
    when pending_attempt_count reaches zero: cancellation does not retract Kafka
    records or the native producer's already-enqueued work.
    """

    def __init__(
        self,
        producer: KafkaTelemetryProducer,
        *,
        publish_timeout_seconds: float = 5.0,
    ) -> None:
        if not isinstance(producer, KafkaTelemetryProducer):
            raise IncidentPublishContractError("A telemetry producer is required.")
        if (
            type(publish_timeout_seconds) not in (int, float)
            or not 0.1 <= publish_timeout_seconds <= 60.0
        ):
            raise IncidentPublishContractError(
                "Publication timeout must be finite and between 0.1 and 60 seconds."
            )
        self._producer = producer
        self._timeout = float(publish_timeout_seconds)
        self._pending: set[asyncio.Task[PublishResult]] = set()

    @property
    def pending_attempt_count(self) -> int:
        return sum(not task.done() for task in self._pending)

    def _settled(self, task: asyncio.Task[PublishResult]) -> None:
        self._pending.discard(task)
        if not task.cancelled():
            task.exception()

    async def _send(
        self,
        event: TelemetryEvent,
        context: TelemetryExecutionContext,
        producer_version: str | None,
    ) -> PublishResult:
        # No await between this comparison and the existing producer's header
        # construction. Never silently change a caller-owned producer's config.
        if self._producer.producer_version != producer_version:
            raise IncidentPublishContractError("Publication producer version mismatch.")
        return await self._producer.publish(event, context)

    async def publish(
        self, record: IncidentPublicationRecord
    ) -> IncidentPublishOutcome:
        try:
            event = incident_publication_to_telemetry_event(record)
            context = record.execution_context.model_copy()
            version = record.producer_version
            expected_bytes = len(record.kafka_value)
        except Exception as exc:
            return _failure(IncidentPublishStatus.FAILED_CONTRACT, exc)

        task = asyncio.create_task(self._send(event, context, version))
        self._pending.add(task)
        task.add_done_callback(self._settled)
        try:
            done, _ = await asyncio.wait({task}, timeout=self._timeout)
            if not done:
                task.cancel()
                return _failure(
                    IncidentPublishStatus.FAILED_AMBIGUOUS,
                    TimeoutError("Publication acknowledgement wait expired."),
                )
            result = task.result()
        except asyncio.CancelledError:
            task.cancel()
            raise
        except IncidentPublishContractError as exc:
            return _failure(IncidentPublishStatus.FAILED_CONTRACT, exc)
        except ProducerNotStartedError as exc:
            return _failure(IncidentPublishStatus.FAILED_UNAVAILABLE, exc)
        except Exception as exc:
            # Only explicit broker rejections use the retryable classification.
            # Network, timeout, post-append and unknown failures remain ambiguous.
            broker_error = exc.__cause__ if type(exc) is ProducerError else exc
            retryable = type(broker_error) in (
                LeaderNotAvailableError,
                NotEnoughReplicasError,
                NotLeaderForPartitionError,
                UnknownTopicOrPartitionError,
            )
            status = (
                IncidentPublishStatus.FAILED_RETRIABLE
                if retryable
                else IncidentPublishStatus.FAILED_AMBIGUOUS
            )
            return _failure(status, exc)
        try:
            return IncidentPublishOutcome(
                IncidentPublishStatus.ACKNOWLEDGED,
                result=_validated_ack(result, expected_bytes),
            )
        except Exception as exc:
            return _failure(IncidentPublishStatus.FAILED_AMBIGUOUS, exc)
