from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any
from typing import Protocol, runtime_checkable
import uuid

from pydantic import BaseModel, ConfigDict

from app.anomaly.handoff import anomaly_signal_from_envelope
from app.anomaly.lineage import (
    AnomalyReproducibilityLineage,
    build_anomaly_reproducibility_lineage,
)
from app.anomaly.models import AnomalySignal
from app.correlation.errors import (
    AnomalyDedupeCapacityError,
    AnomalyDedupeStoreError,
    AnomalyEvidenceLimitError,
    AnomalyIdentityConflictError,
)
from app.correlation.models import MAX_EVIDENCE_ENTRIES, MAX_SIGNAL_REFS
from app.telemetry.reliability.quarantine import QuarantineSink
from app.telemetry.schemas import EventType
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import TelemetryConsumer, TelemetryEnvelope

DEFAULT_INTAKE_CONSUMER_GROUP = "aegis-correlation-anomaly-intake-v1"
DEFAULT_INTAKE_DEDUPE_MAX_SIZE = 10000


class AnomalyIntakeDisposition(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


class DedupeReservationOutcome(StrEnum):
    RESERVED_NEW = "reserved_new"
    ALREADY_COMPLETED = "already_completed"


class AnomalyCorrelationInput(BaseModel):
    """Immutable validated correlation input derived from one anomaly envelope."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    signal: AnomalySignal
    lineage: AnomalyReproducibilityLineage

    @property
    def semantic_fingerprint(self) -> str:
        return self.lineage.semantic_fingerprint


class AnomalyIntakeResult(BaseModel):
    """Immutable intake outcome; duplicate carries no correlation input."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    disposition: AnomalyIntakeDisposition
    correlation_input: AnomalyCorrelationInput | None = None


@runtime_checkable
class AnomalyDedupeStore(Protocol):
    """Atomic reservation lifecycle for (signal_id, semantic_fingerprint)."""

    async def reserve(
        self, signal_id: uuid.UUID, fingerprint: str
    ) -> DedupeReservationOutcome: ...
    async def complete(self, signal_id: uuid.UUID) -> None: ...
    async def release(self, signal_id: uuid.UUID) -> None: ...


@runtime_checkable
class AnomalyDownstreamHandler(Protocol):
    """Downstream boundary invoked exactly once per new semantic identity."""

    async def handle(self, correlation_input: AnomalyCorrelationInput) -> None: ...


class InMemoryAnomalyDedupeStore:
    """Process-local bounded in-memory dedupe store.

    State is process-local only and offers no cross-process or restart
    durability. No exactly-once Kafka guarantee is claimed.

    Synchronization uses a single ``asyncio.Condition`` guarding the
    bounded dictionary. Callers holding ``ALREADY_COMPLETED`` never wait;
    callers seeing ``IN_FLIGHT`` with the same fingerprint wait until the
    owner calls ``complete()`` (which marks ``COMPLETED`` and notifies) or
    ``release()`` (which removes the entry and notifies, allowing the
    waiter to atomically become ``RESERVED_NEW``). Different-fingerprint
    reuse raises ``AnomalyIdentityConflictError`` immediately.
    """

    def __init__(self, max_size: int) -> None:
        if isinstance(max_size, bool) or not isinstance(max_size, int):
            raise ValueError("max_size must be an integer")
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self.max_size = max_size
        self._cond = asyncio.Condition()
        self._store: dict[uuid.UUID, tuple[str, bool]] = {}

    async def reserve(
        self, signal_id: uuid.UUID, fingerprint: str
    ) -> DedupeReservationOutcome:
        async with self._cond:
            while True:
                try:
                    existing = self._store.get(signal_id)
                    if existing is not None:
                        stored_fp, is_completed = existing
                        if stored_fp != fingerprint:
                            raise AnomalyIdentityConflictError(
                                "Signal identity conflicts with accepted fingerprint"
                            )
                        if is_completed:
                            return DedupeReservationOutcome.ALREADY_COMPLETED
                        await self._cond.wait()
                        continue
                    if len(self._store) >= self.max_size:
                        raise AnomalyDedupeCapacityError(
                            "Dedupe store reached maximum entry count"
                        )
                    self._store[signal_id] = (fingerprint, False)
                    return DedupeReservationOutcome.RESERVED_NEW
                except (AnomalyIdentityConflictError, AnomalyDedupeCapacityError):
                    raise
                except Exception as exc:
                    raise AnomalyDedupeStoreError(
                        "Dedupe store reservation failed"
                    ) from exc

    async def complete(self, signal_id: uuid.UUID) -> None:
        async with self._cond:
            try:
                existing = self._store.get(signal_id)
                if existing is None:
                    raise AnomalyDedupeStoreError(
                        "Cannot complete unknown dedupe reservation"
                    )
                self._store[signal_id] = (existing[0], True)
                self._cond.notify_all()
            except AnomalyDedupeStoreError:
                raise
            except Exception as exc:
                raise AnomalyDedupeStoreError("Dedupe store completion failed") from exc

    async def release(self, signal_id: uuid.UUID) -> None:
        async with self._cond:
            try:
                existing = self._store.get(signal_id)
                if existing is not None and not existing[1]:
                    del self._store[signal_id]
                    self._cond.notify_all()
            except Exception as exc:
                raise AnomalyDedupeStoreError("Dedupe store release failed") from exc

    def completed_count(self) -> int:
        return sum(1 for _, done in self._store.values() if done)

    def entry_count(self) -> int:
        return len(self._store)


DownstreamCallable = Callable[[AnomalyCorrelationInput], Awaitable[None]]


class AnomalyIntakeProcessor:
    """Validates envelopes, enforces bounds, dedupes, and invokes downstream once."""

    def __init__(
        self,
        dedupe_store: AnomalyDedupeStore,
        downstream: AnomalyDownstreamHandler | DownstreamCallable,
    ) -> None:
        self.dedupe_store = dedupe_store
        self.downstream = downstream

    async def _invoke_downstream(
        self, correlation_input: AnomalyCorrelationInput
    ) -> None:
        target = self.downstream
        handle = getattr(target, "handle", None)
        if (
            callable(handle)
            and not isinstance(target, type)
            and hasattr(target, "handle")
        ):
            result = handle(correlation_input)
            if isinstance(result, Awaitable):
                await result
            return
        if callable(target):
            result = target(correlation_input)
            if isinstance(result, Awaitable):
                await result
            return
        raise TypeError("Downstream must be callable or expose an async handle()")

    async def process(self, envelope: TelemetryEnvelope) -> AnomalyIntakeResult:
        if not isinstance(envelope, TelemetryEnvelope):
            raise TypeError("Expected TelemetryEnvelope")
        signal = anomaly_signal_from_envelope(envelope)
        signal_copy = signal.model_copy(deep=True)
        lineage = build_anomaly_reproducibility_lineage(signal_copy)
        fingerprint = lineage.semantic_fingerprint

        if len(signal_copy.source_event_ids) > MAX_SIGNAL_REFS:
            raise AnomalyEvidenceLimitError("Source event count exceeds maximum")
        if len(set(signal_copy.source_event_ids)) != len(signal_copy.source_event_ids):
            raise AnomalyEvidenceLimitError("Source event identities must be unique")
        if len(signal_copy.evidence) > MAX_EVIDENCE_ENTRIES:
            raise AnomalyEvidenceLimitError("Evidence count exceeds maximum")
        evidence_ids = [item.evidence_id for item in signal_copy.evidence]
        if len(set(evidence_ids)) != len(evidence_ids):
            raise AnomalyEvidenceLimitError("Evidence identities must be unique")

        decision = await self.dedupe_store.reserve(signal_copy.signal_id, fingerprint)
        if decision == DedupeReservationOutcome.ALREADY_COMPLETED:
            return AnomalyIntakeResult(disposition=AnomalyIntakeDisposition.DUPLICATE)

        correlation_input = AnomalyCorrelationInput(signal=signal_copy, lineage=lineage)
        try:
            await self._invoke_downstream(correlation_input)
        except asyncio.CancelledError:
            await self.dedupe_store.release(signal_copy.signal_id)
            raise
        except Exception:
            await self.dedupe_store.release(signal_copy.signal_id)
            raise

        await self.dedupe_store.complete(signal_copy.signal_id)
        return AnomalyIntakeResult(
            disposition=AnomalyIntakeDisposition.ACCEPTED,
            correlation_input=correlation_input,
        )

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        await self.process(envelope)


class AnomalyIntakeConsumer(TelemetryConsumer):
    """Thin anomaly-only adapter over the frozen generic TelemetryConsumer."""

    def __init__(
        self,
        processor: AnomalyIntakeProcessor,
        bootstrap_servers: str,
        group_id: str = DEFAULT_INTAKE_CONSUMER_GROUP,
        request_timeout_ms: int = 5000,
        auto_offset_reset: str = "latest",
        client_id: str | None = None,
        quarantine_sink: QuarantineSink | None = None,
        consumer_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._processor = processor

        async def _handle(envelope: TelemetryEnvelope) -> None:
            await self._processor.process(envelope)

        super().__init__(
            topics=[KafkaTopic.ANOMALIES.value],
            allowed_event_types={EventType.ANOMALY},
            allowed_topics={KafkaTopic.ANOMALIES.value},
            handler=_handle,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            request_timeout_ms=request_timeout_ms,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id,
            quarantine_sink=quarantine_sink,
            consumer_factory=consumer_factory,
        )
