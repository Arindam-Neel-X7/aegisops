from __future__ import annotations

from collections import deque
from collections.abc import Sequence
import math
import threading
import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict
import structlog

from app.core.config import settings

if TYPE_CHECKING:
    import aiokafka
    from aiokafka import TopicPartition
    from app.telemetry.persistence.metrics import (
        MetricPersistenceResult,
        VictoriaMetricsPersistenceAdapter,
    )
    from app.telemetry.persistence.search import (
        EvidencePersistenceResult,
        OpenSearchPersistenceAdapter,
    )
    from app.telemetry.schemas import TelemetryEvent
    from app.telemetry.transport.consumer import TelemetryEnvelope
    from app.telemetry.transport.producer import (
        KafkaTelemetryProducer,
        PublishResult,
    )
    from app.telemetry.transport.serialization import TelemetryExecutionContext

logger = structlog.get_logger(__name__)


class TelemetryPipelineMetricsSnapshot(BaseModel):
    """Immutable snapshot of pipeline operational metrics."""

    model_config = ConfigDict(frozen=True)

    published_count: int = 0
    persisted_count: int = 0
    quarantined_count: int = 0
    failed_count: int = 0
    retry_count: int = 0
    throughput_per_second: float = 0.0
    consumer_lag: int = 0
    publish_latency_p50_ms: float = 0.0
    publish_latency_p99_ms: float = 0.0
    persistence_latency_p50_ms: float = 0.0
    persistence_latency_p99_ms: float = 0.0
    e2e_latency_p50_ms: float = 0.0
    e2e_latency_p99_ms: float = 0.0


def calculate_percentile(data: Sequence[float], percentile: float) -> float:
    """Calculate deterministic percentile (0.0 to 100.0) from a sequence of numeric observations."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]
    rank = (percentile / 100.0) * (n - 1)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    weight = rank - low
    return sorted_data[low] * (1.0 - weight) + sorted_data[high] * weight


class TelemetryPipelineMetrics:
    """In-memory thread-safe operational metrics collector for the AegisOps telemetry pipeline."""

    def __init__(
        self,
        sample_window: int = settings.TELEMETRY_OBSERVABILITY_SAMPLE_WINDOW,
    ) -> None:
        self.sample_window = sample_window
        self._lock = threading.Lock()

        self._published_count = 0
        self._persisted_count = 0
        self._quarantined_count = 0
        self._failed_count = 0
        self._retry_count = 0
        self._consumer_lag = 0

        self._publish_latencies: deque[float] = deque(maxlen=sample_window)
        self._persistence_latencies: deque[float] = deque(maxlen=sample_window)
        self._e2e_latencies: deque[float] = deque(maxlen=sample_window)

        self._start_time = time.perf_counter()

    def record_publish(self, latency_ms: float) -> None:
        """Record a successful Kafka publish operation and its latency."""
        with self._lock:
            self._published_count += 1
            if latency_ms >= 0:
                self._publish_latencies.append(latency_ms)

    def record_persistence(
        self,
        latency_ms: float,
        retries: int = 0,
        e2e_latency_ms: float | None = None,
    ) -> None:
        """Record a successful persistence operation, retries, and optional end-to-end latency."""
        with self._lock:
            self._persisted_count += 1
            if retries > 0:
                self._retry_count += retries
            if latency_ms >= 0:
                self._persistence_latencies.append(latency_ms)
            if e2e_latency_ms is not None and e2e_latency_ms >= 0:
                self._e2e_latencies.append(e2e_latency_ms)

    def record_quarantine(self, stage: str | None = None) -> None:
        """Record a quarantined failure event."""
        with self._lock:
            self._quarantined_count += 1
            self._failed_count += 1

    def record_failure(self, stage: str | None = None) -> None:
        """Record an unhandled consumer or transport failure."""
        with self._lock:
            self._failed_count += 1

    def record_consumer_lag(self, lag: int) -> None:
        """Record current observed consumer lag in records."""
        with self._lock:
            self._consumer_lag = max(0, lag)

    def snapshot(self) -> TelemetryPipelineMetricsSnapshot:
        """Generate an immutable snapshot of current metrics."""
        with self._lock:
            elapsed = max(0.001, time.perf_counter() - self._start_time)
            total_processed = self._persisted_count + self._quarantined_count
            throughput = total_processed / elapsed

            pub_p50 = calculate_percentile(self._publish_latencies, 50.0)
            pub_p99 = calculate_percentile(self._publish_latencies, 99.0)

            pers_p50 = calculate_percentile(self._persistence_latencies, 50.0)
            pers_p99 = calculate_percentile(self._persistence_latencies, 99.0)

            e2e_p50 = calculate_percentile(self._e2e_latencies, 50.0)
            e2e_p99 = calculate_percentile(self._e2e_latencies, 99.0)

            return TelemetryPipelineMetricsSnapshot(
                published_count=self._published_count,
                persisted_count=self._persisted_count,
                quarantined_count=self._quarantined_count,
                failed_count=self._failed_count,
                retry_count=self._retry_count,
                throughput_per_second=round(throughput, 2),
                consumer_lag=self._consumer_lag,
                publish_latency_p50_ms=round(pub_p50, 3),
                publish_latency_p99_ms=round(pub_p99, 3),
                persistence_latency_p50_ms=round(pers_p50, 3),
                persistence_latency_p99_ms=round(pers_p99, 3),
                e2e_latency_p50_ms=round(e2e_p50, 3),
                e2e_latency_p99_ms=round(e2e_p99, 3),
            )

    def reset(self) -> None:
        """Reset internal metric counters and reservoirs."""
        with self._lock:
            self._published_count = 0
            self._persisted_count = 0
            self._quarantined_count = 0
            self._failed_count = 0
            self._retry_count = 0
            self._consumer_lag = 0
            self._publish_latencies.clear()
            self._persistence_latencies.clear()
            self._e2e_latencies.clear()
            self._start_time = time.perf_counter()


pipeline_metrics = TelemetryPipelineMetrics()


class InstrumentedTelemetryPublisher:
    """Instrumentation wrapper around KafkaTelemetryProducer recording publication metrics."""

    def __init__(
        self,
        producer: KafkaTelemetryProducer,
        metrics: TelemetryPipelineMetrics = pipeline_metrics,
    ) -> None:
        self._producer = producer
        self._metrics = metrics

    async def start(self) -> None:
        """Start the underlying producer."""
        await self._producer.start()

    async def close(self) -> None:
        """Close the underlying producer."""
        await self._producer.close()

    async def __aenter__(self) -> InstrumentedTelemetryPublisher:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def publish(
        self,
        event: TelemetryEvent,
        context: TelemetryExecutionContext,
    ) -> PublishResult:
        """Publish event and automatically record publish latency in metrics."""
        result = await self._producer.publish(event, context)
        self._metrics.record_publish(result.latency_ms)
        return result

    async def publish_batch(
        self,
        events: Sequence[TelemetryEvent],
        context: TelemetryExecutionContext,
    ) -> list[PublishResult]:
        """Publish batch of events and automatically record publish latencies in metrics."""
        results = await self._producer.publish_batch(events, context)
        for res in results:
            self._metrics.record_publish(res.latency_ms)
        return results


class InstrumentedMetricHandler:
    """Instrumentation wrapper around VictoriaMetricsPersistenceAdapter recording persistence metrics."""

    def __init__(
        self,
        adapter: VictoriaMetricsPersistenceAdapter,
        metrics: TelemetryPipelineMetrics = pipeline_metrics,
    ) -> None:
        self._adapter = adapter
        self._metrics = metrics

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        """Satisfy the consumer handler boundary."""
        await self.persist(envelope)

    async def persist(self, envelope: TelemetryEnvelope) -> MetricPersistenceResult:
        """Persist metric sample and automatically record persistence and E2E latencies."""
        result = await self._adapter.persist(envelope)
        e2e_ms: float | None = None
        if envelope.kafka_timestamp_ms > 0:
            e2e_ms = max(0.0, (time.time() * 1000.0) - envelope.kafka_timestamp_ms)
        retries = max(0, result.attempts - 1)
        self._metrics.record_persistence(
            latency_ms=result.latency_ms,
            retries=retries,
            e2e_latency_ms=e2e_ms,
        )
        return result

    async def close(self) -> None:
        """Close underlying persistence adapter."""
        await self._adapter.close()

    async def __aenter__(self) -> InstrumentedMetricHandler:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()


class InstrumentedEvidenceHandler:
    """Instrumentation wrapper around OpenSearchPersistenceAdapter recording persistence metrics."""

    def __init__(
        self,
        adapter: OpenSearchPersistenceAdapter,
        metrics: TelemetryPipelineMetrics = pipeline_metrics,
    ) -> None:
        self._adapter = adapter
        self._metrics = metrics

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        """Satisfy the consumer handler boundary."""
        await self.persist(envelope)

    async def persist(self, envelope: TelemetryEnvelope) -> EvidencePersistenceResult:
        """Persist evidence record and automatically record persistence and E2E latencies."""
        result = await self._adapter.persist(envelope)
        e2e_ms: float | None = None
        if envelope.kafka_timestamp_ms > 0:
            e2e_ms = max(0.0, (time.time() * 1000.0) - envelope.kafka_timestamp_ms)
        retries = max(0, result.attempts - 1)
        self._metrics.record_persistence(
            latency_ms=result.latency_ms,
            retries=retries,
            e2e_latency_ms=e2e_ms,
        )
        return result

    async def close(self) -> None:
        """Close underlying persistence adapter."""
        await self._adapter.close()

    async def __aenter__(self) -> InstrumentedEvidenceHandler:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()


async def sample_consumer_lag(
    consumer: aiokafka.AIOKafkaConsumer,
    topic_partition: TopicPartition,
    metrics: TelemetryPipelineMetrics = pipeline_metrics,
) -> int:
    """Sample real consumer lag from Kafka broker end-offsets and update pipeline metrics."""
    end_offsets = await consumer.end_offsets([topic_partition])
    committed = await consumer.committed(topic_partition)
    current_offset = committed if committed is not None else 0
    end_offset = end_offsets.get(topic_partition, 0)
    lag = int(max(0, end_offset - current_offset))
    metrics.record_consumer_lag(lag)
    return lag
