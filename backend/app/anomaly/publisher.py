from __future__ import annotations

from app.anomaly.errors import AnomalyPublicationError
from app.anomaly.models import AnomalySignal
from app.anomaly.lineage import (
    build_anomaly_reproducibility_lineage,
)
from app.anomaly.observability import (
    FailureCategory,
    ObservabilityCollector,
    ObservabilityContext,
    OperationalStage,
)
from app.anomaly.serialization import to_canonical_telemetry_event
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult
from app.telemetry.transport.serialization import TelemetryExecutionContext


class AnomalySignalPublisher:
    """Application-level service for publishing validated AnomalySignals via Kafka transport."""

    def __init__(
        self,
        producer: KafkaTelemetryProducer,
        observability: ObservabilityCollector | None = None,
    ) -> None:
        self.producer = producer
        self.observability = observability or ObservabilityCollector()

    async def publish(self, signal: AnomalySignal) -> PublishResult:
        """Publish a single AnomalySignal as a canonical TelemetryEvent envelope.

        Validates input type, constructs execution context, and publishes via producer.
        Translates producer failures into AnomalyPublicationError.
        """
        if not isinstance(signal, AnomalySignal):
            raise TypeError(f"Expected AnomalySignal, got {type(signal).__name__}")

        event = to_canonical_telemetry_event(signal)
        context = TelemetryExecutionContext(
            run_id=signal.run_id,
            scenario_id=signal.scenario_id,
            scenario_version=signal.scenario_version,
            reproducibility_key=signal.reproducibility_key,
            seed=signal.seed,
        )
        lineage = build_anomaly_reproducibility_lineage(signal)
        observability_context = ObservabilityContext(
            run_id=signal.run_id,
            scenario_id=signal.scenario_id,
            scenario_version=signal.scenario_version,
            seed=signal.seed,
            reproducibility_key=signal.reproducibility_key,
            model_name=signal.model_name,
            model_version=signal.model_version,
            signal_id=signal.signal_id,
            semantic_fingerprint=lineage.semantic_fingerprint,
        )

        try:
            return await self.observability.async_measured_call(
                OperationalStage.PUBLICATION,
                FailureCategory.PUBLICATION_FAILURE,
                observability_context,
                action=lambda: self.producer.publish(event, context),
            )
        except Exception as exc:
            raise AnomalyPublicationError(
                signal_id=signal.signal_id,
                run_id=signal.run_id,
                scenario_id=signal.scenario_id,
                cause=exc,
            ) from exc
