from __future__ import annotations

from app.anomaly.errors import AnomalyPublicationError
from app.anomaly.models import AnomalySignal
from app.anomaly.serialization import to_canonical_telemetry_event
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult
from app.telemetry.transport.serialization import TelemetryExecutionContext


class AnomalySignalPublisher:
    """Application-level service for publishing validated AnomalySignals via Kafka transport."""

    def __init__(self, producer: KafkaTelemetryProducer) -> None:
        self.producer = producer

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

        try:
            return await self.producer.publish(event, context)
        except Exception as exc:
            raise AnomalyPublicationError(
                signal_id=signal.signal_id,
                run_id=signal.run_id,
                scenario_id=signal.scenario_id,
                cause=exc,
            ) from exc
