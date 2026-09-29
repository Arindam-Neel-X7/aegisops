from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from app.telemetry.pipeline.errors import TelemetryPipelinePublishError
from app.telemetry.pipeline.models import TelemetryRunPublishResult
from app.telemetry.schemas import EventType, TelemetryEvent
from app.telemetry.transport.producer import PublishResult
from app.telemetry.transport.serialization import TelemetryExecutionContext
from app.simulator.runtime.result import ScenarioRunResult

ACTIVE_EVENT_TYPES = frozenset({EventType.METRIC, EventType.LOG, EventType.SYSTEM})


class PipelinePublisher(Protocol):
    """Structural protocol for an object that can publish individual telemetry events."""

    async def publish(self, event: TelemetryEvent, context: TelemetryExecutionContext) -> PublishResult:
        ...


class TelemetryPipelinePublisher(BaseModel):
    """Detached bridge: ScenarioRunResult -> Kafka telemetry producer.

    - Accepts only an already-completed ScenarioRunResult.
    - Does not invoke or rerun the simulator.
    - Derives TelemetryExecutionContext from the ScenarioRunResult.
    - Publishes events in tuple order, preserving event IDs.
    - Rejects future event types not yet in Phase 2 scope.
    - Raises TelemetryPipelinePublishError on publication failure.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    producer: object

    def _derive_context(self, result: ScenarioRunResult) -> TelemetryExecutionContext:
        return TelemetryExecutionContext(
            run_id=result.run_id,
            scenario_id=result.scenario_id,
            scenario_version=result.scenario_version,
            reproducibility_key=result.reproducibility_key,
            seed=result.seed,
        )

    async def _publish_event(self, event: TelemetryEvent, context: TelemetryExecutionContext) -> PublishResult:
        if not hasattr(self.producer, "publish"):
            raise AttributeError("Injected publisher does not implement publish()")
        result: PublishResult = await self.producer.publish(event, context)
        return result

    async def publish_run(self, result: ScenarioRunResult) -> TelemetryRunPublishResult:
        started_at = datetime.now(timezone.utc)

        context = self._derive_context(result)

        total_events = len(result.telemetry_events)
        metric_count = sum(1 for e in result.telemetry_events if e.event_type == EventType.METRIC)
        log_count = sum(1 for e in result.telemetry_events if e.event_type == EventType.LOG)
        system_count = sum(1 for e in result.telemetry_events if e.event_type == EventType.SYSTEM)

        publish_results: list[PublishResult] = []
        published = 0

        for event in result.telemetry_events:
            if event.event_type not in ACTIVE_EVENT_TYPES:
                raise TelemetryPipelinePublishError(
                    f"Event type '{event.event_type}' is not accepted by Phase 2 pipeline; "
                    f"reserved for future phases",
                    run_id=result.run_id,
                    scenario_id=result.scenario_id,
                    published_count=published,
                    failing_event_id=event.event_id,
                )

            try:
                pr = await self._publish_event(event, context)
            except Exception as exc:
                # Translate any publication failure into typed pipeline error
                raise TelemetryPipelinePublishError(
                    f"Failed to publish event {event.event_id} for run {result.run_id}: {exc}",
                    run_id=result.run_id,
                    scenario_id=result.scenario_id,
                    published_count=published,
                    failing_event_id=event.event_id,
                    cause=exc,
                ) from exc

            publish_results.append(pr)
            published += 1

        completed_at = datetime.now(timezone.utc)
        duration_ms = max(0.0, (completed_at - started_at).total_seconds() * 1000.0)

        return TelemetryRunPublishResult(
            run_id=result.run_id,
            scenario_id=result.scenario_id,
            total_events=total_events,
            published_events=published,
            metric_events=metric_count,
            log_events=log_count,
            system_events=system_count,
            publish_results=publish_results,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms,
        )
