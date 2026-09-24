from datetime import timedelta
from typing import Any

from app.simulator.interfaces import TelemetryEmitter
from app.simulator.telemetry.context import TelemetrySynthesisContext
from app.simulator.telemetry.deterministic import deterministic_telemetry_event_id
from app.simulator.workload.models import SyntheticRequest
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent


class SimulatorTelemetryAdapter:
    """Transforms synthetic workload requests and system markers into canonical TelemetryEvents."""

    def __init__(self, context: TelemetrySynthesisContext) -> None:
        self.context = context

    def events_for_request(self, request: SyntheticRequest) -> tuple[TelemetryEvent, ...]:
        """Synthesize canonical metric and log TelemetryEvent objects for one SyntheticRequest."""
        event_time = self.context.run_start_time + timedelta(seconds=request.simulation_offset_seconds)
        service = request.target_service

        # 1. Request Count Metric
        metric_count_id = deterministic_telemetry_event_id(
            f"request:{request.request_id}:metric:count:{service}"
        )
        metric_count_event = TelemetryEvent(
            event_id=metric_count_id,
            event_time=event_time,
            tenant_id=self.context.tenant_id,
            environment=self.context.environment,
            service=service,
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            trace_id=request.trace_id,
            payload={
                "metric_name": "http_requests_total",
                "value": 1,
                "status_code": request.status_code,
                "outcome": request.outcome,
                "request_id": request.request_id,
            },
        )

        # 2. Request Duration Metric
        metric_duration_id = deterministic_telemetry_event_id(
            f"request:{request.request_id}:metric:duration:{service}"
        )
        metric_duration_event = TelemetryEvent(
            event_id=metric_duration_id,
            event_time=event_time,
            tenant_id=self.context.tenant_id,
            environment=self.context.environment,
            service=service,
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            trace_id=request.trace_id,
            payload={
                "metric_name": "http_request_duration_ms",
                "value": request.accumulated_latency_ms,
                "status_code": request.status_code,
                "outcome": request.outcome,
                "request_id": request.request_id,
            },
        )

        # 3. Request Access Log
        log_access_id = deterministic_telemetry_event_id(
            f"request:{request.request_id}:log:access:{service}"
        )
        log_access_event = TelemetryEvent(
            event_id=log_access_id,
            event_time=event_time,
            tenant_id=self.context.tenant_id,
            environment=self.context.environment,
            service=service,
            event_type=EventType.LOG,
            severity=EventSeverity.INFO,
            trace_id=request.trace_id,
            payload={
                "message": f"Completed request {request.request_id} with status {request.status_code}",
                "request_id": request.request_id,
                "route": list(request.route),
                "status_code": request.status_code,
                "outcome": request.outcome,
                "latency_ms": request.accumulated_latency_ms,
                "error": request.error,
            },
        )

        return (metric_count_event, metric_duration_event, log_access_event)

    def events_for_requests(self, requests: list[SyntheticRequest]) -> list[TelemetryEvent]:
        """Synthesize canonical TelemetryEvents for a batch of SyntheticRequests in deterministic order."""
        batch: list[TelemetryEvent] = []
        for req in requests:
            batch.extend(self.events_for_request(req))
        return batch

    def create_system_marker(
        self,
        marker_name: str,
        service: str,
        simulation_offset_seconds: float,
        severity: EventSeverity = EventSeverity.INFO,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
        marker_id: str | None = None,
    ) -> TelemetryEvent:
        """Create a canonical SYSTEM lifecycle or state marker TelemetryEvent."""
        if not marker_name:
            raise ValueError("marker_name must not be empty")
        if not service:
            raise ValueError("service must not be empty")
        if simulation_offset_seconds < 0.0:
            raise ValueError(f"simulation_offset_seconds must be non-negative, got {simulation_offset_seconds}")

        event_time = self.context.run_start_time + timedelta(seconds=simulation_offset_seconds)
        identity_key = f"system:{marker_name}:{service}:{simulation_offset_seconds}:{marker_id or ''}"
        event_id = deterministic_telemetry_event_id(identity_key)

        event_payload: dict[str, Any] = {"marker": marker_name}
        if payload is not None:
            event_payload.update(payload)

        return TelemetryEvent(
            event_id=event_id,
            event_time=event_time,
            tenant_id=self.context.tenant_id,
            environment=self.context.environment,
            service=service,
            event_type=EventType.SYSTEM,
            severity=severity,
            trace_id=trace_id,
            payload=event_payload,
        )

    async def emit_request(self, emitter: TelemetryEmitter, request: SyntheticRequest) -> None:
        """Synthesize and emit all events for one request through the provided emitter."""
        for event in self.events_for_request(request):
            await emitter.emit(event)

    async def emit_requests(self, emitter: TelemetryEmitter, requests: list[SyntheticRequest]) -> None:
        """Synthesize and emit all events for a batch of requests through the provided emitter."""
        for event in self.events_for_requests(requests):
            await emitter.emit(event)
