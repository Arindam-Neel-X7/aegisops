from datetime import datetime
import uuid

from app.simulator.runtime.state import ServiceRuntimeState
from app.simulator.telemetry.deterministic import deterministic_telemetry_event_id
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent


def synthesize_service_state_events(
    service_state: ServiceRuntimeState,
    event_time: datetime,
    tenant_id: uuid.UUID,
    environment: str,
    tick_index: int,
) -> list[TelemetryEvent]:
    """Synthesize canonical METRIC TelemetryEvents reflecting current simulated service runtime state."""
    service = service_state.name
    metrics = [
        ("simulated_cpu_utilization_pct", service_state.cpu_utilization_pct),
        ("simulated_memory_utilization_pct", service_state.memory_utilization_pct),
        ("simulated_connection_pool_used", float(service_state.connection_pool_used)),
        ("simulated_effective_latency_ms", service_state.effective_latency_ms),
        ("simulated_error_rate", service_state.error_rate),
        ("simulated_timeout_rate", service_state.timeout_rate),
        ("simulated_network_reachable", 1.0 if service_state.network_reachable else 0.0),
        ("simulated_is_available", 1.0 if service_state.is_available else 0.0),
        ("simulated_is_crashed", 1.0 if service_state.is_crashed else 0.0),
    ]

    events: list[TelemetryEvent] = []
    for metric_name, val in metrics:
        key = f"state:{service}:{metric_name}:{tick_index}"
        event_id = deterministic_telemetry_event_id(key)
        events.append(
            TelemetryEvent(
                event_id=event_id,
                event_time=event_time,
                tenant_id=tenant_id,
                environment=environment,
                service=service,
                event_type=EventType.METRIC,
                severity=EventSeverity.INFO,
                payload={
                    "metric_name": metric_name,
                    "value": val,
                    "service_id": str(service_state.service_id),
                },
            )
        )
    return events
