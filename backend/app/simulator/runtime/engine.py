from typing import TYPE_CHECKING
import uuid

from app.simulator.runtime.clock import SimulationClock
from app.simulator.runtime.observability import synthesize_service_state_events
from app.simulator.runtime.state import SimulationState
from app.simulator.telemetry.adapter import SimulatorTelemetryAdapter
from app.simulator.telemetry.emitter import InMemoryTelemetryEmitter
from app.simulator.workload.generator import DeterministicWorkloadGenerator
from app.simulator.workload.models import SyntheticRequest

if TYPE_CHECKING:
    from app.simulator.faults.injector import ConcreteFaultInjector


class SimulationEngine:
    """Low-level execution engine coordinating virtual clock, workload, state, and telemetry per tick."""

    def __init__(
        self,
        clock: SimulationClock,
        state: SimulationState,
        injector: "ConcreteFaultInjector",
        workload_gen: DeterministicWorkloadGenerator,
        emitter: InMemoryTelemetryEmitter,
        adapter: SimulatorTelemetryAdapter,
    ) -> None:
        self.clock = clock
        self.state = state
        self.injector = injector
        self.workload_gen = workload_gen
        self.emitter = emitter
        self.adapter = adapter

    async def execute_tick_workload(
        self, tick_index: int, simulation_offset_seconds: float
    ) -> list[SyntheticRequest]:
        """Generate synthetic requests and emit their canonical telemetry for this tick."""
        requests = self.workload_gen.requests_for_tick(
            tick_index=tick_index,
            simulation_offset_seconds=simulation_offset_seconds,
        )
        await self.adapter.emit_requests(self.emitter, requests)
        return requests

    async def emit_state_telemetry(
        self, tenant_id: uuid.UUID, environment: str, tick_index: int
    ) -> None:
        """Synthesize and emit metric telemetry for all simulated services at current virtual time."""
        current_time = self.clock.current_time
        for service_state in self.state.all_states().values():
            events = synthesize_service_state_events(
                service_state=service_state,
                event_time=current_time,
                tenant_id=tenant_id,
                environment=environment,
                tick_index=tick_index,
            )
            for ev in events:
                await self.emitter.emit(ev)

    def reset(self) -> None:
        """Reset all mutable components managed by this engine."""
        self.injector.reset()
        self.state.reset_to_baseline()
        self.workload_gen.reset()
        self.emitter.clear()
        self.clock.reset()
