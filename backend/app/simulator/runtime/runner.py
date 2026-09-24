from datetime import datetime
import uuid

from app.simulator.ground_truth.builder import GroundTruthBuilder
from app.simulator.ground_truth.models import ResearchRunContext
from app.simulator.interfaces import FaultInjectionResult
from app.simulator.runtime.clock import SimulationClock
from app.simulator.runtime.engine import SimulationEngine
from app.simulator.runtime.result import (
    ScenarioLifecycleTransition,
    ScenarioRunResult,
)
from app.simulator.runtime.state import SimulationState
from app.simulator.scenarios import get_scenario
from app.simulator.scenarios.models import ScenarioDefinition
from app.simulator.telemetry.adapter import SimulatorTelemetryAdapter
from app.simulator.telemetry.context import TelemetrySynthesisContext
from app.simulator.telemetry.emitter import InMemoryTelemetryEmitter
from app.simulator.topology import build_canonical_topology
from app.simulator.workload.generator import DeterministicWorkloadGenerator
from app.simulator.workload.models import WorkloadProfile


class ScenarioRunner:
    """Orchestrates deterministic end-to-end execution of a simulator scenario."""

    async def run(
        self,
        scenario: ScenarioDefinition | str,
        *,
        run_id: uuid.UUID,
        run_start_time: datetime,
        seed: int,
        config_reference: str | None = None,
    ) -> ScenarioRunResult:
        """Execute a full scenario lifecycle and return detached result evidence."""
        if not isinstance(run_id, uuid.UUID):
            raise ValueError(f"run_id must be a UUID, got {run_id!r}")
        if run_start_time.tzinfo is None:
            raise ValueError("run_start_time must be timezone-aware")
        if seed < 0:
            raise ValueError(f"seed must be non-negative, got {seed}")

        if isinstance(scenario, str):
            scen = get_scenario(scenario)
        elif hasattr(scenario, "scenario_id"):
            scen = get_scenario(scenario.scenario_id)
        else:
            raise ValueError(f"Invalid scenario argument: {scenario!r}")

        # 1. Build ResearchRunContext
        run_ctx = ResearchRunContext.from_scenario(
            run_id=run_id,
            scenario=scen,
            run_start_time=run_start_time,
            seed=seed,
            config_reference=config_reference,
        )

        # 2. Instantiate run-local components
        from app.simulator.faults.injector import ConcreteFaultInjector

        topology = build_canonical_topology()
        clock = SimulationClock(start_time=run_start_time, tick_seconds=scen.workload.tick_seconds)
        state = SimulationState(topology)
        injector = ConcreteFaultInjector(state)
        workload_gen = DeterministicWorkloadGenerator(
            config=scen.workload, topology=topology, seed=seed
        )
        emitter = InMemoryTelemetryEmitter()
        synth_context = TelemetrySynthesisContext(
            tenant_id=uuid.uuid5(uuid.NAMESPACE_DNS, "aegisops:tenant:default"),
            environment="simulation",
            run_start_time=run_start_time,
        )
        adapter = SimulatorTelemetryAdapter(synth_context)
        engine = SimulationEngine(
            clock=clock,
            state=state,
            injector=injector,
            workload_gen=workload_gen,
            emitter=emitter,
            adapter=adapter,
        )
        gt_builder = GroundTruthBuilder(topology)

        transitions: list[ScenarioLifecycleTransition] = []
        inj_result: FaultInjectionResult | None = None
        total_requests: int = 0

        try:
            # Lifecycle: Initialized
            transitions.append(
                ScenarioLifecycleTransition(
                    phase="initialized",
                    simulation_offset_seconds=0.0,
                    details=f"Scenario {scen.scenario_id} initialized",
                )
            )

            start_marker = adapter.create_system_marker(
                marker_name="scenario_started",
                service="api-gateway",
                simulation_offset_seconds=0.0,
                payload={"scenario_id": scen.scenario_id},
            )
            await emitter.emit(start_marker)

            total_ticks = int(round(scen.total_duration_seconds / scen.workload.tick_seconds))

            for tick_idx in range(total_ticks):
                offset = clock.current_offset_seconds

                # --- Activation Phase ---
                if offset == scen.activation_time_seconds:
                    transitions.append(
                        ScenarioLifecycleTransition(
                            phase="activated",
                            simulation_offset_seconds=offset,
                            details="Activated scenario condition",
                        )
                    )

                    if scen.primary_fault is not None:
                        # Bad deployment system marker declaration if present
                        if scen.system_marker is not None:
                            marker_ev = adapter.create_system_marker(
                                marker_name=scen.system_marker.marker_name,
                                service=scen.system_marker.service,
                                simulation_offset_seconds=offset,
                                payload=scen.system_marker.payload,
                            )
                            await emitter.emit(marker_ev)

                        # Fault injection
                        inj_result = await injector.inject(scen.primary_fault.fault)
                        if not inj_result.accepted:
                            raise RuntimeError(f"Fault injection failed: {inj_result.message}")

                        fault_marker = adapter.create_system_marker(
                            marker_name="fault_injected",
                            service="api-gateway",
                            simulation_offset_seconds=offset,
                            payload={
                                "fault_id": str(scen.primary_fault.fault.fault_id),
                                "fault_type": str(scen.primary_fault.fault.fault_type),
                            },
                        )
                        await emitter.emit(fault_marker)

                    elif scen.workload.profile_name == WorkloadProfile.TRAFFIC_SURGE:
                        surge_marker = adapter.create_system_marker(
                            marker_name="traffic_surge_started",
                            service="api-gateway",
                            simulation_offset_seconds=offset,
                        )
                        await emitter.emit(surge_marker)

                # --- Recovery Phase ---
                if scen.recovery_time_seconds is not None and offset == scen.recovery_time_seconds:
                    transitions.append(
                        ScenarioLifecycleTransition(
                            phase="recovered",
                            simulation_offset_seconds=offset,
                            details="Recovered scenario condition",
                        )
                    )

                    if scen.primary_fault is not None:
                        rec_result = await injector.recover(scen.primary_fault.fault.fault_id)
                        if not rec_result.accepted:
                            raise RuntimeError(f"Fault recovery failed: {rec_result.message}")

                        rec_marker = adapter.create_system_marker(
                            marker_name="fault_recovered",
                            service="api-gateway",
                            simulation_offset_seconds=offset,
                            payload={"fault_id": str(scen.primary_fault.fault.fault_id)},
                        )
                        await emitter.emit(rec_marker)

                    elif scen.workload.profile_name == WorkloadProfile.TRAFFIC_SURGE:
                        surge_end_marker = adapter.create_system_marker(
                            marker_name="traffic_surge_ended",
                            service="api-gateway",
                            simulation_offset_seconds=offset,
                        )
                        await emitter.emit(surge_end_marker)

                # --- Execute Tick Workload ---
                requests = await engine.execute_tick_workload(tick_idx, offset)
                total_requests += len(requests)

                # --- Synthesize & Emit State Telemetry ---
                await engine.emit_state_telemetry(
                    tenant_id=synth_context.tenant_id,
                    environment=synth_context.environment,
                    tick_index=tick_idx,
                )

                # --- Advance Virtual Clock ---
                clock.advance()

            # Lifecycle: Completed
            end_offset = clock.current_offset_seconds
            end_marker = adapter.create_system_marker(
                marker_name="scenario_completed",
                service="api-gateway",
                simulation_offset_seconds=end_offset,
                payload={"scenario_id": scen.scenario_id},
            )
            await emitter.emit(end_marker)

            transitions.append(
                ScenarioLifecycleTransition(
                    phase="completed",
                    simulation_offset_seconds=end_offset,
                    details="Completed scenario execution",
                )
            )

            # Build ground truth
            truth = gt_builder.build_run_truth(
                scenario=scen,
                run_context=run_ctx,
                injection_result=inj_result,
            )

            # Snapshot detached telemetry
            telemetry_snapshot = emitter.snapshot()

            return ScenarioRunResult(
                run_id=run_id,
                scenario_id=scen.scenario_id,
                scenario_version=scen.version,
                seed=seed,
                reproducibility_key=run_ctx.reproducibility_key,
                scenario_truth=truth,
                telemetry_events=telemetry_snapshot,
                request_count=total_requests,
                transitions=tuple(transitions),
            )

        finally:
            # Guaranteed complete reset of all mutable components
            engine.reset()
