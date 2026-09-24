from datetime import datetime, timedelta
import uuid

from app.simulator.ground_truth.deterministic import (
    build_reproducibility_key,
    deterministic_ground_truth_record_id,
)
from app.simulator.ground_truth.models import (
    ResearchRunContext,
    ScenarioRunTruth,
)
from app.simulator.interfaces import (
    FaultInjectionResult,
    GroundTruthRecord,
    ServiceTopology,
)
from app.simulator.scenarios.models import ScenarioDefinition
from app.simulator.topology import (
    CANONICAL_SERVICE_NAMES,
    build_canonical_topology,
    canonical_service_id,
)


class GroundTruthBuilder:
    """Constructs canonical GroundTruthRecord and ScenarioRunTruth artifacts from scenario facts."""

    def __init__(self, topology: ServiceTopology | None = None) -> None:
        self.topology = topology if topology is not None else build_canonical_topology()
        self._validate_topology()

    def _validate_topology(self) -> None:
        known = {s.name for s in self.topology.services}
        missing = set(CANONICAL_SERVICE_NAMES) - known
        if missing:
            raise ValueError(f"Topology missing canonical services: {sorted(missing)}")

    def _validate_run_context_and_scenario(
        self, scenario: ScenarioDefinition, run_context: ResearchRunContext
    ) -> None:
        if run_context.scenario_id != scenario.scenario_id:
            raise ValueError(
                f"run_context scenario_id ({run_context.scenario_id!r}) does not match "
                f"scenario.scenario_id ({scenario.scenario_id!r})"
            )
        if run_context.scenario_version != scenario.version:
            raise ValueError(
                f"run_context scenario_version ({run_context.scenario_version!r}) does not match "
                f"scenario.version ({scenario.version!r})"
            )
        if run_context.workload_identity != run_context.workload_config.config_identity():
            raise ValueError(
                f"run_context workload_identity ({run_context.workload_identity!r}) does not match "
                f"workload_config.config_identity() ({run_context.workload_config.config_identity()!r})"
            )
        if run_context.workload_identity != scenario.workload.config_identity():
            raise ValueError(
                f"run_context workload_identity ({run_context.workload_identity!r}) does not match "
                f"scenario.workload.config_identity() ({scenario.workload.config_identity()!r})"
            )

        expected_key = build_reproducibility_key(scenario, run_context.seed)
        if run_context.reproducibility_key != expected_key:
            raise ValueError(
                f"run_context reproducibility_key ({run_context.reproducibility_key!r}) does not match "
                f"expected key ({expected_key!r}) for scenario {scenario.scenario_id!r} with seed {run_context.seed}"
            )

    def build_fault_ground_truth(
        self,
        scenario: ScenarioDefinition,
        run_context: ResearchRunContext,
        injection_result: FaultInjectionResult,
        injected_at: datetime | None = None,
    ) -> GroundTruthRecord:
        """Construct an immutable GroundTruthRecord for an accepted fault injection."""
        self._validate_run_context_and_scenario(scenario, run_context)

        if scenario.primary_fault is None:
            raise ValueError(
                f"Scenario {scenario.scenario_id!r} has no primary fault to build ground truth for"
            )

        if not injection_result.accepted:
            raise ValueError(
                f"Cannot build GroundTruthRecord for rejected fault injection: {injection_result.message}"
            )

        fault_spec = scenario.primary_fault.fault
        if injection_result.fault_id != fault_spec.fault_id:
            raise ValueError(
                f"Injection result fault_id ({injection_result.fault_id}) does not match "
                f"scenario fault_id ({fault_spec.fault_id})"
            )

        # Resolve target service ID
        target_service_id = fault_spec.target_service_id

        # Resolve affected service names to canonical UUIDs
        affected_ids: list[uuid.UUID] = []
        for name in scenario.expected_affected_services:
            svc_id = canonical_service_id(name)
            if svc_id not in affected_ids:
                affected_ids.append(svc_id)

        # Determine timezone-aware injection timestamp
        expected_injected_at = run_context.run_start_time + timedelta(
            seconds=scenario.activation_time_seconds
        )
        if injected_at is not None:
            if injected_at.tzinfo is None:
                raise ValueError("injected_at must be timezone-aware")
            if injected_at != expected_injected_at:
                raise ValueError(
                    f"Explicit injected_at ({injected_at}) does not match "
                    f"expected activation timestamp ({expected_injected_at})"
                )
            effective_injected_at = injected_at
        else:
            effective_injected_at = expected_injected_at

        # Deterministic record ID derived from reproducibility key and fault ID
        record_id = deterministic_ground_truth_record_id(
            run_context.reproducibility_key, fault_spec.fault_id
        )

        metadata = {
            "parameters": fault_spec.parameters,
            "duration_seconds": fault_spec.duration_seconds,
            "expected_symptoms": list(scenario.expected_symptoms),
            "expected_recovery_condition": scenario.recovery_condition,
        }

        return GroundTruthRecord(
            record_id=record_id,
            fault_id=fault_spec.fault_id,
            target_service_id=target_service_id,
            fault_type=fault_spec.fault_type,
            injected_at=effective_injected_at,
            expected_root_cause=scenario.expected_root_cause,
            expected_affected_service_ids=affected_ids,
            metadata=metadata,
        )

    def build_run_truth(
        self,
        scenario: ScenarioDefinition,
        run_context: ResearchRunContext,
        injection_result: FaultInjectionResult | None = None,
        injected_at: datetime | None = None,
    ) -> ScenarioRunTruth:
        """Construct the run-level ScenarioRunTruth envelope for any scenario execution."""
        self._validate_run_context_and_scenario(scenario, run_context)

        fault_records: tuple[GroundTruthRecord, ...] = ()

        if scenario.primary_fault is not None:
            if injection_result is None:
                raise ValueError(
                    f"Fault scenario {scenario.scenario_id!r} requires an accepted FaultInjectionResult"
                )
            record = self.build_fault_ground_truth(
                scenario=scenario,
                run_context=run_context,
                injection_result=injection_result,
                injected_at=injected_at,
            )
            fault_records = (record,)
        else:
            if injection_result is not None:
                raise ValueError(
                    f"Workload-only scenario {scenario.scenario_id!r} has no primary fault and cannot accept an injection_result"
                )

        return ScenarioRunTruth(
            run=run_context,
            scenario_id=scenario.scenario_id,
            scenario_version=scenario.version,
            expected_root_cause=scenario.expected_root_cause,
            expected_symptoms=scenario.expected_symptoms,
            expected_affected_services=scenario.expected_affected_services,
            activation_time_seconds=scenario.activation_time_seconds,
            recovery_time_seconds=scenario.recovery_time_seconds,
            recovery_condition=scenario.recovery_condition,
            system_marker=scenario.system_marker,
            fault_ground_truth_records=fault_records,
        )
