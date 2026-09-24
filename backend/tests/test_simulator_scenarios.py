import uuid
import pytest
from pydantic import ValidationError

from app.simulator.interfaces import FaultSpec, FaultType
from app.simulator.scenarios import (
    INITIAL_SCENARIOS,
    SCENARIO_VERSION,
    ScenarioDefinition,
    ScenarioSystemMarker,
    ScheduledFault,
    get_scenario,
    list_scenarios,
    scenario_fault_id,
    scenario_ids,
)
from app.simulator.topology import (
    CANONICAL_SERVICE_NAMES,
    CANONICAL_TOPOLOGY_VERSION,
    canonical_service_id,
)
from app.simulator.workload import WorkloadConfig, WorkloadProfile

EXPECTED_SCENARIO_IDS = (
    "cpu-saturation",
    "memory-exhaustion",
    "connection-exhaustion",
    "dependency-latency",
    "dependency-failure",
    "error-rate-spike",
    "traffic-surge",
    "bad-deployment-config",
)


def test_catalogue_inventory_and_order() -> None:
    scenarios = list_scenarios()
    assert len(scenarios) == 8
    assert len(INITIAL_SCENARIOS) == 8
    assert scenario_ids() == EXPECTED_SCENARIO_IDS
    assert [s.scenario_id for s in scenarios] == list(EXPECTED_SCENARIO_IDS)


def test_scenario_ids_are_unique() -> None:
    ids = scenario_ids()
    assert len(ids) == len(set(ids))


def test_all_scenarios_have_expected_version_and_seed() -> None:
    for s in list_scenarios():
        assert s.version == SCENARIO_VERSION == "1.0.0"
        assert s.default_seed == 42
        assert s.topology_reference == "canonical"
        assert s.topology_version == CANONICAL_TOPOLOGY_VERSION == "1.0.0"


def test_scenario_names_and_descriptions() -> None:
    for s in list_scenarios():
        assert len(s.name) > 0
        assert len(s.description) > 0
        assert len(s.expected_root_cause) > 0
        assert len(s.recovery_condition) > 0
        assert len(s.expected_symptoms) > 0
        assert len(s.expected_affected_services) > 0


def test_expected_affected_services_exist_in_canonical_topology() -> None:
    for s in list_scenarios():
        for svc in s.expected_affected_services:
            assert svc in CANONICAL_SERVICE_NAMES


def test_standard_scenario_timings() -> None:
    for s in list_scenarios():
        assert s.activation_time_seconds == 10.0
        assert s.observation_window_seconds == 10.0
        assert s.recovery_time_seconds == 20.0
        assert s.post_recovery_observation_window_seconds == 10.0
        assert s.total_duration_seconds == 30.0
        assert s.workload.total_duration_seconds == 30.0


def test_cpu_saturation_scenario() -> None:
    s = get_scenario("cpu-saturation")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.RESOURCE
    assert fault.target_service_id == canonical_service_id("order-service")
    assert fault.parameters == {"resource_kind": "cpu", "utilization_pct": 95.0}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("cpu-saturation")
    assert "order-service" in s.expected_affected_services
    assert "simulated CPU saturation" in s.expected_root_cause


def test_memory_exhaustion_scenario() -> None:
    s = get_scenario("memory-exhaustion")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.RESOURCE
    assert fault.target_service_id == canonical_service_id("notification-service")
    assert fault.parameters == {"resource_kind": "memory", "utilization_pct": 95.0}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("memory-exhaustion")
    assert s.expected_affected_services == ("notification-service",)
    assert "simulated memory exhaustion" in s.expected_root_cause


def test_connection_exhaustion_scenario() -> None:
    s = get_scenario("connection-exhaustion")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.RESOURCE
    assert fault.target_service_id == canonical_service_id("database")
    assert fault.parameters == {"resource_kind": "connection", "utilization_pct": 100.0}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("connection-exhaustion")
    assert "database" in s.expected_affected_services
    assert "simulated database connection-pool exhaustion" in s.expected_root_cause


def test_dependency_latency_scenario() -> None:
    s = get_scenario("dependency-latency")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.LATENCY
    assert fault.target_service_id == canonical_service_id("payment-service")
    assert fault.parameters == {"latency_add_ms": 150.0}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("dependency-latency")
    assert "payment-service" in s.expected_affected_services
    assert "simulated latency degradation" in s.expected_root_cause


def test_dependency_failure_scenario() -> None:
    s = get_scenario("dependency-failure")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.NETWORK
    assert fault.target_service_id == canonical_service_id("database")
    assert fault.parameters == {}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("dependency-failure")
    assert "database" in s.expected_affected_services
    assert "simulated database dependency network failure" in s.expected_root_cause


def test_error_rate_spike_scenario() -> None:
    s = get_scenario("error-rate-spike")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.ERROR
    assert fault.target_service_id == canonical_service_id("order-service")
    assert fault.parameters == {"error_rate": 0.45}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("error-rate-spike")
    assert "order-service" in s.expected_affected_services
    assert "simulated error-rate spike" in s.expected_root_cause


def test_traffic_surge_scenario() -> None:
    s = get_scenario("traffic-surge")
    assert s.primary_fault is None
    assert s.workload.profile_name == WorkloadProfile.TRAFFIC_SURGE
    assert s.workload.base_requests_per_second == 10.0
    assert s.workload.surge_requests_per_second == 50.0
    assert s.workload.surge_start_seconds == 10.0
    assert s.workload.surge_duration_seconds == 10.0
    assert s.workload.total_duration_seconds == 30.0
    assert "deterministic ingress traffic surge" in s.expected_root_cause
    assert "api-gateway" in s.expected_affected_services


def test_bad_deployment_config_scenario() -> None:
    s = get_scenario("bad-deployment-config")
    assert s.primary_fault is not None
    fault = s.primary_fault.fault
    assert fault.fault_type == FaultType.ERROR
    assert fault.target_service_id == canonical_service_id("order-service")
    assert fault.parameters == {"error_rate": 0.35}
    assert fault.duration_seconds == 10.0
    assert fault.fault_id == scenario_fault_id("bad-deployment-config")

    assert s.system_marker is not None
    assert s.system_marker.marker_name == "deployment_changed"
    assert s.system_marker.service == "order-service"
    assert s.system_marker.payload == {
        "deployment_version": "bad-config-v1",
        "change_type": "configuration",
    }
    assert "order-service" in s.expected_affected_services
    assert "simulated bad configuration deployment" in s.expected_root_cause


def test_scenario_lookup_unknown_rejection() -> None:
    with pytest.raises(ValueError, match="Unknown scenario_id: 'unknown-id'"):
        get_scenario("unknown-id")


def test_deterministic_fault_ids() -> None:
    id1 = scenario_fault_id("cpu-saturation")
    id2 = scenario_fault_id("cpu-saturation")
    id3 = scenario_fault_id("memory-exhaustion")

    assert isinstance(id1, uuid.UUID)
    assert id1 == id2
    assert id1 != id3


def test_scenario_serialization_and_reconstruction() -> None:
    for s in list_scenarios():
        dumped_json = s.model_dump_json()
        reconstructed = ScenarioDefinition.model_validate_json(dumped_json)
        assert reconstructed == s
        assert reconstructed.model_dump() == s.model_dump()


def test_validation_unknown_affected_service() -> None:
    with pytest.raises(ValidationError, match="not a valid canonical service name"):
        ScenarioDefinition(
            scenario_id="invalid-svc",
            name="Invalid",
            description="Test",
            workload=WorkloadConfig(),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    target_service_id=canonical_service_id("order-service"),
                    fault_type=FaultType.CRASH,
                )
            ),
            expected_symptoms=("symptom",),
            expected_affected_services=("non-existent-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )


def test_validation_unknown_marker_service() -> None:
    with pytest.raises(ValidationError, match="not a valid canonical service name"):
        ScenarioSystemMarker(
            marker_name="deployment_changed",
            service="unknown-service",
        )


def test_validation_traffic_surge_cannot_have_fault() -> None:
    with pytest.raises(ValidationError, match="Traffic surge scenario must not define a primary_fault"):
        ScenarioDefinition(
            scenario_id="bad-surge",
            name="Bad Surge",
            description="Test",
            workload=WorkloadConfig(
                profile_name=WorkloadProfile.TRAFFIC_SURGE,
                base_requests_per_second=10.0,
                surge_start_seconds=10.0,
                surge_duration_seconds=10.0,
                surge_requests_per_second=50.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    target_service_id=canonical_service_id("order-service"),
                    fault_type=FaultType.CRASH,
                )
            ),
            expected_symptoms=("symptom",),
            expected_affected_services=("order-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )


def test_validation_traffic_surge_timing_consistency() -> None:
    # 1. Matching timing -> valid
    valid_surge = ScenarioDefinition(
        scenario_id="valid-surge",
        name="Valid Surge",
        description="Test",
        workload=WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            base_requests_per_second=10.0,
            surge_start_seconds=10.0,
            surge_duration_seconds=10.0,
            surge_requests_per_second=50.0,
            total_duration_seconds=30.0,
        ),
        primary_fault=None,
        activation_time_seconds=10.0,
        recovery_time_seconds=20.0,
        expected_symptoms=("symptom",),
        expected_affected_services=("order-service",),
        expected_root_cause="cause",
        recovery_condition="recovery",
    )
    assert valid_surge.activation_time_seconds == 10.0

    # 2. Activation mismatch -> error
    with pytest.raises(ValidationError, match="Traffic surge activation_time_seconds .* must equal workload.surge_start_seconds"):
        ScenarioDefinition(
            scenario_id="bad-activation",
            name="Bad Activation",
            description="Test",
            workload=WorkloadConfig(
                profile_name=WorkloadProfile.TRAFFIC_SURGE,
                base_requests_per_second=10.0,
                surge_start_seconds=10.0,
                surge_duration_seconds=10.0,
                surge_requests_per_second=50.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=None,
            activation_time_seconds=5.0,  # mismatch
            recovery_time_seconds=20.0,
            expected_symptoms=("symptom",),
            expected_affected_services=("order-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )

    # 3. Recovery mismatch -> error
    with pytest.raises(ValidationError, match="Traffic surge recovery_time_seconds .* must equal surge window end"):
        ScenarioDefinition(
            scenario_id="bad-recovery",
            name="Bad Recovery",
            description="Test",
            workload=WorkloadConfig(
                profile_name=WorkloadProfile.TRAFFIC_SURGE,
                base_requests_per_second=10.0,
                surge_start_seconds=10.0,
                surge_duration_seconds=10.0,
                surge_requests_per_second=50.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=None,
            activation_time_seconds=10.0,
            recovery_time_seconds=25.0,  # mismatch
            expected_symptoms=("symptom",),
            expected_affected_services=("order-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )

    # 4. Missing recovery time -> error
    with pytest.raises(ValidationError, match="Traffic surge scenario must define a non-None recovery_time_seconds"):
        ScenarioDefinition(
            scenario_id="missing-recovery",
            name="Missing Recovery",
            description="Test",
            workload=WorkloadConfig(
                profile_name=WorkloadProfile.TRAFFIC_SURGE,
                base_requests_per_second=10.0,
                surge_start_seconds=10.0,
                surge_duration_seconds=10.0,
                surge_requests_per_second=50.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=None,
            activation_time_seconds=10.0,
            recovery_time_seconds=None,  # missing
            expected_symptoms=("symptom",),
            expected_affected_services=("order-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )


def test_topology_version_source_of_truth_validation() -> None:
    for s in list_scenarios():
        assert s.topology_version == CANONICAL_TOPOLOGY_VERSION

    # Attempting to construct with divergent topology version -> error
    with pytest.raises(ValidationError, match="must match CANONICAL_TOPOLOGY_VERSION"):
        ScenarioDefinition(
            scenario_id="bad-topo-ver",
            name="Bad Topo Ver",
            description="Test",
            topology_version="2.0.0",
            workload=WorkloadConfig(base_requests_per_second=10.0),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    target_service_id=canonical_service_id("order-service"),
                    fault_type=FaultType.CRASH,
                )
            ),
            expected_symptoms=("symptom",),
            expected_affected_services=("order-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )


def test_catalogue_deep_isolation_against_nested_mutation() -> None:
    # 1. FaultSpec.parameters mutation attempt
    s_err = get_scenario("error-rate-spike")
    assert s_err.primary_fault is not None
    s_err.primary_fault.fault.parameters["error_rate"] = 999.0

    # Retrieve again: canonical error_rate is still 0.45
    s_err_fresh = get_scenario("error-rate-spike")
    assert s_err_fresh.primary_fault is not None
    assert s_err_fresh.primary_fault.fault.parameters["error_rate"] == 0.45

    # 2. System marker payload mutation attempt
    s_deploy = get_scenario("bad-deployment-config")
    assert s_deploy.system_marker is not None
    s_deploy.system_marker.payload["deployment_version"] = "mutated-version"

    s_deploy_fresh = get_scenario("bad-deployment-config")
    assert s_deploy_fresh.system_marker is not None
    assert s_deploy_fresh.system_marker.payload["deployment_version"] == "bad-config-v1"

    # 3. list_scenarios() mutation cannot corrupt later catalogue reads
    scenarios = list_scenarios()
    scenarios[0].primary_fault.fault.parameters["utilization_pct"] = 0.0  # type: ignore

    fresh_cpu = get_scenario("cpu-saturation")
    assert fresh_cpu.primary_fault is not None
    assert fresh_cpu.primary_fault.fault.parameters["utilization_pct"] == 95.0


def test_validation_fault_scenario_must_have_primary_fault() -> None:
    with pytest.raises(ValidationError, match="Non-traffic-surge fault scenario must define a primary_fault"):
        ScenarioDefinition(
            scenario_id="missing-fault",
            name="Missing Fault",
            description="Test",
            workload=WorkloadConfig(base_requests_per_second=10.0),
            primary_fault=None,
            expected_symptoms=("symptom",),
            expected_affected_services=("order-service",),
            expected_root_cause="cause",
            recovery_condition="recovery",
        )


def test_package_exports() -> None:
    import app.simulator.scenarios as sc_pkg

    expected = [
        "INITIAL_SCENARIOS",
        "SCENARIO_VERSION",
        "ScenarioDefinition",
        "ScenarioSystemMarker",
        "ScheduledFault",
        "get_scenario",
        "list_scenarios",
        "scenario_fault_id",
        "scenario_ids",
    ]
    assert hasattr(sc_pkg, "__all__")
    assert sorted(sc_pkg.__all__) == sorted(expected)
    for sym in expected:
        assert hasattr(sc_pkg, sym)
