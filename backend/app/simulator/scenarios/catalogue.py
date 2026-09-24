import uuid

from app.simulator.interfaces import FaultSpec, FaultType
from app.simulator.scenarios.models import (
    ScenarioDefinition,
    ScenarioSystemMarker,
    ScheduledFault,
)
from app.simulator.topology import (
    AEGISOPS_NAMESPACE,
    CANONICAL_TOPOLOGY_VERSION,
    canonical_service_id,
)
from app.simulator.workload import WorkloadConfig, WorkloadProfile

SCENARIO_VERSION: str = "1.0.0"


def scenario_fault_id(scenario_id: str, version: str = SCENARIO_VERSION) -> uuid.UUID:
    """Derive deterministic UUIDv5 identifier for a scenario's primary fault."""
    return uuid.uuid5(AEGISOPS_NAMESPACE, f"scenario:{scenario_id}:primary-fault:v{version}")


def _build_catalogue() -> tuple[ScenarioDefinition, ...]:
    scenarios = [
        # 1. CPU Saturation
        ScenarioDefinition(
            scenario_id="cpu-saturation",
            version=SCENARIO_VERSION,
            name="CPU Saturation on Order Service",
            description="Simulates heavy CPU saturation on order-service under nominal workload.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("cpu-saturation"),
                    target_service_id=canonical_service_id("order-service"),
                    fault_type=FaultType.RESOURCE,
                    duration_seconds=10.0,
                    parameters={"resource_kind": "cpu", "utilization_pct": 95.0},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "simulated CPU utilization spike",
                "latency degradation evidence",
                "possible request queueing and degradation",
            ),
            expected_affected_services=(
                "order-service",
                "api-gateway",
                "payment-service",
                "inventory-service",
                "notification-service",
            ),
            expected_root_cause="simulated CPU saturation on order-service",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="CPU utilization and service state restored to pre-fault baseline",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 2. Memory Exhaustion
        ScenarioDefinition(
            scenario_id="memory-exhaustion",
            version=SCENARIO_VERSION,
            name="Memory Exhaustion on Notification Service",
            description="Simulates memory exhaustion on the asynchronous notification-service worker.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("memory-exhaustion"),
                    target_service_id=canonical_service_id("notification-service"),
                    fault_type=FaultType.RESOURCE,
                    duration_seconds=10.0,
                    parameters={"resource_kind": "memory", "utilization_pct": 95.0},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "simulated memory utilization trend/spike",
                "potential worker degradation evidence",
            ),
            expected_affected_services=("notification-service",),
            expected_root_cause="simulated memory exhaustion on notification-service",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="memory utilization restored to pre-fault baseline and service returns to nominal simulated state",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 3. Connection Exhaustion
        ScenarioDefinition(
            scenario_id="connection-exhaustion",
            version=SCENARIO_VERSION,
            name="Database Connection Pool Exhaustion",
            description="Simulates complete connection pool saturation on the shared database datastore.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("connection-exhaustion"),
                    target_service_id=canonical_service_id("database"),
                    fault_type=FaultType.RESOURCE,
                    duration_seconds=10.0,
                    parameters={"resource_kind": "connection", "utilization_pct": 100.0},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "simulated connection-pool saturation",
                "downstream datastore timeout/error evidence",
                "cascading degradation across payment and inventory services",
            ),
            expected_affected_services=(
                "database",
                "payment-service",
                "inventory-service",
                "order-service",
                "api-gateway",
            ),
            expected_root_cause="simulated database connection-pool exhaustion",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="connection usage restored below saturation to pre-fault baseline",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 4. Dependency Latency
        ScenarioDefinition(
            scenario_id="dependency-latency",
            version=SCENARIO_VERSION,
            name="Payment Service Dependency Latency",
            description="Simulates elevated response latency on the payment-service downstream dependency.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("dependency-latency"),
                    target_service_id=canonical_service_id("payment-service"),
                    fault_type=FaultType.LATENCY,
                    duration_seconds=10.0,
                    parameters={"latency_add_ms": 150.0},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "payment-service latency increase",
                "upstream order-service latency increase",
                "elevated end-to-end response times at api-gateway",
            ),
            expected_affected_services=(
                "payment-service",
                "order-service",
                "api-gateway",
            ),
            expected_root_cause="simulated latency degradation on payment-service dependency",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="payment-service effective latency restored to pre-fault baseline",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 5. Dependency Failure
        ScenarioDefinition(
            scenario_id="dependency-failure",
            version=SCENARIO_VERSION,
            name="Database Dependency Network Failure",
            description="Simulates network unreachability for the shared database datastore.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("dependency-failure"),
                    target_service_id=canonical_service_id("database"),
                    fault_type=FaultType.NETWORK,
                    duration_seconds=10.0,
                    parameters={},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "database simulated unreachable state",
                "payment and inventory dependency failures",
                "upstream order processing errors",
            ),
            expected_affected_services=(
                "database",
                "payment-service",
                "inventory-service",
                "order-service",
                "api-gateway",
            ),
            expected_root_cause="simulated database dependency network failure",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="database network reachability restored",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 6. Error Rate Spike
        ScenarioDefinition(
            scenario_id="error-rate-spike",
            version=SCENARIO_VERSION,
            name="Order Service Error Rate Spike",
            description="Simulates sudden internal application error rate increase on order-service.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("error-rate-spike"),
                    target_service_id=canonical_service_id("order-service"),
                    fault_type=FaultType.ERROR,
                    duration_seconds=10.0,
                    parameters={"error_rate": 0.45},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "abrupt simulated application error-rate increase",
                "5xx/error metric evidence",
                "structured error-log evidence",
            ),
            expected_affected_services=(
                "order-service",
                "api-gateway",
            ),
            expected_root_cause="simulated error-rate spike on order-service",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="error rate restored to pre-fault baseline",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 7. Traffic Surge
        ScenarioDefinition(
            scenario_id="traffic-surge",
            version=SCENARIO_VERSION,
            name="Ingress Traffic Surge",
            description="Simulates 5x traffic surge on the ingress api-gateway without fault injection.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
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
            expected_symptoms=(
                "request-rate spike",
                "elevated throughput across all services",
                "adaptive-baseline workload change",
            ),
            expected_affected_services=(
                "api-gateway",
                "order-service",
                "payment-service",
                "inventory-service",
                "notification-service",
                "database",
            ),
            expected_root_cause="deterministic ingress traffic surge",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="workload rate returns to configured baseline after surge window",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
        # 8. Bad Deployment / Config
        ScenarioDefinition(
            scenario_id="bad-deployment-config",
            version=SCENARIO_VERSION,
            name="Bad Configuration Deployment on Order Service",
            description="Simulates an erroneous configuration deployment event on order-service accompanied by elevated error rate.",
            default_seed=42,
            topology_reference="canonical",
            topology_version=CANONICAL_TOPOLOGY_VERSION,
            workload=WorkloadConfig(
                base_requests_per_second=10.0,
                total_duration_seconds=30.0,
            ),
            system_marker=ScenarioSystemMarker(
                marker_name="deployment_changed",
                service="order-service",
                payload={"deployment_version": "bad-config-v1", "change_type": "configuration"},
            ),
            primary_fault=ScheduledFault(
                fault=FaultSpec(
                    fault_id=scenario_fault_id("bad-deployment-config"),
                    target_service_id=canonical_service_id("order-service"),
                    fault_type=FaultType.ERROR,
                    duration_seconds=10.0,
                    parameters={"error_rate": 0.35},
                )
            ),
            activation_time_seconds=10.0,
            expected_symptoms=(
                "deployment/config marker",
                "changed error signature",
                "elevated simulated error rate",
                "historical/deployment-context evidence",
            ),
            expected_affected_services=(
                "order-service",
                "api-gateway",
            ),
            expected_root_cause="simulated bad configuration deployment on order-service",
            observation_window_seconds=10.0,
            recovery_time_seconds=20.0,
            recovery_condition="deployment-related degraded error state is reverted to pre-change baseline",
            post_recovery_observation_window_seconds=10.0,
            total_duration_seconds=30.0,
        ),
    ]
    return tuple(scenarios)


INITIAL_SCENARIOS: tuple[ScenarioDefinition, ...] = _build_catalogue()
_SCENARIO_MAP: dict[str, ScenarioDefinition] = {s.scenario_id: s for s in INITIAL_SCENARIOS}


def get_scenario(scenario_id: str) -> ScenarioDefinition:
    """Retrieve an immutable scenario definition from the initial catalogue by ID.

    Returns a deep defensive copy to prevent external mutation of authoritative catalogue state.
    """
    if scenario_id not in _SCENARIO_MAP:
        raise ValueError(f"Unknown scenario_id: {scenario_id!r}. Available: {sorted(_SCENARIO_MAP.keys())}")
    return _SCENARIO_MAP[scenario_id].model_copy(deep=True)


def list_scenarios() -> tuple[ScenarioDefinition, ...]:
    """Return all scenario definitions in the initial catalogue in canonical order.

    Returns deep defensive copies to ensure catalogue immutability.
    """
    return tuple(s.model_copy(deep=True) for s in INITIAL_SCENARIOS)


def scenario_ids() -> tuple[str, ...]:
    """Return tuple of all scenario IDs in canonical catalogue order."""
    return tuple(s.scenario_id for s in INITIAL_SCENARIOS)
