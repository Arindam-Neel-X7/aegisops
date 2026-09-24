import uuid
from pydantic import BaseModel, Field, model_validator

from app.simulator.interfaces import ServiceTopology
from app.simulator.topology import build_canonical_topology


class ServiceRuntimeState(BaseModel):
    """Mutable simulated runtime state for a single service node."""

    service_id: uuid.UUID
    name: str
    is_available: bool = True
    is_crashed: bool = False
    base_latency_ms: float = 0.0
    effective_latency_ms: float = 0.0
    error_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    timeout_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    cpu_utilization_pct: float = Field(default=15.0, ge=0.0, le=100.0)
    memory_utilization_pct: float = Field(default=25.0, ge=0.0, le=100.0)
    connection_pool_used: int = Field(default=5, ge=0)
    connection_pool_capacity: int = Field(default=100, gt=0)
    network_reachable: bool = True
    active_fault_ids: set[uuid.UUID] = Field(default_factory=set)

    @model_validator(mode="after")
    def validate_connection_pool_capacity(self) -> "ServiceRuntimeState":
        if self.connection_pool_used > self.connection_pool_capacity:
            raise ValueError(
                f"connection_pool_used ({self.connection_pool_used}) cannot exceed connection_pool_capacity ({self.connection_pool_capacity})"
            )
        return self

    def clone(self) -> "ServiceRuntimeState":
        """Return an independent copy of this state for snapshotting/restoration."""
        return self.model_copy(deep=True)


class SimulationState:
    """Central container managing mutable in-memory runtime states for all services in a topology."""

    def __init__(self, topology: ServiceTopology | None = None) -> None:
        self.topology = topology if topology is not None else build_canonical_topology()
        self._states: dict[uuid.UUID, ServiceRuntimeState] = {}
        self._baseline_snapshots: dict[uuid.UUID, ServiceRuntimeState] = {}
        self._init_states()

    def _init_states(self) -> None:
        for service in self.topology.services:
            state = ServiceRuntimeState(
                service_id=service.service_id,
                name=service.name,
            )
            self._states[service.service_id] = state
            self._baseline_snapshots[service.service_id] = state.clone()

    def get_service_state(self, service_id: uuid.UUID) -> ServiceRuntimeState:
        """Retrieve the mutable runtime state for a service ID."""
        if service_id not in self._states:
            raise ValueError(f"Unknown service_id: {service_id}")
        return self._states[service_id]

    def has_service(self, service_id: uuid.UUID) -> bool:
        """Check if a service ID exists in the topology."""
        return service_id in self._states

    def reset_to_baseline(self) -> None:
        """Restore all service states to clean baseline snapshots."""
        for service_id, baseline in self._baseline_snapshots.items():
            self._states[service_id] = baseline.clone()

    def all_states(self) -> dict[uuid.UUID, ServiceRuntimeState]:
        """Return a mapping of all service states."""
        return dict(self._states)
