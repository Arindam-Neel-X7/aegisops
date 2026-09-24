import uuid
from typing import Any

from app.simulator.interfaces import (
    FaultInjectionResult,
    FaultSpec,
    FaultType,
)
from app.simulator.runtime.state import ServiceRuntimeState, SimulationState


class ActiveFault:
    """Internal runtime tracking representation for an active injected fault."""

    __slots__ = ("fault_id", "spec", "target_service_id", "snapshot", "is_active")

    def __init__(
        self,
        fault_id: uuid.UUID,
        spec: FaultSpec,
        target_service_id: uuid.UUID,
        snapshot: ServiceRuntimeState,
        is_active: bool = True,
    ) -> None:
        self.fault_id = fault_id
        self.spec = spec
        self.target_service_id = target_service_id
        self.snapshot = snapshot
        self.is_active = is_active

    def clone(self) -> "ActiveFault":
        """Return a defensive deep copy of this ActiveFault."""
        return ActiveFault(
            fault_id=self.fault_id,
            spec=self.spec.model_copy(deep=True),
            target_service_id=self.target_service_id,
            snapshot=self.snapshot.clone(),
            is_active=self.is_active,
        )


class ConcreteFaultInjector:
    """Deterministic in-memory fault injector implementing the frozen FaultInjector protocol.

    Applies and reverses simulated state mutations without any physical host manipulation.
    """

    def __init__(self, state: SimulationState) -> None:
        self.state = state
        self._active_faults: dict[uuid.UUID, ActiveFault] = {}
        self._target_to_fault: dict[uuid.UUID, uuid.UUID] = {}

    async def inject(self, fault: FaultSpec) -> FaultInjectionResult:
        """Validate and apply a simulated fault specification to target service runtime state."""
        # 1. Target service exists
        if not self.state.has_service(fault.target_service_id):
            return FaultInjectionResult(
                fault_id=fault.fault_id,
                accepted=False,
                message=f"Target service {fault.target_service_id} not found in topology",
            )

        # 2. Duplicate fault_id check
        if fault.fault_id in self._active_faults:
            return FaultInjectionResult(
                fault_id=fault.fault_id,
                accepted=False,
                message=f"Fault {fault.fault_id} is already active",
            )

        # 3. Conflicting active fault on target service
        if fault.target_service_id in self._target_to_fault:
            active_id = self._target_to_fault[fault.target_service_id]
            return FaultInjectionResult(
                fault_id=fault.fault_id,
                accepted=False,
                message=f"Target service {fault.target_service_id} already has active fault {active_id}",
            )

        # 4. Parameter validation
        validation_error = self._validate_fault_parameters(fault)
        if validation_error:
            return FaultInjectionResult(
                fault_id=fault.fault_id,
                accepted=False,
                message=validation_error,
            )

        # 5. Capture pre-fault snapshot and apply mutation
        current_state = self.state.get_service_state(fault.target_service_id)
        snapshot = current_state.clone()
        self._apply_fault_mutation(fault, current_state)

        # 6. Register active fault
        current_state.active_fault_ids.add(fault.fault_id)
        active = ActiveFault(
            fault_id=fault.fault_id,
            spec=fault,
            target_service_id=fault.target_service_id,
            snapshot=snapshot,
            is_active=True,
        )
        self._active_faults[fault.fault_id] = active
        self._target_to_fault[fault.target_service_id] = fault.fault_id

        return FaultInjectionResult(
            fault_id=fault.fault_id,
            accepted=True,
            message="Fault injected successfully",
        )

    async def recover(self, fault_id: uuid.UUID) -> FaultInjectionResult:
        """Revert a previously injected fault and restore target service runtime state."""
        if fault_id not in self._active_faults:
            return FaultInjectionResult(
                fault_id=fault_id,
                accepted=False,
                message=f"Fault {fault_id} is not active",
            )

        active = self._active_faults[fault_id]
        target_state = self.state.get_service_state(active.target_service_id)

        # Restore pre-fault snapshot
        target_state.is_available = active.snapshot.is_available
        target_state.is_crashed = active.snapshot.is_crashed
        target_state.base_latency_ms = active.snapshot.base_latency_ms
        target_state.effective_latency_ms = active.snapshot.effective_latency_ms
        target_state.error_rate = active.snapshot.error_rate
        target_state.timeout_rate = active.snapshot.timeout_rate
        target_state.cpu_utilization_pct = active.snapshot.cpu_utilization_pct
        target_state.memory_utilization_pct = active.snapshot.memory_utilization_pct
        target_state.connection_pool_used = active.snapshot.connection_pool_used
        target_state.connection_pool_capacity = active.snapshot.connection_pool_capacity
        target_state.network_reachable = active.snapshot.network_reachable
        target_state.active_fault_ids.discard(fault_id)

        # Unregister
        del self._active_faults[fault_id]
        self._target_to_fault.pop(active.target_service_id, None)
        active.is_active = False

        return FaultInjectionResult(
            fault_id=fault_id,
            accepted=True,
            message="Fault recovered successfully",
        )

    def reset(self) -> None:
        """Clear all active faults and restore all service states to baseline."""
        for active in list(self._active_faults.values()):
            target_state = self.state.get_service_state(active.target_service_id)
            target_state.is_available = active.snapshot.is_available
            target_state.is_crashed = active.snapshot.is_crashed
            target_state.base_latency_ms = active.snapshot.base_latency_ms
            target_state.effective_latency_ms = active.snapshot.effective_latency_ms
            target_state.error_rate = active.snapshot.error_rate
            target_state.timeout_rate = active.snapshot.timeout_rate
            target_state.cpu_utilization_pct = active.snapshot.cpu_utilization_pct
            target_state.memory_utilization_pct = active.snapshot.memory_utilization_pct
            target_state.connection_pool_used = active.snapshot.connection_pool_used
            target_state.connection_pool_capacity = active.snapshot.connection_pool_capacity
            target_state.network_reachable = active.snapshot.network_reachable
            target_state.active_fault_ids.clear()
            active.is_active = False

        self._active_faults.clear()
        self._target_to_fault.clear()
        self.state.reset_to_baseline()

    def active_faults(self) -> tuple[ActiveFault, ...]:
        """Return an immutable tuple of defensive copies of currently active faults."""
        return tuple(f.clone() for f in self._active_faults.values())

    def get_active_fault(self, fault_id: uuid.UUID) -> ActiveFault | None:
        """Retrieve a defensive copy of active fault metadata by fault_id."""
        active = self._active_faults.get(fault_id)
        return active.clone() if active is not None else None

    def is_active(self, fault_id: uuid.UUID) -> bool:
        """Check if a fault is currently active."""
        return fault_id in self._active_faults

    def _validate_fault_parameters(self, fault: FaultSpec) -> str | None:
        params: dict[str, Any] = fault.parameters

        if fault.fault_type == FaultType.LATENCY:
            latency_add = params.get("latency_add_ms", params.get("added_latency_ms"))
            if latency_add is None:
                return "Missing latency_add_ms parameter for LATENCY fault"
            if not isinstance(latency_add, (int, float)) or latency_add <= 0:
                return f"latency_add_ms must be a positive number, got {latency_add!r}"

        elif fault.fault_type == FaultType.ERROR:
            error_rate = params.get("error_rate")
            if error_rate is None:
                return "Missing error_rate parameter for ERROR fault"
            if not isinstance(error_rate, (int, float)) or not (0.0 < error_rate <= 1.0):
                return f"error_rate must be in range (0.0, 1.0], got {error_rate!r}"

        elif fault.fault_type == FaultType.TIMEOUT:
            timeout_rate = params.get("timeout_rate")
            if timeout_rate is None:
                return "Missing timeout_rate parameter for TIMEOUT fault"
            if not isinstance(timeout_rate, (int, float)) or not (0.0 < timeout_rate <= 1.0):
                return f"timeout_rate must be in range (0.0, 1.0], got {timeout_rate!r}"

        elif fault.fault_type == FaultType.CRASH:
            # Crash requires no parameters
            pass

        elif fault.fault_type == FaultType.RESOURCE:
            kind = params.get("resource_kind")
            if kind not in ("cpu", "memory", "connection"):
                return f"Unsupported resource_kind: {kind!r}. Must be 'cpu', 'memory', or 'connection'"

            if kind in ("cpu", "memory"):
                pct = params.get("utilization_pct")
                if pct is None or not isinstance(pct, (int, float)) or not (0.0 < pct <= 100.0):
                    return f"utilization_pct must be in range (0.0, 100.0], got {pct!r}"
            elif kind == "connection":
                target_state = self.state.get_service_state(fault.target_service_id)
                pct = params.get("utilization_pct")
                used = params.get("used_connections")
                if pct is None and used is None:
                    return "utilization_pct or used_connections required for connection resource fault"
                if pct is not None and (not isinstance(pct, (int, float)) or not (0.0 < pct <= 100.0)):
                    return f"utilization_pct must be in range (0.0, 100.0], got {pct!r}"
                if used is not None:
                    if not isinstance(used, int) or used < 0:
                        return f"used_connections must be a non-negative integer, got {used!r}"
                    if used > target_state.connection_pool_capacity:
                        return (
                            f"used_connections ({used}) cannot exceed target service "
                            f"connection_pool_capacity ({target_state.connection_pool_capacity})"
                        )

        elif fault.fault_type == FaultType.NETWORK:
            # Network requires no mandatory parameters
            pass

        return None

    def _apply_fault_mutation(self, fault: FaultSpec, state: ServiceRuntimeState) -> None:
        params = fault.parameters

        if fault.fault_type == FaultType.LATENCY:
            latency_add = float(params.get("latency_add_ms", params.get("added_latency_ms", 0.0)))
            state.effective_latency_ms += latency_add

        elif fault.fault_type == FaultType.ERROR:
            state.error_rate = float(params["error_rate"])

        elif fault.fault_type == FaultType.TIMEOUT:
            state.timeout_rate = float(params["timeout_rate"])

        elif fault.fault_type == FaultType.CRASH:
            state.is_crashed = True
            state.is_available = False

        elif fault.fault_type == FaultType.RESOURCE:
            kind = params["resource_kind"]
            if kind == "cpu":
                state.cpu_utilization_pct = float(params["utilization_pct"])
            elif kind == "memory":
                state.memory_utilization_pct = float(params["utilization_pct"])
            elif kind == "connection":
                if "used_connections" in params:
                    state.connection_pool_used = int(params["used_connections"])
                else:
                    pct = float(params["utilization_pct"])
                    if pct >= 100.0:
                        state.connection_pool_used = state.connection_pool_capacity
                    else:
                        state.connection_pool_used = int(round(state.connection_pool_capacity * (pct / 100.0)))

        elif fault.fault_type == FaultType.NETWORK:
            state.network_reachable = False
