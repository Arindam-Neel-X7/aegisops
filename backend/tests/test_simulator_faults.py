import uuid
import pytest

from app.simulator.faults import ConcreteFaultInjector
from app.simulator.interfaces import (
    FaultInjector,
    FaultSpec,
    FaultType,
)
from app.simulator.runtime import ServiceRuntimeState, SimulationState
from app.simulator.topology import build_canonical_topology, canonical_service_id


@pytest.fixture
def canonical_state() -> SimulationState:
    return SimulationState(build_canonical_topology())


@pytest.fixture
def injector(canonical_state: SimulationState) -> ConcreteFaultInjector:
    return ConcreteFaultInjector(canonical_state)


@pytest.mark.asyncio
async def test_protocol_compatibility(injector: ConcreteFaultInjector) -> None:
    protocol_injector: FaultInjector = injector
    assert hasattr(protocol_injector, "inject")
    assert callable(protocol_injector.inject)
    assert hasattr(protocol_injector, "recover")
    assert callable(protocol_injector.recover)


def test_baseline_runtime_state(canonical_state: SimulationState) -> None:
    gateway_id = canonical_service_id("api-gateway")
    state = canonical_state.get_service_state(gateway_id)

    assert state.service_id == gateway_id
    assert state.name == "api-gateway"
    assert state.is_available is True
    assert state.is_crashed is False
    assert state.effective_latency_ms == 0.0
    assert state.error_rate == 0.0
    assert state.timeout_rate == 0.0
    assert state.cpu_utilization_pct == 15.0
    assert state.memory_utilization_pct == 25.0
    assert state.connection_pool_used == 5
    assert state.network_reachable is True
    assert state.active_fault_ids == set()


@pytest.mark.asyncio
async def test_latency_fault_injection_and_recovery(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("order-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.LATENCY,
        parameters={"latency_add_ms": 150.0},
    )

    # 1. Inject
    result = await injector.inject(spec)
    assert result.accepted is True
    assert result.fault_id == fault_id

    state = canonical_state.get_service_state(service_id)
    assert state.effective_latency_ms == 150.0
    assert fault_id in state.active_fault_ids
    assert injector.is_active(fault_id) is True

    # 2. Recover
    rec_result = await injector.recover(fault_id)
    assert rec_result.accepted is True
    assert rec_result.fault_id == fault_id

    assert state.effective_latency_ms == 0.0
    assert fault_id not in state.active_fault_ids
    assert injector.is_active(fault_id) is False


@pytest.mark.asyncio
async def test_error_fault_injection_and_recovery(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("payment-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.ERROR,
        parameters={"error_rate": 0.45},
    )

    result = await injector.inject(spec)
    assert result.accepted is True
    assert canonical_state.get_service_state(service_id).error_rate == 0.45

    rec_result = await injector.recover(fault_id)
    assert rec_result.accepted is True
    assert canonical_state.get_service_state(service_id).error_rate == 0.0


@pytest.mark.asyncio
async def test_timeout_fault_injection_and_recovery(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("inventory-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.TIMEOUT,
        parameters={"timeout_rate": 0.8},
    )

    result = await injector.inject(spec)
    assert result.accepted is True
    assert canonical_state.get_service_state(service_id).timeout_rate == 0.8

    rec_result = await injector.recover(fault_id)
    assert rec_result.accepted is True
    assert canonical_state.get_service_state(service_id).timeout_rate == 0.0


@pytest.mark.asyncio
async def test_crash_fault_injection_and_recovery(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("notification-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.CRASH,
    )

    result = await injector.inject(spec)
    assert result.accepted is True
    state = canonical_state.get_service_state(service_id)
    assert state.is_crashed is True
    assert state.is_available is False

    rec_result = await injector.recover(fault_id)
    assert rec_result.accepted is True
    assert state.is_crashed is False
    assert state.is_available is True


@pytest.mark.asyncio
async def test_resource_cpu_and_memory_faults(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("database")
    fault_cpu = FaultSpec(
        fault_id=uuid.uuid4(),
        target_service_id=service_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "cpu", "utilization_pct": 95.0},
    )

    result = await injector.inject(fault_cpu)
    assert result.accepted is True
    assert canonical_state.get_service_state(service_id).cpu_utilization_pct == 95.0

    await injector.recover(fault_cpu.fault_id)
    assert canonical_state.get_service_state(service_id).cpu_utilization_pct == 15.0

    # Memory
    fault_mem = FaultSpec(
        fault_id=uuid.uuid4(),
        target_service_id=service_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "memory", "utilization_pct": 90.0},
    )
    result_mem = await injector.inject(fault_mem)
    assert result_mem.accepted is True
    assert canonical_state.get_service_state(service_id).memory_utilization_pct == 90.0

    await injector.recover(fault_mem.fault_id)
    assert canonical_state.get_service_state(service_id).memory_utilization_pct == 25.0


@pytest.mark.asyncio
async def test_resource_connection_fault(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("database")
    fault_conn = FaultSpec(
        fault_id=uuid.uuid4(),
        target_service_id=service_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "connection", "used_connections": 98},
    )

    result = await injector.inject(fault_conn)
    assert result.accepted is True
    assert canonical_state.get_service_state(service_id).connection_pool_used == 98

    await injector.recover(fault_conn.fault_id)
    assert canonical_state.get_service_state(service_id).connection_pool_used == 5


@pytest.mark.asyncio
async def test_network_fault_injection_and_recovery(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("payment-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.NETWORK,
    )

    result = await injector.inject(spec)
    assert result.accepted is True
    assert canonical_state.get_service_state(service_id).network_reachable is False

    rec_result = await injector.recover(fault_id)
    assert rec_result.accepted is True
    assert canonical_state.get_service_state(service_id).network_reachable is True


@pytest.mark.asyncio
async def test_unknown_target_service_rejected(injector: ConcreteFaultInjector) -> None:
    unknown_id = uuid.uuid4()
    spec = FaultSpec(
        target_service_id=unknown_id,
        fault_type=FaultType.CRASH,
    )
    result = await injector.inject(spec)
    assert result.accepted is False
    assert "not found in topology" in (result.message or "")


@pytest.mark.asyncio
async def test_duplicate_fault_id_rejected(injector: ConcreteFaultInjector) -> None:
    service_id = canonical_service_id("api-gateway")
    fault_id = uuid.uuid4()
    spec1 = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.CRASH,
    )
    res1 = await injector.inject(spec1)
    assert res1.accepted is True

    # Same fault_id on another service
    spec2 = FaultSpec(
        fault_id=fault_id,
        target_service_id=canonical_service_id("order-service"),
        fault_type=FaultType.CRASH,
    )
    res2 = await injector.inject(spec2)
    assert res2.accepted is False
    assert "already active" in (res2.message or "")


@pytest.mark.asyncio
async def test_conflicting_same_target_fault_rejected(injector: ConcreteFaultInjector) -> None:
    service_id = canonical_service_id("order-service")
    spec1 = FaultSpec(
        target_service_id=service_id,
        fault_type=FaultType.CRASH,
    )
    res1 = await injector.inject(spec1)
    assert res1.accepted is True

    # Second fault on same service
    spec2 = FaultSpec(
        target_service_id=service_id,
        fault_type=FaultType.LATENCY,
        parameters={"latency_add_ms": 100.0},
    )
    res2 = await injector.inject(spec2)
    assert res2.accepted is False
    assert "already has active fault" in (res2.message or "")


@pytest.mark.asyncio
async def test_invalid_parameters_rejected(injector: ConcreteFaultInjector) -> None:
    service_id = canonical_service_id("api-gateway")

    # Missing latency parameter
    res = await injector.inject(
        FaultSpec(target_service_id=service_id, fault_type=FaultType.LATENCY, parameters={})
    )
    assert res.accepted is False
    assert "Missing latency_add_ms" in (res.message or "")

    # Invalid error rate
    res = await injector.inject(
        FaultSpec(target_service_id=service_id, fault_type=FaultType.ERROR, parameters={"error_rate": 1.5})
    )
    assert res.accepted is False
    assert "error_rate must be in range" in (res.message or "")

    # Invalid timeout rate
    res = await injector.inject(
        FaultSpec(target_service_id=service_id, fault_type=FaultType.TIMEOUT, parameters={"timeout_rate": -0.1})
    )
    assert res.accepted is False
    assert "timeout_rate must be in range" in (res.message or "")

    # Invalid resource kind
    res = await injector.inject(
        FaultSpec(target_service_id=service_id, fault_type=FaultType.RESOURCE, parameters={"resource_kind": "disk"})
    )
    assert res.accepted is False
    assert "Unsupported resource_kind" in (res.message or "")

    # Invalid utilization percentage
    res = await injector.inject(
        FaultSpec(
            target_service_id=service_id,
            fault_type=FaultType.RESOURCE,
            parameters={"resource_kind": "cpu", "utilization_pct": 120.0},
        )
    )
    assert res.accepted is False
    assert "utilization_pct must be in range" in (res.message or "")


@pytest.mark.asyncio
async def test_recover_unknown_and_double_recovery(injector: ConcreteFaultInjector) -> None:
    unknown_id = uuid.uuid4()
    res = await injector.recover(unknown_id)
    assert res.accepted is False
    assert "is not active" in (res.message or "")

    service_id = canonical_service_id("api-gateway")
    fault_id = uuid.uuid4()
    await injector.inject(FaultSpec(fault_id=fault_id, target_service_id=service_id, fault_type=FaultType.CRASH))

    # First recovery -> True
    res1 = await injector.recover(fault_id)
    assert res1.accepted is True

    # Second recovery -> False
    res2 = await injector.recover(fault_id)
    assert res2.accepted is False
    assert "is not active" in (res2.message or "")


@pytest.mark.asyncio
async def test_active_fault_queries(injector: ConcreteFaultInjector) -> None:
    service_id = canonical_service_id("order-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(fault_id=fault_id, target_service_id=service_id, fault_type=FaultType.CRASH)

    assert injector.active_faults() == ()
    assert injector.get_active_fault(fault_id) is None
    assert injector.is_active(fault_id) is False

    await injector.inject(spec)

    active_list = injector.active_faults()
    assert len(active_list) == 1
    assert active_list[0].fault_id == fault_id
    assert active_list[0].is_active is True

    active = injector.get_active_fault(fault_id)
    assert active is not None
    assert active.target_service_id == service_id
    assert injector.is_active(fault_id) is True


@pytest.mark.asyncio
async def test_active_fault_query_defensive_copy_isolation(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("order-service")
    fault_id = uuid.uuid4()
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=service_id,
        fault_type=FaultType.LATENCY,
        parameters={"latency_add_ms": 100.0},
    )
    await injector.inject(spec)

    # Query active fault and attempt to mutate returned snapshot and metadata
    active = injector.get_active_fault(fault_id)
    assert active is not None
    active.snapshot.effective_latency_ms = 9999.0
    active.is_active = False

    # Also mutate list item from active_faults()
    active_items = injector.active_faults()
    active_items[0].snapshot.effective_latency_ms = 8888.0

    # Verify injector internal state was NOT corrupted
    assert injector.is_active(fault_id) is True

    # Recover fault and prove that clean baseline (0.0 ms) is still restored exactly
    rec_result = await injector.recover(fault_id)
    assert rec_result.accepted is True
    assert canonical_state.get_service_state(service_id).effective_latency_ms == 0.0


def test_service_runtime_state_connection_capacity_validation() -> None:
    service_id = uuid.uuid4()
    # Below capacity -> valid
    s1 = ServiceRuntimeState(service_id=service_id, name="s1", connection_pool_used=50, connection_pool_capacity=100)
    assert s1.connection_pool_used == 50

    # Equal to capacity -> valid
    s2 = ServiceRuntimeState(service_id=service_id, name="s2", connection_pool_used=100, connection_pool_capacity=100)
    assert s2.connection_pool_used == 100

    # Exceeding capacity -> raises ValidationError
    with pytest.raises(Exception, match="cannot exceed connection_pool_capacity"):
        ServiceRuntimeState(service_id=service_id, name="s3", connection_pool_used=101, connection_pool_capacity=100)


@pytest.mark.asyncio
async def test_connection_pool_fault_capacity_invariants(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    service_id = canonical_service_id("database")

    # 1. used_connections equal to capacity (100) -> valid
    f_full = FaultSpec(
        target_service_id=service_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "connection", "used_connections": 100},
    )
    res_full = await injector.inject(f_full)
    assert res_full.accepted is True
    assert canonical_state.get_service_state(service_id).connection_pool_used == 100
    await injector.recover(f_full.fault_id)
    assert canonical_state.get_service_state(service_id).connection_pool_used == 5

    # 2. used_connections above capacity (105) -> rejected
    f_overflow = FaultSpec(
        target_service_id=service_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "connection", "used_connections": 105},
    )
    res_overflow = await injector.inject(f_overflow)
    assert res_overflow.accepted is False
    assert "cannot exceed target service connection_pool_capacity" in (res_overflow.message or "")

    # 3. 100% utilization -> maps to exactly capacity (100)
    f_pct = FaultSpec(
        target_service_id=service_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "connection", "utilization_pct": 100.0},
    )
    res_pct = await injector.inject(f_pct)
    assert res_pct.accepted is True
    assert canonical_state.get_service_state(service_id).connection_pool_used == 100
    await injector.recover(f_pct.fault_id)
    assert canonical_state.get_service_state(service_id).connection_pool_used == 5


@pytest.mark.asyncio
async def test_reset_with_multiple_targets(
    injector: ConcreteFaultInjector, canonical_state: SimulationState
) -> None:
    id_order = canonical_service_id("order-service")
    id_payment = canonical_service_id("payment-service")

    f1 = FaultSpec(
        target_service_id=id_order,
        fault_type=FaultType.LATENCY,
        parameters={"latency_add_ms": 200.0},
    )
    f2 = FaultSpec(
        target_service_id=id_payment,
        fault_type=FaultType.ERROR,
        parameters={"error_rate": 0.5},
    )

    await injector.inject(f1)
    await injector.inject(f2)

    assert len(injector.active_faults()) == 2
    assert canonical_state.get_service_state(id_order).effective_latency_ms == 200.0
    assert canonical_state.get_service_state(id_payment).error_rate == 0.5

    # Reset
    injector.reset()

    assert len(injector.active_faults()) == 0
    assert canonical_state.get_service_state(id_order).effective_latency_ms == 0.0
    assert canonical_state.get_service_state(id_payment).error_rate == 0.0

    # Second reset safe and idempotent
    injector.reset()
    assert len(injector.active_faults()) == 0


@pytest.mark.asyncio
async def test_injector_and_state_isolation() -> None:
    topo1 = build_canonical_topology()
    topo2 = build_canonical_topology()

    state1 = SimulationState(topo1)
    state2 = SimulationState(topo2)

    inj1 = ConcreteFaultInjector(state1)
    inj2 = ConcreteFaultInjector(state2)

    service_id = canonical_service_id("api-gateway")
    spec = FaultSpec(target_service_id=service_id, fault_type=FaultType.CRASH)

    await inj1.inject(spec)

    assert inj1.is_active(spec.fault_id) is True
    assert inj2.is_active(spec.fault_id) is False
    assert state1.get_service_state(service_id).is_crashed is True
    assert state2.get_service_state(service_id).is_crashed is False

    inj1.reset()
    assert state1.get_service_state(service_id).is_crashed is False


@pytest.mark.asyncio
async def test_deterministic_replay() -> None:
    # Two identical runs
    topo1 = build_canonical_topology()
    topo2 = build_canonical_topology()
    state1 = SimulationState(topo1)
    state2 = SimulationState(topo2)
    inj1 = ConcreteFaultInjector(state1)
    inj2 = ConcreteFaultInjector(state2)

    target_id = canonical_service_id("database")
    fault_id = uuid.uuid5(uuid.NAMESPACE_DNS, "fault:test:db:cpu")
    spec = FaultSpec(
        fault_id=fault_id,
        target_service_id=target_id,
        fault_type=FaultType.RESOURCE,
        parameters={"resource_kind": "cpu", "utilization_pct": 99.0},
    )

    res1 = await inj1.inject(spec)
    res2 = await inj2.inject(spec)

    assert res1.accepted is True
    assert res2.accepted is True
    assert res1.fault_id == res2.fault_id == fault_id
    assert state1.get_service_state(target_id).model_dump() == state2.get_service_state(target_id).model_dump()

    rec1 = await inj1.recover(fault_id)
    rec2 = await inj2.recover(fault_id)

    assert rec1.accepted is True
    assert rec2.accepted is True
    assert state1.get_service_state(target_id).model_dump() == state2.get_service_state(target_id).model_dump()


def test_package_exports() -> None:
    import app.simulator.faults as faults_pkg
    import app.simulator.runtime as runtime_pkg

    assert sorted(faults_pkg.__all__) == sorted(["ActiveFault", "ConcreteFaultInjector"])
    assert sorted(runtime_pkg.__all__) == sorted(["ServiceRuntimeState", "SimulationState"])
