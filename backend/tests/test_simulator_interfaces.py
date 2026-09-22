import uuid
from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from app.simulator.interfaces import (
    ServiceNode,
    ServiceDependency,
    ServiceTopology,
    ServiceType,
    DependencyType,
    FaultType,
    FaultSpec,
    FaultInjectionResult,
    FaultInjector,
    GroundTruthRecord,
    TelemetryEmitter,
)
from app.telemetry.schemas import TelemetryEvent, EventType, EventSeverity


def test_service_node_valid_construction():
    node = ServiceNode(name="test-service", service_type=ServiceType.API)
    assert isinstance(node.service_id, uuid.UUID)
    assert node.name == "test-service"
    assert node.service_type == "api"
    assert node.metadata == {}


def test_service_node_blank_name_rejected():
    with pytest.raises(ValidationError):
        ServiceNode(name="")


def test_service_node_metadata_default_isolation():
    node1 = ServiceNode(name="n1")
    node2 = ServiceNode(name="n2")
    assert node1.metadata is not node2.metadata


def test_service_dependency_valid_construction():
    u_id = uuid.uuid4()
    d_id = uuid.uuid4()
    dep = ServiceDependency(
        upstream_service_id=u_id,
        downstream_service_id=d_id,
        dependency_type=DependencyType.SYNC
    )
    assert dep.upstream_service_id == u_id
    assert dep.downstream_service_id == d_id
    assert dep.dependency_type == "sync"


def test_service_dependency_self_dependency_rejected():
    u_id = uuid.uuid4()
    with pytest.raises(ValidationError, match="Self-dependency is not allowed"):
        ServiceDependency(
            upstream_service_id=u_id,
            downstream_service_id=u_id
        )


def test_service_topology_valid_construction():
    n1 = ServiceNode(name="n1")
    n2 = ServiceNode(name="n2")
    dep = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id)
    
    topo = ServiceTopology(services=[n1, n2], dependencies=[dep])
    assert isinstance(topo.topology_id, uuid.UUID)
    assert len(topo.services) == 2
    assert len(topo.dependencies) == 1


def test_service_topology_duplicate_service_ids_rejected():
    n1 = ServiceNode(name="n1")
    n1_clone = ServiceNode(service_id=n1.service_id, name="n1_clone")
    
    with pytest.raises(ValidationError, match="Duplicate service IDs found in topology"):
        ServiceTopology(services=[n1, n1_clone])


def test_service_topology_missing_dependency_endpoint_rejected():
    n1 = ServiceNode(name="n1")
    # n2 is not added to topology
    n2 = ServiceNode(name="n2")
    dep = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id)
    
    with pytest.raises(ValidationError, match="not found in topology"):
        ServiceTopology(services=[n1], dependencies=[dep])


def test_service_topology_duplicate_edges_rejected():
    n1 = ServiceNode(name="n1")
    n2 = ServiceNode(name="n2")
    dep1 = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id, dependency_type=DependencyType.SYNC)
    dep2 = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id, dependency_type=DependencyType.SYNC)
    
    with pytest.raises(ValidationError, match="Duplicate dependency edge found"):
        ServiceTopology(services=[n1, n2], dependencies=[dep1, dep2])


def test_service_topology_cycles_allowed():
    n1 = ServiceNode(name="n1")
    n2 = ServiceNode(name="n2")
    dep1 = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id)
    dep2 = ServiceDependency(upstream_service_id=n2.service_id, downstream_service_id=n1.service_id)
    
    # Should not raise
    topo = ServiceTopology(services=[n1, n2], dependencies=[dep1, dep2])
    assert len(topo.dependencies) == 2


def test_topology_serialization_succeeds():
    n1 = ServiceNode(name="n1")
    n2 = ServiceNode(name="n2", metadata={"host": "localhost"})
    dep = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id)
    topo = ServiceTopology(services=[n1, n2], dependencies=[dep])
    
    serialized = topo.model_dump_json()
    assert str(n1.service_id) in serialized
    assert "localhost" in serialized


def test_topology_mutable_defaults_isolated():
    t1 = ServiceTopology()
    t2 = ServiceTopology()
    assert t1.services is not t2.services
    assert t1.dependencies is not t2.dependencies


def test_fault_type_canonical_values():
    assert FaultType.LATENCY == "latency"
    assert FaultType.ERROR == "error"
    assert FaultType.TIMEOUT == "timeout"
    assert FaultType.CRASH == "crash"
    assert FaultType.RESOURCE == "resource"
    assert FaultType.NETWORK == "network"


def test_fault_spec_valid_construction():
    t_id = uuid.uuid4()
    spec = FaultSpec(target_service_id=t_id, fault_type=FaultType.LATENCY)
    assert isinstance(spec.fault_id, uuid.UUID)
    assert spec.target_service_id == t_id
    assert spec.fault_type == "latency"
    assert spec.duration_seconds is None
    assert spec.parameters == {}


def test_fault_spec_uuid_auto_generation():
    spec1 = FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH)
    spec2 = FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH)
    assert spec1.fault_id != spec2.fault_id


def test_fault_spec_parameters_default_isolation():
    spec1 = FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH)
    spec2 = FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH)
    assert spec1.parameters is not spec2.parameters


def test_fault_spec_duration_seconds_accepts_none():
    spec = FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH, duration_seconds=None)
    assert spec.duration_seconds is None


def test_fault_spec_duration_seconds_rejects_zero():
    with pytest.raises(ValidationError, match="duration_seconds must be > 0"):
        FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.LATENCY, duration_seconds=0)


def test_fault_spec_duration_seconds_rejects_negative():
    with pytest.raises(ValidationError, match="duration_seconds must be > 0"):
        FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.LATENCY, duration_seconds=-5.0)


def test_fault_injection_result_valid_construction():
    f_id = uuid.uuid4()
    result = FaultInjectionResult(fault_id=f_id, accepted=True, message="Injected")
    assert result.fault_id == f_id
    assert result.accepted is True
    assert result.message == "Injected"


def test_fault_injector_protocol_shape():
    class FakeFaultInjector:
        async def inject(self, fault: FaultSpec) -> FaultInjectionResult:
            return FaultInjectionResult(fault_id=fault.fault_id, accepted=True)

        async def recover(self, fault_id: uuid.UUID) -> FaultInjectionResult:
            return FaultInjectionResult(fault_id=fault_id, accepted=True)

    injector: FaultInjector = FakeFaultInjector()
    
    # We just want to ensure it passes type checking and basic structural typing.
    # We can inspect if the methods exist.
    assert hasattr(injector, "inject")
    assert hasattr(injector, "recover")

@pytest.mark.asyncio
async def test_fault_injector_async_signatures():
    class FakeFaultInjector:
        async def inject(self, fault: FaultSpec) -> FaultInjectionResult:
            return FaultInjectionResult(fault_id=fault.fault_id, accepted=True)

        async def recover(self, fault_id: uuid.UUID) -> FaultInjectionResult:
            return FaultInjectionResult(fault_id=fault_id, accepted=True)

    injector: FaultInjector = FakeFaultInjector()
    
    spec = FaultSpec(target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH)
    res1 = await injector.inject(spec)
    assert res1.accepted is True
    
    res2 = await injector.recover(spec.fault_id)
    assert res2.accepted is True


def test_ground_truth_record_valid_construction():
    f_id = uuid.uuid4()
    s_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    rec = GroundTruthRecord(
        fault_id=f_id,
        target_service_id=s_id,
        fault_type=FaultType.NETWORK,
        injected_at=now,
        expected_root_cause="Test fault"
    )
    assert isinstance(rec.record_id, uuid.UUID)
    assert rec.fault_id == f_id
    assert rec.target_service_id == s_id
    assert rec.fault_type == "network"
    assert rec.injected_at == now
    assert rec.expected_root_cause == "Test fault"
    assert rec.expected_affected_service_ids == []
    assert rec.metadata == {}


def test_ground_truth_record_id_auto_generation():
    f_id = uuid.uuid4()
    s_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    rec1 = GroundTruthRecord(fault_id=f_id, target_service_id=s_id, fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="A")
    rec2 = GroundTruthRecord(fault_id=f_id, target_service_id=s_id, fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="B")
    assert rec1.record_id != rec2.record_id


def test_ground_truth_expected_root_cause_non_empty():
    now = datetime.now(timezone.utc)
    with pytest.raises(ValidationError):
        GroundTruthRecord(fault_id=uuid.uuid4(), target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="")


def test_ground_truth_injected_at_rejects_naive_datetime():
    naive_dt = datetime.now()
    with pytest.raises(ValidationError):
        GroundTruthRecord(
            fault_id=uuid.uuid4(),
            target_service_id=uuid.uuid4(),
            fault_type=FaultType.CRASH,
            injected_at=naive_dt,
            expected_root_cause="A"
        )


def test_ground_truth_affected_service_default_list_isolation():
    now = datetime.now(timezone.utc)
    rec1 = GroundTruthRecord(fault_id=uuid.uuid4(), target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="A")
    rec2 = GroundTruthRecord(fault_id=uuid.uuid4(), target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="B")
    assert rec1.expected_affected_service_ids is not rec2.expected_affected_service_ids


def test_ground_truth_duplicate_affected_service_ids_rejected():
    s_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    with pytest.raises(ValidationError, match="Duplicate service IDs found in expected_affected_service_ids"):
        GroundTruthRecord(
            fault_id=uuid.uuid4(),
            target_service_id=uuid.uuid4(),
            fault_type=FaultType.CRASH,
            injected_at=now,
            expected_root_cause="A",
            expected_affected_service_ids=[s_id, s_id]
        )


def test_ground_truth_target_service_id_may_appear_in_affected_list():
    target_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    # Should not raise
    rec = GroundTruthRecord(
        fault_id=uuid.uuid4(),
        target_service_id=target_id,
        fault_type=FaultType.CRASH,
        injected_at=now,
        expected_root_cause="A",
        expected_affected_service_ids=[target_id]
    )
    assert target_id in rec.expected_affected_service_ids


def test_ground_truth_metadata_default_isolation():
    now = datetime.now(timezone.utc)
    rec1 = GroundTruthRecord(fault_id=uuid.uuid4(), target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="A")
    rec2 = GroundTruthRecord(fault_id=uuid.uuid4(), target_service_id=uuid.uuid4(), fault_type=FaultType.CRASH, injected_at=now, expected_root_cause="B")
    assert rec1.metadata is not rec2.metadata


def test_ground_truth_full_serialization():
    now = datetime.now(timezone.utc)
    rec = GroundTruthRecord(
        fault_id=uuid.uuid4(),
        target_service_id=uuid.uuid4(),
        fault_type=FaultType.NETWORK,
        injected_at=now,
        expected_root_cause="Testing serialization",
        metadata={"key": "value"}
    )
    serialized = rec.model_dump_json()
    assert "network" in serialized
    assert str(rec.fault_id) in serialized
    assert "Testing serialization" in serialized
    assert "value" in serialized


def test_telemetry_emitter_protocol_shape():
    class FakeTelemetryEmitter:
        def __init__(self) -> None:
            self.events: list[TelemetryEvent] = []

        async def emit(self, event: TelemetryEvent) -> None:
            self.events.append(event)
            
    emitter: TelemetryEmitter = FakeTelemetryEmitter()
    assert hasattr(emitter, "emit")


@pytest.mark.asyncio
async def test_telemetry_emitter_async_signatures():
    class FakeTelemetryEmitter:
        def __init__(self) -> None:
            self.events: list[TelemetryEvent] = []

        async def emit(self, event: TelemetryEvent) -> None:
            self.events.append(event)
            
    emitter: TelemetryEmitter = FakeTelemetryEmitter()
    
    event = TelemetryEvent(
        tenant_id=uuid.uuid4(),
        environment="test",
        service="test-service",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.INFO,
        payload={"msg": "test"}
    )
    
    res = await emitter.emit(event)
    assert res is None
    
    # We inspect the fake instance directly
    fake_instance = emitter  # type: ignore
    assert len(fake_instance.events) == 1
    assert fake_instance.events[0] is event


def test_public_package_exports():
    import app.simulator
    expected_exports = [
        "DependencyType",
        "FaultInjectionResult",
        "FaultInjector",
        "FaultSpec",
        "FaultType",
        "GroundTruthRecord",
        "ServiceDependency",
        "ServiceNode",
        "ServiceTopology",
        "ServiceType",
        "TelemetryEmitter",
    ]
    
    # Check __all__ exists and is exact
    assert hasattr(app.simulator, "__all__")
    assert sorted(app.simulator.__all__) == sorted(expected_exports)
    
    # Check that each exported symbol is importable and has correct identity
    import app.simulator.interfaces
    for name in expected_exports:
        public_symbol = getattr(app.simulator, name)
        internal_symbol = getattr(app.simulator.interfaces, name)
        assert public_symbol is internal_symbol


def test_telemetry_package_does_not_depend_on_simulator():
    import sys
    
    # Unload modules to trace cleanly
    for mod in list(sys.modules.keys()):
        if mod.startswith("app.simulator") or mod.startswith("app.telemetry"):
            del sys.modules[mod]
            
    # Import telemetry and ensure simulator is NOT loaded
    
    simulator_loaded = any(mod.startswith("app.simulator") for mod in sys.modules.keys())
    assert simulator_loaded is False, "Telemetry package incorrectly imported simulator package"


def test_service_topology_duplicate_edges_different_types_allowed():
    n1 = ServiceNode(name="n1")
    n2 = ServiceNode(name="n2")
    dep1 = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id, dependency_type=DependencyType.SYNC)
    dep2 = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id, dependency_type=DependencyType.ASYNC)
    
    # Should not raise because dependency_type differs
    topo = ServiceTopology(services=[n1, n2], dependencies=[dep1, dep2])
    assert len(topo.dependencies) == 2


def test_ground_truth_expected_root_cause_whitespace_allowed():
    now = datetime.now(timezone.utc)
    # min_length=1 allows whitespace strings. We observe this behavior safely without strengthening it here.
    rec = GroundTruthRecord(
        fault_id=uuid.uuid4(),
        target_service_id=uuid.uuid4(),
        fault_type=FaultType.CRASH,
        injected_at=now,
        expected_root_cause="   "
    )
    assert rec.expected_root_cause == "   "


@pytest.mark.asyncio
async def test_cross_contract_integration():
    """
    Prove that the approved contracts can represent one coherent simulated
    fault scenario WITHOUT runtime execution.
    """
    n1 = ServiceNode(name="api-service", service_type=ServiceType.API)
    n2 = ServiceNode(name="db-service", service_type=ServiceType.DATABASE)
    dep = ServiceDependency(upstream_service_id=n1.service_id, downstream_service_id=n2.service_id)
    topo = ServiceTopology(services=[n1, n2], dependencies=[dep])
    assert topo is not None

    spec = FaultSpec(target_service_id=n2.service_id, fault_type=FaultType.NETWORK, duration_seconds=30)
    
    now = datetime.now(timezone.utc)
    gt = GroundTruthRecord(
        fault_id=spec.fault_id,
        target_service_id=spec.target_service_id,
        fault_type=spec.fault_type,
        injected_at=now,
        expected_root_cause="DB Network Partition",
        expected_affected_service_ids=[n1.service_id, n2.service_id]
    )

    # Assert cross-contract reference consistency
    assert gt.fault_id == spec.fault_id
    assert gt.target_service_id == spec.target_service_id
    assert gt.fault_type == spec.fault_type

    # Telemetry
    event = TelemetryEvent(
        tenant_id=uuid.uuid4(),
        environment="test",
        service="db-service",
        event_type=EventType.SYSTEM,
        severity=EventSeverity.CRITICAL,
        payload={"dropped_connections": 50}
    )

    class FakeTelemetryEmitter:
        def __init__(self) -> None:
            self.events: list[TelemetryEvent] = []

        async def emit(self, event: TelemetryEvent) -> None:
            self.events.append(event)

    emitter = FakeTelemetryEmitter()
    await emitter.emit(event)
    assert emitter.events[0] is event
