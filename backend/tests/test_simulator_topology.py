import uuid
import pytest

from app.simulator.interfaces import (
    DependencyType,
    ServiceNode,
    ServiceTopology,
    ServiceType,
)
from app.simulator.topology import (
    AEGISOPS_NAMESPACE,
    CANONICAL_SERVICE_NAMES,
    CANONICAL_TOPOLOGY_VERSION,
    ROOT_NAMESPACE,
    build_canonical_topology,
    canonical_service_id,
    canonical_service_map,
    canonical_topology_id,
)


def test_canonical_constants() -> None:
    assert CANONICAL_TOPOLOGY_VERSION == "1.0.0"
    assert len(CANONICAL_SERVICE_NAMES) == 7
    assert CANONICAL_SERVICE_NAMES == (
        "client",
        "api-gateway",
        "order-service",
        "payment-service",
        "inventory-service",
        "notification-service",
        "database",
    )
    assert isinstance(ROOT_NAMESPACE, uuid.UUID)
    assert isinstance(AEGISOPS_NAMESPACE, uuid.UUID)
    assert AEGISOPS_NAMESPACE == uuid.uuid5(ROOT_NAMESPACE, "aegisops")


def test_canonical_service_id_deterministic() -> None:
    for name in CANONICAL_SERVICE_NAMES:
        id_1 = canonical_service_id(name)
        id_2 = canonical_service_id(name)
        assert isinstance(id_1, uuid.UUID)
        assert id_1 == id_2
        assert id_1 == uuid.uuid5(AEGISOPS_NAMESPACE, f"service:{name}")


def test_canonical_service_id_rejects_unknown_name() -> None:
    with pytest.raises(ValueError, match="Unknown canonical service name: 'redis'"):
        canonical_service_id("redis")


def test_canonical_topology_id_deterministic() -> None:
    top_id_1 = canonical_topology_id()
    top_id_2 = canonical_topology_id()
    assert isinstance(top_id_1, uuid.UUID)
    assert top_id_1 == top_id_2
    assert top_id_1 == uuid.uuid5(AEGISOPS_NAMESPACE, f"topology:canonical:v{CANONICAL_TOPOLOGY_VERSION}")


def test_canonical_topology_service_count_and_names() -> None:
    topology = build_canonical_topology()
    assert len(topology.services) == 7
    service_names = [s.name for s in topology.services]
    assert tuple(service_names) == CANONICAL_SERVICE_NAMES


def test_canonical_topology_service_types() -> None:
    topology = build_canonical_topology()
    service_type_map = {s.name: s.service_type for s in topology.services}
    assert service_type_map["client"] == ServiceType.EXTERNAL
    assert service_type_map["api-gateway"] == ServiceType.API
    assert service_type_map["order-service"] == ServiceType.API
    assert service_type_map["payment-service"] == ServiceType.API
    assert service_type_map["inventory-service"] == ServiceType.API
    assert service_type_map["notification-service"] == ServiceType.WORKER
    assert service_type_map["database"] == ServiceType.DATABASE


def test_canonical_topology_static_metadata_is_clean() -> None:
    topology = build_canonical_topology()
    runtime_state_keys = {"cpu", "memory", "latency", "error_rate", "health", "active_faults", "counters"}
    for service in topology.services:
        assert isinstance(service.metadata, dict)
        assert "role" in service.metadata
        assert runtime_state_keys.isdisjoint(service.metadata.keys())


def test_canonical_topology_dependency_count_and_edges() -> None:
    topology = build_canonical_topology()
    assert len(topology.dependencies) == 7

    id_to_name = {s.service_id: s.name for s in topology.services}
    edges = [
        (id_to_name[dep.upstream_service_id], id_to_name[dep.downstream_service_id], dep.dependency_type)
        for dep in topology.dependencies
    ]

    expected_edges = [
        ("client", "api-gateway", DependencyType.SYNC),
        ("api-gateway", "order-service", DependencyType.SYNC),
        ("order-service", "payment-service", DependencyType.SYNC),
        ("payment-service", "database", DependencyType.DATA),
        ("order-service", "inventory-service", DependencyType.SYNC),
        ("inventory-service", "database", DependencyType.DATA),
        ("order-service", "notification-service", DependencyType.ASYNC),
    ]
    assert edges == expected_edges


def test_canonical_topology_deterministic_reconstruction() -> None:
    topo1 = build_canonical_topology()
    topo2 = build_canonical_topology()

    assert topo1.topology_id == topo2.topology_id
    assert [s.service_id for s in topo1.services] == [s.service_id for s in topo2.services]
    assert [s.name for s in topo1.services] == [s.name for s in topo2.services]
    assert [s.service_type for s in topo1.services] == [s.service_type for s in topo2.services]
    assert [s.metadata for s in topo1.services] == [s.metadata for s in topo2.services]

    deps1 = [(d.upstream_service_id, d.downstream_service_id, d.dependency_type, d.metadata) for d in topo1.dependencies]
    deps2 = [(d.upstream_service_id, d.downstream_service_id, d.dependency_type, d.metadata) for d in topo2.dependencies]
    assert deps1 == deps2


def test_canonical_topology_serialization_and_reconstruction() -> None:
    topology = build_canonical_topology()
    data = topology.model_dump()
    reconstructed = ServiceTopology.model_validate(data)

    assert reconstructed.topology_id == topology.topology_id
    assert len(reconstructed.services) == 7
    assert len(reconstructed.dependencies) == 7
    assert reconstructed.model_dump_json() == topology.model_dump_json()


def test_canonical_topology_mutation_isolation() -> None:
    topo1 = build_canonical_topology()
    topo2 = build_canonical_topology()

    assert topo1 is not topo2
    assert topo1.services is not topo2.services
    assert topo1.dependencies is not topo2.dependencies

    # Mutate topo1 in memory
    topo1.services.append(
        ServiceNode(
            service_id=uuid.uuid4(),
            name="ephemeral-node",
            service_type=ServiceType.GENERIC,
        )
    )
    assert len(topo1.services) == 8
    assert len(topo2.services) == 7

    # Fresh build remains unaffected
    topo3 = build_canonical_topology()
    assert len(topo3.services) == 7


def test_canonical_service_map_helper() -> None:
    s_map = canonical_service_map()
    assert len(s_map) == 7
    assert set(s_map.keys()) == set(CANONICAL_SERVICE_NAMES)
    for name, node in s_map.items():
        assert node.name == name
        assert node.service_id == canonical_service_id(name)

    # Pass explicit topology instance
    topo = build_canonical_topology()
    s_map_custom = canonical_service_map(topo)
    assert len(s_map_custom) == 7
    assert s_map_custom["order-service"].service_id == canonical_service_id("order-service")


def test_topology_package_exports() -> None:
    import app.simulator.topology as topo_pkg

    expected = [
        "AEGISOPS_NAMESPACE",
        "CANONICAL_SERVICE_NAMES",
        "CANONICAL_TOPOLOGY_VERSION",
        "ROOT_NAMESPACE",
        "build_canonical_topology",
        "canonical_service_id",
        "canonical_service_map",
        "canonical_topology_id",
    ]
    assert hasattr(topo_pkg, "__all__")
    assert sorted(topo_pkg.__all__) == sorted(expected)
    for symbol in expected:
        assert hasattr(topo_pkg, symbol)
