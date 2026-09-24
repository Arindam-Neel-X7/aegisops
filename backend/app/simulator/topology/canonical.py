import uuid

from app.simulator.interfaces import (
    DependencyType,
    ServiceDependency,
    ServiceNode,
    ServiceTopology,
    ServiceType,
)

ROOT_NAMESPACE: uuid.UUID = uuid.NAMESPACE_DNS
AEGISOPS_NAMESPACE: uuid.UUID = uuid.uuid5(ROOT_NAMESPACE, "aegisops")

CANONICAL_TOPOLOGY_VERSION: str = "1.0.0"

CANONICAL_SERVICE_NAMES: tuple[str, ...] = (
    "client",
    "api-gateway",
    "order-service",
    "payment-service",
    "inventory-service",
    "notification-service",
    "database",
)


def canonical_service_id(name: str) -> uuid.UUID:
    if name not in CANONICAL_SERVICE_NAMES:
        raise ValueError(f"Unknown canonical service name: {name!r}")
    return uuid.uuid5(AEGISOPS_NAMESPACE, f"service:{name}")


def canonical_topology_id() -> uuid.UUID:
    return uuid.uuid5(AEGISOPS_NAMESPACE, f"topology:canonical:v{CANONICAL_TOPOLOGY_VERSION}")


def build_canonical_topology() -> ServiceTopology:
    services = [
        ServiceNode(
            service_id=canonical_service_id("client"),
            name="client",
            service_type=ServiceType.EXTERNAL,
            metadata={"role": "traffic-origin"},
        ),
        ServiceNode(
            service_id=canonical_service_id("api-gateway"),
            name="api-gateway",
            service_type=ServiceType.API,
            metadata={"role": "edge-gateway"},
        ),
        ServiceNode(
            service_id=canonical_service_id("order-service"),
            name="order-service",
            service_type=ServiceType.API,
            metadata={"role": "order-coordinator"},
        ),
        ServiceNode(
            service_id=canonical_service_id("payment-service"),
            name="payment-service",
            service_type=ServiceType.API,
            metadata={"role": "payment-processor"},
        ),
        ServiceNode(
            service_id=canonical_service_id("inventory-service"),
            name="inventory-service",
            service_type=ServiceType.API,
            metadata={"role": "inventory-manager"},
        ),
        ServiceNode(
            service_id=canonical_service_id("notification-service"),
            name="notification-service",
            service_type=ServiceType.WORKER,
            metadata={"role": "event-worker"},
        ),
        ServiceNode(
            service_id=canonical_service_id("database"),
            name="database",
            service_type=ServiceType.DATABASE,
            metadata={"role": "relational-datastore"},
        ),
    ]

    dependencies = [
        ServiceDependency(
            upstream_service_id=canonical_service_id("client"),
            downstream_service_id=canonical_service_id("api-gateway"),
            dependency_type=DependencyType.SYNC,
            metadata={"protocol": "http"},
        ),
        ServiceDependency(
            upstream_service_id=canonical_service_id("api-gateway"),
            downstream_service_id=canonical_service_id("order-service"),
            dependency_type=DependencyType.SYNC,
            metadata={"protocol": "http"},
        ),
        ServiceDependency(
            upstream_service_id=canonical_service_id("order-service"),
            downstream_service_id=canonical_service_id("payment-service"),
            dependency_type=DependencyType.SYNC,
            metadata={"protocol": "http"},
        ),
        ServiceDependency(
            upstream_service_id=canonical_service_id("payment-service"),
            downstream_service_id=canonical_service_id("database"),
            dependency_type=DependencyType.DATA,
            metadata={"protocol": "sql"},
        ),
        ServiceDependency(
            upstream_service_id=canonical_service_id("order-service"),
            downstream_service_id=canonical_service_id("inventory-service"),
            dependency_type=DependencyType.SYNC,
            metadata={"protocol": "http"},
        ),
        ServiceDependency(
            upstream_service_id=canonical_service_id("inventory-service"),
            downstream_service_id=canonical_service_id("database"),
            dependency_type=DependencyType.DATA,
            metadata={"protocol": "sql"},
        ),
        ServiceDependency(
            upstream_service_id=canonical_service_id("order-service"),
            downstream_service_id=canonical_service_id("notification-service"),
            dependency_type=DependencyType.ASYNC,
            metadata={"protocol": "async-event"},
        ),
    ]

    return ServiceTopology(
        topology_id=canonical_topology_id(),
        services=services,
        dependencies=dependencies,
    )


def canonical_service_map(topology: ServiceTopology | None = None) -> dict[str, ServiceNode]:
    topo = topology if topology is not None else build_canonical_topology()
    return {node.name: node for node in topo.services}
