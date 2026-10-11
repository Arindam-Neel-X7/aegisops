from __future__ import annotations

import uuid
from typing import Any

import pytest
from neo4j.exceptions import ServiceUnavailable
from pydantic import ValidationError

from app.correlation.errors import (
    TopologyContractError,
    TopologyError,
    TopologyNodeNotFoundError,
    TopologyProviderError,
    TopologyQueryLimitError,
    TopologyVersionMismatchError,
)
from app.correlation.topology import (
    MAX_NEIGHBORHOOD_EDGES,
    MAX_NEIGHBORHOOD_NODES,
    MAX_NEIGHBORHOOD_PATHS,
    MAX_PATH_RESULTS,
    MAX_TOPOLOGY_EDGES,
    MAX_TOPOLOGY_NODES,
    SUPPORTED_TOPOLOGY_SCHEMA_VERSION,
    TOPOLOGY_FINGERPRINT_LEN,
    TOPOLOGY_MAX_HOPS,
    BoundedTopologyNeighborhood,
    DirectedTopologyEdge,
    TopologyPath,
    TopologyProvider,
    TopologyServiceNode,
    TopologySnapshot,
    TopologyVersion,
    TraversalDirection,
    build_neighborhood,
    check_expected_version,
    compute_snapshot_fingerprint,
    find_snapshot_paths,
    serialize_topology_snapshot,
    validate_max_hops,
)
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.correlation.topology_neo4j import Neo4jTopologyProvider
from app.simulator.interfaces import (
    DependencyType,
    ServiceDependency,
    ServiceNode,
    ServiceTopology,
    ServiceType,
)
from app.simulator.topology.canonical import (
    CANONICAL_TOPOLOGY_VERSION,
    build_canonical_topology,
    canonical_service_id,
    canonical_topology_id,
)

_TOPOLOGY_ID = canonical_topology_id()
_FP_ZERO = "0" * TOPOLOGY_FINGERPRINT_LEN


def _basic_version(version: str = "1.0.0") -> TopologyVersion:
    return TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version=version)


def _node(name: str = "svc") -> TopologyServiceNode:
    return TopologyServiceNode(
        service_id=uuid.uuid4(), name=name, service_type=ServiceType.API
    )


def _canonical_snapshot() -> TopologySnapshot:
    return _run(CanonicalTopologyProvider().get_snapshot())  # type: ignore[no-any-return]


def _run(coro: Any) -> Any:
    import asyncio

    return asyncio.run(coro)  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# 1. Strict validation and JSON round trips for every contract
# ---------------------------------------------------------------------------
def test_json_round_trip_every_contract() -> None:
    node = _node("client")
    edge = DirectedTopologyEdge(
        upstream_service_id=node.service_id,
        downstream_service_id=uuid.uuid4(),
        dependency_type=DependencyType.SYNC,
    )
    version = _basic_version()
    snapshot = TopologySnapshot.create(version=version, nodes=[node], edges=[])
    path = TopologyPath(
        direction=TraversalDirection.DOWNSTREAM,
        service_ids=(node.service_id, edge.downstream_service_id),
        edges=(edge,),
        hop_count=1,
    )
    neighborhood = BoundedTopologyNeighborhood(
        snapshot_fingerprint=snapshot.snapshot_fingerprint,
        root_service_id=node.service_id,
        direction=TraversalDirection.DOWNSTREAM,
        max_hops=1,
        nodes=(node,),
        edges=(),
        paths=(path,),
        truncated=False,
    )
    for model in (node, edge, version, snapshot, path, neighborhood):
        restored = type(model).model_validate_json(model.model_dump_json())
        assert restored == model


def test_frozen_and_unknown_field_rejection() -> None:
    node = _node()
    with pytest.raises(ValidationError):
        node.name = "other"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        TopologyServiceNode.model_validate(
            {
                "service_id": uuid.uuid4(),
                "name": "svc",
                "service_type": ServiceType.API,
                "unexpected": "x",
            }
        )
    with pytest.raises(ValidationError):
        TopologyVersion.model_validate(
            {
                "topology_id": _TOPOLOGY_ID,
                "topology_version": "1.0.0",
                "extra": True,
            }
        )
    edge = DirectedTopologyEdge(
        upstream_service_id=node.service_id,
        downstream_service_id=uuid.uuid4(),
        dependency_type=DependencyType.SYNC,
    )
    with pytest.raises(ValidationError):
        DirectedTopologyEdge.model_validate(
            {
                "upstream_service_id": edge.upstream_service_id,
                "downstream_service_id": edge.downstream_service_id,
                "dependency_type": DependencyType.SYNC,
                "provider": "neo4j",
            }
        )


def test_invalid_enum_string_uuid_and_bool_rejection() -> None:
    with pytest.raises(ValidationError):
        TopologyServiceNode.model_validate(
            {
                "service_id": uuid.uuid4(),
                "name": "svc",
                "service_type": "not-a-service-type",
            }
        )
    with pytest.raises(ValidationError):
        TopologyServiceNode.model_validate(
            {"service_id": "not-a-uuid", "name": "svc", "service_type": "api"}
        )
    with pytest.raises(ValidationError):
        TopologyVersion.model_validate(
            {"topology_id": _TOPOLOGY_ID, "topology_version": "   "}
        )
    with pytest.raises(ValidationError):
        DirectedTopologyEdge.model_validate(
            {
                "upstream_service_id": uuid.uuid4(),
                "downstream_service_id": uuid.uuid4(),
                "dependency_type": "sync",
            }
        )
    with pytest.raises(ValidationError):
        TopologyPath(
            direction=TraversalDirection.DOWNSTREAM,
            service_ids=(uuid.uuid4(),),
            edges=(),
            hop_count=True,
        )


def test_snapshot_oversized_collections_rejected() -> None:
    node = _node()
    with pytest.raises(TopologyContractError):
        TopologySnapshot.create(
            version=_basic_version(),
            nodes=[node] * (MAX_TOPOLOGY_NODES + 1),
            edges=[],
        )
    edge = DirectedTopologyEdge(
        upstream_service_id=node.service_id,
        downstream_service_id=uuid.uuid4(),
        dependency_type=DependencyType.SYNC,
    )
    with pytest.raises(TopologyContractError):
        TopologySnapshot.create(
            version=_basic_version(),
            nodes=[node],
            edges=[edge] * (MAX_TOPOLOGY_EDGES + 1),
        )


def test_caller_collection_mutation_isolation() -> None:
    node = _node("s1")
    nodes: list[TopologyServiceNode] = [node]
    snapshot = TopologySnapshot.create(version=_basic_version(), nodes=nodes, edges=[])
    nodes.append(_node("s2"))
    assert len(snapshot.nodes) == 1
    assert type(snapshot.nodes) is tuple


# ---------------------------------------------------------------------------
# 2. Deterministic snapshot bytes and fingerprint
# ---------------------------------------------------------------------------
def test_deterministic_snapshot_bytes_and_fingerprint() -> None:
    n1 = _node("a")
    n2 = _node("b")
    edge = DirectedTopologyEdge(
        upstream_service_id=n1.service_id,
        downstream_service_id=n2.service_id,
        dependency_type=DependencyType.SYNC,
    )
    first = TopologySnapshot.create(
        version=_basic_version(), nodes=[n1, n2], edges=[edge]
    )
    second = TopologySnapshot.create(
        version=_basic_version(), nodes=[n1, n2], edges=[edge]
    )
    assert first.snapshot_fingerprint == second.snapshot_fingerprint
    assert serialize_topology_snapshot(first) == serialize_topology_snapshot(second)
    assert len(first.snapshot_fingerprint) == TOPOLOGY_FINGERPRINT_LEN
    assert first.snapshot_fingerprint == first.snapshot_fingerprint.lower()


def test_input_order_permutation_invariance() -> None:
    n1 = _node("a")
    n2 = _node("b")
    e1 = DirectedTopologyEdge(
        upstream_service_id=n1.service_id,
        downstream_service_id=n2.service_id,
        dependency_type=DependencyType.SYNC,
    )
    e2 = DirectedTopologyEdge(
        upstream_service_id=n2.service_id,
        downstream_service_id=n1.service_id,
        dependency_type=DependencyType.DATA,
    )
    forward = TopologySnapshot.create(
        version=_basic_version(), nodes=[n1, n2], edges=[e1, e2]
    )
    reversed_ = TopologySnapshot.create(
        version=_basic_version(), nodes=[n2, n1], edges=[e2, e1]
    )
    assert forward == reversed_
    assert forward.snapshot_fingerprint == reversed_.snapshot_fingerprint
    assert serialize_topology_snapshot(forward) == serialize_topology_snapshot(
        reversed_
    )


def test_mismatched_supplied_fingerprint_rejected() -> None:
    node = _node()
    payload = {
        "schema_version": SUPPORTED_TOPOLOGY_SCHEMA_VERSION,
        "version": _basic_version().model_dump(),
        "nodes": [node.model_dump()],
        "edges": [],
        "snapshot_fingerprint": _FP_ZERO,
    }
    with pytest.raises(ValidationError):
        TopologySnapshot.model_validate(payload)


def test_fingerprint_mutation_sensitivity() -> None:
    n1 = _node("a")
    n2 = _node("b")
    edge = DirectedTopologyEdge(
        upstream_service_id=n1.service_id,
        downstream_service_id=n2.service_id,
        dependency_type=DependencyType.SYNC,
    )
    base = TopologySnapshot.create(
        version=_basic_version(), nodes=[n1, n2], edges=[edge]
    )
    baseline = base.snapshot_fingerprint

    renamed = TopologySnapshot.create(
        version=_basic_version(),
        nodes=[
            TopologyServiceNode(
                service_id=n1.service_id, name="renamed", service_type=n1.service_type
            ),
            n2,
        ],
        edges=[edge],
    )
    assert renamed.snapshot_fingerprint != baseline

    retyped = TopologySnapshot.create(
        version=_basic_version(),
        nodes=[
            TopologyServiceNode(
                service_id=n1.service_id, name="a", service_type=ServiceType.WORKER
            ),
            n2,
        ],
        edges=[edge],
    )
    assert retyped.snapshot_fingerprint != baseline

    reversioned = TopologySnapshot.create(
        version=_basic_version("2.0.0"), nodes=[n1, n2], edges=[edge]
    )
    assert reversioned.snapshot_fingerprint != baseline

    reid = TopologySnapshot.create(
        version=TopologyVersion(topology_id=uuid.uuid4(), topology_version="1.0.0"),
        nodes=[n1, n2],
        edges=[edge],
    )
    assert reid.snapshot_fingerprint != baseline

    redep = TopologySnapshot.create(
        version=_basic_version(),
        nodes=[n1, n2],
        edges=[
            DirectedTopologyEdge(
                upstream_service_id=n1.service_id,
                downstream_service_id=n2.service_id,
                dependency_type=DependencyType.ASYNC,
            )
        ],
    )
    assert redep.snapshot_fingerprint != baseline

    expected = compute_snapshot_fingerprint(
        base.schema_version, base.version, base.nodes, base.edges
    )
    assert expected == baseline


# ---------------------------------------------------------------------------
# 3. Canonical provider mapping and immutability
# ---------------------------------------------------------------------------
def test_canonical_provider_exact_mapping() -> None:
    snapshot = _canonical_snapshot()
    assert len(snapshot.nodes) == 7
    assert len(snapshot.edges) == 7
    assert snapshot.version.topology_id == canonical_topology_id()
    assert snapshot.version.topology_version == CANONICAL_TOPOLOGY_VERSION == "1.0.0"
    names = {n.name for n in snapshot.nodes}
    assert names == {
        "client",
        "api-gateway",
        "order-service",
        "payment-service",
        "inventory-service",
        "notification-service",
        "database",
    }
    by_name = {n.name: n for n in snapshot.nodes}
    assert by_name["api-gateway"].service_id == canonical_service_id("api-gateway")
    assert by_name["client"].service_type == ServiceType.EXTERNAL
    assert by_name["database"].service_type == ServiceType.DATABASE
    assert by_name["notification-service"].service_type == ServiceType.WORKER


def test_canonical_repeated_calls_equal_and_source_immutable() -> None:
    source = build_canonical_topology()
    before = source.model_dump()
    provider = CanonicalTopologyProvider(topology=source)
    first = _run(provider.get_snapshot())
    second = _run(provider.get_snapshot())
    assert first == second
    assert first.snapshot_fingerprint == second.snapshot_fingerprint
    assert serialize_topology_snapshot(first) == serialize_topology_snapshot(second)
    assert source.model_dump() == before
    _run(provider.close())


def test_canonical_provider_satisfies_protocol() -> None:
    provider = CanonicalTopologyProvider()
    assert isinstance(provider, TopologyProvider)


def test_shared_helpers_operate_on_snapshot_directly() -> None:
    n1 = _node("a")
    n2 = _node("b")
    edge = DirectedTopologyEdge(
        upstream_service_id=n1.service_id,
        downstream_service_id=n2.service_id,
        dependency_type=DependencyType.SYNC,
    )
    snapshot = TopologySnapshot.create(
        version=_basic_version(), nodes=[n1, n2], edges=[edge]
    )
    neighborhood = build_neighborhood(
        snapshot,
        root_service_id=n1.service_id,
        max_hops=1,
        direction=TraversalDirection.DOWNSTREAM,
    )
    assert {n.service_id for n in neighborhood.nodes} == {
        n1.service_id,
        n2.service_id,
    }
    paths = find_snapshot_paths(
        snapshot,
        source_service_id=n1.service_id,
        target_service_id=n2.service_id,
        max_hops=1,
        direction=TraversalDirection.DOWNSTREAM,
    )
    assert len(paths) == 1
    assert paths[0].service_ids == (n1.service_id, n2.service_id)


# ---------------------------------------------------------------------------
# 4. Traversal semantics
# ---------------------------------------------------------------------------
async def _build_chain_provider() -> (
    tuple[CanonicalTopologyProvider, dict[str, uuid.UUID]]
):
    ids = {
        "a": canonical_service_id("order-service"),
        "b": canonical_service_id("payment-service"),
        "c": canonical_service_id("database"),
    }
    topology = ServiceTopology(
        topology_id=canonical_topology_id(),
        services=[
            ServiceNode(
                service_id=ids["a"], name="order-service", service_type=ServiceType.API
            ),
            ServiceNode(
                service_id=ids["b"],
                name="payment-service",
                service_type=ServiceType.API,
            ),
            ServiceNode(
                service_id=ids["c"],
                name="database",
                service_type=ServiceType.DATABASE,
            ),
        ],
        dependencies=[
            ServiceDependency(
                upstream_service_id=ids["a"],
                downstream_service_id=ids["b"],
                dependency_type=DependencyType.SYNC,
            ),
            ServiceDependency(
                upstream_service_id=ids["b"],
                downstream_service_id=ids["c"],
                dependency_type=DependencyType.DATA,
            ),
        ],
    )
    return CanonicalTopologyProvider(topology=topology), ids


@pytest.mark.asyncio
async def test_downstream_upstream_both_traversal() -> None:
    provider, ids = await _build_chain_provider()
    downstream = await provider.get_neighborhood(
        service_id=ids["a"],
        max_hops=2,
        direction=TraversalDirection.DOWNSTREAM,
    )
    assert {n.name for n in downstream.nodes} == {
        "order-service",
        "payment-service",
        "database",
    }
    assert len(downstream.edges) == 2

    upstream = await provider.get_neighborhood(
        service_id=ids["c"],
        max_hops=2,
        direction=TraversalDirection.UPSTREAM,
    )
    assert {n.name for n in upstream.nodes} == {
        "order-service",
        "payment-service",
        "database",
    }

    both = await provider.get_neighborhood(
        service_id=ids["b"],
        max_hops=1,
        direction=TraversalDirection.BOTH,
    )
    assert {n.name for n in both.nodes} == {
        "order-service",
        "payment-service",
        "database",
    }


@pytest.mark.asyncio
async def test_max_hops_zero_one_and_bound() -> None:
    provider, ids = await _build_chain_provider()
    zero = await provider.get_neighborhood(
        service_id=ids["a"], max_hops=0, direction=TraversalDirection.DOWNSTREAM
    )
    assert {n.name for n in zero.nodes} == {"order-service"}
    assert zero.edges == ()
    assert zero.truncated is True

    one = await provider.get_neighborhood(
        service_id=ids["a"], max_hops=1, direction=TraversalDirection.DOWNSTREAM
    )
    assert {n.name for n in one.nodes} == {"order-service", "payment-service"}
    assert one.truncated is True

    full = await provider.get_neighborhood(
        service_id=ids["a"],
        max_hops=2,
        direction=TraversalDirection.DOWNSTREAM,
    )
    assert full.truncated is False
    assert len(full.paths) == 3


def test_hops_boundaries_and_bool_rejection() -> None:
    assert validate_max_hops(0) == 0
    assert validate_max_hops(TOPOLOGY_MAX_HOPS) == TOPOLOGY_MAX_HOPS
    for bad in (True, False, -1, TOPOLOGY_MAX_HOPS + 1, 1.5, "1", None):
        with pytest.raises(TopologyContractError):
            validate_max_hops(bad)


@pytest.mark.asyncio
async def test_cycle_termination_and_simple_paths() -> None:
    ids = [uuid.uuid4() for _ in range(3)]
    topology = ServiceTopology(
        topology_id=uuid.uuid4(),
        services=[
            ServiceNode(service_id=ids[i], name=f"n{i}", service_type=ServiceType.API)
            for i in range(3)
        ],
        dependencies=[
            ServiceDependency(
                upstream_service_id=ids[0],
                downstream_service_id=ids[1],
                dependency_type=DependencyType.SYNC,
            ),
            ServiceDependency(
                upstream_service_id=ids[1],
                downstream_service_id=ids[2],
                dependency_type=DependencyType.SYNC,
            ),
            ServiceDependency(
                upstream_service_id=ids[2],
                downstream_service_id=ids[0],
                dependency_type=DependencyType.SYNC,
            ),
        ],
    )
    provider = CanonicalTopologyProvider(topology=topology)
    neighborhood = await provider.get_neighborhood(
        service_id=ids[0],
        max_hops=TOPOLOGY_MAX_HOPS,
        direction=TraversalDirection.DOWNSTREAM,
    )
    assert len(neighborhood.nodes) == 3
    for path in neighborhood.paths:
        assert len(set(path.service_ids)) == len(path.service_ids)

    paths = await provider.find_paths(
        source_service_id=ids[0],
        target_service_id=ids[2],
        max_hops=TOPOLOGY_MAX_HOPS,
        direction=TraversalDirection.DOWNSTREAM,
    )
    simple = [p for p in paths if p.service_ids == (ids[0], ids[1], ids[2])]
    assert len(simple) == 1


@pytest.mark.asyncio
async def test_disconnected_service_behavior() -> None:
    ids = [uuid.uuid4() for _ in range(3)]
    topology = ServiceTopology(
        topology_id=uuid.uuid4(),
        services=[
            ServiceNode(service_id=ids[0], name="a", service_type=ServiceType.API),
            ServiceNode(service_id=ids[1], name="b", service_type=ServiceType.API),
            ServiceNode(service_id=ids[2], name="island", service_type=ServiceType.API),
        ],
        dependencies=[
            ServiceDependency(
                upstream_service_id=ids[0],
                downstream_service_id=ids[1],
                dependency_type=DependencyType.SYNC,
            ),
        ],
    )
    provider = CanonicalTopologyProvider(topology=topology)
    snapshot = await provider.get_snapshot()
    assert len(snapshot.nodes) == 3
    neighborhood = await provider.get_neighborhood(
        service_id=ids[0],
        max_hops=TOPOLOGY_MAX_HOPS,
        direction=TraversalDirection.DOWNSTREAM,
    )
    assert ids[2] not in {n.service_id for n in neighborhood.nodes}


# ---------------------------------------------------------------------------
# 5. Duplicate and missing-node semantics
# ---------------------------------------------------------------------------
def test_duplicate_node_and_edge_rejection() -> None:
    node = _node()
    with pytest.raises(TopologyContractError):
        TopologySnapshot.create(version=_basic_version(), nodes=[node, node], edges=[])
    edge = DirectedTopologyEdge(
        upstream_service_id=node.service_id,
        downstream_service_id=uuid.uuid4(),
        dependency_type=DependencyType.SYNC,
    )
    with pytest.raises(TopologyContractError):
        TopologySnapshot.create(
            version=_basic_version(), nodes=[node], edges=[edge, edge]
        )


@pytest.mark.asyncio
async def test_missing_root_source_target_typed_errors() -> None:
    provider = CanonicalTopologyProvider()
    missing = uuid.uuid4()
    with pytest.raises(TopologyNodeNotFoundError):
        await provider.get_neighborhood(
            service_id=missing,
            max_hops=1,
            direction=TraversalDirection.DOWNSTREAM,
        )
    with pytest.raises(TopologyNodeNotFoundError):
        await provider.find_paths(
            source_service_id=missing,
            target_service_id=canonical_service_id("database"),
            max_hops=2,
            direction=TraversalDirection.DOWNSTREAM,
        )
    with pytest.raises(TopologyNodeNotFoundError):
        await provider.find_paths(
            source_service_id=canonical_service_id("client"),
            target_service_id=missing,
            max_hops=2,
            direction=TraversalDirection.DOWNSTREAM,
        )


@pytest.mark.asyncio
async def test_expected_version_mismatch_typed_error() -> None:
    provider = CanonicalTopologyProvider()
    with pytest.raises(TopologyVersionMismatchError):
        await provider.get_snapshot(expected_version="2.0.0")
    with pytest.raises(TopologyVersionMismatchError):
        await provider.get_neighborhood(
            service_id=canonical_service_id("client"),
            max_hops=1,
            direction=TraversalDirection.DOWNSTREAM,
            expected_version="9.9.9",
        )
    with pytest.raises(TopologyVersionMismatchError):
        await provider.find_paths(
            source_service_id=canonical_service_id("client"),
            target_service_id=canonical_service_id("database"),
            max_hops=2,
            direction=TraversalDirection.DOWNSTREAM,
            expected_version="9.9.9",
        )
    ok = await provider.get_snapshot(expected_version="1.0.0")
    assert ok.version.topology_version == "1.0.0"


# ---------------------------------------------------------------------------
# 6. Bounds and limit errors
# ---------------------------------------------------------------------------
def test_result_and_path_limit_typed_errors() -> None:
    node = _node()
    too_many_nodes = [
        TopologyServiceNode(
            service_id=uuid.uuid4(), name=f"n{i}", service_type=ServiceType.API
        )
        for i in range(MAX_NEIGHBORHOOD_NODES + 1)
    ]
    with pytest.raises(ValidationError):
        BoundedTopologyNeighborhood(
            snapshot_fingerprint=_FP_ZERO,
            root_service_id=node.service_id,
            direction=TraversalDirection.DOWNSTREAM,
            max_hops=1,
            nodes=tuple(too_many_nodes),
            edges=(),
            paths=(),
            truncated=False,
        )
    with pytest.raises(ValidationError):
        TopologyPath(
            direction=TraversalDirection.DOWNSTREAM,
            service_ids=tuple(uuid.uuid4() for _ in range(TOPOLOGY_MAX_HOPS + 2)),
            edges=(),
            hop_count=1,
        )
    assert MAX_NEIGHBORHOOD_EDGES > 0
    assert MAX_NEIGHBORHOOD_PATHS > 0
    assert MAX_PATH_RESULTS > 0


# ---------------------------------------------------------------------------
# 6. Snapshot endpoint integrity (Correction B)
# ---------------------------------------------------------------------------
def _snapshot_payload(
    node_ids: list[uuid.UUID], edges: list[tuple[uuid.UUID, uuid.UUID]]
) -> dict[str, Any]:
    nodes = [
        TopologyServiceNode(
            service_id=sid, name=f"svc-{idx}", service_type=ServiceType.API
        )
        for idx, sid in enumerate(node_ids)
    ]
    version = _basic_version()
    fingerprint_edges = tuple(
        DirectedTopologyEdge(
            upstream_service_id=u,
            downstream_service_id=d,
            dependency_type=DependencyType.SYNC,
        )
        for u, d in edges
    )
    fingerprint = compute_snapshot_fingerprint(
        version.schema_version
        if hasattr(version, "schema_version")
        else SUPPORTED_TOPOLOGY_SCHEMA_VERSION,
        version,
        tuple(nodes),
        fingerprint_edges,
    )
    return {
        "schema_version": SUPPORTED_TOPOLOGY_SCHEMA_VERSION,
        "version": version.model_dump(),
        "nodes": [n.model_dump() for n in nodes],
        "edges": [
            e.model_dump()
            for e in [
                DirectedTopologyEdge(
                    upstream_service_id=u,
                    downstream_service_id=d,
                    dependency_type=DependencyType.SYNC,
                )
                for u, d in edges
            ]
        ],
        "snapshot_fingerprint": fingerprint,
    }


def test_snapshot_missing_upstream_endpoint_rejected() -> None:
    known = uuid.uuid4()
    missing = uuid.uuid4()
    payload = _snapshot_payload([known], [(missing, known)])
    with pytest.raises(ValidationError) as exc_info:
        TopologySnapshot.model_validate(payload)
    assert "upstream" in str(exc_info.value).lower()


def test_snapshot_missing_downstream_endpoint_rejected() -> None:
    known = uuid.uuid4()
    missing = uuid.uuid4()
    payload = _snapshot_payload([known], [(known, missing)])
    with pytest.raises(ValidationError) as exc_info:
        TopologySnapshot.model_validate(payload)
    assert "downstream" in str(exc_info.value).lower()


def test_snapshot_endpoints_present_accepted() -> None:
    first = uuid.uuid4()
    second = uuid.uuid4()
    payload = _snapshot_payload([first, second], [(first, second)])
    snapshot = TopologySnapshot.model_validate(payload)
    assert len(snapshot.nodes) == 2
    assert len(snapshot.edges) == 1


def test_snapshot_endpoint_failure_via_create_is_typed() -> None:
    known = _node("a")
    missing = uuid.uuid4()
    edge = DirectedTopologyEdge(
        upstream_service_id=missing,
        downstream_service_id=known.service_id,
        dependency_type=DependencyType.SYNC,
    )
    with pytest.raises(TopologyContractError) as exc_info:
        TopologySnapshot.create(version=_basic_version(), nodes=[known], edges=[edge])
    assert "upstream" in str(exc_info.value).lower()
    try:
        TopologySnapshot.create(version=_basic_version(), nodes=[known], edges=[edge])
    except TopologyContractError:
        pass
    except KeyError as key_exc:  # pragma: no cover - must never happen
        pytest.fail(f"raw KeyError escaped: {key_exc}")


def test_no_raw_keyerror_from_malformed_snapshot_traversal() -> None:
    known = _node("a")
    missing = uuid.uuid4()
    edge = DirectedTopologyEdge(
        upstream_service_id=known.service_id,
        downstream_service_id=missing,
        dependency_type=DependencyType.SYNC,
    )
    try:
        TopologySnapshot.create(version=_basic_version(), nodes=[known], edges=[edge])
    except TopologyContractError:
        return
    except KeyError as key_exc:  # pragma: no cover - must never happen
        pytest.fail(f"raw KeyError escaped: {key_exc}")
    pytest.fail("expected typed contract rejection before traversal")


# ---------------------------------------------------------------------------
# 7. Neo4j adapter unit behavior
# ---------------------------------------------------------------------------
class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def data(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeSession:
    def __init__(self, run: Any) -> None:
        self._run = run
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def execute_read(self, work: Any) -> Any:
        return await work(self)

    async def run(
        self, query: str, params: dict[str, Any] | None = None
    ) -> _FakeResult:
        self.calls.append((query, params))
        return await self._run(query, params)  # type: ignore[no-any-return]


class _FakeDriver:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session
        self.closed = False

    def session(self, database: str | None = None) -> _FakeSession:
        return self._session

    async def close(self) -> None:
        self.closed = True


def _neo4j_provider(
    driver: _FakeDriver,
    *,
    topology_id: uuid.UUID | None = None,
    topology_version: str | None = None,
) -> Neo4jTopologyProvider:
    return Neo4jTopologyProvider(
        driver=driver,  # type: ignore[arg-type]
        topology_id=topology_id or canonical_topology_id(),
        topology_version=topology_version or CANONICAL_TOPOLOGY_VERSION,
    )


def _fixture_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    topo = build_canonical_topology()
    nodes = [
        {
            "service_id": str(s.service_id),
            "name": s.name,
            "service_type": s.service_type.value,
            "topology_id": str(topo.topology_id),
            "topology_version": CANONICAL_TOPOLOGY_VERSION,
        }
        for s in topo.services
    ]
    edges = [
        {
            "upstream_service_id": str(d.upstream_service_id),
            "downstream_service_id": str(d.downstream_service_id),
            "dependency_type": d.dependency_type.value,
            "topology_id": str(topo.topology_id),
            "topology_version": CANONICAL_TOPOLOGY_VERSION,
        }
        for d in topo.dependencies
    ]
    return nodes, edges


def _rows_driver(
    nodes: list[dict[str, Any]], edges: list[dict[str, Any]]
) -> _FakeDriver:
    async def run(query: str, params: dict[str, Any] | None = None) -> _FakeResult:
        if "DEPENDS_ON" in query:
            return _FakeResult(edges)
        return _FakeResult(nodes)

    return _FakeDriver(_FakeSession(run))


@pytest.mark.asyncio
async def test_neo4j_queries_receive_managed_topology_parameters() -> None:
    nodes, edges = _fixture_rows()
    driver = _rows_driver(nodes, edges)
    provider = _neo4j_provider(driver)
    snapshot = await provider.get_snapshot()
    assert len(snapshot.nodes) == 7
    calls = driver._session.calls
    assert len(calls) == 2
    for query, params in calls:
        assert params is not None
        assert params["topology_id"] == str(canonical_topology_id())
        assert params["topology_version"] == CANONICAL_TOPOLOGY_VERSION
        assert "$topology_id" in query
        assert "$topology_version" in query
        assert "77bb041d" not in query


@pytest.mark.asyncio
async def test_neo4j_edge_query_scopes_endpoints_to_managed_topology() -> None:
    nodes, edges = _fixture_rows()
    driver = _rows_driver(nodes, edges)
    provider = _neo4j_provider(driver)
    await provider.get_snapshot()
    calls = driver._session.calls
    edge_query = next(q for q, _ in calls if "DEPENDS_ON" in q)
    assert "u.topology_id = $topology_id" in edge_query
    assert "d.topology_id = $topology_id" in edge_query
    assert "u.topology_version = $topology_version" in edge_query
    assert "d.topology_version = $topology_version" in edge_query


@pytest.mark.asyncio
async def test_neo4j_unrelated_topology_data_excluded() -> None:
    managed_nodes, managed_edges = _fixture_rows()
    foreign_id = uuid.uuid4()
    foreign_service_id = uuid.uuid4()
    mixed_nodes = managed_nodes + [
        {
            "service_id": str(foreign_service_id),
            "name": "foreign",
            "service_type": "api",
            "topology_id": str(foreign_id),
            "topology_version": "9.9.9",
        }
    ]
    mixed_edges = managed_edges + [
        {
            "upstream_service_id": str(uuid.uuid4()),
            "downstream_service_id": str(uuid.uuid4()),
            "dependency_type": "sync",
            "topology_id": str(foreign_id),
            "topology_version": "9.9.9",
        }
    ]

    async def run(query: str, params: dict[str, Any] | None = None) -> _FakeResult:
        assert params is not None
        tid = params["topology_id"]
        tver = params["topology_version"]
        scoped_nodes = [r for r in mixed_nodes if r["topology_id"] == tid]
        scoped_nodes = [r for r in scoped_nodes if r["topology_version"] == tver]
        scoped_edges = [r for r in mixed_edges if r["topology_id"] == tid]
        scoped_edges = [r for r in scoped_edges if r["topology_version"] == tver]
        if "DEPENDS_ON" in query:
            return _FakeResult(scoped_edges)
        return _FakeResult(scoped_nodes)

    driver = _FakeDriver(_FakeSession(run))
    provider = _neo4j_provider(driver)
    snapshot = await provider.get_snapshot()
    assert len(snapshot.nodes) == 7
    assert len(snapshot.edges) == 7
    assert foreign_service_id not in {node.service_id for node in snapshot.nodes}
    assert "foreign" not in {n.name for n in snapshot.nodes}


@pytest.mark.asyncio
async def test_neo4j_mixed_endpoint_data_rejected() -> None:
    nodes, edges = _fixture_rows()
    contaminated = [dict(r) for r in edges]
    contaminated[0]["topology_version"] = "9.9.9"
    provider = _neo4j_provider(_rows_driver(nodes, contaminated))
    with pytest.raises(TopologyContractError):
        await provider.get_snapshot()


@pytest.mark.asyncio
async def test_neo4j_missing_managed_topology_typed_failure() -> None:
    async def run(query: str, params: dict[str, Any] | None = None) -> _FakeResult:
        return _FakeResult([])

    provider = _neo4j_provider(_FakeDriver(_FakeSession(run)))
    with pytest.raises((TopologyProviderError, TopologyContractError)):
        await provider.get_snapshot()


def test_neo4j_invalid_identity_and_version_rejected() -> None:
    nodes, edges = _fixture_rows()
    driver = _rows_driver(nodes, edges)
    with pytest.raises(TopologyContractError):
        Neo4jTopologyProvider(
            driver=driver,  # type: ignore[arg-type]
            topology_id="not-a-uuid",
            topology_version="1.0.0",
        )
    with pytest.raises(TopologyContractError):
        Neo4jTopologyProvider(
            driver=driver,  # type: ignore[arg-type]
            topology_id=canonical_topology_id(),
            topology_version="   ",
        )
    with pytest.raises(TopologyContractError):
        Neo4jTopologyProvider(driver=driver)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_neo4j_malformed_and_duplicate_rows() -> None:
    nodes, edges = _fixture_rows()

    malformed_nodes = [dict(r) for r in nodes]
    del malformed_nodes[0]["name"]
    provider = _neo4j_provider(_rows_driver(malformed_nodes, edges))
    with pytest.raises(TopologyContractError):
        await provider.get_snapshot()

    malformed_type = [dict(r) for r in nodes]
    malformed_type[0]["service_type"] = "not-a-type"
    provider = _neo4j_provider(_rows_driver(malformed_type, edges))
    with pytest.raises(TopologyContractError):
        await provider.get_snapshot()

    duplicate_nodes = nodes + [nodes[0]]
    provider = _neo4j_provider(_rows_driver(duplicate_nodes, edges))
    with pytest.raises(TopologyContractError):
        await provider.get_snapshot()

    duplicate_edges = edges + [edges[0]]
    provider = _neo4j_provider(_rows_driver(nodes, duplicate_edges))
    with pytest.raises(TopologyContractError):
        await provider.get_snapshot()

    mismatched_version = [dict(r) for r in nodes]
    mismatched_version[0]["topology_version"] = "9.9.9"
    provider = _neo4j_provider(_rows_driver(mismatched_version, edges))
    with pytest.raises(TopologyContractError):
        await provider.get_snapshot()


@pytest.mark.asyncio
async def test_neo4j_row_limit_typed_error() -> None:
    nodes, edges = _fixture_rows()
    oversized = nodes * (MAX_TOPOLOGY_NODES // len(nodes) + 2)
    provider = _neo4j_provider(_rows_driver(oversized, edges))
    with pytest.raises(TopologyQueryLimitError):
        await provider.get_snapshot()


@pytest.mark.asyncio
async def test_neo4j_connection_exception_translation_exact_cause() -> None:
    async def run(query: str, params: dict[str, Any] | None = None) -> _FakeResult:
        raise ServiceUnavailable("cannot reach bolt://user:secret@host:7687")

    provider = _neo4j_provider(_FakeDriver(_FakeSession(run)))
    with pytest.raises(TopologyProviderError) as exc_info:
        await provider.get_snapshot()
    assert isinstance(exc_info.value.__cause__, ServiceUnavailable)
    assert exc_info.value.__cause__ is not None
    message = str(exc_info.value)
    assert "bolt://" not in message
    assert "secret" not in message
    assert "7687" not in message
    assert "Neo4j execution failed" == message


@pytest.mark.asyncio
async def test_neo4j_query_timeout_translation_with_cause() -> None:
    import asyncio

    async def run(query: str, params: dict[str, Any] | None = None) -> _FakeResult:
        raise asyncio.TimeoutError()

    provider = Neo4jTopologyProvider(
        driver=_FakeDriver(_FakeSession(run)),  # type: ignore[arg-type]
        topology_id=canonical_topology_id(),
        topology_version=CANONICAL_TOPOLOGY_VERSION,
        timeout_seconds=0.01,
    )
    with pytest.raises(TopologyProviderError) as exc_info:
        await provider.get_snapshot()
    assert isinstance(exc_info.value.__cause__, asyncio.TimeoutError)
    assert str(exc_info.value) == "Neo4j query timed out"


@pytest.mark.asyncio
async def test_neo4j_failure_never_invokes_canonical_fallback() -> None:
    import app.correlation.topology_neo4j as neo4j_module

    assert not hasattr(neo4j_module, "build_canonical_topology")

    async def run(query: str, params: dict[str, Any] | None = None) -> _FakeResult:
        raise ServiceUnavailable("down")

    provider = _neo4j_provider(_FakeDriver(_FakeSession(run)))
    with pytest.raises(TopologyProviderError):
        await provider.get_snapshot()
    # A second failure must still raise rather than silently returning canonical data.
    with pytest.raises(TopologyProviderError):
        await provider.get_snapshot()


@pytest.mark.asyncio
async def test_injected_driver_ownership_and_close_semantics() -> None:
    nodes, edges = _fixture_rows()
    driver = _rows_driver(nodes, edges)
    provider = _neo4j_provider(driver)
    await provider.close()
    assert driver.closed is False

    owned = Neo4jTopologyProvider(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="x",
        topology_id=canonical_topology_id(),
        topology_version=CANONICAL_TOPOLOGY_VERSION,
    )
    await owned.close()
    assert owned._owns_driver is True


def test_adapter_missing_connection_parameters_rejected() -> None:
    with pytest.raises(TopologyContractError):
        Neo4jTopologyProvider()
    with pytest.raises(TopologyProviderError):
        Neo4jTopologyProvider(
            uri=None,
            username="neo4j",
            password="x",
            topology_id=canonical_topology_id(),
            topology_version=CANONICAL_TOPOLOGY_VERSION,
        )


def test_error_hierarchy_is_typed() -> None:
    for exc in (
        TopologyContractError,
        TopologyNodeNotFoundError,
        TopologyVersionMismatchError,
        TopologyProviderError,
        TopologyQueryLimitError,
    ):
        assert issubclass(exc, TopologyError)


def test_expected_version_bool_rejected() -> None:
    with pytest.raises(TopologyContractError):
        check_expected_version("1.0.0", True)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 8. Public export inventory and Phase 5 leakage rejection
# ---------------------------------------------------------------------------
def test_public_export_inventory() -> None:
    import app.correlation as correlation

    expected = {
        "TopologyProvider",
        "CanonicalTopologyProvider",
        "Neo4jTopologyProvider",
        "TopologyServiceNode",
        "DirectedTopologyEdge",
        "TopologyVersion",
        "TopologySnapshot",
        "TopologyPath",
        "BoundedTopologyNeighborhood",
        "TraversalDirection",
        "TopologyError",
        "TopologyContractError",
        "TopologyNodeNotFoundError",
        "TopologyVersionMismatchError",
        "TopologyProviderError",
        "TopologyQueryLimitError",
    }
    for name in expected:
        assert name in correlation.__all__
        assert hasattr(correlation, name)


def test_no_phase5_leakage_in_topology_contracts() -> None:
    forbidden = {
        "root_cause",
        "rca",
        "remediation",
        "risk_score",
        "approval",
        "embedding",
        "agent",
        "containment",
    }
    models = (
        TopologyServiceNode,
        DirectedTopologyEdge,
        TopologyVersion,
        TopologySnapshot,
        TopologyPath,
        BoundedTopologyNeighborhood,
    )
    for model in models:
        assert forbidden.isdisjoint(set(model.model_fields.keys()))
