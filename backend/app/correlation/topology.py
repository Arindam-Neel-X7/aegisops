from __future__ import annotations

import hashlib
import json
import uuid
from collections import deque
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.correlation.errors import (
    TopologyContractError,
    TopologyNodeNotFoundError,
    TopologyQueryLimitError,
    TopologyVersionMismatchError,
)
from app.simulator.interfaces import DependencyType, ServiceType

SUPPORTED_TOPOLOGY_SCHEMA_VERSION = "1.0"
TOPOLOGY_FINGERPRINT_LEN = 64
TOPOLOGY_MIN_HOPS = 0
TOPOLOGY_MAX_HOPS = 32
MAX_SERVICE_NAME_LEN = 128
MAX_TOPOLOGY_VERSION_LEN = 128
MAX_TOPOLOGY_NODES = 4096
MAX_TOPOLOGY_EDGES = 16384
MAX_NEIGHBORHOOD_NODES = 1024
MAX_NEIGHBORHOOD_EDGES = 1024
MAX_NEIGHBORHOOD_PATHS = 1024
MAX_PATH_RESULTS = 1024
MAX_PATH_SERVICE_IDS = TOPOLOGY_MAX_HOPS + 1

_HEX_CHARS = frozenset("0123456789abcdef")


def _validate_fingerprint(value: Any, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if len(value) != TOPOLOGY_FINGERPRINT_LEN:
        raise ValueError(f"{field_name} must be exactly 64 characters")
    if any(c not in _HEX_CHARS for c in value):
        raise ValueError(f"{field_name} must be lowercase hexadecimal")
    return str(value)


def _validate_bounded_text(value: Any, max_len: int, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field_name} must not be empty or whitespace-only")
    if len(trimmed) > max_len:
        raise ValueError(f"{field_name} exceeds maximum length {max_len}")
    return str(trimmed)


def _validate_not_bool(value: Any) -> Any:
    if isinstance(value, bool):
        raise ValueError("boolean is not accepted where an integer is required")
    return value


def validate_max_hops(value: Any) -> int:
    if isinstance(value, bool):
        raise TopologyContractError("Invalid topology hop bound")
    if not isinstance(value, int):
        raise TopologyContractError("Invalid topology hop bound")
    if value < TOPOLOGY_MIN_HOPS or value > TOPOLOGY_MAX_HOPS:
        raise TopologyContractError("Invalid topology hop bound")
    return value


def check_expected_version(actual: str, expected: str | None) -> None:
    if expected is None:
        return
    if not isinstance(expected, str):
        raise TopologyContractError("Invalid topology version")
    if actual != expected:
        raise TopologyVersionMismatchError("Topology version mismatch")


class _FrozenStrictBase(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class TraversalDirection(StrEnum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    BOTH = "both"


class TopologyServiceNode(_FrozenStrictBase):
    service_id: uuid.UUID
    name: str
    service_type: ServiceType

    @field_validator("name", mode="after")
    @classmethod
    def _check_name(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_SERVICE_NAME_LEN, "name")


class DirectedTopologyEdge(_FrozenStrictBase):
    upstream_service_id: uuid.UUID
    downstream_service_id: uuid.UUID
    dependency_type: DependencyType

    @model_validator(mode="after")
    def _check_endpoints(self) -> DirectedTopologyEdge:
        if self.upstream_service_id == self.downstream_service_id:
            raise ValueError("Self-dependency is not allowed")
        return self


class TopologyVersion(_FrozenStrictBase):
    topology_id: uuid.UUID
    topology_version: str

    @field_validator("topology_version", mode="after")
    @classmethod
    def _check_version(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_TOPOLOGY_VERSION_LEN, "topology_version")


def _node_sort_key(node: TopologyServiceNode) -> str:
    return str(node.service_id)


def _edge_sort_key(edge: DirectedTopologyEdge) -> tuple[str, str, str]:
    return (
        str(edge.upstream_service_id),
        str(edge.downstream_service_id),
        edge.dependency_type.value,
    )


def _canonical_sort_nodes(
    nodes: tuple[TopologyServiceNode, ...],
) -> tuple[TopologyServiceNode, ...]:
    return tuple(sorted(nodes, key=_node_sort_key))


def _canonical_sort_edges(
    edges: tuple[DirectedTopologyEdge, ...],
) -> tuple[DirectedTopologyEdge, ...]:
    return tuple(sorted(edges, key=_edge_sort_key))


def _snapshot_projection(
    schema_version: str,
    version: TopologyVersion,
    nodes: tuple[TopologyServiceNode, ...],
    edges: tuple[DirectedTopologyEdge, ...],
) -> dict[str, Any]:
    sorted_nodes = _canonical_sort_nodes(nodes)
    sorted_edges = _canonical_sort_edges(edges)
    return {
        "edges": [
            {
                "dependency_type": e.dependency_type.value,
                "downstream_service_id": str(e.downstream_service_id),
                "upstream_service_id": str(e.upstream_service_id),
            }
            for e in sorted_edges
        ],
        "nodes": [
            {
                "name": n.name,
                "service_id": str(n.service_id),
                "service_type": n.service_type.value,
            }
            for n in sorted_nodes
        ],
        "schema_version": schema_version,
        "topology_id": str(version.topology_id),
        "topology_version": version.topology_version,
    }


def compute_snapshot_fingerprint(
    schema_version: str,
    version: TopologyVersion,
    nodes: tuple[TopologyServiceNode, ...],
    edges: tuple[DirectedTopologyEdge, ...],
) -> str:
    projection = _snapshot_projection(schema_version, version, nodes, edges)
    canonical = json.dumps(
        projection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def serialize_topology_snapshot(snapshot: TopologySnapshot) -> bytes:
    payload = snapshot.model_dump(mode="json")
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


class TopologySnapshot(_FrozenStrictBase):
    schema_version: str = SUPPORTED_TOPOLOGY_SCHEMA_VERSION
    version: TopologyVersion
    nodes: tuple[TopologyServiceNode, ...]
    edges: tuple[DirectedTopologyEdge, ...]
    snapshot_fingerprint: str

    @field_validator("schema_version", mode="after")
    @classmethod
    def _check_schema(cls, v: str) -> str:
        if v != SUPPORTED_TOPOLOGY_SCHEMA_VERSION:
            raise ValueError("unsupported schema_version")
        return v

    @field_validator("snapshot_fingerprint", mode="after")
    @classmethod
    def _check_fingerprint(cls, v: str) -> str:
        return _validate_fingerprint(v, "snapshot_fingerprint")

    @field_validator("nodes", mode="before")
    @classmethod
    def _check_nodes(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("nodes must be a list or tuple")
        items = list(v)
        if len(items) > MAX_TOPOLOGY_NODES:
            raise ValueError("nodes exceeds maximum entries")
        return tuple(items)

    @field_validator("edges", mode="before")
    @classmethod
    def _check_edges(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("edges must be a list or tuple")
        items = list(v)
        if len(items) > MAX_TOPOLOGY_EDGES:
            raise ValueError("edges exceeds maximum entries")
        return tuple(items)

    @model_validator(mode="after")
    def _check_canonical(self) -> TopologySnapshot:
        sorted_nodes = _canonical_sort_nodes(self.nodes)
        sorted_edges = _canonical_sort_edges(self.edges)
        seen_nodes: set[uuid.UUID] = set()
        for n in sorted_nodes:
            if n.service_id in seen_nodes:
                raise ValueError("Duplicate service node")
            seen_nodes.add(n.service_id)
        seen_edges: set[tuple[str, str, str]] = set()
        for e in sorted_edges:
            sig = (
                str(e.upstream_service_id),
                str(e.downstream_service_id),
                e.dependency_type.value,
            )
            if sig in seen_edges:
                raise ValueError("Duplicate topology edge")
            seen_edges.add(sig)
        for e in sorted_edges:
            if e.upstream_service_id not in seen_nodes:
                raise ValueError("Edge references unknown upstream service")
            if e.downstream_service_id not in seen_nodes:
                raise ValueError("Edge references unknown downstream service")
        object.__setattr__(self, "nodes", sorted_nodes)
        object.__setattr__(self, "edges", sorted_edges)
        expected = compute_snapshot_fingerprint(
            self.schema_version, self.version, sorted_nodes, sorted_edges
        )
        if expected != self.snapshot_fingerprint:
            raise ValueError("snapshot_fingerprint does not match canonical projection")
        return self

    @classmethod
    def create(
        cls,
        *,
        version: TopologyVersion,
        nodes: list[TopologyServiceNode] | tuple[TopologyServiceNode, ...],
        edges: list[DirectedTopologyEdge] | tuple[DirectedTopologyEdge, ...],
        schema_version: str = SUPPORTED_TOPOLOGY_SCHEMA_VERSION,
    ) -> TopologySnapshot:
        if isinstance(nodes, (str, bytes)):
            raise TopologyContractError("Invalid topology nodes")
        if isinstance(edges, (str, bytes)):
            raise TopologyContractError("Invalid topology edges")
        try:
            node_items = list(nodes)  # type: ignore[arg-type]
            edge_items = list(edges)  # type: ignore[arg-type]
        except Exception as exc:
            raise TopologyContractError("Invalid topology nodes or edges") from exc
        if len(node_items) > MAX_TOPOLOGY_NODES:
            raise TopologyContractError("Invalid topology nodes")
        if len(edge_items) > MAX_TOPOLOGY_EDGES:
            raise TopologyContractError("Invalid topology edges")
        node_t = tuple(node_items)
        edge_t = tuple(edge_items)
        for node_entry in node_t:
            if not isinstance(node_entry, TopologyServiceNode):
                raise TopologyContractError("Invalid topology nodes")
        for edge_entry in edge_t:
            if not isinstance(edge_entry, DirectedTopologyEdge):
                raise TopologyContractError("Invalid topology edges")
        seen_n: set[uuid.UUID] = set()
        for n in node_t:
            if n.service_id in seen_n:
                raise TopologyContractError("Duplicate topology node")
            seen_n.add(n.service_id)
        for e in edge_t:
            if e.upstream_service_id not in seen_n:
                raise TopologyContractError("Edge references unknown upstream service")
            if e.downstream_service_id not in seen_n:
                raise TopologyContractError(
                    "Edge references unknown downstream service"
                )
        seen_e: set[tuple[str, str, str]] = set()
        for e in edge_t:
            sig = (
                str(e.upstream_service_id),
                str(e.downstream_service_id),
                e.dependency_type.value,
            )
            if sig in seen_e:
                raise TopologyContractError("Duplicate topology edge")
            seen_e.add(sig)
        sorted_nodes = _canonical_sort_nodes(node_t)
        sorted_edges = _canonical_sort_edges(edge_t)
        fingerprint = compute_snapshot_fingerprint(
            schema_version, version, sorted_nodes, sorted_edges
        )
        return cls.model_validate(
            {
                "edges": list(sorted_edges),
                "nodes": list(sorted_nodes),
                "schema_version": schema_version,
                "snapshot_fingerprint": fingerprint,
                "version": version,
            }
        )


class TopologyPath(_FrozenStrictBase):
    direction: TraversalDirection
    service_ids: tuple[uuid.UUID, ...]
    edges: tuple[DirectedTopologyEdge, ...]
    hop_count: int = Field(ge=TOPOLOGY_MIN_HOPS, le=TOPOLOGY_MAX_HOPS)

    @field_validator("hop_count", mode="before")
    @classmethod
    def _check_hops(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("service_ids", mode="before")
    @classmethod
    def _check_service_ids(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("service_ids must be a list or tuple")
        items = list(v)
        if len(items) < 1:
            raise ValueError("service_ids must contain at least one service")
        if len(items) > MAX_PATH_SERVICE_IDS:
            raise ValueError("service_ids exceeds maximum entries")
        return tuple(items)

    @field_validator("edges", mode="before")
    @classmethod
    def _check_edges(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("edges must be a list or tuple")
        items = list(v)
        if len(items) > TOPOLOGY_MAX_HOPS:
            raise ValueError("edges exceeds maximum entries")
        return tuple(items)

    @model_validator(mode="after")
    def _check_consistency(self) -> TopologyPath:
        if len(set(self.service_ids)) != len(self.service_ids):
            raise ValueError("service_ids must be unique within a path")
        if self.hop_count != len(self.edges):
            raise ValueError("hop_count must equal number of edges")
        if self.hop_count != len(self.service_ids) - 1:
            raise ValueError("service_ids length must equal hop_count plus one")
        for idx, edge in enumerate(self.edges):
            current = self.service_ids[idx]
            nxt = self.service_ids[idx + 1]
            if self.direction == TraversalDirection.DOWNSTREAM:
                if not (
                    edge.upstream_service_id == current
                    and edge.downstream_service_id == nxt
                ):
                    raise ValueError("edge does not follow downstream path order")
            elif self.direction == TraversalDirection.UPSTREAM:
                if not (
                    edge.upstream_service_id == nxt
                    and edge.downstream_service_id == current
                ):
                    raise ValueError("edge does not follow upstream path order")
            else:
                pair = {edge.upstream_service_id, edge.downstream_service_id}
                if pair != {current, nxt}:
                    raise ValueError("edge does not connect path services")
                if edge.upstream_service_id == edge.downstream_service_id:
                    raise ValueError("Self-dependency is not allowed")
        return self


class BoundedTopologyNeighborhood(_FrozenStrictBase):
    snapshot_fingerprint: str
    root_service_id: uuid.UUID
    direction: TraversalDirection
    max_hops: int = Field(ge=TOPOLOGY_MIN_HOPS, le=TOPOLOGY_MAX_HOPS)
    nodes: tuple[TopologyServiceNode, ...]
    edges: tuple[DirectedTopologyEdge, ...]
    paths: tuple[TopologyPath, ...]
    truncated: bool

    @field_validator("snapshot_fingerprint", mode="after")
    @classmethod
    def _check_fingerprint(cls, v: str) -> str:
        return _validate_fingerprint(v, "snapshot_fingerprint")

    @field_validator("max_hops", mode="before")
    @classmethod
    def _check_hops(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("nodes", mode="before")
    @classmethod
    def _check_nodes(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("nodes must be a list or tuple")
        items = list(v)
        if len(items) > MAX_NEIGHBORHOOD_NODES:
            raise ValueError("nodes exceeds maximum entries")
        return tuple(items)

    @field_validator("edges", mode="before")
    @classmethod
    def _check_edges(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("edges must be a list or tuple")
        items = list(v)
        if len(items) > MAX_NEIGHBORHOOD_EDGES:
            raise ValueError("edges exceeds maximum entries")
        return tuple(items)

    @field_validator("paths", mode="before")
    @classmethod
    def _check_paths(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise ValueError("paths must be a list or tuple")
        items = list(v)
        if len(items) > MAX_NEIGHBORHOOD_PATHS:
            raise ValueError("paths exceeds maximum entries")
        return tuple(items)

    @model_validator(mode="after")
    def _check_membership(self) -> BoundedTopologyNeighborhood:
        sorted_nodes = _canonical_sort_nodes(self.nodes)
        sorted_edges = _canonical_sort_edges(self.edges)
        sorted_paths = tuple(
            sorted(self.paths, key=lambda p: tuple(str(s) for s in p.service_ids))
        )
        seen: set[uuid.UUID] = set()
        for n in sorted_nodes:
            if n.service_id in seen:
                raise ValueError("Duplicate service node")
            seen.add(n.service_id)
        if self.root_service_id not in seen:
            raise ValueError("root_service_id must appear in nodes")
        object.__setattr__(self, "nodes", sorted_nodes)
        object.__setattr__(self, "edges", sorted_edges)
        object.__setattr__(self, "paths", sorted_paths)
        return self


@runtime_checkable
class TopologyProvider(Protocol):
    async def get_snapshot(
        self, *, expected_version: str | None = None
    ) -> TopologySnapshot: ...

    async def get_neighborhood(
        self,
        *,
        service_id: uuid.UUID,
        max_hops: int,
        direction: TraversalDirection,
        expected_version: str | None = None,
    ) -> BoundedTopologyNeighborhood: ...

    async def find_paths(
        self,
        *,
        source_service_id: uuid.UUID,
        target_service_id: uuid.UUID,
        max_hops: int,
        direction: TraversalDirection,
        expected_version: str | None = None,
    ) -> tuple[TopologyPath, ...]: ...

    async def close(self) -> None: ...


def _build_adjacency(
    snapshot: TopologySnapshot,
) -> tuple[
    dict[uuid.UUID, list[tuple[uuid.UUID, DirectedTopologyEdge]]],
    dict[uuid.UUID, list[tuple[uuid.UUID, DirectedTopologyEdge]]],
    dict[uuid.UUID, TopologyServiceNode],
]:
    node_map: dict[uuid.UUID, TopologyServiceNode] = {
        n.service_id: n for n in snapshot.nodes
    }
    downstream: dict[uuid.UUID, list[tuple[uuid.UUID, DirectedTopologyEdge]]] = {
        n.service_id: [] for n in snapshot.nodes
    }
    upstream: dict[uuid.UUID, list[tuple[uuid.UUID, DirectedTopologyEdge]]] = {
        n.service_id: [] for n in snapshot.nodes
    }
    for e in snapshot.edges:
        downstream[e.upstream_service_id].append((e.downstream_service_id, e))
        upstream[e.downstream_service_id].append((e.upstream_service_id, e))
    for key in downstream:
        downstream[key] = sorted(downstream[key], key=lambda t: str(t[0]))
    for key in upstream:
        upstream[key] = sorted(upstream[key], key=lambda t: str(t[0]))
    return downstream, upstream, node_map


def _neighbors(
    node_id: uuid.UUID,
    direction: TraversalDirection,
    downstream: dict[uuid.UUID, list[tuple[uuid.UUID, DirectedTopologyEdge]]],
    upstream: dict[uuid.UUID, list[tuple[uuid.UUID, DirectedTopologyEdge]]],
) -> list[tuple[uuid.UUID, DirectedTopologyEdge]]:
    if direction == TraversalDirection.DOWNSTREAM:
        return list(downstream.get(node_id, []))
    if direction == TraversalDirection.UPSTREAM:
        return list(upstream.get(node_id, []))
    combined: dict[uuid.UUID, DirectedTopologyEdge] = {}
    for target, edge in downstream.get(node_id, []):
        combined.setdefault(target, edge)
    for target, edge in upstream.get(node_id, []):
        combined.setdefault(target, edge)
    return sorted(combined.items(), key=lambda t: str(t[0]))


def build_neighborhood(
    snapshot: TopologySnapshot,
    *,
    root_service_id: uuid.UUID,
    max_hops: int,
    direction: TraversalDirection,
) -> BoundedTopologyNeighborhood:
    hops = validate_max_hops(max_hops)
    if not isinstance(direction, TraversalDirection):
        raise TopologyContractError("Invalid traversal direction")
    downstream, upstream, node_map = _build_adjacency(snapshot)
    if root_service_id not in node_map:
        raise TopologyNodeNotFoundError("Topology service node not found")
    visited: dict[uuid.UUID, int] = {root_service_id: 0}
    predecessor: dict[uuid.UUID, tuple[uuid.UUID, DirectedTopologyEdge] | None] = {
        root_service_id: None
    }
    queue: deque[uuid.UUID] = deque([root_service_id])
    while queue:
        current = queue.popleft()
        depth = visited[current]
        if depth >= hops:
            continue
        for target, edge in _neighbors(current, direction, downstream, upstream):
            if target not in visited:
                visited[target] = depth + 1
                predecessor[target] = (current, edge)
                queue.append(target)
    truncated = False
    if hops < TOPOLOGY_MAX_HOPS:
        for node_id, depth in visited.items():
            if depth == hops:
                for target, _ in _neighbors(node_id, direction, downstream, upstream):
                    if target not in visited:
                        truncated = True
                        break
            if truncated:
                break
    neighborhood_nodes = tuple(node_map[nid] for nid in visited)
    neighborhood_node_ids = set(visited)
    neighborhood_edges = tuple(
        e
        for e in snapshot.edges
        if e.upstream_service_id in neighborhood_node_ids
        and e.downstream_service_id in neighborhood_node_ids
    )
    if len(neighborhood_nodes) > MAX_NEIGHBORHOOD_NODES:
        raise TopologyQueryLimitError("Topology query limit exceeded")
    if len(neighborhood_edges) > MAX_NEIGHBORHOOD_EDGES:
        raise TopologyQueryLimitError("Topology query limit exceeded")
    paths: list[TopologyPath] = []
    for target in sorted(visited, key=str):
        chain: list[uuid.UUID] = [target]
        edge_chain: list[DirectedTopologyEdge] = []
        cursor = target
        while predecessor[cursor] is not None:
            prev, edge = predecessor[cursor]  # type: ignore[misc]
            chain.append(prev)
            edge_chain.append(edge)
            cursor = prev
        chain.reverse()
        edge_chain.reverse()
        paths.append(
            TopologyPath(
                direction=direction,
                edges=tuple(edge_chain),
                hop_count=len(edge_chain),
                service_ids=tuple(chain),
            )
        )
    paths_sorted = tuple(
        sorted(paths, key=lambda p: tuple(str(s) for s in p.service_ids))
    )
    if len(paths_sorted) > MAX_NEIGHBORHOOD_PATHS:
        raise TopologyQueryLimitError("Topology query limit exceeded")
    return BoundedTopologyNeighborhood(
        direction=direction,
        edges=_canonical_sort_edges(neighborhood_edges),
        max_hops=hops,
        nodes=_canonical_sort_nodes(neighborhood_nodes),
        paths=paths_sorted,
        root_service_id=root_service_id,
        snapshot_fingerprint=snapshot.snapshot_fingerprint,
        truncated=truncated,
    )


def find_snapshot_paths(
    snapshot: TopologySnapshot,
    *,
    source_service_id: uuid.UUID,
    target_service_id: uuid.UUID,
    max_hops: int,
    direction: TraversalDirection,
) -> tuple[TopologyPath, ...]:
    hops = validate_max_hops(max_hops)
    if not isinstance(direction, TraversalDirection):
        raise TopologyContractError("Invalid traversal direction")
    downstream, upstream, node_map = _build_adjacency(snapshot)
    if source_service_id not in node_map:
        raise TopologyNodeNotFoundError("Topology service node not found")
    if target_service_id not in node_map:
        raise TopologyNodeNotFoundError("Topology service node not found")
    if source_service_id == target_service_id:
        if hops < 0:
            raise TopologyContractError("Invalid topology hop bound")
        zero = TopologyPath(
            direction=direction,
            edges=(),
            hop_count=0,
            service_ids=(source_service_id,),
        )
        return (zero,)
    results: list[TopologyPath] = []
    queue: deque[
        tuple[uuid.UUID, tuple[uuid.UUID, ...], tuple[DirectedTopologyEdge, ...]]
    ] = deque()
    queue.append((source_service_id, (source_service_id,), ()))
    while queue:
        current, sid_chain, edge_chain = queue.popleft()
        depth = len(edge_chain)
        if depth >= hops:
            continue
        for target, edge in _neighbors(current, direction, downstream, upstream):
            if target in sid_chain:
                continue
            new_sids = sid_chain + (target,)
            new_edges = edge_chain + (edge,)
            if target == target_service_id:
                results.append(
                    TopologyPath(
                        direction=direction,
                        edges=new_edges,
                        hop_count=len(new_edges),
                        service_ids=new_sids,
                    )
                )
                if len(results) > MAX_PATH_RESULTS:
                    raise TopologyQueryLimitError("Topology query limit exceeded")
            else:
                if len(new_edges) < hops:
                    queue.append((target, new_sids, new_edges))
    ordered = tuple(sorted(results, key=lambda p: tuple(str(s) for s in p.service_ids)))
    if len(ordered) > MAX_PATH_RESULTS:
        raise TopologyQueryLimitError("Topology query limit exceeded")
    return ordered
