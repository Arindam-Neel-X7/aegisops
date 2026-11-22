"""Versioned blast-radius contracts and structural serialization.

Graph calculation is deliberately outside this contract module's C1 implementation.
Structural decoding authenticates neither graph reachability nor a provider.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, TypeVar
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.correlation.models import IncidentAggregate, TopologySnapshotReference
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    serialize_incident_aggregate,
)
from app.correlation.topology import (
    DirectedTopologyEdge,
    TopologyPath,
    TopologySnapshot,
    TraversalDirection,
)
from app.simulator.interfaces import DependencyType

MAX_CONTEXT_BYTES = 2_097_152
MAX_HANDOFF_BYTES = 67_108_864
Fingerprint = Annotated[
    str,
    StringConstraints(
        strict=True, pattern=r"^[0-9a-f]{64}$", min_length=64, max_length=64
    ),
]
ServiceName = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=128)
]
T = TypeVar("T", bound=BaseModel)


class BlastRadiusInputError(ValueError):
    """Invalid blast-radius input or structurally inconsistent context."""


class BlastRadiusTopologyMismatchError(BlastRadiusInputError):
    """The independently pinned topology does not match."""


class BlastRadiusLimitError(BlastRadiusInputError):
    """A hard resource ceiling was exceeded."""


def _canonical_bytes(payload: Any, limit: int) -> bytes:
    """Count UTF-8 bytes incrementally before retaining an over-limit buffer."""
    buffer = bytearray()
    encoder = json.JSONEncoder(
        sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    for text in encoder.iterencode(payload):
        # Encoding a bounded slice also bounds temporary buffers for long strings.
        for start in range(0, len(text), 4096):
            chunk = text[start : start + 4096].encode("utf-8")
            if len(buffer) + len(chunk) > limit:
                raise BlastRadiusLimitError("Canonical payload exceeds byte limit")
            buffer.extend(chunk)
    return bytes(buffer)


def _decode_object(raw: bytes, limit: int) -> dict[str, Any]:
    if not isinstance(raw, bytes):
        raise BlastRadiusInputError("Payload must be bytes")
    if len(raw) > limit:
        raise BlastRadiusLimitError("Payload exceeds byte limit")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def nonfinite(value: str) -> None:
        raise ValueError("Non-finite JSON number")

    value = json.loads(
        raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=nonfinite
    )
    if not isinstance(value, dict):
        raise ValueError("Payload must be an object")
    return value


def _fresh(value: T, cls: type[T]) -> T:
    # Preserve actual Python types: the serializer can normalize bool to int
    # before validation, and can hide undeclared model_copy update keys.
    def payload(item: Any, ancestors: frozenset[int]) -> Any:
        if isinstance(item, (BaseModel, tuple, list, dict)):
            if id(item) in ancestors:
                raise ValueError("Cyclic contract value")
            ancestors = ancestors | {id(item)}
        if isinstance(item, BaseModel):
            fields = type(item).model_fields
            if set(item.__dict__) != set(fields) or item.__pydantic_extra__:
                raise ValueError("Missing or undeclared model fields")
            return {key: payload(item.__dict__[key], ancestors) for key in fields}
        if isinstance(item, tuple):
            return tuple(payload(v, ancestors) for v in item)
        if isinstance(item, list):
            return [payload(v, ancestors) for v in item]
        if isinstance(item, dict):
            return {k: payload(v, ancestors) for k, v in item.items()}
        return item

    return cls.model_validate(payload(value, frozenset()))


def _edge_key(edge: DirectedTopologyEdge) -> tuple[str, str, str]:
    return (
        str(edge.upstream_service_id),
        str(edge.downstream_service_id),
        edge.dependency_type.value,
    )


def _unique_sorted(values: tuple[T, ...], key: Any) -> tuple[T, ...]:
    keys = [key(v) for v in values]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate collection key")
    return tuple(sorted(values, key=key))


class _Contract(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", revalidate_instances="always"
    )


class BlastRadiusPolicy(_Contract):
    policy_version: Literal["blast-radius-policy/1.0"] = "blast-radius-policy/1.0"
    direction: TraversalDirection = TraversalDirection.DOWNSTREAM
    max_hops: int = Field(default=2, ge=0, le=32)
    allowed_edge_types: tuple[DependencyType, ...] = tuple(
        sorted(DependencyType, key=lambda x: x.value)
    )
    max_services: int = Field(default=512, ge=1, le=512)
    max_edges: int = Field(default=1024, ge=0, le=1024)
    max_examined_arcs: int = Field(default=65536, ge=1, le=65536)
    max_snapshot_age_seconds: int | None = Field(default=None, ge=0, le=31_536_000)

    @field_validator("allowed_edge_types")
    @classmethod
    def _types(cls, value: tuple[DependencyType, ...]) -> tuple[DependencyType, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("Edge types must be nonempty and unique")
        return tuple(sorted(value, key=lambda x: x.value))


class MemberService(_Contract):
    service_id: UUID
    name: ServiceName

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        if value != value.strip() or not value:
            raise ValueError("Service name must be exact nonempty text")
        return value


class ReachableService(MemberService):
    minimum_depth: int = Field(ge=0, le=32)
    witness_root_service_id: UUID


class BlastRadiusPath(TopologyPath):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", revalidate_instances="always"
    )
    root_service_id: UUID
    target_service_id: UUID

    @field_validator("edges", mode="before")
    @classmethod
    def _edges(cls, value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return tuple(
                _fresh(e, DirectedTopologyEdge)
                if isinstance(e, DirectedTopologyEdge)
                else e
                for e in value
            )
        return value

    @model_validator(mode="after")
    def _endpoints(self) -> BlastRadiusPath:
        if (
            self.root_service_id != self.service_ids[0]
            or self.target_service_id != self.service_ids[-1]
        ):
            raise ValueError("Path root/target mismatch")
        return self


class BlastRadiusContext(_Contract):
    schema_version: Literal["blast-radius-context/1.0"] = "blast-radius-context/1.0"
    policy: BlastRadiusPolicy
    topology_snapshot: TopologySnapshotReference
    member_services: tuple[MemberService, ...] = Field(min_length=1, max_length=512)
    reachable_services: tuple[ReachableService, ...] = Field(
        min_length=1, max_length=512
    )
    edges: tuple[DirectedTopologyEdge, ...] = Field(max_length=1024)
    paths: tuple[BlastRadiusPath, ...] = Field(min_length=1, max_length=512)
    potentially_affected_service_ids: tuple[UUID, ...] = Field(
        min_length=1, max_length=512
    )
    truncated: bool
    truncation_reasons: tuple[Literal["edge_limit", "hop_limit", "service_limit"], ...]
    omitted_within_hop_services: int = Field(ge=0, le=4096)
    omitted_eligible_edges: int = Field(ge=0, le=16384)
    examined_arcs: int = Field(ge=0, le=65536)
    context_fingerprint: Fingerprint

    @field_validator("topology_snapshot", mode="before")
    @classmethod
    def _snapshot(cls, value: Any, info: ValidationInfo) -> Any:
        if isinstance(value, TopologySnapshotReference):
            return _fresh(value, TopologySnapshotReference)
        if isinstance(value, dict):
            if info.mode == "python":
                return TopologySnapshotReference.model_validate(value)
            return TopologySnapshotReference.model_validate_json(
                _canonical_bytes(value, MAX_CONTEXT_BYTES)
            )
        return value

    @field_validator("edges", mode="before")
    @classmethod
    def _edges(cls, value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return tuple(
                _fresh(e, DirectedTopologyEdge)
                if isinstance(e, DirectedTopologyEdge)
                else e
                for e in value
            )
        return value

    @model_validator(mode="after")
    def _consistent(self) -> BlastRadiusContext:
        members = _unique_sorted(self.member_services, lambda x: str(x.service_id))
        nodes = _unique_sorted(self.reachable_services, lambda x: str(x.service_id))
        edges = _unique_sorted(self.edges, _edge_key)
        paths = _unique_sorted(self.paths, lambda x: str(x.target_service_id))
        if len(self.potentially_affected_service_ids) != len(
            set(self.potentially_affected_service_ids)
        ):
            raise ValueError("Duplicate affected service")
        affected = tuple(sorted(self.potentially_affected_service_ids, key=str))
        reasons = tuple(sorted(self.truncation_reasons))
        if len(reasons) != len(set(reasons)):
            raise ValueError("Duplicate truncation reason")
        for key, value in dict(
            member_services=members,
            reachable_services=nodes,
            edges=edges,
            paths=paths,
            potentially_affected_service_ids=affected,
            truncation_reasons=reasons,
        ).items():
            object.__setattr__(self, key, value)
        roots = {n.service_id: n for n in members}
        node_map = {n.service_id: n for n in nodes}
        path_map = {p.target_service_id: p for p in paths}
        edge_set = {_edge_key(e) for e in edges}
        if len({n.name for n in members}) != len(members):
            raise ValueError("Ambiguous member names")
        if (
            not set(roots) <= set(node_map)
            or set(path_map) != set(node_map)
            or set(affected) != set(node_map)
        ):
            raise ValueError("Service/path membership mismatch")
        if len(nodes) > self.policy.max_services or len(edges) > self.policy.max_edges:
            raise ValueError("Policy output bound exceeded")
        if self.examined_arcs > self.policy.max_examined_arcs:
            raise ValueError("Work bound exceeded")
        if (
            len(nodes) + self.omitted_within_hop_services > 4096
            or len(edges) + self.omitted_eligible_edges > 16384
        ):
            raise ValueError("Snapshot count bound exceeded")
        for member in members:
            n = node_map[member.service_id]
            if (
                n.name != member.name
                or n.minimum_depth != 0
                or n.witness_root_service_id != member.service_id
            ):
                raise ValueError("Invalid member root")
        for node in nodes:
            p = path_map[node.service_id]
            if (
                p.root_service_id not in roots
                or node.witness_root_service_id != p.root_service_id
            ):
                raise ValueError("Unknown witness root")
            if (
                p.hop_count != node.minimum_depth
                or p.hop_count > self.policy.max_hops
                or p.direction != self.policy.direction
            ):
                raise ValueError("Depth/direction mismatch")
            if node.minimum_depth == 0 and node.service_id not in roots:
                raise ValueError("Nonmember at depth zero")
            for index, sid in enumerate(p.service_ids):
                if sid not in node_map or node_map[sid].minimum_depth != index:
                    raise ValueError("Witness not closed over minimum-depth nodes")
                prefix = path_map[sid]
                if prefix.service_ids != p.service_ids[: index + 1] or tuple(
                    map(_edge_key, prefix.edges)
                ) != tuple(map(_edge_key, p.edges[:index])):
                    raise ValueError("Inconsistent witness prefix")
            if any(_edge_key(e) not in edge_set for e in p.edges):
                raise ValueError("Witness edge absent from graph")
        for e in edges:
            if (
                e.upstream_service_id not in node_map
                or e.downstream_service_id not in node_map
                or e.dependency_type not in self.policy.allowed_edge_types
            ):
                raise ValueError("Invalid context edge")
            a = node_map[e.upstream_service_id].minimum_depth
            b = node_map[e.downstream_service_id].minimum_depth
            if (
                self.policy.direction
                in (TraversalDirection.DOWNSTREAM, TraversalDirection.BOTH)
                and b > a + 1
            ):
                raise ValueError("Graph contradicts minimum depth")
            if (
                self.policy.direction
                in (TraversalDirection.UPSTREAM, TraversalDirection.BOTH)
                and a > b + 1
            ):
                raise ValueError("Graph contradicts minimum depth")
        if self.truncated != bool(reasons) or ("service_limit" in reasons) != bool(
            self.omitted_within_hop_services
        ):
            raise ValueError("Invalid truncation flags")
        if "service_limit" in reasons and len(nodes) != self.policy.max_services:
            raise ValueError("Service truncation below cap")
        if "edge_limit" in reasons and (
            not self.omitted_eligible_edges or len(edges) != self.policy.max_edges
        ):
            raise ValueError("Invalid edge truncation")
        if self.omitted_eligible_edges and not {
            "edge_limit",
            "service_limit",
        }.intersection(reasons):
            raise ValueError("Unexplained omitted edges")
        if self.context_fingerprint != compute_context_fingerprint(
            self.model_dump(mode="json")
        ):
            raise ValueError("Context fingerprint mismatch")
        return self


def compute_context_fingerprint(payload: dict[str, Any]) -> str:
    projection = {k: v for k, v in payload.items() if k != "context_fingerprint"}
    return hashlib.sha256(_canonical_bytes(projection, MAX_CONTEXT_BYTES)).hexdigest()


def serialize_blast_radius_context(context: BlastRadiusContext) -> bytes:
    try:
        if type(context) is not BlastRadiusContext:
            raise ValueError("Wrong context type")
        fresh = _fresh(context, BlastRadiusContext)
        return _canonical_bytes(fresh.model_dump(mode="json"), MAX_CONTEXT_BYTES)
    except BlastRadiusLimitError:
        raise
    except Exception as exc:
        raise BlastRadiusInputError("Invalid blast-radius context") from exc


def decode_blast_radius_context_structural(raw: bytes) -> BlastRadiusContext:
    """Validate structure/hashes only; B1/H1 own independent graph recomputation."""
    try:
        payload = _decode_object(raw, MAX_CONTEXT_BYTES)
        model = BlastRadiusContext.model_validate_json(
            _canonical_bytes(payload, MAX_CONTEXT_BYTES)
        )
        serialize_blast_radius_context(model)
        return model
    except BlastRadiusLimitError:
        raise
    except Exception as exc:
        raise BlastRadiusInputError("Invalid blast-radius context payload") from exc


def _witness_key(
    witness: tuple[tuple[UUID, ...], tuple[DirectedTopologyEdge, ...]],
) -> tuple[tuple[str, ...], tuple[tuple[str, str, str], ...]]:
    return tuple(map(str, witness[0])), tuple(map(_edge_key, witness[1]))


def calculate_blast_radius(
    *,
    incident: IncidentAggregate,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    policy: BlastRadiusPolicy,
    as_of: datetime,
) -> BlastRadiusContext:
    """Calculate pinned, bounded potential exposure without mutating sources.

    One canonical shortest witness is retained per service. No live provider,
    database, wall clock, publication, handoff adapter or RCA is consulted.
    """
    try:
        return _calculate_blast_radius(
            incident=incident,
            snapshot=snapshot,
            expected_topology=expected_topology,
            policy=policy,
            as_of=as_of,
        )
    except BlastRadiusInputError:
        raise
    except Exception as exc:
        raise BlastRadiusInputError("Invalid blast-radius calculation input") from exc


def _calculate_blast_radius(
    *,
    incident: IncidentAggregate,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    policy: BlastRadiusPolicy,
    as_of: datetime,
) -> BlastRadiusContext:
    if not isinstance(incident, IncidentAggregate) or not isinstance(
        snapshot, TopologySnapshot
    ):
        raise BlastRadiusInputError("Incident and snapshot contracts are required")
    if not isinstance(expected_topology, TopologySnapshotReference) or not isinstance(
        policy, BlastRadiusPolicy
    ):
        raise BlastRadiusInputError("Independent topology pin and policy are required")
    if not isinstance(as_of, datetime) or as_of.utcoffset() != timedelta(0):
        raise BlastRadiusInputError("Explicit UTC as_of is required")
    # Reject oversized unsafe/copied collections before recursive revalidation.
    for value, limit in [
        (snapshot.nodes, 4096),
        (snapshot.edges, 16384),
        (incident.signal_references, 1024),
        (incident.correlation_evidence, 4096),
    ]:
        if not isinstance(value, (tuple, list)):
            raise BlastRadiusInputError("Invalid source collection")
        if len(value) > limit:
            raise BlastRadiusLimitError("Source collection exceeds hard limit")
    # Exact matching must not repair whitespace in unsafe copied source values.
    texts = [snapshot.version.topology_version, expected_topology.topology_version]
    texts.extend(node.name for node in snapshot.nodes)
    texts.extend(ref.service for ref in incident.signal_references)
    if any(
        not isinstance(text, str) or not text or text != text.strip() for text in texts
    ):
        raise BlastRadiusInputError("Noncanonical service name or topology version")
    validated_incident = _fresh(incident, IncidentAggregate)
    source_incident_bytes = serialize_incident_aggregate(incident)
    incident = deserialize_incident_aggregate(
        serialize_incident_aggregate(validated_incident)
    )
    if serialize_incident_aggregate(incident) != source_incident_bytes:
        raise BlastRadiusInputError("Incident source is not canonical")
    snapshot = _fresh(snapshot, TopologySnapshot)
    expected_topology = _fresh(expected_topology, TopologySnapshotReference)
    policy = _fresh(policy, BlastRadiusPolicy)
    actual_pin = (
        snapshot.version.topology_id,
        snapshot.version.topology_version,
        snapshot.snapshot_fingerprint,
    )
    pin = (
        expected_topology.topology_id,
        expected_topology.topology_version,
        expected_topology.snapshot_fingerprint,
    )
    if actual_pin != pin:
        raise BlastRadiusTopologyMismatchError(
            "Topology snapshot does not match expected pin"
        )
    if incident.topology_snapshot is not None:
        ref = incident.topology_snapshot
        if (ref.topology_id, ref.topology_version, ref.snapshot_fingerprint) != pin:
            raise BlastRadiusTopologyMismatchError("Incident topology pin mismatch")
    age = as_of - expected_topology.captured_at
    if age < timedelta(0):
        raise BlastRadiusInputError("Topology capture is later than as_of")
    if policy.max_snapshot_age_seconds is not None and age > timedelta(
        seconds=policy.max_snapshot_age_seconds
    ):
        raise BlastRadiusTopologyMismatchError("Topology snapshot exceeds age policy")
    member_names = {ref.service for ref in incident.signal_references}
    if incident.primary_service not in member_names:
        raise BlastRadiusInputError("Primary service must be an incident member")
    if not 1 <= len(member_names) <= min(512, policy.max_services):
        raise BlastRadiusInputError("Member roots exceed service budget")
    by_name: dict[str, list[UUID]] = {}
    nodes = {node.service_id: node for node in snapshot.nodes}
    for node in snapshot.nodes:
        by_name.setdefault(node.name, []).append(node.service_id)
    if any(len(by_name.get(name, [])) != 1 for name in member_names):
        raise BlastRadiusInputError(
            "Incident service is missing or ambiguous in topology"
        )
    roots = tuple(sorted((by_name[name][0] for name in member_names), key=str))
    allowed_edges = tuple(
        edge
        for edge in snapshot.edges
        if edge.dependency_type in policy.allowed_edge_types
    )
    adjacency: dict[UUID, list[tuple[UUID, DirectedTopologyEdge]]] = {
        sid: [] for sid in nodes
    }
    for edge in allowed_edges:
        if policy.direction in (TraversalDirection.DOWNSTREAM, TraversalDirection.BOTH):
            adjacency[edge.upstream_service_id].append(
                (edge.downstream_service_id, edge)
            )
        if policy.direction in (TraversalDirection.UPSTREAM, TraversalDirection.BOTH):
            adjacency[edge.downstream_service_id].append(
                (edge.upstream_service_id, edge)
            )
    for arcs in adjacency.values():
        arcs.sort(key=lambda arc: (str(arc[0]), _edge_key(arc[1])))
    witnesses: dict[UUID, tuple[tuple[UUID, ...], tuple[DirectedTopologyEdge, ...]]] = {
        root: ((root,), ()) for root in roots
    }
    layer = list(roots)
    depth = examined = 0

    def examine() -> None:
        nonlocal examined
        if examined >= policy.max_examined_arcs:
            raise BlastRadiusLimitError("Topology traversal work limit exceeded")
        examined += 1

    while layer and depth < policy.max_hops:
        # Only the best witness per next-layer node is retained; no path enumeration.
        candidates: dict[
            UUID, tuple[tuple[UUID, ...], tuple[DirectedTopologyEdge, ...]]
        ] = {}
        for sid in sorted(layer, key=lambda s: _witness_key(witnesses[s])):
            chain, path_edges = witnesses[sid]
            for target, edge in adjacency[sid]:
                examine()
                if target in witnesses:
                    continue
                candidate = (chain + (target,), path_edges + (edge,))
                if target not in candidates or _witness_key(candidate) < _witness_key(
                    candidates[target]
                ):
                    candidates[target] = candidate
        witnesses.update(candidates)
        layer = list(candidates)
        depth += 1
    hop_omission = False
    if layer and depth == policy.max_hops:
        for sid in sorted(layer, key=str):
            for target, _ in adjacency[sid]:
                examine()
                if target not in witnesses:
                    hop_omission = True
    reasons: list[Literal["edge_limit", "hop_limit", "service_limit"]] = []
    if hop_omission:
        reasons.append("hop_limit")
    root_set = set(roots)
    nonroots = sorted(
        set(witnesses) - root_set,
        key=lambda s: (len(witnesses[s][1]), _witness_key(witnesses[s])),
    )
    kept = root_set | set(nonroots[: policy.max_services - len(roots)])
    if len(kept) < len(witnesses):
        reasons.append("service_limit")
    eligible = {
        _edge_key(e): e
        for e in allowed_edges
        if e.upstream_service_id in witnesses and e.downstream_service_id in witnesses
    }
    induced = {
        key: e
        for key, e in eligible.items()
        if e.upstream_service_id in kept and e.downstream_service_id in kept
    }
    mandatory = {_edge_key(e) for sid in kept for e in witnesses[sid][1]}
    if len(mandatory) > policy.max_edges:
        raise BlastRadiusLimitError("Edge budget cannot retain all witness edges")
    selected = mandatory | set(
        sorted(set(induced) - mandatory)[: policy.max_edges - len(mandatory)]
    )
    if len(selected) < len(induced):
        reasons.append("edge_limit")
    ordered = sorted(kept, key=str)
    payload = dict(
        schema_version="blast-radius-context/1.0",
        policy=policy.model_dump(mode="json"),
        topology_snapshot=expected_topology.model_dump(mode="json"),
        member_services=[dict(service_id=str(s), name=nodes[s].name) for s in roots],
        reachable_services=[
            dict(
                service_id=str(s),
                name=nodes[s].name,
                minimum_depth=len(witnesses[s][1]),
                witness_root_service_id=str(witnesses[s][0][0]),
            )
            for s in ordered
        ],
        edges=[induced[key].model_dump(mode="json") for key in sorted(selected)],
        paths=[
            dict(
                root_service_id=str(witnesses[s][0][0]),
                target_service_id=str(s),
                direction=policy.direction.value,
                service_ids=list(map(str, witnesses[s][0])),
                edges=[e.model_dump(mode="json") for e in witnesses[s][1]],
                hop_count=len(witnesses[s][1]),
            )
            for s in ordered
        ],
        potentially_affected_service_ids=list(map(str, ordered)),
        truncated=bool(reasons),
        truncation_reasons=sorted(reasons),
        omitted_within_hop_services=len(witnesses) - len(kept),
        omitted_eligible_edges=len(eligible) - len(selected),
        examined_arcs=examined,
    )
    payload["context_fingerprint"] = compute_context_fingerprint(payload)
    context = BlastRadiusContext.model_validate_json(
        _canonical_bytes(payload, MAX_CONTEXT_BYTES)
    )
    serialize_blast_radius_context(context)
    return context
