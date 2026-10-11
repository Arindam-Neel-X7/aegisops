from __future__ import annotations

import uuid

from app.correlation.topology import (
    BoundedTopologyNeighborhood,
    DirectedTopologyEdge,
    TopologyPath,
    TopologySnapshot,
    TopologyServiceNode,
    TopologyVersion,
    TraversalDirection,
    build_neighborhood,
    check_expected_version,
    find_snapshot_paths,
)
from app.simulator.topology.canonical import build_canonical_topology, ServiceTopology


class CanonicalTopologyProvider:
    """TopologyProvider backed by the frozen canonical simulation topology."""

    def __init__(self, topology: ServiceTopology | None = None) -> None:
        self._topology = (
            topology if topology is not None else build_canonical_topology()
        )
        nodes = [
            TopologyServiceNode(
                service_id=n.service_id,
                name=n.name,
                service_type=n.service_type,
            )
            for n in self._topology.services
        ]
        edges = [
            DirectedTopologyEdge(
                upstream_service_id=e.upstream_service_id,
                downstream_service_id=e.downstream_service_id,
                dependency_type=e.dependency_type,
            )
            for e in self._topology.dependencies
        ]
        version = TopologyVersion(
            topology_id=self._topology.topology_id,
            topology_version="1.0.0",
        )
        self._snapshot = TopologySnapshot.create(
            version=version,
            nodes=nodes,
            edges=edges,
        )

    async def get_snapshot(
        self, *, expected_version: str | None = None
    ) -> TopologySnapshot:
        check_expected_version(
            self._snapshot.version.topology_version, expected_version
        )
        return self._snapshot

    async def get_neighborhood(
        self,
        *,
        service_id: uuid.UUID,
        max_hops: int,
        direction: TraversalDirection,
        expected_version: str | None = None,
    ) -> BoundedTopologyNeighborhood:
        check_expected_version(
            self._snapshot.version.topology_version, expected_version
        )
        return build_neighborhood(
            self._snapshot,
            root_service_id=service_id,
            max_hops=max_hops,
            direction=direction,
        )

    async def find_paths(
        self,
        *,
        source_service_id: uuid.UUID,
        target_service_id: uuid.UUID,
        max_hops: int,
        direction: TraversalDirection,
        expected_version: str | None = None,
    ) -> tuple[TopologyPath, ...]:
        check_expected_version(
            self._snapshot.version.topology_version, expected_version
        )
        return find_snapshot_paths(
            self._snapshot,
            source_service_id=source_service_id,
            target_service_id=target_service_id,
            max_hops=max_hops,
            direction=direction,
        )

    async def close(self) -> None:
        pass
