from __future__ import annotations

import asyncio
import re
import uuid
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import AuthError, Neo4jError, ServiceUnavailable

from app.correlation.errors import (
    TopologyContractError,
    TopologyError,
    TopologyProviderError,
    TopologyQueryLimitError,
    TopologyVersionMismatchError,
)
from app.correlation.topology import (
    MAX_TOPOLOGY_EDGES,
    MAX_TOPOLOGY_NODES,
    MAX_TOPOLOGY_VERSION_LEN,
    BoundedTopologyNeighborhood,
    DirectedTopologyEdge,
    TopologyPath,
    TopologyServiceNode,
    TopologySnapshot,
    TopologyVersion,
    TraversalDirection,
    build_neighborhood,
    check_expected_version,
    find_snapshot_paths,
)
from app.simulator.interfaces import DependencyType, ServiceType

_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _validate_topology_id(value: Any) -> uuid.UUID:
    if isinstance(value, bool):
        raise TopologyContractError("Invalid topology identity")
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str):
        raise TopologyContractError("Invalid topology identity")
    trimmed = value.strip()
    if not _UUID_PATTERN.match(trimmed.lower()):
        raise TopologyContractError("Invalid topology identity")
    return uuid.UUID(trimmed)


def _validate_topology_version(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise TopologyContractError("Invalid topology version")
    trimmed = value.strip()
    if not trimmed or len(trimmed) > MAX_TOPOLOGY_VERSION_LEN:
        raise TopologyContractError("Invalid topology version")
    return str(trimmed)


class Neo4jTopologyProvider:
    """TopologyProvider backed by a real Neo4j database."""

    def __init__(
        self,
        driver: AsyncDriver | None = None,
        *,
        uri: str | None = None,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        timeout_seconds: float = 5.0,
        topology_id: uuid.UUID | str | None = None,
        topology_version: str | None = None,
    ) -> None:
        self._database = database or "neo4j"
        self._timeout = timeout_seconds
        if topology_id is None:
            raise TopologyContractError("Invalid topology identity")
        if topology_version is None:
            raise TopologyContractError("Invalid topology version")
        self._topology_id = _validate_topology_id(topology_id)
        self._topology_version = _validate_topology_version(topology_version)

        if driver is not None:
            self._driver = driver
            self._owns_driver = False
        else:
            if not uri or not username or not password:
                raise TopologyProviderError(
                    "Missing Neo4j connection parameters or driver"
                )
            try:
                self._driver = AsyncGraphDatabase.driver(
                    uri,
                    auth=(username, password),
                    connection_timeout=timeout_seconds,
                    max_transaction_retry_time=timeout_seconds,
                )
                self._owns_driver = True
            except Exception as exc:
                raise TopologyProviderError(
                    "Failed to initialize Neo4j driver"
                ) from exc

    async def get_snapshot(
        self, *, expected_version: str | None = None
    ) -> TopologySnapshot:
        try:
            nodes, edges, version_info = await self._fetch_raw_topology()
        except (TopologyError, TopologyContractError, TopologyVersionMismatchError):
            raise
        except Exception as exc:
            raise TopologyProviderError("Failed to fetch topology from Neo4j") from exc

        actual_version = version_info.topology_version
        check_expected_version(actual_version, expected_version)

        try:
            return TopologySnapshot.create(
                version=version_info,
                nodes=nodes,
                edges=edges,
            )
        except (TopologyContractError, TopologyError) as exc:
            raise exc
        except Exception as exc:
            raise TopologyContractError(
                "Malformed data prevents snapshot construction"
            ) from exc

    async def get_neighborhood(
        self,
        *,
        service_id: uuid.UUID,
        max_hops: int,
        direction: TraversalDirection,
        expected_version: str | None = None,
    ) -> BoundedTopologyNeighborhood:
        snapshot = await self.get_snapshot(expected_version=expected_version)
        return build_neighborhood(
            snapshot,
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
        snapshot = await self.get_snapshot(expected_version=expected_version)
        return find_snapshot_paths(
            snapshot,
            source_service_id=source_service_id,
            target_service_id=target_service_id,
            max_hops=max_hops,
            direction=direction,
        )

    async def _fetch_raw_topology(
        self,
    ) -> tuple[list[TopologyServiceNode], list[DirectedTopologyEdge], TopologyVersion]:
        node_query = (
            "MATCH (s:Service) "
            "WHERE s.topology_id = $topology_id "
            "AND s.topology_version = $topology_version "
            "RETURN s.service_id AS service_id, "
            "       s.name AS name, "
            "       s.service_type AS service_type, "
            "       s.topology_id AS topology_id, "
            "       s.topology_version AS topology_version "
            "ORDER BY s.service_id ASC"
        )
        edge_query = (
            "MATCH (u:Service)-[r:DEPENDS_ON]->(d:Service) "
            "WHERE r.topology_id = $topology_id "
            "AND r.topology_version = $topology_version "
            "AND u.topology_id = $topology_id "
            "AND u.topology_version = $topology_version "
            "AND d.topology_id = $topology_id "
            "AND d.topology_version = $topology_version "
            "RETURN u.service_id AS upstream_service_id, "
            "       d.service_id AS downstream_service_id, "
            "       r.dependency_type AS dependency_type, "
            "       r.topology_id AS topology_id, "
            "       r.topology_version AS topology_version "
            "ORDER BY u.service_id ASC, d.service_id ASC"
        )
        params = {
            "topology_id": str(self._topology_id),
            "topology_version": self._topology_version,
        }

        try:
            async with self._driver.session(database=self._database) as session:

                async def work(tx: Any) -> tuple[list[Any], list[Any]]:
                    node_res = await tx.run(node_query, params)
                    nodes_data = await node_res.data()
                    edge_res = await tx.run(edge_query, params)
                    edges_data = await edge_res.data()
                    return nodes_data, edges_data

                nodes_data, edges_data = await asyncio.wait_for(
                    session.execute_read(work), timeout=self._timeout
                )
        except asyncio.TimeoutError as exc:
            raise TopologyProviderError("Neo4j query timed out") from exc
        except (ServiceUnavailable, AuthError, Neo4jError) as exc:
            raise TopologyProviderError("Neo4j execution failed") from exc
        except Exception as exc:
            raise TopologyProviderError("Neo4j query failed") from exc

        if len(nodes_data) > MAX_TOPOLOGY_NODES:
            raise TopologyQueryLimitError("Topology service node limit exceeded")
        if len(edges_data) > MAX_TOPOLOGY_EDGES:
            raise TopologyQueryLimitError("Topology dependency edge limit exceeded")

        nodes: list[TopologyServiceNode] = []
        topology_id: uuid.UUID | None = None
        topology_version: str | None = None

        for row in nodes_data:
            for key in (
                "service_id",
                "name",
                "service_type",
                "topology_id",
                "topology_version",
            ):
                if key not in row or row[key] is None:
                    raise TopologyContractError("Malformed topology row")
            try:
                node_sid = uuid.UUID(row["service_id"])
                node_tid = uuid.UUID(row["topology_id"])
                node_type = ServiceType(row["service_type"])
            except ValueError as exc:
                raise TopologyContractError("Malformed topology row") from exc

            if topology_id is None:
                topology_id = node_tid
                topology_version = row["topology_version"]
            elif topology_id != node_tid or topology_version != row["topology_version"]:
                raise TopologyContractError("Managed topology identity mismatch")

            nodes.append(
                TopologyServiceNode(
                    service_id=node_sid,
                    name=row["name"],
                    service_type=node_type,
                )
            )

        if topology_id is None or topology_version is None:
            raise TopologyProviderError("Managed topology not found")
        if topology_id != self._topology_id:
            raise TopologyContractError("Managed topology identity mismatch")
        if topology_version != self._topology_version:
            raise TopologyContractError("Managed topology identity mismatch")

        edges: list[DirectedTopologyEdge] = []
        for row in edges_data:
            for key in (
                "upstream_service_id",
                "downstream_service_id",
                "dependency_type",
                "topology_id",
                "topology_version",
            ):
                if key not in row or row[key] is None:
                    raise TopologyContractError("Malformed topology edge row")
            try:
                edge_usid = uuid.UUID(row["upstream_service_id"])
                edge_dsid = uuid.UUID(row["downstream_service_id"])
                edge_tid = uuid.UUID(row["topology_id"])
                edge_dep_type = DependencyType(row["dependency_type"])
            except ValueError as exc:
                raise TopologyContractError("Malformed topology edge row") from exc

            if edge_tid != topology_id or row["topology_version"] != topology_version:
                raise TopologyContractError("Managed topology identity mismatch")

            edges.append(
                DirectedTopologyEdge(
                    upstream_service_id=edge_usid,
                    downstream_service_id=edge_dsid,
                    dependency_type=edge_dep_type,
                )
            )

        version_info = TopologyVersion(
            topology_id=topology_id,
            topology_version=topology_version,
        )
        return nodes, edges, version_info

    async def close(self) -> None:
        if self._owns_driver:
            try:
                await self._driver.close()
            except Exception as exc:
                raise TopologyProviderError("Failed to close Neo4j driver") from exc
