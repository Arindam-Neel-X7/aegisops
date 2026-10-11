from __future__ import annotations

import os
import uuid

import pytest

from app.correlation.errors import TopologyVersionMismatchError
from app.correlation.topology import (
    TraversalDirection,
    serialize_topology_snapshot,
)
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.correlation.topology_neo4j import Neo4jTopologyProvider
from app.simulator.topology.canonical import (
    CANONICAL_TOPOLOGY_VERSION,
    canonical_service_id,
    canonical_topology_id,
)

_MANAGED_TOPOLOGY_ID = str(canonical_topology_id())
_MANAGED_TOPOLOGY_VERSION = CANONICAL_TOPOLOGY_VERSION


def _connection() -> dict[str, object]:
    return {
        "uri": os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
        "username": os.getenv("NEO4J_USER", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "change-this-development-password"),
        "database": os.getenv("NEO4J_DATABASE", "neo4j"),
        "timeout_seconds": 15.0,
        "topology_id": canonical_topology_id(),
        "topology_version": CANONICAL_TOPOLOGY_VERSION,
    }


@pytest.mark.asyncio
@pytest.mark.integration
async def test_neo4j_is_healthy_and_authenticated() -> None:
    conn = _connection()
    provider = Neo4jTopologyProvider(**conn)  # type: ignore[arg-type]
    try:
        snapshot = await provider.get_snapshot()
        assert snapshot.version.topology_id == canonical_topology_id()
        assert snapshot.version.topology_version == CANONICAL_TOPOLOGY_VERSION
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_managed_database_counts_are_exactly_seven_and_seven() -> None:
    conn = _connection()
    provider = Neo4jTopologyProvider(**conn)  # type: ignore[arg-type]
    try:
        async with provider._driver.session(database=provider._database) as session:
            node_result = await session.run(
                "MATCH (s:Service) "
                "WHERE s.topology_id = $tid AND s.topology_version = $tver "
                "RETURN count(s) AS c",
                {"tid": _MANAGED_TOPOLOGY_ID, "tver": _MANAGED_TOPOLOGY_VERSION},
            )
            node_rows = await node_result.data()
            edge_result = await session.run(
                "MATCH ()-[r:DEPENDS_ON]->() "
                "WHERE r.topology_id = $tid AND r.topology_version = $tver "
                "RETURN count(r) AS c",
                {"tid": _MANAGED_TOPOLOGY_ID, "tver": _MANAGED_TOPOLOGY_VERSION},
            )
            edge_rows = await edge_result.data()
        assert node_rows == [{"c": 7}]
        assert edge_rows == [{"c": 7}]
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_canonical_and_neo4j_parity() -> None:
    canonical = CanonicalTopologyProvider()
    neo4j = Neo4jTopologyProvider(**_connection())  # type: ignore[arg-type]
    try:
        canonical_snapshot = await canonical.get_snapshot()
        neo4j_snapshot = await neo4j.get_snapshot()

        # Complete object equality.
        assert canonical_snapshot == neo4j_snapshot
        # Identical serialized bytes.
        assert serialize_topology_snapshot(
            canonical_snapshot
        ) == serialize_topology_snapshot(neo4j_snapshot)
        # Identical fingerprints.
        assert (
            canonical_snapshot.snapshot_fingerprint
            == neo4j_snapshot.snapshot_fingerprint
        )
        # Exactly 7 nodes and 7 edges through the provider.
        assert len(neo4j_snapshot.nodes) == 7
        assert len(neo4j_snapshot.edges) == 7
    finally:
        await canonical.close()
        await neo4j.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_unrelated_topology_contamination_is_excluded() -> None:
    foreign_id = uuid.uuid5(uuid.NAMESPACE_DNS, "aegisops.c1.foreign-topology")
    foreign_node = uuid.uuid5(uuid.NAMESPACE_DNS, "aegisops.c1.foreign-service")
    managed_anchor = canonical_service_id("client")
    canonical = CanonicalTopologyProvider()
    neo4j = Neo4jTopologyProvider(**_connection())  # type: ignore[arg-type]
    try:
        async with neo4j._driver.session(database=neo4j._database) as session:
            result = await session.run(
                "MERGE (s:Service {service_id: $sid}) "
                "SET s.name = $name, s.service_type = $stype, "
                "s.topology_id = $tid, s.topology_version = $tver",
                {
                    "sid": str(foreign_node),
                    "name": "c1-foreign-service",
                    "stype": "api",
                    "tid": str(foreign_id),
                    "tver": "9.9.9",
                },
            )
            await result.consume()
            edge_result = await session.run(
                "MATCH (u:Service {service_id: $anchor}), "
                "(d:Service {service_id: $foreign}) "
                "MERGE (u)-[r:DEPENDS_ON]->(d) "
                "SET r.dependency_type = $dtype, "
                "r.topology_id = $tid, r.topology_version = $tver",
                {
                    "anchor": str(managed_anchor),
                    "foreign": str(foreign_node),
                    "dtype": "sync",
                    "tid": str(foreign_id),
                    "tver": "9.9.9",
                },
            )
            await edge_result.consume()

        canonical_snapshot = await canonical.get_snapshot()
        managed_snapshot = await neo4j.get_snapshot()
        assert managed_snapshot == canonical_snapshot
        assert serialize_topology_snapshot(
            managed_snapshot
        ) == serialize_topology_snapshot(canonical_snapshot)
        assert (
            managed_snapshot.snapshot_fingerprint
            == canonical_snapshot.snapshot_fingerprint
        )
        assert "c1-foreign-service" not in {n.name for n in managed_snapshot.nodes}
        assert foreign_node not in {n.service_id for n in managed_snapshot.nodes}
    finally:
        try:
            async with neo4j._driver.session(database=neo4j._database) as session:
                delete_edges = await session.run(
                    "MATCH ()-[r:DEPENDS_ON]->() "
                    "WHERE r.topology_id = $tid DELETE r",
                    {"tid": str(foreign_id)},
                )
                await delete_edges.consume()
                delete_nodes = await session.run(
                    "MATCH (s:Service) "
                    "WHERE s.topology_id = $tid "
                    "DETACH DELETE s",
                    {"tid": str(foreign_id)},
                )
                await delete_nodes.consume()
        finally:
            await canonical.close()
            await neo4j.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_canonical_and_neo4j_neighborhoods_match_every_service() -> None:
    canonical = CanonicalTopologyProvider()
    neo4j = Neo4jTopologyProvider(**_connection())  # type: ignore[arg-type]
    try:
        snapshot = await canonical.get_snapshot()
        for node in snapshot.nodes:
            for direction in (
                TraversalDirection.DOWNSTREAM,
                TraversalDirection.UPSTREAM,
                TraversalDirection.BOTH,
            ):
                for hops in (0, 1, 2, 5, 32):
                    expected = await canonical.get_neighborhood(
                        service_id=node.service_id,
                        max_hops=hops,
                        direction=direction,
                    )
                    actual = await neo4j.get_neighborhood(
                        service_id=node.service_id,
                        max_hops=hops,
                        direction=direction,
                    )
                    assert actual == expected
    finally:
        await canonical.close()
        await neo4j.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_canonical_and_neo4j_paths_match() -> None:
    canonical = CanonicalTopologyProvider()
    neo4j = Neo4jTopologyProvider(**_connection())  # type: ignore[arg-type]
    try:
        client = canonical_service_id("client")
        database = canonical_service_id("database")
        order = canonical_service_id("order-service")
        notification = canonical_service_id("notification-service")

        cases = [
            (client, database, 32, TraversalDirection.DOWNSTREAM),
            (database, client, 32, TraversalDirection.UPSTREAM),
            (client, database, 32, TraversalDirection.BOTH),
            (client, client, 5, TraversalDirection.DOWNSTREAM),  # zero-hop
            (order, notification, 1, TraversalDirection.DOWNSTREAM),
            (database, client, 32, TraversalDirection.DOWNSTREAM),  # unreachable
        ]
        for source, target, hops, direction in cases:
            expected = await canonical.find_paths(
                source_service_id=source,
                target_service_id=target,
                max_hops=hops,
                direction=direction,
            )
            actual = await neo4j.find_paths(
                source_service_id=source,
                target_service_id=target,
                max_hops=hops,
                direction=direction,
            )
            assert actual == expected

        reachable = await neo4j.find_paths(
            source_service_id=client,
            target_service_id=database,
            max_hops=32,
            direction=TraversalDirection.DOWNSTREAM,
        )
        assert len(reachable) > 0
    finally:
        await canonical.close()
        await neo4j.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_expected_version_mismatch_raises_typed_error() -> None:
    neo4j = Neo4jTopologyProvider(**_connection())  # type: ignore[arg-type]
    try:
        with pytest.raises(TopologyVersionMismatchError):
            await neo4j.get_snapshot(expected_version="9.9.9")
        ok = await neo4j.get_snapshot(expected_version="1.0.0")
        assert ok.version.topology_version == "1.0.0"
    finally:
        await neo4j.close()
