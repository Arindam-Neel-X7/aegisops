"""V1 real Neo4j fixtures, canonical parity and immutable PostgreSQL readback.

Only disposable topology namespaces and the approved PostgreSQL fixture write.
Expected paths/counters come from frozen A1 examples, not a second traversal.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
import json
import os
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text

from app.correlation import CorrelationWindowState, persist_incident_atomic
from app.correlation.blast_radius import compute_context_fingerprint
from app.correlation.errors import TopologyVersionMismatchError
from app.correlation.handoff import (
    Phase5HandoffContractError,
    build_phase5_handoff,
    prepare_phase5_handoff,
    serialize_phase5_handoff,
    validate_phase5_handoff,
)
from app.correlation.persistence_repository import (
    SqlAlchemyIncidentPersistenceRepository,
)
from app.correlation.serialization import serialize_incident_aggregate
from app.correlation.topology import TopologySnapshot, serialize_topology_snapshot
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.correlation.topology_neo4j import Neo4jTopologyProvider
from test_correlation_blast_radius import _calculator_inputs, context_payload
from test_correlation_persistence_service_integration import (
    _make_group,
    _make_input,
    _make_member,
    _take_snapshot,
    _TENANT_A,
    _TIME1,
    _TIME2,
    _TIME3,
    pg_env as pg_env,
    runtime_factory as runtime_factory,
)

pytestmark = pytest.mark.integration


def _provider(snapshot: TopologySnapshot) -> Neo4jTopologyProvider:
    return Neo4jTopologyProvider(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        username=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "change-this-development-password"),
        database=os.getenv("NEO4J_DATABASE", "neo4j"),
        topology_id=snapshot.version.topology_id,
        topology_version=snapshot.version.topology_version,
        timeout_seconds=15.0,
    )


def _validate(raw: bytes, args: dict[str, Any]) -> Any:
    return validate_phase5_handoff(
        raw=raw,
        snapshot=args["snapshot"],
        expected_topology=args["expected_topology"],
        expected_policy=args["policy"],
    )


@pytest_asyncio.fixture
async def graph_case(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[dict[str, Any], None]:
    case_id = getattr(request, "param", "F06")
    args = _calculator_inputs(case_id)
    original = args["snapshot"]
    tid = uuid4()
    base = uuid4().int & ~0xFFFF
    # Preserve the approved UUID order, hence the canonical witness tie breaks.
    mapping = {
        n.service_id: UUID(int=base + i)
        for i, n in enumerate(
            sorted(original.nodes, key=lambda n: str(n.service_id)), start=1
        )
    }
    snapshot = TopologySnapshot.create(
        version=original.version.model_copy(
            update={"topology_id": tid, "topology_version": f"v1/{case_id}"}
        ),
        nodes=tuple(
            n.model_copy(update={"service_id": mapping[n.service_id]})
            for n in original.nodes
        ),
        edges=tuple(
            e.model_copy(
                update={
                    "upstream_service_id": mapping[e.upstream_service_id],
                    "downstream_service_id": mapping[e.downstream_service_id],
                }
            )
            for e in original.edges
        ),
    )
    pin = args["expected_topology"].model_copy(
        update={
            "topology_id": tid,
            "topology_version": snapshot.version.topology_version,
            "snapshot_fingerprint": snapshot.snapshot_fingerprint,
        }
    )
    replacements = {str(k): str(v) for k, v in mapping.items()}

    def remap(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: remap(v) for k, v in value.items()}
        if isinstance(value, list):
            return [remap(v) for v in value]
        return replacements.get(value, value) if isinstance(value, str) else value

    expected = remap(context_payload(case_id))
    expected["topology_snapshot"] = pin.model_dump(mode="json")
    expected["context_fingerprint"] = compute_context_fingerprint(expected)
    args.update(snapshot=snapshot, expected_topology=pin)
    provider = _provider(snapshot)
    created = False
    try:
        async with provider._driver.session(database=provider._database) as session:
            ids = [str(n.service_id) for n in snapshot.nodes]
            collision = await (
                await session.run(
                    "MATCH (s:Service) WHERE s.topology_id=$tid OR s.service_id IN $ids RETURN count(s) AS n",
                    tid=str(tid),
                    ids=ids,
                )
            ).single()
            assert collision["n"] == 0

            async def seed(tx: Any) -> None:
                nodes = [
                    {
                        **n.model_dump(mode="json"),
                        "topology_id": str(tid),
                        "topology_version": snapshot.version.topology_version,
                    }
                    for n in snapshot.nodes
                ]
                result = await tx.run(
                    "UNWIND $nodes AS row CREATE (s:Service) SET s = row", nodes=nodes
                )
                await result.consume()
                edges = [e.model_dump(mode="json") for e in snapshot.edges]
                result = await tx.run(
                    "UNWIND $edges AS row MATCH (u:Service {service_id:row.upstream_service_id}), "
                    "(d:Service {service_id:row.downstream_service_id}) "
                    "CREATE (u)-[r:DEPENDS_ON]->(d) SET r.dependency_type=row.dependency_type, "
                    "r.topology_id=$tid,r.topology_version=$version",
                    edges=edges,
                    tid=str(tid),
                    version=snapshot.version.topology_version,
                )
                await result.consume()

            await session.execute_write(seed)
            created = True
        fetched = await provider.get_snapshot(expected_version=pin.topology_version)
        assert serialize_topology_snapshot(fetched) == serialize_topology_snapshot(
            snapshot
        )
        print(
            "V1_NEO4J_PIN",
            json.dumps(
                dict(
                    case=case_id,
                    topology_id=str(tid),
                    fingerprint=fetched.snapshot_fingerprint,
                )
            ),
        )
        yield dict(args=args, expected=expected, provider=provider)
    finally:
        try:
            if created:
                async with provider._driver.session(
                    database=provider._database
                ) as session:
                    result = await session.run(
                        "MATCH (s:Service {topology_id:$tid}) DETACH DELETE s",
                        tid=str(tid),
                    )
                    await result.consume()
                    row = await (
                        await session.run(
                            "MATCH (s:Service {topology_id:$tid}) RETURN count(s) AS n",
                            tid=str(tid),
                        )
                    ).single()
                    rel = await (
                        await session.run(
                            "MATCH ()-[r:DEPENDS_ON {topology_id:$tid}]->() RETURN count(r) AS n",
                            tid=str(tid),
                        )
                    ).single()
                    assert row["n"] == rel["n"] == 0
                    print(
                        "V1_NEO4J_CLEANUP",
                        json.dumps(
                            dict(
                                case=case_id,
                                topology_id=str(tid),
                                nodes=row["n"],
                                edges=rel["n"],
                            )
                        ),
                    )
        finally:
            await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "graph_case", [f"F{i:02d}" for i in range(1, 17)], indirect=True
)
async def test_real_neo4j_matches_every_static_a1_fixture(
    graph_case: dict[str, Any],
) -> None:
    args = graph_case["args"]
    before = serialize_incident_aggregate(args["incident"])
    snapshot_bytes = serialize_topology_snapshot(args["snapshot"])
    result = await prepare_phase5_handoff(
        **{k: v for k, v in args.items() if k != "snapshot"},
        provider=graph_case["provider"],
    )
    assert result.topology_context.model_dump(mode="json") == graph_case["expected"]
    assert result.provenance.source_kind == "neo4j"
    assert (
        result.provenance.provider_class
        == "app.correlation.topology_neo4j.Neo4jTopologyProvider"
    )
    raw = serialize_phase5_handoff(result)
    assert serialize_phase5_handoff(_validate(raw, args)) == raw
    assert serialize_incident_aggregate(result.incident) == before
    assert serialize_incident_aggregate(args["incident"]) == before
    assert serialize_topology_snapshot(args["snapshot"]) == snapshot_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize("case_id", ["F01", "F02", "F03", "F12"])
async def test_real_managed_neo4j_and_canonical_handoff_parity(case_id: str) -> None:
    args = _calculator_inputs(case_id)
    canonical = CanonicalTopologyProvider()
    neo4j = _provider(args["snapshot"])
    try:
        snapshot = await canonical.get_snapshot(
            expected_version=args["expected_topology"].topology_version
        )
        assert serialize_topology_snapshot(snapshot) == serialize_topology_snapshot(
            args["snapshot"]
        )
        values = {k: v for k, v in args.items() if k != "snapshot"}
        left = await prepare_phase5_handoff(**values, provider=canonical)
        right = await prepare_phase5_handoff(**values, provider=neo4j)
        assert left.topology_context.model_dump(mode="json") == context_payload(case_id)
        assert left.topology_context == right.topology_context
        assert (
            left.handoff_id != right.handoff_id
        )  # actual provider attribution is bound
        assert _validate(serialize_phase5_handoff(right), args) == right
    finally:
        await canonical.close()
        await neo4j.close()


@pytest.mark.asyncio
async def test_real_neo4j_rejects_stale_version(graph_case: dict[str, Any]) -> None:
    args = graph_case["args"]
    before = await graph_case["provider"].get_snapshot()
    bad = args["expected_topology"].model_copy(update={"topology_version": "stale-v1"})
    with pytest.raises(Phase5HandoffContractError) as error:
        await prepare_phase5_handoff(
            incident=args["incident"],
            provider=graph_case["provider"],
            expected_topology=bad,
            policy=args["policy"],
            as_of=args["as_of"],
        )
    assert isinstance(error.value.__cause__, TopologyVersionMismatchError)
    assert await graph_case["provider"].get_snapshot() == before


@pytest.mark.asyncio
async def test_real_neo4j_rejects_changed_content_under_same_version(
    graph_case: dict[str, Any],
) -> None:
    from app.correlation.blast_radius import BlastRadiusTopologyMismatchError

    args = graph_case["args"]
    provider = graph_case["provider"]
    by_name = {n.name: str(n.service_id) for n in args["snapshot"].nodes}
    async with provider._driver.session(database=provider._database) as session:
        result = await session.run(
            "MATCH (u:Service {service_id:$source}),(d:Service {service_id:$target}) "
            "CREATE (u)-[r:DEPENDS_ON]->(d) SET r.topology_id=$tid,r.topology_version=$version,r.dependency_type='sync'",
            source=by_name["A"],
            target=by_name["D"],
            tid=str(args["expected_topology"].topology_id),
            version=args["expected_topology"].topology_version,
        )
        await result.consume()
    changed = await provider.get_snapshot()
    assert changed.version == args["snapshot"].version
    assert (
        changed.snapshot_fingerprint != args["expected_topology"].snapshot_fingerprint
    )
    with pytest.raises(Phase5HandoffContractError) as error:
        await prepare_phase5_handoff(
            **{k: v for k, v in args.items() if k != "snapshot"}, provider=provider
        )
    assert isinstance(error.value.__cause__, BlastRadiusTopologyMismatchError)
    assert await provider.get_snapshot() == changed


@pytest.mark.asyncio
async def test_real_postgres_readback_enrichment_preserves_five_tables(
    pg_env: dict[str, Any],  # noqa: F811 — reuse the approved imported pytest fixture
    runtime_factory: Any,  # noqa: F811 — reuse the approved imported pytest fixture
) -> None:
    members = [
        _make_member(uuid4(), when, "order-service", str(i) * 64)
        for i, when in [(1, _TIME1), (2, _TIME2)]
    ]
    inputs = {
        m.signal_id: _make_input(
            m.signal_id, m.event_time, m.service, m.semantic_fingerprint
        )
        for m in members
    }
    persisted = await persist_incident_atomic(
        group=_make_group(members),
        signal_inputs=inputs,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME3,
        tenant_id=_TENANT_A,
        session_factory=runtime_factory,
    )
    import asyncpg

    conn = await asyncpg.connect(pg_env["disp_admin_dsn"])
    try:
        before = await _take_snapshot(conn)
        async with runtime_factory() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                repository = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
                await repository.bind_tenant_context()
                assert (
                    await session.execute(text("SHOW transaction_read_only"))
                ).scalar_one() == "on"
                stored = await repository.load_incident(persisted.aggregate.incident_id)
                assert stored is not None
        incident = stored.aggregate
        original = serialize_incident_aggregate(incident)
        assert original == serialize_incident_aggregate(persisted.aggregate)
        args = _calculator_inputs()
        args["incident"] = incident
        direct = build_phase5_handoff(**args)
        provider = _provider(args["snapshot"])
        try:
            enriched = await prepare_phase5_handoff(
                **{k: v for k, v in args.items() if k != "snapshot"}, provider=provider
            )
        finally:
            await provider.close()
        assert direct.topology_context.model_dump(mode="json") == context_payload()
        assert direct.topology_context == enriched.topology_context
        raw = serialize_phase5_handoff(enriched)
        assert serialize_phase5_handoff(_validate(raw, args)) == raw
        assert serialize_incident_aggregate(enriched.incident) == original
        assert serialize_incident_aggregate(incident) == original
        assert (
            enriched.incident.topology_snapshot is None
            and enriched.incident.blast_radius is None
        )
        assert enriched.incident.revision == persisted.aggregate.revision
        assert (
            enriched.incident.revision_fingerprint
            == persisted.aggregate.revision_fingerprint
        )
        after = await _take_snapshot(conn)
        assert after == before
        print(
            "V1_POSTGRES_IMMUTABILITY",
            json.dumps(
                dict(
                    incident_id=str(incident.incident_id),
                    revision=incident.revision,
                    revision_fingerprint=incident.revision_fingerprint,
                    counts_before=before.counts,
                    counts_after=after.counts,
                    hashes_before=before.hashes,
                    hashes_after=after.hashes,
                    read_only=True,
                )
            ),
        )
    finally:
        await conn.close()
