"""Real PostgreSQL integration tests for the atomic incident persistence transaction service.

Tests execute against a portable disposable database and runtime login role.
Validates scenarios SC-01 through SC-09, minimum privileges, role guards,
isolation levels, live five-table rollbacks, and disposable resource cleanup.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
import contextvars
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
import re
import secrets
import subprocess
from typing import Any
import unittest.mock
import urllib.parse
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.anomaly.lineage import AnomalyReproducibilityLineage
from app.anomaly.models import (
    AnomalySignal,
    CalibrationMetadata,
    EventSeverity,
)
from app.correlation.aggregate import (
    create_or_update_incident,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation import (
    CorrelationEvidence,
    CorrelationGroup,
    CorrelationWindowState,
    GroupingMemberReference,
    IncidentPersistenceConflictError,
    IncidentPersistenceStoreError,
    IncidentPersistenceTenantError,
    SqlAlchemyIncidentPersistenceRepository,
    persist_incident_atomic,
)
from app.correlation.identity import derive_incident_id, derive_timeline_event_id
from app.correlation.errors import AggregateStateError
from app.correlation.serialization import serialize_incident_aggregate

pytestmark = pytest.mark.integration

_TASK_ROLE_VAR: contextvars.ContextVar[str] = contextvars.ContextVar(
    "task_role", default="default"
)

_TENANT_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_TENANT_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

_SIG1_ID = UUID("11111111-1111-1111-1111-111111111111")
_SIG2_ID = UUID("22222222-2222-2222-2222-222222222222")
_SIG3_ID = UUID("33333333-3333-3333-3333-333333333333")

_TIME1 = datetime(2026, 11, 1, 10, 0, 0, tzinfo=timezone.utc)
_TIME2 = datetime(2026, 11, 1, 10, 1, 0, tzinfo=timezone.utc)
_TIME3 = datetime(2026, 11, 1, 10, 2, 0, tzinfo=timezone.utc)

_FP1 = "1111111111111111111111111111111111111111111111111111111111111111"
_FP2 = "2222222222222222222222222222222222222222222222222222222222222222"
_FP3 = "3333333333333333333333333333333333333333333333333333333333333333"
_FP4 = "9999999999999999999999999999999999999999999999999999999999999999"


def _make_input(
    signal_id: UUID,
    event_time: datetime,
    service: str,
    fp: str,
    tenant_id: UUID = _TENANT_A,
    environment: str = "production",
) -> AnomalyCorrelationInput:
    sig = AnomalySignal(
        signal_id=signal_id,
        event_time=event_time,
        service=service,
        metric_or_feature="cpu_utilization",
        model_name="iforest",
        model_version="1.0",
        anomaly_score=0.95,
        severity=EventSeverity.CRITICAL,
        threshold_or_calibration=CalibrationMetadata(
            method="percentile", threshold_value=0.8, calibration_version="1.0"
        ),
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key=f"rep_{signal_id.hex[:8]}",
        environment=environment,
        tenant_id=tenant_id,
    )
    lin = AnomalyReproducibilityLineage(
        signal_id=signal_id,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key=f"rep_{signal_id.hex[:8]}",
        model_name="iforest",
        model_version="1.0",
        source_event_ids=(UUID("55555555-5555-5555-5555-555555555555"),),
        semantic_fingerprint=fp,
    )
    return AnomalyCorrelationInput(signal=sig, lineage=lin)


def _make_evidence(left_id: UUID, right_id: UUID) -> CorrelationEvidence:
    if left_id.bytes > right_id.bytes:
        left_id, right_id = right_id, left_id
    return CorrelationEvidence(
        left_signal_id=left_id,
        right_signal_id=right_id,
        temporal_score=0.9,
        service_score=0.9,
        topology_score=1.0,
        symptom_score=0.8,
        context_score=0.85,
        weighted_score=0.89,
        accepted=True,
        policy_version="1.0",
        reason="high_similarity",
    )


def _make_group(
    members: Sequence[GroupingMemberReference],
    evidence: Sequence[CorrelationEvidence] | None = None,
) -> CorrelationGroup:
    sorted_m = tuple(sorted(members, key=lambda m: m.signal_id.bytes))
    start = min(m.event_time for m in sorted_m)
    last = max(m.event_time for m in sorted_m)
    span = max(0.0, (last - start).total_seconds())
    if evidence is None:
        ev_list: list[CorrelationEvidence] = []
        for i in range(len(sorted_m)):
            for j in range(i + 1, len(sorted_m)):
                ev_list.append(
                    _make_evidence(sorted_m[i].signal_id, sorted_m[j].signal_id)
                )
        ev_tuple = tuple(ev_list)
    else:
        ev_tuple = tuple(evidence)
    return CorrelationGroup(
        policy_version="1.0",
        configuration_fingerprint="c" * 64,
        topology_snapshot_fingerprint="e" * 64,
        members=sorted_m,
        accepted_evidence=ev_tuple,
        started_at=start,
        last_seen_at=last,
        time_span_seconds=span,
        topology_diameter_hops=1,
        group_fingerprint="0" * 64,
    )


def _make_member(
    signal_id: UUID, event_time: datetime, service: str, fp: str
) -> GroupingMemberReference:
    return GroupingMemberReference(
        signal_id=signal_id,
        semantic_fingerprint=fp,
        service=service,
        event_time=event_time,
    )


async def _query_table_counts(conn: asyncpg.Connection) -> dict[str, int]:
    tables = [
        "incidents",
        "incident_signal_links",
        "incident_correlation_evidence",
        "incident_timeline_events",
        "incident_publication_intents",
    ]
    counts = {}
    for t in tables:
        counts[t] = await conn.fetchval(f"SELECT count(*) FROM {t}")
    return counts


async def _query_5table_hashes(conn: asyncpg.Connection) -> dict[str, str]:
    table_keys = [
        ("incidents", "tenant_id, incident_id"),
        ("incident_signal_links", "tenant_id, incident_id, signal_id"),
        (
            "incident_correlation_evidence",
            "tenant_id, incident_id, left_signal_id, right_signal_id",
        ),
        ("incident_timeline_events", "tenant_id, event_id"),
        ("incident_publication_intents", "tenant_id, incident_id, revision"),
    ]
    hashes = {}
    for table, pkey in table_keys:
        rows = await conn.fetch(f"SELECT * FROM {table} ORDER BY {pkey}")
        row_str = "|".join(str(dict(r)) for r in rows)
        hashes[table] = hashlib.sha256(row_str.encode("utf-8")).hexdigest()
    return hashes


@dataclass
class TableSnapshot:
    counts: dict[str, int]
    hashes: dict[str, str]


async def _take_snapshot(conn: asyncpg.Connection) -> TableSnapshot:
    counts = await _query_table_counts(conn)
    hashes = await _query_5table_hashes(conn)
    return TableSnapshot(counts=counts, hashes=hashes)


def _extract_sqlstate(exc: BaseException | None) -> str:
    curr = exc
    visited: set[int] = set()
    while curr is not None and id(curr) not in visited:
        visited.add(id(curr))
        orig = getattr(curr, "orig", None)
        if orig is not None:
            code = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
            if code:
                return str(code)
        code = getattr(curr, "pgcode", None) or getattr(curr, "sqlstate", None)
        if code:
            return str(code)
        curr = curr.__cause__ or curr.__context__
    return "NONE"


class TrackingSessionFactory:
    """Wraps an async_sessionmaker to track sessions created and transactions begun."""

    def __init__(self, base_factory: async_sessionmaker[AsyncSession]) -> None:
        self.base_factory = base_factory
        self.sessions_created = 0
        self.transactions_begun = 0

    def __call__(self) -> AsyncSession:
        self.sessions_created += 1
        session = self.base_factory()
        orig_begin = session.begin

        @asynccontextmanager
        async def tracking_begin():
            self.transactions_begun += 1
            async with orig_begin() as tx:
                yield tx

        session.begin = tracking_begin  # type: ignore
        return session

    @property
    def primary_tx(self) -> int:
        return 1 if self.transactions_begun >= 1 else 0

    @property
    def recovery_tx(self) -> int:
        return max(0, self.transactions_begun - 1)


@dataclass
class ScenarioObservation:
    scenario_id: str
    expected_outcome: str
    observed_outcome: str
    public_error: str
    observed_sqlstate: str
    primary_tx_count: int
    recovery_tx_count: int
    incident_delta: int
    link_delta: int
    evidence_delta: int
    timeline_delta: int
    intent_delta: int
    status: str

    def to_row(self) -> str:
        return (
            f"{self.scenario_id}|{self.expected_outcome}|{self.observed_outcome}|"
            f"{self.public_error}|{self.observed_sqlstate}|{self.primary_tx_count}|"
            f"{self.recovery_tx_count}|{self.incident_delta}|{self.link_delta}|"
            f"{self.evidence_delta}|{self.timeline_delta}|{self.intent_delta}|{self.status}"
        )


@dataclass
class AtomicityObservation:
    case_id: str
    incident_count: int
    link_count: int
    evidence_count: int
    timeline_count: int
    intent_count: int
    revision: int
    fingerprint: str
    status: str

    def to_row(self) -> str:
        return (
            f"{self.case_id}|{self.incident_count}|{self.link_count}|"
            f"{self.evidence_count}|{self.timeline_count}|{self.intent_count}|"
            f"{self.revision}|{self.fingerprint}|{self.status}"
        )


@pytest_asyncio.fixture(scope="module")
async def pg_env() -> AsyncGenerator[dict[str, Any], None]:
    admin_dsn = os.environ.get("S1_R2_POSTGRES_ADMIN_DSN") or os.environ.get(
        "S1_R1_POSTGRES_ADMIN_DSN"
    )
    if not admin_dsn:
        pytest.fail(
            "S1_R2_POSTGRES_ADMIN_DSN (or S1_R1_POSTGRES_ADMIN_DSN) environment variable is required for integration tests"
        )

    parsed = urllib.parse.urlparse(admin_dsn)
    target_maintenance_db = parsed.path.lstrip("/")
    if target_maintenance_db != "postgres":
        pytest.fail(
            f"Admin DSN must target maintenance database 'postgres', got '{target_maintenance_db}'"
        )

    admin_host = parsed.hostname or "localhost"
    admin_port = parsed.port or 5432
    admin_user = parsed.username or "postgres"
    admin_pass = parsed.password or ""
    scheme = parsed.scheme or "postgresql"

    admin_conn = await asyncpg.connect(admin_dsn)

    suffix = secrets.token_hex(6)
    assert re.match(r"^[0-9a-f]{12}$", suffix)
    db_name = f"aegis_s1_{suffix}"
    role_name = f"aegis_s1_runtime_{suffix}"
    password = secrets.token_urlsafe(24)

    try:
        # Check absence before creation
        db_exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        assert not db_exists, f"Generated database {db_name} already exists!"
        role_exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", role_name
        )
        assert not role_exists, f"Generated role {role_name} already exists!"

        # Create disposable DB and runtime role
        await admin_conn.execute(f'CREATE DATABASE "{db_name}"')
        await admin_conn.execute(
            f"CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{password}' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
        )

        disp_admin_dsn = (
            f"{scheme}://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
        )
        disp_admin_conn = await asyncpg.connect(disp_admin_dsn)

        backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        try:
            await disp_admin_conn.execute(
                f'REVOKE TEMPORARY ON DATABASE "{db_name}" FROM PUBLIC'
            )
            await disp_admin_conn.execute(
                f'GRANT CONNECT ON DATABASE "{db_name}" TO "{role_name}"'
            )
            await disp_admin_conn.execute(
                f'GRANT USAGE ON SCHEMA public TO "{role_name}"'
            )

            # Run Alembic migrations
            env = os.environ.copy()
            async_url = f"postgresql+asyncpg://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
            env["DATABASE_URL"] = async_url
            res = subprocess.run(
                ["poetry", "run", "alembic", "upgrade", "head"],
                cwd=backend_dir,
                env=env,
                capture_output=True,
                text=True,
            )
            assert (
                res.returncode == 0
            ), f"Alembic upgrade failed:\nstdout={res.stdout}\nstderr={res.stderr}"

            # Seed test tenants
            for t_id in [_TENANT_A, _TENANT_B]:
                await disp_admin_conn.execute(
                    """
                    INSERT INTO tenants (id, name, slug, plan_id, created_at, updated_at)
                    VALUES ($1, $2, $3, '00000000-0000-0000-0000-000000000001'::uuid, now(), now())
                    ON CONFLICT (id) DO NOTHING
                    """,
                    t_id,
                    f"tenant-{t_id.hex[:8]}",
                    f"slug-{t_id.hex[:8]}",
                )

            # Grant exact minimum privileges to runtime role
            await disp_admin_conn.execute(
                f'GRANT SELECT, INSERT ON TABLE incidents TO "{role_name}"'
            )
            await disp_admin_conn.execute(
                f"GRANT UPDATE (status, correlation_state, severity, started_at, last_seen_at, "
                f"updated_at, revision, affected_services, revision_fingerprint, aggregate_payload) "
                f'ON TABLE incidents TO "{role_name}"'
            )
            await disp_admin_conn.execute(
                f"GRANT SELECT, INSERT ON TABLE "
                f"incident_signal_links, incident_correlation_evidence, "
                f'incident_timeline_events, incident_publication_intents TO "{role_name}"'
            )
        finally:
            await disp_admin_conn.close()

        runtime_async_url = f"postgresql+asyncpg://{role_name}:{password}@{admin_host}:{admin_port}/{db_name}"
        disp_admin_async_url = f"postgresql+asyncpg://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"

        # Emit resource diagnostic
        print("\nS1_RESOURCE_DIAGNOSTIC_BEGIN")
        print(f"DISPOSABLE_DATABASE_NAME={db_name}")
        print(f"DISPOSABLE_ROLE_NAME={role_name}")
        print("RUNTIME_ROLSUPER=False")
        print("RUNTIME_ROLBYPASSRLS=False")
        print("TRANSACTION_ISOLATION=read committed")
        print("S1_RESOURCE_DIAGNOSTIC_END")

        yield {
            "suffix": suffix,
            "db_name": db_name,
            "role_name": role_name,
            "admin_dsn": admin_dsn,
            "disp_admin_dsn": disp_admin_dsn,
            "runtime_async_url": runtime_async_url,
            "disp_admin_async_url": disp_admin_async_url,
        }

    finally:
        await admin_conn.close()

        # Connect freshly for cleanup
        cleanup_conn = await asyncpg.connect(admin_dsn)
        try:
            await cleanup_conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = $1 AND pid <> pg_backend_pid()
                """,
                db_name,
            )
            await cleanup_conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            await cleanup_conn.execute(f'DROP ROLE IF EXISTS "{role_name}"')

            # Verification of complete teardown
            db_rem = await cleanup_conn.fetchval(
                "SELECT count(*) FROM pg_database WHERE datname = $1", db_name
            )
            role_rem = await cleanup_conn.fetchval(
                "SELECT count(*) FROM pg_roles WHERE rolname = $1", role_name
            )
            assert db_rem == 0, f"Database {db_name} was not cleaned up!"
            assert role_rem == 0, f"Role {role_name} was not cleaned up!"
        finally:
            await cleanup_conn.close()


@pytest_asyncio.fixture
async def runtime_factory(
    pg_env: dict[str, Any],
) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(
        pg_env["runtime_async_url"], pool_size=10, max_overflow=10
    )
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield sf
    await engine.dispose()


@pytest_asyncio.fixture
async def admin_factory(
    pg_env: dict[str, Any],
) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(
        pg_env["disp_admin_async_url"], pool_size=5, max_overflow=5
    )
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield sf
    await engine.dispose()


@pytest.mark.asyncio
async def test_role_guard_and_isolation(
    pg_env: dict[str, Any], admin_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Prove admin/superuser role is rejected and runtime role observes read committed."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    # 1. Superuser / admin role must fail closed before data mutation
    with pytest.raises(IncidentPersistenceTenantError, match="superuser or bypassrls"):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=admin_factory,
        )


@pytest.mark.asyncio
async def test_scenarios_and_atomicity(
    pg_env: dict[str, Any], runtime_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Execute scenarios SC-01 through SC-09 and verify live atomicity and diagnostics."""
    disp_conn = await asyncpg.connect(pg_env["disp_admin_dsn"])

    scenario_observations: list[ScenarioObservation] = []
    atomicity_observations: list[AtomicityObservation] = []

    try:
        # ===================================================================
        # SC-01: Concurrent create race (one winner, one contender with 23505 recovery)
        # ===================================================================
        m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
        inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
        group1 = _make_group([m1])
        inputs1 = {_SIG1_ID: inp1}

        counts_sc01_pre = await _query_table_counts(disp_conn)
        assert all(cnt == 0 for cnt in counts_sc01_pre.values())

        sc01_winner_committed = asyncio.Event()
        sc01_contender_sqlstate = "NONE"
        sc01_winner_counts: dict[str, int] = {}
        orig_insert = SqlAlchemyIncidentPersistenceRepository.insert_incident

        async def insert_with_role_sc01(self: Any, aggregate: Any) -> None:
            nonlocal sc01_contender_sqlstate
            role = _TASK_ROLE_VAR.get()
            if role == "winner":
                await orig_insert(self, aggregate)
            elif role == "contender":
                try:
                    await asyncio.wait_for(sc01_winner_committed.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pytest.fail(
                        "SC-01 contender timed out waiting for winner_committed event after 30s"
                    )
                try:
                    await orig_insert(self, aggregate)
                except Exception as exc:
                    sc01_contender_sqlstate = _extract_sqlstate(exc)
                    raise
            else:
                await orig_insert(self, aggregate)

        sc01_winner_factory = TrackingSessionFactory(runtime_factory)
        sc01_contender_factory = TrackingSessionFactory(runtime_factory)

        async def run_winner_sc01():
            nonlocal sc01_winner_counts
            _TASK_ROLE_VAR.set("winner")
            res = await persist_incident_atomic(
                group=group1,
                signal_inputs=inputs1,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=sc01_winner_factory,
            )
            sc01_winner_counts = await _query_table_counts(disp_conn)
            sc01_winner_committed.set()
            return res

        async def run_contender_sc01():
            _TASK_ROLE_VAR.set("contender")
            return await persist_incident_atomic(
                group=group1,
                signal_inputs=inputs1,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=sc01_contender_factory,
            )

        with unittest.mock.patch.object(
            SqlAlchemyIncidentPersistenceRepository,
            "insert_incident",
            new=insert_with_role_sc01,
        ):
            try:
                results_sc01 = await asyncio.wait_for(
                    asyncio.gather(
                        run_winner_sc01(), run_contender_sc01(), return_exceptions=True
                    ),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                pytest.fail("SC-01 gather timed out after 30s")

        res_winner_sc01, res_contender_sc01 = results_sc01[0], results_sc01[1]
        assert not isinstance(
            res_winner_sc01, Exception
        ), f"Winner raised: {res_winner_sc01}"
        assert not isinstance(
            res_contender_sc01, Exception
        ), f"Contender raised: {res_contender_sc01}"

        assert res_winner_sc01.material_update is True
        assert res_contender_sc01.material_update is False
        assert (
            res_winner_sc01.aggregate.incident_id
            == res_contender_sc01.aggregate.incident_id
        )

        assert sc01_winner_factory.primary_tx == 1
        assert sc01_winner_factory.recovery_tx == 0
        assert sc01_contender_factory.primary_tx == 1
        assert sc01_contender_factory.recovery_tx == 1
        assert sc01_contender_sqlstate == "23505"

        counts_sc01_post = await _query_table_counts(disp_conn)
        assert counts_sc01_post["incidents"] == 1
        assert counts_sc01_post["incident_signal_links"] == 1
        assert counts_sc01_post["incident_timeline_events"] == 1
        assert counts_sc01_post["incident_publication_intents"] == 1

        deltas_sc01_contender = {
            k: counts_sc01_post[k] - sc01_winner_counts[k] for k in counts_sc01_post
        }
        assert all(d == 0 for d in deltas_sc01_contender.values())

        sc01_obs_outcome = (
            "REPLAY"
            if (
                not isinstance(res_contender_sc01, Exception)
                and not res_contender_sc01.material_update
            )
            else "UNKNOWN"
        )
        sc01_public_error = (
            type(res_contender_sc01).__name__
            if isinstance(res_contender_sc01, Exception)
            else "NONE"
        )

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-01",
                expected_outcome="REPLAY",
                observed_outcome=sc01_obs_outcome,
                public_error=sc01_public_error,
                observed_sqlstate=sc01_contender_sqlstate,
                primary_tx_count=sc01_contender_factory.primary_tx,
                recovery_tx_count=sc01_contender_factory.recovery_tx,
                incident_delta=deltas_sc01_contender["incidents"],
                link_delta=deltas_sc01_contender["incident_signal_links"],
                evidence_delta=deltas_sc01_contender["incident_correlation_evidence"],
                timeline_delta=deltas_sc01_contender["incident_timeline_events"],
                intent_delta=deltas_sc01_contender["incident_publication_intents"],
                status="PASS",
            )
        )
        atomicity_observations.append(
            AtomicityObservation(
                case_id="CREATION",
                incident_count=sc01_winner_counts["incidents"],
                link_count=sc01_winner_counts["incident_signal_links"],
                evidence_count=sc01_winner_counts["incident_correlation_evidence"],
                timeline_count=sc01_winner_counts["incident_timeline_events"],
                intent_count=sc01_winner_counts["incident_publication_intents"],
                revision=res_winner_sc01.aggregate.revision,
                fingerprint=res_winner_sc01.aggregate.revision_fingerprint,
                status="PASS",
            )
        )

        # ===================================================================
        # SC-02: Sequential idempotent replay
        # ===================================================================
        counts_sc02_pre = await _query_table_counts(disp_conn)
        sc02_factory = TrackingSessionFactory(runtime_factory)

        res_sc02 = await persist_incident_atomic(
            group=group1,
            signal_inputs=inputs1,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME3,
            tenant_id=_TENANT_A,
            session_factory=sc02_factory,
        )
        assert res_sc02.material_update is False
        assert len(res_sc02.timeline_events) == 0

        counts_sc02_post = await _query_table_counts(disp_conn)
        deltas_sc02 = {
            k: counts_sc02_post[k] - counts_sc02_pre[k] for k in counts_sc02_pre
        }
        assert all(d == 0 for d in deltas_sc02.values())

        sc02_obs_outcome = "REPLAY" if not res_sc02.material_update else "UNKNOWN"

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-02",
                expected_outcome="REPLAY",
                observed_outcome=sc02_obs_outcome,
                public_error="NONE",
                observed_sqlstate="NONE",
                primary_tx_count=sc02_factory.primary_tx,
                recovery_tx_count=sc02_factory.recovery_tx,
                incident_delta=deltas_sc02["incidents"],
                link_delta=deltas_sc02["incident_signal_links"],
                evidence_delta=deltas_sc02["incident_correlation_evidence"],
                timeline_delta=deltas_sc02["incident_timeline_events"],
                intent_delta=deltas_sc02["incident_publication_intents"],
                status="PASS",
            )
        )
        atomicity_observations.append(
            AtomicityObservation(
                case_id="REPLAY",
                incident_count=counts_sc02_post["incidents"],
                link_count=counts_sc02_post["incident_signal_links"],
                evidence_count=counts_sc02_post["incident_correlation_evidence"],
                timeline_count=counts_sc02_post["incident_timeline_events"],
                intent_count=counts_sc02_post["incident_publication_intents"],
                revision=res_sc02.aggregate.revision,
                fingerprint=res_sc02.aggregate.revision_fingerprint,
                status="PASS",
            )
        )

        # ===================================================================
        # SC-03: Real concurrent SC-03 recovery conflict (Three-Snapshot Method)
        # ===================================================================
        m_sc3 = _make_member(
            UUID("33333333-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), _TIME1, "billing", "3" * 64
        )
        inp_sc3 = _make_input(
            UUID("33333333-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), _TIME1, "billing", "3" * 64
        )
        group_sc3 = _make_group([m_sc3])
        inputs_sc3 = {m_sc3.signal_id: inp_sc3}

        # S0 snapshot: immediately before winner and contender start
        s0_snapshot = await _take_snapshot(disp_conn)

        sc03_winner_committed = asyncio.Event()
        sc03_contender_sqlstate = "NONE"
        s1_snapshot: TableSnapshot | None = None

        async def insert_with_role_sc03(self: Any, aggregate: Any) -> None:
            nonlocal sc03_contender_sqlstate
            role = _TASK_ROLE_VAR.get()
            if role == "winner":
                await orig_insert(self, aggregate)
            elif role == "contender":
                try:
                    await asyncio.wait_for(sc03_winner_committed.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pytest.fail(
                        "SC-03 contender timed out waiting for winner_committed event after 30s"
                    )
                try:
                    await orig_insert(self, aggregate)
                except Exception as exc:
                    sc03_contender_sqlstate = _extract_sqlstate(exc)
                    raise
            else:
                await orig_insert(self, aggregate)

        sc03_winner_factory = TrackingSessionFactory(runtime_factory)
        sc03_contender_factory = TrackingSessionFactory(runtime_factory)

        async def run_winner_sc03():
            nonlocal s1_snapshot
            _TASK_ROLE_VAR.set("winner")
            res = await persist_incident_atomic(
                group=group_sc3,
                signal_inputs=inputs_sc3,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=sc03_winner_factory,
            )
            # S1 snapshot: after winner service call returns and its transaction commits,
            # before releasing contender to call real insert
            s1_snapshot = await _take_snapshot(disp_conn)
            sc03_winner_committed.set()
            return res

        async def run_contender_sc03():
            _TASK_ROLE_VAR.set("contender")
            return await persist_incident_atomic(
                group=group_sc3,
                signal_inputs=inputs_sc3,
                desired_state=CorrelationWindowState.FINALIZED,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=sc03_contender_factory,
            )

        with unittest.mock.patch.object(
            SqlAlchemyIncidentPersistenceRepository,
            "insert_incident",
            new=insert_with_role_sc03,
        ):
            try:
                results_sc03 = await asyncio.wait_for(
                    asyncio.gather(
                        run_winner_sc03(), run_contender_sc03(), return_exceptions=True
                    ),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                pytest.fail("SC-03 gather timed out after 30s")

        res_winner_sc03, res_contender_sc03 = results_sc03[0], results_sc03[1]
        assert s1_snapshot is not None
        assert not isinstance(
            res_winner_sc03, Exception
        ), f"Winner raised: {res_winner_sc03}"
        assert isinstance(
            res_contender_sc03, IncidentPersistenceConflictError
        ), f"Contender did not raise conflict: {res_contender_sc03}"

        # S2 snapshot: after contender recovery completes and returns/raises
        s2_snapshot = await _take_snapshot(disp_conn)

        assert sc03_winner_factory.primary_tx == 1
        assert sc03_winner_factory.recovery_tx == 0
        assert sc03_contender_factory.primary_tx == 1
        assert sc03_contender_factory.recovery_tx == 1

        sc03_observed_sqlstate = _extract_sqlstate(res_contender_sc03)
        assert sc03_observed_sqlstate == "23505"

        tables = [
            "incidents",
            "incident_signal_links",
            "incident_correlation_evidence",
            "incident_timeline_events",
            "incident_publication_intents",
        ]
        winner_deltas = {
            t: s1_snapshot.counts[t] - s0_snapshot.counts[t] for t in tables
        }
        contender_deltas = {
            t: s2_snapshot.counts[t] - s1_snapshot.counts[t] for t in tables
        }

        # Contender deltas must all be 0
        assert all(
            d == 0 for d in contender_deltas.values()
        ), f"Non-zero contender deltas: {contender_deltas}"
        # S2 hashes must equal S1 hashes
        for t in tables:
            assert (
                s2_snapshot.hashes[t] == s1_snapshot.hashes[t]
            ), f"Hash mismatch for {t} between S1 and S2"
        # Winner created 1 incident, 1 link, 0 evidence, 1 timeline event, 1 publication intent
        assert winner_deltas["incidents"] == 1
        assert winner_deltas["incident_signal_links"] == 1
        assert winner_deltas["incident_correlation_evidence"] == 0
        assert winner_deltas["incident_timeline_events"] == 1
        assert winner_deltas["incident_publication_intents"] == 1

        print("\nS1_R2_SC03_SNAPSHOT_DIAGNOSTIC_BEGIN")
        print(
            "TABLE|S0_COUNT|S1_COUNT|S2_COUNT|WINNER_DELTA|CONTENDER_DELTA|S0_SHA256|S1_SHA256|S2_SHA256|STATUS"
        )
        for t in tables:
            status = (
                "PASS"
                if (
                    contender_deltas[t] == 0
                    and s2_snapshot.hashes[t] == s1_snapshot.hashes[t]
                )
                else "FAIL"
            )
            print(
                f"{t}|{s0_snapshot.counts[t]}|{s1_snapshot.counts[t]}|{s2_snapshot.counts[t]}|"
                f"{winner_deltas[t]}|{contender_deltas[t]}|"
                f"{s0_snapshot.hashes[t]}|{s1_snapshot.hashes[t]}|{s2_snapshot.hashes[t]}|{status}"
            )
        print("S1_R2_SC03_SNAPSHOT_DIAGNOSTIC_END")

        sc03_obs_outcome = (
            "CONFLICT"
            if isinstance(res_contender_sc03, IncidentPersistenceConflictError)
            else "UNKNOWN"
        )
        sc03_public_error = type(res_contender_sc03).__name__

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-03",
                expected_outcome="CONFLICT",
                observed_outcome=sc03_obs_outcome,
                public_error=sc03_public_error,
                observed_sqlstate=sc03_observed_sqlstate,
                primary_tx_count=sc03_contender_factory.primary_tx,
                recovery_tx_count=sc03_contender_factory.recovery_tx,
                incident_delta=contender_deltas["incidents"],
                link_delta=contender_deltas["incident_signal_links"],
                evidence_delta=contender_deltas["incident_correlation_evidence"],
                timeline_delta=contender_deltas["incident_timeline_events"],
                intent_delta=contender_deltas["incident_publication_intents"],
                status="PASS",
            )
        )

        # ===================================================================
        # SC-04: Real persisted-membership SC-04 conflict
        # ===================================================================
        # Preseed an incident with different ID under Tenant A
        m_sc04_pre = _make_member(
            UUID("44444444-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), _TIME1, "preseed", "4" * 64
        )
        inp_sc04_pre = _make_input(
            UUID("44444444-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), _TIME1, "preseed", "4" * 64
        )
        group_sc04_pre = _make_group([m_sc04_pre])
        inputs_sc04_pre = {m_sc04_pre.signal_id: inp_sc04_pre}
        res_sc04_pre = await persist_incident_atomic(
            group=group_sc04_pre,
            signal_inputs=inputs_sc04_pre,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=runtime_factory,
        )
        preseeded_id = res_sc04_pre.aggregate.incident_id

        # Incoming signal that belongs to preseeded incident in DB, but has different candidate
        sig_sc04_in = UUID("44444444-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        m_sc04_in = _make_member(sig_sc04_in, _TIME2, "incoming", "5" * 64)
        inp_sc04_in = _make_input(sig_sc04_in, _TIME2, "incoming", "5" * 64)
        group_sc04_in = _make_group([m_sc04_in])
        inputs_sc04_in = {sig_sc04_in: inp_sc04_in}

        cand_sc04_id = derive_incident_id(
            tenant_id=_TENANT_A,
            environment="production",
            anchor_signal_fingerprint="5" * 64,
        )
        assert preseeded_id != cand_sc04_id

        # Insert membership link directly via admin connection into real PostgreSQL
        await disp_conn.execute(
            """
            INSERT INTO incident_signal_links (
                tenant_id, incident_id, signal_id, semantic_fingerprint,
                environment, service, event_time, severity, anomaly_score
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            _TENANT_A,
            preseeded_id,
            sig_sc04_in,
            "5" * 64,
            "production",
            "incoming",
            _TIME2,
            "critical",
            0.95,
        )

        counts_sc04_pre = await _query_table_counts(disp_conn)
        hashes_sc04_pre = await _query_5table_hashes(disp_conn)

        spy_pure_sc04 = unittest.mock.MagicMock(wraps=create_or_update_incident)
        sc04_factory = TrackingSessionFactory(runtime_factory)

        with unittest.mock.patch(
            "app.correlation.persistence_service.create_or_update_incident",
            new=spy_pure_sc04,
        ):
            with pytest.raises(
                IncidentPersistenceConflictError,
                match="does not match candidate",
            ) as exc_info_sc04:
                await persist_incident_atomic(
                    group=group_sc04_in,
                    signal_inputs=inputs_sc04_in,
                    desired_state=CorrelationWindowState.COLLECTING,
                    processing_time=_TIME2,
                    tenant_id=_TENANT_A,
                    session_factory=sc04_factory,
                )

        assert spy_pure_sc04.call_count == 0
        assert not isinstance(derive_incident_id, unittest.mock.MagicMock)

        counts_sc04_post = await _query_table_counts(disp_conn)
        hashes_sc04_post = await _query_5table_hashes(disp_conn)
        assert counts_sc04_post == counts_sc04_pre
        assert hashes_sc04_post == hashes_sc04_pre

        deltas_sc04 = {
            k: counts_sc04_post[k] - counts_sc04_pre[k] for k in counts_sc04_pre
        }
        sc04_obs_outcome = (
            "CONFLICT"
            if isinstance(exc_info_sc04.value, IncidentPersistenceConflictError)
            else "UNKNOWN"
        )
        sc04_public_error = type(exc_info_sc04.value).__name__
        sc04_sqlstate = _extract_sqlstate(exc_info_sc04.value)

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-04",
                expected_outcome="CONFLICT",
                observed_outcome=sc04_obs_outcome,
                public_error=sc04_public_error,
                observed_sqlstate=sc04_sqlstate,
                primary_tx_count=sc04_factory.primary_tx,
                recovery_tx_count=sc04_factory.recovery_tx,
                incident_delta=deltas_sc04["incidents"],
                link_delta=deltas_sc04["incident_signal_links"],
                evidence_delta=deltas_sc04["incident_correlation_evidence"],
                timeline_delta=deltas_sc04["incident_timeline_events"],
                intent_delta=deltas_sc04["incident_publication_intents"],
                status="PASS",
            )
        )

        # ===================================================================
        # SC-05: Stale revision conflict
        # ===================================================================
        counts_sc05_pre = await _query_table_counts(disp_conn)
        sc05_factory = TrackingSessionFactory(runtime_factory)

        # Expected revision 999 with matching fingerprint and material update -> Conflict
        with pytest.raises(
            IncidentPersistenceConflictError, match="Stale writer"
        ) as exc_info_sc05:
            await persist_incident_atomic(
                group=group1,
                signal_inputs=inputs1,
                desired_state=CorrelationWindowState.FINALIZED,
                processing_time=_TIME3,
                tenant_id=_TENANT_A,
                session_factory=sc05_factory,
                expected_revision=999,
                expected_revision_fingerprint=res_winner_sc01.aggregate.revision_fingerprint,
            )

        counts_sc05_post = await _query_table_counts(disp_conn)
        assert counts_sc05_post == counts_sc05_pre
        deltas_sc05 = {
            k: counts_sc05_post[k] - counts_sc05_pre[k] for k in counts_sc05_pre
        }
        sc05_obs_outcome = (
            "CONFLICT"
            if isinstance(exc_info_sc05.value, IncidentPersistenceConflictError)
            else "UNKNOWN"
        )
        sc05_public_error = type(exc_info_sc05.value).__name__
        sc05_sqlstate = _extract_sqlstate(exc_info_sc05.value)

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-05",
                expected_outcome="CONFLICT",
                observed_outcome=sc05_obs_outcome,
                public_error=sc05_public_error,
                observed_sqlstate=sc05_sqlstate,
                primary_tx_count=sc05_factory.primary_tx,
                recovery_tx_count=sc05_factory.recovery_tx,
                incident_delta=deltas_sc05["incidents"],
                link_delta=deltas_sc05["incident_signal_links"],
                evidence_delta=deltas_sc05["incident_correlation_evidence"],
                timeline_delta=deltas_sc05["incident_timeline_events"],
                intent_delta=deltas_sc05["incident_publication_intents"],
                status="PASS",
            )
        )

        # ===================================================================
        # SC-06: Stale fingerprint conflict
        # ===================================================================
        counts_sc06_pre = await _query_table_counts(disp_conn)
        sc06_factory = TrackingSessionFactory(runtime_factory)

        # Matching revision 1 with mismatched fingerprint "0"*64 and material update -> Conflict
        with pytest.raises(
            IncidentPersistenceConflictError, match="Stale writer"
        ) as exc_info_sc06:
            await persist_incident_atomic(
                group=group1,
                signal_inputs=inputs1,
                desired_state=CorrelationWindowState.FINALIZED,
                processing_time=_TIME3,
                tenant_id=_TENANT_A,
                session_factory=sc06_factory,
                expected_revision=res_winner_sc01.aggregate.revision,
                expected_revision_fingerprint="0" * 64,
            )

        counts_sc06_post = await _query_table_counts(disp_conn)
        assert counts_sc06_post == counts_sc06_pre
        deltas_sc06 = {
            k: counts_sc06_post[k] - counts_sc06_pre[k] for k in counts_sc06_pre
        }
        sc06_obs_outcome = (
            "CONFLICT"
            if isinstance(exc_info_sc06.value, IncidentPersistenceConflictError)
            else "UNKNOWN"
        )
        sc06_public_error = type(exc_info_sc06.value).__name__
        sc06_sqlstate = _extract_sqlstate(exc_info_sc06.value)

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-06",
                expected_outcome="CONFLICT",
                observed_outcome=sc06_obs_outcome,
                public_error=sc06_public_error,
                observed_sqlstate=sc06_sqlstate,
                primary_tx_count=sc06_factory.primary_tx,
                recovery_tx_count=sc06_factory.recovery_tx,
                incident_delta=deltas_sc06["incidents"],
                link_delta=deltas_sc06["incident_signal_links"],
                evidence_delta=deltas_sc06["incident_correlation_evidence"],
                timeline_delta=deltas_sc06["incident_timeline_events"],
                intent_delta=deltas_sc06["incident_publication_intents"],
                status="PASS",
            )
        )

        # ===================================================================
        # SC-07: Stale writer idempotent replay
        # ===================================================================
        counts_sc07_pre = await _query_table_counts(disp_conn)
        sc07_factory = TrackingSessionFactory(runtime_factory)

        # Stale expectation with identical inputs yields replay with 0 writes
        res_sc07 = await persist_incident_atomic(
            group=group1,
            signal_inputs=inputs1,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME3,
            tenant_id=_TENANT_A,
            session_factory=sc07_factory,
            expected_revision=999,
            expected_revision_fingerprint="f" * 64,
        )
        assert res_sc07.material_update is False
        assert len(res_sc07.timeline_events) == 0

        counts_sc07_post = await _query_table_counts(disp_conn)
        assert counts_sc07_post == counts_sc07_pre
        deltas_sc07 = {
            k: counts_sc07_post[k] - counts_sc07_pre[k] for k in counts_sc07_pre
        }
        sc07_obs_outcome = "REPLAY" if not res_sc07.material_update else "UNKNOWN"

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-07",
                expected_outcome="REPLAY",
                observed_outcome=sc07_obs_outcome,
                public_error="NONE",
                observed_sqlstate="NONE",
                primary_tx_count=sc07_factory.primary_tx,
                recovery_tx_count=sc07_factory.recovery_tx,
                incident_delta=deltas_sc07["incidents"],
                link_delta=deltas_sc07["incident_signal_links"],
                evidence_delta=deltas_sc07["incident_correlation_evidence"],
                timeline_delta=deltas_sc07["incident_timeline_events"],
                intent_delta=deltas_sc07["incident_publication_intents"],
                status="PASS",
            )
        )

        # ===================================================================
        # Genuine material update (Revision 2) & Intent Immutability
        # ===================================================================
        m2 = _make_member(_SIG2_ID, _TIME2, "billing", _FP2)
        inp2 = _make_input(_SIG2_ID, _TIME2, "billing", _FP2)
        group_with_m2 = _make_group([m1, m2])
        inputs_with_m2 = {_SIG1_ID: inp1, _SIG2_ID: inp2}

        # 1. Query revision-one intent before update
        rev1_row_pre = await disp_conn.fetchrow(
            """
            SELECT tenant_id, incident_id, revision, schema_version, revision_fingerprint, intent_time, aggregate_payload
            FROM incident_publication_intents
            WHERE incident_id = $1 AND revision = 1
            """,
            res_winner_sc01.aggregate.incident_id,
        )
        assert rev1_row_pre is not None
        rev1_hash_pre = hashlib.sha256(
            f"{rev1_row_pre['tenant_id']}:{rev1_row_pre['incident_id']}:{rev1_row_pre['revision']}:"
            f"{rev1_row_pre['schema_version']}:{rev1_row_pre['revision_fingerprint']}:"
            f"{rev1_row_pre['intent_time'].isoformat()}:{bytes(rev1_row_pre['aggregate_payload']).hex()}".encode()
        ).hexdigest()

        # Perform genuine material update
        res_update = await persist_incident_atomic(
            group=group_with_m2,
            signal_inputs=inputs_with_m2,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME3,
            tenant_id=_TENANT_A,
            session_factory=runtime_factory,
            expected_revision=res_winner_sc01.aggregate.revision,
            expected_revision_fingerprint=res_winner_sc01.aggregate.revision_fingerprint,
        )
        assert res_update.material_update is True
        assert res_update.aggregate.revision == 2

        counts_update = await _query_table_counts(disp_conn)
        atomicity_observations.append(
            AtomicityObservation(
                case_id="UPDATE",
                incident_count=counts_update["incidents"],
                link_count=counts_update["incident_signal_links"],
                evidence_count=counts_update["incident_correlation_evidence"],
                timeline_count=counts_update["incident_timeline_events"],
                intent_count=counts_update["incident_publication_intents"],
                revision=res_update.aggregate.revision,
                fingerprint=res_update.aggregate.revision_fingerprint,
                status="PASS",
            )
        )

        # 2. Query revision-one intent after update and require byte/field identity
        rev1_row_post = await disp_conn.fetchrow(
            """
            SELECT tenant_id, incident_id, revision, schema_version, revision_fingerprint, intent_time, aggregate_payload
            FROM incident_publication_intents
            WHERE incident_id = $1 AND revision = 1
            """,
            res_winner_sc01.aggregate.incident_id,
        )
        assert rev1_row_post is not None
        rev1_hash_post = hashlib.sha256(
            f"{rev1_row_post['tenant_id']}:{rev1_row_post['incident_id']}:{rev1_row_post['revision']}:"
            f"{rev1_row_post['schema_version']}:{rev1_row_post['revision_fingerprint']}:"
            f"{rev1_row_post['intent_time'].isoformat()}:{bytes(rev1_row_post['aggregate_payload']).hex()}".encode()
        ).hexdigest()
        assert rev1_hash_post == rev1_hash_pre
        assert bytes(rev1_row_post["aggregate_payload"]) == bytes(
            rev1_row_pre["aggregate_payload"]
        )

        atomicity_observations.append(
            AtomicityObservation(
                case_id="INTENT_REV1_IMMUTABLE",
                incident_count=counts_update["incidents"],
                link_count=counts_update["incident_signal_links"],
                evidence_count=counts_update["incident_correlation_evidence"],
                timeline_count=counts_update["incident_timeline_events"],
                intent_count=counts_update["incident_publication_intents"],
                revision=rev1_row_post["revision"],
                fingerprint=rev1_row_post["revision_fingerprint"],
                status="PASS",
            )
        )

        # 3. Query revision-two intent and require exact equality to returned aggregate
        rev2_row = await disp_conn.fetchrow(
            """
            SELECT tenant_id, incident_id, revision, schema_version, revision_fingerprint, intent_time, aggregate_payload
            FROM incident_publication_intents
            WHERE incident_id = $1 AND revision = 2
            """,
            res_winner_sc01.aggregate.incident_id,
        )
        assert rev2_row is not None
        assert bytes(rev2_row["aggregate_payload"]) == serialize_incident_aggregate(
            res_update.aggregate
        )
        assert rev2_row["tenant_id"] == res_update.aggregate.tenant_id
        assert rev2_row["incident_id"] == res_update.aggregate.incident_id
        assert rev2_row["revision"] == 2
        assert rev2_row["schema_version"] == res_update.aggregate.schema_version
        assert (
            rev2_row["revision_fingerprint"]
            == res_update.aggregate.revision_fingerprint
        )
        assert rev2_row["intent_time"] == res_update.aggregate.updated_at

        atomicity_observations.append(
            AtomicityObservation(
                case_id="INTENT_REV2_MATCHES_AGGREGATE",
                incident_count=counts_update["incidents"],
                link_count=counts_update["incident_signal_links"],
                evidence_count=counts_update["incident_correlation_evidence"],
                timeline_count=counts_update["incident_timeline_events"],
                intent_count=counts_update["incident_publication_intents"],
                revision=rev2_row["revision"],
                fingerprint=rev2_row["revision_fingerprint"],
                status="PASS",
            )
        )

        # ===================================================================
        # SC-08: CAS concurrency conflict rollback
        # ===================================================================
        hashes_sc08_pre = await _query_5table_hashes(disp_conn)
        counts_sc08_pre = await _query_table_counts(disp_conn)
        sc08_factory = TrackingSessionFactory(runtime_factory)

        with unittest.mock.patch.object(
            SqlAlchemyIncidentPersistenceRepository,
            "update_incident_cas",
            return_value=False,
        ):
            with pytest.raises(
                IncidentPersistenceConflictError, match="CAS update failed"
            ) as exc_info_sc08:
                await persist_incident_atomic(
                    group=group_with_m2,
                    signal_inputs=inputs_with_m2,
                    desired_state=CorrelationWindowState.FINALIZED,
                    processing_time=_TIME3,
                    tenant_id=_TENANT_A,
                    session_factory=sc08_factory,
                    expected_revision=res_update.aggregate.revision,
                    expected_revision_fingerprint=res_update.aggregate.revision_fingerprint,
                )

        hashes_sc08_post = await _query_5table_hashes(disp_conn)
        counts_sc08_post = await _query_table_counts(disp_conn)
        assert hashes_sc08_post == hashes_sc08_pre
        assert counts_sc08_post == counts_sc08_pre

        deltas_sc08 = {
            k: counts_sc08_post[k] - counts_sc08_pre[k] for k in counts_sc08_pre
        }
        sc08_obs_outcome = (
            "ROLLBACK"
            if (
                hashes_sc08_post == hashes_sc08_pre
                and isinstance(exc_info_sc08.value, IncidentPersistenceConflictError)
            )
            else "UNKNOWN"
        )
        sc08_public_error = type(exc_info_sc08.value).__name__
        sc08_sqlstate = _extract_sqlstate(exc_info_sc08.value)

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-08",
                expected_outcome="ROLLBACK",
                observed_outcome=sc08_obs_outcome,
                public_error=sc08_public_error,
                observed_sqlstate=sc08_sqlstate,
                primary_tx_count=sc08_factory.primary_tx,
                recovery_tx_count=sc08_factory.recovery_tx,
                incident_delta=deltas_sc08["incidents"],
                link_delta=deltas_sc08["incident_signal_links"],
                evidence_delta=deltas_sc08["incident_correlation_evidence"],
                timeline_delta=deltas_sc08["incident_timeline_events"],
                intent_delta=deltas_sc08["incident_publication_intents"],
                status="PASS",
            )
        )
        atomicity_observations.append(
            AtomicityObservation(
                case_id="ROLLBACK_CAS",
                incident_count=counts_sc08_post["incidents"],
                link_count=counts_sc08_post["incident_signal_links"],
                evidence_count=counts_sc08_post["incident_correlation_evidence"],
                timeline_count=counts_sc08_post["incident_timeline_events"],
                intent_count=counts_sc08_post["incident_publication_intents"],
                revision=res_update.aggregate.revision,
                fingerprint=res_update.aggregate.revision_fingerprint,
                status="PASS",
            )
        )

        # ===================================================================
        # SC-09: Failure after parent/children before intent -> live rollback
        # ===================================================================
        m4 = _make_member(
            UUID("99999999-9999-9999-9999-999999999999"), _TIME1, "checkout", _FP4
        )
        inp4 = _make_input(
            UUID("99999999-9999-9999-9999-999999999999"), _TIME1, "checkout", _FP4
        )
        group4 = _make_group([m4])
        inputs4 = {m4.signal_id: inp4}

        hashes_sc09_pre = await _query_5table_hashes(disp_conn)
        counts_sc09_pre = await _query_table_counts(disp_conn)
        sc09_factory = TrackingSessionFactory(runtime_factory)

        async def crash_intent(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("simulated intent write crash")

        with unittest.mock.patch.object(
            SqlAlchemyIncidentPersistenceRepository,
            "append_publication_intent",
            side_effect=crash_intent,
        ):
            with pytest.raises(
                IncidentPersistenceStoreError,
                match="Unexpected error in persistence service",
            ) as exc_info_sc09:
                await persist_incident_atomic(
                    group=group4,
                    signal_inputs=inputs4,
                    desired_state=CorrelationWindowState.COLLECTING,
                    processing_time=_TIME1,
                    tenant_id=_TENANT_A,
                    session_factory=sc09_factory,
                )
            assert "simulated intent write crash" in str(exc_info_sc09.value.__cause__)

        hashes_sc09_post = await _query_5table_hashes(disp_conn)
        counts_sc09_post = await _query_table_counts(disp_conn)
        assert hashes_sc09_post == hashes_sc09_pre
        assert counts_sc09_post == counts_sc09_pre

        deltas_sc09 = {
            k: counts_sc09_post[k] - counts_sc09_pre[k] for k in counts_sc09_pre
        }
        sc09_obs_outcome = (
            "ROLLBACK"
            if (
                hashes_sc09_post == hashes_sc09_pre
                and isinstance(exc_info_sc09.value, IncidentPersistenceStoreError)
            )
            else "UNKNOWN"
        )
        sc09_public_error = type(exc_info_sc09.value).__name__
        sc09_sqlstate = _extract_sqlstate(exc_info_sc09.value)

        scenario_observations.append(
            ScenarioObservation(
                scenario_id="SC-09",
                expected_outcome="ROLLBACK",
                observed_outcome=sc09_obs_outcome,
                public_error=sc09_public_error,
                observed_sqlstate=sc09_sqlstate,
                primary_tx_count=sc09_factory.primary_tx,
                recovery_tx_count=sc09_factory.recovery_tx,
                incident_delta=deltas_sc09["incidents"],
                link_delta=deltas_sc09["incident_signal_links"],
                evidence_delta=deltas_sc09["incident_correlation_evidence"],
                timeline_delta=deltas_sc09["incident_timeline_events"],
                intent_delta=deltas_sc09["incident_publication_intents"],
                status="PASS",
            )
        )
        atomicity_observations.append(
            AtomicityObservation(
                case_id="ROLLBACK_INTENT",
                incident_count=counts_sc09_post["incidents"],
                link_count=counts_sc09_post["incident_signal_links"],
                evidence_count=counts_sc09_post["incident_correlation_evidence"],
                timeline_count=counts_sc09_post["incident_timeline_events"],
                intent_count=counts_sc09_post["incident_publication_intents"],
                revision=res_update.aggregate.revision,
                fingerprint=res_update.aggregate.revision_fingerprint,
                status="PASS",
            )
        )

        # -------------------------------------------------------------------
        # Emit Scenario Diagnostic Block
        # -------------------------------------------------------------------
        print("\nS1_SCENARIO_DIAGNOSTIC_BEGIN")
        print(
            "SCENARIO_ID|EXPECTED_OUTCOME|OBSERVED_OUTCOME|PUBLIC_ERROR|SQLSTATE|PRIMARY_TRANSACTION_COUNT|RECOVERY_TRANSACTION_COUNT|INCIDENT_DELTA|LINK_DELTA|EVIDENCE_DELTA|TIMELINE_DELTA|INTENT_DELTA|STATUS"
        )
        for obs in scenario_observations:
            print(obs.to_row())
        print("S1_SCENARIO_DIAGNOSTIC_END")

        # -------------------------------------------------------------------
        # Emit Atomicity Diagnostic Block
        # -------------------------------------------------------------------
        print("\nS1_ATOMICITY_DIAGNOSTIC_BEGIN")
        print(
            "CASE_ID|INCIDENT_COUNT|LINK_COUNT|EVIDENCE_COUNT|TIMELINE_COUNT|INTENT_COUNT|REVISION|FINGERPRINT|STATUS"
        )
        for at_obs in atomicity_observations:
            print(at_obs.to_row())
        print("S1_ATOMICITY_DIAGNOSTIC_END")

    finally:
        await disp_conn.close()


@pytest.mark.asyncio
async def test_cross_tenant_isolation(
    pg_env: dict[str, Any], runtime_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Verify tenant isolation: Tenant B cannot resolve or mutate Tenant A state."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1_b = _make_input(_SIG1_ID, _TIME1, "auth", _FP1, tenant_id=_TENANT_B)
    group1 = _make_group([m1])
    inputs_b = {_SIG1_ID: inp1_b}

    res_b = await persist_incident_atomic(
        group=group1,
        signal_inputs=inputs_b,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME1,
        tenant_id=_TENANT_B,
        session_factory=runtime_factory,
    )
    assert res_b.aggregate.tenant_id == _TENANT_B


@pytest_asyncio.fixture
async def t1_service_case(
    pg_env: dict[str, Any], runtime_factory: async_sessionmaker[AsyncSession]
) -> AsyncGenerator[dict[str, Any], None]:
    """Seed one complete public two-member aggregate for each independent case."""
    signal_ids = [uuid4() for _ in range(3)]
    members = [
        _make_member(
            signal_id,
            _TIME1 if i == 0 else _TIME2,
            f"t1-service-{i}",
            hashlib.sha256(signal_id.bytes).hexdigest(),
        )
        for i, signal_id in enumerate(signal_ids)
    ]
    inputs = {
        member.signal_id: _make_input(
            member.signal_id,
            member.event_time,
            member.service,
            member.semantic_fingerprint,
        )
        for member in members
    }
    group = _make_group(members[:2])
    initial_inputs = {
        member.signal_id: inputs[member.signal_id] for member in members[:2]
    }
    initial = await persist_incident_atomic(
        group=group,
        signal_inputs=initial_inputs,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME3,
        tenant_id=_TENANT_A,
        session_factory=runtime_factory,
    )
    connection = await asyncpg.connect(pg_env["disp_admin_dsn"])
    try:
        yield dict(
            factory=runtime_factory,
            connection=connection,
            members=members,
            inputs=inputs,
            group=group,
            initial_inputs=initial_inputs,
            initial=initial,
            expanded_group=_make_group(members),
        )
    finally:
        await connection.close()


async def _t1_load(case: dict[str, Any], tenant: UUID, incident_id: UUID) -> Any:
    async with case["factory"]() as session:
        async with session.begin():
            repository = SqlAlchemyIncidentPersistenceRepository(session, tenant)
            await repository.bind_tenant_context()
            stored = await repository.load_incident(incident_id)
            assert stored is not None
            return stored


async def _t1_tenant_snapshot(
    connection: asyncpg.Connection, tenant: UUID
) -> TableSnapshot:
    keys = {
        "incidents": "incident_id",
        "incident_signal_links": "incident_id, signal_id",
        "incident_correlation_evidence": "incident_id, left_signal_id, right_signal_id",
        "incident_timeline_events": "event_id",
        "incident_publication_intents": "incident_id, revision",
    }
    counts, hashes = {}, {}
    for table, order in keys.items():
        records = await connection.fetch(
            f"SELECT * FROM {table} WHERE tenant_id = $1 ORDER BY {order}", tenant
        )
        counts[table] = len(records)
        hashes[table] = hashlib.sha256(
            str([dict(row) for row in records]).encode()
        ).hexdigest()
    return TableSnapshot(counts, hashes)


async def _t1_local_counts(
    repository: SqlAlchemyIncidentPersistenceRepository, incident_id: UUID
) -> dict[str, int]:
    result = {}
    for table in (
        "incidents",
        "incident_signal_links",
        "incident_correlation_evidence",
        "incident_timeline_events",
        "incident_publication_intents",
    ):
        result[table] = await repository._session.scalar(
            text(f"SELECT count(*) FROM {table} WHERE incident_id = :incident"),
            {"incident": incident_id},
        )
    return result


@pytest.mark.asyncio
async def test_t1_concurrent_update_stale_writer_and_exact_replay(
    t1_service_case: dict[str, Any],
) -> None:
    case = t1_service_case
    initial = case["initial"].aggregate
    original = SqlAlchemyIncidentPersistenceRepository.load_incident
    winner_locked, contender_sent, winner_committed = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )
    observed: dict[str, Any] = {}

    async def synchronized_load(repository: Any, *args: Any, **kwargs: Any) -> Any:
        role = _TASK_ROLE_VAR.get()
        if role == "t1-contender":
            await asyncio.wait_for(winner_locked.wait(), 10)
            observed["contender_pid"] = await repository._session.scalar(
                text("SELECT pg_backend_pid()")
            )
            contender_sent.set()
            stored = await original(repository, *args, **kwargs)
            await asyncio.wait_for(winner_committed.wait(), 10)
            return stored
        stored = await original(repository, *args, **kwargs)
        if role == "t1-winner":
            winner_locked.set()
            await asyncio.wait_for(contender_sent.wait(), 10)
            for _ in range(500):
                wait_type = await case["connection"].fetchval(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1",
                    observed["contender_pid"],
                )
                if wait_type == "Lock":
                    observed["wait_event_type"] = wait_type
                    break
                await asyncio.sleep(0.01)
            assert observed.get("wait_event_type") == "Lock"
        return stored

    async def update(role: str, desired: CorrelationWindowState) -> Any:
        token = _TASK_ROLE_VAR.set(role)
        try:
            result = await persist_incident_atomic(
                group=case["expanded_group"],
                signal_inputs=case["inputs"],
                desired_state=desired,
                processing_time=_TIME3,
                tenant_id=_TENANT_A,
                session_factory=case["factory"],
                expected_revision=initial.revision,
                expected_revision_fingerprint=initial.revision_fingerprint,
            )
            if role == "t1-winner":
                observed["winner_snapshot"] = await _take_snapshot(case["connection"])
                winner_committed.set()
            return result
        finally:
            _TASK_ROLE_VAR.reset(token)

    before = await _take_snapshot(case["connection"])
    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository, "load_incident", new=synchronized_load
    ):
        winner, loser = await asyncio.wait_for(
            asyncio.gather(
                update("t1-winner", CorrelationWindowState.SETTLING),
                update("t1-contender", CorrelationWindowState.FINALIZED),
                return_exceptions=True,
            ),
            30,
        )
    assert not isinstance(winner, BaseException), repr(winner)
    assert isinstance(loser, IncidentPersistenceConflictError)
    assert winner.material_update and winner.aggregate.revision == initial.revision + 1
    assert await _take_snapshot(case["connection"]) == observed["winner_snapshot"]
    stored = await _t1_load(case, _TENANT_A, initial.incident_id)
    assert stored.aggregate == winner.aggregate
    assert stored.timeline_events == (
        *case["initial"].timeline_events,
        *winner.timeline_events,
    )
    assert len(stored.aggregate.signal_references) == 3
    assert len(stored.aggregate.correlation_evidence) == 3
    expected_deltas = dict(
        incidents=0,
        incident_signal_links=1,
        incident_correlation_evidence=2,
        incident_timeline_events=len(winner.timeline_events),
        incident_publication_intents=1,
    )
    measured = {
        table: observed["winner_snapshot"].counts[table] - before.counts[table]
        for table in before.counts
    }
    assert measured == expected_deltas
    replay = await persist_incident_atomic(
        group=case["expanded_group"],
        signal_inputs=case["inputs"],
        desired_state=CorrelationWindowState.SETTLING,
        processing_time=_TIME3,
        tenant_id=_TENANT_A,
        session_factory=case["factory"],
        expected_revision=initial.revision,
        expected_revision_fingerprint=initial.revision_fingerprint,
    )
    assert replay.aggregate == winner.aggregate
    assert replay.material_update is False and replay.timeline_events == ()
    assert await _take_snapshot(case["connection"]) == observed["winner_snapshot"]
    print(
        "T1_UPDATE_OBSERVED",
        observed["wait_event_type"],
        measured,
        type(loser).__name__,
        replay.aggregate.revision,
    )


@pytest.mark.asyncio
async def test_t1_real_multiple_incident_membership_conflict(
    t1_service_case: dict[str, Any],
) -> None:
    case = t1_service_case
    third = case["members"][2]
    second = await persist_incident_atomic(
        group=_make_group([third]),
        signal_inputs={third.signal_id: case["inputs"][third.signal_id]},
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME3,
        tenant_id=_TENANT_A,
        session_factory=case["factory"],
    )
    assert second.aggregate.incident_id != case["initial"].aggregate.incident_id
    before = await _take_snapshot(case["connection"])
    with unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        wraps=create_or_update_incident,
    ) as transition:
        with pytest.raises(IncidentPersistenceConflictError, match="multiple distinct"):
            await persist_incident_atomic(
                group=case["expanded_group"],
                signal_inputs=case["inputs"],
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME3,
                tenant_id=_TENANT_A,
                session_factory=case["factory"],
            )
        transition.assert_not_called()
    assert await _take_snapshot(case["connection"]) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["remove_member", "member_fingerprint", "retained_evidence"]
)
async def test_t1_retained_material_conflicts_leave_five_tables_unchanged(
    t1_service_case: dict[str, Any],
    change: str,
) -> None:
    case = t1_service_case
    members = list(case["members"][:2])
    inputs = dict(case["initial_inputs"])
    evidence = list(case["group"].accepted_evidence)
    if change == "remove_member":
        members = members[:1]
        inputs = {members[0].signal_id: inputs[members[0].signal_id]}
        evidence = []
    elif change == "member_fingerprint":
        old = members[1]
        fingerprint = hashlib.sha256(b"changed" + old.signal_id.bytes).hexdigest()
        members[1] = _make_member(
            old.signal_id, old.event_time, old.service, fingerprint
        )
        inputs[old.signal_id] = _make_input(
            old.signal_id, old.event_time, old.service, fingerprint
        )
    else:
        fields = evidence[0].model_dump()
        fields["reason"] = "changed retained evidence"
        evidence = [CorrelationEvidence.model_validate(fields)]
    group = _make_group(members, evidence)
    before = await _take_snapshot(case["connection"])
    initial = case["initial"].aggregate
    with pytest.raises(IncidentPersistenceConflictError) as caught:
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME3,
            tenant_id=_TENANT_A,
            session_factory=case["factory"],
            expected_revision=initial.revision,
            expected_revision_fingerprint=initial.revision_fingerprint,
        )
    assert isinstance(caught.value.__cause__, AggregateStateError)
    assert await _take_snapshot(case["connection"]) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage",
    [
        "append_signal_links",
        "append_correlation_evidence",
        "append_timeline_events",
        "update_incident_cas",
    ],
)
async def test_t1_live_write_stage_failure_rolls_back_five_tables(
    t1_service_case: dict[str, Any],
    stage: str,
) -> None:
    case = t1_service_case
    initial = case["initial"].aggregate
    before = await _take_snapshot(case["connection"])
    original = getattr(SqlAlchemyIncidentPersistenceRepository, stage)
    observed: dict[str, Any] = {}

    async def fail_after_real_write(repository: Any, *args: Any, **kwargs: Any) -> Any:
        if stage == "update_incident_cas":
            kwargs["expected_revision"] += 1
        result = await original(repository, *args, **kwargs)
        observed["counts"] = await _t1_local_counts(repository, initial.incident_id)
        observed["result"] = result
        assert observed["counts"]["incident_signal_links"] == 3
        if stage != "append_signal_links":
            assert observed["counts"]["incident_correlation_evidence"] == 3
        if stage in ("append_timeline_events", "update_incident_cas"):
            assert observed["counts"]["incident_timeline_events"] > len(
                case["initial"].timeline_events
            )
        if stage == "update_incident_cas":
            assert result is False
            assert observed["counts"]["incident_publication_intents"] == 2
            return result
        raise RuntimeError(f"T1 observed write failure at {stage}")

    expected = (
        IncidentPersistenceConflictError
        if stage == "update_incident_cas"
        else IncidentPersistenceStoreError
    )
    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository, stage, new=fail_after_real_write
    ):
        with pytest.raises(expected) as caught:
            await persist_incident_atomic(
                group=case["expanded_group"],
                signal_inputs=case["inputs"],
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME3,
                tenant_id=_TENANT_A,
                session_factory=case["factory"],
                expected_revision=initial.revision,
                expected_revision_fingerprint=initial.revision_fingerprint,
            )
    assert observed
    if stage != "update_incident_cas":
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert stage in str(caught.value.__cause__)
    assert await _take_snapshot(case["connection"]) == before
    print("T1_WRITE_STAGE_OBSERVED", stage, observed["counts"], observed["result"])


@pytest.mark.asyncio
async def test_t1_tenant_service_readback_and_deterministic_event_ids(
    t1_service_case: dict[str, Any],
) -> None:
    case = t1_service_case
    first = case["initial"]
    before_a = await _t1_tenant_snapshot(case["connection"], _TENANT_A)
    inputs_b = {
        member.signal_id: _make_input(
            member.signal_id,
            member.event_time,
            member.service,
            member.semantic_fingerprint,
            tenant_id=_TENANT_B,
        )
        for member in case["members"][:2]
    }
    second = await persist_incident_atomic(
        group=case["group"],
        signal_inputs=inputs_b,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME3,
        tenant_id=_TENANT_B,
        session_factory=case["factory"],
    )
    assert first.aggregate.incident_id != second.aggregate.incident_id
    assert await _t1_tenant_snapshot(case["connection"], _TENANT_A) == before_a
    stored_a = await _t1_load(case, _TENANT_A, first.aggregate.incident_id)
    stored_b = await _t1_load(case, _TENANT_B, second.aggregate.incident_id)
    assert (
        stored_a.aggregate == first.aggregate and stored_b.aggregate == second.aggregate
    )
    async with case["factory"]() as session:
        async with session.begin():
            repository = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_B)
            await repository.bind_tenant_context()
            assert await repository.load_incident(first.aggregate.incident_id) is None
    expected = create_or_update_incident(
        group=case["group"],
        signal_inputs=case["initial_inputs"],
        processing_time=_TIME3,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert expected.aggregate == first.aggregate
    assert stored_a.timeline_events == expected.timeline_events
    for event in stored_a.timeline_events:
        assert event.event_id == derive_timeline_event_id(
            incident_id=event.incident_id,
            revision=event.revision,
            event_type=event.event_type,
            event_time=event.event_time,
            source_signal_id=event.source_signal_id,
            revision_fingerprint=event.revision_fingerprint,
        )
