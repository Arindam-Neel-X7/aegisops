"""Real PostgreSQL integration tests for Task 4.9-P1 persistence repository.

Marked pytest.mark.integration.
Tests execute against a portable disposable database and runtime login role.
Validates:
1. All repository write methods persist exact rows and load_incident reconstructs validated state;
2. Two tenants with overlapping services/signals are isolated by RLS;
3. Missing and mismatched transaction-local tenant context fail closed;
4. Transaction-local context is absent on checked-out pooled connection after commit and rollback;
5. Runtime role is non-superuser/non-bypass (rolsuper=false, rolbypassrls=false);
6. Membership resolution returns only tenant-local incident IDs;
7. Parent FOR UPDATE blocks competing transactions until release;
8. Caller rollback removes every repository write because adapter never commits;
9. Append-only uniqueness and RLS failures translate to typed persistence errors with causes;
10. CAS succeeds for current pair, returns False for stale pair, and never commits;
11. Replay classification is explicitly not implemented in P1;
12. Cleanup leaves zero residual disposable databases and roles.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
from typing import Any, AsyncGenerator
import urllib.parse
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.anomaly.lineage import AnomalyReproducibilityLineage
from app.anomaly.models import AnomalySignal, CalibrationMetadata
from app.correlation.aggregate import create_or_update_incident
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    IncidentPersistenceConflictError,
    IncidentPersistenceInputError,
    IncidentPersistenceStoreError,
    IncidentPersistenceTenantError,
)
from app.correlation.grouping import (
    CorrelationGroup,
    GroupingMemberReference,
)
from app.correlation.identity import compute_revision_fingerprint
from app.correlation.models import (
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentTimelineEvent,
)
from app.correlation.persistence_repository import (
    IncidentVersion,
    SqlAlchemyIncidentPersistenceRepository,
)
from app.telemetry.schemas import EventSeverity

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BACKEND_DIR = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Test Helpers
# ---------------------------------------------------------------------------


def _utc7(moment: datetime) -> str:
    """Format a UTC instant with exactly seven fractional digits."""
    return moment.strftime("%Y-%m-%dT%H:%M:%S") + f".{moment.microsecond:06d}0Z"


def _make_two_member_aggregate(
    tenant_id: UUID,
    signal1_id: UUID,
    signal2_id: UUID,
    fp1: str,
    fp2: str,
    time1: datetime,
    time2: datetime,
    proc_time: datetime,
    *,
    existing_aggregate: IncidentAggregate | None = None,
    desired_state: CorrelationWindowState = CorrelationWindowState.COLLECTING,
) -> tuple[IncidentAggregate, tuple[IncidentTimelineEvent, ...]]:
    sig1 = AnomalySignal(
        signal_id=signal1_id,
        event_time=time1,
        service="auth",
        metric_or_feature="cpu_utilization",
        model_name="isolation_forest",
        model_version="1.0",
        anomaly_score=0.9,
        severity=EventSeverity.WARNING,
        threshold_or_calibration=CalibrationMetadata(
            method="percentile", threshold_value=0.8, calibration_version="1.0"
        ),
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        environment="production",
        tenant_id=tenant_id,
    )
    lin1 = AnomalyReproducibilityLineage(
        signal_id=signal1_id,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(UUID("55555555-5555-5555-5555-555555555555"),),
        semantic_fingerprint=fp1,
    )
    sig2 = AnomalySignal(
        signal_id=signal2_id,
        event_time=time2,
        service="billing",
        metric_or_feature="memory_utilization",
        model_name="isolation_forest",
        model_version="1.0",
        anomaly_score=0.85,
        severity=EventSeverity.WARNING,
        threshold_or_calibration=CalibrationMetadata(
            method="percentile", threshold_value=0.8, calibration_version="1.0"
        ),
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_2",
        environment="production",
        tenant_id=tenant_id,
    )
    lin2 = AnomalyReproducibilityLineage(
        signal_id=signal2_id,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_2",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(UUID("66666666-6666-6666-6666-666666666666"),),
        semantic_fingerprint=fp2,
    )
    inputs = {
        signal1_id: AnomalyCorrelationInput(signal=sig1, lineage=lin1),
        signal2_id: AnomalyCorrelationInput(signal=sig2, lineage=lin2),
    }

    m1 = GroupingMemberReference(
        signal_id=signal1_id, semantic_fingerprint=fp1, service="auth", event_time=time1
    )
    m2 = GroupingMemberReference(
        signal_id=signal2_id,
        semantic_fingerprint=fp2,
        service="billing",
        event_time=time2,
    )
    left = min(signal1_id, signal2_id, key=lambda u: u.bytes)
    right = max(signal1_id, signal2_id, key=lambda u: u.bytes)
    ev = CorrelationEvidence(
        left_signal_id=left,
        right_signal_id=right,
        temporal_score=0.9,
        service_score=0.9,
        topology_score=0.9,
        symptom_score=0.9,
        context_score=0.9,
        weighted_score=0.9,
        accepted=True,
        policy_version="correlation/v1",
        reason="accepted test evidence",
    )
    times = [time1, time2]
    st = min(times)
    lt = max(times)
    span = (lt - st).total_seconds()
    group = CorrelationGroup(
        policy_version="correlation/v1",
        configuration_fingerprint="c" * 64,
        topology_snapshot_fingerprint="d" * 64,
        members=tuple(sorted([m1, m2], key=lambda m: m.signal_id.bytes)),
        accepted_evidence=(ev,),
        started_at=st,
        last_seen_at=lt,
        time_span_seconds=span,
        topology_diameter_hops=1,
        group_fingerprint="0" * 64,
    )
    res = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=proc_time,
        desired_correlation_state=desired_state,
        existing_aggregate=existing_aggregate,
    )
    return res.aggregate, res.timeline_events


# ===========================================================================
# PORTABLE FIXTURE: DISPOSABLE DB, ROLE, AND CLEANUP
# ===========================================================================


@pytest_asyncio.fixture(scope="module")
async def pg_env() -> AsyncGenerator[dict[str, Any], None]:
    admin_dsn = os.environ.get("P1_POSTGRES_ADMIN_DSN")
    if not admin_dsn:
        pytest.fail(
            "P1_POSTGRES_ADMIN_DSN environment variable is required for integration tests"
        )

    parsed = urllib.parse.urlparse(admin_dsn)
    target_maintenance_db = parsed.path.lstrip("/")
    if target_maintenance_db != "postgres":
        pytest.fail(
            f"P1_POSTGRES_ADMIN_DSN must target maintenance database 'postgres', got '{target_maintenance_db}'"
        )

    admin_host = parsed.hostname or "localhost"
    admin_port = parsed.port or 5432
    admin_user = parsed.username or "postgres"
    admin_pass = parsed.password or ""
    scheme = parsed.scheme or "postgresql"

    # Connect to admin maintenance DB
    try:
        admin_conn = await asyncpg.connect(admin_dsn)
    except Exception as exc:
        pytest.fail(f"Could not connect to PostgreSQL admin database: {exc}")

    suffix = secrets.token_hex(6)
    assert re.match(r"^[0-9a-f]{12}$", suffix)
    db_name = f"aegis_p1_{suffix}"
    role_name = f"aegis_p1_runtime_{suffix}"
    password = secrets.token_urlsafe(24)

    start_time_utc = _utc7(datetime.now(timezone.utc))

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

        # Create disposable DB and minimum-privilege runtime role
        await admin_conn.execute(f'CREATE DATABASE "{db_name}"')
        await admin_conn.execute(
            f"CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{password}' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
        )

        disp_admin_dsn = (
            f"{scheme}://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
        )
        disp_admin_conn = await asyncpg.connect(disp_admin_dsn)

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

            # Run Alembic migrations through head
            env = os.environ.copy()
            async_url = f"postgresql+asyncpg://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
            env["DATABASE_URL"] = async_url
            res = subprocess.run(
                ["poetry", "run", "alembic", "upgrade", "head"],
                cwd=BACKEND_DIR,
                env=env,
                capture_output=True,
                text=True,
            )
            assert (
                res.returncode == 0
            ), f"Alembic upgrade failed:\nstdout={res.stdout}\nstderr={res.stderr}"

            # Grant exact minimum privileges:
            # - SELECT, INSERT on incidents
            # - UPDATE on 10 mutable columns of incidents
            # - SELECT, INSERT on 4 child tables
            # - Zero DELETE on any table, zero UPDATE on child/foundation tables
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

        print(
            f"\nP1_RESOURCE_DIAGNOSTIC {json.dumps({'database_name': db_name, 'role_name': role_name, 'start_utc': start_time_utc})}"
        )

        yield {
            "suffix": suffix,
            "db_name": db_name,
            "role_name": role_name,
            "admin_dsn": admin_dsn,
            "disp_admin_dsn": disp_admin_dsn,
            "runtime_async_url": runtime_async_url,
        }

    finally:
        # Close setup admin connection
        await admin_conn.close()

        # Connect freshly for cleanup teardown
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

            # Live verification of absence
            db_rem = await cleanup_conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", db_name
            )
            role_rem = await cleanup_conn.fetchval(
                "SELECT 1 FROM pg_roles WHERE rolname = $1", role_name
            )
            assert db_rem is None, f"Database {db_name} was not dropped!"
            assert role_rem is None, f"Role {role_name} was not dropped!"

            end_time_utc = _utc7(datetime.now(timezone.utc))
            print(
                f"\nP1_CLEANUP_DIAGNOSTIC {json.dumps({'database_dropped': db_name, 'role_dropped': role_name, 'database_remainder': bool(db_rem), 'role_remainder': bool(role_rem), 'end_utc': end_time_utc})}"
            )
        finally:
            await cleanup_conn.close()


@pytest_asyncio.fixture
async def session_factory(
    pg_env: dict[str, Any],
) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(
        pg_env["runtime_async_url"], pool_size=5, max_overflow=5
    )
    sf = async_sessionmaker(engine, expire_on_commit=False)
    yield sf
    await engine.dispose()


async def _insert_tenant_row(pg_env: dict[str, Any], tenant_id: UUID) -> None:
    conn = await asyncpg.connect(pg_env["disp_admin_dsn"])
    try:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, slug, plan_id, created_at, updated_at)
            VALUES ($1, $2, $3, '00000000-0000-0000-0000-000000000001'::uuid, now(), now())
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
            f"tenant-{tenant_id.hex[:8]}",
            f"slug-{tenant_id.hex[:8]}",
        )
    finally:
        await conn.close()


# ===========================================================================
# 12 REQUIRED REAL POSTGRESQL INTEGRATION TEST CASES
# ===========================================================================


# 1. Roundtrip write/read canonical reconstruction
@pytest.mark.asyncio
async def test_write_read_roundtrip_canonical_reconstruction(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    sig1_id = uuid4()
    sig2_id = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)

    agg, events = _make_two_member_aggregate(
        tenant_id, sig1_id, sig2_id, "1" * 64, "2" * 64, time1, time2, proc_time
    )

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()

            await repo.insert_incident(agg)
            await repo.append_signal_links(agg.incident_id, agg.signal_references)
            await repo.append_correlation_evidence(
                agg.incident_id, agg.correlation_evidence
            )
            await repo.append_timeline_events(events)
            await repo.append_publication_intent(agg, intent_time=proc_time)

    # Read back in new transaction
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()

            state = await repo.load_incident(agg.incident_id)
            assert state is not None
            assert state.aggregate == agg
            assert len(state.timeline_events) == len(events)
            assert state.timeline_events == events
            assert len(state.publication_intents) == 1
            assert state.publication_intents[0].aggregate == agg
            assert state.publication_intents[0].revision == 1

            version = await repo.load_incident_version(agg.incident_id)
            assert version == IncidentVersion(
                revision=1, revision_fingerprint=agg.revision_fingerprint
            )

            pi = await repo.load_publication_intent(agg.incident_id, 1)
            assert pi is not None
            assert pi.aggregate == agg


# 2. Two-tenant read/write isolation
@pytest.mark.asyncio
async def test_two_tenant_isolation(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    common_sig1 = uuid4()
    common_sig2 = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_a)
    await _insert_tenant_row(pg_env, tenant_b)

    agg_a, events_a = _make_two_member_aggregate(
        tenant_a, common_sig1, common_sig2, "a" * 64, "b" * 64, time1, time2, proc_time
    )
    agg_b, events_b = _make_two_member_aggregate(
        tenant_b, common_sig1, common_sig2, "c" * 64, "d" * 64, time1, time2, proc_time
    )

    # Populate tenant A
    async with session_factory() as session:
        async with session.begin():
            repo_a = SqlAlchemyIncidentPersistenceRepository(session, tenant_a)
            await repo_a.bind_tenant_context()
            await repo_a.insert_incident(agg_a)
            await repo_a.append_signal_links(agg_a.incident_id, agg_a.signal_references)
            await repo_a.append_correlation_evidence(
                agg_a.incident_id, agg_a.correlation_evidence
            )
            await repo_a.append_timeline_events(events_a)
            await repo_a.append_publication_intent(agg_a, intent_time=proc_time)

    # Populate tenant B
    async with session_factory() as session:
        async with session.begin():
            repo_b = SqlAlchemyIncidentPersistenceRepository(session, tenant_b)
            await repo_b.bind_tenant_context()
            await repo_b.insert_incident(agg_b)
            await repo_b.append_signal_links(agg_b.incident_id, agg_b.signal_references)
            await repo_b.append_correlation_evidence(
                agg_b.incident_id, agg_b.correlation_evidence
            )
            await repo_b.append_timeline_events(events_b)
            await repo_b.append_publication_intent(agg_b, intent_time=proc_time)

    # Verify Tenant A cannot see Tenant B's incident
    async with session_factory() as session:
        async with session.begin():
            repo_a = SqlAlchemyIncidentPersistenceRepository(session, tenant_a)
            await repo_a.bind_tenant_context()
            assert await repo_a.load_incident(agg_b.incident_id) is None
            assert await repo_a.load_incident_version(agg_b.incident_id) is None

            # Membership resolution returns only Tenant A's incident
            ids = await repo_a.resolve_incident_ids([common_sig1, common_sig2])
            assert ids == (agg_a.incident_id,)

    # Verify Tenant B cannot see Tenant A's incident
    async with session_factory() as session:
        async with session.begin():
            repo_b = SqlAlchemyIncidentPersistenceRepository(session, tenant_b)
            await repo_b.bind_tenant_context()
            assert await repo_b.load_incident(agg_a.incident_id) is None
            assert await repo_b.load_incident_version(agg_a.incident_id) is None

            ids = await repo_b.resolve_incident_ids([common_sig1, common_sig2])
            assert ids == (agg_b.incident_id,)


# 3. Missing and mismatched transaction-local context fail closed
@pytest.mark.asyncio
async def test_missing_and_mismatched_tenant_context(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    other_tenant = uuid4()

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            # 1. Missing context
            with pytest.raises(IncidentPersistenceTenantError, match="not set"):
                await repo.load_incident(uuid4())

            # 2. Mismatched context
            await session.execute(
                text("SELECT set_config('app.current_tenant_id', :id, true)"),
                {"id": str(other_tenant)},
            )
            with pytest.raises(IncidentPersistenceTenantError, match="mismatch"):
                await repo.load_incident(uuid4())


# 4. Pooled context reset after commit and rollback
@pytest.mark.asyncio
async def test_pooled_context_reset_after_commit_and_rollback(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()

    # After commit
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            val = await session.scalar(
                text("SELECT current_setting('app.current_tenant_id', true)")
            )
            assert val == str(tenant_id)

    async with session_factory() as session:
        async with session.begin():
            # In a new transaction, setting must be NULL or empty
            val = await session.scalar(
                text("SELECT current_setting('app.current_tenant_id', true)")
            )
            assert val is None or val == ""

    # After rollback
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            val = await session.scalar(
                text("SELECT current_setting('app.current_tenant_id', true)")
            )
            assert val == str(tenant_id)
            await session.rollback()

    async with session_factory() as session:
        async with session.begin():
            val = await session.scalar(
                text("SELECT current_setting('app.current_tenant_id', true)")
            )
            assert val is None or val == ""


# 5. Runtime role is non-superuser and non-bypass
@pytest.mark.asyncio
async def test_runtime_role_attributes(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    async with session_factory() as session:
        res = await session.execute(
            text(
                "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
        )
        row = res.first()
        assert row is not None
        assert row[0] is False, "Runtime role must not be superuser"
        assert row[1] is False, "Runtime role must not bypass RLS"


# 6. Membership resolution returns only tenant-local incident IDs
@pytest.mark.asyncio
async def test_membership_resolution_tenant_local(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_a = uuid4()
    sig1 = uuid4()
    sig2 = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_a)
    agg, _ = _make_two_member_aggregate(
        tenant_a, sig1, sig2, "e" * 64, "f" * 64, time1, time2, proc_time
    )

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_a)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg)
            await repo.append_signal_links(agg.incident_id, agg.signal_references)

            res = await repo.resolve_incident_ids([sig1])
            assert res == (agg.incident_id,)


# 7. Parent FOR UPDATE blocks competing transaction
@pytest.mark.asyncio
async def test_parent_for_update_row_locking(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    sig1 = uuid4()
    sig2 = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)
    agg, _ = _make_two_member_aggregate(
        tenant_id, sig1, sig2, "1" * 64, "2" * 64, time1, time2, proc_time
    )

    # Insert incident
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg)

    # Acquire lock in Tx 1
    session1 = session_factory()
    await session1.begin()
    repo1 = SqlAlchemyIncidentPersistenceRepository(session1, tenant_id)
    await repo1.bind_tenant_context()
    ver1 = await repo1.load_incident_version(agg.incident_id, for_update=True)
    assert ver1 is not None

    # In competing Tx 2, attempting FOR UPDATE NOWAIT must fail with 55P03 lock_not_available
    session2 = session_factory()
    await session2.begin()
    repo2 = SqlAlchemyIncidentPersistenceRepository(session2, tenant_id)
    await repo2.bind_tenant_context()

    try:
        with pytest.raises(Exception) as exc_info:
            await session2.execute(
                text(
                    "SELECT 1 FROM incidents WHERE tenant_id = :t AND incident_id = :i FOR UPDATE NOWAIT"
                ),
                {"t": str(tenant_id), "i": str(agg.incident_id)},
            )
        assert (
            "55P03" in str(exc_info.value)
            or "could not obtain lock" in str(exc_info.value).lower()
        )
    finally:
        await session2.rollback()
        await session2.close()
        await session1.rollback()
        await session1.close()


# 8. Caller rollback removes all repository writes
@pytest.mark.asyncio
async def test_caller_rollback_purges_all_writes(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    sig1 = uuid4()
    sig2 = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)
    agg, events = _make_two_member_aggregate(
        tenant_id, sig1, sig2, "1" * 64, "2" * 64, time1, time2, proc_time
    )

    before = await _t1_tenant_table_state(pg_env, tenant_id)

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg)
            await repo.append_signal_links(agg.incident_id, agg.signal_references)
            await repo.append_correlation_evidence(
                agg.incident_id, agg.correlation_evidence
            )
            await repo.append_timeline_events(events)
            await repo.append_publication_intent(agg, intent_time=proc_time)
            for table in before:
                staged = await session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE tenant_id = :tenant"),
                    {"tenant": tenant_id},
                )
                assert staged > 0, f"No real writes observed in {table} before rollback"
            # Explicit rollback
            await session.rollback()

    # Query in new transaction: row must be absent
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            assert await repo.load_incident(agg.incident_id) is None

    assert await _t1_tenant_table_state(pg_env, tenant_id) == before


# 9. Append-only uniqueness error translation (SQLSTATE 23505)
@pytest.mark.asyncio
async def test_append_uniqueness_error_translation(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    sig1 = uuid4()
    sig2 = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)
    agg, _ = _make_two_member_aggregate(
        tenant_id, sig1, sig2, "1" * 64, "2" * 64, time1, time2, proc_time
    )

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg)

            # Duplicate parent insert -> ConflictError (23505)
            with pytest.raises(IncidentPersistenceConflictError) as exc_info:
                await repo.insert_incident(agg)
            assert exc_info.value.__cause__ is not None


# 10. CAS update rowcount behavior
@pytest.mark.asyncio
async def test_cas_update_behavior(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    sig1 = uuid4()
    sig2 = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)
    agg, _ = _make_two_member_aggregate(
        tenant_id, sig1, sig2, "1" * 64, "2" * 64, time1, time2, proc_time
    )

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg)

    # 1. CAS update with stale revision returns False
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            stale_res = await repo.update_incident_cas(
                agg, expected_revision=999, expected_revision_fingerprint="0" * 64
            )
            assert stale_res is False

    # 2. CAS update with current pair succeeds (rowcount == 1)
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            # Construct revision 2 aggregate with valid computed fingerprint
            agg_v2_prov = agg.model_copy(update={"revision": 2})
            agg_v2 = agg.model_copy(
                update={
                    "revision": 2,
                    "revision_fingerprint": compute_revision_fingerprint(agg_v2_prov),
                }
            )
            ok = await repo.update_incident_cas(
                agg_v2,
                expected_revision=1,
                expected_revision_fingerprint=agg.revision_fingerprint,
            )
            assert ok is True

    # Check updated revision in DB
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            ver = await repo.load_incident_version(agg.incident_id)
            assert ver is not None
            assert ver.revision == 2


# 11. Replay classification is explicitly not implemented by P1
@pytest.mark.asyncio
async def test_replay_classification_not_implemented() -> None:
    from app.correlation import persistence_repository

    assert not hasattr(persistence_repository, "create_or_update_incident")
    assert not hasattr(persistence_repository, "classify_transition")
    assert not hasattr(persistence_repository, "is_replay")
    assert not hasattr(SqlAlchemyIncidentPersistenceRepository, "classify_transition")


# 12. Cleanup leaves zero remainder (verified live in fixture teardown)
@pytest.mark.asyncio
async def test_disposable_resource_verification(pg_env: dict[str, Any]) -> None:
    conn = await asyncpg.connect(pg_env["admin_dsn"])
    try:
        db_name = pg_env["db_name"]
        role_name = pg_env["role_name"]

        # Verify that while running, the resources exist
        db_exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        role_exists = await conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", role_name
        )
        assert db_exists == 1
        assert role_exists == 1
    finally:
        await conn.close()


# ===========================================================================
# 13. REQUIRED P1-R1 INTEGRATION TEST CLOSURES (SECTION 12)
# ===========================================================================


_INCIDENT_TABLES = (
    "incidents",
    "incident_signal_links",
    "incident_correlation_evidence",
    "incident_timeline_events",
    "incident_publication_intents",
)


def _sqlstate_of(exc: BaseException) -> str:
    """Walk the cause/context chain and return the PostgreSQL SQLSTATE or NONE."""
    seen: set[int] = set()
    stack: list[BaseException | None] = [exc]
    while stack:
        cur = stack.pop()
        if cur is None or id(cur) in seen:
            continue
        seen.add(id(cur))
        for attr in ("sqlstate", "pgcode"):
            val = getattr(cur, attr, None)
            if isinstance(val, str) and re.fullmatch(r"[0-9A-Z]{5}", val):
                return val
        stack.append(getattr(cur, "orig", None))
        stack.append(cur.__cause__)
        stack.append(cur.__context__)
    return "NONE"


async def _tenant_table_counts(
    conn: asyncpg.Connection, tenant_id: UUID
) -> dict[str, int]:
    out: dict[str, int] = {}
    for table in _INCIDENT_TABLES:
        out[table] = int(
            await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE tenant_id = $1", tenant_id
            )
        )
    return out


async def _tenant_table_hashes(
    conn: asyncpg.Connection, tenant_id: UUID
) -> dict[str, str]:
    out: dict[str, str] = {}
    for table in _INCIDENT_TABLES:
        out[table] = str(
            await conn.fetchval(
                f"SELECT coalesce(md5(string_agg(t::text, '|' ORDER BY t::text)), 'EMPTY') "
                f"FROM {table} t WHERE t.tenant_id = $1",
                tenant_id,
            )
        )
    return out


@pytest.mark.asyncio
async def test_cross_tenant_write_rejection_all_repository_primitives(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    # 7.1 Seed: foundation rows through the administrative connection only.
    await _insert_tenant_row(pg_env, tenant_a)
    await _insert_tenant_row(pg_env, tenant_b)

    agg_b, events_b = _make_two_member_aggregate(
        tenant_b, uuid4(), uuid4(), "c" * 64, "d" * 64, time1, time2, proc_time
    )

    # Persist one complete valid Tenant B graph through a Tenant B repository.
    async with session_factory() as session:
        async with session.begin():
            repo_b = SqlAlchemyIncidentPersistenceRepository(session, tenant_b)
            await repo_b.bind_tenant_context()
            await repo_b.insert_incident(agg_b)
            await repo_b.append_signal_links(agg_b.incident_id, agg_b.signal_references)
            await repo_b.append_correlation_evidence(
                agg_b.incident_id, agg_b.correlation_evidence
            )
            await repo_b.append_timeline_events(events_b)
            await repo_b.append_publication_intent(agg_b, intent_time=proc_time)

    admin_conn = await asyncpg.connect(pg_env["disp_admin_dsn"])
    try:
        seed_a = await _tenant_table_counts(admin_conn, tenant_a)
        seed_b = await _tenant_table_counts(admin_conn, tenant_b)
        seed_b_hash = await _tenant_table_hashes(admin_conn, tenant_b)
    finally:
        await admin_conn.close()

    assert all(v == 0 for v in seed_a.values()), seed_a
    assert seed_b["incidents"] == 1
    assert seed_b["incident_signal_links"] == 2
    assert seed_b["incident_correlation_evidence"] == 1
    assert seed_b["incident_timeline_events"] >= 1
    assert seed_b["incident_publication_intents"] == 1
    assert len(agg_b.signal_references) == 2
    assert len(agg_b.correlation_evidence) >= 1
    assert len(events_b) >= 1

    dependency = {
        "tenant_b_incident_id": str(agg_b.incident_id),
        "tenant_b_signal_ids": sorted(
            str(r.signal_id) for r in agg_b.signal_references
        ),
        "tenant_b_revision": agg_b.revision,
        "tenant_b_revision_fingerprint": agg_b.revision_fingerprint,
        "tenant_b_counts": seed_b,
    }

    wrong_tenant_class = "IncidentPersistenceTenantError"
    typed_class = "TYPED_PERSISTENCE_ERROR"

    async def _case_insert(repo: SqlAlchemyIncidentPersistenceRepository) -> None:
        await repo.insert_incident(agg_b)

    async def _case_links(repo: SqlAlchemyIncidentPersistenceRepository) -> None:
        await repo.append_signal_links(agg_b.incident_id, agg_b.signal_references)

    async def _case_intent(repo: SqlAlchemyIncidentPersistenceRepository) -> None:
        await repo.append_publication_intent(agg_b, intent_time=proc_time)

    async def _case_cas(repo: SqlAlchemyIncidentPersistenceRepository) -> None:
        await repo.update_incident_cas(
            agg_b,
            expected_revision=agg_b.revision,
            expected_revision_fingerprint=agg_b.revision_fingerprint,
        )

    async def _case_evidence(repo: SqlAlchemyIncidentPersistenceRepository) -> None:
        await repo.append_correlation_evidence(
            agg_b.incident_id, agg_b.correlation_evidence
        )

    async def _case_timeline(repo: SqlAlchemyIncidentPersistenceRepository) -> None:
        await repo.append_timeline_events(events_b)

    cases = [
        ("C1_INSERT_INCIDENT", "insert_incident", wrong_tenant_class, _case_insert),
        (
            "C2_APPEND_SIGNAL_LINKS",
            "append_signal_links",
            wrong_tenant_class,
            _case_links,
        ),
        (
            "C3_APPEND_PUBLICATION_INTENT",
            "append_publication_intent",
            wrong_tenant_class,
            _case_intent,
        ),
        (
            "C4_UPDATE_INCIDENT_CAS",
            "update_incident_cas",
            wrong_tenant_class,
            _case_cas,
        ),
        (
            "C5_APPEND_CORRELATION_EVIDENCE",
            "append_correlation_evidence",
            typed_class,
            _case_evidence,
        ),
        (
            "C6_APPEND_TIMELINE_EVENTS",
            "append_timeline_events",
            typed_class,
            _case_timeline,
        ),
    ]

    rows: list[dict[str, Any]] = []
    for case_id, primitive, expected_class, runner in cases:
        pre = await asyncpg.connect(pg_env["disp_admin_dsn"])
        try:
            before_a = await _tenant_table_counts(pre, tenant_a)
            before_b = await _tenant_table_counts(pre, tenant_b)
        finally:
            await pre.close()

        observed_class = "NONE"
        observed_sqlstate = "NONE"
        observed_typed = False
        async with session_factory() as session:
            try:
                async with session.begin():
                    repo_a = SqlAlchemyIncidentPersistenceRepository(session, tenant_a)
                    await repo_a.bind_tenant_context()
                    await runner(repo_a)
            except Exception as exc:
                observed_class = type(exc).__name__
                observed_sqlstate = _sqlstate_of(exc)
                observed_typed = isinstance(
                    exc,
                    (
                        IncidentPersistenceTenantError,
                        IncidentPersistenceInputError,
                        IncidentPersistenceConflictError,
                        IncidentPersistenceStoreError,
                    ),
                )

        post = await asyncpg.connect(pg_env["disp_admin_dsn"])
        try:
            after_a = await _tenant_table_counts(post, tenant_a)
            after_b = await _tenant_table_counts(post, tenant_b)
            after_b_hash = await _tenant_table_hashes(post, tenant_b)
        finally:
            await post.close()

        if expected_class == typed_class:
            class_ok = observed_typed and observed_sqlstate in ("23503", "42501")
        else:
            class_ok = observed_class == expected_class and observed_sqlstate == "NONE"
        counts_ok = (
            before_a == after_a
            and all(v == 0 for v in after_a.values())
            and before_b == after_b
            and after_b == seed_b
            and after_b_hash == seed_b_hash
        )
        rows.append(
            {
                "case_id": case_id,
                "primitive": primitive,
                "repository_tenant": "TENANT_A",
                "source_tenant": "TENANT_B",
                "target_tenant": "TENANT_B",
                "dependency_counts": seed_b,
                "expected_class": expected_class,
                "observed_class": observed_class,
                "observed_sqlstate": observed_sqlstate,
                "before_tenant_a": before_a,
                "after_tenant_a": after_a,
                "before_tenant_b": before_b,
                "after_tenant_b": after_b,
                "tenant_b_hashes_unchanged": after_b_hash == seed_b_hash,
                "status": "PASS" if (class_ok and counts_ok) else "FAIL",
            }
        )

    print("\nP1_R2_TENANT_WRITE_DIAGNOSTIC_BEGIN")
    print(json.dumps({"dependency": dependency}, sort_keys=True))
    for row in rows:
        print(json.dumps(row, sort_keys=True))
    print("P1_R2_TENANT_WRITE_DIAGNOSTIC_END")

    failed = [r["case_id"] for r in rows if r["status"] != "PASS"]
    assert not failed, f"Tenant-negative cases failed: {failed}"


_EXPECTED_TABLE_PRIVILEGES: dict[str, dict[str, bool]] = {
    "plans": {},
    "tenants": {},
    "users": {},
    "tenant_memberships": {},
    "sessions": {},
    "incidents": {"SELECT": True, "INSERT": True},
    "incident_signal_links": {"SELECT": True, "INSERT": True},
    "incident_correlation_evidence": {"SELECT": True, "INSERT": True},
    "incident_timeline_events": {"SELECT": True, "INSERT": True},
    "incident_publication_intents": {"SELECT": True, "INSERT": True},
}
_TABLE_PRIVILEGE_NAMES = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "REFERENCES",
    "TRIGGER",
)
_MUTABLE_INCIDENT_COLUMNS = frozenset(
    {
        "status",
        "correlation_state",
        "severity",
        "started_at",
        "last_seen_at",
        "updated_at",
        "revision",
        "affected_services",
        "revision_fingerprint",
        "aggregate_payload",
    }
)
_FOUNDATION_PROBES: dict[str, dict[str, str]] = {
    "plans": {
        "SELECT": "SELECT id, name FROM plans",
        "INSERT": (
            "INSERT INTO plans (id, name, monthly_price_cents, telemetry_ingest_limit_mb, "
            "agent_concurrency_limit, features, created_at, updated_at) VALUES "
            "(gen_random_uuid(), 'probe-plan', 0, 0, 0, '{}'::jsonb, now(), now())"
        ),
        "UPDATE": "UPDATE plans SET name = 'probe-plan' WHERE false",
        "DELETE": "DELETE FROM plans WHERE false",
    },
    "tenants": {
        "SELECT": "SELECT id, name FROM tenants",
        "INSERT": (
            "INSERT INTO tenants (id, name, slug, plan_id, created_at, updated_at) VALUES "
            "(gen_random_uuid(), 'probe', 'probe', "
            "'00000000-0000-0000-0000-000000000001'::uuid, now(), now())"
        ),
        "UPDATE": "UPDATE tenants SET name = 'probe' WHERE false",
        "DELETE": "DELETE FROM tenants WHERE false",
    },
    "users": {
        "SELECT": "SELECT id, email FROM users",
        "INSERT": (
            "INSERT INTO users (id, email, first_name, last_name, created_at, updated_at) "
            "VALUES (gen_random_uuid(), 'probe@example.invalid', 'p', 'p', now(), now())"
        ),
        "UPDATE": "UPDATE users SET first_name = 'probe' WHERE false",
        "DELETE": "DELETE FROM users WHERE false",
    },
    "tenant_memberships": {
        "SELECT": "SELECT id, tenant_id FROM tenant_memberships",
        "INSERT": (
            "INSERT INTO tenant_memberships (id, tenant_id, user_id, role, created_at, "
            "updated_at) VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), "
            "'viewer', now(), now())"
        ),
        "UPDATE": "UPDATE tenant_memberships SET role = 'viewer' WHERE false",
        "DELETE": "DELETE FROM tenant_memberships WHERE false",
    },
    "sessions": {
        "SELECT": "SELECT id, user_id FROM sessions",
        "INSERT": (
            "INSERT INTO sessions (id, user_id, tenant_id, ip_address, user_agent, "
            "expires_at, created_at, updated_at) VALUES (gen_random_uuid(), "
            "gen_random_uuid(), gen_random_uuid(), '127.0.0.1'::inet, 'probe', now(), "
            "now(), now())"
        ),
        "UPDATE": "UPDATE sessions SET user_agent = 'probe' WHERE false",
        "DELETE": "DELETE FROM sessions WHERE false",
    },
}


@pytest.mark.asyncio
async def test_runtime_role_exact_minimum_privileges(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    role_name = pg_env["role_name"]
    rows: list[dict[str, Any]] = []
    problems: list[str] = []

    admin_conn = await asyncpg.connect(pg_env["disp_admin_dsn"])
    try:
        attrs = await admin_conn.fetchrow(
            "SELECT rolsuper, rolbypassrls, rolcreatedb, rolcreaterole, rolcanlogin "
            "FROM pg_roles WHERE rolname = $1",
            role_name,
        )
        assert attrs is not None
        rows.append(
            {
                "kind": "role_attributes",
                "role": role_name,
                "rolsuper": attrs["rolsuper"],
                "rolbypassrls": attrs["rolbypassrls"],
                "rolcreatedb": attrs["rolcreatedb"],
                "rolcreaterole": attrs["rolcreaterole"],
                "rolcanlogin": attrs["rolcanlogin"],
            }
        )
        if attrs["rolsuper"] or attrs["rolbypassrls"]:
            problems.append("role is superuser or bypassrls")

        db_priv = {
            p: bool(
                await admin_conn.fetchval(
                    "SELECT has_database_privilege($1, current_database(), $2)",
                    role_name,
                    p,
                )
            )
            for p in ("CONNECT", "TEMPORARY", "CREATE")
        }
        rows.append({"kind": "database", **{k.lower(): v for k, v in db_priv.items()}})
        if db_priv != {"CONNECT": True, "TEMPORARY": False, "CREATE": False}:
            problems.append(f"database privileges {db_priv}")

        schema_priv = {
            p: bool(
                await admin_conn.fetchval(
                    "SELECT has_schema_privilege($1, 'public', $2)", role_name, p
                )
            )
            for p in ("USAGE", "CREATE")
        }
        rows.append(
            {"kind": "schema", **{k.lower(): v for k, v in schema_priv.items()}}
        )
        if schema_priv != {"USAGE": True, "CREATE": False}:
            problems.append(f"schema privileges {schema_priv}")

        for table, granted in _EXPECTED_TABLE_PRIVILEGES.items():
            observed: dict[str, bool] = {}
            for priv in _TABLE_PRIVILEGE_NAMES:
                observed[priv] = bool(
                    await admin_conn.fetchval(
                        "SELECT has_table_privilege($1, $2, $3)",
                        role_name,
                        f"public.{table}",
                        priv,
                    )
                )
            expected = {p: granted.get(p, False) for p in _TABLE_PRIVILEGE_NAMES}
            rows.append(
                {
                    "kind": "table_privileges",
                    "table": table,
                    "observed": observed,
                    "expected": expected,
                    "status": "PASS" if observed == expected else "FAIL",
                }
            )
            if observed != expected:
                problems.append(f"table privileges mismatch on {table}")

        columns = [
            r["attname"]
            for r in await admin_conn.fetch(
                "SELECT attname FROM pg_attribute WHERE attrelid = 'public.incidents'::regclass "
                "AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            )
        ]
        for col in columns:
            upd = bool(
                await admin_conn.fetchval(
                    "SELECT has_column_privilege($1, 'public.incidents', $2, 'UPDATE')",
                    role_name,
                    col,
                )
            )
            expected_upd = col in _MUTABLE_INCIDENT_COLUMNS
            rows.append(
                {
                    "kind": "incident_column_update",
                    "column": col,
                    "observed": upd,
                    "expected": expected_upd,
                    "status": "PASS" if upd == expected_upd else "FAIL",
                }
            )
            if upd != expected_upd:
                problems.append(f"column update mismatch on incidents.{col}")
        if not _MUTABLE_INCIDENT_COLUMNS.issubset(set(columns)):
            problems.append("a mutable column is absent from incidents")

        seq_rows = await admin_conn.fetch(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind = 'S' AND n.nspname = 'public' ORDER BY c.relname"
        )
        seq_report: list[dict[str, Any]] = []
        for s in seq_rows:
            privs = {
                p: bool(
                    await admin_conn.fetchval(
                        "SELECT has_sequence_privilege($1, $2, $3)",
                        role_name,
                        f"public.{s['relname']}",
                        p,
                    )
                )
                for p in ("USAGE", "SELECT", "UPDATE")
            }
            seq_report.append({"sequence": s["relname"], "privileges": privs})
            if any(privs.values()):
                problems.append(f"sequence privilege granted on {s['relname']}")
        rows.append({"kind": "sequences", "sequences": seq_report})
    finally:
        await admin_conn.close()

    # Foundation probes: every operation on every foundation table must be 42501,
    # each inside its own savepoint with rollback after the error.
    async with session_factory() as session:
        async with session.begin():
            for table, ops in _FOUNDATION_PROBES.items():
                for op in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                    sqlstate = "NONE"
                    sp = await session.begin_nested()
                    try:
                        await session.execute(text(ops[op]))
                        await sp.rollback()
                    except Exception as exc:
                        sqlstate = _sqlstate_of(exc)
                        if sp.is_active:
                            await sp.rollback()
                    rows.append(
                        {
                            "kind": "foundation_probe",
                            "table": table,
                            "operation": op,
                            "expected_sqlstate": "42501",
                            "observed_sqlstate": sqlstate,
                            "status": "PASS" if sqlstate == "42501" else "FAIL",
                        }
                    )
                    if sqlstate != "42501":
                        problems.append(f"probe {table}.{op} sqlstate {sqlstate}")

    print("\nP1_R2_PRIVILEGE_DIAGNOSTIC_BEGIN")
    for row in rows:
        print(json.dumps(row, sort_keys=True))
    print("P1_R2_PRIVILEGE_DIAGNOSTIC_END")

    assert not problems, f"Privilege matrix problems: {problems}"


@pytest.mark.asyncio
async def test_parent_for_update_waiter_completes_after_release(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)
    agg, _ = _make_two_member_aggregate(
        tenant_id, uuid4(), uuid4(), "1" * 64, "2" * 64, time1, time2, proc_time
    )

    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg)

    # Tx 1 acquires FOR UPDATE lock
    session1 = session_factory()
    await session1.begin()
    repo1 = SqlAlchemyIncidentPersistenceRepository(session1, tenant_id)
    await repo1.bind_tenant_context()
    ver1 = await repo1.load_incident_version(agg.incident_id, for_update=True)
    assert ver1 is not None

    # Tx 2 calls load_incident_version(..., for_update=True) without NOWAIT
    session2 = session_factory()
    await session2.begin()
    repo2 = SqlAlchemyIncidentPersistenceRepository(session2, tenant_id)
    await repo2.bind_tenant_context()

    tx2_started = asyncio.Event()
    tx2_result: list[Any] = []
    tx2_error: list[Exception] = []

    async def tx2_worker() -> None:
        try:
            tx2_started.set()
            res = await repo2.load_incident_version(agg.incident_id, for_update=True)
            tx2_result.append(res)
        except Exception as exc:
            tx2_error.append(exc)

    task2 = asyncio.create_task(tx2_worker())
    await tx2_started.wait()

    # Give worker time to block on row lock
    await asyncio.sleep(0.4)
    pending_while_held = not task2.done()
    assert pending_while_held
    assert len(tx2_result) == 0

    # Release Tx 1 lock via rollback
    release_mark = time.perf_counter()
    await session1.rollback()
    await session1.close()

    # Tx 2 worker should now unblock and complete
    await asyncio.wait_for(task2, timeout=5.0)
    completion_after_release = time.perf_counter() - release_mark
    assert len(tx2_error) == 0
    assert len(tx2_result) == 1
    assert tx2_result[0] == IncidentVersion(
        revision=1, revision_fingerprint=agg.revision_fingerprint
    )
    print(
        "\nP1_R2_LOCK_DIAGNOSTIC "
        + json.dumps(
            {
                "holder_locked": ver1 is not None,
                "waiter_started": tx2_started.is_set(),
                "waiter_pending_while_held": pending_while_held,
                "waiter_error_count": len(tx2_error),
                "waiter_completed_after_release": True,
                "waiter_result_matches": True,
                "hold_seconds_before_release": 0.4,
                "completion_after_release_seconds": round(completion_after_release, 6),
                "completion_bound_seconds": 5.0,
                "status": "PASS",
            },
            sort_keys=True,
        )
    )

    await session2.rollback()
    await session2.close()


@pytest.mark.asyncio
async def test_cas_uses_valid_material_revision(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant_id = uuid4()
    sig1_id = uuid4()
    sig2_id = uuid4()
    time1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    time2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
    proc_time1 = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)
    proc_time2 = datetime(2026, 1, 1, 10, 6, 0, tzinfo=timezone.utc)

    await _insert_tenant_row(pg_env, tenant_id)

    # 1. Build initial aggregate v1
    agg_v1, events_v1 = _make_two_member_aggregate(
        tenant_id, sig1_id, sig2_id, "1" * 64, "2" * 64, time1, time2, proc_time1
    )

    # 2. Insert v1
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            await repo.insert_incident(agg_v1)
            await repo.append_signal_links(agg_v1.incident_id, agg_v1.signal_references)
            await repo.append_correlation_evidence(
                agg_v1.incident_id, agg_v1.correlation_evidence
            )
            await repo.append_timeline_events(events_v1)
            await repo.append_publication_intent(agg_v1, intent_time=proc_time1)

    # 3. Build genuine revision 2 via create_or_update_incident
    sig1 = AnomalySignal(
        signal_id=sig1_id,
        event_time=time1,
        service="auth",
        metric_or_feature="cpu_utilization",
        model_name="isolation_forest",
        model_version="1.0",
        anomaly_score=0.9,
        severity=EventSeverity.WARNING,
        threshold_or_calibration=CalibrationMetadata(
            method="percentile", threshold_value=0.8, calibration_version="1.0"
        ),
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        environment="production",
        tenant_id=tenant_id,
    )
    lin1 = AnomalyReproducibilityLineage(
        signal_id=sig1_id,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(UUID("55555555-5555-5555-5555-555555555555"),),
        semantic_fingerprint="1" * 64,
    )
    sig2 = AnomalySignal(
        signal_id=sig2_id,
        event_time=time2,
        service="billing",
        metric_or_feature="memory_utilization",
        model_name="isolation_forest",
        model_version="1.0",
        anomaly_score=0.85,
        severity=EventSeverity.WARNING,
        threshold_or_calibration=CalibrationMetadata(
            method="percentile", threshold_value=0.8, calibration_version="1.0"
        ),
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_2",
        environment="production",
        tenant_id=tenant_id,
    )
    lin2 = AnomalyReproducibilityLineage(
        signal_id=sig2_id,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_2",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(UUID("66666666-6666-6666-6666-666666666666"),),
        semantic_fingerprint="2" * 64,
    )
    inputs = {
        sig1_id: AnomalyCorrelationInput(signal=sig1, lineage=lin1),
        sig2_id: AnomalyCorrelationInput(signal=sig2, lineage=lin2),
    }
    m1 = GroupingMemberReference(
        signal_id=sig1_id,
        semantic_fingerprint="1" * 64,
        service="auth",
        event_time=time1,
    )
    m2 = GroupingMemberReference(
        signal_id=sig2_id,
        semantic_fingerprint="2" * 64,
        service="billing",
        event_time=time2,
    )
    left = min(sig1_id, sig2_id, key=lambda u: u.bytes)
    right = max(sig1_id, sig2_id, key=lambda u: u.bytes)
    ev = CorrelationEvidence(
        left_signal_id=left,
        right_signal_id=right,
        temporal_score=0.9,
        service_score=0.9,
        topology_score=0.9,
        symptom_score=0.9,
        context_score=0.9,
        weighted_score=0.9,
        accepted=True,
        policy_version="correlation/v1",
        reason="accepted test evidence",
    )
    group = CorrelationGroup(
        policy_version="correlation/v1",
        configuration_fingerprint="c" * 64,
        topology_snapshot_fingerprint="d" * 64,
        members=tuple(sorted([m1, m2], key=lambda m: m.signal_id.bytes)),
        accepted_evidence=(ev,),
        started_at=min(time1, time2),
        last_seen_at=max(time1, time2),
        time_span_seconds=(max(time1, time2) - min(time1, time2)).total_seconds(),
        topology_diameter_hops=1,
        group_fingerprint="0" * 64,
    )

    update_res = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=proc_time2,
        desired_correlation_state=CorrelationWindowState.SETTLING,
        existing_aggregate=agg_v1,
    )
    agg_v2 = update_res.aggregate
    assert agg_v2.revision == 2
    assert agg_v2.revision_fingerprint != agg_v1.revision_fingerprint

    # 4. Negative test: stale fingerprint copy rejected before SQL
    stale_copy = agg_v2.model_copy(
        update={"revision_fingerprint": agg_v1.revision_fingerprint}
    )
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            with pytest.raises(
                IncidentPersistenceInputError, match="revision_fingerprint mismatch"
            ):
                await repo.update_incident_cas(
                    stale_copy,
                    expected_revision=1,
                    expected_revision_fingerprint=agg_v1.revision_fingerprint,
                )

    # 5. Stale expected pair CAS returns False
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            stale_cas = await repo.update_incident_cas(
                agg_v2,
                expected_revision=999,
                expected_revision_fingerprint="0" * 64,
            )
            assert stale_cas is False

    # 6. Current expected pair CAS returns True
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            cas_ok = await repo.update_incident_cas(
                agg_v2,
                expected_revision=1,
                expected_revision_fingerprint=agg_v1.revision_fingerprint,
            )
            assert cas_ok is True

    # 7. Verify persisted revision is now 2
    async with session_factory() as session:
        async with session.begin():
            repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
            await repo.bind_tenant_context()
            ver = await repo.load_incident_version(agg_v1.incident_id)
            assert ver is not None
            assert ver.revision == 2
            assert ver.revision_fingerprint == agg_v2.revision_fingerprint


async def _t1_tenant_table_state(
    pg_env: dict[str, Any], tenant_id: UUID
) -> dict[str, Any]:
    """Read five committed row sets from a separate administrative connection."""
    keys = {
        "incidents": "incident_id",
        "incident_signal_links": "incident_id, signal_id",
        "incident_correlation_evidence": "incident_id, left_signal_id, right_signal_id",
        "incident_timeline_events": "event_id",
        "incident_publication_intents": "incident_id, revision",
    }
    connection = await asyncpg.connect(pg_env["disp_admin_dsn"])
    try:
        snapshot = {}
        for table, order in keys.items():
            rows = await connection.fetch(
                f"SELECT * FROM {table} WHERE tenant_id = $1 ORDER BY {order}",
                tenant_id,
            )
            snapshot[table] = dict(
                count=len(rows),
                sha256=hashlib.sha256(
                    str([dict(row) for row in rows]).encode()
                ).hexdigest(),
            )
        return snapshot
    finally:
        await connection.close()


async def test_t1_timeline_roundtrip_orders_ties_across_revisions(
    pg_env: dict[str, Any], session_factory: async_sessionmaker[AsyncSession]
) -> None:
    tenant, signal1, signal2 = uuid4(), uuid4(), uuid4()
    moment = datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc)
    await _insert_tenant_row(pg_env, tenant)
    args = (tenant, signal1, signal2, "a" * 64, "b" * 64, moment, moment, moment)
    initial, first_events = _make_two_member_aggregate(*args)
    updated, second_events = _make_two_member_aggregate(
        *args, existing_aggregate=initial, desired_state=CorrelationWindowState.SETTLING
    )
    assert updated.revision == initial.revision + 1
    assert first_events and second_events
    assert {event.event_time for event in (*first_events, *second_events)} == {moment}
    async with session_factory() as session:
        async with session.begin():
            repository = SqlAlchemyIncidentPersistenceRepository(session, tenant)
            await repository.bind_tenant_context()
            await repository.insert_incident(initial)
            await repository.append_signal_links(
                initial.incident_id, initial.signal_references
            )
            await repository.append_correlation_evidence(
                initial.incident_id, initial.correlation_evidence
            )
            await repository.append_timeline_events(first_events)
            await repository.append_publication_intent(initial, intent_time=moment)
    async with session_factory() as session:
        async with session.begin():
            repository = SqlAlchemyIncidentPersistenceRepository(session, tenant)
            await repository.bind_tenant_context()
            await repository.append_timeline_events(second_events)
            await repository.append_publication_intent(updated, intent_time=moment)
            assert await repository.update_incident_cas(
                updated,
                expected_revision=initial.revision,
                expected_revision_fingerprint=initial.revision_fingerprint,
            )
    expected = (*first_events, *second_events)
    async with session_factory() as session:
        async with session.begin():
            repository = SqlAlchemyIncidentPersistenceRepository(session, tenant)
            await repository.bind_tenant_context()
            stored = await repository.load_incident(initial.incident_id)
            assert stored is not None and stored.aggregate == updated
            assert stored.timeline_events == expected
            assert tuple(intent.revision for intent in stored.publication_intents) == (
                1,
                2,
            )
            rows = (
                await session.execute(
                    text(
                        "SELECT event_id, revision, event_ordinal FROM incident_timeline_events "
                        "WHERE tenant_id = :tenant AND incident_id = :incident "
                        "ORDER BY event_time, revision, event_ordinal, event_id"
                    ),
                    {"tenant": tenant, "incident": initial.incident_id},
                )
            ).all()
            assert [row.event_id for row in rows] == [
                event.event_id for event in expected
            ]
            for revision in (1, 2):
                ordinals = [
                    row.event_ordinal for row in rows if row.revision == revision
                ]
                assert ordinals == list(range(len(ordinals)))
