"""Real PostgreSQL integration tests for Task 4.12-D1 durable dispatch & recovery.

Marked ``pytest.mark.integration``. Executes against a disposable PostgreSQL 16
database and least-privilege non-bypass runtime role (``NOSUPERUSER NOBYPASSRLS
NOCREATEDB NOCREATEROLE``) with exact column-scoped UPDATE grants on the 10
mutable ``incidents`` columns and 16 mutable ``incident_publication_dispatches``
columns, zero DELETE privileges, and zero foundation-table privileges.

Covers every D1 matrix requirement (D1-01 through D1-07 and D1-R1-01):
- Reversible Alembic migration 20261127_0003 catalog & Task 4.9 preservation
- Non-bypass role guard, column grants, trigger guards (55000), and RLS (FIX-08)
- Separate-session proof of committed-only visibility & rollback safety (CW-02)
- Immutable old intent & wire bytes during rev 2 update + ascending order (CW-10)
- Transient failure, 5-table preservation, exhaustion & oversized rejection (FIX-04/05)
- Exact replay after ACK with measured 0 row deltas (CW-07/FIX-02), restart with
  unstaged/pending work (CW-03/04), and post-send pre-ACK crash recovery (CW-06/FIX-03)
- Competing two-session dispatcher race (CW-09/FIX-09) and expired owner late
  completion after R+1 acknowledgement (CW-11/FIX-15)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import replace
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
from typing import Any
import urllib.parse
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import AnomalySignal
from app.correlation.aggregate import create_or_update_incident
from app.correlation.blast_radius import BlastRadiusPolicy
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.dispatch_repository import (
    IMMUTABLE_DISPATCH_COLUMNS,
    MUTABLE_DISPATCH_COLUMNS,
    DispatchClaimLostError,
    DispatchInputError,
    DispatchState,
    SqlAlchemyIncidentDispatchRepository,
)
from app.correlation.dispatch_service import (
    ClaimedDispatchAttempt,
    DispatchConfig,
    DispatchOutcomeStatus,
    DispatchStagingContext,
    IncidentDispatchService,
)
from app.correlation.errors import IncidentPersistenceTenantError
from app.correlation.grouping import group_candidate_signals
from app.correlation.models import (
    CorrelationWindowState,
    TopologySnapshotReference,
)
from app.correlation.persistence_repository import (
    SqlAlchemyIncidentPersistenceRepository,
)
from app.correlation.persistence_service import persist_incident_atomic
from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    IncidentPublicationRecord,
    IncidentPublicationSizeLimitError,
    build_incident_publication,
)
from app.correlation.publisher import (
    IncidentPublishAmbiguousError,
    IncidentPublisher,
    IncidentPublishOutcome,
    IncidentPublishRetriableError,
    IncidentPublishStatus,
)
from app.correlation.scoring import score_candidate_pair
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult
from app.telemetry.transport.serialization import TelemetryExecutionContext
from tests.test_correlation_publication import (
    PINNED_BLAST_RADIUS_POLICY,
    PINNED_CONFIG_FINGERPRINT,
    PINNED_CORRELATION_CONFIG,
    PINNED_EXECUTION_CONTEXT,
    PINNED_SIGNALS,
    PINNED_TOPOLOGY_CAPTURED_AT,
    PINNED_TOPOLOGY_FINGERPRINT,
    PINNED_TOPOLOGY_ID,
    PINNED_TOPOLOGY_VERSION,
    WIRE_ORACLES,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(scope="module")]

BACKEND_DIR = Path(__file__).resolve().parents[1]
TENANT_A = UUID("1dfc0469-7644-5efe-8b07-d1c7c31254f5")
TENANT_B = UUID("d8526eee-5633-5644-b721-9d5a678684dc")
SIX_TABLES: tuple[str, ...] = (
    "incidents",
    "incident_signal_links",
    "incident_correlation_evidence",
    "incident_timeline_events",
    "incident_publication_intents",
    "incident_publication_dispatches",
)
FIVE_TASK_4_9_TABLES: tuple[str, ...] = SIX_TABLES[:5]


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _resolve_admin_dsn() -> str:
    dsn = (
        os.environ.get("D1_POSTGRES_ADMIN_DSN")
        or os.environ.get("S1_R2_POSTGRES_ADMIN_DSN")
        or os.environ.get("M1_POSTGRES_ADMIN_DSN")
        or os.environ.get("P1_POSTGRES_ADMIN_DSN")
    )
    if not dsn:
        pytest.fail(
            "D1_POSTGRES_ADMIN_DSN (or S1_R2/M1/P1_POSTGRES_ADMIN_DSN) is required"
        )
    parsed = urllib.parse.urlparse(dsn)
    if parsed.path.lstrip("/") != "postgres":
        pytest.fail("Admin DSN must target maintenance database 'postgres'")
    return dsn


async def _grant_minimum_runtime_privileges(
    conn: asyncpg.Connection,
    *,
    db_name: str,
    role_name: str,
) -> None:
    await conn.execute(f'REVOKE TEMPORARY ON DATABASE "{db_name}" FROM PUBLIC')
    await conn.execute(f'GRANT CONNECT ON DATABASE "{db_name}" TO "{role_name}"')
    await conn.execute(f'GRANT USAGE ON SCHEMA public TO "{role_name}"')
    # Task 4.9 exact minimum grants (5 tables, 10 mutable incidents columns)
    await conn.execute(f'GRANT SELECT, INSERT ON TABLE incidents TO "{role_name}"')
    await conn.execute(
        "GRANT UPDATE (status, correlation_state, severity, started_at, last_seen_at, "
        "updated_at, revision, affected_services, revision_fingerprint, aggregate_payload) "
        f'ON TABLE incidents TO "{role_name}"'
    )
    await conn.execute(
        "GRANT SELECT, INSERT ON TABLE "
        "incident_signal_links, incident_correlation_evidence, "
        f'incident_timeline_events, incident_publication_intents TO "{role_name}"'
    )
    # Task 4.12-D1 exact minimum grants (16 mutable dispatch columns, 0 DELETE)
    await conn.execute(
        f'GRANT SELECT, INSERT ON TABLE incident_publication_dispatches TO "{role_name}"'
    )
    mutable_cols = ", ".join(MUTABLE_DISPATCH_COLUMNS)
    await conn.execute(
        f'GRANT UPDATE ({mutable_cols}) ON TABLE incident_publication_dispatches TO "{role_name}"'
    )


async def _measure_table_counts(
    conn: asyncpg.Connection,
    tables: tuple[str, ...] = SIX_TABLES,
) -> dict[str, int]:
    """Measure live row counts across tables directly from PostgreSQL."""
    counts: dict[str, int] = {}
    for tbl in tables:
        counts[tbl] = int(await conn.fetchval(f"SELECT count(*) FROM {tbl}"))
    return counts


async def _measure_table_hashes(
    conn: asyncpg.Connection,
    tables: tuple[str, ...] = SIX_TABLES,
) -> dict[str, str]:
    """Measure deterministic SHA-256 content hashes across tables."""
    pk_map = {
        "incidents": "tenant_id, incident_id",
        "incident_signal_links": "tenant_id, incident_id, signal_id",
        "incident_correlation_evidence": (
            "tenant_id, incident_id, left_signal_id, right_signal_id"
        ),
        "incident_timeline_events": "tenant_id, event_id",
        "incident_publication_intents": "tenant_id, incident_id, revision",
        "incident_publication_dispatches": "tenant_id, incident_id, revision",
    }
    hashes: dict[str, str] = {}
    for tbl in tables:
        rows = await conn.fetch(f"SELECT * FROM {tbl} ORDER BY {pk_map[tbl]}")
        serialized = "|".join(repr(dict(r)) for r in rows)
        hashes[tbl] = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return hashes


def _compute_deltas(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {tbl: after[tbl] - before[tbl] for tbl in before}


async def _truncate_incident_tables(admin_conn: asyncpg.Connection) -> None:
    """Reset test rows cleanly between isolated test scenarios using admin role."""
    await admin_conn.execute(
        "TRUNCATE TABLE "
        "incident_publication_dispatches, "
        "incident_publication_intents, "
        "incident_timeline_events, "
        "incident_correlation_evidence, "
        "incident_signal_links, "
        "incidents"
    )


class _FakeRecordMetadata:
    def __init__(self, topic: str, partition: int, offset: int, timestamp: int) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp


class _SimulatedNativeKafkaProducer:
    """In-process native aiokafka transport verifying real P1 -> KafkaTelemetryProducer."""

    def __init__(self, start_offset: int = 100, delay_seconds: float = 0.0) -> None:
        self._offset = start_offset
        self._delay_seconds = delay_seconds
        self.published: list[dict[str, Any]] = []

    async def send_and_wait(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        headers: list[tuple[str, bytes]],
        timestamp_ms: int | None = None,
    ) -> _FakeRecordMetadata:
        if self._delay_seconds > 0:
            await asyncio.sleep(self._delay_seconds)
        self._offset += 1
        self.published.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": list(headers),
                "timestamp_ms": timestamp_ms,
                "offset": self._offset,
            }
        )
        return _FakeRecordMetadata(
            topic=topic,
            partition=0,
            offset=self._offset,
            timestamp=timestamp_ms or 1_795_737_665_000,
        )


def _make_real_p1_publisher(
    *,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
    start_offset: int = 100,
    publish_timeout_seconds: float = 1.0,
    delay_seconds: float = 0.0,
) -> tuple[IncidentPublisher, _SimulatedNativeKafkaProducer]:
    native = _SimulatedNativeKafkaProducer(
        start_offset=start_offset, delay_seconds=delay_seconds
    )
    ktp = KafkaTelemetryProducer(
        bootstrap_servers="localhost:9092",
        producer_version=producer_version,
    )
    ktp._producer = native  # type: ignore[assignment]
    ktp._started = True
    return (
        IncidentPublisher(ktp, publish_timeout_seconds=publish_timeout_seconds),
        native,
    )


class _ScriptedPublishAdapter:
    """Scripted publish port for deterministic fault/crash-window injection."""

    def __init__(
        self, script: list[Any] | None = None, start_offset: int = 200
    ) -> None:
        self.script: list[Any] = list(script or [])
        self.sent: list[dict[str, Any]] = []
        self._offset = start_offset

    async def publish(
        self, record: IncidentPublicationRecord
    ) -> IncidentPublishOutcome:
        if self.script:
            action = self.script.pop(0)
            if callable(action):
                return await action(record)
            if isinstance(action, BaseException):
                raise action
            self.sent.append(
                {
                    "record": record,
                    "offset": None,
                    "status": action.status.value,
                }
            )
            return action
        self._offset += 1
        result = PublishResult(
            topic=record.topic.value,
            partition=0,
            offset=self._offset,
            timestamp_ms=1_795_737_665_000,
            latency_ms=1.2,
            serialized_bytes=len(record.kafka_value),
        )
        self.sent.append(
            {
                "record": record,
                "offset": self._offset,
                "status": "ACKNOWLEDGED",
            }
        )
        return IncidentPublishOutcome(IncidentPublishStatus.ACKNOWLEDGED, result=result)


# ============================================================================
# DISPOSABLE POSTGRESQL 16 ENVIRONMENT & FIX-01 DOMAIN FIXTURES
# ============================================================================


@pytest_asyncio.fixture(scope="module")
async def pg_d1_env() -> AsyncGenerator[dict[str, Any], None]:
    admin_dsn = _resolve_admin_dsn()
    parsed = urllib.parse.urlparse(admin_dsn)
    scheme = parsed.scheme or "postgresql"
    admin_host = parsed.hostname or "localhost"
    admin_port = parsed.port or 5432
    admin_user = parsed.username or "aegisops"
    admin_pass = parsed.password or ""

    admin_conn = await asyncpg.connect(admin_dsn)
    suffix = secrets.token_hex(6)
    assert re.match(r"^[0-9a-f]{12}$", suffix)
    db_name = f"aegis_d1_{suffix}"
    role_name = f"aegis_d1_runtime_{suffix}"
    password = secrets.token_urlsafe(24)

    runtime_engine = None
    admin_engine = None

    try:
        ver = await admin_conn.fetchval("SELECT version()")
        assert "PostgreSQL 16" in ver or " 16." in ver, ver
        assert not await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        assert not await admin_conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", role_name
        )

        await admin_conn.execute(f'CREATE DATABASE "{db_name}"')
        await admin_conn.execute(
            f"CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{password}' "
            "NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
        )

        disp_admin_dsn = (
            f"{scheme}://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
        )
        disp_admin_async_url = f"postgresql+asyncpg://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
        runtime_dsn = (
            f"{scheme}://{role_name}:{password}@{admin_host}:{admin_port}/{db_name}"
        )
        runtime_async_url = f"postgresql+asyncpg://{role_name}:{password}@{admin_host}:{admin_port}/{db_name}"

        env = os.environ.copy()
        env["DATABASE_URL"] = disp_admin_async_url
        res = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=BACKEND_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert (
            res.returncode == 0
        ), f"alembic upgrade failed:\n{res.stdout}\n{res.stderr}"

        disp_admin_conn = await asyncpg.connect(disp_admin_dsn)
        try:
            plan_id = await disp_admin_conn.fetchval("SELECT id FROM plans LIMIT 1")
            for tid, label in ((TENANT_A, "tenant-a"), (TENANT_B, "tenant-b")):
                await disp_admin_conn.execute(
                    """
                    INSERT INTO tenants (id, name, slug, plan_id, seats_limit, data_retention_days)
                    VALUES ($1, $2, $3, $4, 10, 30)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    tid,
                    f"D1 {label}",
                    f"d1-{label}-{suffix}",
                    plan_id,
                )
            await _grant_minimum_runtime_privileges(
                disp_admin_conn, db_name=db_name, role_name=role_name
            )
        finally:
            await disp_admin_conn.close()

        runtime_engine = create_async_engine(runtime_async_url, pool_pre_ping=True)
        admin_engine = create_async_engine(disp_admin_async_url, pool_pre_ping=True)
        runtime_sf = async_sessionmaker(runtime_engine, expire_on_commit=False)
        admin_sf = async_sessionmaker(admin_engine, expire_on_commit=False)

        yield {
            "db_name": db_name,
            "role_name": role_name,
            "disp_admin_dsn": disp_admin_dsn,
            "disp_admin_async_url": disp_admin_async_url,
            "runtime_dsn": runtime_dsn,
            "runtime_sf": runtime_sf,
            "admin_sf": admin_sf,
        }
    finally:
        if runtime_engine is not None:
            await runtime_engine.dispose()
        if admin_engine is not None:
            await admin_engine.dispose()
        await admin_conn.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = $1 AND pid <> pg_backend_pid()
            """,
            db_name,
        )
        await admin_conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        await admin_conn.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        db_rem = await admin_conn.fetchval(
            "SELECT count(*) FROM pg_database WHERE datname = $1", db_name
        )
        role_rem = await admin_conn.fetchval(
            "SELECT count(*) FROM pg_roles WHERE rolname = $1", role_name
        )
        await admin_conn.close()
        assert db_rem == 0
        assert role_rem == 0


@pytest_asyncio.fixture(scope="module")
async def fix01_inputs() -> dict[str, Any]:
    snapshot = await CanonicalTopologyProvider().get_snapshot()
    assert snapshot.snapshot_fingerprint == PINNED_TOPOLOGY_FINGERPRINT
    topology_ref = TopologySnapshotReference(
        topology_id=UUID(PINNED_TOPOLOGY_ID),
        topology_version=PINNED_TOPOLOGY_VERSION,
        snapshot_fingerprint=snapshot.snapshot_fingerprint,
        captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
        provider="canonical",
    )
    policy = BlastRadiusPolicy.model_validate_json(
        json.dumps(PINNED_BLAST_RADIUS_POLICY)
    )
    config = CorrelationConfig.model_validate_json(
        json.dumps(PINNED_CORRELATION_CONFIG)
    )
    assert compute_correlation_config_fingerprint(config) == PINNED_CONFIG_FINGERPRINT
    exec_ctx = TelemetryExecutionContext.model_validate(PINNED_EXECUTION_CONTEXT)

    inputs: dict[str, AnomalyCorrelationInput] = {}
    for name in ("S1", "S2"):
        signal = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS[name]))
        inputs[name] = AnomalyCorrelationInput(
            signal=signal,
            lineage=build_anomaly_reproducibility_lineage(signal),
        )

    t1 = _utc("2026-11-27T00:01:05Z")
    g1 = group_candidate_signals(
        [inputs["S1"]],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=t1,
    ).groups[0]

    t2 = _utc("2026-11-27T00:01:25Z")
    ev = score_candidate_pair(
        inputs["S1"], inputs["S2"], topology_snapshot=snapshot, config=config
    )
    g2 = group_candidate_signals(
        [inputs["S1"], inputs["S2"]],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=t1,
    ).groups[0]

    staging = DispatchStagingContext(
        snapshot=snapshot,
        expected_topology=topology_ref,
        blast_radius_policy=policy,
        correlation_config=config,
        execution_context=exec_ctx,
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    return {
        "staging": staging,
        "g1": g1,
        "g2": g2,
        "inputs": inputs,
        "t1": t1,
        "t2": t2,
    }


async def _persist_rev1(
    runtime_sf: async_sessionmaker[AsyncSession],
    fix01_inputs: dict[str, Any],
) -> Any:
    s1 = fix01_inputs["inputs"]["S1"]
    return await persist_incident_atomic(
        group=fix01_inputs["g1"],
        signal_inputs={s1.signal.signal_id: s1},
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=fix01_inputs["t1"],
        tenant_id=TENANT_A,
        session_factory=runtime_sf,
    )


async def _persist_rev2(
    runtime_sf: async_sessionmaker[AsyncSession],
    fix01_inputs: dict[str, Any],
    rev1_agg: Any,
) -> Any:
    s1 = fix01_inputs["inputs"]["S1"]
    s2 = fix01_inputs["inputs"]["S2"]
    return await persist_incident_atomic(
        group=fix01_inputs["g2"],
        signal_inputs={s1.signal.signal_id: s1, s2.signal.signal_id: s2},
        desired_state=CorrelationWindowState.SETTLING,
        processing_time=fix01_inputs["t2"],
        tenant_id=TENANT_A,
        session_factory=runtime_sf,
        expected_revision=rev1_agg.revision,
        expected_revision_fingerprint=rev1_agg.revision_fingerprint,
    )


# ============================================================================
# D1-01: MIGRATION 20261127_0003 CATALOG & REVERSIBLE ROUNDTRIP
# ============================================================================


async def test_d1_01_migration_catalog_and_reversible_roundtrip(
    pg_d1_env: dict[str, Any],
) -> None:
    conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        head = await conn.fetchval("SELECT version_num FROM alembic_version")
        assert head == "20261127_0003"

        # Verify exact 39 columns (23 immutable + 16 mutable).
        cols = [
            r["attname"]
            for r in await conn.fetch(
                """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = 'incident_publication_dispatches'
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """
            )
        ]
        assert cols == list(IMMUTABLE_DISPATCH_COLUMNS) + list(MUTABLE_DISPATCH_COLUMNS)

        # Verify PK, FKs (NO ACTION), UQ, indexes, RLS, and trigger.
        constraints = {
            r["conname"]: (r["contype"], r["confdeltype"], r["confupdtype"])
            for r in await conn.fetch(
                """
                SELECT
                    con.conname,
                    con.contype::text AS contype,
                    con.confdeltype::text AS confdeltype,
                    con.confupdtype::text AS confupdtype
                FROM pg_constraint con
                JOIN pg_class c ON c.oid = con.conrelid
                WHERE c.relname = 'incident_publication_dispatches'
                """
            )
        }
        assert "pk_incident_publication_dispatches" in constraints
        assert constraints["fk_incident_pub_dispatches_tenant"] == ("f", "a", "a")
        assert constraints["fk_incident_pub_dispatches_intent"] == ("f", "a", "a")
        assert "uq_incident_pub_dispatches_tenant_event" in constraints

        indexes = {
            r["relname"]
            for r in await conn.fetch(
                """
                SELECT i.relname
                FROM pg_index ix
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_class t ON t.oid = ix.indrelid
                WHERE t.relname = 'incident_publication_dispatches'
                """
            )
        }
        assert "ix_incident_pub_dispatches_recoverable" in indexes
        assert "ix_incident_pub_dispatches_incident_rev" in indexes

        rls = await conn.fetchrow(
            """
            SELECT relrowsecurity, relforcerowsecurity
            FROM pg_class
            WHERE relname = 'incident_publication_dispatches'
            """
        )
        assert rls is not None and rls["relrowsecurity"] and rls["relforcerowsecurity"]

        policy = await conn.fetchval(
            """
            SELECT polname FROM pg_policy p
            JOIN pg_class c ON c.oid = p.polrelid
            WHERE c.relname = 'incident_publication_dispatches'
            """
        )
        assert policy == "pol_incident_publication_dispatches_tenant"

        trig = await conn.fetchrow(
            """
            SELECT tg.tgname, p.proname
            FROM pg_trigger tg
            JOIN pg_class c ON c.oid = tg.tgrelid
            JOIN pg_proc p ON p.oid = tg.tgfoid
            WHERE c.relname = 'incident_publication_dispatches'
              AND NOT tg.tgisinternal
            """
        )
        assert trig is not None
        assert trig["tgname"] == "trg_incident_publication_dispatches_guard"
        assert trig["proname"] == "guard_incident_publication_dispatch_change"

        # Reversible downgrade to 20261103_0002 leaves all 5 Task 4.9 tables intact.
        env = os.environ.copy()
        env["DATABASE_URL"] = pg_d1_env["disp_admin_async_url"]
        down = subprocess.run(
            [sys.executable, "-m", "alembic", "downgrade", "20261103_0002"],
            cwd=BACKEND_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert down.returncode == 0, down.stdout + down.stderr
        assert (
            await conn.fetchval("SELECT version_num FROM alembic_version")
            == "20261103_0002"
        )
        assert (
            await conn.fetchval(
                "SELECT to_regclass('public.incident_publication_dispatches')"
            )
            is None
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'guard_incident_publication_dispatch_change')"
        )
        for tbl in FIVE_TASK_4_9_TABLES:
            assert (
                await conn.fetchval("SELECT to_regclass($1)", f"public.{tbl}")
                is not None
            )

        # Re-upgrade to head and re-grant runtime privileges.
        up = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=BACKEND_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert up.returncode == 0, up.stdout + up.stderr
        assert (
            await conn.fetchval("SELECT version_num FROM alembic_version")
            == "20261127_0003"
        )
        await _grant_minimum_runtime_privileges(
            conn, db_name=pg_d1_env["db_name"], role_name=pg_d1_env["role_name"]
        )
    finally:
        await conn.close()


# ============================================================================
# D1-02: NON-BYPASS ROLE, COLUMN GRANTS, TRIGGER GUARDS & TENANT ISOLATION (FIX-08)
# ============================================================================


async def test_d1_02_role_guards_triggers_column_grants_and_tenant_isolation(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    runtime_conn = await asyncpg.connect(pg_d1_env["runtime_dsn"])
    try:
        await _truncate_incident_tables(admin_conn)

        # 1. Superuser session rejected with IncidentPersistenceTenantError.
        p1_pub, _ = _make_real_p1_publisher()
        superuser_svc = IncidentDispatchService(
            session_factory=pg_d1_env["admin_sf"],
            publisher=p1_pub,
            config=DispatchConfig(worker_id="superuser-test"),
        )
        with pytest.raises(IncidentPersistenceTenantError):
            await superuser_svc.load_dispatch_state(
                tenant_id=TENANT_A,
                incident_id=uuid4(),
                revision=1,
            )

        # 2. Persist and acknowledge Tenant A revision 1 via non-bypass runtime role.
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id
        p1_pub, native = _make_real_p1_publisher()
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub,
            config=DispatchConfig(worker_id="runtime-worker"),
        )
        outcome = await svc.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert outcome is not None
        assert outcome.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert len(native.published) == 1

        # 3. Tenant B isolation: zero visibility, zero claims, zero recovery sends.
        b_state = await svc.load_dispatch_state(
            tenant_id=TENANT_B, incident_id=incident_id, revision=1
        )
        assert b_state is None
        b_recovery = await svc.recover_pending_dispatches(tenant_id=TENANT_B)
        assert b_recovery.total_sends_attempted == 0
        assert len(b_recovery.outcomes) == 0

        # Direct SQL under runtime role with Tenant B context sees 0 rows & rejects cross-tenant INSERT/UPDATE/DELETE.
        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(TENANT_B)
            )
            seen = await runtime_conn.fetchval(
                "SELECT count(*) FROM incident_publication_dispatches"
            )
            assert seen == 0

        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(TENANT_B)
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError) as exc_info:
                await runtime_conn.execute(
                    "UPDATE incident_publication_dispatches SET kafka_topic = 'aegis.incidents'"
                )
            assert exc_info.value.sqlstate == "42501"

        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(TENANT_B)
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError) as del_exc:
                await runtime_conn.execute(
                    "DELETE FROM incident_publication_dispatches"
                )
            assert del_exc.value.sqlstate == "42501"

        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(TENANT_B)
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError) as ins_exc:
                await runtime_conn.execute(
                    """
                    INSERT INTO incident_publication_dispatches (
                        tenant_id, incident_id, revision, revision_fingerprint,
                        publication_contract_version, envelope_schema_version,
                        handoff_schema_version, event_id, publication_fingerprint,
                        handoff_id, handoff_fingerprint, topology_id, topology_version,
                        topology_snapshot_fingerprint, correlation_policy_version,
                        correlation_config_fingerprint, intent_time, kafka_topic,
                        kafka_key, kafka_value, kafka_headers_json, wire_record_bytes,
                        created_at, dispatch_state, attempt_count, updated_at
                    ) VALUES (
                        $1, $2, 99, repeat('a', 64),
                        'incident-publication/1.0', '1.0',
                        'phase5-investigation-handoff/1.0', $3, repeat('b', 64),
                        $4, repeat('c', 64), $5, '1.0.0',
                        repeat('d', 64), 'v1.0',
                        repeat('e', 64), now(), 'aegis.incidents',
                        '\\x6b'::bytea, '\\x7b7d'::bytea, '[]'::jsonb, 10,
                        now(), 'pending', 0, now()
                    )
                    """,
                    TENANT_A,
                    incident_id,
                    uuid4(),
                    uuid4(),
                    UUID(PINNED_TOPOLOGY_ID),
                )
            assert ins_exc.value.sqlstate == "42501"

        # 4. Trigger enforcement (SQLSTATE 55000): DELETE, acknowledged UPDATE, immutable column UPDATE.
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as t_del:
            await admin_conn.execute(
                "DELETE FROM incident_publication_dispatches WHERE tenant_id = $1",
                TENANT_A,
            )
        assert t_del.value.sqlstate == "55000"

        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as t_ack:
            await admin_conn.execute(
                """
                UPDATE incident_publication_dispatches
                SET last_error_code = 'MUTATED'
                WHERE tenant_id = $1 AND incident_id = $2 AND revision = 1
                """,
                TENANT_A,
                incident_id,
            )
        assert t_ack.value.sqlstate == "55000"

        # Also test immutable column guard on a non-terminal pending row (rev 2).
        await _persist_rev2(pg_d1_env["runtime_sf"], fix01_inputs, rev1.aggregate)
        staged2 = await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=fix01_inputs["staging"],
        )
        assert (
            staged2 is not None
            and staged2.dispatch.dispatch_state is DispatchState.PENDING
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as t_imm:
            await admin_conn.execute(
                """
                UPDATE incident_publication_dispatches
                SET wire_record_bytes = wire_record_bytes + 1
                WHERE tenant_id = $1 AND incident_id = $2 AND revision = 2
                """,
                TENANT_A,
                incident_id,
            )
        assert t_imm.value.sqlstate == "55000"
    finally:
        await runtime_conn.close()
        await admin_conn.close()


# ============================================================================
# D1-03: SEPARATE-SESSION COMMITTED VISIBILITY & ROLLBACK EMITS NOTHING (CW-02)
# ============================================================================


async def test_d1_03_separate_session_visibility_and_rollback_emits_nothing(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        await _truncate_incident_tables(admin_conn)
        counts_before = await _measure_table_counts(admin_conn)

        s1 = fix01_inputs["inputs"]["S1"]
        pure_rev1 = create_or_update_incident(
            group=fix01_inputs["g1"],
            signal_inputs={s1.signal.signal_id: s1},
            processing_time=fix01_inputs["t1"],
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=None,
        )
        incident_id = pure_rev1.aggregate.incident_id

        p1_pub, native = _make_real_p1_publisher()
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub,
            config=DispatchConfig(worker_id="separate-session-dispatcher"),
        )

        # Open Session 1, insert all 5 tables + intent WITHOUT committing, then roll back.
        async with pg_d1_env["runtime_sf"]() as writer_session:
            tx = await writer_session.begin()
            try:
                repo = SqlAlchemyIncidentPersistenceRepository(writer_session, TENANT_A)
                await repo.bind_tenant_context()
                await repo.insert_incident(pure_rev1.aggregate)
                await repo.append_signal_links(
                    incident_id, pure_rev1.aggregate.signal_references
                )
                await repo.append_correlation_evidence(
                    incident_id, pure_rev1.aggregate.correlation_evidence
                )
                await repo.append_timeline_events(pure_rev1.timeline_events)
                await repo.append_publication_intent(
                    pure_rev1.aggregate, intent_time=pure_rev1.aggregate.updated_at
                )
                await writer_session.flush()

                # Separate Session 2 MUST NOT see uncommitted intent and MUST emit 0 records.
                uncommitted_stage = await svc.stage_committed_intent(
                    tenant_id=TENANT_A,
                    incident_id=incident_id,
                    revision=1,
                    staging_context=fix01_inputs["staging"],
                )
                assert uncommitted_stage is None
                uncommitted_rec = await svc.recover_pending_dispatches(
                    tenant_id=TENANT_A,
                    staging_context=fix01_inputs["staging"],
                )
                assert uncommitted_rec.staged_count == 0
                assert uncommitted_rec.total_sends_attempted == 0
                assert len(native.published) == 0
            finally:
                await tx.rollback()

        # After rollback, separate session still sees nothing and measured deltas == 0.
        post_rb = await svc.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert post_rb is None
        assert len(native.published) == 0
        counts_after_rb = await _measure_table_counts(admin_conn)
        assert _compute_deltas(counts_before, counts_after_rb) == {
            tbl: 0 for tbl in SIX_TABLES
        }

        # Now commit through persist_incident_atomic -> separate session dispatches FIX-01-REV1.
        await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        committed_out = await svc.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert committed_out is not None
        assert committed_out.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert len(native.published) == 1
        oracle = WIRE_ORACLES["FIX-01-REV1"]
        assert _sha256(native.published[0]["value"]) == oracle["kafka_value_sha256"]
    finally:
        await admin_conn.close()


# ============================================================================
# D1-04: IMMUTABLE OLD INTENT DURING LATER REVISION & ASCENDING ORDER (CW-10)
# ============================================================================


async def test_d1_04_immutable_rev1_during_rev2_and_ascending_dispatch_order(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        p1_pub, native = _make_real_p1_publisher()
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub,
            config=DispatchConfig(worker_id="cw10-worker"),
        )

        # Stage rev 1 (pending) and capture exact intent & dispatch immutable bytes.
        staged1 = await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert staged1 is not None
        rev1_intent_bytes_before = await admin_conn.fetchval(
            """
            SELECT aggregate_payload FROM incident_publication_intents
            WHERE tenant_id = $1 AND incident_id = $2 AND revision = 1
            """,
            TENANT_A,
            incident_id,
        )
        rev1_wire_before = staged1.dispatch.kafka_value
        assert rev1_wire_before is not None

        # Commit rev 2 while rev 1 is still pending.
        await _persist_rev2(pg_d1_env["runtime_sf"], fix01_inputs, rev1.aggregate)
        staged2 = await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=fix01_inputs["staging"],
        )
        assert staged2 is not None

        # Verify rev 1 intent & wire bytes are completely unchanged.
        rev1_intent_bytes_after = await admin_conn.fetchval(
            """
            SELECT aggregate_payload FROM incident_publication_intents
            WHERE tenant_id = $1 AND incident_id = $2 AND revision = 1
            """,
            TENANT_A,
            incident_id,
        )
        assert bytes(rev1_intent_bytes_after) == bytes(rev1_intent_bytes_before)

        # Rev 2 MUST be blocked until rev 1 is acknowledged.
        blocked_r2 = await svc.dispatch_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=2
        )
        assert blocked_r2 is not None
        assert blocked_r2.status is DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR
        assert blocked_r2.sends_attempted == 0
        assert len(native.published) == 0

        # Bounded recovery dispatches strictly in ascending order: rev 1 then rev 2.
        batch = await svc.recover_pending_dispatches(
            tenant_id=TENANT_A, incident_id=incident_id
        )
        assert batch.acknowledged_count == 2
        assert [o.revision for o in batch.outcomes] == [1, 2]
        assert len(native.published) == 2
        assert (
            _sha256(native.published[0]["value"])
            == WIRE_ORACLES["FIX-01-REV1"]["kafka_value_sha256"]
        )
        assert (
            _sha256(native.published[1]["value"])
            == WIRE_ORACLES["FIX-01-REV2"]["kafka_value_sha256"]
        )
        assert native.published[0]["key"] == native.published[1]["key"]

        # Rev 1 wire bytes remain identical after rev 2 ACK.
        rev1_final = await svc.load_dispatch_state(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert rev1_final is not None and rev1_final.kafka_value == rev1_wire_before
    finally:
        await admin_conn.close()


# ============================================================================
# D1-05: FAILED SEND PRESERVES 5 TABLES, EXHAUSTION & OVERSIZED REJECTION (FIX-04/05)
# ============================================================================


async def test_d1_05_failed_send_exhaustion_and_oversized_unsendable(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        # Part A (FIX-04): Transient failure leaves 5 Task 4.9 tables untouched and recovers.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        five_counts_before = await _measure_table_counts(
            admin_conn, FIVE_TASK_4_9_TABLES
        )
        five_hashes_before = await _measure_table_hashes(
            admin_conn, FIVE_TASK_4_9_TABLES
        )

        retriable = IncidentPublishOutcome(
            IncidentPublishStatus.FAILED_RETRIABLE,
            error=IncidentPublishRetriableError("broker leader election"),
        )
        adapter = _ScriptedPublishAdapter([retriable])
        cfg = DispatchConfig(
            worker_id="fix04-worker",
            max_dispatch_attempts=3,
            retry_backoff_initial_ms=50,
            retry_backoff_max_ms=100,
            claim_lease_seconds=5,
            publish_timeout_seconds=1.0,
            lease_settlement_margin_seconds=1.0,
        )
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=adapter,
            config=cfg,
        )

        out1 = await svc.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert out1 is not None
        assert out1.status is DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION
        assert out1.input_offset_committable is False
        assert out1.dispatch.dispatch_state is DispatchState.RETRY_SCHEDULED
        assert out1.dispatch.attempt_count == 1

        # Measure 5-table counts and content hashes: 0 deltas, byte-identical!
        five_counts_after = await _measure_table_counts(
            admin_conn, FIVE_TASK_4_9_TABLES
        )
        five_hashes_after = await _measure_table_hashes(
            admin_conn, FIVE_TASK_4_9_TABLES
        )
        assert _compute_deltas(five_counts_before, five_counts_after) == {
            tbl: 0 for tbl in FIVE_TASK_4_9_TABLES
        }
        assert five_hashes_after == five_hashes_before

        # Wait real PostgreSQL backoff (50ms) and recover WITHOUT staging_context.
        await asyncio.sleep(0.1)
        rec = await svc.recover_pending_dispatches(tenant_id=TENANT_A)
        assert rec.acknowledged_count == 1
        assert rec.outcomes[0].dispatch.attempt_count == 2
        assert (
            adapter.sent[0]["record"].kafka_value
            == adapter.sent[1]["record"].kafka_value
        )

        # Part B: Exhaustion at max_dispatch_attempts=2.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id
        adapter_ex = _ScriptedPublishAdapter([retriable, retriable])
        svc_ex = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=adapter_ex,
            config=DispatchConfig(
                worker_id="exhaust-worker",
                max_dispatch_attempts=2,
                retry_backoff_initial_ms=20,
                retry_backoff_max_ms=50,
                claim_lease_seconds=5,
                publish_timeout_seconds=1.0,
                lease_settlement_margin_seconds=1.0,
            ),
        )
        o_try1 = await svc_ex.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert (
            o_try1 is not None
            and o_try1.dispatch.dispatch_state is DispatchState.RETRY_SCHEDULED
        )
        await asyncio.sleep(0.05)
        o_try2 = await svc_ex.dispatch_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert o_try2 is not None
        assert o_try2.status is DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED
        assert o_try2.dispatch.dispatch_state is DispatchState.EXHAUSTED
        assert o_try2.dispatch.attempt_count == 2
        assert o_try2.input_offset_committable is False

        # Part C (FIX-05): Oversized payload persists rejected_unsendable with kafka_value IS NULL.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        def _raise_oversized(**kwargs: Any) -> Any:
            raise IncidentPublicationSizeLimitError("exceeds 983040 bytes")

        monkeypatch.setattr(
            "app.correlation.dispatch_service.build_incident_publication_from_intent",
            _raise_oversized,
        )
        adapter_ov = _ScriptedPublishAdapter()
        svc_ov = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=adapter_ov,
            config=DispatchConfig(worker_id="oversized-worker"),
        )
        o_ov = await svc_ov.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert o_ov is not None
        assert o_ov.status is DispatchOutcomeStatus.COMMITTED_UNSENDABLE
        assert o_ov.input_offset_committable is False
        assert o_ov.dispatch.dispatch_state is DispatchState.REJECTED_UNSENDABLE
        assert o_ov.dispatch.kafka_value is None
        assert o_ov.dispatch.attempt_count == 0
        assert len(adapter_ov.sent) == 0
        # Trigger enforces terminal immutability on rejected_unsendable.
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as t_un:
            await admin_conn.execute(
                """
                UPDATE incident_publication_dispatches
                SET dispatch_state = 'pending'
                WHERE tenant_id = $1 AND incident_id = $2 AND revision = 1
                """,
                TENANT_A,
                incident_id,
            )
        assert t_un.value.sqlstate == "55000"
    finally:
        await admin_conn.close()


# ============================================================================
# D1-06: REPLAY AFTER ACK (CW-07), RESTART RECOVERY (CW-03/04) & POST-SEND CRASH (CW-06)
# ============================================================================


async def test_d1_06_replay_after_ack_restart_pending_and_crash_after_send(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        # 1. CW-07: Crash after ACK bookkeeping before input-offset commit -> replay has 0 DB deltas & 0 sends.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        p1_pub1, native1 = _make_real_p1_publisher(start_offset=500)
        svc1 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub1,
            config=DispatchConfig(worker_id="cw07-run1"),
        )
        first = await svc1.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert (
            first is not None
            and first.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        )

        # Measure all 6 tables right after ACK bookkeeping (simulating crash before input-offset commit).
        counts_before_replay = await _measure_table_counts(admin_conn)
        hashes_before_replay = await _measure_table_hashes(admin_conn)

        # Restart with brand new service & publisher instance -> exact replay.
        p1_pub2, native2 = _make_real_p1_publisher(start_offset=600)
        svc2 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub2,
            config=DispatchConfig(worker_id="cw07-run2"),
        )
        replay = await svc2.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert replay is not None
        assert replay.status is DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED
        assert replay.input_offset_committable is True
        assert replay.sends_attempted == 0
        assert len(native2.published) == 0

        counts_after_replay = await _measure_table_counts(admin_conn)
        hashes_after_replay = await _measure_table_hashes(admin_conn)
        assert _compute_deltas(counts_before_replay, counts_after_replay) == {
            tbl: 0 for tbl in SIX_TABLES
        }
        assert hashes_after_replay == hashes_before_replay

        # 2. CW-03 & CW-04: Restart recovery with unstaged intent (CW-03) and staged pending row (CW-04).
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id
        # Restarted worker recovers unstaged intent (CW-03) via staging_context.
        p1_cw03, native_cw03 = _make_real_p1_publisher(start_offset=700)
        svc_cw03 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_cw03,
            config=DispatchConfig(worker_id="cw03-restart"),
        )
        rec_cw03 = await svc_cw03.recover_pending_dispatches(
            tenant_id=TENANT_A, staging_context=fix01_inputs["staging"]
        )
        assert rec_cw03.staged_count == 1
        assert rec_cw03.acknowledged_count == 1
        assert len(native_cw03.published) == 1

        # Stage rev 2 as pending (CW-04), then restart WITHOUT staging_context (zero Neo4j calls).
        await _persist_rev2(pg_d1_env["runtime_sf"], fix01_inputs, rev1.aggregate)
        await svc_cw03.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=fix01_inputs["staging"],
        )
        p1_cw04, native_cw04 = _make_real_p1_publisher(start_offset=800)
        svc_cw04 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_cw04,
            config=DispatchConfig(worker_id="cw04-restart"),
        )
        rec_cw04 = await svc_cw04.recover_pending_dispatches(tenant_id=TENANT_A)
        assert rec_cw04.staged_count == 0
        assert rec_cw04.acknowledged_count == 1
        assert len(native_cw04.published) == 1
        assert (
            _sha256(native_cw04.published[0]["value"])
            == WIRE_ORACLES["FIX-01-REV2"]["kafka_value_sha256"]
        )

        # 3. CW-06 / FIX-03: Crash AFTER real send (K1) and BEFORE PostgreSQL ACK bookkeeping!
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        short_lease_cfg = DispatchConfig(
            worker_id="cw06-worker-1",
            claim_lease_seconds=5,
            publish_timeout_seconds=1.0,
            lease_settlement_margin_seconds=1.0,
            retry_backoff_initial_ms=20,
            retry_backoff_max_ms=50,
        )
        p1_shared, native_shared = _make_real_p1_publisher(start_offset=900)
        worker1 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_shared,
            config=short_lease_cfg,
        )
        await worker1.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        claim1 = await worker1.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim1.claimed and claim1.attempt is not None

        # Broker accepts physical delivery K1 (offset 901), then process crashes BEFORE bookkeeping!
        k1_outcome = await p1_shared.publish(claim1.attempt.record)
        assert k1_outcome.status is IncidentPublishStatus.ACKNOWLEDGED
        assert k1_outcome.result is not None and k1_outcome.result.offset == 901

        # Verify DB still shows in_flight with broker_offset IS NULL.
        mid_state = await worker1.load_dispatch_state(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert mid_state is not None
        assert mid_state.dispatch_state is DispatchState.IN_FLIGHT
        assert mid_state.broker_offset is None
        assert mid_state.attempt_count == 1
        assert mid_state.input_offset_committable is False

        # Wait for real PostgreSQL 5-second lease expiry, then restarted Worker 2 recovers.
        await asyncio.sleep(5.2)
        worker2 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_shared,
            config=DispatchConfig(
                worker_id="cw06-worker-2",
                claim_lease_seconds=5,
                publish_timeout_seconds=1.0,
                lease_settlement_margin_seconds=1.0,
                retry_backoff_initial_ms=20,
                retry_backoff_max_ms=50,
            ),
        )
        rec_cw06 = await worker2.recover_pending_dispatches(tenant_id=TENANT_A)
        assert rec_cw06.acknowledged_count == 1
        final_cw06 = rec_cw06.outcomes[0].dispatch
        assert final_cw06.dispatch_state is DispatchState.ACKNOWLEDGED
        assert final_cw06.attempt_count == 2
        assert final_cw06.broker_offset == 902

        # Separate physical vs logical accounting: 2 physical records, 1 logical identity, byte-identical!
        assert len(native_shared.published) == 2
        assert native_shared.published[0]["key"] == native_shared.published[1]["key"]
        assert (
            native_shared.published[0]["value"] == native_shared.published[1]["value"]
        )
        assert (
            native_shared.published[0]["headers"]
            == native_shared.published[1]["headers"]
        )
        assert {_sha256(p["value"]) for p in native_shared.published} == {
            WIRE_ORACLES["FIX-01-REV1"]["kafka_value_sha256"]
        }
    finally:
        await admin_conn.close()


# ============================================================================
# D1-07 & D1-R1-01: COMPETING DISPATCHERS (CW-09) & EXPIRED OWNER AFTER R+1 (FIX-15)
# ============================================================================


async def test_d1_07_competing_dispatchers_and_fix15_expired_owner_stale_fence(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        # Part A (CW-09 / FIX-09): Two competing real PostgreSQL sessions race on pending rev 1.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        p1_race, native_race = _make_real_p1_publisher(start_offset=1000)
        worker_a = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_race,
            config=DispatchConfig(worker_id="race-worker-A"),
        )
        worker_b = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_race,
            config=DispatchConfig(worker_id="race-worker-B"),
        )
        await worker_a.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )

        # Race atomic claim_revision concurrently across two real PostgreSQL connections!
        res_a, res_b = await asyncio.gather(
            worker_a.claim_revision(
                tenant_id=TENANT_A, incident_id=incident_id, revision=1
            ),
            worker_b.claim_revision(
                tenant_id=TENANT_A, incident_id=incident_id, revision=1
            ),
        )
        claimed_flags = [res_a.claimed, res_b.claimed]
        assert claimed_flags.count(True) == 1
        assert claimed_flags.count(False) == 1

        winner_res = res_a if res_a.claimed else res_b
        loser_res = res_b if res_a.claimed else res_a
        winner_svc = worker_a if res_a.claimed else worker_b

        assert loser_res.outcome is not None
        assert loser_res.outcome.status is DispatchOutcomeStatus.SKIPPED_ALREADY_CLAIMED
        assert loser_res.outcome.sends_attempted == 0
        assert winner_res.attempt is not None

        # Winner executes publish -> exactly 1 physical send, 1 dispatch row.
        win_out = await winner_svc.execute_claimed_attempt(winner_res.attempt)
        assert win_out.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert len(native_race.published) == 1
        counts = await _measure_table_counts(admin_conn)
        assert counts["incident_publication_dispatches"] == 1

        # Part B (D1-R1-01 / FIX-15 / CW-11): Expired owner A times out -> B ACKs R1 & R2 -> A's late ACK raises DispatchClaimLostError!
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id
        await _persist_rev2(pg_d1_env["runtime_sf"], fix01_inputs, rev1.aggregate)

        p1_fix15, native_fix15 = _make_real_p1_publisher(start_offset=1100)
        cfg_a = DispatchConfig(
            worker_id="fix15-worker-A",
            claim_lease_seconds=5,
            publish_timeout_seconds=1.0,
            lease_settlement_margin_seconds=1.0,
            retry_backoff_initial_ms=20,
            retry_backoff_max_ms=50,
        )
        cfg_b = DispatchConfig(
            worker_id="fix15-worker-B",
            claim_lease_seconds=5,
            publish_timeout_seconds=1.0,
            lease_settlement_margin_seconds=1.0,
            retry_backoff_initial_ms=20,
            retry_backoff_max_ms=50,
        )
        svc_a = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_fix15,
            config=cfg_a,
        )
        svc_b = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_fix15,
            config=cfg_b,
        )

        # Stage rev 1 and rev 2.
        await svc_a.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        await svc_b.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=fix01_inputs["staging"],
        )

        # 1. Worker A claims rev 1 and times out (records ambiguous_outcome, retaining 5s lease).
        claim_a1 = await svc_a.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim_a1.claimed and claim_a1.attempt is not None
        ambig = await svc_a.complete_claimed_attempt(
            claim_a1.attempt,
            IncidentPublishOutcome(
                IncidentPublishStatus.FAILED_AMBIGUOUS,
                error=IncidentPublishAmbiguousError("timeout"),
            ),
        )
        assert ambig.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
        assert ambig.dispatch.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
        assert ambig.dispatch.claim_expires_at is not None

        # Before lease expiry, Worker B cannot reclaim rev 1 or claim rev 2.
        early_b1 = await svc_b.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert not early_b1.claimed
        assert early_b1.outcome is not None
        assert (
            early_b1.outcome.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
        )

        early_b2 = await svc_b.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=2
        )
        assert not early_b2.claimed
        assert early_b2.outcome is not None
        assert early_b2.outcome.status is DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR

        # 2. Wait for real PostgreSQL 5-second lease expiry -> Worker B reclaims & ACKs R1, then R2.
        await asyncio.sleep(5.2)
        b_r1 = await svc_b.dispatch_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert (
            b_r1 is not None
            and b_r1.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        )
        assert b_r1.dispatch.broker_offset == 1101

        b_r2 = await svc_b.dispatch_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=2
        )
        assert (
            b_r2 is not None
            and b_r2.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        )
        assert b_r2.dispatch.broker_offset == 1102

        # 3. Worker A's stale already-started rev 1 send finishes late (offset 1103).
        late_send_out = await p1_fix15.publish(claim_a1.attempt.record)
        assert late_send_out.status is IncidentPublishStatus.ACKNOWLEDGED
        assert late_send_out.result is not None and late_send_out.result.offset == 1103

        hashes_before_stale = await _measure_table_hashes(admin_conn)
        with pytest.raises(DispatchClaimLostError):
            await svc_a.complete_claimed_attempt(claim_a1.attempt, late_send_out)
        hashes_after_stale = await _measure_table_hashes(admin_conn)
        assert hashes_after_stale == hashes_before_stale

        # Authoritative receipt for rev 1 remains Worker B's offset 1101!
        final_r1 = await svc_b.load_dispatch_state(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert final_r1 is not None and final_r1.broker_offset == 1101

        # Reconcile physical vs logical records: [R1, R2, R1] -> 3 physical, 2 unique logical identities!
        assert len(native_fix15.published) == 3
        val_shas = [_sha256(p["value"]) for p in native_fix15.published]
        assert val_shas == [
            WIRE_ORACLES["FIX-01-REV1"]["kafka_value_sha256"],
            WIRE_ORACLES["FIX-01-REV2"]["kafka_value_sha256"],
            WIRE_ORACLES["FIX-01-REV1"]["kafka_value_sha256"],
        ]
        assert len(set(val_shas)) == 2
    finally:
        await admin_conn.close()


# ============================================================================
# D1-R1 RETAINED REAL POSTGRESQL + REAL P1 BLOCKER REGRESSION PROOFS
# ============================================================================


async def test_d1_r1_blocker1_concurrent_execute_same_token_sends_once(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    """Blocker 1 PG+P1 proof: concurrent execute_claimed_attempt on one token emits 1 send."""
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        # Case A: two concurrent calls on the same IncidentDispatchService instance.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        p1_pub, native = _make_real_p1_publisher(start_offset=2000, delay_seconds=0.05)
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub,
            config=DispatchConfig(worker_id="b1-worker"),
        )
        await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        claim = await svc.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim.claimed and claim.attempt is not None

        results = await asyncio.gather(
            svc.execute_claimed_attempt(claim.attempt),
            svc.execute_claimed_attempt(claim.attempt),
            return_exceptions=True,
        )
        outcomes = [r for r in results if not isinstance(r, BaseException)]
        errors = [r for r in results if isinstance(r, BaseException)]
        assert len(outcomes) == 1
        assert outcomes[0].status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert len(errors) == 1
        assert isinstance(errors[0], DispatchClaimLostError)
        assert len(native.published) == 1

        # Case B: two separate IncidentDispatchService instances given the same claimed token.
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        p1_pub2, native2 = _make_real_p1_publisher(
            start_offset=2100, delay_seconds=0.05
        )
        svc_1 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub2,
            config=DispatchConfig(worker_id="b1-worker-1"),
        )
        svc_2 = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub2,
            config=DispatchConfig(worker_id="b1-worker-2"),
        )
        await svc_1.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        claim2 = await svc_1.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim2.claimed and claim2.attempt is not None

        results2 = await asyncio.gather(
            svc_1.execute_claimed_attempt(claim2.attempt),
            svc_2.execute_claimed_attempt(claim2.attempt),
            return_exceptions=True,
        )
        outcomes2 = [r for r in results2 if not isinstance(r, BaseException)]
        errors2 = [r for r in results2 if isinstance(r, BaseException)]
        assert len(outcomes2) == 1
        assert outcomes2[0].status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert len(errors2) == 1
        assert isinstance(errors2[0], DispatchClaimLostError)
        assert len(native2.published) == 1
    finally:
        await admin_conn.close()


async def test_d1_r1_blocker2_substituted_r2_into_r1_attempt_rejected(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    """Blocker 2 PG+P1 proof: substituting R2 into R1 cannot publish R2 or acknowledge R1."""
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id
        await _persist_rev2(pg_d1_env["runtime_sf"], fix01_inputs, rev1.aggregate)

        p1_pub, native = _make_real_p1_publisher(start_offset=2200)
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub,
            config=DispatchConfig(worker_id="b2-worker"),
        )
        s1 = await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        s2 = await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=fix01_inputs["staging"],
        )
        assert s1 is not None and s2 is not None and s2.record is not None

        claim_r1 = await svc.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim_r1.claimed and claim_r1.attempt is not None

        counts_before = await _measure_table_counts(admin_conn)
        hashes_before = await _measure_table_hashes(admin_conn)

        # 1. dataclasses.replace substitution fails immediately.
        with pytest.raises(DispatchInputError):
            replace(claim_r1.attempt, record=s2.record)

        # 2. object.__setattr__ substitution fails before send and before ACK bookkeeping.
        tampered = claim_r1.attempt
        orig_record = tampered.record
        object.__setattr__(tampered, "record", s2.record)
        try:
            with pytest.raises(DispatchInputError):
                await svc.execute_claimed_attempt(tampered)
            with pytest.raises(DispatchInputError):
                await svc.complete_claimed_attempt(
                    tampered,
                    IncidentPublishOutcome(
                        IncidentPublishStatus.ACKNOWLEDGED,
                        result=PublishResult(
                            topic="aegis.incidents",
                            partition=0,
                            offset=2299,
                            timestamp_ms=1_795_737_665_000,
                            latency_ms=1.0,
                            serialized_bytes=len(s2.record.kafka_value),
                        ),
                    ),
                )
        finally:
            object.__setattr__(tampered, "record", orig_record)

        # 0 sends, 0 DB deltas, R1 still in_flight, R2 still blocked by predecessor order.
        assert len(native.published) == 0
        counts_after_tamper = await _measure_table_counts(admin_conn)
        hashes_after_tamper = await _measure_table_hashes(admin_conn)
        assert _compute_deltas(counts_before, counts_after_tamper) == {
            tbl: 0 for tbl in SIX_TABLES
        }
        assert hashes_after_tamper == hashes_before

        blocked_r2 = await svc.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=2
        )
        assert not blocked_r2.claimed
        assert blocked_r2.outcome is not None
        assert blocked_r2.outcome.status is DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR

        # Legitimate R1 attempt still executes cleanly, then R2 proceeds in order.
        r1_out = await svc.execute_claimed_attempt(claim_r1.attempt)
        assert r1_out.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        r2_out = await svc.dispatch_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=2
        )
        assert r2_out is not None
        assert r2_out.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert [_sha256(p["value"]) for p in native.published] == [
            WIRE_ORACLES["FIX-01-REV1"]["kafka_value_sha256"],
            WIRE_ORACLES["FIX-01-REV2"]["kafka_value_sha256"],
        ]
    finally:
        await admin_conn.close()


async def test_d1_r1_blocker3_dispatch_timeout_enforced_over_slower_p1_publisher(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    """Blocker 3 PG+P1 proof: 0.1s DispatchConfig timeout fires <0.3s despite 5.0s P1 publisher."""
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        # Real P1 publisher has default 5.0s timeout and slow 2.0s broker.
        p1_pub, native = _make_real_p1_publisher(
            start_offset=2300,
            publish_timeout_seconds=5.0,
            delay_seconds=2.0,
        )
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=p1_pub,
            config=DispatchConfig(
                worker_id="b3-timeout-worker",
                publish_timeout_seconds=0.1,
                claim_lease_seconds=5,
                lease_settlement_margin_seconds=1.0,
                retry_backoff_initial_ms=20,
                retry_backoff_max_ms=50,
            ),
        )
        await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        claim = await svc.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim.claimed and claim.attempt is not None

        start = time.monotonic()
        outcome = await svc.execute_claimed_attempt(claim.attempt)
        elapsed = time.monotonic() - start

        assert (
            elapsed < 0.3
        ), f"Configured 0.1s dispatch timeout waited {elapsed:.3f}s (>= 0.3s)"
        assert outcome.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
        assert outcome.input_offset_committable is False
        assert outcome.dispatch.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
        assert outcome.dispatch.claim_expires_at is not None
        assert outcome.dispatch.broker_offset is None
        assert outcome.dispatch.last_error_code == "PUBLISH_AMBIGUOUS"

        # Wait briefly and confirm cancelled send never flipped the DB row to acknowledged.
        await asyncio.sleep(0.35)
        after_row = await svc.load_dispatch_state(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert after_row is not None
        assert after_row.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
        assert after_row.broker_offset is None
        assert len(native.published) == 0
    finally:
        await admin_conn.close()


async def test_d1_r2_same_revision_alternative_wire_completion_rejected(
    pg_d1_env: dict[str, Any],
    fix01_inputs: dict[str, Any],
) -> None:
    """D1-R2 PG+P1 proof: internally consistent alternative wire (producer_version=None) cannot ACK stored R1."""
    admin_conn = await asyncpg.connect(pg_d1_env["disp_admin_dsn"])
    try:
        await _truncate_incident_tables(admin_conn)
        rev1 = await _persist_rev1(pg_d1_env["runtime_sf"], fix01_inputs)
        incident_id = rev1.aggregate.incident_id

        # Original service uses FIX-01-REV1 (producer_version="phase4-incident-publisher/1.0", 7721 bytes).
        orig_p1, orig_native = _make_real_p1_publisher(
            producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
            start_offset=2400,
        )
        svc = IncidentDispatchService(
            session_factory=pg_d1_env["runtime_sf"],
            publisher=orig_p1,
            config=DispatchConfig(worker_id="d1-r2-orig-worker"),
        )
        staged = await svc.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=fix01_inputs["staging"],
        )
        assert staged is not None
        claim = await svc.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim.claimed and claim.attempt is not None
        assert len(claim.attempt.record.kafka_value) == 7721
        assert (
            _sha256(claim.attempt.record.kafka_value)
            == "9c9e20c228ee31b1cfe5e75b41caa8dcfb5cfb38a15dd908d3fbd39901c45d5a"
        )

        # Build approved alternative C1 wire variant (producer_version=None, 7694 bytes).
        staging_ctx: DispatchStagingContext = fix01_inputs["staging"]
        alt_record = build_incident_publication(
            incident=rev1.aggregate,
            snapshot=staging_ctx.snapshot,
            expected_topology=staging_ctx.expected_topology,
            blast_radius_policy=staging_ctx.blast_radius_policy,
            correlation_config=staging_ctx.correlation_config,
            execution_context=staging_ctx.execution_context,
            producer_version=None,
        )
        assert len(alt_record.kafka_value) == 7694
        assert (
            _sha256(alt_record.kafka_value)
            == "ee290d4063d96d8215a5dba5b1197339dd5443917b6789fca553e93f3ff8c5c1"
        )

        # Publish the alternative through a real P1 adapter with producer_version=None.
        alt_p1, alt_native = _make_real_p1_publisher(
            producer_version=None, start_offset=2500
        )
        alt_outcome = await alt_p1.publish(alt_record)
        assert alt_outcome.status is IncidentPublishStatus.ACKNOWLEDGED
        assert alt_outcome.result is not None
        assert alt_outcome.result.serialized_bytes == 7694
        assert len(alt_native.published) == 1
        assert len(orig_native.published) == 0

        # Build an internally consistent copied dispatch + ClaimedDispatchAttempt passing __post_init__.
        alt_dispatch = replace(
            claim.attempt.dispatch,
            publication_fingerprint=alt_record.publication_fingerprint,
            kafka_value=alt_record.kafka_value,
            kafka_headers_json=tuple(
                (p[0], p[1]) for p in alt_record.kafka_headers_json
            ),
            wire_record_bytes=alt_record.estimated_record_bytes,
        )
        alt_attempt = ClaimedDispatchAttempt(
            tenant_id=claim.attempt.tenant_id,
            incident_id=claim.attempt.incident_id,
            revision=claim.attempt.revision,
            claim_token=claim.attempt.claim_token,
            dispatch=alt_dispatch,
            record=alt_record,
        )

        counts_before = await _measure_table_counts(admin_conn)
        hashes_before = await _measure_table_hashes(admin_conn)

        # 1. Public service completion MUST reject the alternative wire attempt/receipt.
        with pytest.raises(DispatchInputError):
            await svc.complete_claimed_attempt(alt_attempt, alt_outcome)

        # 2. Direct repository record_acknowledged MUST also reject (both with & without expected_record).
        async with pg_d1_env["runtime_sf"]() as session:
            async with session.begin():
                repo = SqlAlchemyIncidentDispatchRepository(session, TENANT_A)
                await repo.bind_tenant_context()
                with pytest.raises(DispatchInputError):
                    await repo.record_acknowledged(
                        incident_id=incident_id,
                        revision=1,
                        claim_token=claim.attempt.claim_token,
                        publish_result=alt_outcome.result,
                        expected_record=alt_record,
                    )

        async with pg_d1_env["runtime_sf"]() as session:
            async with session.begin():
                repo = SqlAlchemyIncidentDispatchRepository(session, TENANT_A)
                await repo.bind_tenant_context()
                with pytest.raises(DispatchInputError):
                    await repo.record_acknowledged(
                        incident_id=incident_id,
                        revision=1,
                        claim_token=claim.attempt.claim_token,
                        publish_result=alt_outcome.result,
                    )

        # All 6 table counts & full content hashes MUST be completely unchanged!
        counts_after = await _measure_table_counts(admin_conn)
        hashes_after = await _measure_table_hashes(admin_conn)
        assert _compute_deltas(counts_before, counts_after) == {
            tbl: 0 for tbl in SIX_TABLES
        }
        assert hashes_after == hashes_before
        assert len(orig_native.published) == 0

        mid_row = await svc.load_dispatch_state(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert mid_row is not None
        assert mid_row.dispatch_state is DispatchState.IN_FLIGHT
        assert mid_row.broker_offset is None

        # 3. Clean completion with genuine FIX-01-REV1 (7721 bytes) still succeeds.
        orig_outcome = await orig_p1.publish(claim.attempt.record)
        assert orig_outcome.status is IncidentPublishStatus.ACKNOWLEDGED
        assert orig_outcome.result is not None
        assert orig_outcome.result.serialized_bytes == 7721
        assert len(orig_native.published) == 1

        ack_out = await svc.complete_claimed_attempt(claim.attempt, orig_outcome)
        assert ack_out.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert ack_out.dispatch.dispatch_state is DispatchState.ACKNOWLEDGED
        assert ack_out.dispatch.broker_offset == orig_outcome.result.offset
        assert (
            ack_out.dispatch.kafka_value is not None
            and len(ack_out.dispatch.kafka_value) == 7721
        )

        # 4. Subsequent late/stale completion with the genuine attempt still yields DispatchClaimLostError.
        with pytest.raises(DispatchClaimLostError):
            await svc.complete_claimed_attempt(claim.attempt, orig_outcome)
    finally:
        await admin_conn.close()
