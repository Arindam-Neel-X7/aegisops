"""Real Kafka / PostgreSQL / Neo4j integration validation for Task 4.12-V1.

Marked ``pytest.mark.integration``. Exercises the unmodified production
correlation stack end-to-end against live Apache Kafka 3.7.0, PostgreSQL 16
(migrated to head ``20261127_0003`` with an actual least-privilege non-bypass
runtime role), and Neo4j 5.26.30 (pinned authenticated topology snapshot):

- Existing anomaly transport: ``AnomalySignalPublisher`` + ``KafkaTelemetryProducer``
- Production coordinator: ``CorrelationRuntimeCoordinator`` / ``run_controlled_correlation_runtime``
- Atomic persistence: ``persist_incident_atomic`` (Task 4.9 five tables)
- Durable dispatch: ``IncidentDispatchService`` + ``IncidentPublisher`` (Tasks D1/P1/C1)
- Real incident readback: ``AIOKafkaConsumer`` + ``validate_incident_publication`` + ``validate_phase5_handoff``

Covers all four Task 4.12 acceptance gates and A1-R1 V1 requirements:
- Readiness, actual non-bypass PostgreSQL role verification, negative guards,
  and declared disposable Neo4j topology isolation & cleanup.
- AC1 & AC4 (FIX-01): exact anomaly membership creates 1 durable incident with
  1 intent and 1 logical publication identity per revision (rev 1 & rev 2);
  consumed real Kafka key, complete payload bytes, ordered headers, revision,
  semantic fingerprint, and reconstructed handoff match approved wire oracles,
  PostgreSQL readback, and the pinned authenticated Neo4j snapshot.
- AC2 (FIX-02): replay and coordinator restart (both post-commit history reread
  and pre-commit fresh-group replay) produce 0 extra incidents, memberships,
  timelines, intents, dispatches, or Kafka records.
- AC3 (FIX-04): controlled publication failure preserves all 5 committed tables
  and durable ``retry_scheduled`` work with explicit typed outcomes; bounded
  recovery publishes the exact same approved event.
- AC2 & AC3 (FIX-03 / CW-06): injected post-real-broker-send pre-DB-ACK crash
  boundary preserves committed state and reconciles 1 logical publication
  identity against 2 byte-identical physical Kafka deliveries (1 allowed duplicate).
- AC2 (A1-R1 FIX-15 / CW-11): real-broker expired-owner late completion trace
  reconciling physical ``R1, R2, R1`` deliveries against 2 logical identities
  with PostgreSQL fencing raising ``DispatchClaimLostError`` and 0 stale updates.
- Failure-path cleanup proof verifying all declared disposable PostgreSQL,
  Kafka consumer-group, and Neo4j resources are removed even on exception.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
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
from uuid import NAMESPACE_DNS, UUID, uuid5

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.protocol.admin import DeleteGroupsRequest
import asyncpg
from neo4j import AsyncGraphDatabase
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import AnomalySignal
from app.anomaly.publisher import AnomalySignalPublisher
from app.core.config import settings
from app.correlation.aggregate import create_or_update_incident
from app.correlation.blast_radius import BlastRadiusPolicy
from app.correlation.config import CorrelationConfig
from app.correlation.consumer import (
    AnomalyCorrelationInput,
    InMemoryAnomalyDedupeStore,
)
from app.correlation.dispatch_repository import (
    MUTABLE_DISPATCH_COLUMNS,
    DispatchClaimLostError,
    DispatchState,
    verify_runtime_role_and_isolation,
)
from app.correlation.dispatch_service import (
    DispatchConfig,
    DispatchOutcomeStatus,
    DispatchStagingContext,
    IncidentDispatchService,
)
from app.correlation.errors import IncidentPersistenceTenantError
from app.correlation.grouping import group_candidate_signals
from app.correlation.handoff import (
    serialize_phase5_handoff,
    validate_phase5_handoff,
)
from app.correlation.models import (
    CorrelationWindowState,
    TopologySnapshotReference,
)
from app.correlation.persistence_service import persist_incident_atomic
from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    IncidentPublicationRecord,
    PublicationTransportMetadata,
    validate_incident_publication,
)
from app.correlation.publisher import (
    IncidentPublishAmbiguousError,
    IncidentPublisher,
    IncidentPublishOutcome,
    IncidentPublishStatus,
)
from app.correlation.runtime import (
    CorrelationRuntimeConfig,
    CorrelationRuntimeCoordinator,
    CorrelationRuntimeRunStatus,
    PersistenceDisposition,
    RuntimeStartupError,
    check_correlation_runtime_readiness,
    compute_runtime_plan_sha256,
    compute_source_record_digests,
    run_controlled_correlation_runtime,
)
from app.correlation.scoring import score_candidate_pair
from app.correlation.topology_neo4j import Neo4jTopologyProvider
from app.telemetry.topics import KafkaTopic
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
FIVE_INCIDENT_TABLES: tuple[str, ...] = SIX_TABLES[:5]
ANOMALY_PRODUCER_VERSION = "phase4-anomaly-publisher/1.0"
EXPECTED_ALEMBIC_HEAD = "20261127_0003"


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _sha256_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _resolve_pg_admin_dsn() -> str:
    dsn = (
        os.environ.get("V1_POSTGRES_ADMIN_DSN")
        or os.environ.get("D1_POSTGRES_ADMIN_DSN")
        or os.environ.get("S1_R2_POSTGRES_ADMIN_DSN")
        or os.environ.get("M1_POSTGRES_ADMIN_DSN")
        or os.environ.get("P1_POSTGRES_ADMIN_DSN")
    )
    if not dsn:
        pytest.fail(
            "V1_POSTGRES_ADMIN_DSN (or D1/S1_R2/M1/P1_POSTGRES_ADMIN_DSN) is required; "
            "a missing-service skip cannot satisfy the live V1 acceptance gate."
        )
    parsed = urllib.parse.urlparse(dsn)
    if parsed.path.lstrip("/") != "postgres":
        pytest.fail("Admin DSN must target maintenance database 'postgres'")
    return dsn


def _kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", settings.KAFKA_BOOTSTRAP_SERVERS)


def _neo4j_connection(
    *,
    topology_id: UUID | None = None,
    topology_version: str | None = None,
) -> dict[str, Any]:
    return {
        "uri": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        "username": os.environ.get("NEO4J_USER", "neo4j"),
        "password": os.environ.get(
            "NEO4J_PASSWORD", "change-this-development-password"
        ),
        "database": os.environ.get("NEO4J_DATABASE", "neo4j"),
        "timeout_seconds": 15.0,
        "topology_id": topology_id or UUID(PINNED_TOPOLOGY_ID),
        "topology_version": topology_version or PINNED_TOPOLOGY_VERSION,
    }


def _expected_canonical_topology_ref() -> TopologySnapshotReference:
    return TopologySnapshotReference(
        topology_id=UUID(PINNED_TOPOLOGY_ID),
        topology_version=PINNED_TOPOLOGY_VERSION,
        snapshot_fingerprint=PINNED_TOPOLOGY_FINGERPRINT,
        captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
        provider="canonical",
    )


def _pinned_correlation_config() -> CorrelationConfig:
    return CorrelationConfig.model_validate_json(json.dumps(PINNED_CORRELATION_CONFIG))


def _pinned_blast_radius_policy() -> BlastRadiusPolicy:
    return BlastRadiusPolicy.model_validate_json(json.dumps(PINNED_BLAST_RADIUS_POLICY))


def _pinned_execution_context() -> TelemetryExecutionContext:
    return TelemetryExecutionContext(
        run_id=UUID(PINNED_EXECUTION_CONTEXT["run_id"]),
        scenario_id=str(PINNED_EXECUTION_CONTEXT["scenario_id"]),
        scenario_version=str(PINNED_EXECUTION_CONTEXT["scenario_version"]),
        reproducibility_key=str(PINNED_EXECUTION_CONTEXT["reproducibility_key"]),
        seed=int(PINNED_EXECUTION_CONTEXT["seed"]),
    )


def _make_signal(name: str) -> AnomalySignal:
    return AnomalySignal.model_validate(PINNED_SIGNALS[name])


def _replace_dsn_db_and_creds(
    admin_dsn: str,
    *,
    db_name: str,
    user: str | None = None,
    password: str | None = None,
    asyncpg_scheme: bool = False,
) -> str:
    parsed = urllib.parse.urlparse(admin_dsn)
    scheme = "postgresql+asyncpg" if asyncpg_scheme else "postgresql"
    effective_user = user if user is not None else (parsed.username or "")
    effective_pw = password if password is not None else (parsed.password or "")
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    userinfo = urllib.parse.quote(effective_user, safe="")
    if effective_pw:
        userinfo = f"{userinfo}:{urllib.parse.quote(effective_pw, safe='')}"
    return f"{scheme}://{userinfo}@{host}:{port}/{db_name}"


async def _grant_minimum_runtime_privileges(
    conn: asyncpg.Connection,
    *,
    db_name: str,
    role_name: str,
) -> None:
    await conn.execute(f'REVOKE TEMPORARY ON DATABASE "{db_name}" FROM PUBLIC')
    await conn.execute(f'GRANT CONNECT ON DATABASE "{db_name}" TO "{role_name}"')
    await conn.execute(f'GRANT USAGE ON SCHEMA public TO "{role_name}"')
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
    await conn.execute(
        f'GRANT SELECT, INSERT ON TABLE incident_publication_dispatches TO "{role_name}"'
    )
    mutable_cols = ", ".join(MUTABLE_DISPATCH_COLUMNS)
    await conn.execute(
        f'GRANT UPDATE ({mutable_cols}) ON TABLE incident_publication_dispatches TO "{role_name}"'
    )


def _run_alembic_upgrade_head(db_async_dsn: str) -> str:
    env = os.environ.copy()
    env["DATABASE_URL"] = db_async_dsn
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "alembic", "upgrade", "head"],
        cwd=str(BACKEND_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Alembic upgrade head failed (rc={proc.returncode}): {proc.stdout}\n{proc.stderr}"
        )
    cur = subprocess.run(
        [sys.executable, "-B", "-m", "alembic", "current"],
        cwd=str(BACKEND_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if cur.returncode != 0:
        raise RuntimeError(
            f"Alembic current failed (rc={cur.returncode}): {cur.stdout}\n{cur.stderr}"
        )
    return cur.stdout.strip()


async def _measure_table_counts(
    conn: asyncpg.Connection,
    tables: tuple[str, ...] = SIX_TABLES,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for tbl in tables:
        counts[tbl] = int(await conn.fetchval(f"SELECT count(*) FROM {tbl}"))
    return counts


async def _measure_table_hashes(
    conn: asyncpg.Connection,
    tables: tuple[str, ...] = SIX_TABLES,
) -> dict[str, str]:
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
        hashes[tbl] = _sha256_hex(serialized.encode("utf-8"))
    return hashes


async def _truncate_incident_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "TRUNCATE TABLE "
        "incident_publication_dispatches, "
        "incident_publication_intents, "
        "incident_timeline_events, "
        "incident_correlation_evidence, "
        "incident_signal_links, "
        "incidents"
    )


async def _fetch_topic_offsets(
    bootstrap: str, topic: str
) -> tuple[list[int], dict[int, int], dict[int, int]]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        described = await admin.describe_topics([topic])
    finally:
        await admin.close()
    parts = sorted(int(p["partition"]) for p in described[0]["partitions"])
    tps = [TopicPartition(topic, p) for p in parts]
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    await consumer.start()
    await asyncio.sleep(0)
    try:
        beg = await consumer.beginning_offsets(tps)
        end = await consumer.end_offsets(tps)
    finally:
        await asyncio.sleep(0)
        await consumer.stop()
    return (
        parts,
        {tp.partition: int(beg[tp]) for tp in tps},
        {tp.partition: int(end[tp]) for tp in tps},
    )


async def _preposition_consumer_group(
    bootstrap: str,
    *,
    group_id: str,
    topic: str,
    offsets_by_partition: Mapping[int, int],
) -> dict[int, int]:
    """Commit initial starting offsets for a disposable consumer group before producing."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    consumer.subscribe(topics=[topic])
    await consumer.start()
    try:
        assigned = set(consumer.assignment())
        commit_map = {
            tp: int(offsets_by_partition[tp.partition])
            for tp in assigned
            if tp.topic == topic and tp.partition in offsets_by_partition
        }
        await consumer.commit(commit_map)
        confirmed: dict[int, int] = {}
        for tp in sorted(assigned, key=lambda p: p.partition):
            pos = await consumer.committed(tp)
            confirmed[tp.partition] = -1 if pos is None else int(pos)
        return confirmed
    finally:
        await consumer.stop()


async def _read_consumer_group_committed_offsets(
    bootstrap: str,
    *,
    group_id: str,
    topic: str,
    partitions: Sequence[int],
) -> dict[int, int | None]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        tps = [TopicPartition(topic, p) for p in partitions]
        offsets = await admin.list_consumer_group_offsets(group_id, partitions=tps)
        return {
            tp.partition: (
                int(offsets[tp].offset)
                if tp in offsets and offsets[tp].offset >= 0
                else None
            )
            for tp in tps
        }
    finally:
        await admin.close()


async def _delete_consumer_groups_best_effort(
    bootstrap: str, group_ids: Sequence[str]
) -> dict[str, str]:
    if not group_ids:
        return {}
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    results: dict[str, str] = {}
    try:
        for gid in group_ids:
            last_status = "not_attempted"
            for attempt in range(5):
                try:
                    node_id = await admin.find_coordinator(gid)
                    resp = await admin._send_request(
                        DeleteGroupsRequest(group_names=[gid]), node_id
                    )
                    code = int(resp.results[0][1]) if resp.results else -1
                    if code in (0, 69):  # 0=NONE, 69=GROUP_ID_NOT_FOUND
                        last_status = "deleted" if code == 0 else "already_absent"
                        break
                    last_status = f"broker_error_code:{code}"
                except Exception as exc:
                    last_status = f"error:{type(exc).__name__}:{exc}"
                if attempt < 4:
                    await asyncio.sleep(0.25)
            results[gid] = last_status
    finally:
        await admin.close()
    return results


async def _consume_exact_record(
    bootstrap: str,
    *,
    topic: str,
    partition: int,
    offset: int,
    timeout_seconds: float = 15.0,
) -> Any:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    tp = TopicPartition(topic, partition)
    await consumer.start()
    await asyncio.sleep(0)
    try:
        consumer.assign([tp])
        consumer.seek(tp, offset)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            batch = await consumer.getmany(tp, timeout_ms=500, max_records=16)
            for rec in batch.get(tp, ()):
                if int(rec.offset) == offset:
                    return rec
                if int(rec.offset) > offset:
                    raise AssertionError(
                        f"Expected offset {offset} on {topic}[{partition}], observed {rec.offset}"
                    )
        raise TimeoutError(
            f"Timed out reading exact record {topic}[{partition}]@{offset}"
        )
    finally:
        await asyncio.sleep(0)
        await consumer.stop()


async def _produce_real_anomalies_and_build_plan(
    bootstrap: str,
    *,
    signal_specs: Sequence[tuple[str, AnomalySignal]],
    steps: Sequence[dict[str, Any]],
    run_end_logical_time: str,
    max_dispatch_attempts: int = 3,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
) -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
    """Produce real anomalies via AnomalySignalPublisher and bind authentic plan."""
    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap,
        client_id=f"aegisops-v1-anomaly-pub-{secrets.token_hex(4)}",
        producer_version=ANOMALY_PRODUCER_VERSION,
    )
    await producer.start()
    source_records: list[dict[str, Any]] = []
    ack_records: list[dict[str, Any]] = []
    try:
        publisher = AnomalySignalPublisher(producer=producer)
        for delivery_id, signal in signal_specs:
            pub_res = await publisher.publish(signal)
            assert pub_res.topic == KafkaTopic.ANOMALIES.value
            raw = await _consume_exact_record(
                bootstrap,
                topic=pub_res.topic,
                partition=pub_res.partition,
                offset=pub_res.offset,
            )
            digests = compute_source_record_digests(raw)
            corr_input = AnomalyCorrelationInput(
                signal=signal,
                lineage=build_anomaly_reproducibility_lineage(signal),
            )
            source_records.append(
                {
                    "delivery_id": delivery_id,
                    "headers_sha256": digests.headers_sha256,
                    "key_sha256": digests.key_sha256,
                    "offset": int(pub_res.offset),
                    "partition": int(pub_res.partition),
                    "semantic_fingerprint": corr_input.semantic_fingerprint,
                    "signal_id": str(signal.signal_id),
                    "topic": pub_res.topic,
                    "value_sha256": digests.value_sha256,
                }
            )
            ack_records.append(
                {
                    "delivery_id": delivery_id,
                    "signal_id": str(signal.signal_id),
                    "topic": pub_res.topic,
                    "partition": int(pub_res.partition),
                    "offset": int(pub_res.offset),
                    "timestamp_ms": pub_res.timestamp_ms,
                    "serialized_bytes": int(pub_res.serialized_bytes),
                    "latency_ms": float(pub_res.latency_ms),
                    "key_sha256": digests.key_sha256,
                    "value_sha256": digests.value_sha256,
                    "headers_sha256": digests.headers_sha256,
                }
            )
    finally:
        await producer.close()

    plan: dict[str, Any] = {
        "blast_radius_policy": dict(PINNED_BLAST_RADIUS_POLICY),
        "config": dict(PINNED_CORRELATION_CONFIG),
        "config_sha256": PINNED_CONFIG_FINGERPRINT,
        "execution_context": dict(PINNED_EXECUTION_CONTEXT),
        "max_dispatch_attempts": max_dispatch_attempts,
        "producer_version": producer_version,
        "run_end_logical_time": run_end_logical_time,
        "run_id": str(PINNED_EXECUTION_CONTEXT["run_id"]),
        "schema_version": "controlled-correlation-runtime/1.0",
        "source_records": source_records,
        "steps": [dict(s) for s in steps],
        "topology_reference": {
            "topology_captured_at": PINNED_TOPOLOGY_CAPTURED_AT,
            "topology_id": PINNED_TOPOLOGY_ID,
            "topology_snapshot_fingerprint": PINNED_TOPOLOGY_FINGERPRINT,
            "topology_version": PINNED_TOPOLOGY_VERSION,
        },
    }
    plan_sha256 = compute_runtime_plan_sha256(plan)
    return plan, plan_sha256, ack_records


class _PostRealBrokerSendCrashInjector:
    """Explicitly identified CW-06 fault wrapper around the REAL IncidentPublisher.

    Delegates directly to the real ``IncidentPublisher`` (real
    ``KafkaTelemetryProducer`` -> real Kafka broker). Only AFTER the real broker
    acknowledges the record does it record the authentic ``PublishResult`` and
    return ``FAILED_AMBIGUOUS`` on the configured attempt, simulating a process
    crash between broker acceptance and PostgreSQL acknowledgement bookkeeping.
    Never fabricates a broker acknowledgement or database write.
    """

    FAULT_ID = "INJECTED_FAULT_CW06_POST_REAL_BROKER_SEND_BEFORE_DB_ACK"

    def __init__(self, inner: IncidentPublisher, *, inject_once: bool = True) -> None:
        self._inner = inner
        self._inject_remaining = 1 if inject_once else 0
        self.real_broker_acks: list[PublishResult] = []
        self.injected_events: list[dict[str, Any]] = []

    async def publish(
        self, record: IncidentPublicationRecord
    ) -> IncidentPublishOutcome:
        real_outcome = await self._inner.publish(record)
        assert real_outcome.status is IncidentPublishStatus.ACKNOWLEDGED
        assert real_outcome.result is not None
        self.real_broker_acks.append(real_outcome.result)
        if self._inject_remaining > 0:
            self._inject_remaining -= 1
            cause = RuntimeError(self.FAULT_ID)
            err = IncidentPublishAmbiguousError(
                "Incident publication acknowledgement is unconfirmed."
            )
            err.__cause__ = cause
            self.injected_events.append(
                {
                    "fault_id": self.FAULT_ID,
                    "event_id": str(record.identity.event_id),
                    "revision": record.identity.revision,
                    "real_broker_topic": real_outcome.result.topic,
                    "real_broker_partition": int(real_outcome.result.partition),
                    "real_broker_offset": int(real_outcome.result.offset),
                    "real_broker_timestamp_ms": real_outcome.result.timestamp_ms,
                    "real_serialized_bytes": int(real_outcome.result.serialized_bytes),
                }
            )
            return IncidentPublishOutcome(
                status=IncidentPublishStatus.FAILED_AMBIGUOUS,
                error=err,
            )
        return real_outcome


@dataclass
class _V1LiveEnvironment:
    token: str
    admin_dsn: str
    db_name: str
    runtime_role: str
    runtime_password: str
    admin_db_dsn: str
    runtime_async_dsn: str
    admin_async_dsn: str
    alembic_current: str
    kafka_bootstrap: str
    runtime_engine: AsyncEngine
    admin_engine: AsyncEngine
    runtime_session_factory: async_sessionmaker[AsyncSession]
    admin_session_factory: async_sessionmaker[AsyncSession]
    declared_consumer_groups: list[str]
    declared_neo4j_topology_ids: list[str]
    evidence: dict[str, Any]

    def register_consumer_group(self, suffix: str) -> str:
        gid = f"aegisops-v1-{suffix}-{self.token}"
        self.declared_consumer_groups.append(gid)
        return gid


@pytest_asyncio.fixture(scope="module")
async def v1_env() -> AsyncGenerator[_V1LiveEnvironment, None]:
    admin_dsn = _resolve_pg_admin_dsn()
    bootstrap = _kafka_bootstrap()
    token = secrets.token_hex(4)
    db_name = f"v1_corr_{token}"
    runtime_role = f"v1_runtime_{token}"
    runtime_password = f"v1_pw_{secrets.token_hex(12)}"
    assert re.fullmatch(r"[a-z0-9_]+", db_name)
    assert re.fullmatch(r"[a-z0-9_]+", runtime_role)

    maint_conn = await asyncpg.connect(admin_dsn)
    runtime_engine: AsyncEngine | None = None
    admin_engine: AsyncEngine | None = None
    declared_groups: list[str] = []
    declared_neo4j_ids: list[str] = []
    evidence: dict[str, Any] = {
        "task": "4.12-V1",
        "token": token,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "declared_disposable_resources": {
            "postgresql_database": db_name,
            "postgresql_runtime_role": runtime_role,
            "tenants": [str(TENANT_A), str(TENANT_B)],
            "kafka_consumer_groups": declared_groups,
            "neo4j_disposable_topology_ids": declared_neo4j_ids,
        },
        "gates": {},
    }

    try:
        await maint_conn.execute(f'CREATE DATABASE "{db_name}"')
        await maint_conn.execute(
            f"CREATE ROLE \"{runtime_role}\" LOGIN PASSWORD '{runtime_password}' "
            "NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION"
        )
        admin_db_dsn = _replace_dsn_db_and_creds(admin_dsn, db_name=db_name)
        admin_async_dsn = _replace_dsn_db_and_creds(
            admin_dsn, db_name=db_name, asyncpg_scheme=True
        )
        runtime_async_dsn = _replace_dsn_db_and_creds(
            admin_dsn,
            db_name=db_name,
            user=runtime_role,
            password=runtime_password,
            asyncpg_scheme=True,
        )

        alembic_current = _run_alembic_upgrade_head(admin_async_dsn)
        assert EXPECTED_ALEMBIC_HEAD in alembic_current, alembic_current

        db_conn = await asyncpg.connect(admin_db_dsn)
        try:
            plan_id = await db_conn.fetchval("SELECT id FROM plans LIMIT 1")
            await _grant_minimum_runtime_privileges(
                db_conn, db_name=db_name, role_name=runtime_role
            )
            for tid, slug in (
                (TENANT_A, f"v1-tenant-a-{token}"),
                (TENANT_B, f"v1-tenant-b-{token}"),
            ):
                await db_conn.execute(
                    "INSERT INTO tenants (id, name, slug, plan_id, seats_limit, data_retention_days) "
                    "VALUES ($1, $2, $3, $4, 10, 30) ON CONFLICT (id) DO NOTHING",
                    tid,
                    f"V1 Disposable {slug}",
                    slug,
                    plan_id,
                )
        finally:
            await db_conn.close()

        runtime_engine = create_async_engine(
            runtime_async_dsn,
            isolation_level="READ COMMITTED",
            pool_pre_ping=True,
        )
        admin_engine = create_async_engine(
            admin_async_dsn,
            isolation_level="READ COMMITTED",
            pool_pre_ping=True,
        )
        env_obj = _V1LiveEnvironment(
            token=token,
            admin_dsn=admin_dsn,
            db_name=db_name,
            runtime_role=runtime_role,
            runtime_password=runtime_password,
            admin_db_dsn=admin_db_dsn,
            runtime_async_dsn=runtime_async_dsn,
            admin_async_dsn=admin_async_dsn,
            alembic_current=alembic_current,
            kafka_bootstrap=bootstrap,
            runtime_engine=runtime_engine,
            admin_engine=admin_engine,
            runtime_session_factory=async_sessionmaker(
                runtime_engine, expire_on_commit=False
            ),
            admin_session_factory=async_sessionmaker(
                admin_engine, expire_on_commit=False
            ),
            declared_consumer_groups=declared_groups,
            declared_neo4j_topology_ids=declared_neo4j_ids,
            evidence=evidence,
        )
        yield env_obj
    finally:
        cleanup_summary: dict[str, Any] = {}
        if runtime_engine is not None:
            await runtime_engine.dispose()
        if admin_engine is not None:
            await admin_engine.dispose()

        # 1. Cleanup declared Kafka consumer groups
        cleanup_summary[
            "kafka_consumer_groups"
        ] = await _delete_consumer_groups_best_effort(bootstrap, declared_groups)

        # 2. Cleanup declared Neo4j disposable topologies (if any remain)
        if declared_neo4j_ids:
            neo_cfg = _neo4j_connection()
            driver = AsyncGraphDatabase.driver(
                neo_cfg["uri"], auth=(neo_cfg["username"], neo_cfg["password"])
            )
            try:
                async with driver.session(database=neo_cfg["database"]) as session:
                    for tid in declared_neo4j_ids:
                        res = await session.run(
                            "MATCH (s:Service) WHERE s.topology_id = $tid DETACH DELETE s",
                            {"tid": tid},
                        )
                        await res.consume()
                cleanup_summary["neo4j_disposable_topologies"] = "deleted"
            finally:
                await driver.close()

        # 3. Terminate connections, drop disposable PG database, drop disposable runtime role
        try:
            await maint_conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                db_name,
            )
            await maint_conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            await maint_conn.execute(f'DROP ROLE IF EXISTS "{runtime_role}"')
            cleanup_summary["postgresql_database_dropped"] = db_name
            cleanup_summary["postgresql_role_dropped"] = runtime_role
        finally:
            await maint_conn.close()

        evidence["cleanup"] = cleanup_summary
        evidence["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        out_path = os.environ.get("V1_EVIDENCE_OUT_PATH")
        if out_path:
            target = Path(out_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )


async def _verify_kafka_incident_record_against_oracle_and_db(
    *,
    raw_msg: Any,
    oracle_key: str,
    neo4j_snapshot: Any,
    db_dispatch_row: Mapping[str, Any],
    db_intent_row: Mapping[str, Any],
) -> dict[str, Any]:
    oracle = WIRE_ORACLES[oracle_key]
    raw_key = bytes(raw_msg.key)
    raw_value = bytes(raw_msg.value)
    raw_headers = tuple((str(k), bytes(v)) for k, v in (raw_msg.headers or ()))

    # 1. Exact wire match vs approved A1-R1 oracle
    assert raw_msg.topic == oracle["kafka_topic"] == KafkaTopic.INCIDENTS.value
    assert raw_key.decode("utf-8") == oracle["kafka_key_utf8"]
    assert len(raw_key) == oracle["kafka_key_bytes"]
    assert len(raw_value) == oracle["kafka_value_bytes"]
    assert _sha256_hex(raw_value) == oracle["kafka_value_sha256"]
    decoded_headers = [[k, v.decode("utf-8")] for k, v in raw_headers]
    assert decoded_headers == oracle["kafka_headers"]

    # 2. Exact match vs PostgreSQL readback
    assert bytes(db_dispatch_row["kafka_key"]) == raw_key
    assert bytes(db_dispatch_row["kafka_value"]) == raw_value
    db_headers_json = (
        json.loads(db_dispatch_row["kafka_headers_json"])
        if isinstance(db_dispatch_row["kafka_headers_json"], str)
        else db_dispatch_row["kafka_headers_json"]
    )
    assert db_headers_json == oracle["kafka_headers"]
    assert str(db_dispatch_row["event_id"]) == oracle["event_id"]
    assert (
        db_dispatch_row["publication_fingerprint"] == oracle["publication_fingerprint"]
    )
    assert int(db_dispatch_row["revision"]) == oracle["revision"]
    assert db_dispatch_row["revision_fingerprint"] == oracle["revision_fingerprint"]
    assert db_intent_row["revision_fingerprint"] == oracle["revision_fingerprint"]
    assert int(db_intent_row["revision"]) == oracle["revision"]
    assert (
        db_dispatch_row["topology_snapshot_fingerprint"] == PINNED_TOPOLOGY_FINGERPRINT
    )

    # 3. Full C1 semantic validation + Task 4.10 Phase 5 handoff validation against pinned Neo4j snapshot
    transport_meta = PublicationTransportMetadata(
        partition=int(raw_msg.partition),
        offset=int(raw_msg.offset),
        broker_timestamp_ms=int(raw_msg.timestamp),
        kafka_timestamp_ms=int(raw_msg.timestamp),
        consumed_at=datetime.now(timezone.utc),
    )
    validated = validate_incident_publication(
        raw_value=raw_value,
        raw_key=raw_key,
        headers=raw_headers,
        snapshot=neo4j_snapshot,
        expected_topology=_expected_canonical_topology_ref(),
        expected_blast_radius_policy=_pinned_blast_radius_policy(),
        expected_correlation_config=_pinned_correlation_config(),
        expected_execution_context=_pinned_execution_context(),
        expected_producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
        expected_tenant_id=TENANT_A,
        topic=raw_msg.topic,
        transport_metadata=transport_meta,
        require_canonical_bytes=True,
    )
    assert str(validated.identity.event_id) == oracle["event_id"]
    assert (
        validated.identity.publication_fingerprint == oracle["publication_fingerprint"]
    )
    assert validated.identity.revision == oracle["revision"]
    assert str(validated.identity.incident_id) == oracle["incident_id"]
    assert validated.payload.incident_number == oracle["incident_number"]
    assert validated.payload.handoff.handoff_id == UUID(oracle["handoff_id"])
    assert (
        validated.payload.handoff.handoff_fingerprint == oracle["handoff_fingerprint"]
    )

    handoff_bytes = serialize_phase5_handoff(validated.payload.handoff)
    assert len(handoff_bytes) == oracle["handoff_bytes"]
    assert _sha256_hex(handoff_bytes) == oracle["handoff_sha256"]

    validated_handoff = validate_phase5_handoff(
        raw=handoff_bytes,
        snapshot=neo4j_snapshot,
        expected_topology=_expected_canonical_topology_ref(),
        expected_policy=_pinned_blast_radius_policy(),
    )
    assert validated_handoff == validated.payload.handoff
    reachable = [
        {
            "minimum_depth": int(s.minimum_depth),
            "name": s.name,
            "service_id": str(s.service_id),
        }
        for s in validated_handoff.topology_context.reachable_services
    ]
    assert reachable == [
        {
            "minimum_depth": 1,
            "name": "notification-service",
            "service_id": "10ed11a8-4faf-50f5-9903-2417c842278c",
        },
        {
            "minimum_depth": 1,
            "name": "payment-service",
            "service_id": "2b8e9c49-61b0-5a53-821d-6193136b1db2",
        },
        {
            "minimum_depth": 1,
            "name": "inventory-service",
            "service_id": "936fb446-a6f1-5cf9-88d2-65789b6e6eae",
        },
        {
            "minimum_depth": 2,
            "name": "database",
            "service_id": "a50eab12-f3a8-544a-be74-060c56b3e254",
        },
        {
            "minimum_depth": 0,
            "name": "order-service",
            "service_id": "fdface04-c1df-5daa-ab47-23debcebb2fd",
        },
    ]

    return {
        "oracle_key": oracle_key,
        "topic": raw_msg.topic,
        "partition": int(raw_msg.partition),
        "offset": int(raw_msg.offset),
        "timestamp_ms": int(raw_msg.timestamp),
        "key_utf8": raw_key.decode("utf-8"),
        "key_bytes": len(raw_key),
        "value_bytes": len(raw_value),
        "value_sha256": _sha256_hex(raw_value),
        "headers": decoded_headers,
        "event_id": str(validated.identity.event_id),
        "incident_id": str(validated.identity.incident_id),
        "revision": validated.identity.revision,
        "revision_fingerprint": validated.identity.revision_fingerprint,
        "publication_fingerprint": validated.identity.publication_fingerprint,
        "handoff_id": str(validated_handoff.handoff_id),
        "handoff_fingerprint": validated_handoff.handoff_fingerprint,
        "handoff_sha256": _sha256_hex(handoff_bytes),
        "topology_snapshot_fingerprint": neo4j_snapshot.snapshot_fingerprint,
        "blast_radius_service_count": len(reachable),
    }


# ============================================================================
# TEST 1: READINESS, NON-BYPASS ROLE & DISPOSABLE NEO4J TOPOLOGY ISOLATION
# ============================================================================


async def test_v1_01_readiness_non_bypass_role_and_disposable_neo4j_topology(
    v1_env: _V1LiveEnvironment,
) -> None:
    """Verify live 3-service readiness, non-bypass PG role, negative guards & Neo4j pin."""
    # 1. Verify actual runtime role catalog attributes
    db_conn = await asyncpg.connect(v1_env.admin_db_dsn)
    try:
        role_row = await db_conn.fetchrow(
            "SELECT rolname, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole "
            "FROM pg_roles WHERE rolname = $1",
            v1_env.runtime_role,
        )
        assert role_row is not None
        assert role_row["rolsuper"] is False
        assert role_row["rolbypassrls"] is False
        assert role_row["rolcreatedb"] is False
        assert role_row["rolcreaterole"] is False
    finally:
        await db_conn.close()

    # 2. Verify positive readiness with real non-bypass role, Kafka, and Neo4j
    neo4j_provider = Neo4jTopologyProvider(**_neo4j_connection())
    try:
        readiness = await check_correlation_runtime_readiness(
            session_factory=v1_env.runtime_session_factory,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            bootstrap_servers=v1_env.kafka_bootstrap,
            timeout_seconds=15.0,
        )
        assert readiness.ready is True, readiness
        assert readiness.status == "ready"
        assert readiness.error_codes == ()
        assert readiness.components["postgresql"]["ready"] is True
        assert readiness.components["kafka"]["ready"] is True
        assert readiness.components["kafka"]["present_required_topics"] == [
            KafkaTopic.INCIDENTS.value,
            KafkaTopic.ANOMALIES.value,
        ]
        assert readiness.components["topology"]["ready"] is True
        assert (
            readiness.components["topology"]["observed_fingerprint"]
            == PINNED_TOPOLOGY_FINGERPRINT
        )

        # 3. Negative role guard: superuser session must fail readiness and direct guard
        bad_role_readiness = await check_correlation_runtime_readiness(
            session_factory=v1_env.admin_session_factory,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            bootstrap_servers=v1_env.kafka_bootstrap,
            timeout_seconds=15.0,
        )
        assert bad_role_readiness.ready is False
        assert "POSTGRES_NOT_READY" in bad_role_readiness.error_codes

        async with v1_env.admin_session_factory() as admin_sess:
            async with admin_sess.begin():
                with pytest.raises(IncidentPersistenceTenantError):
                    await verify_runtime_role_and_isolation(admin_sess)

        # 4. Negative Neo4j pin guard: stale fingerprint must fail readiness and coordinator start
        wrong_pin = TopologySnapshotReference(
            topology_id=UUID(PINNED_TOPOLOGY_ID),
            topology_version=PINNED_TOPOLOGY_VERSION,
            snapshot_fingerprint="0" * 64,
            captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
            provider="neo4j",
        )
        bad_pin_readiness = await check_correlation_runtime_readiness(
            session_factory=v1_env.runtime_session_factory,
            topology_provider=neo4j_provider,
            expected_topology=wrong_pin,
            bootstrap_servers=v1_env.kafka_bootstrap,
            timeout_seconds=15.0,
        )
        assert bad_pin_readiness.ready is False
        assert "TOPOLOGY_PIN_UNAVAILABLE_OR_MISMATCH" in bad_pin_readiness.error_codes

        dummy_producer = KafkaTelemetryProducer(
            bootstrap_servers=v1_env.kafka_bootstrap,
            client_id=f"v1-bad-pin-{v1_env.token}",
            producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
        )
        dummy_dispatch = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=IncidentPublisher(dummy_producer),
        )
        bad_coord = CorrelationRuntimeCoordinator(
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=dummy_dispatch,
            topology_provider=neo4j_provider,
            expected_topology=wrong_pin,
            bootstrap_servers=v1_env.kafka_bootstrap,
        )
        with pytest.raises(RuntimeStartupError):
            await bad_coord.start()

        # 5. Declared disposable Neo4j topology fixture: pin independently & prove canonical isolation
        disp_topo_id = uuid5(
            NAMESPACE_DNS, f"aegisops.v1.disposable-topology.{v1_env.token}"
        )
        disp_topo_ver = f"v1-disp-{v1_env.token}"
        svc_a = uuid5(NAMESPACE_DNS, f"aegisops.v1.svc-a.{v1_env.token}")
        svc_b = uuid5(NAMESPACE_DNS, f"aegisops.v1.svc-b.{v1_env.token}")
        v1_env.declared_neo4j_topology_ids.append(str(disp_topo_id))

        disp_provider = Neo4jTopologyProvider(
            **_neo4j_connection(
                topology_id=disp_topo_id, topology_version=disp_topo_ver
            )
        )
        try:
            async with disp_provider._driver.session(
                database=disp_provider._database
            ) as session:
                await (
                    await session.run(
                        "CREATE (a:Service {service_id: $sa, name: 'v1-disp-gateway', "
                        "service_type: 'api', topology_id: $tid, topology_version: $tver}) "
                        "CREATE (b:Service {service_id: $sb, name: 'v1-disp-worker', "
                        "service_type: 'worker', topology_id: $tid, topology_version: $tver}) "
                        "CREATE (a)-[:DEPENDS_ON {dependency_type: 'sync', "
                        "topology_id: $tid, topology_version: $tver}]->(b)",
                        {
                            "sa": str(svc_a),
                            "sb": str(svc_b),
                            "tid": str(disp_topo_id),
                            "tver": disp_topo_ver,
                        },
                    )
                ).consume()

            disp_snap = await disp_provider.get_snapshot(expected_version=disp_topo_ver)
            assert len(disp_snap.nodes) == 2
            assert len(disp_snap.edges) == 1
            assert disp_snap.version.topology_id == disp_topo_id

            canonical_snap = await neo4j_provider.get_snapshot(
                expected_version=PINNED_TOPOLOGY_VERSION
            )
            assert len(canonical_snap.nodes) == 7
            assert len(canonical_snap.edges) == 7
            assert canonical_snap.snapshot_fingerprint == PINNED_TOPOLOGY_FINGERPRINT
        finally:
            async with disp_provider._driver.session(
                database=disp_provider._database
            ) as session:
                await (
                    await session.run(
                        "MATCH (s:Service) WHERE s.topology_id = $tid DETACH DELETE s",
                        {"tid": str(disp_topo_id)},
                    )
                ).consume()
                rem = await (
                    await session.run(
                        "MATCH (s:Service) WHERE s.topology_id = $tid RETURN count(s) AS c",
                        {"tid": str(disp_topo_id)},
                    )
                ).single()
                assert rem is not None and int(rem["c"]) == 0
            await disp_provider.close()

        v1_env.evidence["gates"]["readiness_and_role"] = {
            "runtime_role": v1_env.runtime_role,
            "rolsuper": False,
            "rolbypassrls": False,
            "alembic_current": v1_env.alembic_current,
            "readiness_status": readiness.status,
            "readiness_components": readiness.components,
            "superuser_rejected": True,
            "mismatched_neo4j_pin_rejected": True,
            "disposable_neo4j_topology_id": str(disp_topo_id),
            "disposable_neo4j_topology_fingerprint": disp_snap.snapshot_fingerprint,
            "disposable_neo4j_cleaned_remaining_nodes": 0,
            "canonical_neo4j_topology_fingerprint": PINNED_TOPOLOGY_FINGERPRINT,
        }
    finally:
        await neo4j_provider.close()


# ============================================================================
# TEST 2 & 3: AC1, AC4 (FIX-01) AND AC2 ORDINARY REPLAY / RESTART (FIX-02)
# ============================================================================


async def test_v1_02_ac1_ac4_fix01_and_ac2_fix02_replay_restart(
    v1_env: _V1LiveEnvironment,
) -> None:
    """Verify live FIX-01 (AC1 + AC4) followed by FIX-02 replay & restart (AC2)."""
    admin_conn = await asyncpg.connect(v1_env.admin_db_dsn)
    neo4j_provider = Neo4jTopologyProvider(**_neo4j_connection())
    inc_producer = KafkaTelemetryProducer(
        bootstrap_servers=v1_env.kafka_bootstrap,
        client_id=f"aegisops-v1-inc-pub-ac1-{v1_env.token}",
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    try:
        await _truncate_incident_tables(admin_conn)
        neo4j_snapshot = await neo4j_provider.get_snapshot(
            expected_version=PINNED_TOPOLOGY_VERSION
        )
        assert neo4j_snapshot.snapshot_fingerprint == PINNED_TOPOLOGY_FINGERPRINT

        anom_parts, _, anom_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.ANOMALIES.value
        )
        inc_parts, _, inc_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )

        # Register & preposition disposable consumer group BEFORE producing S1 & S2
        gid_run1 = v1_env.register_consumer_group("ac1-run1")
        startup_pos_run1 = await _preposition_consumer_group(
            v1_env.kafka_bootstrap,
            group_id=gid_run1,
            topic=KafkaTopic.ANOMALIES.value,
            offsets_by_partition=anom_end_before,
        )

        # Produce real S1 & S2 anomalies through existing AnomalySignalPublisher
        plan, plan_sha256, anomaly_acks = await _produce_real_anomalies_and_build_plan(
            v1_env.kafka_bootstrap,
            signal_specs=[("S1", _make_signal("S1")), ("S2", _make_signal("S2"))],
            steps=[
                {
                    "delivery_id": "S1",
                    "logical_time": "2026-11-27T00:01:00Z",
                    "op": "ingest",
                },
                {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
                {
                    "delivery_id": "S2",
                    "logical_time": "2026-11-27T00:01:10Z",
                    "op": "ingest",
                },
                {"logical_time": "2026-11-27T00:01:25Z", "op": "evaluate"},
            ],
            run_end_logical_time="2026-11-27T00:01:25Z",
        )

        await inc_producer.start()
        publisher = IncidentPublisher(inc_producer, publish_timeout_seconds=10.0)
        dispatch_service = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=publisher,
            config=DispatchConfig(
                worker_id=f"v1-ac1-worker-{v1_env.token}",
                max_dispatch_attempts=3,
                claim_lease_seconds=30,
                publish_timeout_seconds=10.0,
                lease_settlement_margin_seconds=5.0,
            ),
        )

        # Execute production coordinator end-to-end
        run1 = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=dispatch_service,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid_run1,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
        )

        assert run1.status is CorrelationRuntimeRunStatus.COMPLETED
        assert run1.phase5_activated is False
        assert run1.unresolved_operation_count == 0
        assert run1.unresolved_publish_count == 0
        assert run1.topology_snapshot_fingerprint == PINNED_TOPOLOGY_FINGERPRINT
        assert len(run1.revisions) == 2
        assert run1.revisions[0].revision == 1
        assert (
            run1.revisions[0].persistence_disposition
            is PersistenceDisposition.PERSISTED_CREATED
        )
        assert run1.revisions[1].revision == 2
        assert (
            run1.revisions[1].persistence_disposition
            is PersistenceDisposition.PERSISTED_UPDATED
        )
        assert [o.status for o in run1.dispatch_outcomes] == [
            DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED,
            DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED,
        ]

        # AC1: Exact PostgreSQL counts and durable state
        counts_after_run1 = await _measure_table_counts(admin_conn)
        assert counts_after_run1 == {
            "incidents": 1,
            "incident_signal_links": 2,
            "incident_correlation_evidence": 1,
            "incident_timeline_events": 4,
            "incident_publication_intents": 2,
            "incident_publication_dispatches": 2,
        }
        hashes_after_run1 = await _measure_table_hashes(admin_conn)

        # Verify Tenant B RLS isolation via runtime role (0 rows visible across all 6 tables)
        async with v1_env.runtime_session_factory() as rls_sess:
            async with rls_sess.begin():
                from sqlalchemy import text

                await rls_sess.execute(
                    text("SELECT set_config('app.current_tenant_id', :tid, true)"),
                    {"tid": str(TENANT_B)},
                )
                for tbl in SIX_TABLES:
                    cnt = await rls_sess.scalar(text(f"SELECT count(*) FROM {tbl}"))
                    assert int(cnt or 0) == 0, f"Tenant B saw rows in {tbl}"

        # Load DB readback for rev 1 and rev 2
        dispatch_rows = await admin_conn.fetch(
            "SELECT * FROM incident_publication_dispatches "
            "WHERE tenant_id = $1 ORDER BY revision ASC",
            TENANT_A,
        )
        intent_rows = await admin_conn.fetch(
            "SELECT * FROM incident_publication_intents "
            "WHERE tenant_id = $1 ORDER BY revision ASC",
            TENANT_A,
        )
        assert len(dispatch_rows) == 2 and len(intent_rows) == 2

        # AC4: Consume real incident events from Kafka aegis.incidents & reconcile
        _, _, inc_end_after_run1 = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        physical_added_run1 = sum(
            inc_end_after_run1[p] - inc_end_before[p] for p in inc_parts
        )
        assert physical_added_run1 == 2

        consumed_wire_proofs: list[dict[str, Any]] = []
        for idx, oracle_key in enumerate(("FIX-01-REV1", "FIX-01-REV2")):
            drow = dispatch_rows[idx]
            irow = intent_rows[idx]
            assert drow["dispatch_state"] == DispatchState.ACKNOWLEDGED.value
            assert int(drow["attempt_count"]) == 1
            assert drow["broker_topic"] == KafkaTopic.INCIDENTS.value
            assert drow["broker_partition"] is not None
            assert drow["broker_offset"] is not None
            raw_inc = await _consume_exact_record(
                v1_env.kafka_bootstrap,
                topic=drow["broker_topic"],
                partition=int(drow["broker_partition"]),
                offset=int(drow["broker_offset"]),
            )
            proof = await _verify_kafka_incident_record_against_oracle_and_db(
                raw_msg=raw_inc,
                oracle_key=oracle_key,
                neo4j_snapshot=neo4j_snapshot,
                db_dispatch_row=dict(drow),
                db_intent_row=dict(irow),
            )
            consumed_wire_proofs.append(proof)

        # Verify real Kafka consumer group committed offset advanced past S1 & S2
        committed_after_run1 = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid_run1,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        for p in anom_parts:
            p_acks = [a["offset"] for a in anomaly_acks if a["partition"] == p]
            if p_acks:
                assert committed_after_run1[p] == max(p_acks) + 1

        # ====================================================================
        # AC2 (FIX-02): Replay A — same committed consumer group restart
        # ====================================================================
        replay_a = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=dispatch_service,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid_run1,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
            dedupe_store=InMemoryAnomalyDedupeStore(max_size=1024),
        )
        assert replay_a.status is CorrelationRuntimeRunStatus.COMPLETED
        assert all(
            r.persistence_disposition
            is PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY
            for r in replay_a.revisions
        )
        assert all(
            o.status is DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED
            and o.sends_attempted == 0
            for o in replay_a.dispatch_outcomes
        )
        counts_after_replay_a = await _measure_table_counts(admin_conn)
        hashes_after_replay_a = await _measure_table_hashes(admin_conn)
        assert counts_after_replay_a == counts_after_run1
        assert hashes_after_replay_a == hashes_after_run1
        _, _, inc_end_after_replay_a = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        assert inc_end_after_replay_a == inc_end_after_run1
        committed_after_replay_a = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid_run1,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        assert committed_after_replay_a == committed_after_run1

        # ====================================================================
        # AC2 (FIX-02 / CW-07): Replay B — fresh uncommitted consumer group restart
        # ====================================================================
        gid_replay_b = v1_env.register_consumer_group("ac2-replay-uncommitted")
        await _preposition_consumer_group(
            v1_env.kafka_bootstrap,
            group_id=gid_replay_b,
            topic=KafkaTopic.ANOMALIES.value,
            offsets_by_partition=anom_end_before,
        )
        replay_b = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=dispatch_service,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid_replay_b,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
            dedupe_store=InMemoryAnomalyDedupeStore(max_size=1024),
        )
        assert replay_b.status is CorrelationRuntimeRunStatus.COMPLETED
        assert all(
            r.persistence_disposition
            is PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY
            for r in replay_b.revisions
        )
        assert all(
            o.status is DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED
            and o.sends_attempted == 0
            for o in replay_b.dispatch_outcomes
        )
        counts_after_replay_b = await _measure_table_counts(admin_conn)
        hashes_after_replay_b = await _measure_table_hashes(admin_conn)
        assert counts_after_replay_b == counts_after_run1
        assert hashes_after_replay_b == hashes_after_run1
        _, _, inc_end_after_replay_b = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        assert inc_end_after_replay_b == inc_end_after_run1
        committed_after_replay_b = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid_replay_b,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        assert committed_after_replay_b == committed_after_run1

        v1_env.evidence["gates"]["ac1_ac4_fix01"] = {
            "consumer_group_id": gid_run1,
            "startup_committed_offsets": startup_pos_run1,
            "final_committed_offsets": committed_after_run1,
            "plan_sha256": plan_sha256,
            "anomaly_broker_acks": anomaly_acks,
            "db_counts": counts_after_run1,
            "db_content_hashes": hashes_after_run1,
            "tenant_b_rls_rows_visible": 0,
            "logical_publications": 2,
            "physical_deliveries": physical_added_run1,
            "incident_wire_proofs": consumed_wire_proofs,
        }
        v1_env.evidence["gates"]["ac2_fix02_replay_restart"] = {
            "replay_a_history_reader": {
                "consumer_group_id": gid_run1,
                "status": replay_a.status.value,
                "dispatch_outcomes": [
                    o.status.value for o in replay_a.dispatch_outcomes
                ],
                "db_row_deltas": {
                    k: counts_after_replay_a[k] - counts_after_run1[k]
                    for k in SIX_TABLES
                },
                "db_hashes_identical": hashes_after_replay_a == hashes_after_run1,
                "additional_kafka_records": 0,
                "committed_offsets_unchanged": committed_after_replay_a
                == committed_after_run1,
            },
            "replay_b_uncommitted_group_restart": {
                "consumer_group_id": gid_replay_b,
                "status": replay_b.status.value,
                "dispatch_outcomes": [
                    o.status.value for o in replay_b.dispatch_outcomes
                ],
                "db_row_deltas": {
                    k: counts_after_replay_b[k] - counts_after_run1[k]
                    for k in SIX_TABLES
                },
                "db_hashes_identical": hashes_after_replay_b == hashes_after_run1,
                "additional_kafka_records": 0,
                "final_committed_offsets": committed_after_replay_b,
            },
        }
    finally:
        await inc_producer.close()
        await neo4j_provider.close()
        await admin_conn.close()


# ============================================================================
# TEST 4: AC3 CONTROLLED PUBLICATION FAILURE & BOUNDED RECOVERY (FIX-04)
# ============================================================================


async def test_v1_03_ac3_fix04_controlled_publication_failure_and_recovery(
    v1_env: _V1LiveEnvironment,
) -> None:
    """Verify controlled live publish failure preserves DB & pending work; recovery publishes exact event."""
    admin_conn = await asyncpg.connect(v1_env.admin_db_dsn)
    neo4j_provider = Neo4jTopologyProvider(**_neo4j_connection())
    # Attempt 1 producer intentionally NOT started -> real ProducerNotStartedError -> FAILED_UNAVAILABLE
    unstarted_producer = KafkaTelemetryProducer(
        bootstrap_servers=v1_env.kafka_bootstrap,
        client_id=f"aegisops-v1-fix04-unstarted-{v1_env.token}",
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    live_producer = KafkaTelemetryProducer(
        bootstrap_servers=v1_env.kafka_bootstrap,
        client_id=f"aegisops-v1-fix04-live-{v1_env.token}",
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    try:
        await _truncate_incident_tables(admin_conn)
        neo4j_snapshot = await neo4j_provider.get_snapshot(
            expected_version=PINNED_TOPOLOGY_VERSION
        )
        anom_parts, _, anom_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.ANOMALIES.value
        )
        inc_parts, _, inc_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )

        gid = v1_env.register_consumer_group("ac3-fix04")
        startup_pos = await _preposition_consumer_group(
            v1_env.kafka_bootstrap,
            group_id=gid,
            topic=KafkaTopic.ANOMALIES.value,
            offsets_by_partition=anom_end_before,
        )

        plan, plan_sha256, anom_acks = await _produce_real_anomalies_and_build_plan(
            v1_env.kafka_bootstrap,
            signal_specs=[("S1", _make_signal("S1"))],
            steps=[
                {
                    "delivery_id": "S1",
                    "logical_time": "2026-11-27T00:01:00Z",
                    "op": "ingest",
                },
                {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
            ],
            run_end_logical_time="2026-11-27T00:01:05Z",
        )

        # Attempt 1: Controlled publication failure (unstarted real KafkaTelemetryProducer)
        failing_dispatch = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=IncidentPublisher(
                unstarted_producer, publish_timeout_seconds=5.0
            ),
            config=DispatchConfig(
                worker_id=f"v1-fix04-attempt1-{v1_env.token}",
                max_dispatch_attempts=3,
                retry_backoff_initial_ms=200,
                retry_backoff_max_ms=1000,
                claim_lease_seconds=10,
                publish_timeout_seconds=5.0,
                lease_settlement_margin_seconds=2.0,
            ),
        )
        run_fail = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=failing_dispatch,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
        )

        assert (
            run_fail.status
            is CorrelationRuntimeRunStatus.COMPLETED_WITH_PENDING_DISPATCH
        )
        assert len(run_fail.dispatch_outcomes) >= 1
        assert all(
            o.status is DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION
            for o in run_fail.dispatch_outcomes
        )
        assert sum(o.sends_attempted for o in run_fail.dispatch_outcomes) == 1
        fail_outcome = run_fail.dispatch_outcomes[0]
        assert (
            fail_outcome.status is DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION
        )
        assert fail_outcome.attempt_number == 1
        assert fail_outcome.sends_attempted == 1
        assert fail_outcome.error_code == "PUBLISH_UNAVAILABLE"

        counts_after_fail = await _measure_table_counts(admin_conn)
        assert counts_after_fail == {
            "incidents": 1,
            "incident_signal_links": 1,
            "incident_correlation_evidence": 0,
            "incident_timeline_events": 1,
            "incident_publication_intents": 1,
            "incident_publication_dispatches": 1,
        }
        five_table_hashes_after_fail = await _measure_table_hashes(
            admin_conn, FIVE_INCIDENT_TABLES
        )
        drow_fail = await admin_conn.fetchrow(
            "SELECT * FROM incident_publication_dispatches WHERE tenant_id = $1 AND revision = 1",
            TENANT_A,
        )
        assert drow_fail is not None
        assert drow_fail["dispatch_state"] == DispatchState.RETRY_SCHEDULED.value
        assert int(drow_fail["attempt_count"]) == 1
        assert drow_fail["broker_offset"] is None
        assert drow_fail["next_retry_at"] is not None

        # Verify 0 records sent to aegis.incidents and input offset NOT committed
        _, _, inc_end_after_fail = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        assert inc_end_after_fail == inc_end_before
        committed_after_fail = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        assert committed_after_fail == startup_pos

        # Wait past bounded retry_backoff_initial_ms (200ms) for recovery eligibility
        await asyncio.sleep(0.5)

        # Attempt 2: Bounded recovery with real started KafkaTelemetryProducer
        await live_producer.start()
        recover_dispatch = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=IncidentPublisher(live_producer, publish_timeout_seconds=10.0),
            config=DispatchConfig(
                worker_id=f"v1-fix04-attempt2-{v1_env.token}",
                max_dispatch_attempts=3,
                retry_backoff_initial_ms=200,
                retry_backoff_max_ms=1000,
                claim_lease_seconds=15,
                publish_timeout_seconds=10.0,
                lease_settlement_margin_seconds=2.0,
            ),
        )
        run_recover = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=recover_dispatch,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
        )

        assert run_recover.status is CorrelationRuntimeRunStatus.COMPLETED
        assert len(run_recover.dispatch_outcomes) == 1
        rec_outcome = run_recover.dispatch_outcomes[0]
        assert rec_outcome.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert rec_outcome.attempt_number == 2
        assert rec_outcome.sends_attempted == 1
        assert rec_outcome.publish_result is not None

        # Verify 5 incident tables are byte-identical (0 extra rows)
        counts_after_recover = await _measure_table_counts(admin_conn)
        assert counts_after_recover == counts_after_fail
        assert (
            await _measure_table_hashes(admin_conn, FIVE_INCIDENT_TABLES)
            == five_table_hashes_after_fail
        )

        # Verify exactly 1 physical delivery on aegis.incidents matching FIX-01-REV1 & DB
        _, _, inc_end_after_recover = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        assert sum(inc_end_after_recover[p] - inc_end_before[p] for p in inc_parts) == 1

        drow_rec = await admin_conn.fetchrow(
            "SELECT * FROM incident_publication_dispatches WHERE tenant_id = $1 AND revision = 1",
            TENANT_A,
        )
        irow_rec = await admin_conn.fetchrow(
            "SELECT * FROM incident_publication_intents WHERE tenant_id = $1 AND revision = 1",
            TENANT_A,
        )
        assert drow_rec is not None and irow_rec is not None
        assert drow_rec["dispatch_state"] == DispatchState.ACKNOWLEDGED.value
        assert int(drow_rec["attempt_count"]) == 2
        assert int(drow_rec["broker_offset"]) == int(rec_outcome.publish_result.offset)

        raw_inc = await _consume_exact_record(
            v1_env.kafka_bootstrap,
            topic=drow_rec["broker_topic"],
            partition=int(drow_rec["broker_partition"]),
            offset=int(drow_rec["broker_offset"]),
        )
        wire_proof = await _verify_kafka_incident_record_against_oracle_and_db(
            raw_msg=raw_inc,
            oracle_key="FIX-01-REV1",
            neo4j_snapshot=neo4j_snapshot,
            db_dispatch_row=dict(drow_rec),
            db_intent_row=dict(irow_rec),
        )

        committed_after_recover = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        assert (
            committed_after_recover[anom_acks[0]["partition"]]
            == anom_acks[0]["offset"] + 1
        )

        v1_env.evidence["gates"]["ac3_fix04_failure_and_recovery"] = {
            "injected_fault": "INJECTED_FAULT_FIX04_UNSTARTED_REAL_PRODUCER",
            "consumer_group_id": gid,
            "anomaly_broker_ack": anom_acks[0],
            "attempt_1_failure": {
                "run_status": run_fail.status.value,
                "typed_outcome": fail_outcome.status.value,
                "error_code": fail_outcome.error_code,
                "dispatch_state": drow_fail["dispatch_state"],
                "attempt_count": int(drow_fail["attempt_count"]),
                "broker_offset_in_db": drow_fail["broker_offset"],
                "kafka_records_added": 0,
                "input_offset_committed": False,
                "db_counts": counts_after_fail,
            },
            "attempt_2_recovery": {
                "run_status": run_recover.status.value,
                "typed_outcome": rec_outcome.status.value,
                "dispatch_state": drow_rec["dispatch_state"],
                "attempt_count": int(drow_rec["attempt_count"]),
                "five_tables_unchanged": True,
                "logical_identities": 1,
                "physical_deliveries": 1,
                "final_committed_offsets": committed_after_recover,
                "wire_proof": wire_proof,
            },
        }
    finally:
        await live_producer.close()
        await neo4j_provider.close()
        await admin_conn.close()


# ============================================================================
# TEST 5: AC2/AC3 CW-06 POST-REAL-BROKER-SEND CRASH WINDOW (FIX-03)
# ============================================================================


async def test_v1_04_ac2_ac3_cw06_post_broker_send_crash_window_reconciliation(
    v1_env: _V1LiveEnvironment,
) -> None:
    """Verify CW-06 post-real-broker-send fault and 1-logical vs 2-physical reconciliation."""
    admin_conn = await asyncpg.connect(v1_env.admin_db_dsn)
    neo4j_provider = Neo4jTopologyProvider(**_neo4j_connection())
    inc_producer = KafkaTelemetryProducer(
        bootstrap_servers=v1_env.kafka_bootstrap,
        client_id=f"aegisops-v1-cw06-{v1_env.token}",
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    try:
        await _truncate_incident_tables(admin_conn)
        neo4j_snapshot = await neo4j_provider.get_snapshot(
            expected_version=PINNED_TOPOLOGY_VERSION
        )
        anom_parts, _, anom_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.ANOMALIES.value
        )
        inc_parts, _, inc_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )

        gid = v1_env.register_consumer_group("cw06-crash")
        startup_pos = await _preposition_consumer_group(
            v1_env.kafka_bootstrap,
            group_id=gid,
            topic=KafkaTopic.ANOMALIES.value,
            offsets_by_partition=anom_end_before,
        )

        plan, plan_sha256, anom_acks = await _produce_real_anomalies_and_build_plan(
            v1_env.kafka_bootstrap,
            signal_specs=[("S1", _make_signal("S1"))],
            steps=[
                {
                    "delivery_id": "S1",
                    "logical_time": "2026-11-27T00:01:00Z",
                    "op": "ingest",
                },
                {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
            ],
            run_end_logical_time="2026-11-27T00:01:05Z",
        )

        await inc_producer.start()
        real_publisher = IncidentPublisher(inc_producer, publish_timeout_seconds=2.0)
        fault_publisher = _PostRealBrokerSendCrashInjector(
            real_publisher, inject_once=True
        )

        # Short valid lease (5s >= 2.0s + 1.0s) so bounded recovery completes swiftly
        cw06_dispatch_cfg = DispatchConfig(
            worker_id=f"v1-cw06-run1-{v1_env.token}",
            max_dispatch_attempts=3,
            retry_backoff_initial_ms=200,
            retry_backoff_max_ms=1000,
            claim_lease_seconds=5,
            publish_timeout_seconds=2.0,
            lease_settlement_margin_seconds=1.0,
        )
        dispatch_run1 = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=fault_publisher,
            config=cw06_dispatch_cfg,
        )

        # Run 1: Real broker send succeeds (K1), injected post-send crash leaves ambiguous_outcome
        run1 = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=dispatch_run1,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
        )

        assert (
            run1.status is CorrelationRuntimeRunStatus.COMPLETED_WITH_PENDING_DISPATCH
        )
        assert len(run1.dispatch_outcomes) >= 1
        assert all(
            o.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
            for o in run1.dispatch_outcomes
        )
        assert sum(o.sends_attempted for o in run1.dispatch_outcomes) == 1
        assert (
            run1.dispatch_outcomes[0].status
            is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
        )
        assert len(fault_publisher.real_broker_acks) == 1
        k1_ack = fault_publisher.real_broker_acks[0]

        _, _, inc_end_after_k1 = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        assert sum(inc_end_after_k1[p] - inc_end_before[p] for p in inc_parts) == 1

        drow_ambig = await admin_conn.fetchrow(
            "SELECT * FROM incident_publication_dispatches WHERE tenant_id = $1 AND revision = 1",
            TENANT_A,
        )
        assert drow_ambig is not None
        assert drow_ambig["dispatch_state"] == DispatchState.AMBIGUOUS_OUTCOME.value
        assert int(drow_ambig["attempt_count"]) == 1
        assert drow_ambig["broker_offset"] is None
        five_hashes_before_recover = await _measure_table_hashes(
            admin_conn, FIVE_INCIDENT_TABLES
        )

        committed_after_k1 = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        assert committed_after_k1 == startup_pos

        # Wait for real PostgreSQL server clock to pass claim_expires_at (5s lease + margin)
        await asyncio.sleep(5.5)

        # Run 2: Bounded recovery through normal unwrapped real IncidentPublisher
        dispatch_run2 = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=real_publisher,
            config=DispatchConfig(
                worker_id=f"v1-cw06-run2-{v1_env.token}",
                max_dispatch_attempts=3,
                retry_backoff_initial_ms=200,
                retry_backoff_max_ms=1000,
                claim_lease_seconds=5,
                publish_timeout_seconds=2.0,
                lease_settlement_margin_seconds=1.0,
            ),
        )
        run2 = await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha256,
            session_factory=v1_env.runtime_session_factory,
            dispatch_service=dispatch_run2,
            topology_provider=neo4j_provider,
            expected_topology=_expected_canonical_topology_ref(),
            config=CorrelationRuntimeConfig(
                consumer_group_id=gid,
                max_run_duration_seconds=60,
                poll_timeout_ms=250,
                shutdown_grace_seconds=5.0,
            ),
            bootstrap_servers=v1_env.kafka_bootstrap,
        )

        assert run2.status is CorrelationRuntimeRunStatus.COMPLETED
        assert len(run2.dispatch_outcomes) == 1
        assert (
            run2.dispatch_outcomes[0].status
            is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        )
        assert run2.dispatch_outcomes[0].attempt_number == 2
        k2_ack = run2.dispatch_outcomes[0].publish_result
        assert k2_ack is not None
        assert k2_ack.partition == k1_ack.partition
        assert k2_ack.offset == k1_ack.offset + 1

        # Verify 5 incident tables untouched
        assert (
            await _measure_table_hashes(admin_conn, FIVE_INCIDENT_TABLES)
            == five_hashes_before_recover
        )

        # Physical vs Logical reconciliation on real Kafka aegis.incidents
        _, _, inc_end_after_k2 = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        physical_deliveries = sum(
            inc_end_after_k2[p] - inc_end_before[p] for p in inc_parts
        )
        assert physical_deliveries == 2

        drow_final = await admin_conn.fetchrow(
            "SELECT * FROM incident_publication_dispatches WHERE tenant_id = $1 AND revision = 1",
            TENANT_A,
        )
        irow_final = await admin_conn.fetchrow(
            "SELECT * FROM incident_publication_intents WHERE tenant_id = $1 AND revision = 1",
            TENANT_A,
        )
        assert drow_final is not None and irow_final is not None
        assert drow_final["dispatch_state"] == DispatchState.ACKNOWLEDGED.value
        assert int(drow_final["attempt_count"]) == 2
        assert int(drow_final["broker_offset"]) == int(k2_ack.offset)

        raw_k1 = await _consume_exact_record(
            v1_env.kafka_bootstrap,
            topic=k1_ack.topic,
            partition=int(k1_ack.partition),
            offset=int(k1_ack.offset),
        )
        raw_k2 = await _consume_exact_record(
            v1_env.kafka_bootstrap,
            topic=k2_ack.topic,
            partition=int(k2_ack.partition),
            offset=int(k2_ack.offset),
        )
        # Prove K1 and K2 are byte-for-byte identical in key, value, and ordered headers
        assert bytes(raw_k1.key) == bytes(raw_k2.key)
        assert bytes(raw_k1.value) == bytes(raw_k2.value)
        assert tuple(raw_k1.headers) == tuple(raw_k2.headers)
        assert int(raw_k2.offset) == int(raw_k1.offset) + 1

        k1_proof = await _verify_kafka_incident_record_against_oracle_and_db(
            raw_msg=raw_k1,
            oracle_key="FIX-01-REV1",
            neo4j_snapshot=neo4j_snapshot,
            db_dispatch_row=dict(drow_final),
            db_intent_row=dict(irow_final),
        )
        k2_proof = await _verify_kafka_incident_record_against_oracle_and_db(
            raw_msg=raw_k2,
            oracle_key="FIX-01-REV1",
            neo4j_snapshot=neo4j_snapshot,
            db_dispatch_row=dict(drow_final),
            db_intent_row=dict(irow_final),
        )
        assert k1_proof["event_id"] == k2_proof["event_id"]
        assert (
            k1_proof["publication_fingerprint"] == k2_proof["publication_fingerprint"]
        )

        committed_after_k2 = await _read_consumer_group_committed_offsets(
            v1_env.kafka_bootstrap,
            group_id=gid,
            topic=KafkaTopic.ANOMALIES.value,
            partitions=anom_parts,
        )
        assert (
            committed_after_k2[anom_acks[0]["partition"]] == anom_acks[0]["offset"] + 1
        )

        v1_env.evidence["gates"]["ac2_ac3_cw06_crash_window"] = {
            "injected_fault": _PostRealBrokerSendCrashInjector.FAULT_ID,
            "injected_events": fault_publisher.injected_events,
            "consumer_group_id": gid,
            "before_recovery": {
                "run_status": run1.status.value,
                "typed_outcome": run1.dispatch_outcomes[0].status.value,
                "dispatch_state": drow_ambig["dispatch_state"],
                "broker_offset_in_db": drow_ambig["broker_offset"],
                "input_offset_committed": False,
                "physical_records_on_broker": 1,
            },
            "after_recovery": {
                "run_status": run2.status.value,
                "typed_outcome": run2.dispatch_outcomes[0].status.value,
                "dispatch_state": drow_final["dispatch_state"],
                "attempt_count": int(drow_final["attempt_count"]),
                "authoritative_db_broker_offset": int(drow_final["broker_offset"]),
                "input_offset_committed": True,
                "final_committed_offsets": committed_after_k2,
                "logical_identities": 1,
                "physical_deliveries": physical_deliveries,
                "allowed_crash_window_duplicates": 1,
                "k1_k2_byte_identical": True,
                "k1_wire_proof": k1_proof,
                "k2_wire_proof": k2_proof,
            },
        }
    finally:
        await inc_producer.close()
        await neo4j_provider.close()
        await admin_conn.close()


# ============================================================================
# TEST 6: A1-R1 CW-11 / FIX-15 REAL-BROKER STALE-OWNER LATE COMPLETION TRACE
# ============================================================================


async def test_v1_05_a1_r1_cw11_real_broker_stale_owner_trace(
    v1_env: _V1LiveEnvironment,
) -> None:
    """Verify A1-R1 FIX-15 / CW-11 on real PostgreSQL + real Kafka (physical R1, R2, R1)."""
    admin_conn = await asyncpg.connect(v1_env.admin_db_dsn)
    neo4j_provider = Neo4jTopologyProvider(**_neo4j_connection())
    prod_a = KafkaTelemetryProducer(
        bootstrap_servers=v1_env.kafka_bootstrap,
        client_id=f"aegisops-v1-cw11-a-{v1_env.token}",
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    prod_b = KafkaTelemetryProducer(
        bootstrap_servers=v1_env.kafka_bootstrap,
        client_id=f"aegisops-v1-cw11-b-{v1_env.token}",
        producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
    )
    try:
        await _truncate_incident_tables(admin_conn)
        neo4j_snapshot = await neo4j_provider.get_snapshot(
            expected_version=PINNED_TOPOLOGY_VERSION
        )
        inc_parts, _, inc_end_before = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )

        # Persist rev 1 and rev 2 using production scorer, grouping, and persist_incident_atomic
        cfg = _pinned_correlation_config()
        s1_sig = _make_signal("S1")
        s2_sig = _make_signal("S2")
        s1_in = AnomalyCorrelationInput(
            signal=s1_sig, lineage=build_anomaly_reproducibility_lineage(s1_sig)
        )
        s2_in = AnomalyCorrelationInput(
            signal=s2_sig, lineage=build_anomaly_reproducibility_lineage(s2_sig)
        )
        g1 = group_candidate_signals(
            [s1_in],
            [],
            topology_snapshot=neo4j_snapshot,
            config=cfg,
            event_time_watermark=_utc("2026-11-27T00:01:05Z"),
        ).groups[0]
        p1 = await persist_incident_atomic(
            group=g1,
            signal_inputs={s1_sig.signal_id: s1_in},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_utc("2026-11-27T00:01:05Z"),
            tenant_id=TENANT_A,
            session_factory=v1_env.runtime_session_factory,
        )
        pair_ev = score_candidate_pair(
            s1_in, s2_in, topology_snapshot=neo4j_snapshot, config=cfg
        )
        g2 = group_candidate_signals(
            [s1_in, s2_in],
            [pair_ev],
            topology_snapshot=neo4j_snapshot,
            config=cfg,
            event_time_watermark=_utc("2026-11-27T00:01:05Z"),
        ).groups[0]
        p2 = await persist_incident_atomic(
            group=g2,
            signal_inputs={s1_sig.signal_id: s1_in, s2_sig.signal_id: s2_in},
            desired_state=CorrelationWindowState.SETTLING,
            processing_time=_utc("2026-11-27T00:01:25Z"),
            tenant_id=TENANT_A,
            session_factory=v1_env.runtime_session_factory,
            expected_revision=1,
            expected_revision_fingerprint=p1.aggregate.revision_fingerprint,
        )
        incident_id = p1.aggregate.incident_id
        assert p2.aggregate.revision == 2

        # Verify pure constructor guard remains enforced (A1-R1 B1 retention check)
        with pytest.raises(Exception, match="Existing correlation evidence"):
            create_or_update_incident(
                group=g1,
                signal_inputs={s1_sig.signal_id: s1_in},
                processing_time=_utc("2026-11-27T00:01:05Z"),
                desired_correlation_state=CorrelationWindowState.COLLECTING,
                existing_aggregate=p2.aggregate,
            )

        await prod_a.start()
        await prod_b.start()
        pub_a = IncidentPublisher(prod_a, publish_timeout_seconds=2.0)
        pub_b = IncidentPublisher(prod_b, publish_timeout_seconds=2.0)

        staging = DispatchStagingContext(
            snapshot=neo4j_snapshot,
            expected_topology=_expected_canonical_topology_ref(),
            blast_radius_policy=_pinned_blast_radius_policy(),
            correlation_config=cfg,
            execution_context=_pinned_execution_context(),
            producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
        )
        svc_a = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=pub_a,
            config=DispatchConfig(
                worker_id=f"v1-cw11-worker-a-{v1_env.token}",
                max_dispatch_attempts=3,
                claim_lease_seconds=5,
                publish_timeout_seconds=2.0,
                lease_settlement_margin_seconds=1.0,
            ),
        )
        svc_b = IncidentDispatchService(
            session_factory=v1_env.runtime_session_factory,
            publisher=pub_b,
            config=DispatchConfig(
                worker_id=f"v1-cw11-worker-b-{v1_env.token}",
                max_dispatch_attempts=3,
                claim_lease_seconds=5,
                publish_timeout_seconds=2.0,
                lease_settlement_margin_seconds=1.0,
            ),
        )

        # Stage rev 1 & rev 2; Worker A claims rev 1 and stalls before broker delivery
        await svc_a.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=staging,
        )
        await svc_a.stage_committed_intent(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=staging,
        )
        claim_a_r1 = await svc_a.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=1
        )
        assert claim_a_r1.claimed is True and claim_a_r1.attempt is not None

        # Before lease expiry, rev 2 is blocked by unacknowledged rev 1
        blocked_r2 = await svc_b.claim_revision(
            tenant_id=TENANT_A, incident_id=incident_id, revision=2
        )
        assert blocked_r2.claimed is False
        assert blocked_r2.outcome is not None
        assert blocked_r2.outcome.status is DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR

        # Wait for Worker A's 5s lease to expire on PostgreSQL server clock
        await asyncio.sleep(5.5)

        # Worker B reclaims rev 1, publishes to real Kafka (P1_rev1), records ACK
        out_b_r1 = await svc_b.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=1,
            staging_context=staging,
        )
        assert out_b_r1 is not None
        assert out_b_r1.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert out_b_r1.attempt_number == 2
        assert out_b_r1.publish_result is not None
        ack_b_r1 = out_b_r1.publish_result

        # Worker B now claims & publishes rev 2 to real Kafka (P2_rev2), records ACK
        out_b_r2 = await svc_b.dispatch_revision(
            tenant_id=TENANT_A,
            incident_id=incident_id,
            revision=2,
            staging_context=staging,
        )
        assert out_b_r2 is not None
        assert out_b_r2.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
        assert out_b_r2.attempt_number == 1
        assert out_b_r2.publish_result is not None
        ack_b_r2 = out_b_r2.publish_result

        hashes_before_stale_a = await _measure_table_hashes(admin_conn)

        # Worker A's delayed in-flight send now lands on real Kafka (P3_rev1_late -> physical order R1, R2, R1)
        late_pub_a = await pub_a.publish(claim_a_r1.attempt.record)
        assert late_pub_a.status is IncidentPublishStatus.ACKNOWLEDGED
        assert late_pub_a.result is not None
        ack_a_r1_late = late_pub_a.result

        # PostgreSQL token fencing MUST reject Worker A's stale completion with 0 DB mutations
        with pytest.raises(DispatchClaimLostError):
            await svc_a.complete_claimed_attempt(claim_a_r1.attempt, late_pub_a)
        assert await _measure_table_hashes(admin_conn) == hashes_before_stale_a

        # Verify physical broker log order [rev 1, rev 2, rev 1] and exact byte parity
        _, _, inc_end_after = await _fetch_topic_offsets(
            v1_env.kafka_bootstrap, KafkaTopic.INCIDENTS.value
        )
        assert sum(inc_end_after[p] - inc_end_before[p] for p in inc_parts) == 3
        assert ack_b_r1.partition == ack_b_r2.partition == ack_a_r1_late.partition
        assert ack_b_r1.offset < ack_b_r2.offset < ack_a_r1_late.offset

        raw_1 = await _consume_exact_record(
            v1_env.kafka_bootstrap,
            topic=ack_b_r1.topic,
            partition=int(ack_b_r1.partition),
            offset=int(ack_b_r1.offset),
        )
        raw_2 = await _consume_exact_record(
            v1_env.kafka_bootstrap,
            topic=ack_b_r2.topic,
            partition=int(ack_b_r2.partition),
            offset=int(ack_b_r2.offset),
        )
        raw_3 = await _consume_exact_record(
            v1_env.kafka_bootstrap,
            topic=ack_a_r1_late.topic,
            partition=int(ack_a_r1_late.partition),
            offset=int(ack_a_r1_late.offset),
        )
        assert bytes(raw_1.key) == bytes(raw_3.key)
        assert bytes(raw_1.value) == bytes(raw_3.value)
        assert tuple(raw_1.headers) == tuple(raw_3.headers)

        drows = await admin_conn.fetch(
            "SELECT * FROM incident_publication_dispatches WHERE tenant_id = $1 ORDER BY revision ASC",
            TENANT_A,
        )
        irows = await admin_conn.fetch(
            "SELECT * FROM incident_publication_intents WHERE tenant_id = $1 ORDER BY revision ASC",
            TENANT_A,
        )
        assert int(drows[0]["broker_offset"]) == int(ack_b_r1.offset)
        assert int(drows[1]["broker_offset"]) == int(ack_b_r2.offset)

        p1_proof = await _verify_kafka_incident_record_against_oracle_and_db(
            raw_msg=raw_1,
            oracle_key="FIX-01-REV1",
            neo4j_snapshot=neo4j_snapshot,
            db_dispatch_row=dict(drows[0]),
            db_intent_row=dict(irows[0]),
        )
        p2_proof = await _verify_kafka_incident_record_against_oracle_and_db(
            raw_msg=raw_2,
            oracle_key="FIX-01-REV2",
            neo4j_snapshot=neo4j_snapshot,
            db_dispatch_row=dict(drows[1]),
            db_intent_row=dict(irows[1]),
        )
        p3_proof = await _verify_kafka_incident_record_against_oracle_and_db(
            raw_msg=raw_3,
            oracle_key="FIX-01-REV1",
            neo4j_snapshot=neo4j_snapshot,
            db_dispatch_row=dict(drows[0]),
            db_intent_row=dict(irows[0]),
        )

        v1_env.evidence["gates"]["a1_r1_cw11_stale_owner_trace"] = {
            "first_durable_ack_revisions": [1, 2],
            "physical_revisions_in_broker_order": [1, 2, 1],
            "logical_identities": 2,
            "physical_deliveries": 3,
            "stale_completion_error": "DispatchClaimLostError",
            "stale_db_updates": 0,
            "authoritative_db_offsets": {
                "rev1": int(drows[0]["broker_offset"]),
                "rev2": int(drows[1]["broker_offset"]),
            },
            "physical_records": [p1_proof, p2_proof, p3_proof],
        }
    finally:
        await prod_a.close()
        await prod_b.close()
        await neo4j_provider.close()
        await admin_conn.close()


# ============================================================================
# TEST 7: FAILURE-PATH CLEANUP GUARANTEE FOR DECLARED DISPOSABLE RESOURCES
# ============================================================================


async def test_v1_06_failure_cleanup_removes_declared_disposable_resources(
    v1_env: _V1LiveEnvironment,
) -> None:
    """Prove declared disposable PG/Kafka/Neo4j resources are removed even after a mid-run failure."""
    probe_tok = secrets.token_hex(4)
    probe_db = f"v1_failclean_{probe_tok}"
    probe_role = f"v1_failrole_{probe_tok}"
    probe_gid = f"aegisops-v1-failclean-{probe_tok}"
    probe_topo_id = str(uuid5(NAMESPACE_DNS, f"aegisops.v1.failclean.{probe_tok}"))
    probe_svc_id = str(uuid5(NAMESPACE_DNS, f"aegisops.v1.failclean.svc.{probe_tok}"))

    maint_conn = await asyncpg.connect(v1_env.admin_dsn)
    neo_cfg = _neo4j_connection()
    neo_driver = AsyncGraphDatabase.driver(
        neo_cfg["uri"], auth=(neo_cfg["username"], neo_cfg["password"])
    )
    caught_injected_failure = False
    try:
        try:
            # Create declared disposable PG DB + role, Kafka consumer group, and Neo4j node
            await maint_conn.execute(f'CREATE DATABASE "{probe_db}"')
            await maint_conn.execute(
                f"CREATE ROLE \"{probe_role}\" LOGIN PASSWORD 'probe_pw_{probe_tok}' "
                "NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
            )
            _, _, anom_end = await _fetch_topic_offsets(
                v1_env.kafka_bootstrap, KafkaTopic.ANOMALIES.value
            )
            await _preposition_consumer_group(
                v1_env.kafka_bootstrap,
                group_id=probe_gid,
                topic=KafkaTopic.ANOMALIES.value,
                offsets_by_partition=anom_end,
            )
            async with neo_driver.session(database=neo_cfg["database"]) as session:
                await (
                    await session.run(
                        "CREATE (:Service {service_id: $sid, name: 'v1-failclean-svc', "
                        "service_type: 'api', topology_id: $tid, topology_version: '0.0.1-fail'})",
                        {"sid": probe_svc_id, "tid": probe_topo_id},
                    )
                ).consume()

            # Deliberately trigger an injected failure before normal completion
            raise RuntimeError("INJECTED_FAULT_FAILURE_CLEANUP_PROBE")
        except RuntimeError as exc:
            assert str(exc) == "INJECTED_FAULT_FAILURE_CLEANUP_PROBE"
            caught_injected_failure = True
        finally:
            await _delete_consumer_groups_best_effort(
                v1_env.kafka_bootstrap, [probe_gid]
            )
            async with neo_driver.session(database=neo_cfg["database"]) as session:
                await (
                    await session.run(
                        "MATCH (s:Service) WHERE s.topology_id = $tid DETACH DELETE s",
                        {"tid": probe_topo_id},
                    )
                ).consume()
            await maint_conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                probe_db,
            )
            await maint_conn.execute(f'DROP DATABASE IF EXISTS "{probe_db}"')
            await maint_conn.execute(f'DROP ROLE IF EXISTS "{probe_role}"')

        assert caught_injected_failure is True

        # Independently verify all 4 declared failure-probe resources are gone
        db_exists = await maint_conn.fetchval(
            "SELECT count(*) FROM pg_database WHERE datname = $1", probe_db
        )
        role_exists = await maint_conn.fetchval(
            "SELECT count(*) FROM pg_roles WHERE rolname = $1", probe_role
        )
        assert int(db_exists) == 0
        assert int(role_exists) == 0

        admin = AIOKafkaAdminClient(bootstrap_servers=v1_env.kafka_bootstrap)
        await admin.start()
        try:
            groups = {str(g[0]) for g in await admin.list_consumer_groups()}
        finally:
            await admin.close()
        assert probe_gid not in groups

        async with neo_driver.session(database=neo_cfg["database"]) as session:
            neo_rem = await (
                await session.run(
                    "MATCH (s:Service) WHERE s.topology_id = $tid RETURN count(s) AS c",
                    {"tid": probe_topo_id},
                )
            ).single()
            assert neo_rem is not None and int(neo_rem["c"]) == 0

        v1_env.evidence["gates"]["failure_cleanup_probe"] = {
            "injected_fault": "INJECTED_FAULT_FAILURE_CLEANUP_PROBE",
            "probe_db": probe_db,
            "probe_role": probe_role,
            "probe_consumer_group": probe_gid,
            "probe_neo4j_topology_id": probe_topo_id,
            "remaining_pg_db": 0,
            "remaining_pg_role": 0,
            "remaining_kafka_group": False,
            "remaining_neo4j_nodes": 0,
        }
    finally:
        await neo_driver.close()
        await maint_conn.close()
