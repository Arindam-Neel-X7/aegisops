"""Task 4.12-RT1 unit tests: controlled correlation runtime coordinator.

Exercises ``CorrelationRuntimeCoordinator``, ``run_controlled_correlation_runtime``,
``check_correlation_runtime_readiness``, ``PartitionOffsetTracker``, and
``parse_and_authenticate_runtime_plan`` against approved A1-R1, C1-R1, P1, and
D1-R2 contracts using the real production scorer, grouping, aggregation,
``persist_incident_atomic``, ``IncidentDispatchService``, and C1 wire contract
without starting live Kafka, PostgreSQL, or Neo4j or activating Phase 5.
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Sequence
from contextlib import asynccontextmanager
import copy
from dataclasses import dataclass, replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import time
from typing import Any
from uuid import UUID, uuid5

import aiokafka
from aiokafka import TopicPartition
import pytest

from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import AnomalySignal
from app.anomaly.serialization import to_canonical_telemetry_event
from app.correlation import (
    aggregate as aggregate_mod,
    grouping as grouping_mod,
    persistence_service as persistence_service_mod,
    scoring as scoring_mod,
)
from app.correlation.blast_radius import BlastRadiusPolicy
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
)
from app.correlation.consumer import (
    AnomalyCorrelationInput,
    InMemoryAnomalyDedupeStore,
)
from app.correlation.dispatch_repository import DispatchState
from app.correlation.dispatch_service import (
    DispatchConfig,
    DispatchOutcomeStatus,
    IncidentDispatchService,
)
from app.correlation.errors import (
    AggregateStateError,
    GroupingInputError,
    IncidentPersistenceStoreError,
)
from app.correlation.models import (
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentSignalReference,
    IncidentTimelineEvent,
    TopologySnapshotReference,
)
from app.correlation.persistence_repository import (
    IncidentVersion,
    PersistedIncidentState,
    PersistedPublicationIntent,
)
from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    IncidentPublicationRecord,
    IncidentPublicationSizeLimitError,
    validate_incident_publication,
)
from app.correlation.publisher import (
    IncidentPublishAmbiguousError,
    IncidentPublishOutcome,
    IncidentPublishRetriableError,
    IncidentPublishStatus,
)
from app.correlation.runtime import (
    CorrelationRuntimeConfig,
    CorrelationRuntimeCoordinator,
    CorrelationRuntimeRunStatus,
    CorrelationRuntimeState,
    DeliverySettlementState,
    PartitionOffsetTracker,
    PersistenceDisposition,
    RuntimeCapacityError,
    RuntimeConfigurationError,
    RuntimeInputValidationError,
    RuntimeOffsetCommitError,
    RuntimePersistenceError,
    RuntimeRebalanceError,
    RuntimeRecoveryInputError,
    RuntimeStartupError,
    check_correlation_runtime_readiness,
    compute_runtime_plan_sha256,
    compute_source_record_digests,
    parse_and_authenticate_runtime_plan,
    run_controlled_correlation_runtime,
)
from app.correlation import runtime as runtime_mod
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    serialize_incident_aggregate,
)
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.telemetry.reliability.quarantine import QuarantineRecord
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.producer import PublishResult
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)
from tests.test_correlation_dispatch import (
    FakeClock,
    FakeSession,
    InMemoryDispatchStore,
    InMemoryDispatchRepository,
)
from tests.test_correlation_publication import (
    PINNED_BLAST_RADIUS_POLICY,
    PINNED_CORRELATION_CONFIG,
    PINNED_EXECUTION_CONTEXT,
    PINNED_SIGNALS,
    PINNED_TOPOLOGY_CAPTURED_AT,
    PINNED_TOPOLOGY_FINGERPRINT,
    PINNED_TOPOLOGY_ID,
    PINNED_TOPOLOGY_VERSION,
    WIRE_ORACLES,
)

PINNED_FIX01_PLAN_SHA256 = (
    "1e29b4ea1370fc5ec59cc4c92ec82943bfaced3c099f54613b96fefb8955b3d4"
)
TENANT_A = UUID("1dfc0469-7644-5efe-8b07-d1c7c31254f5")
TENANT_B = UUID("d8526eee-5633-5644-b721-9d5a678684dc")
ANOMALY_PRODUCER_VERSION = "phase4-anomaly-publisher/1.0"


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


# ============================================================================
# IN-MEMORY CONTROLLABLE HARNESS BACKING REAL PRODUCTION FUNCTIONS
# ============================================================================


@dataclass(frozen=True)
class FakeConsumerRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes], ...] | None
    timestamp: int = 1_795_737_660_000


def _make_anomaly_kafka_record(
    signal: AnomalySignal,
    *,
    partition: int,
    offset: int,
    ctx: TelemetryExecutionContext | None = None,
    producer_version: str | None = ANOMALY_PRODUCER_VERSION,
) -> tuple[FakeConsumerRecord, AnomalyCorrelationInput]:
    event = to_canonical_telemetry_event(signal)
    exec_ctx = ctx or TelemetryExecutionContext(
        run_id=signal.run_id,
        scenario_id=signal.scenario_id,
        scenario_version=signal.scenario_version,
        reproducibility_key=signal.reproducibility_key,
        seed=signal.seed,
    )
    key = construct_kafka_key(event)
    value = serialize_event(event)
    headers = tuple(construct_kafka_headers(exec_ctx, producer_version))
    rec = FakeConsumerRecord(
        topic=KafkaTopic.ANOMALIES.value,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        headers=headers,
    )
    corr_input = AnomalyCorrelationInput(
        signal=signal,
        lineage=build_anomaly_reproducibility_lineage(signal),
    )
    return rec, corr_input


def _source_entry(
    delivery_id: str,
    record: FakeConsumerRecord,
    corr_input: AnomalyCorrelationInput,
) -> dict[str, Any]:
    digests = compute_source_record_digests(record)
    return {
        "delivery_id": delivery_id,
        "headers_sha256": digests.headers_sha256,
        "key_sha256": digests.key_sha256,
        "offset": record.offset,
        "partition": record.partition,
        "semantic_fingerprint": corr_input.semantic_fingerprint,
        "signal_id": str(corr_input.signal.signal_id),
        "topic": record.topic,
        "value_sha256": digests.value_sha256,
    }


def _build_plan_dict(
    *,
    source_records: list[dict[str, Any]],
    steps: list[dict[str, Any]],
    run_end_logical_time: str,
    config_dict: dict[str, Any] | None = None,
    blast_policy_dict: dict[str, Any] | None = None,
    exec_context_dict: dict[str, Any] | None = None,
    topology_ref_dict: dict[str, Any] | None = None,
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION,
    max_dispatch_attempts: int = 3,
) -> tuple[dict[str, Any], str]:
    cfg_raw = copy.deepcopy(config_dict or PINNED_CORRELATION_CONFIG)
    cfg = CorrelationConfig.model_validate_json(json.dumps(cfg_raw))
    cfg_fp = compute_correlation_config_fingerprint(cfg)
    ctx_raw = copy.deepcopy(exec_context_dict or PINNED_EXECUTION_CONTEXT)
    bp_raw = copy.deepcopy(blast_policy_dict or PINNED_BLAST_RADIUS_POLICY)
    topo_raw = copy.deepcopy(
        topology_ref_dict
        or {
            "topology_captured_at": PINNED_TOPOLOGY_CAPTURED_AT,
            "topology_id": PINNED_TOPOLOGY_ID,
            "topology_snapshot_fingerprint": PINNED_TOPOLOGY_FINGERPRINT,
            "topology_version": PINNED_TOPOLOGY_VERSION,
        }
    )
    plan: dict[str, Any] = {
        "blast_radius_policy": bp_raw,
        "config": cfg_raw,
        "config_sha256": cfg_fp,
        "execution_context": ctx_raw,
        "max_dispatch_attempts": max_dispatch_attempts,
        "producer_version": producer_version,
        "run_end_logical_time": run_end_logical_time,
        "run_id": ctx_raw["run_id"],
        "schema_version": "controlled-correlation-runtime/1.0",
        "source_records": source_records,
        "steps": steps,
        "topology_reference": topo_raw,
    }
    return plan, compute_runtime_plan_sha256(plan)


def _pinned_topology_ref() -> TopologySnapshotReference:
    return TopologySnapshotReference(
        topology_id=UUID(PINNED_TOPOLOGY_ID),
        topology_version=PINNED_TOPOLOGY_VERSION,
        snapshot_fingerprint=PINNED_TOPOLOGY_FINGERPRINT,
        captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
        provider="canonical",
    )


class InMemorySharedDatabase:
    """Tenant-isolated transactional in-memory store backing BOTH 5-table persistence and D1 dispatch."""

    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock or FakeClock()
        self.dispatch_store = InMemoryDispatchStore(self.clock)
        self.incidents: dict[tuple[UUID, UUID], IncidentAggregate] = {}
        self.signal_links: dict[tuple[UUID, UUID], list[IncidentSignalReference]] = {}
        self.evidence: dict[tuple[UUID, UUID], list[CorrelationEvidence]] = {}
        self.timelines: dict[tuple[UUID, UUID], list[IncidentTimelineEvent]] = {}
        self.intents: dict[tuple[UUID, UUID, int], PersistedPublicationIntent] = (
            self.dispatch_store.intents
        )
        self.incident_write_ops: int = 0
        self.fail_next_persist_with: Exception | None = None
        self.call_log: list[str] = []

    def row_counts(self, tenant_id: UUID | None = None) -> dict[str, int]:
        def _match(tid: UUID) -> bool:
            return tenant_id is None or tid == tenant_id

        return {
            "incidents": sum(1 for (t, _) in self.incidents if _match(t)),
            "incident_signal_links": sum(
                len(v) for (t, _), v in self.signal_links.items() if _match(t)
            ),
            "incident_correlation_evidence": sum(
                len(v) for (t, _), v in self.evidence.items() if _match(t)
            ),
            "incident_timeline_events": sum(
                len(v) for (t, _), v in self.timelines.items() if _match(t)
            ),
            "incident_publication_intents": sum(
                1 for (t, _, _) in self.intents if _match(t)
            ),
            "incident_publication_dispatches": sum(
                1 for (t, _, _) in self.dispatch_store.dispatches if _match(t)
            ),
        }

    def session_factory(self) -> Any:
        @asynccontextmanager
        async def _ctx() -> Any:
            yield FakeSession()

        return _ctx()

    def make_persistence_repo(
        self, session: Any, tenant_id: UUID
    ) -> InMemoryPersistenceRepository:
        return InMemoryPersistenceRepository(self, tenant_id)


class InMemoryPersistenceRepository:
    """Faithful in-memory implementation of ``IncidentPersistenceRepository`` for real ``persist_incident_atomic``."""

    def __init__(self, db: InMemorySharedDatabase, tenant_id: UUID) -> None:
        self._db = db
        self._tenant_id = tenant_id
        self._bound = False

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    async def bind_tenant_context(self) -> None:
        self._bound = True

    async def resolve_incident_ids(
        self, signal_ids: Collection[UUID]
    ) -> tuple[UUID, ...]:
        assert self._bound
        wanted = set(signal_ids)
        found: set[UUID] = set()
        for (tid, inc_id), links in self._db.signal_links.items():
            if tid != self._tenant_id:
                continue
            if any(link.signal_id in wanted for link in links):
                found.add(inc_id)
        return tuple(sorted(found, key=lambda u: u.bytes))

    async def load_incident(
        self, incident_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        assert self._bound
        agg = self._db.incidents.get((self._tenant_id, incident_id))
        if agg is None:
            return None
        tl = tuple(self._db.timelines.get((self._tenant_id, incident_id), ()))
        intents = tuple(
            sorted(
                (
                    v
                    for (t, iid, _), v in self._db.intents.items()
                    if t == self._tenant_id and iid == incident_id
                ),
                key=lambda x: x.revision,
            )
        )
        return PersistedIncidentState(
            aggregate=agg,
            timeline_events=tl,
            publication_intents=intents,
        )

    async def load_incident_version(
        self, incident_id: UUID, *, for_update: bool = False
    ) -> IncidentVersion | None:
        assert self._bound
        agg = self._db.incidents.get((self._tenant_id, incident_id))
        if agg is None:
            return None
        return IncidentVersion(
            revision=agg.revision,
            revision_fingerprint=agg.revision_fingerprint,
        )

    async def insert_incident(self, aggregate: IncidentAggregate) -> None:
        assert self._bound
        if self._db.fail_next_persist_with is not None:
            exc = self._db.fail_next_persist_with
            self._db.fail_next_persist_with = None
            raise exc
        key = (self._tenant_id, aggregate.incident_id)
        self._db.incidents[key] = deserialize_incident_aggregate(
            serialize_incident_aggregate(aggregate)
        )
        self._db.incident_write_ops += 1

    async def append_signal_links(
        self, incident_id: UUID, links: Sequence[IncidentSignalReference]
    ) -> None:
        assert self._bound
        key = (self._tenant_id, incident_id)
        self._db.signal_links.setdefault(key, []).extend(links)
        self._db.incident_write_ops += len(links)

    async def append_correlation_evidence(
        self, incident_id: UUID, evidence: Sequence[CorrelationEvidence]
    ) -> None:
        assert self._bound
        key = (self._tenant_id, incident_id)
        self._db.evidence.setdefault(key, []).extend(evidence)
        self._db.incident_write_ops += len(evidence)

    async def append_timeline_events(
        self, events: Sequence[IncidentTimelineEvent]
    ) -> None:
        assert self._bound
        for ev in events:
            key = (self._tenant_id, ev.incident_id)
            self._db.timelines.setdefault(key, []).append(ev)
        self._db.incident_write_ops += len(events)

    async def append_publication_intent(
        self, aggregate: IncidentAggregate, *, intent_time: datetime
    ) -> None:
        assert self._bound
        intent = PersistedPublicationIntent(
            tenant_id=self._tenant_id,
            incident_id=aggregate.incident_id,
            revision=aggregate.revision,
            schema_version=aggregate.schema_version,
            revision_fingerprint=aggregate.revision_fingerprint,
            intent_time=intent_time,
            aggregate=deserialize_incident_aggregate(
                serialize_incident_aggregate(aggregate)
            ),
        )
        self._db.intents[
            (self._tenant_id, aggregate.incident_id, aggregate.revision)
        ] = intent
        self._db.incident_write_ops += 1

    async def update_incident_cas(
        self,
        aggregate: IncidentAggregate,
        *,
        expected_revision: int,
        expected_revision_fingerprint: str,
    ) -> bool:
        assert self._bound
        key = (self._tenant_id, aggregate.incident_id)
        current = self._db.incidents.get(key)
        if (
            current is None
            or current.revision != expected_revision
            or current.revision_fingerprint != expected_revision_fingerprint
        ):
            return False
        self._db.incidents[key] = deserialize_incident_aggregate(
            serialize_incident_aggregate(aggregate)
        )
        self._db.incident_write_ops += 1
        return True

    async def load_publication_intent(
        self, incident_id: UUID, revision: int
    ) -> PersistedPublicationIntent | None:
        assert self._bound
        return self._db.intents.get((self._tenant_id, incident_id, revision))


class FakeKafkaConsumer:
    """Controllable async consumer double supporting batches, commits, seeks, and fault injection."""

    def __init__(
        self,
        *,
        batches: Sequence[Sequence[FakeConsumerRecord]],
        assigned_partitions: Sequence[TopicPartition],
        initial_committed: dict[TopicPartition, int] | None = None,
        topics: set[str] | None = None,
        commit_error: Exception | None = None,
        fail_commit_once: bool = False,
        on_poll: Any | None = None,
        on_commit: Any | None = None,
        beginning: dict[TopicPartition, int] | None = None,
        ending: dict[TopicPartition, int] | None = None,
        cancel_on_poll_index: int | None = None,
    ) -> None:
        self._batches = [list(b) for b in batches]
        self._poll_idx = 0
        self._assigned = set(assigned_partitions)
        self.committed_positions: dict[TopicPartition, int] = dict(
            initial_committed or {tp: 0 for tp in assigned_partitions}
        )
        self.commit_history: list[dict[TopicPartition, int]] = []
        self._topics = topics or {
            KafkaTopic.ANOMALIES.value,
            KafkaTopic.INCIDENTS.value,
        }
        self._commit_error = commit_error
        self._fail_commit_once = fail_commit_once
        self._on_poll = on_poll
        self._on_commit = on_commit
        self._beginning = beginning
        self._ending = ending
        self._cancel_on_poll_index = cancel_on_poll_index
        self.started = False
        self.stopped = False
        self.seek_history: list[tuple[TopicPartition, int]] = []

    @property
    def exhausted(self) -> bool:
        return self._poll_idx >= len(self._batches)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def topics(self) -> set[str]:
        return set(self._topics)

    def assignment(self) -> set[TopicPartition]:
        return set(self._assigned)

    def assign(self, partitions: Sequence[TopicPartition]) -> None:
        self._assigned = set(partitions)

    def seek(self, partition: TopicPartition, offset: int) -> None:
        self.seek_history.append((partition, offset))

    async def committed(self, partition: TopicPartition) -> int | None:
        return self.committed_positions.get(partition)

    async def beginning_offsets(
        self, partitions: Sequence[TopicPartition]
    ) -> dict[TopicPartition, int]:
        if self._beginning is not None:
            return {tp: self._beginning.get(tp, 0) for tp in partitions}
        return {tp: 0 for tp in partitions}

    async def end_offsets(
        self, partitions: Sequence[TopicPartition]
    ) -> dict[TopicPartition, int]:
        if self._ending is not None:
            return {tp: self._ending[tp] for tp in partitions if tp in self._ending}
        out: dict[TopicPartition, int] = {}
        for tp in partitions:
            offs = [
                r.offset
                for b in self._batches
                for r in b
                if r.topic == tp.topic and r.partition == tp.partition
            ]
            out[tp] = (max(offs) + 1) if offs else 0
        return out

    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[FakeConsumerRecord]]:
        idx = self._poll_idx
        if self._cancel_on_poll_index is not None and idx == self._cancel_on_poll_index:
            raise asyncio.CancelledError("injected poll cancellation")
        if self._on_poll is not None:
            self._on_poll(idx)
        if self._poll_idx >= len(self._batches):
            await asyncio.sleep(0)
            return {}
        batch = self._batches[self._poll_idx]
        self._poll_idx += 1
        grouped: dict[TopicPartition, list[FakeConsumerRecord]] = {}
        for rec in batch:
            tp = TopicPartition(rec.topic, rec.partition)
            if partitions and tp not in partitions:
                continue
            grouped.setdefault(tp, []).append(rec)
        return grouped

    async def commit(self, offsets: dict[TopicPartition, int] | None = None) -> None:
        assert offsets is not None
        if self._commit_error is not None:
            err = self._commit_error
            if self._fail_commit_once:
                self._commit_error = None
            raise err
        for tp, pos in offsets.items():
            self.committed_positions[tp] = pos
        self.commit_history.append(dict(offsets))
        if self._on_commit is not None:
            self._on_commit(dict(offsets))


class FakeHistoryReader:
    """Uncommitted history reader double proving history reads never invoke ``commit()``."""

    def __init__(
        self, records_by_coord: dict[tuple[str, int, int], FakeConsumerRecord]
    ) -> None:
        self._records = dict(records_by_coord)
        self._assigned: list[TopicPartition] = []
        self._seek_offset: dict[TopicPartition, int] = {}
        self.started = False
        self.stopped = False
        self.commit_calls = 0
        self.read_coords: list[tuple[str, int, int]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def assign(self, partitions: Sequence[TopicPartition]) -> None:
        self._assigned = list(partitions)

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self._seek_offset[tp] = offset

    async def beginning_offsets(
        self, partitions: Sequence[TopicPartition]
    ) -> dict[TopicPartition, int]:
        return {tp: 0 for tp in partitions}

    async def end_offsets(
        self, partitions: Sequence[TopicPartition]
    ) -> dict[TopicPartition, int]:
        out: dict[TopicPartition, int] = {}
        for tp in partitions:
            offs = [
                off
                for (t, p, off) in self._records
                if t == tp.topic and p == tp.partition
            ]
            out[tp] = (max(offs) + 1) if offs else 0
        return out

    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[FakeConsumerRecord]]:
        targets = list(partitions) if partitions else self._assigned
        out: dict[TopicPartition, list[FakeConsumerRecord]] = {}
        for tp in targets:
            start_off = self._seek_offset.get(tp, 0)
            matched = [
                self._records[k]
                for k in sorted(self._records)
                if k[0] == tp.topic and k[1] == tp.partition and k[2] >= start_off
            ]
            if matched:
                out[tp] = matched
                self._seek_offset[tp] = matched[-1].offset + 1
                for r in matched:
                    self.read_coords.append((r.topic, r.partition, r.offset))
        return out

    async def commit(self, offsets: Any = None) -> None:
        self.commit_calls += 1
        raise AssertionError("History reader must NEVER commit group offsets")


class RecordingPublisher:
    """Controllable ``IncidentPublishPort`` recording exact published wire records & call events."""

    def __init__(
        self,
        *,
        outcomes: Sequence[IncidentPublishOutcome | BaseException] | None = None,
        on_publish: Any | None = None,
    ) -> None:
        self._scripted = list(outcomes or ())
        self._on_publish = on_publish
        self.published: list[IncidentPublicationRecord] = []
        self._next_offset = 100

    async def publish(
        self, record: IncidentPublicationRecord
    ) -> IncidentPublishOutcome:
        if self._on_publish is not None:
            self._on_publish(record)
        self.published.append(record)
        if self._scripted:
            nxt = self._scripted.pop(0)
            if isinstance(nxt, BaseException):
                raise nxt
            return nxt
        off = self._next_offset
        self._next_offset += 1
        return IncidentPublishOutcome(
            IncidentPublishStatus.ACKNOWLEDGED,
            result=PublishResult(
                topic=KafkaTopic.INCIDENTS.value,
                partition=0,
                offset=off,
                timestamp_ms=1_795_737_665_000 + off,
                latency_ms=1.5,
                serialized_bytes=len(record.kafka_value),
            ),
        )


class RecordingQuarantineSink:
    def __init__(self) -> None:
        self.records: list[QuarantineRecord] = []

    async def quarantine(self, record: QuarantineRecord) -> None:
        self.records.append(record)


async def _noop_verify_role(session: Any) -> None:
    return None


@pytest.fixture
def patched_persistence(monkeypatch: pytest.MonkeyPatch) -> InMemorySharedDatabase:
    """Wire real ``persist_incident_atomic`` to a faithful in-memory tenant-isolated store."""
    db = InMemorySharedDatabase()
    monkeypatch.setattr(
        persistence_service_mod,
        "SqlAlchemyIncidentPersistenceRepository",
        db.make_persistence_repo,
    )
    monkeypatch.setattr(
        persistence_service_mod,
        "_verify_role_and_isolation",
        _noop_verify_role,
    )
    return db


def _make_fix01_materials() -> (
    tuple[
        FakeConsumerRecord,
        FakeConsumerRecord,
        dict[str, Any],
        str,
    ]
):
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    s2 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S2"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    rec2, inp2 = _make_anomaly_kafka_record(s2, partition=1, offset=20)
    plan, plan_sha = _build_plan_dict(
        source_records=[
            _source_entry("S1", rec1, inp1),
            _source_entry("S2", rec2, inp2),
        ],
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
    return rec1, rec2, plan, plan_sha


def _make_dispatch_service(
    db: InMemorySharedDatabase,
    publisher: RecordingPublisher,
    *,
    max_dispatch_attempts: int = 3,
    retry_backoff_initial_ms: int = 250,
    retry_backoff_max_ms: int = 5000,
) -> IncidentDispatchService:
    return IncidentDispatchService(
        session_factory=db.session_factory,
        publisher=publisher,
        config=DispatchConfig(
            max_dispatch_attempts=max_dispatch_attempts,
            retry_backoff_initial_ms=retry_backoff_initial_ms,
            retry_backoff_max_ms=retry_backoff_max_ms,
        ),
        repository_factory=db.dispatch_store.make_repo,
        verify_role_fn=_noop_verify_role,
    )


# ============================================================================
# 1. CONFIGURATION & AUTHENTICATED PLAN VALIDATION (FIX-14 / FIX-16)
# ============================================================================


def test_runtime_config_validates_bounds_and_timing_coherence() -> None:
    cfg = CorrelationRuntimeConfig()
    assert cfg.max_window_members == 91
    assert cfg.claim_lease_seconds >= (
        cfg.publish_timeout_seconds + cfg.lease_settlement_margin_seconds
    )
    dcfg = cfg.to_dispatch_config()
    assert dcfg.claim_lease_seconds == cfg.claim_lease_seconds

    # FIX-16 invalid timing pair: 5s lease < 60s timeout + 5s margin
    with pytest.raises(RuntimeConfigurationError, match="claim_lease_seconds"):
        CorrelationRuntimeConfig(
            claim_lease_seconds=5,
            publish_timeout_seconds=60.0,
            lease_settlement_margin_seconds=5.0,
        )

    # Booleans and non-finite floats must reject
    for kwargs in [
        {"max_input_records": True},
        {"max_window_members": 92},
        {"publish_timeout_seconds": float("nan")},
        {"lease_settlement_margin_seconds": False},
        {"retry_backoff_initial_ms": 5000, "retry_backoff_max_ms": 1000},
        {"worker_id": "  padded  "},
    ]:
        with pytest.raises(RuntimeConfigurationError):
            CorrelationRuntimeConfig(**kwargs)  # type: ignore[arg-type]


def test_fix01_plan_reproduces_exact_a1_r1_plan_sha256_and_rejects_malformed() -> None:
    _, _, plan, plan_sha = _make_fix01_materials()
    assert plan_sha == PINNED_FIX01_PLAN_SHA256

    parsed = parse_and_authenticate_runtime_plan(plan, expected_plan_sha256=plan_sha)
    assert parsed.plan_sha256 == PINNED_FIX01_PLAN_SHA256
    assert len(parsed.source_records) == 2
    assert len(parsed.steps) == 4

    # Missing plan
    with pytest.raises(RuntimeRecoveryInputError) as exc_missing:
        parse_and_authenticate_runtime_plan(None, expected_plan_sha256=plan_sha)
    assert exc_missing.value.pointer == "/plan"

    # Plan hash drift
    drifted = copy.deepcopy(plan)
    drifted["producer_version"] = "changed/1.0"
    with pytest.raises(RuntimeRecoveryInputError) as exc_drift:
        parse_and_authenticate_runtime_plan(drifted, expected_plan_sha256=plan_sha)
    assert exc_drift.value.pointer == "/plan_sha256"

    # Duplicate JSON keys rejected
    raw_dup = (
        b'{"schema_version":"controlled-correlation-runtime/1.0","schema_version":"x"}'
    )
    with pytest.raises(RuntimeRecoveryInputError):
        parse_and_authenticate_runtime_plan(
            raw_dup, expected_plan_sha256=_sha256(raw_dup)
        )

    # Non-decreasing logical clock & equal-time ingest-before-evaluate
    bad_order = copy.deepcopy(plan)
    bad_order["steps"] = [
        {"delivery_id": "S1", "logical_time": "2026-11-27T00:01:05Z", "op": "ingest"},
        {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
        {"delivery_id": "S2", "logical_time": "2026-11-27T00:01:05Z", "op": "ingest"},
    ]
    with pytest.raises(RuntimeRecoveryInputError):
        parse_and_authenticate_runtime_plan(
            bad_order, expected_plan_sha256=compute_runtime_plan_sha256(bad_order)
        )


# ============================================================================
# 2. RT1-01: BOUNDED WINDOW + MATERIAL REVISION UPDATE + CALL ORDER (FIX-01 / FIX-13)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("poll_layout", ["sequential", "reordered_single_batch"])
async def test_rt1_01_bounded_window_rev1_rev2_call_order_and_exact_wire_oracles(
    patched_persistence: InMemorySharedDatabase,
    monkeypatch: pytest.MonkeyPatch,
    poll_layout: str,
) -> None:
    db = patched_persistence
    rec1, rec2, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)

    events: list[str] = []

    # Wrap REAL production functions to assert exact call order without changing decisions
    real_score = scoring_mod.score_candidate_pair
    real_group = grouping_mod.group_candidate_signals
    real_agg = aggregate_mod.create_or_update_incident
    real_persist = persistence_service_mod.persist_incident_atomic

    def score_spy(*args: Any, **kwargs: Any) -> Any:
        events.append("score_candidate_pair")
        return real_score(*args, **kwargs)

    def group_spy(*args: Any, **kwargs: Any) -> Any:
        events.append("group_candidate_signals")
        return real_group(*args, **kwargs)

    def agg_spy(*args: Any, **kwargs: Any) -> Any:
        events.append("create_or_update_incident")
        return real_agg(*args, **kwargs)

    async def persist_spy(*args: Any, **kwargs: Any) -> Any:
        res = await real_persist(*args, **kwargs)
        events.append(f"persist_committed:rev{res.aggregate.revision}")
        return res

    monkeypatch.setattr(scoring_mod, "score_candidate_pair", score_spy)
    monkeypatch.setattr(grouping_mod, "group_candidate_signals", group_spy)
    monkeypatch.setattr(aggregate_mod, "create_or_update_incident", agg_spy)
    monkeypatch.setattr(persistence_service_mod, "persist_incident_atomic", persist_spy)

    def on_publish(record: IncidentPublicationRecord) -> None:
        # CRITICAL INVARIANT: committed intent MUST already exist in DB before ANY Kafka send!
        rev = record.identity.revision
        key = (record.identity.tenant_id, record.identity.incident_id, rev)
        assert key in db.intents, "Kafka send attempted before durable intent commit!"
        events.append(f"publish:rev{rev}")

    def on_commit(offsets: dict[TopicPartition, int]) -> None:
        parts = sorted((tp.partition, off) for tp, off in offsets.items())
        events.append(f"commit:{parts}")

    batches = [[rec1], [rec2]] if poll_layout == "sequential" else [[rec2, rec1]]
    consumer = FakeKafkaConsumer(
        batches=batches,
        assigned_partitions=[tp0, tp1],
        initial_committed={tp0: 10, tp1: 20},
        on_commit=on_commit,
    )
    publisher = RecordingPublisher(on_publish=on_publish)
    dispatch_service = _make_dispatch_service(db, publisher)
    provider = CanonicalTopologyProvider()
    expected_topology = _pinned_topology_ref()

    result = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=dispatch_service,
        topology_provider=provider,
        expected_topology=expected_topology,
        consumer=consumer,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert result.status is CorrelationRuntimeRunStatus.COMPLETED
    assert result.all_settled is True
    assert result.phase5_activated is False
    assert result.final_committed_offsets == {tp0: 11, tp1: 21}
    assert result.unsettled_offsets == ()
    assert consumer.stopped is False  # caller-owned consumer never closed

    # Exact call order: persist rev1 -> publish rev1 -> commit P0:11 -> score -> persist rev2 -> publish rev2 -> commit P1:21
    idx_p1 = events.index("persist_committed:rev1")
    idx_pub1 = events.index("publish:rev1")
    idx_c1 = events.index("commit:[(0, 11)]")
    idx_score = events.index("score_candidate_pair")
    idx_p2 = events.index("persist_committed:rev2")
    idx_pub2 = events.index("publish:rev2")
    idx_c2 = events.index("commit:[(1, 21)]")
    assert idx_p1 < idx_pub1 < idx_c1 < idx_score < idx_p2 < idx_pub2 < idx_c2

    # Exact FIX-01 DB counts
    assert db.row_counts(TENANT_A) == {
        "incidents": 1,
        "incident_signal_links": 2,
        "incident_correlation_evidence": 1,
        "incident_timeline_events": 4,
        "incident_publication_intents": 2,
        "incident_publication_dispatches": 2,
    }

    # Exact FIX-01-REV1 and FIX-01-REV2 wire oracles
    assert len(publisher.published) == 2
    snapshot = await provider.get_snapshot()
    blast_policy = BlastRadiusPolicy.model_validate_json(
        json.dumps(PINNED_BLAST_RADIUS_POLICY)
    )
    for oracle_name, record in zip(("FIX-01-REV1", "FIX-01-REV2"), publisher.published):
        oracle = WIRE_ORACLES[oracle_name]
        assert str(record.event_id) == oracle["event_id"]
        assert record.publication_fingerprint == oracle["publication_fingerprint"]
        assert record.kafka_key.decode("utf-8") == oracle["kafka_key_utf8"]
        assert len(record.kafka_value) == oracle["kafka_value_bytes"]
        assert _sha256(record.kafka_value) == oracle["kafka_value_sha256"]
        assert (
            record.estimated_record_bytes == oracle["aiokafka_estimated_record_bytes"]
        )
        assert [list(h) for h in record.kafka_headers_json] == oracle["kafka_headers"]
        # Semantic roundtrip validation through frozen C1/4.10 validators
        validated = validate_incident_publication(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            snapshot=snapshot,
            expected_topology=expected_topology,
            expected_blast_radius_policy=blast_policy,
            require_canonical_bytes=True,
        )
        assert validated.event_id == record.event_id


# ============================================================================
# 3. RT1-02 / RT1-R1-01: DUPLICATE, REPLAY, DEDUPE NON-SUPPRESSION & PARTIAL COMMIT
# ============================================================================


@pytest.mark.asyncio
async def test_rt1_02_replay_after_ack_has_zero_db_deltas_and_zero_sends_plus_fix11_control(
    patched_persistence: InMemorySharedDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = patched_persistence
    rec1, rec2, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)

    # Run 1: create & acknowledge rev 1 and rev 2
    pub1 = RecordingPublisher()
    res1 = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub1),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1], [rec2]],
            assigned_partitions=[tp0, tp1],
            initial_committed={tp0: 10, tp1: 20},
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res1.all_settled
    assert len(pub1.published) == 2

    # Retained FIX-11 original production control: passing rev1 group directly to rev2 head raises AggregateStateError
    rev1_group = res1.evaluations[0].grouping_result.groups[0]
    rev2_agg = next(iter(db.incidents.values()))
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    s1_inp = AnomalyCorrelationInput(
        signal=s1, lineage=build_anomaly_reproducibility_lineage(s1)
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing correlation evidence cannot be removed or modified",
    ):
        aggregate_mod.create_or_update_incident(
            group=rev1_group,
            signal_inputs={s1.signal_id: s1_inp},
            processing_time=_utc("2026-11-27T00:02:00Z"),
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=rev2_agg,
        )

    # Run 2 (FIX-02 + FIX-11): restart with lost in-memory state & uncommitted offsets -> 0 persists, 0 DB deltas, 0 sends
    persist_calls = 0
    orig_persist = persistence_service_mod.persist_incident_atomic

    async def count_persist(*args: Any, **kwargs: Any) -> Any:
        nonlocal persist_calls
        persist_calls += 1
        return await orig_persist(*args, **kwargs)

    monkeypatch.setattr(
        persistence_service_mod, "persist_incident_atomic", count_persist
    )
    counts_before = db.row_counts()
    writes_before = db.incident_write_ops
    dispatch_writes_before = db.dispatch_store.row_write_ops

    pub2 = RecordingPublisher()
    res2 = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub2),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1], [rec2]],
            assigned_partitions=[tp0, tp1],
            initial_committed={tp0: 10, tp1: 20},
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res2.status is CorrelationRuntimeRunStatus.COMPLETED
    assert res2.all_settled is True
    assert persist_calls == 0
    assert db.row_counts() == counts_before
    assert db.incident_write_ops == writes_before
    assert db.dispatch_store.row_write_ops == dispatch_writes_before
    assert len(pub2.published) == 0
    assert all(
        o.status is DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED
        for o in res2.dispatch_outcomes
    )
    assert all(
        r.persistence_disposition is PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY
        for r in res2.revisions
    )
    assert res2.final_committed_offsets == {tp0: 11, tp1: 21}


@pytest.mark.asyncio
async def test_rt1_02_completed_in_memory_dedupe_never_suppresses_d1_recovery(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """In-memory dedupe ALREADY_COMPLETED must still recover committed unstaged/pending revision (CW-03)."""
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # First persist rev 1 directly (simulating CW-03: T1 committed intent, T2 dispatch never staged)
    snap = await CanonicalTopologyProvider().get_snapshot()
    cfg = CorrelationConfig.model_validate_json(json.dumps(PINNED_CORRELATION_CONFIG))
    grouped = grouping_mod.group_candidate_signals(
        [inp1],
        [],
        topology_snapshot=snap,
        config=cfg,
        event_time_watermark=_utc("2026-11-27T00:01:05Z"),
    )
    await persistence_service_mod.persist_incident_atomic(
        group=grouped.groups[0],
        signal_inputs={s1.signal_id: inp1},
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_utc("2026-11-27T00:01:05Z"),
        tenant_id=TENANT_A,
        session_factory=db.session_factory,
    )
    assert len(db.intents) == 1
    assert len(db.dispatch_store.dispatches) == 0

    # Pre-mark S1 as ALREADY_COMPLETED in the in-memory dedupe store!
    dedupe = InMemoryAnomalyDedupeStore(max_size=100)
    await dedupe.reserve(s1.signal_id, inp1.semantic_fingerprint)
    await dedupe.complete(s1.signal_id)
    assert dedupe.completed_count() == 1

    pub = RecordingPublisher()
    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1]],
            assigned_partitions=[tp0],
            initial_committed={tp0: 10},
        ),
        dedupe_store=dedupe,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res.status is CorrelationRuntimeRunStatus.COMPLETED
    assert len(pub.published) == 1
    assert str(pub.published[0].event_id) == WIRE_ORACLES["FIX-01-REV1"]["event_id"]
    assert (
        res.revisions[0].persistence_disposition
        is PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY
    )
    assert (
        res.dispatch_outcomes[0].status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
    )
    assert res.final_committed_offsets == {tp0: 11}


@pytest.mark.asyncio
async def test_rt1_02_duplicate_delivery_coordinate_settles_without_extra_incident(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    rec1_dup, _ = _make_anomaly_kafka_record(s1, partition=0, offset=11)
    plan, plan_sha = _build_plan_dict(
        source_records=[
            _source_entry("S1", rec1, inp1),
            _source_entry("S1_DUP", rec1_dup, inp1),
        ],
        steps=[
            {
                "delivery_id": "S1",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
            {
                "delivery_id": "S1_DUP",
                "logical_time": "2026-11-27T00:01:06Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:07Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:01:07Z",
    )
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    pub = RecordingPublisher()
    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1], [rec1_dup]],
            assigned_partitions=[tp0],
            initial_committed={tp0: 10},
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res.status is CorrelationRuntimeRunStatus.COMPLETED
    assert len(pub.published) == 1
    assert db.row_counts(TENANT_A)["incidents"] == 1
    assert db.row_counts(TENANT_A)["incident_publication_intents"] == 1
    assert res.final_committed_offsets == {tp0: 12}


@pytest.mark.asyncio
async def test_rt1_r1_fix12_partial_cross_partition_commit_and_history_reader_without_rewind(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    rec1, rec2, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)

    # First run commits both revisions to DB & ACKs them
    await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1], [rec2]],
            assigned_partitions=[tp0, tp1],
            initial_committed={tp0: 10, tp1: 20},
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    # FIX-12 restart: P0 already committed at 11, P1 still at 20 -> history reader fetches P0:10
    history_reader = FakeHistoryReader(
        {(rec1.topic, rec1.partition, rec1.offset): rec1}
    )
    consumer = FakeKafkaConsumer(
        batches=[[rec2]],
        assigned_partitions=[tp0, tp1],
        initial_committed={tp0: 11, tp1: 20},
    )
    pub_restart = RecordingPublisher()
    writes_before = db.incident_write_ops

    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_restart),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer,
        history_reader=history_reader,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res.status is CorrelationRuntimeRunStatus.COMPLETED
    assert history_reader.commit_calls == 0
    assert history_reader.read_coords == [(KafkaTopic.ANOMALIES.value, 0, 10)]
    assert len(pub_restart.published) == 0
    assert db.incident_write_ops == writes_before
    # Only P1 advanced to 21; P0 never rewound
    assert consumer.commit_history == [{tp1: 21}]
    assert res.final_committed_offsets == {tp0: 11, tp1: 21}


@pytest.mark.asyncio
async def test_rt1_r1_exact_predecessor_cas_continuation_from_rev1_to_rev2(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """When rev 1 is already committed/acked and restart supplies full S1+S2 plan, rev1 replays (0 DML) and rev2 continues via CAS."""
    db = patched_persistence
    rec1, rec2, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)

    # Seed rev 1 only via a 1-step run
    s1_plan, s1_sha = _build_plan_dict(
        source_records=[plan["source_records"][0]],
        steps=plan["steps"][:2],
        run_end_logical_time="2026-11-27T00:01:05Z",
    )
    pub_seed = RecordingPublisher()
    await run_controlled_correlation_runtime(
        plan=s1_plan,
        expected_plan_sha256=s1_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_seed),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert len(pub_seed.published) == 1

    # Restart with full plan (P0=11 already committed, P1=20 uncommitted)
    history_reader = FakeHistoryReader(
        {(rec1.topic, rec1.partition, rec1.offset): rec1}
    )
    pub_cont = RecordingPublisher()
    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_cont),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec2]],
            assigned_partitions=[tp0, tp1],
            initial_committed={tp0: 11, tp1: 20},
        ),
        history_reader=history_reader,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res.status is CorrelationRuntimeRunStatus.COMPLETED
    assert len(pub_cont.published) == 1  # ONLY rev 2 published
    assert (
        str(pub_cont.published[0].event_id) == WIRE_ORACLES["FIX-01-REV2"]["event_id"]
    )
    assert (
        res.revisions[0].persistence_disposition
        is PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY
    )
    assert (
        res.revisions[1].persistence_disposition
        is PersistenceDisposition.PERSISTED_UPDATED
    )
    assert res.final_committed_offsets == {tp0: 11, tp1: 21}


# ============================================================================
# 4. RT1-R1-01 / FIX-14: ALL 7 RECOVERY CORRUPTION MUTATIONS FAIL CLOSED
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "missing_plan",
        "plan_hash_drift",
        "source_fingerprint_drift",
        "missing_historical_intent",
        "historical_bytes_drift",
        "durable_head_drift",
        "missing_retained_record",
    ],
)
async def test_rt1_r1_fix14_all_recovery_mutations_fail_closed_with_zero_side_effects(
    patched_persistence: InMemorySharedDatabase,
    mutation: str,
) -> None:
    db = patched_persistence
    rec1, rec2, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)

    if mutation in (
        "missing_historical_intent",
        "historical_bytes_drift",
        "durable_head_drift",
    ):
        # Seed rev 1 + rev 2 into DB first, then corrupt durable state / shrink plan
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=FakeKafkaConsumer(
                batches=[[rec1], [rec2]],
                assigned_partitions=[tp0, tp1],
                initial_committed={tp0: 10, tp1: 20},
            ),
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )
        # Clear dispatch rows so we prove 0 sends happen even when dispatches are unstaged!
        db.dispatch_store.dispatches.clear()
        inc_id = next(iter(db.incidents.keys()))[1]

        if mutation == "missing_historical_intent":
            del db.intents[(TENANT_A, inc_id, 1)]
        elif mutation == "historical_bytes_drift":
            # Drift revision 1 schedule so intent 1 bytes no longer match reconstructed rev 1
            plan = copy.deepcopy(plan)
            plan["steps"][1]["logical_time"] = "2026-11-27T00:01:06Z"
            plan_sha = compute_runtime_plan_sha256(plan)
        elif mutation == "durable_head_drift":
            # Supply only 1-step plan when durable head is already rev 2
            plan, plan_sha = _build_plan_dict(
                source_records=[plan["source_records"][0]],
                steps=plan["steps"][:2],
                run_end_logical_time="2026-11-27T00:01:05Z",
            )

    writes_before = db.incident_write_ops
    pub = RecordingPublisher()
    batches: list[list[FakeConsumerRecord]] = [[rec1], [rec2]]
    plan_arg: Any = plan
    sha_arg: Any = plan_sha

    if mutation == "missing_plan":
        plan_arg = None
    elif mutation == "plan_hash_drift":
        sha_arg = "0" * 64
    elif mutation == "source_fingerprint_drift":
        tampered_rec1 = replace(rec1, value=(rec1.value or b"") + b" ")
        batches = [[tampered_rec1], [rec2]]
    elif mutation == "missing_retained_record":
        batches = [[rec1]]  # S2 is missing from Kafka!

    consumer = FakeKafkaConsumer(
        batches=batches,
        assigned_partitions=[tp0, tp1],
        initial_committed={tp0: 10, tp1: 20},
    )

    with pytest.raises(RuntimeRecoveryInputError):
        await run_controlled_correlation_runtime(
            plan=plan_arg,
            expected_plan_sha256=sha_arg,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=consumer,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )

    assert len(pub.published) == 0
    assert db.incident_write_ops == writes_before
    assert consumer.commit_history == []


# ============================================================================
# 5. RT1-03: LATE INPUT, MALFORMED QUARANTINE/FAIL-CLOSED, TOPOLOGY & CAPACITY
# ============================================================================


@pytest.mark.asyncio
async def test_rt1_03_late_input_rejected_delayed_and_lifecycle_non_regression(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    base_s1 = copy.deepcopy(PINNED_SIGNALS["S1"])
    base_s1["event_time"] = "2026-11-27T00:00:01Z"
    base_s1["signal_id"] = "7fb47fae-bf2e-5e85-8d4b-5a4a25de48b6"
    s_late = AnomalySignal.model_validate_json(json.dumps(base_s1))
    rec_late, inp_late = _make_anomaly_kafka_record(s_late, partition=0, offset=5)
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # Case A: late_signal_disposition="reject" -> 0 incidents, 0 sends, offset committed (FIX-06)
    plan_rej, sha_rej = _build_plan_dict(
        source_records=[_source_entry("S_LATE", rec_late, inp_late)],
        steps=[
            {
                "delivery_id": "S_LATE",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:01:05Z",
    )
    pub_rej = RecordingPublisher()
    res_rej = await run_controlled_correlation_runtime(
        plan=plan_rej,
        expected_plan_sha256=sha_rej,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_rej),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec_late]], assigned_partitions=[tp0], initial_committed={tp0: 5}
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res_rej.status is CorrelationRuntimeRunStatus.COMPLETED
    assert len(pub_rej.published) == 0
    assert db.row_counts()["incidents"] == 0
    assert res_rej.final_committed_offsets == {tp0: 6}

    # Case B: late_signal_disposition="delay" -> remains UNSETTLED, never silently committed!
    delay_cfg = copy.deepcopy(PINNED_CORRELATION_CONFIG)
    delay_cfg["late_signal_disposition"] = "delay"
    plan_del, sha_del = _build_plan_dict(
        source_records=[_source_entry("S_LATE", rec_late, inp_late)],
        steps=[
            {
                "delivery_id": "S_LATE",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:01:05Z",
        config_dict=delay_cfg,
    )
    consumer_del = FakeKafkaConsumer(
        batches=[[rec_late]], assigned_partitions=[tp0], initial_committed={tp0: 5}
    )
    res_del = await run_controlled_correlation_runtime(
        plan=plan_del,
        expected_plan_sha256=sha_del,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer_del,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res_del.status is CorrelationRuntimeRunStatus.INCOMPLETE_DELAYED_INPUTS
    assert res_del.all_settled is False
    assert res_del.unsettled_offsets == ((tp0, 5),)
    assert consumer_del.commit_history == []

    # Case C: Lifecycle cannot regress (SETTLING -> COLLECTING rejected with RuntimeRecoveryInputError)
    accept_cfg = copy.deepcopy(PINNED_CORRELATION_CONFIG)
    accept_cfg["late_signal_disposition"] = "accept"
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    s2 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S2"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    rec2, inp2 = _make_anomaly_kafka_record(s2, partition=0, offset=11)
    # Eval 1 at 00:01:15Z (15s >= 10s quiet period -> SETTLING); S2 ingested at 00:01:16Z, eval 2 at 00:01:17Z (1s < 10s -> would regress to COLLECTING!)
    plan_reg, sha_reg = _build_plan_dict(
        source_records=[
            _source_entry("S1", rec1, inp1),
            _source_entry("S2", rec2, inp2),
        ],
        steps=[
            {
                "delivery_id": "S1",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:15Z", "op": "evaluate"},
            {
                "delivery_id": "S2",
                "logical_time": "2026-11-27T00:01:16Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:17Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:01:17Z",
        config_dict=accept_cfg,
    )
    with pytest.raises(RuntimeRecoveryInputError, match="regress"):
        await run_controlled_correlation_runtime(
            plan=plan_reg,
            expected_plan_sha256=sha_reg,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=FakeKafkaConsumer(
                batches=[[rec1, rec2]],
                assigned_partitions=[tp0],
                initial_committed={tp0: 10},
            ),
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )


@pytest.mark.asyncio
async def test_rt1_03_fix06_malformed_input_quarantined_vs_fail_closed(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=11)
    bad_rec = FakeConsumerRecord(
        topic=KafkaTopic.ANOMALIES.value,
        partition=0,
        offset=10,
        key=b"bad-key",
        value=b"{not-valid-json",
        headers=(),
    )
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # Without QuarantineSink -> fail closed with RuntimeInputValidationError, 0 commits, 0 sends
    pub_fail = RecordingPublisher()
    consumer_fail = FakeKafkaConsumer(
        batches=[[bad_rec, rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
    )
    with pytest.raises(RuntimeInputValidationError):
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub_fail),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=consumer_fail,
            quarantine_sink=None,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )
    assert consumer_fail.commit_history == []
    assert len(pub_fail.published) == 0

    # With QuarantineSink -> quarantines bad_rec (offset 10), then processes S1 (offset 11), commits up to 12
    sink = RecordingQuarantineSink()
    pub_ok = RecordingPublisher()
    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_ok),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[bad_rec, rec1]],
            assigned_partitions=[tp0],
            initial_committed={tp0: 10},
        ),
        quarantine_sink=sink,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res.status is CorrelationRuntimeRunStatus.COMPLETED
    assert len(sink.records) == 1
    assert len(res.quarantined_inputs) == 1
    assert res.quarantined_inputs[0].status == "QUARANTINED_INVALID_INPUT"
    assert res.quarantined_inputs[0].offset == 10
    assert len(pub_ok.published) == 1
    assert res.final_committed_offsets == {tp0: 12}


@pytest.mark.asyncio
async def test_rt1_03_stale_topology_pin_and_insufficient_topology_fail_closed(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    rec1, _, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # FIX-07: Stale/mismatched topology fingerprint rejected at startup with 0 persists, 0 sends, 0 commits
    stale_ref = TopologySnapshotReference(
        topology_id=UUID(PINNED_TOPOLOGY_ID),
        topology_version=PINNED_TOPOLOGY_VERSION,
        snapshot_fingerprint="f" * 64,
        captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
        provider="canonical",
    )
    pub = RecordingPublisher()
    consumer = FakeKafkaConsumer(
        batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
    )
    with pytest.raises(RuntimeStartupError):
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=stale_ref,
            consumer=consumer,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )
    assert db.row_counts()["incidents"] == 0
    assert len(pub.published) == 0
    assert consumer.commit_history == []

    # Insufficient topology: anomaly references a service missing from pinned topology snapshot
    unknown_sig_raw = copy.deepcopy(PINNED_SIGNALS["S1"])
    unknown_sig_raw["service"] = "unmapped-edge-service"
    unknown_sig_raw["signal_id"] = str(uuid5(TENANT_A, "unmapped-edge-service"))
    unknown_sig = AnomalySignal.model_validate_json(json.dumps(unknown_sig_raw))
    rec_u, inp_u = _make_anomaly_kafka_record(unknown_sig, partition=0, offset=30)
    plan_u, sha_u = _build_plan_dict(
        source_records=[_source_entry("U1", rec_u, inp_u)],
        steps=[
            {
                "delivery_id": "U1",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:01:05Z",
    )
    consumer_u = FakeKafkaConsumer(
        batches=[[rec_u]], assigned_partitions=[tp0], initial_committed={tp0: 30}
    )
    with pytest.raises(
        GroupingInputError, match="Service missing in topology snapshot"
    ):
        await run_controlled_correlation_runtime(
            plan=plan_u,
            expected_plan_sha256=sha_u,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=consumer_u,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )
    assert db.row_counts()["incidents"] == 0
    assert len(pub.published) == 0
    assert consumer_u.commit_history == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cap_name",
    [
        "max_input_records",
        "max_active_windows",
        "max_buffered_records",
        "max_window_members",
    ],
)
async def test_rt1_03_finite_capacity_bounds_rejected_without_partial_commit(
    patched_persistence: InMemorySharedDatabase,
    cap_name: str,
) -> None:
    db = patched_persistence
    rec1, rec2, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)

    if cap_name == "max_active_windows":
        # Two distinct tumbling windows (different buckets) with max_active_windows=1
        s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
        s2_raw = copy.deepcopy(PINNED_SIGNALS["S2"])
        s2_raw["event_time"] = "2026-11-27T00:10:00Z"  # Next 5-min bucket
        s2 = AnomalySignal.model_validate_json(json.dumps(s2_raw))
        r1, i1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
        r2, i2 = _make_anomaly_kafka_record(s2, partition=1, offset=20)
        plan, plan_sha = _build_plan_dict(
            source_records=[_source_entry("S1", r1, i1), _source_entry("S2", r2, i2)],
            steps=[
                {
                    "delivery_id": "S1",
                    "logical_time": "2026-11-27T00:01:00Z",
                    "op": "ingest",
                },
                {
                    "delivery_id": "S2",
                    "logical_time": "2026-11-27T00:10:00Z",
                    "op": "ingest",
                },
                {"logical_time": "2026-11-27T00:10:05Z", "op": "evaluate"},
            ],
            run_end_logical_time="2026-11-27T00:10:05Z",
        )
        batches = [[r1, r2]]
        cfg = CorrelationRuntimeConfig(max_active_windows=1)
    elif cap_name == "max_window_members":
        # Both S1 and S2 ingested before first evaluation so member cap trips before any persist
        plan, plan_sha = _build_plan_dict(
            source_records=plan["source_records"],
            steps=[
                {
                    "delivery_id": "S1",
                    "logical_time": "2026-11-27T00:01:00Z",
                    "op": "ingest",
                },
                {
                    "delivery_id": "S2",
                    "logical_time": "2026-11-27T00:01:10Z",
                    "op": "ingest",
                },
                {"logical_time": "2026-11-27T00:01:25Z", "op": "evaluate"},
            ],
            run_end_logical_time="2026-11-27T00:01:25Z",
        )
        batches = [[rec1, rec2]]
        cfg = CorrelationRuntimeConfig(max_window_members=1)
    elif cap_name == "max_buffered_records":
        batches = [[rec1, rec2]]
        cfg = CorrelationRuntimeConfig(max_buffered_records=1)
    else:
        batches = [[rec1, rec2]]
        cfg = CorrelationRuntimeConfig(max_input_records=1)

    pub = RecordingPublisher()
    consumer = FakeKafkaConsumer(
        batches=batches,
        assigned_partitions=[tp0, tp1],
        initial_committed={tp0: 10, tp1: 20},
    )
    with pytest.raises(RuntimeCapacityError):
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            config=cfg,
            consumer=consumer,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )
    assert db.row_counts()["incidents"] == 0
    assert len(pub.published) == 0
    assert consumer.commit_history == []


# ============================================================================
# 6. RT1-05: RECOVERABLE & TERMINAL PUBLICATION FAILURES + PERSISTENCE FAILURE
# ============================================================================


@pytest.mark.asyncio
async def test_rt1_05_recoverable_publication_failure_and_bounded_recovery_fix04(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    sink = RecordingQuarantineSink()

    # Run 1: broker returns FAILED_RETRIABLE -> DB rows intact, retry_scheduled, NOT quarantined, offset uncommitted
    pub_fail = RecordingPublisher(
        outcomes=[
            IncidentPublishOutcome(
                IncidentPublishStatus.FAILED_RETRIABLE,
                error=IncidentPublishRetriableError("retryable"),
            )
        ]
    )
    consumer1 = FakeKafkaConsumer(
        batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
    )
    res1 = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_fail),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer1,
        quarantine_sink=sink,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res1.status is CorrelationRuntimeRunStatus.COMPLETED_WITH_PENDING_DISPATCH
    assert res1.all_settled is False
    assert len(sink.records) == 0  # Valid anomaly NEVER quarantined!
    assert consumer1.commit_history == []
    assert res1.unsettled_offsets == ((tp0, 10),)
    assert db.row_counts(TENANT_A)["incidents"] == 1
    assert db.row_counts(TENANT_A)["incident_publication_intents"] == 1
    assert (
        res1.dispatch_outcomes[0].status
        is DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION
    )
    assert (
        res1.dispatch_outcomes[0].dispatch.dispatch_state
        is DispatchState.RETRY_SCHEDULED
    )

    # Advance server clock past retry backoff and restart fresh coordinator -> recovers exact frozen wire bytes!
    db.clock.advance(5.0)
    writes_before = db.incident_write_ops
    pub_rec = RecordingPublisher()
    res2 = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_rec),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
        ),
        quarantine_sink=sink,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res2.status is CorrelationRuntimeRunStatus.COMPLETED
    assert res2.all_settled is True
    assert db.incident_write_ops == writes_before  # 0 incident DML
    assert len(pub_rec.published) == 1
    assert pub_rec.published[0].kafka_value == pub_fail.published[0].kafka_value
    assert str(pub_rec.published[0].event_id) == WIRE_ORACLES["FIX-01-REV1"]["event_id"]
    assert res2.final_committed_offsets == {tp0: 11}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["ambiguous", "exhausted", "unsendable"])
async def test_rt1_05_ambiguous_exhausted_and_unsendable_outcomes_never_mask_or_commit(
    patched_persistence: InMemorySharedDatabase,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    attempts = 1 if failure_mode == "exhausted" else 3
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
        steps=[
            {
                "delivery_id": "S1",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:01:05Z",
        max_dispatch_attempts=attempts,
    )
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    sink = RecordingQuarantineSink()

    if failure_mode == "unsendable":
        import app.correlation.dispatch_service as ds_mod

        def raise_oversized(*args: Any, **kwargs: Any) -> Any:
            raise IncidentPublicationSizeLimitError("exceeds 983040 bytes")

        monkeypatch.setattr(
            ds_mod, "build_incident_publication_from_intent", raise_oversized
        )
        pub = RecordingPublisher()
    else:
        pub = RecordingPublisher(
            outcomes=[
                IncidentPublishOutcome(
                    IncidentPublishStatus.FAILED_AMBIGUOUS,
                    error=IncidentPublishAmbiguousError("ambiguous"),
                )
            ]
        )

    consumer = FakeKafkaConsumer(
        batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
    )
    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(
            db, pub, max_dispatch_attempts=attempts
        ),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        config=CorrelationRuntimeConfig(max_dispatch_attempts=attempts),
        consumer=consumer,
        quarantine_sink=sink,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res.all_settled is False
    assert len(sink.records) == 0
    assert consumer.commit_history == []
    assert db.row_counts(TENANT_A)["incident_publication_intents"] == 1
    if failure_mode == "ambiguous":
        assert res.status is CorrelationRuntimeRunStatus.COMPLETED_WITH_PENDING_DISPATCH
        assert (
            res.dispatch_outcomes[0].status
            is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
        )
    elif failure_mode == "exhausted":
        assert (
            res.status
            is CorrelationRuntimeRunStatus.COMPLETED_WITH_TERMINAL_DISPATCH_FAILURE
        )
        assert (
            res.dispatch_outcomes[0].status
            is DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED
        )
    else:
        assert (
            res.status
            is CorrelationRuntimeRunStatus.COMPLETED_WITH_TERMINAL_DISPATCH_FAILURE
        )
        assert (
            res.dispatch_outcomes[0].status
            is DispatchOutcomeStatus.COMMITTED_UNSENDABLE
        )
        assert len(pub.published) == 0


@pytest.mark.asyncio
async def test_rt1_05_persistence_failure_emits_zero_sends_and_never_quarantines(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    db.fail_next_persist_with = IncidentPersistenceStoreError("injected DB failure")
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    pub = RecordingPublisher()
    sink = RecordingQuarantineSink()
    consumer = FakeKafkaConsumer(
        batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
    )

    with pytest.raises(RuntimePersistenceError) as exc_info:
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=consumer,
            quarantine_sink=sink,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )

    assert isinstance(exc_info.value.__cause__, IncidentPersistenceStoreError)
    assert len(pub.published) == 0
    assert len(sink.records) == 0
    assert consumer.commit_history == []


# ============================================================================
# 7. RT1-04: OFFSET TRACKER, BUFFERED CRASH SURVIVAL, COMMIT FAILURE & REBALANCE
# ============================================================================


def test_rt1_04_partition_offset_tracker_contiguous_gaps_and_no_rewind() -> None:
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tracker = PartitionOffsetTracker({tp0: 10})

    # Unsettled offset 12 blocks settled offset 13; offset 10 settles -> advances only to 11
    tracker.observe(tp0, 10, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    tracker.observe(tp0, 12, state=DeliverySettlementState.BUFFERED_UNCOMMITTED)
    tracker.observe(tp0, 13, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    assert tracker.compute_committable_offsets() == {tp0: 11}
    tracker.record_committed({tp0: 11})

    # Compacted gap (offset 11 absent): once 12 settles, jumps straight to 14
    tracker.mark_settled(tp0, 12, DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    assert tracker.compute_committable_offsets() == {tp0: 14}
    tracker.record_committed({tp0: 14})

    # Observing older offset < 14 never rewinds
    tracker.observe(tp0, 10, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    assert tracker.compute_committable_offsets() == {}

    # Revoking partition removes commit eligibility
    tracker.observe(tp0, 15, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    tracker.revoke_partitions([tp0])
    assert tracker.compute_committable_offsets() == {}


@pytest.mark.asyncio
async def test_rt1_04_cw01_buffered_inputs_survive_shutdown_crash_and_replay_fix10(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """Buffered in-memory record before evaluate writes 0 DB rows, 0 sends, 0 commits, and survives restart."""
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    pub1 = RecordingPublisher()
    coord_holder: list[CorrelationRuntimeCoordinator] = []

    def stop_right_after_poll(poll_idx: int) -> None:
        # Request shutdown immediately after S1 is polled into buffer, BEFORE evaluate runs!
        if poll_idx == 0 and coord_holder:
            coord_holder[0].request_shutdown()

    consumer1 = FakeKafkaConsumer(
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
        on_poll=stop_right_after_poll,
    )
    coord1 = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub1),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer1,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    coord_holder.append(coord1)
    await coord1.start()
    res1 = await coord1.run_plan(plan=plan, expected_plan_sha256=plan_sha)
    report1 = await coord1.shutdown()

    # No implicit shutdown evaluation: 0 DB rows, 0 sends, 0 commits, offset 10 still buffered
    assert res1.status is CorrelationRuntimeRunStatus.STOPPED_BY_SHUTDOWN
    assert res1.all_settled is False
    assert db.row_counts()["incidents"] == 0
    assert len(pub1.published) == 0
    assert consumer1.commit_history == []
    assert report1.unsettled_offset_count == 1

    # Restart brand-new coordinator from uncommitted offset 10 -> full reconstruction & delivery!
    pub2 = RecordingPublisher()
    res2 = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub2),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res2.status is CorrelationRuntimeRunStatus.COMPLETED
    assert db.row_counts(TENANT_A)["incidents"] == 1
    assert len(pub2.published) == 1
    assert str(pub2.published[0].event_id) == WIRE_ORACLES["FIX-01-REV1"]["event_id"]
    assert res2.final_committed_offsets == {tp0: 11}


@pytest.mark.asyncio
async def test_rt1_04_cw08_offset_commit_failure_surfaces_typed_error_and_replays_cleanly_fix10(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # Run 1: commit raises after durable ACK
    pub1 = RecordingPublisher()
    consumer1 = FakeKafkaConsumer(
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
        commit_error=RuntimeError("broker coordinator unavailable"),
    )
    with pytest.raises(RuntimeOffsetCommitError) as exc_commit:
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub1),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=consumer1,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )
    assert isinstance(exc_commit.value.__cause__, RuntimeError)
    assert len(pub1.published) == 1
    assert consumer1.committed_positions[tp0] == 10

    # Run 2 (CW-07/CW-08 replay): 0 additional DB writes, 0 additional sends, commits P0:11!
    writes_before = db.incident_write_ops
    dispatch_writes_before = db.dispatch_store.row_write_ops
    pub2 = RecordingPublisher()
    res2 = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub2),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec1]], assigned_partitions=[tp0], initial_committed={tp0: 10}
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert res2.status is CorrelationRuntimeRunStatus.COMPLETED
    assert len(pub2.published) == 0
    assert db.incident_write_ops == writes_before
    assert db.dispatch_store.row_write_ops == dispatch_writes_before
    assert res2.final_committed_offsets == {tp0: 11}


@pytest.mark.asyncio
async def test_rt1_04_rebalance_revocation_cancels_commit_ownership(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    coord_ref: list[CorrelationRuntimeCoordinator] = []

    def trigger_rebalance(poll_idx: int) -> None:
        if poll_idx == 0 and coord_ref:
            coord_ref[0].on_partitions_revoked([tp0])

    consumer = FakeKafkaConsumer(
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
        on_poll=trigger_rebalance,
    )
    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    coord_ref.append(coord)
    await coord.start()
    with pytest.raises(RuntimeRebalanceError):
        await coord.run_plan(plan=plan, expected_plan_sha256=plan_sha)
    assert consumer.commit_history == []
    await coord.shutdown()


# ============================================================================
# 8. RT1-05: CANCELLATION, OWNED-RESOURCE SHUTDOWN, READINESS & MAIN.PY GUARD
# ============================================================================


@pytest.mark.asyncio
async def test_rt1_05_cancellation_propagates_closes_owned_consumer_and_releases_dedupe(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    owned_consumer = FakeKafkaConsumer(
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
        cancel_on_poll_index=0,
    )
    dedupe = InMemoryAnomalyDedupeStore(max_size=100)

    with pytest.raises(asyncio.CancelledError):
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer_factory=lambda *a, **kw: owned_consumer,
            dedupe_store=dedupe,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )

    assert owned_consumer.started is True
    assert owned_consumer.stopped is True  # Coordinator-owned handle closed!
    assert owned_consumer.commit_history == []
    assert dedupe.entry_count() == 0


@pytest.mark.asyncio
async def test_rt1_05_readiness_probe_and_main_py_non_activation(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    expected_ref = _pinned_topology_ref()

    # Healthy readiness across PostgreSQL, Kafka (both topics), and pinned topology
    ok_consumer = FakeKafkaConsumer(
        batches=[],
        assigned_partitions=[],
        topics={KafkaTopic.ANOMALIES.value, KafkaTopic.INCIDENTS.value},
    )
    ready = await check_correlation_runtime_readiness(
        session_factory=db.session_factory,
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=expected_ref,
        kafka_consumer_factory=lambda **kw: ok_consumer,
        verify_db_fn=_noop_verify_role,
    )
    assert ready.ready is True
    assert ready.status == "ready"
    assert ready.error_codes == ()
    assert ok_consumer.stopped is True

    # Missing aegis.incidents topic -> not_ready with explicit safe error code
    missing_topic_consumer = FakeKafkaConsumer(
        batches=[],
        assigned_partitions=[],
        topics={KafkaTopic.ANOMALIES.value},
    )
    not_ready = await check_correlation_runtime_readiness(
        session_factory=db.session_factory,
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=expected_ref,
        kafka_consumer_factory=lambda **kw: missing_topic_consumer,
        verify_db_fn=_noop_verify_role,
    )
    assert not_ready.ready is False
    assert not_ready.status == "not_ready"
    assert "KAFKA_TOPICS_MISSING" in not_ready.error_codes

    # Stale topology pin -> not_ready
    stale_ref = TopologySnapshotReference(
        topology_id=UUID(PINNED_TOPOLOGY_ID),
        topology_version=PINNED_TOPOLOGY_VERSION,
        snapshot_fingerprint="a" * 64,
        captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
        provider="canonical",
    )
    stale_ready = await check_correlation_runtime_readiness(
        session_factory=db.session_factory,
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=stale_ref,
        kafka_consumer_factory=lambda **kw: FakeKafkaConsumer(
            batches=[], assigned_partitions=[]
        ),
        verify_db_fn=_noop_verify_role,
    )
    assert stale_ready.ready is False
    assert "TOPOLOGY_PIN_UNAVAILABLE_OR_MISMATCH" in stale_ready.error_codes

    # Verify app/main.py activates NO background correlation coordinator or Phase 5 consumer
    main_py = (Path(__file__).resolve().parents[1] / "app" / "main.py").read_text(
        encoding="utf-8"
    )
    assert "CorrelationRuntimeCoordinator" not in main_py
    assert "run_controlled_correlation_runtime" not in main_py
    assert "AnomalyIntakeConsumer" not in main_py


# ============================================================================
# 9. RT1-06: MULTI-TENANT AND MULTI-INCIDENT ISOLATION
# ============================================================================


@pytest.mark.asyncio
async def test_rt1_06_multi_tenant_and_multi_incident_isolation(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """Interleaved Tenant A (2 separate incidents across 2 buckets) & Tenant B (1 incident) remain strictly isolated."""
    db = patched_persistence

    # Tenant A, Incident 1 (bucket 00:00..00:05)
    sig_a1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))

    # Tenant A, Incident 2 (separate bucket 00:10..00:15 -> distinct incident in Tenant A)
    raw_a2 = copy.deepcopy(PINNED_SIGNALS["S2"])
    raw_a2["event_time"] = "2026-11-27T00:10:00Z"
    raw_a2["signal_id"] = str(uuid5(TENANT_A, "tenant-a-incident-2"))
    sig_a2 = AnomalySignal.model_validate_json(json.dumps(raw_a2))

    # Tenant B, Incident 1 (same service & timestamp as sig_a1, DIFFERENT tenant!)
    raw_b1 = copy.deepcopy(PINNED_SIGNALS["S1"])
    raw_b1["tenant_id"] = str(TENANT_B)
    raw_b1["signal_id"] = str(uuid5(TENANT_B, "tenant-b-incident-1"))
    sig_b1 = AnomalySignal.model_validate_json(json.dumps(raw_b1))

    rec_a1, inp_a1 = _make_anomaly_kafka_record(sig_a1, partition=0, offset=10)
    rec_b1, inp_b1 = _make_anomaly_kafka_record(sig_b1, partition=0, offset=11)
    rec_a2, inp_a2 = _make_anomaly_kafka_record(sig_a2, partition=1, offset=20)

    plan, plan_sha = _build_plan_dict(
        source_records=[
            _source_entry("A1", rec_a1, inp_a1),
            _source_entry("B1", rec_b1, inp_b1),
            _source_entry("A2", rec_a2, inp_a2),
        ],
        steps=[
            {
                "delivery_id": "A1",
                "logical_time": "2026-11-27T00:01:00Z",
                "op": "ingest",
            },
            {
                "delivery_id": "B1",
                "logical_time": "2026-11-27T00:01:01Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:01:05Z", "op": "evaluate"},
            {
                "delivery_id": "A2",
                "logical_time": "2026-11-27T00:10:00Z",
                "op": "ingest",
            },
            {"logical_time": "2026-11-27T00:10:05Z", "op": "evaluate"},
        ],
        run_end_logical_time="2026-11-27T00:10:05Z",
    )

    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    tp1 = TopicPartition(KafkaTopic.ANOMALIES.value, 1)
    pub = RecordingPublisher()

    res = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[[rec_a1, rec_b1], [rec_a2]],
            assigned_partitions=[tp0, tp1],
            initial_committed={tp0: 10, tp1: 20},
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )

    assert res.status is CorrelationRuntimeRunStatus.COMPLETED
    assert res.all_settled is True
    assert db.row_counts(TENANT_A)["incidents"] == 2
    assert db.row_counts(TENANT_B)["incidents"] == 1
    # At 00:01:05Z: A1 rev1 (COLLECTING) + B1 rev1 (COLLECTING)
    # At 00:10:05Z: A1 rev2 (FINALIZED) + B1 rev2 (FINALIZED) + A2 rev1 (COLLECTING)
    assert len(pub.published) == 5

    tenant_a_records = [r for r in pub.published if r.identity.tenant_id == TENANT_A]
    tenant_b_records = [r for r in pub.published if r.identity.tenant_id == TENANT_B]
    assert len(tenant_a_records) == 3
    assert len(tenant_b_records) == 2
    assert len({r.identity.incident_id for r in tenant_a_records}) == 2
    assert len({r.identity.incident_id for r in tenant_b_records}) == 1
    assert len({r.identity.incident_id for r in pub.published}) == 3
    assert len({r.event_id for r in pub.published}) == 5
    assert all(
        r.kafka_key.decode("utf-8").startswith(f"{TENANT_A}:") for r in tenant_a_records
    )
    assert all(
        r.kafka_key.decode("utf-8").startswith(f"{TENANT_B}:") for r in tenant_b_records
    )
    assert res.final_committed_offsets == {tp0: 12, tp1: 21}


@pytest.mark.asyncio
async def test_rt1_05_coordinator_lifecycle_state_machine_guards(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    _, _, plan, plan_sha = _make_fix01_materials()
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    consumer = FakeKafkaConsumer(batches=[], assigned_partitions=[tp0])

    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    assert coord.state is CorrelationRuntimeState.CREATED

    # Running before start() fails closed
    with pytest.raises(RuntimeStartupError):
        await coord.run_plan(plan=plan, expected_plan_sha256=plan_sha)

    await coord.start()
    assert coord.state is CorrelationRuntimeState.READY
    # Idempotent start()
    snap = await coord.start()
    assert snap.snapshot_fingerprint == PINNED_TOPOLOGY_FINGERPRINT

    report = await coord.shutdown()
    assert report.state is CorrelationRuntimeState.STOPPED
    assert coord.state is CorrelationRuntimeState.STOPPED
    # Idempotent shutdown()
    report2 = await coord.shutdown()
    assert report2.state is CorrelationRuntimeState.STOPPED

    # Cannot restart from STOPPED
    with pytest.raises(RuntimeStartupError):
        await coord.start()


# ============================================================================
# 10. RT1-R1 RETAINED REPRODUCTIONS FOR INDEPENDENT REVIEW BLOCKERS 1–4
# ============================================================================


@pytest.mark.asyncio
async def test_rt1_r1_blocker1_rebalance_revoke_and_reassign_blocks_offset_rewind(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """Blocker 1: owned consumer registers ConsumerRebalanceListener; revoking & reassigning
    the same partition (even after external commit to 40) must NEVER rewind to 11 or report COMPLETED.
    """
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # Part A: Owned consumer via consumer_factory gets ConsumerRebalanceListener via subscribe().
    class SubscribingFakeConsumer(FakeKafkaConsumer):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.subscribed_topics: list[str] = []
            self.registered_listener: Any = None

        def subscribe(
            self,
            topics: Sequence[str] = (),
            pattern: Any = None,
            listener: Any = None,
        ) -> None:
            self.subscribed_topics = list(topics)
            self.registered_listener = listener

    owned_consumer = SubscribingFakeConsumer(
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
    )

    def revoke_and_reassign_same_partition(poll_idx: int) -> None:
        if poll_idx == 0:
            listener = owned_consumer.registered_listener
            assert isinstance(listener, aiokafka.ConsumerRebalanceListener)
            listener.on_partitions_revoked([tp0])
            # Another group member advances committed offset to 40, then tp0 is reassigned back!
            owned_consumer.committed_positions[tp0] = 40
            listener.on_partitions_assigned([tp0])

    owned_consumer._on_poll = revoke_and_reassign_same_partition

    with pytest.raises(RuntimeRebalanceError):
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer_factory=lambda *a, **kw: owned_consumer,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )

    assert owned_consumer.subscribed_topics == [KafkaTopic.ANOMALIES.value]
    assert isinstance(
        owned_consumer.registered_listener, aiokafka.ConsumerRebalanceListener
    )
    assert owned_consumer.commit_history == []
    assert owned_consumer.committed_positions[tp0] == 40
    assert owned_consumer.stopped is True

    # Part B: Caller-owned consumer WITHOUT listener where external broker commit advances 10 -> 40
    # before commit() runs: pre-commit committed() re-check refuses rewind from 40 to 11.
    caller_consumer = FakeKafkaConsumer(
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
    )

    def advance_external_offset_on_publish(_record: IncidentPublicationRecord) -> None:
        caller_consumer.committed_positions[tp0] = 40

    pub_b = RecordingPublisher(on_publish=advance_external_offset_on_publish)
    with pytest.raises(RuntimeRebalanceError):
        await run_controlled_correlation_runtime(
            plan=plan,
            expected_plan_sha256=plan_sha,
            session_factory=db.session_factory,
            dispatch_service=_make_dispatch_service(db, pub_b),
            topology_provider=CanonicalTopologyProvider(),
            expected_topology=_pinned_topology_ref(),
            consumer=caller_consumer,
            persistence_repo_factory=db.make_persistence_repo,
            verify_role_fn=_noop_verify_role,
        )

    assert caller_consumer.commit_history == []
    assert caller_consumer.committed_positions[tp0] == 40


@pytest.mark.asyncio
async def test_rt1_r1_blocker2_run_deadline_prevents_late_publish_and_completed_status(
    patched_persistence: InMemorySharedDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blocker 2: a 1-second run must not publish after the deadline or return COMPLETED."""
    db = patched_persistence
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    # Case A: slow poll (0.6s) + slow persist (0.8s) on 1s run -> interrupts at ~1.0s with 0 publishes!
    class SlowPollConsumer(FakeKafkaConsumer):
        def __init__(self, delay: float, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._delay = delay

        async def getmany(
            self,
            *partitions: TopicPartition,
            timeout_ms: int = 0,
            max_records: int | None = None,
        ) -> dict[TopicPartition, list[FakeConsumerRecord]]:
            await asyncio.sleep(self._delay)
            return await super().getmany(
                *partitions, timeout_ms=timeout_ms, max_records=max_records
            )

    orig_persist = persistence_service_mod.persist_incident_atomic

    async def slow_persist(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(0.8)
        return await orig_persist(*args, **kwargs)

    monkeypatch.setattr(
        persistence_service_mod, "persist_incident_atomic", slow_persist
    )

    pub_a = RecordingPublisher()
    consumer_a = SlowPollConsumer(
        delay=0.6,
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
    )
    t0 = time.monotonic()
    res_a = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_a),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        config=CorrelationRuntimeConfig(max_run_duration_seconds=1),
        consumer=consumer_a,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    elapsed_a = time.monotonic() - t0
    assert res_a.status is CorrelationRuntimeRunStatus.INCOMPLETE_DEADLINE_EXCEEDED
    assert res_a.all_settled is False
    assert len(pub_a.published) == 0
    assert consumer_a.commit_history == []
    assert elapsed_a < 1.35

    # Case B: slow publish starting within deadline (0.6s poll + 0.8s publish) -> cancels at 1.0s,
    # never returns COMPLETED, never commits offset, leaves dispatch honestly ambiguous!
    monkeypatch.setattr(
        persistence_service_mod, "persist_incident_atomic", orig_persist
    )

    class SlowPublisher(RecordingPublisher):
        async def publish(
            self, record: IncidentPublicationRecord
        ) -> IncidentPublishOutcome:
            await asyncio.sleep(0.8)
            return await super().publish(record)

    pub_b = SlowPublisher()
    consumer_b = SlowPollConsumer(
        delay=0.6,
        batches=[[rec1]],
        assigned_partitions=[tp0],
        initial_committed={tp0: 10},
    )
    t1 = time.monotonic()
    res_b = await run_controlled_correlation_runtime(
        plan=plan,
        expected_plan_sha256=plan_sha,
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, pub_b),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        config=CorrelationRuntimeConfig(max_run_duration_seconds=1),
        consumer=consumer_b,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    elapsed_b = time.monotonic() - t1
    assert res_b.status is CorrelationRuntimeRunStatus.INCOMPLETE_DEADLINE_EXCEEDED
    assert res_b.all_settled is False
    assert consumer_b.commit_history == []
    assert elapsed_b < 1.35
    # Cancellation bookkeeping is now explicitly retained rather than included
    # in the run duration. Verify its durable result once it has settled.
    await asyncio.gather(*runtime_mod._RUNTIME_OPERATIONS, return_exceptions=True)
    await asyncio.sleep(0)
    disp_rows = list(db.dispatch_store.dispatches.values())
    assert len(disp_rows) == 1
    assert disp_rows[0].dispatch_state is DispatchState.AMBIGUOUS_OUTCOME


@pytest.mark.asyncio
async def test_rt1_r1_blocker3_shutdown_grace_budget_is_shared_not_multiplied(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """Blocker 3: a 0.5-second shutdown_grace_seconds is shared across commit + consumer.stop + history.stop."""
    db = patched_persistence
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    class HangingOwnedConsumer(FakeKafkaConsumer):
        def __init__(self, *, hang_commit: bool, hang_stop: bool) -> None:
            super().__init__(
                batches=[],
                assigned_partitions=[tp0],
                initial_committed={tp0: 10},
            )
            self._hang_commit = hang_commit
            self._hang_stop = hang_stop
            self.stop_calls = 0

        async def commit(
            self, offsets: dict[TopicPartition, int] | None = None
        ) -> None:
            if self._hang_commit:
                await asyncio.sleep(2.0)
            await super().commit(offsets)

        async def stop(self) -> None:
            self.stop_calls += 1
            if self._hang_stop:
                await asyncio.sleep(2.0)
            await super().stop()

    class HangingOwnedHistoryReader(FakeHistoryReader):
        def __init__(self) -> None:
            super().__init__({})
            self.stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1
            await asyncio.sleep(2.0)
            await super().stop()

    # Subcase A: commit hangs + owned consumer.stop hangs + owned history.stop hangs (would take 1.5s if multiplied!)
    cons_a = HangingOwnedConsumer(hang_commit=True, hang_stop=True)
    hist_a = HangingOwnedHistoryReader()
    coord_a = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        config=CorrelationRuntimeConfig(shutdown_grace_seconds=0.5),
        consumer_factory=lambda *a, **kw: cons_a,
        history_reader_factory=lambda *a, **kw: hist_a,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord_a.start()
    tracker_a = PartitionOffsetTracker({tp0: 10})
    tracker_a.observe(tp0, 10, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    coord_a._offset_tracker = tracker_a

    t0 = time.monotonic()
    with pytest.raises(RuntimeOffsetCommitError):
        await coord_a.shutdown()
    elapsed_a = time.monotonic() - t0
    assert elapsed_a < 0.85
    assert coord_a.state is CorrelationRuntimeState.STOPPED

    # Subcase B: no pending commit, BOTH owned consumer.stop and history.stop hang 2.0s each ->
    # closed concurrently within ONE single 0.5s grace budget (< 0.85s, NOT 1.0s+!)
    cons_b = HangingOwnedConsumer(hang_commit=False, hang_stop=True)
    hist_b = HangingOwnedHistoryReader()
    coord_b = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        config=CorrelationRuntimeConfig(shutdown_grace_seconds=0.5),
        consumer_factory=lambda *a, **kw: cons_b,
        history_reader_factory=lambda *a, **kw: hist_b,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord_b.start()

    t1 = time.monotonic()
    report_b = await coord_b.shutdown()
    elapsed_b = time.monotonic() - t1
    assert report_b.state is CorrelationRuntimeState.STOPPED
    assert cons_b.stop_calls == 1
    assert hist_b.stop_calls == 1
    assert report_b.closed_owned_consumer is False
    assert report_b.closed_owned_history_reader is False
    assert elapsed_b < 0.85

    # Subcase C: commit takes 0.2s, leaving 0.3s of the 0.5s grace budget for both hanging stops ->
    # total wall time remains bounded by the single 0.5s grace period!
    class DelayedCommitConsumer(HangingOwnedConsumer):
        async def commit(
            self, offsets: dict[TopicPartition, int] | None = None
        ) -> None:
            await asyncio.sleep(0.2)
            await FakeKafkaConsumer.commit(self, offsets)

    cons_c = DelayedCommitConsumer(hang_commit=False, hang_stop=True)
    hist_c = HangingOwnedHistoryReader()
    coord_c = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        config=CorrelationRuntimeConfig(shutdown_grace_seconds=0.5),
        consumer_factory=lambda *a, **kw: cons_c,
        history_reader_factory=lambda *a, **kw: hist_c,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord_c.start()
    tracker_c = PartitionOffsetTracker({tp0: 10})
    tracker_c.observe(tp0, 10, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED)
    coord_c._offset_tracker = tracker_c

    t2 = time.monotonic()
    report_c = await coord_c.shutdown()
    elapsed_c = time.monotonic() - t2
    assert report_c.state is CorrelationRuntimeState.STOPPED
    assert cons_c.committed_positions[tp0] == 11
    assert cons_c.stop_calls == 1
    assert hist_c.stop_calls == 1
    assert elapsed_c < 0.85

    # Grace expiry leaves retained cleanup running; drain it before closing the loop.
    await asyncio.gather(
        *(
            task
            for coord in (coord_a, coord_b, coord_c)
            for task in coord._cleanup_tasks.values()
        )
    )
    await asyncio.sleep(0)
    assert all(
        coord.unclosed_owned_handle_count == 0 for coord in (coord_a, coord_b, coord_c)
    )


@pytest.mark.asyncio
async def test_rt1_r1_blocker4_failed_start_cleans_up_partially_started_owned_handles(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """Blocker 4: partially started owned consumers, history readers, and readiness probes always receive stop()."""
    db = patched_persistence
    tp0 = TopicPartition(KafkaTopic.ANOMALIES.value, 0)

    class PartiallyStartingHandle(FakeKafkaConsumer):
        def __init__(self, *, fail_start: bool) -> None:
            super().__init__(batches=[], assigned_partitions=[tp0])
            self._fail_start = fail_start
            self.stop_calls = 0

        async def start(self) -> None:
            self.started = True
            if self._fail_start:
                raise ConnectionError("broker handshake failed mid-start")

        async def stop(self) -> None:
            self.stop_calls += 1
            await super().stop()

    # Case A: owned consumer fails mid-start -> stop() MUST be called
    cons_fail = PartiallyStartingHandle(fail_start=True)
    coord_a = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer_factory=lambda *a, **kw: cons_fail,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    with pytest.raises(RuntimeStartupError):
        await coord_a.start()
    assert coord_a.state is CorrelationRuntimeState.FAILED
    assert cons_fail.stop_calls == 1

    # Case B: owned consumer starts, then owned history reader fails mid-start -> BOTH receive stop()!
    cons_ok = PartiallyStartingHandle(fail_start=False)
    hist_fail = PartiallyStartingHandle(fail_start=True)
    coord_b = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer_factory=lambda *a, **kw: cons_ok,
        history_reader_factory=lambda *a, **kw: hist_fail,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    with pytest.raises(RuntimeStartupError):
        await coord_b.start()
    assert coord_b.state is CorrelationRuntimeState.FAILED
    assert cons_ok.stop_calls == 1
    assert hist_fail.stop_calls == 1

    # Case C: readiness probe consumer fails mid-start -> stop() MUST be called
    probe_fail = PartiallyStartingHandle(fail_start=True)
    readiness = await check_correlation_runtime_readiness(
        session_factory=db.session_factory,
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        kafka_consumer_factory=lambda **kw: probe_fail,
        verify_db_fn=_noop_verify_role,
    )
    assert readiness.ready is False
    assert "KAFKA_NOT_READY" in readiness.error_codes
    assert probe_fail.stop_calls == 1

    # Case D: ephemeral history reader created during _fetch_history_coordinates fails mid-start -> stop() called!
    s1 = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    rec1, inp1 = _make_anomaly_kafka_record(s1, partition=0, offset=10)
    plan, plan_sha = _build_plan_dict(
        source_records=[_source_entry("S1", rec1, inp1)],
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
    ephemeral_hist = PartiallyStartingHandle(fail_start=True)
    # Start coordinator with caller consumer already at committed=11 (forcing history fetch for offset 10),
    # and attach history_reader_factory only after start so _fetch_history_coordinates creates an ephemeral reader.
    coord_d = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=FakeKafkaConsumer(
            batches=[],
            assigned_partitions=[tp0],
            initial_committed={tp0: 11},
        ),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord_d.start()
    coord_d._history_reader_factory = lambda *a, **kw: ephemeral_hist
    with pytest.raises(ConnectionError, match="handshake failed"):
        await coord_d.run_plan(plan=plan, expected_plan_sha256=plan_sha)
    assert ephemeral_hist.stop_calls == 1
    await coord_d.shutdown()


@pytest.mark.asyncio
async def test_rt1_r2_real_dispatch_bookkeeping_does_not_extend_deadline(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    """The real D1 cancellation handler stays owned after RT1's one-second return."""
    db = patched_persistence
    release = asyncio.Event()
    bookkeeping_started = asyncio.Event()

    class DelayedRepository(InMemoryDispatchRepository):
        async def record_ambiguous_outcome(self, **kwargs: Any) -> Any:
            bookkeeping_started.set()
            await release.wait()
            return await super().record_ambiguous_outcome(**kwargs)

    class SlowPublisher(RecordingPublisher):
        async def publish(
            self, record: IncidentPublicationRecord
        ) -> IncidentPublishOutcome:
            await asyncio.sleep(10)
            return await super().publish(record)

    publisher = SlowPublisher()
    dispatch = IncidentDispatchService(
        session_factory=db.session_factory,
        publisher=publisher,
        repository_factory=lambda session, tenant: DelayedRepository(
            db.dispatch_store, tenant
        ),
        verify_role_fn=_noop_verify_role,
    )
    signal = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    raw, inp = _make_anomaly_kafka_record(signal, partition=0, offset=10)
    plan, digest = _build_plan_dict(
        source_records=[_source_entry("S1", raw, inp)],
        steps=[
            dict(op="ingest", delivery_id="S1", logical_time="2026-11-27T00:01:00Z"),
            dict(op="evaluate", logical_time="2026-11-27T00:01:05Z"),
        ],
        run_end_logical_time="2026-11-27T00:01:05Z",
    )
    tp = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    consumer = FakeKafkaConsumer(
        batches=[[raw]], assigned_partitions=[tp], initial_committed={tp: 10}
    )
    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=dispatch,
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer=consumer,
        config=CorrelationRuntimeConfig(max_run_duration_seconds=1),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord.start()
    started = time.monotonic()
    result = await coord.run_plan(plan=plan, expected_plan_sha256=digest)
    elapsed = time.monotonic() - started
    try:
        assert elapsed < 1.3
        assert result.status is CorrelationRuntimeRunStatus.INCOMPLETE_DEADLINE_EXCEEDED
        assert not result.all_settled
        assert result.unresolved_operation_count > 0
        await asyncio.wait_for(bookkeeping_started.wait(), 0.5)
        assert coord.pending_operation_count > 0
        with pytest.raises(RuntimeCapacityError, match="still settling"):
            await coord.run_plan(plan=plan, expected_plan_sha256=digest)
        assert consumer.commit_history == []
        assert publisher.published == []
        print(
            f"R2 real D1 deadline: {elapsed:.6f}s; retained={coord.pending_operation_count}"
        )
    finally:
        release.set()
        await asyncio.gather(*coord._pending_operations, return_exceptions=True)
        await asyncio.sleep(0)
        await coord.shutdown()
    assert coord.pending_operation_count == 0
    assert (
        list(db.dispatch_store.dispatches.values())[0].dispatch_state
        is DispatchState.AMBIGUOUS_OUTCOME
    )
    assert consumer.committed_positions[tp] == 10


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_cancel", [False, True])
async def test_rt1_r2_bounded_wait_retains_cancellation_and_late_errors(
    parent_cancel: bool,
) -> None:
    pending: set[asyncio.Future[Any]] = set()
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    errors: list[Any] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: errors.append(context))

    async def slow_cancel() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
            raise ValueError("late cleanup failure")

    try:
        waiter = asyncio.create_task(
            runtime_mod._await_before_deadline(
                slow_cancel(),
                deadline=time.monotonic() + (10 if parent_cancel else 0.05),
                pending=pending,
            )
        )
        await entered.wait()
        if parent_cancel:
            waiter.cancel("caller cancelled")
        with pytest.raises(asyncio.CancelledError if parent_cancel else TimeoutError):
            await waiter
        await cancelled.wait()
        assert len(pending) == 1
        child = next(iter(pending))
        assert child in runtime_mod._RUNTIME_OPERATIONS
        release.set()
        await asyncio.wait({child}, timeout=0.5)
        await asyncio.sleep(0)
        assert not pending
        assert child not in runtime_mod._RUNTIME_OPERATIONS
        assert (
            child._log_traceback is False
        )  # Exception was retrieved by the done callback.
        assert errors == []
    finally:
        release.set()
        loop.set_exception_handler(previous)


@pytest.mark.asyncio
async def test_rt1_r2_exhausted_commit_grace_retains_both_handles_and_resumes_cleanup(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    tp = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
    release, commit_started = asyncio.Event(), asyncio.Event()

    class Consumer(FakeKafkaConsumer):
        stop_calls = 0
        commit_calls = 0

        async def commit(
            self, offsets: dict[TopicPartition, int] | None = None
        ) -> None:
            self.commit_calls += 1
            commit_started.set()
            await asyncio.Event().wait()

        async def stop(self) -> None:
            self.stop_calls += 1
            await release.wait()
            await super().stop()

    class History(FakeHistoryReader):
        stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1
            await release.wait()
            await super().stop()

    consumer = Consumer(
        batches=[], assigned_partitions=[tp], initial_committed={tp: 10}
    )
    history = History({})
    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer_factory=lambda *args, **kw: consumer,
        history_reader_factory=lambda **kw: history,
        config=CorrelationRuntimeConfig(shutdown_grace_seconds=0.5),
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord.start()
    coord._offset_tracker = PartitionOffsetTracker({tp: 10})
    coord._offset_tracker.observe(
        tp, 10, state=DeliverySettlementState.SETTLED_ACKNOWLEDGED
    )
    started = time.monotonic()
    with pytest.raises(RuntimeOffsetCommitError):
        await coord.shutdown()
    elapsed = time.monotonic() - started
    assert elapsed < 0.85
    assert commit_started.is_set()
    assert coord.unclosed_owned_handle_count == 2
    assert (
        coord._active_consumer is consumer and coord._active_history_reader is history
    )
    await asyncio.sleep(0)
    assert consumer.stop_calls == history.stop_calls == 1
    report = await coord.shutdown()
    assert report.unclosed_owned_handle_count == 2
    assert report.unresolved_operation_count == 2
    assert not report.closed_owned_consumer and not report.closed_owned_history_reader
    release.set()
    report = await coord.shutdown()
    assert report.closed_owned_consumer and report.closed_owned_history_reader
    assert coord.unclosed_owned_handle_count == coord.pending_operation_count == 0
    assert consumer.stop_calls == history.stop_calls == consumer.commit_calls == 1
    assert coord._offset_tracker.snapshot_confirmed_committed()[tp] == 10
    print(f"R2 exhausted commit grace: {elapsed:.6f}s; both stops resumed once")


@pytest.mark.asyncio
async def test_rt1_r2_failed_stop_is_retryable_and_caller_handles_are_not_closed(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence

    class RetryConsumer(FakeKafkaConsumer):
        stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1
            if self.stop_calls == 1:
                raise ConnectionError("stop failed")
            await super().stop()

    consumer = RetryConsumer(batches=[], assigned_partitions=[])
    caller_history = FakeHistoryReader({})
    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer_factory=lambda *args, **kw: consumer,
        history_reader=caller_history,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord.start()
    first = await coord.shutdown()
    assert not first.closed_owned_consumer and first.unclosed_owned_handle_count == 1
    assert coord._active_consumer is consumer
    second = await coord.shutdown()
    assert second.closed_owned_consumer and second.unclosed_owned_handle_count == 0
    assert consumer.stop_calls == 2
    assert not caller_history.stopped


@pytest.mark.asyncio
async def test_rt1_r2_cancelled_shutdown_retains_cleanup(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence
    entered, release = asyncio.Event(), asyncio.Event()

    class Consumer(FakeKafkaConsumer):
        stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1
            entered.set()
            await release.wait()
            await super().stop()

    consumer = Consumer(batches=[], assigned_partitions=[])
    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer_factory=lambda *args, **kw: consumer,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord.start()
    closing = asyncio.create_task(coord.shutdown())
    await entered.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert coord.unclosed_owned_handle_count == coord.pending_operation_count == 1
    release.set()
    report = await coord.shutdown()
    assert report.closed_owned_consumer
    assert report.unclosed_owned_handle_count == report.unresolved_operation_count == 0
    assert consumer.stop_calls == 1


@pytest.mark.asyncio
async def test_rt1_r2_immediate_stop_is_not_repeated_before_done_callback(
    patched_persistence: InMemorySharedDatabase,
) -> None:
    db = patched_persistence

    class Consumer(FakeKafkaConsumer):
        stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1
            await super().stop()

    consumer = Consumer(batches=[], assigned_partitions=[])
    coord = CorrelationRuntimeCoordinator(
        session_factory=db.session_factory,
        dispatch_service=_make_dispatch_service(db, RecordingPublisher()),
        topology_provider=CanonicalTopologyProvider(),
        expected_topology=_pinned_topology_ref(),
        consumer_factory=lambda *args, **kw: consumer,
        persistence_repo_factory=db.make_persistence_repo,
        verify_role_fn=_noop_verify_role,
    )
    await coord.start()
    assert await coord._close_owned_handles_best_effort(deadline=time.monotonic()) == (
        False,
        False,
    )
    task = coord._cleanup_tasks["consumer"]
    await asyncio.sleep(0)
    assert task.done() and coord._owns_consumer
    report = await coord.shutdown()
    assert report.closed_owned_consumer
    assert report.unclosed_owned_handle_count == 0
    assert consumer.stop_calls == 1
    await asyncio.sleep(0)
    assert coord.pending_operation_count == 0
