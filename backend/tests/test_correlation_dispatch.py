"""Task 4.12-D1 unit tests: durable dispatch state machine, fencing & recovery.

Exercises ``IncidentDispatchService``, ``DispatchConfig``, ``DispatchStagingContext``,
frozen wire decoding, all approved transitions TR-01..TR-11, predecessor-intent
gating, pre-send active-claim verification, single-invocation-per-token ownership,
FIX-03/04/05/07/09/15/16 semantics, and cancellation propagation without requiring
live Kafka, PostgreSQL, or Neo4j.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time
from typing import Any
from uuid import UUID

import pytest

from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import AnomalySignal
from app.correlation.aggregate import create_or_update_incident
from app.correlation.blast_radius import (
    BlastRadiusPolicy,
    BlastRadiusTopologyMismatchError,
)
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.dispatch_repository import (
    DispatchClaimLostError,
    DispatchConfigurationError,
    DispatchCorruptionError,
    DispatchInputError,
    DispatchState,
    IncidentDispatchRepository,
    PersistedPublicationDispatch,
    PredecessorCheckResult,
    StageDispatchRecordResult,
    UnsendableDispatchDraft,
    decode_dispatch_publication_record,
)
from app.correlation.dispatch_service import (
    ClaimedDispatchAttempt,
    DispatchConfig,
    DispatchOutcomeStatus,
    DispatchStagingContext,
    IncidentDispatchService,
)
from app.correlation.grouping import group_candidate_signals
from app.correlation.models import (
    CorrelationWindowState,
    IncidentAggregate,
    TopologySnapshotReference,
)
from app.correlation.persistence_repository import PersistedPublicationIntent
from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    ENVELOPE_SCHEMA_VERSION,
    HANDOFF_SCHEMA_VERSION,
    MAX_KAFKA_VALUE_BYTES,
    PUBLICATION_CONTRACT_VERSION,
    IncidentPublicationRecord,
    IncidentPublicationSizeLimitError,
    build_incident_publication_from_intent,
)
from app.correlation.publisher import (
    IncidentPublishAmbiguousError,
    IncidentPublishContractError,
    IncidentPublishOutcome,
    IncidentPublishRetriableError,
    IncidentPublishStatus,
    IncidentPublishUnavailableError,
)
from app.correlation.scoring import score_candidate_pair
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.telemetry.transport.producer import PublishResult
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


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


# ============================================================================
# IN-MEMORY CONTROLLABLE REPOSITORY & SESSION DOUBLE FOR PURE UNIT PROOFS
# ============================================================================


class FakeClock:
    def __init__(self, initial: datetime | None = None) -> None:
        self.now = initial or datetime(2026, 11, 27, 12, 0, 0, tzinfo=timezone.utc)

    def advance(self, seconds: float) -> datetime:
        self.now = self.now + timedelta(seconds=seconds)
        return self.now


class FakeSession:
    def __init__(self) -> None:
        self._in_tx = False

    def in_transaction(self) -> bool:
        return self._in_tx

    @asynccontextmanager
    async def begin(self) -> Any:
        self._in_tx = True
        try:
            yield self
        finally:
            self._in_tx = False

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._in_tx = False


class InMemoryDispatchStore:
    """Tenant-isolated in-memory store modeling PostgreSQL D1 semantics."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.intents: dict[tuple[UUID, UUID, int], PersistedPublicationIntent] = {}
        self.dispatches: dict[tuple[UUID, UUID, int], PersistedPublicationDispatch] = {}
        self.row_write_ops: int = 0

    def add_committed_intent(self, intent: PersistedPublicationIntent) -> None:
        self.intents[(intent.tenant_id, intent.incident_id, intent.revision)] = intent

    def make_repo(self, session: Any, tenant_id: UUID) -> IncidentDispatchRepository:
        return InMemoryDispatchRepository(self, tenant_id)


class InMemoryDispatchRepository:
    def __init__(self, store: InMemoryDispatchStore, tenant_id: UUID) -> None:
        self._store = store
        self._tenant_id = tenant_id
        self._bound = False

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    async def bind_tenant_context(self) -> None:
        self._bound = True

    async def get_server_utc_time(self) -> datetime:
        assert self._bound
        return self._store.clock.now

    async def load_publication_intent(
        self, incident_id: UUID, revision: int
    ) -> PersistedPublicationIntent | None:
        assert self._bound
        return self._store.intents.get((self._tenant_id, incident_id, revision))

    async def list_incident_publication_intents(
        self, incident_id: UUID
    ) -> tuple[PersistedPublicationIntent, ...]:
        assert self._bound
        matches = [
            v
            for (tid, iid, _), v in self._store.intents.items()
            if tid == self._tenant_id and iid == incident_id
        ]
        return tuple(sorted(matches, key=lambda x: x.revision))

    async def list_unstaged_publication_intents(
        self, *, limit: int, incident_id: UUID | None = None
    ) -> tuple[PersistedPublicationIntent, ...]:
        assert self._bound
        out = [
            v
            for key, v in self._store.intents.items()
            if key[0] == self._tenant_id
            and (incident_id is None or key[1] == incident_id)
            and key not in self._store.dispatches
        ]
        out.sort(key=lambda x: (str(x.incident_id), x.revision))
        return tuple(out[:limit])

    async def load_dispatch(
        self, incident_id: UUID, revision: int, *, verify_wire: bool = True
    ) -> PersistedPublicationDispatch | None:
        assert self._bound
        return self._store.dispatches.get((self._tenant_id, incident_id, revision))

    async def list_incident_dispatches(
        self, incident_id: UUID
    ) -> tuple[PersistedPublicationDispatch, ...]:
        assert self._bound
        matches = [
            v
            for (tid, iid, _), v in self._store.dispatches.items()
            if tid == self._tenant_id and iid == incident_id
        ]
        return tuple(sorted(matches, key=lambda x: x.revision))

    async def inspect_predecessors(
        self, incident_id: UUID, revision: int
    ) -> PredecessorCheckResult:
        assert self._bound
        expected = revision - 1
        intent_count = sum(
            1
            for (tid, iid, rev) in self._store.intents
            if tid == self._tenant_id and iid == incident_id and rev < revision
        )
        acked_count = sum(
            1
            for (tid, iid, rev), row in self._store.dispatches.items()
            if tid == self._tenant_id
            and iid == incident_id
            and rev < revision
            and row.dispatch_state is DispatchState.ACKNOWLEDGED
        )
        return PredecessorCheckResult(
            expected_predecessors=expected,
            committed_predecessor_intents=intent_count,
            acknowledged_predecessor_dispatches=acked_count,
        )

    async def stage_pending_dispatch(
        self, record: IncidentPublicationRecord
    ) -> StageDispatchRecordResult:
        assert self._bound
        ident = record.identity
        key = (ident.tenant_id, ident.incident_id, ident.revision)
        if key not in self._store.intents:
            raise DispatchInputError("Missing committed intent FK")
        if key in self._store.dispatches:
            return StageDispatchRecordResult(
                dispatch=self._store.dispatches[key], created=False
            )
        now = self._store.clock.now
        row = PersistedPublicationDispatch(
            tenant_id=ident.tenant_id,
            incident_id=ident.incident_id,
            revision=ident.revision,
            revision_fingerprint=ident.revision_fingerprint,
            publication_contract_version=ident.publication_contract_version,
            envelope_schema_version=ident.envelope_schema_version,
            handoff_schema_version=ident.handoff_schema_version,
            event_id=record.event_id,
            publication_fingerprint=record.publication_fingerprint,
            handoff_id=ident.handoff_id,
            handoff_fingerprint=ident.handoff_fingerprint,
            topology_id=ident.topology_id,
            topology_version=ident.topology_version,
            topology_snapshot_fingerprint=ident.topology_snapshot_fingerprint,
            correlation_policy_version=ident.correlation_policy_version,
            correlation_config_fingerprint=ident.correlation_config_fingerprint,
            intent_time=ident.intent_time,
            kafka_topic=record.topic.value,
            kafka_key=record.kafka_key,
            kafka_value=record.kafka_value,
            kafka_headers_json=tuple((p[0], p[1]) for p in record.kafka_headers_json),
            wire_record_bytes=record.estimated_record_bytes,
            created_at=now,
            dispatch_state=DispatchState.PENDING,
            attempt_count=0,
            claim_token=None,
            claimed_by=None,
            claimed_at=None,
            claim_expires_at=None,
            last_attempt_at=None,
            next_retry_at=None,
            last_error_code=None,
            last_error_message=None,
            broker_topic=None,
            broker_partition=None,
            broker_offset=None,
            broker_timestamp_ms=None,
            acknowledged_at=None,
            updated_at=now,
        )
        self._store.dispatches[key] = row
        self._store.row_write_ops += 1
        return StageDispatchRecordResult(dispatch=row, created=True)

    async def stage_rejected_unsendable_dispatch(
        self, draft: UnsendableDispatchDraft
    ) -> StageDispatchRecordResult:
        assert self._bound
        key = (draft.tenant_id, draft.incident_id, draft.revision)
        if key not in self._store.intents:
            raise DispatchInputError("Missing committed intent FK")
        if key in self._store.dispatches:
            return StageDispatchRecordResult(
                dispatch=self._store.dispatches[key], created=False
            )
        now = self._store.clock.now
        row = PersistedPublicationDispatch(
            tenant_id=draft.tenant_id,
            incident_id=draft.incident_id,
            revision=draft.revision,
            revision_fingerprint=draft.revision_fingerprint,
            publication_contract_version=PUBLICATION_CONTRACT_VERSION,
            envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
            handoff_schema_version=HANDOFF_SCHEMA_VERSION,
            event_id=draft.event_id,
            publication_fingerprint=draft.publication_fingerprint,
            handoff_id=draft.handoff_id,
            handoff_fingerprint=draft.handoff_fingerprint,
            topology_id=draft.topology_id,
            topology_version=draft.topology_version,
            topology_snapshot_fingerprint=draft.topology_snapshot_fingerprint,
            correlation_policy_version=draft.correlation_policy_version,
            correlation_config_fingerprint=draft.correlation_config_fingerprint,
            intent_time=draft.intent_time,
            kafka_topic=draft.kafka_topic,
            kafka_key=draft.kafka_key,
            kafka_value=None,
            kafka_headers_json=draft.kafka_headers_json,
            wire_record_bytes=draft.wire_record_bytes,
            created_at=now,
            dispatch_state=DispatchState.REJECTED_UNSENDABLE,
            attempt_count=0,
            claim_token=None,
            claimed_by=None,
            claimed_at=None,
            claim_expires_at=None,
            last_attempt_at=None,
            next_retry_at=None,
            last_error_code=draft.error_code,
            last_error_message=draft.error_message,
            broker_topic=None,
            broker_partition=None,
            broker_offset=None,
            broker_timestamp_ms=None,
            acknowledged_at=None,
            updated_at=now,
        )
        self._store.dispatches[key] = row
        self._store.row_write_ops += 1
        return StageDispatchRecordResult(dispatch=row, created=True)

    async def claim_dispatch(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        claimed_by: str,
        claim_lease_seconds: int,
        max_dispatch_attempts: int,
    ) -> PersistedPublicationDispatch | None:
        assert self._bound
        key = (self._tenant_id, incident_id, revision)
        row = self._store.dispatches.get(key)
        if row is None or row.attempt_count >= max_dispatch_attempts:
            return None
        pred = await self.inspect_predecessors(incident_id, revision)
        if not pred.satisfied:
            return None
        now = self._store.clock.now
        eligible = (
            row.dispatch_state is DispatchState.PENDING
            or (
                row.dispatch_state is DispatchState.RETRY_SCHEDULED
                and row.next_retry_at is not None
                and now >= row.next_retry_at
            )
            or (
                row.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
                and row.claim_expires_at is not None
                and now >= row.claim_expires_at
                and (row.next_retry_at is None or now >= row.next_retry_at)
            )
            or (
                row.dispatch_state is DispatchState.IN_FLIGHT
                and row.claim_expires_at is not None
                and now >= row.claim_expires_at
                and (row.next_retry_at is None or now >= row.next_retry_at)
            )
        )
        if not eligible:
            return None
        updated = replace(
            row,
            dispatch_state=DispatchState.IN_FLIGHT,
            attempt_count=row.attempt_count + 1,
            claim_token=claim_token,
            claimed_by=claimed_by,
            claimed_at=now,
            claim_expires_at=now + timedelta(seconds=claim_lease_seconds),
            last_attempt_at=now,
            next_retry_at=None,
            updated_at=max(now, row.updated_at),
        )
        self._store.dispatches[key] = updated
        self._store.row_write_ops += 1
        return updated

    async def verify_active_claim(
        self, *, incident_id: UUID, revision: int, claim_token: UUID
    ) -> bool:
        return (
            await self.load_active_claimed_dispatch(
                incident_id=incident_id,
                revision=revision,
                claim_token=claim_token,
            )
            is not None
        )

    async def load_active_claimed_dispatch(
        self, *, incident_id: UUID, revision: int, claim_token: UUID
    ) -> PersistedPublicationDispatch | None:
        assert self._bound
        row = self._store.dispatches.get((self._tenant_id, incident_id, revision))
        if row is None:
            return None
        now = self._store.clock.now
        if (
            row.dispatch_state is DispatchState.IN_FLIGHT
            and row.claim_token == claim_token
            and row.claim_expires_at is not None
            and row.claim_expires_at > now
        ):
            return row
        return None

    async def mark_expired_in_flight_exhausted(
        self,
        *,
        incident_id: UUID,
        revision: int,
        max_dispatch_attempts: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch | None:
        assert self._bound
        key = (self._tenant_id, incident_id, revision)
        row = self._store.dispatches.get(key)
        now = self._store.clock.now
        if (
            row is None
            or row.dispatch_state is not DispatchState.IN_FLIGHT
            or row.claim_expires_at is None
            or now < row.claim_expires_at
            or row.attempt_count < max_dispatch_attempts
        ):
            return None
        updated = replace(
            row,
            dispatch_state=DispatchState.EXHAUSTED,
            claim_token=None,
            next_retry_at=None,
            last_error_code=error_code,
            last_error_message=error_message,
            updated_at=max(now, row.updated_at),
        )
        self._store.dispatches[key] = updated
        self._store.row_write_ops += 1
        return updated

    async def record_acknowledged(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        publish_result: PublishResult,
        expected_record: IncidentPublicationRecord | None = None,
    ) -> PersistedPublicationDispatch:
        assert self._bound
        key = (self._tenant_id, incident_id, revision)
        row = self._store.dispatches.get(key)
        if (
            row is None
            or row.dispatch_state is not DispatchState.IN_FLIGHT
            or row.claim_token != claim_token
        ):
            raise DispatchClaimLostError("Stale claim token rejected on ACK")
        if row.kafka_value is None or publish_result.serialized_bytes != len(
            row.kafka_value
        ):
            raise DispatchInputError(
                "Receipt serialized_bytes does not match stored dispatch wire bytes"
            )
        if expected_record is not None and (
            expected_record.event_id != row.event_id
            or expected_record.publication_fingerprint != row.publication_fingerprint
            or expected_record.kafka_value != row.kafka_value
            or expected_record.kafka_key != row.kafka_key
            or tuple((p[0], p[1]) for p in expected_record.kafka_headers_json)
            != row.kafka_headers_json
            or expected_record.estimated_record_bytes != row.wire_record_bytes
        ):
            raise DispatchInputError(
                "Expected publication record does not match stored dispatch wire identity"
            )
        now = self._store.clock.now
        updated = replace(
            row,
            dispatch_state=DispatchState.ACKNOWLEDGED,
            claim_token=None,
            next_retry_at=None,
            last_error_code=None,
            last_error_message=None,
            broker_topic=publish_result.topic,
            broker_partition=publish_result.partition,
            broker_offset=publish_result.offset,
            broker_timestamp_ms=publish_result.timestamp_ms,
            acknowledged_at=now,
            updated_at=max(now, row.updated_at),
        )
        self._store.dispatches[key] = updated
        self._store.row_write_ops += 1
        return updated

    async def record_retry_scheduled(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        max_dispatch_attempts: int,
        backoff_ms: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch:
        assert self._bound
        key = (self._tenant_id, incident_id, revision)
        row = self._store.dispatches.get(key)
        if (
            row is None
            or row.dispatch_state is not DispatchState.IN_FLIGHT
            or row.claim_token != claim_token
            or row.attempt_count >= max_dispatch_attempts
        ):
            raise DispatchClaimLostError("Stale claim token rejected on retry schedule")
        now = self._store.clock.now
        updated = replace(
            row,
            dispatch_state=DispatchState.RETRY_SCHEDULED,
            claim_token=None,
            claim_expires_at=None,
            next_retry_at=now + timedelta(milliseconds=backoff_ms),
            last_error_code=error_code,
            last_error_message=error_message,
            updated_at=max(now, row.updated_at),
        )
        self._store.dispatches[key] = updated
        self._store.row_write_ops += 1
        return updated

    async def record_ambiguous_outcome(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        max_dispatch_attempts: int,
        backoff_ms: int,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch:
        assert self._bound
        key = (self._tenant_id, incident_id, revision)
        row = self._store.dispatches.get(key)
        if (
            row is None
            or row.dispatch_state is not DispatchState.IN_FLIGHT
            or row.claim_token != claim_token
            or row.attempt_count >= max_dispatch_attempts
        ):
            raise DispatchClaimLostError("Stale claim token rejected on ambiguous")
        now = self._store.clock.now
        updated = replace(
            row,
            dispatch_state=DispatchState.AMBIGUOUS_OUTCOME,
            claim_token=None,
            next_retry_at=now + timedelta(milliseconds=backoff_ms),
            last_error_code=error_code,
            last_error_message=error_message,
            updated_at=max(now, row.updated_at),
        )
        self._store.dispatches[key] = updated
        self._store.row_write_ops += 1
        return updated

    async def record_exhausted(
        self,
        *,
        incident_id: UUID,
        revision: int,
        claim_token: UUID,
        error_code: str,
        error_message: str,
    ) -> PersistedPublicationDispatch:
        assert self._bound
        key = (self._tenant_id, incident_id, revision)
        row = self._store.dispatches.get(key)
        if (
            row is None
            or row.dispatch_state is not DispatchState.IN_FLIGHT
            or row.claim_token != claim_token
        ):
            raise DispatchClaimLostError("Stale claim token rejected on exhausted")
        now = self._store.clock.now
        updated = replace(
            row,
            dispatch_state=DispatchState.EXHAUSTED,
            claim_token=None,
            next_retry_at=None,
            last_error_code=error_code,
            last_error_message=error_message,
            updated_at=max(now, row.updated_at),
        )
        self._store.dispatches[key] = updated
        self._store.row_write_ops += 1
        return updated

    async def list_recoverable_dispatches(
        self, *, limit: int, incident_id: UUID | None = None
    ) -> tuple[PersistedPublicationDispatch, ...]:
        assert self._bound
        now = self._store.clock.now
        out = [
            row
            for (tid, iid, _), row in self._store.dispatches.items()
            if tid == self._tenant_id
            and (incident_id is None or iid == incident_id)
            and (
                row.dispatch_state is DispatchState.PENDING
                or (
                    row.dispatch_state is DispatchState.RETRY_SCHEDULED
                    and row.next_retry_at is not None
                    and now >= row.next_retry_at
                )
                or (
                    row.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
                    and row.claim_expires_at is not None
                    and now >= row.claim_expires_at
                    and (row.next_retry_at is None or now >= row.next_retry_at)
                )
                or (
                    row.dispatch_state is DispatchState.IN_FLIGHT
                    and row.claim_expires_at is not None
                    and now >= row.claim_expires_at
                    and (row.next_retry_at is None or now >= row.next_retry_at)
                )
            )
        ]
        out.sort(key=lambda r: (str(r.incident_id), r.revision))
        return tuple(out[:limit])


class ScriptedPublisher:
    """Deterministic publisher double capturing exact sent records and scripted results."""

    def __init__(self, script: list[Any] | None = None) -> None:
        self.script: list[Any] = list(script or [])
        self.sent_records: list[IncidentPublicationRecord] = []
        self._offset = 100

    async def publish(
        self, record: IncidentPublicationRecord
    ) -> IncidentPublishOutcome:
        self.sent_records.append(record)
        if self.script:
            action = self.script.pop(0)
            if callable(action):
                return await action(record)
            if isinstance(action, BaseException):
                raise action
            return action
        self._offset += 1
        return IncidentPublishOutcome(
            IncidentPublishStatus.ACKNOWLEDGED,
            result=PublishResult(
                topic=record.topic.value,
                partition=0,
                offset=self._offset,
                timestamp_ms=1_795_737_665_000,
                latency_ms=1.5,
                serialized_bytes=len(record.kafka_value),
            ),
        )


async def _noop_verify_role(session: Any) -> None:
    return None


@pytest.fixture(scope="module")
def fix01_context() -> dict[str, Any]:
    snapshot = asyncio.run(CanonicalTopologyProvider().get_snapshot())
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
    )
    res1 = create_or_update_incident(
        group=g1.groups[0],
        signal_inputs={inputs["S1"].signal.signal_id: inputs["S1"]},
        processing_time=t1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )

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
    )
    res2 = create_or_update_incident(
        group=g2.groups[0],
        signal_inputs={
            inputs["S1"].signal.signal_id: inputs["S1"],
            inputs["S2"].signal.signal_id: inputs["S2"],
        },
        processing_time=t2,
        desired_correlation_state=CorrelationWindowState.SETTLING,
        existing_aggregate=res1.aggregate,
    )

    def _intent(agg: IncidentAggregate) -> PersistedPublicationIntent:
        return PersistedPublicationIntent(
            tenant_id=agg.tenant_id,
            incident_id=agg.incident_id,
            revision=agg.revision,
            schema_version=agg.schema_version,
            revision_fingerprint=agg.revision_fingerprint,
            intent_time=agg.updated_at,
            aggregate=agg,
        )

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
        "rev1_intent": _intent(res1.aggregate),
        "rev2_intent": _intent(res2.aggregate),
        "tenant_id": res1.aggregate.tenant_id,
        "incident_id": res1.aggregate.incident_id,
    }


def _make_service(
    store: InMemoryDispatchStore,
    publisher: ScriptedPublisher,
    config: DispatchConfig | None = None,
) -> IncidentDispatchService:
    return IncidentDispatchService(
        session_factory=lambda: FakeSession(),  # type: ignore[arg-type]
        publisher=publisher,
        config=config or DispatchConfig(),
        repository_factory=store.make_repo,
        verify_role_fn=_noop_verify_role,
    )


# ============================================================================
# FIX-16: COHERENT TIMING & OPERATIONAL BOUNDS VALIDATION
# ============================================================================


def test_dispatch_config_accepts_approved_defaults_and_coherent_timing() -> None:
    cfg = DispatchConfig()
    assert cfg.claim_lease_seconds == 30
    assert cfg.publish_timeout_seconds == 5.0
    assert cfg.lease_settlement_margin_seconds == 5.0
    assert cfg.max_dispatch_attempts == 3
    assert cfg.compute_retry_backoff_ms(1) == 250
    assert cfg.compute_retry_backoff_ms(2) == 500
    assert cfg.compute_retry_backoff_ms(3) == 1000
    assert cfg.compute_retry_backoff_ms(10) == 5000


@pytest.mark.parametrize(
    "kwargs",
    [
        # FIX-16 invalid pair: lease=5 < timeout=60 + margin=5
        {
            "claim_lease_seconds": 5,
            "publish_timeout_seconds": 60.0,
            "lease_settlement_margin_seconds": 5.0,
        },
        # Boundary violation: lease=9 < 5 + 5
        {
            "claim_lease_seconds": 9,
            "publish_timeout_seconds": 5.0,
            "lease_settlement_margin_seconds": 5.0,
        },
        # Boolean rejections
        {"claim_lease_seconds": True},
        {"max_dispatch_attempts": True},
        {"publish_timeout_seconds": False},
        {"lease_settlement_margin_seconds": True},
        # Non-finite numbers
        {"publish_timeout_seconds": float("nan")},
        {"publish_timeout_seconds": float("inf")},
        {"lease_settlement_margin_seconds": float("nan")},
        # Range bounds
        {"max_dispatch_attempts": 0},
        {"max_dispatch_attempts": 11},
        {"retry_backoff_initial_ms": 500, "retry_backoff_max_ms": 200},
        {"claim_lease_seconds": 4},
        {"claim_lease_seconds": 601},
        {"max_recovery_batch_size": 0},
        {"max_recovery_batch_size": 257},
        {"worker_id": ""},
        {"worker_id": "  padded  "},
        {"worker_id": "bad\nworker"},
        {"worker_id": "x" * 129},
    ],
)
def test_dispatch_config_rejects_invalid_bounds_and_timing(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(DispatchConfigurationError):
        DispatchConfig(**kwargs)


# ============================================================================
# HAPPY PATH, EXACT WIRE BYTES, ORDINARY REPLAY & ASCENDING REVISIONS
# ============================================================================


@pytest.mark.asyncio
async def test_stage_dispatch_matches_wire_oracle_and_replay_is_zero_send_zero_write(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    outcome = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert outcome is not None
    assert outcome.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
    assert outcome.input_offset_committable is True
    assert outcome.sends_attempted == 1
    assert len(pub.sent_records) == 1

    oracle = WIRE_ORACLES["FIX-01-REV1"]
    sent = pub.sent_records[0]
    assert str(sent.event_id) == oracle["event_id"]
    assert sent.publication_fingerprint == oracle["publication_fingerprint"]
    assert _sha256(sent.kafka_value) == oracle["kafka_value_sha256"]
    assert outcome.dispatch.dispatch_state is DispatchState.ACKNOWLEDGED
    assert outcome.dispatch.broker_offset == 101

    writes_after_ack = store.row_write_ops
    replay = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert replay is not None
    assert replay.status is DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED
    assert replay.input_offset_committable is True
    assert replay.sends_attempted == 0
    assert len(pub.sent_records) == 1
    assert store.row_write_ops == writes_after_ack


@pytest.mark.asyncio
async def test_uncommitted_intent_is_invisible_and_emits_nothing(
    fix01_context: dict[str, Any],
) -> None:
    store = InMemoryDispatchStore(FakeClock())
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    outcome = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert outcome is None
    assert len(pub.sent_records) == 0
    assert store.row_write_ops == 0


# ============================================================================
# FIX-04: TRANSIENT FAILURE, BACKOFF, RECOVERY & EXHAUSTION
# ============================================================================


@pytest.mark.asyncio
async def test_transient_failure_schedules_retry_and_recovers_exact_frozen_bytes(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])

    retriable = IncidentPublishOutcome(
        IncidentPublishStatus.FAILED_RETRIABLE,
        error=IncidentPublishRetriableError("retryable"),
    )
    pub = ScriptedPublisher([retriable])
    cfg = DispatchConfig(max_dispatch_attempts=3, retry_backoff_initial_ms=250)
    svc = _make_service(store, pub, cfg)

    o1 = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert o1 is not None
    assert o1.status is DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION
    assert o1.input_offset_committable is False
    assert o1.dispatch.dispatch_state is DispatchState.RETRY_SCHEDULED
    assert o1.dispatch.attempt_count == 1
    assert o1.dispatch.last_error_code == "PUBLISH_RETRIABLE"

    # Before backoff elapses, recovery sweep does not claim or send.
    early = await svc.recover_pending_dispatches(tenant_id=fix01_context["tenant_id"])
    assert early.total_sends_attempted == 0
    assert len(pub.sent_records) == 1

    # Advance past 250ms backoff: recovery sends exact frozen bytes WITHOUT staging_context.
    clock.advance(0.3)
    rec = await svc.recover_pending_dispatches(tenant_id=fix01_context["tenant_id"])
    assert rec.acknowledged_count == 1
    assert rec.total_sends_attempted == 1
    assert len(pub.sent_records) == 2
    assert pub.sent_records[0].kafka_value == pub.sent_records[1].kafka_value
    assert pub.sent_records[0].kafka_key == pub.sent_records[1].kafka_key
    assert pub.sent_records[0].kafka_headers == pub.sent_records[1].kafka_headers


@pytest.mark.asyncio
async def test_exhausted_caps_at_max_dispatch_attempts_and_never_auto_resets(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    unavail = IncidentPublishOutcome(
        IncidentPublishStatus.FAILED_UNAVAILABLE,
        error=IncidentPublishUnavailableError("unavailable"),
    )
    pub = ScriptedPublisher([unavail, unavail])
    cfg = DispatchConfig(max_dispatch_attempts=2, retry_backoff_initial_ms=100)
    svc = _make_service(store, pub, cfg)

    o1 = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert o1 is not None
    assert o1.dispatch.dispatch_state is DispatchState.RETRY_SCHEDULED
    clock.advance(0.2)
    o2 = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert o2 is not None
    assert o2.status is DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED
    assert o2.input_offset_committable is False
    assert o2.dispatch.dispatch_state is DispatchState.EXHAUSTED
    assert o2.dispatch.attempt_count == 2

    # Subsequent replay or recovery never resets exhausted work.
    clock.advance(60.0)
    o3 = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert o3 is not None
    assert o3.status is DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED
    assert o3.sends_attempted == 0
    assert len(pub.sent_records) == 2


# ============================================================================
# FIX-05 & FIX-07: OVERSIZED REJECTED_UNSENDABLE VS TOPOLOGY MISMATCH
# ============================================================================


@pytest.mark.asyncio
async def test_oversized_revision_persists_rejected_unsendable_with_null_value(
    fix01_context: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryDispatchStore(FakeClock())
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    def _raise_oversized(**kwargs: Any) -> Any:
        raise IncidentPublicationSizeLimitError("exceeds 983040 bytes")

    monkeypatch.setattr(
        "app.correlation.dispatch_service.build_incident_publication_from_intent",
        _raise_oversized,
    )

    outcome = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert outcome is not None
    assert outcome.status is DispatchOutcomeStatus.COMMITTED_UNSENDABLE
    assert outcome.input_offset_committable is False
    assert outcome.dispatch.dispatch_state is DispatchState.REJECTED_UNSENDABLE
    assert outcome.dispatch.kafka_value is None
    assert outcome.dispatch.attempt_count == 0
    assert outcome.dispatch.wire_record_bytes > MAX_KAFKA_VALUE_BYTES
    assert outcome.dispatch.last_error_code == "SIZE_LIMIT_EXCEEDED"
    assert len(pub.sent_records) == 0
    # Committed intent remains intact.
    assert (
        fix01_context["tenant_id"],
        fix01_context["incident_id"],
        1,
    ) in store.intents


@pytest.mark.asyncio
async def test_mismatched_topology_pin_fails_closed_without_staging_or_sending(
    fix01_context: dict[str, Any],
) -> None:
    store = InMemoryDispatchStore(FakeClock())
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    bad_topo_ref = fix01_context["staging"].expected_topology.model_copy(
        update={"snapshot_fingerprint": "0" * 64}
    )
    bad_staging = replace(fix01_context["staging"], expected_topology=bad_topo_ref)

    with pytest.raises((BlastRadiusTopologyMismatchError, Exception)):
        await svc.dispatch_revision(
            tenant_id=fix01_context["tenant_id"],
            incident_id=fix01_context["incident_id"],
            revision=1,
            staging_context=bad_staging,
        )
    assert len(store.dispatches) == 0
    assert len(pub.sent_records) == 0


# ============================================================================
# CW-06 / FIX-03 / FIX-15: AMBIGUOUS LEASE RETENTION, PREDECESSOR GATE & STALE OWNER
# ============================================================================


@pytest.mark.asyncio
async def test_ambiguous_outcome_retains_lease_and_blocks_reclaim_until_both_expire(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])

    ambiguous = IncidentPublishOutcome(
        IncidentPublishStatus.FAILED_AMBIGUOUS,
        error=IncidentPublishAmbiguousError("timeout"),
    )
    pub = ScriptedPublisher([ambiguous])
    cfg = DispatchConfig(
        worker_id="worker-a",
        claim_lease_seconds=30,
        publish_timeout_seconds=5.0,
        lease_settlement_margin_seconds=5.0,
        retry_backoff_initial_ms=250,
    )
    svc = _make_service(store, pub, cfg)

    o1 = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert o1 is not None
    assert o1.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
    assert o1.dispatch.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
    assert o1.dispatch.claim_expires_at == clock.now + timedelta(seconds=30)

    # Backoff elapsed (1s) but old lease (30s) has NOT expired -> cannot reclaim.
    clock.advance(1.0)
    early = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert early is not None
    assert early.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
    assert early.sends_attempted == 0
    assert len(pub.sent_records) == 1

    # Advance past old lease expiry (total 30s) -> reclaims and retransmits exact bytes.
    clock.advance(29.0)
    o2 = await svc.dispatch_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert o2 is not None
    assert o2.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
    assert o2.dispatch.attempt_count == 2
    assert len(pub.sent_records) == 2
    assert pub.sent_records[0].kafka_value == pub.sent_records[1].kafka_value


@pytest.mark.asyncio
async def test_fix15_expired_owner_late_completion_after_r2_is_fenced(
    fix01_context: dict[str, Any],
) -> None:
    """Reproduces A1-R1 FIX-15: worker A times out, worker B ACKs R1 and R2, A's late ACK fails."""
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    store.add_committed_intent(fix01_context["rev2_intent"])

    physical_log: list[tuple[int, str, str]] = []

    pub_a = ScriptedPublisher()
    pub_b = ScriptedPublisher()
    svc_a = _make_service(store, pub_a, DispatchConfig(worker_id="worker-A"))
    svc_b = _make_service(store, pub_b, DispatchConfig(worker_id="worker-B"))

    # Stage rev 1 and rev 2
    await svc_a.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    await svc_b.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=2,
        staging_context=fix01_context["staging"],
    )

    # Worker A claims rev 1 at t=0
    claim_a1 = await svc_a.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim_a1.claimed and claim_a1.attempt is not None

    # Predecessor gate: Worker B cannot claim rev 2 while rev 1 is unacknowledged.
    blocked_r2 = await svc_b.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=2,
    )
    assert not blocked_r2.claimed
    assert blocked_r2.outcome is not None
    assert blocked_r2.outcome.status is DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR

    # Worker A times out at t=6 (records ambiguous_outcome, retains 30s lease).
    clock.advance(6.0)
    await svc_a.complete_claimed_attempt(
        claim_a1.attempt,
        IncidentPublishOutcome(
            IncidentPublishStatus.FAILED_AMBIGUOUS,
            error=IncidentPublishAmbiguousError("timeout"),
        ),
    )

    # At t=30 lease expires -> Worker B reclaims rev 1, broker accepts, B ACKs rev 1.
    clock.advance(24.0)
    claim_b1 = await svc_b.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim_b1.claimed and claim_b1.attempt is not None
    res_b1 = PublishResult(
        topic="aegis.incidents",
        partition=0,
        offset=201,
        timestamp_ms=1_795_737_700_000,
        latency_ms=1.0,
        serialized_bytes=len(claim_b1.attempt.record.kafka_value),
    )
    physical_log.append((1, "B", _sha256(claim_b1.attempt.record.kafka_value)))
    ack_b1 = await svc_b.complete_claimed_attempt(
        claim_b1.attempt,
        IncidentPublishOutcome(IncidentPublishStatus.ACKNOWLEDGED, result=res_b1),
    )
    assert ack_b1.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED

    # Now rev 1 is acknowledged -> Worker B claims and ACKs rev 2.
    claim_b2 = await svc_b.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=2,
    )
    assert claim_b2.claimed and claim_b2.attempt is not None
    res_b2 = PublishResult(
        topic="aegis.incidents",
        partition=0,
        offset=202,
        timestamp_ms=1_795_737_705_000,
        latency_ms=1.0,
        serialized_bytes=len(claim_b2.attempt.record.kafka_value),
    )
    physical_log.append((2, "B", _sha256(claim_b2.attempt.record.kafka_value)))
    ack_b2 = await svc_b.complete_claimed_attempt(
        claim_b2.attempt,
        IncidentPublishOutcome(IncidentPublishStatus.ACKNOWLEDGED, result=res_b2),
    )
    assert ack_b2.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED

    # Worker A's old already-started rev 1 send finishes late at broker offset 203.
    physical_log.append((1, "A", _sha256(claim_a1.attempt.record.kafka_value)))
    res_a1_late = PublishResult(
        topic="aegis.incidents",
        partition=0,
        offset=203,
        timestamp_ms=1_795_737_710_000,
        latency_ms=35_000.0,
        serialized_bytes=len(claim_a1.attempt.record.kafka_value),
    )
    with pytest.raises(DispatchClaimLostError):
        await svc_a.complete_claimed_attempt(
            claim_a1.attempt,
            IncidentPublishOutcome(
                IncidentPublishStatus.ACKNOWLEDGED, result=res_a1_late
            ),
        )

    # Replacement receipt from Worker B remains authoritative (offset 201, NOT 203).
    final_r1 = await svc_b.load_dispatch_state(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert final_r1 is not None
    assert final_r1.broker_offset == 201
    assert [rev for rev, _, _ in physical_log] == [1, 2, 1]
    assert len({(rev, sha) for rev, _, sha in physical_log}) == 2


@pytest.mark.asyncio
async def test_pre_send_check_aborts_known_lost_claim_with_zero_publish_calls(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub, DispatchConfig(claim_lease_seconds=10))

    await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    claim = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim.claimed and claim.attempt is not None

    # Expire lease before execute_claimed_attempt runs pre-send check.
    clock.advance(11.0)
    with pytest.raises(DispatchClaimLostError):
        await svc.execute_claimed_attempt(claim.attempt)
    assert len(pub.sent_records) == 0


@pytest.mark.asyncio
async def test_single_invocation_per_token_and_cancellation_records_ambiguous(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])

    async def _cancel_send(record: IncidentPublicationRecord) -> IncidentPublishOutcome:
        raise asyncio.CancelledError()

    pub = ScriptedPublisher([_cancel_send])
    svc = _make_service(store, pub)

    await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    claim = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim.claimed and claim.attempt is not None

    with pytest.raises(asyncio.CancelledError):
        await svc.execute_claimed_attempt(claim.attempt)

    row = await svc.load_dispatch_state(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert row is not None
    assert row.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
    assert row.claim_expires_at is not None
    assert row.last_error_code == "PUBLISH_CANCELLED"

    # Reusing the same token is forbidden immediately.
    with pytest.raises(DispatchClaimLostError):
        await svc.execute_claimed_attempt(claim.attempt)
    assert len(pub.sent_records) == 1


@pytest.mark.asyncio
async def test_expired_in_flight_at_max_attempts_transitions_to_exhausted_tr07(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    cfg = DispatchConfig(max_dispatch_attempts=1, claim_lease_seconds=10)
    svc = _make_service(store, pub, cfg)

    await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    # Claim attempt 1 (max=1) and simulate hard crash before bookkeeping.
    claim = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim.claimed and claim.attempt is not None
    assert claim.attempt.dispatch.attempt_count == 1

    # After lease expiry, bounded recovery transitions expired in_flight -> exhausted (TR-07).
    clock.advance(11.0)
    batch = await svc.recover_pending_dispatches(tenant_id=fix01_context["tenant_id"])
    assert batch.exhausted_count == 1
    assert batch.total_sends_attempted == 0
    assert len(pub.sent_records) == 0


@pytest.mark.asyncio
async def test_decode_dispatch_publication_record_rejects_corrupted_row(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    staged = await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert staged is not None
    valid_rec = decode_dispatch_publication_record(staged.dispatch)
    assert valid_rec.event_id == staged.dispatch.event_id

    corrupted_fp = replace(staged.dispatch, publication_fingerprint="0" * 64)
    with pytest.raises(DispatchCorruptionError):
        decode_dispatch_publication_record(corrupted_fp)

    corrupted_bytes = replace(staged.dispatch, kafka_value=b"not-json")
    with pytest.raises(DispatchCorruptionError):
        decode_dispatch_publication_record(corrupted_bytes)

    unsendable = replace(
        staged.dispatch,
        dispatch_state=DispatchState.REJECTED_UNSENDABLE,
        kafka_value=None,
    )
    with pytest.raises(DispatchInputError):
        decode_dispatch_publication_record(unsendable)


@pytest.mark.asyncio
async def test_recover_pending_dispatches_respects_unstaged_without_context_and_contract_error(
    fix01_context: dict[str, Any],
) -> None:
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    contract_err = IncidentPublishOutcome(
        IncidentPublishStatus.FAILED_CONTRACT,
        error=IncidentPublishContractError("version mismatch"),
    )
    pub = ScriptedPublisher([contract_err])
    svc = _make_service(store, pub, DispatchConfig(max_dispatch_attempts=2))

    # Without staging_context, unstaged intent is reported, never fabricated with defaults.
    batch0 = await svc.recover_pending_dispatches(tenant_id=fix01_context["tenant_id"])
    assert batch0.staged_count == 0
    assert batch0.unstaged_remaining_count == 1
    assert batch0.total_sends_attempted == 0

    # With staging_context, stages and attempts; contract failure schedules retry without losing intent.
    batch1 = await svc.recover_pending_dispatches(
        tenant_id=fix01_context["tenant_id"],
        staging_context=fix01_context["staging"],
    )
    assert batch1.staged_count == 1
    assert batch1.pending_count == 1
    assert batch1.outcomes[0].error_code == "PUBLISH_CONTRACT_ERROR"


# ============================================================================
# D1-R1 REGRESSION TESTS FOR THE THREE INDEPENDENT REVIEW BLOCKERS
# ============================================================================


@pytest.mark.asyncio
async def test_d1_r1_blocker1_concurrent_execute_same_token_emits_single_send(
    fix01_context: dict[str, Any],
) -> None:
    """Blocker 1: concurrent execute_claimed_attempt on one token must never duplicate sends."""
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])

    async def _slow_ack(record: IncidentPublicationRecord) -> IncidentPublishOutcome:
        await asyncio.sleep(0.05)
        return IncidentPublishOutcome(
            IncidentPublishStatus.ACKNOWLEDGED,
            result=PublishResult(
                topic=record.topic.value,
                partition=0,
                offset=501,
                timestamp_ms=1_795_737_665_000,
                latency_ms=1.0,
                serialized_bytes=len(record.kafka_value),
            ),
        )

    pub = ScriptedPublisher([_slow_ack, _slow_ack])
    svc = _make_service(store, pub)

    await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    claim = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
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
    assert len(pub.sent_records) == 1


@pytest.mark.asyncio
async def test_d1_r1_blocker2_substituted_r2_record_in_r1_attempt_rejected(
    fix01_context: dict[str, Any],
) -> None:
    """Blocker 2: substituting R2 into an R1 attempt must fail closed with 0 sends."""
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    store.add_committed_intent(fix01_context["rev2_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    s1 = await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    s2 = await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=2,
        staging_context=fix01_context["staging"],
    )
    assert s1 is not None and s2 is not None and s2.record is not None

    claim_r1 = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim_r1.claimed and claim_r1.attempt is not None

    # 1. dataclasses.replace substitution fails immediately in __post_init__.
    with pytest.raises(DispatchInputError):
        replace(claim_r1.attempt, record=s2.record)

    # 2. object.__setattr__ bypass fails in execute_claimed_attempt & complete_claimed_attempt.
    tampered = claim_r1.attempt
    orig_record = tampered.record
    object.__setattr__(tampered, "record", s2.record)
    try:
        with pytest.raises(DispatchInputError):
            await svc.execute_claimed_attempt(tampered)
        r2_ack = IncidentPublishOutcome(
            IncidentPublishStatus.ACKNOWLEDGED,
            result=PublishResult(
                topic="aegis.incidents",
                partition=0,
                offset=777,
                timestamp_ms=1_795_737_665_000,
                latency_ms=1.0,
                serialized_bytes=len(s2.record.kafka_value),
            ),
        )
        with pytest.raises(DispatchInputError):
            await svc.complete_claimed_attempt(tampered, r2_ack)
    finally:
        object.__setattr__(tampered, "record", orig_record)

    # 3. Wrong serialized_bytes receipt rejected even on untampered attempt.
    with pytest.raises(DispatchInputError):
        await svc.complete_claimed_attempt(
            claim_r1.attempt,
            IncidentPublishOutcome(
                IncidentPublishStatus.ACKNOWLEDGED,
                result=PublishResult(
                    topic="aegis.incidents",
                    partition=0,
                    offset=778,
                    timestamp_ms=1_795_737_665_000,
                    latency_ms=1.0,
                    serialized_bytes=len(s2.record.kafka_value),
                ),
            ),
        )

    assert len(pub.sent_records) == 0
    r1_state = await svc.load_dispatch_state(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert r1_state is not None and r1_state.dispatch_state is DispatchState.IN_FLIGHT


@pytest.mark.asyncio
async def test_d1_r1_blocker3_dispatch_timeout_strictly_bounded_to_0_1s(
    fix01_context: dict[str, Any],
) -> None:
    """Blocker 3: 0.1s DispatchConfig timeout bounds publish wait well below 0.3s."""
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])

    async def _hang_forever(
        record: IncidentPublicationRecord,
    ) -> IncidentPublishOutcome:
        await asyncio.sleep(5.0)
        raise AssertionError("Should have timed out before 5s")

    pub = ScriptedPublisher([_hang_forever])
    cfg = DispatchConfig(
        worker_id="timeout-worker",
        publish_timeout_seconds=0.1,
        claim_lease_seconds=5,
        lease_settlement_margin_seconds=1.0,
        retry_backoff_initial_ms=20,
        retry_backoff_max_ms=50,
    )
    svc = _make_service(store, pub, cfg)

    await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    claim = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim.claimed and claim.attempt is not None

    start = time.monotonic()
    outcome = await svc.execute_claimed_attempt(claim.attempt)
    elapsed = time.monotonic() - start

    assert elapsed < 0.3, f"Dispatch timeout was not enforced: elapsed={elapsed:.3f}s"
    assert outcome.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
    assert outcome.dispatch.dispatch_state is DispatchState.AMBIGUOUS_OUTCOME
    assert outcome.dispatch.claim_expires_at is not None
    assert outcome.dispatch.broker_offset is None
    assert outcome.input_offset_committable is False


@pytest.mark.asyncio
async def test_d1_r2_same_revision_alternative_wire_completion_rejected(
    fix01_context: dict[str, Any],
) -> None:
    """D1-R2: internally consistent alternative wire (producer_version=None) cannot ACK stored R1."""
    clock = FakeClock()
    store = InMemoryDispatchStore(clock)
    store.add_committed_intent(fix01_context["rev1_intent"])
    pub = ScriptedPublisher()
    svc = _make_service(store, pub)

    staged = await svc.stage_committed_intent(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
        staging_context=fix01_context["staging"],
    )
    assert staged is not None
    claim = await svc.claim_revision(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert claim.claimed and claim.attempt is not None
    assert len(claim.attempt.record.kafka_value) == 7721
    assert (
        _sha256(claim.attempt.record.kafka_value)
        == WIRE_ORACLES["FIX-01-REV1"]["kafka_value_sha256"]
    )

    # Build valid approved C1 alternative wire variant (producer_version=None, 7694 bytes).
    staging_ctx: DispatchStagingContext = fix01_context["staging"]
    alt_record = build_incident_publication_from_intent(
        intent=fix01_context["rev1_intent"],
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
        == WIRE_ORACLES["FIX-01-REV1-NO-PRODUCER-VERSION"]["kafka_value_sha256"]
    )

    # Internally consistent copied dispatch/attempt passes __post_init__ locally.
    alt_dispatch = replace(
        claim.attempt.dispatch,
        publication_fingerprint=alt_record.publication_fingerprint,
        kafka_value=alt_record.kafka_value,
        kafka_headers_json=tuple((p[0], p[1]) for p in alt_record.kafka_headers_json),
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
    alt_receipt = IncidentPublishOutcome(
        IncidentPublishStatus.ACKNOWLEDGED,
        result=PublishResult(
            topic="aegis.incidents",
            partition=0,
            offset=901,
            timestamp_ms=1_795_737_665_000,
            latency_ms=1.0,
            serialized_bytes=len(alt_record.kafka_value),
        ),
    )

    writes_before = store.row_write_ops
    with pytest.raises(DispatchInputError):
        await svc.complete_claimed_attempt(alt_attempt, alt_receipt)
    assert store.row_write_ops == writes_before
    row_after = await svc.load_dispatch_state(
        tenant_id=fix01_context["tenant_id"],
        incident_id=fix01_context["incident_id"],
        revision=1,
    )
    assert row_after is not None and row_after.dispatch_state is DispatchState.IN_FLIGHT

    # Clean completion with the genuine stored record still succeeds.
    good_receipt = IncidentPublishOutcome(
        IncidentPublishStatus.ACKNOWLEDGED,
        result=PublishResult(
            topic="aegis.incidents",
            partition=0,
            offset=902,
            timestamp_ms=1_795_737_665_000,
            latency_ms=1.0,
            serialized_bytes=len(claim.attempt.record.kafka_value),
        ),
    )
    ok = await svc.complete_claimed_attempt(claim.attempt, good_receipt)
    assert ok.status is DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED
    assert ok.dispatch.dispatch_state is DispatchState.ACKNOWLEDGED
    assert ok.dispatch.broker_offset == 902
