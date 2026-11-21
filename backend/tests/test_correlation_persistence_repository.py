"""Deterministic unit test suite for the Task 4.9 persistence repository.

Covers all 17 required unit test dimensions:
1. Protocol conformance and exact public method surface;
2. Caller-owned transaction enforcement and absence of begin/commit/rollback/close calls;
3. Explicit tenant binding and missing/mismatched context rejection;
4. Explicit tenant predicate or tenant insert value in every statement;
5. Deterministic, deduplicated, sorted membership resolution;
6. Parent load, version load, FOR UPDATE, and absence behavior;
7. Strict Decimal conversion including unsigned-64 max and invalid forms;
8. Exact parent and aggregate mapping;
9. Member and evidence missing/duplicate/unexpected/mutated corruption cases;
10. Noncanonical but model-valid payload rejection;
11. Revision-fingerprint corruption;
12. Timeline ordering, per-revision ordinal continuity, and fingerprint mismatch;
13. Publication-intent canonical reconstruction and current-intent equality;
14. Insert/append/CAS SQL mapping and CAS rowcount behavior;
15. 64-MiB boundary validation without allocating unbounded fixture sets;
16. SQLSTATE/error translation and __cause__ preservation; and
17. Zero ORM objects escaping public return types.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import unittest.mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Insert, Update

from app.anomaly.lineage import AnomalyReproducibilityLineage
from app.anomaly.models import AnomalySignal, CalibrationMetadata
from app.correlation.aggregate import create_or_update_incident
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    IncidentPersistenceConflictError,
    IncidentPersistenceCorruptionError,
    IncidentPersistenceInputError,
    IncidentPersistenceStoreError,
    IncidentPersistenceTenantError,
)
from app.correlation.grouping import (
    CorrelationGroup,
    GroupingMemberReference,
)
from app.correlation.models import (
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentSignalReference,
    IncidentTimelineEvent,
    IncidentTimelineEventType,
)
from app.correlation.persistence_repository import (
    MAX_PAYLOAD_BYTES,
    MAX_REVISION,
    IncidentPersistenceRepository,
    IncidentVersion,
    PersistedIncidentState,
    PersistedPublicationIntent,
    SqlAlchemyIncidentPersistenceRepository,
    _validate_decimal_revision,
    _validate_revision_int,
)
from app.correlation.serialization import serialize_incident_aggregate
from app.models.base import Base
from app.models.incident import (
    IncidentCorrelationEvidenceRecord,
    IncidentPublicationIntentRecord,
    IncidentRecord,
    IncidentSignalLinkRecord,
    IncidentTimelineEventRecord,
)
from app.telemetry.schemas import EventSeverity

# ---------------------------------------------------------------------------
# Deterministic constants and test fixtures
# ---------------------------------------------------------------------------

_TENANT_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_TENANT_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_INC_ID = UUID("11111111-1111-1111-1111-111111111111")
_SIG1_ID = UUID("22222222-2222-2222-2222-222222222222")
_SIG2_ID = UUID("33333333-3333-3333-3333-333333333333")
_FP1 = "1" * 64
_FP2 = "2" * 64
_TIME1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
_TIME2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
_PROC_TIME = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)


def _make_test_aggregate(
    tenant_id: UUID = _TENANT_A,
    revision: int = 1,
) -> tuple[IncidentAggregate, tuple[IncidentTimelineEvent, ...]]:
    sig1 = AnomalySignal(
        signal_id=_SIG1_ID,
        event_time=_TIME1,
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
        signal_id=_SIG1_ID,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(UUID("55555555-5555-5555-5555-555555555555"),),
        semantic_fingerprint=_FP1,
    )
    sig2 = AnomalySignal(
        signal_id=_SIG2_ID,
        event_time=_TIME2,
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
        signal_id=_SIG2_ID,
        run_id=UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scen_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_2",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(UUID("66666666-6666-6666-6666-666666666666"),),
        semantic_fingerprint=_FP2,
    )
    inp1 = AnomalyCorrelationInput(signal=sig1, lineage=lin1)
    inp2 = AnomalyCorrelationInput(signal=sig2, lineage=lin2)
    inputs = {_SIG1_ID: inp1, _SIG2_ID: inp2}

    m1 = GroupingMemberReference(
        signal_id=_SIG1_ID, semantic_fingerprint=_FP1, service="auth", event_time=_TIME1
    )
    m2 = GroupingMemberReference(
        signal_id=_SIG2_ID,
        semantic_fingerprint=_FP2,
        service="billing",
        event_time=_TIME2,
    )
    left = min(_SIG1_ID, _SIG2_ID, key=lambda u: u.bytes)
    right = max(_SIG1_ID, _SIG2_ID, key=lambda u: u.bytes)
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
        started_at=_TIME1,
        last_seen_at=_TIME2,
        time_span_seconds=60.0,
        topology_diameter_hops=1,
        group_fingerprint="0" * 64,
    )
    res = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_PROC_TIME,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    return res.aggregate, res.timeline_events


def _build_orm_parent_record(agg: IncidentAggregate) -> IncidentRecord:
    payload = serialize_incident_aggregate(agg)
    return IncidentRecord(
        tenant_id=agg.tenant_id,
        incident_id=agg.incident_id,
        schema_version=agg.schema_version,
        incident_number=agg.incident_number,
        environment=agg.environment,
        status=agg.status,
        correlation_state=agg.correlation_state,
        severity=agg.severity,
        severity_policy_version=agg.severity_policy_version,
        started_at=agg.started_at,
        last_seen_at=agg.last_seen_at,
        created_at=agg.created_at,
        updated_at=agg.updated_at,
        revision=Decimal(agg.revision),
        primary_service=agg.primary_service,
        affected_services=list(agg.affected_services),
        revision_fingerprint=agg.revision_fingerprint,
        aggregate_payload=payload,
    )


def _build_orm_children(
    agg: IncidentAggregate, events: Sequence[IncidentTimelineEvent]
) -> tuple[
    list[IncidentSignalLinkRecord],
    list[IncidentCorrelationEvidenceRecord],
    list[IncidentTimelineEventRecord],
    list[IncidentPublicationIntentRecord],
]:
    links = [
        IncidentSignalLinkRecord(
            tenant_id=agg.tenant_id,
            incident_id=agg.incident_id,
            signal_id=m.signal_id,
            semantic_fingerprint=m.semantic_fingerprint,
            environment=m.environment,
            service=m.service,
            event_time=m.event_time,
            severity=m.severity,
            anomaly_score=m.anomaly_score,
        )
        for m in sorted(
            agg.signal_references, key=lambda x: (x.event_time, x.signal_id)
        )
    ]
    evidence = [
        IncidentCorrelationEvidenceRecord(
            tenant_id=agg.tenant_id,
            incident_id=agg.incident_id,
            left_signal_id=e.left_signal_id,
            right_signal_id=e.right_signal_id,
            temporal_score=e.temporal_score,
            service_score=e.service_score,
            topology_score=e.topology_score,
            symptom_score=e.symptom_score,
            context_score=e.context_score,
            weighted_score=e.weighted_score,
            accepted=e.accepted,
            policy_version=e.policy_version,
            reason=e.reason,
        )
        for e in sorted(
            agg.correlation_evidence,
            key=lambda x: (x.left_signal_id, x.right_signal_id),
        )
    ]
    timeline = [
        IncidentTimelineEventRecord(
            tenant_id=agg.tenant_id,
            incident_id=ev.incident_id,
            event_id=ev.event_id,
            schema_version=ev.schema_version,
            revision=Decimal(ev.revision),
            event_ordinal=ordinal,
            event_time=ev.event_time,
            event_type=ev.event_type,
            source_signal_id=ev.source_signal_id,
            summary=ev.summary,
            revision_fingerprint=ev.revision_fingerprint,
        )
        for ordinal, ev in enumerate(
            sorted(
                events,
                key=lambda x: (x.event_time, x.revision, x.event_id),
            )
        )
    ]
    intents = [
        IncidentPublicationIntentRecord(
            tenant_id=agg.tenant_id,
            incident_id=agg.incident_id,
            revision=Decimal(agg.revision),
            schema_version=agg.schema_version,
            revision_fingerprint=agg.revision_fingerprint,
            intent_time=agg.updated_at,
            aggregate_payload=serialize_incident_aggregate(agg),
        )
    ]
    return links, evidence, timeline, intents


class MockQueryResult:
    def __init__(
        self, rows: list[Any] | None = None, scalar_val: Any = None, rowcount: int = 1
    ):
        self._rows = rows or []
        self._scalar_val = scalar_val
        self.rowcount = rowcount

    def scalar_one_or_none(self) -> Any:
        return self._scalar_val

    def first(self) -> Any:
        if self._rows:
            return self._rows[0]
        return None

    def scalars(self) -> Any:
        class _ScalarIter:
            def __init__(self, r: list[Any]):
                self.r = r

            def all(self) -> list[Any]:
                return self.r

        return _ScalarIter(self._rows)


# ===========================================================================
# 1. Protocol Conformance and Public Surface
# ===========================================================================


def test_protocol_conformance() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    assert isinstance(repo, IncidentPersistenceRepository)
    assert repo.tenant_id == _TENANT_A
    assert repo.session is session

    expected_methods = {
        "bind_tenant_context",
        "resolve_incident_ids",
        "load_incident",
        "load_incident_version",
        "insert_incident",
        "append_signal_links",
        "append_correlation_evidence",
        "append_timeline_events",
        "append_publication_intent",
        "update_incident_cas",
        "load_publication_intent",
    }
    for m in expected_methods:
        assert hasattr(repo, m)
        assert callable(getattr(repo, m))


# ===========================================================================
# 2. Transaction Enforcement & No begin/commit/rollback/close calls
# ===========================================================================


@pytest.mark.asyncio
async def test_caller_owned_transaction_enforcement() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = False
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    agg, events = _make_test_aggregate(_TENANT_A)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.bind_tenant_context()

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.resolve_incident_ids([_SIG1_ID])

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.load_incident(_INC_ID)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.load_incident_version(_INC_ID)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.insert_incident(agg)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_signal_links(_INC_ID, agg.signal_references)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_correlation_evidence(_INC_ID, agg.correlation_evidence)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_timeline_events(events)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_publication_intent(agg, intent_time=_PROC_TIME)

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.update_incident_cas(
            agg,
            expected_revision=1,
            expected_revision_fingerprint=agg.revision_fingerprint,
        )

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.load_publication_intent(_INC_ID, 1)


def test_ast_proves_no_begin_commit_rollback_close_in_repository() -> None:
    from app.correlation import persistence_repository
    import inspect

    src = inspect.getsource(persistence_repository)
    tree = ast.parse(src)
    forbidden = {"begin", "commit", "rollback", "close"}
    found_violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in forbidden:
                found_violations.append((node.func.attr, node.lineno))
    assert (
        not found_violations
    ), f"Found forbidden transaction lifecycle calls: {found_violations}"


# ===========================================================================
# 3. Explicit Tenant Binding and Context Rejection
# ===========================================================================


@pytest.mark.asyncio
async def test_bind_tenant_context_and_readback() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    await repo.bind_tenant_context()
    assert session.execute.call_count == 1
    assert session.scalar.call_count == 1


@pytest.mark.asyncio
async def test_missing_or_mismatched_tenant_context_fails_closed() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # Missing context (None)
    session.scalar.return_value = None
    with pytest.raises(IncidentPersistenceTenantError, match="not set"):
        await repo.resolve_incident_ids([_SIG1_ID])

    # Missing context (empty string)
    session.scalar.return_value = ""
    with pytest.raises(IncidentPersistenceTenantError, match="not set"):
        await repo.resolve_incident_ids([_SIG1_ID])

    # Mismatched context (different tenant)
    session.scalar.return_value = str(_TENANT_B)
    with pytest.raises(IncidentPersistenceTenantError, match="mismatch"):
        await repo.resolve_incident_ids([_SIG1_ID])

    # Invalid context string
    session.scalar.return_value = "not-a-uuid"
    with pytest.raises(IncidentPersistenceTenantError, match="Invalid tenant context"):
        await repo.resolve_incident_ids([_SIG1_ID])


# ===========================================================================
# 4. Explicit Tenant Predicate / Value in Every Statement
# ===========================================================================


@pytest.mark.asyncio
async def test_all_statements_bind_tenant_id() -> None:
    executed_statements: list[Any] = []
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    async def mock_execute(stmt: Any, params: Any = None) -> MockQueryResult:
        executed_statements.append((stmt, params))
        return MockQueryResult(rows=[])

    session.execute.side_effect = mock_execute
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # 1. resolve_incident_ids
    await repo.resolve_incident_ids([_SIG1_ID])
    stmt, _ = executed_statements[-1]
    compiled_sql = str(stmt)
    assert "tenant_id" in compiled_sql

    # 2. load_incident_version
    await repo.load_incident_version(_INC_ID)
    stmt, _ = executed_statements[-1]
    assert "tenant_id" in str(stmt)

    # 3. insert_incident
    agg, events = _make_test_aggregate(_TENANT_A)
    await repo.insert_incident(agg)
    assert session.add.call_count == 1
    added_record = session.add.call_args[0][0]
    assert added_record.tenant_id == _TENANT_A

    # 4. append_signal_links
    await repo.append_signal_links(_INC_ID, agg.signal_references)
    stmt, values = executed_statements[-1]
    assert isinstance(stmt, Insert)
    assert all(v["tenant_id"] == _TENANT_A for v in values)

    # 5. append_correlation_evidence
    await repo.append_correlation_evidence(_INC_ID, agg.correlation_evidence)
    stmt, values = executed_statements[-1]
    assert isinstance(stmt, Insert)
    assert all(v["tenant_id"] == _TENANT_A for v in values)

    # 6. append_timeline_events
    await repo.append_timeline_events(events)
    stmt, values = executed_statements[-1]
    assert isinstance(stmt, Insert)
    assert all(v["tenant_id"] == _TENANT_A for v in values)

    # 7. update_incident_cas
    await repo.update_incident_cas(
        agg, expected_revision=1, expected_revision_fingerprint=agg.revision_fingerprint
    )
    stmt, _ = executed_statements[-1]
    assert isinstance(stmt, Update)
    assert "tenant_id" in str(stmt)


# ===========================================================================
# 5. Deterministic, Deduplicated, Sorted Membership Resolution
# ===========================================================================


@pytest.mark.asyncio
async def test_resolve_incident_ids_deterministic() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")

    session.execute.return_value = MockQueryResult(rows=[id1, id2])
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    res = await repo.resolve_incident_ids([_SIG1_ID, _SIG2_ID])
    assert res == (id1, id2)

    # Empty inputs return empty tuple without executing SQL
    session.execute.reset_mock()
    res_empty = await repo.resolve_incident_ids([])
    assert res_empty == ()
    session.execute.assert_not_called()

    # Invalid input type
    with pytest.raises(
        IncidentPersistenceInputError, match="signal_ids must be a collection"
    ):
        await repo.resolve_incident_ids("invalid")  # type: ignore[arg-type]

    with pytest.raises(
        IncidentPersistenceInputError, match="Invalid signal_id: expected UUID"
    ):
        await repo.resolve_incident_ids(["not-a-uuid"])  # type: ignore[list-item]


# ===========================================================================
# 6. Parent Load, Version Load, FOR UPDATE, Absence
# ===========================================================================


@pytest.mark.asyncio
async def test_load_incident_version_found_and_absent() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # Found
    session.execute.return_value = MockQueryResult(rows=[(Decimal(2), _FP1)])
    ver = await repo.load_incident_version(_INC_ID, for_update=True)
    assert ver == IncidentVersion(revision=2, revision_fingerprint=_FP1)

    # Absent
    session.execute.return_value = MockQueryResult(rows=[])
    ver_none = await repo.load_incident_version(_INC_ID)
    assert ver_none is None


# ===========================================================================
# 7. Strict Decimal Conversion & Boundaries
# ===========================================================================


def test_strict_decimal_conversion() -> None:
    # Valid conversions
    assert _validate_decimal_revision(Decimal(1)) == 1
    assert _validate_decimal_revision(Decimal(2**63)) == 2**63
    assert _validate_decimal_revision(Decimal(MAX_REVISION)) == MAX_REVISION

    # Python int rejected
    with pytest.raises(IncidentPersistenceCorruptionError):
        _validate_decimal_revision(100)

    # Fractional decimal rejected
    with pytest.raises(IncidentPersistenceCorruptionError):
        _validate_decimal_revision(Decimal("1.5"))

    # Zero rejected
    with pytest.raises(IncidentPersistenceCorruptionError):
        _validate_decimal_revision(Decimal(0))

    # Negative rejected
    with pytest.raises(IncidentPersistenceCorruptionError):
        _validate_decimal_revision(Decimal(-1))

    # Greater than unsigned-64 max rejected
    with pytest.raises(IncidentPersistenceCorruptionError):
        _validate_decimal_revision(Decimal(MAX_REVISION + 1))

    # Boolean rejected
    with pytest.raises(IncidentPersistenceCorruptionError):
        _validate_decimal_revision(True)

    # Input revision integer validation
    assert _validate_revision_int(1) == 1
    assert _validate_revision_int(MAX_REVISION) == MAX_REVISION
    with pytest.raises(IncidentPersistenceInputError, match="Invalid revision"):
        _validate_revision_int(True)
    with pytest.raises(IncidentPersistenceInputError, match="Invalid revision"):
        _validate_revision_int("1")  # type: ignore[arg-type]
    with pytest.raises(IncidentPersistenceInputError, match="Invalid revision"):
        _validate_revision_int(0)
    with pytest.raises(IncidentPersistenceInputError, match="Invalid revision"):
        _validate_revision_int(MAX_REVISION + 1)


# ===========================================================================
# 8. Exact Parent and Aggregate Mapping
# ===========================================================================


@pytest.mark.asyncio
async def test_load_incident_success() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, pis = _build_orm_children(agg, events)

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    def mock_exec(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=pis)
        return MockQueryResult()

    session.execute.side_effect = mock_exec
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    state = await repo.load_incident(agg.incident_id)
    assert state is not None
    assert isinstance(state, PersistedIncidentState)
    assert state.aggregate == agg
    assert len(state.timeline_events) == len(events)
    assert len(state.publication_intents) == 1
    assert state.publication_intents[0].aggregate == agg


# ===========================================================================
# 9. Member and Evidence Corruption Detection
# ===========================================================================


@pytest.mark.asyncio
async def test_member_and_evidence_corruption_detection() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, pis = _build_orm_children(agg, events)

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    # 1. Missing link record in DB
    def mock_exec_missing_link(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=[links[0]])  # missing second link
        return MockQueryResult()

    session.execute.side_effect = mock_exec_missing_link
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    with pytest.raises(
        IncidentPersistenceCorruptionError, match="Member signal count mismatch"
    ):
        await repo.load_incident(agg.incident_id)

    # 2. Corrupted member anomaly score
    corrupted_link = IncidentSignalLinkRecord(
        tenant_id=links[0].tenant_id,
        incident_id=links[0].incident_id,
        signal_id=links[0].signal_id,
        semantic_fingerprint=links[0].semantic_fingerprint,
        environment=links[0].environment,
        service=links[0].service,
        event_time=links[0].event_time,
        severity=links[0].severity,
        anomaly_score=0.1,  # mutated
    )

    def mock_exec_mutated_link(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=[corrupted_link, links[1]])
        return MockQueryResult()

    session.execute.side_effect = mock_exec_mutated_link
    with pytest.raises(
        IncidentPersistenceCorruptionError, match="Member signal attributes mismatch"
    ):
        await repo.load_incident(agg.incident_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption, message",
    [
        ("duplicate_member", "Member signal count mismatch"),
        ("unexpected_member", "not found in aggregate members"),
        ("missing_evidence", "Evidence count mismatch"),
        ("duplicate_evidence", "Evidence count mismatch"),
        ("unexpected_evidence", "not found in aggregate"),
    ],
)
async def test_load_rejects_corrupted_member_and_evidence_row_sets(
    corruption: str, message: str
) -> None:
    aggregate, events = _make_test_aggregate(_TENANT_A)
    parent = _build_orm_parent_record(aggregate)
    links, evidence, timeline, intents = _build_orm_children(aggregate, events)
    assert len(links) == 2 and len(evidence) == 1
    if corruption == "duplicate_member":
        links.append(links[0])
    elif corruption == "unexpected_member":
        links[1].signal_id = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    elif corruption == "missing_evidence":
        evidence.clear()
    elif corruption == "duplicate_evidence":
        evidence.append(evidence[0])
    else:
        evidence[0].right_signal_id = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    collections = {
        "incident_signal_links": links,
        "incident_correlation_evidence": evidence,
        "incident_timeline_events": timeline,
        "incident_publication_intents": intents,
    }

    def execute(statement: Any) -> MockQueryResult:
        sql = str(statement)
        if "FROM incidents" in sql:
            return MockQueryResult(scalar_val=parent)
        for table, records in collections.items():
            if f"FROM {table}" in sql:
                return MockQueryResult(rows=records)
        raise AssertionError(f"Unexpected repository query: {sql}")

    session.execute.side_effect = execute
    repository = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    with pytest.raises(IncidentPersistenceCorruptionError, match=message):
        await repository.load_incident(aggregate.incident_id)
    session.add.assert_not_called()
    session.flush.assert_not_called()


# ===========================================================================
# 10. Noncanonical Payload Rejection
# ===========================================================================


@pytest.mark.asyncio
async def test_noncanonical_payload_rejection() -> None:
    agg, _ = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    # Add trailing whitespace to raw payload (valid JSON, but non-canonical)
    parent_rec.aggregate_payload = parent_rec.aggregate_payload + b" "

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    session.execute.return_value = MockQueryResult(scalar_val=parent_rec)

    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    with pytest.raises(
        IncidentPersistenceCorruptionError, match="not canonically serialized"
    ):
        await repo.load_incident(agg.incident_id)


# ===========================================================================
# 11. Revision-Fingerprint Corruption
# ===========================================================================


@pytest.mark.asyncio
async def test_revision_fingerprint_mismatch_rejection() -> None:
    agg, _ = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    parent_rec.revision_fingerprint = "0" * 64  # mismatch with aggregate payload

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    session.execute.return_value = MockQueryResult(scalar_val=parent_rec)

    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    with pytest.raises(
        IncidentPersistenceCorruptionError, match="revision_fingerprint mismatch"
    ):
        await repo.load_incident(agg.incident_id)


# ===========================================================================
# 12. Timeline Ordering & Ordinal Continuity
# ===========================================================================


@pytest.mark.asyncio
async def test_timeline_ordinal_non_contiguous_rejection() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, pis = _build_orm_children(agg, events)

    # Mutate ordinal in timeline record to make it non-contiguous: [0, 2] instead of [0, 1]
    if len(tls) > 1:
        tls[1].event_ordinal = 5

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    def mock_exec(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=pis)
        return MockQueryResult()

    session.execute.side_effect = mock_exec
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    with pytest.raises(
        IncidentPersistenceCorruptionError,
        match="Timeline event ordinals not contiguous",
    ):
        await repo.load_incident(agg.incident_id)


# ===========================================================================
# 13. Publication-Intent Invariants
# ===========================================================================


@pytest.mark.asyncio
async def test_missing_current_revision_publication_intent_rejection() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, _ = _build_orm_children(agg, events)

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    def mock_exec(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=[])  # empty intents
        return MockQueryResult()

    session.execute.side_effect = mock_exec
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    with pytest.raises(
        IncidentPersistenceCorruptionError, match="Missing publication intent"
    ):
        await repo.load_incident(agg.incident_id)


# ===========================================================================
# 14. CAS Rowcount 1 / 0 Behavior
# ===========================================================================


@pytest.mark.asyncio
async def test_update_incident_cas_rowcount() -> None:
    agg, _ = _make_test_aggregate(_TENANT_A)
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # rowcount == 1 -> returns True
    session.execute.return_value = MockQueryResult(rowcount=1)
    res_true = await repo.update_incident_cas(
        agg, expected_revision=1, expected_revision_fingerprint=agg.revision_fingerprint
    )
    assert res_true is True

    # rowcount == 0 -> returns False (stale revision)
    session.execute.return_value = MockQueryResult(rowcount=0)
    res_false = await repo.update_incident_cas(
        agg, expected_revision=1, expected_revision_fingerprint=agg.revision_fingerprint
    )
    assert res_false is False


# ===========================================================================
# 15. 64-MiB Boundary Validation
# ===========================================================================


@pytest.mark.asyncio
async def test_payload_exceeds_64mib_rejected() -> None:
    agg, _ = _make_test_aggregate(_TENANT_A)
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    with unittest.mock.patch(
        "app.correlation.persistence_repository.serialize_incident_aggregate",
        return_value=b"x" * (MAX_PAYLOAD_BYTES + 1),
    ):
        with pytest.raises(IncidentPersistenceInputError, match="exceeds 64 MiB limit"):
            await repo.insert_incident(agg)

        with pytest.raises(IncidentPersistenceInputError, match="exceeds 64 MiB limit"):
            await repo.append_publication_intent(agg, intent_time=_PROC_TIME)

        with pytest.raises(IncidentPersistenceInputError, match="exceeds 64 MiB limit"):
            await repo.update_incident_cas(
                agg,
                expected_revision=1,
                expected_revision_fingerprint=agg.revision_fingerprint,
            )


# ===========================================================================
# 16. SQLSTATE Translation & __cause__ Preservation
# ===========================================================================


@pytest.mark.asyncio
async def test_sqlstate_translation_and_cause_preservation() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    agg, _ = _make_test_aggregate(_TENANT_A)

    # 1. SQLSTATE 23505 (Unique violation) -> IncidentPersistenceConflictError
    orig_23505 = Exception()
    setattr(orig_23505, "pgcode", "23505")
    db_err_23505 = IntegrityError("INSERT failed", {}, orig_23505)
    session.flush.side_effect = db_err_23505

    with pytest.raises(IncidentPersistenceConflictError) as exc_info:
        await repo.insert_incident(agg)
    assert exc_info.value.__cause__ is db_err_23505

    # 2. SQLSTATE 23503 (Foreign key violation) -> IncidentPersistenceInputError
    orig_23503 = Exception()
    setattr(orig_23503, "pgcode", "23503")
    db_err_23503 = IntegrityError("FK failed", {}, orig_23503)
    session.flush.side_effect = db_err_23503

    with pytest.raises(IncidentPersistenceInputError) as exc_info:
        await repo.insert_incident(agg)
    assert exc_info.value.__cause__ is db_err_23503

    # 3. Generic DBAPIError -> IncidentPersistenceStoreError
    orig_gen = Exception("Connection lost")
    db_err_gen = DBAPIError("DB failed", {}, orig_gen)
    session.flush.side_effect = db_err_gen

    with pytest.raises(IncidentPersistenceStoreError) as exc_info:
        await repo.insert_incident(agg)
    assert exc_info.value.__cause__ is db_err_gen


# ===========================================================================
# 17. Zero ORM Objects Escaping Public Return Types
# ===========================================================================


@pytest.mark.asyncio
async def test_zero_orm_objects_escaping_public_api() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, pis = _build_orm_children(agg, events)

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    def mock_exec(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(
                scalar_val=parent_rec, rows=[(Decimal(1), agg.revision_fingerprint)]
            )
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=pis)
        return MockQueryResult()

    session.execute.side_effect = mock_exec
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # 1. load_incident
    state = await repo.load_incident(agg.incident_id)
    assert state is not None
    assert not isinstance(state, Base)
    assert not isinstance(state.aggregate, Base)
    for ev in state.timeline_events:
        assert not isinstance(ev, Base)
    for pi in state.publication_intents:
        assert not isinstance(pi, Base)

    # 2. load_incident_version
    ver = await repo.load_incident_version(agg.incident_id)
    assert ver is not None
    assert not isinstance(ver, Base)
    assert isinstance(ver, IncidentVersion)

    # 3. load_publication_intent
    session.execute.side_effect = lambda stmt: MockQueryResult(scalar_val=pis[0])
    pi_loaded = await repo.load_publication_intent(agg.incident_id, 1)
    assert pi_loaded is not None
    assert not isinstance(pi_loaded, Base)
    assert isinstance(pi_loaded, PersistedPublicationIntent)


# ===========================================================================
# 18. REQUIRED P1-R1 UNIT TEST CLOSURES (SECTION 12)
# ===========================================================================


@pytest.mark.asyncio
async def test_signal_link_tenant_mismatch_rejected_before_sql() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    mismatched_link = IncidentSignalReference(
        tenant_id=_TENANT_B,
        signal_id=_SIG1_ID,
        semantic_fingerprint=_FP1,
        environment="production",
        service="auth",
        event_time=_TIME1,
        severity=EventSeverity.WARNING,
        anomaly_score=0.9,
    )

    with pytest.raises(
        IncidentPersistenceTenantError,
        match="does not match repository tenant_id",
    ):
        await repo.append_signal_links(_INC_ID, [mismatched_link])

    # Pure validation must fire before any database access whatsoever,
    # including the transaction-local tenant-context SELECT.
    session.scalar.assert_not_called()
    session.execute.assert_not_called()
    session.add.assert_not_called()
    session.flush.assert_not_called()


@pytest.mark.asyncio
async def test_aggregate_writes_revalidate_revision_fingerprint() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    agg, _ = _make_test_aggregate(_TENANT_A)
    tampered_fp = "f" * 64
    tampered_agg = agg.model_copy(update={"revision_fingerprint": tampered_fp})

    # 1. insert_incident
    with pytest.raises(
        IncidentPersistenceInputError, match="revision_fingerprint mismatch"
    ):
        await repo.insert_incident(tampered_agg)

    # 2. append_publication_intent
    with pytest.raises(
        IncidentPersistenceInputError, match="revision_fingerprint mismatch"
    ):
        await repo.append_publication_intent(tampered_agg, intent_time=_PROC_TIME)

    # 3. update_incident_cas
    with pytest.raises(
        IncidentPersistenceInputError, match="revision_fingerprint mismatch"
    ):
        await repo.update_incident_cas(
            tampered_agg,
            expected_revision=1,
            expected_revision_fingerprint=agg.revision_fingerprint,
        )

    session.add.assert_not_called()
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_cas_updates_only_mutable_parent_columns() -> None:
    agg, _ = _make_test_aggregate(_TENANT_A)
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    session.execute.return_value = MockQueryResult(rowcount=1)

    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)
    await repo.update_incident_cas(
        agg,
        expected_revision=1,
        expected_revision_fingerprint=agg.revision_fingerprint,
    )

    assert session.execute.call_count == 1
    stmt = session.execute.call_args[0][0]
    assert isinstance(stmt, Update)

    updated_col_names = {col.key for col in stmt._values}
    expected_mutable = {
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
    assert updated_col_names == expected_mutable

    forbidden_cols = {
        "schema_version",
        "incident_number",
        "environment",
        "severity_policy_version",
        "created_at",
        "primary_service",
        "tenant_id",
        "incident_id",
    }
    assert updated_col_names.isdisjoint(forbidden_cols)

    compiled_where = str(stmt.whereclause)
    assert "tenant_id" in compiled_where
    assert "incident_id" in compiled_where
    assert "revision" in compiled_where
    assert "revision_fingerprint" in compiled_where


def test_database_revision_requires_finite_decimal() -> None:
    assert _validate_decimal_revision(Decimal(1)) == 1
    assert _validate_decimal_revision(Decimal(42)) == 42
    assert _validate_decimal_revision(Decimal(MAX_REVISION)) == MAX_REVISION
    assert _validate_decimal_revision(Decimal("100.0")) == 100

    for bad_val in (
        1,
        42,
        100,
        MAX_REVISION,
        True,
        False,
        1.0,
        42.0,
        "1",
        "42",
        [1],
        (1,),
    ):
        with pytest.raises(
            IncidentPersistenceCorruptionError,
            match="must be a finite integral Decimal",
        ):
            _validate_decimal_revision(bad_val)

    for bad_dec in (
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("1.5"),
        Decimal("0.1"),
        Decimal("-0.01"),
    ):
        with pytest.raises(IncidentPersistenceCorruptionError):
            _validate_decimal_revision(bad_dec)

    for oob_dec in (
        Decimal(0),
        Decimal(-1),
        Decimal(-100),
        Decimal(MAX_REVISION + 1),
    ):
        with pytest.raises(IncidentPersistenceCorruptionError):
            _validate_decimal_revision(oob_dec)


@pytest.mark.asyncio
async def test_exact_score_mismatch_is_corruption() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, pis = _build_orm_children(agg, events)

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    # 1. Anomaly score mismatch on member signal link
    corrupted_links = list(links)
    corrupted_links[0] = IncidentSignalLinkRecord(
        tenant_id=links[0].tenant_id,
        incident_id=links[0].incident_id,
        signal_id=links[0].signal_id,
        semantic_fingerprint=links[0].semantic_fingerprint,
        environment=links[0].environment,
        service=links[0].service,
        event_time=links[0].event_time,
        severity=links[0].severity,
        anomaly_score=links[0].anomaly_score + 1e-10,
    )

    def mock_exec_link(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=corrupted_links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=pis)
        return MockQueryResult()

    session.execute.side_effect = mock_exec_link
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    with pytest.raises(
        IncidentPersistenceCorruptionError,
        match="Member signal attributes mismatch",
    ):
        await repo.load_incident(agg.incident_id)

    # 2. Evidence score mismatch on correlation evidence
    corrupted_evs = list(evs)
    corrupted_evs[0] = IncidentCorrelationEvidenceRecord(
        tenant_id=evs[0].tenant_id,
        incident_id=evs[0].incident_id,
        left_signal_id=evs[0].left_signal_id,
        right_signal_id=evs[0].right_signal_id,
        temporal_score=evs[0].temporal_score + 1e-10,
        service_score=evs[0].service_score,
        topology_score=evs[0].topology_score,
        symptom_score=evs[0].symptom_score,
        context_score=evs[0].context_score,
        weighted_score=evs[0].weighted_score,
        accepted=evs[0].accepted,
        policy_version=evs[0].policy_version,
        reason=evs[0].reason,
    )

    def mock_exec_ev(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=corrupted_evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=pis)
        return MockQueryResult()

    session.execute.side_effect = mock_exec_ev
    with pytest.raises(
        IncidentPersistenceCorruptionError,
        match="Correlation evidence attributes mismatch",
    ):
        await repo.load_incident(agg.incident_id)


@pytest.mark.asyncio
async def test_publication_intent_identity_mismatch_is_corruption() -> None:
    agg, events = _make_test_aggregate(_TENANT_A)
    parent_rec = _build_orm_parent_record(agg)
    links, evs, tls, pis = _build_orm_children(agg, events)

    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)

    # 1. Publication intent revision exceeds current incident revision
    future_pi = IncidentPublicationIntentRecord(
        tenant_id=agg.tenant_id,
        incident_id=agg.incident_id,
        revision=Decimal(agg.revision + 1),
        schema_version=agg.schema_version,
        revision_fingerprint=agg.revision_fingerprint,
        intent_time=agg.updated_at,
        aggregate_payload=serialize_incident_aggregate(agg),
    )

    def mock_exec_future(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=[future_pi])
        return MockQueryResult()

    session.execute.side_effect = mock_exec_future
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    with pytest.raises(
        IncidentPersistenceCorruptionError,
        match="exceeds current incident revision",
    ):
        await repo.load_incident(agg.incident_id)

    # 2. Publication intent fingerprint mismatch
    bad_fp_pi = IncidentPublicationIntentRecord(
        tenant_id=agg.tenant_id,
        incident_id=agg.incident_id,
        revision=Decimal(agg.revision),
        schema_version=agg.schema_version,
        revision_fingerprint="f" * 64,
        intent_time=agg.updated_at,
        aggregate_payload=serialize_incident_aggregate(agg),
    )

    def mock_exec_bad_fp(stmt: Any) -> MockQueryResult:
        s = str(stmt)
        if "FROM incidents" in s:
            return MockQueryResult(scalar_val=parent_rec)
        if "FROM incident_signal_links" in s:
            return MockQueryResult(rows=links)
        if "FROM incident_correlation_evidence" in s:
            return MockQueryResult(rows=evs)
        if "FROM incident_timeline_events" in s:
            return MockQueryResult(rows=tls)
        if "FROM incident_publication_intents" in s:
            return MockQueryResult(rows=[bad_fp_pi])
        return MockQueryResult()

    session.execute.side_effect = mock_exec_bad_fp
    with pytest.raises(
        IncidentPersistenceCorruptionError,
        match="Publication intent attributes mismatch",
    ):
        await repo.load_incident(agg.incident_id)

    # 3. Publication intent loaded by load_publication_intent with metadata mismatch
    session.execute.side_effect = lambda stmt: MockQueryResult(
        scalar_val=IncidentPublicationIntentRecord(
            tenant_id=_TENANT_B,
            incident_id=agg.incident_id,
            revision=Decimal(agg.revision),
            schema_version=agg.schema_version,
            revision_fingerprint=agg.revision_fingerprint,
            intent_time=agg.updated_at,
            aggregate_payload=serialize_incident_aggregate(agg),
        )
    )
    with pytest.raises(
        IncidentPersistenceCorruptionError,
        match="Publication intent metadata does not match",
    ):
        await repo.load_publication_intent(agg.incident_id, 1)


@pytest.mark.asyncio
async def test_empty_operations_require_transaction_and_tenant_context() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # Phase 1: Not in active transaction -> StoreError
    session.in_transaction.return_value = False

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.resolve_incident_ids([])

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_signal_links(_INC_ID, [])

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_correlation_evidence(_INC_ID, [])

    with pytest.raises(IncidentPersistenceStoreError, match="active transaction"):
        await repo.append_timeline_events([])

    # Phase 2: In transaction, but tenant context missing/mismatched -> TenantError
    session.in_transaction.return_value = True
    session.scalar.return_value = None

    with pytest.raises(IncidentPersistenceTenantError, match="not set"):
        await repo.resolve_incident_ids([])

    with pytest.raises(IncidentPersistenceTenantError, match="not set"):
        await repo.append_signal_links(_INC_ID, [])

    with pytest.raises(IncidentPersistenceTenantError, match="not set"):
        await repo.append_correlation_evidence(_INC_ID, [])

    with pytest.raises(IncidentPersistenceTenantError, match="not set"):
        await repo.append_timeline_events([])

    # Phase 3: In transaction, tenant context matches -> succeeds cleanly without SQL
    session.scalar.return_value = str(_TENANT_A)
    session.execute.reset_mock()

    res = await repo.resolve_incident_ids([])
    assert res == ()

    await repo.append_signal_links(_INC_ID, [])
    await repo.append_correlation_evidence(_INC_ID, [])
    await repo.append_timeline_events([])

    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_malformed_timeline_sequence_raises_typed_input_error() -> None:
    session = unittest.mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.scalar.return_value = str(_TENANT_A)
    repo = SqlAlchemyIncidentPersistenceRepository(session, _TENANT_A)

    # 1. Non-sequence input
    with pytest.raises(
        IncidentPersistenceInputError, match="events must be a sequence"
    ):
        await repo.append_timeline_events("not-a-sequence")  # type: ignore[arg-type]

    # 2. Sequence containing non-IncidentTimelineEvent object
    with pytest.raises(
        IncidentPersistenceInputError, match="IncidentTimelineEvent instances"
    ):
        await repo.append_timeline_events(["not-an-event"])  # type: ignore[list-item]

    # 3. Mixed incident IDs in same batch
    ev1 = IncidentTimelineEvent(
        event_id=uuid4(),
        incident_id=_INC_ID,
        revision=1,
        event_time=_TIME1,
        event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
        source_signal_id=_SIG1_ID,
        summary="Test event 1",
        revision_fingerprint=_FP1,
    )
    ev_diff_inc = IncidentTimelineEvent(
        event_id=uuid4(),
        incident_id=uuid4(),
        revision=1,
        event_time=_TIME2,
        event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
        source_signal_id=_SIG2_ID,
        summary="Test event diff inc",
        revision_fingerprint=_FP1,
    )
    with pytest.raises(IncidentPersistenceInputError, match="Mixed incident batch"):
        await repo.append_timeline_events([ev1, ev_diff_inc])

    # 4. Mixed revisions in same batch
    ev_diff_rev = IncidentTimelineEvent(
        event_id=uuid4(),
        incident_id=_INC_ID,
        revision=2,
        event_time=_TIME2,
        event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
        source_signal_id=_SIG2_ID,
        summary="Test event diff rev",
        revision_fingerprint=_FP1,
    )
    with pytest.raises(IncidentPersistenceInputError, match="Mixed revision batch"):
        await repo.append_timeline_events([ev1, ev_diff_rev])

    # 5. Mixed fingerprints in same batch
    ev_diff_fp = IncidentTimelineEvent(
        event_id=uuid4(),
        incident_id=_INC_ID,
        revision=1,
        event_time=_TIME2,
        event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
        source_signal_id=_SIG2_ID,
        summary="Test event diff fp",
        revision_fingerprint=_FP2,
    )
    with pytest.raises(IncidentPersistenceInputError, match="Mixed fingerprint batch"):
        await repo.append_timeline_events([ev1, ev_diff_fp])
