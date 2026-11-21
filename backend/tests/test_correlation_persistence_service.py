"""Unit tests for the atomic incident persistence transaction service.

Tests pre-SQL request validation, deterministic target resolution,
the 8-row definitive decision table, write order, SQLSTATE 23505 recovery bounds,
and typed error translation.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import inspect
from typing import Any
import unittest.mock
from uuid import UUID

import pytest
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from app.anomaly.lineage import AnomalyReproducibilityLineage
from app.anomaly.models import (
    AnomalySignal,
    CalibrationMetadata,
    EventSeverity,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    AggregateInputError,
    AggregateStateError,
)
from app.correlation import (
    IncidentAggregationResult,
    IncidentPersistenceConflictError,
    IncidentPersistenceCorruptionError,
    IncidentPersistenceError,
    IncidentPersistenceInputError,
    IncidentPersistenceStoreError,
    IncidentPersistenceTenantError,
    PersistedIncidentState,
    PersistedPublicationIntent,
    SqlAlchemyIncidentPersistenceRepository,
    create_or_update_incident,
    persist_incident_atomic,
)
from app.correlation.grouping import (
    CorrelationGroup,
    GroupingMemberReference,
)
from app.correlation.identity import derive_incident_id
from app.correlation.models import (
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
)
from app.correlation.persistence_service import (
    _validate_pre_sql_request,
)

_TENANT_A = UUID("11111111-1111-1111-1111-111111111111")
_TENANT_B = UUID("22222222-2222-2222-2222-222222222222")

_SIG1_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_SIG2_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_SIG3_ID = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")

_TIME1 = datetime(2026, 11, 1, 10, 0, 0, tzinfo=timezone.utc)
_TIME2 = datetime(2026, 11, 1, 10, 1, 0, tzinfo=timezone.utc)
_TIME3 = datetime(2026, 11, 1, 10, 2, 0, tzinfo=timezone.utc)

_FP1 = "1111111111111111111111111111111111111111111111111111111111111111"
_FP2 = "2222222222222222222222222222222222222222222222222222222222222222"
_FP3 = "3333333333333333333333333333333333333333333333333333333333333333"


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


def _make_evidence(
    left_id: UUID, right_id: UUID, accepted: bool = True
) -> CorrelationEvidence:
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
        accepted=accepted,
        policy_version="1.0",
        reason="high_similarity",
    )


def _make_persisted_state(agg: IncidentAggregate) -> PersistedIncidentState:
    intent = PersistedPublicationIntent(
        tenant_id=_TENANT_A,
        incident_id=agg.incident_id,
        revision=agg.revision,
        schema_version=agg.schema_version,
        revision_fingerprint=agg.revision_fingerprint,
        intent_time=agg.updated_at,
        aggregate=agg,
    )
    return PersistedIncidentState(
        aggregate=agg,
        timeline_events=(),
        publication_intents=(intent,),
    )


class MockSessionContext(AsyncSession):
    """Mock session and transaction context tracker."""

    def __init__(
        self,
        rolsuper: bool = False,
        rolbypassrls: bool = False,
        isolation: str = "read committed",
    ):
        self.rolsuper = rolsuper
        self.rolbypassrls = rolbypassrls
        self.isolation = isolation
        self.transaction_begun = False
        self.transaction_committed = False
        self.transaction_rolled_back = False
        self.session_closed = False
        self.calls: list[str] = []

    @asynccontextmanager
    async def begin(self) -> AsyncGenerator[None, None]:
        self.transaction_begun = True
        self.calls.append("begin")
        try:
            yield
            self.transaction_committed = True
            self.calls.append("commit")
        except Exception:
            self.transaction_rolled_back = True
            self.calls.append("rollback")
            raise

    async def execute(self, stmt: Any) -> Any:
        sql = str(stmt).lower()
        if "show transaction_isolation" in sql:
            mock_res = unittest.mock.MagicMock()
            mock_res.scalar_one_or_none.return_value = self.isolation
            return mock_res
        if "from pg_roles" in sql:
            mock_res = unittest.mock.MagicMock()
            mock_res.first.return_value = (self.rolsuper, self.rolbypassrls)
            return mock_res
        mock_res = unittest.mock.MagicMock()
        mock_res.scalar_one_or_none.return_value = None
        mock_res.first.return_value = None
        return mock_res

    async def close(self) -> None:
        self.session_closed = True
        self.calls.append("close")


class MockSessionFactory:
    """Factory creating trackable mock sessions."""

    def __init__(
        self,
        rolsuper: bool = False,
        rolbypassrls: bool = False,
        isolation: str = "read committed",
    ):
        self.rolsuper = rolsuper
        self.rolbypassrls = rolbypassrls
        self.isolation = isolation
        self.sessions: list[MockSessionContext] = []

    @asynccontextmanager
    async def __call__(self) -> AsyncGenerator[MockSessionContext, None]:
        session = MockSessionContext(
            rolsuper=self.rolsuper,
            rolbypassrls=self.rolbypassrls,
            isolation=self.isolation,
        )
        self.sessions.append(session)
        try:
            yield session
        finally:
            await session.close()


# ---------------------------------------------------------------------------
# TEST 1: test_public_signature_and_exports
# ---------------------------------------------------------------------------
def test_public_signature_and_exports() -> None:
    """Verify exact public signature, return contract, and module exports."""
    sig = inspect.signature(persist_incident_atomic)
    params = list(sig.parameters.values())

    # All parameters must be keyword-only
    for p in params:
        assert p.kind == inspect.Parameter.KEYWORD_ONLY

    param_names = [p.name for p in params]
    expected_params = [
        "group",
        "signal_inputs",
        "desired_state",
        "processing_time",
        "tenant_id",
        "session_factory",
        "expected_revision",
        "expected_revision_fingerprint",
    ]
    assert param_names == expected_params
    assert sig.return_annotation == "IncidentAggregationResult"

    # Export surface verification
    import app.correlation as c

    assert hasattr(c, "persist_incident_atomic")
    assert hasattr(c, "IncidentPersistenceRepository")
    assert hasattr(c, "SqlAlchemyIncidentPersistenceRepository")
    assert hasattr(c, "PersistedIncidentState")
    assert hasattr(c, "PersistedPublicationIntent")
    assert hasattr(c, "IncidentVersion")
    assert hasattr(c, "IncidentPersistenceError")
    assert hasattr(c, "IncidentPersistenceInputError")
    assert hasattr(c, "IncidentPersistenceTenantError")
    assert hasattr(c, "IncidentPersistenceConflictError")
    assert hasattr(c, "IncidentPersistenceCorruptionError")
    assert hasattr(c, "IncidentPersistenceStoreError")


# ---------------------------------------------------------------------------
# TEST 2: test_one_sided_expectation_rejected_before_session_factory
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_one_sided_expectation_rejected_before_session_factory() -> None:
    """One-sided expectation must be rejected before session factory is touched."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = unittest.mock.MagicMock()

    # expected_revision present, fingerprint absent
    with pytest.raises(
        IncidentPersistenceInputError, match="both present or both absent"
    ):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
            expected_revision=1,
            expected_revision_fingerprint=None,
        )
    assert factory.call_count == 0

    # fingerprint present, expected_revision absent
    with pytest.raises(
        IncidentPersistenceInputError, match="both present or both absent"
    ):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
            expected_revision=None,
            expected_revision_fingerprint="a" * 64,
        )
    assert factory.call_count == 0


# ---------------------------------------------------------------------------
# TEST 3: test_expected_revision_and_fingerprint_validation_before_sql
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expected_revision_and_fingerprint_validation_before_sql() -> None:
    """Expected revision bounds and fingerprint syntax must be validated before SQL."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = unittest.mock.MagicMock()

    # Bool revision
    with pytest.raises(IncidentPersistenceInputError):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
            expected_revision=True,  # type: ignore
            expected_revision_fingerprint="a" * 64,
        )

    # 0 revision
    with pytest.raises(IncidentPersistenceInputError):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
            expected_revision=0,
            expected_revision_fingerprint="a" * 64,
        )

    # Malformed fingerprint (uppercase, short, non-hex)
    for bad_fp in ["A" * 64, "a" * 63, "z" * 64, ""]:
        with pytest.raises(IncidentPersistenceInputError):
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,
                expected_revision=1,
                expected_revision_fingerprint=bad_fp,
            )

    assert factory.call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [1.5, Decimal("1.5")])
async def test_fractional_expected_revision_rejected_before_sql(
    revision: float | Decimal,
) -> None:
    member = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    factory = unittest.mock.MagicMock()
    with pytest.raises(IncidentPersistenceInputError, match="non-boolean integer"):
        await persist_incident_atomic(
            group=_make_group([member]),
            signal_inputs={_SIG1_ID: _make_input(_SIG1_ID, _TIME1, "auth", _FP1)},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
            expected_revision=revision,  # type: ignore[arg-type]
            expected_revision_fingerprint=_FP1,
        )
    factory.assert_not_called()


# ---------------------------------------------------------------------------
# TEST 4: test_authoritative_member_reconciliation_rejected_before_sql
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_authoritative_member_reconciliation_rejected_before_sql() -> None:
    """Authoritative member input mismatches must be rejected before SQL."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    factory = unittest.mock.MagicMock()

    # 1. Missing input
    group = _make_group([m1])
    with pytest.raises(IncidentPersistenceInputError, match="input missing"):
        await persist_incident_atomic(
            group=group,
            signal_inputs={},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
        )

    # 2. Fingerprint mismatch
    inp_bad_fp = _make_input(_SIG1_ID, _TIME1, "auth", _FP2)
    with pytest.raises(IncidentPersistenceInputError, match="fingerprint mismatch"):
        await persist_incident_atomic(
            group=group,
            signal_inputs={_SIG1_ID: inp_bad_fp},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
        )

    # 3. Service mismatch
    inp_bad_svc = _make_input(_SIG1_ID, _TIME1, "billing", _FP1)
    with pytest.raises(IncidentPersistenceInputError, match="service mismatch"):
        await persist_incident_atomic(
            group=group,
            signal_inputs={_SIG1_ID: inp_bad_svc},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
        )

    # 4. Multi-tenant members
    m2 = _make_member(_SIG2_ID, _TIME2, "billing", _FP2)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1, tenant_id=_TENANT_A)
    inp2_diff_tenant = _make_input(
        _SIG2_ID, _TIME2, "billing", _FP2, tenant_id=_TENANT_B
    )
    group_two = _make_group([m1, m2])
    with pytest.raises(IncidentPersistenceTenantError, match="differing from"):
        await persist_incident_atomic(
            group=group_two,
            signal_inputs={_SIG1_ID: inp1, _SIG2_ID: inp2_diff_tenant},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,
        )

    # 5. Derived tenant != request tenant_id
    with pytest.raises(
        IncidentPersistenceTenantError, match="does not match request tenant_id"
    ):
        await persist_incident_atomic(
            group=group,
            signal_inputs={_SIG1_ID: inp1},
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_B,
            session_factory=factory,
        )

    assert factory.call_count == 0


# ---------------------------------------------------------------------------
# TEST 5: test_tenant_environment_anchor_and_candidate_are_authoritative
# ---------------------------------------------------------------------------
def test_tenant_environment_anchor_and_candidate_are_authoritative() -> None:
    """Founding anchor selection and candidate ID derivation must be authoritative."""
    m1 = _make_member(_SIG1_ID, _TIME2, "auth", _FP1)
    m2 = _make_member(_SIG2_ID, _TIME1, "billing", _FP2)  # earlier event_time -> anchor
    inp1 = _make_input(_SIG1_ID, _TIME2, "auth", _FP1, environment="staging")
    inp2 = _make_input(_SIG2_ID, _TIME1, "billing", _FP2, environment="staging")

    group = _make_group([m1, m2])
    inputs = {_SIG1_ID: inp1, _SIG2_ID: inp2}

    env, anchor, cand_id = _validate_pre_sql_request(
        group=group,
        signal_inputs=inputs,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME2,
        tenant_id=_TENANT_A,
        expected_revision=None,
        expected_revision_fingerprint=None,
    )

    assert env == "staging"
    assert anchor.signal_id == _SIG2_ID
    expected_cand_id = derive_incident_id(
        tenant_id=_TENANT_A,
        environment="staging",
        anchor_signal_fingerprint=_FP2,
    )
    assert cand_id == expected_cand_id


# ---------------------------------------------------------------------------
# TEST 6: test_nonmember_signal_inputs_do_not_change_identity
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("reverse_members", [False, True])
def test_anchor_ties_use_uuid_bytes_in_both_member_orders(
    reverse_members: bool,
) -> None:
    members = [
        _make_member(_SIG1_ID, _TIME1, "auth", _FP1),
        _make_member(_SIG2_ID, _TIME1, "billing", _FP2),
    ]
    if reverse_members:
        members.reverse()
    group = _make_group(members)
    inputs = {
        member.signal_id: _make_input(
            member.signal_id,
            member.event_time,
            member.service,
            member.semantic_fingerprint,
        )
        for member in members
    }
    environment, anchor, candidate = _validate_pre_sql_request(
        group=group,
        signal_inputs=inputs,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME2,
        tenant_id=_TENANT_A,
        expected_revision=None,
        expected_revision_fingerprint=None,
    )
    assert anchor.signal_id == min((_SIG1_ID, _SIG2_ID), key=lambda value: value.bytes)
    assert candidate == derive_incident_id(
        tenant_id=_TENANT_A,
        environment=environment,
        anchor_signal_fingerprint=_FP1,
    )


def test_nonmember_signal_inputs_do_not_change_identity() -> None:
    """Extra nonmember signal inputs must not influence identity or candidate derivation."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    inp_extra = _make_input(
        _SIG3_ID,
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        "extra",
        _FP3,
        tenant_id=_TENANT_B,
    )

    group = _make_group([m1])
    inputs_with_extra = {_SIG1_ID: inp1, _SIG3_ID: inp_extra}

    _, anchor, cand_id = _validate_pre_sql_request(
        group=group,
        signal_inputs=inputs_with_extra,
        desired_state=CorrelationWindowState.COLLECTING,
        processing_time=_TIME1,
        tenant_id=_TENANT_A,
        expected_revision=None,
        expected_revision_fingerprint=None,
    )
    assert anchor.signal_id == _SIG1_ID
    assert cand_id == derive_incident_id(
        tenant_id=_TENANT_A,
        environment="production",
        anchor_signal_fingerprint=_FP1,
    )


# ---------------------------------------------------------------------------
# TEST 7: test_superuser_and_bypass_roles_fail_closed
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_superuser_and_bypass_roles_fail_closed() -> None:
    """Superuser and BYPASSRLS roles must be rejected by the role guard."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    # Superuser role
    super_factory = MockSessionFactory(rolsuper=True, rolbypassrls=False)
    with pytest.raises(IncidentPersistenceTenantError, match="superuser or bypassrls"):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=super_factory,  # type: ignore
        )

    # Bypass RLS role
    bypass_factory = MockSessionFactory(rolsuper=False, rolbypassrls=True)
    with pytest.raises(IncidentPersistenceTenantError, match="superuser or bypassrls"):
        await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=bypass_factory,  # type: ignore
        )


# ---------------------------------------------------------------------------
# TEST 8: test_target_resolution_zero_one_mismatch_and_multiple
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_target_resolution_zero_one_mismatch_and_multiple() -> None:
    """Target resolution rules: zero resolved -> candidate; 1 mismatch -> conflict; >1 -> conflict."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()

    # 1. One mismatch
    other_id = UUID("99999999-9999-9999-9999-999999999999")
    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(other_id,),
    ):
        with pytest.raises(
            IncidentPersistenceConflictError, match="does not match candidate"
        ):
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

    # 2. Multiple resolved
    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(other_id, UUID("88888888-8888-8888-8888-888888888888")),
    ):
        with pytest.raises(
            IncidentPersistenceConflictError, match="multiple distinct incidents"
        ):
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )


# ---------------------------------------------------------------------------
# TEST 9: test_membership_without_parent_is_corruption
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_membership_without_parent_is_corruption() -> None:
    """Resolved membership with missing parent incident record must raise corruption."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    cand_id = derive_incident_id(
        tenant_id=_TENANT_A,
        environment="production",
        anchor_signal_fingerprint=_FP1,
    )
    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(cand_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=None,  # parent missing
    ):
        with pytest.raises(
            IncidentPersistenceCorruptionError, match="parent record is missing"
        ):
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )


# ---------------------------------------------------------------------------
# TEST 10: test_absent_incident_with_expectation_is_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_absent_incident_with_expectation_is_conflict() -> None:
    """Absent incident when expectation is supplied must raise conflict with zero writes."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
    ) as mock_insert:
        with pytest.raises(
            IncidentPersistenceConflictError, match="incident does not exist"
        ):
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=1,
                expected_revision_fingerprint="a" * 64,
            )
        assert mock_insert.call_count == 0


# ---------------------------------------------------------------------------
# TEST 11: test_creation_calls_all_five_write_primitives_once_in_order
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_creation_calls_all_five_write_primitives_once_in_order() -> None:
    """Creation must invoke all 5 repository write primitives once in exact order."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()
    call_order: list[str] = []

    async def mock_insert(*args: Any, **kwargs: Any) -> None:
        call_order.append("insert_incident")

    async def mock_links(*args: Any, **kwargs: Any) -> None:
        call_order.append("append_signal_links")

    async def mock_evidence(*args: Any, **kwargs: Any) -> None:
        call_order.append("append_correlation_evidence")

    async def mock_timeline(*args: Any, **kwargs: Any) -> None:
        call_order.append("append_timeline_events")

    async def mock_intent(*args: Any, **kwargs: Any) -> None:
        call_order.append("append_publication_intent")

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
        side_effect=mock_insert,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_signal_links",
        side_effect=mock_links,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_correlation_evidence",
        side_effect=mock_evidence,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_timeline_events",
        side_effect=mock_timeline,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_publication_intent",
        side_effect=mock_intent,
    ):
        result = await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,  # type: ignore
        )

        assert isinstance(result, IncidentAggregationResult)
        assert result.material_update is True
        assert call_order == [
            "insert_incident",
            "append_signal_links",
            "append_correlation_evidence",
            "append_timeline_events",
            "append_publication_intent",
        ]


# ---------------------------------------------------------------------------
# TEST 12: test_absent_expectation_exact_replay_has_zero_writes
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_absent_expectation_exact_replay_has_zero_writes() -> None:
    """Present incident with absent expectation and material_update=False is replay with zero writes."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    # First get existing aggregate
    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate

    persisted = _make_persisted_state(existing_agg)

    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
    ) as m_ins, unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_signal_links",
    ) as m_links, unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "update_incident_cas",
    ) as m_cas:
        # Same request, no expectation -> exact replay
        res = await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,  # type: ignore
            expected_revision=None,
            expected_revision_fingerprint=None,
        )

        assert res.material_update is False
        assert len(res.timeline_events) == 0
        assert res.aggregate == existing_agg
        assert m_ins.call_count == 0
        assert m_links.call_count == 0
        assert m_cas.call_count == 0


# ---------------------------------------------------------------------------
# TEST 13: test_stale_expectation_exact_replay_has_zero_writes
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_expectation_exact_replay_has_zero_writes() -> None:
    """Present incident with stale expectation and material_update=False is replay exemption with zero writes."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate

    persisted = _make_persisted_state(existing_agg)

    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
    ) as m_ins, unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "update_incident_cas",
    ) as m_cas:
        # Request with stale expectation (rev 999), but transition is no-op -> replay exemption!
        res = await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,  # type: ignore
            expected_revision=999,
            expected_revision_fingerprint="f" * 64,
        )

        assert res.material_update is False
        assert len(res.timeline_events) == 0
        assert res.aggregate == existing_agg
        assert m_ins.call_count == 0
        assert m_cas.call_count == 0


# ---------------------------------------------------------------------------
# TEST 14: test_create_versus_existing_material_result_is_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_create_versus_existing_material_result_is_conflict() -> None:
    """Present incident with absent expectation but material changes is a conflict."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group1 = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    initial_result = create_or_update_incident(
        group=group1,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate

    persisted = _make_persisted_state(existing_agg)

    # New request introduces member m2 -> would be a material update
    m2 = _make_member(_SIG2_ID, _TIME2, "billing", _FP2)
    inp2 = _make_input(_SIG2_ID, _TIME2, "billing", _FP2)
    group2 = _make_group([m1, m2])
    inputs2 = {_SIG1_ID: inp1, _SIG2_ID: inp2}

    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted,
    ):
        with pytest.raises(
            IncidentPersistenceConflictError, match="cannot perform material update"
        ):
            await persist_incident_atomic(
                group=group2,
                signal_inputs=inputs2,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=None,
                expected_revision_fingerprint=None,
            )


# ---------------------------------------------------------------------------
# TEST 15: test_matching_expectation_material_update_appends_deltas_and_cas
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_matching_expectation_material_update_appends_deltas_and_cas() -> None:
    """Matching expectation with material changes appends only deltas and CAS updates parent."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group1 = _make_group([m1])
    inputs1 = {_SIG1_ID: inp1}

    initial_result = create_or_update_incident(
        group=group1,
        signal_inputs=inputs1,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate

    persisted = _make_persisted_state(existing_agg)

    # Material update with member 2 and evidence
    m2 = _make_member(_SIG2_ID, _TIME2, "billing", _FP2)
    inp2 = _make_input(_SIG2_ID, _TIME2, "billing", _FP2)
    ev1_2 = _make_evidence(_SIG1_ID, _SIG2_ID)
    group2 = _make_group([m1, m2], [ev1_2])
    inputs2 = {_SIG1_ID: inp1, _SIG2_ID: inp2}

    factory = MockSessionFactory()
    calls: list[str] = []

    async def mock_links(inc_id: UUID, links: Any) -> None:
        calls.append("append_signal_links")
        assert len(links) == 1
        assert links[0].signal_id == _SIG2_ID

    async def mock_evidence(inc_id: UUID, ev: Any) -> None:
        calls.append("append_correlation_evidence")
        assert len(ev) == 1

    async def mock_timeline(events: Any) -> None:
        calls.append("append_timeline_events")
        assert len(events) > 0

    async def mock_intent(agg: IncidentAggregate, *, intent_time: datetime) -> None:
        calls.append("append_publication_intent")
        assert agg.revision == 2

    async def mock_cas(
        agg: IncidentAggregate,
        *,
        expected_revision: int,
        expected_revision_fingerprint: str,
    ) -> bool:
        calls.append("update_incident_cas")
        assert expected_revision == existing_agg.revision
        assert expected_revision_fingerprint == existing_agg.revision_fingerprint
        return True

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_signal_links",
        side_effect=mock_links,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_correlation_evidence",
        side_effect=mock_evidence,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_timeline_events",
        side_effect=mock_timeline,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_publication_intent",
        side_effect=mock_intent,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "update_incident_cas",
        side_effect=mock_cas,
    ):
        res = await persist_incident_atomic(
            group=group2,
            signal_inputs=inputs2,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME2,
            tenant_id=_TENANT_A,
            session_factory=factory,  # type: ignore
            expected_revision=existing_agg.revision,
            expected_revision_fingerprint=existing_agg.revision_fingerprint,
        )

        assert res.material_update is True
        assert res.aggregate.revision == 2
        assert calls == [
            "append_signal_links",
            "append_correlation_evidence",
            "append_timeline_events",
            "append_publication_intent",
            "update_incident_cas",
        ]


# ---------------------------------------------------------------------------
# TEST 16: test_stale_material_update_is_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_material_update_is_conflict() -> None:
    """Stale expectation when material update is required must raise conflict."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group1 = _make_group([m1])
    inputs1 = {_SIG1_ID: inp1}

    initial_result = create_or_update_incident(
        group=group1,
        signal_inputs=inputs1,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate

    persisted = _make_persisted_state(existing_agg)

    m2 = _make_member(_SIG2_ID, _TIME2, "billing", _FP2)
    inp2 = _make_input(_SIG2_ID, _TIME2, "billing", _FP2)
    group2 = _make_group([m1, m2])
    inputs2 = {_SIG1_ID: inp1, _SIG2_ID: inp2}

    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted,
    ):
        # Expectation revision 999 is stale
        with pytest.raises(IncidentPersistenceConflictError, match="Stale writer"):
            await persist_incident_atomic(
                group=group2,
                signal_inputs=inputs2,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=999,
                expected_revision_fingerprint="f" * 64,
            )


# ---------------------------------------------------------------------------
# TEST 17: test_cas_false_rolls_back_as_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cas_false_rolls_back_as_conflict() -> None:
    """CAS update returning False must trigger rollback and raise conflict."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group1 = _make_group([m1])
    inputs1 = {_SIG1_ID: inp1}

    initial_result = create_or_update_incident(
        group=group1,
        signal_inputs=inputs1,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate

    persisted = _make_persisted_state(existing_agg)

    m2 = _make_member(_SIG2_ID, _TIME2, "billing", _FP2)
    inp2 = _make_input(_SIG2_ID, _TIME2, "billing", _FP2)
    group2 = _make_group([m1, m2])
    inputs2 = {_SIG1_ID: inp1, _SIG2_ID: inp2}

    factory = MockSessionFactory()

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_signal_links",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_correlation_evidence",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_timeline_events",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_publication_intent",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "update_incident_cas",
        return_value=False,  # CAS failed
    ):
        with pytest.raises(IncidentPersistenceConflictError, match="CAS update failed"):
            await persist_incident_atomic(
                group=group2,
                signal_inputs=inputs2,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=existing_agg.revision,
                expected_revision_fingerprint=existing_agg.revision_fingerprint,
            )

        # Confirm rollback happened on session
        assert len(factory.sessions) == 1
        assert factory.sessions[0].transaction_rolled_back is True


# ---------------------------------------------------------------------------
# TEST 18: test_23505_recovery_starts_one_new_transaction
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_23505_recovery_starts_one_new_transaction() -> None:
    """SQLSTATE 23505 during creation must roll back primary and open exactly one recovery transaction."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()

    # Pre-calculate what the winner would be
    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    winner_agg = initial_result.aggregate

    winner_persisted = _make_persisted_state(winner_agg)

    primary_attempt = True

    async def mock_load(
        inc_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        nonlocal primary_attempt
        if primary_attempt:
            return None
        return winner_persisted

    # Create dummy 23505 error
    dummy_orig = unittest.mock.MagicMock()
    dummy_orig.pgcode = "23505"
    dummy_orig.sqlstate = "23505"
    db_err = DBAPIError("insert into incidents...", {}, dummy_orig)
    conflict_err = IncidentPersistenceConflictError("unique constraint")
    conflict_err.__cause__ = db_err

    async def mock_insert(agg: IncidentAggregate) -> None:
        nonlocal primary_attempt
        primary_attempt = False
        raise conflict_err

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        side_effect=[(), (winner_agg.incident_id,)],
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        side_effect=mock_load,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
        side_effect=mock_insert,
    ):
        res = await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,  # type: ignore
        )

        assert res.material_update is False
        # Exactly two sessions: 1 primary (rolled back), 1 recovery (committed)
        assert len(factory.sessions) == 2
        assert factory.sessions[0].transaction_rolled_back is True
        assert factory.sessions[1].transaction_committed is True


# ---------------------------------------------------------------------------
# TEST 19: test_23505_recovery_replay_has_zero_writes
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_23505_recovery_replay_has_zero_writes() -> None:
    """During 23505 recovery, replay must execute zero write operations."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()

    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    winner_agg = initial_result.aggregate

    winner_persisted = _make_persisted_state(winner_agg)

    primary_attempt = True
    recovery_writes = 0

    async def mock_load(
        inc_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        nonlocal primary_attempt
        if primary_attempt:
            return None
        return winner_persisted

    dummy_orig = unittest.mock.MagicMock()
    dummy_orig.pgcode = "23505"
    db_err = DBAPIError("insert into incidents...", {}, dummy_orig)
    conflict_err = IncidentPersistenceConflictError("unique violation")
    conflict_err.__cause__ = db_err

    async def mock_insert(agg: IncidentAggregate) -> None:
        nonlocal primary_attempt, recovery_writes
        if primary_attempt:
            primary_attempt = False
            raise conflict_err
        recovery_writes += 1

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        side_effect=[(), (winner_agg.incident_id,)],
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        side_effect=mock_load,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
        side_effect=mock_insert,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "append_signal_links",
    ) as m_links, unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "update_incident_cas",
    ) as m_cas:
        res = await persist_incident_atomic(
            group=group,
            signal_inputs=inputs,
            desired_state=CorrelationWindowState.COLLECTING,
            processing_time=_TIME1,
            tenant_id=_TENANT_A,
            session_factory=factory,  # type: ignore
        )

        assert res.material_update is False
        assert recovery_writes == 0
        assert m_links.call_count == 0
        assert m_cas.call_count == 0


# ---------------------------------------------------------------------------
# TEST 20: test_23505_recovery_material_or_rejection_is_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_23505_recovery_material_or_rejection_is_conflict() -> None:
    """If recovery against winner yields material update or rejection, it must raise conflict."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()

    # Create winner with different status or correlation state so transition yields material update
    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.FINALIZED,  # CLOSED winner
        existing_aggregate=None,
    )
    winner_agg = initial_result.aggregate

    winner_persisted = _make_persisted_state(winner_agg)

    primary_attempt = True

    async def mock_load(
        inc_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        nonlocal primary_attempt
        if primary_attempt:
            return None
        return winner_persisted

    dummy_orig = unittest.mock.MagicMock()
    dummy_orig.pgcode = "23505"
    db_err = DBAPIError("insert into incidents...", {}, dummy_orig)
    conflict_err = IncidentPersistenceConflictError("unique violation")
    conflict_err.__cause__ = db_err

    async def mock_insert(agg: IncidentAggregate) -> None:
        nonlocal primary_attempt
        primary_attempt = False
        raise conflict_err

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        side_effect=[(), (winner_agg.incident_id,)],
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        side_effect=mock_load,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
        side_effect=mock_insert,
    ):
        # We request OPEN, winner is CLOSED -> transition yields material update -> Conflict
        with pytest.raises(
            IncidentPersistenceConflictError, match="Concurrent-create.*winner"
        ):
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )


# ---------------------------------------------------------------------------
# TEST 21: test_error_translation_preserves_cause_and_hides_driver_error
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_error_translation_preserves_cause_and_hides_driver_error() -> None:
    """Low-level database exceptions must be translated to IncidentPersistenceStoreError preserving cause."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}

    factory = MockSessionFactory()

    raw_driver_error = OperationalError("connection refused", {}, Exception("TCP drop"))

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        side_effect=raw_driver_error,
    ):
        with pytest.raises(IncidentPersistenceStoreError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

        assert exc_info.value.__cause__ is raw_driver_error
        assert isinstance(exc_info.value, IncidentPersistenceError)


# ---------------------------------------------------------------------------
# TEST 22: test_creation_aggregate_input_error_translates_to_persistence_input
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_creation_aggregate_input_error_translates_to_persistence_input() -> None:
    """AggregateInputError during creation must translate to IncidentPersistenceInputError."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    simulated_err = AggregateInputError("Invalid input during creation")

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=None,
    ), unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        side_effect=simulated_err,
    ):
        with pytest.raises(IncidentPersistenceInputError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

        assert exc_info.value.__cause__ is simulated_err
        assert "Invalid aggregate input during incident creation" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TEST 23: test_creation_aggregate_state_error_translates_to_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_creation_aggregate_state_error_translates_to_conflict() -> None:
    """AggregateStateError during creation must translate to IncidentPersistenceConflictError."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    simulated_err = AggregateStateError("State invariant violated during creation")

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=None,
    ), unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        side_effect=simulated_err,
    ):
        with pytest.raises(IncidentPersistenceConflictError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

        assert exc_info.value.__cause__ is simulated_err
        assert "Aggregate state conflict during incident creation" in str(
            exc_info.value
        )


# ---------------------------------------------------------------------------
# TEST 24: test_matching_update_aggregate_input_error_translates_to_persistence_input
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_matching_update_aggregate_input_error_translates_to_persistence_input() -> (
    None
):
    """AggregateInputError during matching update must translate to IncidentPersistenceInputError."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate
    persisted_state = _make_persisted_state(existing_agg)

    simulated_err = AggregateInputError("Invalid input during update")

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted_state,
    ), unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        side_effect=simulated_err,
    ):
        with pytest.raises(IncidentPersistenceInputError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=existing_agg.revision,
                expected_revision_fingerprint=existing_agg.revision_fingerprint,
            )

        assert exc_info.value.__cause__ is simulated_err
        assert "Invalid aggregate input during incident update" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TEST 25: test_matching_update_aggregate_state_error_translates_to_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_matching_update_aggregate_state_error_translates_to_conflict() -> None:
    """AggregateStateError during matching update must translate to IncidentPersistenceConflictError."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate
    persisted_state = _make_persisted_state(existing_agg)

    simulated_err = AggregateStateError("State invariant violated during update")

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted_state,
    ), unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        side_effect=simulated_err,
    ):
        with pytest.raises(IncidentPersistenceConflictError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=existing_agg.revision,
                expected_revision_fingerprint=existing_agg.revision_fingerprint,
            )

        assert exc_info.value.__cause__ is simulated_err
        assert "Aggregate state conflict during incident update" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TEST 26: test_replay_classification_aggregate_error_translates_to_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_replay_classification_aggregate_error_translates_to_conflict() -> None:
    """Aggregate error during replay classification must translate to IncidentPersistenceConflictError."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    existing_agg = initial_result.aggregate
    persisted_state = _make_persisted_state(existing_agg)

    simulated_err = AggregateStateError("State error during replay check")

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        return_value=(existing_agg.incident_id,),
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        return_value=persisted_state,
    ), unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        side_effect=simulated_err,
    ):
        with pytest.raises(IncidentPersistenceConflictError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME2,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
                expected_revision=None,
                expected_revision_fingerprint=None,
            )

        assert exc_info.value.__cause__ is simulated_err
        assert "Aggregate state conflict during replay classification" in str(
            exc_info.value
        )


# ---------------------------------------------------------------------------
# TEST 27: test_recovery_aggregate_error_translates_to_conflict
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_recovery_aggregate_error_translates_to_conflict() -> None:
    """Aggregate error during 23505 recovery must translate to IncidentPersistenceConflictError."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    initial_result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )
    winner_agg = initial_result.aggregate
    winner_persisted = _make_persisted_state(winner_agg)

    primary_attempt = True

    async def mock_load(
        inc_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        nonlocal primary_attempt
        if primary_attempt:
            return None
        return winner_persisted

    dummy_orig = unittest.mock.MagicMock()
    dummy_orig.pgcode = "23505"
    db_err = DBAPIError("insert into incidents...", {}, dummy_orig)
    conflict_err = IncidentPersistenceConflictError("unique violation")
    conflict_err.__cause__ = db_err

    async def mock_insert(agg: IncidentAggregate) -> None:
        nonlocal primary_attempt
        primary_attempt = False
        raise conflict_err

    simulated_recovery_err = AggregateStateError("Recovery state rejection")

    def mock_create_or_update(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("existing_aggregate") is None:
            return initial_result
        raise simulated_recovery_err

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        side_effect=[(), (winner_agg.incident_id,)],
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "load_incident",
        side_effect=mock_load,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "insert_incident",
        side_effect=mock_insert,
    ), unittest.mock.patch(
        "app.correlation.persistence_service.create_or_update_incident",
        side_effect=mock_create_or_update,
    ):
        with pytest.raises(IncidentPersistenceConflictError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

        assert exc_info.value.__cause__ is simulated_recovery_err
        assert "Concurrent-create recovery rejected transition against winner" in str(
            exc_info.value
        )


# ---------------------------------------------------------------------------
# TEST 28: test_database_error_public_message_excludes_sql_parameters_and_driver_text
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_database_error_public_message_excludes_sql_parameters_and_driver_text() -> (
    None
):
    """Public persistence errors must sanitize database diagnostics and driver text."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    sensitive_sql = "SELECT secret_col FROM incidents WHERE pw = 'super_secret_p@ss'"
    sensitive_driver = (
        "psycopg.errors.UniqueViolation: duplicate key value violates unique constraint"
    )
    orig_driver_err = Exception(
        f"{sensitive_driver} ({sensitive_sql}) [SQLSTATE 23505]"
    )

    raw_db_err = DBAPIError(sensitive_sql, {"pw": "super_secret_p@ss"}, orig_driver_err)
    repo_err = IncidentPersistenceStoreError(
        f"Database persistence store error: {raw_db_err}"
    )
    repo_err.__cause__ = raw_db_err

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        side_effect=repo_err,
    ):
        with pytest.raises(IncidentPersistenceStoreError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

        public_msg = str(exc_info.value)
        assert "SELECT" not in public_msg
        assert "super_secret_p@ss" not in public_msg
        assert "psycopg" not in public_msg
        assert "SQLSTATE" not in public_msg
        assert "23505" not in public_msg
        assert public_msg == "Database operation failed during incident persistence"


# ---------------------------------------------------------------------------
# TEST 29: test_sanitized_database_error_preserves_original_cause_chain
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sanitized_database_error_preserves_original_cause_chain() -> None:
    """Sanitized database errors must preserve the original repository error and DBAPI cause chain."""
    m1 = _make_member(_SIG1_ID, _TIME1, "auth", _FP1)
    inp1 = _make_input(_SIG1_ID, _TIME1, "auth", _FP1)
    group = _make_group([m1])
    inputs = {_SIG1_ID: inp1}
    factory = MockSessionFactory()

    orig_driver = Exception("low-level socket drop")
    raw_db_err = DBAPIError("INSERT INTO incidents ...", {}, orig_driver)
    repo_err = IncidentPersistenceStoreError(
        "Database persistence store error: connection closed"
    )
    repo_err.__cause__ = raw_db_err

    with unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "bind_tenant_context",
        return_value=None,
    ), unittest.mock.patch.object(
        SqlAlchemyIncidentPersistenceRepository,
        "resolve_incident_ids",
        side_effect=repo_err,
    ):
        with pytest.raises(IncidentPersistenceStoreError) as exc_info:
            await persist_incident_atomic(
                group=group,
                signal_inputs=inputs,
                desired_state=CorrelationWindowState.COLLECTING,
                processing_time=_TIME1,
                tenant_id=_TENANT_A,
                session_factory=factory,  # type: ignore
            )

        public_err = exc_info.value
        assert isinstance(public_err, IncidentPersistenceStoreError)
        assert (
            str(public_err) == "Database operation failed during incident persistence"
        )

        assert public_err.__cause__ is repo_err
        assert repo_err.__cause__ is raw_db_err
        assert raw_db_err.orig is orig_driver
