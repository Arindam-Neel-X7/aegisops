"""Deterministic focused test suite for the Task 4.8 incident aggregate contract."""

from __future__ import annotations

import itertools
import unittest.mock
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.anomaly.lineage import AnomalyReproducibilityLineage
from app.anomaly.models import AnomalySignal, CalibrationMetadata
from app.correlation.aggregate import (
    create_or_update_incident,
)
from app.correlation.config import (
    ConflictingSignalDisposition,
    CorrelationConfig,
    CorrelationScoreWeights,
    DisconnectedSignalDisposition,
    DuplicateSignalDisposition,
    InsufficientContextDisposition,
    LateSignalDisposition,
    SeverityAggregationPolicy,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    AggregateInputError,
    AggregateStateError,
    GroupingContractError,
    IncidentIdentityError,
)
from app.correlation.grouping import (
    CorrelationGroup,
    GroupingMemberReference,
    group_candidate_signals,
)
from app.correlation.identity import derive_incident_id
from app.correlation.models import (
    MAX_REVISION,
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentStatus,
    IncidentTimelineEventType,
    _canonical_model_key,
)
from app.correlation.serialization import (
    serialize_incident_aggregate,
    serialize_incident_timeline_event,
)
from app.correlation.topology import (
    DirectedTopologyEdge,
    TopologyServiceNode,
    TopologySnapshot,
    TopologyVersion,
)
from app.simulator.interfaces import DependencyType, ServiceType
from app.telemetry.schemas import EventSeverity

# ---------------------------------------------------------------------------
# Deterministic constants and fixtures
# ---------------------------------------------------------------------------

_FIXED_TENANT = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_FIXED_ENV = "production"

_SIG1_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_SIG2_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
_SIG3_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")

_FP1 = "1" * 64
_FP2 = "2" * 64
_FP3 = "3" * 64

_TIME1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
_TIME2 = datetime(2026, 1, 1, 10, 1, 0, tzinfo=timezone.utc)
_TIME3 = datetime(2026, 1, 1, 10, 2, 0, tzinfo=timezone.utc)

_PROC_TIME1 = datetime(2026, 1, 1, 10, 10, 0, tzinfo=timezone.utc)
_PROC_TIME2 = datetime(2026, 1, 1, 10, 20, 0, tzinfo=timezone.utc)


def _make_input(
    sig_id: uuid.UUID,
    service: str,
    event_time: datetime,
    score: float,
    severity: EventSeverity,
    fp: str,
    tenant_id: uuid.UUID = _FIXED_TENANT,
    environment: str = _FIXED_ENV,
) -> AnomalyCorrelationInput:
    sig = AnomalySignal(
        signal_id=sig_id,
        event_time=event_time,
        service=service,
        metric_or_feature="cpu_utilization",
        model_name="isolation_forest",
        model_version="1.0",
        anomaly_score=score,
        severity=severity,
        threshold_or_calibration=CalibrationMetadata(
            method="percentile", threshold_value=0.8, calibration_version="1.0"
        ),
        run_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scenario_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        environment=environment,
        tenant_id=tenant_id,
    )
    lin = AnomalyReproducibilityLineage(
        signal_id=sig_id,
        run_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        scenario_id="scenario_1",
        scenario_version="1.0",
        seed=42,
        reproducibility_key="rep_key_1",
        model_name="isolation_forest",
        model_version="1.0",
        source_event_ids=(uuid.UUID("55555555-5555-5555-5555-555555555555"),),
        semantic_fingerprint=fp,
    )
    return AnomalyCorrelationInput(signal=sig, lineage=lin)


def _make_evidence(
    sig_a: uuid.UUID,
    sig_b: uuid.UUID,
    score: float = 0.9,
    accepted: bool = True,
    policy_version: str = "correlation/v1",
) -> CorrelationEvidence:
    left = min(sig_a, sig_b, key=lambda u: u.bytes)
    right = max(sig_a, sig_b, key=lambda u: u.bytes)
    return CorrelationEvidence(
        left_signal_id=left,
        right_signal_id=right,
        temporal_score=score,
        service_score=score,
        topology_score=score,
        symptom_score=score,
        context_score=score,
        weighted_score=score,
        accepted=accepted,
        policy_version=policy_version,
        reason="accepted test pair",
    )


def _make_group(
    members: Sequence[GroupingMemberReference],
    evidence: Sequence[CorrelationEvidence],
    started_at: datetime | None = None,
    last_seen_at: datetime | None = None,
    time_span_seconds: float | None = None,
    topology_diameter_hops: int = 1,
) -> CorrelationGroup:
    times = [m.event_time for m in members]
    st = min(times) if started_at is None else started_at
    lt = max(times) if last_seen_at is None else last_seen_at
    span = (lt - st).total_seconds() if time_span_seconds is None else time_span_seconds
    sorted_m = tuple(sorted(members, key=lambda m: m.signal_id.bytes))
    return CorrelationGroup(
        policy_version="correlation/v1",
        configuration_fingerprint="c" * 64,
        topology_snapshot_fingerprint="d" * 64,
        members=sorted_m,
        accepted_evidence=tuple(evidence),
        started_at=st,
        last_seen_at=lt,
        time_span_seconds=span,
        topology_diameter_hops=topology_diameter_hops,
        group_fingerprint="0" * 64,
    )


# Standard 2-member and 3-member baseline fixtures
_M1 = GroupingMemberReference(
    signal_id=_SIG1_ID, semantic_fingerprint=_FP1, service="auth", event_time=_TIME1
)
_M2 = GroupingMemberReference(
    signal_id=_SIG2_ID, semantic_fingerprint=_FP2, service="billing", event_time=_TIME2
)
_M3 = GroupingMemberReference(
    signal_id=_SIG3_ID, semantic_fingerprint=_FP3, service="orders", event_time=_TIME3
)

_EV12 = _make_evidence(_SIG1_ID, _SIG2_ID)
_EV13 = _make_evidence(_SIG1_ID, _SIG3_ID)
_EV23 = _make_evidence(_SIG2_ID, _SIG3_ID)

_INP1 = _make_input(_SIG1_ID, "auth", _TIME1, 0.9, EventSeverity.WARNING, _FP1)
_INP2 = _make_input(_SIG2_ID, "billing", _TIME2, 0.85, EventSeverity.WARNING, _FP2)
_INP3 = _make_input(_SIG3_ID, "orders", _TIME3, 0.95, EventSeverity.CRITICAL, _FP3)

_INPUTS_12 = {_SIG1_ID: _INP1, _SIG2_ID: _INP2}
_INPUTS_123 = {_SIG1_ID: _INP1, _SIG2_ID: _INP2, _SIG3_ID: _INP3}

_GROUP_12 = _make_group([_M1, _M2], [_EV12])
_GROUP_123 = _make_group(
    [_M1, _M2, _M3], [_EV12, _EV13, _EV23], topology_diameter_hops=2
)

_GROUP_1 = _make_group([_M1], [])
_INPUTS_1 = {_SIG1_ID: _INP1}
_GROUP_2 = _make_group([_M2], [])
_INPUTS_2 = {_SIG2_ID: _INP2}


# ---------------------------------------------------------------------------
# 1. Creation, identity, and canonical projection
# ---------------------------------------------------------------------------


def test_create_incident_identity_uses_canonical_anchor() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    expected_id = derive_incident_id(
        tenant_id=_FIXED_TENANT,
        environment=_FIXED_ENV,
        anchor_signal_fingerprint=_FP1,
    )
    assert res.aggregate.incident_id == expected_id
    assert res.aggregate.primary_service == "auth"


def test_create_incident_number_uses_complete_uuid() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    inc_id = res.aggregate.incident_id
    assert res.aggregate.incident_number == f"INC-{inc_id.hex.upper()}"
    assert len(res.aggregate.incident_number) == 36


def test_create_member_order_is_canonical() -> None:
    res = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    refs = res.aggregate.signal_references
    sorted_refs = tuple(sorted(refs, key=_canonical_model_key))
    assert refs == sorted_refs


def test_create_affected_service_order_is_sorted_unique() -> None:
    res = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res.aggregate.affected_services == ("auth", "billing", "orders")
    assert len(res.aggregate.affected_services) == len(
        set(res.aggregate.affected_services)
    )


def test_create_preserves_member_severity_and_anomaly_score() -> None:
    res = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    ref_map = {r.signal_id: r for r in res.aggregate.signal_references}
    assert ref_map[_SIG1_ID].severity == EventSeverity.WARNING
    assert ref_map[_SIG1_ID].anomaly_score == 0.9
    assert ref_map[_SIG2_ID].severity == EventSeverity.WARNING
    assert ref_map[_SIG2_ID].anomaly_score == 0.85
    assert ref_map[_SIG3_ID].severity == EventSeverity.CRITICAL
    assert ref_map[_SIG3_ID].anomaly_score == 0.95


def test_create_uses_member_time_bounds_and_processing_timestamps() -> None:
    res = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res.aggregate.started_at == _TIME1
    assert res.aggregate.last_seen_at == _TIME3
    assert res.aggregate.created_at == _PROC_TIME1
    assert res.aggregate.updated_at == _PROC_TIME1


def test_create_sets_open_status_and_requested_correlation_state() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.SETTLING,
    )
    assert res.aggregate.status == IncidentStatus.OPEN
    assert res.aggregate.correlation_state == CorrelationWindowState.SETTLING


def test_create_collecting_emits_exact_creation_and_attachment_timeline() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res.material_update is True
    assert len(res.timeline_events) == 2
    ev0 = res.timeline_events[0]
    ev1 = res.timeline_events[1]
    assert ev0.event_type == IncidentTimelineEventType.CREATED
    assert ev0.source_signal_id == _SIG1_ID
    assert (
        ev0.summary
        == "Incident aggregate initialized from founding correlation anchor signal"
    )
    assert ev1.event_type == IncidentTimelineEventType.SIGNAL_ATTACHED
    assert ev1.source_signal_id == _SIG2_ID
    assert ev1.summary == "Correlated signal attached to incident aggregate"


def test_create_finalized_emits_exact_finalization_timeline() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
    )
    assert res.material_update is True
    assert len(res.timeline_events) == 3
    assert res.timeline_events[0].event_type == IncidentTimelineEventType.CREATED
    assert (
        res.timeline_events[1].event_type == IncidentTimelineEventType.SIGNAL_ATTACHED
    )
    ev2 = res.timeline_events[2]
    assert ev2.event_type == IncidentTimelineEventType.CORRELATION_FINALIZED
    assert ev2.source_signal_id is None
    assert ev2.summary == "Correlation aggregation window finalized"


def test_create_result_and_models_are_frozen() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    with pytest.raises(ValidationError):
        res.material_update = False  # type: ignore[misc]
    with pytest.raises(ValidationError):
        res.aggregate.revision = 99  # type: ignore[misc]
    with pytest.raises(ValidationError):
        res.timeline_events[0].summary = "modified"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 2. Replay and update semantics
# ---------------------------------------------------------------------------


def test_exact_replay_returns_existing_aggregate_and_no_events() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    assert res2.material_update is False
    assert res2.timeline_events == ()
    assert res2.aggregate is res1.aggregate


def test_exact_replay_preserves_revision_timestamps_and_fingerprint() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    assert res2.aggregate.revision == res1.aggregate.revision
    assert res2.aggregate.created_at == res1.aggregate.created_at
    assert res2.aggregate.updated_at == res1.aggregate.updated_at
    assert res2.aggregate.revision_fingerprint == res1.aggregate.revision_fingerprint
    assert serialize_incident_aggregate(res2.aggregate) == serialize_incident_aggregate(
        res1.aggregate
    )


def test_attach_member_increments_revision_once() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    assert res2.material_update is True
    assert res2.aggregate.revision == res1.aggregate.revision + 1
    assert len(res2.aggregate.signal_references) == 3


def test_full_snapshot_cannot_remove_existing_member() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_1,
        signal_inputs=_INPUTS_1,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    with pytest.raises(
        AggregateStateError, match="Existing aggregate members cannot be removed"
    ):
        create_or_update_incident(
            group=_GROUP_2,
            signal_inputs=_INPUTS_2,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_retained_member_attributes_cannot_change() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Alter member 2 service
    inp2_mod = _make_input(
        _SIG2_ID, "altered_service", _TIME2, 0.85, EventSeverity.WARNING, _FP2
    )
    m2_mod = GroupingMemberReference(
        signal_id=_SIG2_ID,
        semantic_fingerprint=_FP2,
        service="altered_service",
        event_time=_TIME2,
    )
    group_mod = _make_group([_M1, m2_mod], [_EV12])
    with pytest.raises(
        AggregateStateError,
        match="Retained aggregate member attributes cannot be modified",
    ):
        create_or_update_incident(
            group=group_mod,
            signal_inputs={_SIG1_ID: _INP1, _SIG2_ID: inp2_mod},
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_material_update_rejects_processing_time_before_updated_at() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    with pytest.raises(
        AggregateStateError,
        match="Processing time cannot precede existing aggregate updated time",
    ):
        create_or_update_incident(
            group=_GROUP_123,
            signal_inputs=_INPUTS_123,
            processing_time=_PROC_TIME1,  # Precedes res1.updated_at
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_finalized_state_cannot_regress() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
    )
    with pytest.raises(
        AggregateStateError, match="Correlation window state cannot regress"
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_collecting_to_settling_emits_exact_state_event() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.SETTLING,
        existing_aggregate=res1.aggregate,
    )
    assert res2.material_update is True
    assert len(res2.timeline_events) == 1
    ev = res2.timeline_events[0]
    assert ev.event_type == IncidentTimelineEventType.CORRELATION_STATE_CHANGED
    assert ev.source_signal_id is None
    assert ev.summary == "Correlation aggregation window advanced to settling"


def test_settling_to_finalized_emits_exact_finalization_event() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.SETTLING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    assert res2.material_update is True
    assert len(res2.timeline_events) == 1
    ev = res2.timeline_events[0]
    assert ev.event_type == IncidentTimelineEventType.CORRELATION_FINALIZED
    assert ev.source_signal_id is None
    assert ev.summary == "Correlation aggregation window finalized"


def test_finalized_replay_is_idempotent() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    assert res2.material_update is False
    assert res2.timeline_events == ()
    assert res2.aggregate is res1.aggregate


# ---------------------------------------------------------------------------
# 3. Severity and member evidence
# ---------------------------------------------------------------------------


def test_creation_severity_is_maximum_member_severity() -> None:
    res = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res.aggregate.severity == EventSeverity.CRITICAL


def test_lower_severity_attachment_does_not_reduce_severity() -> None:
    # Initial aggregate with CRITICAL
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs={
            _SIG1_ID: _make_input(
                _SIG1_ID, "auth", _TIME1, 0.9, EventSeverity.CRITICAL, _FP1
            ),
            _SIG2_ID: _make_input(
                _SIG2_ID, "billing", _TIME2, 0.85, EventSeverity.WARNING, _FP2
            ),
        },
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res1.aggregate.severity == EventSeverity.CRITICAL

    # Attach member 3 with WARNING (lower)
    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs={
            _SIG1_ID: _make_input(
                _SIG1_ID, "auth", _TIME1, 0.9, EventSeverity.CRITICAL, _FP1
            ),
            _SIG2_ID: _make_input(
                _SIG2_ID, "billing", _TIME2, 0.85, EventSeverity.WARNING, _FP2
            ),
            _SIG3_ID: _make_input(
                _SIG3_ID, "orders", _TIME3, 0.7, EventSeverity.WARNING, _FP3
            ),
        },
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    assert res2.aggregate.severity == EventSeverity.CRITICAL
    event_types = [e.event_type for e in res2.timeline_events]
    assert IncidentTimelineEventType.SEVERITY_CHANGED not in event_types


def test_higher_severity_attachment_emits_severity_event() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,  # both WARNING
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res1.aggregate.severity == EventSeverity.WARNING

    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,  # member 3 is CRITICAL
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    assert res2.aggregate.severity == EventSeverity.CRITICAL
    sev_events = [
        e
        for e in res2.timeline_events
        if e.event_type == IncidentTimelineEventType.SEVERITY_CHANGED
    ]
    assert len(sev_events) == 1
    assert sev_events[0].source_signal_id == _SIG3_ID
    assert sev_events[0].summary == "Incident aggregate severity escalated"


def test_severity_event_source_is_canonical_for_tied_candidates() -> None:
    # Start with WARNING
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Add two new members simultaneously, both CRITICAL
    sig4_id = uuid.UUID("44444444-4444-4444-4444-444444444444")
    m4 = GroupingMemberReference(
        signal_id=sig4_id,
        semantic_fingerprint="4" * 64,
        service="payments",
        event_time=_TIME3,
    )
    ev14 = _make_evidence(_SIG1_ID, sig4_id)
    ev24 = _make_evidence(_SIG2_ID, sig4_id)
    ev34 = _make_evidence(_SIG3_ID, sig4_id)

    inp4 = _make_input(
        sig4_id, "payments", _TIME3, 0.95, EventSeverity.CRITICAL, "4" * 64
    )
    group_4 = _make_group(
        [_M1, _M2, _M3, m4],
        [_EV12, _EV13, _EV23, ev14, ev24, ev34],
        topology_diameter_hops=2,
    )
    res2 = create_or_update_incident(
        group=group_4,
        signal_inputs={
            _SIG1_ID: _INP1,
            _SIG2_ID: _INP2,
            _SIG3_ID: _INP3,
            sig4_id: inp4,
        },
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    sev_event = next(
        e
        for e in res2.timeline_events
        if e.event_type == IncidentTimelineEventType.SEVERITY_CHANGED
    )
    expected_canonical = min([_SIG3_ID, sig4_id], key=lambda u: u.bytes)
    assert sev_event.source_signal_id == expected_canonical


def test_existing_severity_inconsistent_with_members_is_rejected() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Construct invalid existing aggregate with inconsistent severity
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=EventSeverity.CRITICAL,  # Members are only WARNING
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing aggregate severity is inconsistent with severity policy",
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_severity_policy_version_mismatch_is_rejected() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    agg_bytes_before = res1.aggregate.model_dump_json()
    group_bytes_before = _GROUP_12.model_dump_json()
    inputs_bytes_before = {k: v.model_dump_json() for k, v in _INPUTS_12.items()}

    payload = res1.aggregate.model_dump(mode="python")
    modified = dict(payload)
    modified["severity_policy_version"] = "unsupported_policy/v99"

    with pytest.raises(ValidationError, match="unsupported severity_policy_version"):
        IncidentAggregate.model_validate(modified, strict=True)

    assert res1.aggregate.model_dump_json() == agg_bytes_before
    assert _GROUP_12.model_dump_json() == group_bytes_before
    assert {
        k: v.model_dump_json() for k, v in _INPUTS_12.items()
    } == inputs_bytes_before


def test_existing_member_anomaly_score_change_is_rejected() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Alter anomaly score of member 2
    inp2_mod = _make_input(
        _SIG2_ID, "billing", _TIME2, 0.99, EventSeverity.WARNING, _FP2
    )
    with pytest.raises(
        AggregateStateError,
        match="Retained aggregate member attributes cannot be modified",
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs={_SIG1_ID: _INP1, _SIG2_ID: inp2_mod},
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


# ---------------------------------------------------------------------------
# 4. State, identity, input, and evidence guards
# ---------------------------------------------------------------------------


def test_existing_affected_services_must_match_members_exactly() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=("auth", "billing", "stale_service"),
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing aggregate affected services are not canonical",
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_started_at_must_equal_earliest_member_time() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc),  # shifted
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError, match="Existing aggregate time bounds do not match members"
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_last_seen_at_must_equal_latest_member_time() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=datetime(2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc),  # shifted
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError, match="Existing aggregate time bounds do not match members"
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_incident_id_must_match_founding_signal() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    mismatched_id = uuid.UUID("99999999-9999-9999-9999-999999999999")
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=mismatched_id,
        incident_number=f"INC-{mismatched_id.hex.upper()}",
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing aggregate incident id does not match founding signal",
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_incident_number_must_match_full_incident_uuid() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number="INC-SHORTNUM123",
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing aggregate incident number does not match founding signal",
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_primary_service_must_match_founding_signal() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service="billing",
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing aggregate primary service does not match founding signal",
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_existing_tenant_or_environment_mismatch_is_rejected() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    diff_tenant_group = _make_group([_M1, _M2], [_EV12])
    diff_inputs = {
        _SIG1_ID: _make_input(
            _SIG1_ID,
            "auth",
            _TIME1,
            0.9,
            EventSeverity.WARNING,
            _FP1,
            tenant_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        ),
        _SIG2_ID: _make_input(
            _SIG2_ID,
            "billing",
            _TIME2,
            0.85,
            EventSeverity.WARNING,
            _FP2,
            tenant_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        ),
    }
    with pytest.raises(
        AggregateInputError,
        match="Aggregation inputs tenant or environment does not match existing aggregate",
    ):
        create_or_update_incident(
            group=diff_tenant_group,
            signal_inputs=diff_inputs,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_existing_schema_version_mismatch_is_rejected() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    agg_bytes_before = res1.aggregate.model_dump_json()
    group_bytes_before = _GROUP_12.model_dump_json()
    inputs_bytes_before = {k: v.model_dump_json() for k, v in _INPUTS_12.items()}

    payload = res1.aggregate.model_dump(mode="python")
    modified = dict(payload)
    modified["schema_version"] = "0.9.0"

    with pytest.raises(ValidationError, match="unsupported schema_version"):
        IncidentAggregate.model_validate(modified, strict=True)

    assert res1.aggregate.model_dump_json() == agg_bytes_before
    assert _GROUP_12.model_dump_json() == group_bytes_before
    assert {
        k: v.model_dump_json() for k, v in _INPUTS_12.items()
    } == inputs_bytes_before


def test_existing_missing_clique_evidence_is_rejected() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # 3 members requires 3 pairs; provide only 2
    invalid_existing = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=res1.aggregate.revision,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=(_EV12, _EV13),  # missing EV23
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing aggregate correlation evidence is not canonical",
    ):
        create_or_update_incident(
            group=_GROUP_123,
            signal_inputs=_INPUTS_123,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=invalid_existing,
        )


def test_public_group_construction_rejects_incomplete_clique() -> None:
    with pytest.raises(
        GroupingContractError,
        match="Accepted evidence count must match complete clique size",
    ):
        CorrelationGroup(
            policy_version="correlation/v1",
            configuration_fingerprint="c" * 64,
            topology_snapshot_fingerprint="d" * 64,
            members=(_M1, _M2, _M3),
            accepted_evidence=(_EV12, _EV13),  # missing EV23
            started_at=_TIME1,
            last_seen_at=_TIME3,
            time_span_seconds=(_TIME3 - _TIME1).total_seconds(),
            topology_diameter_hops=2,
            group_fingerprint="0" * 64,
        )


def test_existing_correlation_evidence_cannot_be_modified() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Update modifies EV12 score
    ev12_modified = _make_evidence(_SIG1_ID, _SIG2_ID, score=0.99)
    group_mod = _make_group(
        [_M1, _M2, _M3], [ev12_modified, _EV13, _EV23], topology_diameter_hops=2
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing correlation evidence cannot be removed or modified",
    ):
        create_or_update_incident(
            group=group_mod,
            signal_inputs=_INPUTS_123,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_incoming_retained_edge_cannot_change() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    ev12_mod = _make_evidence(_SIG1_ID, _SIG2_ID, score=0.5)
    group_mod = _make_group(
        [_M1, _M2, _M3], [ev12_mod, _EV13, _EV23], topology_diameter_hops=2
    )
    with pytest.raises(
        AggregateStateError,
        match="Existing correlation evidence cannot be removed or modified",
    ):
        create_or_update_incident(
            group=group_mod,
            signal_inputs=_INPUTS_123,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=res1.aggregate,
        )


def test_maximum_revision_blocks_material_update() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    max_rev_aggregate = IncidentAggregate.create(
        schema_version=res1.aggregate.schema_version,
        incident_id=res1.aggregate.incident_id,
        incident_number=res1.aggregate.incident_number,
        tenant_id=res1.aggregate.tenant_id,
        environment=res1.aggregate.environment,
        status=res1.aggregate.status,
        correlation_state=res1.aggregate.correlation_state,
        severity=res1.aggregate.severity,
        severity_policy_version=res1.aggregate.severity_policy_version,
        started_at=res1.aggregate.started_at,
        last_seen_at=res1.aggregate.last_seen_at,
        created_at=res1.aggregate.created_at,
        updated_at=res1.aggregate.updated_at,
        revision=MAX_REVISION,
        primary_service=res1.aggregate.primary_service,
        affected_services=res1.aggregate.affected_services,
        signal_references=res1.aggregate.signal_references,
        correlation_evidence=res1.aggregate.correlation_evidence,
        topology_snapshot=res1.aggregate.topology_snapshot,
        blast_radius=res1.aggregate.blast_radius,
    )
    with pytest.raises(
        AggregateStateError,
        match="Incident aggregate revision exceeds maximum allowed value",
    ):
        create_or_update_incident(
            group=_GROUP_123,
            signal_inputs=_INPUTS_123,
            processing_time=_PROC_TIME2,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
            existing_aggregate=max_rev_aggregate,
        )


def test_missing_signal_input_is_rejected() -> None:
    with pytest.raises(
        AggregateInputError, match="Signal input missing for correlation group member"
    ):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs={_SIG1_ID: _INP1},  # missing SIG2_ID
            processing_time=_PROC_TIME1,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
        )


def test_signal_input_identifier_mismatch_is_rejected() -> None:
    # Key does not match signal.signal_id
    wrong_key = uuid.UUID("99999999-9999-9999-9999-999999999999")
    m_wrong = GroupingMemberReference(
        signal_id=wrong_key,
        semantic_fingerprint=_FP2,
        service="billing",
        event_time=_TIME2,
    )
    g = _make_group([_M1, m_wrong], [_make_evidence(_SIG1_ID, wrong_key)])
    with pytest.raises(
        AggregateInputError, match="Signal input signal_id mismatch with member"
    ):
        create_or_update_incident(
            group=g,
            signal_inputs={
                _SIG1_ID: _INP1,
                wrong_key: _INP2,
            },  # INP2 has SIG2_ID != wrong_key
            processing_time=_PROC_TIME1,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
        )


def test_signal_input_semantic_fingerprint_mismatch_is_rejected() -> None:
    m_wrong_fp = GroupingMemberReference(
        signal_id=_SIG2_ID,
        semantic_fingerprint="f" * 64,
        service="billing",
        event_time=_TIME2,
    )
    g = _make_group([_M1, m_wrong_fp], [_EV12])
    with pytest.raises(
        AggregateInputError,
        match="Signal input semantic fingerprint mismatch with member",
    ):
        create_or_update_incident(
            group=g,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME1,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
        )


def test_signal_input_service_or_event_time_mismatch_is_rejected() -> None:
    # Service mismatch
    m_wrong_svc = GroupingMemberReference(
        signal_id=_SIG2_ID,
        semantic_fingerprint=_FP2,
        service="wrong_svc",
        event_time=_TIME2,
    )
    g_svc = _make_group([_M1, m_wrong_svc], [_EV12])
    with pytest.raises(
        AggregateInputError, match="Signal input service mismatch with member"
    ):
        create_or_update_incident(
            group=g_svc,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME1,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
        )

    # Event time mismatch
    m_wrong_time = GroupingMemberReference(
        signal_id=_SIG2_ID,
        semantic_fingerprint=_FP2,
        service="billing",
        event_time=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    g_time = _make_group([_M1, m_wrong_time], [_EV12])
    with pytest.raises(
        AggregateInputError, match="Signal input event_time mismatch with member"
    ):
        create_or_update_incident(
            group=g_time,
            signal_inputs=_INPUTS_12,
            processing_time=_PROC_TIME1,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
        )


# ---------------------------------------------------------------------------
# 5. Timeline, workflow separation, and typed causes
# ---------------------------------------------------------------------------


def test_timeline_event_ids_are_deterministic() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert [e.event_id for e in res1.timeline_events] == [
        e.event_id for e in res2.timeline_events
    ]


def test_material_update_event_order_is_canonical() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Update with new member (CRITICAL), escalating severity, and finalizing state
    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    types = [e.event_type for e in res2.timeline_events]
    assert types == [
        IncidentTimelineEventType.SIGNAL_ATTACHED,
        IncidentTimelineEventType.SEVERITY_CHANGED,
        IncidentTimelineEventType.CORRELATION_FINALIZED,
    ]


def test_attachment_events_use_exact_source_signal_ids() -> None:
    res = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    attached = [
        e
        for e in res.timeline_events
        if e.event_type == IncidentTimelineEventType.SIGNAL_ATTACHED
    ]
    assert [e.source_signal_id for e in attached] == [_SIG2_ID, _SIG3_ID]


def test_severity_event_uses_exact_source_signal_id() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    sev_event = next(
        e
        for e in res2.timeline_events
        if e.event_type == IncidentTimelineEventType.SEVERITY_CHANGED
    )
    assert sev_event.source_signal_id == _SIG3_ID


def test_state_and_finalization_events_have_no_source_signal_id() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    fin_event = next(
        e
        for e in res2.timeline_events
        if e.event_type == IncidentTimelineEventType.CORRELATION_FINALIZED
    )
    assert fin_event.source_signal_id is None


def test_phase4_never_invents_downstream_workflow_transitions() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res1.aggregate.status == IncidentStatus.OPEN
    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    assert res2.aggregate.status == IncidentStatus.OPEN
    # Zero workflow status transition events
    allowed_types = {
        IncidentTimelineEventType.CREATED,
        IncidentTimelineEventType.SIGNAL_ATTACHED,
        IncidentTimelineEventType.SEVERITY_CHANGED,
        IncidentTimelineEventType.CORRELATION_STATE_CHANGED,
        IncidentTimelineEventType.CORRELATION_FINALIZED,
    }
    for ev in res1.timeline_events + res2.timeline_events:
        assert ev.event_type in allowed_types


def test_creation_identity_failure_preserves_nested_cause_identity() -> None:
    root = RuntimeError("root identity derivation crash")
    identity_err = IncidentIdentityError("failed derive incident id")
    identity_err.__cause__ = root

    with unittest.mock.patch(
        "app.correlation.aggregate.derive_incident_id", side_effect=identity_err
    ):
        with pytest.raises(
            AggregateInputError, match="Failed to derive incident identity"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_12,
                signal_inputs=_INPUTS_12,
                processing_time=_PROC_TIME1,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
            )
        assert exc_info.value.__cause__ is identity_err
        assert exc_info.value.__cause__.__cause__ is root


def test_existing_identity_failure_preserves_nested_cause_identity() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    root = RuntimeError("root existing identity crash")
    identity_err = IncidentIdentityError("failed validate existing incident id")
    identity_err.__cause__ = root

    with unittest.mock.patch(
        "app.correlation.aggregate.derive_incident_id", side_effect=identity_err
    ):
        with pytest.raises(
            AggregateStateError, match="Failed to validate existing incident identity"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_12,
                signal_inputs=_INPUTS_12,
                processing_time=_PROC_TIME2,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
                existing_aggregate=res1.aggregate,
            )
        assert exc_info.value.__cause__ is identity_err
        assert exc_info.value.__cause__.__cause__ is root


def test_creation_aggregate_model_failure_preserves_nested_cause_identity() -> None:
    root = RuntimeError("root model creation crash")
    identity_err = IncidentIdentityError("incident identity invariant failed")
    identity_err.__cause__ = root

    with unittest.mock.patch(
        "app.correlation.models.IncidentAggregate.create", side_effect=identity_err
    ):
        with pytest.raises(
            AggregateInputError, match="Aggregate creation violated model invariants"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_12,
                signal_inputs=_INPUTS_12,
                processing_time=_PROC_TIME1,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
            )
        assert exc_info.value.__cause__ is identity_err
        assert exc_info.value.__cause__.__cause__ is root


def test_update_aggregate_model_failure_preserves_nested_cause_identity() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    root = RuntimeError("root model update crash")
    identity_err = IncidentIdentityError("incident identity invariant failed on update")
    identity_err.__cause__ = root

    with unittest.mock.patch(
        "app.correlation.models.IncidentAggregate.create", side_effect=identity_err
    ):
        with pytest.raises(
            AggregateStateError, match="Aggregate update violated model invariants"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_123,
                signal_inputs=_INPUTS_123,
                processing_time=_PROC_TIME2,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
                existing_aggregate=res1.aggregate,
            )
        assert exc_info.value.__cause__ is identity_err
        assert exc_info.value.__cause__.__cause__ is root


def test_creation_timeline_failure_preserves_nested_cause_identity() -> None:
    root = RuntimeError("root timeline derivation crash")
    identity_err = IncidentIdentityError("timeline id derivation failed")
    identity_err.__cause__ = root

    with unittest.mock.patch(
        "app.correlation.aggregate.derive_timeline_event_id", side_effect=identity_err
    ):
        with pytest.raises(
            AggregateInputError, match="Failed to construct incident timeline event"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_12,
                signal_inputs=_INPUTS_12,
                processing_time=_PROC_TIME1,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
            )
        assert exc_info.value.__cause__ is identity_err
        assert exc_info.value.__cause__.__cause__ is root


def test_update_timeline_failure_preserves_nested_cause_identity() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    root = RuntimeError("root update timeline derivation crash")
    identity_err = IncidentIdentityError("update timeline id derivation failed")
    identity_err.__cause__ = root

    with unittest.mock.patch(
        "app.correlation.aggregate.derive_timeline_event_id", side_effect=identity_err
    ):
        with pytest.raises(
            AggregateStateError, match="Failed to construct incident timeline event"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_123,
                signal_inputs=_INPUTS_123,
                processing_time=_PROC_TIME2,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
                existing_aggregate=res1.aggregate,
            )
        assert exc_info.value.__cause__ is identity_err
        assert exc_info.value.__cause__.__cause__ is root


def test_timeline_model_failure_preserves_original_cause_identity() -> None:
    root_val = ValueError("invalid summary text for event")
    with unittest.mock.patch(
        "app.correlation.models.IncidentTimelineEvent.__init__", side_effect=root_val
    ):
        with pytest.raises(
            AggregateInputError, match="Failed to construct incident timeline event"
        ) as exc_info:
            create_or_update_incident(
                group=_GROUP_12,
                signal_inputs=_INPUTS_12,
                processing_time=_PROC_TIME1,
                desired_correlation_state=CorrelationWindowState.COLLECTING,
            )
        assert exc_info.value.__cause__ is root_val


# ---------------------------------------------------------------------------
# 6. Permutations, golden evidence, and immutability
# ---------------------------------------------------------------------------


def test_all_three_member_permutations_produce_identical_results() -> None:
    auth_id = uuid.UUID("11111111-0000-0000-0000-000000000001")
    billing_id = uuid.UUID("22222222-0000-0000-0000-000000000002")
    orders_id = uuid.UUID("33333333-0000-0000-0000-000000000003")

    node_auth = TopologyServiceNode(
        service_id=auth_id, name="auth", service_type=ServiceType.API
    )
    node_billing = TopologyServiceNode(
        service_id=billing_id, name="billing", service_type=ServiceType.WORKER
    )
    node_orders = TopologyServiceNode(
        service_id=orders_id, name="orders", service_type=ServiceType.DATABASE
    )

    edge_auth_billing = DirectedTopologyEdge(
        upstream_service_id=auth_id,
        downstream_service_id=billing_id,
        dependency_type=DependencyType.SYNC,
    )
    edge_billing_orders = DirectedTopologyEdge(
        upstream_service_id=billing_id,
        downstream_service_id=orders_id,
        dependency_type=DependencyType.SYNC,
    )

    topo_snapshot = TopologySnapshot.create(
        version=TopologyVersion(
            topology_id=uuid.UUID("99999999-9999-9999-9999-999999999999"),
            topology_version="1.0.0",
        ),
        nodes=[node_auth, node_billing, node_orders],
        edges=[edge_auth_billing, edge_billing_orders],
    )

    corr_config = CorrelationConfig(
        schema_version="1.0.0",
        policy_version="correlation/v1",
        window_seconds=300,
        allowed_lateness_seconds=300,
        quiet_period_seconds=10,
        hop_limit=2,
        score_weights=CorrelationScoreWeights(
            temporal=0.25, service=0.25, topology=0.20, symptom=0.15, context=0.15
        ),
        acceptance_threshold=0.60,
        severity_aggregation=SeverityAggregationPolicy.MAXIMUM,
        maximum_group_size=3,
        late_signal_disposition=LateSignalDisposition.ACCEPT,
        duplicate_signal_disposition=DuplicateSignalDisposition.IGNORE,
        conflicting_signal_disposition=ConflictingSignalDisposition.REJECT,
        disconnected_signal_disposition=DisconnectedSignalDisposition.ISOLATE,
        insufficient_context_disposition=InsufficientContextDisposition.IGNORE,
    )

    raw_inputs_list = [_INP1, _INP2, _INP3]
    raw_evidence_list = [_EV12, _EV13, _EV23]

    base_agg_bytes: bytes | None = None
    base_agg_fp: str | None = None
    base_timeline_models: tuple | None = None
    base_timeline_bytes: list[bytes] | None = None
    base_timeline_fps: list[str] | None = None

    member_order_keys: set[tuple[uuid.UUID, ...]] = set()
    evidence_order_keys: set[tuple[tuple[uuid.UUID, uuid.UUID], ...]] = set()
    mapping_order_keys: set[tuple[uuid.UUID, ...]] = set()
    combination_keys: set[
        tuple[
            tuple[uuid.UUID, ...],
            tuple[tuple[uuid.UUID, uuid.UUID], ...],
            tuple[uuid.UUID, ...],
        ]
    ] = set()
    distinct_group_fps: set[str] = set()
    distinct_agg_fps: set[str] = set()

    grouping_invocation_count = 0
    aggregate_invocation_count = 0

    for inp_perm in itertools.permutations(raw_inputs_list):
        inp_key = tuple(x.signal.signal_id for x in inp_perm)
        member_order_keys.add(inp_key)
        for ev_perm in itertools.permutations(raw_evidence_list):
            ev_key = tuple((x.left_signal_id, x.right_signal_id) for x in ev_perm)
            evidence_order_keys.add(ev_key)

            grp_res = group_candidate_signals(
                inputs=list(inp_perm),
                pair_evidence=list(ev_perm),
                topology_snapshot=topo_snapshot,
                config=corr_config,
                event_time_watermark=_TIME3,
            )
            grouping_invocation_count += 1
            assert len(grp_res.groups) == 1
            grp = grp_res.groups[0]
            distinct_group_fps.add(grp.group_fingerprint)

            for map_perm in itertools.permutations(raw_inputs_list):
                map_key = tuple(x.signal.signal_id for x in map_perm)
                mapping_order_keys.add(map_key)
                comb_key = (inp_key, ev_key, map_key)
                combination_keys.add(comb_key)

                s_map = {x.signal.signal_id: x for x in map_perm}
                res = create_or_update_incident(
                    group=grp,
                    signal_inputs=s_map,
                    processing_time=_PROC_TIME1,
                    desired_correlation_state=CorrelationWindowState.COLLECTING,
                )
                aggregate_invocation_count += 1
                assert res.material_update is True
                distinct_agg_fps.add(res.aggregate.revision_fingerprint)

                agg_b = serialize_incident_aggregate(res.aggregate)
                ev_b = [
                    serialize_incident_timeline_event(ev) for ev in res.timeline_events
                ]
                ev_fps = [ev.revision_fingerprint for ev in res.timeline_events]

                if base_agg_bytes is None:
                    base_agg_bytes = agg_b
                    base_agg_fp = res.aggregate.revision_fingerprint
                    base_timeline_models = res.timeline_events
                    base_timeline_bytes = ev_b
                    base_timeline_fps = ev_fps

                assert agg_b == base_agg_bytes
                assert res.aggregate.revision_fingerprint == base_agg_fp
                assert res.timeline_events == base_timeline_models
                assert ev_b == base_timeline_bytes
                assert ev_fps == base_timeline_fps

    raw_member_order_key_count = len(member_order_keys)
    raw_evidence_order_key_count = len(evidence_order_keys)
    mapping_order_key_count = len(mapping_order_keys)
    raw_combination_key_count = len(combination_keys)
    distinct_canonical_grouping_result_count = len(distinct_group_fps)
    distinct_aggregate_result_count = len(distinct_agg_fps)

    assert raw_member_order_key_count == 6
    assert raw_evidence_order_key_count == 6
    assert mapping_order_key_count == 6
    assert raw_combination_key_count == 216
    assert grouping_invocation_count == 36
    assert aggregate_invocation_count == 216
    assert distinct_canonical_grouping_result_count == 1
    assert distinct_aggregate_result_count == 1

    print(f"RAW_MEMBER_ORDER_KEY_COUNT = {raw_member_order_key_count}")
    print(f"RAW_EVIDENCE_ORDER_KEY_COUNT = {raw_evidence_order_key_count}")
    print(f"MAPPING_ORDER_KEY_COUNT = {mapping_order_key_count}")
    print(f"RAW_COMBINATION_KEY_COUNT = {raw_combination_key_count}")
    print(f"GROUPING_INVOCATION_COUNT = {grouping_invocation_count}")
    print(f"AGGREGATE_INVOCATION_COUNT = {aggregate_invocation_count}")
    print(
        f"DISTINCT_CANONICAL_GROUPING_RESULT_COUNT = {distinct_canonical_grouping_result_count}"
    )
    print(f"DISTINCT_AGGREGATE_RESULT_COUNT = {distinct_aggregate_result_count}")


def test_creation_golden_model_bytes_fingerprint_and_events() -> None:
    res = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    # Exact hard-coded golden constants
    expected_incident_id = uuid.UUID("20291309-7d37-575e-bb19-2012ff232525")
    expected_incident_number = "INC-202913097D37575EBB192012FF232525"
    expected_revision_fp = (
        "ff8742d7cbc8ecd79c93ab108a2ce40ab315943efc22171bccdcf24bdac3612b"
    )
    expected_agg_json = (
        '{"affected_services":["auth","billing"],"blast_radius":null,"correlation_evidence":'
        '[{"accepted":true,"context_score":0.9,"left_signal_id":"11111111-1111-1111-1111-111111111111",'
        '"policy_version":"correlation/v1","reason":"accepted test pair","right_signal_id":'
        '"22222222-2222-2222-2222-222222222222","service_score":0.9,"symptom_score":0.9,"temporal_score":0.9,'
        '"topology_score":0.9,"weighted_score":0.9}],"correlation_state":"collecting","created_at":'
        '"2026-01-01T10:10:00Z","environment":"production","incident_id":"20291309-7d37-575e-bb19-2012ff232525",'
        '"incident_number":"INC-202913097D37575EBB192012FF232525","last_seen_at":"2026-01-01T10:01:00Z",'
        '"primary_service":"auth","revision":1,"revision_fingerprint":'
        '"ff8742d7cbc8ecd79c93ab108a2ce40ab315943efc22171bccdcf24bdac3612b","schema_version":"1.2",'
        '"severity":"warning","severity_policy_version":"max-member-severity/v1","signal_references":'
        '[{"anomaly_score":0.85,"environment":"production","event_time":"2026-01-01T10:01:00Z",'
        '"semantic_fingerprint":"2222222222222222222222222222222222222222222222222222222222222222",'
        '"service":"billing","severity":"warning","signal_id":"22222222-2222-2222-2222-222222222222",'
        '"tenant_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},{"anomaly_score":0.9,"environment":'
        '"production","event_time":"2026-01-01T10:00:00Z","semantic_fingerprint":'
        '"1111111111111111111111111111111111111111111111111111111111111111","service":"auth",'
        '"severity":"warning","signal_id":"11111111-1111-1111-1111-111111111111","tenant_id":'
        '"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}],"started_at":"2026-01-01T10:00:00Z","status":"open",'
        '"tenant_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","topology_snapshot":null,"updated_at":'
        '"2026-01-01T10:10:00Z"}'
    )
    expected_ev0_json = (
        '{"event_id":"f91c2e52-48a4-5511-a50e-8edf6a1df771","event_time":"2026-01-01T10:10:00Z",'
        '"event_type":"created","incident_id":"20291309-7d37-575e-bb19-2012ff232525","revision":1,'
        '"revision_fingerprint":"ff8742d7cbc8ecd79c93ab108a2ce40ab315943efc22171bccdcf24bdac3612b",'
        '"schema_version":"1.2","source_signal_id":"11111111-1111-1111-1111-111111111111","summary":'
        '"Incident aggregate initialized from founding correlation anchor signal"}'
    )
    expected_ev1_json = (
        '{"event_id":"532fdc06-0b27-5e17-bc84-02cbe4bdde85","event_time":"2026-01-01T10:10:00Z",'
        '"event_type":"signal_attached","incident_id":"20291309-7d37-575e-bb19-2012ff232525","revision":1,'
        '"revision_fingerprint":"ff8742d7cbc8ecd79c93ab108a2ce40ab315943efc22171bccdcf24bdac3612b",'
        '"schema_version":"1.2","source_signal_id":"22222222-2222-2222-2222-222222222222","summary":'
        '"Correlated signal attached to incident aggregate"}'
    )

    assert res.aggregate.incident_id == expected_incident_id
    assert res.aggregate.incident_number == expected_incident_number
    assert res.aggregate.revision_fingerprint == expected_revision_fp
    assert (
        serialize_incident_aggregate(res.aggregate).decode("utf-8") == expected_agg_json
    )
    assert len(res.timeline_events) == 2
    assert (
        serialize_incident_timeline_event(res.timeline_events[0]).decode("utf-8")
        == expected_ev0_json
    )
    assert (
        serialize_incident_timeline_event(res.timeline_events[1]).decode("utf-8")
        == expected_ev1_json
    )

    # Repeatability in a fresh second invocation
    res_repeat = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert res_repeat.aggregate == res.aggregate
    assert res_repeat.timeline_events == res.timeline_events


def test_material_update_golden_model_bytes_fingerprint_and_events() -> None:
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    res2 = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    # Exact hard-coded golden constants
    expected_rev = 2
    expected_rev_fp = "8ab30b04cb51a31ab6334a7d8eadf04c4fcf0e490f641e3708bea43c049ce908"
    expected_agg_json = (
        '{"affected_services":["auth","billing","orders"],"blast_radius":null,"correlation_evidence":'
        '[{"accepted":true,"context_score":0.9,"left_signal_id":"11111111-1111-1111-1111-111111111111",'
        '"policy_version":"correlation/v1","reason":"accepted test pair","right_signal_id":'
        '"22222222-2222-2222-2222-222222222222","service_score":0.9,"symptom_score":0.9,"temporal_score":0.9,'
        '"topology_score":0.9,"weighted_score":0.9},{"accepted":true,"context_score":0.9,"left_signal_id":'
        '"11111111-1111-1111-1111-111111111111","policy_version":"correlation/v1","reason":"accepted test pair",'
        '"right_signal_id":"33333333-3333-3333-3333-333333333333","service_score":0.9,"symptom_score":0.9,'
        '"temporal_score":0.9,"topology_score":0.9,"weighted_score":0.9},{"accepted":true,"context_score":0.9,'
        '"left_signal_id":"22222222-2222-2222-2222-222222222222","policy_version":"correlation/v1","reason":'
        '"accepted test pair","right_signal_id":"33333333-3333-3333-3333-333333333333","service_score":0.9,'
        '"symptom_score":0.9,"temporal_score":0.9,"topology_score":0.9,"weighted_score":0.9}],"correlation_state":'
        '"finalized","created_at":"2026-01-01T10:10:00Z","environment":"production","incident_id":'
        '"20291309-7d37-575e-bb19-2012ff232525","incident_number":"INC-202913097D37575EBB192012FF232525",'
        '"last_seen_at":"2026-01-01T10:02:00Z","primary_service":"auth","revision":2,"revision_fingerprint":'
        '"8ab30b04cb51a31ab6334a7d8eadf04c4fcf0e490f641e3708bea43c049ce908","schema_version":"1.2","severity":'
        '"critical","severity_policy_version":"max-member-severity/v1","signal_references":[{"anomaly_score":0.85,'
        '"environment":"production","event_time":"2026-01-01T10:01:00Z","semantic_fingerprint":'
        '"2222222222222222222222222222222222222222222222222222222222222222","service":"billing","severity":'
        '"warning","signal_id":"22222222-2222-2222-2222-222222222222","tenant_id":'
        '"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},{"anomaly_score":0.9,"environment":"production","event_time":'
        '"2026-01-01T10:00:00Z","semantic_fingerprint":'
        '"1111111111111111111111111111111111111111111111111111111111111111","service":"auth","severity":'
        '"warning","signal_id":"11111111-1111-1111-1111-111111111111","tenant_id":'
        '"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},{"anomaly_score":0.95,"environment":"production","event_time":'
        '"2026-01-01T10:02:00Z","semantic_fingerprint":'
        '"3333333333333333333333333333333333333333333333333333333333333333","service":"orders","severity":'
        '"critical","signal_id":"33333333-3333-3333-3333-333333333333","tenant_id":'
        '"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}],"started_at":"2026-01-01T10:00:00Z","status":"open","tenant_id":'
        '"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","topology_snapshot":null,"updated_at":"2026-01-01T10:20:00Z"}'
    )
    expected_ev0_json = (
        '{"event_id":"8929aecc-5ce8-57e8-8230-4cb40cbff288","event_time":"2026-01-01T10:20:00Z",'
        '"event_type":"signal_attached","incident_id":"20291309-7d37-575e-bb19-2012ff232525","revision":2,'
        '"revision_fingerprint":"8ab30b04cb51a31ab6334a7d8eadf04c4fcf0e490f641e3708bea43c049ce908",'
        '"schema_version":"1.2","source_signal_id":"33333333-3333-3333-3333-333333333333","summary":'
        '"Correlated signal attached to incident aggregate"}'
    )
    expected_ev1_json = (
        '{"event_id":"1ba9e0cb-e715-54a0-aea6-115847b22c80","event_time":"2026-01-01T10:20:00Z",'
        '"event_type":"severity_changed","incident_id":"20291309-7d37-575e-bb19-2012ff232525","revision":2,'
        '"revision_fingerprint":"8ab30b04cb51a31ab6334a7d8eadf04c4fcf0e490f641e3708bea43c049ce908",'
        '"schema_version":"1.2","source_signal_id":"33333333-3333-3333-3333-333333333333","summary":'
        '"Incident aggregate severity escalated"}'
    )
    expected_ev2_json = (
        '{"event_id":"d3394c56-093f-5cd1-940c-d89725217b4e","event_time":"2026-01-01T10:20:00Z",'
        '"event_type":"correlation_finalized","incident_id":"20291309-7d37-575e-bb19-2012ff232525","revision":2,'
        '"revision_fingerprint":"8ab30b04cb51a31ab6334a7d8eadf04c4fcf0e490f641e3708bea43c049ce908",'
        '"schema_version":"1.2","source_signal_id":null,"summary":"Correlation aggregation window finalized"}'
    )

    assert res2.aggregate.revision == expected_rev
    assert res2.aggregate.revision_fingerprint == expected_rev_fp
    assert (
        serialize_incident_aggregate(res2.aggregate).decode("utf-8")
        == expected_agg_json
    )
    assert len(res2.timeline_events) == 3
    assert (
        serialize_incident_timeline_event(res2.timeline_events[0]).decode("utf-8")
        == expected_ev0_json
    )
    assert (
        serialize_incident_timeline_event(res2.timeline_events[1]).decode("utf-8")
        == expected_ev1_json
    )
    assert (
        serialize_incident_timeline_event(res2.timeline_events[2]).decode("utf-8")
        == expected_ev2_json
    )


def test_creation_replay_update_inputs_are_immutable() -> None:
    # Snapshot inputs before execution
    group_json_before = _GROUP_12.model_dump_json()
    inp1_json_before = _INP1.model_dump_json()
    inp2_json_before = _INP2.model_dump_json()
    signal_keys_before = list(_INPUTS_12.keys())

    # 1. Creation
    res1 = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    assert _GROUP_12.model_dump_json() == group_json_before
    assert _INP1.model_dump_json() == inp1_json_before
    assert _INP2.model_dump_json() == inp2_json_before
    assert list(_INPUTS_12.keys()) == signal_keys_before

    # Snapshot existing aggregate before replay
    agg1_json_before = res1.aggregate.model_dump_json()

    # 2. Replay
    res_replay = create_or_update_incident(
        group=_GROUP_12,
        signal_inputs=_INPUTS_12,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=res1.aggregate,
    )
    assert _GROUP_12.model_dump_json() == group_json_before
    assert res1.aggregate.model_dump_json() == agg1_json_before
    assert res_replay.aggregate is res1.aggregate

    # 3. Update
    group123_json_before = _GROUP_123.model_dump_json()
    inp3_json_before = _INP3.model_dump_json()
    signal_keys_123_before = list(_INPUTS_123.keys())

    res_update = create_or_update_incident(
        group=_GROUP_123,
        signal_inputs=_INPUTS_123,
        processing_time=_PROC_TIME2,
        desired_correlation_state=CorrelationWindowState.FINALIZED,
        existing_aggregate=res1.aggregate,
    )
    assert _GROUP_123.model_dump_json() == group123_json_before
    assert _INP3.model_dump_json() == inp3_json_before
    assert list(_INPUTS_123.keys()) == signal_keys_123_before
    assert res1.aggregate.model_dump_json() == agg1_json_before
    assert res_update.aggregate.revision == 2


def test_rejection_paths_preserve_all_caller_owned_inputs() -> None:
    group_json_before = _GROUP_12.model_dump_json()
    inp1_json_before = _INP1.model_dump_json()
    inp2_json_before = _INP2.model_dump_json()

    # Trigger rejection by missing signal input
    with pytest.raises(AggregateInputError):
        create_or_update_incident(
            group=_GROUP_12,
            signal_inputs={_SIG1_ID: _INP1},
            processing_time=_PROC_TIME1,
            desired_correlation_state=CorrelationWindowState.COLLECTING,
        )
    assert _GROUP_12.model_dump_json() == group_json_before
    assert _INP1.model_dump_json() == inp1_json_before
    assert _INP2.model_dump_json() == inp2_json_before
