from __future__ import annotations

import itertools
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.anomaly.models import AnomalySignal, AnomalyEvidence, CalibrationMetadata
from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.correlation.config import (
    CorrelationConfig,
    CorrelationScoreWeights,
    SeverityAggregationPolicy,
    LateSignalDisposition,
    DuplicateSignalDisposition,
    ConflictingSignalDisposition,
    DisconnectedSignalDisposition,
    InsufficientContextDisposition,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.models import (
    CorrelationEvidence,
    MAX_EVIDENCE_ENTRIES,
    MAX_SIGNAL_REFS,
)
from app.correlation.scoring import score_candidate_pair
from app.correlation.topology import (
    TopologySnapshot,
    TopologyVersion,
    TopologyServiceNode,
    DirectedTopologyEdge,
)
from app.correlation.errors import (
    GroupingContractError,
    GroupingInputError,
)
from app.correlation.grouping import (
    CorrelationGroup,
    GroupingSignalDisposition,
    GroupingEdgeRejectionReason,
    GroupingMemberReference,
    GroupingSignalRecord,
    RejectedGroupingEdge,
    CorrelationGroupingResult,
    compute_group_fingerprint,
    group_candidate_signals,
    serialize_grouping_result,
    deserialize_grouping_result,
)
from app.simulator.interfaces import ServiceType, DependencyType
from app.telemetry.schemas import EventSeverity

_UTC_NOW = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
_TENANT_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_RUN_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
_TOPOLOGY_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
_UNKNOWN_ID = uuid.UUID("99999999-9999-4999-8999-999999999999")
_SIG_A_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_SIG_B_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
_SIG_C_ID = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
_SIG_D_ID = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
_EV_ID = uuid.UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
_SRC_ID = uuid.UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")


def _sample_calibration() -> CalibrationMetadata:
    return CalibrationMetadata(
        schema_version="1.0",
        method="quantile",
        threshold_value=0.9,
        calibration_version="1.0.0",
        parameters={},
        calibrated_at=_UTC_NOW,
    )


def _sample_signal(
    signal_id: uuid.UUID = _SIG_A_ID,
    service: str = "order-service",
    event_time: datetime = _UTC_NOW,
    severity: EventSeverity = EventSeverity.ERROR,
    anomaly_score: float = 0.95,
    metric_or_feature: str = "latency_ms",
    tenant_id: uuid.UUID = _TENANT_ID,
    environment: str = "prod",
    run_id: uuid.UUID = _RUN_ID,
    scenario_id: str = "scen-1",
    scenario_version: str = "1.0.0",
    seed: int = 42,
    reproducibility_key: str = "rep-123",
) -> AnomalySignal:
    return AnomalySignal(
        signal_id=signal_id,
        event_time=event_time,
        service=service,
        metric_or_feature=metric_or_feature,
        model_name="iforest",
        model_version="1.0.0",
        anomaly_score=anomaly_score,
        severity=severity,
        evidence=[
            AnomalyEvidence(
                evidence_id=_EV_ID,
                evidence_type="threshold",
                metric_or_feature=metric_or_feature,
                observed_value=500.0,
                expected_value=100.0,
                deviation=400.0,
            )
        ],
        threshold_or_calibration=_sample_calibration(),
        run_id=run_id,
        scenario_id=scenario_id,
        scenario_version=scenario_version,
        seed=seed,
        reproducibility_key=reproducibility_key,
        environment=environment,
        tenant_id=tenant_id,
        source_event_ids=[_SRC_ID],
    )


def _make_input(sig: AnomalySignal) -> AnomalyCorrelationInput:
    lineage = build_anomaly_reproducibility_lineage(sig)
    return AnomalyCorrelationInput(signal=sig, lineage=lineage)


def _sample_config(
    window_seconds: int = 300,
    hop_limit: int = 3,
    acceptance_threshold: float = 0.6,
    maximum_group_size: int = 100,
    late_signal_disposition: LateSignalDisposition = LateSignalDisposition.ACCEPT,
    duplicate_signal_disposition: DuplicateSignalDisposition = DuplicateSignalDisposition.IGNORE,
) -> CorrelationConfig:
    w = {
        "temporal": 0.25,
        "service": 0.25,
        "topology": 0.2,
        "symptom": 0.15,
        "context": 0.15,
    }
    return CorrelationConfig(
        schema_version="1.0.0",
        policy_version="v1.0",
        window_seconds=window_seconds,
        allowed_lateness_seconds=30,
        quiet_period_seconds=10,
        hop_limit=hop_limit,
        score_weights=CorrelationScoreWeights.model_validate(w),
        acceptance_threshold=acceptance_threshold,
        severity_aggregation=SeverityAggregationPolicy.MAXIMUM,
        maximum_group_size=maximum_group_size,
        late_signal_disposition=late_signal_disposition,
        duplicate_signal_disposition=duplicate_signal_disposition,
        conflicting_signal_disposition=ConflictingSignalDisposition.REJECT,
        disconnected_signal_disposition=DisconnectedSignalDisposition.ISOLATE,
        insufficient_context_disposition=InsufficientContextDisposition.IGNORE,
    )


def _sample_topology() -> TopologySnapshot:
    n1_id = uuid.uuid5(uuid.NAMESPACE_DNS, "order-service")
    n2_id = uuid.uuid5(uuid.NAMESPACE_DNS, "payment-service")
    n3_id = uuid.uuid5(uuid.NAMESPACE_DNS, "db-service")
    nodes = [
        TopologyServiceNode(
            service_id=n1_id, name="order-service", service_type=ServiceType.API
        ),
        TopologyServiceNode(
            service_id=n2_id, name="payment-service", service_type=ServiceType.API
        ),
        TopologyServiceNode(
            service_id=n3_id, name="db-service", service_type=ServiceType.DATABASE
        ),
    ]
    edges = [
        DirectedTopologyEdge(
            upstream_service_id=n1_id,
            downstream_service_id=n2_id,
            dependency_type=DependencyType.SYNC,
        ),
        DirectedTopologyEdge(
            upstream_service_id=n2_id,
            downstream_service_id=n3_id,
            dependency_type=DependencyType.SYNC,
        ),
    ]
    version = TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0")
    return TopologySnapshot.create(version=version, nodes=nodes, edges=edges)


# 14.1 Contract and serialization tests
def test_grouping_models_strict_frozen_forbid() -> None:
    sig_id = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    fp = "a" * 64
    dt = _UTC_NOW

    member = GroupingMemberReference(
        signal_id=sig_id,
        semantic_fingerprint=fp,
        service="order-service",
        event_time=dt,
    )
    with pytest.raises(ValidationError):
        GroupingMemberReference.model_validate(
            {
                "signal_id": sig_id,
                "semantic_fingerprint": fp,
                "service": "order-service",
                "event_time": dt,
                "extra_field": 123,
            }
        )

    with pytest.raises(ValidationError):
        member.service = "other"  # type: ignore[misc]


def test_grouping_serialization_roundtrip_and_bytes() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    inp_a = _make_input(sig_a)

    result = group_candidate_signals(
        [inp_a],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )

    serialized = serialize_grouping_result(result)
    assert isinstance(serialized, bytes)
    deserialized = deserialize_grouping_result(serialized)
    assert result == deserialized
    assert result.result_fingerprint == deserialized.result_fingerprint


def test_grouping_invalid_payload_deserialization() -> None:
    with pytest.raises(GroupingContractError):
        deserialize_grouping_result(b"not-json")
    with pytest.raises(GroupingContractError):
        deserialize_grouping_result(b"[1, 2, 3]")


# 14.2 Empty, singleton, duplicate, conflict, and late cases
def test_empty_inputs_produces_zero_groups() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    result = group_candidate_signals(
        [], [], topology_snapshot=snapshot, config=config, event_time_watermark=_UTC_NOW
    )
    assert len(result.groups) == 0
    assert len(result.signal_records) == 0
    assert len(result.rejected_edges) == 0
    assert len(result.result_fingerprint) == 64


def test_singleton_input_produces_one_group() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    inp_a = _make_input(sig_a)

    result = group_candidate_signals(
        [inp_a],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )
    assert len(result.groups) == 1
    assert len(result.groups[0].members) == 1
    assert result.groups[0].members[0].signal_id == _SIG_A_ID
    assert len(result.signal_records) == 1
    assert result.signal_records[0].included is True
    assert result.signal_records[0].disposition == GroupingSignalDisposition.GROUPED


def test_exact_duplicate_ignored() -> None:
    config = _sample_config(
        duplicate_signal_disposition=DuplicateSignalDisposition.IGNORE
    )
    snapshot = _sample_topology()
    sig_a1 = _sample_signal(signal_id=_SIG_A_ID)
    sig_a2 = _sample_signal(signal_id=_SIG_A_ID)
    inp_a1 = _make_input(sig_a1)
    inp_a2 = _make_input(sig_a2)

    result = group_candidate_signals(
        [inp_a1, inp_a2],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )
    assert len(result.groups) == 1
    assert len(result.groups[0].members) == 1
    assert len(result.signal_records) == 1
    assert result.signal_records[0].duplicate_count == 2
    assert (
        result.signal_records[0].disposition
        == GroupingSignalDisposition.DUPLICATE_IGNORED
    )


def test_exact_duplicate_rejected() -> None:
    config = _sample_config(
        duplicate_signal_disposition=DuplicateSignalDisposition.REJECT
    )
    snapshot = _sample_topology()
    sig_a1 = _sample_signal(signal_id=_SIG_A_ID)
    sig_a2 = _sample_signal(signal_id=_SIG_A_ID)
    inp_a1 = _make_input(sig_a1)
    inp_a2 = _make_input(sig_a2)

    result = group_candidate_signals(
        [inp_a1, inp_a2],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )
    assert len(result.groups) == 0
    assert len(result.signal_records) == 1
    assert result.signal_records[0].included is False
    assert (
        result.signal_records[0].disposition
        == GroupingSignalDisposition.DUPLICATE_REJECTED
    )


def test_conflicting_inputs_raises_grouping_input_error() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a1 = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_a2 = _sample_signal(signal_id=_SIG_A_ID, service="payment-service")
    inp_a1 = _make_input(sig_a1)
    inp_a2 = _make_input(sig_a2)

    with pytest.raises(GroupingInputError):
        group_candidate_signals(
            [inp_a1, inp_a2],
            [],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )


def test_lateness_boundary_rules() -> None:
    # allowed_lateness_seconds = 30. Watermark = _UTC_NOW (12:00:30).
    # Cutoff = 12:00:00.
    watermark = _UTC_NOW + timedelta(seconds=30)
    config = _sample_config(late_signal_disposition=LateSignalDisposition.REJECT)
    snapshot = _sample_topology()

    # Exactly at cutoff (12:00:00) -> not late
    sig_on_time = _sample_signal(signal_id=_SIG_A_ID, event_time=_UTC_NOW)
    # One microsecond before cutoff (12:00:00 - 1us) -> late
    sig_late = _sample_signal(
        signal_id=_SIG_B_ID, event_time=_UTC_NOW - timedelta(microseconds=1)
    )

    inp_on_time = _make_input(sig_on_time)
    inp_late = _make_input(sig_late)

    result = group_candidate_signals(
        [inp_on_time, inp_late],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=watermark,
    )
    assert len(result.groups) == 1
    assert len(result.groups[0].members) == 1
    assert result.groups[0].members[0].signal_id == _SIG_A_ID

    recs = {r.signal_id: r for r in result.signal_records}
    assert recs[_SIG_A_ID].included is True
    assert recs[_SIG_B_ID].included is False
    assert recs[_SIG_B_ID].disposition == GroupingSignalDisposition.LATE_REJECTED


# 14.3 Graph and bridge protection
def test_complete_clique_grouping() -> None:
    config = _sample_config(acceptance_threshold=0.5)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=10),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    ev = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)

    result = group_candidate_signals(
        [inp_a, inp_b],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(result.groups) == 1
    assert len(result.groups[0].members) == 2
    assert len(result.groups[0].accepted_evidence) == 1


def test_bridge_chain_collapse_protection() -> None:
    # A - B - C chain where A-B and B-C are accepted, but A-C is rejected/missing.
    # Complete-link rule must produce a 2-member group (A, B) and a singleton (C) [or starting from seed order].
    config = _sample_config(acceptance_threshold=0.5)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=5),
    )
    sig_c = _sample_signal(
        signal_id=_SIG_C_ID,
        service="db-service",
        event_time=_UTC_NOW + timedelta(seconds=10),
    )

    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)
    inp_c = _make_input(sig_c)

    ev_ab = score_candidate_pair(
        inp_a, inp_b, topology_snapshot=snapshot, config=config
    )
    ev_bc = score_candidate_pair(
        inp_b, inp_c, topology_snapshot=snapshot, config=config
    )

    # Ensure A-B and B-C are accepted
    assert ev_ab.accepted is True
    assert ev_bc.accepted is True

    # No A-C evidence supplied (missing pair evidence)

    result = group_candidate_signals(
        [inp_a, inp_b, inp_c],
        [ev_ab, ev_bc],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )

    # With complete-link, group 1 starts with A, checks B (accepted), checks C (no A-C edge -> skipped). Group (A, B).
    # Next unassigned is C -> singleton (C).
    assert len(result.groups) == 2
    group_member_sets = [set(m.signal_id for m in g.members) for g in result.groups]
    assert {_SIG_A_ID, _SIG_B_ID} in group_member_sets
    assert {_SIG_C_ID} in group_member_sets

    # Verify that rejected edge A-C (or bridge_chain_blocked / missing_pair_evidence) is recorded
    rejections = {
        (e.left_signal_id, e.right_signal_id, e.reason) for e in result.rejected_edges
    }
    # Check that A-C (or canonical sorted order) has a rejection reason
    ac_pair = tuple(sorted([_SIG_A_ID, _SIG_C_ID], key=lambda u: u.bytes))
    assert any(r[0] == ac_pair[0] and r[1] == ac_pair[1] for r in rejections)


# 14.4 Guard boundaries
def test_maximum_group_size_guard() -> None:
    config = _sample_config(acceptance_threshold=0.5, maximum_group_size=1)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=5),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    ev = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)

    result = group_candidate_signals(
        [inp_a, inp_b],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    # Maximum group size 1 forces split into two singletons
    assert len(result.groups) == 2
    assert len(result.groups[0].members) == 1
    assert len(result.groups[1].members) == 1

    rejections = {e.reason for e in result.rejected_edges}
    assert GroupingEdgeRejectionReason.MAXIMUM_GROUP_SIZE_EXCEEDED in rejections


def test_time_span_exceeded_guard() -> None:
    score_config = _sample_config(acceptance_threshold=0.1, window_seconds=20)
    group_config = _sample_config(acceptance_threshold=0.1, window_seconds=10)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=15),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    ev = score_candidate_pair(
        inp_a, inp_b, topology_snapshot=snapshot, config=score_config
    )

    result = group_candidate_signals(
        [inp_a, inp_b],
        [ev],
        topology_snapshot=snapshot,
        config=group_config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(result.groups) == 2
    rejections = {e.reason for e in result.rejected_edges}
    assert GroupingEdgeRejectionReason.TIME_SPAN_EXCEEDED in rejections


# 14.5 Permutation and property coverage
def test_exhaustive_permutation_invariance_four_signals() -> None:
    config = _sample_config(acceptance_threshold=0.5)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=2),
    )
    sig_c = _sample_signal(
        signal_id=_SIG_C_ID,
        service="db-service",
        event_time=_UTC_NOW + timedelta(seconds=4),
    )
    sig_d = _sample_signal(
        signal_id=_SIG_D_ID,
        service="order-service",
        event_time=_UTC_NOW + timedelta(seconds=6),
    )

    inputs = [
        _make_input(sig_a),
        _make_input(sig_b),
        _make_input(sig_c),
        _make_input(sig_d),
    ]
    pairs = list(itertools.combinations(inputs, 2))
    evidence = [
        score_candidate_pair(p[0], p[1], topology_snapshot=snapshot, config=config)
        for p in pairs
    ]

    baseline_result = group_candidate_signals(
        inputs,
        evidence,
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=10),
    )

    input_perm_count = 0
    for perm_inputs in itertools.permutations(inputs):
        input_perm_count += 1
        res = group_candidate_signals(
            list(perm_inputs),
            evidence,
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW + timedelta(minutes=10),
        )
        assert res == baseline_result
        assert res.result_fingerprint == baseline_result.result_fingerprint
        assert serialize_grouping_result(res) == serialize_grouping_result(
            baseline_result
        )
    assert input_perm_count == 24

    evidence_perm_count = 0
    for perm_ev in itertools.permutations(evidence):
        evidence_perm_count += 1
        res = group_candidate_signals(
            inputs,
            list(perm_ev),
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW + timedelta(minutes=10),
        )
        assert res == baseline_result
        assert res.result_fingerprint == baseline_result.result_fingerprint
        assert serialize_grouping_result(res) == serialize_grouping_result(
            baseline_result
        )
    assert evidence_perm_count == 720


def test_result_contract_invariants_enforced() -> None:
    config = _sample_config(acceptance_threshold=0.5)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    inp_a = _make_input(sig_a)
    res = group_candidate_signals(
        [inp_a],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )

    # Tamper with result by making a record excluded while keeping a group member
    with pytest.raises((ValidationError, GroupingContractError)):
        # Construct invalid result model directly
        from app.correlation.grouping import (
            CorrelationGroupingResult,
            GroupingSignalRecord,
        )

        CorrelationGroupingResult(
            policy_version=res.policy_version,
            configuration_fingerprint=res.configuration_fingerprint,
            topology_snapshot_fingerprint=res.topology_snapshot_fingerprint,
            event_time_watermark=res.event_time_watermark,
            groups=res.groups,
            rejected_edges=res.rejected_edges,
            signal_records=(
                GroupingSignalRecord(
                    signal_id=res.signal_records[0].signal_id,
                    semantic_fingerprint=res.signal_records[0].semantic_fingerprint,
                    included=False,
                    duplicate_count=res.signal_records[0].duplicate_count,
                    late=res.signal_records[0].late,
                    disposition=res.signal_records[0].disposition,
                    group_fingerprint=None,
                ),
            ),
            result_fingerprint=res.result_fingerprint,
        )


def test_topology_prevalidation_missing_precedes_ambiguous() -> None:
    config = _sample_config()
    # Create snapshot with ambiguous service 'order-service' (2 nodes) via TopologySnapshot.create
    nodes = (
        TopologyServiceNode(
            service_id=uuid.uuid5(uuid.NAMESPACE_DNS, "order-service-1"),
            name="order-service",
            service_type=ServiceType.API,
        ),
        TopologyServiceNode(
            service_id=uuid.uuid5(uuid.NAMESPACE_DNS, "order-service-2"),
            name="order-service",
            service_type=ServiceType.API,
        ),
    )
    version = TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0")
    snapshot = TopologySnapshot.create(version=version, nodes=nodes, edges=())
    sig_missing = _sample_signal(signal_id=_SIG_A_ID, service="non-existent")
    sig_ambig = _sample_signal(signal_id=_SIG_B_ID, service="order-service")
    inp_missing = _make_input(sig_missing)
    inp_ambig = _make_input(sig_ambig)

    # Missing service should raise GroupingInputError regardless of input order
    for perm in ([inp_missing, inp_ambig], [inp_ambig, inp_missing]):
        with pytest.raises(GroupingInputError) as exc_info:
            group_candidate_signals(
                perm,
                [],
                topology_snapshot=snapshot,
                config=config,
                event_time_watermark=_UTC_NOW,
            )
        assert "snapshot" in str(exc_info.value).lower()


# 14.7 Immutability and purity
def test_purity_and_immutability() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    inp_a = _make_input(sig_a)

    inputs = [inp_a]
    inputs_bytes_before = serialize_grouping_result(
        group_candidate_signals(
            inputs,
            [],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )
    )

    # Run again to ensure no mutation
    group_candidate_signals(
        inputs,
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )

    inputs_bytes_after = serialize_grouping_result(
        group_candidate_signals(
            inputs,
            [],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )
    )
    assert inputs_bytes_before == inputs_bytes_after


def test_unknown_endpoint_in_evidence_raises_input_error() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    inp_a = _make_input(sig_a)
    unknown_id = _UNKNOWN_ID
    ev = CorrelationEvidence(
        left_signal_id=_SIG_A_ID,
        right_signal_id=unknown_id,
        temporal_score=0.9,
        service_score=1.0,
        topology_score=1.0,
        symptom_score=0.9,
        context_score=1.0,
        weighted_score=0.95,
        accepted=True,
        policy_version=config.policy_version,
        reason="accepted;topology=same_service;hops=0",
    )
    with pytest.raises(GroupingInputError):
        group_candidate_signals(
            [inp_a],
            [ev],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )


def test_duplicate_unordered_pair_evidence_raises_input_error() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    sig_b = _sample_signal(signal_id=_SIG_B_ID)
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)
    ev1 = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)
    ev2 = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)
    with pytest.raises(GroupingInputError):
        group_candidate_signals(
            [inp_a, inp_b],
            [ev1, ev2],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )


def test_generic_conflict_error_non_disclosure() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a1 = _sample_signal(signal_id=_SIG_A_ID, metric_or_feature="latency_ms")
    sig_a2 = _sample_signal(signal_id=_SIG_A_ID, metric_or_feature="cpu_pct")
    inp_a1 = _make_input(sig_a1)
    inp_a2 = _make_input(sig_a2)
    with pytest.raises(GroupingInputError) as exc_info:
        group_candidate_signals(
            [inp_a1, inp_a2],
            [],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )
    err_msg = str(exc_info.value)
    assert str(_SIG_A_ID) not in err_msg


def test_singleton_missing_or_ambiguous_service_raises_input_error() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_missing = _sample_signal(signal_id=_SIG_A_ID, service="non-existent-service")
    inp_missing = _make_input(sig_missing)
    with pytest.raises(GroupingInputError):
        group_candidate_signals(
            [inp_missing],
            [],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )


def test_explicit_rejected_source_pair_yields_exactly_one_rejection() -> None:
    config = _sample_config(acceptance_threshold=0.8)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(signal_id=_SIG_B_ID, service="payment-service")
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    ev = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)
    assert ev.accepted is False

    res1 = group_candidate_signals(
        [inp_a, inp_b],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(res1.groups) == 2
    assert len(res1.rejected_edges) == 1
    assert (
        res1.rejected_edges[0].reason
        == GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED
    )

    res2 = group_candidate_signals(
        [inp_b, inp_a],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert res1 == res2
    assert res1.result_fingerprint == res2.result_fingerprint
    assert serialize_grouping_result(res1) == serialize_grouping_result(res2)


def test_ineligible_signal_precedence_over_source_rejection() -> None:
    config = _sample_config(
        late_signal_disposition=LateSignalDisposition.REJECT,
        acceptance_threshold=0.8,
    )
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, event_time=_UTC_NOW)
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID, event_time=_UTC_NOW - timedelta(seconds=100)
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)
    ev = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)

    res = group_candidate_signals(
        [inp_a, inp_b],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )
    assert len(res.rejected_edges) == 1
    assert res.rejected_edges[0].reason == GroupingEdgeRejectionReason.INELIGIBLE_SIGNAL


def test_result_contract_invariants_independent_matrix() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    inp_a = _make_input(sig_a)
    baseline = group_candidate_signals(
        [inp_a],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW,
    )

    # 1. Duplicate record IDs
    with pytest.raises((ValidationError, GroupingContractError)):
        CorrelationGroupingResult(
            policy_version=baseline.policy_version,
            configuration_fingerprint=baseline.configuration_fingerprint,
            topology_snapshot_fingerprint=baseline.topology_snapshot_fingerprint,
            event_time_watermark=baseline.event_time_watermark,
            groups=baseline.groups,
            rejected_edges=baseline.rejected_edges,
            signal_records=(baseline.signal_records[0], baseline.signal_records[0]),
            result_fingerprint=baseline.result_fingerprint,
        )

    # 2. Included signal record IDs do not equal group-member set
    with pytest.raises((ValidationError, GroupingContractError)):
        extra_rec = GroupingSignalRecord(
            signal_id=_SIG_B_ID,
            semantic_fingerprint="b" * 64,
            included=True,
            duplicate_count=1,
            late=False,
            disposition=GroupingSignalDisposition.GROUPED,
            group_fingerprint=baseline.groups[0].group_fingerprint,
        )
        CorrelationGroupingResult(
            policy_version=baseline.policy_version,
            configuration_fingerprint=baseline.configuration_fingerprint,
            topology_snapshot_fingerprint=baseline.topology_snapshot_fingerprint,
            event_time_watermark=baseline.event_time_watermark,
            groups=baseline.groups,
            rejected_edges=baseline.rejected_edges,
            signal_records=baseline.signal_records + (extra_rec,),
            result_fingerprint=baseline.result_fingerprint,
        )

    # 3. Mismatched group policy_version
    with pytest.raises((ValidationError, GroupingContractError)):
        CorrelationGroupingResult(
            policy_version="different_policy",
            configuration_fingerprint=baseline.configuration_fingerprint,
            topology_snapshot_fingerprint=baseline.topology_snapshot_fingerprint,
            event_time_watermark=baseline.event_time_watermark,
            groups=baseline.groups,
            rejected_edges=baseline.rejected_edges,
            signal_records=baseline.signal_records,
            result_fingerprint=baseline.result_fingerprint,
        )

    # 4. Result fingerprint mismatch
    with pytest.raises((ValidationError, GroupingContractError)):
        CorrelationGroupingResult(
            policy_version=baseline.policy_version,
            configuration_fingerprint=baseline.configuration_fingerprint,
            topology_snapshot_fingerprint=baseline.topology_snapshot_fingerprint,
            event_time_watermark=baseline.event_time_watermark,
            groups=baseline.groups,
            rejected_edges=baseline.rejected_edges,
            signal_records=baseline.signal_records,
            result_fingerprint="f" * 64,
        )


def test_topology_hop_limit_boundary_accepted_and_exceeded() -> None:
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_c = _sample_signal(
        signal_id=_SIG_C_ID,
        service="db-service",
        event_time=_UTC_NOW + timedelta(seconds=1),
    )
    inp_a = _make_input(sig_a)
    inp_c = _make_input(sig_c)

    config_accept = _sample_config(hop_limit=2, acceptance_threshold=0.1)
    ev = score_candidate_pair(
        inp_a, inp_c, topology_snapshot=snapshot, config=config_accept
    )
    res_accept = group_candidate_signals(
        [inp_a, inp_c],
        [ev],
        topology_snapshot=snapshot,
        config=config_accept,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(res_accept.groups) == 1
    assert len(res_accept.groups[0].members) == 2

    config_reject = _sample_config(hop_limit=1, acceptance_threshold=0.1)
    res_reject = group_candidate_signals(
        [inp_a, inp_c],
        [ev],
        topology_snapshot=snapshot,
        config=config_reject,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(res_reject.groups) == 2
    assert len(res_reject.rejected_edges) == 1
    assert (
        res_reject.rejected_edges[0].reason
        == GroupingEdgeRejectionReason.TOPOLOGY_DIAMETER_EXCEEDED
    )


def test_guard_attribution_mismatches() -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()

    sig_a = _sample_signal(signal_id=_SIG_A_ID, tenant_id=_TENANT_ID)
    sig_b_diff_tenant = _sample_signal(
        signal_id=_SIG_B_ID,
        tenant_id=uuid.UUID("99999999-9999-4999-8999-999999999999"),
    )
    inp_a = _make_input(sig_a)
    inp_b_diff = _make_input(sig_b_diff_tenant)
    ev_tenant = CorrelationEvidence(
        left_signal_id=_SIG_A_ID,
        right_signal_id=_SIG_B_ID,
        temporal_score=0.9,
        service_score=1.0,
        topology_score=1.0,
        symptom_score=0.9,
        context_score=1.0,
        weighted_score=0.95,
        accepted=True,
        policy_version=config.policy_version,
        reason="accepted;topology=same_service;hops=0",
    )
    res_tenant = group_candidate_signals(
        [inp_a, inp_b_diff],
        [ev_tenant],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(res_tenant.rejected_edges) == 1
    assert (
        res_tenant.rejected_edges[0].reason
        == GroupingEdgeRejectionReason.TENANT_MISMATCH
    )

    sig_b_diff_env = _sample_signal(signal_id=_SIG_B_ID, environment="staging")
    inp_b_env = _make_input(sig_b_diff_env)
    ev_env = CorrelationEvidence(
        left_signal_id=_SIG_A_ID,
        right_signal_id=_SIG_B_ID,
        temporal_score=0.9,
        service_score=1.0,
        topology_score=1.0,
        symptom_score=0.9,
        context_score=1.0,
        weighted_score=0.95,
        accepted=True,
        policy_version=config.policy_version,
        reason="accepted;topology=same_service;hops=0",
    )
    res_env = group_candidate_signals(
        [inp_a, inp_b_env],
        [ev_env],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )
    assert len(res_env.rejected_edges) == 1
    assert (
        res_env.rejected_edges[0].reason
        == GroupingEdgeRejectionReason.ENVIRONMENT_MISMATCH
    )


def test_complete_input_immutability_snapshot() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID)
    sig_b = _sample_signal(signal_id=_SIG_B_ID)
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)
    ev = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)

    sig_a_dump = sig_a.model_dump()
    sig_b_dump = sig_b.model_dump()
    ev_dump = ev.model_dump()
    cfg_dump = config.model_dump()

    group_candidate_signals(
        [inp_a, inp_b],
        [ev],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=_UTC_NOW + timedelta(minutes=5),
    )

    assert sig_a.model_dump() == sig_a_dump
    assert sig_b.model_dump() == sig_b_dump
    assert ev.model_dump() == ev_dump
    assert config.model_dump() == cfg_dump


# 14.8 T1 Grouping Contract Completion Matrices
def _build_validated_two_member_group(
    m1: GroupingMemberReference,
    m2: GroupingMemberReference,
    ev: CorrelationEvidence,
    *,
    policy_version: str,
    configuration_fingerprint: str,
    topology_snapshot_fingerprint: str,
    topology_diameter_hops: int = 1,
) -> CorrelationGroup:
    ordered_members = tuple(sorted((m1, m2), key=lambda m: m.signal_id.bytes))
    times = [m.event_time for m in ordered_members]
    started_at = min(times)
    last_seen_at = max(times)
    span = float((last_seen_at - started_at).total_seconds())
    provisional = CorrelationGroup(
        policy_version=policy_version,
        configuration_fingerprint=configuration_fingerprint,
        topology_snapshot_fingerprint=topology_snapshot_fingerprint,
        members=ordered_members,
        accepted_evidence=(ev,),
        started_at=started_at,
        last_seen_at=last_seen_at,
        time_span_seconds=span,
        topology_diameter_hops=topology_diameter_hops,
        group_fingerprint="0" * 64,
    )
    fp = compute_group_fingerprint(provisional)
    return CorrelationGroup(
        policy_version=policy_version,
        configuration_fingerprint=configuration_fingerprint,
        topology_snapshot_fingerprint=topology_snapshot_fingerprint,
        members=ordered_members,
        accepted_evidence=(ev,),
        started_at=started_at,
        last_seen_at=last_seen_at,
        time_span_seconds=span,
        topology_diameter_hops=topology_diameter_hops,
        group_fingerprint=fp,
    )


@pytest.mark.parametrize(
    "case_id",
    [
        pytest.param("canonical-source-rejection", id="canonical-source-rejection"),
        pytest.param("reversed-input-order", id="reversed-input-order"),
        pytest.param("reversed-evidence-endpoints", id="reversed-evidence-endpoints"),
        pytest.param(
            "ineligible-precedes-source-rejection",
            id="ineligible-precedes-source-rejection",
        ),
        pytest.param(
            "duplicate-rejected-pair-contract",
            id="duplicate-rejected-pair-contract",
        ),
    ],
)
def test_one_pair_one_rejection_matrix(case_id: str) -> None:
    config = _sample_config(acceptance_threshold=0.8)
    snapshot = _sample_topology()
    watermark = _UTC_NOW + timedelta(minutes=5)
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID, service="payment-service", event_time=_UTC_NOW
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    ev_canonical = score_candidate_pair(
        inp_a, inp_b, topology_snapshot=snapshot, config=config
    )
    assert ev_canonical.accepted is False
    assert ev_canonical.left_signal_id == _SIG_A_ID
    assert ev_canonical.right_signal_id == _SIG_B_ID

    baseline = group_candidate_signals(
        [inp_a, inp_b],
        [ev_canonical],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=watermark,
    )
    baseline_bytes = serialize_grouping_result(baseline)

    if case_id == "canonical-source-rejection":
        assert len(baseline.groups) == 2
        assert len(baseline.rejected_edges) == 1
        rej = baseline.rejected_edges[0]
        assert rej.left_signal_id == _SIG_A_ID
        assert rej.right_signal_id == _SIG_B_ID
        assert rej.left_signal_id.bytes < rej.right_signal_id.bytes
        assert rej.reason == GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED
        assert all(
            e.reason != GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE
            for e in baseline.rejected_edges
        )
        assert rej.source_evidence == ev_canonical
    elif case_id == "reversed-input-order":
        reversed_res = group_candidate_signals(
            [inp_b, inp_a],
            [ev_canonical],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=watermark,
        )
        assert len(reversed_res.rejected_edges) == 1
        assert (
            reversed_res.rejected_edges[0].reason
            == GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED
        )
        assert all(
            e.reason != GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE
            for e in reversed_res.rejected_edges
        )
        assert reversed_res == baseline
        assert serialize_grouping_result(reversed_res) == baseline_bytes
        assert reversed_res.groups == baseline.groups
        assert reversed_res.rejected_edges == baseline.rejected_edges
        assert [g.group_fingerprint for g in reversed_res.groups] == [
            g.group_fingerprint for g in baseline.groups
        ]
        assert reversed_res.result_fingerprint == baseline.result_fingerprint
    elif case_id == "reversed-evidence-endpoints":
        ev_reversed = CorrelationEvidence(
            left_signal_id=_SIG_B_ID,
            right_signal_id=_SIG_A_ID,
            temporal_score=ev_canonical.temporal_score,
            service_score=ev_canonical.service_score,
            topology_score=ev_canonical.topology_score,
            symptom_score=ev_canonical.symptom_score,
            context_score=ev_canonical.context_score,
            weighted_score=ev_canonical.weighted_score,
            accepted=ev_canonical.accepted,
            policy_version=ev_canonical.policy_version,
            reason=ev_canonical.reason,
        )
        assert ev_reversed.left_signal_id == _SIG_A_ID
        assert ev_reversed.right_signal_id == _SIG_B_ID
        assert ev_reversed == ev_canonical
        reversed_ev_res = group_candidate_signals(
            [inp_a, inp_b],
            [ev_reversed],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=watermark,
        )
        assert len(reversed_ev_res.rejected_edges) == 1
        assert (
            reversed_ev_res.rejected_edges[0].reason
            == GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED
        )
        assert all(
            e.reason != GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE
            for e in reversed_ev_res.rejected_edges
        )
        assert reversed_ev_res == baseline
        assert serialize_grouping_result(reversed_ev_res) == baseline_bytes
        assert reversed_ev_res.groups == baseline.groups
        assert reversed_ev_res.rejected_edges == baseline.rejected_edges
        assert [g.group_fingerprint for g in reversed_ev_res.groups] == [
            g.group_fingerprint for g in baseline.groups
        ]
        assert reversed_ev_res.result_fingerprint == baseline.result_fingerprint
    elif case_id == "ineligible-precedes-source-rejection":
        late_config = _sample_config(
            late_signal_disposition=LateSignalDisposition.REJECT,
            acceptance_threshold=0.8,
        )
        sig_late_b = _sample_signal(
            signal_id=_SIG_B_ID,
            service="payment-service",
            event_time=_UTC_NOW - timedelta(seconds=100),
        )
        inp_late_b = _make_input(sig_late_b)
        ev_late = score_candidate_pair(
            inp_a, inp_late_b, topology_snapshot=snapshot, config=late_config
        )
        assert ev_late.accepted is False
        res_late = group_candidate_signals(
            [inp_a, inp_late_b],
            [ev_late],
            topology_snapshot=snapshot,
            config=late_config,
            event_time_watermark=_UTC_NOW,
        )
        assert len(res_late.rejected_edges) == 1
        rej_late = res_late.rejected_edges[0]
        assert rej_late.left_signal_id == _SIG_A_ID
        assert rej_late.right_signal_id == _SIG_B_ID
        assert rej_late.reason == GroupingEdgeRejectionReason.INELIGIBLE_SIGNAL
        assert all(
            e.reason != GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED
            for e in res_late.rejected_edges
        )
        assert all(
            e.reason != GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE
            for e in res_late.rejected_edges
        )
        recs = {r.signal_id: r for r in res_late.signal_records}
        assert recs[_SIG_B_ID].included is False
        assert recs[_SIG_B_ID].disposition == GroupingSignalDisposition.LATE_REJECTED
    elif case_id == "duplicate-rejected-pair-contract":
        rej1 = RejectedGroupingEdge(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            reason=GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE,
            source_evidence=None,
            detail="Missing pair evidence between eligible signals",
        )
        rej2 = RejectedGroupingEdge(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            reason=GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED,
            source_evidence=ev_canonical,
            detail="Source pair explicitly rejected by scorer",
        )
        with pytest.raises(GroupingContractError) as exc_info:
            CorrelationGroupingResult(
                policy_version=baseline.policy_version,
                configuration_fingerprint=baseline.configuration_fingerprint,
                topology_snapshot_fingerprint=baseline.topology_snapshot_fingerprint,
                event_time_watermark=baseline.event_time_watermark,
                groups=baseline.groups,
                rejected_edges=(rej1, rej2),
                signal_records=baseline.signal_records,
                result_fingerprint="0" * 64,
            )
        assert type(exc_info.value) is GroupingContractError
        assert str(exc_info.value) == "Duplicate rejected edge for unordered pair"
        assert serialize_grouping_result(baseline) == baseline_bytes
    else:
        raise AssertionError(f"Unhandled case_id: {case_id}")


@pytest.mark.parametrize(
    ("case_id", "expected_message"),
    [
        pytest.param(
            "groups-over-limit",
            "Groups count exceeds maximum",
            id="groups-over-limit",
        ),
        pytest.param(
            "rejections-over-limit",
            "Rejected edges count exceeds maximum",
            id="rejections-over-limit",
        ),
        pytest.param(
            "records-over-limit",
            "Signal records count exceeds maximum",
            id="records-over-limit",
        ),
        pytest.param(
            "groups-not-canonical",
            "Groups must be canonically sorted by first member signal_id bytes",
            id="groups-not-canonical",
        ),
        pytest.param(
            "rejections-not-canonical",
            "Rejected edges must be canonically sorted",
            id="rejections-not-canonical",
        ),
        pytest.param(
            "records-not-canonical",
            "Signal records must be canonically sorted by signal_id bytes",
            id="records-not-canonical",
        ),
        pytest.param(
            "duplicate-record-id",
            "Signal record IDs must be unique",
            id="duplicate-record-id",
        ),
        pytest.param(
            "duplicate-group-fingerprint",
            "Group fingerprints must be unique",
            id="duplicate-group-fingerprint",
        ),
        pytest.param(
            "overlapping-group-member",
            "Signal appears in multiple groups",
            id="overlapping-group-member",
        ),
        pytest.param(
            "included-membership-extra-record",
            "Included signal records must equal exactly group members",
            id="included-membership-extra-record",
        ),
        pytest.param(
            "included-membership-missing-record",
            "Included signal records must equal exactly group members",
            id="included-membership-missing-record",
        ),
        pytest.param(
            "policy-mismatch",
            "Group policy_version must match result policy_version",
            id="policy-mismatch",
        ),
        pytest.param(
            "configuration-fingerprint-mismatch",
            "Group configuration_fingerprint must match result configuration_fingerprint",
            id="configuration-fingerprint-mismatch",
        ),
        pytest.param(
            "topology-fingerprint-mismatch",
            "Group topology_snapshot_fingerprint must match result topology_snapshot_fingerprint",
            id="topology-fingerprint-mismatch",
        ),
        pytest.param(
            "record-group-fingerprint-mismatch",
            "Included record group_fingerprint must match containing group",
            id="record-group-fingerprint-mismatch",
        ),
        pytest.param(
            "semantic-fingerprint-mismatch",
            "Member semantic fingerprint must match signal record",
            id="semantic-fingerprint-mismatch",
        ),
        pytest.param(
            "unknown-rejection-endpoint",
            "Rejected edge endpoints must exist in signal records",
            id="unknown-rejection-endpoint",
        ),
        pytest.param(
            "duplicate-rejected-pair",
            "Duplicate rejected edge for unordered pair",
            id="duplicate-rejected-pair",
        ),
        pytest.param(
            "internal-group-rejection",
            "Rejected edge cannot be internal to an emitted group",
            id="internal-group-rejection",
        ),
        pytest.param(
            "source-evidence-pair-mismatch",
            "Source evidence on rejected edge must reference that exact unordered pair",
            id="source-evidence-pair-mismatch",
        ),
        pytest.param(
            "result-fingerprint-mismatch",
            "result_fingerprint does not match canonical projection",
            id="result-fingerprint-mismatch",
        ),
    ],
)
def test_result_contract_invariant_matrix(case_id: str, expected_message: str) -> None:
    config = _sample_config(acceptance_threshold=0.8)
    snapshot = _sample_topology()
    watermark = _UTC_NOW + timedelta(minutes=5)

    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", event_time=_UTC_NOW
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=5),
    )
    sig_c = _sample_signal(
        signal_id=_SIG_C_ID,
        service="db-service",
        event_time=_UTC_NOW + timedelta(seconds=10),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)
    inp_c = _make_input(sig_c)

    baseline = group_candidate_signals(
        [inp_a, inp_b, inp_c],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=watermark,
    )
    baseline_dump = baseline.model_dump()
    baseline_bytes = serialize_grouping_result(baseline)

    groups = baseline.groups
    rejected_edges = baseline.rejected_edges
    signal_records = baseline.signal_records
    policy_version = baseline.policy_version
    cfg_fp = baseline.configuration_fingerprint
    topo_fp = baseline.topology_snapshot_fingerprint
    res_fp = "0" * 64

    if case_id == "groups-over-limit":
        groups = tuple(groups[0] for _ in range(MAX_SIGNAL_REFS + 1))
    elif case_id == "rejections-over-limit":
        rejected_edges = tuple(
            rejected_edges[0] for _ in range(MAX_EVIDENCE_ENTRIES + 1)
        )
    elif case_id == "records-over-limit":
        signal_records = tuple(signal_records[0] for _ in range(MAX_SIGNAL_REFS + 1))
    elif case_id == "groups-not-canonical":
        groups = (groups[1], groups[0], groups[2])
    elif case_id == "rejections-not-canonical":
        rejected_edges = (rejected_edges[1], rejected_edges[0], rejected_edges[2])
    elif case_id == "records-not-canonical":
        signal_records = (signal_records[1], signal_records[0], signal_records[2])
    elif case_id == "duplicate-record-id":
        signal_records = (signal_records[0], signal_records[0], signal_records[1])
    elif case_id == "duplicate-group-fingerprint":
        g_zero_a = CorrelationGroup(
            policy_version=policy_version,
            configuration_fingerprint=cfg_fp,
            topology_snapshot_fingerprint=topo_fp,
            members=groups[0].members,
            accepted_evidence=(),
            started_at=groups[0].started_at,
            last_seen_at=groups[0].last_seen_at,
            time_span_seconds=0.0,
            topology_diameter_hops=0,
            group_fingerprint="0" * 64,
        )
        g_zero_b = CorrelationGroup(
            policy_version=policy_version,
            configuration_fingerprint=cfg_fp,
            topology_snapshot_fingerprint=topo_fp,
            members=groups[1].members,
            accepted_evidence=(),
            started_at=groups[1].started_at,
            last_seen_at=groups[1].last_seen_at,
            time_span_seconds=0.0,
            topology_diameter_hops=0,
            group_fingerprint="0" * 64,
        )
        groups = (g_zero_a, g_zero_b, groups[2])
    elif case_id == "overlapping-group-member":
        ev_ab = CorrelationEvidence(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            temporal_score=0.95,
            service_score=1.0,
            topology_score=1.0,
            symptom_score=0.95,
            context_score=1.0,
            weighted_score=0.98,
            accepted=True,
            policy_version=policy_version,
            reason="accepted;topology=direct;hops=1",
        )
        g_ab = _build_validated_two_member_group(
            groups[0].members[0],
            groups[1].members[0],
            ev_ab,
            policy_version=policy_version,
            configuration_fingerprint=cfg_fp,
            topology_snapshot_fingerprint=topo_fp,
        )
        groups = (groups[0], g_ab, groups[2])
    elif case_id == "included-membership-extra-record":
        extra_rec = GroupingSignalRecord(
            signal_id=_SIG_D_ID,
            semantic_fingerprint="d" * 64,
            included=True,
            duplicate_count=1,
            late=False,
            disposition=GroupingSignalDisposition.GROUPED,
            group_fingerprint=groups[0].group_fingerprint,
        )
        signal_records = signal_records + (extra_rec,)
    elif case_id == "included-membership-missing-record":
        excluded_c = GroupingSignalRecord(
            signal_id=_SIG_C_ID,
            semantic_fingerprint=signal_records[2].semantic_fingerprint,
            included=False,
            duplicate_count=1,
            late=True,
            disposition=GroupingSignalDisposition.LATE_REJECTED,
            group_fingerprint=None,
        )
        signal_records = (signal_records[0], signal_records[1], excluded_c)
    elif case_id == "policy-mismatch":
        policy_version = "v2.0-mismatch"
    elif case_id == "configuration-fingerprint-mismatch":
        cfg_fp = "e" * 64
    elif case_id == "topology-fingerprint-mismatch":
        topo_fp = "f" * 64
    elif case_id == "record-group-fingerprint-mismatch":
        swapped_rec_a = GroupingSignalRecord(
            signal_id=_SIG_A_ID,
            semantic_fingerprint=signal_records[0].semantic_fingerprint,
            included=True,
            duplicate_count=1,
            late=False,
            disposition=GroupingSignalDisposition.GROUPED,
            group_fingerprint=groups[1].group_fingerprint,
        )
        signal_records = (swapped_rec_a, signal_records[1], signal_records[2])
    elif case_id == "semantic-fingerprint-mismatch":
        diff_fp_rec_a = GroupingSignalRecord(
            signal_id=_SIG_A_ID,
            semantic_fingerprint="1" * 64,
            included=True,
            duplicate_count=1,
            late=False,
            disposition=GroupingSignalDisposition.GROUPED,
            group_fingerprint=groups[0].group_fingerprint,
        )
        signal_records = (diff_fp_rec_a, signal_records[1], signal_records[2])
    elif case_id == "unknown-rejection-endpoint":
        unknown_rej = RejectedGroupingEdge(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_UNKNOWN_ID,
            reason=GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE,
            source_evidence=None,
            detail="Unknown endpoint test",
        )
        rejected_edges = tuple(
            sorted(
                rejected_edges + (unknown_rej,),
                key=lambda e: (
                    e.left_signal_id.bytes,
                    e.right_signal_id.bytes,
                    e.reason.value,
                ),
            )
        )
    elif case_id == "duplicate-rejected-pair":
        ev_rej = CorrelationEvidence(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            temporal_score=0.2,
            service_score=0.2,
            topology_score=0.2,
            symptom_score=0.2,
            context_score=0.2,
            weighted_score=0.2,
            accepted=False,
            policy_version=policy_version,
            reason="rejected;below_threshold",
        )
        dup_rej = RejectedGroupingEdge(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            reason=GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED,
            source_evidence=ev_rej,
            detail="Duplicate unordered pair rejection",
        )
        rejected_edges = tuple(
            sorted(
                rejected_edges + (dup_rej,),
                key=lambda e: (
                    e.left_signal_id.bytes,
                    e.right_signal_id.bytes,
                    e.reason.value,
                ),
            )
        )
    elif case_id == "internal-group-rejection":
        ev_ab = CorrelationEvidence(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            temporal_score=0.95,
            service_score=1.0,
            topology_score=1.0,
            symptom_score=0.95,
            context_score=1.0,
            weighted_score=0.98,
            accepted=True,
            policy_version=policy_version,
            reason="accepted;topology=direct;hops=1",
        )
        g_ab = _build_validated_two_member_group(
            groups[0].members[0],
            groups[1].members[0],
            ev_ab,
            policy_version=policy_version,
            configuration_fingerprint=cfg_fp,
            topology_snapshot_fingerprint=topo_fp,
        )
        rec_a = GroupingSignalRecord(
            signal_id=_SIG_A_ID,
            semantic_fingerprint=signal_records[0].semantic_fingerprint,
            included=True,
            duplicate_count=1,
            late=False,
            disposition=GroupingSignalDisposition.GROUPED,
            group_fingerprint=g_ab.group_fingerprint,
        )
        rec_b = GroupingSignalRecord(
            signal_id=_SIG_B_ID,
            semantic_fingerprint=signal_records[1].semantic_fingerprint,
            included=True,
            duplicate_count=1,
            late=False,
            disposition=GroupingSignalDisposition.GROUPED,
            group_fingerprint=g_ab.group_fingerprint,
        )
        groups = (g_ab, groups[2])
        signal_records = (rec_a, rec_b, signal_records[2])
    elif case_id == "source-evidence-pair-mismatch":
        ev_bc = CorrelationEvidence(
            left_signal_id=_SIG_B_ID,
            right_signal_id=_SIG_C_ID,
            temporal_score=0.2,
            service_score=0.2,
            topology_score=0.2,
            symptom_score=0.2,
            context_score=0.2,
            weighted_score=0.2,
            accepted=False,
            policy_version=policy_version,
            reason="rejected;below_threshold",
        )
        bad_rej_ab = RejectedGroupingEdge(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            reason=GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED,
            source_evidence=ev_bc,
            detail="Mismatched source evidence pair",
        )
        rejected_edges = (bad_rej_ab, rejected_edges[1], rejected_edges[2])
    elif case_id == "result-fingerprint-mismatch":
        res_fp = "a" * 64
    else:
        raise AssertionError(f"Unhandled case_id: {case_id}")

    with pytest.raises(GroupingContractError) as exc_info:
        CorrelationGroupingResult(
            policy_version=policy_version,
            configuration_fingerprint=cfg_fp,
            topology_snapshot_fingerprint=topo_fp,
            event_time_watermark=baseline.event_time_watermark,
            groups=groups,
            rejected_edges=rejected_edges,
            signal_records=signal_records,
            result_fingerprint=res_fp,
        )

    assert type(exc_info.value) is GroupingContractError
    assert str(exc_info.value) == expected_message
    assert baseline.model_dump() == baseline_dump
    assert serialize_grouping_result(baseline) == baseline_bytes


@pytest.mark.parametrize(
    "case_id",
    [
        pytest.param(
            "excluded-record-with-group-fingerprint",
            id="excluded-record-with-group-fingerprint",
        ),
        pytest.param(
            "included-record-without-group-fingerprint",
            id="included-record-without-group-fingerprint",
        ),
        pytest.param(
            "self-referential-rejected-edge",
            id="self-referential-rejected-edge",
        ),
        pytest.param(
            "reversed-rejected-edge-canonicalization",
            id="reversed-rejected-edge-canonicalization",
        ),
    ],
)
def test_child_contract_matrix(case_id: str) -> None:
    if case_id == "excluded-record-with-group-fingerprint":
        with pytest.raises(GroupingContractError) as exc_info:
            GroupingSignalRecord(
                signal_id=_SIG_A_ID,
                semantic_fingerprint="a" * 64,
                included=False,
                duplicate_count=1,
                late=True,
                disposition=GroupingSignalDisposition.LATE_REJECTED,
                group_fingerprint="b" * 64,
            )
        assert type(exc_info.value) is GroupingContractError
        assert str(exc_info.value) == "Excluded signal cannot have group_fingerprint"
    elif case_id == "included-record-without-group-fingerprint":
        with pytest.raises(GroupingContractError) as exc_info:
            GroupingSignalRecord(
                signal_id=_SIG_A_ID,
                semantic_fingerprint="a" * 64,
                included=True,
                duplicate_count=1,
                late=False,
                disposition=GroupingSignalDisposition.GROUPED,
                group_fingerprint=None,
            )
        assert type(exc_info.value) is GroupingContractError
        assert str(exc_info.value) == "Included signal must have group_fingerprint"
    elif case_id == "self-referential-rejected-edge":
        with pytest.raises(GroupingContractError) as exc_info:
            RejectedGroupingEdge(
                left_signal_id=_SIG_A_ID,
                right_signal_id=_SIG_A_ID,
                reason=GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE,
                source_evidence=None,
                detail="Self-referential edge",
            )
        assert type(exc_info.value) is GroupingContractError
        assert str(exc_info.value) == "Rejected edge cannot be self-referential"
    elif case_id == "reversed-rejected-edge-canonicalization":
        canonical_edge = RejectedGroupingEdge(
            left_signal_id=_SIG_A_ID,
            right_signal_id=_SIG_B_ID,
            reason=GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE,
            source_evidence=None,
            detail="Canonical edge ordering test",
        )
        reversed_edge = RejectedGroupingEdge(
            left_signal_id=_SIG_B_ID,
            right_signal_id=_SIG_A_ID,
            reason=GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE,
            source_evidence=None,
            detail="Canonical edge ordering test",
        )
        assert reversed_edge.left_signal_id == _SIG_A_ID
        assert reversed_edge.right_signal_id == _SIG_B_ID
        assert reversed_edge == canonical_edge
        assert reversed_edge.model_dump_json() == canonical_edge.model_dump_json()
    else:
        raise AssertionError(f"Unhandled case_id: {case_id}")


@pytest.mark.parametrize(
    ("case_id", "expected_message"),
    [
        pytest.param(
            "missing-only",
            "Service missing in topology snapshot",
            id="missing-only",
        ),
        pytest.param(
            "ambiguous-only",
            "Service ambiguous in topology snapshot",
            id="ambiguous-only",
        ),
        pytest.param(
            "mixed-missing-ambiguous-original-order",
            "Service missing in topology snapshot",
            id="mixed-missing-ambiguous-original-order",
        ),
        pytest.param(
            "mixed-missing-ambiguous-reversed-order",
            "Service missing in topology snapshot",
            id="mixed-missing-ambiguous-reversed-order",
        ),
    ],
)
def test_canonical_topology_prevalidation_matrix(
    case_id: str, expected_message: str
) -> None:
    config = _sample_config()
    node_1_id = uuid.uuid5(uuid.NAMESPACE_DNS, "ambig-service-node-1")
    node_2_id = uuid.uuid5(uuid.NAMESPACE_DNS, "ambig-service-node-2")
    valid_id = uuid.uuid5(uuid.NAMESPACE_DNS, "valid-service-node")
    nodes = (
        TopologyServiceNode(
            service_id=node_1_id,
            name="aaa-ambiguous-svc",
            service_type=ServiceType.API,
        ),
        TopologyServiceNode(
            service_id=node_2_id,
            name="aaa-ambiguous-svc",
            service_type=ServiceType.API,
        ),
        TopologyServiceNode(
            service_id=valid_id,
            name="mmm-valid-svc",
            service_type=ServiceType.API,
        ),
    )
    version = TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0")
    snapshot = TopologySnapshot.create(version=version, nodes=nodes, edges=())

    sig_missing = _sample_signal(
        signal_id=_SIG_A_ID,
        service="zzz-missing-svc",
        tenant_id=_TENANT_ID,
        run_id=_RUN_ID,
        scenario_id="secret-scenario-payload",
        reproducibility_key="secret-repro-token-999",
    )
    sig_ambig = _sample_signal(
        signal_id=_SIG_B_ID,
        service="aaa-ambiguous-svc",
        tenant_id=_TENANT_ID,
        run_id=_RUN_ID,
        scenario_id="secret-scenario-payload",
        reproducibility_key="secret-repro-token-999",
    )
    inp_missing = _make_input(sig_missing)
    inp_ambig = _make_input(sig_ambig)

    if case_id == "missing-only":
        call_inputs = [inp_missing]
    elif case_id == "ambiguous-only":
        call_inputs = [inp_ambig]
    elif case_id == "mixed-missing-ambiguous-original-order":
        call_inputs = [inp_missing, inp_ambig]
    elif case_id == "mixed-missing-ambiguous-reversed-order":
        call_inputs = [inp_ambig, inp_missing]
    else:
        raise AssertionError(f"Unhandled case_id: {case_id}")

    with pytest.raises(GroupingInputError) as exc_info:
        group_candidate_signals(
            call_inputs,
            [],
            topology_snapshot=snapshot,
            config=config,
            event_time_watermark=_UTC_NOW,
        )

    assert type(exc_info.value) is GroupingInputError
    err_text = str(exc_info.value)
    assert err_text == expected_message

    sensitive_tokens = (
        "zzz-missing-svc",
        "aaa-ambiguous-svc",
        "mmm-valid-svc",
        str(_SIG_A_ID),
        str(_SIG_B_ID),
        str(_TENANT_ID),
        str(_RUN_ID),
        str(_TOPOLOGY_ID),
        str(node_1_id),
        str(node_2_id),
        str(valid_id),
        "D:\\",
        "backend/",
        "secret-scenario-payload",
        "secret-repro-token-999",
    )
    for token in sensitive_tokens:
        assert token not in err_text
