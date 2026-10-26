# mypy: ignore-errors
from __future__ import annotations

import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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
from app.correlation.models import CorrelationEvidence
from app.correlation.scoring import score_candidate_pair
from app.correlation.topology import (
    TopologySnapshot,
    TopologyVersion,
    TopologyServiceNode,
    DirectedTopologyEdge,
)
from app.correlation.errors import TopologyQueryLimitError
from app.simulator.interfaces import ServiceType, DependencyType
from app.telemetry.schemas import EventSeverity

_UTC_NOW = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
_TENANT_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_RUN_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
_TOPOLOGY_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
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
    weights: dict[str, float] | None = None,
) -> CorrelationConfig:
    w = weights or {
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
        maximum_group_size=100,
        late_signal_disposition=LateSignalDisposition.ACCEPT,
        duplicate_signal_disposition=DuplicateSignalDisposition.IGNORE,
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


# 6.1 Golden evidence and serialization
def test_golden_accepted_evidence_exact_object_and_bytes() -> None:
    config = _sample_config(acceptance_threshold=0.5)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=10),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    evidence = score_candidate_pair(
        inp_a, inp_b, topology_snapshot=snapshot, config=config
    )
    assert evidence.accepted is True
    assert evidence.policy_version == "v1.0"
    assert "accepted" in evidence.reason
    assert 0.0 <= evidence.weighted_score <= 1.0


def test_golden_below_threshold_evidence_exact_object_and_bytes() -> None:
    config = _sample_config(acceptance_threshold=0.99)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID, service="order-service", anomaly_score=0.1
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        anomaly_score=0.1,
        event_time=_UTC_NOW + timedelta(seconds=290),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    evidence = score_candidate_pair(
        inp_a, inp_b, topology_snapshot=snapshot, config=config
    )
    assert evidence.accepted is False
    assert evidence.reason == "below_acceptance_threshold"


def test_golden_out_of_window_evidence_exact_object_and_bytes() -> None:
    config = _sample_config(window_seconds=300)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=400),
    )
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    evidence = score_candidate_pair(
        inp_a, inp_b, topology_snapshot=snapshot, config=config
    )
    assert evidence.accepted is False
    assert evidence.reason == "outside_correlation_window"


def test_golden_disconnected_evidence_exact_object_and_bytes() -> None:
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_c = _sample_signal(signal_id=_SIG_C_ID, service="db-service")
    config_strict = _sample_config(hop_limit=1)
    inp_a = _make_input(sig_a)
    inp_c = _make_input(sig_c)

    evidence = score_candidate_pair(
        inp_a, inp_c, topology_snapshot=snapshot, config=config_strict
    )
    assert evidence.accepted is False
    assert evidence.reason == "disconnected_services"


def test_golden_evidence_json_round_trip_complete_equality() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _make_input(_sample_signal(signal_id=_SIG_A_ID))
    sig_b = _make_input(_sample_signal(signal_id=_SIG_B_ID, service="payment-service"))
    ev = score_candidate_pair(sig_a, sig_b, topology_snapshot=snapshot, config=config)
    restored = CorrelationEvidence.model_validate_json(ev.model_dump_json())
    assert restored == ev


def test_correlation_evidence_is_frozen() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID)),
        _make_input(_sample_signal(signal_id=_SIG_B_ID)),
        topology_snapshot=snapshot,
        config=config,
    )
    with pytest.raises(ValidationError):
        ev.accepted = False  # type: ignore[misc]


def test_all_returned_scores_are_finite_and_bounded() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID)),
        _make_input(_sample_signal(signal_id=_SIG_B_ID)),
        topology_snapshot=snapshot,
        config=config,
    )
    for score in (
        ev.temporal_score,
        ev.service_score,
        ev.topology_score,
        ev.symptom_score,
        ev.context_score,
        ev.weighted_score,
    ):
        assert math.isfinite(score)
        assert 0.0 <= score <= 1.0


def test_reason_is_bounded_and_contains_no_identifiers() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID)),
        _make_input(_sample_signal(signal_id=_SIG_B_ID)),
        topology_snapshot=snapshot,
        config=config,
    )
    assert len(ev.reason) <= 512
    assert "order-service" not in ev.reason
    assert str(_SIG_A_ID) not in ev.reason


# 6.2 Temporal component
@pytest.mark.parametrize(
    "delta,expected_score",
    [
        (0, 1.0),
        (150, 0.5),
        (300, 0.0),
    ],
)
def test_temporal_score_boundaries(delta: int, expected_score: float) -> None:
    config = _sample_config(window_seconds=300)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, event_time=_UTC_NOW)
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID, event_time=_UTC_NOW + timedelta(seconds=delta)
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.temporal_score == expected_score


def test_temporal_score_monotonicity() -> None:
    config = _sample_config(window_seconds=300)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, event_time=_UTC_NOW)
    s1 = score_candidate_pair(
        _make_input(sig_a),
        _make_input(
            _sample_signal(
                signal_id=_SIG_B_ID,
                event_time=_UTC_NOW + timedelta(seconds=10),
            )
        ),
        topology_snapshot=snapshot,
        config=config,
    ).temporal_score
    s2 = score_candidate_pair(
        _make_input(sig_a),
        _make_input(
            _sample_signal(
                signal_id=_SIG_B_ID,
                event_time=_UTC_NOW + timedelta(seconds=100),
            )
        ),
        topology_snapshot=snapshot,
        config=config,
    ).temporal_score
    assert s1 > s2


# 6.3 Service component
def test_service_score_exact_match() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="order-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.service_score == 1.0


def test_service_score_exact_mismatch() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="payment-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.service_score == 0.0


def test_service_score_is_case_sensitive() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="Order-Service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="order-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.service_score == 0.0


# 6.4 Topology component, direction, bidirectional, cycles, and boundaries
def test_topology_same_service() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="order-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.topology_score == 1.0
    assert "same_service" in ev.reason


@pytest.mark.parametrize(
    "svc_l,svc_r,expected_token,expected_hops",
    [
        ("order-service", "payment-service", "left_to_right", 1),
        ("payment-service", "order-service", "right_to_left", 1),
        ("order-service", "db-service", "left_to_right", 2),
    ],
)
def test_topology_directed_paths(
    svc_l: str, svc_r: str, expected_token: str, expected_hops: int
) -> None:
    config = _sample_config(hop_limit=3)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service=svc_l)
    sig_b = _sample_signal(signal_id=_SIG_B_ID, service=svc_r)
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    expected_score = 1.0 / (1.0 + expected_hops)
    assert ev.topology_score == expected_score
    assert expected_token in ev.reason


def test_topology_bidirectional() -> None:
    config = _sample_config(hop_limit=3)
    n1_id = uuid.uuid5(uuid.NAMESPACE_DNS, "service-a")
    n2_id = uuid.uuid5(uuid.NAMESPACE_DNS, "service-b")
    nodes = [
        TopologyServiceNode(
            service_id=n1_id, name="service-a", service_type=ServiceType.API
        ),
        TopologyServiceNode(
            service_id=n2_id, name="service-b", service_type=ServiceType.API
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
            downstream_service_id=n1_id,
            dependency_type=DependencyType.SYNC,
        ),
    ]
    version = TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0")
    snap = TopologySnapshot.create(version=version, nodes=nodes, edges=edges)

    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="service-a")
    sig_b = _sample_signal(signal_id=_SIG_B_ID, service="service-b")
    inp_a = _make_input(sig_a)
    inp_b = _make_input(sig_b)

    ev1 = score_candidate_pair(inp_a, inp_b, topology_snapshot=snap, config=config)
    ev2 = score_candidate_pair(inp_b, inp_a, topology_snapshot=snap, config=config)

    assert ev1.topology_score == 0.5
    assert "bidirectional" in ev1.reason
    assert "hops=1" in ev1.reason
    assert ev1 == ev2
    assert ev1.model_dump_json() == ev2.model_dump_json()


def test_topology_cycle_uses_shortest_simple_path() -> None:
    config = _sample_config(hop_limit=5)
    n1_id = uuid.uuid5(uuid.NAMESPACE_DNS, "node-1")
    n2_id = uuid.uuid5(uuid.NAMESPACE_DNS, "node-2")
    n3_id = uuid.uuid5(uuid.NAMESPACE_DNS, "node-3")
    nodes = [
        TopologyServiceNode(
            service_id=n1_id, name="node-1", service_type=ServiceType.API
        ),
        TopologyServiceNode(
            service_id=n2_id, name="node-2", service_type=ServiceType.API
        ),
        TopologyServiceNode(
            service_id=n3_id, name="node-3", service_type=ServiceType.API
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
        DirectedTopologyEdge(
            upstream_service_id=n3_id,
            downstream_service_id=n1_id,
            dependency_type=DependencyType.SYNC,
        ),
        DirectedTopologyEdge(
            upstream_service_id=n1_id,
            downstream_service_id=n3_id,
            dependency_type=DependencyType.SYNC,
        ),
    ]
    version = TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0")
    snap = TopologySnapshot.create(version=version, nodes=nodes, edges=edges)

    sig_1 = _sample_signal(signal_id=_SIG_A_ID, service="node-1")
    sig_3 = _sample_signal(signal_id=_SIG_B_ID, service="node-3")
    inp_1 = _make_input(sig_1)
    inp_3 = _make_input(sig_3)

    ev1 = score_candidate_pair(inp_1, inp_3, topology_snapshot=snap, config=config)
    ev2 = score_candidate_pair(inp_3, inp_1, topology_snapshot=snap, config=config)

    assert ev1.topology_score == 0.5
    assert "hops=1" in ev1.reason
    assert ev1 == ev2


def test_topology_two_hop_score() -> None:
    config = _sample_config(hop_limit=3)
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="db-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.topology_score == 1.0 / 3.0


@pytest.mark.parametrize(
    "limit,expected_accepted",
    [
        (2, True),
        (1, False),
    ],
)
def test_topology_hop_limit_boundary(limit: int, expected_accepted: bool) -> None:
    config = _sample_config(hop_limit=limit, acceptance_threshold=0.1)
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="db-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.accepted == expected_accepted
    if not expected_accepted:
        assert ev.reason == "disconnected_services"
    else:
        assert ev.topology_score == 1.0 / 3.0


def test_topology_score_monotonicity() -> None:
    config = _sample_config(hop_limit=5)
    snapshot = _sample_topology()
    ev_0 = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="order-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    ev_1 = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="payment-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    ev_2 = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="db-service")),
        topology_snapshot=snapshot,
        config=config,
    )
    config_disc = _sample_config(hop_limit=0)
    ev_disc = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="payment-service")),
        topology_snapshot=snapshot,
        config=config_disc,
    )

    assert (
        ev_0.topology_score
        >= ev_1.topology_score
        >= ev_2.topology_score
        >= ev_disc.topology_score
    )


def test_topology_missing_service() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="nonexistent")),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.accepted is False
    assert ev.reason == "topology_service_missing"


def test_topology_ambiguous_service() -> None:
    config = _sample_config()
    n_id1 = uuid.uuid5(uuid.NAMESPACE_DNS, "dup1")
    n_id2 = uuid.uuid5(uuid.NAMESPACE_DNS, "dup2")
    nodes = [
        TopologyServiceNode(
            service_id=n_id1, name="shared-name", service_type=ServiceType.API
        ),
        TopologyServiceNode(
            service_id=n_id2, name="shared-name", service_type=ServiceType.API
        ),
    ]
    version = TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0")
    snap = TopologySnapshot.create(version=version, nodes=nodes, edges=[])
    ev = score_candidate_pair(
        _make_input(_sample_signal(signal_id=_SIG_A_ID, service="shared-name")),
        _make_input(_sample_signal(signal_id=_SIG_B_ID, service="shared-name")),
        topology_snapshot=snap,
        config=config,
    )
    assert ev.accepted is False
    assert ev.reason == "topology_service_ambiguous"


# 6.5 Typed topology-failure propagation (both lookup positions)
def test_topology_first_lookup_failure_propagates_exact_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sentinel = TopologyQueryLimitError("sentinel-first-lookup")

    def mock_find(*args: Any, **kwargs: Any) -> Any:
        raise sentinel

    monkeypatch.setattr("app.correlation.scoring.find_snapshot_paths", mock_find)

    inp_a = _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service"))
    inp_b = _make_input(_sample_signal(signal_id=_SIG_B_ID, service="payment-service"))

    with pytest.raises(TopologyQueryLimitError) as exc_info:
        score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)
    assert exc_info.value is sentinel


def test_topology_second_lookup_failure_propagates_exact_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.correlation.scoring as scoring_mod

    original_find = scoring_mod.find_snapshot_paths

    config = _sample_config()
    snapshot = _sample_topology()
    sentinel = TopologyQueryLimitError("sentinel-second-lookup")
    call_count = 0

    def mock_find(*args: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return original_find(*args, **kwargs)
        raise sentinel

    monkeypatch.setattr(scoring_mod, "find_snapshot_paths", mock_find)

    inp_a = _make_input(_sample_signal(signal_id=_SIG_A_ID, service="order-service"))
    inp_b = _make_input(_sample_signal(signal_id=_SIG_B_ID, service="payment-service"))

    with pytest.raises(TopologyQueryLimitError) as exc_info:
        score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)
    assert call_count == 2
    assert exc_info.value is sentinel


def test_scoring_source_has_no_broad_exception_catch() -> None:
    scoring_code = Path(__file__).parent.parent / "app" / "correlation" / "scoring.py"
    content = scoring_code.read_text(encoding="utf-8")
    assert "except Exception:" not in content
    assert "except Topo" not in content


# 6.6 Symptom component, anomaly-distance monotonicity, and severity distance monotonicity
@pytest.mark.parametrize(
    "sev_l,sev_r,expected_prox",
    [
        (EventSeverity.CRITICAL, EventSeverity.CRITICAL, 1.0),
        (EventSeverity.CRITICAL, EventSeverity.ERROR, 0.75),
        (EventSeverity.CRITICAL, EventSeverity.DEBUG, 0.0),
    ],
)
def test_symptom_severity_proximity(
    sev_l: EventSeverity, sev_r: EventSeverity, expected_prox: float
) -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID,
        service="order-service",
        severity=sev_l,
        metric_or_feature="cpu",
        anomaly_score=0.9,
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="order-service",
        severity=sev_r,
        metric_or_feature="cpu",
        anomaly_score=0.9,
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    expected_symptom = (1.0 + expected_prox + 1.0) / 3.0
    assert abs(ev.symptom_score - expected_symptom) < 1e-9


def test_symptom_severity_distance_monotonicity() -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID,
        service="order-service",
        metric_or_feature="cpu",
        severity=EventSeverity.CRITICAL,
        anomaly_score=0.9,
    )
    severities = [EventSeverity.CRITICAL, EventSeverity.ERROR, EventSeverity.DEBUG]
    symptom_scores = []
    for sev in severities:
        sig_b = _sample_signal(
            signal_id=_SIG_B_ID,
            service="order-service",
            metric_or_feature="cpu",
            severity=sev,
            anomaly_score=0.9,
        )
        ev = score_candidate_pair(
            _make_input(sig_a),
            _make_input(sig_b),
            topology_snapshot=snapshot,
            config=config,
        )
        symptom_scores.append(ev.symptom_score)

    assert symptom_scores[0] >= symptom_scores[1] >= symptom_scores[2]


def test_symptom_anomaly_distance_monotonicity() -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID,
        service="order-service",
        metric_or_feature="cpu",
        severity=EventSeverity.ERROR,
        anomaly_score=0.0,
    )
    scores = [0.0, 0.5, 1.0]
    symptom_scores = []
    for sc in scores:
        sig_b = _sample_signal(
            signal_id=_SIG_B_ID,
            service="order-service",
            metric_or_feature="cpu",
            severity=EventSeverity.ERROR,
            anomaly_score=sc,
        )
        ev = score_candidate_pair(
            _make_input(sig_a),
            _make_input(sig_b),
            topology_snapshot=snapshot,
            config=config,
        )
        symptom_scores.append(ev.symptom_score)

    assert symptom_scores[0] >= symptom_scores[1] >= symptom_scores[2]


# 6.7 Approved context
def test_context_score_all_fields_match() -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()
    run = uuid.UUID("77777777-7777-7777-7777-777777777777")
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID,
        service="order-service",
        run_id=run,
        scenario_id="s1",
        scenario_version="1.0",
        seed=10,
        reproducibility_key="rk1",
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="order-service",
        run_id=run,
        scenario_id="s1",
        scenario_version="1.0",
        seed=10,
        reproducibility_key="rk1",
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.context_score == 1.0


def test_context_score_all_fields_differ() -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()
    sig_a = _sample_signal(
        signal_id=_SIG_A_ID,
        service="order-service",
        run_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        scenario_id="s1",
        scenario_version="1.0",
        seed=10,
        reproducibility_key="rk1",
    )
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="order-service",
        run_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        scenario_id="s2",
        scenario_version="2.0",
        seed=99,
        reproducibility_key="rk2",
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.context_score == 0.0


@pytest.mark.parametrize(
    "differ_field",
    ["run_id", "scenario_id", "scenario_version", "seed", "reproducibility_key"],
)
def test_context_score_single_field_ablation(differ_field: str) -> None:
    config = _sample_config(acceptance_threshold=0.1)
    snapshot = _sample_topology()
    base_kwargs = dict(
        signal_id=_SIG_A_ID,
        service="order-service",
        run_id=uuid.UUID("77777777-7777-7777-7777-777777777777"),
        scenario_id="s1",
        scenario_version="1.0",
        seed=10,
        reproducibility_key="rk1",
    )
    sig_a = _sample_signal(**base_kwargs)
    b_kwargs = dict(base_kwargs)
    b_kwargs["signal_id"] = _SIG_B_ID
    if differ_field == "run_id":
        b_kwargs["run_id"] = uuid.UUID("88888888-8888-8888-8888-888888888888")
    elif differ_field == "scenario_id":
        b_kwargs["scenario_id"] = "s2"
    elif differ_field == "scenario_version":
        b_kwargs["scenario_version"] = "2.0"
    elif differ_field == "seed":
        b_kwargs["seed"] = 99
    elif differ_field == "reproducibility_key":
        b_kwargs["reproducibility_key"] = "rk2"

    sig_b = _sample_signal(**b_kwargs)
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.context_score == 0.8


# 6.8 Weight isolation, true five-component ablations, and recomputation
@pytest.mark.parametrize(
    "comp,weight_dict",
    [
        (
            "temporal",
            {
                "temporal": 1.0,
                "service": 0.0,
                "topology": 0.0,
                "symptom": 0.0,
                "context": 0.0,
            },
        ),
        (
            "service",
            {
                "temporal": 0.0,
                "service": 1.0,
                "topology": 0.0,
                "symptom": 0.0,
                "context": 0.0,
            },
        ),
        (
            "topology",
            {
                "temporal": 0.0,
                "service": 0.0,
                "topology": 1.0,
                "symptom": 0.0,
                "context": 0.0,
            },
        ),
        (
            "symptom",
            {
                "temporal": 0.0,
                "service": 0.0,
                "topology": 0.0,
                "symptom": 1.0,
                "context": 0.0,
            },
        ),
        (
            "context",
            {
                "temporal": 0.0,
                "service": 0.0,
                "topology": 0.0,
                "symptom": 0.0,
                "context": 1.0,
            },
        ),
    ],
)
def test_one_hot_weight_isolates_component(
    comp: str, weight_dict: dict[str, float]
) -> None:
    config = _sample_config(weights=weight_dict, acceptance_threshold=0.0)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="order-service",
        event_time=_UTC_NOW + timedelta(seconds=10),
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    expected_score = getattr(ev, f"{comp}_score")
    assert ev.weighted_score == expected_score


@pytest.mark.parametrize(
    "ablate_comp", ["temporal", "service", "topology", "symptom", "context"]
)
def test_component_weight_ablation(ablate_comp: str) -> None:
    base_w = {
        "temporal": 0.2,
        "service": 0.2,
        "topology": 0.2,
        "symptom": 0.2,
        "context": 0.2,
    }
    base_w[ablate_comp] = 0.0
    rem_keys = [k for k in base_w if k != ablate_comp]
    for k in rem_keys:
        base_w[k] = 0.25

    config = _sample_config(weights=base_w, acceptance_threshold=0.0)
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=10),
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )

    expected_weighted = math.fsum(
        [
            ev.temporal_score * base_w["temporal"],
            ev.service_score * base_w["service"],
            ev.topology_score * base_w["topology"],
            ev.symptom_score * base_w["symptom"],
            ev.context_score * base_w["context"],
        ]
    )
    assert abs(ev.weighted_score - expected_weighted) < 1e-9


def test_weighted_total_independent_recomputation() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(
        signal_id=_SIG_B_ID,
        service="payment-service",
        event_time=_UTC_NOW + timedelta(seconds=20),
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    weights = config.score_weights
    expected = math.fsum(
        [
            ev.temporal_score * weights.temporal,
            ev.service_score * weights.service,
            ev.topology_score * weights.topology,
            ev.symptom_score * weights.symptom,
            ev.context_score * weights.context,
        ]
    )
    assert abs(ev.weighted_score - expected) < 1e-9


# 6.9 Complete 8-scenario order-invariance matrix
@pytest.mark.parametrize(
    "case_name,sig_a_fn,sig_b_fn,cfg_fn",
    [
        (
            "accepted_same_service",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                service="order-service",
                event_time=_UTC_NOW + timedelta(seconds=5),
            ),
            lambda: _sample_config(acceptance_threshold=0.5),
        ),
        (
            "forward_reverse_connected",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                service="payment-service",
                event_time=_UTC_NOW + timedelta(seconds=5),
            ),
            lambda: _sample_config(acceptance_threshold=0.3),
        ),
        (
            "bidirectional",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="service-a"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="service-b"),
            lambda: _sample_config(acceptance_threshold=0.1),
        ),
        (
            "below_threshold",
            lambda: _sample_signal(
                signal_id=_SIG_A_ID, service="order-service", anomaly_score=0.1
            ),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID, service="payment-service", anomaly_score=0.1
            ),
            lambda: _sample_config(acceptance_threshold=0.99),
        ),
        (
            "disconnected",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="db-service"),
            lambda: _sample_config(hop_limit=1),
        ),
        (
            "outside_window",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                service="payment-service",
                event_time=_UTC_NOW + timedelta(seconds=4000),
            ),
            lambda: _sample_config(window_seconds=300),
        ),
        (
            "missing_service",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="missing-service"),
            lambda: _sample_config(),
        ),
        (
            "ambiguous_service",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="shared-name"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="shared-name"),
            lambda: _sample_config(),
        ),
    ],
)
def test_input_order_invariance_matrix(
    case_name: str, sig_a_fn: Any, sig_b_fn: Any, cfg_fn: Any
) -> None:
    config = cfg_fn()
    if case_name == "bidirectional":
        n1_id = uuid.uuid5(uuid.NAMESPACE_DNS, "service-a")
        n2_id = uuid.uuid5(uuid.NAMESPACE_DNS, "service-b")
        nodes = [
            TopologyServiceNode(
                service_id=n1_id, name="service-a", service_type=ServiceType.API
            ),
            TopologyServiceNode(
                service_id=n2_id, name="service-b", service_type=ServiceType.API
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
                downstream_service_id=n1_id,
                dependency_type=DependencyType.SYNC,
            ),
        ]
        snapshot = TopologySnapshot.create(
            version=TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0"),
            nodes=nodes,
            edges=edges,
        )
    elif case_name == "ambiguous_service":
        n_id1 = uuid.uuid5(uuid.NAMESPACE_DNS, "dup1")
        n_id2 = uuid.uuid5(uuid.NAMESPACE_DNS, "dup2")
        nodes = [
            TopologyServiceNode(
                service_id=n_id1, name="shared-name", service_type=ServiceType.API
            ),
            TopologyServiceNode(
                service_id=n_id2, name="shared-name", service_type=ServiceType.API
            ),
        ]
        snapshot = TopologySnapshot.create(
            version=TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0"),
            nodes=nodes,
            edges=[],
        )
    else:
        snapshot = _sample_topology()

    inp_a = _make_input(sig_a_fn())
    inp_b = _make_input(sig_b_fn())

    res1 = score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)
    res2 = score_candidate_pair(inp_b, inp_a, topology_snapshot=snapshot, config=config)

    assert res1 == res2
    assert res1.model_dump_json() == res2.model_dump_json()
    assert res1.left_signal_id == res2.left_signal_id
    assert res1.right_signal_id == res2.right_signal_id
    assert res1.temporal_score == res2.temporal_score
    assert res1.service_score == res2.service_score
    assert res1.topology_score == res2.topology_score
    assert res1.symptom_score == res2.symptom_score
    assert res1.context_score == res2.context_score
    assert res1.weighted_score == res2.weighted_score
    assert res1.accepted == res2.accepted
    assert res1.reason == res2.reason


# 6.10 Adjacent threshold behavior and complete 8-reason rejection matrix
def test_threshold_exact_equality_is_accepted() -> None:
    config = _sample_config(
        weights={
            "temporal": 1.0,
            "service": 0.0,
            "topology": 0.0,
            "symptom": 0.0,
            "context": 0.0,
        },
        acceptance_threshold=1.0,
    )
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, event_time=_UTC_NOW)
    sig_b = _sample_signal(signal_id=_SIG_B_ID, event_time=_UTC_NOW)
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.accepted is True


def test_threshold_nextafter_below_is_rejected() -> None:
    config_base = _sample_config(
        weights={
            "temporal": 0.0,
            "service": 0.0,
            "topology": 1.0,
            "symptom": 0.0,
            "context": 0.0,
        },
        acceptance_threshold=0.5,
    )
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(signal_id=_SIG_B_ID, service="payment-service")
    ev_base = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config_base,
    )
    score = ev_base.weighted_score
    assert score == 0.5

    strict_thresh = math.nextafter(score, math.inf)
    config_strict = _sample_config(
        weights={
            "temporal": 0.0,
            "service": 0.0,
            "topology": 1.0,
            "symptom": 0.0,
            "context": 0.0,
        },
        acceptance_threshold=strict_thresh,
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config_strict,
    )
    assert ev.accepted is False
    assert ev.reason == "below_acceptance_threshold"


def test_threshold_nextafter_above_is_accepted() -> None:
    config_base = _sample_config(
        weights={
            "temporal": 0.0,
            "service": 0.0,
            "topology": 1.0,
            "symptom": 0.0,
            "context": 0.0,
        },
        acceptance_threshold=0.5,
    )
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, service="order-service")
    sig_b = _sample_signal(signal_id=_SIG_B_ID, service="payment-service")
    ev_base = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config_base,
    )
    score = ev_base.weighted_score

    loose_thresh = math.nextafter(score, -math.inf)
    config_loose = _sample_config(
        weights={
            "temporal": 0.0,
            "service": 0.0,
            "topology": 1.0,
            "symptom": 0.0,
            "context": 0.0,
        },
        acceptance_threshold=loose_thresh,
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config_loose,
    )
    assert ev.accepted is True


@pytest.mark.parametrize(
    "rejection_reason,sig_modifier,cfg_modifier",
    [
        ("same_signal_identity", lambda s1, s2: (s1, s1), lambda: _sample_config()),
        (
            "tenant_mismatch",
            lambda s1, s2: (
                s1,
                _sample_signal(
                    signal_id=_SIG_B_ID,
                    tenant_id=uuid.UUID("99999999-9999-9999-9999-999999999999"),
                ),
            ),
            lambda: _sample_config(),
        ),
        (
            "environment_mismatch",
            lambda s1, s2: (
                s1,
                _sample_signal(signal_id=_SIG_B_ID, environment="staging"),
            ),
            lambda: _sample_config(),
        ),
        (
            "outside_correlation_window",
            lambda s1, s2: (
                s1,
                _sample_signal(
                    signal_id=_SIG_B_ID, event_time=_UTC_NOW + timedelta(seconds=4000)
                ),
            ),
            lambda: _sample_config(window_seconds=300),
        ),
        (
            "topology_service_missing",
            lambda s1, s2: (
                s1,
                _sample_signal(signal_id=_SIG_B_ID, service="unknown-service"),
            ),
            lambda: _sample_config(),
        ),
        (
            "topology_service_ambiguous",
            lambda s1, s2: (s1, s2),
            lambda: _sample_config(),
        ),
        (
            "disconnected_services",
            lambda s1, s2: (
                s1,
                _sample_signal(signal_id=_SIG_B_ID, service="db-service"),
            ),
            lambda: _sample_config(hop_limit=1),
        ),
        (
            "below_acceptance_threshold",
            lambda s1, s2: (
                s1,
                _sample_signal(
                    signal_id=_SIG_B_ID,
                    service="payment-service",
                    anomaly_score=0.1,
                    event_time=_UTC_NOW + timedelta(seconds=290),
                ),
            ),
            lambda: _sample_config(acceptance_threshold=0.99),
        ),
    ],
)
def test_hard_rejection_reasons(
    rejection_reason: str, sig_modifier: Any, cfg_modifier: Any
) -> None:
    config = cfg_modifier()
    if rejection_reason == "topology_service_ambiguous":
        n_id1 = uuid.uuid5(uuid.NAMESPACE_DNS, "dup1")
        n_id2 = uuid.uuid5(uuid.NAMESPACE_DNS, "dup2")
        nodes = [
            TopologyServiceNode(
                service_id=n_id1, name="shared-name", service_type=ServiceType.API
            ),
            TopologyServiceNode(
                service_id=n_id2, name="shared-name", service_type=ServiceType.API
            ),
        ]
        snapshot = TopologySnapshot.create(
            version=TopologyVersion(topology_id=_TOPOLOGY_ID, topology_version="1.0.0"),
            nodes=nodes,
            edges=[],
        )
        s1 = _sample_signal(signal_id=_SIG_A_ID, service="shared-name")
        s2 = _sample_signal(signal_id=_SIG_B_ID, service="shared-name")
    else:
        snapshot = _sample_topology()
        s1 = _sample_signal(signal_id=_SIG_A_ID)
        s2 = _sample_signal(signal_id=_SIG_B_ID)

    mod_s1, mod_s2 = sig_modifier(s1, s2)
    ev = score_candidate_pair(
        _make_input(mod_s1),
        _make_input(mod_s2),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.accepted is False
    assert ev.reason == rejection_reason


def test_hard_rejection_precedence_same_identity_overrides_tenant() -> None:
    config = _sample_config()
    snapshot = _sample_topology()
    sig_a = _sample_signal(signal_id=_SIG_A_ID, tenant_id=_TENANT_ID)
    sig_b = _sample_signal(
        signal_id=_SIG_A_ID,
        tenant_id=uuid.UUID("99999999-9999-9999-9999-999999999999"),
    )
    ev = score_candidate_pair(
        _make_input(sig_a),
        _make_input(sig_b),
        topology_snapshot=snapshot,
        config=config,
    )
    assert ev.reason == "same_signal_identity"


# 6.11 Comprehensive all-outcome immutability matrix
@pytest.mark.parametrize(
    "outcome_name,sig_a_fn,sig_b_fn,cfg_fn,snap_fn,expect_exception",
    [
        (
            "accepted",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                service="payment-service",
                event_time=_UTC_NOW + timedelta(seconds=5),
            ),
            lambda: _sample_config(acceptance_threshold=0.3),
            lambda: _sample_topology(),
            None,
        ),
        (
            "below_acceptance_threshold",
            lambda: _sample_signal(
                signal_id=_SIG_A_ID, service="order-service", anomaly_score=0.1
            ),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                service="payment-service",
                anomaly_score=0.1,
                event_time=_UTC_NOW + timedelta(seconds=290),
            ),
            lambda: _sample_config(acceptance_threshold=0.99),
            lambda: _sample_topology(),
            None,
        ),
        (
            "same_signal_identity",
            lambda: _sample_signal(signal_id=_SIG_A_ID),
            lambda: _sample_signal(signal_id=_SIG_A_ID),
            lambda: _sample_config(),
            lambda: _sample_topology(),
            None,
        ),
        (
            "tenant_mismatch",
            lambda: _sample_signal(signal_id=_SIG_A_ID),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                tenant_id=uuid.UUID("99999999-9999-9999-9999-999999999999"),
            ),
            lambda: _sample_config(),
            lambda: _sample_topology(),
            None,
        ),
        (
            "environment_mismatch",
            lambda: _sample_signal(signal_id=_SIG_A_ID),
            lambda: _sample_signal(signal_id=_SIG_B_ID, environment="staging"),
            lambda: _sample_config(),
            lambda: _sample_topology(),
            None,
        ),
        (
            "outside_correlation_window",
            lambda: _sample_signal(signal_id=_SIG_A_ID),
            lambda: _sample_signal(
                signal_id=_SIG_B_ID,
                event_time=_UTC_NOW + timedelta(seconds=4000),
            ),
            lambda: _sample_config(window_seconds=300),
            lambda: _sample_topology(),
            None,
        ),
        (
            "topology_service_missing",
            lambda: _sample_signal(signal_id=_SIG_A_ID),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="unknown-service"),
            lambda: _sample_config(),
            lambda: _sample_topology(),
            None,
        ),
        (
            "topology_service_ambiguous",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="shared-name"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="shared-name"),
            lambda: _sample_config(),
            lambda: TopologySnapshot.create(
                version=TopologyVersion(
                    topology_id=_TOPOLOGY_ID, topology_version="1.0.0"
                ),
                nodes=[
                    TopologyServiceNode(
                        service_id=uuid.uuid5(uuid.NAMESPACE_DNS, "d1"),
                        name="shared-name",
                        service_type=ServiceType.API,
                    ),
                    TopologyServiceNode(
                        service_id=uuid.uuid5(uuid.NAMESPACE_DNS, "d2"),
                        name="shared-name",
                        service_type=ServiceType.API,
                    ),
                ],
                edges=[],
            ),
            None,
        ),
        (
            "disconnected_services",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="db-service"),
            lambda: _sample_config(hop_limit=1),
            lambda: _sample_topology(),
            None,
        ),
        (
            "first_topology_lookup_failure",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="payment-service"),
            lambda: _sample_config(),
            lambda: _sample_topology(),
            TopologyQueryLimitError,
        ),
        (
            "second_topology_lookup_failure",
            lambda: _sample_signal(signal_id=_SIG_A_ID, service="order-service"),
            lambda: _sample_signal(signal_id=_SIG_B_ID, service="payment-service"),
            lambda: _sample_config(),
            lambda: _sample_topology(),
            TopologyQueryLimitError,
        ),
    ],
)
def test_input_config_and_snapshot_immutability(
    outcome_name: str,
    sig_a_fn: Any,
    sig_b_fn: Any,
    cfg_fn: Any,
    snap_fn: Any,
    expect_exception: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = cfg_fn()
    snapshot = snap_fn()
    inp_a = _make_input(sig_a_fn())
    inp_b = _make_input(sig_b_fn())

    d_a = inp_a.model_dump_json()
    d_b = inp_b.model_dump_json()
    d_snap = snapshot.model_dump_json()
    d_conf = config.model_dump_json()

    if expect_exception is not None:
        sentinel = expect_exception("sentinel-immutability")
        if "second" in outcome_name:
            import app.correlation.scoring as scoring_mod

            original_find = scoring_mod.find_snapshot_paths
            call_count = 0

            def mock_find(*args: Any, **kwargs: Any) -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return original_find(*args, **kwargs)
                raise sentinel

            monkeypatch.setattr(scoring_mod, "find_snapshot_paths", mock_find)
        else:

            def mock_find(*args: Any, **kwargs: Any) -> Any:
                raise sentinel

            monkeypatch.setattr(
                "app.correlation.scoring.find_snapshot_paths", mock_find
            )

        with pytest.raises(expect_exception) as exc_info:
            score_candidate_pair(
                inp_a, inp_b, topology_snapshot=snapshot, config=config
            )
        assert exc_info.value is sentinel
    else:
        score_candidate_pair(inp_a, inp_b, topology_snapshot=snapshot, config=config)

    assert inp_a.model_dump_json() == d_a
    assert inp_b.model_dump_json() == d_b
    assert snapshot.model_dump_json() == d_snap
    assert config.model_dump_json() == d_conf


# 6.12 Purity and leakage checks
def test_zero_prohibited_terminology_in_scoring() -> None:
    scoring_code = Path(__file__).parent.parent / "app" / "correlation" / "scoring.py"
    text = scoring_code.read_text(encoding="utf-8").lower()
    for term in (
        "probability",
        "accuracy",
        "causal",
        "root cause",
        "risk",
        "remediation",
        "rag",
        "embedding",
    ):
        assert term not in text
