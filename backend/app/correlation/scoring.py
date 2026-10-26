from __future__ import annotations

import math
from typing import Any

from app.correlation.config import CorrelationConfig
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.models import CorrelationEvidence
from app.correlation.topology import (
    TopologyServiceNode,
    TopologySnapshot,
    TraversalDirection,
    find_snapshot_paths,
)
from app.telemetry.schemas import EventSeverity

_SEVERITY_RANK = {
    EventSeverity.DEBUG: 0,
    EventSeverity.INFO: 1,
    EventSeverity.WARNING: 2,
    EventSeverity.ERROR: 3,
    EventSeverity.CRITICAL: 4,
}


def score_candidate_pair(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
    *,
    topology_snapshot: TopologySnapshot,
    config: CorrelationConfig,
) -> CorrelationEvidence:
    """Compute deterministic correlation evidence and eligibility for a candidate pair."""
    if not isinstance(left, AnomalyCorrelationInput):
        raise TypeError("left must be an AnomalyCorrelationInput instance")
    if not isinstance(right, AnomalyCorrelationInput):
        raise TypeError("right must be an AnomalyCorrelationInput instance")
    if not isinstance(topology_snapshot, TopologySnapshot):
        raise TypeError("topology_snapshot must be a TopologySnapshot instance")
    if not isinstance(config, CorrelationConfig):
        raise TypeError("config must be a CorrelationConfig instance")

    # Canonicalize pair by signal_id bytes (lower UUID first)
    left_bytes = left.signal.signal_id.bytes
    right_bytes = right.signal.signal_id.bytes
    if left_bytes <= right_bytes:
        can_left, can_right = left, right
    else:
        can_left, can_right = right, left

    # Compute component scores and topology details
    temporal_score = _compute_temporal(can_left, can_right, config)
    service_score = _compute_service(can_left, can_right)
    topology_score, topology_token, hop_count = _compute_topology(
        can_left, can_right, topology_snapshot, config
    )
    symptom_score = _compute_symptom(can_left, can_right)
    context_score = _compute_context(can_left, can_right)

    weighted_score = _compute_weighted(
        temporal_score,
        service_score,
        topology_score,
        symptom_score,
        context_score,
        config.score_weights,
    )

    # Determine eligibility hard rejections in exact precedence order
    hard_rejection = _determine_hard_rejection(
        can_left, can_right, topology_snapshot, config, topology_score
    )

    if hard_rejection is not None:
        accepted = False
        reason = hard_rejection
    else:
        accepted = weighted_score >= config.acceptance_threshold
        if accepted:
            reason = f"accepted;topology={topology_token};hops={hop_count}"
        else:
            reason = "below_acceptance_threshold"

    return CorrelationEvidence(
        left_signal_id=can_left.signal.signal_id,
        right_signal_id=can_right.signal.signal_id,
        temporal_score=float(max(0.0, min(1.0, temporal_score))),
        service_score=float(max(0.0, min(1.0, service_score))),
        topology_score=float(max(0.0, min(1.0, topology_score))),
        symptom_score=float(max(0.0, min(1.0, symptom_score))),
        context_score=float(max(0.0, min(1.0, context_score))),
        weighted_score=float(max(0.0, min(1.0, weighted_score))),
        accepted=bool(accepted),
        policy_version=config.policy_version,
        reason=reason,
    )


def _compute_temporal(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
    config: CorrelationConfig,
) -> float:
    delta_seconds = abs(
        (left.signal.event_time - right.signal.event_time).total_seconds()
    )
    if config.window_seconds <= 0:
        return 0.0
    score = 1.0 - (delta_seconds / config.window_seconds)
    return float(max(0.0, min(1.0, score)))


def _compute_service(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
) -> float:
    return 1.0 if left.signal.service == right.signal.service else 0.0


def _resolve_service_nodes(
    service_name: str,
    snapshot: TopologySnapshot,
) -> list[TopologyServiceNode]:
    return [node for node in snapshot.nodes if node.name == service_name]


def _compute_topology(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
    snapshot: TopologySnapshot,
    config: CorrelationConfig,
) -> tuple[float, str, int]:
    left_service = left.signal.service
    right_service = right.signal.service

    left_nodes = _resolve_service_nodes(left_service, snapshot)
    right_nodes = _resolve_service_nodes(right_service, snapshot)

    if not left_nodes or not right_nodes:
        return 0.0, "missing", 0
    if len(left_nodes) > 1 or len(right_nodes) > 1:
        return 0.0, "ambiguous", 0

    left_id = left_nodes[0].service_id
    right_id = right_nodes[0].service_id

    if left_id == right_id:
        return 1.0, "same_service", 0

    l2r_paths = find_snapshot_paths(
        snapshot,
        source_service_id=left_id,
        target_service_id=right_id,
        max_hops=config.hop_limit,
        direction=TraversalDirection.DOWNSTREAM,
    )

    r2l_paths = find_snapshot_paths(
        snapshot,
        source_service_id=right_id,
        target_service_id=left_id,
        max_hops=config.hop_limit,
        direction=TraversalDirection.DOWNSTREAM,
    )

    has_l2r = len(l2r_paths) > 0
    has_r2l = len(r2l_paths) > 0

    if not has_l2r and not has_r2l:
        return 0.0, "disconnected", 0

    min_l2r = (
        min((len(p.service_ids) - 1 for p in l2r_paths), default=999)
        if has_l2r
        else 999
    )
    min_r2l = (
        min((len(p.service_ids) - 1 for p in r2l_paths), default=999)
        if has_r2l
        else 999
    )

    if has_l2r and not has_r2l:
        hops = min_l2r
        token = "left_to_right"
    elif has_r2l and not has_l2r:
        hops = min_r2l
        token = "right_to_left"
    else:
        hops = min(min_l2r, min_r2l)
        token = "bidirectional"

    score = 1.0 / (1.0 + hops)
    return float(max(0.0, min(1.0, score))), token, hops


def _compute_symptom(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
) -> float:
    metric_match = (
        1.0 if left.signal.metric_or_feature == right.signal.metric_or_feature else 0.0
    )

    left_rank = _SEVERITY_RANK.get(left.signal.severity, 2)
    right_rank = _SEVERITY_RANK.get(right.signal.severity, 2)
    severity_proximity = 1.0 - (abs(left_rank - right_rank) / 4.0)

    score_agreement = 1.0 - abs(left.signal.anomaly_score - right.signal.anomaly_score)

    symptom_score = math.fsum((metric_match, severity_proximity, score_agreement)) / 3.0
    return float(max(0.0, min(1.0, symptom_score)))


def _compute_context(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
) -> float:
    matches = 0
    if left.signal.run_id == right.signal.run_id:
        matches += 1
    if left.signal.scenario_id == right.signal.scenario_id:
        matches += 1
    if left.signal.scenario_version == right.signal.scenario_version:
        matches += 1
    if left.signal.seed == right.signal.seed:
        matches += 1
    if left.signal.reproducibility_key == right.signal.reproducibility_key:
        matches += 1
    return float(matches / 5.0)


def _compute_weighted(
    temporal: float,
    service: float,
    topology: float,
    symptom: float,
    context: float,
    weights: Any,
) -> float:
    weighted = math.fsum(
        (
            temporal * weights.temporal,
            service * weights.service,
            topology * weights.topology,
            symptom * weights.symptom,
            context * weights.context,
        )
    )
    return float(max(0.0, min(1.0, weighted)))


def _determine_hard_rejection(
    left: AnomalyCorrelationInput,
    right: AnomalyCorrelationInput,
    snapshot: TopologySnapshot,
    config: CorrelationConfig,
    topology_score: float,
) -> str | None:
    # 1. same_signal_identity
    if left.signal.signal_id == right.signal.signal_id:
        return "same_signal_identity"

    # 2. tenant_mismatch
    if left.signal.tenant_id != right.signal.tenant_id:
        return "tenant_mismatch"

    # 3. environment_mismatch
    if left.signal.environment != right.signal.environment:
        return "environment_mismatch"

    # 4. outside_correlation_window
    delta_seconds = abs(
        (left.signal.event_time - right.signal.event_time).total_seconds()
    )
    if delta_seconds > config.window_seconds:
        return "outside_correlation_window"

    # 5. topology_service_missing
    left_nodes = _resolve_service_nodes(left.signal.service, snapshot)
    right_nodes = _resolve_service_nodes(right.signal.service, snapshot)
    if not left_nodes or not right_nodes:
        return "topology_service_missing"

    # 6. topology_service_ambiguous
    if len(left_nodes) > 1 or len(right_nodes) > 1:
        return "topology_service_ambiguous"

    # 7. disconnected_services (if services are different and no path found)
    if left.signal.service != right.signal.service and topology_score == 0.0:
        return "disconnected_services"

    # 8. below_acceptance_threshold checked later against weighted score
    return None


__all__ = ["score_candidate_pair"]
