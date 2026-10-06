from __future__ import annotations

import argparse
import asyncio
import collections
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import json
import math
from pathlib import Path
import time
from typing import Any, Literal
import uuid
import xml.etree.ElementTree as ET

from pydantic import ValidationError
import pytest

import app.anomaly
from app.anomaly.errors import (
    EvaluationArtifactError,
    EvaluationConfigurationError,
    EvaluationExecutionError,
)
from app.anomaly.evaluation import (
    CANONICAL_MODEL_ORDER,
    CANONICAL_SCENARIO_ORDER,
    EXPECTED_ARTIFACT_MEDIA_TYPES,
    EXPECTED_RECORD_COUNT_REQUIRED_SUBPATHS,
    LINEAGE_FIELD_ERROR_MAP,
    LINEAGE_FIELD_ERROR_PRIORITY,
    REQUIRED_RELATIVE_DIGESTED_ARTIFACT_PATHS,
    SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
    SUPPORTED_DECISION_POLICY_VERSION,
    SUPPORTED_EVALUATION_SCHEMA_VERSION,
    SUPPORTED_LABEL_POLICY_VERSION,
    ArtifactManifestEntry,
    ConfusionCounts,
    DetectionLatencyResult,
    EvaluationDecisionPolicy,
    EvaluationHarnessConfig,
    EvaluationLabelPolicy,
    EvaluationOutcome,
    EvaluationOutcomeStatus,
    EvaluationPackageManifest,
    EvaluationRunMetadata,
    LatencySummary,
    MetricResult,
    ModelComparisonSummary,
    Phase3EvaluationHarness,
    ScenarioCoverageSummary,
    ScenarioModelEvaluation,
    ScenarioRunLineage,
    ScoreDistributionSummary,
    _build_evaluation_run_metadata,
    _build_scenario_run_lineage,
    _get_sanitized_lineage_errors,
    _guard_finite_serialization,
    _raise_lineage_translation_fallback,
    _translate_lineage_error_details,
    _translate_lineage_validation_error,
    _encode_text_payload,
    calculate_confusion_counts,
    calculate_f1,
    calculate_false_positive_rate,
    calculate_precision,
    calculate_recall,
    calculate_score_distribution,
    classify_confusion_category,
    compute_binary_decision,
    compute_truth_interval,
    create_default_decision_policy,
    create_default_evaluation_harness_config,
    create_default_label_policy,
    derive_calibration_sample_id,
    derive_outcome_id,
    derive_run_id,
    evaluate_ground_truth_label,
    generate_detection_latency_svg,
    generate_evaluation_report,
    generate_model_quality_svg,
    resolve_evaluation_package_paths,
    run_phase3_evaluation,
    validate_evaluation_outcomes,
    validate_evaluation_run_metadata,
)
from app.anomaly.calibration import CalibrationConfig
from app.anomaly.calibration import (
    SUPPORTED_CALIBRATION_METHOD_VERSION,
    SUPPORTED_CALIBRATION_SPLIT_VERSION,
    SUPPORTED_SEVERITY_MAPPING_VERSION,
)
from app.anomaly.models import (
    MAX_UINT64,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.prophet import ProphetScoreResult, ProphetScoreStatus
from app.simulator.scenarios import get_scenario, scenario_ids
from app.telemetry.schemas import EventSeverity


def _sample_outcome(
    is_tp: bool = False,
    is_fp: bool = False,
    is_tn: bool = False,
    is_fn: bool = False,
    status: EvaluationOutcomeStatus = EvaluationOutcomeStatus.SUCCESS,
    score: float | None = 0.6,
    event_time: datetime | None = None,
    scenario_id: str = "cpu-saturation",
    model_name: str = "prophet",
) -> EvaluationOutcome:
    t = event_time or datetime(2026, 1, 1, 0, 0, 15, tzinfo=timezone.utc)
    gt_pos = is_tp or is_fn
    pred_pos = is_tp or is_fp

    outcome_id = derive_outcome_id(
        package_id="phase3-task-3-9-comparison",
        package_version="1.0.0",
        model_name=model_name,
        run_id=uuid.uuid4(),
        scenario_id=scenario_id,
        unit_id=f"{scenario_id}:1",
    )

    return EvaluationOutcome(
        outcome_id=outcome_id,
        unit_id=f"{scenario_id}:1",
        unit_index=1,
        scenario_id=scenario_id,
        run_id=uuid.uuid4(),
        model_name=model_name,
        model_version="1.0.0",
        event_time=t,
        ground_truth_positive=gt_pos,
        status=status,
        raw_score=score if status == EvaluationOutcomeStatus.SUCCESS else None,
        baseline_normalized_score=score
        if status == EvaluationOutcomeStatus.SUCCESS
        else None,
        calibrated_score=score if status == EvaluationOutcomeStatus.SUCCESS else None,
        predicted_positive=pred_pos,
        is_true_positive=is_tp,
        is_false_positive=is_fp,
        is_true_negative=is_tn,
        is_false_negative=is_fn,
    )


# 1. DETERMINISTIC IDENTITY CONTRACT TESTS (CHECKPOINT C1)


def test_deterministic_run_id_reproducibility() -> None:
    id1 = derive_run_id("pkg1", "1.0.0", "calibration", "cpu-saturation", 41)
    id2 = derive_run_id("pkg1", "1.0.0", "calibration", "cpu-saturation", 41)
    assert id1 == id2
    assert isinstance(id1, uuid.UUID)
    assert id1.version == 5


def test_sample_id_pure_determinism() -> None:
    r_id = uuid.uuid4()
    t_ev = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    ev_ids = [uuid.uuid4(), uuid.uuid4()]

    sample1 = derive_calibration_sample_id(
        package_id="pkg1",
        package_version="1.0.0",
        model_name="prophet",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        window_index=5,
        event_time=t_ev,
        source_event_ids=ev_ids,
    )
    sample2 = derive_calibration_sample_id(
        package_id="pkg1",
        package_version="1.0.0",
        model_name="prophet",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        window_index=5,
        event_time=t_ev,
        source_event_ids=ev_ids,
    )
    assert sample1 == sample2
    assert sample1.version == 5


def test_sample_id_random_source_signal_independence() -> None:
    r_id = uuid.uuid4()
    t_ev = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    ev_ids = [uuid.uuid4(), uuid.uuid4()]

    sample_a = derive_calibration_sample_id(
        package_id="pkg1",
        package_version="1.0.0",
        model_name="prophet",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        window_index=5,
        event_time=t_ev,
        source_event_ids=ev_ids,
    )
    sample_b = derive_calibration_sample_id(
        package_id="pkg1",
        package_version="1.0.0",
        model_name="prophet",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        window_index=5,
        event_time=t_ev,
        source_event_ids=ev_ids,
    )
    assert sample_a == sample_b


def test_sample_id_source_event_order_canonicalization() -> None:
    r_id = uuid.uuid4()
    t_ev = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    ev1 = uuid.uuid4()
    ev2 = uuid.uuid4()
    ev3 = uuid.uuid4()

    sample_order_1 = derive_calibration_sample_id(
        package_id="pkg1",
        package_version="1.0.0",
        model_name="isolation_forest",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="memory-exhaustion",
        scenario_version="1.0.0",
        window_index=3,
        event_time=t_ev,
        source_event_ids=[ev1, ev2, ev3],
    )
    sample_order_2 = derive_calibration_sample_id(
        package_id="pkg1",
        package_version="1.0.0",
        model_name="isolation_forest",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="memory-exhaustion",
        scenario_version="1.0.0",
        window_index=3,
        event_time=t_ev,
        source_event_ids=[ev3, ev1, ev2],
    )
    assert sample_order_1 == sample_order_2


def test_sample_id_semantic_difference_sensitivity() -> None:
    r_id1 = uuid.uuid4()
    r_id2 = uuid.uuid4()
    t_ev1 = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    t_ev2 = datetime(2026, 1, 1, 0, 0, 11, tzinfo=timezone.utc)
    ev_a = [uuid.uuid4()]
    ev_b = [uuid.uuid4()]

    base = derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.0", 1, t_ev1, ev_a
    )

    assert base != derive_calibration_sample_id(
        "pkg2", "1.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.0", 1, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "2.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.0", 1, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "autoencoder", "1.0.0", r_id1, "scen", "1.0.0", 1, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.1.0", r_id1, "scen", "1.0.0", 1, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.0.0", r_id2, "scen", "1.0.0", 1, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1",
        "1.0.0",
        "prophet",
        "1.0.0",
        r_id1,
        "other-scen",
        "1.0.0",
        1,
        t_ev1,
        ev_a,
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.1", 1, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.0", 2, t_ev1, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.0", 1, t_ev2, ev_a
    )
    assert base != derive_calibration_sample_id(
        "pkg1", "1.0.0", "prophet", "1.0.0", r_id1, "scen", "1.0.0", 1, t_ev1, ev_b
    )


def test_sample_id_matrix_collision_check() -> None:
    sample_ids: set[uuid.UUID] = set()
    r_id = uuid.uuid4()
    t_start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    for s_id in CANONICAL_SCENARIO_ORDER:
        for m_name in CANONICAL_MODEL_ORDER:
            for w_idx in range(30):
                t_w = t_start + timedelta(seconds=w_idx)
                smp_id = derive_calibration_sample_id(
                    package_id="phase3-task-3-9-comparison",
                    package_version="1.0.0",
                    model_name=m_name,
                    model_version="1.0.0",
                    run_id=r_id,
                    scenario_id=s_id,
                    scenario_version="1.0.0",
                    window_index=w_idx,
                    event_time=t_w,
                )
                assert smp_id not in sample_ids
                sample_ids.add(smp_id)

    assert len(sample_ids) == 8 * 3 * 30


def test_sample_id_harness_call_site_proof() -> None:
    r_id = uuid.uuid4()
    t_ev = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    ev_ids = [uuid.uuid4(), uuid.uuid4()]
    tw = EventTimeWindow(start_time=t_ev, end_time=t_ev + timedelta(seconds=1))

    sig1 = AnomalySignal(
        signal_id=uuid.uuid4(),
        event_time=t_ev,
        service="order-service",
        metric_or_feature="http_request_duration_ms:mean",
        model_name="prophet",
        model_version="1.0.0",
        anomaly_score=0.75,
        severity=EventSeverity.WARNING,
        evidence=[
            AnomalyEvidence(
                evidence_id=uuid.uuid4(),
                evidence_type="forecast_deviation",
                metric_or_feature="http_request_duration_ms",
                timestamp=t_ev,
            )
        ],
        threshold_or_calibration=CalibrationMetadata(
            method="baseline_normalization",
            threshold_value=0.5,
            calibration_version="1.0.0",
        ),
        schema_version="1.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        seed=41,
        reproducibility_key="key",
        source_event_ids=ev_ids,
        source_event_time_window=tw,
        environment="simulation",
        tenant_id=uuid.uuid4(),
    )
    res1 = ProphetScoreResult(
        target_window_index=5,
        target_timestamp=t_ev,
        status=ProphetScoreStatus.SUCCESS,
        anomaly_score=0.75,
        signal=sig1,
    )

    sig2 = sig1.model_copy(update={"signal_id": uuid.uuid4()})
    assert sig1.signal_id != sig2.signal_id
    res2 = ProphetScoreResult(
        target_window_index=5,
        target_timestamp=t_ev,
        status=ProphetScoreStatus.SUCCESS,
        anomaly_score=0.75,
        signal=sig2,
    )

    assert res1.signal is not None
    assert res2.signal is not None

    sample_id_1 = derive_calibration_sample_id(
        package_id="phase3-task-3-9-comparison",
        package_version="1.0.0",
        model_name="prophet",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        window_index=5,
        event_time=res1.signal.event_time,
        source_event_ids=res1.signal.source_event_ids,
    )
    sample_id_2 = derive_calibration_sample_id(
        package_id="phase3-task-3-9-comparison",
        package_version="1.0.0",
        model_name="prophet",
        model_version="1.0.0",
        run_id=r_id,
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        window_index=5,
        event_time=res2.signal.event_time,
        source_event_ids=res2.signal.source_event_ids,
    )

    assert sample_id_1 == sample_id_2


def test_calibration_and_evaluation_run_ids_disjoint_and_non_colliding() -> None:
    calib_ids: set[uuid.UUID] = set()
    eval_ids: set[uuid.UUID] = set()

    for s_id in CANONICAL_SCENARIO_ORDER:
        c_id = derive_run_id("pkg", "1.0.0", "calibration", s_id, 41)
        e_id = derive_run_id("pkg", "1.0.0", "evaluation", s_id, 42)
        assert c_id != e_id
        calib_ids.add(c_id)
        eval_ids.add(e_id)

    assert len(calib_ids) == 8
    assert len(eval_ids) == 8
    assert calib_ids.isdisjoint(eval_ids)


def test_outcome_ids_no_collisions_across_matrix() -> None:
    outcome_ids: set[uuid.UUID] = set()
    r_id = uuid.uuid4()
    for s_id in CANONICAL_SCENARIO_ORDER:
        for m_name in CANONICAL_MODEL_ORDER:
            for w_idx in range(30):
                u_id = f"{s_id}:{w_idx}"
                o_id = derive_outcome_id("pkg", "1.0.0", m_name, r_id, s_id, u_id)
                assert o_id not in outcome_ids
                outcome_ids.add(o_id)

    assert len(outcome_ids) == 8 * 3 * 30


def test_public_exports_no_duplicates() -> None:
    all_exports = app.anomaly.__all__
    assert len(all_exports) == len(
        set(all_exports)
    ), "Duplicate entries detected in app.anomaly.__all__"
    assert "observations_from_scenario_result" in all_exports
    assert "observations_from_telemetry_events" in all_exports
    assert "compute_truth_interval" in all_exports
    assert "evaluate_ground_truth_label" in all_exports
    assert "compute_binary_decision" in all_exports
    assert "classify_confusion_category" in all_exports


# 2. REQUIRED AND SUPPORTED VERSION CONTRACT TESTS


def test_evaluation_schema_version_accepted() -> None:
    cfg = create_default_evaluation_harness_config()
    assert cfg.schema_version == SUPPORTED_EVALUATION_SCHEMA_VERSION
    assert cfg.decision_policy.policy_version == SUPPORTED_DECISION_POLICY_VERSION
    assert cfg.label_policy.policy_version == SUPPORTED_LABEL_POLICY_VERSION


def test_missing_and_unsupported_harness_config_versions_rejected() -> None:
    data = create_default_evaluation_harness_config().model_dump()
    del data["schema_version"]
    with pytest.raises(ValidationError):
        EvaluationHarnessConfig.model_validate(data)

    data["schema_version"] = "2.0"
    with pytest.raises(ValidationError, match="Unsupported evaluation schema_version"):
        EvaluationHarnessConfig.model_validate(data)


def test_decision_policy_version_validation() -> None:
    with pytest.raises(ValidationError):
        EvaluationDecisionPolicy.model_validate(
            {
                "policy_name": "name",
                "decision_threshold": 0.5,
                "description": "desc",
            }
        )

    with pytest.raises(ValidationError, match="Unsupported policy_version"):
        EvaluationDecisionPolicy(
            policy_name="name",
            policy_version="2.0.0",
            decision_threshold=0.5,
            description="desc",
        )

    pol = create_default_decision_policy(decision_threshold=0.50)
    assert pol.policy_version == SUPPORTED_DECISION_POLICY_VERSION


def test_label_policy_version_validation() -> None:
    with pytest.raises(ValidationError):
        EvaluationLabelPolicy.model_validate(
            {
                "policy_name": "name",
                "overlap_rule": "rule",
                "description": "desc",
            }
        )

    with pytest.raises(ValidationError, match="Unsupported policy_version"):
        EvaluationLabelPolicy(
            policy_name="name",
            policy_version="0.9.0",
            overlap_rule="rule",
            description="desc",
        )

    pol = create_default_label_policy()
    assert pol.policy_version == SUPPORTED_LABEL_POLICY_VERSION


def test_artifact_manifest_entry_validation() -> None:
    entry = ArtifactManifestEntry(
        path="research/results/processed/test.json",
        media_type="application/json",
        schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
        record_count=10,
        sha256="a" * 64,
    )
    assert entry.schema_version == "1.0"
    assert entry.record_count == 10

    raw_data = entry.model_dump()
    del raw_data["schema_version"]
    with pytest.raises(ValidationError):
        ArtifactManifestEntry.model_validate(raw_data)

    raw_data["schema_version"] = "2.0"
    with pytest.raises(
        ValidationError, match="Unsupported artifact entry schema_version"
    ):
        ArtifactManifestEntry.model_validate(raw_data)

    with pytest.raises(ValidationError, match="sha256"):
        ArtifactManifestEntry(
            path="research/results/test.json",
            media_type="application/json",
            schema_version="1.0",
            sha256="A" * 64,
        )

    with pytest.raises(ValidationError, match="sha256"):
        ArtifactManifestEntry(
            path="research/results/test.json",
            media_type="application/json",
            schema_version="1.0",
            sha256="abc123",
        )

    with pytest.raises(ValidationError, match="record_count"):
        ArtifactManifestEntry(
            path="research/results/test.json",
            media_type="application/json",
            schema_version="1.0",
            record_count=-1,
            sha256="a" * 64,
        )

    with pytest.raises(ValidationError, match="record_count"):
        ArtifactManifestEntry(
            path="research/results/test.json",
            media_type="application/json",
            schema_version="1.0",
            record_count=True,  # type: ignore[arg-type]
            sha256="a" * 64,
        )


def _build_valid_manifest_artifacts(
    root: str = "research",
) -> list[ArtifactManifestEntry]:
    return [
        ArtifactManifestEntry(
            path=f"{root}/{p}" if root else p,
            media_type=EXPECTED_ARTIFACT_MEDIA_TYPES[p],
            schema_version="1.0",
            record_count=10 if p in EXPECTED_RECORD_COUNT_REQUIRED_SUBPATHS else None,
            sha256="a" * 64,
        )
        for p in REQUIRED_RELATIVE_DIGESTED_ARTIFACT_PATHS
    ]


def test_package_manifest_version_and_invariants() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    valid_entries = _build_valid_manifest_artifacts("research")

    manifest = EvaluationPackageManifest(
        schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
        package_id="pkg",
        package_version="1.0.0",
        code_revision="rev123",
        created_at=t0,
        environment="simulation",
        calibration_seed=41,
        evaluation_seed=42,
        decision_threshold=0.5,
        models=list(CANONICAL_MODEL_ORDER),
        scenarios=list(CANONICAL_SCENARIO_ORDER),
        artifact_manifest_path="research/results/processed/phase3/task_3_9/artifact_manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=valid_entries,
    )
    assert manifest.schema_version == "1.0"
    assert len(manifest.artifacts) == 11

    raw = manifest.model_dump()
    del raw["schema_version"]
    with pytest.raises(ValidationError):
        EvaluationPackageManifest.model_validate(raw)


# 3. SAFE ARTIFACT-ROOT CONTRACT TESTS


def test_default_artifact_root_layout(tmp_path: Path) -> None:
    paths = resolve_evaluation_package_paths(tmp_path, "research")
    assert paths.normalized_root == "research"
    assert (
        paths.experiments_dir
        == tmp_path / "research" / "experiments" / "phase3" / "task_3_9"
    )
    assert (
        paths.raw_dir
        == tmp_path / "research" / "results" / "raw" / "phase3" / "task_3_9"
    )
    assert (
        paths.processed_dir
        == tmp_path / "research" / "results" / "processed" / "phase3" / "task_3_9"
    )
    assert (
        paths.figures_dir
        == tmp_path / "research" / "results" / "figures" / "phase3" / "task_3_9"
    )
    assert (
        paths.reports_dir == tmp_path / "research" / "reports" / "phase3" / "task_3_9"
    )
    assert (
        paths.relative_artifact_path("experiments/phase3/task_3_9/manifest.json")
        == "research/experiments/phase3/task_3_9/manifest.json"
    )


def test_valid_artifact_root_override_layout(tmp_path: Path) -> None:
    paths = resolve_evaluation_package_paths(tmp_path, "research/task_3_9_override")
    assert paths.normalized_root == "research/task_3_9_override"
    assert (
        paths.experiments_dir
        == tmp_path
        / "research"
        / "task_3_9_override"
        / "experiments"
        / "phase3"
        / "task_3_9"
    )
    assert (
        paths.relative_artifact_path("reports/phase3/task_3_9/report.md")
        == "research/task_3_9_override/reports/phase3/task_3_9/report.md"
    )


def test_unsafe_artifact_root_rejection(tmp_path: Path) -> None:
    unsafe_roots = [
        "",
        "   ",
        ".",
        "./",
        "../outside",
        "/absolute/posix",
        "C:\\windows\\absolute",
        "D:/drive/absolute",
        "\\\\unc\\share",
        "research/../../escape",
        "research/./nested_dot",
    ]
    for root in unsafe_roots:
        with pytest.raises((ValueError, EvaluationArtifactError)):
            resolve_evaluation_package_paths(tmp_path, root)


# 4. PRODUCTION TRUTH, DECISION, AND TEMPORAL TESTS (CHECKPOINT C2)


def test_production_binary_decision_logic() -> None:
    # 1. Success at or above threshold -> True
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.75, 0.50) is True
    # 2. Success exact threshold -> True
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.50, 0.50) is True
    # 3. Success just below threshold -> False
    assert (
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.499999, 0.50)
        is False
    )
    # score boundaries 0.0 and 1.0
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.0, 0.0) is True
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 1.0, 1.0) is True
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.0, 0.5) is False
    # threshold boundaries 0.0 and 1.0
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, 0.0) is True
    assert compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, 1.0) is False

    # missing successful score
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, None, 0.50)

    # score NaN, +Inf, and -Inf
    for bad_score in [float("nan"), float("inf"), float("-inf")]:
        with pytest.raises(EvaluationConfigurationError):
            compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, bad_score, 0.50)

    # negative and >1 score
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, -0.1, 0.50)
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 1.1, 0.50)

    # threshold NaN, +Inf, and -Inf
    for bad_thresh in [float("nan"), float("inf"), float("-inf")]:
        with pytest.raises(EvaluationConfigurationError):
            compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, bad_thresh)

    # negative and >1 threshold
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, -0.1)
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, 1.1)

    # boolean score and threshold
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, True, 0.5)
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, False)

    # non-numeric score and threshold
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, "0.5", 0.5)  # type: ignore[arg-type]
    with pytest.raises(EvaluationConfigurationError):
        compute_binary_decision(EvaluationOutcomeStatus.SUCCESS, 0.5, "0.5")  # type: ignore[arg-type]

    non_success_statuses = [
        EvaluationOutcomeStatus.INSUFFICIENT_DATA,
        EvaluationOutcomeStatus.MISSING_CALIBRATION,
        EvaluationOutcomeStatus.NON_CONVERGENCE,
        EvaluationOutcomeStatus.FIT_FAILURE,
        EvaluationOutcomeStatus.INVALID_INPUT,
        EvaluationOutcomeStatus.MODEL_FAILURE,
        EvaluationOutcomeStatus.NOT_APPLICABLE,
    ]

    # every non-success enum value with score=None -> returns False
    for status in non_success_statuses:
        assert compute_binary_decision(status, None, 0.50) is False

    # every non-success enum value with a supplied score -> throws
    for status in non_success_statuses:
        with pytest.raises(EvaluationConfigurationError):
            compute_binary_decision(status, 0.99, 0.50)


def test_production_classify_confusion_category() -> None:
    assert classify_confusion_category(True, True) == (True, False, False, False)
    assert classify_confusion_category(False, True) == (False, True, False, False)
    assert classify_confusion_category(False, False) == (False, False, True, False)
    assert classify_confusion_category(True, False) == (False, False, False, True)


# 5. NUMERIC VALIDATION AND CONTRACT HARDENING TESTS (CHECKPOINT C2)


def test_evaluation_outcome_finite_and_bounds_validation() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    uid = uuid.uuid4()

    # Rejects NaN / Inf raw_score
    with pytest.raises(ValidationError, match="finite float"):
        EvaluationOutcome(
            outcome_id=uuid.uuid4(),
            unit_id="u1",
            unit_index=1,
            scenario_id="cpu-saturation",
            run_id=uid,
            model_name="prophet",
            model_version="1.0.0",
            event_time=t0,
            ground_truth_positive=True,
            status=EvaluationOutcomeStatus.SUCCESS,
            raw_score=float("nan"),
            calibrated_score=0.8,
            predicted_positive=True,
            is_true_positive=True,
        )

    # Rejects out-of-bounds calibrated_score
    with pytest.raises(ValidationError, match="finite float in \\[0.0, 1.0\\]"):
        EvaluationOutcome(
            outcome_id=uuid.uuid4(),
            unit_id="u1",
            unit_index=1,
            scenario_id="cpu-saturation",
            run_id=uid,
            model_name="prophet",
            model_version="1.0.0",
            event_time=t0,
            ground_truth_positive=True,
            status=EvaluationOutcomeStatus.SUCCESS,
            calibrated_score=1.5,
            predicted_positive=True,
            is_true_positive=True,
        )

    # Rejects non-success outcome with score present
    with pytest.raises(ValidationError, match="must have calibrated_score=None"):
        EvaluationOutcome(
            outcome_id=uuid.uuid4(),
            unit_id="u1",
            unit_index=1,
            scenario_id="cpu-saturation",
            run_id=uid,
            model_name="prophet",
            model_version="1.0.0",
            event_time=t0,
            ground_truth_positive=True,
            status=EvaluationOutcomeStatus.INSUFFICIENT_DATA,
            calibrated_score=0.8,
            predicted_positive=False,
            is_false_negative=True,
        )


def test_svg_generation_well_formed_and_accessible() -> None:
    counts = ConfusionCounts(
        true_positives=10,
        false_positives=2,
        true_negatives=15,
        false_negatives=3,
        total_units=30,
        evaluable_units=30,
        success_units=25,
        insufficient_units=5,
        failure_units=0,
    )
    summary = ModelComparisonSummary(
        model_name="prophet",
        model_version="1.0.0",
        micro_confusion_counts=counts,
        micro_precision=calculate_precision(10, 2),
        micro_recall=calculate_recall(10, 3),
        micro_f1=calculate_f1(
            precision=calculate_precision(10, 2), recall=calculate_recall(10, 3)
        ),
        micro_false_positive_rate=calculate_false_positive_rate(2, 15),
        macro_precision=calculate_precision(10, 2),
        macro_recall=calculate_recall(10, 3),
        macro_f1=calculate_f1(
            precision=calculate_precision(10, 2), recall=calculate_recall(10, 3)
        ),
        macro_false_positive_rate=calculate_false_positive_rate(2, 15),
        macro_contributing_scenario_count=1,
        latency_summary=LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=1,
            missing_detection_count=0,
            min_seconds=2.0,
            mean_seconds=2.0,
            median_seconds=2.0,
            p95_seconds=2.0,
            max_seconds=2.0,
        ),
        scenario_coverage=ScenarioCoverageSummary(
            requested_count=8,
            requested_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            executed_count=8,
            executed_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            feature_ready_count=8,
            feature_ready_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            scored_count=8,
            scored_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            evaluable_count=8,
            evaluable_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            detected_count=8,
            detected_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            insufficient_count=0,
            insufficient_scenario_ids=[],
            failed_count=0,
            failed_scenario_ids=[],
        ),
        truth_positive_distribution=calculate_score_distribution([0.8, 0.9], "tp"),
        truth_negative_distribution=calculate_score_distribution([0.1, 0.2], "tn"),
        predicted_positive_distribution=calculate_score_distribution([0.8, 0.9], "pp"),
        predicted_negative_distribution=calculate_score_distribution([0.1, 0.2], "pn"),
        scenario_evaluations=[],
    )

    svg_quality = generate_model_quality_svg([summary])
    assert "<svg" in svg_quality
    assert "</svg>" in svg_quality
    assert "<title>" in svg_quality
    assert "<desc>" in svg_quality
    assert "Precision" in svg_quality

    svg_lat = generate_detection_latency_svg([summary], ["cpu-saturation"])
    assert "<svg" in svg_lat
    assert "</svg>" in svg_lat
    assert "<title>" in svg_lat
    assert "<desc>" in svg_lat
    assert "cpu-saturation" in svg_lat


def test_all_eight_scenarios_in_catalogue() -> None:
    cat_ids = scenario_ids()
    assert len(cat_ids) == 8
    for scen_id in CANONICAL_SCENARIO_ORDER:
        assert scen_id in cat_ids
        scen = get_scenario(scen_id)
        assert scen.scenario_id == scen_id


# ======================================================================
# 6. TRUTH-INTERVAL TEST MATRIX
# ======================================================================


def test_truth_interval_valid_recovery_and_no_recovery() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t_start, t_end = compute_truth_interval(
        run_start_time=t0,
        activation_time_seconds=10.0,
        recovery_time_seconds=20.0,
        total_duration_seconds=30.0,
    )
    assert t_start == t0 + timedelta(seconds=10.0)
    assert t_end == t0 + timedelta(seconds=20.0)

    t_start2, t_end2 = compute_truth_interval(
        run_start_time=t0,
        activation_time_seconds=10.0,
        recovery_time_seconds=None,
        total_duration_seconds=30.0,
    )
    assert t_end2 == t0 + timedelta(seconds=30.0)


def test_truth_interval_rejects_naive_run_start() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 10.0, None, 30.0)


@pytest.mark.parametrize(
    "val", [True, False, "10", float("nan"), float("inf"), float("-inf"), -1.0]
)
def test_truth_interval_rejects_invalid_activation_values(val: Any) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, val, None, 30.0)


@pytest.mark.parametrize(
    "val", [True, False, "10", float("nan"), float("inf"), float("-inf"), -1.0, 0.0]
)
def test_truth_interval_rejects_invalid_duration_values(val: Any) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 10.0, None, val)


def test_truth_interval_rejects_activation_at_or_after_duration() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 30.0, None, 30.0)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 35.0, None, 30.0)


@pytest.mark.parametrize(
    "val", [True, False, "10", float("nan"), float("inf"), float("-inf"), -1.0]
)
def test_truth_interval_rejects_invalid_recovery_values(val: Any) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 10.0, val, 30.0)


def test_truth_interval_rejects_recovery_at_or_before_activation() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 10.0, 10.0, 30.0)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 10.0, 5.0, 30.0)


def test_truth_interval_rejects_recovery_beyond_duration() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        compute_truth_interval(t0, 10.0, 35.0, 30.0)


def test_ground_truth_label_rejects_naive_datetimes() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    naive = datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError):
        evaluate_ground_truth_label(naive, t0, t0 + timedelta(seconds=1))
    with pytest.raises(ValueError):
        evaluate_ground_truth_label(t0, naive, t0 + timedelta(seconds=1))
    with pytest.raises(ValueError):
        evaluate_ground_truth_label(t0, t0, naive)


def test_ground_truth_label_rejects_invalid_interval_order() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=1)
    with pytest.raises(ValueError):
        evaluate_ground_truth_label(t0, t1, t0)


# ======================================================================
# 7. METRIC FUNCTION AND METRICRESULT TEST MATRIX
# ======================================================================


@pytest.mark.parametrize(
    "func", [calculate_precision, calculate_recall, calculate_false_positive_rate]
)
@pytest.mark.parametrize("val", [-1, -10])
def test_metric_count_functions_reject_negative_inputs(func: Any, val: Any) -> None:
    with pytest.raises(ValueError):
        func(val, 10)
    with pytest.raises(ValueError):
        func(10, val)


@pytest.mark.parametrize(
    "func", [calculate_precision, calculate_recall, calculate_false_positive_rate]
)
@pytest.mark.parametrize("val", [True, False])
def test_metric_count_functions_reject_boolean_inputs(func: Any, val: Any) -> None:
    with pytest.raises(ValueError):
        func(val, 10)
    with pytest.raises(ValueError):
        func(10, val)


@pytest.mark.parametrize(
    "func", [calculate_precision, calculate_recall, calculate_false_positive_rate]
)
@pytest.mark.parametrize("val", [1.0, 1.5, float("nan"), float("inf")])
def test_metric_count_functions_reject_float_inputs(func: Any, val: Any) -> None:
    with pytest.raises(ValueError):
        func(val, 10)
    with pytest.raises(ValueError):
        func(10, val)


@pytest.mark.parametrize(
    "func", [calculate_precision, calculate_recall, calculate_false_positive_rate]
)
@pytest.mark.parametrize("val", ["1", "10"])
def test_metric_count_functions_reject_string_inputs(func: Any, val: Any) -> None:
    with pytest.raises(ValueError):
        func(val, 10)
    with pytest.raises(ValueError):
        func(10, val)


def test_calculate_f1_supports_positional_and_keyword_calls() -> None:
    p = MetricResult(
        metric_name="precision",
        value=0.8,
        status="defined",
        numerator=8,
        denominator=10,
    )
    r = MetricResult(
        metric_name="recall", value=0.5, status="defined", numerator=5, denominator=10
    )
    # Both positional and keyword calls succeed
    f1_pos = calculate_f1(p, r)
    f1_kw = calculate_f1(precision=p, recall=r)
    expected = 2 * (0.8 * 0.5) / (0.8 + 0.5)
    assert f1_pos.value == pytest.approx(expected)
    assert f1_kw.value == pytest.approx(expected)
    assert f1_pos.value == f1_kw.value


def test_calculate_f1_rejects_swapped_or_incorrect_metric_names() -> None:
    p = MetricResult(
        metric_name="precision",
        value=0.8,
        status="defined",
        numerator=8,
        denominator=10,
    )
    r = MetricResult(
        metric_name="recall", value=0.5, status="defined", numerator=5, denominator=10
    )
    # Swapped objects are rejected
    with pytest.raises(ValueError, match="correctly named precision and recall"):
        calculate_f1(r, p)
    # Incorrectly named precision input is rejected
    wrong_p = MetricResult(
        metric_name="wrong",
        value=0.8,
        status="defined",
        numerator=8,
        denominator=10,
    )
    with pytest.raises(ValueError, match="correctly named precision and recall"):
        calculate_f1(wrong_p, r)
    # Incorrectly named recall input is rejected
    wrong_r = MetricResult(
        metric_name="wrong", value=0.5, status="defined", numerator=5, denominator=10
    )
    with pytest.raises(ValueError, match="correctly named precision and recall"):
        calculate_f1(p, wrong_r)


def test_calculate_f1_preserves_undefined_propagation() -> None:
    p = MetricResult(
        metric_name="precision",
        value=0.8,
        status="defined",
        numerator=8,
        denominator=10,
    )
    p_undef = MetricResult(
        metric_name="precision",
        value=None,
        status="undefined",
        numerator=0,
        denominator=0,
        reason="zero",
    )
    r = MetricResult(
        metric_name="recall", value=0.5, status="defined", numerator=5, denominator=10
    )
    r_undef = MetricResult(
        metric_name="recall",
        value=None,
        status="undefined",
        numerator=0,
        denominator=0,
        reason="zero",
    )

    f1 = calculate_f1(precision=p_undef, recall=r)
    assert f1.value is None
    assert f1.status == "undefined"
    f2 = calculate_f1(precision=p, recall=r_undef)
    assert f2.value is None
    assert f2.status == "undefined"
    f3 = calculate_f1(precision=p_undef, recall=r_undef)
    assert f3.value is None
    assert f3.status == "undefined"


@pytest.mark.parametrize("val", [True, False, "0.5"])
def test_metric_result_rejects_boolean_and_non_numeric_values(val: Any) -> None:
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=val,
            status="defined",
            numerator=1,
            denominator=2,
        )


@pytest.mark.parametrize("val", [float("nan"), float("inf"), float("-inf")])
def test_metric_result_rejects_non_finite_values(val: Any) -> None:
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=val,
            status="defined",
            numerator=1,
            denominator=2,
        )


def test_metric_result_rejects_negative_parts() -> None:
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=0.5,
            status="defined",
            numerator=-1,
            denominator=2,
        )
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=0.5,
            status="defined",
            numerator=1,
            denominator=-2,
        )


def test_metric_result_defined_contract() -> None:
    m = MetricResult(
        metric_name="precision", value=0.5, status="defined", numerator=1, denominator=2
    )
    assert m.value == 0.5
    # Reject value outside [0.0, 1.0]
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=1.5,
            status="defined",
            numerator=1,
            denominator=2,
        )
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=-0.1,
            status="defined",
            numerator=1,
            denominator=2,
        )
    # Reject defined missing numerator
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision", value=0.5, status="defined", denominator=2
        )
    # Reject defined missing denominator
    with pytest.raises(ValidationError):
        MetricResult(metric_name="precision", value=0.5, status="defined", numerator=1)
    # Reject defined zero denominator
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=0.5,
            status="defined",
            numerator=1,
            denominator=0,
        )
    # Reject numerator exceeding denominator
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=0.5,
            status="defined",
            numerator=3,
            denominator=2,
        )
    # Reject defined non-None reason
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision",
            value=0.5,
            status="defined",
            numerator=1,
            denominator=2,
            reason="reason",
        )


def test_metric_result_undefined_contract() -> None:
    m = MetricResult(
        metric_name="precision",
        value=None,
        status="undefined",
        reason="zero denominator",
        numerator=0,
        denominator=0,
    )
    assert m.value is None
    # Reject undefined non-None value
    with pytest.raises(ValidationError):
        MetricResult(
            metric_name="precision", value=0.5, status="undefined", reason="reason"
        )
    # Reject undefined empty reason
    with pytest.raises(ValidationError):
        MetricResult(metric_name="precision", value=None, status="undefined", reason="")
    # Valid undefined zero denominator
    m2 = MetricResult(
        metric_name="precision",
        value=None,
        status="undefined",
        reason="zero",
        numerator=1,
        denominator=0,
    )
    assert m2.status == "undefined"


# ======================================================================
# 8. COMPLETE OUTCOME-STATUS ACCOUNTING TEST
# ======================================================================


def test_every_evaluation_outcome_status_is_preserved_and_counted() -> None:
    outcomes = []
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    for i, status in enumerate(EvaluationOutcomeStatus):
        score = 0.5 if status == EvaluationOutcomeStatus.SUCCESS else None
        outcomes.append(
            EvaluationOutcome(
                outcome_id=uuid.uuid4(),
                unit_id=f"u{i}",
                unit_index=i,
                scenario_id="cpu-saturation",
                run_id=uuid.uuid4(),
                model_name="prophet",
                model_version="1.0",
                event_time=t0,
                ground_truth_positive=True,
                status=status,
                raw_score=score,
                baseline_normalized_score=score,
                calibrated_score=score,
                predicted_positive=True
                if status == EvaluationOutcomeStatus.SUCCESS
                else False,
                is_true_positive=True
                if status == EvaluationOutcomeStatus.SUCCESS
                else False,
                is_false_negative=False
                if status == EvaluationOutcomeStatus.SUCCESS
                else True,
            )
        )

    counts = calculate_confusion_counts(outcomes)

    # Prove every enum member is represented exactly once
    assert len(outcomes) == len(EvaluationOutcomeStatus)
    statuses_present = {o.status for o in outcomes}
    assert len(statuses_present) == len(EvaluationOutcomeStatus)

    # Prove totals
    assert counts.total_units == len(EvaluationOutcomeStatus)
    assert counts.success_units == 1
    assert counts.insufficient_units == 2  # INSUFFICIENT_DATA, NOT_APPLICABLE
    assert (
        counts.failure_units == 5
    )  # MISSING_CALIBRATION, NON_CONVERGENCE, FIT_FAILURE, INVALID_INPUT, MODEL_FAILURE
    assert (
        counts.success_units + counts.insufficient_units + counts.failure_units
        == counts.total_units
    )


# ======================================================================
# 9. DETECTION-LATENCY TEST MATRIX
# ======================================================================


def test_detection_latency_result_valid_detected_and_exact_zero() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    d1 = DetectionLatencyResult(
        scenario_id="s1",
        model_name="m1",
        status="detected",
        latency_seconds=10.0,
        first_true_positive_time=t0 + timedelta(seconds=10),
        truth_activation_time=t0,
    )
    assert d1.latency_seconds == 10.0

    d2 = DetectionLatencyResult(
        scenario_id="s1",
        model_name="m1",
        status="detected",
        latency_seconds=0.0,
        first_true_positive_time=t0,
        truth_activation_time=t0,
    )
    assert d2.latency_seconds == 0.0


def test_detection_latency_result_requires_activation_and_first_tp() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError):
        DetectionLatencyResult(
            scenario_id="s1",
            model_name="m1",
            status="detected",
            latency_seconds=10.0,
            truth_activation_time=t0,
        )
    with pytest.raises(ValidationError):
        DetectionLatencyResult(
            scenario_id="s1",
            model_name="m1",
            status="detected",
            latency_seconds=10.0,
            first_true_positive_time=t0,
        )


def test_detection_latency_result_rejects_tp_before_activation() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError):
        DetectionLatencyResult(
            scenario_id="s1",
            model_name="m1",
            status="detected",
            latency_seconds=10.0,
            first_true_positive_time=t0 - timedelta(seconds=10),
            truth_activation_time=t0,
        )


def test_detection_latency_result_rejects_mismatched_latency() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError):
        DetectionLatencyResult(
            scenario_id="s1",
            model_name="m1",
            status="detected",
            latency_seconds=5.0,
            first_true_positive_time=t0 + timedelta(seconds=10),
            truth_activation_time=t0,
        )


@pytest.mark.parametrize(
    "val", [True, False, "10", float("nan"), float("inf"), float("-inf"), -1.0]
)
def test_detection_latency_result_rejects_invalid_numeric_latency(val: Any) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError):
        DetectionLatencyResult(
            scenario_id="s1",
            model_name="m1",
            status="detected",
            latency_seconds=val,
            first_true_positive_time=t0 + timedelta(seconds=10),
            truth_activation_time=t0,
        )


def test_detection_latency_result_non_detected_contract() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    non_detected_statuses: tuple[
        Literal["not_detected", "insufficient_data", "failed"],
        ...,
    ] = (
        "not_detected",
        "insufficient_data",
        "failed",
    )

    for status in non_detected_statuses:
        # Prohibit latency and first_tp
        with pytest.raises(ValidationError):
            DetectionLatencyResult(
                scenario_id="s1", model_name="m1", status=status, latency_seconds=10.0
            )
        with pytest.raises(ValidationError):
            DetectionLatencyResult(
                scenario_id="s1",
                model_name="m1",
                status=status,
                first_true_positive_time=t0,
            )
        # Activation time is allowed
        res = DetectionLatencyResult(
            scenario_id="s1", model_name="m1", status=status, truth_activation_time=t0
        )
        assert res.truth_activation_time == t0


def test_latency_summary_valid_empty_and_populated() -> None:
    s_empty = LatencySummary(
        total_scenario_count=1,
        contributing_scenario_count=0,
        missing_detection_count=1,
        min_seconds=None,
        mean_seconds=None,
        median_seconds=None,
        p95_seconds=None,
        max_seconds=None,
    )
    assert s_empty.total_scenario_count == 1
    s_pop = LatencySummary(
        total_scenario_count=3,
        contributing_scenario_count=1,
        missing_detection_count=2,
        min_seconds=1.0,
        mean_seconds=1.0,
        median_seconds=1.0,
        p95_seconds=1.0,
        max_seconds=1.0,
    )
    assert s_pop.min_seconds == 1.0


@pytest.mark.parametrize("val", [True, False, 1.0, "1", -1])
def test_latency_summary_rejects_invalid_counts(val: Any) -> None:
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=val,
            contributing_scenario_count=0,
            missing_detection_count=0,
        )
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=val,
            missing_detection_count=0,
        )


def test_latency_summary_rejects_total_mismatch() -> None:
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=10,
            contributing_scenario_count=1,
            missing_detection_count=1,
        )


@pytest.mark.parametrize(
    "val", [True, False, "1", float("nan"), float("inf"), float("-inf"), -1.0]
)
def test_latency_summary_rejects_invalid_statistics(val: Any) -> None:
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=1,
            missing_detection_count=0,
            min_seconds=val,
            mean_seconds=1.0,
            median_seconds=1.0,
            p95_seconds=1.0,
            max_seconds=1.0,
        )


def test_latency_summary_rejects_presence_contract_violations() -> None:
    # Detected > 0 requires stats
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=1,
            missing_detection_count=0,
            min_seconds=None,
            mean_seconds=1.0,
            median_seconds=1.0,
            p95_seconds=1.0,
            max_seconds=1.0,
        )
    # Detected == 0 requires stats to be None
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=0,
            missing_detection_count=1,
            min_seconds=1.0,
            mean_seconds=None,
            median_seconds=None,
            p95_seconds=None,
            max_seconds=None,
        )


def test_latency_summary_rejects_statistic_ordering() -> None:
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=1,
            missing_detection_count=0,
            min_seconds=10.0,
            mean_seconds=10.0,
            median_seconds=5.0,
            p95_seconds=10.0,
            max_seconds=10.0,
        )
    with pytest.raises(ValidationError):
        LatencySummary(
            total_scenario_count=1,
            contributing_scenario_count=1,
            missing_detection_count=0,
            min_seconds=10.0,
            mean_seconds=10.0,
            median_seconds=10.0,
            p95_seconds=20.0,
            max_seconds=15.0,
        )


def test_latency_summary_production_median_and_p95_are_deterministic() -> None:
    from app.anomaly.evaluation import compute_quantile

    lats = [float(i) for i in range(1, 11)]  # 1 to 10
    median = compute_quantile(sorted(lats), 0.50)
    p95 = compute_quantile(sorted(lats), 0.95)
    summ = LatencySummary(
        total_scenario_count=10,
        contributing_scenario_count=10,
        missing_detection_count=0,
        min_seconds=1.0,
        max_seconds=10.0,
        mean_seconds=5.5,
        median_seconds=median,
        p95_seconds=p95,
    )
    assert summ.min_seconds == 1.0
    assert summ.max_seconds == 10.0
    assert summ.median_seconds == 5.5
    import math

    assert math.isclose(summ.p95_seconds or 0.0, 9.55)


# ======================================================================
# 10. SCORE-DISTRIBUTION TEST MATRIX
# ======================================================================


def test_score_distribution_accepts_empty_and_boundary_values() -> None:
    s_empty = ScoreDistributionSummary(distribution_name="d", count=0, status="empty")
    assert s_empty.status == "empty"
    s_bounds = ScoreDistributionSummary(
        distribution_name="d",
        count=1,
        status="defined",
        min=0.0,
        max=1.0,
        mean=0.5,
        median=0.5,
        p25=0.25,
        p75=0.75,
        p95=0.95,
    )
    assert s_bounds.min == 0.0
    assert s_bounds.max == 1.0


@pytest.mark.parametrize("val", [True, False, "0.5"])
def test_score_distribution_rejects_boolean_and_non_numeric_inputs(val: Any) -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=val,
            max=1.0,
            mean=0.5,
            median=0.5,
            p25=0.25,
            p75=0.75,
            p95=0.95,
        )


@pytest.mark.parametrize("val", [float("nan"), float("inf"), float("-inf")])
def test_score_distribution_rejects_non_finite_inputs(val: Any) -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=val,
            max=1.0,
            mean=0.5,
            median=0.5,
            p25=0.25,
            p75=0.75,
            p95=0.95,
        )


@pytest.mark.parametrize("val", [-0.1, 1.1])
def test_score_distribution_rejects_out_of_bounds_inputs(val: Any) -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=val,
            max=1.0,
            mean=0.5,
            median=0.5,
            p25=0.25,
            p75=0.75,
            p95=0.95,
        )


@pytest.mark.parametrize("val", [True, False, 1.0, "1", -1])
def test_score_distribution_summary_rejects_invalid_count(val: Any) -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(distribution_name="d", count=val, status="empty")


@pytest.mark.parametrize("val", [float("nan"), float("inf"), float("-inf")])
def test_score_distribution_summary_rejects_non_finite_statistics(val: Any) -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=0.0,
            max=1.0,
            mean=0.5,
            median=val,
            p25=0.25,
            p75=0.75,
            p95=0.95,
        )


@pytest.mark.parametrize("val", [-0.1, 1.1])
def test_score_distribution_summary_rejects_out_of_bounds_statistics(val: Any) -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=0.0,
            max=1.0,
            mean=0.5,
            median=val,
            p25=0.25,
            p75=0.75,
            p95=0.95,
        )


def test_score_distribution_summary_rejects_percentile_ordering() -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=0.0,
            max=1.0,
            mean=0.5,
            median=0.5,
            p25=0.8,
            p75=0.75,
            p95=0.95,
        )


def test_score_distribution_summary_rejects_mean_outside_range() -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=0.1,
            max=0.9,
            mean=0.05,
            median=0.5,
            p25=0.25,
            p75=0.75,
            p95=0.85,
        )
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=1,
            status="defined",
            min=0.1,
            max=0.9,
            mean=0.95,
            median=0.5,
            p25=0.25,
            p75=0.75,
            p95=0.85,
        )


def test_score_distribution_summary_enforces_empty_contract() -> None:
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d",
            count=0,
            status="defined",
            min=0.0,
            max=1.0,
            mean=0.5,
            median=0.5,
            p25=0.25,
            p75=0.75,
            p95=0.95,
        )
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(distribution_name="d", count=1, status="empty")
    with pytest.raises(ValidationError):
        ScoreDistributionSummary(
            distribution_name="d", count=0, status="empty", min=0.0
        )


# ======================================================================
# 11. CANONICAL SCENARIO-COVERAGE TEST MATRIX
# ======================================================================


def _canonical_coverage(**kwargs: Any) -> ScenarioCoverageSummary:
    base = dict(
        requested_count=8,
        executed_count=8,
        feature_ready_count=8,
        scored_count=8,
        evaluable_count=8,
        detected_count=0,
        insufficient_count=0,
        failed_count=0,
        requested_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
        executed_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
        feature_ready_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
        scored_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
        evaluable_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
        detected_scenario_ids=[],
        insufficient_scenario_ids=[],
        failed_scenario_ids=[],
    )
    base.update(kwargs)
    return ScenarioCoverageSummary(**base)  # type: ignore


def test_scenario_coverage_accepts_valid_canonical_summary() -> None:
    cov = _canonical_coverage()
    assert cov.requested_count == 8


@pytest.mark.parametrize("val", [True, False, 1.0, "1", -1])
@pytest.mark.parametrize(
    "field",
    [
        "requested_count",
        "executed_count",
        "feature_ready_count",
        "scored_count",
        "evaluable_count",
        "detected_count",
        "insufficient_count",
        "failed_count",
    ],
)
def test_scenario_coverage_rejects_invalid_count_types(val: Any, field: str) -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(**{field: val})


def test_scenario_coverage_requires_exact_requested_count() -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(requested_count=7)


def test_scenario_coverage_requires_exact_requested_ids() -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(requested_scenario_ids=["cpu-saturation"])


def test_scenario_coverage_rejects_unknown_ids() -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(
            executed_scenario_ids=list(CANONICAL_SCENARIO_ORDER)[:-1] + ["unknown"]
        )


def test_scenario_coverage_rejects_duplicate_ids() -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(executed_scenario_ids=["cpu-saturation"] * 8)


def test_scenario_coverage_rejects_noncanonical_order() -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(executed_scenario_ids=list(CANONICAL_SCENARIO_ORDER)[::-1])


@pytest.mark.parametrize(
    "prefix",
    [
        "executed",
        "feature_ready",
        "scored",
        "evaluable",
        "detected",
        "insufficient",
        "failed",
    ],
)
def test_scenario_coverage_rejects_count_list_mismatch(prefix: str) -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(
            **{f"{prefix}_count": 0, f"{prefix}_scenario_ids": ["cpu-saturation"]}
        )
    with pytest.raises(ValidationError):
        _canonical_coverage(**{f"{prefix}_count": 1, f"{prefix}_scenario_ids": []})


def test_scenario_coverage_rejects_invalid_subset_relationships() -> None:
    with pytest.raises(ValidationError):
        _canonical_coverage(
            executed_scenario_ids=[],
            executed_count=0,
            # but evaluable is 8
        )


def test_scenario_coverage_allows_semantically_valid_overlap() -> None:
    # Insufficient and Failed can overlap with Detected or others if they want, but usually subset
    cov = _canonical_coverage(
        detected_scenario_ids=["cpu-saturation"],
        detected_count=1,
        insufficient_scenario_ids=["cpu-saturation"],
        insufficient_count=1,
    )
    assert "cpu-saturation" in cov.detected_scenario_ids


# ======================================================================
# 12. FINITE-SERIALIZATION GUARD TEST MATRIX
# ======================================================================


def test_finite_serialization_guard_accepts_nested_finite_payloads() -> None:
    data = {"a": [1.0, 2.0], "b": {"c": (3.0, "str", 4)}}
    _guard_finite_serialization(data)


def test_finite_serialization_guard_accepts_model_dump() -> None:
    m = MetricResult(
        metric_name="precision", value=0.5, status="defined", numerator=1, denominator=2
    )
    _guard_finite_serialization(m)


def test_finite_serialization_guard_rejects_nested_nan() -> None:
    data = {"a": [1.0, float("nan")]}
    with pytest.raises(EvaluationArtifactError):
        _guard_finite_serialization(data)


def test_finite_serialization_guard_rejects_nested_infinities() -> None:
    data = {"a": [1.0, float("inf")]}
    with pytest.raises(EvaluationArtifactError):
        _guard_finite_serialization(data)


def test_finite_serialization_guard_rejects_non_finite_outcome_details() -> None:
    class FakeOutcome:
        def model_dump(self):
            return {"score": float("-inf")}

    with pytest.raises(EvaluationArtifactError):
        _guard_finite_serialization(FakeOutcome())


def test_finite_serialization_guard_bounds_long_diagnostic_paths() -> None:
    inner = {"x": float("nan")}
    data = inner
    for _ in range(200):
        data = {"x": data}  # type: ignore
    with pytest.raises(EvaluationArtifactError) as exc:
        _guard_finite_serialization(data)
    assert len(str(exc.value)) < 250
    assert "..." in str(exc.value)


def test_finite_serialization_guard_does_not_substitute_values() -> None:
    data = {"a": 1.0}
    _guard_finite_serialization(data)
    assert data["a"] == 1.0


@pytest.mark.asyncio
async def test_phase3_writer_guards_all_required_json_outputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from app.anomaly.evaluation import (
        Phase3EvaluationHarness,
        run_phase3_evaluation,
        ModelComparisonSummary,
        LatencySummary,
        EvaluationOutcome,
        EvaluationOutcomeStatus,
        ConfusionCounts,
        MetricResult,
        ScenarioCoverageSummary,
        ScoreDistributionSummary,
        EvaluationHarnessConfig,
        EvaluationPackageManifest,
    )
    import uuid
    from datetime import datetime, timezone

    guard_calls: list[str] = []
    orig_guard = app.anomaly.evaluation._guard_finite_serialization

    def mock_guard(data, path="$"):
        # Classify payload by type/shape to distinguish all four writer sites
        if isinstance(data, EvaluationHarnessConfig):
            guard_calls.append("comparison_manifest.json")
        elif isinstance(data, EvaluationOutcome):
            guard_calls.append("evaluation_outcomes.jsonl")
        elif isinstance(data, EvaluationPackageManifest):
            guard_calls.append("artifact_manifest.json")
        elif isinstance(data, dict) and {
            "schema_version",
            "package_id",
            "package_version",
            "code_revision",
            "created_at",
            "decision_threshold",
            "models",
            "metadata",
        }.issubset(data):
            guard_calls.append("model_comparison.json")
        orig_guard(data, path)

    monkeypatch.setattr(
        app.anomaly.evaluation, "_guard_finite_serialization", mock_guard
    )

    cfg = create_default_evaluation_harness_config()
    cfg = cfg.model_copy(
        update={
            "artifact_root": "test_artifacts",
            "scenarios": ["cpu-saturation"],
            "models": ["prophet"],
        }
    )

    outcomes = [
        EvaluationOutcome(
            outcome_id=uuid.uuid4(),
            unit_id="u",
            unit_index=0,
            scenario_id="cpu-saturation",
            run_id=uuid.uuid4(),
            model_name="prophet",
            model_version="1.0",
            event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ground_truth_positive=True,
            status=EvaluationOutcomeStatus.SUCCESS,
            raw_score=0.5,
            baseline_normalized_score=0.5,
            calibrated_score=0.5,
            predicted_positive=True,
            is_true_positive=True,
        )
    ]

    m_sum = ModelComparisonSummary(
        model_name="prophet",
        model_version="1.0",
        micro_confusion_counts=ConfusionCounts(
            true_positives=0,
            false_positives=0,
            true_negatives=0,
            false_negatives=0,
            total_units=0,
            evaluable_units=0,
            success_units=0,
            insufficient_units=0,
            failure_units=0,
        ),
        micro_precision=MetricResult(
            metric_name="precision",
            value=1.0,
            status="defined",
            numerator=1.0,
            denominator=1.0,
        ),
        micro_recall=MetricResult(
            metric_name="recall",
            value=1.0,
            status="defined",
            numerator=1.0,
            denominator=1.0,
        ),
        micro_f1=MetricResult(
            metric_name="f1",
            value=1.0,
            status="defined",
            numerator=1.0,
            denominator=1.0,
        ),
        micro_false_positive_rate=MetricResult(
            metric_name="fpr",
            value=0.0,
            status="defined",
            numerator=0.0,
            denominator=1.0,
        ),
        macro_precision=MetricResult(
            metric_name="precision",
            value=1.0,
            status="defined",
            numerator=1.0,
            denominator=1.0,
        ),
        macro_recall=MetricResult(
            metric_name="recall",
            value=1.0,
            status="defined",
            numerator=1.0,
            denominator=1.0,
        ),
        macro_f1=MetricResult(
            metric_name="f1",
            value=1.0,
            status="defined",
            numerator=1.0,
            denominator=1.0,
        ),
        macro_false_positive_rate=MetricResult(
            metric_name="fpr",
            value=0.0,
            status="defined",
            numerator=0.0,
            denominator=1.0,
        ),
        macro_contributing_scenario_count=0,
        latency_summary=LatencySummary(
            total_scenario_count=0,
            contributing_scenario_count=0,
            missing_detection_count=0,
        ),
        scenario_coverage=ScenarioCoverageSummary(
            requested_count=8,
            requested_scenario_ids=list(
                app.anomaly.evaluation.CANONICAL_SCENARIO_ORDER
            ),
            executed_count=0,
            executed_scenario_ids=[],
            feature_ready_count=0,
            feature_ready_scenario_ids=[],
            scored_count=0,
            scored_scenario_ids=[],
            evaluable_count=0,
            evaluable_scenario_ids=[],
            detected_count=0,
            detected_scenario_ids=[],
            insufficient_count=0,
            insufficient_scenario_ids=[],
            failed_count=0,
            failed_scenario_ids=[],
        ),
        truth_positive_distribution=ScoreDistributionSummary(
            distribution_name="a", count=0, status="empty"
        ),
        truth_negative_distribution=ScoreDistributionSummary(
            distribution_name="a", count=0, status="empty"
        ),
        predicted_positive_distribution=ScoreDistributionSummary(
            distribution_name="a", count=0, status="empty"
        ),
        predicted_negative_distribution=ScoreDistributionSummary(
            distribution_name="a", count=0, status="empty"
        ),
        scenario_evaluations=[],
    )

    _, _, metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return (
            outcomes,
            [m_sum],
            metadata,
        )

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    await run_phase3_evaluation(cfg, base_dir=tmp_path)

    # The test fixture creates exactly one outcome, so we expect:
    # 1 comparison_manifest.json (from config guard)
    # 1 evaluation_outcomes.jsonl (from the single outcome)
    # 1 model_comparison.json (from the processed comparison dict)
    # 1 artifact_manifest.json (from the final manifest)
    assert guard_calls == [
        "comparison_manifest.json",
        "evaluation_outcomes.jsonl",
        "model_comparison.json",
        "artifact_manifest.json",
    ]


# ======================================================================
# 7. TASK 3.9-C3 PACKAGE INTEGRITY & WRITER CONTRACT TESTS
# ======================================================================


def test_package_manifest_self_reference_and_exclusion_contract() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    valid_entries = _build_valid_manifest_artifacts("research")

    # Valid manifest with default artifact_manifest_path and self_digest_policy
    manifest = EvaluationPackageManifest(
        schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
        package_id="pkg",
        package_version="1.0.0",
        code_revision="rev123",
        created_at=t0,
        environment="simulation",
        calibration_seed=41,
        evaluation_seed=42,
        decision_threshold=0.5,
        models=list(CANONICAL_MODEL_ORDER),
        scenarios=list(CANONICAL_SCENARIO_ORDER),
        artifact_manifest_path="research/results/processed/phase3/task_3_9/artifact_manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=valid_entries,
    )
    assert (
        manifest.artifact_manifest_path
        == "research/results/processed/phase3/task_3_9/artifact_manifest.json"
    )
    assert manifest.self_digest_policy == "excluded_from_manifest_digest"
    assert len(manifest.artifacts) == 11

    # Self-digest rejection: artifact_manifest_path inside artifacts list
    self_entry = ArtifactManifestEntry(
        path="research/results/processed/phase3/task_3_9/artifact_manifest.json",
        media_type="application/json",
        schema_version="1.0",
        sha256="c" * 64,
    )
    with pytest.raises(ValidationError, match="Self-digest violation"):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifact_manifest_path="research/results/processed/phase3/task_3_9/artifact_manifest.json",
            artifacts=[*valid_entries[:-1], self_entry],
        )

    # Duplicate artifact path rejection
    with pytest.raises(ValidationError, match="Duplicate artifact paths"):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifacts=[*valid_entries[:-1], valid_entries[0]],
        )

    # Invalid artifact_manifest_path rejection (traversal / absolute)
    with pytest.raises(ValidationError):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifact_manifest_path="/absolute/path/manifest.json",
            artifacts=valid_entries,
        )


def test_manifest_topology_rejects_missing_or_unexpected_entry() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    valid_entries = _build_valid_manifest_artifacts("research")

    # Missing entry (10 entries instead of 11)
    with pytest.raises(ValidationError, match="requires exactly 11 digested artifacts"):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifacts=valid_entries[:-1],
        )

    # Unexpected entry replacing a valid entry
    unexpected_entry = ArtifactManifestEntry(
        path="research/results/extra/unexpected.json",
        media_type="application/json",
        schema_version="1.0",
        sha256="d" * 64,
    )
    with pytest.raises(ValidationError, match="Missing required artifact"):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifacts=[*valid_entries[:-1], unexpected_entry],
        )


def test_manifest_topology_rejects_wrong_media_type_or_record_count() -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    valid_entries = _build_valid_manifest_artifacts("research")

    # Wrong media type on SVG
    bad_svg_entry = ArtifactManifestEntry(
        path="research/results/figures/phase3/task_3_9/model_quality_metrics.svg",
        media_type="image/png",
        schema_version="1.0",
        sha256="e" * 64,
    )
    replaced_entries = [
        e if "model_quality_metrics.svg" not in e.path else bad_svg_entry
        for e in valid_entries
    ]
    with pytest.raises(ValidationError, match="Invalid media_type"):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifacts=replaced_entries,
        )

    # Missing record_count on CSV
    bad_csv_entry = ArtifactManifestEntry(
        path="research/results/processed/phase3/task_3_9/model_comparison.csv",
        media_type="text/csv",
        schema_version="1.0",
        record_count=None,
        sha256="f" * 64,
    )
    replaced_csv_entries = [
        e if "model_comparison.csv" not in e.path else bad_csv_entry
        for e in valid_entries
    ]
    with pytest.raises(
        ValidationError, match="requires a non-negative integer record_count"
    ):
        EvaluationPackageManifest(
            schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
            package_id="pkg",
            package_version="1.0.0",
            code_revision="rev123",
            created_at=t0,
            environment="simulation",
            calibration_seed=41,
            evaluation_seed=42,
            decision_threshold=0.5,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifacts=replaced_csv_entries,
        )


def _build_mock_evaluation_data(
    package_id: str = "phase3-task-3-9-comparison",
    package_version: str = "1.0.0",
    calibration_seed: int = 41,
    evaluation_seed: int = 42,
) -> tuple[
    list[EvaluationOutcome],
    list[ModelComparisonSummary],
    EvaluationRunMetadata,
]:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    outcomes: list[EvaluationOutcome] = []
    summaries: list[ModelComparisonSummary] = []

    # Build 2 units across 8 canonical scenarios for all 3 models
    for s_idx, scen_id in enumerate(CANONICAL_SCENARIO_ORDER):
        scen_eval_run_id = derive_run_id(
            package_id=package_id,
            package_version=package_version,
            partition_role="evaluation",
            scenario_id=scen_id,
            seed=evaluation_seed,
        )
        for unit_idx in range(2):
            unit_id = f"{scen_id}:unit:{unit_idx}"
            gt_pos = unit_idx == 1  # 1 negative, 1 positive
            for mod_name in CANONICAL_MODEL_ORDER:
                is_tp = gt_pos and (mod_name != "autoencoder" or s_idx > 0)
                is_fn = gt_pos and not is_tp
                is_fp = False
                is_tn = not gt_pos
                pred_pos = is_tp or is_fp

                status = (
                    EvaluationOutcomeStatus.NON_CONVERGENCE
                    if (mod_name == "autoencoder" and s_idx == 0 and gt_pos)
                    else EvaluationOutcomeStatus.SUCCESS
                )
                score = (
                    0.85
                    if pred_pos
                    else (0.15 if status == EvaluationOutcomeStatus.SUCCESS else None)
                )

                out = EvaluationOutcome(
                    outcome_id=uuid.uuid4(),
                    unit_id=unit_id,
                    unit_index=unit_idx,
                    scenario_id=scen_id,
                    run_id=scen_eval_run_id,
                    model_name=mod_name,
                    model_version="1.0.0",
                    event_time=t0 + timedelta(seconds=unit_idx * 10),
                    ground_truth_positive=gt_pos,
                    status=status,
                    raw_score=score,
                    baseline_normalized_score=score,
                    calibrated_score=score,
                    predicted_positive=pred_pos,
                    is_true_positive=is_tp,
                    is_false_positive=is_fp,
                    is_true_negative=is_tn,
                    is_false_negative=is_fn,
                )
                outcomes.append(out)

    for mod_name in CANONICAL_MODEL_ORDER:
        mod_outs = [o for o in outcomes if o.model_name == mod_name]
        micro_c = calculate_confusion_counts(mod_outs)
        scen_evals: list[ScenarioModelEvaluation] = []
        for scen_id in CANONICAL_SCENARIO_ORDER:
            s_outs = [o for o in mod_outs if o.scenario_id == scen_id]
            sc_counts = calculate_confusion_counts(s_outs)
            tp = sc_counts.true_positives
            fp = sc_counts.false_positives
            tn = sc_counts.true_negatives
            fn = sc_counts.false_negatives

            detected = tp > 0
            lat_res = DetectionLatencyResult(
                scenario_id=scen_id,
                model_name=mod_name,
                status="detected" if detected else "not_detected",
                latency_seconds=5.0 if detected else None,
                first_true_positive_time=t0 + timedelta(seconds=10)
                if detected
                else None,
                truth_activation_time=t0 + timedelta(seconds=5) if detected else None,
            )

            p_res = calculate_precision(tp, fp)
            r_res = calculate_recall(tp, fn)
            f1_res = calculate_f1(p_res, r_res)
            fpr_res = calculate_false_positive_rate(fp, tn)

            pos_scores = [
                o.calibrated_score
                for o in s_outs
                if o.ground_truth_positive and o.calibrated_score is not None
            ]
            neg_scores = [
                o.calibrated_score
                for o in s_outs
                if not o.ground_truth_positive and o.calibrated_score is not None
            ]

            scen_evals.append(
                ScenarioModelEvaluation(
                    scenario_id=scen_id,
                    model_name=mod_name,
                    model_version="1.0.0",
                    confusion_counts=sc_counts,
                    precision=p_res,
                    recall=r_res,
                    f1=f1_res,
                    false_positive_rate=fpr_res,
                    detection_latency=lat_res,
                    truth_positive_distribution=calculate_score_distribution(
                        pos_scores, "tp"
                    ),
                    truth_negative_distribution=calculate_score_distribution(
                        neg_scores, "tn"
                    ),
                    predicted_positive_distribution=calculate_score_distribution(
                        pos_scores, "pp"
                    ),
                    predicted_negative_distribution=calculate_score_distribution(
                        neg_scores, "pn"
                    ),
                    status="success",
                )
            )

        det_count = sum(
            1 for e in scen_evals if e.detection_latency.status == "detected"
        )
        lat_sum = LatencySummary(
            total_scenario_count=len(scen_evals),
            contributing_scenario_count=det_count,
            missing_detection_count=len(scen_evals) - det_count,
            min_seconds=5.0 if det_count > 0 else None,
            mean_seconds=5.0 if det_count > 0 else None,
            median_seconds=5.0 if det_count > 0 else None,
            p95_seconds=5.0 if det_count > 0 else None,
            max_seconds=5.0 if det_count > 0 else None,
        )

        cov = ScenarioCoverageSummary(
            requested_count=8,
            requested_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            executed_count=8,
            executed_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            feature_ready_count=8,
            feature_ready_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            scored_count=8,
            scored_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            evaluable_count=8,
            evaluable_scenario_ids=list(CANONICAL_SCENARIO_ORDER),
            detected_count=det_count,
            detected_scenario_ids=[
                e.scenario_id
                for e in scen_evals
                if e.detection_latency.status == "detected"
            ],
            insufficient_count=0,
            insufficient_scenario_ids=[],
            failed_count=8 - det_count if mod_name == "autoencoder" else 0,
            failed_scenario_ids=[
                e.scenario_id
                for e in scen_evals
                if e.detection_latency.status != "detected"
            ]
            if mod_name == "autoencoder"
            else [],
        )

        all_pos = [
            o.calibrated_score
            for o in mod_outs
            if o.ground_truth_positive and o.calibrated_score is not None
        ]
        all_neg = [
            o.calibrated_score
            for o in mod_outs
            if not o.ground_truth_positive and o.calibrated_score is not None
        ]

        p_micro = calculate_precision(micro_c.true_positives, micro_c.false_positives)
        r_micro = calculate_recall(micro_c.true_positives, micro_c.false_negatives)
        f1_micro = calculate_f1(p_micro, r_micro)
        fpr_micro = calculate_false_positive_rate(
            micro_c.false_positives, micro_c.true_negatives
        )

        summaries.append(
            ModelComparisonSummary(
                model_name=mod_name,
                model_version="1.0.0",
                micro_confusion_counts=micro_c,
                micro_precision=p_micro,
                micro_recall=r_micro,
                micro_f1=f1_micro,
                micro_false_positive_rate=fpr_micro,
                macro_precision=p_micro,
                macro_recall=r_micro,
                macro_f1=f1_micro,
                macro_false_positive_rate=fpr_micro,
                macro_contributing_scenario_count=8,
                latency_summary=lat_sum,
                scenario_coverage=cov,
                truth_positive_distribution=calculate_score_distribution(all_pos, "tp"),
                truth_negative_distribution=calculate_score_distribution(all_neg, "tn"),
                predicted_positive_distribution=calculate_score_distribution(
                    all_pos, "pp"
                ),
                predicted_negative_distribution=calculate_score_distribution(
                    all_neg, "pn"
                ),
                scenario_evaluations=scen_evals,
            )
        )

    cal_runs = tuple(
        ScenarioRunLineage(
            scenario_id=sid,
            run_id=derive_run_id(
                package_id=package_id,
                package_version=package_version,
                partition_role="calibration",
                scenario_id=sid,
                seed=calibration_seed,
            ),
            seed=calibration_seed,
        )
        for sid in CANONICAL_SCENARIO_ORDER
    )
    eval_runs = tuple(
        ScenarioRunLineage(
            scenario_id=sid,
            run_id=derive_run_id(
                package_id=package_id,
                package_version=package_version,
                partition_role="evaluation",
                scenario_id=sid,
                seed=evaluation_seed,
            ),
            seed=evaluation_seed,
        )
        for sid in CANONICAL_SCENARIO_ORDER
    )
    metadata = EvaluationRunMetadata(
        calibration_cutoff=datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
        evaluation_start=datetime(2026, 1, 1, 0, 6, 0, tzinfo=timezone.utc),
        calibration_runs=cal_runs,
        evaluation_runs=eval_runs,
    )

    return outcomes, summaries, metadata


@pytest.mark.asyncio
async def test_package_generation_topology_default_and_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    # 1. Default root ("research")
    cfg_default = create_default_evaluation_harness_config(artifact_root="research")
    manifest_def, _ = await run_phase3_evaluation(cfg_default, base_dir=tmp_path)

    expected_relative_paths = [
        "research/experiments/phase3/task_3_9/prophet_experiment.json",
        "research/experiments/phase3/task_3_9/isolation_forest_experiment.json",
        "research/experiments/phase3/task_3_9/autoencoder_experiment.json",
        "research/experiments/phase3/task_3_9/comparison_manifest.json",
        "research/results/raw/phase3/task_3_9/evaluation_outcomes.jsonl",
        "research/results/processed/phase3/task_3_9/model_comparison.json",
        "research/results/processed/phase3/task_3_9/model_comparison.csv",
        "research/results/processed/phase3/task_3_9/scenario_comparison.csv",
        "research/results/figures/phase3/task_3_9/model_quality_metrics.svg",
        "research/results/figures/phase3/task_3_9/detection_latency_by_scenario.svg",
        "research/reports/phase3/task_3_9/evaluation_report.md",
        "research/results/processed/phase3/task_3_9/artifact_manifest.json",
    ]

    for rel in expected_relative_paths:
        full = tmp_path / rel
        assert full.exists(), f"Expected artifact missing: {rel}"
        assert full.stat().st_size > 0

    assert (
        manifest_def.artifact_manifest_path
        == "research/results/processed/phase3/task_3_9/artifact_manifest.json"
    )

    # 2. Overridden root ("custom_artifacts")
    cfg_override = create_default_evaluation_harness_config(
        artifact_root="custom_artifacts"
    )
    manifest_ovr, _ = await run_phase3_evaluation(cfg_override, base_dir=tmp_path)

    for rel in expected_relative_paths:
        ovr_rel = rel.replace("research/", "custom_artifacts/")
        full = tmp_path / ovr_rel
        assert full.exists(), f"Expected override artifact missing: {ovr_rel}"

    assert (
        manifest_ovr.artifact_manifest_path
        == "custom_artifacts/results/processed/phase3/task_3_9/artifact_manifest.json"
    )


@pytest.mark.asyncio
async def test_package_manifest_integrity_and_digest_verification(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config(artifact_root="research")
    returned_manifest, _ = await run_phase3_evaluation(cfg, base_dir=tmp_path)

    # Read artifact_manifest.json from disk
    manifest_file = (
        tmp_path
        / "research"
        / "results"
        / "processed"
        / "phase3"
        / "task_3_9"
        / "artifact_manifest.json"
    )
    assert manifest_file.exists()
    disk_manifest = EvaluationPackageManifest.model_validate_json(
        manifest_file.read_text(encoding="utf-8")
    )

    # Manifest must digest exactly 11 artifacts (all non-manifest artifacts)
    assert len(disk_manifest.artifacts) == 11
    assert disk_manifest.self_digest_policy == "excluded_from_manifest_digest"
    assert (
        disk_manifest.artifact_manifest_path
        == "research/results/processed/phase3/task_3_9/artifact_manifest.json"
    )

    # Verify SHA-256 for all 11 digested files matches actual disk bytes
    for entry in disk_manifest.artifacts:
        target_path = tmp_path / entry.path
        assert (
            target_path.exists()
        ), f"Manifest lists non-existent artifact: {entry.path}"
        assert not entry.path.startswith(("/", "\\", "C:", "D:"))
        assert ".." not in entry.path.split("/")

        actual_sha = hashlib.sha256(target_path.read_bytes()).hexdigest()
        assert (
            entry.sha256 == actual_sha
        ), f"SHA-256 mismatch for {entry.path}: expected {entry.sha256}, got {actual_sha}"

    # Self-path is NOT inside the digested artifacts list
    digested_paths = [a.path for a in disk_manifest.artifacts]
    assert disk_manifest.artifact_manifest_path not in digested_paths

    # Verify external digest of artifact_manifest.json can be computed
    ext_sha = hashlib.sha256(manifest_file.read_bytes()).hexdigest()
    assert len(ext_sha) == 64


@pytest.mark.asyncio
async def test_deterministic_repeated_generation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    fixed_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = create_default_evaluation_harness_config(code_revision="deterministic-rev-1")

    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    await run_phase3_evaluation(cfg, base_dir=dir1, package_created_at=fixed_time)
    await run_phase3_evaluation(cfg, base_dir=dir2, package_created_at=fixed_time)

    # Compare all 12 files byte-for-byte between run1 and run2
    rel_paths = [
        "research/experiments/phase3/task_3_9/prophet_experiment.json",
        "research/experiments/phase3/task_3_9/isolation_forest_experiment.json",
        "research/experiments/phase3/task_3_9/autoencoder_experiment.json",
        "research/experiments/phase3/task_3_9/comparison_manifest.json",
        "research/results/raw/phase3/task_3_9/evaluation_outcomes.jsonl",
        "research/results/processed/phase3/task_3_9/model_comparison.json",
        "research/results/processed/phase3/task_3_9/model_comparison.csv",
        "research/results/processed/phase3/task_3_9/scenario_comparison.csv",
        "research/results/figures/phase3/task_3_9/model_quality_metrics.svg",
        "research/results/figures/phase3/task_3_9/detection_latency_by_scenario.svg",
        "research/reports/phase3/task_3_9/evaluation_report.md",
        "research/results/processed/phase3/task_3_9/artifact_manifest.json",
    ]

    for rel in rel_paths:
        f1 = dir1 / rel
        f2 = dir2 / rel
        assert f1.exists() and f2.exists(), f"File missing in one of runs: {rel}"
        assert f1.read_bytes() == f2.read_bytes(), f"Non-deterministic output in {rel}"


def _build_boundary_evaluation_data() -> (
    tuple[
        list[EvaluationOutcome],
        list[ModelComparisonSummary],
        EvaluationRunMetadata,
    ]
):
    base_outcomes, _, metadata = _build_mock_evaluation_data()
    config = create_default_evaluation_harness_config()
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    first_four = set(CANONICAL_SCENARIO_ORDER[:4])

    transformed_outcomes: list[EvaluationOutcome] = []
    for o in base_outcomes:
        if o.model_name == "isolation_forest":
            if o.ground_truth_positive:
                pred_pos = False
                is_tp = False
                is_fp = False
                is_tn = False
                is_fn = True
                score = 0.15
            else:
                if o.scenario_id in first_four:
                    pred_pos = True
                    is_tp = False
                    is_fp = True
                    is_tn = False
                    is_fn = False
                    score = 0.85
                else:
                    pred_pos = False
                    is_tp = False
                    is_fp = False
                    is_tn = True
                    is_fn = False
                    score = 0.15

            new_dict = o.model_dump()
            new_dict.update(
                {
                    "predicted_positive": pred_pos,
                    "is_true_positive": is_tp,
                    "is_false_positive": is_fp,
                    "is_true_negative": is_tn,
                    "is_false_negative": is_fn,
                    "status": EvaluationOutcomeStatus.SUCCESS,
                    "raw_score": score,
                    "baseline_normalized_score": score,
                    "calibrated_score": score,
                }
            )
            transformed_outcomes.append(EvaluationOutcome.model_validate(new_dict))
        else:
            transformed_outcomes.append(o)

    harness = Phase3EvaluationHarness(config=config)
    summaries: list[ModelComparisonSummary] = []
    for mod_name in CANONICAL_MODEL_ORDER:
        mod_outs = [o for o in transformed_outcomes if o.model_name == mod_name]
        scen_evals: list[ScenarioModelEvaluation] = []
        for scen_id in CANONICAL_SCENARIO_ORDER:
            sm_outs = [o for o in mod_outs if o.scenario_id == scen_id]
            se = harness._compute_scenario_model_evaluation(
                scenario_id=scen_id,
                model_name=mod_name,
                model_version="1.0.0",
                outcomes=sm_outs,
                truth_activation_time=t0 + timedelta(seconds=5),
            )
            scen_evals.append(se)
        summary = harness._compute_model_comparison_summary(
            model_name=mod_name,
            outcomes=mod_outs,
            scenario_evaluations=scen_evals,
        )
        summaries.append(summary)

    return transformed_outcomes, summaries, metadata


@pytest.mark.asyncio
async def test_strict_json_serialization_rejects_non_finite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    await run_phase3_evaluation(cfg, base_dir=tmp_path)

    def parse_strict_constant(c: str) -> None:
        raise ValueError(f"Strict parsing rejection of non-finite constant: {c}")

    json_files = [
        "research/experiments/phase3/task_3_9/prophet_experiment.json",
        "research/experiments/phase3/task_3_9/isolation_forest_experiment.json",
        "research/experiments/phase3/task_3_9/autoencoder_experiment.json",
        "research/experiments/phase3/task_3_9/comparison_manifest.json",
        "research/results/processed/phase3/task_3_9/model_comparison.json",
        "research/results/processed/phase3/task_3_9/artifact_manifest.json",
    ]

    for rel in json_files:
        p = tmp_path / rel
        assert p.exists()
        raw_bytes = p.read_bytes()
        assert len(raw_bytes) > 0
        text = raw_bytes.decode("utf-8")
        data = json.loads(text, parse_constant=parse_strict_constant)
        assert isinstance(data, dict)
        assert len(data) > 0

    raw_jsonl_path = (
        tmp_path / "research/results/raw/phase3/task_3_9/evaluation_outcomes.jsonl"
    )
    assert raw_jsonl_path.exists()
    raw_jsonl_bytes = raw_jsonl_path.read_bytes()
    assert len(raw_jsonl_bytes) > 0
    raw_jsonl_text = raw_jsonl_bytes.decode("utf-8")
    lines = [line.strip() for line in raw_jsonl_text.splitlines() if line.strip()]
    assert len(lines) == len(outcomes)
    assert len(lines) > 0
    for line in lines:
        rec = json.loads(line, parse_constant=parse_strict_constant)
        assert isinstance(rec, dict)
        assert len(rec) > 0

    for literal in ("NaN", "Infinity", "-Infinity"):
        probe_json = f'{{"val": {literal}}}'
        with pytest.raises(
            ValueError, match="Strict parsing rejection of non-finite constant"
        ):
            json.loads(probe_json, parse_constant=parse_strict_constant)


@pytest.mark.asyncio
async def test_csv_headers_canonical_order_and_undefined_representation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_boundary_evaluation_data()

    logical_units = {
        (o.scenario_id, o.run_id, o.unit_id, o.unit_index) for o in outcomes
    }
    assert len(logical_units) == 16
    logical_slots = {
        (o.scenario_id, o.run_id, o.unit_id, o.unit_index, o.model_name)
        for o in outcomes
    }
    assert len(logical_slots) == 48
    assert len(outcomes) == 48

    for u_key in logical_units:
        u_outcomes = [
            o
            for o in outcomes
            if (o.scenario_id, o.run_id, o.unit_id, o.unit_index) == u_key
        ]
        assert len(u_outcomes) == 3
        assert {o.model_name for o in u_outcomes} == set(CANONICAL_MODEL_ORDER)

    for o in outcomes:
        assert ":p:" not in o.unit_id
        assert ":if:" not in o.unit_id
        assert ":ae:" not in o.unit_id

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    await run_phase3_evaluation(cfg, base_dir=tmp_path)

    model_csv_path = (
        tmp_path / "research/results/processed/phase3/task_3_9/model_comparison.csv"
    )
    scen_csv_path = (
        tmp_path / "research/results/processed/phase3/task_3_9/scenario_comparison.csv"
    )

    m_bytes = model_csv_path.read_bytes()
    s_bytes = scen_csv_path.read_bytes()

    assert m_bytes.decode("utf-8")
    assert s_bytes.decode("utf-8")
    assert not m_bytes.startswith(b"\xef\xbb\xbf")
    assert not s_bytes.startswith(b"\xef\xbb\xbf")
    assert m_bytes.endswith(b"\r\n")
    assert s_bytes.endswith(b"\r\n")
    assert b"\n" not in m_bytes.replace(b"\r\n", b"")
    assert b"\r" not in m_bytes.replace(b"\r\n", b"")
    assert b"\n" not in s_bytes.replace(b"\r\n", b"")
    assert b"\r" not in s_bytes.replace(b"\r\n", b"")

    with model_csv_path.open(encoding="utf-8") as f:
        model_reader = csv.reader(f)
        header = next(model_reader)
        model_rows = list(model_reader)

    assert header == [
        "model_name",
        "model_version",
        "total_units",
        "true_positives",
        "false_positives",
        "true_negatives",
        "false_negatives",
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "scenarios_detected",
        "detection_latency_mean_sec",
        "detection_latency_median_sec",
        "detection_latency_p95_sec",
        "success_units",
        "insufficient_units",
        "failure_units",
    ]

    assert len(model_rows) == len(CANONICAL_MODEL_ORDER)
    for i, row in enumerate(model_rows):
        assert row[0] == CANONICAL_MODEL_ORDER[i]
        assert row[1] == "1.0.0"

    p_row = model_rows[0]
    assert p_row[0] == "prophet"
    assert p_row[7] == "1.0"
    assert math.isfinite(float(p_row[7]))
    assert p_row[8] == "1.0"
    assert math.isfinite(float(p_row[8]))
    assert p_row[9] == "1.0"
    assert math.isfinite(float(p_row[9]))
    assert p_row[10] == "0.0"
    assert p_row[11] == "8"
    assert p_row[12] == "5.0"
    assert p_row[13] == "5.0"
    assert p_row[14] == "5.0"

    if_row = model_rows[1]
    assert if_row[0] == "isolation_forest"
    assert if_row[7] == "0.0"
    assert if_row[8] == "0.0"
    assert if_row[9] == "undefined"
    assert if_row[10] == "0.5"
    assert if_row[11] == "0"
    assert if_row[12] == ""
    assert if_row[13] == ""
    assert if_row[14] == ""

    with scen_csv_path.open(encoding="utf-8") as f:
        scen_reader = csv.reader(f)
        scen_header = next(scen_reader)
        scen_rows = list(scen_reader)

    assert scen_header == [
        "scenario_id",
        "model_name",
        "total_units",
        "true_positives",
        "false_positives",
        "true_negatives",
        "false_negatives",
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "detection_status",
        "detection_latency_sec",
    ]

    assert len(scen_rows) == len(CANONICAL_SCENARIO_ORDER) * len(CANONICAL_MODEL_ORDER)
    row_idx = 0
    for s_id in CANONICAL_SCENARIO_ORDER:
        for m_name in CANONICAL_MODEL_ORDER:
            assert scen_rows[row_idx][0] == s_id
            assert scen_rows[row_idx][1] == m_name
            row_idx += 1

    scen0_p_row = scen_rows[0]
    assert scen0_p_row[0] == CANONICAL_SCENARIO_ORDER[0]
    assert scen0_p_row[1] == "prophet"
    assert scen0_p_row[7] == "1.0"
    assert scen0_p_row[11] == "detected"
    assert scen0_p_row[12] == "5.0"
    assert math.isfinite(float(scen0_p_row[12]))

    scen0_if_row = scen_rows[1]
    assert scen0_if_row[0] == CANONICAL_SCENARIO_ORDER[0]
    assert scen0_if_row[1] == "isolation_forest"
    assert scen0_if_row[7] == "0.0"
    assert scen0_if_row[8] == "0.0"
    assert scen0_if_row[9] == "undefined"
    assert scen0_if_row[10] == "1.0"
    assert scen0_if_row[11] == "not_detected"
    assert scen0_if_row[12] == ""


@pytest.mark.asyncio
async def test_outcome_matrix_validation_rejects_duplicate_outcome_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()
    # Duplicate an outcome_id
    duplicate_outcome = outcomes[0].model_copy(update={"unit_id": "different-unit"})
    bad_outcomes = [outcomes[0], duplicate_outcome, *outcomes[1:]]

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return bad_outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    with pytest.raises(EvaluationArtifactError, match="Duplicate outcome_id detected"):
        await run_phase3_evaluation(cfg, base_dir=tmp_path)

    # Prove no files were written
    assert not (tmp_path / "research").exists()


@pytest.mark.asyncio
async def test_outcome_matrix_validation_rejects_duplicate_logical_slot_bounded_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()
    long_unit_id = "very-long-unit-id-payload-" + "z" * 1000
    base_outcome = outcomes[0].model_copy(update={"unit_id": long_unit_id})
    duplicate_logical = base_outcome.model_copy(update={"outcome_id": uuid.uuid4()})
    bad_outcomes = [base_outcome, duplicate_logical, *outcomes[1:]]

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return bad_outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    with pytest.raises(EvaluationArtifactError) as exc_info:
        await run_phase3_evaluation(cfg, base_dir=tmp_path)

    assert str(exc_info.value) == "Duplicate logical model-outcome slot detected"
    assert not (tmp_path / "research").exists()


@pytest.mark.asyncio
async def test_outcome_matrix_validation_rejects_unknown_scenario(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()
    bad_scenario_outcome = outcomes[0].model_copy(
        update={"scenario_id": "unknown-scenario-xyz"}
    )
    bad_outcomes = [bad_scenario_outcome, *outcomes[1:]]

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return bad_outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    with pytest.raises(
        EvaluationArtifactError,
        match="Outcome contains unknown scenario_id 'unknown-scenario-xyz' not in configured scenario_ids",
    ):
        await run_phase3_evaluation(cfg, base_dir=tmp_path)

    assert not (tmp_path / "research").exists()


@pytest.mark.asyncio
async def test_outcome_matrix_validation_rejects_unknown_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()
    bad_model_outcome = outcomes[0].model_copy(
        update={"model_name": "unknown-model-abc"}
    )
    bad_outcomes = [bad_model_outcome, *outcomes[1:]]

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return bad_outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    with pytest.raises(
        EvaluationArtifactError,
        match="Outcome contains unknown model_name 'unknown-model-abc' not in configured model_names",
    ):
        await run_phase3_evaluation(cfg, base_dir=tmp_path)

    assert not (tmp_path / "research").exists()


@pytest.mark.parametrize(
    "component",
    ["scenario_id", "run_id", "unit_id", "unit_index"],
)
def test_generated_report_slot_accounting_uses_each_logical_unit_identity_component(
    component: str,
) -> None:
    cfg = create_default_evaluation_harness_config()
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_run_id = uuid.uuid4()
    base_scenario = CANONICAL_SCENARIO_ORDER[0]
    base_unit_id = "base-unit"
    base_unit_index = 0

    u1_scenario_id = base_scenario
    u1_run_id = base_run_id
    u1_unit_id = base_unit_id
    u1_unit_index = base_unit_index

    u2_scenario_id = (
        CANONICAL_SCENARIO_ORDER[1] if component == "scenario_id" else base_scenario
    )
    u2_run_id = uuid.uuid4() if component == "run_id" else base_run_id
    u2_unit_id = "different-unit" if component == "unit_id" else base_unit_id
    u2_unit_index = 1 if component == "unit_index" else base_unit_index

    units_data = [
        (u1_scenario_id, u1_run_id, u1_unit_id, u1_unit_index),
        (u2_scenario_id, u2_run_id, u2_unit_id, u2_unit_index),
    ]

    outcomes: list[EvaluationOutcome] = []
    for s_id, r_id, u_id, u_idx in units_data:
        for mod in CANONICAL_MODEL_ORDER:
            outcomes.append(
                EvaluationOutcome(
                    outcome_id=uuid.uuid4(),
                    unit_id=u_id,
                    unit_index=u_idx,
                    scenario_id=s_id,
                    run_id=r_id,
                    model_name=mod,
                    model_version="1.0.0",
                    event_time=t0,
                    ground_truth_positive=True,
                    status=EvaluationOutcomeStatus.SUCCESS,
                    raw_score=0.85,
                    baseline_normalized_score=0.85,
                    calibrated_score=0.85,
                    predicted_positive=True,
                    is_true_positive=True,
                    is_false_positive=False,
                    is_true_negative=False,
                    is_false_negative=False,
                )
            )

    validate_evaluation_outcomes(outcomes, cfg)

    _, summaries, run_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
    )
    manifest = EvaluationPackageManifest.model_construct(
        schema_version="1.0",
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        code_revision=cfg.code_revision,
        created_at=datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc),
        environment=cfg.environment,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
        decision_threshold=cfg.decision_policy.decision_threshold,
        models=list(CANONICAL_MODEL_ORDER),
        scenarios=list(CANONICAL_SCENARIO_ORDER),
        artifact_manifest_path="custom/results/artifact_manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=[],
    )

    report = generate_evaluation_report(
        manifest=manifest,
        summaries=summaries,
        outcomes=outcomes,
        config=cfg,
        run_metadata=run_metadata,
    )

    assert "- **Unique Evaluation Units:** 2" in report
    assert "- **Expected Model-Outcome Slots:** 6 (2 units × 3 models)" in report
    assert "- **Recorded Model Outcomes:** 6" in report
    assert "- **Duplicate Model Outcomes:** 0" in report
    assert "- **Missing Model-Outcome Slots:** 0" in report


@pytest.mark.asyncio
async def test_outcome_matrix_partial_unit_with_changed_index_is_distinct_and_missing_slots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()
    extra_outcome = outcomes[0].model_copy(
        update={
            "outcome_id": uuid.uuid4(),
            "unit_index": 999,
        }
    )
    outcomes_with_partial_unit = [*outcomes, extra_outcome]

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes_with_partial_unit, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    await run_phase3_evaluation(cfg, base_dir=tmp_path)

    rep_path = (
        tmp_path
        / cfg.artifact_root
        / "reports"
        / "phase3"
        / "task_3_9"
        / "evaluation_report.md"
    )
    assert rep_path.exists()
    content = rep_path.read_text(encoding="utf-8")

    assert "- **Unique Evaluation Units:** 17" in content
    assert "- **Expected Model-Outcome Slots:** 51 (17 units × 3 models)" in content
    assert "- **Recorded Model Outcomes:** 49" in content
    assert "- **Duplicate Model Outcomes:** 0" in content
    assert "- **Missing Model-Outcome Slots:** 2" in content


def test_generated_report_exact_missing_slot_accounting() -> None:
    cfg = create_default_evaluation_harness_config()
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    scen_id = CANONICAL_SCENARIO_ORDER[0]
    run_id = uuid.uuid4()

    outcomes = []
    for mod in CANONICAL_MODEL_ORDER:
        outcomes.append(
            EvaluationOutcome(
                outcome_id=uuid.uuid4(),
                unit_id="unit:0",
                unit_index=0,
                scenario_id=scen_id,
                run_id=run_id,
                model_name=mod,
                model_version="1.0.0",
                event_time=t0,
                ground_truth_positive=True,
                status=EvaluationOutcomeStatus.SUCCESS,
                raw_score=0.85,
                baseline_normalized_score=0.85,
                calibrated_score=0.85,
                predicted_positive=True,
                is_true_positive=True,
                is_false_positive=False,
                is_true_negative=False,
                is_false_negative=False,
            )
        )
    for mod in ["prophet", "isolation_forest"]:
        outcomes.append(
            EvaluationOutcome(
                outcome_id=uuid.uuid4(),
                unit_id="unit:1",
                unit_index=1,
                scenario_id=scen_id,
                run_id=run_id,
                model_name=mod,
                model_version="1.0.0",
                event_time=t0 + timedelta(seconds=60),
                ground_truth_positive=True,
                status=EvaluationOutcomeStatus.SUCCESS,
                raw_score=0.85,
                baseline_normalized_score=0.85,
                calibrated_score=0.85,
                predicted_positive=True,
                is_true_positive=True,
                is_false_positive=False,
                is_true_negative=False,
                is_false_negative=False,
            )
        )

    validate_evaluation_outcomes(outcomes, cfg)

    _, summaries, run_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
    )
    manifest = EvaluationPackageManifest.model_construct(
        schema_version="1.0",
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        code_revision=cfg.code_revision,
        created_at=datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc),
        environment=cfg.environment,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
        decision_threshold=cfg.decision_policy.decision_threshold,
        models=list(CANONICAL_MODEL_ORDER),
        scenarios=list(CANONICAL_SCENARIO_ORDER),
        artifact_manifest_path="custom/results/artifact_manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=[],
    )

    report = generate_evaluation_report(
        manifest=manifest,
        summaries=summaries,
        outcomes=outcomes,
        config=cfg,
        run_metadata=run_metadata,
    )

    assert "- **Unique Evaluation Units:** 2" in report
    assert "- **Expected Model-Outcome Slots:** 6 (2 units × 3 models)" in report
    assert "- **Recorded Model Outcomes:** 5" in report
    assert "- **Duplicate Model Outcomes:** 0" in report
    assert "- **Missing Model-Outcome Slots:** 1" in report


@pytest.mark.asyncio
async def test_generated_report_complete_dynamic_metadata_and_lineage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    custom_calib_cfg = CalibrationConfig(
        schema_version="1.0",
        calibration_method="piecewise_linear_quantile",
        calibration_method_version="1.0.0",
        calibration_reference_id="ref-custom-999",
        calibration_reference_version="2.0.0",
        calibration_split_rule="temporal_sliding_window",
        calibration_split_version="1.0.0",
        severity_mapping_version="1.0.0",
    )
    base_cfg = create_default_evaluation_harness_config(
        code_revision="lineage-rev-999",
        calibration_seed=77,
        evaluation_seed=88,
        decision_threshold=0.65,
        artifact_root="custom_lineage_root",
        calibration_partition_id="custom-calib-part-001",
        evaluation_partition_id="custom-eval-part-002",
    )
    cfg = base_cfg.model_copy(update={"calibration_config": custom_calib_cfg})
    outcomes, summaries, custom_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=77,
        evaluation_seed=88,
    )

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, custom_metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    await run_phase3_evaluation(cfg, base_dir=tmp_path)

    rep_path = (
        tmp_path
        / "custom_lineage_root"
        / "reports"
        / "phase3"
        / "task_3_9"
        / "evaluation_report.md"
    )
    assert rep_path.exists()
    content = rep_path.read_text(encoding="utf-8")

    # Section 2 Lineage assertions
    assert "**Artifact Root:** `custom_lineage_root`" in content
    assert "`77` (Partition ID: `custom-calib-part-001`)" in content
    assert "`88` (Partition ID: `custom-eval-part-002`)" in content
    assert "0.65" in content
    assert "Calibration Scenario Runs Recorded:** 8" in content
    assert "Evaluation Scenario Runs Recorded:** 8" in content
    assert "Decision Policy:" in content
    assert "Label Policy:" in content
    assert "Feature and Windowing Lineage" in content
    assert "- **Calibration Method Name:** `piecewise_linear_quantile`" in content
    assert "- **Calibration Method Version:** `1.0.0`" in content
    assert "- **Calibration Reference ID:** `ref-custom-999`" in content
    assert "- **Calibration Reference Version:** `2.0.0`" in content
    assert "- **Calibration Split Rule:** `temporal_sliding_window`" in content
    assert "- **Calibration Split Version:** `1.0.0`" in content
    assert "- **Severity Mapping Version:** `1.0.0`" in content
    assert "Detailed Outcome Status Breakdown" in content

    # Assert stale fallbacks are excluded
    assert "empirical_cdf_percentile" not in content
    assert "calib-ref-v1" not in content
    assert "independent_reference_set" not in content

    # Assert all scenario run pairs are present in the table
    calib_runs_map = {
        r.scenario_id: str(r.run_id) for r in custom_metadata.calibration_runs
    }
    eval_runs_map = {
        r.scenario_id: str(r.run_id) for r in custom_metadata.evaluation_runs
    }
    for sid in CANONICAL_SCENARIO_ORDER:
        assert (
            f"| `{sid}` | `{calib_runs_map[sid]}` | `{eval_runs_map[sid]}` |" in content
        )

    # Section 8 Lineage and slot accounting assertions using canonical 4-part unit identity
    unique_unit_keys = set(
        (o.scenario_id, o.run_id, o.unit_id, o.unit_index) for o in outcomes
    )
    expected_slots = len(unique_unit_keys) * len(cfg.model_names)
    assert f"- **Unique Evaluation Units:** {len(unique_unit_keys)}" in content
    assert (
        f"- **Expected Model-Outcome Slots:** {expected_slots} ({len(unique_unit_keys)} units × {len(cfg.model_names)} models)"
        in content
    )
    assert f"- **Recorded Model Outcomes:** {len(outcomes)}" in content
    assert "- **Duplicate Model Outcomes:** 0" in content
    assert "- **Missing Model-Outcome Slots:** 0" in content


def test_generated_report_typed_metadata_provenance() -> None:
    custom_calibration = CalibrationConfig.model_validate(
        {
            **create_default_evaluation_harness_config().calibration_config.model_dump(),
            "calibration_method": "identity",
            "calibration_reference_id": "f1e-c1-identity-reference",
            "calibration_reference_version": "9.8.7",
            "calibration_split_rule": "f1e_c1_distinctive_holdout",
        }
    )

    custom_decision_policy = EvaluationDecisionPolicy.model_validate(
        {
            "policy_name": "f1e_c1_custom_decision",
            "policy_version": SUPPORTED_DECISION_POLICY_VERSION,
            "decision_threshold": 0.73,
            "description": "F1E-C1 distinctive decision policy for testing",
        }
    )

    custom_label_policy = EvaluationLabelPolicy.model_validate(
        {
            "policy_name": "f1e_c1_custom_label",
            "policy_version": SUPPORTED_LABEL_POLICY_VERSION,
            "overlap_rule": "f1e_c1_distinctive_overlap",
            "description": "F1E-C1 distinctive label policy for testing",
        }
    )

    base_cfg = create_default_evaluation_harness_config(
        code_revision="f1e-c1-rev-custom",
        calibration_seed=111,
        evaluation_seed=222,
        calibration_partition_id="f1e-c1-cal-111",
        evaluation_partition_id="f1e-c1-eval-222",
    )

    cfg = EvaluationHarnessConfig.model_validate(
        {
            **base_cfg.model_dump(),
            "package_id": "f1e-c1-pkg-distinctive",
            "package_version": "9.8.7",
            "environment": "f1e-c1-env",
            "artifact_root": "f1e_c1_artifacts",
            "calibration_config": custom_calibration,
            "decision_policy": custom_decision_policy,
            "label_policy": custom_label_policy,
        }
    )

    outcomes, summaries, custom_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=111,
        evaluation_seed=222,
    )
    manifest = EvaluationPackageManifest.model_construct(
        schema_version="1.0",
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        code_revision=cfg.code_revision,
        created_at=datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc),
        environment=cfg.environment,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
        decision_threshold=cfg.decision_policy.decision_threshold,
        models=list(CANONICAL_MODEL_ORDER),
        scenarios=list(CANONICAL_SCENARIO_ORDER),
        artifact_manifest_path="custom/results/artifact_manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=[],
    )

    report = generate_evaluation_report(
        manifest=manifest,
        summaries=summaries,
        outcomes=outcomes,
        config=cfg,
        run_metadata=custom_metadata,
    )

    assert isinstance(report, str)
    assert "**Package ID:** `f1e-c1-pkg-distinctive`" in report
    assert "**Package Version:** `9.8.7`" in report
    assert "**Code Revision:** `f1e-c1-rev-custom`" in report
    assert "**Generated:** 2026-10-03 12:00:00 UTC" in report
    assert "**Environment:** `f1e-c1-env`" in report
    assert "`111` (Partition ID: `f1e-c1-cal-111`)" in report
    assert "`222` (Partition ID: `f1e-c1-eval-222`)" in report
    assert (
        f"Cutoff Timestamp: `{custom_metadata.calibration_cutoff.isoformat()}`"
        in report
    )
    assert (
        f"- **Evaluation Start:** `{custom_metadata.evaluation_start.isoformat()}`"
        in report
    )
    assert "- **Calibration Method Name:** `identity`" in report
    assert (
        f"- **Calibration Method Version:** `{SUPPORTED_CALIBRATION_METHOD_VERSION}`"
        in report
    )
    assert "- **Calibration Reference ID:** `f1e-c1-identity-reference`" in report
    assert "- **Calibration Reference Version:** `9.8.7`" in report
    assert "- **Calibration Split Rule:** `f1e_c1_distinctive_holdout`" in report
    assert (
        f"- **Calibration Split Version:** `{SUPPORTED_CALIBRATION_SPLIT_VERSION}`"
        in report
    )
    assert (
        f"- **Severity Mapping Version:** `{SUPPORTED_SEVERITY_MAPPING_VERSION}`"
        in report
    )
    assert (
        f"- **Decision Policy:** `f1e_c1_custom_decision` "
        f"(v`{SUPPORTED_DECISION_POLICY_VERSION}`)" in report
    )
    assert (
        "- **Decision Threshold:** `0.73` "
        "(F1E-C1 distinctive decision policy for testing)" in report
    )
    assert (
        f"- **Label Policy:** `f1e_c1_custom_label` "
        f"(v`{SUPPORTED_LABEL_POLICY_VERSION}`, "
        "overlap rule: `f1e_c1_distinctive_overlap`)" in report
    )

    assert "empirical_cdf_percentile" not in report
    assert "calib-ref-v1" not in report
    assert "independent_reference_set" not in report
    assert "warning_threshold_binary_decision" not in report
    assert "half_open_active_interval" not in report
    assert "window_start_in_active_interval" not in report

    calib_runs_map = {
        r.scenario_id: str(r.run_id) for r in custom_metadata.calibration_runs
    }
    eval_runs_map = {
        r.scenario_id: str(r.run_id) for r in custom_metadata.evaluation_runs
    }
    for sid in CANONICAL_SCENARIO_ORDER:
        assert (
            f"| `{sid}` | `{calib_runs_map[sid]}` | `{eval_runs_map[sid]}` |" in report
        )


def test_generate_evaluation_report_uses_config_as_authoritative_source() -> None:
    # 1. Authoritative config with distinctive values
    base_cfg = create_default_evaluation_harness_config(
        code_revision="auth-rev-999",
        calibration_seed=77,
        evaluation_seed=88,
        decision_threshold=0.65,
        calibration_partition_id="part-cal-77",
        evaluation_partition_id="part-eval-88",
    )
    cfg = base_cfg.model_copy(
        update={
            "package_id": "pkg-auth-test",
            "package_version": "1.2.3",
            "environment": "simulation-auth",
        }
    )
    outcomes, summaries, run_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=77,
        evaluation_seed=88,
    )

    # 2. Deliberately conflicting manifest values
    manifest = EvaluationPackageManifest.model_construct(
        schema_version="1.0",
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        code_revision=cfg.code_revision,
        created_at=datetime(2026, 10, 3, 15, 30, 0, tzinfo=timezone.utc),
        environment=cfg.environment,
        calibration_seed=9999,  # Conflicting
        evaluation_seed=8888,  # Conflicting
        decision_threshold=0.12,  # Conflicting
        models=["prophet"],  # Conflicting cardinality
        scenarios=list(reversed(CANONICAL_SCENARIO_ORDER)),  # Conflicting order
        artifact_manifest_path="test/manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=[],
    )

    report = generate_evaluation_report(
        manifest=manifest,
        summaries=summaries,
        outcomes=outcomes,
        config=cfg,
        run_metadata=run_metadata,
    )

    # 1 & 2: Section 4 latency threshold matches config (0.65) and excludes manifest (0.12)
    assert "(`score >= 0.65`)" in report
    assert "(`score >= 0.12`)" not in report
    assert "0.12" not in report

    # 3: Section 6 scenario order follows config.scenario_ids, not manifest.scenarios
    assert "## 6. Detailed Per-Scenario Performance" in report
    assert "## 7. Score Distributions and Calibration Lineage" in report
    sec6_start = report.index("## 6. Detailed Per-Scenario Performance")
    sec7_start = report.index("## 7. Score Distributions and Calibration Lineage")
    sec6_content = report[sec6_start:sec7_start]

    first_scen = CANONICAL_SCENARIO_ORDER[0]
    last_scen = CANONICAL_SCENARIO_ORDER[-1]
    first_scen_pos = sec6_content.index(f"| `{first_scen}` |")
    last_scen_pos = sec6_content.index(f"| `{last_scen}` |")
    assert first_scen_pos < last_scen_pos

    # 4 & 5: Expected model-outcome cardinality uses canonical 4-part unit identity
    unique_unit_keys = set(
        (o.scenario_id, o.run_id, o.unit_id, o.unit_index) for o in outcomes
    )
    expected_slots = len(unique_unit_keys) * len(cfg.model_names)
    assert (
        f"- **Expected Model-Outcome Slots:** {expected_slots} ({len(unique_unit_keys)} units × 3 models)"
        in report
    )
    assert "1 models" not in report

    # 6 & 7: Limitations section contains config seeds (77 and 88) and excludes manifest seeds (9999 and 8888)
    assert (
        "Calibration and evaluation were run with fixed seeds (`77` and `88`);"
        in report
    )
    assert "9999" not in report
    assert "8888" not in report

    # 8 & 9: Limitations section contains config threshold (0.65) and excludes manifest threshold (0.12)
    assert "Threshold `0.65` represents an operational standard" in report
    assert "Threshold `0.12`" not in report

    # 10: Manifest-owned properties are preserved
    assert "**Generated:** 2026-10-03 15:30:00 UTC" in report
    assert "`test/manifest.json`" in report
    assert "self_digest_policy: excluded_from_manifest_digest" in report


def test_generate_evaluation_report_forbidden_manifest_access_guard() -> None:
    src = inspect.getsource(generate_evaluation_report)

    forbidden_patterns = [
        "manifest.decision_threshold",
        "manifest.scenarios",
        "manifest.models",
        "manifest.calibration_seed",
        "manifest.evaluation_seed",
    ]

    for pattern in forbidden_patterns:
        assert (
            pattern not in src
        ), f"Forbidden pattern '{pattern}' detected in generate_evaluation_report source."


def test_encode_text_payload_matches_path_write_text(tmp_path: Path) -> None:
    text = "AegisOps café\nline-two\r\nline-three\n"
    expected_path = tmp_path / "expected.txt"
    expected_path.write_text(text, encoding="utf-8")
    expected_bytes = expected_path.read_bytes()

    actual_bytes = _encode_text_payload(text)

    assert actual_bytes == expected_bytes
    assert actual_bytes.decode("utf-8") is not None


@pytest.mark.asyncio
async def test_run_phase3_evaluation_late_finite_guard_fails_before_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    # Monkeypatch _guard_finite_serialization to fail on comp_data during Phase A
    orig_guard = _guard_finite_serialization

    def failing_guard(data: Any, path: str = "$") -> None:
        if isinstance(data, dict) and "schema_version" in data and "models" in data:
            raise EvaluationArtifactError("Injected late finite guard failure")
        orig_guard(data, path)

    monkeypatch.setattr(
        app.anomaly.evaluation, "_guard_finite_serialization", failing_guard
    )

    cfg = create_default_evaluation_harness_config()
    target_sandbox = tmp_path / "sandbox_finite_fail"

    with pytest.raises(
        EvaluationArtifactError, match="Injected late finite guard failure"
    ):
        await run_phase3_evaluation(cfg, base_dir=target_sandbox)

    assert not target_sandbox.exists()


@pytest.mark.asyncio
async def test_run_phase3_evaluation_report_failure_fails_before_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    def failing_report(*args: Any, **kwargs: Any) -> str:
        raise EvaluationArtifactError("Simulated report generation failure")

    monkeypatch.setattr(
        app.anomaly.evaluation, "generate_evaluation_report", failing_report
    )

    cfg = create_default_evaluation_harness_config()
    target_sandbox = tmp_path / "sandbox_report_fail"

    with pytest.raises(
        EvaluationArtifactError, match="Simulated report generation failure"
    ):
        await run_phase3_evaluation(cfg, base_dir=target_sandbox)

    assert not target_sandbox.exists()


@pytest.mark.asyncio
async def test_fail_before_write_on_invalid_lineage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = create_default_evaluation_harness_config()
    outcomes, summaries, metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    invalid_metadata = metadata.model_copy(
        update={"calibration_runs": metadata.calibration_runs[:-1]}
    )

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, invalid_metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    target_sandbox = tmp_path / "sandbox_invalid_lineage"

    with pytest.raises(
        EvaluationConfigurationError,
        match="lineage.scenario.missing: configured scenario lineage is incomplete",
    ) as exc:
        await run_phase3_evaluation(cfg, base_dir=target_sandbox)

    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True
    assert not target_sandbox.exists()


@pytest.mark.asyncio
async def test_fail_before_write_on_temporal_equality(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = create_default_evaluation_harness_config()
    outcomes, summaries, metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    invalid_metadata = metadata.model_copy(
        update={"evaluation_start": metadata.calibration_cutoff}
    )

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, invalid_metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    target_sandbox = tmp_path / "sandbox_temporal_equality"

    with pytest.raises(
        EvaluationConfigurationError,
        match="lineage.temporal.order: evaluation start must be strictly after calibration cutoff",
    ) as exc:
        await run_phase3_evaluation(cfg, base_dir=target_sandbox)

    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True
    assert not target_sandbox.exists()


@pytest.mark.asyncio
async def test_run_phase3_evaluation_explicit_preflight_before_first_mkdir_ordering(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_mock_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    recorded_events: list[str] = []

    orig_val_outcomes = validate_evaluation_outcomes
    orig_val_lineage = validate_evaluation_run_metadata
    orig_gen_report = generate_evaluation_report

    def tracked_val_outcomes(*args: Any, **kwargs: Any) -> None:
        recorded_events.append("validate_outcomes")
        orig_val_outcomes(*args, **kwargs)

    def tracked_val_lineage(*args: Any, **kwargs: Any) -> None:
        recorded_events.append("validate_lineage")
        orig_val_lineage(*args, **kwargs)

    def tracked_gen_report(*args: Any, **kwargs: Any) -> str:
        recorded_events.append("prepare_report")
        return orig_gen_report(*args, **kwargs)

    def stopping_mkdir(self: Path, *args: Any, **kwargs: Any) -> None:
        recorded_events.append("first_mkdir")
        raise RuntimeError("Stop deliberately at first mkdir")

    monkeypatch.setattr(
        app.anomaly.evaluation, "validate_evaluation_outcomes", tracked_val_outcomes
    )
    monkeypatch.setattr(
        app.anomaly.evaluation,
        "validate_evaluation_run_metadata",
        tracked_val_lineage,
    )
    monkeypatch.setattr(
        app.anomaly.evaluation, "generate_evaluation_report", tracked_gen_report
    )
    monkeypatch.setattr(Path, "mkdir", stopping_mkdir)

    cfg = create_default_evaluation_harness_config()
    target_sandbox = tmp_path / "sandbox_ordering"

    with pytest.raises(RuntimeError, match="Stop deliberately at first mkdir"):
        await run_phase3_evaluation(cfg, base_dir=target_sandbox)

    assert recorded_events == [
        "validate_outcomes",
        "validate_lineage",
        "prepare_report",
        "first_mkdir",
    ]
    idx_outcomes = recorded_events.index("validate_outcomes")
    idx_lineage = recorded_events.index("validate_lineage")
    idx_report = recorded_events.index("prepare_report")
    idx_mkdir = recorded_events.index("first_mkdir")

    assert idx_outcomes < idx_lineage
    assert idx_lineage < idx_report
    assert idx_report < idx_mkdir


@pytest.mark.asyncio
async def test_svg_xml_structure_and_non_zero_undefined_representation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outcomes, summaries, metadata = _build_boundary_evaluation_data()

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    cfg = create_default_evaluation_harness_config()
    await run_phase3_evaluation(cfg, base_dir=tmp_path)

    q_svg_path = (
        tmp_path / "research/results/figures/phase3/task_3_9/model_quality_metrics.svg"
    )
    l_svg_path = (
        tmp_path
        / "research/results/figures/phase3/task_3_9/detection_latency_by_scenario.svg"
    )

    q_bytes = q_svg_path.read_bytes()
    l_bytes = l_svg_path.read_bytes()

    q_text = q_bytes.decode("utf-8")
    l_text = l_bytes.decode("utf-8")

    for svg_str in (q_text, l_text):
        root = ET.fromstring(svg_str)
        assert root.tag == "{http://www.w3.org/2000/svg}svg"

        titles = [
            child for child in root if child.tag == "{http://www.w3.org/2000/svg}title"
        ]
        descs = [
            child for child in root if child.tag == "{http://www.w3.org/2000/svg}desc"
        ]
        assert len(titles) == 1 and titles[0].text and titles[0].text.strip()
        assert len(descs) == 1 and descs[0].text and descs[0].text.strip()

        scripts = list(root.iter("{http://www.w3.org/2000/svg}script"))
        assert len(scripts) == 0

        for bad in ("NaN", "Infinity", "-Infinity"):
            assert bad not in svg_str

    q_root = ET.fromstring(q_text)
    q_text_elements = list(q_root.iter("{http://www.w3.org/2000/svg}text"))
    q_texts = [e.text for e in q_text_elements if e.text]

    for grp in ("Precision", "Recall", "F1", "FPR"):
        assert grp in q_texts

    for mod_name in CANONICAL_MODEL_ORDER:
        assert mod_name.replace("_", " ").title() in q_texts

    actual_quality_labels = [
        e.text
        for e in q_text_elements
        if e.attrib.get("font-size") in ("10", "9") and e.text
    ]

    expected_quality_labels = []
    for s in summaries:
        for metric_obj in (
            s.micro_precision,
            s.micro_recall,
            s.micro_f1,
            s.micro_false_positive_rate,
        ):
            if metric_obj.value is not None:
                expected_quality_labels.append(f"{metric_obj.value:.2f}")
            else:
                expected_quality_labels.append("N/A")

    assert collections.Counter(actual_quality_labels) == collections.Counter(
        expected_quality_labels
    )
    assert len(actual_quality_labels) == 12
    assert actual_quality_labels.count("N/A") == 1
    assert "0.00" in actual_quality_labels
    assert "1.00" in actual_quality_labels
    assert "0.50" in actual_quality_labels

    l_root = ET.fromstring(l_text)
    l_text_elements = list(l_root.iter("{http://www.w3.org/2000/svg}text"))
    l_texts = [e.text for e in l_text_elements if e.text]

    for scen_id in CANONICAL_SCENARIO_ORDER:
        assert scen_id in l_texts

    actual_detected_labels = [
        e.text for e in l_text_elements if e.attrib.get("font-size") == "8.5" and e.text
    ]
    actual_nd_labels = [
        e.text for e in l_text_elements if e.attrib.get("font-size") == "7.5" and e.text
    ]

    expected_detected_labels = []
    expected_nd_count = 0

    for s in summaries:
        for se in s.scenario_evaluations:
            if (
                se.detection_latency.status == "detected"
                and se.detection_latency.latency_seconds is not None
            ):
                expected_detected_labels.append(
                    f"{se.detection_latency.latency_seconds:.1f}s"
                )
            else:
                expected_nd_count += 1

    assert collections.Counter(actual_detected_labels) == collections.Counter(
        expected_detected_labels
    )
    assert len(actual_detected_labels) == 15
    assert len(actual_nd_labels) == expected_nd_count == 9
    assert all(label == "ND" for label in actual_nd_labels)
    assert len(actual_detected_labels) + len(actual_nd_labels) == 24

    assert "ND = Not Detected" in l_texts


def _assert_f5_cli_bounded_failure(
    *,
    exit_code: int,
    stdout: str,
    stderr: str,
    expected_body: str,
    prohibited: tuple[str, ...] = (),
) -> None:
    assert exit_code == 1, f"Expected exit code 1, got {exit_code}"
    assert (
        stderr == f"Error: {expected_body}\n"
    ), f"Expected 'Error: {expected_body}\\n', got {repr(stderr)}"
    assert (
        len(stderr.splitlines()) == 1
    ), f"Expected 1 line in stderr, got {len(stderr.splitlines())}"
    assert stderr.count("Error: ") == 1
    assert "Configuration error:" not in stderr
    assert "Evaluation error:" not in stderr
    assert "Evaluation execution failed:" not in stderr
    assert "Traceback" not in stderr
    assert "Traceback (most recent call last)" not in stderr
    assert "PHASE 3 TASK 3.9 EVALUATION HARNESS COMPLETE — SUCCESS" not in stdout
    for p in prohibited:
        assert p not in stdout, f"Prohibited string '{p}' found in stdout"
        assert p not in stderr, f"Prohibited string '{p}' found in stderr"


def _load_cli_module() -> Any:
    import importlib.util

    cli_path = (
        Path(__file__).resolve().parent.parent.parent
        / "scripts"
        / "run_phase3_evaluation.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_phase3_evaluation_script", cli_path
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_failing_summaries() -> Any:
    """Build a ModelComparisonSummary list with a single exploding summary."""
    _, sums, _ = _build_mock_evaluation_data()

    class _ExplodingMetricValue:
        def __format__(self, format_spec: str) -> str:
            raise ValueError("F5C1_METRIC_FORMAT_SECRET")

    exploding_metric = MetricResult.model_construct(
        metric_name="precision",
        value=_ExplodingMetricValue(),
        status="defined",
        numerator=1.0,
        denominator=1.0,
    )
    sums[0] = ModelComparisonSummary.model_construct(
        model_name=sums[0].model_name,
        model_version=sums[0].model_version,
        micro_confusion_counts=sums[0].micro_confusion_counts,
        micro_precision=exploding_metric,
        micro_recall=sums[0].micro_recall,
        micro_f1=sums[0].micro_f1,
        micro_false_positive_rate=sums[0].micro_false_positive_rate,
        macro_precision=sums[0].macro_precision,
        macro_recall=sums[0].macro_recall,
        macro_f1=sums[0].macro_f1,
        macro_false_positive_rate=sums[0].macro_false_positive_rate,
        macro_contributing_scenario_count=sums[0].macro_contributing_scenario_count,
        latency_summary=sums[0].latency_summary,
        scenario_coverage=sums[0].scenario_coverage,
        truth_positive_distribution=sums[0].truth_positive_distribution,
        truth_negative_distribution=sums[0].truth_negative_distribution,
        predicted_positive_distribution=sums[0].predicted_positive_distribution,
        predicted_negative_distribution=sums[0].predicted_negative_distribution,
        scenario_evaluations=sums[0].scenario_evaluations,
    )
    return sums


def _make_exploding_manifest_path() -> Any:
    class _ExplodingManifestPath:
        @property
        def artifact_manifest_path(self) -> str:
            raise RuntimeError("F5C1_MANIFEST_PATH_SECRET")

    return _ExplodingManifestPath()


def _make_exploding_artifacts_list() -> Any:
    class _ExplodingArtifactsList(list):
        def __len__(self) -> int:
            return 11

        def __iter__(self):
            raise RuntimeError("F5C1_ARTIFACT_ITERATION_SECRET")

    return _ExplodingArtifactsList()


def _make_standard_manifest(config: Any) -> Any:
    return EvaluationPackageManifest.model_construct(
        schema_version="1.0",
        package_id=config.package_id,
        package_version=config.package_version,
        code_revision=config.code_revision,
        created_at=datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc),
        environment=config.environment,
        calibration_seed=config.calibration_seed,
        evaluation_seed=config.evaluation_seed,
        decision_threshold=config.decision_policy.decision_threshold,
        models=list(CANONICAL_MODEL_ORDER),
        scenarios=list(CANONICAL_SCENARIO_ORDER),
        artifact_manifest_path="manifest.json",
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=[],
    )


def _setup_cli_repo_root(tmp_path: Path) -> Any:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)
    cli_mod = _load_cli_module()
    cli_mod.REPO_ROOT = tmp_path
    cli_mod._get_git_revision = lambda: "mock-git-rev"
    return cli_mod


def _capture_cli_output(cli_mod: Any, argv: list[str]) -> tuple[int, str, str]:
    import io
    import sys

    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
    try:
        exit_code = asyncio.run(cli_mod.main_async(argv))
        out, err = sys.stdout.getvalue(), sys.stderr.getvalue()
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr
    return exit_code, out, err


def test_f5_cli_allowlist_integrity() -> None:
    cli_mod = _load_cli_module()
    allowlist = cli_mod.APPROVED_CLI_PAIR_ALLOWLIST

    assert isinstance(allowlist, frozenset)
    assert len(allowlist) == 18

    expected_pairs = {
        (
            EvaluationConfigurationError,
            "lineage.scenario_id.type: scenario_id must be a string",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.type: seed must be an exact integer",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.range: seed must be within unsigned 64-bit range",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.type: run_id must be a UUID instance",
        ),
        (
            EvaluationConfigurationError,
            "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.missing: configured scenario lineage is incomplete",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.unknown: lineage contains an unconfigured scenario",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.duplicate: lineage contains duplicate scenario entries",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.order: lineage does not follow canonical scenario order",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.mismatch: lineage seed does not match configured partition seed",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.collision: calibration and evaluation partition run IDs overlap",
        ),
        (
            EvaluationConfigurationError,
            "lineage.temporal.order: evaluation start must be strictly after calibration cutoff",
        ),
        (
            EvaluationExecutionError,
            "Evaluation lineage validation failed",
        ),
        (
            EvaluationExecutionError,
            "Evaluation validation failed",
        ),
        (
            EvaluationConfigurationError,
            "Failed to discover git revision from repository",
        ),
        (
            EvaluationConfigurationError,
            "Git rev-parse returned empty revision",
        ),
    }

    assert allowlist == expected_pairs
    for cls, msg in allowlist:
        assert type(cls) is type  # noqa: E721
        assert cls in (EvaluationConfigurationError, EvaluationExecutionError)
        assert type(msg) is str  # noqa: E721


@pytest.mark.parametrize(
    "exc_cls, msg",
    [
        (
            EvaluationConfigurationError,
            "lineage.scenario_id.type: scenario_id must be a string",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.type: seed must be an exact integer",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.range: seed must be within unsigned 64-bit range",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.type: run_id must be a UUID instance",
        ),
        (
            EvaluationConfigurationError,
            "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.missing: configured scenario lineage is incomplete",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.unknown: lineage contains an unconfigured scenario",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.duplicate: lineage contains duplicate scenario entries",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.order: lineage does not follow canonical scenario order",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.mismatch: lineage seed does not match configured partition seed",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.collision: calibration and evaluation partition run IDs overlap",
        ),
        (
            EvaluationConfigurationError,
            "lineage.temporal.order: evaluation start must be strictly after calibration cutoff",
        ),
        (
            EvaluationExecutionError,
            "Evaluation lineage validation failed",
        ),
        (
            EvaluationExecutionError,
            "Evaluation validation failed",
        ),
        (
            EvaluationConfigurationError,
            "Failed to discover git revision from repository",
        ),
        (
            EvaluationConfigurationError,
            "Git rev-parse returned empty revision",
        ),
    ],
)
def test_f5_cli_safe_translator_approved_pairs(
    exc_cls: type[Exception], msg: str
) -> None:
    cli_mod = _load_cli_module()
    exc = exc_cls(msg)
    assert cli_mod._safe_cli_error_message(exc) == msg


def test_f5_cli_safe_translator_fallbacks() -> None:
    cli_mod = _load_cli_module()

    assert (
        cli_mod._safe_cli_error_message(
            EvaluationArtifactError(
                "lineage.scenario_id.type: scenario_id must be a string"
            )
        )
        == "Evaluation artifact processing failed"
    )
    assert (
        cli_mod._safe_cli_error_message(
            RuntimeError("lineage.scenario_id.type: scenario_id must be a string")
        )
        == "Evaluation failed due to an unexpected internal error"
    )

    assert (
        cli_mod._safe_cli_error_message(
            EvaluationConfigurationError(
                "  lineage.scenario_id.type: scenario_id must be a string  "
            )
        )
        == "Evaluation configuration failed"
    )
    assert (
        cli_mod._safe_cli_error_message(
            EvaluationConfigurationError(
                "LINEAGE.SCENARIO_ID.TYPE: scenario_id must be a string"
            )
        )
        == "Evaluation configuration failed"
    )

    assert (
        cli_mod._safe_cli_error_message(EvaluationConfigurationError())
        == "Evaluation configuration failed"
    )
    assert (
        cli_mod._safe_cli_error_message(
            EvaluationConfigurationError(
                "lineage.scenario_id.type: scenario_id must be a string",
                "extra arg",
            )
        )
        == "Evaluation configuration failed"
    )
    assert (
        cli_mod._safe_cli_error_message(EvaluationConfigurationError(12345))
        == "Evaluation configuration failed"
    )

    class CustomStr(str):
        pass

    assert (
        cli_mod._safe_cli_error_message(
            EvaluationConfigurationError(
                CustomStr("lineage.scenario_id.type: scenario_id must be a string")
            )
        )
        == "Evaluation configuration failed"
    )

    assert (
        cli_mod._safe_cli_error_message(
            EvaluationConfigurationError("sensitive config error on /etc/shadow")
        )
        == "Evaluation configuration failed"
    )
    assert (
        cli_mod._safe_cli_error_message(
            EvaluationArtifactError("sensitive artifact error token=XYZ")
        )
        == "Evaluation artifact processing failed"
    )
    assert (
        cli_mod._safe_cli_error_message(
            EvaluationExecutionError("model execution crashed /tmp/path")
        )
        == "Evaluation execution failed"
    )

    try:
        MetricResult.model_validate({"metric_name": 123})
    except ValidationError as v_err:
        assert (
            cli_mod._safe_cli_error_message(v_err)
            == "Evaluation configuration validation failed"
        )

    class SubConfigError(EvaluationConfigurationError):
        pass

    class SubArtifactError(EvaluationArtifactError):
        pass

    class SubExecError(EvaluationExecutionError):
        pass

    assert (
        cli_mod._safe_cli_error_message(SubConfigError("test"))
        == "Evaluation failed due to an unexpected internal error"
    )
    assert (
        cli_mod._safe_cli_error_message(SubArtifactError("test"))
        == "Evaluation failed due to an unexpected internal error"
    )
    assert (
        cli_mod._safe_cli_error_message(SubExecError("test"))
        == "Evaluation failed due to an unexpected internal error"
    )

    assert (
        cli_mod._safe_cli_error_message(ValueError("invalid integer"))
        == "Evaluation failed due to an unexpected internal error"
    )
    assert (
        cli_mod._safe_cli_error_message(KeyError("missing_key"))
        == "Evaluation failed due to an unexpected internal error"
    )

    with pytest.raises(KeyboardInterrupt):
        cli_mod._safe_cli_error_message(KeyboardInterrupt())

    with pytest.raises(SystemExit):
        cli_mod._safe_cli_error_message(SystemExit(1))


def test_f5_cli_single_prefix_emitter(capsys: pytest.CaptureFixture[str]) -> None:
    cli_mod = _load_cli_module()
    cli_mod._emit_cli_error("test error message")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "Error: test error message\n"
    assert captured.err.count("Error: ") == 1


@pytest.mark.asyncio
async def test_f5_cli_fixed_preflight_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)

    (tmp_path / "research").mkdir(parents=True, exist_ok=True)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Invalid repository root structure\n"
    assert str(tmp_path) not in captured.err

    (tmp_path / "research").rmdir()
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Invalid repository root structure\n"

    (tmp_path / "research").mkdir(parents=True, exist_ok=True)

    exit_code = await cli_mod.main_async(["--artifact-root", "../outside"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Invalid artifact root path\n"
    assert "../outside" not in captured.err

    exit_code = await cli_mod.main_async(["--artifact-root", "C:/secrets"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Invalid artifact root path\n"
    assert "C:/secrets" not in captured.err

    exit_code = await cli_mod.main_async(["--artifact-root", "/var/log"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Invalid artifact root path\n"
    assert "/var/log" not in captured.err

    exit_code = await cli_mod.main_async(
        ["--calibration-seed", "42", "--evaluation-seed", "42"]
    )
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Calibration seed and evaluation seed must differ\n"

    def mock_exists_crash(self: Any) -> bool:
        raise RuntimeError("SECRET_SENTINEL_EXISTS_CRASH")

    monkeypatch.setattr(Path, "exists", mock_exists_crash)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert (
        captured.err == "Error: Evaluation failed due to an unexpected internal error\n"
    )
    assert "SECRET_SENTINEL" not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.asyncio
async def test_f5_cli_decision_threshold_matrix(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)

    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli_mod, "_get_git_revision", lambda: "rev-123")

    async def mock_run_eval(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _build_mock_evaluation_data(
            package_id=self.config.package_id,
            package_version=self.config.package_version,
            calibration_seed=self.config.calibration_seed,
            evaluation_seed=self.config.evaluation_seed,
        )

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run_eval)

    for valid_dt_str in ("0", "1", "0.0", "1.0", "0.65"):
        exit_code = await cli_mod.main_async(["--decision-threshold", valid_dt_str])
        assert exit_code == 0
        captured = capsys.readouterr()
        assert captured.err == ""

    class IntSub(int):
        pass

    class FloatSub(float):
        pass

    invalid_thresholds = [
        True,
        False,
        IntSub(1),
        FloatSub(0.5),
        float("nan"),
        float("inf"),
        float("-inf"),
        -0.1,
        1.1,
        10**1000,
        -(10**1000),
        "string_threshold",
        object(),
    ]

    for inv_dt in invalid_thresholds:
        monkeypatch.setattr(
            cli_mod,
            "parse_args",
            lambda argv=None, inv=inv_dt: argparse.Namespace(
                artifact_root="research",
                calibration_seed=41,
                evaluation_seed=42,
                decision_threshold=inv,
            ),
        )
        exit_code = await cli_mod.main_async([])
        assert exit_code == 1
        captured = capsys.readouterr()
        assert (
            captured.err
            == "Error: Decision threshold must be a finite float in [0.0, 1.0]\n"
        )


@pytest.mark.asyncio
async def test_f5_cli_git_revision_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import subprocess

    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)

    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)

    def mock_subprocess_cpe(*args: Any, **kwargs: Any) -> Any:
        raise subprocess.CalledProcessError(
            returncode=128, cmd=["git", "SECRET_TOKEN_REV_PARSE"]
        )

    monkeypatch.setattr(subprocess, "run", mock_subprocess_cpe)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Failed to discover git revision from repository\n"
    assert "SECRET_TOKEN" not in captured.err

    def mock_subprocess_fnf(*args: Any, **kwargs: Any) -> Any:
        raise FileNotFoundError("C:\\Windows\\System32\\git.exe not found")

    monkeypatch.setattr(subprocess, "run", mock_subprocess_fnf)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Failed to discover git revision from repository\n"
    assert "System32" not in captured.err

    mock_res_empty = subprocess.CompletedProcess(
        args=["git"], returncode=0, stdout="", stderr=""
    )
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: mock_res_empty)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Git rev-parse returned empty revision\n"

    mock_res_ws = subprocess.CompletedProcess(
        args=["git"], returncode=0, stdout="   \n\t  ", stderr=""
    )
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: mock_res_ws)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Git rev-parse returned empty revision\n"

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    with pytest.raises(KeyboardInterrupt):
        await cli_mod.main_async([])

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(SystemExit(123)),
    )
    with pytest.raises(SystemExit):
        await cli_mod.main_async([])


@pytest.mark.asyncio
async def test_f5_cli_operational_failures_fail_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)

    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli_mod, "_get_git_revision", lambda: "rev-123")

    def mock_cfg_fail(*args: Any, **kwargs: Any) -> Any:
        raise EvaluationConfigurationError("SECRET_CONFIG_FAILURE_SENTINEL")

    monkeypatch.setattr(
        cli_mod, "create_default_evaluation_harness_config", mock_cfg_fail
    )
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Evaluation configuration failed\n"
    assert "SECRET_CONFIG" not in captured.err
    assert "Traceback" not in captured.err

    monkeypatch.setattr(
        cli_mod,
        "create_default_evaluation_harness_config",
        create_default_evaluation_harness_config,
    )

    for exc, expected_err in [
        (
            EvaluationExecutionError("SECRET_EXEC_FAIL"),
            "Error: Evaluation execution failed\n",
        ),
        (
            EvaluationArtifactError("SECRET_ARTIFACT_FAIL"),
            "Error: Evaluation artifact processing failed\n",
        ),
        (
            RuntimeError("SECRET_INTERNAL_CRASH"),
            "Error: Evaluation failed due to an unexpected internal error\n",
        ),
    ]:

        async def mock_run_fail(*args: Any, e=exc, **kwargs: Any) -> Any:
            raise e

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run_fail)
        exit_code = await cli_mod.main_async([])
        assert exit_code == 1
        captured = capsys.readouterr()
        assert captured.err == expected_err
        assert "SECRET_" not in captured.err
        assert "SUCCESS" not in captured.out

    async def mock_run_missing_manifest(config: Any, base_dir: Any) -> Any:
        outcomes, summaries, meta = _build_mock_evaluation_data()
        manifest = EvaluationPackageManifest.model_construct(
            schema_version="1.0",
            package_id=config.package_id,
            package_version=config.package_version,
            code_revision=config.code_revision,
            created_at=datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc),
            environment=config.environment,
            calibration_seed=config.calibration_seed,
            evaluation_seed=config.evaluation_seed,
            decision_threshold=config.decision_policy.decision_threshold,
            models=list(CANONICAL_MODEL_ORDER),
            scenarios=list(CANONICAL_SCENARIO_ORDER),
            artifact_manifest_path="nonexistent/manifest.json",
            self_digest_policy="excluded_from_manifest_digest",
            artifacts=[],
        )
        return manifest, summaries

    monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run_missing_manifest)
    exit_code = await cli_mod.main_async([])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err == "Error: Evaluation artifact processing failed\n"
    assert "PHASE 3 TASK 3.9 EVALUATION HARNESS COMPLETE — SUCCESS" not in captured.out


def test_f5_cli_parser_and_base_exceptions() -> None:
    cli_mod = _load_cli_module()

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--help"])
    assert exc_info.value.code == 0

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--unknown-flag"])
    assert exc_info.value.code == 2

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--calibration-seed", "not_an_int"])
    assert exc_info.value.code == 2

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--decision-threshold", "not_a_float"])
    assert exc_info.value.code == 2


@pytest.mark.asyncio
async def test_f5_cli_complete_success_console_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)

    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli_mod, "_get_git_revision", lambda: "mock-cli-rev-123")

    captured_mock_summaries: list[ModelComparisonSummary] = []

    async def mock_run_eval(self: Any, *args: Any, **kwargs: Any) -> Any:
        outs, sums, meta = _build_mock_evaluation_data(
            package_id=self.config.package_id,
            package_version=self.config.package_version,
            calibration_seed=self.config.calibration_seed,
            evaluation_seed=self.config.evaluation_seed,
        )
        captured_mock_summaries.extend(sums)
        return outs, sums, meta

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run_eval)

    perf_counter_values = [100.0, 105.25]
    monkeypatch.setattr(
        time,
        "perf_counter",
        lambda: perf_counter_values.pop(0) if perf_counter_values else 105.25,
    )

    exit_code = await cli_mod.main_async(
        [
            "--artifact-root",
            "cli_test_root",
            "--calibration-seed",
            "33",
            "--evaluation-seed",
            "44",
            "--decision-threshold",
            "0.65",
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert captured.err == ""
    stdout_text = captured.out

    out_lines = stdout_text.splitlines()

    art_manifest_path = (
        tmp_path
        / "cli_test_root"
        / "results"
        / "processed"
        / "phase3"
        / "task_3_9"
        / "artifact_manifest.json"
    )
    assert art_manifest_path.exists()
    gen_manifest = json.loads(art_manifest_path.read_text(encoding="utf-8"))
    expected_art_manifest_sha = hashlib.sha256(
        art_manifest_path.read_bytes()
    ).hexdigest()

    expected_lines = [
        "======================================================================",
        "AEGISOPS PHASE 3 TASK 3.9 - EVALUATION HARNESS & COMPARISON",
        "======================================================================",
        "Code revision:       mock-cli-rev-123",
        "Calibration seed:    33",
        "Evaluation seed:     44",
        "Decision threshold:  0.65",
        "Canonical scenarios: 8 (cpu-saturation, memory-exhaustion, connection-exhaustion, dependency-latency, dependency-failure, error-rate-spike, traffic-surge, bad-deployment-config)",
        "Baseline models:     3 (prophet, isolation_forest, autoencoder)",
        "----------------------------------------------------------------------",
        "Running evaluation across all 8 canonical scenarios...",
        "Evaluation completed in 5.25s",
        "----------------------------------------------------------------------",
        "SUMMARY OF RESULTS (Micro Aggregations across 8 Canonical Scenarios):",
        f"{'Model':<20} {'TP':<5} {'FP':<5} {'TN':<5} {'FN':<5} {'Precision':<10} {'Recall':<10} {'F1':<10} {'FPR':<10} {'Detected':<10}",
        "-" * 95,
    ]
    for s in captured_mock_summaries:
        c = s.micro_confusion_counts
        p_str = (
            f"{s.micro_precision.value:.4f}"
            if s.micro_precision.value is not None
            else "undefined"
        )
        r_str = (
            f"{s.micro_recall.value:.4f}"
            if s.micro_recall.value is not None
            else "undefined"
        )
        f1_str = (
            f"{s.micro_f1.value:.4f}" if s.micro_f1.value is not None else "undefined"
        )
        fpr_str = (
            f"{s.micro_false_positive_rate.value:.4f}"
            if s.micro_false_positive_rate.value is not None
            else "undefined"
        )
        det_str = f"{s.latency_summary.contributing_scenario_count}/8"
        disp = s.model_name.replace("_", " ").title()
        expected_lines.append(
            f"{disp:<20} {c.true_positives:<5} {c.false_positives:<5} {c.true_negatives:<5} {c.false_negatives:<5} {p_str:<10} {r_str:<10} {f1_str:<10} {fpr_str:<10} {det_str:<10}"
        )
    expected_lines.extend(
        [
            "----------------------------------------------------------------------",
            "Generated complete 12-file evaluation package (11 manifest-digested + manifest):",
            "Manifest-digested artifacts (11 files):",
        ]
    )
    for art in gen_manifest["artifacts"]:
        expected_lines.append(f"  - {art['path']} (SHA256: {art['sha256'][:12]}...)")
    expected_lines.extend(
        [
            "Artifact manifest (excluded_from_manifest_digest):",
            f"  - {gen_manifest['artifact_manifest_path']} (SHA256: {expected_art_manifest_sha[:12]}...)",
            "======================================================================",
            "PHASE 3 TASK 3.9 EVALUATION HARNESS COMPLETE — SUCCESS",
            "======================================================================",
        ]
    )

    assert out_lines == expected_lines


def _make_dummy_config() -> Any:
    return create_default_evaluation_harness_config(
        code_revision="rev",
        calibration_seed=41,
        evaluation_seed=42,
        decision_threshold=0.5,
    )


# ======================================================================
# 7. TYPED RUNTIME LINEAGE AND ERROR TRANSLATOR CONTRACT TESTS (C3-F1B)
# ======================================================================


class _HostileLineageProbe:
    """Hostile test probe asserting that custom inspection methods are not invoked."""

    def __str__(self) -> str:
        raise AssertionError("__str__ must not be invoked on hostile lineage probe")

    def __repr__(self) -> str:
        raise AssertionError("__repr__ must not be invoked on hostile lineage probe")

    def __eq__(self, other: object) -> bool:
        raise AssertionError("__eq__ must not be invoked on hostile lineage probe")

    def __hash__(self) -> int:
        raise AssertionError("__hash__ must not be invoked on hostile lineage probe")


class _ListSubclass(list):
    pass


class _DictSubclass(dict):
    pass


class _StrSubclass(str):
    pass


class _TupleSubclass(tuple):
    pass


# 7.1 Model Contract Tests


def test_scenario_run_lineage_strict_validation() -> None:
    valid_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    lineage = ScenarioRunLineage(
        scenario_id="cpu-saturation",
        run_id=valid_uuid,
        seed=42,
    )
    assert lineage.scenario_id == "cpu-saturation"
    assert lineage.run_id == valid_uuid
    assert lineage.seed == 42

    # Immutability
    with pytest.raises(ValidationError):
        lineage.scenario_id = "memory-exhaustion"  # type: ignore[misc]

    # Seed boundaries
    assert ScenarioRunLineage(scenario_id="s", run_id=valid_uuid, seed=0).seed == 0
    assert (
        ScenarioRunLineage(scenario_id="s", run_id=valid_uuid, seed=MAX_UINT64).seed
        == MAX_UINT64
    )

    # scenario_id validation
    for invalid_scen in [
        None,
        123,
        True,
        1.5,
        ["cpu-saturation"],
        _StrSubclass("cpu-saturation"),
    ]:
        with pytest.raises(ValidationError):
            ScenarioRunLineage(
                scenario_id=invalid_scen,  # type: ignore[arg-type]
                run_id=valid_uuid,
                seed=42,
            )
    for blank_scen in ["", "   ", "\t\n"]:
        with pytest.raises(ValidationError):
            ScenarioRunLineage(scenario_id=blank_scen, run_id=valid_uuid, seed=42)

    # seed validation
    for invalid_seed in [
        True,
        False,
        41.0,
        1.5,
        "42",
        None,
        -1,
        MAX_UINT64 + 1,
        -999999999,
    ]:
        with pytest.raises(ValidationError):
            ScenarioRunLineage(
                scenario_id="s",
                run_id=valid_uuid,
                seed=invalid_seed,  # type: ignore[arg-type]
            )

    # run_id validation
    for invalid_run_id in [
        "11111111-1111-1111-1111-111111111111",
        12345,
        None,
        True,
        b"raw-bytes",
    ]:
        with pytest.raises(ValidationError):
            ScenarioRunLineage(
                scenario_id="s",
                run_id=invalid_run_id,  # type: ignore[arg-type]
                seed=42,
            )


def test_evaluation_run_metadata_strict_validation() -> None:
    valid_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    t_cutoff = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
    t_start = datetime(2026, 1, 1, 0, 6, 0, tzinfo=timezone.utc)
    single_run = ScenarioRunLineage(
        scenario_id="cpu-saturation", run_id=valid_uuid, seed=41
    )

    metadata = EvaluationRunMetadata(
        calibration_cutoff=t_cutoff,
        evaluation_start=t_start,
        calibration_runs=(single_run,),
        evaluation_runs=(single_run,),
    )
    assert metadata.calibration_cutoff == t_cutoff
    assert metadata.evaluation_start == t_start
    assert isinstance(metadata.calibration_runs, tuple)
    assert isinstance(metadata.evaluation_runs, tuple)

    # Immutability
    with pytest.raises(ValidationError):
        metadata.calibration_cutoff = t_start  # type: ignore[misc]

    # Naive and non-UTC timestamps
    naive_t = datetime(2026, 1, 1, 0, 5, 0)
    non_utc_t = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone(timedelta(hours=2)))
    for bad_t in [
        naive_t,
        non_utc_t,
        "2026-01-01T00:05:00Z",
        1735689900,
        True,
        False,
        None,
    ]:
        with pytest.raises(ValidationError):
            EvaluationRunMetadata(
                calibration_cutoff=bad_t,  # type: ignore[arg-type]
                evaluation_start=t_start,
                calibration_runs=(single_run,),
                evaluation_runs=(single_run,),
            )
        with pytest.raises(ValidationError):
            EvaluationRunMetadata(
                calibration_cutoff=t_cutoff,
                evaluation_start=bad_t,  # type: ignore[arg-type]
                calibration_runs=(single_run,),
                evaluation_runs=(single_run,),
            )

    # Run collections validation for both partitions
    for partition_field in ["calibration_runs", "evaluation_runs"]:
        for bad_collection in [(), [], [single_run], (123,), (None,)]:
            kwargs = {
                "calibration_cutoff": t_cutoff,
                "evaluation_start": t_start,
                "calibration_runs": (single_run,),
                "evaluation_runs": (single_run,),
            }
            kwargs[partition_field] = bad_collection  # type: ignore[assignment]
            with pytest.raises(ValidationError):
                EvaluationRunMetadata(**kwargs)  # type: ignore[arg-type]

    # JSON serialization
    dumped = metadata.model_dump(mode="json")
    assert isinstance(dumped["calibration_cutoff"], str)
    assert isinstance(dumped["evaluation_start"], str)
    assert isinstance(dumped["calibration_runs"], list)
    assert dumped["calibration_runs"][0]["run_id"] == str(valid_uuid)
    assert dumped["calibration_runs"][0]["scenario_id"] == "cpu-saturation"
    assert dumped["calibration_runs"][0]["seed"] == 41


# 7.2 Translator Map and Invariants Tests


def test_lineage_field_error_map_and_priority_invariants() -> None:
    assert len(LINEAGE_FIELD_ERROR_MAP) == 7
    assert len(LINEAGE_FIELD_ERROR_PRIORITY) == 7
    assert set(LINEAGE_FIELD_ERROR_MAP.keys()) == set(LINEAGE_FIELD_ERROR_PRIORITY)

    expected_messages = {
        ("lineage_scenario_id_type", ("scenario_id",)): (
            "lineage.scenario_id.type: scenario_id must be a string"
        ),
        ("lineage_scenario_id_empty", ("scenario_id",)): (
            "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only"
        ),
        ("lineage_seed_type", ("seed",)): (
            "lineage.seed.type: seed must be an exact integer"
        ),
        ("lineage_seed_range", ("seed",)): (
            "lineage.seed.range: seed must be within unsigned 64-bit range"
        ),
        ("lineage_run_id_type", ("run_id",)): (
            "lineage.run_id.type: run_id must be a UUID instance"
        ),
        ("lineage_timestamp_naive", ("calibration_cutoff",)): (
            "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
        ),
        ("lineage_timestamp_naive", ("evaluation_start",)): (
            "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
        ),
    }
    assert LINEAGE_FIELD_ERROR_MAP == expected_messages

    for key, msg in LINEAGE_FIELD_ERROR_MAP.items():
        assert len(msg) <= 160
        assert "{" not in msg and "}" not in msg


# 7.3 Sanitized Extraction Tests


def test_lineage_sanitized_extraction() -> None:
    sentinel = "R3D_LINEAGE_EXTRACTION_SECRET_SENTINEL"
    try:
        ScenarioRunLineage(
            scenario_id="cpu-saturation",
            run_id=uuid.uuid4(),
            seed=sentinel,  # type: ignore[arg-type]
        )
    except ValidationError as val_err:
        errors = _get_sanitized_lineage_errors(val_err)
        assert type(errors) is list  # noqa: E721
        assert len(errors) > 0
        for err in errors:
            assert type(err) is dict  # noqa: E721
            assert "input" not in err
            assert "ctx" not in err
            assert "url" not in err
            assert sentinel not in str(err)
            assert sentinel not in repr(err)
        assert sentinel not in str(errors)

    # Non-ValidationError input fails closed
    with pytest.raises(EvaluationExecutionError) as exc_info:
        _get_sanitized_lineage_errors(ValueError("ordinary error"))  # type: ignore[arg-type]
    assert str(exc_info.value) == "Evaluation lineage validation failed"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_raise_lineage_translation_fallback_directly() -> None:
    with pytest.raises(EvaluationExecutionError) as exc_info:
        _raise_lineage_translation_fallback()
    assert str(exc_info.value) == "Evaluation lineage validation failed"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_translate_lineage_validation_error_directly() -> None:
    try:
        ScenarioRunLineage(scenario_id="   ", run_id=uuid.uuid4(), seed=42)
    except ValidationError as val_err:
        with pytest.raises(EvaluationConfigurationError) as exc_info:
            _translate_lineage_validation_error(val_err)
        assert (
            str(exc_info.value)
            == "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only"
        )
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__suppress_context__ is True


# 7.4 Pure Translator Tests


def test_lineage_pure_translator_recognized_and_priority() -> None:
    # All 7 individually
    for key, expected_msg in LINEAGE_FIELD_ERROR_MAP.items():
        err_type, loc = key
        err_list = [{"type": err_type, "loc": loc, "msg": "dummy message"}]
        with pytest.raises(EvaluationConfigurationError) as exc_info:
            _translate_lineage_error_details(err_list)
        assert str(exc_info.value) == expected_msg
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__suppress_context__ is True

    # Priority independent of order
    multi_errors = [
        {"type": "lineage_run_id_type", "loc": ("run_id",)},
        {"type": "lineage_scenario_id_empty", "loc": ("scenario_id",)},
        {"type": "lineage_seed_type", "loc": ("seed",)},
    ]
    # In priority order: scenario_id_empty comes before seed_type and run_id_type
    with pytest.raises(EvaluationConfigurationError) as exc_info:
        _translate_lineage_error_details(multi_errors)
    assert (
        str(exc_info.value)
        == "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only"
    )
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True

    # Reverse input order
    with pytest.raises(EvaluationConfigurationError) as exc_info:
        _translate_lineage_error_details(list(reversed(multi_errors)))
    assert (
        str(exc_info.value)
        == "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only"
    )
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True

    # Duplicate recognized entries
    dup_errors = [
        {"type": "lineage_seed_type", "loc": ("seed",)},
        {"type": "lineage_seed_type", "loc": ("seed",)},
    ]
    with pytest.raises(EvaluationConfigurationError) as exc_info:
        _translate_lineage_error_details(dup_errors)
    assert str(exc_info.value) == "lineage.seed.type: seed must be an exact integer"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_lineage_pure_translator_fail_closed_on_malformed_and_unknown() -> None:
    recognized = {"type": "lineage_seed_type", "loc": ("seed",)}
    unknown = {"type": "unknown_code", "loc": ("seed",)}

    malformed_inputs: list[Any] = [
        [],  # Empty list
        (),  # Tuple
        _ListSubclass([recognized]),  # List subclass
        [123],  # Non-dict entry
        [_DictSubclass(recognized)],  # Dict subclass
        [{"type": "lineage_seed_type"}],  # Missing loc
        [{"loc": ("seed",)}],  # Missing type
        [{"type": 123, "loc": ("seed",)}],  # Non-string type
        [
            {"type": _StrSubclass("lineage_seed_type"), "loc": ("seed",)}
        ],  # String subclass
        [{"type": "lineage_seed_type", "loc": ["seed"]}],  # Non-tuple loc
        [
            {"type": "lineage_seed_type", "loc": _TupleSubclass(("seed",))}
        ],  # Tuple subclass
        [{"type": "lineage_seed_type", "loc": ()}],  # Empty loc
        [{"type": "lineage_seed_type", "loc": (True,)}],  # bool loc component
        [{"type": "lineage_seed_type", "loc": (1.5,)}],  # float loc component
        [
            {"type": "lineage_seed_type", "loc": (_HostileLineageProbe(),)}
        ],  # hostile loc
        [{"type": "value_error", "loc": ("seed",)}],  # Generic error
        [{"type": "lineage_seed_type", "loc": ("other_field",)}],  # Wrong loc
        [recognized, unknown],  # Known + unknown
        [unknown, recognized],  # Unknown + known
        [recognized, unknown, recognized],  # Unknown in middle
    ]

    for bad_input in malformed_inputs:
        with pytest.raises(EvaluationExecutionError) as exc_info:
            _translate_lineage_error_details(bad_input)
        assert str(exc_info.value) == "Evaluation lineage validation failed"
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__suppress_context__ is True


@pytest.mark.parametrize(
    ("param_id", "entry"),
    [
        (
            "correct_looking_msg_unknown_code",
            {
                "type": "unrecognized_seed_code",
                "loc": ("seed",),
                "msg": "lineage.seed.type: seed must be an exact integer",
            },
        ),
        (
            "uppercase_code",
            {
                "type": "LINEAGE_SEED_TYPE",
                "loc": ("seed",),
            },
        ),
        (
            "titlecase_code",
            {
                "type": "Lineage_Seed_Type",
                "loc": ("seed",),
            },
        ),
        (
            "prefix_code",
            {
                "type": "prefix_lineage_seed_type",
                "loc": ("seed",),
            },
        ),
        (
            "suffix_code",
            {
                "type": "lineage_seed_type_suffix",
                "loc": ("seed",),
            },
        ),
    ],
    ids=[
        "correct_looking_msg_unknown_code",
        "uppercase_code",
        "titlecase_code",
        "prefix_code",
        "suffix_code",
    ],
)
def test_lineage_pure_translator_untrusted_code_variations(
    param_id: str, entry: dict[str, Any]
) -> None:
    with pytest.raises(EvaluationExecutionError) as exc_info:
        _translate_lineage_error_details([entry])
    assert str(exc_info.value) == "Evaluation lineage validation failed"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


# 7.5 Factory Tests


def test_build_scenario_run_lineage_translated_errors() -> None:
    valid_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    sentinel = "R3D_LINEAGE_INPUT_SENTINEL"

    # Non-string scenario_id
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id=123, run_id=valid_uuid, seed=42)
    assert str(exc.value) == "lineage.scenario_id.type: scenario_id must be a string"
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Empty scenario_id
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id="   ", run_id=valid_uuid, seed=42)
    assert (
        str(exc.value)
        == "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # bool seed
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id="s", run_id=valid_uuid, seed=True)
    assert str(exc.value) == "lineage.seed.type: seed must be an exact integer"

    # float seed
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id="s", run_id=valid_uuid, seed=42.5)
    assert str(exc.value) == "lineage.seed.type: seed must be an exact integer"

    # string seed with sentinel
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id="s", run_id=valid_uuid, seed=sentinel)
    assert str(exc.value) == "lineage.seed.type: seed must be an exact integer"
    assert sentinel not in str(exc.value)
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Negative seed
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id="s", run_id=valid_uuid, seed=-1)
    assert (
        str(exc.value)
        == "lineage.seed.range: seed must be within unsigned 64-bit range"
    )

    # Overflow seed
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(
            scenario_id="s", run_id=valid_uuid, seed=MAX_UINT64 + 1
        )
    assert (
        str(exc.value)
        == "lineage.seed.range: seed must be within unsigned 64-bit range"
    )

    # Non-UUID run_id
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_scenario_run_lineage(scenario_id="s", run_id="str-uuid", seed=42)
    assert str(exc.value) == "lineage.run_id.type: run_id must be a UUID instance"

    # Valid construction
    res = _build_scenario_run_lineage(
        scenario_id="cpu-saturation", run_id=valid_uuid, seed=42
    )
    assert isinstance(res, ScenarioRunLineage)
    assert res.scenario_id == "cpu-saturation"


def test_build_evaluation_run_metadata_translated_errors() -> None:
    valid_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    t_utc = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
    t_naive = datetime(2026, 1, 1, 0, 5, 0)
    t_non_utc = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone(timedelta(hours=2)))
    single_run = ScenarioRunLineage(
        scenario_id="cpu-saturation", run_id=valid_uuid, seed=41
    )

    # Naive calibration_cutoff
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_evaluation_run_metadata(
            calibration_cutoff=t_naive,
            evaluation_start=t_utc,
            calibration_runs=(single_run,),
            evaluation_runs=(single_run,),
        )
    assert (
        str(exc.value)
        == "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Naive evaluation_start
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_evaluation_run_metadata(
            calibration_cutoff=t_utc,
            evaluation_start=t_naive,
            calibration_runs=(single_run,),
            evaluation_runs=(single_run,),
        )
    assert (
        str(exc.value)
        == "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
    )

    # Non-UTC timestamp
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_evaluation_run_metadata(
            calibration_cutoff=t_non_utc,
            evaluation_start=t_utc,
            calibration_runs=(single_run,),
            evaluation_runs=(single_run,),
        )
    assert (
        str(exc.value)
        == "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
    )

    # String timestamp
    with pytest.raises(EvaluationConfigurationError) as exc:
        _build_evaluation_run_metadata(
            calibration_cutoff="2026-01-01T00:05:00Z",
            evaluation_start=t_utc,
            calibration_runs=(single_run,),
            evaluation_runs=(single_run,),
        )
    assert (
        str(exc.value)
        == "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
    )

    # Valid construction
    res = _build_evaluation_run_metadata(
        calibration_cutoff=t_utc,
        evaluation_start=datetime(2026, 1, 1, 0, 6, 0, tzinfo=timezone.utc),
        calibration_runs=(single_run,),
        evaluation_runs=(single_run,),
    )
    assert isinstance(res, EvaluationRunMetadata)


# 7.6 Cross-Object Invariant Tests


def test_validate_evaluation_run_metadata_temporal_invariants() -> None:
    cfg = create_default_evaluation_harness_config()
    _, _, valid_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    # Valid metadata passes
    validate_evaluation_run_metadata(valid_metadata, cfg)

    # Equality fails
    bad_meta_equal = valid_metadata.model_copy(
        update={"evaluation_start": valid_metadata.calibration_cutoff}
    )
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta_equal, cfg)
    assert (
        str(exc.value)
        == "lineage.temporal.order: evaluation start must be strictly after calibration cutoff"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Earlier start fails
    bad_meta_earlier = valid_metadata.model_copy(
        update={
            "evaluation_start": valid_metadata.calibration_cutoff
            - timedelta(seconds=10)
        }
    )
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta_earlier, cfg)
    assert (
        str(exc.value)
        == "lineage.temporal.order: evaluation start must be strictly after calibration cutoff"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True


def test_validate_evaluation_run_metadata_scenario_completeness() -> None:
    cfg = create_default_evaluation_harness_config()
    _, _, valid_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    for partition_field, seed_value in [
        ("calibration_runs", cfg.calibration_seed),
        ("evaluation_runs", cfg.evaluation_seed),
    ]:
        valid_runs = getattr(valid_metadata, partition_field)

        # 1. Missing scenario
        missing_runs = valid_runs[:-1]
        bad_meta = valid_metadata.model_copy(update={partition_field: missing_runs})
        with pytest.raises(EvaluationConfigurationError) as exc:
            validate_evaluation_run_metadata(bad_meta, cfg)
        assert (
            str(exc.value)
            == "lineage.scenario.missing: configured scenario lineage is incomplete"
        )
        assert exc.value.__cause__ is None
        assert exc.value.__suppress_context__ is True

        # 2. Unknown scenario
        unknown_run = ScenarioRunLineage(
            scenario_id="unknown-scenario-xyz",
            run_id=uuid.uuid4(),
            seed=seed_value,
        )
        bad_meta = valid_metadata.model_copy(
            update={partition_field: (unknown_run, *valid_runs[1:])}
        )
        with pytest.raises(EvaluationConfigurationError) as exc:
            validate_evaluation_run_metadata(bad_meta, cfg)
        assert (
            str(exc.value)
            == "lineage.scenario.unknown: lineage contains an unconfigured scenario"
        )
        assert exc.value.__cause__ is None
        assert exc.value.__suppress_context__ is True

        # 3. Duplicate scenario
        dup_runs = (valid_runs[0], *valid_runs[:-1])
        bad_meta = valid_metadata.model_copy(update={partition_field: dup_runs})
        with pytest.raises(EvaluationConfigurationError) as exc:
            validate_evaluation_run_metadata(bad_meta, cfg)
        assert (
            str(exc.value)
            == "lineage.scenario.duplicate: lineage contains duplicate scenario entries"
        )
        assert exc.value.__cause__ is None
        assert exc.value.__suppress_context__ is True

        # 4. Noncanonical scenario order
        reversed_runs = tuple(reversed(valid_runs))
        bad_meta = valid_metadata.model_copy(update={partition_field: reversed_runs})
        with pytest.raises(EvaluationConfigurationError) as exc:
            validate_evaluation_run_metadata(bad_meta, cfg)
        assert (
            str(exc.value)
            == "lineage.scenario.order: lineage does not follow canonical scenario order"
        )
        assert exc.value.__cause__ is None
        assert exc.value.__suppress_context__ is True


def test_validate_evaluation_run_metadata_seed_alignment() -> None:
    cfg = create_default_evaluation_harness_config()
    _, _, valid_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    # Calibration seed mismatch
    bad_seed_cal = tuple(
        r.model_copy(update={"seed": 999}) if i == 0 else r
        for i, r in enumerate(valid_metadata.calibration_runs)
    )
    bad_meta = valid_metadata.model_copy(update={"calibration_runs": bad_seed_cal})
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta, cfg)
    assert (
        str(exc.value)
        == "lineage.seed.mismatch: lineage seed does not match configured partition seed"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Evaluation seed mismatch
    bad_seed_eval = tuple(
        r.model_copy(update={"seed": 999}) if i == 0 else r
        for i, r in enumerate(valid_metadata.evaluation_runs)
    )
    bad_meta = valid_metadata.model_copy(update={"evaluation_runs": bad_seed_eval})
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta, cfg)
    assert (
        str(exc.value)
        == "lineage.seed.mismatch: lineage seed does not match configured partition seed"
    )

    # Valid aligned seeds pass
    validate_evaluation_run_metadata(valid_metadata, cfg)


def test_validate_evaluation_run_metadata_derived_run_ids() -> None:
    cfg = create_default_evaluation_harness_config()
    _, _, valid_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )
    validate_evaluation_run_metadata(valid_metadata, cfg)

    expected_calibration_ids = tuple(
        derive_run_id(
            cfg.package_id,
            cfg.package_version,
            "calibration",
            scenario_id,
            cfg.calibration_seed,
        )
        for scenario_id in cfg.scenario_ids
    )

    expected_evaluation_ids = tuple(
        derive_run_id(
            cfg.package_id,
            cfg.package_version,
            "evaluation",
            scenario_id,
            cfg.evaluation_seed,
        )
        for scenario_id in cfg.scenario_ids
    )

    actual_calibration_ids = tuple(
        run.run_id for run in valid_metadata.calibration_runs
    )
    actual_evaluation_ids = tuple(run.run_id for run in valid_metadata.evaluation_runs)

    assert len(expected_calibration_ids) == len(CANONICAL_SCENARIO_ORDER)
    assert len(expected_evaluation_ids) == len(CANONICAL_SCENARIO_ORDER)
    assert actual_calibration_ids == expected_calibration_ids
    assert actual_evaluation_ids == expected_evaluation_ids

    assert tuple(run.scenario_id for run in valid_metadata.calibration_runs) == tuple(
        CANONICAL_SCENARIO_ORDER
    )

    assert tuple(run.scenario_id for run in valid_metadata.evaluation_runs) == tuple(
        CANONICAL_SCENARIO_ORDER
    )

    # Corrupted calibration run ID
    arbitrary_run_id = uuid.UUID("99999999-9999-9999-9999-999999999999")
    mismatched_cal = tuple(
        r.model_copy(update={"run_id": arbitrary_run_id}) if i == 0 else r
        for i, r in enumerate(valid_metadata.calibration_runs)
    )
    bad_meta = valid_metadata.model_copy(update={"calibration_runs": mismatched_cal})
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta, cfg)
    assert (
        str(exc.value)
        == "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Corrupted evaluation run ID
    mismatched_eval = tuple(
        r.model_copy(update={"run_id": arbitrary_run_id}) if i == 0 else r
        for i, r in enumerate(valid_metadata.evaluation_runs)
    )
    bad_meta = valid_metadata.model_copy(update={"evaluation_runs": mismatched_eval})
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta, cfg)
    assert (
        str(exc.value)
        == "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation"
    )


def test_validate_evaluation_run_metadata_partition_leakage() -> None:
    cfg = create_default_evaluation_harness_config()
    _, _, valid_metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    # Valid disjoint partition passes
    validate_evaluation_run_metadata(valid_metadata, cfg)

    # Calibration run ID copied into evaluation partition
    colliding_eval = tuple(
        r.model_copy(update={"run_id": valid_metadata.calibration_runs[0].run_id})
        if i == 0
        else r
        for i, r in enumerate(valid_metadata.evaluation_runs)
    )
    bad_meta = valid_metadata.model_copy(update={"evaluation_runs": colliding_eval})
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta, cfg)
    assert (
        str(exc.value)
        == "lineage.run_id.collision: calibration and evaluation partition run IDs overlap"
    )
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__ is True

    # Evaluation run ID copied into calibration partition
    colliding_cal = tuple(
        r.model_copy(update={"run_id": valid_metadata.evaluation_runs[0].run_id})
        if i == 0
        else r
        for i, r in enumerate(valid_metadata.calibration_runs)
    )
    bad_meta = valid_metadata.model_copy(update={"calibration_runs": colliding_cal})
    with pytest.raises(EvaluationConfigurationError) as exc:
        validate_evaluation_run_metadata(bad_meta, cfg)
    assert (
        str(exc.value)
        == "lineage.run_id.collision: calibration and evaluation partition run IDs overlap"
    )


# 7.7 Harness Migration and Pipeline Tests


@pytest.mark.asyncio
async def test_harness_run_evaluation_typed_metadata_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = create_default_evaluation_harness_config()
    harness = Phase3EvaluationHarness(config=cfg)

    # Run pipeline with real components on single scenario/model
    outcomes, summaries, run_metadata = await harness.run_evaluation()

    assert isinstance(run_metadata, EvaluationRunMetadata)
    assert len(run_metadata.calibration_runs) == len(cfg.scenario_ids)
    assert len(run_metadata.evaluation_runs) == len(cfg.scenario_ids)

    # Canonical scenario order and seeds
    for idx, sid in enumerate(cfg.scenario_ids):
        cal_r = run_metadata.calibration_runs[idx]
        eval_r = run_metadata.evaluation_runs[idx]

        assert cal_r.scenario_id == sid
        assert cal_r.seed == cfg.calibration_seed
        assert cal_r.run_id == derive_run_id(
            cfg.package_id,
            cfg.package_version,
            "calibration",
            sid,
            cfg.calibration_seed,
        )

        assert eval_r.scenario_id == sid
        assert eval_r.seed == cfg.evaluation_seed
        assert eval_r.run_id == derive_run_id(
            cfg.package_id,
            cfg.package_version,
            "evaluation",
            sid,
            cfg.evaluation_seed,
        )

    # Disjoint run-IDs
    cal_ids = {r.run_id for r in run_metadata.calibration_runs}
    eval_ids = {r.run_id for r in run_metadata.evaluation_runs}
    assert bool(cal_ids & eval_ids) is False

    # Temporal ordering
    assert run_metadata.evaluation_start > run_metadata.calibration_cutoff
    assert run_metadata.calibration_cutoff.tzinfo is not None
    assert run_metadata.evaluation_start.tzinfo is not None

    # Cross-object validator succeeds on real output
    validate_evaluation_run_metadata(run_metadata, cfg)

    # Serialization compatibility
    dumped = run_metadata.model_dump(mode="json")
    for key in [
        "calibration_cutoff",
        "evaluation_start",
        "calibration_runs",
        "evaluation_runs",
    ]:
        assert key in dumped

    # Validation occurs before writes on injected invalid metadata
    invalid_meta = run_metadata.model_copy(
        update={"evaluation_start": run_metadata.calibration_cutoff}
    )

    async def mock_run_invalid(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, invalid_meta

    target_dir = tmp_path / "preflight_fail_sandbox"

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run_invalid)
    with pytest.raises(EvaluationConfigurationError, match="lineage.temporal.order"):
        await run_phase3_evaluation(cfg, base_dir=target_dir)

    assert not target_dir.exists()


@pytest.mark.asyncio
async def test_model_comparison_json_serialization_compatibility(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = create_default_evaluation_harness_config()
    outcomes, summaries, metadata = _build_mock_evaluation_data(
        package_id=cfg.package_id,
        package_version=cfg.package_version,
        calibration_seed=cfg.calibration_seed,
        evaluation_seed=cfg.evaluation_seed,
    )

    async def mock_run(*args: Any, **kwargs: Any) -> Any:
        return outcomes, summaries, metadata

    monkeypatch.setattr(Phase3EvaluationHarness, "run_evaluation", mock_run)

    fixed_time = datetime(2026, 10, 3, 10, 30, 0, tzinfo=timezone.utc)
    await run_phase3_evaluation(cfg, base_dir=tmp_path, package_created_at=fixed_time)

    json_path = (
        tmp_path
        / cfg.artifact_root
        / "results"
        / "processed"
        / "phase3"
        / "task_3_9"
        / "model_comparison.json"
    )
    assert json_path.exists()

    def strict_constant(c: str) -> None:
        raise ValueError(f"Non-finite constant: {c}")

    parsed = json.loads(
        json_path.read_text(encoding="utf-8"), parse_constant=strict_constant
    )

    # Top-level contract
    assert parsed["schema_version"] == SUPPORTED_EVALUATION_SCHEMA_VERSION
    assert parsed["package_id"] == cfg.package_id
    assert parsed["package_version"] == cfg.package_version
    assert parsed["code_revision"] == cfg.code_revision
    assert parsed["decision_threshold"] == cfg.decision_policy.decision_threshold
    assert parsed["created_at"] == fixed_time.isoformat()
    assert len(parsed["models"]) == len(CANONICAL_MODEL_ORDER)

    # Metadata contract
    assert parsed["metadata"]["calibration_seed"] == cfg.calibration_seed
    assert parsed["metadata"]["evaluation_seed"] == cfg.evaluation_seed
    assert (
        parsed["metadata"]["calibration_partition_id"] == cfg.calibration_partition_id
    )
    assert parsed["metadata"]["evaluation_partition_id"] == cfg.evaluation_partition_id
    assert parsed["metadata"]["artifact_root"] == str(cfg.artifact_root)
    assert parsed["metadata"]["scenario_ids"] == list(CANONICAL_SCENARIO_ORDER)
    assert parsed["metadata"]["model_names"] == list(CANONICAL_MODEL_ORDER)

    # Typed nested configuration
    assert parsed["metadata"]["decision_policy"] == cfg.decision_policy.model_dump(
        mode="json"
    )
    assert parsed["metadata"]["label_policy"] == cfg.label_policy.model_dump(
        mode="json"
    )
    assert parsed["metadata"]["feature_window"] == cfg.feature_window.model_dump(
        mode="json"
    )
    assert parsed["metadata"][
        "calibration_config"
    ] == cfg.calibration_config.model_dump(mode="json")

    # Runtime lineage
    cal_cutoff_str = parsed["metadata"]["calibration_cutoff"]
    eval_start_str = parsed["metadata"]["evaluation_start"]
    assert cal_cutoff_str.endswith("+00:00") or cal_cutoff_str.endswith("Z")
    assert eval_start_str.endswith("+00:00") or eval_start_str.endswith("Z")

    # Verify UTC timestamps match
    if cal_cutoff_str.endswith("Z"):
        cal_cutoff_str = cal_cutoff_str[:-1] + "+00:00"
    if eval_start_str.endswith("Z"):
        eval_start_str = eval_start_str[:-1] + "+00:00"
    assert cal_cutoff_str == metadata.calibration_cutoff.isoformat()
    assert eval_start_str == metadata.evaluation_start.isoformat()

    assert len(parsed["metadata"]["calibration_runs"]) == 8
    assert len(parsed["metadata"]["evaluation_runs"]) == 8

    for idx, scenario_id in enumerate(CANONICAL_SCENARIO_ORDER):
        cal_run = parsed["metadata"]["calibration_runs"][idx]
        eval_run = parsed["metadata"]["evaluation_runs"][idx]
        assert cal_run["scenario_id"] == scenario_id
        assert eval_run["scenario_id"] == scenario_id

        cal_run_obj = metadata.calibration_runs[idx]
        eval_run_obj = metadata.evaluation_runs[idx]
        assert cal_run["run_id"] == str(cal_run_obj.run_id)
        assert eval_run["run_id"] == str(eval_run_obj.run_id)

        # UUID validation
        cal_uuid = uuid.UUID(cal_run["run_id"])
        eval_uuid = uuid.UUID(eval_run["run_id"])
        assert str(cal_uuid) == cal_run["run_id"]
        assert str(eval_uuid) == eval_run["run_id"]

    # Calibration cutoff duration
    assert (
        parsed["metadata"]["calibration_cutoff_seconds"]
        == cfg.calibration_cutoff_seconds
    )

    # Stored canonical-order fields
    assert parsed["metadata"]["canonical_scenario_order"] == list(
        CANONICAL_SCENARIO_ORDER
    )
    assert parsed["metadata"]["canonical_model_order"] == list(CANONICAL_MODEL_ORDER)

    # Actual serialized model order
    assert [model_summary["model_name"] for model_summary in parsed["models"]] == list(
        CANONICAL_MODEL_ORDER
    )

    # Complete serialized run equality
    expected_calibration_runs = [
        run.model_dump(mode="json") for run in metadata.calibration_runs
    ]
    expected_evaluation_runs = [
        run.model_dump(mode="json") for run in metadata.evaluation_runs
    ]

    assert parsed["metadata"]["calibration_runs"] == expected_calibration_runs
    assert parsed["metadata"]["evaluation_runs"] == expected_evaluation_runs

    # Partition disjointness
    cal_ids = {r["run_id"] for r in parsed["metadata"]["calibration_runs"]}
    eval_ids = {r["run_id"] for r in parsed["metadata"]["evaluation_runs"]}
    assert len(cal_ids & eval_ids) == 0


def _independent_linear_quantile(sorted_values: list[float], q: float) -> float:
    if len(sorted_values) == 1 or q <= 0.0:
        return sorted_values[0]
    if q >= 1.0:
        return sorted_values[-1]
    index = q * (len(sorted_values) - 1)
    lower = math.floor(index)
    fraction = index - lower
    return sorted_values[lower] + fraction * (
        sorted_values[lower + 1] - sorted_values[lower]
    )


def _independent_score_distribution(scores: list[float], name: str) -> dict[str, Any]:
    if not scores:
        return {
            "distribution_name": name,
            "count": 0,
            "status": "empty",
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "p25": None,
            "p75": None,
            "p95": None,
        }
    sorted_scores = sorted(scores)
    return {
        "distribution_name": name,
        "count": len(sorted_scores),
        "status": "defined",
        "min": round(sorted_scores[0], 6),
        "max": round(sorted_scores[-1], 6),
        "mean": round(sum(sorted_scores) / len(sorted_scores), 6),
        "median": round(_independent_linear_quantile(sorted_scores, 0.50), 6),
        "p25": round(_independent_linear_quantile(sorted_scores, 0.25), 6),
        "p75": round(_independent_linear_quantile(sorted_scores, 0.75), 6),
        "p95": round(_independent_linear_quantile(sorted_scores, 0.95), 6),
    }


def _independent_confusion_counts(outcomes: list[dict[str, Any]]) -> dict[str, int]:
    tp = sum(1 for o in outcomes if o["is_true_positive"])
    fp = sum(1 for o in outcomes if o["is_false_positive"])
    tn = sum(1 for o in outcomes if o["is_true_negative"])
    fn = sum(1 for o in outcomes if o["is_false_negative"])
    tot = len(outcomes)
    succ = sum(1 for o in outcomes if o["status"] == "success")
    insuf = sum(
        1 for o in outcomes if o["status"] in ("insufficient_data", "not_applicable")
    )
    fail = sum(
        1
        for o in outcomes
        if o["status"]
        in (
            "fit_failure",
            "model_failure",
            "non_convergence",
            "invalid_input",
            "missing_calibration",
        )
    )
    return {
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "total_units": tot,
        "evaluable_units": tot,
        "success_units": succ,
        "insufficient_units": insuf,
        "failure_units": fail,
    }


def _independent_precision(tp: int, fp: int) -> dict[str, Any]:
    denom = tp + fp
    if denom == 0:
        return {
            "metric_name": "precision",
            "value": None,
            "status": "undefined",
            "reason": "zero_denominator: TP + FP == 0",
            "numerator": float(tp),
            "denominator": 0.0,
        }
    return {
        "metric_name": "precision",
        "value": round(tp / denom, 6),
        "status": "defined",
        "reason": None,
        "numerator": float(tp),
        "denominator": float(denom),
    }


def _independent_recall(tp: int, fn: int) -> dict[str, Any]:
    denom = tp + fn
    if denom == 0:
        return {
            "metric_name": "recall",
            "value": None,
            "status": "undefined",
            "reason": "zero_denominator: TP + FN == 0",
            "numerator": float(tp),
            "denominator": 0.0,
        }
    return {
        "metric_name": "recall",
        "value": round(tp / denom, 6),
        "status": "defined",
        "reason": None,
        "numerator": float(tp),
        "denominator": float(denom),
    }


def _independent_f1(
    prec_result: dict[str, Any], rec_result: dict[str, Any]
) -> dict[str, Any]:
    if (
        prec_result["status"] == "undefined"
        or rec_result["status"] == "undefined"
        or prec_result["value"] is None
        or rec_result["value"] is None
    ):
        return {
            "metric_name": "f1",
            "value": None,
            "status": "undefined",
            "reason": "undefined_component: precision or recall is undefined",
            "numerator": None,
            "denominator": None,
        }
    p_val = prec_result["value"]
    r_val = rec_result["value"]
    denom = p_val + r_val
    if denom == 0.0:
        return {
            "metric_name": "f1",
            "value": None,
            "status": "undefined",
            "reason": "zero_denominator: precision + recall == 0.0",
            "numerator": 0.0,
            "denominator": 0.0,
        }
    return {
        "metric_name": "f1",
        "value": round(2 * p_val * r_val / denom, 6),
        "status": "defined",
        "reason": None,
        "numerator": round(2 * p_val * r_val, 6),
        "denominator": round(denom, 6),
    }


def _independent_fpr(fp: int, tn: int) -> dict[str, Any]:
    denom = fp + tn
    if denom == 0:
        return {
            "metric_name": "false_positive_rate",
            "value": None,
            "status": "undefined",
            "reason": "zero_denominator: FP + TN == 0",
            "numerator": float(fp),
            "denominator": 0.0,
        }
    return {
        "metric_name": "false_positive_rate",
        "value": round(fp / denom, 6),
        "status": "defined",
        "reason": None,
        "numerator": float(fp),
        "denominator": float(denom),
    }


def _independent_detection_latency(
    outcomes: list[dict[str, Any]],
    truth_activation_time_str: str,
    scenario_id: str,
    model_name: str,
) -> dict[str, Any]:
    truth_activation_dt = datetime.fromisoformat(
        truth_activation_time_str.replace("Z", "+00:00")
    )
    tp_outcomes = [o for o in outcomes if o["is_true_positive"]]
    if not tp_outcomes:
        has_success = any(o["status"] == "success" for o in outcomes)
        if not outcomes:
            status = "insufficient_data"
        elif not has_success:
            status = "failed"
        else:
            status = "not_detected"
        return {
            "scenario_id": scenario_id,
            "model_name": model_name,
            "status": status,
            "latency_seconds": None,
            "first_true_positive_time": None,
            "truth_activation_time": truth_activation_time_str,
            "details": "No true positive detections recorded during active scenario window",
        }

    sorted_tp = sorted(
        tp_outcomes,
        key=lambda o: datetime.fromisoformat(o["event_time"].replace("Z", "+00:00")),
    )
    first_tp = sorted_tp[0]
    first_tp_dt = datetime.fromisoformat(first_tp["event_time"].replace("Z", "+00:00"))

    latency_sec = round((first_tp_dt - truth_activation_dt).total_seconds(), 6)
    first_tp_iso = first_tp_dt.isoformat()
    return {
        "scenario_id": scenario_id,
        "model_name": model_name,
        "status": "detected",
        "latency_seconds": latency_sec,
        "first_true_positive_time": first_tp["event_time"],
        "truth_activation_time": truth_activation_time_str,
        "details": f"First true positive detected at {first_tp_iso}",
    }


def test_raw_to_processed_independent_recomputation() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = (
        repo_root / "research/experiments/phase3/task_3_9/comparison_manifest.json"
    )
    raw_path = (
        repo_root / "research/results/raw/phase3/task_3_9/evaluation_outcomes.jsonl"
    )
    processed_json_path = (
        repo_root / "research/results/processed/phase3/task_3_9/model_comparison.json"
    )
    model_csv_path = (
        repo_root / "research/results/processed/phase3/task_3_9/model_comparison.csv"
    )
    scenario_csv_path = (
        repo_root / "research/results/processed/phase3/task_3_9/scenario_comparison.csv"
    )
    report_path = repo_root / "research/reports/phase3/task_3_9/evaluation_report.md"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert isinstance(manifest, dict)

    raw_lines = [
        line.strip()
        for line in raw_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    raw_outcomes = [json.loads(line) for line in raw_lines]
    assert len(raw_outcomes) == 720

    for o in raw_outcomes:
        assert isinstance(o, dict)
        for req_field in (
            "outcome_id",
            "unit_id",
            "unit_index",
            "scenario_id",
            "run_id",
            "model_name",
            "model_version",
            "event_time",
            "ground_truth_positive",
            "status",
            "predicted_positive",
            "is_true_positive",
            "is_false_positive",
            "is_true_negative",
            "is_false_negative",
            "details",
        ):
            assert req_field in o
        assert isinstance(o["unit_index"], int) and not isinstance(
            o["unit_index"], bool
        )
        assert o["unit_index"] >= 0
        if o["raw_score"] is not None:
            assert isinstance(o["raw_score"], (int, float)) and not isinstance(
                o["raw_score"], bool
            )
            assert math.isfinite(o["raw_score"])
        if o["baseline_normalized_score"] is not None:
            assert isinstance(
                o["baseline_normalized_score"], (int, float)
            ) and not isinstance(o["baseline_normalized_score"], bool)
            assert (
                math.isfinite(o["baseline_normalized_score"])
                and 0.0 <= o["baseline_normalized_score"] <= 1.0
            )
        if o["calibrated_score"] is not None:
            assert isinstance(o["calibrated_score"], (int, float)) and not isinstance(
                o["calibrated_score"], bool
            )
            assert (
                math.isfinite(o["calibrated_score"])
                and 0.0 <= o["calibrated_score"] <= 1.0
            )

    processed_json = json.loads(processed_json_path.read_text(encoding="utf-8"))
    assert isinstance(processed_json, dict)

    with model_csv_path.open(encoding="utf-8") as f:
        model_csv_rows = list(csv.DictReader(f))

    with scenario_csv_path.open(encoding="utf-8") as f:
        scenario_csv_rows = list(csv.DictReader(f))

    report_content = report_path.read_text(encoding="utf-8")

    outcome_ids = [o["outcome_id"] for o in raw_outcomes]
    assert len(set(outcome_ids)) == 720 == len(raw_outcomes)

    logical_unit_keys = {
        (o["scenario_id"], o["run_id"], o["unit_id"], o["unit_index"])
        for o in raw_outcomes
    }
    assert len(logical_unit_keys) == 240

    logical_slot_keys = {
        (
            o["scenario_id"],
            o["run_id"],
            o["unit_id"],
            o["unit_index"],
            o["model_name"],
        )
        for o in raw_outcomes
    }
    assert len(logical_slot_keys) == 720

    duplicate_slots = len(raw_outcomes) - len(logical_slot_keys)
    assert duplicate_slots == 0

    expected_slots = len(logical_unit_keys) * len(manifest["model_names"])
    assert expected_slots == 720

    missing_slots = expected_slots - len(logical_slot_keys)
    assert missing_slots == 0

    assert {o["model_name"] for o in raw_outcomes} == set(manifest["model_names"])
    assert {o["scenario_id"] for o in raw_outcomes} == set(manifest["scenario_ids"])

    for u_key in logical_unit_keys:
        u_outcomes = [
            o
            for o in raw_outcomes
            if (o["scenario_id"], o["run_id"], o["unit_id"], o["unit_index"]) == u_key
        ]
        assert len(u_outcomes) == 3
        assert {o["model_name"] for o in u_outcomes} == set(manifest["model_names"])

    assert all(
        sum(1 for o in raw_outcomes if o["model_name"] == m) == 240
        for m in manifest["model_names"]
    )
    assert all(
        sum(1 for o in raw_outcomes if o["scenario_id"] == s) == 90
        for s in manifest["scenario_ids"]
    )
    assert all(
        sum(1 for o in raw_outcomes if o["scenario_id"] == s and o["model_name"] == m)
        == 30
        for s in manifest["scenario_ids"]
        for m in manifest["model_names"]
    )

    for o in raw_outcomes:
        gt = bool(o["ground_truth_positive"])
        pred = bool(o["predicted_positive"])
        tp = gt and pred
        fp = (not gt) and pred
        tn = (not gt) and (not pred)
        fn = gt and (not pred)
        assert o["is_true_positive"] is tp
        assert o["is_false_positive"] is fp
        assert o["is_true_negative"] is tn
        assert o["is_false_negative"] is fn
        assert sum([int(tp), int(fp), int(tn), int(fn)]) == 1

    status_counts = collections.Counter(o["status"] for o in raw_outcomes)
    assert status_counts["success"] == 600
    assert status_counts["insufficient_data"] == 120
    assert status_counts["missing_calibration"] == 0
    assert status_counts["non_convergence"] == 0
    assert status_counts["fit_failure"] == 0
    assert status_counts["invalid_input"] == 0
    assert status_counts["model_failure"] == 0
    assert status_counts["not_applicable"] == 0
    assert sum(status_counts.values()) == 720

    scenario_truth_activation: dict[str, str] = {}
    for s_id in manifest["scenario_ids"]:
        gt_pos = [
            o
            for o in raw_outcomes
            if o["scenario_id"] == s_id and o["ground_truth_positive"]
        ]
        assert len(gt_pos) > 0
        min_ws = min(o["details"]["window_start"] for o in gt_pos)
        dt = datetime.fromisoformat(min_ws.replace("Z", "+00:00"))
        scenario_truth_activation[s_id] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    expected_models: list[dict[str, Any]] = []

    for m_name in manifest["model_names"]:
        m_outcomes = [o for o in raw_outcomes if o["model_name"] == m_name]
        assert len(m_outcomes) == 240

        scen_evals: list[dict[str, Any]] = []
        for s_id in manifest["scenario_ids"]:
            sm_outcomes = [
                o
                for o in raw_outcomes
                if o["model_name"] == m_name and o["scenario_id"] == s_id
            ]
            assert len(sm_outcomes) == 30
            c_counts = _independent_confusion_counts(sm_outcomes)
            prec = _independent_precision(
                c_counts["true_positives"], c_counts["false_positives"]
            )
            rec = _independent_recall(
                c_counts["true_positives"], c_counts["false_negatives"]
            )
            f1 = _independent_f1(prec, rec)
            fpr = _independent_fpr(
                c_counts["false_positives"], c_counts["true_negatives"]
            )
            lat = _independent_detection_latency(
                sm_outcomes, scenario_truth_activation[s_id], s_id, m_name
            )

            gt_p_scores = [
                float(o["calibrated_score"])
                for o in sm_outcomes
                if o["ground_truth_positive"] and o["calibrated_score"] is not None
            ]
            gt_n_scores = [
                float(o["calibrated_score"])
                for o in sm_outcomes
                if not o["ground_truth_positive"] and o["calibrated_score"] is not None
            ]
            pr_p_scores = [
                float(o["calibrated_score"])
                for o in sm_outcomes
                if o["predicted_positive"] and o["calibrated_score"] is not None
            ]
            pr_n_scores = [
                float(o["calibrated_score"])
                for o in sm_outcomes
                if not o["predicted_positive"] and o["calibrated_score"] is not None
            ]

            se_dict = {
                "scenario_id": s_id,
                "model_name": m_name,
                "model_version": sm_outcomes[0]["model_version"],
                "confusion_counts": c_counts,
                "precision": prec,
                "recall": rec,
                "f1": f1,
                "false_positive_rate": fpr,
                "detection_latency": lat,
                "truth_positive_distribution": _independent_score_distribution(
                    gt_p_scores, "truth_positive"
                ),
                "truth_negative_distribution": _independent_score_distribution(
                    gt_n_scores, "truth_negative"
                ),
                "predicted_positive_distribution": _independent_score_distribution(
                    pr_p_scores, "predicted_positive"
                ),
                "predicted_negative_distribution": _independent_score_distribution(
                    pr_n_scores, "predicted_negative"
                ),
                "status": "completed",
            }
            scen_evals.append(se_dict)

        micro_counts = _independent_confusion_counts(m_outcomes)
        micro_prec = _independent_precision(
            micro_counts["true_positives"], micro_counts["false_positives"]
        )
        micro_rec = _independent_recall(
            micro_counts["true_positives"], micro_counts["false_negatives"]
        )
        micro_f1 = _independent_f1(micro_prec, micro_rec)
        micro_fpr = _independent_fpr(
            micro_counts["false_positives"], micro_counts["true_negatives"]
        )

        def_prec_vals = [
            se["precision"]["value"]
            for se in scen_evals
            if se["precision"]["status"] == "defined"
            and se["precision"]["value"] is not None
        ]
        def_rec_vals = [
            se["recall"]["value"]
            for se in scen_evals
            if se["recall"]["status"] == "defined" and se["recall"]["value"] is not None
        ]
        def_f1_vals = [
            se["f1"]["value"]
            for se in scen_evals
            if se["f1"]["status"] == "defined" and se["f1"]["value"] is not None
        ]
        def_fpr_vals = [
            se["false_positive_rate"]["value"]
            for se in scen_evals
            if se["false_positive_rate"]["status"] == "defined"
            and se["false_positive_rate"]["value"] is not None
        ]

        macro_prec = (
            {
                "metric_name": "macro_precision",
                "value": round(sum(def_prec_vals) / len(def_prec_vals), 6),
                "status": "defined",
                "reason": None,
                "numerator": sum(def_prec_vals),
                "denominator": float(len(def_prec_vals)),
            }
            if def_prec_vals
            else {
                "metric_name": "macro_precision",
                "value": None,
                "status": "undefined",
                "reason": "no_defined_scenario_precisions",
                "numerator": None,
                "denominator": None,
            }
        )

        macro_rec = (
            {
                "metric_name": "macro_recall",
                "value": round(sum(def_rec_vals) / len(def_rec_vals), 6),
                "status": "defined",
                "reason": None,
                "numerator": sum(def_rec_vals),
                "denominator": float(len(def_rec_vals)),
            }
            if def_rec_vals
            else {
                "metric_name": "macro_recall",
                "value": None,
                "status": "undefined",
                "reason": "no_defined_scenario_recalls",
                "numerator": None,
                "denominator": None,
            }
        )

        macro_f1 = (
            {
                "metric_name": "macro_f1",
                "value": round(sum(def_f1_vals) / len(def_f1_vals), 6),
                "status": "defined",
                "reason": None,
                "numerator": sum(def_f1_vals),
                "denominator": float(len(def_f1_vals)),
            }
            if def_f1_vals
            else {
                "metric_name": "macro_f1",
                "value": None,
                "status": "undefined",
                "reason": "no_defined_scenario_f1s",
                "numerator": None,
                "denominator": None,
            }
        )

        macro_fpr = (
            {
                "metric_name": "macro_false_positive_rate",
                "value": round(sum(def_fpr_vals) / len(def_fpr_vals), 6),
                "status": "defined",
                "reason": None,
                "numerator": sum(def_fpr_vals),
                "denominator": float(len(def_fpr_vals)),
            }
            if def_fpr_vals
            else {
                "metric_name": "macro_false_positive_rate",
                "value": None,
                "status": "undefined",
                "reason": "no_defined_scenario_fprs",
                "numerator": None,
                "denominator": None,
            }
        )

        detected_latencies = [
            se["detection_latency"]["latency_seconds"]
            for se in scen_evals
            if se["detection_latency"]["status"] == "detected"
            and se["detection_latency"]["latency_seconds"] is not None
        ]
        missing_count = len(scen_evals) - len(detected_latencies)

        if detected_latencies:
            lat_summary = {
                "total_scenario_count": len(scen_evals),
                "contributing_scenario_count": len(detected_latencies),
                "missing_detection_count": missing_count,
                "min_seconds": round(min(detected_latencies), 6),
                "mean_seconds": round(
                    sum(detected_latencies) / len(detected_latencies), 6
                ),
                "median_seconds": round(
                    _independent_linear_quantile(sorted(detected_latencies), 0.50), 6
                ),
                "p95_seconds": round(
                    _independent_linear_quantile(sorted(detected_latencies), 0.95), 6
                ),
                "max_seconds": round(max(detected_latencies), 6),
            }
        else:
            lat_summary = {
                "total_scenario_count": len(scen_evals),
                "contributing_scenario_count": 0,
                "missing_detection_count": missing_count,
                "min_seconds": None,
                "mean_seconds": None,
                "median_seconds": None,
                "p95_seconds": None,
                "max_seconds": None,
            }

        req_scenarios = list(manifest["scenario_ids"])
        exec_scenarios = [se["scenario_id"] for se in scen_evals]
        feat_scenarios = exec_scenarios
        scored_scenarios = [
            se["scenario_id"]
            for se in scen_evals
            if se["confusion_counts"]["success_units"] > 0
        ]
        eval_scenarios = exec_scenarios
        det_scenarios = [
            se["scenario_id"]
            for se in scen_evals
            if se["detection_latency"]["status"] == "detected"
        ]
        insuf_scenarios = [
            se["scenario_id"]
            for se in scen_evals
            if se["confusion_counts"]["insufficient_units"] > 0
        ]
        fail_scenarios = [
            se["scenario_id"]
            for se in scen_evals
            if se["confusion_counts"]["failure_units"] > 0
        ]

        coverage = {
            "requested_count": len(req_scenarios),
            "requested_scenario_ids": req_scenarios,
            "executed_count": len(exec_scenarios),
            "executed_scenario_ids": exec_scenarios,
            "feature_ready_count": len(feat_scenarios),
            "feature_ready_scenario_ids": feat_scenarios,
            "scored_count": len(scored_scenarios),
            "scored_scenario_ids": scored_scenarios,
            "evaluable_count": len(eval_scenarios),
            "evaluable_scenario_ids": eval_scenarios,
            "detected_count": len(det_scenarios),
            "detected_scenario_ids": det_scenarios,
            "insufficient_count": len(insuf_scenarios),
            "insufficient_scenario_ids": insuf_scenarios,
            "failed_count": len(fail_scenarios),
            "failed_scenario_ids": fail_scenarios,
        }

        m_gt_p_scores = [
            float(o["calibrated_score"])
            for o in m_outcomes
            if o["ground_truth_positive"] and o["calibrated_score"] is not None
        ]
        m_gt_n_scores = [
            float(o["calibrated_score"])
            for o in m_outcomes
            if not o["ground_truth_positive"] and o["calibrated_score"] is not None
        ]
        m_pr_p_scores = [
            float(o["calibrated_score"])
            for o in m_outcomes
            if o["predicted_positive"] and o["calibrated_score"] is not None
        ]
        m_pr_n_scores = [
            float(o["calibrated_score"])
            for o in m_outcomes
            if not o["predicted_positive"] and o["calibrated_score"] is not None
        ]

        model_summary = {
            "model_name": m_name,
            "model_version": scen_evals[0]["model_version"],
            "micro_confusion_counts": micro_counts,
            "micro_precision": micro_prec,
            "micro_recall": micro_rec,
            "micro_f1": micro_f1,
            "micro_false_positive_rate": micro_fpr,
            "macro_precision": macro_prec,
            "macro_recall": macro_rec,
            "macro_f1": macro_f1,
            "macro_false_positive_rate": macro_fpr,
            "macro_contributing_scenario_count": len(def_prec_vals),
            "latency_summary": lat_summary,
            "scenario_coverage": coverage,
            "truth_positive_distribution": _independent_score_distribution(
                m_gt_p_scores, "truth_positive"
            ),
            "truth_negative_distribution": _independent_score_distribution(
                m_gt_n_scores, "truth_negative"
            ),
            "predicted_positive_distribution": _independent_score_distribution(
                m_pr_p_scores, "predicted_positive"
            ),
            "predicted_negative_distribution": _independent_score_distribution(
                m_pr_n_scores, "predicted_negative"
            ),
            "scenario_evaluations": scen_evals,
        }
        expected_models.append(model_summary)

    assert processed_json["schema_version"] == manifest["schema_version"] == "1.0"
    assert processed_json["package_id"] == manifest["package_id"]
    assert processed_json["package_version"] == manifest["package_version"]
    assert processed_json["code_revision"] == manifest["code_revision"]
    assert processed_json["decision_threshold"] == 0.5
    assert "created_at" in processed_json
    assert "metadata" in processed_json
    assert processed_json["models"] == expected_models

    expected_m_headers = [
        "model_name",
        "model_version",
        "total_units",
        "true_positives",
        "false_positives",
        "true_negatives",
        "false_negatives",
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "scenarios_detected",
        "detection_latency_mean_sec",
        "detection_latency_median_sec",
        "detection_latency_p95_sec",
        "success_units",
        "insufficient_units",
        "failure_units",
    ]
    assert list(model_csv_rows[0].keys()) == expected_m_headers
    assert len(model_csv_rows) == len(manifest["model_names"]) == 3

    for idx, m_sum in enumerate(expected_models):
        row = model_csv_rows[idx]
        assert row["model_name"] == m_sum["model_name"]
        assert row["model_version"] == m_sum["model_version"]
        assert int(row["total_units"]) == m_sum["micro_confusion_counts"]["total_units"]
        assert (
            int(row["true_positives"])
            == m_sum["micro_confusion_counts"]["true_positives"]
        )
        assert (
            int(row["false_positives"])
            == m_sum["micro_confusion_counts"]["false_positives"]
        )
        assert (
            int(row["true_negatives"])
            == m_sum["micro_confusion_counts"]["true_negatives"]
        )
        assert (
            int(row["false_negatives"])
            == m_sum["micro_confusion_counts"]["false_negatives"]
        )
        assert float(row["precision"]) == m_sum["micro_precision"]["value"]
        assert float(row["recall"]) == m_sum["micro_recall"]["value"]
        assert float(row["f1"]) == m_sum["micro_f1"]["value"]
        assert (
            float(row["false_positive_rate"])
            == m_sum["micro_false_positive_rate"]["value"]
        )
        assert (
            int(row["scenarios_detected"])
            == m_sum["latency_summary"]["contributing_scenario_count"]
        )
        assert (
            float(row["detection_latency_mean_sec"])
            == m_sum["latency_summary"]["mean_seconds"]
        )
        assert (
            float(row["detection_latency_median_sec"])
            == m_sum["latency_summary"]["median_seconds"]
        )
        assert (
            float(row["detection_latency_p95_sec"])
            == m_sum["latency_summary"]["p95_seconds"]
        )
        assert (
            int(row["success_units"])
            == m_sum["micro_confusion_counts"]["success_units"]
        )
        assert (
            int(row["insufficient_units"])
            == m_sum["micro_confusion_counts"]["insufficient_units"]
        )
        assert (
            int(row["failure_units"])
            == m_sum["micro_confusion_counts"]["failure_units"]
        )

    expected_s_headers = [
        "scenario_id",
        "model_name",
        "total_units",
        "true_positives",
        "false_positives",
        "true_negatives",
        "false_negatives",
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "detection_status",
        "detection_latency_sec",
    ]
    assert list(scenario_csv_rows[0].keys()) == expected_s_headers
    assert (
        len(scenario_csv_rows)
        == len(manifest["scenario_ids"]) * len(manifest["model_names"])
        == 24
    )

    s_row_idx = 0
    for s_id in manifest["scenario_ids"]:
        for m_sum in expected_models:
            se = next(
                s for s in m_sum["scenario_evaluations"] if s["scenario_id"] == s_id
            )
            row = scenario_csv_rows[s_row_idx]
            s_row_idx += 1
            assert row["scenario_id"] == s_id
            assert row["model_name"] == m_sum["model_name"]
            assert int(row["total_units"]) == se["confusion_counts"]["total_units"]
            assert (
                int(row["true_positives"]) == se["confusion_counts"]["true_positives"]
            )
            assert (
                int(row["false_positives"]) == se["confusion_counts"]["false_positives"]
            )
            assert (
                int(row["true_negatives"]) == se["confusion_counts"]["true_negatives"]
            )
            assert (
                int(row["false_negatives"]) == se["confusion_counts"]["false_negatives"]
            )
            assert float(row["precision"]) == se["precision"]["value"]
            assert float(row["recall"]) == se["recall"]["value"]
            assert float(row["f1"]) == se["f1"]["value"]
            assert (
                float(row["false_positive_rate"]) == se["false_positive_rate"]["value"]
            )
            assert row["detection_status"] == se["detection_latency"]["status"]
            assert (
                float(row["detection_latency_sec"])
                == se["detection_latency"]["latency_seconds"]
            )

    for m_sum in expected_models:
        disp_m = m_sum["model_name"].replace("_", " ").title()
        mc = m_sum["micro_confusion_counts"]
        p_val = m_sum["micro_precision"]["value"]
        r_val = m_sum["micro_recall"]["value"]
        f1_val = m_sum["micro_f1"]["value"]
        fpr_val = m_sum["micro_false_positive_rate"]["value"]
        p_str = f"{p_val:.4f}" if p_val is not None else "N/A"
        r_str = f"{r_val:.4f}" if r_val is not None else "N/A"
        f1_str = f"{f1_val:.4f}" if f1_val is not None else "N/A"
        fpr_str = f"{fpr_val:.4f}" if fpr_val is not None else "N/A"
        micro_row = (
            f"| {disp_m} | {mc['total_units']} | {mc['true_positives']} | {mc['false_positives']} | "
            f"{mc['true_negatives']} | {mc['false_negatives']} | {p_str} | {r_str} | {f1_str} | {fpr_str} |"
        )
        assert micro_row in report_content

        lat = m_sum["latency_summary"]
        min_s = (
            f"{lat['min_seconds']:.2f}s" if lat["min_seconds"] is not None else "N/A"
        )
        mean_s = (
            f"{lat['mean_seconds']:.2f}s" if lat["mean_seconds"] is not None else "N/A"
        )
        med_s = (
            f"{lat['median_seconds']:.2f}s"
            if lat["median_seconds"] is not None
            else "N/A"
        )
        p95_s = (
            f"{lat['p95_seconds']:.2f}s" if lat["p95_seconds"] is not None else "N/A"
        )
        max_s = (
            f"{lat['max_seconds']:.2f}s" if lat["max_seconds"] is not None else "N/A"
        )
        lat_row = (
            f"| {disp_m} | {lat['contributing_scenario_count']} | {lat['missing_detection_count']} | "
            f"{min_s} | {mean_s} | {med_s} | {p95_s} | {max_s} |"
        )
        assert lat_row in report_content

        cov = m_sum["scenario_coverage"]
        cov_row = (
            f"| {disp_m} | {cov['requested_count']} | {cov['executed_count']} | {cov['scored_count']} | "
            f"{cov['evaluable_count']} | {cov['detected_count']} | {cov['insufficient_count']} | {cov['failed_count']} |"
        )
        assert cov_row in report_content

        tp_d = m_sum["truth_positive_distribution"]
        tn_d = m_sum["truth_negative_distribution"]
        assert f"### {disp_m}" in report_content
        tp_dist_line = (
            f"- **Truth-Positive Distribution (N={tp_d['count']}):** Min={tp_d['min']}, "
            f"Mean={tp_d['mean']}, Median={tp_d['median']}, P95={tp_d['p95']}, Max={tp_d['max']}"
        )
        tn_dist_line = (
            f"- **Truth-Negative Distribution (N={tn_d['count']}):** Min={tn_d['min']}, "
            f"Mean={tn_d['mean']}, Median={tn_d['median']}, P95={tn_d['p95']}, Max={tn_d['max']}"
        )
        assert tp_dist_line in report_content
        assert tn_dist_line in report_content

    for s_id in manifest["scenario_ids"]:
        for m_sum in expected_models:
            se = next(
                s for s in m_sum["scenario_evaluations"] if s["scenario_id"] == s_id
            )
            disp_m = m_sum["model_name"].replace("_", " ").title()
            p_val = se["precision"]["value"]
            r_val = se["recall"]["value"]
            f1_val = se["f1"]["value"]
            lat_val = se["detection_latency"]["latency_seconds"]
            p_str = f"{p_val:.2f}" if p_val is not None else "N/A"
            r_str = f"{r_val:.2f}" if r_val is not None else "N/A"
            f1_str = f"{f1_val:.2f}" if f1_val is not None else "N/A"
            lat_str = f"{lat_val:.1f}s" if lat_val is not None else "N/A"
            scen_row = (
                f"| `{s_id}` | {disp_m} | {se['confusion_counts']['true_positives']} | {se['confusion_counts']['false_positives']} | "
                f"{se['confusion_counts']['true_negatives']} | {se['confusion_counts']['false_negatives']} | "
                f"{p_str} | {r_str} | {f1_str} | {lat_str} | {se['status']} |"
            )
            assert scen_row in report_content

    tot_fp = sum(
        m["micro_confusion_counts"]["false_positives"] for m in expected_models
    )
    tot_fn = sum(
        m["micro_confusion_counts"]["false_negatives"] for m in expected_models
    )
    # Obsolete assertions removed (C4-C1 / C4-C2); replaced with Section 8 assertions above

    assert (
        f"- **Total False Positives:** {tot_fp} across all models and scenarios."
        in report_content
    )
    assert (
        f"- **Total False Negatives:** {tot_fn} across all models and scenarios."
        in report_content
    )
    # Slot-accounting lines (Section 8, independent from raw)
    expected_slot_count = len(logical_unit_keys) * len(manifest["model_names"])
    duplicate_slot_count = len(raw_outcomes) - len(logical_slot_keys)
    missing_slot_count = expected_slot_count - len(logical_slot_keys)
    assert f"- **Unique Evaluation Units:** {len(logical_unit_keys)}" in report_content
    assert (
        f"- **Expected Model-Outcome Slots:** {expected_slot_count} ({len(logical_unit_keys)} units × {len(manifest['model_names'])} models)"
        in report_content
    )
    assert f"- **Recorded Model Outcomes:** {len(raw_outcomes)}" in report_content
    assert f"- **Duplicate Model Outcomes:** {duplicate_slot_count}" in report_content
    assert f"- **Missing Model-Outcome Slots:** {missing_slot_count}" in report_content

    assert "### 8.1 Detailed Outcome Status Breakdown" in report_content
    assert (
        "| Model | Success | Insufficient Data | Missing Calibration | Non-Convergence | Fit Failure | Invalid Input | Model Failure | Not Applicable | Total Recorded |"
        in report_content
    )

    for m_name in manifest["model_names"]:
        disp = m_name.replace("_", " ").title()
        m_raw = [o for o in raw_outcomes if o["model_name"] == m_name]
        statuses = [o["status"] for o in m_raw]
        success = statuses.count("success")
        insuf = statuses.count("insufficient_data")
        missing_cal = statuses.count("missing_calibration")
        non_conv = statuses.count("non_convergence")
        fit_fail = statuses.count("fit_failure")
        invalid = statuses.count("invalid_input")
        mf = statuses.count("model_failure")
        not_app = statuses.count("not_applicable")
        total_rec = len(m_raw)
        row_text = f"| {disp} | {success} | {insuf} | {missing_cal} | {non_conv} | {fit_fail} | {invalid} | {mf} | {not_app} | {total_rec} |"
        assert row_text in report_content

    total_statuses = [o["status"] for o in raw_outcomes]
    total_row = f"| **Total** | **{total_statuses.count('success')}** | **{total_statuses.count('insufficient_data')}** | **{total_statuses.count('missing_calibration')}** | **{total_statuses.count('non_convergence')}** | **{total_statuses.count('fit_failure')}** | **{total_statuses.count('invalid_input')}** | **{total_statuses.count('model_failure')}** | **{total_statuses.count('not_applicable')}** | **{len(raw_outcomes)}** |"
    assert total_row in report_content

    # Confusion totals (independently derived from raw)
    tot_tp = sum(m["micro_confusion_counts"]["true_positives"] for m in expected_models)
    tot_tn = sum(m["micro_confusion_counts"]["true_negatives"] for m in expected_models)
    assert (
        f"- **Total True Positives:** {tot_tp} across all models and scenarios."
        in report_content
    )
    assert (
        f"- **Total False Positives:** {tot_fp} across all models and scenarios."
        in report_content
    )
    assert (
        f"- **Total False Negatives:** {tot_fn} across all models and scenarios."
        in report_content
    )
    assert (
        f"- **Total True Negatives:** {tot_tn} across all models and scenarios."
        in report_content
    )


# ======================================================================
# C3-F5-C1: Shared failure helper and post-run surface coverage
# ======================================================================


@pytest.mark.parametrize(
    "case_name",
    [
        "summary_iteration",
        "metric_formatting",
        "manifest_path_access",
        "manifest_hashing",
        "artifact_iteration",
        "final_console_output",
    ],
)
def test_f5_cli_c1_post_run_failure_surfaces(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    case_name: str,
) -> None:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)
    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli_mod, "_get_git_revision", lambda: "mock-git-rev")
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")

    sentinel = f"F5C1_{case_name.upper()}_SECRET"
    prohibited: tuple[str, ...] = (sentinel,)

    if case_name == "summary_iteration":

        class FailingSummaries:
            def __iter__(self):
                raise RuntimeError(sentinel)

        async def mock_run(config, base_dir):
            return _make_standard_manifest(config), FailingSummaries()

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)
        expected_body = "Evaluation failed due to an unexpected internal error"

    elif case_name == "metric_formatting":
        sums = _make_failing_summaries()

        async def mock_run(config, base_dir):
            return _make_standard_manifest(config), sums

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)
        expected_body = "Evaluation failed due to an unexpected internal error"
        prohibited = ("F5C1_METRIC_FORMAT_SECRET",)

    elif case_name == "manifest_path_access":
        man = _make_exploding_manifest_path()
        _, sums, _ = _build_mock_evaluation_data()

        async def mock_run(config, base_dir):
            return man, sums

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)
        expected_body = "Evaluation failed due to an unexpected internal error"
        prohibited = ("F5C1_MANIFEST_PATH_SECRET",)

    elif case_name == "manifest_hashing":
        _, sums, _ = _build_mock_evaluation_data()
        man = _make_standard_manifest(_make_dummy_config())

        async def mock_run(config, base_dir):
            return man, sums

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)

        def exploding_sha(p):
            raise OSError(f"{sentinel} C:\\private\\manifest.json")

        monkeypatch.setattr(cli_mod, "_compute_sha256", exploding_sha)
        expected_body = "Evaluation failed due to an unexpected internal error"
        prohibited = (sentinel, "C:\\private\\manifest.json")

    elif case_name == "artifact_iteration":
        _, sums, _ = _build_mock_evaluation_data()
        man = _make_standard_manifest(_make_dummy_config()).model_copy(
            update={"artifacts": _make_exploding_artifacts_list()}
        )

        async def mock_run(config, base_dir):
            return man, sums

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)
        expected_body = "Evaluation failed due to an unexpected internal error"
        prohibited = ("F5C1_ARTIFACT_ITERATION_SECRET",)

    elif case_name == "final_console_output":
        _, sums, _ = _build_mock_evaluation_data()
        man = _make_standard_manifest(_make_dummy_config())

        async def mock_run(config, base_dir):
            return man, sums

        monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)

        real_print = print

        def exploding_print(*args, **kwargs):
            file = kwargs.get("file", None)
            if file is None or file is cli_mod.sys.stdout:
                if any(
                    "PHASE 3 TASK 3.9 EVALUATION HARNESS COMPLETE — SUCCESS" in str(a)
                    for a in args
                ):
                    raise OSError(sentinel)
            return real_print(*args, **kwargs)

        monkeypatch.setattr("builtins.print", exploding_print)
        expected_body = "Evaluation failed due to an unexpected internal error"

    exit_code, out, err = _capture_cli_output(cli_mod, [])

    _assert_f5_cli_bounded_failure(
        exit_code=exit_code,
        stdout=out,
        stderr=err,
        expected_body=expected_body,
        prohibited=prohibited,
    )


def test_f5_cli_c1_unexpected_artifact_validator_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)
    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli_mod, "_get_git_revision", lambda: "mock-git-rev")

    def mock_validator(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(
            "F5C1_ARTIFACT_VALIDATOR_SECRET C:\\Users\\private\\artifact-root"
        )

    monkeypatch.setattr(cli_mod, "_validate_safe_artifact_root", mock_validator)

    exit_code, out, err = _capture_cli_output(cli_mod, [])
    _assert_f5_cli_bounded_failure(
        exit_code=exit_code,
        stdout=out,
        stderr=err,
        expected_body="Evaluation failed due to an unexpected internal error",
        prohibited=(
            "F5C1_ARTIFACT_VALIDATOR_SECRET",
            "C:\\Users\\private\\artifact-root",
        ),
    )


def test_f5_cli_c1_valid_git_subprocess_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import subprocess

    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)
    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)

    valid_rev = "abcdef1234567890abcdef1234567890abcdef12"
    mock_res = subprocess.CompletedProcess(
        args=["git", "rev-parse", "HEAD"],
        returncode=0,
        stdout=f"{valid_rev}\n",
        stderr="",
    )

    captured_args: dict[str, Any] = {}

    def mock_subprocess_run(*args, **kwargs):
        captured_args["args"] = args
        captured_args["kwargs"] = kwargs
        return mock_res

    monkeypatch.setattr(cli_mod.subprocess, "run", mock_subprocess_run)

    async def mock_run(config, base_dir):
        man = _make_standard_manifest(config)
        # Write manifest file to disk so CLI hashing succeeds
        (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
        return man, []

    monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run)

    exit_code = asyncio.run(cli_mod.main_async([]))
    assert exit_code == 0
    captured = capsys.readouterr()
    assert f"Code revision:       {valid_rev}" in captured.out
    assert captured.err == ""
    assert "PHASE 3 TASK 3.9 EVALUATION HARNESS COMPLETE — SUCCESS" in captured.out
    assert captured_args["args"][0] == ["git", "rev-parse", "HEAD"]
    assert captured_args["kwargs"].get("cwd") == str(tmp_path)
    assert captured_args["kwargs"].get("stdout") == subprocess.PIPE
    assert captured_args["kwargs"].get("stderr") == subprocess.PIPE
    assert captured_args["kwargs"].get("text") is True
    assert captured_args["kwargs"].get("check") is True


def test_f5_cli_c1_parser_output_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli_mod = _load_cli_module()

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "usage:" in captured.out.lower()
    assert "AegisOps Phase 3" in captured.out
    assert captured.err == ""

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--unknown-flag"])
    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "usage:" in captured.err.lower()
    assert "unrecognized arguments" in captured.err.lower()

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--calibration-seed", "not_an_int"])
    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "usage:" in captured.err.lower()
    assert "invalid int value" in captured.err.lower()

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.parse_args(["--decision-threshold", "not_a_float"])
    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "usage:" in captured.err.lower()
    assert "invalid float value" in captured.err.lower()


@pytest.mark.asyncio
async def test_f5_cli_c1_operational_baseexceptions_propagate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "backend").mkdir(parents=True, exist_ok=True)
    (tmp_path / "research").mkdir(parents=True, exist_ok=True)
    cli_mod = _load_cli_module()
    monkeypatch.setattr(cli_mod, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli_mod, "_get_git_revision", lambda: "mock-git-rev")

    async def mock_run_ki(config, base_dir):
        raise KeyboardInterrupt()

    monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run_ki)
    with pytest.raises(KeyboardInterrupt):
        await cli_mod.main_async([])
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Error: " not in captured.err

    async def mock_run_se(config, base_dir):
        raise SystemExit(73)

    monkeypatch.setattr(cli_mod, "run_phase3_evaluation", mock_run_se)
    with pytest.raises(SystemExit) as exc_info:
        await cli_mod.main_async([])
    assert exc_info.value.code == 73
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Error: " not in captured.err
