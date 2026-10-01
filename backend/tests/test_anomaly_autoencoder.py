from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import tempfile
from typing import Any
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.autoencoder import (
    SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION,
    SUPPORTED_AE_CONFIG_SCHEMA_VERSION,
    AutoencoderArtifact,
    AutoencoderBaseline,
    AutoencoderBaselineConfig,
    AutoencoderScoreStatus,
    run_autoencoder_baseline,
)
from app.anomaly.errors import AutoencoderArtifactError, AutoencoderFitError
from app.anomaly.experiment import FeatureWindowConfig
from app.anomaly.features import (
    DeterministicFeatureExtractor,
    FeatureExtractionResult,
    FeatureWindow,
    FeatureWindowStatus,
    RawObservation,
)
from app.anomaly.isolation_forest import MAX_UINT32
from app.anomaly.models import MAX_UINT64, AnomalySignal
from app.telemetry.schemas import EventSeverity


def _make_observation(
    event_time: datetime,
    value: float,
    metric_name: str = "http_request_duration_ms",
    service: str = "order-service",
    event_id: uuid.UUID | None = None,
    seed: int = 42,
) -> RawObservation:
    return RawObservation(
        event_id=event_id or uuid.uuid4(),
        event_time=event_time,
        service=service,
        metric_name=metric_name,
        value=value,
        tenant_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        environment="simulation",
        run_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        scenario_id="dependency-latency",
        scenario_version="1.0.0",
        seed=seed,
        reproducibility_key="repro-key-ae",
    )


def _build_multivariate_feature_result(
    latencies: list[float],
    requests: list[float] | None = None,
    errors: list[float] | None = None,
    interval_seconds: float = 10.0,
) -> FeatureExtractionResult:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    observations: list[RawObservation] = []

    for i, lat in enumerate(latencies):
        t = t0 + timedelta(seconds=i * interval_seconds)
        observations.append(_make_observation(event_time=t, value=lat, metric_name="http_request_duration_ms"))
        if requests is not None:
            observations.append(
                _make_observation(event_time=t, value=requests[i], metric_name="http_requests_total")
            )
        if errors is not None:
            observations.append(
                _make_observation(event_time=t, value=errors[i], metric_name="http_errors_total")
            )

    feature_names = ["http_request_duration_ms:mean"]
    if requests is not None:
        feature_names.append("http_requests_total:mean")
    if errors is not None:
        feature_names.append("http_errors_total:mean")

    config = FeatureWindowConfig(
        feature_config_id="ae-test-feat-cfg",
        feature_names=feature_names,
        window_size_seconds=interval_seconds,
        step_size_seconds=interval_seconds,
        aggregation_methods=["mean"],
        imputation_strategy="none",
    )

    extractor = DeterministicFeatureExtractor(config=config)
    return extractor.extract_from_observations(observations)


def _sample_ae_config(
    feature_names: list[str] | None = None,
    bottleneck_dimension: int = 1,
    min_history: int = 10,
    scaling_method: str = "standard",
    random_state: int = 42,
    solver: str = "lbfgs",
) -> AutoencoderBaselineConfig:
    feats = feature_names or ["http_request_duration_ms:mean", "http_requests_total:mean"]
    return AutoencoderBaselineConfig(
        schema_version="1.0",
        model_name="autoencoder",
        model_version="1.0.0",
        feature_names=feats,
        bottleneck_dimension=bottleneck_dimension,
        hidden_layer_sizes=(bottleneck_dimension,),
        scaling_method=scaling_method,  # type: ignore[arg-type]
        min_training_windows=min_history,
        solver=solver,  # type: ignore[arg-type]
        max_iter=200,
        random_state=random_state,
    )


def test_ae_config_validation() -> None:
    cfg = _sample_ae_config()
    assert cfg.schema_version == SUPPORTED_AE_CONFIG_SCHEMA_VERSION
    assert cfg.model_name == "autoencoder"
    assert cfg.model_version == "1.0.0"
    assert cfg.bottleneck_dimension == 1
    assert len(cfg.feature_names) == 2

    # Reject invalid model name
    with pytest.raises(ValidationError):
        AutoencoderBaselineConfig(feature_names=["a", "b"], model_name="prophet")

    # Reject fewer than 2 features
    with pytest.raises(ValidationError):
        AutoencoderBaselineConfig(feature_names=["single_feature"])

    # Reject bottleneck_dimension >= input dimension
    with pytest.raises(ValidationError, match="bottleneck_dimension"):
        AutoencoderBaselineConfig(
            feature_names=["a", "b"],
            bottleneck_dimension=2,
            hidden_layer_sizes=(2,),
        )

    # Reject negative bottleneck
    with pytest.raises(ValidationError):
        AutoencoderBaselineConfig(
            feature_names=["a", "b"],
            bottleneck_dimension=0,
            hidden_layer_sizes=(0,),
        )


def test_ae_random_state_domain_validation() -> None:
    # Negative rejected
    with pytest.raises(ValidationError, match="random_state"):
        AutoencoderBaselineConfig(feature_names=["a", "b"], random_state=-1)

    # Boolean rejected
    with pytest.raises(ValidationError, match="random_state cannot be a boolean"):
        AutoencoderBaselineConfig(feature_names=["a", "b"], random_state=True)  # type: ignore[arg-type]

    # 0 accepted
    cfg_zero = AutoencoderBaselineConfig(feature_names=["a", "b"], random_state=0)
    assert cfg_zero.random_state == 0

    # MAX_UINT32 accepted
    cfg_max32 = AutoencoderBaselineConfig(feature_names=["a", "b"], random_state=MAX_UINT32)
    assert cfg_max32.random_state == 4294967295

    # 2**32 rejected
    with pytest.raises(ValidationError, match="random_state"):
        AutoencoderBaselineConfig(feature_names=["a", "b"], random_state=2**32)

    # MAX_UINT64 rejected
    with pytest.raises(ValidationError, match="random_state"):
        AutoencoderBaselineConfig(feature_names=["a", "b"], random_state=MAX_UINT64)


def test_valid_normal_controlled_multivariate_fixture() -> None:
    latencies = [100.0 + (i % 5) * 3.0 for i in range(20)]
    requests = [10.0 + (i % 3) * 1.5 for i in range(20)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    result = baseline.score_target_window(feat_res, target_window_index=19)

    assert result.status == AutoencoderScoreStatus.SUCCESS
    assert result.raw_reconstruction_error is not None
    assert result.anomaly_score is not None
    assert 0.0 <= result.anomaly_score <= 0.40  # Normal inlier has low score
    assert result.signal is not None
    assert result.signal.severity == EventSeverity.INFO
    assert result.history_window_count == 19


def test_valid_anomalous_controlled_multivariate_fixture() -> None:
    # 19 normal points (correlation between latency and requests), followed by severe anomaly at index 19
    latencies = [100.0 + (i % 5) * 4.0 for i in range(19)] + [900.0]
    requests = [10.0 + (i % 3) * 2.0 for i in range(19)] + [100.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    result = baseline.score_target_window(feat_res, target_window_index=19)

    assert result.status == AutoencoderScoreStatus.SUCCESS
    assert result.anomaly_score is not None
    assert result.anomaly_score > 0.60  # Severe anomaly has elevated score
    assert result.signal is not None
    assert result.signal.severity in (EventSeverity.WARNING, EventSeverity.ERROR, EventSeverity.CRITICAL)


def test_anomalous_score_higher_than_normal_fixture() -> None:
    lat_normal = [100.0 + (i % 5) * 4.0 for i in range(20)]
    req_normal = [10.0 + (i % 3) * 2.0 for i in range(20)]

    lat_anom = [100.0 + (i % 5) * 4.0 for i in range(19)] + [800.0]
    req_anom = [10.0 + (i % 3) * 2.0 for i in range(19)] + [80.0]

    feat_norm = _build_multivariate_feature_result(lat_normal, requests=req_normal)
    feat_anom = _build_multivariate_feature_result(lat_anom, requests=req_anom)

    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    res_norm = baseline.score_target_window(feat_norm, target_window_index=19)
    res_anom = baseline.score_target_window(feat_anom, target_window_index=19)

    assert res_norm.anomaly_score is not None
    assert res_anom.anomaly_score is not None
    assert res_anom.anomaly_score > res_norm.anomaly_score
    assert res_anom.raw_reconstruction_error is not None and res_norm.raw_reconstruction_error is not None
    assert res_anom.raw_reconstruction_error > res_norm.raw_reconstruction_error


def test_deterministic_repeated_execution_with_same_seeds() -> None:
    latencies = [100.0 + i * 2.0 for i in range(19)] + [600.0]
    requests = [10.0 + i for i in range(19)] + [50.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = _sample_iforest_config_like(random_state=123)
    baseline1 = AutoencoderBaseline(config=cfg)
    baseline2 = AutoencoderBaseline(config=cfg)

    res1 = baseline1.score_target_window(feat_res, target_window_index=19)
    res2 = baseline2.score_target_window(feat_res, target_window_index=19)

    assert res1.status == res2.status == AutoencoderScoreStatus.SUCCESS
    assert res1.raw_reconstruction_error == pytest.approx(res2.raw_reconstruction_error, rel=1e-5)
    assert res1.anomaly_score == pytest.approx(res2.anomaly_score, rel=1e-5)
    assert res1.actual_iterations == res2.actual_iterations


def _sample_iforest_config_like(random_state: int = 42) -> AutoencoderBaselineConfig:
    return _sample_ae_config(random_state=random_state)


def test_schema_valid_anomaly_signal_output() -> None:
    latencies = [100.0 + (i % 4) * 2.0 for i in range(19)] + [700.0]
    requests = [10.0 + (i % 2) * 1.0 for i in range(19)] + [50.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)

    res = run_autoencoder_baseline(feat_res, config=cfg)

    assert res.signal is not None
    signal: AnomalySignal = res.signal

    assert signal.schema_version == "1.0"
    assert signal.model_name == "autoencoder"
    assert signal.model_version == "1.0.0"
    assert signal.service == "order-service"
    assert 0.0 <= signal.anomaly_score <= 1.0
    assert len(signal.evidence) == 1

    ev = signal.evidence[0]
    assert ev.evidence_type == "autoencoder_reconstruction_error"
    assert ev.details["ordered_features"] == ["http_request_duration_ms:mean", "http_requests_total:mean"]
    assert "raw_input_vector" in ev.details
    assert "scaled_input_vector" in ev.details
    assert "reconstructed_vector" in ev.details
    assert "per_feature_errors" in ev.details
    assert "architecture" in ev.details
    assert ev.details["architecture"]["bottleneck_dim"] == 1
    assert ev.details["random_state"] == 42
    assert ev.details["effective_model_seed"] == 42
    assert ev.details["history_window_count"] == 19

    cal = signal.threshold_or_calibration
    assert cal.schema_version == "1.0"
    assert cal.method == "empirical_tail_ratio_sigmoid"
    assert cal.threshold_value == 0.75

    assert signal.run_id == feat_res.run_id
    assert signal.scenario_id == feat_res.scenario_id
    assert signal.seed == feat_res.seed
    assert signal.reproducibility_key == feat_res.reproducibility_key


def test_temporal_causality_target_and_future_excluded() -> None:
    # 25 windows: 0..14 normal (100.0), 15 target (100.0), 16..24 huge future anomalies (9999.0)
    latencies = [100.0 + (i % 4) * 2.0 for i in range(16)] + [9999.0 for _ in range(9)]
    requests = [10.0 + (i % 2) * 1.0 for i in range(16)] + [999.0 for _ in range(9)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    # Score target index 15: training history must strictly be 0..14 (length 15)
    res = baseline.score_target_window(feat_res, target_window_index=15)

    assert res.status == AutoencoderScoreStatus.SUCCESS
    assert res.history_window_count == 15
    assert res.anomaly_score is not None and res.anomaly_score <= 0.40


def test_scaling_methods_standard_robust_minmax_none() -> None:
    latencies = [100.0 + (i % 5) * 5.0 for i in range(19)] + [800.0]
    requests = [10.0 + (i % 3) * 2.0 for i in range(19)] + [90.0]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    for method in ("standard", "robust", "minmax", "none"):
        cfg = _sample_ae_config(min_history=10, scaling_method=method)
        baseline = AutoencoderBaseline(config=cfg)
        res = baseline.score_target_window(feat_res, target_window_index=19)

        assert res.status == AutoencoderScoreStatus.SUCCESS
        assert res.anomaly_score is not None
        assert res.signal is not None
        assert res.signal.evidence[0].details["scaling_method"] == method


def test_zero_variance_features_handled_safely() -> None:
    latencies = [100.0 + i * 2.0 for i in range(19)] + [600.0]
    requests = [10.0 for _ in range(20)]  # Constant 10.0

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10, scaling_method="standard")
    baseline = AutoencoderBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == AutoencoderScoreStatus.SUCCESS
    assert res.anomaly_score is not None
    assert math.isfinite(res.scaled_input_vector["http_requests_total:mean"])
    assert math.isfinite(res.reconstructed_vector["http_requests_total:mean"])


def test_measured_zero_accepted_and_scored() -> None:
    latencies = [0.0 for _ in range(20)]
    requests = [0.0 for _ in range(20)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == AutoencoderScoreStatus.SUCCESS
    assert res.anomaly_score is not None
    assert res.anomaly_score == 0.0


def test_insufficient_history_returns_explicit_status() -> None:
    latencies = [100.0 for _ in range(5)]
    requests = [10.0 for _ in range(5)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=4)

    assert res.status == AutoencoderScoreStatus.INSUFFICIENT_HISTORY
    assert res.signal is None
    assert "requires at least 10" in str(res.error_message)


def test_empty_series_returns_invalid_input() -> None:
    cfg_feat = FeatureWindowConfig(
        feature_config_id="empty-cfg",
        feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
        window_size_seconds=10.0,
        step_size_seconds=10.0,
    )
    extractor = DeterministicFeatureExtractor(config=cfg_feat)
    feat_empty = extractor.extract_from_observations([])

    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    res = baseline.score_target_window(feat_empty)

    assert res.status == AutoencoderScoreStatus.INVALID_INPUT
    assert res.signal is None


def test_missing_feature_in_target_window() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=10)

    w0 = FeatureWindow(
        window_index=0,
        window_start=t0,
        window_end=t1,
        observation_timestamp=t1,
        service="order-service",
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        run_id=uuid.UUID(int=2),
        scenario_id="dependency-latency",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-1",
        feature_config_id="cfg-1",
        values={"http_request_duration_ms:mean": 100.0, "http_requests_total:mean": 10.0},
    )
    w1 = FeatureWindow(
        window_index=1,
        window_start=t1,
        window_end=t1 + timedelta(seconds=10),
        observation_timestamp=t1 + timedelta(seconds=10),
        service="order-service",
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        run_id=uuid.UUID(int=2),
        scenario_id="dependency-latency",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-1",
        feature_config_id="cfg-1",
        values={"http_request_duration_ms:mean": 100.0},
        missing_features=["http_requests_total:mean"],
        status=FeatureWindowStatus.INSUFFICIENT_DATA,
    )

    feat_res = FeatureExtractionResult(
        schema_version="1.0",
        feature_config_id="cfg-1",
        service="order-service",
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        run_id=uuid.UUID(int=2),
        scenario_id="dependency-latency",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-1",
        window_size_seconds=10.0,
        step_size_seconds=10.0,
        feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
        windows=[w0, w1],
        total_windows=2,
        valid_windows=1,
        extracted_at=datetime.now(timezone.utc),
    )

    cfg = _sample_ae_config(min_history=3)
    baseline = AutoencoderBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=1)

    assert res.status == AutoencoderScoreStatus.MISSING_FEATURES
    assert res.signal is None
    assert "http_requests_total:mean" in str(res.error_message)


def test_score_series_sequential_causal_evaluation() -> None:
    # 20 windows: 0..14 normal with variance, 15..19 severe anomaly (off-manifold latency spikes)
    latencies = [100.0 + (i % 5) * 4.0 for i in range(15)] + [500.0, 600.0, 700.0, 800.0, 900.0]
    requests = [10.0 + (i % 3) * 2.0 for i in range(15)] + [10.0, 10.0, 10.0, 10.0, 10.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    series_results = baseline.score_series(feat_res, start_index=10)

    assert len(series_results) == 10  # 10..19
    for r in series_results:
        assert r.status == AutoencoderScoreStatus.SUCCESS

    # Anomalous windows (15..19) score significantly higher than baseline windows (10..14)
    baseline_scores = [r.anomaly_score for r in series_results[:5] if r.anomaly_score is not None]
    anom_scores = [r.anomaly_score for r in series_results[5:] if r.anomaly_score is not None]
    assert sum(anom_scores) / len(anom_scores) > sum(baseline_scores) / len(baseline_scores)
    for r in series_results[5:]:
        assert r.anomaly_score is not None and r.anomaly_score > 0.60


def test_fit_failure_handling(monkeypatch: pytest.MonkeyPatch) -> None:
    latencies = [100.0 for _ in range(20)]
    requests = [10.0 for _ in range(20)]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    def mock_fail(*args: object, **kwargs: object) -> tuple[Any, ...]:
        raise AutoencoderFitError("Optimizer numerical explosion")

    monkeypatch.setattr(baseline, "_fit_and_reconstruct", mock_fail)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == AutoencoderScoreStatus.FIT_FAILURE
    assert res.signal is None
    assert "Optimizer numerical explosion" in str(res.error_message)


def test_artifact_save_load_round_trip() -> None:
    latencies = [100.0 + (i % 5) * 3.0 for i in range(20)]
    requests = [10.0 + (i % 3) * 1.5 for i in range(20)]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = _sample_ae_config(min_history=10)
    baseline = AutoencoderBaseline(config=cfg)

    artifact = baseline.export_artifact(feat_res, target_window_index=19)

    assert artifact.artifact_schema_version == SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION
    assert artifact.model_name == "autoencoder"
    assert artifact.bottleneck_dimension == 1
    assert len(artifact.coefs) > 0
    assert len(artifact.intercepts) > 0

    with tempfile.TemporaryDirectory() as tmp_dir:
        art_path = Path(tmp_dir) / "autoencoder_artifact.json"
        artifact.save(art_path)

        loaded_art = AutoencoderArtifact.load(art_path)
        assert loaded_art.artifact_schema_version == artifact.artifact_schema_version
        assert loaded_art.feature_names == artifact.feature_names
        assert loaded_art.bottleneck_dimension == artifact.bottleneck_dimension
        assert loaded_art.coefs == artifact.coefs
        assert loaded_art.intercepts == artifact.intercepts


def test_corrupt_and_missing_artifact_handling() -> None:
    # Missing file
    with pytest.raises(AutoencoderArtifactError, match="Missing artifact"):
        AutoencoderArtifact.load("non_existent_file_path_12345.json")

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Corrupt JSON
        corrupt_path = Path(tmp_dir) / "corrupt.json"
        corrupt_path.write_text("INVALID_JSON_CONTENT", encoding="utf-8")
        with pytest.raises(AutoencoderArtifactError, match="Corrupt artifact"):
            AutoencoderArtifact.load(corrupt_path)

        # Unsupported schema version
        bad_ver_path = Path(tmp_dir) / "bad_version.json"
        bad_ver_path.write_text('{"artifact_schema_version": "99.0.0"}', encoding="utf-8")
        with pytest.raises(AutoencoderArtifactError, match="Unsupported artifact schema version"):
            AutoencoderArtifact.load(bad_ver_path)


def test_non_convergence_handling_and_signal_suppression() -> None:
    # Force non-convergence deterministically with max_iter=10 and adam solver with tight tol
    latencies = [100.0 + i * 5.0 for i in range(20)]
    requests = [10.0 + i * 3.0 for i in range(20)]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = AutoencoderBaselineConfig(
        feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
        bottleneck_dimension=1,
        min_training_windows=10,
        solver="adam",
        max_iter=10,  # Forces non-convergence in 10 iterations
        tol=1e-15,
        random_state=42,
    )
    baseline = AutoencoderBaseline(config=cfg)
    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == AutoencoderScoreStatus.NON_CONVERGENCE
    assert res.converged is False
    assert res.signal is None
    assert res.anomaly_score is None
    assert res.actual_iterations == 10
    assert res.final_loss is not None and math.isfinite(res.final_loss)
    assert "without convergence" in str(res.error_message)


def test_non_convergence_artifact_export_rejection() -> None:
    latencies = [100.0 + i * 5.0 for i in range(20)]
    requests = [10.0 + i * 3.0 for i in range(20)]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = AutoencoderBaselineConfig(
        feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
        bottleneck_dimension=1,
        min_training_windows=10,
        solver="adam",
        max_iter=10,
        tol=1e-15,
        random_state=42,
    )
    baseline = AutoencoderBaseline(config=cfg)

    with pytest.raises(AutoencoderArtifactError, match="Cannot export artifact from non-converged"):
        baseline.export_artifact(feat_res, target_window_index=19)


def _sample_valid_artifact_dict() -> dict[str, Any]:
    return {
        "artifact_schema_version": "1.0.0",
        "configuration_schema_version": "1.0",
        "model_name": "autoencoder",
        "model_version": "1.0.0",
        "framework_name": "scikit-learn",
        "framework_version": "1.9.1",
        "feature_names": ["http_request_duration_ms:mean", "http_requests_total:mean"],
        "input_dimension": 2,
        "bottleneck_dimension": 1,
        "hidden_layer_sizes": [1],
        "activation": "relu",
        "solver": "lbfgs",
        "scaling_parameters": {
            "method": "standard",
            "feature_names": ["http_request_duration_ms:mean", "http_requests_total:mean"],
            "centers": [100.0, 10.0],
            "scales": [5.0, 2.0],
        },
        "reconstruction_error_method": "mean_squared_error",
        "reconstruction_error_version": "1.0.0",
        "score_normalization_method": "empirical_tail_ratio_sigmoid",
        "score_normalization_version": "1.0.0",
        "score_normalization_parameters": {
            "scale_factor": 3.0,
            "epsilon_spread": 0.0001,
        },
        "training_stats": {
            "s_min": 0.001,
            "s_mean": 0.01,
            "s_std": 0.005,
            "history_count": 15,
            "actual_iterations": 20,
            "final_loss": 0.0005,
            "converged": True,
        },
        "coefs": [
            [[0.5], [0.5]],  # 2x1 input -> bottleneck
            [[0.5, 0.5]],    # 1x2 bottleneck -> output
        ],
        "intercepts": [
            [0.1],      # bottleneck bias
            [0.1, 0.1], # output bias
        ],
    }


def test_artifact_incompatible_model_or_config_version() -> None:
    data = _sample_valid_artifact_dict()
    data["model_name"] = "isolation_forest"
    with pytest.raises(AutoencoderArtifactError, match="Incompatible model_name"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["model_version"] = "2.0.0"
    with pytest.raises(AutoencoderArtifactError, match="Incompatible model_version"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["configuration_schema_version"] = "99.0"
    with pytest.raises(AutoencoderArtifactError, match="Unsupported configuration_schema_version"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_reconstruction_method_and_version_validation() -> None:
    # Missing / unsupported reconstruction method
    data = _sample_valid_artifact_dict()
    data["reconstruction_error_method"] = "root_mean_squared_error"
    with pytest.raises(AutoencoderArtifactError, match="Unsupported reconstruction_error_method"):
        AutoencoderArtifact.from_json(json.dumps(data))

    # Unsupported reconstruction error version
    data = _sample_valid_artifact_dict()
    data["reconstruction_error_version"] = "2.0.0"
    with pytest.raises(AutoencoderArtifactError, match="Unsupported reconstruction_error_version"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_final_loss_validation_domain() -> None:
    # 0.0 loss is accepted
    data_zero = _sample_valid_artifact_dict()
    data_zero["training_stats"]["final_loss"] = 0.0
    art_zero = AutoencoderArtifact.from_json(json.dumps(data_zero))
    assert art_zero.training_stats["final_loss"] == 0.0

    # Positive finite loss is accepted
    data_pos = _sample_valid_artifact_dict()
    data_pos["training_stats"]["final_loss"] = 1.2345
    art_pos = AutoencoderArtifact.from_json(json.dumps(data_pos))
    assert art_pos.training_stats["final_loss"] == 1.2345

    # Negative loss is rejected
    data_neg = _sample_valid_artifact_dict()
    data_neg["training_stats"]["final_loss"] = -0.001
    with pytest.raises(AutoencoderArtifactError, match="must be a finite non-negative float"):
        AutoencoderArtifact.from_json(json.dumps(data_neg))

    # NaN loss is rejected
    data_nan = _sample_valid_artifact_dict()
    data_nan["training_stats"]["final_loss"] = float("nan")
    with pytest.raises(AutoencoderArtifactError, match="must be a finite non-negative float"):
        AutoencoderArtifact.from_json(json.dumps(data_nan))

    # +Inf loss is rejected
    data_inf = _sample_valid_artifact_dict()
    data_inf["training_stats"]["final_loss"] = float("inf")
    with pytest.raises(AutoencoderArtifactError, match="must be a finite non-negative float"):
        AutoencoderArtifact.from_json(json.dumps(data_inf))

    # Missing / None loss is rejected
    data_none = _sample_valid_artifact_dict()
    data_none["training_stats"]["final_loss"] = None
    with pytest.raises(AutoencoderArtifactError, match="must be a numeric float"):
        AutoencoderArtifact.from_json(json.dumps(data_none))

    # Boolean loss is rejected
    data_bool = _sample_valid_artifact_dict()
    data_bool["training_stats"]["final_loss"] = True
    with pytest.raises(AutoencoderArtifactError, match="must be a numeric float"):
        AutoencoderArtifact.from_json(json.dumps(data_bool))


def test_artifact_incompatible_framework_name_or_version() -> None:
    data = _sample_valid_artifact_dict()
    data["framework_name"] = "pytorch"
    with pytest.raises(AutoencoderArtifactError, match="Incompatible framework_name"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["framework_version"] = "0.24.2"
    with pytest.raises(AutoencoderArtifactError, match="Incompatible framework_version"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_missing_or_invalid_scaling_metadata() -> None:
    data = _sample_valid_artifact_dict()
    data["scaling_parameters"]["centers"] = [100.0]  # Mismatch dimension (1 vs 2)
    with pytest.raises(AutoencoderArtifactError, match="scaling_parameters centers length mismatch"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["scaling_parameters"]["scales"] = [5.0, 0.0]  # Zero scale
    with pytest.raises(AutoencoderArtifactError, match="must be finite positive"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_missing_or_invalid_normalization_metadata() -> None:
    data = _sample_valid_artifact_dict()
    data["score_normalization_method"] = "unsupported_method"
    with pytest.raises(AutoencoderArtifactError, match="Unsupported score_normalization_method"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["score_normalization_parameters"] = {}  # Missing parameters
    with pytest.raises(AutoencoderArtifactError, match="Missing required normalization parameter"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_non_converged_or_non_finite_loss_rejection() -> None:
    data = _sample_valid_artifact_dict()
    data["training_stats"]["converged"] = False
    with pytest.raises(AutoencoderArtifactError, match="converged is False"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["training_stats"]["final_loss"] = None
    with pytest.raises(AutoencoderArtifactError, match="final_loss must be a numeric float"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_non_finite_weights_and_intercepts_rejection() -> None:
    data = _sample_valid_artifact_dict()
    data["coefs"][0][0][0] = float("nan")  # NaN weight
    with pytest.raises(AutoencoderArtifactError, match="Non-finite weight detected"):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["intercepts"][0][0] = float("inf")  # Inf bias
    with pytest.raises(AutoencoderArtifactError, match="Non-finite bias detected"):
        AutoencoderArtifact.from_json(json.dumps(data))


def test_artifact_dimension_and_layer_shape_mismatch_rejection() -> None:
    data = _sample_valid_artifact_dict()
    data["feature_names"] = ["single"]  # Less than 2 features
    data["input_dimension"] = 1
    with pytest.raises(AutoencoderArtifactError):
        AutoencoderArtifact.from_json(json.dumps(data))

    data = _sample_valid_artifact_dict()
    data["coefs"] = [[[0.5], [0.5]]]  # Only 1 layer instead of 2
    with pytest.raises(AutoencoderArtifactError, match="coefs layer count mismatch"):
        AutoencoderArtifact.from_json(json.dumps(data))
