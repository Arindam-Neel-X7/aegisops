from datetime import datetime, timedelta, timezone
import math
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.errors import IsolationForestFitError
from app.anomaly.experiment import FeatureWindowConfig
from app.anomaly.features import (
    DeterministicFeatureExtractor,
    FeatureExtractionResult,
    FeatureWindow,
    FeatureWindowStatus,
    RawObservation,
)
from app.anomaly.isolation_forest import (
    MAX_UINT32,
    SUPPORTED_IF_CONFIG_SCHEMA_VERSION,
    IsolationForestBaseline,
    IsolationForestBaselineConfig,
    IsolationForestScoreStatus,
    run_isolation_forest_baseline,
)
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
        reproducibility_key="repro-key-iforest",
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
        feature_config_id="iforest-test-feat-cfg",
        feature_names=feature_names,
        window_size_seconds=interval_seconds,
        step_size_seconds=interval_seconds,
        aggregation_methods=["mean"],
        imputation_strategy="none",
    )

    extractor = DeterministicFeatureExtractor(config=config)
    return extractor.extract_from_observations(observations)


def _sample_iforest_config(
    feature_names: list[str] | None = None,
    min_history: int = 10,
    scaling_method: str = "standard",
    contamination: float | str = 0.05,
    random_state: int = 42,
) -> IsolationForestBaselineConfig:
    return IsolationForestBaselineConfig(
        schema_version="1.0",
        model_name="isolation_forest",
        model_version="1.0.0",
        feature_names=feature_names or ["http_request_duration_ms:mean", "http_requests_total:mean"],
        scaling_method=scaling_method,  # type: ignore[arg-type]
        min_training_windows=min_history,
        contamination=contamination,  # type: ignore[arg-type]
        n_estimators=50,
        random_state=random_state,
        n_jobs=1,
    )


def test_iforest_config_validation() -> None:
    cfg = _sample_iforest_config()
    assert cfg.schema_version == SUPPORTED_IF_CONFIG_SCHEMA_VERSION
    assert cfg.model_name == "isolation_forest"
    assert cfg.model_version == "1.0.0"
    assert len(cfg.feature_names) == 2

    # Reject invalid model name
    with pytest.raises(ValidationError):
        IsolationForestBaselineConfig(feature_names=["x"], model_name="prophet")

    # Reject empty feature names list
    with pytest.raises(ValidationError):
        IsolationForestBaselineConfig(feature_names=[])

    # Reject invalid contamination
    with pytest.raises(ValidationError):
        IsolationForestBaselineConfig(feature_names=["x"], contamination=0.6)
    with pytest.raises(ValidationError):
        IsolationForestBaselineConfig(feature_names=["x"], contamination=-0.01)
    with pytest.raises(ValidationError):
        IsolationForestBaselineConfig(feature_names=["x"], contamination="invalid")  # type: ignore[arg-type]

    # Reject invalid threshold ordering
    with pytest.raises(ValidationError, match="Threshold hierarchy invalid"):
        IsolationForestBaselineConfig(
            feature_names=["x"],
            warning_threshold=0.8,
            error_threshold=0.7,
            critical_threshold=0.9,
        )


def test_valid_normal_controlled_multivariate_fixture() -> None:
    latencies = [100.0 + (i % 3) * 5.0 for i in range(20)]
    requests = [10.0 + (i % 2) * 2.0 for i in range(20)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    result = baseline.score_target_window(feat_res, target_window_index=19)

    assert result.status == IsolationForestScoreStatus.SUCCESS
    assert result.raw_score_samples is not None
    assert result.raw_decision_function is not None
    assert result.anomaly_score is not None
    assert 0.0 <= result.anomaly_score < 0.5  # Normal point has low score
    assert result.signal is not None
    assert result.signal.severity == EventSeverity.INFO
    assert result.history_window_count == 19


def test_valid_anomalous_controlled_multivariate_fixture() -> None:
    # 19 normal points (100.0 ms base with typical variance, 10 reqs), followed by severe anomaly (900.0 ms, 100 reqs)
    latencies = [100.0 + (i % 5) * 4.0 for i in range(19)] + [900.0]
    requests = [10.0 + (i % 3) * 2.0 for i in range(19)] + [100.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    result = baseline.score_target_window(feat_res, target_window_index=19)

    assert result.status == IsolationForestScoreStatus.SUCCESS
    assert result.anomaly_score is not None
    assert result.anomaly_score > 0.6  # Severe anomaly has elevated score
    assert result.signal is not None
    assert result.signal.severity in (EventSeverity.WARNING, EventSeverity.ERROR, EventSeverity.CRITICAL)


def test_anomalous_score_higher_than_normal_fixture() -> None:
    lat_normal = [100.0 + (i % 5) * 4.0 for i in range(20)]
    req_normal = [10.0 + (i % 3) * 2.0 for i in range(20)]

    lat_anom = [100.0 + (i % 5) * 4.0 for i in range(19)] + [800.0]
    req_anom = [10.0 + (i % 3) * 2.0 for i in range(19)] + [80.0]

    feat_norm = _build_multivariate_feature_result(lat_normal, requests=req_normal)
    feat_anom = _build_multivariate_feature_result(lat_anom, requests=req_anom)

    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    res_norm = baseline.score_target_window(feat_norm, target_window_index=19)
    res_anom = baseline.score_target_window(feat_anom, target_window_index=19)

    assert res_norm.anomaly_score is not None
    assert res_anom.anomaly_score is not None
    assert res_anom.anomaly_score > res_norm.anomaly_score
    # In scikit-learn score_samples, more negative = more anomalous
    assert res_anom.raw_score_samples is not None and res_norm.raw_score_samples is not None
    assert res_anom.raw_score_samples < res_norm.raw_score_samples


def test_deterministic_repeated_execution_with_same_random_state() -> None:
    latencies = [100.0 + i * 3.0 for i in range(19)] + [600.0]
    requests = [10.0 + i for i in range(19)] + [50.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = _sample_iforest_config(min_history=10, random_state=123)
    baseline1 = IsolationForestBaseline(config=cfg)
    baseline2 = IsolationForestBaseline(config=cfg)

    res1 = baseline1.score_target_window(feat_res, target_window_index=19)
    res2 = baseline2.score_target_window(feat_res, target_window_index=19)

    assert res1.status == res2.status == IsolationForestScoreStatus.SUCCESS
    assert res1.raw_score_samples == pytest.approx(res2.raw_score_samples, rel=1e-5)
    assert res1.raw_decision_function == pytest.approx(res2.raw_decision_function, rel=1e-5)
    assert res1.anomaly_score == pytest.approx(res2.anomaly_score, rel=1e-5)


def test_schema_valid_anomaly_signal_output() -> None:
    latencies = [100.0 for _ in range(19)] + [700.0]
    requests = [10.0 for _ in range(19)] + [50.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)

    res = run_isolation_forest_baseline(feat_res, config=cfg)

    assert res.signal is not None
    signal: AnomalySignal = res.signal

    assert signal.schema_version == "1.0"
    assert signal.model_name == "isolation_forest"
    assert signal.model_version == "1.0.0"
    assert signal.service == "order-service"
    assert 0.0 <= signal.anomaly_score <= 1.0
    assert len(signal.evidence) == 1

    ev = signal.evidence[0]
    assert ev.evidence_type == "isolation_forest_decision"
    assert ev.details["ordered_features"] == ["http_request_duration_ms:mean", "http_requests_total:mean"]
    assert "raw_feature_vector" in ev.details
    assert "scaled_feature_vector" in ev.details
    assert "raw_score_samples" in ev.details
    assert ev.details["raw_score_interpretation"] == "sklearn_score_samples_opposite_anomaly_direction"
    assert ev.details["scaling_method"] == "standard"
    assert ev.details["random_state"] == 42
    assert ev.details["history_window_count"] == 19

    cal = signal.threshold_or_calibration
    assert cal.schema_version == "1.0"
    assert cal.method == "empirical_spread_offset_sigmoid"
    assert cal.threshold_value == 0.75

    assert signal.run_id == feat_res.run_id
    assert signal.scenario_id == feat_res.scenario_id
    assert signal.seed == feat_res.seed
    assert signal.reproducibility_key == feat_res.reproducibility_key


def test_temporal_causality_target_and_future_excluded() -> None:
    # 25 windows: 0..14 normal (100.0), 15 target (100.0), 16..24 huge future anomalies (9999.0)
    latencies = [100.0 for _ in range(16)] + [9999.0 for _ in range(9)]
    requests = [10.0 for _ in range(16)] + [999.0 for _ in range(9)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    # Score target index 15: training history must strictly be 0..14 (length 15)
    res = baseline.score_target_window(feat_res, target_window_index=15)

    assert res.status == IsolationForestScoreStatus.SUCCESS
    assert res.history_window_count == 15
    assert res.anomaly_score is not None and res.anomaly_score < 0.2


def test_scaling_methods_standard_robust_minmax_none() -> None:
    latencies = [100.0 + i * 10.0 for i in range(19)] + [800.0]
    requests = [10.0 + i for i in range(19)] + [90.0]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    for method in ("standard", "robust", "minmax", "none"):
        cfg = _sample_iforest_config(min_history=10, scaling_method=method)
        baseline = IsolationForestBaseline(config=cfg)
        res = baseline.score_target_window(feat_res, target_window_index=19)

        assert res.status == IsolationForestScoreStatus.SUCCESS
        assert res.anomaly_score is not None
        assert res.signal is not None
        assert res.signal.evidence[0].details["scaling_method"] == method


def test_zero_variance_features_handled_safely() -> None:
    # latencies vary, but requests is strictly constant 10.0 across all history windows
    latencies = [100.0 + i * 5 for i in range(19)] + [600.0]
    requests = [10.0 for _ in range(20)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10, scaling_method="standard")
    baseline = IsolationForestBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == IsolationForestScoreStatus.SUCCESS
    assert res.anomaly_score is not None
    # Constant feature scaled without NaN/Inf
    assert math.isfinite(res.scaled_feature_vector["http_requests_total:mean"])


def test_measured_zero_accepted_and_scored() -> None:
    latencies = [0.0 for _ in range(20)]
    requests = [0.0 for _ in range(20)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == IsolationForestScoreStatus.SUCCESS
    assert res.anomaly_score is not None
    assert res.anomaly_score == 0.0


def test_insufficient_history_returns_explicit_status() -> None:
    latencies = [100.0 for _ in range(5)]
    requests = [10.0 for _ in range(5)]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=4)

    assert res.status == IsolationForestScoreStatus.INSUFFICIENT_HISTORY
    assert res.signal is None
    assert "requires at least 10" in str(res.error_message)


def test_empty_series_returns_invalid_input() -> None:
    cfg_feat = FeatureWindowConfig(
        feature_config_id="empty-cfg",
        feature_names=["http_request_duration_ms:mean"],
        window_size_seconds=10.0,
        step_size_seconds=10.0,
    )
    extractor = DeterministicFeatureExtractor(config=cfg_feat)
    feat_empty = extractor.extract_from_observations([])

    cfg = _sample_iforest_config(feature_names=["http_request_duration_ms:mean"], min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    res = baseline.score_target_window(feat_empty)

    assert res.status == IsolationForestScoreStatus.INVALID_INPUT
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

    cfg = _sample_iforest_config(min_history=3)
    baseline = IsolationForestBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=1)

    assert res.status == IsolationForestScoreStatus.MISSING_FEATURES
    assert res.signal is None
    assert "http_requests_total:mean" in str(res.error_message)


def test_score_series_sequential_causal_evaluation() -> None:
    # 20 windows: 0..14 normal with typical variation, 15..19 severe anomaly
    latencies = [100.0 + (i % 5) * 4.0 for i in range(15)] + [500.0, 600.0, 700.0, 800.0, 900.0]
    requests = [10.0 + (i % 3) * 2.0 for i in range(15)] + [50.0, 60.0, 70.0, 80.0, 90.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests)
    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    series_results = baseline.score_series(feat_res, start_index=10)

    assert len(series_results) == 10  # 10..19
    for r in series_results:
        assert r.status == IsolationForestScoreStatus.SUCCESS

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

    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    def mock_fail(*args: object, **kwargs: object) -> tuple[dict[str, float], dict[str, float], float, float, float, object, int]:
        raise IsolationForestFitError("Tree builder crashed")

    monkeypatch.setattr(baseline, "_fit_and_score", mock_fail)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == IsolationForestScoreStatus.FIT_FAILURE
    assert res.signal is None
    assert "Tree builder crashed" in str(res.error_message)


def test_explicit_random_state_propagation() -> None:
    latencies = [100.0 + i * 4.0 for i in range(19)] + [800.0]
    requests = [10.0 + i * 2.0 for i in range(19)] + [80.0]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    # Different seeds yield valid deterministic results
    cfg1 = _sample_iforest_config(min_history=10, random_state=42)
    cfg2 = _sample_iforest_config(min_history=10, random_state=999)

    baseline1 = IsolationForestBaseline(config=cfg1)
    baseline2 = IsolationForestBaseline(config=cfg2)

    res1 = baseline1.score_target_window(feat_res, target_window_index=19)
    res2 = baseline2.score_target_window(feat_res, target_window_index=19)

    assert res1.status == res2.status == IsolationForestScoreStatus.SUCCESS
    assert res1.anomaly_score is not None and res2.anomaly_score is not None
    assert res1.signal is not None and res2.signal is not None
    assert res1.signal.evidence[0].details["random_state"] == 42
    assert res2.signal.evidence[0].details["random_state"] == 999


def test_deterministic_feature_order_preserved() -> None:
    latencies = [100.0 for _ in range(19)] + [500.0]
    requests = [10.0 for _ in range(19)] + [50.0]
    errors = [0.0 for _ in range(19)] + [10.0]

    feat_res = _build_multivariate_feature_result(latencies, requests=requests, errors=errors)

    order_a = ["http_request_duration_ms:mean", "http_requests_total:mean", "http_errors_total:mean"]
    order_b = ["http_errors_total:mean", "http_requests_total:mean", "http_request_duration_ms:mean"]

    cfg_a = _sample_iforest_config(feature_names=order_a, min_history=10)
    cfg_b = _sample_iforest_config(feature_names=order_b, min_history=10)

    baseline_a = IsolationForestBaseline(config=cfg_a)
    baseline_b = IsolationForestBaseline(config=cfg_b)

    res_a = baseline_a.score_target_window(feat_res, target_window_index=19)
    res_b = baseline_b.score_target_window(feat_res, target_window_index=19)

    assert res_a.signal is not None and res_b.signal is not None
    assert res_a.signal.evidence[0].details["ordered_features"] == order_a
    assert res_b.signal.evidence[0].details["ordered_features"] == order_b


def test_warmup_only_input_returns_insufficient_history() -> None:
    # 5 warmup windows, min_history required is 10
    latencies = [100.0 for _ in range(5)]
    requests = [10.0 for _ in range(5)]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    cfg = _sample_iforest_config(min_history=10)
    baseline = IsolationForestBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=4)
    assert res.status == IsolationForestScoreStatus.INSUFFICIENT_HISTORY
    assert res.signal is None


def test_random_state_negative_rejection() -> None:
    with pytest.raises(ValidationError, match="random_state"):
        IsolationForestBaselineConfig(feature_names=["x"], random_state=-1)

    with pytest.raises(ValidationError, match="random_state"):
        IsolationForestBaselineConfig(feature_names=["x"], random_state=-100)


def test_random_state_boolean_rejection() -> None:
    with pytest.raises(ValidationError, match="random_state cannot be a boolean"):
        IsolationForestBaselineConfig(feature_names=["x"], random_state=True)  # type: ignore[arg-type]

    with pytest.raises(ValidationError, match="random_state cannot be a boolean"):
        IsolationForestBaselineConfig(feature_names=["x"], random_state=False)  # type: ignore[arg-type]


def test_random_state_zero_boundary() -> None:
    cfg = IsolationForestBaselineConfig(feature_names=["x"], random_state=0)
    assert cfg.random_state == 0

    latencies = [100.0 + (i % 5) * 4.0 for i in range(19)] + [900.0]
    feat_res = _build_multivariate_feature_result(latencies)
    cfg_full = _sample_iforest_config(feature_names=["http_request_duration_ms:mean"], min_history=10, random_state=0)
    baseline = IsolationForestBaseline(config=cfg_full)
    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == IsolationForestScoreStatus.SUCCESS
    assert res.signal is not None
    assert res.signal.evidence[0].details["random_state"] == 0
    assert res.signal.evidence[0].details["effective_model_seed"] == 0


def test_random_state_max_uint32_boundary() -> None:
    cfg = IsolationForestBaselineConfig(feature_names=["x"], random_state=MAX_UINT32)
    assert cfg.random_state == 4294967295

    latencies = [100.0 + (i % 5) * 4.0 for i in range(19)] + [900.0]
    feat_res = _build_multivariate_feature_result(latencies)
    cfg_full = _sample_iforest_config(feature_names=["http_request_duration_ms:mean"], min_history=10, random_state=MAX_UINT32)
    baseline = IsolationForestBaseline(config=cfg_full)
    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == IsolationForestScoreStatus.SUCCESS
    assert res.signal is not None
    assert res.signal.evidence[0].details["random_state"] == MAX_UINT32
    assert res.signal.evidence[0].details["effective_model_seed"] == MAX_UINT32


def test_random_state_2_pow_32_rejected() -> None:
    with pytest.raises(ValidationError, match="random_state"):
        IsolationForestBaselineConfig(feature_names=["x"], random_state=2**32)


def test_random_state_max_uint64_rejected() -> None:
    with pytest.raises(ValidationError, match="random_state"):
        IsolationForestBaselineConfig(feature_names=["x"], random_state=MAX_UINT64)


def test_random_state_effective_seed_propagation() -> None:
    cfg = _sample_iforest_config(random_state=12345)
    baseline = IsolationForestBaseline(config=cfg)

    latencies = [100.0 + (i % 5) * 4.0 for i in range(19)] + [900.0]
    requests = [10.0 + (i % 3) * 2.0 for i in range(19)] + [100.0]
    feat_res = _build_multivariate_feature_result(latencies, requests=requests)

    res = baseline.score_target_window(feat_res, target_window_index=19)

    assert res.status == IsolationForestScoreStatus.SUCCESS
    assert res.signal is not None
    ev = res.signal.evidence[0]
    assert ev.details["random_state"] == 12345
    assert ev.details["effective_model_seed"] == 12345
