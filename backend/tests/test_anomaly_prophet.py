from datetime import datetime, timedelta, timezone
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.errors import ProphetFitError
from app.anomaly.experiment import FeatureWindowConfig
from app.anomaly.features import (
    DeterministicFeatureExtractor,
    FeatureExtractionResult,
    FeatureWindow,
    FeatureWindowStatus,
    RawObservation,
)
from app.anomaly.models import AnomalySignal
from app.anomaly.prophet import (
    SUPPORTED_PROPHET_CONFIG_SCHEMA_VERSION,
    ProphetBaseline,
    ProphetBaselineConfig,
    ProphetScoreStatus,
    run_prophet_baseline,
)
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
        reproducibility_key="repro-key-prophet",
    )


def _build_feature_result(
    values: list[float],
    metric_name: str = "http_request_duration_ms",
    interval_seconds: float = 10.0,
    regressors_data: dict[str, list[float]] | None = None,
) -> FeatureExtractionResult:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    observations: list[RawObservation] = []

    for i, val in enumerate(values):
        t = t0 + timedelta(seconds=i * interval_seconds)
        observations.append(
            _make_observation(event_time=t, value=val, metric_name=metric_name)
        )
        if regressors_data:
            for reg_name, reg_vals in regressors_data.items():
                observations.append(
                    _make_observation(
                        event_time=t, value=reg_vals[i], metric_name=reg_name
                    )
                )

    feature_names = [f"{metric_name}:mean"]
    if regressors_data:
        for reg_name in regressors_data:
            feature_names.append(f"{reg_name}:mean")

    config = FeatureWindowConfig(
        feature_config_id="prophet-test-feat-cfg",
        feature_names=feature_names,
        window_size_seconds=interval_seconds,
        step_size_seconds=interval_seconds,
        aggregation_methods=["mean"],
        imputation_strategy="none",
    )

    extractor = DeterministicFeatureExtractor(config=config)
    return extractor.extract_from_observations(observations)


def _sample_prophet_config(
    target_feature: str = "http_request_duration_ms:mean",
    min_history: int = 10,
    regressors: list[str] | None = None,
) -> ProphetBaselineConfig:
    return ProphetBaselineConfig(
        schema_version="1.0",
        model_name="prophet",
        model_version="1.0.0",
        target_feature=target_feature,
        regressors=regressors or [],
        min_history_windows=min_history,
        interval_width=0.95,
        growth="linear",
        seasonality_mode="additive",
        uncertainty_samples=0,  # Fast deterministic point & interval calculation
        seed=42,
    )


def test_prophet_config_validation() -> None:
    cfg = _sample_prophet_config()
    assert cfg.schema_version == SUPPORTED_PROPHET_CONFIG_SCHEMA_VERSION
    assert cfg.model_name == "prophet"
    assert cfg.model_version == "1.0.0"
    assert cfg.target_feature == "http_request_duration_ms:mean"

    # Reject invalid model name
    with pytest.raises(ValidationError):
        ProphetBaselineConfig(target_feature="x", model_name="isolation_forest")

    # Reject invalid growth
    with pytest.raises(ValidationError):
        ProphetBaselineConfig(target_feature="x", growth="exponential")  # type: ignore[arg-type]

    # Reject invalid seasonality mode
    with pytest.raises(ValidationError):
        ProphetBaselineConfig(target_feature="x", seasonality_mode="invalid")  # type: ignore[arg-type]

    # Reject invalid threshold ordering
    with pytest.raises(ValidationError, match="Threshold hierarchy invalid"):
        ProphetBaselineConfig(
            target_feature="x",
            warning_threshold=0.8,
            error_threshold=0.7,
            critical_threshold=0.9,
        )


def test_valid_normal_controlled_series() -> None:
    # 15 windows of normal steady latency (100.0 ms with minor variation)
    normal_values = [100.0 + (i % 3) for i in range(15)]
    feat_res = _build_feature_result(normal_values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    # Score window 14 (normal point)
    result = baseline.score_target_window(feat_res, target_window_index=14)

    assert result.status == ProphetScoreStatus.SUCCESS
    assert result.observed_value is not None
    assert result.forecast_value is not None
    assert result.anomaly_score is not None
    assert 0.0 <= result.anomaly_score < 0.5  # Low anomaly score for normal point
    assert result.signal is not None
    assert result.signal.severity == EventSeverity.INFO
    assert result.history_window_count == 14


def test_valid_anomalous_controlled_series() -> None:
    # 14 normal points (100.0 ms), followed by a massive spike at index 14 (800.0 ms)
    anomalous_values = [100.0 for _ in range(14)] + [800.0]
    feat_res = _build_feature_result(anomalous_values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    result = baseline.score_target_window(feat_res, target_window_index=14)

    assert result.status == ProphetScoreStatus.SUCCESS
    assert result.observed_value == 800.0
    assert result.anomaly_score is not None
    assert result.anomaly_score > 0.85  # High anomaly score for 8x spike
    assert result.signal is not None
    assert result.signal.severity in (EventSeverity.ERROR, EventSeverity.CRITICAL)


def test_anomalous_score_higher_than_normal_fixture() -> None:
    normal_values = [100.0 for _ in range(15)]
    anomalous_values = [100.0 for _ in range(14)] + [500.0]

    feat_normal = _build_feature_result(normal_values)
    feat_anom = _build_feature_result(anomalous_values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    res_norm = baseline.score_target_window(feat_normal, target_window_index=14)
    res_anom = baseline.score_target_window(feat_anom, target_window_index=14)

    assert res_norm.anomaly_score is not None
    assert res_anom.anomaly_score is not None
    assert res_anom.anomaly_score > res_norm.anomaly_score


def test_deterministic_repeated_execution() -> None:
    values = [100.0 + i * 2 for i in range(14)] + [400.0]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline1 = ProphetBaseline(config=cfg)
    baseline2 = ProphetBaseline(config=cfg)

    res1 = baseline1.score_target_window(feat_res, target_window_index=14)
    res2 = baseline2.score_target_window(feat_res, target_window_index=14)

    assert res1.status == res2.status == ProphetScoreStatus.SUCCESS
    assert res1.forecast_value == pytest.approx(res2.forecast_value, rel=1e-5)
    assert res1.anomaly_score == pytest.approx(res2.anomaly_score, rel=1e-5)
    assert res1.residual == pytest.approx(res2.residual, rel=1e-5)


def test_schema_valid_anomaly_signal_output() -> None:
    values = [100.0 for _ in range(14)] + [600.0]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    res = run_prophet_baseline(feat_res, config=cfg)

    assert res.signal is not None
    signal: AnomalySignal = res.signal

    assert signal.schema_version == "1.0"
    assert signal.model_name == "prophet"
    assert signal.model_version == "1.0.0"
    assert signal.service == "order-service"
    assert signal.metric_or_feature == "http_request_duration_ms:mean"
    assert 0.0 <= signal.anomaly_score <= 1.0
    assert len(signal.evidence) == 1

    ev = signal.evidence[0]
    assert ev.evidence_type == "prophet_forecast_residual"
    assert ev.metric_or_feature == "http_request_duration_ms:mean"
    assert ev.observed_value == 600.0
    assert ev.expected_value is not None
    assert ev.details["history_window_count"] == 14
    assert ev.details["score_normalization_method"] == "residual_interval_ratio_sigmoid"

    # Calibration contract
    cal = signal.threshold_or_calibration
    assert cal.schema_version == "1.0"
    assert cal.method == "residual_interval_ratio_sigmoid"
    assert cal.calibration_version == "1.0.0"
    assert cal.threshold_value == 0.75

    # Provenance fields
    assert signal.run_id == feat_res.run_id
    assert signal.scenario_id == feat_res.scenario_id
    assert signal.seed == feat_res.seed
    assert signal.reproducibility_key == feat_res.reproducibility_key
    assert len(signal.source_event_ids) > 0


def test_temporal_causality_target_excluded_from_training() -> None:
    # 15 windows: index 0..13 normal (100.0), index 14 is 999.0
    values = [100.0 for _ in range(14)] + [999.0]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    # Score window 14: training history MUST be strictly indices 0..13
    res = baseline.score_target_window(feat_res, target_window_index=14)

    assert res.status == ProphetScoreStatus.SUCCESS
    assert res.history_window_count == 14
    # The forecast for index 14 based on history 0..13 (all 100.0) should be approx 100.0, NOT shifted by 999.0!
    assert res.forecast_value == pytest.approx(100.0, abs=1.0)


def test_temporal_causality_future_windows_excluded_from_training() -> None:
    # 20 windows: 0..9 normal (100.0), target index 10 (100.0), future 11..19 extreme (5000.0)
    values = [100.0 for _ in range(11)] + [5000.0 for _ in range(9)]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    # Score target index 10: future windows (11..19) MUST have 0 influence on forecast at index 10
    res = baseline.score_target_window(feat_res, target_window_index=10)

    assert res.status == ProphetScoreStatus.SUCCESS
    assert res.history_window_count == 10
    assert res.forecast_value == pytest.approx(100.0, abs=1.0)
    assert res.anomaly_score is not None and res.anomaly_score < 0.1


def test_measured_zero_remains_valid_input() -> None:
    # 15 windows with measured 0.0 values (e.g. 0.0 error count or 0.0 rate)
    zero_values = [0.0 for _ in range(15)]
    feat_res = _build_feature_result(zero_values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=14)

    assert res.status == ProphetScoreStatus.SUCCESS
    assert res.observed_value == 0.0
    assert res.forecast_value == pytest.approx(0.0, abs=1e-3)
    assert res.anomaly_score == 0.0


def test_insufficient_history_returns_explicit_status() -> None:
    # Only 5 windows available, but config requires 10
    values = [100.0 for _ in range(5)]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=4)

    assert res.status == ProphetScoreStatus.INSUFFICIENT_HISTORY
    assert res.signal is None
    assert res.anomaly_score is None
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

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_empty)

    assert res.status == ProphetScoreStatus.INVALID_INPUT
    assert res.signal is None


def test_missing_target_value_in_target_window() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=10)

    # Window 0 has values; Window 1 is incomplete (missing target feature)
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
        values={"http_request_duration_ms:mean": 100.0},
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
        values={},
        missing_features=["http_request_duration_ms:mean"],
        status=FeatureWindowStatus.EMPTY,
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
        feature_names=["http_request_duration_ms:mean"],
        windows=[w0, w1],
        total_windows=2,
        valid_windows=1,
        extracted_at=datetime.now(timezone.utc),
    )

    cfg = _sample_prophet_config(min_history=3)
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=1)

    assert res.status == ProphetScoreStatus.MISSING_TARGET_VALUE
    assert res.signal is None


def test_constant_series_scores_zero_for_constant_target() -> None:
    values = [50.0 for _ in range(15)]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=14)

    assert res.status == ProphetScoreStatus.SUCCESS
    assert res.observed_value == 50.0
    assert res.forecast_value == pytest.approx(50.0, abs=1e-3)
    assert res.anomaly_score == 0.0


def test_multivariate_prophet_with_regressors() -> None:
    # 15 windows with duration and request rate
    durations = [100.0 + i * 2 for i in range(14)] + [600.0]
    requests = [10.0 + (i % 2) * 5 for i in range(15)]

    feat_res = _build_feature_result(
        durations,
        metric_name="http_request_duration_ms",
        regressors_data={"http_requests_total": requests},
    )

    cfg = _sample_prophet_config(
        target_feature="http_request_duration_ms:mean",
        regressors=["http_requests_total:mean"],
        min_history=10,
    )
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=14)

    assert res.status == ProphetScoreStatus.SUCCESS
    assert res.anomaly_score is not None and res.anomaly_score > 0.8
    assert res.signal is not None
    assert res.signal.metric_or_feature == "http_request_duration_ms:mean"


def test_score_series_sequential_causal_evaluation() -> None:
    # 15 windows: 0..11 normal (100.0), 12..14 spike (400.0, 500.0, 600.0)
    values = [100.0 for _ in range(12)] + [400.0, 500.0, 600.0]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    series_results = baseline.score_series(feat_res, start_index=10)

    # Scored indices: 10, 11, 12, 13, 14 -> 5 results
    assert len(series_results) == 5
    for r in series_results:
        assert r.status == ProphetScoreStatus.SUCCESS

    # Index 10 & 11 (normal points) have low scores
    assert (
        series_results[0].anomaly_score is not None
        and series_results[0].anomaly_score < 0.2
    )
    assert (
        series_results[1].anomaly_score is not None
        and series_results[1].anomaly_score < 0.2
    )

    # Index 12, 13, 14 (anomalies) have elevated scores significantly above baseline
    assert (
        series_results[2].anomaly_score is not None
        and series_results[2].anomaly_score > 0.7
    )
    assert (
        series_results[3].anomaly_score is not None
        and series_results[3].anomaly_score > 0.5
    )
    assert (
        series_results[4].anomaly_score is not None
        and series_results[4].anomaly_score > 0.5
    )
    assert series_results[2].anomaly_score > series_results[0].anomaly_score
    assert series_results[3].anomaly_score > series_results[0].anomaly_score
    assert series_results[4].anomaly_score > series_results[0].anomaly_score


def test_boundary_anomaly_scores_clamped_in_zero_one() -> None:
    # Test normalization function directly for boundary values
    cfg = _sample_prophet_config()
    baseline = ProphetBaseline(config=cfg)

    # Exact zero deviation -> 0.0
    _, _, score_zero = baseline._compute_normalized_score(
        100.0, 100.0, 95.0, 105.0, [100.0] * 10
    )
    assert score_zero == 0.0

    # Huge deviation -> approaching 1.0, strictly bounded
    _, _, score_huge = baseline._compute_normalized_score(
        100000.0, 100.0, 95.0, 105.0, [100.0] * 10
    )
    assert 0.999 <= score_huge <= 1.0


def test_fit_failure_returns_fit_failure_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = [100.0 for _ in range(15)]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    # Force _fit_and_predict to raise ProphetFitError
    def mock_fit_fail(*args: object, **kwargs: object) -> tuple[float, float, float]:
        raise ProphetFitError("Stan optimizer did not converge")

    monkeypatch.setattr(baseline, "_fit_and_predict", mock_fit_fail)

    res = baseline.score_target_window(feat_res, target_window_index=14)

    assert res.status == ProphetScoreStatus.FIT_FAILURE
    assert res.signal is None
    assert "Stan optimizer did not converge" in str(res.error_message)


def test_provenance_and_context_propagation() -> None:
    values = [100.0 for _ in range(14)] + [350.0]
    feat_res = _build_feature_result(values)

    cfg = _sample_prophet_config(min_history=10)
    baseline = ProphetBaseline(config=cfg)

    res = baseline.score_target_window(feat_res, target_window_index=14)

    assert res.status == ProphetScoreStatus.SUCCESS
    assert res.signal is not None
    sig = res.signal

    # Ensure complete execution context is faithfully propagated
    assert sig.run_id == feat_res.run_id
    assert sig.scenario_id == feat_res.scenario_id
    assert sig.scenario_version == feat_res.scenario_version
    assert sig.seed == feat_res.seed
    assert sig.reproducibility_key == feat_res.reproducibility_key
    assert sig.environment == feat_res.environment
    assert sig.tenant_id == feat_res.tenant_id
    assert sig.service == feat_res.service
    assert sig.source_event_ids == feat_res.windows[14].source_event_ids
    assert sig.source_event_time_window is not None
    assert sig.source_event_time_window.start_time == feat_res.windows[14].window_start
    assert sig.source_event_time_window.end_time == feat_res.windows[14].window_end
