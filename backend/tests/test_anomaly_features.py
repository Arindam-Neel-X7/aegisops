from datetime import datetime, timedelta, timezone
import math
import random
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.errors import (
    FeatureExtractionError,
    InsufficientDataError,
    InvalidFeatureInputError,
)
from app.anomaly.experiment import FeatureWindowConfig
from app.anomaly.features import (
    SUPPORTED_FEATURE_SCHEMA_VERSION,
    DeterministicFeatureExtractor,
    FeatureWindow,
    FeatureWindowStatus,
    RawObservation,
    compute_aggregation,
    compute_quantile,
    extract_features,
)
from app.simulator.ground_truth import GroundTruthBuilder, ResearchRunContext
from app.simulator.runtime.result import ScenarioRunResult
from app.simulator.scenarios import get_scenario
from app.telemetry.query.models import MetricSample
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.transport.serialization import TelemetryExecutionContext


def _sample_raw_observation(
    event_time: datetime,
    metric_name: str = "http_request_duration_ms",
    value: float = 100.0,
    service: str = "order-service",
    event_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
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
        run_id=run_id or uuid.UUID("11111111-1111-1111-1111-111111111111"),
        scenario_id="latency-spike",
        scenario_version="1.0.0",
        seed=seed,
        reproducibility_key="repro-key-42",
        trace_id="trace-123",
        tags={"tier": "backend"},
    )


def _sample_feature_config(
    window_size: float = 10.0,
    step_size: float = 5.0,
    features: list[str] | None = None,
    imputation: str = "forward_fill",
) -> FeatureWindowConfig:
    return FeatureWindowConfig(
        feature_config_id="feat-cfg-v1",
        feature_names=features
        or ["http_request_duration_ms:mean", "http_requests_total:sum"],
        window_size_seconds=window_size,
        step_size_seconds=step_size,
        aggregation_methods=["mean", "sum", "p95"],
        imputation_strategy=imputation,
    )


def test_compute_quantile_deterministic() -> None:
    data = [10.0, 20.0, 30.0, 40.0, 50.0]
    assert compute_quantile(data, 0.0) == 10.0
    assert compute_quantile(data, 1.0) == 50.0
    assert compute_quantile(data, 0.5) == 30.0
    assert compute_quantile(data, 0.25) == 20.0
    assert compute_quantile(data, 0.75) == 40.0

    # Single element
    assert compute_quantile([42.0], 0.5) == 42.0

    # Empty raises
    with pytest.raises(ValueError, match="empty"):
        compute_quantile([], 0.5)


def test_compute_aggregation_all_methods() -> None:
    vals = [10.0, 20.0, 30.0, 40.0, 50.0]

    assert compute_aggregation(vals, "mean") == 30.0
    assert compute_aggregation(vals, "median") == 30.0
    assert compute_aggregation(vals, "p50") == 30.0
    assert compute_aggregation(vals, "p90") == 46.0
    assert compute_aggregation(vals, "p95") == 48.0
    assert compute_aggregation(vals, "p99") == 49.6
    assert compute_aggregation(vals, "min") == 10.0
    assert compute_aggregation(vals, "max") == 50.0
    assert compute_aggregation(vals, "sum") == 150.0
    assert compute_aggregation(vals, "count") == 5.0
    assert compute_aggregation(vals, "rate", window_duration_seconds=10.0) == 0.5
    assert compute_aggregation(vals, "first") == 10.0
    assert compute_aggregation(vals, "last") == 50.0

    # Std deviation
    std_val = compute_aggregation(vals, "std")
    expected_std = math.sqrt(sum((x - 30.0) ** 2 for x in vals) / 4)
    assert math.isclose(std_val, expected_std)

    # Single item std is 0.0
    assert compute_aggregation([10.0], "std") == 0.0

    # Unsupported method raises
    with pytest.raises(ValueError, match="Unsupported aggregation method"):
        compute_aggregation(vals, "invalid_method")

    # Empty values raise
    with pytest.raises(ValueError, match="empty"):
        compute_aggregation([], "mean")


def test_raw_observation_validation() -> None:
    base_time = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    obs = _sample_raw_observation(event_time=base_time)

    assert obs.service == "order-service"
    assert obs.metric_name == "http_request_duration_ms"
    assert obs.value == 100.0
    assert obs.seed == 42

    # Reject non-finite values
    with pytest.raises(ValidationError):
        _sample_raw_observation(event_time=base_time, value=float("nan"))
    with pytest.raises(ValidationError):
        _sample_raw_observation(event_time=base_time, value=float("inf"))

    # Reject naive datetime
    naive_dt = datetime(2026, 10, 1, 12, 0, 0)
    with pytest.raises(ValidationError):
        _sample_raw_observation(event_time=naive_dt)

    # Reject empty service / metric_name
    with pytest.raises(ValidationError):
        _sample_raw_observation(event_time=base_time, service="   ")
    with pytest.raises(ValidationError):
        _sample_raw_observation(event_time=base_time, metric_name="")


def test_feature_window_validation() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=10)

    win = FeatureWindow(
        window_index=0,
        window_start=t0,
        window_end=t1,
        observation_timestamp=t1,
        service="order-service",
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        run_id=uuid.UUID(int=2),
        scenario_id="latency-spike",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-1",
        feature_config_id="cfg-1",
        values={"http_request_duration_ms:mean": 120.5},
        source_event_ids=[uuid.uuid4()],
        source_event_count=1,
    )
    assert win.window_index == 0
    assert win.status == FeatureWindowStatus.COMPLETE

    # Reject start > end
    with pytest.raises(ValidationError):
        FeatureWindow(
            window_index=0,
            window_start=t1,
            window_end=t0,
            observation_timestamp=t1,
            service="order-service",
            tenant_id=uuid.UUID(int=1),
            environment="simulation",
            run_id=uuid.UUID(int=2),
            scenario_id="latency-spike",
            scenario_version="1.0.0",
            seed=42,
            reproducibility_key="rep-1",
            feature_config_id="cfg-1",
        )


def test_deterministic_feature_extraction_minimal_and_full() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(window_size=10.0, step_size=10.0)

    # 1. Minimal: 1 observation
    obs1 = _sample_raw_observation(
        event_time=t0, metric_name="http_request_duration_ms", value=50.0
    )
    extractor = DeterministicFeatureExtractor(config=cfg)
    res_min = extractor.extract_from_observations([obs1])

    assert res_min.schema_version == SUPPORTED_FEATURE_SCHEMA_VERSION
    assert res_min.total_windows >= 1
    assert res_min.valid_windows >= 1
    assert res_min.windows[0].window_index == 0
    assert res_min.windows[0].values["http_request_duration_ms:mean"] == 50.0
    assert len(res_min.source_event_ids) == 1
    assert res_min.source_event_ids[0] == obs1.event_id

    # 2. Fully populated: observations across multiple windows
    observations = []
    for sec in range(0, 30, 2):
        t = t0 + timedelta(seconds=sec)
        eid = uuid.UUID(f"00000000-0000-0000-0000-{sec:012d}")
        observations.append(
            _sample_raw_observation(
                event_time=t,
                metric_name="http_request_duration_ms",
                value=100.0 + sec,
                event_id=eid,
            )
        )
        observations.append(
            _sample_raw_observation(
                event_time=t,
                metric_name="http_requests_total",
                value=1.0,
                event_id=uuid.UUID(f"10000000-0000-0000-0000-{sec:012d}"),
            )
        )

    res_full = extractor.extract_from_observations(observations)
    assert res_full.total_windows >= 3
    assert res_full.time_range is not None
    assert res_full.time_range.start_time == t0
    assert res_full.time_range.end_time == t0 + timedelta(seconds=28)


def test_same_input_same_seed_determinism() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(window_size=10.0, step_size=5.0)

    raw_data = []
    for i in range(20):
        raw_data.append(
            _sample_raw_observation(
                event_time=t0 + timedelta(seconds=i),
                metric_name="http_request_duration_ms",
                value=float(i * 10),
                event_id=uuid.UUID(f"00000000-0000-0000-0000-{i:012d}"),
                seed=12345,
            )
        )

    extractor1 = DeterministicFeatureExtractor(config=cfg)
    extractor2 = DeterministicFeatureExtractor(config=cfg)

    res1 = extractor1.extract_from_observations(raw_data)
    res2 = extractor2.extract_from_observations(raw_data)

    assert len(res1.windows) == len(res2.windows)
    for w1, w2 in zip(res1.windows, res2.windows):
        assert w1.window_index == w2.window_index
        assert w1.window_start == w2.window_start
        assert w1.window_end == w2.window_end
        assert w1.observation_timestamp == w2.observation_timestamp
        assert w1.values == w2.values
        assert w1.source_event_ids == w2.source_event_ids
        assert w1.status == w2.status


def test_out_of_order_and_late_input_produces_identical_output() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(window_size=10.0, step_size=5.0)

    sorted_obs = []
    for i in range(25):
        sorted_obs.append(
            _sample_raw_observation(
                event_time=t0 + timedelta(seconds=i),
                metric_name="http_request_duration_ms",
                value=float(i * 5),
                event_id=uuid.UUID(f"00000000-0000-0000-0000-{i:012d}"),
            )
        )

    # Scramble the list
    shuffled_obs = list(sorted_obs)
    random.Random(42).shuffle(shuffled_obs)

    extractor = DeterministicFeatureExtractor(config=cfg)
    res_sorted = extractor.extract_from_observations(sorted_obs)
    res_shuffled = extractor.extract_from_observations(shuffled_obs)

    assert len(res_sorted.windows) == len(res_shuffled.windows)
    for w_sorted, w_shuffled in zip(res_sorted.windows, res_shuffled.windows):
        assert w_sorted.window_index == w_shuffled.window_index
        assert w_sorted.values == w_shuffled.values
        assert w_sorted.source_event_ids == w_shuffled.source_event_ids


def test_stable_tie_breaking_for_equal_timestamps() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:first", "http_request_duration_ms:last"],
    )

    # Multiple events at the exact same millisecond with deterministic UUIDs
    id_a = uuid.UUID("00000000-0000-0000-0000-000000000001")
    id_b = uuid.UUID("00000000-0000-0000-0000-000000000002")
    id_c = uuid.UUID("00000000-0000-0000-0000-000000000003")

    obs_b = _sample_raw_observation(event_time=t0, value=20.0, event_id=id_b)
    obs_a = _sample_raw_observation(event_time=t0, value=10.0, event_id=id_a)
    obs_c = _sample_raw_observation(event_time=t0, value=30.0, event_id=id_c)

    # Pass in scrambled order
    extractor = DeterministicFeatureExtractor(config=cfg)
    res1 = extractor.extract_from_observations([obs_b, obs_c, obs_a])
    res2 = extractor.extract_from_observations([obs_c, obs_a, obs_b])

    assert (
        res1.windows[0].values["http_request_duration_ms:first"] == 10.0
    )  # id_a is first alphabetically
    assert (
        res1.windows[0].values["http_request_duration_ms:last"] == 30.0
    )  # id_c is last alphabetically
    assert res1.windows[0].source_event_ids == [id_a, id_b, id_c]
    assert res1.windows[0].values == res2.windows[0].values


def test_duplicate_events_discarded() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:count"]
    )

    dup_id = uuid.UUID("00000000-0000-0000-0000-000000000099")
    obs1 = _sample_raw_observation(event_time=t0, value=100.0, event_id=dup_id)
    obs2 = _sample_raw_observation(event_time=t0, value=100.0, event_id=dup_id)

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs1, obs2])

    assert res.windows[0].values["http_request_duration_ms:count"] == 1.0
    assert len(res.windows[0].source_event_ids) == 1


def test_window_boundary_inclusion() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:count"]
    )

    # Observation exactly at start (t0) and exactly at boundary (t0 + 10s)
    obs_start = _sample_raw_observation(event_time=t0, event_id=uuid.UUID(int=1))
    obs_boundary = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=10), event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs_start, obs_boundary])

    # Window 0: [t0, t0 + 10s) -> contains obs_start, does NOT contain obs_boundary
    # Window 1: [t0 + 10s, t0 + 20s) -> contains obs_boundary
    assert len(res.windows) == 2
    assert res.windows[0].values["http_request_duration_ms:count"] == 1.0
    assert res.windows[0].source_event_ids == [obs_start.event_id]
    assert res.windows[1].values["http_request_duration_ms:count"] == 1.0
    assert res.windows[1].source_event_ids == [obs_boundary.event_id]


def test_warmup_windows_marking() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=5.0,
        step_size=5.0,
        features=["http_request_duration_ms:mean"],
    )

    obs = [
        _sample_raw_observation(
            event_time=t0 + timedelta(seconds=i * 5), event_id=uuid.UUID(int=i + 1)
        )
        for i in range(4)
    ]

    extractor = DeterministicFeatureExtractor(config=cfg, warm_up_windows=2)
    res = extractor.extract_from_observations(obs)

    assert len(res.windows) == 4
    assert res.warmup_windows == 2
    assert res.windows[0].is_warmup is True
    assert res.windows[0].status == FeatureWindowStatus.WARMUP
    assert res.windows[1].is_warmup is True
    assert res.windows[1].status == FeatureWindowStatus.WARMUP
    assert res.windows[2].is_warmup is False
    assert res.windows[2].status == FeatureWindowStatus.COMPLETE
    assert res.windows[3].is_warmup is False
    assert res.windows[3].status == FeatureWindowStatus.COMPLETE


def test_missing_data_forward_fill_imputation() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="forward_fill",
    )

    # Window 0 has data at t0, Window 1 has NO data (gap from t0+10 to t0+20), Window 2 has data at t0+20
    obs0 = _sample_raw_observation(
        event_time=t0, value=150.0, event_id=uuid.UUID(int=1)
    )
    obs2 = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=200.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs0, obs2])

    assert len(res.windows) == 3
    # Window 0: 150.0, COMPLETE
    assert res.windows[0].values["http_request_duration_ms:mean"] == 150.0
    assert res.windows[0].is_imputed is False
    assert res.windows[0].status == FeatureWindowStatus.COMPLETE

    # Window 1: forward-filled from Window 0 -> 150.0, IMPUTED
    assert res.windows[1].values["http_request_duration_ms:mean"] == 150.0
    assert res.windows[1].is_imputed is True
    assert res.windows[1].status == FeatureWindowStatus.IMPUTED
    assert res.windows[1].imputed_features == ["http_request_duration_ms:mean"]
    assert res.windows[1].source_event_ids == []

    # Window 2: 200.0, COMPLETE
    assert res.windows[2].values["http_request_duration_ms:mean"] == 200.0
    assert res.windows[2].is_imputed is False
    assert res.windows[2].status == FeatureWindowStatus.COMPLETE


def test_missing_data_zero_fill_imputation() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="zero_fill",
    )

    obs0 = _sample_raw_observation(
        event_time=t0, value=150.0, event_id=uuid.UUID(int=1)
    )
    obs2 = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=200.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs0, obs2])

    assert res.windows[1].values["http_request_duration_ms:mean"] == 0.0
    assert res.windows[1].is_imputed is True
    assert res.windows[1].status == FeatureWindowStatus.IMPUTED


def test_empty_input_observations() -> None:
    cfg = _sample_feature_config()
    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([])

    assert res.total_windows == 0
    assert res.valid_windows == 0
    assert res.windows == []
    assert res.source_event_ids == []
    assert res.time_range is None


def test_invalid_extractor_configuration() -> None:
    cfg = _sample_feature_config()

    with pytest.raises(InvalidFeatureInputError, match="warm_up_windows"):
        DeterministicFeatureExtractor(config=cfg, warm_up_windows=-1)

    with pytest.raises(InvalidFeatureInputError, match="min_samples_per_window"):
        DeterministicFeatureExtractor(config=cfg, min_samples_per_window=-1)

    with pytest.raises(InvalidFeatureInputError, match="window_timestamp_alignment"):
        DeterministicFeatureExtractor(
            config=cfg, window_timestamp_alignment="invalid_alignment"
        )


def test_model_family_comparability_exports() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean", "http_requests_total:sum"],
    )

    obs = [
        _sample_raw_observation(
            event_time=t0 + timedelta(seconds=i * 10),
            metric_name="http_request_duration_ms",
            value=100.0 + i * 10,
            event_id=uuid.UUID(f"00000000-0000-0000-0000-{i:012d}"),
        )
        for i in range(5)
    ]
    for i in range(5):
        obs.append(
            _sample_raw_observation(
                event_time=t0 + timedelta(seconds=i * 10),
                metric_name="http_requests_total",
                value=float(i + 1),
                event_id=uuid.UUID(f"10000000-0000-0000-0000-{i:012d}"),
            )
        )

    extractor = DeterministicFeatureExtractor(config=cfg)
    result = extractor.extract_from_observations(obs)

    # 1. Prophet series export
    prophet_records = result.to_prophet_series(
        feature_name="http_request_duration_ms:mean"
    )
    assert len(prophet_records) == 5
    for idx, row in enumerate(prophet_records):
        assert "ds" in row
        assert "y" in row
        assert row["y"] == 100.0 + idx * 10
        assert row["window_index"] == idx
        assert row["service"] == "order-service"
        assert len(row["source_event_ids"]) > 0

    # 2. Isolation Forest matrix export
    if_matrix, if_windows = result.to_isolation_forest_matrix()
    assert len(if_matrix) == 5
    assert len(if_windows) == 5
    assert len(if_matrix[0]) == 2
    for idx, mat_row in enumerate(if_matrix):
        assert mat_row[0] == 100.0 + idx * 10  # http_request_duration_ms:mean
        assert mat_row[1] == float(idx + 1)  # http_requests_total:sum

    # 3. Autoencoder matrix export (identical 2D tabular representation)
    ae_matrix, ae_windows = result.to_autoencoder_matrix()
    assert ae_matrix == if_matrix
    assert len(ae_windows) == 5

    # 4. Autoencoder sequence tensor export (sequence length = 3)
    seqs, seq_wins = result.to_autoencoder_sequences(sequence_length=3)
    assert len(seqs) == 3  # 5 windows with seq_len 3 -> 3 sequences (0..2, 1..3, 2..4)
    assert len(seq_wins) == 3
    assert len(seqs[0]) == 3  # seq_len 3
    assert len(seqs[0][0]) == 2  # 2 features


def test_conversion_from_scenario_run_result() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    ev1 = TelemetryEvent(
        event_id=uuid.uuid4(),
        event_time=t0,
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": "http_request_duration_ms", "value": 150.0},
    )
    ev2 = TelemetryEvent(
        event_id=uuid.uuid4(),
        event_time=t0 + timedelta(seconds=1),
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": "http_requests_total", "value": 1},
    )

    scenario = get_scenario("traffic-surge")
    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.UUID(int=10),
        scenario=scenario,
        run_start_time=t0,
        seed=42,
    )
    builder = GroundTruthBuilder()
    scenario_truth = builder.build_run_truth(
        scenario=scenario,
        run_context=run_ctx,
        injection_result=None,
    )

    run_result = ScenarioRunResult(
        run_id=uuid.UUID(int=10),
        scenario_id="traffic-surge",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key=run_ctx.reproducibility_key,
        scenario_truth=scenario_truth,
        telemetry_events=(ev1, ev2),
        request_count=2,
        transitions=(),
    )

    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
    )
    feat_res = extract_features(run_result, config=cfg, target_service="order-service")

    assert feat_res.total_windows >= 1
    assert feat_res.run_id == uuid.UUID(int=10)
    assert feat_res.scenario_id == "traffic-surge"
    assert feat_res.seed == 42
    assert feat_res.reproducibility_key == run_ctx.reproducibility_key
    assert feat_res.windows[0].values["http_request_duration_ms:mean"] == 150.0


def test_conversion_from_metric_samples() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    sample1 = MetricSample(
        metric="http_request_duration_ms",
        event_id=uuid.uuid4(),
        run_id=uuid.UUID(int=5),
        service="order-service",
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        seed=42,
        timestamp=t0,
        value=110.0,
        scenario_id="latency-spike",
    )
    sample2 = MetricSample(
        metric="http_request_duration_ms",
        event_id=uuid.uuid4(),
        run_id=uuid.UUID(int=5),
        service="order-service",
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        seed=42,
        timestamp=t0 + timedelta(seconds=2),
        value=130.0,
        scenario_id="latency-spike",
    )

    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:mean"]
    )
    res = extract_features([sample1, sample2], config=cfg)

    assert res.total_windows == 1
    assert res.windows[0].values["http_request_duration_ms:mean"] == 120.0
    assert len(res.windows[0].source_event_ids) == 2


def test_conversion_from_telemetry_events() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    ev = TelemetryEvent(
        event_id=uuid.uuid4(),
        event_time=t0,
        tenant_id=uuid.UUID(int=1),
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": "http_request_duration_ms", "value": 75.0},
    )
    ctx = TelemetryExecutionContext(
        run_id=uuid.UUID(int=7),
        scenario_id="latency-spike",
        scenario_version="1.0.0",
        reproducibility_key="repro-ctx",
        seed=99,
    )

    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:mean"]
    )
    res = extract_features([ev], config=cfg, context=ctx)

    assert res.run_id == uuid.UUID(int=7)
    assert res.seed == 99
    assert res.windows[0].values["http_request_duration_ms:mean"] == 75.0


def test_window_timestamp_alignment_options() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:mean"]
    )
    obs = [_sample_raw_observation(event_time=t0, value=100.0)]

    extractor_end = DeterministicFeatureExtractor(
        config=cfg, window_timestamp_alignment="end"
    )
    extractor_start = DeterministicFeatureExtractor(
        config=cfg, window_timestamp_alignment="start"
    )
    extractor_center = DeterministicFeatureExtractor(
        config=cfg, window_timestamp_alignment="center"
    )

    res_end = extractor_end.extract_from_observations(obs)
    res_start = extractor_start.extract_from_observations(obs)
    res_center = extractor_center.extract_from_observations(obs)

    assert res_end.windows[0].observation_timestamp == t0 + timedelta(seconds=10)
    assert res_start.windows[0].observation_timestamp == t0
    assert res_center.windows[0].observation_timestamp == t0 + timedelta(seconds=5)


def test_service_filtering() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:mean"]
    )

    obs_order = _sample_raw_observation(
        event_time=t0, service="order-service", value=100.0, event_id=uuid.UUID(int=1)
    )
    obs_payment = _sample_raw_observation(
        event_time=t0, service="payment-service", value=500.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(
        config=cfg, target_service="order-service"
    )
    res = extractor.extract_from_observations([obs_order, obs_payment])

    assert res.service == "order-service"
    assert len(res.source_event_ids) == 1
    assert res.source_event_ids[0] == obs_order.event_id
    assert res.windows[0].values["http_request_duration_ms:mean"] == 100.0


def test_imputation_none_behavior() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="none",
    )

    obs0 = _sample_raw_observation(
        event_time=t0, value=150.0, event_id=uuid.UUID(int=1)
    )
    obs2 = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=200.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs0, obs2])

    assert len(res.windows) == 3
    # Window 0: 150.0, COMPLETE, no missing features
    assert res.windows[0].values["http_request_duration_ms:mean"] == 150.0
    assert res.windows[0].missing_features == []
    assert res.windows[0].is_complete is True
    assert res.windows[0].status == FeatureWindowStatus.COMPLETE

    # Window 1 is empty with imputation="none": feature is NOT in values, missing_features is populated, status EMPTY
    assert "http_request_duration_ms:mean" not in res.windows[1].values
    assert res.windows[1].missing_features == ["http_request_duration_ms:mean"]
    assert res.windows[1].is_complete is False
    assert res.windows[1].is_imputed is False
    assert res.windows[1].status == FeatureWindowStatus.EMPTY

    # Window 2: 200.0, COMPLETE
    assert res.windows[2].values["http_request_duration_ms:mean"] == 200.0
    assert res.windows[2].missing_features == []
    assert res.windows[2].is_complete is True
    assert res.windows[2].status == FeatureWindowStatus.COMPLETE


def test_measured_zero_differs_from_missing_value() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="none",
    )

    # Window 0 has a genuine measured value of 0.0 (e.g. 0 latency or 0 errors)
    obs_zero = _sample_raw_observation(
        event_time=t0, value=0.0, event_id=uuid.UUID(int=1)
    )
    # Window 2 has a measured value of 50.0; Window 1 has NO observations (missing)
    obs_val = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=50.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs_zero, obs_val])

    # Window 0: Measured zero
    w0 = res.windows[0]
    assert "http_request_duration_ms:mean" in w0.values
    assert w0.values["http_request_duration_ms:mean"] == 0.0
    assert w0.missing_features == []
    assert w0.raw_sample_count.get("http_request_duration_ms", 0) == 1
    assert w0.is_complete is True
    assert w0.is_imputed is False
    assert w0.status == FeatureWindowStatus.COMPLETE

    # Window 1: Missing value under strategy none
    w1 = res.windows[1]
    assert "http_request_duration_ms:mean" not in w1.values
    assert w1.missing_features == ["http_request_duration_ms:mean"]
    assert w1.raw_sample_count.get("http_request_duration_ms", 0) == 0
    assert w1.is_complete is False
    assert w1.is_imputed is False
    assert w1.status == FeatureWindowStatus.EMPTY

    # Assert unambiguous difference between measured 0.0 and missing
    assert w0.values != w1.values
    assert w0.missing_features != w1.missing_features
    assert w0.status != w1.status


def test_imputation_none_performs_no_imputation() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean", "http_requests_total:sum"],
        imputation="none",
    )

    # Window 0: only duration is observed with value 0.0; requests is missing
    obs = _sample_raw_observation(
        event_time=t0, metric_name="http_request_duration_ms", value=0.0
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs])

    w = res.windows[0]
    # duration is present with 0.0
    assert w.values.get("http_request_duration_ms:mean") == 0.0
    # requests is NOT fabricated as 0.0
    assert "http_requests_total:sum" not in w.values
    assert w.missing_features == ["http_requests_total:sum"]
    assert w.is_imputed is False
    assert w.imputed_features == []
    assert w.status == FeatureWindowStatus.INSUFFICIENT_DATA


def test_model_export_rejects_unresolved_missingness_by_default() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean", "http_requests_total:sum"],
        imputation="none",
    )

    # Window 0 has duration only; Window 1 has no observations
    obs = _sample_raw_observation(
        event_time=t0, metric_name="http_request_duration_ms", value=100.0
    )
    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs])

    # 1. Isolation Forest matrix export rejects by default
    with pytest.raises(
        InsufficientDataError, match="Cannot export numeric feature matrix"
    ):
        res.to_isolation_forest_matrix()

    # 2. Autoencoder matrix export rejects by default
    with pytest.raises(
        InsufficientDataError, match="Cannot export numeric feature matrix"
    ):
        res.to_autoencoder_matrix()

    # 3. Autoencoder sequence export rejects by default
    with pytest.raises(
        InsufficientDataError, match="Cannot export numeric feature matrix"
    ):
        res.to_autoencoder_sequences(sequence_length=1)

    # 4. Prophet series export rejects by default when target feature or regressor is missing
    with pytest.raises(InsufficientDataError, match="Cannot export Prophet series"):
        res.to_prophet_series(feature_name="http_requests_total:sum")


def test_model_export_allows_incomplete_when_explicitly_requested() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="none",
    )

    # Window 0 has data at t0, Window 1 is empty, Window 2 has data at t0+20
    obs0 = _sample_raw_observation(
        event_time=t0, value=150.0, event_id=uuid.UUID(int=1)
    )
    obs2 = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=200.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs0, obs2])

    # 1. Isolation Forest matrix with allow_incomplete=True excludes the empty window
    matrix, complete_windows = res.to_isolation_forest_matrix(allow_incomplete=True)
    assert len(matrix) == 2  # Window 0 and Window 2 only
    assert len(complete_windows) == 2
    assert matrix[0] == [150.0]
    assert matrix[1] == [200.0]
    assert complete_windows[0].window_index == 0
    assert complete_windows[1].window_index == 2

    # 2. Prophet series with allow_incomplete=True exports explicit None for missing target
    prophet_rows = res.to_prophet_series(
        feature_name="http_request_duration_ms:mean", allow_incomplete=True
    )
    assert len(prophet_rows) == 3
    assert prophet_rows[0]["y"] == 150.0
    assert prophet_rows[0]["is_missing"] is False
    assert prophet_rows[1]["y"] is None
    assert prophet_rows[1]["is_missing"] is True
    assert prophet_rows[2]["y"] == 200.0
    assert prophet_rows[2]["is_missing"] is False


def test_zero_fill_explicit_imputation_preserves_contract() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="zero_fill",
    )

    obs0 = _sample_raw_observation(
        event_time=t0, value=150.0, event_id=uuid.UUID(int=1)
    )
    obs2 = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=200.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs0, obs2])

    w1 = res.windows[1]
    assert w1.values["http_request_duration_ms:mean"] == 0.0
    assert w1.is_imputed is True
    assert w1.imputed_features == ["http_request_duration_ms:mean"]
    assert w1.missing_features == []
    assert w1.status == FeatureWindowStatus.IMPUTED
    assert w1.is_complete is True

    # Exports cleanly without raising InsufficientDataError
    matrix, wins = res.to_isolation_forest_matrix()
    assert len(matrix) == 3
    assert matrix[1] == [0.0]


def test_forward_fill_explicit_imputation_preserves_contract() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0,
        step_size=10.0,
        features=["http_request_duration_ms:mean"],
        imputation="forward_fill",
    )

    obs0 = _sample_raw_observation(
        event_time=t0, value=150.0, event_id=uuid.UUID(int=1)
    )
    obs2 = _sample_raw_observation(
        event_time=t0 + timedelta(seconds=20), value=200.0, event_id=uuid.UUID(int=2)
    )

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs0, obs2])

    w1 = res.windows[1]
    assert w1.values["http_request_duration_ms:mean"] == 150.0
    assert w1.is_imputed is True
    assert w1.imputed_features == ["http_request_duration_ms:mean"]
    assert w1.missing_features == []
    assert w1.status == FeatureWindowStatus.IMPUTED
    assert w1.is_complete is True

    matrix, wins = res.to_isolation_forest_matrix()
    assert len(matrix) == 3
    assert matrix[1] == [150.0]


def test_model_export_errors_on_invalid_requests() -> None:
    t0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    cfg = _sample_feature_config(
        window_size=10.0, step_size=10.0, features=["http_request_duration_ms:mean"]
    )
    obs = [_sample_raw_observation(event_time=t0, value=100.0)]

    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations(obs)

    # Prophet with non-existent feature raises
    with pytest.raises(FeatureExtractionError, match="Requested target feature"):
        res.to_prophet_series(feature_name="non_existent_feature")

    # Isolation forest with non-existent feature raises
    with pytest.raises(FeatureExtractionError, match="Requested feature"):
        res.to_isolation_forest_matrix(feature_names=["non_existent_feature"])

    # Autoencoder sequences with invalid sequence length raises
    with pytest.raises(
        FeatureExtractionError, match="sequence_length must be positive"
    ):
        res.to_autoencoder_sequences(sequence_length=0)

    # Autoencoder sequences with sequence length greater than total windows returns empty
    seqs, seq_wins = res.to_autoencoder_sequences(sequence_length=10)
    assert seqs == []
    assert seq_wins == []


def test_separation_of_event_time_from_transport_time() -> None:
    # TelemetryEvent generated with event_time in the past, but ingested at a different time
    historical_event_time = datetime(2026, 1, 15, 8, 30, 0, tzinfo=timezone.utc)
    obs = _sample_raw_observation(event_time=historical_event_time, value=42.0)

    cfg = _sample_feature_config(window_size=10.0, step_size=10.0)
    extractor = DeterministicFeatureExtractor(config=cfg)
    res = extractor.extract_from_observations([obs])

    # Observation window start and end MUST strictly match the historical event_time, not current wall clock
    assert res.windows[0].window_start == historical_event_time
    assert res.windows[0].window_end == historical_event_time + timedelta(seconds=10)
    assert res.windows[0].observation_timestamp == historical_event_time + timedelta(
        seconds=10
    )
