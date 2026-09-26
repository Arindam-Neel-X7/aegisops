from app.telemetry.reliability.metrics import (
    TelemetryPipelineMetrics,
    calculate_percentile,
)


def test_calculate_percentile_deterministic() -> None:
    data = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    p50 = calculate_percentile(data, 50.0)
    p99 = calculate_percentile(data, 99.0)

    assert p50 == 55.0
    assert p99 == 99.1

    # Empty data
    assert calculate_percentile([], 50.0) == 0.0

    # Single element
    assert calculate_percentile([42.0], 50.0) == 42.0


def test_telemetry_pipeline_metrics_recording_and_snapshot() -> None:
    metrics = TelemetryPipelineMetrics(sample_window=100)
    metrics.reset()

    # Record publishes
    for i in range(1, 11):
        metrics.record_publish(latency_ms=float(i * 10))

    # Record persistences
    for i in range(1, 11):
        metrics.record_persistence(latency_ms=float(i * 5), retries=1 if i % 3 == 0 else 0, e2e_latency_ms=float(i * 15))

    # Record quarantines & failures
    metrics.record_quarantine(stage="deserialization")
    metrics.record_quarantine(stage="persistence")
    metrics.record_failure(stage="transport")
    metrics.record_consumer_lag(lag=25)

    snap = metrics.snapshot()

    assert snap.published_count == 10
    assert snap.persisted_count == 10
    assert snap.quarantined_count == 2
    assert snap.failed_count == 3  # 2 quarantined + 1 failure
    assert snap.retry_count == 3   # 3 retries
    assert snap.consumer_lag == 25
    assert snap.throughput_per_second > 0.0

    # Latencies
    assert snap.publish_latency_p50_ms == 55.0
    assert snap.persistence_latency_p50_ms == 27.5
    assert snap.e2e_latency_p50_ms == 82.5

    # Reset
    metrics.reset()
    reset_snap = metrics.snapshot()
    assert reset_snap.published_count == 0
    assert reset_snap.persisted_count == 0
    assert reset_snap.consumer_lag == 0
    assert reset_snap.publish_latency_p50_ms == 0.0
