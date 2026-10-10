from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.errors import (
    AnomalyDeserializationError,
    AnomalySerializationError,
)
from app.anomaly.models import (
    MAX_UINT64,
    SUPPORTED_ANOMALY_SCHEMA_VERSION,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.serialization import (
    ANOMALY_EVENT_TYPE,
    ANOMALY_KAFKA_TOPIC,
    construct_anomaly_kafka_key,
    deserialize_anomaly_signal,
    serialize_anomaly_signal,
    to_canonical_telemetry_event,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic


def _sample_calibration() -> CalibrationMetadata:
    return CalibrationMetadata(
        schema_version="1.0",
        method="quantile_calibration",
        threshold_value=0.85,
        calibration_version="v1.0.0",
        parameters={"alpha": 0.05, "contamination": 0.01},
        calibrated_at=datetime(2026, 9, 30, 10, 0, 0, tzinfo=timezone.utc),
    )


def _sample_signal_kwargs() -> dict:
    return {
        "tenant_id": uuid.uuid4(),
        "environment": "simulation",
        "service": "order-service",
        "metric_or_feature": "http_request_latency_ms",
        "model_name": "isolation_forest",
        "model_version": "1.0.0",
        "anomaly_score": 0.92,
        "severity": EventSeverity.ERROR,
        "threshold_or_calibration": _sample_calibration(),
        "run_id": uuid.uuid4(),
        "scenario_id": "latency-spike",
        "scenario_version": "1.0.0",
        "seed": 42,
        "reproducibility_key": "rep-key-12345",
    }


def test_valid_minimum_signal() -> None:
    kwargs = _sample_signal_kwargs()
    signal = AnomalySignal(**kwargs)

    assert signal.schema_version == SUPPORTED_ANOMALY_SCHEMA_VERSION
    assert isinstance(signal.signal_id, uuid.UUID)
    assert isinstance(signal.event_time, datetime)
    assert signal.event_time.tzinfo is not None
    assert signal.service == "order-service"
    assert signal.metric_or_feature == "http_request_latency_ms"
    assert signal.model_name == "isolation_forest"
    assert signal.model_version == "1.0.0"
    assert signal.anomaly_score == 0.92
    assert signal.severity == EventSeverity.ERROR
    assert signal.evidence == []
    assert signal.source_event_ids == []
    assert signal.source_event_time_window is None
    assert signal.trace_id is None
    assert signal.tags == {}


def test_valid_fully_populated_signal() -> None:
    now = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
    source_id1 = uuid.uuid4()
    source_id2 = uuid.uuid4()
    custom_signal_id = uuid.uuid4()

    evidence1 = AnomalyEvidence(
        evidence_id=uuid.uuid4(),
        evidence_type="metric_residual",
        metric_or_feature="http_request_latency_ms",
        observed_value=450.5,
        expected_value=120.0,
        deviation=330.5,
        details={"z_score": 3.8, "baseline_median": 118.2},
        timestamp=now,
    )
    evidence2 = AnomalyEvidence(
        evidence_type="log_error_burst",
        details={"error_count_in_window": 14, "sample_message": "Connection timeout"},
    )

    window = EventTimeWindow(
        start_time=now - timedelta(seconds=60),
        end_time=now,
    )

    kwargs = _sample_signal_kwargs()
    kwargs.update(
        {
            "signal_id": custom_signal_id,
            "event_time": now,
            "evidence": [evidence1, evidence2],
            "source_event_ids": [source_id1, source_id2],
            "source_event_time_window": window,
            "trace_id": "trace-abc-123",
            "tags": {"cluster": "us-east-1", "layer": "api"},
        }
    )

    signal = AnomalySignal(**kwargs)
    assert signal.signal_id == custom_signal_id
    assert signal.event_time == now
    assert len(signal.evidence) == 2
    assert signal.evidence[0].evidence_type == "metric_residual"
    assert signal.evidence[0].observed_value == 450.5
    assert signal.evidence[1].evidence_type == "log_error_burst"
    assert signal.source_event_ids == [source_id1, source_id2]
    assert signal.source_event_time_window == window
    assert signal.trace_id == "trace-abc-123"
    assert signal.tags == {"cluster": "us-east-1", "layer": "api"}


@pytest.mark.parametrize(
    "missing_field",
    [
        "service",
        "metric_or_feature",
        "model_name",
        "model_version",
        "anomaly_score",
        "severity",
        "threshold_or_calibration",
        "run_id",
        "scenario_id",
        "scenario_version",
        "seed",
        "reproducibility_key",
        "environment",
        "tenant_id",
    ],
)
def test_missing_required_fields_rejection(missing_field: str) -> None:
    kwargs = _sample_signal_kwargs()
    del kwargs[missing_field]
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


@pytest.mark.parametrize("invalid_version", ["2.0", "0.9", "v1", "", " "])
def test_invalid_schema_version_rejection(invalid_version: str) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["schema_version"] = invalid_version
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


@pytest.mark.parametrize(
    "string_field",
    [
        "service",
        "metric_or_feature",
        "model_name",
        "model_version",
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "environment",
    ],
)
@pytest.mark.parametrize("empty_val", ["", "   ", "\t\n"])
def test_empty_or_blank_identifier_rejection(string_field: str, empty_val: str) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs[string_field] = empty_val
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


@pytest.mark.parametrize("valid_score", [0.0, 1.0, 0.5, 0.0001, 0.9999])
def test_score_boundary_valid_values(valid_score: float) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["anomaly_score"] = valid_score
    signal = AnomalySignal(**kwargs)
    assert signal.anomaly_score == valid_score


@pytest.mark.parametrize("invalid_score", [-0.01, -1.0, 1.001, 2.0, 100.0])
def test_score_out_of_bounds_rejection(invalid_score: float) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["anomaly_score"] = invalid_score
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


@pytest.mark.parametrize(
    "non_finite_score", [float("nan"), float("inf"), float("-inf")]
)
def test_score_non_finite_rejection(non_finite_score: float) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["anomaly_score"] = non_finite_score
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


@pytest.mark.parametrize(
    "severity",
    [
        EventSeverity.DEBUG,
        EventSeverity.INFO,
        EventSeverity.WARNING,
        EventSeverity.ERROR,
        EventSeverity.CRITICAL,
    ],
)
def test_valid_severity_values(severity: EventSeverity) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["severity"] = severity
    signal = AnomalySignal(**kwargs)
    assert signal.severity == severity


def test_invalid_severity_rejection() -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["severity"] = "FATAL_UNKNOWN"
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


def test_calibration_metadata_validation() -> None:
    cal = _sample_calibration()
    assert cal.schema_version == "1.0"
    assert cal.threshold_value == 0.85
    assert cal.method == "quantile_calibration"

    # Non-finite threshold value rejection
    with pytest.raises(ValidationError):
        CalibrationMetadata(
            method="test",
            threshold_value=float("nan"),
            calibration_version="v1",
        )
    with pytest.raises(ValidationError):
        CalibrationMetadata(
            method="test",
            threshold_value=float("inf"),
            calibration_version="v1",
        )

    # Empty method or version rejection
    with pytest.raises(ValidationError):
        CalibrationMetadata(
            method="  ",
            threshold_value=0.5,
            calibration_version="v1",
        )
    with pytest.raises(ValidationError):
        CalibrationMetadata(
            method="test",
            threshold_value=0.5,
            calibration_version="",
        )

    # Invalid schema version rejection
    with pytest.raises(ValidationError):
        CalibrationMetadata(
            schema_version="2.0",
            method="test",
            threshold_value=0.5,
            calibration_version="v1",
        )


def test_evidence_structure_and_numeric_validation() -> None:
    ev = AnomalyEvidence(
        evidence_type="residual_analysis",
        observed_value=10.0,
        expected_value=2.0,
        deviation=8.0,
    )
    assert ev.evidence_type == "residual_analysis"
    assert ev.observed_value == 10.0

    # Empty evidence_type rejection
    with pytest.raises(ValidationError):
        AnomalyEvidence(evidence_type="  ")

    # Non-finite numeric rejection
    with pytest.raises(ValidationError):
        AnomalyEvidence(evidence_type="test", observed_value=float("nan"))
    with pytest.raises(ValidationError):
        AnomalyEvidence(evidence_type="test", expected_value=float("inf"))
    with pytest.raises(ValidationError):
        AnomalyEvidence(evidence_type="test", deviation=float("-inf"))


def test_time_window_validation() -> None:
    t0 = datetime(2026, 9, 30, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 9, 30, 10, 5, 0, tzinfo=timezone.utc)

    window = EventTimeWindow(start_time=t0, end_time=t1)
    assert window.start_time == t0
    assert window.end_time == t1

    # Equal start and end time is valid (instantaneous window)
    window_instant = EventTimeWindow(start_time=t0, end_time=t0)
    assert window_instant.start_time == window_instant.end_time

    # Reversed time window rejection
    with pytest.raises(
        ValidationError, match="start_time .* cannot be greater than end_time"
    ):
        EventTimeWindow(start_time=t1, end_time=t0)

    # Naive datetime rejection
    naive_dt = datetime(2026, 9, 30, 10, 0, 0)
    with pytest.raises(ValidationError):
        EventTimeWindow(start_time=naive_dt, end_time=t1)


@pytest.mark.parametrize("valid_seed", [0, 42, 1000, MAX_UINT64])
def test_seed_bounds_valid(valid_seed: int) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["seed"] = valid_seed
    signal = AnomalySignal(**kwargs)
    assert signal.seed == valid_seed


@pytest.mark.parametrize("invalid_seed", [-1, -100, MAX_UINT64 + 1])
def test_seed_bounds_invalid(invalid_seed: int) -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["seed"] = invalid_seed
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


def test_naive_event_time_rejection() -> None:
    kwargs = _sample_signal_kwargs()
    kwargs["event_time"] = datetime(2026, 9, 30, 12, 0, 0)  # naive
    with pytest.raises(ValidationError):
        AnomalySignal(**kwargs)


def test_signal_immutability() -> None:
    signal = AnomalySignal(**_sample_signal_kwargs())
    with pytest.raises(ValidationError):
        signal.anomaly_score = 0.5  # type: ignore[misc]


def test_json_serialization_round_trip() -> None:
    now = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
    kwargs = _sample_signal_kwargs()
    kwargs.update(
        {
            "event_time": now,
            "evidence": [
                AnomalyEvidence(
                    evidence_type="residual_spike",
                    observed_value=250.0,
                    expected_value=50.0,
                    deviation=200.0,
                    details={"model": "autoencoder", "loss": 0.45},
                )
            ],
            "source_event_ids": [uuid.uuid4(), uuid.uuid4()],
            "source_event_time_window": EventTimeWindow(
                start_time=now - timedelta(seconds=120),
                end_time=now,
            ),
            "trace_id": "trace-rt-test",
            "tags": {"env": "sim", "region": "local"},
        }
    )
    original = AnomalySignal(**kwargs)

    raw_bytes = serialize_anomaly_signal(original)
    assert isinstance(raw_bytes, bytes)
    assert len(raw_bytes) > 0

    restored = deserialize_anomaly_signal(raw_bytes)
    assert restored == original
    assert restored.signal_id == original.signal_id
    assert restored.anomaly_score == original.anomaly_score
    assert restored.threshold_or_calibration == original.threshold_or_calibration
    assert len(restored.evidence) == 1
    assert restored.evidence[0].deviation == 200.0
    assert len(restored.source_event_ids) == 2
    assert restored.source_event_time_window == original.source_event_time_window
    assert restored.tags == original.tags


def test_deserialization_unsupported_version_rejected() -> None:
    kwargs = _sample_signal_kwargs()
    signal = AnomalySignal(**kwargs)
    data = signal.model_dump(mode="json")
    data["schema_version"] = "2.0"

    import json

    payload_bytes = json.dumps(data).encode("utf-8")
    with pytest.raises(AnomalyDeserializationError, match="Unsupported schema_version"):
        deserialize_anomaly_signal(payload_bytes)


@pytest.mark.parametrize(
    "invalid_raw", [b"NOT_JSON_BYTES", b"123", b'"string_root"', b"[1, 2, 3]"]
)
def test_deserialization_malformed_json_rejected(invalid_raw: bytes) -> None:
    with pytest.raises(AnomalyDeserializationError):
        deserialize_anomaly_signal(invalid_raw)


def test_deterministic_serialization_output() -> None:
    now = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
    sig_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
    tenant_id = uuid.UUID("22222222-2222-2222-2222-222222222222")
    run_id = uuid.UUID("33333333-3333-3333-3333-333333333333")

    cal = CalibrationMetadata(
        schema_version="1.0",
        method="static",
        threshold_value=0.8,
        calibration_version="v1.0",
        parameters={"p1": "v1"},
        calibrated_at=now,
    )

    signal1 = AnomalySignal(
        signal_id=sig_id,
        event_time=now,
        tenant_id=tenant_id,
        environment="simulation",
        service="payment-service",
        metric_or_feature="cpu_usage",
        model_name="prophet",
        model_version="1.0.0",
        anomaly_score=0.85,
        severity=EventSeverity.WARNING,
        threshold_or_calibration=cal,
        run_id=run_id,
        scenario_id="cpu-surge",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-key-det",
    )
    signal2 = AnomalySignal(
        signal_id=sig_id,
        event_time=now,
        tenant_id=tenant_id,
        environment="simulation",
        service="payment-service",
        metric_or_feature="cpu_usage",
        model_name="prophet",
        model_version="1.0.0",
        anomaly_score=0.85,
        severity=EventSeverity.WARNING,
        threshold_or_calibration=cal,
        run_id=run_id,
        scenario_id="cpu-surge",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-key-det",
    )

    bytes1 = serialize_anomaly_signal(signal1)
    bytes2 = serialize_anomaly_signal(signal2)
    assert bytes1 == bytes2


def test_kafka_key_and_topic_compatibility() -> None:
    signal = AnomalySignal(**_sample_signal_kwargs())
    key = construct_anomaly_kafka_key(signal)
    expected_key = f"{signal.tenant_id}:{signal.environment}:{signal.service}".encode(
        "utf-8"
    )
    assert key == expected_key

    assert ANOMALY_KAFKA_TOPIC == KafkaTopic.ANOMALIES
    assert ANOMALY_KAFKA_TOPIC.value == "aegis.ml.anomalies"
    assert ANOMALY_EVENT_TYPE == EventType.ANOMALY
    assert ANOMALY_EVENT_TYPE.value == "anomaly"
    assert EVENT_TYPE_TO_TOPIC[EventType.ANOMALY] == KafkaTopic.ANOMALIES


def test_to_canonical_telemetry_event_adapter() -> None:
    signal = AnomalySignal(**_sample_signal_kwargs())
    event = to_canonical_telemetry_event(signal)

    assert isinstance(event, TelemetryEvent)
    assert event.event_type == EventType.ANOMALY
    assert event.event_id == signal.signal_id
    assert event.event_time == signal.event_time
    assert event.tenant_id == signal.tenant_id
    assert event.environment == signal.environment
    assert event.service == signal.service
    assert event.severity == signal.severity
    assert event.payload["model_name"] == signal.model_name
    assert event.payload["anomaly_score"] == signal.anomaly_score
    assert event.payload["threshold_or_calibration"]["threshold_value"] == 0.85


def test_serialization_failure_wraps_in_anomaly_serialization_error() -> None:
    signal = AnomalySignal(**_sample_signal_kwargs())
    mock_signal = MagicMock(spec=AnomalySignal)
    mock_signal.model_dump_json.side_effect = RuntimeError("Mock serialization failure")
    mock_signal.signal_id = signal.signal_id

    with pytest.raises(
        AnomalySerializationError, match="Failed to serialize AnomalySignal"
    ):
        serialize_anomaly_signal(mock_signal)  # type: ignore[arg-type]
