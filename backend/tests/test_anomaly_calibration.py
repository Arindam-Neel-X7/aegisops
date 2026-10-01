from datetime import datetime, timedelta, timezone
import json
from typing import Any
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.autoencoder import AutoencoderScoreResult, AutoencoderScoreStatus
from app.anomaly.calibration import (
    SUPPORTED_CALIBRATION_CONFIG_SCHEMA_VERSION,
    SUPPORTED_CALIBRATION_METHOD_VERSION,
    SUPPORTED_CALIBRATION_SPLIT_VERSION,
    SUPPORTED_SEVERITY_MAPPING_VERSION,
    CalibratedScoreStatus,
    CalibrationConfig,
    CalibrationInput,
    CalibrationReferenceData,
    CalibrationReferenceSample,
    CommonScoreCalibrator,
    ModelFittedCalibration,
    ModelRawComparisonRecord,
    SeverityThresholdsConfig,
    calibration_input_from_autoencoder,
    calibration_input_from_isolation_forest,
    calibration_input_from_prophet,
    calibration_input_from_signal,
    create_default_calibration_config,
    create_raw_comparison_record,
)
from app.anomaly.errors import CalibrationError, CalibrationReferenceError
from app.anomaly.isolation_forest import IsolationForestScoreResult, IsolationForestScoreStatus
from app.anomaly.models import (
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.prophet import ProphetScoreResult, ProphetScoreStatus
from app.telemetry.schemas import EventSeverity


def _sample_calibration_config(**kwargs: Any) -> CalibrationConfig:
    defaults: dict[str, Any] = {
        "schema_version": "1.0",
        "calibration_method_version": "1.0.0",
        "calibration_split_version": "1.0.0",
        "severity_mapping_version": "1.0.0",
    }
    defaults.update(kwargs)
    return CalibrationConfig.model_validate(defaults)


def _sample_signal(
    model_name: str = "prophet",
    anomaly_score: float = 0.65,
    event_time: datetime | None = None,
    signal_id: uuid.UUID | None = None,
    residual: float = 50.0,
) -> AnomalySignal:
    t = event_time or datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    ev = AnomalyEvidence(
        evidence_id=uuid.uuid4(),
        evidence_type=f"{model_name}_evidence",
        metric_or_feature="http_request_duration_ms:mean",
        observed_value=150.0,
        expected_value=100.0,
        deviation=residual,
        details={
            "absolute_deviation": abs(residual),
            "raw_score_samples": -0.58 if model_name == "isolation_forest" else None,
            "raw_reconstruction_error": 0.045 if model_name == "autoencoder" else None,
        },
        timestamp=t,
    )
    cal = CalibrationMetadata(
        schema_version="1.0",
        method="baseline_normalization",
        threshold_value=0.75,
        calibration_version="1.0.0",
        parameters={},
        calibrated_at=t,
    )
    return AnomalySignal(
        signal_id=signal_id or uuid.uuid4(),
        event_time=t,
        service="order-service",
        metric_or_feature="http_request_duration_ms:mean",
        model_name=model_name,
        model_version="1.0.0",
        anomaly_score=anomaly_score,
        severity=EventSeverity.WARNING,
        evidence=[ev],
        threshold_or_calibration=cal,
        schema_version="1.0",
        run_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        scenario_id="dependency-latency",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="repro-test",
        source_event_ids=[uuid.uuid4()],
        source_event_time_window=EventTimeWindow(start_time=t, end_time=t),
        environment="simulation",
        tenant_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
    )


def _sample_reference_dataset(
    model_name: str,
    scores: list[float] | None = None,
    reference_id: str = "calib-ref-v1",
    partition_role: str = "calibration",
    sample_ids: list[uuid.UUID] | None = None,
    calibration_cutoff_time: datetime | None = None,
) -> CalibrationReferenceData:
    default_scores = [0.05 * i for i in range(21)]  # 0.0 to 1.0 in steps of 0.05
    ref_scores = scores if scores is not None else default_scores
    t0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)

    samples = []
    for i, s in enumerate(ref_scores):
        sid = (
            sample_ids[i]
            if sample_ids and i < len(sample_ids)
            else uuid.UUID(f"00000000-0000-0000-0000-{i:012d}")
        )
        samples.append(
            CalibrationReferenceSample(
                sample_id=sid,
                model_name=model_name,
                model_version="1.0.0",
                baseline_normalized_score=s,
                event_time=t0 + timedelta(minutes=i),
                run_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                scenario_id="dependency-latency",
                scenario_version="1.0.0",
                seed=42,
                reproducibility_key="repro-test",
                partition_role=partition_role,  # type: ignore[arg-type]
            )
        )

    return CalibrationReferenceData(
        reference_id=reference_id,
        reference_version="1.0.0",
        model_name=model_name,
        model_version="1.0.0",
        samples=samples,
        calibration_cutoff_time=calibration_cutoff_time,
        allowed_partition_role="calibration",
        created_at=datetime.now(timezone.utc),
    )


def test_severity_thresholds_configuration_and_ordering() -> None:
    t = SeverityThresholdsConfig(
        info_threshold=0.20,
        warning_threshold=0.50,
        error_threshold=0.75,
        critical_threshold=0.90,
    )
    assert t.info_threshold == 0.20
    assert t.critical_threshold == 0.90

    with pytest.raises(ValidationError, match="must be strictly ordered with non-empty DEBUG interval"):
        SeverityThresholdsConfig(
            info_threshold=0.50,
            warning_threshold=0.20,
            error_threshold=0.75,
            critical_threshold=0.90,
        )

    with pytest.raises(ValidationError, match="must be strictly ordered with non-empty DEBUG interval"):
        SeverityThresholdsConfig(
            info_threshold=0.20,
            warning_threshold=0.50,
            error_threshold=0.50,
            critical_threshold=0.90,
        )


def test_severity_thresholds_info_zero_rejected() -> None:
    with pytest.raises(ValidationError):
        SeverityThresholdsConfig(
            info_threshold=0.0,
            warning_threshold=0.50,
            error_threshold=0.75,
            critical_threshold=0.90,
        )


def test_severity_thresholds_boolean_rejected() -> None:
    with pytest.raises(ValidationError, match="cannot be a boolean"):
        SeverityThresholdsConfig(
            info_threshold=True,  # type: ignore[arg-type]
            warning_threshold=0.50,
            error_threshold=0.75,
            critical_threshold=0.90,
        )


def test_calibration_config_supported_versions_accepted() -> None:
    cfg = create_default_calibration_config()
    assert cfg.schema_version == SUPPORTED_CALIBRATION_CONFIG_SCHEMA_VERSION
    assert cfg.calibration_method_version == SUPPORTED_CALIBRATION_METHOD_VERSION
    assert cfg.calibration_split_version == SUPPORTED_CALIBRATION_SPLIT_VERSION
    assert cfg.severity_mapping_version == SUPPORTED_SEVERITY_MAPPING_VERSION
    assert cfg.calibration_method == "empirical_cdf_percentile"
    assert len(cfg.supported_models) == 3


@pytest.mark.parametrize(
    "field_name",
    [
        "schema_version",
        "calibration_method_version",
        "calibration_split_version",
        "severity_mapping_version",
    ],
)
def test_calibration_config_required_version_omission_rejected(field_name: str) -> None:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibration_method_version": "1.0.0",
        "calibration_split_version": "1.0.0",
        "severity_mapping_version": "1.0.0",
    }
    payload.pop(field_name)
    with pytest.raises(ValidationError):
        CalibrationConfig.model_validate(payload)


@pytest.mark.parametrize(
    "field_name",
    [
        "schema_version",
        "calibration_method_version",
        "calibration_split_version",
        "severity_mapping_version",
    ],
)
def test_calibration_config_required_version_none_rejected(field_name: str) -> None:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibration_method_version": "1.0.0",
        "calibration_split_version": "1.0.0",
        "severity_mapping_version": "1.0.0",
    }
    payload[field_name] = None
    with pytest.raises(ValidationError):
        CalibrationConfig.model_validate(payload)


@pytest.mark.parametrize(
    "field_name",
    [
        "schema_version",
        "calibration_method_version",
        "calibration_split_version",
        "severity_mapping_version",
    ],
)
def test_calibration_config_required_version_empty_rejected(field_name: str) -> None:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibration_method_version": "1.0.0",
        "calibration_split_version": "1.0.0",
        "severity_mapping_version": "1.0.0",
    }
    payload[field_name] = ""
    with pytest.raises(ValidationError):
        CalibrationConfig.model_validate(payload)


@pytest.mark.parametrize(
    "field_name",
    [
        "schema_version",
        "calibration_method_version",
        "calibration_split_version",
        "severity_mapping_version",
    ],
)
def test_calibration_config_required_version_whitespace_rejected(field_name: str) -> None:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibration_method_version": "1.0.0",
        "calibration_split_version": "1.0.0",
        "severity_mapping_version": "1.0.0",
    }
    payload[field_name] = "   "
    with pytest.raises(ValidationError):
        CalibrationConfig.model_validate(payload)


@pytest.mark.parametrize(
    ("field_name", "unsupported_val"),
    [
        ("schema_version", "2.0"),
        ("calibration_method_version", "2.0.0"),
        ("calibration_split_version", "2.0.0"),
        ("severity_mapping_version", "2.0.0"),
    ],
)
def test_calibration_config_unsupported_versions_rejected(
    field_name: str, unsupported_val: str
) -> None:
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "calibration_method_version": "1.0.0",
        "calibration_split_version": "1.0.0",
        "severity_mapping_version": "1.0.0",
    }
    payload[field_name] = unsupported_val
    with pytest.raises(ValidationError):
        CalibrationConfig.model_validate(payload)


def test_calibration_config_serialization_round_trip() -> None:
    cfg = CalibrationConfig(
        schema_version="1.0",
        calibration_method="empirical_cdf_percentile",
        calibration_method_version="1.0.0",
        supported_models=["prophet", "isolation_forest", "autoencoder"],
        min_calibration_samples_per_model=10,
        calibration_reference_id="ref-json-v1",
        calibration_reference_version="1.0.0",
        calibration_split_rule="independent_reference_set",
        calibration_split_version="1.0.0",
        severity_mapping_version="1.0.0",
        severity_thresholds=SeverityThresholdsConfig(),
    )

    json_str = cfg.model_dump_json(indent=2)
    loaded_cfg = CalibrationConfig.model_validate_json(json_str)

    assert loaded_cfg == cfg
    assert loaded_cfg.schema_version == "1.0"
    assert loaded_cfg.calibration_method_version == "1.0.0"
    assert loaded_cfg.calibration_split_version == "1.0.0"
    assert loaded_cfg.severity_mapping_version == "1.0.0"


def test_calibration_config_required_key_deletion_deserialization_failure() -> None:
    cfg = create_default_calibration_config()
    raw_dict = json.loads(cfg.model_dump_json())

    for key in (
        "schema_version",
        "calibration_method_version",
        "calibration_split_version",
        "severity_mapping_version",
    ):
        tampered = dict(raw_dict)
        tampered.pop(key)
        with pytest.raises(ValidationError):
            CalibrationConfig.model_validate(tampered)


def test_prophet_two_sided_residual_and_absolute_deviation_semantics() -> None:
    sig_pos = _sample_signal("prophet", anomaly_score=0.70, residual=80.0)
    sig_neg = _sample_signal("prophet", anomaly_score=0.70, residual=-80.0)

    inp_pos = calibration_input_from_signal(sig_pos)
    inp_neg = calibration_input_from_signal(sig_neg)

    assert inp_pos.raw_score == 80.0
    assert inp_neg.raw_score == 80.0
    assert inp_pos.raw_score_type == "forecast_absolute_deviation"
    assert inp_neg.raw_score_type == "forecast_absolute_deviation"
    assert inp_pos.raw_score_direction == "higher_is_more_anomalous"
    assert inp_neg.raw_score_direction == "higher_is_more_anomalous"

    assert inp_pos.signed_residual == 80.0
    assert inp_neg.signed_residual == -80.0


def test_raw_comparison_record_creation() -> None:
    sig = _sample_signal("prophet", 0.75, residual=65.0)
    inp = calibration_input_from_signal(sig)
    rec = create_raw_comparison_record(inp)

    assert isinstance(rec, ModelRawComparisonRecord)
    assert rec.model_name == "prophet"
    assert rec.raw_score == 65.0
    assert rec.raw_score_type == "forecast_absolute_deviation"
    assert rec.raw_score_direction == "higher_is_more_anomalous"
    assert rec.signed_residual == 65.0
    assert rec.baseline_normalized_score == 0.75
    assert rec.service == "order-service"
    assert rec.run_id == sig.run_id


def test_calibration_reference_sample_validation() -> None:
    ds = _sample_reference_dataset("prophet")
    assert ds.model_name == "prophet"
    assert len(ds.samples) == 21
    assert len(ds.reference_scores) == 21

    with pytest.raises(ValidationError):
        CalibrationReferenceSample(
            sample_id=uuid.uuid4(),
            model_name="prophet",
            baseline_normalized_score=float("nan"),
            event_time=datetime.now(timezone.utc),
            run_id=uuid.uuid4(),
            scenario_id="scen",
            scenario_version="1.0.0",
            seed=42,
            reproducibility_key="rep",
        )

    with pytest.raises(ValidationError):
        CalibrationReferenceSample(
            sample_id=uuid.uuid4(),
            model_name="prophet",
            baseline_normalized_score=1.5,
            event_time=datetime.now(timezone.utc),
            run_id=uuid.uuid4(),
            scenario_id="scen",
            scenario_version="1.0.0",
            seed=42,
            reproducibility_key="rep",
        )


def test_reference_sample_calibration_partition_accepted() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_cal = _sample_reference_dataset("prophet", partition_role="calibration")
    calibrator.fit_reference_data([ref_cal])

    assert ("prophet", "1.0.0") in calibrator._fitted_models
    fitted = calibrator._fitted_models[("prophet", "1.0.0")]
    assert fitted.allowed_partition_role == "calibration"
    assert fitted.sample_count == 21


def test_reference_sample_evaluation_partition_rejected() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_eval = _sample_reference_dataset("prophet", partition_role="evaluation")
    with pytest.raises(CalibrationReferenceError, match="Leakage rejected: reference sample .* has partition_role 'evaluation'"):
        calibrator.fit_reference_data([ref_eval])


def test_reference_sample_test_partition_rejected() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_test = _sample_reference_dataset("prophet", partition_role="test")
    with pytest.raises(CalibrationReferenceError, match="Leakage rejected: reference sample .* has partition_role 'test'"):
        calibrator.fit_reference_data([ref_test])


def test_reference_sample_mixed_calibration_evaluation_rejected() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    t0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
    s1 = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.2,
        event_time=t0,
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
        partition_role="calibration",
    )
    s2 = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.4,
        event_time=t0 + timedelta(minutes=1),
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
        partition_role="evaluation",
    )

    ds_mixed = CalibrationReferenceData(
        reference_id="ref-mixed",
        reference_version="1.0.0",
        model_name="prophet",
        samples=[s1, s2] * 5,
        allowed_partition_role="calibration",
        created_at=t0,
    )

    with pytest.raises(CalibrationReferenceError, match="Leakage rejected: reference sample .* has partition_role 'evaluation'"):
        calibrator.fit_reference_data([ds_mixed])


def test_reference_sample_mixed_calibration_test_rejected() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    t0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
    s1 = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.2,
        event_time=t0,
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
        partition_role="calibration",
    )
    s2 = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.4,
        event_time=t0 + timedelta(minutes=1),
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
        partition_role="test",
    )

    ds_mixed = CalibrationReferenceData(
        reference_id="ref-mixed-test",
        reference_version="1.0.0",
        model_name="prophet",
        samples=[s1, s2] * 5,
        allowed_partition_role="calibration",
        created_at=t0,
    )

    with pytest.raises(CalibrationReferenceError, match="Leakage rejected: reference sample .* has partition_role 'test'"):
        calibrator.fit_reference_data([ds_mixed])


def test_reference_data_evaluation_override_rejected() -> None:
    t0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
    s = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.2,
        event_time=t0,
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
        partition_role="calibration",
    )

    with pytest.raises(ValidationError):
        CalibrationReferenceData(
            reference_id="ref-override-eval",
            reference_version="1.0.0",
            model_name="prophet",
            samples=[s] * 10,
            allowed_partition_role="evaluation",  # type: ignore[arg-type]
            created_at=t0,
        )


def test_reference_data_test_override_rejected() -> None:
    t0 = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
    s = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.2,
        event_time=t0,
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
        partition_role="calibration",
    )

    with pytest.raises(ValidationError):
        CalibrationReferenceData(
            reference_id="ref-override-test",
            reference_version="1.0.0",
            model_name="prophet",
            samples=[s] * 10,
            allowed_partition_role="test",  # type: ignore[arg-type]
            created_at=t0,
        )


def test_fitted_artifact_evaluation_override_rejected() -> None:
    with pytest.raises(ValidationError):
        ModelFittedCalibration(
            model_name="prophet",
            model_version="1.0.0",
            reference_id="ref1",
            reference_version="1.0.0",
            sample_count=10,
            ordered_reference_sample_ids=[uuid.uuid4() for _ in range(10)],
            allowed_partition_role="evaluation",  # type: ignore[arg-type]
            calibration_split_rule="independent_reference_set",
            calibration_split_version="1.0.0",
            sorted_reference_scores=[0.1 * i for i in range(10)],
            quantiles=[0.1 * i for i in range(10)],
            is_degenerate=False,
        )


def test_fitted_artifact_test_override_rejected() -> None:
    with pytest.raises(ValidationError):
        ModelFittedCalibration(
            model_name="prophet",
            model_version="1.0.0",
            reference_id="ref1",
            reference_version="1.0.0",
            sample_count=10,
            ordered_reference_sample_ids=[uuid.uuid4() for _ in range(10)],
            allowed_partition_role="test",  # type: ignore[arg-type]
            calibration_split_rule="independent_reference_set",
            calibration_split_version="1.0.0",
            sorted_reference_scores=[0.1 * i for i in range(10)],
            quantiles=[0.1 * i for i in range(10)],
            is_degenerate=False,
        )


def test_no_fitted_entry_stored_after_rejected_fit() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_eval = _sample_reference_dataset("prophet", partition_role="evaluation")
    with pytest.raises(CalibrationReferenceError):
        calibrator.fit_reference_data([ref_eval])

    assert len(calibrator._fitted_models) == 0


def test_target_identity_valid_and_distinct_succeeds() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    calibrator.fit_reference_data([_sample_reference_dataset("prophet")])

    target_signal = _sample_signal("prophet", 0.65, signal_id=uuid.uuid4())
    inp = calibration_input_from_signal(target_signal)

    res = calibrator.calibrate_input(inp)
    assert res.status == CalibratedScoreStatus.SUCCESS
    assert res.calibrated_score is not None
    assert res.calibrated_signal is not None


def test_missing_target_identity_rejected_with_invalid_input() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    calibrator.fit_reference_data([_sample_reference_dataset("prophet")])

    t = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    tw = EventTimeWindow(start_time=t, end_time=t)

    inp_no_id = CalibrationInput(
        source_model_name="prophet",
        source_model_version="1.0.0",
        source_configuration_version="1.0",
        source_normalization_method="norm",
        source_normalization_version="1.0.0",
        raw_score=10.0,
        raw_score_type="forecast_absolute_deviation",
        raw_score_direction="higher_is_more_anomalous",
        baseline_normalized_score=0.5,
        source_signal_id=None,
        event_time=t,
        service="order-service",
        metric_or_feature="http_request_duration_ms:mean",
        source_event_time_window=tw,
        run_id=uuid.uuid4(),
        scenario_id="scenario",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="repro",
        tenant_id=uuid.uuid4(),
        environment="simulation",
    )

    res = calibrator.calibrate_input(inp_no_id)
    assert res.status == CalibratedScoreStatus.INVALID_INPUT
    assert res.calibrated_score is None
    assert res.severity is None
    assert res.calibrated_signal is None
    assert "Target source_signal_id is required" in str(res.error_message)


def test_calibration_cutoff_before_equal_after_boundary_behavior() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    t_cutoff = datetime(2026, 10, 1, 10, 10, 0, tzinfo=timezone.utc)

    s_before = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.2,
        event_time=t_cutoff - timedelta(seconds=1),
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
    )
    s_equal = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.3,
        event_time=t_cutoff,
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
    )

    ds_valid = CalibrationReferenceData(
        reference_id="ref-cutoff",
        reference_version="1.0.0",
        model_name="prophet",
        samples=[s_before, s_equal] * 5,
        calibration_cutoff_time=t_cutoff,
        allowed_partition_role="calibration",
        created_at=t_cutoff,
    )
    calibrator.fit_reference_data([ds_valid])
    assert ("prophet", "1.0.0") in calibrator._fitted_models

    s_after = CalibrationReferenceSample(
        sample_id=uuid.uuid4(),
        model_name="prophet",
        baseline_normalized_score=0.4,
        event_time=t_cutoff + timedelta(seconds=1),
        run_id=uuid.uuid4(),
        scenario_id="scen",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep",
    )
    ds_invalid = CalibrationReferenceData(
        reference_id="ref-future",
        reference_version="1.0.0",
        model_name="prophet",
        samples=[s_before, s_equal, s_after] * 4,
        calibration_cutoff_time=t_cutoff,
        allowed_partition_role="calibration",
        created_at=t_cutoff,
    )
    with pytest.raises(CalibrationReferenceError, match="Future leakage rejected"):
        calibrator.fit_reference_data([ds_invalid])


def test_model_fitted_calibration_preserves_split_metadata() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    ds = _sample_reference_dataset("prophet")
    calibrator.fit_reference_data([ds])

    fitted = calibrator._fitted_models[("prophet", "1.0.0")]
    assert fitted.calibration_split_rule == "independent_reference_set"
    assert fitted.calibration_split_version == "1.0.0"
    assert fitted.allowed_partition_role == "calibration"
    assert len(fitted.ordered_reference_sample_ids) == 21
    assert fitted.earliest_reference_event_time is not None
    assert fitted.latest_reference_event_time is not None


def test_fitted_calibration_serialization_round_trip() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    ds = _sample_reference_dataset("prophet")
    calibrator.fit_reference_data([ds])

    fitted = calibrator._fitted_models[("prophet", "1.0.0")]
    json_str = fitted.model_dump_json(indent=2)
    loaded_fitted = ModelFittedCalibration.model_validate_json(json_str)

    assert loaded_fitted == fitted
    assert loaded_fitted.model_name == "prophet"
    assert loaded_fitted.model_version == "1.0.0"
    assert loaded_fitted.reference_id == ds.reference_id
    assert loaded_fitted.reference_version == ds.reference_version
    assert loaded_fitted.sample_count == 21
    assert loaded_fitted.ordered_reference_sample_ids == fitted.ordered_reference_sample_ids
    assert loaded_fitted.earliest_reference_event_time == fitted.earliest_reference_event_time
    assert loaded_fitted.latest_reference_event_time == fitted.latest_reference_event_time
    assert loaded_fitted.calibration_cutoff_time == fitted.calibration_cutoff_time
    assert loaded_fitted.allowed_partition_role == "calibration"
    assert loaded_fitted.calibration_split_rule == "independent_reference_set"
    assert loaded_fitted.calibration_split_version == "1.0.0"
    assert loaded_fitted.sorted_reference_scores == fitted.sorted_reference_scores
    assert loaded_fitted.quantiles == fitted.quantiles
    assert loaded_fitted.is_degenerate == fitted.is_degenerate
    assert loaded_fitted.degenerate_constant_value == fitted.degenerate_constant_value


def test_fitted_calibration_tampered_partition_role_deserialization_rejected() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    ds = _sample_reference_dataset("prophet")
    calibrator.fit_reference_data([ds])

    fitted = calibrator._fitted_models[("prophet", "1.0.0")]
    raw_dict = json.loads(fitted.model_dump_json())

    # Tampered to evaluation
    dict_eval = dict(raw_dict)
    dict_eval["allowed_partition_role"] = "evaluation"
    with pytest.raises(ValidationError):
        ModelFittedCalibration.model_validate(dict_eval)

    # Tampered to test
    dict_test = dict(raw_dict)
    dict_test["allowed_partition_role"] = "test"
    with pytest.raises(ValidationError):
        ModelFittedCalibration.model_validate(dict_test)


def test_target_signal_leakage_in_reference_lineage_rejected() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    target_signal_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    ref_ds = _sample_reference_dataset("prophet", sample_ids=[target_signal_id] + [uuid.uuid4() for _ in range(20)])
    calibrator.fit_reference_data([ref_ds])

    target_signal = _sample_signal("prophet", 0.65, signal_id=target_signal_id)
    inp = calibration_input_from_signal(target_signal)

    res = calibrator.calibrate_input(inp)
    assert res.status == CalibratedScoreStatus.INVALID_INPUT
    assert res.calibrated_signal is None
    assert "Target signal leakage rejected" in str(res.error_message)


def test_common_calibration_across_all_three_model_families() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_p = _sample_reference_dataset("prophet")
    ref_if = _sample_reference_dataset("isolation_forest")
    ref_ae = _sample_reference_dataset("autoencoder")

    calibrator.fit_reference_data([ref_p, ref_if, ref_ae])

    # Calibrate Prophet output
    sig_p = _sample_signal("prophet", 0.50)
    inp_p = calibration_input_from_signal(sig_p)
    res_p = calibrator.calibrate_input(inp_p)

    assert res_p.status == CalibratedScoreStatus.SUCCESS
    assert res_p.calibrated_score == pytest.approx(0.50, abs=1e-3)
    assert res_p.severity == EventSeverity.WARNING

    # Calibrate Isolation Forest output
    sig_if = _sample_signal("isolation_forest", 0.75)
    inp_if = calibration_input_from_signal(sig_if)
    res_if = calibrator.calibrate_input(inp_if)

    assert res_if.status == CalibratedScoreStatus.SUCCESS
    assert res_if.calibrated_score == pytest.approx(0.75, abs=1e-3)
    assert res_if.severity == EventSeverity.ERROR

    # Calibrate Autoencoder output
    sig_ae = _sample_signal("autoencoder", 0.90)
    inp_ae = calibration_input_from_signal(sig_ae)
    res_ae = calibrator.calibrate_input(inp_ae)

    assert res_ae.status == CalibratedScoreStatus.SUCCESS
    assert res_ae.calibrated_score == pytest.approx(0.90, abs=1e-3)
    assert res_ae.severity == EventSeverity.CRITICAL


def test_monotonicity_of_calibrated_scores() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    calibrator.fit_reference_data([_sample_reference_dataset("prophet")])

    scores = [0.1, 0.3, 0.5, 0.7, 0.9]
    cal_scores = []
    for s in scores:
        sig = _sample_signal("prophet", s)
        inp = calibration_input_from_signal(sig)
        res = calibrator.calibrate_input(inp)
        assert res.calibrated_score is not None
        cal_scores.append(res.calibrated_score)

    for i in range(len(cal_scores) - 1):
        assert cal_scores[i] <= cal_scores[i + 1]


def test_boundary_values_zero_and_one() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    calibrator.fit_reference_data([_sample_reference_dataset("prophet")])

    res_0 = calibrator.calibrate_input(calibration_input_from_signal(_sample_signal("prophet", 0.0)))
    assert res_0.calibrated_score == 0.0
    assert res_0.severity == EventSeverity.DEBUG

    res_1 = calibrator.calibrate_input(calibration_input_from_signal(_sample_signal("prophet", 1.0)))
    assert res_1.calibrated_score == 1.0
    assert res_1.severity == EventSeverity.CRITICAL


def test_severity_mapping_exact_threshold_boundaries() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)

    # Info threshold = 0.20:
    assert calibrator.map_score_to_severity(0.0) == EventSeverity.DEBUG
    assert calibrator.map_score_to_severity(0.199) == EventSeverity.DEBUG
    assert calibrator.map_score_to_severity(0.200) == EventSeverity.INFO
    assert calibrator.map_score_to_severity(0.201) == EventSeverity.INFO

    # Warning threshold = 0.50:
    assert calibrator.map_score_to_severity(0.499) == EventSeverity.INFO
    assert calibrator.map_score_to_severity(0.500) == EventSeverity.WARNING
    assert calibrator.map_score_to_severity(0.501) == EventSeverity.WARNING

    # Error threshold = 0.75:
    assert calibrator.map_score_to_severity(0.749) == EventSeverity.WARNING
    assert calibrator.map_score_to_severity(0.750) == EventSeverity.ERROR
    assert calibrator.map_score_to_severity(0.751) == EventSeverity.ERROR

    # Critical threshold = 0.90:
    assert calibrator.map_score_to_severity(0.899) == EventSeverity.ERROR
    assert calibrator.map_score_to_severity(0.900) == EventSeverity.CRITICAL
    assert calibrator.map_score_to_severity(0.901) == EventSeverity.CRITICAL
    assert calibrator.map_score_to_severity(1.000) == EventSeverity.CRITICAL


def test_immutability_of_source_signal_and_lineage_preservation() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    calibrator.fit_reference_data([_sample_reference_dataset("prophet")])

    orig_signal = _sample_signal("prophet", 0.50)
    orig_signal_id = orig_signal.signal_id
    orig_score = orig_signal.anomaly_score
    orig_severity = orig_signal.severity

    inp = calibration_input_from_signal(orig_signal)
    res = calibrator.calibrate_input(inp)

    # Source signal was NOT mutated
    assert orig_signal.signal_id == orig_signal_id
    assert orig_signal.anomaly_score == orig_score
    assert orig_signal.severity == orig_severity

    # Calibrated signal preserves source lineage in tags and metadata
    assert res.calibrated_signal is not None
    cal_sig = res.calibrated_signal
    assert cal_sig.tags["source_signal_id"] == str(orig_signal_id)
    assert cal_sig.tags["calibration_status"] == "calibrated"
    assert cal_sig.threshold_or_calibration.parameters["source_baseline_normalized_score"] == orig_score


def test_missing_model_calibration_returns_explicit_status() -> None:
    cfg = _sample_calibration_config()
    calibrator = CommonScoreCalibrator(config=cfg)
    calibrator.fit_reference_data([_sample_reference_dataset("prophet")])

    sig_if = _sample_signal("isolation_forest", 0.70)
    inp_if = calibration_input_from_signal(sig_if)
    res_if = calibrator.calibrate_input(inp_if)

    assert res_if.status == CalibratedScoreStatus.MISSING_CALIBRATION
    assert res_if.calibrated_signal is None
    assert "No fitted calibration reference" in str(res_if.error_message)


def test_insufficient_reference_data_rejection() -> None:
    cfg = _sample_calibration_config(min_calibration_samples_per_model=10)
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_short = _sample_reference_dataset("prophet", scores=[0.1, 0.2, 0.3, 0.4, 0.5])
    with pytest.raises(CalibrationReferenceError, match="requires at least 10"):
        calibrator.fit_reference_data([ref_short])


def test_tied_and_degenerate_reference_distribution() -> None:
    cfg = _sample_calibration_config(
        min_calibration_samples_per_model=10,
        degenerate_distribution_policy="center_or_zero",
    )
    calibrator = CommonScoreCalibrator(config=cfg)

    ref_degen = _sample_reference_dataset("prophet", scores=[0.50 for _ in range(10)])
    calibrator.fit_reference_data([ref_degen])

    res_low = calibrator.calibrate_input(calibration_input_from_signal(_sample_signal("prophet", 0.40)))
    assert res_low.calibrated_score == 0.0

    res_high = calibrator.calibrate_input(calibration_input_from_signal(_sample_signal("prophet", 0.60)))
    assert res_high.calibrated_score == 1.0


def test_deterministic_repeated_calibration() -> None:
    cfg = _sample_calibration_config()
    calibrator1 = CommonScoreCalibrator(config=cfg)
    calibrator2 = CommonScoreCalibrator(config=cfg)

    ref = _sample_reference_dataset("prophet")
    calibrator1.fit_reference_data([ref])
    calibrator2.fit_reference_data([ref])

    sig = _sample_signal("prophet", 0.63)
    inp = calibration_input_from_signal(sig)

    res1 = calibrator1.calibrate_input(inp)
    res2 = calibrator2.calibrate_input(inp)

    assert res1.status == res2.status == CalibratedScoreStatus.SUCCESS
    assert res1.calibrated_score == pytest.approx(res2.calibrated_score, rel=1e-5)
    assert res1.severity == res2.severity


def test_calibration_input_converters_from_direct_model_results() -> None:
    sig_p = _sample_signal("prophet", 0.55)
    res_p = ProphetScoreResult(
        status=ProphetScoreStatus.SUCCESS,
        target_window_index=14,
        target_timestamp=sig_p.event_time,
        residual=50.0,
        absolute_deviation=50.0,
        anomaly_score=0.55,
        signal=sig_p,
    )
    inp_p = calibration_input_from_prophet(res_p)
    assert inp_p.source_model_name == "prophet"
    assert inp_p.raw_score == 50.0
    assert inp_p.raw_score_type == "forecast_absolute_deviation"
    assert inp_p.signed_residual == 50.0
    assert inp_p.baseline_normalized_score == 0.55

    sig_if = _sample_signal("isolation_forest", 0.65)
    res_if = IsolationForestScoreResult(
        status=IsolationForestScoreStatus.SUCCESS,
        target_window_index=19,
        target_timestamp=sig_if.event_time,
        raw_score_samples=-0.58,
        raw_decision_function=-0.08,
        anomaly_score=0.65,
        signal=sig_if,
    )
    inp_if = calibration_input_from_isolation_forest(res_if)
    assert inp_if.source_model_name == "isolation_forest"
    assert inp_if.raw_score == -0.58
    assert inp_if.raw_score_direction == "lower_is_more_anomalous"

    sig_ae = _sample_signal("autoencoder", 0.75)
    res_ae = AutoencoderScoreResult(
        status=AutoencoderScoreStatus.SUCCESS,
        target_window_index=19,
        target_timestamp=sig_ae.event_time,
        raw_reconstruction_error=0.045,
        anomaly_score=0.75,
        signal=sig_ae,
    )
    inp_ae = calibration_input_from_autoencoder(res_ae)
    assert inp_ae.source_model_name == "autoencoder"
    assert inp_ae.raw_score == 0.045
    assert inp_ae.raw_score_direction == "higher_is_more_anomalous"


def test_non_success_model_result_conversion_rejected() -> None:
    res_failed = ProphetScoreResult(
        status=ProphetScoreStatus.FIT_FAILURE,
        target_window_index=10,
        target_timestamp=datetime.now(timezone.utc),
        error_message="Stan failed",
    )
    with pytest.raises(CalibrationError, match="Cannot build CalibrationInput from non-success"):
        calibration_input_from_prophet(res_failed)


def test_unsupported_model_returns_unsupported_status() -> None:
    cfg = _sample_calibration_config(supported_models=["prophet"])
    calibrator = CommonScoreCalibrator(config=cfg)

    sig_if = _sample_signal("isolation_forest", 0.70)
    inp_if = calibration_input_from_signal(sig_if)

    res = calibrator.calibrate_input(inp_if)
    assert res.status == CalibratedScoreStatus.UNSUPPORTED_MODEL
    assert res.calibrated_signal is None
    assert "Unsupported source model name" in str(res.error_message)


def test_non_finite_or_out_of_bounds_source_score_rejection() -> None:
    t = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    tw = EventTimeWindow(start_time=t, end_time=t)

    # Out of bounds baseline score
    with pytest.raises(ValidationError):
        CalibrationInput(
            source_model_name="prophet",
            source_model_version="1.0.0",
            source_configuration_version="1.0",
            source_normalization_method="norm",
            source_normalization_version="1.0.0",
            raw_score=10.0,
            raw_score_type="residual",
            raw_score_direction="higher_is_more_anomalous",
            baseline_normalized_score=1.5,
            event_time=t,
            service="order-service",
            metric_or_feature="http_request_duration_ms:mean",
            source_event_time_window=tw,
            run_id=uuid.uuid4(),
            scenario_id="scenario",
            scenario_version="1.0.0",
            seed=42,
            reproducibility_key="repro",
            tenant_id=uuid.uuid4(),
            environment="simulation",
        )

    # Non-finite raw score
    with pytest.raises(ValidationError):
        CalibrationInput(
            source_model_name="prophet",
            source_model_version="1.0.0",
            source_configuration_version="1.0",
            source_normalization_method="norm",
            source_normalization_version="1.0.0",
            raw_score=float("nan"),
            raw_score_type="residual",
            raw_score_direction="higher_is_more_anomalous",
            baseline_normalized_score=0.5,
            event_time=t,
            service="order-service",
            metric_or_feature="http_request_duration_ms:mean",
            source_event_time_window=tw,
            run_id=uuid.uuid4(),
            scenario_id="scenario",
            scenario_version="1.0.0",
            seed=42,
            reproducibility_key="repro",
            tenant_id=uuid.uuid4(),
            environment="simulation",
        )
