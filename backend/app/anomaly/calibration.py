from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import math
from typing import Any, Literal, Sequence
import uuid

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.anomaly.autoencoder import AutoencoderBaselineConfig, AutoencoderScoreResult
from app.anomaly.errors import (
    CalibrationError,
    CalibrationReferenceError,
)
from app.anomaly.isolation_forest import IsolationForestBaselineConfig, IsolationForestScoreResult
from app.anomaly.models import (
    MAX_UINT64,
    SUPPORTED_ANOMALY_SCHEMA_VERSION,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.prophet import ProphetBaselineConfig, ProphetScoreResult
from app.telemetry.schemas import EventSeverity

SUPPORTED_CALIBRATION_CONFIG_SCHEMA_VERSION = "1.0"
SUPPORTED_CALIBRATION_METHOD = "empirical_cdf_percentile"
SUPPORTED_CALIBRATION_METHOD_VERSION = "1.0.0"
SUPPORTED_CALIBRATION_SPLIT_VERSION = "1.0.0"
SUPPORTED_SEVERITY_MAPPING_VERSION = "1.0.0"


class SeverityThresholdsConfig(BaseModel):
    """Typed, ordered thresholds mapping calibrated scores in [0.0, 1.0] to EventSeverity."""

    model_config = ConfigDict(frozen=True)

    info_threshold: float = Field(default=0.20, gt=0.0, le=1.0)
    warning_threshold: float = Field(default=0.50, gt=0.0, le=1.0)
    error_threshold: float = Field(default=0.75, gt=0.0, le=1.0)
    critical_threshold: float = Field(default=0.90, gt=0.0, le=1.0)

    @field_validator(
        "info_threshold",
        "warning_threshold",
        "error_threshold",
        "critical_threshold",
        mode="before",
    )
    @classmethod
    def validate_threshold_types(cls, v: Any) -> float:
        if isinstance(v, bool):
            raise ValueError("Threshold cannot be a boolean value")
        if not isinstance(v, (int, float)):
            raise ValueError(f"Threshold must be a numeric float, got {type(v).__name__}")
        val = float(v)
        if not math.isfinite(val):
            raise ValueError(f"Threshold must be finite, got {val}")
        return val

    @model_validator(mode="after")
    def validate_strict_threshold_ordering(self) -> SeverityThresholdsConfig:
        if not (
            0.0
            < self.info_threshold
            < self.warning_threshold
            < self.error_threshold
            < self.critical_threshold
            <= 1.0
        ):
            raise ValueError(
                f"Severity thresholds must be strictly ordered with non-empty DEBUG interval: "
                f"0.0 < info ({self.info_threshold}) < warning ({self.warning_threshold}) < "
                f"error ({self.error_threshold}) < critical ({self.critical_threshold}) <= 1.0"
            )
        return self


class CalibrationInput(BaseModel):
    """Immutable common input record representing an uncalibrated model output."""

    model_config = ConfigDict(frozen=True)

    source_model_name: Literal["prophet", "isolation_forest", "autoencoder"]
    source_model_version: str = Field(default="1.0.0", min_length=1)
    source_configuration_version: str = Field(default="1.0", min_length=1)
    source_normalization_method: str = Field(min_length=1)
    source_normalization_version: str = Field(default="1.0.0", min_length=1)
    raw_score: float
    raw_score_type: str = Field(min_length=1)
    raw_score_direction: Literal["higher_is_more_anomalous", "lower_is_more_anomalous"]
    signed_residual: float | None = None
    baseline_normalized_score: float = Field(ge=0.0, le=1.0)
    source_signal_id: uuid.UUID | None = None
    source_signal: AnomalySignal | None = None
    event_time: AwareDatetime
    service: str = Field(min_length=1)
    metric_or_feature: str = Field(min_length=1)
    source_event_ids: list[uuid.UUID] = Field(default_factory=list)
    source_event_time_window: EventTimeWindow | None = None
    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)
    reproducibility_key: str = Field(min_length=1)
    tenant_id: uuid.UUID
    environment: str = Field(min_length=1)

    @field_validator("raw_score", "baseline_normalized_score")
    @classmethod
    def validate_finite_scores(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"Score must be a finite float, got {v}")
        return float(v)


class ModelRawComparisonRecord(BaseModel):
    """Structured pre-calibration comparison representation across model families."""

    model_config = ConfigDict(frozen=True)

    model_name: str
    model_version: str
    raw_score_type: str
    raw_score: float
    raw_score_direction: str
    signed_residual: float | None = None
    baseline_normalized_score: float
    baseline_normalization_method: str
    feature_name: str
    event_time: AwareDatetime
    service: str
    run_id: uuid.UUID
    scenario_id: str
    seed: int
    source_event_ids: list[uuid.UUID]


class CalibrationConfig(BaseModel):
    """Typed, immutable, versioned configuration for common score calibration and severity mapping."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(min_length=1)
    calibration_method: Literal[
        "empirical_cdf_percentile", "piecewise_linear_quantile", "identity"
    ] = "empirical_cdf_percentile"
    calibration_method_version: str = Field(min_length=1)
    supported_models: list[str] = Field(
        default_factory=lambda: ["prophet", "isolation_forest", "autoencoder"]
    )
    min_calibration_samples_per_model: int = Field(default=10, ge=3)
    calibration_reference_id: str = Field(default="calib-ref-v1", min_length=1)
    calibration_reference_version: str = Field(default="1.0.0", min_length=1)
    calibration_split_rule: str = Field(default="independent_reference_set", min_length=1)
    calibration_split_version: str = Field(min_length=1)
    tie_handling_rule: Literal["average", "strict_less", "weak_less"] = "average"
    interpolation_rule: Literal["linear", "nearest", "step"] = "linear"
    clipping_policy: Literal["clamp_0_1", "error_out_of_bounds"] = "clamp_0_1"
    missing_calibration_policy: Literal["error", "reject"] = "error"
    degenerate_distribution_policy: Literal["center_or_zero", "error"] = "center_or_zero"
    severity_mapping_version: str = Field(min_length=1)
    severity_thresholds: SeverityThresholdsConfig = Field(default_factory=SeverityThresholdsConfig)

    @field_validator(
        "schema_version",
        "calibration_method_version",
        "calibration_reference_id",
        "calibration_reference_version",
        "calibration_split_rule",
        "calibration_split_version",
        "severity_mapping_version",
    )
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @model_validator(mode="after")
    def validate_strict_supported_versions(self) -> CalibrationConfig:
        if self.schema_version != SUPPORTED_CALIBRATION_CONFIG_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported schema_version: expected '{SUPPORTED_CALIBRATION_CONFIG_SCHEMA_VERSION}', "
                f"got '{self.schema_version}'"
            )
        if self.calibration_method_version != SUPPORTED_CALIBRATION_METHOD_VERSION:
            raise ValueError(
                f"Unsupported calibration_method_version: expected '{SUPPORTED_CALIBRATION_METHOD_VERSION}', "
                f"got '{self.calibration_method_version}'"
            )
        if self.calibration_split_version != SUPPORTED_CALIBRATION_SPLIT_VERSION:
            raise ValueError(
                f"Unsupported calibration_split_version: expected '{SUPPORTED_CALIBRATION_SPLIT_VERSION}', "
                f"got '{self.calibration_split_version}'"
            )
        if self.severity_mapping_version != SUPPORTED_SEVERITY_MAPPING_VERSION:
            raise ValueError(
                f"Unsupported severity_mapping_version: expected '{SUPPORTED_SEVERITY_MAPPING_VERSION}', "
                f"got '{self.severity_mapping_version}'"
            )
        return self


def create_default_calibration_config() -> CalibrationConfig:
    """Helper factory producing a fully validated default CalibrationConfig."""
    return CalibrationConfig(
        schema_version=SUPPORTED_CALIBRATION_CONFIG_SCHEMA_VERSION,
        calibration_method_version=SUPPORTED_CALIBRATION_METHOD_VERSION,
        calibration_split_version=SUPPORTED_CALIBRATION_SPLIT_VERSION,
        severity_mapping_version=SUPPORTED_SEVERITY_MAPPING_VERSION,
    )


class CalibrationReferenceSample(BaseModel):
    """Immutable per-sample calibration reference record ensuring auditable lineage."""

    model_config = ConfigDict(frozen=True)

    sample_id: uuid.UUID
    model_name: str = Field(min_length=1)
    model_version: str = Field(default="1.0.0", min_length=1)
    baseline_normalized_score: float = Field(ge=0.0, le=1.0)
    event_time: AwareDatetime
    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)
    reproducibility_key: str = Field(min_length=1)
    partition_role: Literal["calibration", "evaluation", "test"] = "calibration"

    @field_validator("baseline_normalized_score")
    @classmethod
    def validate_finite_score(cls, v: float) -> float:
        if not math.isfinite(v) or v < 0.0 or v > 1.0:
            raise ValueError(f"Score must be a finite float in [0.0, 1.0], got {v}")
        return float(v)


class CalibrationReferenceData(BaseModel):
    """Immutable calibration-reference dataset permanently restricted to the calibration partition."""

    model_config = ConfigDict(frozen=True)

    reference_id: str = Field(min_length=1)
    reference_version: str = Field(min_length=1)
    model_name: str = Field(min_length=1)
    model_version: str = Field(default="1.0.0", min_length=1)
    samples: list[CalibrationReferenceSample] = Field(min_length=1)
    calibration_cutoff_time: AwareDatetime | None = None
    allowed_partition_role: Literal["calibration"] = "calibration"
    created_at: AwareDatetime

    @property
    def reference_scores(self) -> list[float]:
        return [s.baseline_normalized_score for s in self.samples]

    @property
    def sample_count(self) -> int:
        return len(self.samples)


class ModelFittedCalibration(BaseModel):
    """Fitted calibration parameters and auditable sample lineage for a single model family."""

    model_config = ConfigDict(frozen=True)

    model_name: str
    model_version: str
    reference_id: str
    reference_version: str
    sample_count: int
    ordered_reference_sample_ids: list[uuid.UUID]
    earliest_reference_event_time: AwareDatetime | None = None
    latest_reference_event_time: AwareDatetime | None = None
    calibration_cutoff_time: AwareDatetime | None = None
    allowed_partition_role: Literal["calibration"] = "calibration"
    calibration_split_rule: str = Field(min_length=1)
    calibration_split_version: str = Field(min_length=1)
    sorted_reference_scores: list[float]
    quantiles: list[float]
    is_degenerate: bool
    degenerate_constant_value: float | None = None


class CalibratedScoreStatus(StrEnum):
    """Execution status for a single calibration operation."""

    SUCCESS = "success"
    MISSING_CALIBRATION = "missing_calibration"
    INSUFFICIENT_REFERENCE_DATA = "insufficient_reference_data"
    INVALID_INPUT = "invalid_input"
    UNSUPPORTED_MODEL = "unsupported_model"


class CalibratedScoreResult(BaseModel):
    """Detailed result of common score calibration and severity mapping."""

    model_config = ConfigDict(frozen=True)

    status: CalibratedScoreStatus
    source_model_name: str
    source_model_version: str
    raw_score: float | None = None
    raw_score_type: str | None = None
    raw_score_direction: str | None = None
    signed_residual: float | None = None
    baseline_normalized_score: float | None = None
    calibrated_score: float | None = None
    severity: EventSeverity | None = None
    calibrated_signal: AnomalySignal | None = None
    source_signal_id: uuid.UUID | None = None
    calibration_reference_id: str | None = None
    calibration_reference_version: str | None = None
    reference_sample_count: int = 0
    error_message: str | None = None


class CommonScoreCalibrator:
    """Deterministic score calibration and severity mapping engine across Phase 3 baseline models."""

    def __init__(self, config: CalibrationConfig) -> None:
        self.config = config
        self._fitted_models: dict[tuple[str, str], ModelFittedCalibration] = {}

    def fit_reference_data(
        self, reference_datasets: Sequence[CalibrationReferenceData]
    ) -> None:
        """Fit deterministic per-model empirical CDF calibration parameters from reference datasets."""
        for ds in reference_datasets:
            if ds.model_name not in self.config.supported_models:
                raise CalibrationReferenceError(
                    f"Unsupported model name '{ds.model_name}' in reference data. Supported: {self.config.supported_models}"
                )
            if len(ds.samples) < self.config.min_calibration_samples_per_model:
                raise CalibrationReferenceError(
                    f"Reference dataset for '{ds.model_name}' has {len(ds.samples)} samples, "
                    f"requires at least {self.config.min_calibration_samples_per_model}"
                )

            # Leakage check: ensure all samples strictly belong to the calibration partition
            for s in ds.samples:
                if s.partition_role != "calibration":
                    raise CalibrationReferenceError(
                        f"Leakage rejected: reference sample {s.sample_id} has partition_role '{s.partition_role}', "
                        f"expected 'calibration'."
                    )
                if ds.calibration_cutoff_time is not None and s.event_time > ds.calibration_cutoff_time:
                    raise CalibrationReferenceError(
                        f"Future leakage rejected: reference sample {s.sample_id} with event_time {s.event_time} "
                        f"exceeds calibration cutoff {ds.calibration_cutoff_time}."
                    )

            # Sort reference samples deterministically: (score, event_time, str(sample_id))
            sorted_samples = sorted(
                ds.samples,
                key=lambda smp: (
                    smp.baseline_normalized_score,
                    smp.event_time,
                    str(smp.sample_id),
                ),
            )
            n = len(sorted_samples)

            sorted_scores = [smp.baseline_normalized_score for smp in sorted_samples]
            ordered_sample_ids = [smp.sample_id for smp in sorted_samples]
            earliest_time = min(smp.event_time for smp in sorted_samples)
            latest_time = max(smp.event_time for smp in sorted_samples)

            is_degenerate = sorted_scores[0] == sorted_scores[-1]
            degenerate_val = sorted_scores[0] if is_degenerate else None

            # Empirical CDF quantiles
            quantiles = [i / (n - 1) if n > 1 else 0.50 for i in range(n)]

            fitted = ModelFittedCalibration(
                model_name=ds.model_name,
                model_version=ds.model_version,
                reference_id=ds.reference_id,
                reference_version=ds.reference_version,
                sample_count=n,
                ordered_reference_sample_ids=ordered_sample_ids,
                earliest_reference_event_time=earliest_time,
                latest_reference_event_time=latest_time,
                calibration_cutoff_time=ds.calibration_cutoff_time,
                allowed_partition_role="calibration",
                calibration_split_rule=self.config.calibration_split_rule,
                calibration_split_version=self.config.calibration_split_version,
                sorted_reference_scores=sorted_scores,
                quantiles=quantiles,
                is_degenerate=is_degenerate,
                degenerate_constant_value=degenerate_val,
            )
            self._fitted_models[(ds.model_name, ds.model_version)] = fitted

    def _calibrate_score(
        self, score: float, fitted: ModelFittedCalibration
    ) -> float:
        """Transform a baseline-normalized score into a calibrated score in [0.0, 1.0]."""
        if fitted.is_degenerate:
            if self.config.degenerate_distribution_policy == "center_or_zero":
                return 0.0 if score <= (fitted.degenerate_constant_value or 0.0) else 1.0
            raise CalibrationError("Encountered degenerate calibration distribution")

        if self.config.calibration_method == "identity":
            return max(0.0, min(1.0, score))

        # Empirical CDF percentile calibration
        scores = fitted.sorted_reference_scores
        n = len(scores)

        if score <= scores[0]:
            return 0.0
        if score >= scores[-1]:
            return 1.0

        # Binary search / linear scan for interval [scores[k], scores[k+1]]
        k = 0
        while k < n - 1 and scores[k + 1] < score:
            k += 1

        if scores[k] == scores[k + 1]:
            quantile_val = (k + 0.5) / (n - 1)
        else:
            fraction = (score - scores[k]) / (scores[k + 1] - scores[k])
            quantile_val = (k + fraction) / (n - 1)

        return max(0.0, min(1.0, float(quantile_val)))

    def map_score_to_severity(self, calibrated_score: float) -> EventSeverity:
        """Map calibrated score deterministically to EventSeverity based on strict threshold boundaries."""
        t = self.config.severity_thresholds
        if calibrated_score < t.info_threshold:
            return EventSeverity.DEBUG
        elif calibrated_score < t.warning_threshold:
            return EventSeverity.INFO
        elif calibrated_score < t.error_threshold:
            return EventSeverity.WARNING
        elif calibrated_score < t.critical_threshold:
            return EventSeverity.ERROR
        else:
            return EventSeverity.CRITICAL

    def calibrate_input(self, input_data: CalibrationInput) -> CalibratedScoreResult:
        """Calibrate a single input record and produce a schema-valid calibrated AnomalySignal."""
        model_key = (input_data.source_model_name, input_data.source_model_version)

        # Target identity requirement: must have verifiable source_signal_id
        if input_data.source_signal_id is None:
            return CalibratedScoreResult(
                status=CalibratedScoreStatus.INVALID_INPUT,
                source_model_name=input_data.source_model_name,
                source_model_version=input_data.source_model_version,
                raw_score=input_data.raw_score,
                raw_score_type=input_data.raw_score_type,
                raw_score_direction=input_data.raw_score_direction,
                signed_residual=input_data.signed_residual,
                baseline_normalized_score=input_data.baseline_normalized_score,
                source_signal_id=None,
                error_message="Target source_signal_id is required for reference-leakage verification.",
            )

        if input_data.source_model_name not in self.config.supported_models:
            return CalibratedScoreResult(
                status=CalibratedScoreStatus.UNSUPPORTED_MODEL,
                source_model_name=input_data.source_model_name,
                source_model_version=input_data.source_model_version,
                raw_score=input_data.raw_score,
                raw_score_type=input_data.raw_score_type,
                raw_score_direction=input_data.raw_score_direction,
                signed_residual=input_data.signed_residual,
                baseline_normalized_score=input_data.baseline_normalized_score,
                source_signal_id=input_data.source_signal_id,
                error_message=f"Unsupported source model name '{input_data.source_model_name}'",
            )

        if model_key not in self._fitted_models:
            return CalibratedScoreResult(
                status=CalibratedScoreStatus.MISSING_CALIBRATION,
                source_model_name=input_data.source_model_name,
                source_model_version=input_data.source_model_version,
                raw_score=input_data.raw_score,
                raw_score_type=input_data.raw_score_type,
                raw_score_direction=input_data.raw_score_direction,
                signed_residual=input_data.signed_residual,
                baseline_normalized_score=input_data.baseline_normalized_score,
                source_signal_id=input_data.source_signal_id,
                error_message=(
                    f"No fitted calibration reference for model '{input_data.source_model_name}' "
                    f"version '{input_data.source_model_version}'"
                ),
            )

        fitted = self._fitted_models[model_key]

        # Target leakage prevention: reject if target source signal ID appears in calibration reference samples
        if input_data.source_signal_id in fitted.ordered_reference_sample_ids:
            return CalibratedScoreResult(
                status=CalibratedScoreStatus.INVALID_INPUT,
                source_model_name=input_data.source_model_name,
                source_model_version=input_data.source_model_version,
                raw_score=input_data.raw_score,
                raw_score_type=input_data.raw_score_type,
                raw_score_direction=input_data.raw_score_direction,
                signed_residual=input_data.signed_residual,
                baseline_normalized_score=input_data.baseline_normalized_score,
                source_signal_id=input_data.source_signal_id,
                error_message=(
                    f"Target signal leakage rejected: source_signal_id {input_data.source_signal_id} "
                    "was detected in calibration reference dataset."
                ),
            )

        try:
            calibrated_score = self._calibrate_score(
                input_data.baseline_normalized_score, fitted
            )
        except Exception as exc:
            return CalibratedScoreResult(
                status=CalibratedScoreStatus.INVALID_INPUT,
                source_model_name=input_data.source_model_name,
                source_model_version=input_data.source_model_version,
                raw_score=input_data.raw_score,
                raw_score_type=input_data.raw_score_type,
                raw_score_direction=input_data.raw_score_direction,
                signed_residual=input_data.signed_residual,
                baseline_normalized_score=input_data.baseline_normalized_score,
                source_signal_id=input_data.source_signal_id,
                error_message=f"Score calibration computation failed: {exc}",
            )

        severity = self.map_score_to_severity(calibrated_score)

        # Build CalibrationMetadata
        calibration_meta = CalibrationMetadata(
            schema_version=SUPPORTED_ANOMALY_SCHEMA_VERSION,
            method=self.config.calibration_method,
            threshold_value=self.config.severity_thresholds.error_threshold,
            calibration_version=self.config.calibration_method_version,
            parameters={
                "calibration_config_schema_version": self.config.schema_version,
                "calibration_reference_id": fitted.reference_id,
                "calibration_reference_version": fitted.reference_version,
                "calibration_split_rule": fitted.calibration_split_rule,
                "calibration_split_version": fitted.calibration_split_version,
                "sample_count": fitted.sample_count,
                "source_model_name": input_data.source_model_name,
                "source_model_version": input_data.source_model_version,
                "source_configuration_version": input_data.source_configuration_version,
                "source_normalization_method": input_data.source_normalization_method,
                "source_normalization_version": input_data.source_normalization_version,
                "source_baseline_normalized_score": input_data.baseline_normalized_score,
                "raw_score": input_data.raw_score,
                "raw_score_type": input_data.raw_score_type,
                "raw_score_direction": input_data.raw_score_direction,
                "signed_residual": input_data.signed_residual,
                "calibrated_score": calibrated_score,
                "severity_mapping_version": self.config.severity_mapping_version,
                "severity_thresholds": {
                    "info": self.config.severity_thresholds.info_threshold,
                    "warning": self.config.severity_thresholds.warning_threshold,
                    "error": self.config.severity_thresholds.error_threshold,
                    "critical": self.config.severity_thresholds.critical_threshold,
                },
            },
            calibrated_at=datetime.now(timezone.utc),
        )

        # Build evidence preserving raw and source scores
        evidence_items: list[AnomalyEvidence] = []
        if input_data.source_signal and input_data.source_signal.evidence:
            for ev in input_data.source_signal.evidence:
                updated_details = dict(ev.details)
                updated_details["calibrated_score"] = calibrated_score
                updated_details["calibration_method"] = self.config.calibration_method
                updated_details["source_baseline_score"] = input_data.baseline_normalized_score
                if input_data.signed_residual is not None:
                    updated_details["signed_residual"] = input_data.signed_residual
                evidence_items.append(
                    AnomalyEvidence(
                        evidence_id=uuid.uuid4(),
                        evidence_type=ev.evidence_type,
                        metric_or_feature=ev.metric_or_feature,
                        observed_value=ev.observed_value,
                        expected_value=ev.expected_value,
                        deviation=ev.deviation,
                        details=updated_details,
                        timestamp=ev.timestamp,
                    )
                )
        else:
            evidence_items.append(
                AnomalyEvidence(
                    evidence_id=uuid.uuid4(),
                    evidence_type=f"{input_data.source_model_name}_calibrated_score",
                    metric_or_feature=input_data.metric_or_feature,
                    observed_value=input_data.raw_score,
                    expected_value=0.0,
                    deviation=input_data.raw_score,
                    details={
                        "raw_score": input_data.raw_score,
                        "raw_score_type": input_data.raw_score_type,
                        "raw_score_direction": input_data.raw_score_direction,
                        "signed_residual": input_data.signed_residual,
                        "source_baseline_score": input_data.baseline_normalized_score,
                        "calibrated_score": calibrated_score,
                        "calibration_method": self.config.calibration_method,
                    },
                    timestamp=input_data.event_time,
                )
            )

        # Build schema-valid calibrated AnomalySignal
        calibrated_signal = AnomalySignal(
            signal_id=uuid.uuid4(),
            event_time=input_data.event_time,
            service=input_data.service,
            metric_or_feature=input_data.metric_or_feature,
            model_name=input_data.source_model_name,
            model_version=input_data.source_model_version,
            anomaly_score=calibrated_score,
            severity=severity,
            evidence=evidence_items,
            threshold_or_calibration=calibration_meta,
            schema_version=SUPPORTED_ANOMALY_SCHEMA_VERSION,
            run_id=input_data.run_id,
            scenario_id=input_data.scenario_id,
            scenario_version=input_data.scenario_version,
            seed=input_data.seed,
            reproducibility_key=input_data.reproducibility_key,
            source_event_ids=input_data.source_event_ids,
            source_event_time_window=input_data.source_event_time_window,
            environment=input_data.environment,
            tenant_id=input_data.tenant_id,
            tags={
                "calibration_status": "calibrated",
                "calibration_reference_id": fitted.reference_id,
                "source_signal_id": str(input_data.source_signal_id)
                if input_data.source_signal_id
                else "none",
            },
        )

        return CalibratedScoreResult(
            status=CalibratedScoreStatus.SUCCESS,
            source_model_name=input_data.source_model_name,
            source_model_version=input_data.source_model_version,
            raw_score=input_data.raw_score,
            raw_score_type=input_data.raw_score_type,
            raw_score_direction=input_data.raw_score_direction,
            signed_residual=input_data.signed_residual,
            baseline_normalized_score=input_data.baseline_normalized_score,
            calibrated_score=calibrated_score,
            severity=severity,
            calibrated_signal=calibrated_signal,
            source_signal_id=input_data.source_signal_id,
            calibration_reference_id=fitted.reference_id,
            calibration_reference_version=fitted.reference_version,
            reference_sample_count=fitted.sample_count,
        )

    def calibrate_inputs(
        self, inputs: Sequence[CalibrationInput]
    ) -> list[CalibratedScoreResult]:
        """Calibrate a sequence of calibration input records."""
        return [self.calibrate_input(inp) for inp in inputs]


# Helper converters from baseline outputs
def calibration_input_from_prophet(
    result: ProphetScoreResult,
    config: ProphetBaselineConfig | None = None,
) -> CalibrationInput:
    """Create CalibrationInput from ProphetScoreResult using Option A (absolute deviation raw score)."""
    if result.status != "success" or result.signal is None or result.anomaly_score is None:
        raise CalibrationError(f"Cannot build CalibrationInput from non-success Prophet result: {result.status}")

    sig = result.signal
    norm_method = (
        config.score_normalization_method
        if config
        else "residual_interval_ratio_sigmoid"
    )
    norm_ver = config.score_normalization_version if config else "1.0.0"
    cfg_ver = config.schema_version if config else "1.0"

    abs_dev = (
        result.absolute_deviation
        if result.absolute_deviation is not None
        else (abs(result.residual) if result.residual is not None else 0.0)
    )

    return CalibrationInput(
        source_model_name="prophet",
        source_model_version=sig.model_version,
        source_configuration_version=cfg_ver,
        source_normalization_method=norm_method,
        source_normalization_version=norm_ver,
        raw_score=abs_dev,
        raw_score_type="forecast_absolute_deviation",
        raw_score_direction="higher_is_more_anomalous",
        signed_residual=result.residual,
        baseline_normalized_score=result.anomaly_score,
        source_signal_id=sig.signal_id,
        source_signal=sig,
        event_time=sig.event_time,
        service=sig.service,
        metric_or_feature=sig.metric_or_feature,
        source_event_ids=list(sig.source_event_ids),
        source_event_time_window=sig.source_event_time_window,
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        seed=sig.seed,
        reproducibility_key=sig.reproducibility_key,
        tenant_id=sig.tenant_id,
        environment=sig.environment,
    )


def calibration_input_from_isolation_forest(
    result: IsolationForestScoreResult,
    config: IsolationForestBaselineConfig | None = None,
) -> CalibrationInput:
    """Create CalibrationInput from IsolationForestScoreResult."""
    if result.status != "success" or result.signal is None or result.anomaly_score is None:
        raise CalibrationError(
            f"Cannot build CalibrationInput from non-success Isolation Forest result: {result.status}"
        )

    sig = result.signal
    norm_method = (
        config.score_normalization_method
        if config
        else "empirical_spread_offset_sigmoid"
    )
    norm_ver = config.score_normalization_version if config else "1.0.0"
    cfg_ver = config.schema_version if config else "1.0"

    return CalibrationInput(
        source_model_name="isolation_forest",
        source_model_version=sig.model_version,
        source_configuration_version=cfg_ver,
        source_normalization_method=norm_method,
        source_normalization_version=norm_ver,
        raw_score=result.raw_score_samples if result.raw_score_samples is not None else 0.0,
        raw_score_type="score_samples",
        raw_score_direction="lower_is_more_anomalous",
        signed_residual=None,
        baseline_normalized_score=result.anomaly_score,
        source_signal_id=sig.signal_id,
        source_signal=sig,
        event_time=sig.event_time,
        service=sig.service,
        metric_or_feature=sig.metric_or_feature,
        source_event_ids=list(sig.source_event_ids),
        source_event_time_window=sig.source_event_time_window,
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        seed=sig.seed,
        reproducibility_key=sig.reproducibility_key,
        tenant_id=sig.tenant_id,
        environment=sig.environment,
    )


def calibration_input_from_autoencoder(
    result: AutoencoderScoreResult,
    config: AutoencoderBaselineConfig | None = None,
) -> CalibrationInput:
    """Create CalibrationInput from AutoencoderScoreResult."""
    if result.status != "success" or result.signal is None or result.anomaly_score is None:
        raise CalibrationError(
            f"Cannot build CalibrationInput from non-success Autoencoder result: {result.status}"
        )

    sig = result.signal
    norm_method = (
        config.score_normalization_method
        if config
        else "empirical_tail_ratio_sigmoid"
    )
    norm_ver = config.score_normalization_version if config else "1.0.0"
    cfg_ver = config.schema_version if config else "1.0"

    return CalibrationInput(
        source_model_name="autoencoder",
        source_model_version=sig.model_version,
        source_configuration_version=cfg_ver,
        source_normalization_method=norm_method,
        source_normalization_version=norm_ver,
        raw_score=result.raw_reconstruction_error
        if result.raw_reconstruction_error is not None
        else 0.0,
        raw_score_type="reconstruction_error",
        raw_score_direction="higher_is_more_anomalous",
        signed_residual=None,
        baseline_normalized_score=result.anomaly_score,
        source_signal_id=sig.signal_id,
        source_signal=sig,
        event_time=sig.event_time,
        service=sig.service,
        metric_or_feature=sig.metric_or_feature,
        source_event_ids=list(sig.source_event_ids),
        source_event_time_window=sig.source_event_time_window,
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        seed=sig.seed,
        reproducibility_key=sig.reproducibility_key,
        tenant_id=sig.tenant_id,
        environment=sig.environment,
    )


def calibration_input_from_signal(signal: AnomalySignal) -> CalibrationInput:
    """Create CalibrationInput from a valid schema-compliant AnomalySignal."""
    raw_score_val = signal.anomaly_score
    raw_type = "anomaly_score"
    raw_direction: Literal["higher_is_more_anomalous", "lower_is_more_anomalous"] = (
        "higher_is_more_anomalous"
    )
    signed_res: float | None = None

    if signal.evidence:
        ev = signal.evidence[0]
        if "raw_score_samples" in ev.details and ev.details["raw_score_samples"] is not None:
            raw_score_val = float(ev.details["raw_score_samples"])
            raw_type = "score_samples"
            raw_direction = "lower_is_more_anomalous"
        elif "raw_reconstruction_error" in ev.details and ev.details["raw_reconstruction_error"] is not None:
            raw_score_val = float(ev.details["raw_reconstruction_error"])
            raw_type = "reconstruction_error"
        elif ev.deviation is not None:
            signed_res = float(ev.deviation)
            abs_d = ev.details.get("absolute_deviation")
            raw_score_val = float(abs_d) if abs_d is not None else abs(signed_res)
            raw_type = "forecast_absolute_deviation"
            raw_direction = "higher_is_more_anomalous"

    cal_method = (
        signal.threshold_or_calibration.method
        if signal.threshold_or_calibration
        else "baseline_normalization"
    )
    cal_ver = (
        signal.threshold_or_calibration.calibration_version
        if signal.threshold_or_calibration
        else "1.0.0"
    )

    model_name: Literal["prophet", "isolation_forest", "autoencoder"]
    if signal.model_name in ("prophet", "isolation_forest", "autoencoder"):
        model_name = signal.model_name  # type: ignore[assignment]
    else:
        raise CalibrationError(f"Unsupported model name in signal: '{signal.model_name}'")

    return CalibrationInput(
        source_model_name=model_name,
        source_model_version=signal.model_version,
        source_configuration_version="1.0",
        source_normalization_method=cal_method,
        source_normalization_version=cal_ver,
        raw_score=raw_score_val,
        raw_score_type=raw_type,
        raw_score_direction=raw_direction,
        signed_residual=signed_res,
        baseline_normalized_score=signal.anomaly_score,
        source_signal_id=signal.signal_id,
        source_signal=signal,
        event_time=signal.event_time,
        service=signal.service,
        metric_or_feature=signal.metric_or_feature,
        source_event_ids=list(signal.source_event_ids),
        source_event_time_window=signal.source_event_time_window,
        run_id=signal.run_id,
        scenario_id=signal.scenario_id,
        scenario_version=signal.scenario_version,
        seed=signal.seed,
        reproducibility_key=signal.reproducibility_key,
        tenant_id=signal.tenant_id,
        environment=signal.environment,
    )


def create_raw_comparison_record(input_data: CalibrationInput) -> ModelRawComparisonRecord:
    """Create a structured ModelRawComparisonRecord from CalibrationInput."""
    return ModelRawComparisonRecord(
        model_name=input_data.source_model_name,
        model_version=input_data.source_model_version,
        raw_score_type=input_data.raw_score_type,
        raw_score=input_data.raw_score,
        raw_score_direction=input_data.raw_score_direction,
        signed_residual=input_data.signed_residual,
        baseline_normalized_score=input_data.baseline_normalized_score,
        baseline_normalization_method=input_data.source_normalization_method,
        feature_name=input_data.metric_or_feature,
        event_time=input_data.event_time,
        service=input_data.service,
        run_id=input_data.run_id,
        scenario_id=input_data.scenario_id,
        seed=input_data.seed,
        source_event_ids=list(input_data.source_event_ids),
    )
