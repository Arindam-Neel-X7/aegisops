from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import math
from typing import Any, Literal
import uuid

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
import sklearn
from sklearn.ensemble import IsolationForest

from app.anomaly.errors import (
    IsolationForestFitError,
)
from app.anomaly.features import FeatureExtractionResult, FeatureWindow, compute_quantile
from app.anomaly.models import (
    SUPPORTED_ANOMALY_SCHEMA_VERSION,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.telemetry.schemas import EventSeverity

SUPPORTED_IF_CONFIG_SCHEMA_VERSION = "1.0"
MAX_UINT32 = 4294967295  # 2**32 - 1: maximum valid random_state for scikit-learn


class IsolationForestScoreStatus(StrEnum):
    """Execution status for a single Isolation Forest scoring operation."""

    SUCCESS = "success"
    INSUFFICIENT_HISTORY = "insufficient_history"
    MISSING_FEATURES = "missing_features"
    FIT_FAILURE = "fit_failure"
    INVALID_INPUT = "invalid_input"
    DEGENERATE_SERIES = "degenerate_series"


class ScalingParameters(BaseModel):
    """Deterministic, immutable feature scaling parameters fitted on causal training history."""

    model_config = ConfigDict(frozen=True)

    method: str
    feature_names: list[str]
    centers: list[float]
    scales: list[float]


class IsolationForestBaselineConfig(BaseModel):
    """Typed, immutable, versioned configuration for Isolation Forest anomaly detection baseline."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(default=SUPPORTED_IF_CONFIG_SCHEMA_VERSION, min_length=1)
    model_name: str = Field(default="isolation_forest", min_length=1)
    model_version: str = Field(default="1.0.0", min_length=1)
    feature_names: list[str] = Field(min_length=1)
    scaling_method: Literal["standard", "robust", "minmax", "none"] = "standard"
    scaling_version: str = Field(default="1.0.0", min_length=1)
    n_estimators: int = Field(default=100, ge=10, le=1000)
    max_samples: int | float | Literal["auto"] = "auto"
    contamination: float | Literal["auto"] = 0.05
    max_features: float = Field(default=1.0, gt=0.0, le=1.0)
    bootstrap: bool = False
    random_state: int = Field(default=42, ge=0, le=MAX_UINT32)
    n_jobs: int = Field(default=1, ge=1)
    min_training_windows: int = Field(default=10, ge=3)
    raw_score_interpretation: str = Field(
        default="sklearn_score_samples_opposite_anomaly_direction", min_length=1
    )
    score_normalization_method: str = Field(
        default="empirical_spread_offset_sigmoid", min_length=1
    )
    score_normalization_version: str = Field(default="1.0.0", min_length=1)
    score_normalization_parameters: dict[str, Any] = Field(
        default_factory=lambda: {"scale_factor": 2.0, "epsilon_spread": 0.05}
    )
    warning_threshold: float = Field(default=0.50, ge=0.0, le=1.0)
    error_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    critical_threshold: float = Field(default=0.90, ge=0.0, le=1.0)
    artifact_metadata_version: str = Field(default="1.0.0", min_length=1)

    @field_validator("model_name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        cleaned = v.strip().lower()
        if cleaned != "isolation_forest":
            raise ValueError(f"model_name must be 'isolation_forest', got '{v}'")
        return cleaned

    @field_validator(
        "schema_version",
        "model_version",
        "scaling_version",
        "raw_score_interpretation",
        "score_normalization_method",
        "score_normalization_version",
        "artifact_metadata_version",
    )
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("feature_names")
    @classmethod
    def validate_feature_names(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("feature_names list cannot be empty")
        cleaned = []
        for fname in v:
            if not isinstance(fname, str) or not fname.strip():
                raise ValueError("Feature name cannot be empty or whitespace-only")
            cleaned.append(fname.strip())
        return cleaned

    @field_validator("contamination")
    @classmethod
    def validate_contamination(cls, v: float | str) -> float | str:
        if isinstance(v, str):
            if v.strip().lower() != "auto":
                raise ValueError(f"contamination string must be 'auto', got '{v}'")
            return "auto"
        if not math.isfinite(v) or v <= 0.0 or v > 0.5:
            raise ValueError(f"contamination float must be in range (0.0, 0.5], got {v}")
        return float(v)

    @field_validator("random_state", mode="before")
    @classmethod
    def validate_random_state(cls, v: Any) -> int:
        if isinstance(v, bool):
            raise ValueError("random_state cannot be a boolean value")
        if not isinstance(v, int):
            raise ValueError(f"random_state must be an integer, got {type(v).__name__}")
        if v < 0 or v > MAX_UINT32:
            raise ValueError(
                f"random_state must be in range [0, {MAX_UINT32}] (2**32 - 1), got {v}"
            )
        return v

    @model_validator(mode="after")
    def validate_threshold_hierarchy(self) -> IsolationForestBaselineConfig:
        if not (self.warning_threshold <= self.error_threshold <= self.critical_threshold):
            raise ValueError(
                f"Threshold hierarchy invalid: warning ({self.warning_threshold}) <= "
                f"error ({self.error_threshold}) <= critical ({self.critical_threshold})"
            )
        return self


class IsolationForestScoreResult(BaseModel):
    """Detailed result of scoring a target observation window with Isolation Forest."""

    model_config = ConfigDict(frozen=True)

    status: IsolationForestScoreStatus
    target_window_index: int
    target_timestamp: AwareDatetime
    raw_feature_vector: dict[str, float] = Field(default_factory=dict)
    scaled_feature_vector: dict[str, float] = Field(default_factory=dict)
    raw_score_samples: float | None = None
    raw_decision_function: float | None = None
    anomaly_score: float | None = None
    signal: AnomalySignal | None = None
    history_window_count: int = 0
    error_message: str | None = None


class IsolationForestBaseline:
    """Multivariate anomaly detection baseline using scikit-learn Isolation Forest.

    Consumes deterministic Task 3.4 feature matrices, fits feature scaling and tree partitions
    strictly on preceding causal history, scores target window, and emits schema-valid AnomalySignal outputs.
    """

    def __init__(self, config: IsolationForestBaselineConfig) -> None:
        self.config = config

    def _fit_scaler(self, X_train: list[list[float]]) -> ScalingParameters:
        """Fit deterministic scaling parameters on training data matrix."""
        num_rows = len(X_train)
        num_cols = len(self.config.feature_names)
        centers: list[float] = []
        scales: list[float] = []

        method = self.config.scaling_method

        for col_idx in range(num_cols):
            col_vals = [X_train[row_idx][col_idx] for row_idx in range(num_rows)]

            if method == "standard":
                mean_val = sum(col_vals) / num_rows
                if num_rows > 1:
                    var_val = sum((v - mean_val) ** 2 for v in col_vals) / (num_rows - 1)
                    std_val = math.sqrt(var_val)
                else:
                    std_val = 0.0
                scale_val = std_val if std_val >= 1e-8 else 1.0
                centers.append(mean_val)
                scales.append(scale_val)

            elif method == "robust":
                sorted_vals = sorted(col_vals)
                median_val = compute_quantile(sorted_vals, 0.50)
                q25 = compute_quantile(sorted_vals, 0.25)
                q75 = compute_quantile(sorted_vals, 0.75)
                iqr = q75 - q25
                scale_val = iqr if iqr >= 1e-8 else 1.0
                centers.append(median_val)
                scales.append(scale_val)

            elif method == "minmax":
                min_val = min(col_vals)
                max_val = max(col_vals)
                range_val = max_val - min_val
                scale_val = range_val if range_val >= 1e-8 else 1.0
                centers.append(min_val)
                scales.append(scale_val)

            else:  # none
                centers.append(0.0)
                scales.append(1.0)

        return ScalingParameters(
            method=method,
            feature_names=list(self.config.feature_names),
            centers=centers,
            scales=scales,
        )

    def _transform(
        self, X: list[list[float]], scaler: ScalingParameters
    ) -> list[list[float]]:
        """Transform feature matrix using fitted scaling parameters."""
        transformed: list[list[float]] = []
        for row in X:
            scaled_row = [
                (val - scaler.centers[idx]) / scaler.scales[idx]
                for idx, val in enumerate(row)
            ]
            transformed.append(scaled_row)
        return transformed

    def _fit_and_score(
        self,
        history_windows: list[FeatureWindow],
        target_window: FeatureWindow,
    ) -> tuple[
        dict[str, float],
        dict[str, float],
        float,
        float,
        float,
        ScalingParameters,
        int,
    ]:
        """Fit scaler and Isolation Forest on causal history windows and score target window.

        Returns (raw_vector, scaled_vector, raw_score_samples, raw_decision_function, anomaly_score, scaler, history_count).
        """
        # 1. Build training matrix X_train
        X_train: list[list[float]] = []
        for w in history_windows:
            row = [w.values[fname] for fname in self.config.feature_names]
            X_train.append(row)

        # 2. Build target row X_target
        X_target_row = [target_window.values[fname] for fname in self.config.feature_names]

        # 3. Fit scaler only on training data
        scaler = self._fit_scaler(X_train)
        X_train_scaled = self._transform(X_train, scaler)
        X_target_scaled = self._transform([X_target_row], scaler)

        raw_vector = {
            fname: X_target_row[idx] for idx, fname in enumerate(self.config.feature_names)
        }
        scaled_vector = {
            fname: X_target_scaled[0][idx]
            for idx, fname in enumerate(self.config.feature_names)
        }

        # 4. Fit Isolation Forest model
        try:
            clf = IsolationForest(
                n_estimators=self.config.n_estimators,
                max_samples=self.config.max_samples,
                contamination=self.config.contamination,
                max_features=self.config.max_features,
                bootstrap=self.config.bootstrap,
                random_state=self.config.random_state,
                n_jobs=self.config.n_jobs,
            )
            clf.fit(X_train_scaled)

            # Score target window
            raw_scores = clf.score_samples(X_target_scaled)
            raw_decisions = clf.decision_function(X_target_scaled)

            raw_score = float(raw_scores[0])
            raw_decision = float(raw_decisions[0])

            if not (math.isfinite(raw_score) and math.isfinite(raw_decision)):
                raise IsolationForestFitError(
                    f"Isolation Forest computed non-finite outputs: score={raw_score}, decision={raw_decision}"
                )

            # Score training set to establish empirical normalization reference
            s_train = [-float(s) for s in clf.score_samples(X_train_scaled)]
            s_min = min(s_train)
            s_mean = sum(s_train) / len(s_train)
            var_s = (
                sum((v - s_mean) ** 2 for v in s_train) / (len(s_train) - 1)
                if len(s_train) > 1
                else 0.0
            )
            s_std = math.sqrt(var_s)

            # Check for degenerate constant training series matching target
            is_constant_training = all(row == X_train[0] for row in X_train)
            is_target_matching_constant = is_constant_training and (X_target_row == X_train[0])

            s_target = -raw_score

            if is_target_matching_constant:
                anomaly_score = 0.0
            elif s_target <= s_min:
                anomaly_score = 0.0
            elif s_target <= s_mean:
                denom = max(s_mean - s_min, 0.01)
                anomaly_score = max(0.0, min(0.30, 0.30 * (s_target - s_min) / denom))
            else:
                excess = s_target - s_mean
                scale_factor = float(
                    self.config.score_normalization_parameters.get("scale_factor", 2.0)
                )
                epsilon_spread = float(
                    self.config.score_normalization_parameters.get("epsilon_spread", 0.03)
                )
                scale = max(s_std * scale_factor, epsilon_spread)
                anomaly_score = min(1.0, 0.30 + 0.70 * (1.0 - math.exp(-excess / scale)))

            anomaly_score = max(0.0, min(1.0, float(anomaly_score)))

            return (
                raw_vector,
                scaled_vector,
                raw_score,
                raw_decision,
                anomaly_score,
                scaler,
                len(history_windows),
            )

        except Exception as exc:
            if isinstance(exc, IsolationForestFitError):
                raise
            raise IsolationForestFitError(
                f"Isolation Forest model execution failed: {exc}"
            ) from exc

    def _determine_severity(self, anomaly_score: float) -> EventSeverity:
        if anomaly_score >= self.config.critical_threshold:
            return EventSeverity.CRITICAL
        elif anomaly_score >= self.config.error_threshold:
            return EventSeverity.ERROR
        elif anomaly_score >= self.config.warning_threshold:
            return EventSeverity.WARNING
        return EventSeverity.INFO

    def score_target_window(
        self,
        feature_result: FeatureExtractionResult,
        target_window_index: int | None = None,
    ) -> IsolationForestScoreResult:
        """Score a single target window causally using Isolation Forest fitted on preceding history."""
        windows = feature_result.windows
        if not windows:
            return IsolationForestScoreResult(
                status=IsolationForestScoreStatus.INVALID_INPUT,
                target_window_index=-1,
                target_timestamp=datetime.now(timezone.utc),
                error_message="FeatureExtractionResult contains no windows",
            )

        target_idx = (len(windows) - 1) if target_window_index is None else target_window_index
        if target_idx < 0 or target_idx >= len(windows):
            return IsolationForestScoreResult(
                status=IsolationForestScoreStatus.INVALID_INPUT,
                target_window_index=target_idx,
                target_timestamp=datetime.now(timezone.utc),
                error_message=f"target_window_index {target_idx} out of range [0, {len(windows) - 1}]",
            )

        target_window = windows[target_idx]

        # Check all required features exist in target window
        missing_target_feats = [
            f
            for f in self.config.feature_names
            if f not in target_window.values or f in target_window.missing_features
        ]
        if missing_target_feats:
            return IsolationForestScoreResult(
                status=IsolationForestScoreStatus.MISSING_FEATURES,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                error_message=(
                    f"Configured features missing in target window {target_idx}: {missing_target_feats}"
                ),
            )

        # Validate finite values in target window
        for fname in self.config.feature_names:
            v = target_window.values[fname]
            if not math.isfinite(v):
                return IsolationForestScoreResult(
                    status=IsolationForestScoreStatus.INVALID_INPUT,
                    target_window_index=target_idx,
                    target_timestamp=target_window.observation_timestamp,
                    error_message=f"Target feature '{fname}' contains non-finite value: {v}",
                )

        # Causal history: all preceding complete windows up to target_idx - 1
        history_windows: list[FeatureWindow] = []
        for i in range(target_idx):
            w = windows[i]
            if all(
                fname in w.values and fname not in w.missing_features
                for fname in self.config.feature_names
            ):
                history_windows.append(w)

        if len(history_windows) < self.config.min_training_windows:
            return IsolationForestScoreResult(
                status=IsolationForestScoreStatus.INSUFFICIENT_HISTORY,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                raw_feature_vector={
                    fname: target_window.values[fname]
                    for fname in self.config.feature_names
                },
                history_window_count=len(history_windows),
                error_message=(
                    f"Insufficient history: available {len(history_windows)} complete windows, "
                    f"requires at least {self.config.min_training_windows}"
                ),
            )

        # Fit and score
        try:
            (
                raw_vector,
                scaled_vector,
                raw_score,
                raw_decision,
                anomaly_score,
                scaler,
                history_count,
            ) = self._fit_and_score(history_windows, target_window)
        except Exception as exc:
            return IsolationForestScoreResult(
                status=IsolationForestScoreStatus.FIT_FAILURE,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                raw_feature_vector={
                    fname: target_window.values[fname]
                    for fname in self.config.feature_names
                },
                history_window_count=len(history_windows),
                error_message=str(exc),
            )

        severity = self._determine_severity(anomaly_score)
        primary_feature = (
            self.config.feature_names[0]
            if len(self.config.feature_names) == 1
            else "multivariate_features"
        )

        # Build structured evidence
        evidence = AnomalyEvidence(
            evidence_id=uuid.uuid4(),
            evidence_type="isolation_forest_decision",
            metric_or_feature=primary_feature,
            observed_value=raw_score,
            expected_value=scaler.centers[0] if scaler.centers else 0.0,
            deviation=abs(raw_score),
            details={
                "ordered_features": list(self.config.feature_names),
                "raw_feature_vector": raw_vector,
                "scaled_feature_vector": scaled_vector,
                "raw_score_samples": raw_score,
                "raw_decision_function": raw_decision,
                "raw_score_interpretation": self.config.raw_score_interpretation,
                "scaling_method": self.config.scaling_method,
                "scaling_version": self.config.scaling_version,
                "scaling_parameters": {
                    "centers": scaler.centers,
                    "scales": scaler.scales,
                },
                "n_estimators": self.config.n_estimators,
                "contamination": self.config.contamination,
                "random_state": self.config.random_state,
                "effective_model_seed": self.config.random_state,
                "history_window_count": history_count,
                "training_start": history_windows[0].observation_timestamp.isoformat(),
                "training_end": history_windows[-1].observation_timestamp.isoformat(),
                "score_normalization_method": self.config.score_normalization_method,
                "score_normalization_version": self.config.score_normalization_version,
                "sklearn_version": sklearn.__version__,
            },
            timestamp=target_window.observation_timestamp,
        )

        # Build calibration metadata
        calibration = CalibrationMetadata(
            schema_version=SUPPORTED_ANOMALY_SCHEMA_VERSION,
            method=self.config.score_normalization_method,
            threshold_value=self.config.error_threshold,
            calibration_version=self.config.score_normalization_version,
            parameters=self.config.score_normalization_parameters,
            calibrated_at=datetime.now(timezone.utc),
        )

        # Build schema-valid AnomalySignal
        signal = AnomalySignal(
            signal_id=uuid.uuid4(),
            event_time=target_window.observation_timestamp,
            service=target_window.service,
            metric_or_feature=primary_feature,
            model_name=self.config.model_name,
            model_version=self.config.model_version,
            anomaly_score=anomaly_score,
            severity=severity,
            evidence=[evidence],
            threshold_or_calibration=calibration,
            schema_version=SUPPORTED_ANOMALY_SCHEMA_VERSION,
            run_id=target_window.run_id,
            scenario_id=target_window.scenario_id,
            scenario_version=target_window.scenario_version,
            seed=target_window.seed,
            reproducibility_key=target_window.reproducibility_key,
            source_event_ids=target_window.source_event_ids,
            source_event_time_window=EventTimeWindow(
                start_time=target_window.window_start, end_time=target_window.window_end
            ),
            environment=target_window.environment,
            tenant_id=target_window.tenant_id,
            tags={
                "model": "isolation_forest",
                "scaling_method": self.config.scaling_method,
            },
        )

        return IsolationForestScoreResult(
            status=IsolationForestScoreStatus.SUCCESS,
            target_window_index=target_idx,
            target_timestamp=target_window.observation_timestamp,
            raw_feature_vector=raw_vector,
            scaled_feature_vector=scaled_vector,
            raw_score_samples=raw_score,
            raw_decision_function=raw_decision,
            anomaly_score=anomaly_score,
            signal=signal,
            history_window_count=history_count,
        )

    def score_series(
        self,
        feature_result: FeatureExtractionResult,
        start_index: int | None = None,
    ) -> list[IsolationForestScoreResult]:
        """Score sequential windows causally across a feature extraction result."""
        windows = feature_result.windows
        if not windows:
            return []

        start = (
            self.config.min_training_windows if start_index is None else start_index
        )
        if start < 0:
            start = 0

        results: list[IsolationForestScoreResult] = []
        for idx in range(start, len(windows)):
            score_res = self.score_target_window(
                feature_result, target_window_index=idx
            )
            results.append(score_res)

        return results


def run_isolation_forest_baseline(
    feature_result: FeatureExtractionResult,
    config: IsolationForestBaselineConfig,
    target_window_index: int | None = None,
) -> IsolationForestScoreResult:
    """Convenience functional interface for executing Isolation Forest baseline scoring."""
    baseline = IsolationForestBaseline(config=config)
    return baseline.score_target_window(
        feature_result, target_window_index=target_window_index
    )
