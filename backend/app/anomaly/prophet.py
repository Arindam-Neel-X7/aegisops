from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import logging
import math
from typing import Any, Literal
import uuid

import pandas as pd
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# Suppress verbose prophet / cmdstanpy output during fitting
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
logging.getLogger("prophet").setLevel(logging.WARNING)

from prophet import Prophet  # noqa: E402

from app.anomaly.errors import ProphetFitError  # noqa: E402
from app.anomaly.features import FeatureExtractionResult, FeatureWindow  # noqa: E402
from app.anomaly.models import (  # noqa: E402
    MAX_UINT64,
    SUPPORTED_ANOMALY_SCHEMA_VERSION,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.telemetry.schemas import EventSeverity  # noqa: E402

SUPPORTED_PROPHET_CONFIG_SCHEMA_VERSION = "1.0"


class ProphetScoreStatus(StrEnum):
    """Outcome status for a single Prophet scoring execution."""

    SUCCESS = "success"
    INSUFFICIENT_HISTORY = "insufficient_history"
    MISSING_TARGET_VALUE = "missing_target_value"
    FIT_FAILURE = "fit_failure"
    INVALID_INPUT = "invalid_input"
    DEGENERATE_SERIES = "degenerate_series"


class ProphetBaselineConfig(BaseModel):
    """Typed, immutable, versioned configuration for Prophet anomaly detection baseline."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(
        default=SUPPORTED_PROPHET_CONFIG_SCHEMA_VERSION, min_length=1
    )
    model_name: str = Field(default="prophet", min_length=1)
    model_version: str = Field(default="1.0.0", min_length=1)
    target_feature: str = Field(min_length=1)
    regressors: list[str] = Field(default_factory=list)
    min_history_windows: int = Field(default=10, ge=3)
    forecast_horizon_windows: int = Field(default=1, ge=1)
    interval_width: float = Field(default=0.95, gt=0.0, lt=1.0)
    growth: Literal["linear", "flat"] = "linear"
    changepoint_prior_scale: float = Field(default=0.05, gt=0.0)
    seasonality_prior_scale: float = Field(default=10.0, gt=0.0)
    seasonality_mode: Literal["additive", "multiplicative"] = "additive"
    yearly_seasonality: bool = False
    weekly_seasonality: bool = False
    daily_seasonality: bool = False
    uncertainty_samples: int = Field(default=1000, ge=0)
    seed: int = Field(default=42, ge=0, le=MAX_UINT64)
    score_normalization_method: str = Field(
        default="residual_interval_ratio_sigmoid", min_length=1
    )
    score_normalization_version: str = Field(default="1.0.0", min_length=1)
    score_normalization_parameters: dict[str, Any] = Field(
        default_factory=lambda: {
            "scale_factor": 3.0,
            "floor": 0.0,
            "cap": 1.0,
            "epsilon": 1e-4,
        }
    )
    warning_threshold: float = Field(default=0.50, ge=0.0, le=1.0)
    error_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    critical_threshold: float = Field(default=0.90, ge=0.0, le=1.0)
    preprocessing_assumptions: dict[str, Any] = Field(
        default_factory=lambda: {
            "time_unit": "seconds",
            "imputation": "none_or_explicit",
        }
    )

    @field_validator("model_name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        cleaned = v.strip().lower()
        if cleaned != "prophet":
            raise ValueError(f"model_name must be 'prophet', got '{v}'")
        return cleaned

    @field_validator(
        "schema_version",
        "model_version",
        "target_feature",
        "score_normalization_method",
        "score_normalization_version",
    )
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @model_validator(mode="after")
    def validate_threshold_hierarchy(self) -> ProphetBaselineConfig:
        if not (
            self.warning_threshold <= self.error_threshold <= self.critical_threshold
        ):
            raise ValueError(
                f"Threshold hierarchy invalid: warning ({self.warning_threshold}) <= "
                f"error ({self.error_threshold}) <= critical ({self.critical_threshold})"
            )
        return self


class ProphetScoreResult(BaseModel):
    """Detailed result of scoring a target observation window with Prophet."""

    model_config = ConfigDict(frozen=True)

    status: ProphetScoreStatus
    target_window_index: int
    target_timestamp: AwareDatetime
    observed_value: float | None = None
    forecast_value: float | None = None
    forecast_lower: float | None = None
    forecast_upper: float | None = None
    residual: float | None = None
    absolute_deviation: float | None = None
    anomaly_score: float | None = None
    signal: AnomalySignal | None = None
    history_window_count: int = 0
    error_message: str | None = None


class ProphetBaseline:
    """Interpretable univariate/multivariate time-series anomaly detection baseline using Prophet.

    Consumes deterministic Task 3.4 feature windows, fits on strictly preceding causal history,
    forecasts target window bounds, calculates residuals, and emits schema-valid AnomalySignal outputs.
    """

    def __init__(self, config: ProphetBaselineConfig) -> None:
        self.config = config

    def _fit_and_predict(
        self,
        history_windows: list[FeatureWindow],
        target_window: FeatureWindow,
    ) -> tuple[float, float, float]:
        """Fit Prophet model on history windows and forecast target point.

        Returns (yhat, yhat_lower, yhat_upper).
        """
        # Build training records with timezone-naive UTC timestamps for Prophet compatibility
        train_rows: list[dict[str, Any]] = []
        for w in history_windows:
            ts_naive = w.observation_timestamp.astimezone(timezone.utc).replace(
                tzinfo=None
            )
            row: dict[str, Any] = {
                "ds": ts_naive,
                "y": float(w.values[self.config.target_feature]),
            }
            for reg in self.config.regressors:
                if reg not in w.values:
                    raise ProphetFitError(
                        f"Regressor '{reg}' missing in history window index {w.window_index}"
                    )
                row[reg] = float(w.values[reg])
            train_rows.append(row)

        df_train = pd.DataFrame(train_rows)

        # Build future record
        target_ts_naive = target_window.observation_timestamp.astimezone(
            timezone.utc
        ).replace(tzinfo=None)
        future_row: dict[str, Any] = {
            "ds": target_ts_naive,
        }
        for reg in self.config.regressors:
            if reg not in target_window.values:
                raise ProphetFitError(
                    f"Regressor '{reg}' missing in target window index {target_window.window_index}"
                )
            future_row[reg] = float(target_window.values[reg])

        df_future = pd.DataFrame([future_row])

        try:
            m = Prophet(
                growth=self.config.growth,
                changepoint_prior_scale=self.config.changepoint_prior_scale,
                seasonality_prior_scale=self.config.seasonality_prior_scale,
                seasonality_mode=self.config.seasonality_mode,
                yearly_seasonality=self.config.yearly_seasonality,
                weekly_seasonality=self.config.weekly_seasonality,
                daily_seasonality=self.config.daily_seasonality,
                interval_width=self.config.interval_width,
                uncertainty_samples=self.config.uncertainty_samples,
            )
            for reg in self.config.regressors:
                m.add_regressor(reg)

            # Fit model
            m.fit(df_train, seed=self.config.seed)

            # Predict target point
            forecast = m.predict(df_future)

            yhat = float(forecast.loc[0, "yhat"])
            yhat_lower = (
                float(forecast.loc[0, "yhat_lower"])
                if "yhat_lower" in forecast.columns
                else yhat
            )
            yhat_upper = (
                float(forecast.loc[0, "yhat_upper"])
                if "yhat_upper" in forecast.columns
                else yhat
            )

            if not (
                math.isfinite(yhat)
                and math.isfinite(yhat_lower)
                and math.isfinite(yhat_upper)
            ):
                raise ProphetFitError(
                    f"Prophet predicted non-finite outputs: yhat={yhat}, "
                    f"lower={yhat_lower}, upper={yhat_upper}"
                )

            return yhat, yhat_lower, yhat_upper

        except Exception as exc:
            if isinstance(exc, ProphetFitError):
                raise
            raise ProphetFitError(f"Prophet model fitting failed: {exc}") from exc

    def _compute_normalized_score(
        self,
        observed: float,
        yhat: float,
        yhat_lower: float,
        yhat_upper: float,
        history_values: list[float],
    ) -> tuple[float, float, float]:
        """Compute residual, absolute deviation, and normalized score in [0.0, 1.0]."""
        residual = observed - yhat
        abs_deviation = abs(residual)

        if abs_deviation == 0.0:
            return 0.0, 0.0, 0.0

        # Compute historical dispersion
        if len(history_values) > 1:
            avg = sum(history_values) / len(history_values)
            variance = sum((v - avg) ** 2 for v in history_values) / (
                len(history_values) - 1
            )
            hist_std = math.sqrt(variance)
        else:
            hist_std = 0.0

        band = max(yhat_upper - yhat, yhat - yhat_lower, 0.0)
        epsilon = float(self.config.score_normalization_parameters.get("epsilon", 1e-4))
        scale_factor = float(
            self.config.score_normalization_parameters.get("scale_factor", 3.0)
        )

        # Effective scale combines forecast uncertainty band and historical dispersion
        scale = max(band, hist_std, 0.01 * max(abs(yhat), 1.0), epsilon)
        deviation_ratio = abs_deviation / scale

        # Smooth monotonic saturation in [0.0, 1.0]
        score = 1.0 - math.exp(-deviation_ratio / scale_factor)
        normalized_score = max(0.0, min(1.0, float(score)))

        return residual, abs_deviation, normalized_score

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
    ) -> ProphetScoreResult:
        """Score a single target window causally against its preceding history windows."""
        windows = feature_result.windows
        if not windows:
            return ProphetScoreResult(
                status=ProphetScoreStatus.INVALID_INPUT,
                target_window_index=-1,
                target_timestamp=datetime.now(timezone.utc),
                error_message="FeatureExtractionResult contains no windows",
            )

        target_idx = (
            (len(windows) - 1) if target_window_index is None else target_window_index
        )
        if target_idx < 0 or target_idx >= len(windows):
            return ProphetScoreResult(
                status=ProphetScoreStatus.INVALID_INPUT,
                target_window_index=target_idx,
                target_timestamp=datetime.now(timezone.utc),
                error_message=f"target_window_index {target_idx} out of range [0, {len(windows) - 1}]",
            )

        target_window = windows[target_idx]

        # Check target window value
        if (
            self.config.target_feature not in target_window.values
            or self.config.target_feature in target_window.missing_features
        ):
            return ProphetScoreResult(
                status=ProphetScoreStatus.MISSING_TARGET_VALUE,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                error_message=(
                    f"Target feature '{self.config.target_feature}' is missing in window index {target_idx}"
                ),
            )

        observed_val = target_window.values[self.config.target_feature]
        if not math.isfinite(observed_val):
            return ProphetScoreResult(
                status=ProphetScoreStatus.INVALID_INPUT,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                error_message=f"Observed value is not a finite float: {observed_val}",
            )

        # Check regressors in target window
        for reg in self.config.regressors:
            if reg not in target_window.values or reg in target_window.missing_features:
                return ProphetScoreResult(
                    status=ProphetScoreStatus.MISSING_TARGET_VALUE,
                    target_window_index=target_idx,
                    target_timestamp=target_window.observation_timestamp,
                    error_message=f"Regressor '{reg}' is missing in target window index {target_idx}",
                )

        # Strictly causal history: all preceding complete windows up to target_idx - 1
        history_windows: list[FeatureWindow] = []
        for i in range(target_idx):
            w = windows[i]
            if (
                self.config.target_feature in w.values
                and self.config.target_feature not in w.missing_features
                and all(
                    reg in w.values and reg not in w.missing_features
                    for reg in self.config.regressors
                )
            ):
                history_windows.append(w)

        if len(history_windows) < self.config.min_history_windows:
            return ProphetScoreResult(
                status=ProphetScoreStatus.INSUFFICIENT_HISTORY,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                observed_value=observed_val,
                history_window_count=len(history_windows),
                error_message=(
                    f"Insufficient history: available {len(history_windows)} complete windows, "
                    f"requires at least {self.config.min_history_windows}"
                ),
            )

        # Fit and forecast
        try:
            yhat, yhat_lower, yhat_upper = self._fit_and_predict(
                history_windows, target_window
            )
        except Exception as exc:
            return ProphetScoreResult(
                status=ProphetScoreStatus.FIT_FAILURE,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                observed_value=observed_val,
                history_window_count=len(history_windows),
                error_message=str(exc),
            )

        history_vals = [w.values[self.config.target_feature] for w in history_windows]
        residual, abs_deviation, anomaly_score = self._compute_normalized_score(
            observed_val, yhat, yhat_lower, yhat_upper, history_vals
        )
        severity = self._determine_severity(anomaly_score)

        # Build structured evidence
        evidence = AnomalyEvidence(
            evidence_id=uuid.uuid4(),
            evidence_type="prophet_forecast_residual",
            metric_or_feature=self.config.target_feature,
            observed_value=observed_val,
            expected_value=yhat,
            deviation=residual,
            details={
                "forecast_lower": yhat_lower,
                "forecast_upper": yhat_upper,
                "interval_width": yhat_upper - yhat_lower,
                "absolute_deviation": abs_deviation,
                "history_window_count": len(history_windows),
                "training_start": history_windows[0].observation_timestamp.isoformat(),
                "training_end": history_windows[-1].observation_timestamp.isoformat(),
                "score_normalization_method": self.config.score_normalization_method,
                "score_normalization_version": self.config.score_normalization_version,
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
            metric_or_feature=self.config.target_feature,
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
            tags={"model": "prophet", "target_feature": self.config.target_feature},
        )

        return ProphetScoreResult(
            status=ProphetScoreStatus.SUCCESS,
            target_window_index=target_idx,
            target_timestamp=target_window.observation_timestamp,
            observed_value=observed_val,
            forecast_value=yhat,
            forecast_lower=yhat_lower,
            forecast_upper=yhat_upper,
            residual=residual,
            absolute_deviation=abs_deviation,
            anomaly_score=anomaly_score,
            signal=signal,
            history_window_count=len(history_windows),
        )

    def score_series(
        self,
        feature_result: FeatureExtractionResult,
        start_index: int | None = None,
    ) -> list[ProphetScoreResult]:
        """Score sequential windows causally across a feature extraction result."""
        windows = feature_result.windows
        if not windows:
            return []

        start = self.config.min_history_windows if start_index is None else start_index
        if start < 0:
            start = 0

        results: list[ProphetScoreResult] = []
        for idx in range(start, len(windows)):
            score_res = self.score_target_window(
                feature_result, target_window_index=idx
            )
            results.append(score_res)

        return results


def run_prophet_baseline(
    feature_result: FeatureExtractionResult,
    config: ProphetBaselineConfig,
    target_window_index: int | None = None,
) -> ProphetScoreResult:
    """Convenience functional interface for executing Prophet baseline scoring."""
    baseline = ProphetBaseline(config=config)
    return baseline.score_target_window(
        feature_result, target_window_index=target_window_index
    )
