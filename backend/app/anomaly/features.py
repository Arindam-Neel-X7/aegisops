from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import StrEnum
import math
from typing import Any, Sequence, cast
import uuid

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.anomaly.errors import (
    FeatureExtractionError,
    InsufficientDataError,
    InvalidFeatureInputError,
    WindowingError,
)
from app.anomaly.experiment import FeatureWindowConfig
from app.anomaly.models import MAX_UINT64, EventTimeWindow
from app.simulator.runtime.result import ScenarioRunResult
from app.telemetry.query.models import MetricSample
from app.telemetry.schemas import EventType, TelemetryEvent
from app.telemetry.transport.serialization import TelemetryExecutionContext

SUPPORTED_FEATURE_SCHEMA_VERSION = "1.0"


class FeatureWindowStatus(StrEnum):
    """Execution status for a single observation window."""

    COMPLETE = "complete"
    WARMUP = "warmup"
    IMPUTED = "imputed"
    INSUFFICIENT_DATA = "insufficient_data"
    EMPTY = "empty"


class ImputationStrategy(StrEnum):
    """Strategy for handling missing feature values in observation windows."""

    FORWARD_FILL = "forward_fill"
    ZERO_FILL = "zero_fill"
    MEAN_FILL = "mean_fill"
    MEDIAN_FILL = "median_fill"
    LINEAR_INTERPOLATE = "linear_interpolate"
    NONE = "none"


class RawObservation(BaseModel):
    """Normalized atomic telemetry observation for deterministic feature extraction.

    Decouples raw telemetry inputs (events, database samples, simulator results)
    from observation windowing and feature aggregation.
    """

    model_config = ConfigDict(frozen=True)

    event_id: uuid.UUID
    event_time: AwareDatetime
    service: str = Field(min_length=1)
    metric_name: str = Field(min_length=1)
    value: float
    tenant_id: uuid.UUID
    environment: str = Field(min_length=1)
    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)
    reproducibility_key: str = Field(min_length=1)
    trace_id: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @field_validator(
        "service",
        "metric_name",
        "environment",
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
    )
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("value")
    @classmethod
    def validate_finite_value(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"Observation value must be a finite float, got {v}")
        return float(v)

    @field_validator("event_time")
    @classmethod
    def validate_timezone_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("event_time must be timezone-aware (UTC)")
        return v


def compute_quantile(values: list[float], quantile: float) -> float:
    """Compute deterministic quantile using linear interpolation on sorted values.

    Equivalent to standard linear percentile interpolation without external dependencies.
    """
    if not values:
        raise ValueError("Cannot compute quantile on empty values")
    if len(values) == 1:
        return values[0]
    if quantile <= 0.0:
        return values[0]
    if quantile >= 1.0:
        return values[-1]

    idx = quantile * (len(values) - 1)
    k = int(idx)
    d = idx - k
    if k >= len(values) - 1:
        return values[-1]
    return values[k] + d * (values[k + 1] - values[k])


def compute_aggregation(
    values: list[float], method: str, window_duration_seconds: float = 1.0
) -> float:
    """Compute standard deterministic aggregation over a list of numeric observation values."""
    if not values:
        raise ValueError("Cannot compute aggregation on empty values list")

    m = method.strip().lower()
    n = len(values)

    if m == "mean":
        return sum(values) / n
    elif m in ("median", "p50"):
        sorted_vals = sorted(values)
        return compute_quantile(sorted_vals, 0.50)
    elif m == "p90":
        sorted_vals = sorted(values)
        return compute_quantile(sorted_vals, 0.90)
    elif m == "p95":
        sorted_vals = sorted(values)
        return compute_quantile(sorted_vals, 0.95)
    elif m == "p99":
        sorted_vals = sorted(values)
        return compute_quantile(sorted_vals, 0.99)
    elif m == "min":
        return min(values)
    elif m == "max":
        return max(values)
    elif m == "sum":
        return sum(values)
    elif m == "count":
        return float(n)
    elif m == "rate":
        if window_duration_seconds <= 0:
            raise ValueError(
                "window_duration_seconds must be positive for rate aggregation"
            )
        return float(n) / window_duration_seconds
    elif m in ("std", "stddev"):
        if n < 2:
            return 0.0
        avg = sum(values) / n
        variance = sum((x - avg) ** 2 for x in values) / (n - 1)
        return math.sqrt(variance)
    elif m == "first":
        return values[0]
    elif m == "last":
        return values[-1]
    else:
        raise ValueError(f"Unsupported aggregation method: '{method}'")


class FeatureWindow(BaseModel):
    """Typed, immutable observation window containing aggregated feature values and provenance."""

    model_config = ConfigDict(frozen=True)

    window_index: int = Field(ge=0)
    window_start: AwareDatetime
    window_end: AwareDatetime
    observation_timestamp: AwareDatetime
    service: str = Field(min_length=1)
    tenant_id: uuid.UUID
    environment: str = Field(min_length=1)
    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)
    reproducibility_key: str = Field(min_length=1)
    feature_config_id: str = Field(min_length=1)
    values: dict[str, float] = Field(default_factory=dict)
    missing_features: list[str] = Field(default_factory=list)
    source_event_ids: list[uuid.UUID] = Field(default_factory=list)
    source_event_count: int = 0
    raw_sample_count: dict[str, int] = Field(default_factory=dict)
    is_warmup: bool = False
    is_imputed: bool = False
    status: FeatureWindowStatus = FeatureWindowStatus.COMPLETE
    imputed_features: list[str] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        """Return True if all declared features have valid measured/imputed values."""
        return len(self.missing_features) == 0 and len(self.values) > 0

    @model_validator(mode="after")
    def validate_window_bounds(self) -> FeatureWindow:
        if self.window_start > self.window_end:
            raise ValueError(
                f"window_start ({self.window_start}) cannot be greater than window_end ({self.window_end})"
            )
        return self


class FeatureExtractionResult(BaseModel):
    """Complete container of extracted observation windows with model-comparability export interfaces."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(default=SUPPORTED_FEATURE_SCHEMA_VERSION, min_length=1)
    feature_config_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    tenant_id: uuid.UUID
    environment: str = Field(min_length=1)
    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)
    reproducibility_key: str = Field(min_length=1)
    window_size_seconds: float = Field(gt=0)
    step_size_seconds: float = Field(gt=0)
    feature_names: list[str] = Field(min_length=1)
    aggregation_methods: list[str] = Field(default_factory=list)
    imputation_strategy: str = Field(default="forward_fill", min_length=1)
    windows: list[FeatureWindow] = Field(default_factory=list)
    total_windows: int = 0
    valid_windows: int = 0
    warmup_windows: int = 0
    imputed_windows: int = 0
    empty_windows: int = 0
    source_event_ids: list[uuid.UUID] = Field(default_factory=list)
    time_range: EventTimeWindow | None = None
    extracted_at: AwareDatetime

    def to_prophet_series(
        self, feature_name: str | None = None, allow_incomplete: bool = False
    ) -> list[dict[str, Any]]:
        """Export normalized time-series rows for Prophet model consumption.

        For windows with unresolved missing features:
        - If allow_incomplete is False (default): raises InsufficientDataError.
        - If allow_incomplete is True: exports explicit None for missing values with is_missing=True.

        Returns a list of dicts with standard Prophet column mapping:
        - `ds`: ISO-8601 observation timestamp string
        - `y`: numeric target value (or None if incomplete and allowed)
        - `is_missing`: boolean flag indicating if target value was missing
        - additional features, window_index, service, and source_event_ids for research provenance.
        """
        target_feature = feature_name
        if target_feature is None:
            if not self.feature_names:
                raise FeatureExtractionError(
                    "No feature names declared in FeatureExtractionResult"
                )
            target_feature = self.feature_names[0]

        if target_feature not in self.feature_names:
            raise FeatureExtractionError(
                f"Requested target feature '{target_feature}' not found in available features: {self.feature_names}"
            )

        rows: list[dict[str, Any]] = []
        for w in self.windows:
            is_target_missing = (
                target_feature not in w.values or target_feature in w.missing_features
            )
            if is_target_missing and not allow_incomplete:
                raise InsufficientDataError(
                    f"Cannot export Prophet series: window index {w.window_index} "
                    f"({w.window_start.isoformat()}) has unresolved missing target feature '{target_feature}'. "
                    "Apply an imputation strategy (e.g. 'forward_fill', 'zero_fill') or set allow_incomplete=True."
                )

            y_val = None if is_target_missing else w.values[target_feature]

            row: dict[str, Any] = {
                "ds": w.observation_timestamp.isoformat(),
                "y": y_val,
                "is_missing": is_target_missing,
                "window_index": w.window_index,
                "service": w.service,
                "source_event_ids": [str(eid) for eid in w.source_event_ids],
                "is_warmup": w.is_warmup,
                "is_imputed": w.is_imputed,
                "status": w.status.value,
            }
            # Include all feature values as potential regressors
            for fname in self.feature_names:
                if fname != target_feature:
                    row[fname] = w.values.get(fname, None)
            rows.append(row)
        return rows

    def to_isolation_forest_matrix(
        self, feature_names: list[str] | None = None, allow_incomplete: bool = False
    ) -> tuple[list[list[float]], list[FeatureWindow]]:
        """Export normalized 2D feature matrix for Isolation Forest model consumption.

        For windows with unresolved missing features:
        - If allow_incomplete is False (default): raises InsufficientDataError preventing silent zero imputation.
        - If allow_incomplete is True: excludes incomplete windows and returns only fully populated windows.

        Returns:
        - `matrix`: 2D list of float values `X[row][col]` where rows correspond to complete windows
          and columns correspond strictly to the ordered `feature_names`.
        - `windows`: Corresponding list of `FeatureWindow` metadata preserving timestamps and provenance.
        """
        ordered_features = (
            feature_names if feature_names is not None else self.feature_names
        )
        for fname in ordered_features:
            if fname not in self.feature_names:
                raise FeatureExtractionError(
                    f"Requested feature '{fname}' not available in FeatureExtractionResult: {self.feature_names}"
                )

        matrix: list[list[float]] = []
        included_windows: list[FeatureWindow] = []

        for w in self.windows:
            missing_in_window = [
                f
                for f in ordered_features
                if f not in w.values or f in w.missing_features
            ]
            if missing_in_window:
                if not allow_incomplete:
                    raise InsufficientDataError(
                        f"Cannot export numeric feature matrix: window index {w.window_index} "
                        f"({w.window_start.isoformat()}) has unresolved missing features: {missing_in_window}. "
                        "Apply an imputation strategy (e.g. 'forward_fill', 'zero_fill') or set allow_incomplete=True."
                    )
                # If allow_incomplete is True, exclude the window from the numeric matrix
                continue

            row = [w.values[fname] for fname in ordered_features]
            matrix.append(row)
            included_windows.append(w)

        return matrix, included_windows

    def to_autoencoder_matrix(
        self, feature_names: list[str] | None = None, allow_incomplete: bool = False
    ) -> tuple[list[list[float]], list[FeatureWindow]]:
        """Export normalized 2D tabular matrix for Autoencoder reconstruction model consumption."""
        return self.to_isolation_forest_matrix(
            feature_names=feature_names, allow_incomplete=allow_incomplete
        )

    def to_autoencoder_sequences(
        self,
        sequence_length: int,
        feature_names: list[str] | None = None,
        allow_incomplete: bool = False,
    ) -> tuple[list[list[list[float]]], list[list[FeatureWindow]]]:
        """Export rolling 3D sequence tensors for temporal sequence Autoencoder consumption.

        Shape: `[num_sequences, sequence_length, num_features]`.
        """
        if sequence_length <= 0:
            raise FeatureExtractionError(
                f"sequence_length must be positive, got {sequence_length}"
            )

        matrix, windows = self.to_autoencoder_matrix(
            feature_names=feature_names, allow_incomplete=allow_incomplete
        )
        if len(windows) < sequence_length:
            return [], []

        num_sequences = len(matrix) - sequence_length + 1

        sequences: list[list[list[float]]] = []
        window_sequences: list[list[FeatureWindow]] = []

        for i in range(num_sequences):
            seq_matrix = matrix[i : i + sequence_length]
            seq_windows = windows[i : i + sequence_length]
            sequences.append(seq_matrix)
            window_sequences.append(seq_windows)

        return sequences, window_sequences


def _parse_feature_declaration(
    feature_decl: str, default_aggregation: str
) -> tuple[str, str]:
    """Parse a feature declaration into (metric_name, aggregation_method).

    Supports:
    - `"http_request_duration_ms:p95"` -> `("http_request_duration_ms", "p95")`
    - `"http_request_duration_ms"` -> `("http_request_duration_ms", default_aggregation)`
    """
    clean = feature_decl.strip()
    if ":" in clean:
        parts = clean.split(":", 1)
        metric = parts[0].strip()
        agg = parts[1].strip()
        if not metric:
            raise InvalidFeatureInputError(
                f"Empty metric name in feature declaration: '{feature_decl}'"
            )
        if not agg:
            raise InvalidFeatureInputError(
                f"Empty aggregation in feature declaration: '{feature_decl}'"
            )
        return metric, agg
    return clean, default_aggregation


class DeterministicFeatureExtractor:
    """Deterministic feature extractor and observation window generator for Phase 3 models.

    Consumes raw telemetry observations, sorts strictly by (event_time, event_id),
    slices into reproducible windows, aggregates feature values, handles warm-up and imputation,
    and preserves complete research provenance.
    """

    def __init__(
        self,
        config: FeatureWindowConfig,
        target_service: str | None = None,
        warm_up_windows: int = 0,
        min_samples_per_window: int = 1,
        window_timestamp_alignment: str = "end",
    ) -> None:
        if warm_up_windows < 0:
            raise InvalidFeatureInputError(
                f"warm_up_windows must be non-negative, got {warm_up_windows}"
            )
        if min_samples_per_window < 0:
            raise InvalidFeatureInputError(
                f"min_samples_per_window must be non-negative, got {min_samples_per_window}"
            )
        if window_timestamp_alignment not in ("end", "start", "center"):
            raise InvalidFeatureInputError(
                f"window_timestamp_alignment must be 'end', 'start', or 'center', got '{window_timestamp_alignment}'"
            )

        self.config = config
        self.target_service = (
            target_service.strip() if target_service is not None else None
        )
        self.warm_up_windows = warm_up_windows
        self.min_samples_per_window = min_samples_per_window
        self.window_timestamp_alignment = window_timestamp_alignment

        default_agg = (
            self.config.aggregation_methods[0]
            if self.config.aggregation_methods
            else "mean"
        )
        self.parsed_features: list[tuple[str, str, str]] = []
        for fdecl in self.config.feature_names:
            metric, agg = _parse_feature_declaration(fdecl, default_agg)
            self.parsed_features.append((fdecl, metric, agg))

    def extract_from_observations(
        self,
        observations: Sequence[RawObservation],
        execution_context: dict[str, Any] | None = None,
    ) -> FeatureExtractionResult:
        """Extract and window deterministic features from a sequence of RawObservations."""
        now_utc = datetime.now(timezone.utc)

        # 1. Filter by target service if configured
        filtered_obs: list[RawObservation] = []
        for obs in observations:
            if self.target_service is not None and obs.service != self.target_service:
                continue
            filtered_obs.append(obs)

        # 2. Extract execution context
        resolved_service = self.target_service or (
            filtered_obs[0].service if filtered_obs else "unknown"
        )

        resolved_tenant_id: uuid.UUID
        if filtered_obs:
            resolved_tenant_id = filtered_obs[0].tenant_id
        elif execution_context and "tenant_id" in execution_context:
            t_val = execution_context["tenant_id"]
            resolved_tenant_id = (
                t_val if isinstance(t_val, uuid.UUID) else uuid.UUID(str(t_val))
            )
        else:
            resolved_tenant_id = uuid.UUID(int=0)

        resolved_environment: str = (
            filtered_obs[0].environment
            if filtered_obs
            else str(
                execution_context.get("environment", "simulation")
                if execution_context
                else "simulation"
            )
        )

        resolved_run_id: uuid.UUID
        if filtered_obs:
            resolved_run_id = filtered_obs[0].run_id
        elif execution_context and "run_id" in execution_context:
            r_val = execution_context["run_id"]
            resolved_run_id = (
                r_val if isinstance(r_val, uuid.UUID) else uuid.UUID(str(r_val))
            )
        else:
            resolved_run_id = uuid.UUID(int=0)

        resolved_scenario_id: str = (
            filtered_obs[0].scenario_id
            if filtered_obs
            else str(
                execution_context.get("scenario_id", "unknown")
                if execution_context
                else "unknown"
            )
        )
        resolved_scenario_version: str = (
            filtered_obs[0].scenario_version
            if filtered_obs
            else str(
                execution_context.get("scenario_version", "1.0.0")
                if execution_context
                else "1.0.0"
            )
        )
        resolved_seed: int = (
            filtered_obs[0].seed
            if filtered_obs
            else int(execution_context.get("seed", 0) if execution_context else 0)
        )
        resolved_repro_key: str = (
            filtered_obs[0].reproducibility_key
            if filtered_obs
            else str(
                execution_context.get("reproducibility_key", "default")
                if execution_context
                else "default"
            )
        )

        if not filtered_obs:
            return FeatureExtractionResult(
                schema_version=SUPPORTED_FEATURE_SCHEMA_VERSION,
                feature_config_id=self.config.feature_config_id,
                service=resolved_service,
                tenant_id=resolved_tenant_id,
                environment=resolved_environment,
                run_id=resolved_run_id,
                scenario_id=resolved_scenario_id,
                scenario_version=resolved_scenario_version,
                seed=resolved_seed,
                reproducibility_key=resolved_repro_key,
                window_size_seconds=self.config.window_size_seconds,
                step_size_seconds=self.config.step_size_seconds,
                feature_names=self.config.feature_names,
                aggregation_methods=self.config.aggregation_methods,
                imputation_strategy=self.config.imputation_strategy,
                windows=[],
                total_windows=0,
                valid_windows=0,
                warmup_windows=0,
                imputed_windows=0,
                empty_windows=0,
                source_event_ids=[],
                time_range=None,
                extracted_at=now_utc,
            )

        # 3. Deduplicate and sort observations strictly by (event_time, str(event_id), metric_name)
        seen_keys: set[tuple[uuid.UUID, str]] = set()
        deduped_obs: list[RawObservation] = []
        for obs in filtered_obs:
            key = (obs.event_id, obs.metric_name)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_obs.append(obs)

        # Stable deterministic sort
        deduped_obs.sort(key=lambda o: (o.event_time, str(o.event_id), o.metric_name))

        all_source_event_ids = sorted({obs.event_id for obs in deduped_obs}, key=str)
        min_time = deduped_obs[0].event_time
        max_time = deduped_obs[-1].event_time
        time_range = EventTimeWindow(start_time=min_time, end_time=max_time)

        # 4. Generate observation windows
        window_size_delta = timedelta(seconds=self.config.window_size_seconds)
        step_delta = timedelta(seconds=self.config.step_size_seconds)

        windows: list[FeatureWindow] = []
        cur_start = min_time
        window_idx = 0

        # Step through windows while cur_start <= max_time
        while cur_start <= max_time:
            cur_end = cur_start + window_size_delta

            # Determine observation timestamp alignment
            if self.window_timestamp_alignment == "end":
                obs_ts = cur_end
            elif self.window_timestamp_alignment == "start":
                obs_ts = cur_start
            else:  # center
                obs_ts = cur_start + timedelta(
                    seconds=self.config.window_size_seconds / 2.0
                )

            # Collect observations falling in [cur_start, cur_end)
            win_obs = [o for o in deduped_obs if cur_start <= o.event_time < cur_end]
            win_source_ids = sorted({o.event_id for o in win_obs}, key=str)

            # Group metric samples
            metric_values: dict[str, list[float]] = {}
            raw_sample_count: dict[str, int] = {}
            for o in win_obs:
                metric_values.setdefault(o.metric_name, []).append(o.value)
                raw_sample_count[o.metric_name] = (
                    raw_sample_count.get(o.metric_name, 0) + 1
                )

            # Compute features for window
            feature_values: dict[str, float] = {}
            missing_features: list[str] = []

            for fdecl, mname, agg in self.parsed_features:
                samples = metric_values.get(mname, [])
                if samples:
                    try:
                        feature_values[fdecl] = compute_aggregation(
                            samples, agg, self.config.window_size_seconds
                        )
                    except Exception as exc:
                        raise WindowingError(
                            f"Aggregation '{agg}' failed on feature '{fdecl}': {exc}"
                        ) from exc
                else:
                    missing_features.append(fdecl)

            # Determine status, warm-up, and missing-data imputation
            is_warmup = window_idx < self.warm_up_windows
            is_imputed = False
            imputed_features: list[str] = []
            unresolved_missing: list[str] = []
            status: FeatureWindowStatus

            total_samples = len(win_obs)
            if total_samples == 0:
                status = FeatureWindowStatus.EMPTY
            elif total_samples < self.min_samples_per_window or missing_features:
                status = FeatureWindowStatus.INSUFFICIENT_DATA
            elif is_warmup:
                status = FeatureWindowStatus.WARMUP
            else:
                status = FeatureWindowStatus.COMPLETE

            # Imputation handling for missing features
            if missing_features:
                strategy = self.config.imputation_strategy.strip().lower()
                if strategy == ImputationStrategy.FORWARD_FILL.value:
                    if windows:
                        # Forward fill from last valid window
                        prev_window = windows[-1]
                        for mf in missing_features:
                            if mf in prev_window.values:
                                feature_values[mf] = prev_window.values[mf]
                                imputed_features.append(mf)
                            else:
                                feature_values[mf] = 0.0
                                imputed_features.append(mf)
                        is_imputed = True
                        unresolved_missing = []
                        if status in (
                            FeatureWindowStatus.EMPTY,
                            FeatureWindowStatus.INSUFFICIENT_DATA,
                        ):
                            status = (
                                FeatureWindowStatus.WARMUP
                                if is_warmup
                                else FeatureWindowStatus.IMPUTED
                            )
                    else:
                        # First window missing data: zero-fill fallback
                        for mf in missing_features:
                            feature_values[mf] = 0.0
                            imputed_features.append(mf)
                        is_imputed = True
                        unresolved_missing = []
                        if status in (
                            FeatureWindowStatus.EMPTY,
                            FeatureWindowStatus.INSUFFICIENT_DATA,
                        ):
                            status = (
                                FeatureWindowStatus.WARMUP
                                if is_warmup
                                else FeatureWindowStatus.IMPUTED
                            )
                elif strategy == ImputationStrategy.ZERO_FILL.value:
                    for mf in missing_features:
                        feature_values[mf] = 0.0
                        imputed_features.append(mf)
                    is_imputed = True
                    unresolved_missing = []
                    if status in (
                        FeatureWindowStatus.EMPTY,
                        FeatureWindowStatus.INSUFFICIENT_DATA,
                    ):
                        status = (
                            FeatureWindowStatus.WARMUP
                            if is_warmup
                            else FeatureWindowStatus.IMPUTED
                        )
                elif strategy == ImputationStrategy.NONE.value:
                    # Strategy 'none': perform NO imputation, record missing features explicitly,
                    # and do NOT insert fabricated numeric values into feature_values dict.
                    unresolved_missing = list(missing_features)
                    is_imputed = False
                    imputed_features = []
                    if status == FeatureWindowStatus.COMPLETE:
                        status = FeatureWindowStatus.INSUFFICIENT_DATA
                else:
                    unresolved_missing = list(missing_features)
                    is_imputed = False
                    imputed_features = []

            window_entry = FeatureWindow(
                window_index=window_idx,
                window_start=cur_start,
                window_end=cur_end,
                observation_timestamp=obs_ts,
                service=resolved_service,
                tenant_id=resolved_tenant_id,
                environment=resolved_environment,
                run_id=resolved_run_id,
                scenario_id=resolved_scenario_id,
                scenario_version=resolved_scenario_version,
                seed=resolved_seed,
                reproducibility_key=resolved_repro_key,
                feature_config_id=self.config.feature_config_id,
                values=feature_values,
                missing_features=unresolved_missing,
                source_event_ids=win_source_ids,
                source_event_count=len(win_source_ids),
                raw_sample_count=raw_sample_count,
                is_warmup=is_warmup,
                is_imputed=is_imputed,
                status=status,
                imputed_features=imputed_features,
            )
            windows.append(window_entry)

            cur_start += step_delta
            window_idx += 1

        total_wins = len(windows)
        warmup_count = sum(1 for w in windows if w.is_warmup)
        imputed_count = sum(1 for w in windows if w.is_imputed)
        empty_count = sum(1 for w in windows if w.status == FeatureWindowStatus.EMPTY)
        valid_count = sum(
            1
            for w in windows
            if w.status
            in (
                FeatureWindowStatus.COMPLETE,
                FeatureWindowStatus.IMPUTED,
                FeatureWindowStatus.WARMUP,
            )
            and len(w.missing_features) == 0
        )

        return FeatureExtractionResult(
            schema_version=SUPPORTED_FEATURE_SCHEMA_VERSION,
            feature_config_id=self.config.feature_config_id,
            service=resolved_service,
            tenant_id=resolved_tenant_id,
            environment=resolved_environment,
            run_id=resolved_run_id,
            scenario_id=resolved_scenario_id,
            scenario_version=resolved_scenario_version,
            seed=resolved_seed,
            reproducibility_key=resolved_repro_key,
            window_size_seconds=self.config.window_size_seconds,
            step_size_seconds=self.config.step_size_seconds,
            feature_names=self.config.feature_names,
            aggregation_methods=self.config.aggregation_methods,
            imputation_strategy=self.config.imputation_strategy,
            windows=windows,
            total_windows=total_wins,
            valid_windows=valid_count,
            warmup_windows=warmup_count,
            imputed_windows=imputed_count,
            empty_windows=empty_count,
            source_event_ids=all_source_event_ids,
            time_range=time_range,
            extracted_at=now_utc,
        )


def observations_from_scenario_result(
    result: ScenarioRunResult, target_service: str | None = None
) -> list[RawObservation]:
    """Extract RawObservations from an immutable completed ScenarioRunResult."""
    observations: list[RawObservation] = []
    for event in result.telemetry_events:
        if event.event_type != EventType.METRIC:
            continue
        if target_service is not None and event.service != target_service:
            continue

        metric_name = event.payload.get("metric_name")
        val = event.payload.get("value")
        if not metric_name or val is None:
            continue

        try:
            numeric_val = float(val)
        except (ValueError, TypeError):
            continue

        obs = RawObservation(
            event_id=event.event_id,
            event_time=event.event_time,
            service=event.service,
            metric_name=metric_name,
            value=numeric_val,
            tenant_id=event.tenant_id,
            environment=event.environment,
            run_id=result.run_id,
            scenario_id=result.scenario_id,
            scenario_version=result.scenario_version,
            seed=result.seed,
            reproducibility_key=result.reproducibility_key,
            trace_id=event.trace_id,
            tags={"source": "scenario_run_result"},
        )
        observations.append(obs)
    return observations


def observations_from_telemetry_events(
    events: Sequence[TelemetryEvent],
    context: TelemetryExecutionContext | None = None,
    target_service: str | None = None,
) -> list[RawObservation]:
    """Extract RawObservations from canonical TelemetryEvents."""
    observations: list[RawObservation] = []
    for event in events:
        if event.event_type != EventType.METRIC:
            continue
        if target_service is not None and event.service != target_service:
            continue

        metric_name = event.payload.get("metric_name")
        val = event.payload.get("value")
        if not metric_name or val is None:
            continue

        try:
            numeric_val = float(val)
        except (ValueError, TypeError):
            continue

        run_id = context.run_id if context else uuid.UUID(int=0)
        scenario_id = context.scenario_id if context else "unknown"
        scenario_version = context.scenario_version if context else "1.0.0"
        seed = context.seed if context else 0
        repro_key = context.reproducibility_key if context else "default"

        obs = RawObservation(
            event_id=event.event_id,
            event_time=event.event_time,
            service=event.service,
            metric_name=metric_name,
            value=numeric_val,
            tenant_id=event.tenant_id,
            environment=event.environment,
            run_id=run_id,
            scenario_id=scenario_id,
            scenario_version=scenario_version,
            seed=seed,
            reproducibility_key=repro_key,
            trace_id=event.trace_id,
            tags={"source": "telemetry_event"},
        )
        observations.append(obs)
    return observations


def observations_from_metric_samples(
    samples: Sequence[MetricSample],
    scenario_version: str = "1.0.0",
    reproducibility_key: str = "",
) -> list[RawObservation]:
    """Extract RawObservations from VictoriaMetrics MetricSample records."""
    observations: list[RawObservation] = []
    for sample in samples:
        obs = RawObservation(
            event_id=sample.event_id,
            event_time=sample.timestamp,
            service=sample.service,
            metric_name=sample.metric,
            value=sample.value,
            tenant_id=sample.tenant_id,
            environment=sample.environment,
            run_id=sample.run_id,
            scenario_id=sample.scenario_id or "unknown",
            scenario_version=scenario_version,
            seed=sample.seed,
            reproducibility_key=reproducibility_key or f"seed-{sample.seed}",
            tags={"source": "metric_sample"},
        )
        observations.append(obs)
    return observations


def extract_features(
    input_data: Sequence[RawObservation]
    | ScenarioRunResult
    | Sequence[TelemetryEvent]
    | Sequence[MetricSample],
    config: FeatureWindowConfig,
    target_service: str | None = None,
    warm_up_windows: int = 0,
    min_samples_per_window: int = 1,
    context: TelemetryExecutionContext | None = None,
    window_timestamp_alignment: str = "end",
) -> FeatureExtractionResult:
    """High-level deterministic feature extraction and observation windowing entry point."""
    observations: list[RawObservation]
    if isinstance(input_data, ScenarioRunResult):
        observations = observations_from_scenario_result(
            input_data, target_service=target_service
        )
    elif input_data and isinstance(input_data[0], RawObservation):
        observations = list(input_data)  # type: ignore[arg-type]
    elif input_data and isinstance(input_data[0], MetricSample):
        observations = observations_from_metric_samples(input_data)  # type: ignore[arg-type]
    elif input_data and isinstance(input_data[0], TelemetryEvent):
        observations = observations_from_telemetry_events(
            cast(Sequence[TelemetryEvent], input_data),
            context=context,
            target_service=target_service,
        )
    else:
        observations = []

    extractor = DeterministicFeatureExtractor(
        config=config,
        target_service=target_service,
        warm_up_windows=warm_up_windows,
        min_samples_per_window=min_samples_per_window,
        window_timestamp_alignment=window_timestamp_alignment,
    )
    return extractor.extract_from_observations(observations)
