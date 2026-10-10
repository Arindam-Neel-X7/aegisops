from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import json
import math
from pathlib import Path
from typing import Any, Literal
import uuid
import warnings

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
import sklearn
from sklearn.exceptions import ConvergenceWarning
from sklearn.neural_network import MLPRegressor

from app.anomaly.errors import (
    AutoencoderArtifactError,
    AutoencoderFitError,
)
from app.anomaly.features import (
    FeatureExtractionResult,
    FeatureWindow,
    compute_quantile,
)
from app.anomaly.isolation_forest import MAX_UINT32, ScalingParameters
from app.anomaly.models import (
    SUPPORTED_ANOMALY_SCHEMA_VERSION,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.telemetry.schemas import EventSeverity

SUPPORTED_AE_CONFIG_SCHEMA_VERSION = "1.0"
SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION = "1.0.0"


class AutoencoderScoreStatus(StrEnum):
    """Execution status for a single Autoencoder scoring operation."""

    SUCCESS = "success"
    INSUFFICIENT_HISTORY = "insufficient_history"
    MISSING_FEATURES = "missing_features"
    FIT_FAILURE = "fit_failure"
    INVALID_INPUT = "invalid_input"
    NON_CONVERGENCE = "non_convergence"


class AutoencoderBaselineConfig(BaseModel):
    """Typed, immutable, versioned configuration for bounded feed-forward Autoencoder baseline."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(
        default=SUPPORTED_AE_CONFIG_SCHEMA_VERSION, min_length=1
    )
    model_name: str = Field(default="autoencoder", min_length=1)
    model_version: str = Field(default="1.0.0", min_length=1)
    framework_name: str = Field(default="scikit-learn", min_length=1)
    framework_version: str = Field(default="1.9.1", min_length=1)
    feature_names: list[str] = Field(min_length=2)
    scaling_method: Literal["standard", "robust", "minmax", "none"] = "standard"
    scaling_version: str = Field(default="1.0.0", min_length=1)
    bottleneck_dimension: int = Field(default=1, ge=1)
    hidden_layer_sizes: tuple[int, ...] = Field(default=(1,))
    activation: Literal["relu", "tanh", "identity", "logistic"] = "relu"
    solver: Literal["lbfgs", "adam", "sgd"] = "lbfgs"
    alpha: float = Field(default=0.0001, ge=0.0)
    batch_size: int | Literal["auto"] = "auto"
    learning_rate_init: float = Field(default=0.001, gt=0.0)
    max_iter: int = Field(default=200, ge=1, le=5000)
    tol: float = Field(default=1e-4, gt=0.0)
    random_state: int = Field(default=42, ge=0, le=MAX_UINT32)
    min_training_windows: int = Field(default=10, ge=3)
    training_split_rule: str = Field(default="causal_prefix", min_length=1)
    training_split_version: str = Field(default="1.0.0", min_length=1)
    reconstruction_error_method: str = Field(default="mean_squared_error", min_length=1)
    reconstruction_error_version: str = Field(default="1.0.0", min_length=1)
    score_normalization_method: str = Field(
        default="empirical_tail_ratio_sigmoid", min_length=1
    )
    score_normalization_version: str = Field(default="1.0.0", min_length=1)
    score_normalization_parameters: dict[str, Any] = Field(
        default_factory=lambda: {"scale_factor": 3.0, "epsilon_spread": 1e-4}
    )
    warning_threshold: float = Field(default=0.50, ge=0.0, le=1.0)
    error_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    critical_threshold: float = Field(default=0.90, ge=0.0, le=1.0)
    artifact_schema_version: str = Field(
        default=SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION, min_length=1
    )

    @field_validator("model_name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        cleaned = v.strip().lower()
        if cleaned != "autoencoder":
            raise ValueError(f"model_name must be 'autoencoder', got '{v}'")
        return cleaned

    @field_validator(
        "schema_version",
        "model_version",
        "framework_name",
        "framework_version",
        "scaling_version",
        "training_split_rule",
        "training_split_version",
        "reconstruction_error_method",
        "reconstruction_error_version",
        "score_normalization_method",
        "score_normalization_version",
        "artifact_schema_version",
    )
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("feature_names")
    @classmethod
    def validate_feature_names(cls, v: list[str]) -> list[str]:
        if len(v) < 2:
            raise ValueError(
                "Autoencoder requires at least 2 feature names for bottleneck compression"
            )
        cleaned = []
        for fname in v:
            if not isinstance(fname, str) or not fname.strip():
                raise ValueError("Feature name cannot be empty or whitespace-only")
            cleaned.append(fname.strip())
        return cleaned

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
    def validate_architecture_and_thresholds(self) -> AutoencoderBaselineConfig:
        input_dim = len(self.feature_names)
        if self.bottleneck_dimension >= input_dim:
            raise ValueError(
                f"bottleneck_dimension ({self.bottleneck_dimension}) must be strictly less than "
                f"input_dimension ({input_dim})"
            )
        for h_dim in self.hidden_layer_sizes:
            if h_dim <= 0:
                raise ValueError(
                    f"Hidden layer dimensions must be positive, got {h_dim}"
                )

        if not (
            self.warning_threshold <= self.error_threshold <= self.critical_threshold
        ):
            raise ValueError(
                f"Threshold hierarchy invalid: warning ({self.warning_threshold}) <= "
                f"error ({self.error_threshold}) <= critical ({self.critical_threshold})"
            )
        return self


class AutoencoderArtifact(BaseModel):
    """Deterministic, self-contained JSON-serializable artifact for a fitted Autoencoder model."""

    model_config = ConfigDict(frozen=True)

    artifact_schema_version: str = Field(
        default=SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION, min_length=1
    )
    configuration_schema_version: str = Field(
        default=SUPPORTED_AE_CONFIG_SCHEMA_VERSION, min_length=1
    )
    model_name: str = "autoencoder"
    model_version: str = "1.0.0"
    framework_name: str = "scikit-learn"
    framework_version: str = "1.9.1"
    feature_names: list[str]
    input_dimension: int
    bottleneck_dimension: int
    hidden_layer_sizes: list[int]
    activation: str
    solver: str
    scaling_parameters: ScalingParameters
    reconstruction_error_method: str = "mean_squared_error"
    reconstruction_error_version: str = "1.0.0"
    score_normalization_method: str = "empirical_tail_ratio_sigmoid"
    score_normalization_version: str = "1.0.0"
    score_normalization_parameters: dict[str, Any]
    training_stats: dict[str, Any]
    coefs: list[list[list[float]]]
    intercepts: list[list[float]]

    @model_validator(mode="after")
    def validate_artifact_compatibility(self) -> AutoencoderArtifact:
        # 1. Schema, Model, and Configuration version validation
        if self.artifact_schema_version != SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported artifact_schema_version: expected '{SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION}', "
                f"got '{self.artifact_schema_version}'"
            )
        if self.configuration_schema_version != SUPPORTED_AE_CONFIG_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported configuration_schema_version: expected '{SUPPORTED_AE_CONFIG_SCHEMA_VERSION}', "
                f"got '{self.configuration_schema_version}'"
            )
        if self.model_name != "autoencoder":
            raise ValueError(
                f"Incompatible model_name: expected 'autoencoder', got '{self.model_name}'"
            )
        if self.model_version != "1.0.0":
            raise ValueError(
                f"Incompatible model_version: expected '1.0.0', got '{self.model_version}'"
            )

        # 2. Framework validation
        if self.framework_name != "scikit-learn":
            raise ValueError(
                f"Incompatible framework_name: expected 'scikit-learn', got '{self.framework_name}'"
            )
        if self.framework_version != "1.9.1":
            raise ValueError(
                f"Incompatible framework_version: expected '1.9.1', got '{self.framework_version}'"
            )

        # 3. Feature and dimension validation
        input_dim = len(self.feature_names)
        if input_dim != self.input_dimension:
            raise ValueError(
                f"input_dimension mismatch: feature_names has {input_dim}, declared {self.input_dimension}"
            )
        if input_dim < 2:
            raise ValueError(f"input_dimension must be at least 2, got {input_dim}")
        if self.bottleneck_dimension <= 0 or self.bottleneck_dimension >= input_dim:
            raise ValueError(
                f"bottleneck_dimension ({self.bottleneck_dimension}) must be in range [1, {input_dim - 1}]"
            )

        # 4. Scaling parameters validation
        if self.scaling_parameters.feature_names != self.feature_names:
            raise ValueError(
                "scaling_parameters feature_names mismatch with artifact feature_names"
            )
        if len(self.scaling_parameters.centers) != input_dim:
            raise ValueError(
                "scaling_parameters centers length mismatch with input_dimension"
            )
        if len(self.scaling_parameters.scales) != input_dim:
            raise ValueError(
                "scaling_parameters scales length mismatch with input_dimension"
            )
        for idx, c in enumerate(self.scaling_parameters.centers):
            if not math.isfinite(c):
                raise ValueError(
                    f"scaling_parameters center at index {idx} is non-finite: {c}"
                )
        for idx, s in enumerate(self.scaling_parameters.scales):
            if not math.isfinite(s) or s <= 0.0:
                raise ValueError(
                    f"scaling_parameters scale at index {idx} must be finite positive, got: {s}"
                )

        # 5. Reconstruction and normalization parameters validation
        if self.reconstruction_error_method != "mean_squared_error":
            raise ValueError(
                f"Unsupported reconstruction_error_method: expected 'mean_squared_error', "
                f"got '{self.reconstruction_error_method}'"
            )
        if self.reconstruction_error_version != "1.0.0":
            raise ValueError(
                f"Unsupported reconstruction_error_version: expected '1.0.0', "
                f"got '{self.reconstruction_error_version}'"
            )
        if self.score_normalization_method != "empirical_tail_ratio_sigmoid":
            raise ValueError(
                f"Unsupported score_normalization_method: expected 'empirical_tail_ratio_sigmoid', got '{self.score_normalization_method}'"
            )
        if self.score_normalization_version != "1.0.0":
            raise ValueError(
                f"Unsupported score_normalization_version: expected '1.0.0', got '{self.score_normalization_version}'"
            )
        if (
            "scale_factor" not in self.score_normalization_parameters
            or "epsilon_spread" not in self.score_normalization_parameters
        ):
            raise ValueError(
                "Missing required normalization parameter ('scale_factor' or 'epsilon_spread')"
            )
        for k, v in self.score_normalization_parameters.items():
            if (
                not isinstance(v, (int, float))
                or not math.isfinite(float(v))
                or float(v) <= 0.0
            ):
                raise ValueError(
                    f"Normalization parameter '{k}' must be finite positive, got {v}"
                )

        # 6. Training stats and convergence validation
        if not self.training_stats.get("converged", False):
            raise ValueError("Artifact rejected: training_stats.converged is False")
        final_loss = self.training_stats.get("final_loss")
        if (
            final_loss is None
            or isinstance(final_loss, bool)
            or not isinstance(final_loss, (int, float))
        ):
            raise ValueError(
                "Artifact rejected: training_stats.final_loss must be a numeric float"
            )
        final_loss_val = float(final_loss)
        if not math.isfinite(final_loss_val) or final_loss_val < 0.0:
            raise ValueError(
                f"Artifact rejected: training_stats.final_loss must be a finite non-negative float (>= 0.0), got {final_loss_val}"
            )

        # 7. Weights (coefs) and biases (intercepts) validation
        layer_dims = [input_dim] + list(self.hidden_layer_sizes) + [input_dim]
        expected_num_layers = len(layer_dims) - 1

        if len(self.coefs) != expected_num_layers:
            raise ValueError(
                f"coefs layer count mismatch: expected {expected_num_layers}, got {len(self.coefs)}"
            )
        if len(self.intercepts) != expected_num_layers:
            raise ValueError(
                f"intercepts layer count mismatch: expected {expected_num_layers}, got {len(self.intercepts)}"
            )

        for l_idx in range(expected_num_layers):
            in_d = layer_dims[l_idx]
            out_d = layer_dims[l_idx + 1]

            w_mat = self.coefs[l_idx]
            if len(w_mat) != in_d:
                raise ValueError(
                    f"coefs layer {l_idx} input dimension mismatch: expected {in_d}, got {len(w_mat)}"
                )
            for row in w_mat:
                if len(row) != out_d:
                    raise ValueError(
                        f"coefs layer {l_idx} output dimension mismatch: expected {out_d}, got {len(row)}"
                    )
                for val in row:
                    if not math.isfinite(val):
                        raise ValueError(
                            f"Non-finite weight detected in coefs layer {l_idx}: {val}"
                        )

            b_vec = self.intercepts[l_idx]
            if len(b_vec) != out_d:
                raise ValueError(
                    f"intercepts layer {l_idx} dimension mismatch: expected {out_d}, got {len(b_vec)}"
                )
            for val in b_vec:
                if not math.isfinite(val):
                    raise ValueError(
                        f"Non-finite bias detected in intercepts layer {l_idx}: {val}"
                    )

        return self

    def to_json(self) -> str:
        """Serialize artifact to deterministic JSON string."""
        return self.model_dump_json(indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> AutoencoderArtifact:
        """Deserialize and validate artifact from JSON string."""
        try:
            data = json.loads(json_str)
        except Exception as exc:
            raise AutoencoderArtifactError(
                f"Corrupt artifact: invalid JSON: {exc}"
            ) from exc

        if not isinstance(data, dict):
            raise AutoencoderArtifactError(
                "Corrupt artifact: root must be a JSON object"
            )

        schema_ver = data.get("artifact_schema_version")
        if schema_ver != SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION:
            raise AutoencoderArtifactError(
                f"Unsupported artifact schema version: expected '{SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION}', got '{schema_ver}'"
            )

        try:
            return cls.model_validate(data)
        except Exception as exc:
            raise AutoencoderArtifactError(
                f"Artifact contract validation failed: {exc}"
            ) from exc

    def save(self, file_path: str | Path) -> None:
        """Save artifact to a file path."""
        p = Path(file_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, file_path: str | Path) -> AutoencoderArtifact:
        """Load artifact from a file path."""
        p = Path(file_path)
        if not p.exists():
            raise AutoencoderArtifactError(
                f"Missing artifact file at path: {file_path}"
            )
        try:
            content = p.read_text(encoding="utf-8")
        except Exception as exc:
            raise AutoencoderArtifactError(
                f"Failed to read artifact file: {exc}"
            ) from exc
        return cls.from_json(content)


class AutoencoderScoreResult(BaseModel):
    """Detailed result of scoring a target observation window with Autoencoder."""

    model_config = ConfigDict(frozen=True)

    status: AutoencoderScoreStatus
    target_window_index: int
    target_timestamp: AwareDatetime
    raw_input_vector: dict[str, float] = Field(default_factory=dict)
    scaled_input_vector: dict[str, float] = Field(default_factory=dict)
    reconstructed_vector: dict[str, float] = Field(default_factory=dict)
    per_feature_errors: dict[str, float] = Field(default_factory=dict)
    raw_reconstruction_error: float | None = None
    anomaly_score: float | None = None
    signal: AnomalySignal | None = None
    history_window_count: int = 0
    actual_iterations: int = 0
    final_loss: float | None = None
    converged: bool = False
    error_message: str | None = None


class AutoencoderBaseline:
    """Bounded feed-forward Autoencoder anomaly detection baseline using scikit-learn MLPRegressor.

    Consumes deterministic Task 3.4 feature matrices, compresses through an explicit bottleneck,
    computes causal reconstruction errors, and emits schema-valid AnomalySignal outputs.
    """

    def __init__(self, config: AutoencoderBaselineConfig) -> None:
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
                    var_val = sum((v - mean_val) ** 2 for v in col_vals) / (
                        num_rows - 1
                    )
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

    def _fit_and_reconstruct(
        self,
        history_windows: list[FeatureWindow],
        target_window: FeatureWindow,
    ) -> tuple[
        dict[str, float],
        dict[str, float],
        dict[str, float],
        dict[str, float],
        float,
        float,
        ScalingParameters,
        int,
        int,
        float,
        bool,
        MLPRegressor,
        dict[str, Any],
    ]:
        """Fit scaler and Autoencoder on causal history, reconstruct target window, and compute normalized score."""
        # 1. Build training matrix X_train
        X_train: list[list[float]] = []
        for w in history_windows:
            row = [w.values[fname] for fname in self.config.feature_names]
            X_train.append(row)

        # 2. Build target row X_target
        X_target_row = [
            target_window.values[fname] for fname in self.config.feature_names
        ]

        # 3. Fit scaler only on training data
        scaler = self._fit_scaler(X_train)
        X_train_scaled = self._transform(X_train, scaler)
        X_target_scaled = self._transform([X_target_row], scaler)

        raw_vector = {
            fname: X_target_row[idx]
            for idx, fname in enumerate(self.config.feature_names)
        }
        scaled_vector = {
            fname: X_target_scaled[0][idx]
            for idx, fname in enumerate(self.config.feature_names)
        }

        # 4. Check for degenerate constant training series matching target
        is_constant_training = all(row == X_train[0] for row in X_train)
        is_target_matching_constant = is_constant_training and (
            X_target_row == X_train[0]
        )

        # 5. Fit Autoencoder (MLPRegressor)
        try:
            mlp = MLPRegressor(
                hidden_layer_sizes=self.config.hidden_layer_sizes,
                activation=self.config.activation,
                solver=self.config.solver,
                alpha=self.config.alpha,
                batch_size=self.config.batch_size,
                learning_rate_init=self.config.learning_rate_init,
                max_iter=self.config.max_iter,
                tol=self.config.tol,
                random_state=self.config.random_state,
                early_stopping=False,
            )

            with warnings.catch_warnings(record=True) as w_list:
                warnings.simplefilter("always", ConvergenceWarning)
                mlp.fit(X_train_scaled, X_train_scaled)

            has_convergence_warning = any(
                issubclass(warn.category, ConvergenceWarning) for warn in w_list
            )
            actual_iterations = int(mlp.n_iter_)
            final_loss = float(mlp.loss_)
            converged = (
                actual_iterations < self.config.max_iter
            ) and not has_convergence_warning

            if not math.isfinite(final_loss):
                raise AutoencoderFitError(
                    f"Autoencoder training produced non-finite loss: {final_loss}"
                )

            # Reconstruct target
            pred_target_scaled = mlp.predict(X_target_scaled)
            reconstructed_row = [float(v) for v in pred_target_scaled[0]]

            if not all(math.isfinite(v) for v in reconstructed_row):
                raise AutoencoderFitError(
                    "Autoencoder predicted non-finite reconstructed values"
                )

            reconstructed_vector = {
                fname: reconstructed_row[idx]
                for idx, fname in enumerate(self.config.feature_names)
            }

            # Calculate per-feature squared errors and aggregate MSE
            per_feature_errors: dict[str, float] = {}
            total_se = 0.0
            for idx, fname in enumerate(self.config.feature_names):
                diff = X_target_scaled[0][idx] - reconstructed_row[idx]
                se = diff**2
                per_feature_errors[fname] = se
                total_se += se

            raw_reconstruction_error = total_se / len(self.config.feature_names)

            # 6. Compute training reconstruction errors for empirical normalization reference
            train_preds_scaled = mlp.predict(X_train_scaled)
            train_mses: list[float] = []
            for r_idx in range(len(X_train_scaled)):
                t_row = X_train_scaled[r_idx]
                p_row = train_preds_scaled[r_idx]
                mse = sum(
                    (t_row[c] - p_row[c]) ** 2
                    for c in range(len(self.config.feature_names))
                ) / len(self.config.feature_names)
                train_mses.append(mse)

            s_min = min(train_mses)
            s_mean = sum(train_mses) / len(train_mses)
            var_s = (
                sum((v - s_mean) ** 2 for v in train_mses) / (len(train_mses) - 1)
                if len(train_mses) > 1
                else 0.0
            )
            s_std = math.sqrt(var_s)

            # Normalization formula
            if is_target_matching_constant:
                anomaly_score = 0.0
            elif raw_reconstruction_error <= s_min:
                anomaly_score = 0.0
            elif raw_reconstruction_error <= s_mean:
                denom = max(s_mean - s_min, 1e-4)
                anomaly_score = max(
                    0.0, min(0.30, 0.30 * (raw_reconstruction_error - s_min) / denom)
                )
            else:
                excess = raw_reconstruction_error - s_mean
                scale_factor = float(
                    self.config.score_normalization_parameters.get("scale_factor", 3.0)
                )
                epsilon_spread = float(
                    self.config.score_normalization_parameters.get(
                        "epsilon_spread", 1e-4
                    )
                )
                scale = max(s_std * scale_factor, epsilon_spread)
                anomaly_score = min(
                    1.0, 0.30 + 0.70 * (1.0 - math.exp(-excess / scale))
                )

            anomaly_score = max(0.0, min(1.0, float(anomaly_score)))

            training_stats = {
                "s_min": s_min,
                "s_mean": s_mean,
                "s_std": s_std,
                "history_count": len(history_windows),
                "actual_iterations": actual_iterations,
                "final_loss": final_loss,
                "converged": converged,
            }

            return (
                raw_vector,
                scaled_vector,
                reconstructed_vector,
                per_feature_errors,
                raw_reconstruction_error,
                anomaly_score,
                scaler,
                len(history_windows),
                actual_iterations,
                final_loss,
                converged,
                mlp,
                training_stats,
            )

        except Exception as exc:
            if isinstance(exc, AutoencoderFitError):
                raise
            raise AutoencoderFitError(f"Autoencoder training failed: {exc}") from exc

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
    ) -> AutoencoderScoreResult:
        """Score a single target window causally using Autoencoder fitted on preceding history."""
        windows = feature_result.windows
        if not windows:
            return AutoencoderScoreResult(
                status=AutoencoderScoreStatus.INVALID_INPUT,
                target_window_index=-1,
                target_timestamp=datetime.now(timezone.utc),
                error_message="FeatureExtractionResult contains no windows",
            )

        target_idx = (
            (len(windows) - 1) if target_window_index is None else target_window_index
        )
        if target_idx < 0 or target_idx >= len(windows):
            return AutoencoderScoreResult(
                status=AutoencoderScoreStatus.INVALID_INPUT,
                target_window_index=target_idx,
                target_timestamp=datetime.now(timezone.utc),
                error_message=f"target_window_index {target_idx} out of range [0, {len(windows) - 1}]",
            )

        target_window = windows[target_idx]

        # Check required features exist in target window
        missing_target_feats = [
            f
            for f in self.config.feature_names
            if f not in target_window.values or f in target_window.missing_features
        ]
        if missing_target_feats:
            return AutoencoderScoreResult(
                status=AutoencoderScoreStatus.MISSING_FEATURES,
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
                return AutoencoderScoreResult(
                    status=AutoencoderScoreStatus.INVALID_INPUT,
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
            return AutoencoderScoreResult(
                status=AutoencoderScoreStatus.INSUFFICIENT_HISTORY,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                raw_input_vector={
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
                reconstructed_vector,
                per_feature_errors,
                raw_reconstruction_error,
                anomaly_score,
                scaler,
                history_count,
                actual_iterations,
                final_loss,
                converged,
                _mlp,
                _stats,
            ) = self._fit_and_reconstruct(history_windows, target_window)
        except Exception as exc:
            return AutoencoderScoreResult(
                status=AutoencoderScoreStatus.FIT_FAILURE,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                raw_input_vector={
                    fname: target_window.values[fname]
                    for fname in self.config.feature_names
                },
                history_window_count=len(history_windows),
                error_message=str(exc),
            )

        # Non-convergence guard: suppress normal score and signal
        if not converged:
            return AutoencoderScoreResult(
                status=AutoencoderScoreStatus.NON_CONVERGENCE,
                target_window_index=target_idx,
                target_timestamp=target_window.observation_timestamp,
                raw_input_vector=raw_vector,
                scaled_input_vector=scaled_vector,
                history_window_count=history_count,
                actual_iterations=actual_iterations,
                final_loss=final_loss,
                converged=False,
                error_message=(
                    f"Bounded Autoencoder training reached maximum iterations ({self.config.max_iter}) "
                    "without convergence."
                ),
            )

        severity = self._determine_severity(anomaly_score)

        # Build structured evidence
        evidence = AnomalyEvidence(
            evidence_id=uuid.uuid4(),
            evidence_type="autoencoder_reconstruction_error",
            metric_or_feature="multivariate_features",
            observed_value=raw_reconstruction_error,
            expected_value=0.0,
            deviation=raw_reconstruction_error,
            details={
                "ordered_features": list(self.config.feature_names),
                "raw_input_vector": raw_vector,
                "scaled_input_vector": scaled_vector,
                "reconstructed_vector": reconstructed_vector,
                "per_feature_errors": per_feature_errors,
                "raw_reconstruction_error": raw_reconstruction_error,
                "reconstruction_error_method": self.config.reconstruction_error_method,
                "reconstruction_error_version": self.config.reconstruction_error_version,
                "architecture": {
                    "input_dim": len(self.config.feature_names),
                    "bottleneck_dim": self.config.bottleneck_dimension,
                    "hidden_layer_sizes": list(self.config.hidden_layer_sizes),
                    "activation": self.config.activation,
                    "solver": self.config.solver,
                },
                "scaling_method": self.config.scaling_method,
                "scaling_version": self.config.scaling_version,
                "scaling_parameters": {
                    "centers": scaler.centers,
                    "scales": scaler.scales,
                },
                "score_normalization_method": self.config.score_normalization_method,
                "score_normalization_version": self.config.score_normalization_version,
                "random_state": self.config.random_state,
                "effective_model_seed": self.config.random_state,
                "history_window_count": history_count,
                "training_start": history_windows[0].observation_timestamp.isoformat(),
                "training_end": history_windows[-1].observation_timestamp.isoformat(),
                "actual_iterations": actual_iterations,
                "final_loss": final_loss,
                "converged": converged,
                "framework_name": self.config.framework_name,
                "framework_version": sklearn.__version__,
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
            metric_or_feature="multivariate_features",
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
                "model": "autoencoder",
                "bottleneck_dim": str(self.config.bottleneck_dimension),
                "scaling_method": self.config.scaling_method,
            },
        )

        return AutoencoderScoreResult(
            status=AutoencoderScoreStatus.SUCCESS,
            target_window_index=target_idx,
            target_timestamp=target_window.observation_timestamp,
            raw_input_vector=raw_vector,
            scaled_input_vector=scaled_vector,
            reconstructed_vector=reconstructed_vector,
            per_feature_errors=per_feature_errors,
            raw_reconstruction_error=raw_reconstruction_error,
            anomaly_score=anomaly_score,
            signal=signal,
            history_window_count=history_count,
            actual_iterations=actual_iterations,
            final_loss=final_loss,
            converged=converged,
        )

    def score_series(
        self,
        feature_result: FeatureExtractionResult,
        start_index: int | None = None,
    ) -> list[AutoencoderScoreResult]:
        """Score sequential windows causally across a feature extraction result."""
        windows = feature_result.windows
        if not windows:
            return []

        start = self.config.min_training_windows if start_index is None else start_index
        if start < 0:
            start = 0

        results: list[AutoencoderScoreResult] = []
        for idx in range(start, len(windows)):
            score_res = self.score_target_window(
                feature_result, target_window_index=idx
            )
            results.append(score_res)

        return results

    def export_artifact(
        self,
        feature_result: FeatureExtractionResult,
        target_window_index: int | None = None,
    ) -> AutoencoderArtifact:
        """Fit model and export self-contained AutoencoderArtifact."""
        windows = feature_result.windows
        target_idx = (
            (len(windows) - 1) if target_window_index is None else target_window_index
        )
        target_window = windows[target_idx]

        history_windows = [
            w
            for i, w in enumerate(windows[:target_idx])
            if all(
                fname in w.values and fname not in w.missing_features
                for fname in self.config.feature_names
            )
        ]

        (
            _raw_v,
            _scaled_v,
            _recon_v,
            _per_feat_e,
            _raw_mse,
            _score,
            scaler,
            _hist_count,
            _iters,
            _loss,
            converged,
            mlp,
            stats,
        ) = self._fit_and_reconstruct(history_windows, target_window)

        if not converged:
            raise AutoencoderArtifactError(
                f"Cannot export artifact from non-converged Autoencoder model (reached {self.config.max_iter} iterations)."
            )

        # Extract weights and biases in plain float lists
        coefs = [[[float(w) for w in col] for col in mat] for mat in mlp.coefs_]
        intercepts = [[float(b) for b in arr] for arr in mlp.intercepts_]

        return AutoencoderArtifact(
            artifact_schema_version=SUPPORTED_AE_ARTIFACT_SCHEMA_VERSION,
            configuration_schema_version=self.config.schema_version,
            model_name=self.config.model_name,
            model_version=self.config.model_version,
            framework_name=self.config.framework_name,
            framework_version="1.9.1",
            feature_names=list(self.config.feature_names),
            input_dimension=len(self.config.feature_names),
            bottleneck_dimension=self.config.bottleneck_dimension,
            hidden_layer_sizes=list(self.config.hidden_layer_sizes),
            activation=self.config.activation,
            solver=self.config.solver,
            scaling_parameters=scaler,
            reconstruction_error_method=self.config.reconstruction_error_method,
            reconstruction_error_version=self.config.reconstruction_error_version,
            score_normalization_method=self.config.score_normalization_method,
            score_normalization_version=self.config.score_normalization_version,
            score_normalization_parameters=self.config.score_normalization_parameters,
            training_stats=stats,
            coefs=coefs,
            intercepts=intercepts,
        )


def run_autoencoder_baseline(
    feature_result: FeatureExtractionResult,
    config: AutoencoderBaselineConfig,
    target_window_index: int | None = None,
) -> AutoencoderScoreResult:
    """Convenience functional interface for executing Autoencoder baseline scoring."""
    baseline = AutoencoderBaseline(config=config)
    return baseline.score_target_window(
        feature_result, target_window_index=target_window_index
    )
