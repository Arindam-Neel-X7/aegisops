from __future__ import annotations

import json
import math
import re
from typing import Any
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.anomaly.errors import (
    AnomalyExperimentDeserializationError,
    AnomalyExperimentSerializationError,
)
from app.anomaly.models import MAX_UINT64

SUPPORTED_EXPERIMENT_SCHEMA_VERSION = "1.0"

FORBIDDEN_SECRET_PATTERNS = (
    "password",
    "secret",
    "api_key",
    "apikey",
    "access_token",
    "bearer ",
    "private_key",
    "client_secret",
)

FORBIDDEN_MEASURED_KEYS = (
    "measured_results",
    "measured_metrics",
    "predictions",
    "predicted_labels",
    "anomaly_scores",
    "anomaly_score",
    "detected_anomalies",
    "true_positives",
    "false_positives",
    "evaluation_scores",
    "evaluation_results",
    "f1_score_value",
    "accuracy_score",
)


def _validate_portable_path(path_str: str, field_name: str) -> str:
    """Validate that a path reference is portable and non-secret.

    Rejects machine-specific absolute paths (e.g. C:\\..., /home/..., /Users/...).
    Allows project-portable relative paths and clean logical URIs.
    """
    cleaned = path_str.strip()
    if not cleaned:
        raise ValueError(f"Field '{field_name}' cannot be empty or whitespace-only")

    # Reject Windows drive letters (e.g. C:\, D:/) and UNC network paths (\\server\share)
    if re.match(r"^[a-zA-Z]:[\\/]", cleaned) or cleaned.startswith(("\\\\", "//")):
        raise ValueError(
            f"Field '{field_name}' contains a machine-specific absolute path: '{cleaned}'. "
            "Must be a project-portable relative path or logical identifier."
        )

    # Reject POSIX absolute paths (/home, /Users, /var, /tmp, /etc, etc.)
    if cleaned.startswith("/"):
        raise ValueError(
            f"Field '{field_name}' contains an absolute path: '{cleaned}'. "
            "Must be a project-portable relative path or logical identifier."
        )

    # Check for secret patterns in path/URI
    lower = cleaned.lower()
    for pattern in FORBIDDEN_SECRET_PATTERNS:
        if pattern in lower:
            raise ValueError(
                f"Field '{field_name}' contains sensitive/secret credential pattern '{pattern}'"
            )

    return cleaned


def _check_no_secrets(obj: Any, context_name: str) -> None:
    """Recursively verify that an object or dictionary contains no secret patterns."""
    if isinstance(obj, str):
        lower = obj.lower()
        for pattern in FORBIDDEN_SECRET_PATTERNS:
            if pattern in lower:
                raise ValueError(
                    f"Sensitive/secret credential pattern '{pattern}' detected in '{context_name}'"
                )
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _check_no_secrets(k, f"{context_name}.key({k})")
            _check_no_secrets(v, f"{context_name}.value({k})")
    elif isinstance(obj, (list, tuple, set)):
        for item in obj:
            _check_no_secrets(item, context_name)


def _check_no_measured_results(obj: Any, context_name: str) -> None:
    """Recursively verify that no measured results or predictions are embedded in configuration."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            lower_k = str(k).lower()
            for forbidden in FORBIDDEN_MEASURED_KEYS:
                if forbidden == lower_k or forbidden in lower_k:
                    raise ValueError(
                        f"Configuration boundary violation: measured result field '{k}' "
                        f"is not permitted in '{context_name}'. Results must reside under research/results/."
                    )
            _check_no_measured_results(v, f"{context_name}.{k}")
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            _check_no_measured_results(item, context_name)


class DatasetReference(BaseModel):
    """Specification of target dataset and version for Phase 3 experiments."""

    model_config = ConfigDict(frozen=True)

    dataset_id: str = Field(min_length=1)
    dataset_version: str = Field(min_length=1)
    dataset_split: str = Field(default="evaluation", min_length=1)
    location_reference: str = Field(min_length=1)

    @field_validator("dataset_id", "dataset_version", "dataset_split")
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        _check_no_secrets(v, "dataset")
        return v.strip()

    @field_validator("location_reference")
    @classmethod
    def validate_location(cls, v: str) -> str:
        return _validate_portable_path(v, "location_reference")


class ScenarioReference(BaseModel):
    """Reference to the external scenario and configuration being analyzed."""

    model_config = ConfigDict(frozen=True)

    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    config_reference: str = Field(min_length=1)

    @field_validator("scenario_id", "scenario_version")
    @classmethod
    def validate_non_blank_scenario(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        _check_no_secrets(v, "scenario")
        return v.strip()

    @field_validator("config_reference")
    @classmethod
    def validate_scenario_config_ref(cls, v: str) -> str:
        return _validate_portable_path(v, "config_reference")


class ExperimentReproducibility(BaseModel):
    """Deterministic seed, code revision, and environment constraints for experiment reproducibility."""

    model_config = ConfigDict(frozen=True)

    seed: int = Field(ge=0, le=MAX_UINT64)
    code_revision: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    reproducibility_key: str | None = None
    conditions: dict[str, Any] = Field(default_factory=dict)

    @field_validator("code_revision", "environment")
    @classmethod
    def validate_repro_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        _check_no_secrets(v, "reproducibility")
        return v.strip()

    @field_validator("conditions")
    @classmethod
    def validate_conditions(cls, v: dict[str, Any]) -> dict[str, Any]:
        _check_no_secrets(v, "reproducibility.conditions")
        _check_no_measured_results(v, "reproducibility.conditions")
        return v


class FeatureWindowConfig(BaseModel):
    """Specification of feature extraction and observation windowing parameters."""

    model_config = ConfigDict(frozen=True)

    feature_config_id: str = Field(min_length=1)
    feature_names: list[str] = Field(min_length=1)
    window_size_seconds: float = Field(gt=0)
    step_size_seconds: float = Field(gt=0)
    aggregation_methods: list[str] = Field(
        default_factory=lambda: ["mean", "p95", "std"]
    )
    imputation_strategy: str = Field(default="forward_fill", min_length=1)

    @field_validator("feature_config_id", "imputation_strategy")
    @classmethod
    def validate_feature_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("feature_names", "aggregation_methods")
    @classmethod
    def validate_string_lists(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("List cannot be empty")
        cleaned = []
        for item in v:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("Item in list cannot be empty or whitespace-only")
            cleaned.append(item.strip())
        return cleaned

    @field_validator("window_size_seconds", "step_size_seconds")
    @classmethod
    def validate_finite_positive(cls, v: float) -> float:
        if not math.isfinite(v) or v <= 0:
            raise ValueError(f"Value must be a finite positive number, got {v}")
        return v

    @model_validator(mode="after")
    def validate_step_and_window(self) -> FeatureWindowConfig:
        if self.step_size_seconds > self.window_size_seconds:
            raise ValueError(
                f"step_size_seconds ({self.step_size_seconds}) cannot be greater than "
                f"window_size_seconds ({self.window_size_seconds})"
            )
        return self


class ModelSpecification(BaseModel):
    """Model architecture settings, hyperparameters, and calibration definitions."""

    model_config = ConfigDict(frozen=True)

    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    hyperparameters: dict[str, Any] = Field(default_factory=dict)
    calibration_method: str = Field(min_length=1)
    calibration_version: str = Field(min_length=1)
    calibration_parameters: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "model_name", "model_version", "calibration_method", "calibration_version"
    )
    @classmethod
    def validate_model_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        _check_no_secrets(v, "model")
        return v.strip()

    @field_validator("hyperparameters", "calibration_parameters")
    @classmethod
    def validate_parameters(cls, v: dict[str, Any]) -> dict[str, Any]:
        _check_no_secrets(v, "model.parameters")
        _check_no_measured_results(v, "model.parameters")
        return v


class EvaluationDeclaration(BaseModel):
    """Declaration of metrics to evaluate against ground truth (not measured results)."""

    model_config = ConfigDict(frozen=True)

    requested_metrics: list[str] = Field(min_length=1)
    target_service: str = Field(min_length=1)
    ground_truth_reference: str = Field(min_length=1)

    @field_validator("target_service")
    @classmethod
    def validate_target_service(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("target_service cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("ground_truth_reference")
    @classmethod
    def validate_ground_truth_ref(cls, v: str) -> str:
        return _validate_portable_path(v, "ground_truth_reference")

    @field_validator("requested_metrics")
    @classmethod
    def validate_requested_metrics(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("requested_metrics list cannot be empty")
        cleaned = []
        for m in v:
            if not isinstance(m, str) or not m.strip():
                raise ValueError("Metric name in requested_metrics cannot be empty")
            _check_no_secrets(m, "requested_metrics")
            # Verify metric declaration does not contain embedded numeric results e.g. "f1=0.9"
            if re.search(r"[=:]\s*\d", m):
                raise ValueError(
                    f"requested_metrics must declare metric names, not measured values: '{m}'"
                )
            cleaned.append(m.strip())
        return cleaned


class OutputDeclaration(BaseModel):
    """Specification of destination paths and requested output artifact types."""

    model_config = ConfigDict(frozen=True)

    artifact_root: str = Field(min_length=1)
    requested_outputs: list[str] = Field(min_length=1)
    save_intermediate_features: bool = False

    @field_validator("artifact_root")
    @classmethod
    def validate_artifact_root(cls, v: str) -> str:
        return _validate_portable_path(v, "artifact_root")

    @field_validator("requested_outputs")
    @classmethod
    def validate_requested_outputs(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("requested_outputs list cannot be empty")
        cleaned = []
        for out in v:
            if not isinstance(out, str) or not out.strip():
                raise ValueError("Output declaration cannot be empty")
            cleaned.append(out.strip())
        return cleaned


class AnomalyExperimentConfig(BaseModel):
    """Typed, versioned Phase 3 anomaly detection experiment configuration.

    Establishes a clean, reproducible boundary between experiment definition,
    dataset selection, model parameters, ground truth, and output destinations,
    strictly forbidding embedded measured results and machine-specific secrets/paths.
    """

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(
        default=SUPPORTED_EXPERIMENT_SCHEMA_VERSION, min_length=1
    )
    experiment_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str = Field(min_length=1)
    description: str = ""

    dataset: DatasetReference
    scenario: ScenarioReference
    reproducibility: ExperimentReproducibility
    feature_window: FeatureWindowConfig
    model: ModelSpecification
    evaluation: EvaluationDeclaration
    outputs: OutputDeclaration
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v != SUPPORTED_EXPERIMENT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported experiment schema_version: '{v}'. "
                f"Expected '{SUPPORTED_EXPERIMENT_SCHEMA_VERSION}'"
            )
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field 'name' cannot be empty or whitespace-only")
        _check_no_secrets(v, "name")
        return v.strip()

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, v: dict[str, Any]) -> dict[str, Any]:
        _check_no_secrets(v, "metadata")
        _check_no_measured_results(v, "metadata")
        return v


def serialize_experiment_config(config: AnomalyExperimentConfig) -> bytes:
    """Serialize an AnomalyExperimentConfig to UTF-8 encoded JSON bytes."""
    try:
        return config.model_dump_json(indent=2).encode("utf-8")
    except Exception as exc:
        raise AnomalyExperimentSerializationError(
            f"Failed to serialize AnomalyExperimentConfig {getattr(config, 'experiment_id', 'unknown')}: {exc}"
        ) from exc


def deserialize_experiment_config(
    raw: bytes | bytearray | memoryview,
) -> AnomalyExperimentConfig:
    """Deserialize UTF-8 encoded JSON bytes into an AnomalyExperimentConfig."""
    try:
        raw_bytes = bytes(raw)
        data: Any = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(data, dict):
            raise AnomalyExperimentDeserializationError(
                "Deserialized JSON root must be an object"
            )

        if "schema_version" in data:
            declared_version = data["schema_version"]
            if declared_version != SUPPORTED_EXPERIMENT_SCHEMA_VERSION:
                raise AnomalyExperimentDeserializationError(
                    f"Unsupported schema_version: '{declared_version}'. "
                    f"Expected '{SUPPORTED_EXPERIMENT_SCHEMA_VERSION}'"
                )

        return AnomalyExperimentConfig.model_validate(data)
    except AnomalyExperimentDeserializationError:
        raise
    except Exception as exc:
        raise AnomalyExperimentDeserializationError(
            f"Failed to deserialize AnomalyExperimentConfig: {exc}"
        ) from exc
