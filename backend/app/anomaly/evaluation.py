from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
import io
import json
import math
from pathlib import Path
import re
from typing import Any, Literal, NamedTuple, NoReturn, Sequence, cast
import uuid

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from app.anomaly.autoencoder import (
    AutoencoderBaseline,
    AutoencoderBaselineConfig,
    AutoencoderScoreStatus,
)
from app.anomaly.calibration import (
    CalibratedScoreResult,
    CalibratedScoreStatus,
    CalibrationConfig,
    CalibrationInput,
    CalibrationReferenceData,
    CalibrationReferenceSample,
    CommonScoreCalibrator,
    calibration_input_from_autoencoder,
    calibration_input_from_isolation_forest,
    calibration_input_from_prophet,
    create_default_calibration_config,
)
from app.anomaly.errors import (
    EvaluationArtifactError,
    EvaluationConfigurationError,
    EvaluationExecutionError,
)
from app.anomaly.experiment import (
    AnomalyExperimentConfig,
    DatasetReference,
    EvaluationDeclaration,
    ExperimentReproducibility,
    FeatureWindowConfig,
    ModelSpecification,
    OutputDeclaration,
    ScenarioReference,
    serialize_experiment_config,
)
from app.anomaly.features import (
    compute_quantile,
    extract_features,
)
from app.anomaly.isolation_forest import (
    IsolationForestBaseline,
    IsolationForestBaselineConfig,
    IsolationForestScoreStatus,
)
from app.anomaly.models import MAX_UINT64
from app.anomaly.models import AnomalySignal
from app.anomaly.observability import (
    FailureCategory,
    FailureRecord,
    ObservabilityCollector,
    ObservabilityContext,
    OperationalStage,
)
from app.anomaly.prophet import (
    ProphetBaseline,
    ProphetBaselineConfig,
    ProphetScoreStatus,
)
from app.simulator.runtime.runner import ScenarioRunner
from app.simulator.runtime.result import ScenarioRunResult
from app.telemetry.schemas import EventSeverity

SUPPORTED_EVALUATION_SCHEMA_VERSION = "1.0"
SUPPORTED_DECISION_POLICY_VERSION = "1.0.0"
SUPPORTED_LABEL_POLICY_VERSION = "1.0.0"
SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION = "1.0"
SUPPORTED_EVALUATION_METHOD_VERSION = "1.0.0"

TASK_3_9_NAMESPACE: uuid.UUID = uuid.uuid5(
    uuid.NAMESPACE_DNS, "aegisops:anomaly:phase3:task_3_9"
)

CANONICAL_SCENARIO_ORDER: tuple[str, ...] = (
    "cpu-saturation",
    "memory-exhaustion",
    "connection-exhaustion",
    "dependency-latency",
    "dependency-failure",
    "error-rate-spike",
    "traffic-surge",
    "bad-deployment-config",
)

CANONICAL_MODEL_ORDER: tuple[str, ...] = (
    "prophet",
    "isolation_forest",
    "autoencoder",
)


def derive_run_id(
    package_id: str,
    package_version: str,
    partition_role: str,
    scenario_id: str,
    seed: int,
) -> uuid.UUID:
    """Derive deterministic UUIDv5 for a scenario run in calibration or evaluation."""
    key = f"run:{package_id}:{package_version}:{partition_role}:{scenario_id}:{seed}"
    return uuid.uuid5(TASK_3_9_NAMESPACE, key)


def derive_calibration_sample_id(
    package_id: str,
    package_version: str,
    model_name: str,
    model_version: str,
    run_id: uuid.UUID,
    scenario_id: str,
    scenario_version: str,
    window_index: int,
    event_time: datetime | str,
    source_event_ids: Sequence[uuid.UUID] | None = None,
) -> uuid.UUID:
    """Derive deterministic UUIDv5 for a calibration reference sample."""
    if isinstance(event_time, datetime):
        event_time_str = event_time.isoformat()
    else:
        event_time_str = str(event_time).strip()

    if source_event_ids:
        sorted_ev_strs = sorted(str(eid) for eid in source_event_ids)
        source_events_key = ",".join(sorted_ev_strs)
    else:
        source_events_key = "none"

    key = (
        f"calib-sample:{package_id}:{package_version}:{model_name}:{model_version}:"
        f"{run_id}:{scenario_id}:{scenario_version}:{window_index}:{event_time_str}:{source_events_key}"
    )
    return uuid.uuid5(TASK_3_9_NAMESPACE, key)


def derive_outcome_id(
    package_id: str,
    package_version: str,
    model_name: str,
    run_id: uuid.UUID,
    scenario_id: str,
    unit_id: str,
) -> uuid.UUID:
    """Derive deterministic UUIDv5 for an evaluation outcome."""
    key = f"outcome:{package_id}:{package_version}:{model_name}:{run_id}:{scenario_id}:{unit_id}"
    return uuid.uuid5(TASK_3_9_NAMESPACE, key)


def compute_truth_interval(
    run_start_time: datetime,
    activation_time_seconds: float,
    recovery_time_seconds: float | None,
    total_duration_seconds: float,
) -> tuple[datetime, datetime]:
    """Compute exact half-open ground-truth interval [truth_start, truth_end)."""
    if run_start_time.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")
    if (
        isinstance(activation_time_seconds, bool)
        or not isinstance(activation_time_seconds, (int, float))
        or not math.isfinite(activation_time_seconds)
        or activation_time_seconds < 0
    ):
        raise ValueError(
            "activation_time_seconds must be numeric, finite, and non-negative"
        )
    if (
        isinstance(total_duration_seconds, bool)
        or not isinstance(total_duration_seconds, (int, float))
        or not math.isfinite(total_duration_seconds)
        or total_duration_seconds <= 0
    ):
        raise ValueError("total_duration_seconds must be numeric, finite, and positive")
    if activation_time_seconds >= total_duration_seconds:
        raise ValueError("activation must be strictly before total duration")

    truth_start = run_start_time + timedelta(seconds=activation_time_seconds)
    if recovery_time_seconds is not None:
        if (
            isinstance(recovery_time_seconds, bool)
            or not isinstance(recovery_time_seconds, (int, float))
            or not math.isfinite(recovery_time_seconds)
        ):
            raise ValueError("recovery_time_seconds must be numeric and finite")
        if recovery_time_seconds <= activation_time_seconds:
            raise ValueError("recovery must be strictly after activation")
        if recovery_time_seconds > total_duration_seconds:
            raise ValueError("recovery must not exceed total duration")
        truth_end = run_start_time + timedelta(seconds=recovery_time_seconds)
    else:
        truth_end = run_start_time + timedelta(seconds=total_duration_seconds)

    if truth_start >= truth_end:
        raise ValueError("truth_start must be strictly before truth_end")
    return truth_start, truth_end


def evaluate_ground_truth_label(
    window_start: datetime,
    truth_start: datetime,
    truth_end: datetime,
) -> bool:
    """Evaluate whether an observation window start falls within [truth_start, truth_end)."""
    if (
        window_start.tzinfo is None
        or truth_start.tzinfo is None
        or truth_end.tzinfo is None
    ):
        raise ValueError("all datetimes must be timezone-aware")
    if truth_start >= truth_end:
        raise ValueError("truth_start must be before truth_end")
    return truth_start <= window_start < truth_end


def compute_binary_decision(
    status: EvaluationOutcomeStatus,
    calibrated_score: float | None,
    decision_threshold: float,
) -> bool:
    """Evaluate whether an outcome is predicted positive under the operational decision threshold."""
    if isinstance(decision_threshold, bool) or not isinstance(
        decision_threshold, (int, float)
    ):
        raise EvaluationConfigurationError("decision_threshold must be numeric")
    if (
        not math.isfinite(decision_threshold)
        or decision_threshold < 0.0
        or decision_threshold > 1.0
    ):
        raise EvaluationConfigurationError(
            "decision_threshold must be a finite float in [0.0, 1.0]"
        )

    if status == EvaluationOutcomeStatus.SUCCESS:
        if calibrated_score is None:
            raise EvaluationConfigurationError(
                "Successful outcome must carry calibrated_score"
            )
        if isinstance(calibrated_score, bool) or not isinstance(
            calibrated_score, (int, float)
        ):
            raise EvaluationConfigurationError("calibrated_score must be numeric")
        if (
            not math.isfinite(calibrated_score)
            or calibrated_score < 0.0
            or calibrated_score > 1.0
        ):
            raise EvaluationConfigurationError(
                "calibrated_score must be a finite float in [0.0, 1.0]"
            )
        return float(calibrated_score) >= float(decision_threshold)
    else:
        if calibrated_score is not None:
            raise EvaluationConfigurationError(
                f"Non-success outcome ({status}) must have calibrated_score=None"
            )
        return False


def classify_confusion_category(
    ground_truth_positive: bool,
    predicted_positive: bool,
) -> tuple[bool, bool, bool, bool]:
    """Assign mutually exclusive confusion category: (is_tp, is_fp, is_tn, is_fn)."""
    is_tp = ground_truth_positive and predicted_positive
    is_fp = (not ground_truth_positive) and predicted_positive
    is_tn = (not ground_truth_positive) and (not predicted_positive)
    is_fn = ground_truth_positive and (not predicted_positive)
    return is_tp, is_fp, is_tn, is_fn


def _validate_safe_artifact_root(artifact_root: str) -> str:
    cleaned = artifact_root.strip().replace("\\", "/")
    if not cleaned:
        raise ValueError("artifact_root cannot be empty or whitespace-only")
    if cleaned in (".", "./"):
        raise ValueError(f"artifact_root cannot be dot-only: '{artifact_root}'")
    if re.match(r"^[a-zA-Z]:[\\/]", cleaned) or cleaned.startswith(("//", "\\\\", "/")):
        raise ValueError(
            f"Machine-specific absolute path rejected: '{cleaned}'. "
            "Must be a project-portable relative path."
        )
    segments = [s for s in cleaned.split("/") if s]
    if ".." in segments or "." in segments:
        raise ValueError(
            f"Path traversal or relative dot segment rejected: '{cleaned}'"
        )
    return "/".join(segments)


class EvaluationPackagePaths(BaseModel):
    """Resolved, validated directory and file paths for an evaluation package."""

    model_config = ConfigDict(frozen=True)

    normalized_root: str
    experiments_dir: Path
    raw_dir: Path
    processed_dir: Path
    figures_dir: Path
    reports_dir: Path

    def relative_artifact_path(self, subpath: str) -> str:
        """Return project-relative forward-slash path for an artifact."""
        norm_sub = subpath.strip().replace("\\", "/").lstrip("/")
        return f"{self.normalized_root}/{norm_sub}"


def resolve_evaluation_package_paths(
    base_dir: Path,
    artifact_root: str = "research",
) -> EvaluationPackagePaths:
    """Resolve and validate package subtrees beneath the base directory."""
    normalized_root = _validate_safe_artifact_root(artifact_root)
    resolved_base = base_dir.resolve()
    resolved_root = (resolved_base / normalized_root).resolve()

    # Verify containment
    if resolved_root != resolved_base and resolved_base not in resolved_root.parents:
        raise EvaluationArtifactError(
            f"Resolved artifact root '{resolved_root}' escapes base directory '{resolved_base}'"
        )

    return EvaluationPackagePaths(
        normalized_root=normalized_root,
        experiments_dir=resolved_root / "experiments" / "phase3" / "task_3_9",
        raw_dir=resolved_root / "results" / "raw" / "phase3" / "task_3_9",
        processed_dir=resolved_root / "results" / "processed" / "phase3" / "task_3_9",
        figures_dir=resolved_root / "results" / "figures" / "phase3" / "task_3_9",
        reports_dir=resolved_root / "reports" / "phase3" / "task_3_9",
    )


class EvaluationOutcomeStatus(StrEnum):
    """Detailed outcome classification for an individual evaluation unit."""

    SUCCESS = "success"
    INSUFFICIENT_DATA = "insufficient_data"
    MISSING_CALIBRATION = "missing_calibration"
    NON_CONVERGENCE = "non_convergence"
    FIT_FAILURE = "fit_failure"
    INVALID_INPUT = "invalid_input"
    MODEL_FAILURE = "model_failure"
    NOT_APPLICABLE = "not_applicable"


class EvaluationDecisionPolicy(BaseModel):
    """Typed, versioned binary decision rule for calibrated anomaly scores."""

    model_config = ConfigDict(frozen=True)

    policy_name: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    decision_threshold: float = Field(ge=0.0, le=1.0)
    description: str = Field(min_length=1)

    @field_validator("decision_threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ValueError("decision_threshold must be a numeric float")
        val = float(v)
        if not math.isfinite(val) or val < 0.0 or val > 1.0:
            raise ValueError(
                f"decision_threshold must be a finite float in [0.0, 1.0], got {val}"
            )
        return val

    @field_validator("policy_name", "policy_version", "description")
    @classmethod
    def validate_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @model_validator(mode="after")
    def validate_supported_version(self) -> EvaluationDecisionPolicy:
        if self.policy_version != SUPPORTED_DECISION_POLICY_VERSION:
            raise ValueError(
                f"Unsupported policy_version: '{self.policy_version}'. "
                f"Expected '{SUPPORTED_DECISION_POLICY_VERSION}'"
            )
        return self


def create_default_decision_policy(
    decision_threshold: float = 0.50,
) -> EvaluationDecisionPolicy:
    return EvaluationDecisionPolicy(
        policy_name="warning_threshold_binary_decision",
        policy_version=SUPPORTED_DECISION_POLICY_VERSION,
        decision_threshold=decision_threshold,
        description=(
            "Calibrated anomaly score >= decision_threshold (WARNING, ERROR, or CRITICAL) is comparison-positive"
        ),
    )


class EvaluationLabelPolicy(BaseModel):
    """Typed, versioned ground-truth alignment rule for simulation scenario windows."""

    model_config = ConfigDict(frozen=True)

    policy_name: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    overlap_rule: str = Field(min_length=1)
    description: str = Field(min_length=1)

    @field_validator("policy_name", "policy_version", "overlap_rule", "description")
    @classmethod
    def validate_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @model_validator(mode="after")
    def validate_supported_version(self) -> EvaluationLabelPolicy:
        if self.policy_version != SUPPORTED_LABEL_POLICY_VERSION:
            raise ValueError(
                f"Unsupported policy_version: '{self.policy_version}'. "
                f"Expected '{SUPPORTED_LABEL_POLICY_VERSION}'"
            )
        return self


def create_default_label_policy() -> EvaluationLabelPolicy:
    return EvaluationLabelPolicy(
        policy_name="half_open_active_interval",
        policy_version=SUPPORTED_LABEL_POLICY_VERSION,
        overlap_rule="window_start_in_active_interval",
        description=(
            "Ground truth positive when window_start falls in [truth_start, truth_end), "
            "pre-activation and post-recovery are ground truth negative"
        ),
    )


class EvaluationHarnessConfig(BaseModel):
    """Typed, immutable, versioned configuration for Phase 3 evaluation harness."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    package_version: str = Field(min_length=1)
    code_revision: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    scenario_ids: list[str] = Field(min_length=1)
    model_names: list[str] = Field(min_length=1)
    calibration_seed: int = Field(ge=0, le=MAX_UINT64)
    evaluation_seed: int = Field(ge=0, le=MAX_UINT64)
    calibration_partition_id: str = Field(min_length=1)
    evaluation_partition_id: str = Field(min_length=1)
    calibration_cutoff_seconds: float = Field(gt=0.0)

    feature_window: FeatureWindowConfig
    prophet_config: ProphetBaselineConfig
    isolation_forest_config: IsolationForestBaselineConfig
    autoencoder_config: AutoencoderBaselineConfig
    calibration_config: CalibrationConfig
    decision_policy: EvaluationDecisionPolicy
    label_policy: EvaluationLabelPolicy

    artifact_root: str = Field(min_length=1)
    requested_metrics: list[str] = Field(min_length=1)

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v != SUPPORTED_EVALUATION_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported evaluation schema_version: '{v}'. Expected '{SUPPORTED_EVALUATION_SCHEMA_VERSION}'"
            )
        return v

    @field_validator("artifact_root")
    @classmethod
    def validate_artifact_root_path(cls, v: str) -> str:
        return _validate_safe_artifact_root(v)

    @model_validator(mode="after")
    def validate_seeds_and_scenarios(self) -> EvaluationHarnessConfig:
        if self.calibration_seed == self.evaluation_seed:
            raise ValueError(
                f"Calibration seed ({self.calibration_seed}) and evaluation seed ({self.evaluation_seed}) "
                "must be strictly distinct to prevent data leakage."
            )
        if tuple(self.scenario_ids) != CANONICAL_SCENARIO_ORDER:
            raise ValueError(
                f"Scenario IDs must follow canonical catalogue order: {CANONICAL_SCENARIO_ORDER}, got {self.scenario_ids}"
            )
        if tuple(self.model_names) != CANONICAL_MODEL_ORDER:
            raise ValueError(
                f"Model names must follow canonical model order: {CANONICAL_MODEL_ORDER}, got {self.model_names}"
            )
        return self


def create_default_evaluation_harness_config(
    code_revision: str = "481652c2047a59e697c3008115da0faefcbbde01",
    calibration_seed: int = 41,
    evaluation_seed: int = 42,
    artifact_root: str = "research",
    decision_threshold: float = 0.50,
    calibration_partition_id: str | None = None,
    evaluation_partition_id: str | None = None,
) -> EvaluationHarnessConfig:
    return EvaluationHarnessConfig(
        schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
        package_id="phase3-task-3-9-comparison",
        package_version="1.0.0",
        code_revision=code_revision,
        environment="simulation",
        scenario_ids=list(CANONICAL_SCENARIO_ORDER),
        model_names=list(CANONICAL_MODEL_ORDER),
        calibration_seed=calibration_seed,
        evaluation_seed=evaluation_seed,
        calibration_partition_id=calibration_partition_id
        or f"partition-calibration-seed-{calibration_seed}",
        evaluation_partition_id=evaluation_partition_id
        or f"partition-evaluation-seed-{evaluation_seed}",
        calibration_cutoff_seconds=300.0,
        feature_window=FeatureWindowConfig(
            feature_config_id="feat-cfg-canonical-v1",
            feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
            window_size_seconds=1.0,
            step_size_seconds=1.0,
            aggregation_methods=["mean", "p95", "std"],
            imputation_strategy="forward_fill",
        ),
        prophet_config=ProphetBaselineConfig(
            schema_version="1.0",
            model_name="prophet",
            model_version="1.0.0",
            target_feature="http_request_duration_ms:mean",
            min_history_windows=5,
            uncertainty_samples=0,
            seed=42,
        ),
        isolation_forest_config=IsolationForestBaselineConfig(
            schema_version="1.0",
            model_name="isolation_forest",
            model_version="1.0.0",
            feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
            min_training_windows=5,
            n_estimators=50,
            random_state=42,
            n_jobs=1,
        ),
        autoencoder_config=AutoencoderBaselineConfig(
            schema_version="1.0",
            model_name="autoencoder",
            model_version="1.0.0",
            feature_names=["http_request_duration_ms:mean", "http_requests_total:mean"],
            bottleneck_dimension=1,
            min_training_windows=5,
            max_iter=200,
            random_state=42,
        ),
        calibration_config=create_default_calibration_config(),
        decision_policy=create_default_decision_policy(
            decision_threshold=decision_threshold
        ),
        label_policy=create_default_label_policy(),
        artifact_root=artifact_root,
        requested_metrics=[
            "precision",
            "recall",
            "f1",
            "false_positive_rate",
            "detection_latency",
        ],
    )


class ScenarioRunLineage(BaseModel):
    """Immutable runtime execution lineage for a scenario run."""

    model_config = ConfigDict(frozen=True)

    scenario_id: str
    run_id: uuid.UUID
    seed: int

    @field_validator("scenario_id", mode="before")
    @classmethod
    def validate_scenario_id(cls, v: Any) -> str:
        if type(v) is not str:  # noqa: E721
            raise PydanticCustomError(
                "lineage_scenario_id_type", "scenario_id must be a string"
            )
        if not v.strip():
            raise PydanticCustomError(
                "lineage_scenario_id_empty",
                "scenario_id cannot be empty or whitespace-only",
            )
        return cast(str, v)

    @field_validator("run_id", mode="before")
    @classmethod
    def validate_run_id(cls, v: Any) -> uuid.UUID:
        if type(v) is not uuid.UUID:  # noqa: E721
            raise PydanticCustomError(
                "lineage_run_id_type", "run_id must be a UUID instance"
            )
        return cast(uuid.UUID, v)

    @field_validator("seed", mode="before")
    @classmethod
    def validate_seed(cls, v: Any) -> int:
        if type(v) is bool or type(v) is not int:  # noqa: E721
            raise PydanticCustomError(
                "lineage_seed_type", "seed must be an exact integer"
            )
        if v < 0 or v > MAX_UINT64:
            raise PydanticCustomError(
                "lineage_seed_range",
                "seed must be within unsigned 64-bit range",
            )
        return cast(int, v)


class EvaluationRunMetadata(BaseModel):
    """Immutable typed metadata for full Phase 3 evaluation runtime execution."""

    model_config = ConfigDict(frozen=True)

    calibration_cutoff: AwareDatetime
    evaluation_start: AwareDatetime
    calibration_runs: tuple[ScenarioRunLineage, ...]
    evaluation_runs: tuple[ScenarioRunLineage, ...]

    @field_validator("calibration_cutoff", "evaluation_start", mode="before")
    @classmethod
    def validate_runtime_timestamp(cls, v: Any) -> datetime:
        if type(v) is not datetime:  # noqa: E721
            raise PydanticCustomError(
                "lineage_timestamp_naive",
                "runtime lineage timestamps must be timezone-aware UTC",
            )
        if v.tzinfo is None or v.utcoffset() != timedelta(0):
            raise PydanticCustomError(
                "lineage_timestamp_naive",
                "runtime lineage timestamps must be timezone-aware UTC",
            )
        return cast(datetime, v)

    @field_validator("calibration_runs", "evaluation_runs", mode="before")
    @classmethod
    def validate_run_collection(cls, v: Any) -> tuple[ScenarioRunLineage, ...]:
        if type(v) is not tuple or len(v) == 0:  # noqa: E721
            raise ValueError(
                "Run collection must be a non-empty tuple of ScenarioRunLineage"
            )
        for item in v:
            if type(item) is not ScenarioRunLineage:  # noqa: E721
                raise ValueError(
                    "Run collection items must be ScenarioRunLineage instances"
                )
        return v


LINEAGE_FIELD_ERROR_MAP: dict[tuple[str, tuple[str, ...]], str] = {
    ("lineage_scenario_id_type", ("scenario_id",)): (
        "lineage.scenario_id.type: scenario_id must be a string"
    ),
    ("lineage_scenario_id_empty", ("scenario_id",)): (
        "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only"
    ),
    ("lineage_seed_type", ("seed",)): (
        "lineage.seed.type: seed must be an exact integer"
    ),
    ("lineage_seed_range", ("seed",)): (
        "lineage.seed.range: seed must be within unsigned 64-bit range"
    ),
    ("lineage_run_id_type", ("run_id",)): (
        "lineage.run_id.type: run_id must be a UUID instance"
    ),
    ("lineage_timestamp_naive", ("calibration_cutoff",)): (
        "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
    ),
    ("lineage_timestamp_naive", ("evaluation_start",)): (
        "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC"
    ),
}

LINEAGE_FIELD_ERROR_PRIORITY: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("lineage_scenario_id_type", ("scenario_id",)),
    ("lineage_scenario_id_empty", ("scenario_id",)),
    ("lineage_seed_type", ("seed",)),
    ("lineage_seed_range", ("seed",)),
    ("lineage_run_id_type", ("run_id",)),
    ("lineage_timestamp_naive", ("calibration_cutoff",)),
    ("lineage_timestamp_naive", ("evaluation_start",)),
)


def _raise_lineage_translation_fallback() -> NoReturn:
    raise EvaluationExecutionError("Evaluation lineage validation failed") from None


def _get_sanitized_lineage_errors(exc: ValidationError) -> list[dict[str, Any]]:
    if type(exc) is not ValidationError:  # noqa: E721
        _raise_lineage_translation_fallback()
    try:
        errs = exc.errors(
            include_input=False,
            include_context=False,
            include_url=False,
        )
        if type(errs) is not list:  # noqa: E721
            _raise_lineage_translation_fallback()
        return cast(list[dict[str, Any]], errs)
    except Exception:
        _raise_lineage_translation_fallback()


def _translate_lineage_error_details(errors: object) -> NoReturn:
    if type(errors) is not list or len(errors) == 0:  # noqa: E721
        _raise_lineage_translation_fallback()

    present_keys: list[tuple[str, tuple[str, ...]]] = []

    for entry in errors:
        if type(entry) is not dict:  # noqa: E721
            _raise_lineage_translation_fallback()

        err_type = entry.get("type")
        if type(err_type) is not str:  # noqa: E721
            _raise_lineage_translation_fallback()

        loc = entry.get("loc")
        if type(loc) is not tuple or len(loc) == 0:  # noqa: E721
            _raise_lineage_translation_fallback()

        for comp in loc:
            if type(comp) is not str and (type(comp) is not int or type(comp) is bool):  # noqa: E721
                _raise_lineage_translation_fallback()

        key = (err_type, loc)
        if key not in LINEAGE_FIELD_ERROR_MAP:
            _raise_lineage_translation_fallback()

        present_keys.append(key)

    for priority_key in LINEAGE_FIELD_ERROR_PRIORITY:
        if priority_key in present_keys:
            raise EvaluationConfigurationError(
                LINEAGE_FIELD_ERROR_MAP[priority_key]
            ) from None

    _raise_lineage_translation_fallback()


def _translate_lineage_validation_error(exc: ValidationError) -> NoReturn:
    errors = _get_sanitized_lineage_errors(exc)
    _translate_lineage_error_details(errors)


def _build_scenario_run_lineage(
    scenario_id: Any,
    run_id: Any,
    seed: Any,
) -> ScenarioRunLineage:
    try:
        return ScenarioRunLineage(
            scenario_id=scenario_id,
            run_id=run_id,
            seed=seed,
        )
    except ValidationError as exc:
        _translate_lineage_validation_error(exc)


def _build_evaluation_run_metadata(
    calibration_cutoff: Any,
    evaluation_start: Any,
    calibration_runs: Any,
    evaluation_runs: Any,
) -> EvaluationRunMetadata:
    try:
        return EvaluationRunMetadata(
            calibration_cutoff=calibration_cutoff,
            evaluation_start=evaluation_start,
            calibration_runs=calibration_runs,
            evaluation_runs=evaluation_runs,
        )
    except ValidationError as exc:
        _translate_lineage_validation_error(exc)


def validate_evaluation_run_metadata(
    metadata: EvaluationRunMetadata,
    config: EvaluationHarnessConfig,
) -> None:
    """Validate cross-object invariants for runtime lineage partitions and temporal ordering."""
    configured_scenarios = list(config.scenario_ids)

    # 1. Calibration partition scenarios
    cal_scens = [r.scenario_id for r in metadata.calibration_runs]
    for s in cal_scens:
        if s not in configured_scenarios:
            raise EvaluationConfigurationError(
                "lineage.scenario.unknown: lineage contains an unconfigured scenario"
            ) from None
    if len(cal_scens) != len(set(cal_scens)):
        raise EvaluationConfigurationError(
            "lineage.scenario.duplicate: lineage contains duplicate scenario entries"
        ) from None
    if set(configured_scenarios) - set(cal_scens):
        raise EvaluationConfigurationError(
            "lineage.scenario.missing: configured scenario lineage is incomplete"
        ) from None
    if cal_scens != configured_scenarios:
        raise EvaluationConfigurationError(
            "lineage.scenario.order: lineage does not follow canonical scenario order"
        ) from None

    # 2. Evaluation partition scenarios
    eval_scens = [r.scenario_id for r in metadata.evaluation_runs]
    for s in eval_scens:
        if s not in configured_scenarios:
            raise EvaluationConfigurationError(
                "lineage.scenario.unknown: lineage contains an unconfigured scenario"
            ) from None
    if len(eval_scens) != len(set(eval_scens)):
        raise EvaluationConfigurationError(
            "lineage.scenario.duplicate: lineage contains duplicate scenario entries"
        ) from None
    if set(configured_scenarios) - set(eval_scens):
        raise EvaluationConfigurationError(
            "lineage.scenario.missing: configured scenario lineage is incomplete"
        ) from None
    if eval_scens != configured_scenarios:
        raise EvaluationConfigurationError(
            "lineage.scenario.order: lineage does not follow canonical scenario order"
        ) from None

    # 3. Seed validation
    for r in metadata.calibration_runs:
        if r.seed != config.calibration_seed:
            raise EvaluationConfigurationError(
                "lineage.seed.mismatch: lineage seed does not match configured partition seed"
            ) from None

    for r in metadata.evaluation_runs:
        if r.seed != config.evaluation_seed:
            raise EvaluationConfigurationError(
                "lineage.seed.mismatch: lineage seed does not match configured partition seed"
            ) from None

    # 4. Cross-partition run ID collision
    cal_run_ids = {r.run_id for r in metadata.calibration_runs}
    eval_run_ids = {r.run_id for r in metadata.evaluation_runs}
    if bool(cal_run_ids & eval_run_ids):
        raise EvaluationConfigurationError(
            "lineage.run_id.collision: calibration and evaluation partition run IDs overlap"
        ) from None

    # 5. Deterministic run ID derivation
    for r in metadata.calibration_runs:
        expected_run_id = derive_run_id(
            package_id=config.package_id,
            package_version=config.package_version,
            partition_role="calibration",
            scenario_id=r.scenario_id,
            seed=config.calibration_seed,
        )
        if r.run_id != expected_run_id:
            raise EvaluationConfigurationError(
                "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation"
            ) from None

    for r in metadata.evaluation_runs:
        expected_run_id = derive_run_id(
            package_id=config.package_id,
            package_version=config.package_version,
            partition_role="evaluation",
            scenario_id=r.scenario_id,
            seed=config.evaluation_seed,
        )
        if r.run_id != expected_run_id:
            raise EvaluationConfigurationError(
                "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation"
            ) from None

    # 6. Temporal ordering
    if metadata.evaluation_start <= metadata.calibration_cutoff:
        raise EvaluationConfigurationError(
            "lineage.temporal.order: evaluation start must be strictly after calibration cutoff"
        ) from None


class ArtifactManifestEntry(BaseModel):
    """Typed, immutable manifest entry for an evaluation artifact with verified digest."""

    model_config = ConfigDict(frozen=True)

    path: str = Field(min_length=1)
    media_type: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    record_count: int | None = None
    sha256: str = Field(min_length=64, max_length=64)

    @field_validator("path")
    @classmethod
    def validate_entry_path(cls, v: str) -> str:
        cleaned = v.strip().replace("\\", "/")
        if not cleaned:
            raise ValueError("path cannot be empty or whitespace-only")
        if re.match(r"^[a-zA-Z]:[\\/]", cleaned) or cleaned.startswith(
            ("//", "\\\\", "/")
        ):
            raise ValueError(f"path must be a project-relative path: '{cleaned}'")
        if ".." in cleaned.split("/"):
            raise ValueError(f"path cannot contain traversal ('..'): '{cleaned}'")
        return cleaned

    @field_validator("media_type")
    @classmethod
    def validate_media_type(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned or "/" not in cleaned:
            raise ValueError(f"Invalid media_type: '{cleaned}'")
        return cleaned

    @field_validator("record_count", mode="before")
    @classmethod
    def validate_record_count(cls, v: Any) -> int | None:
        if v is None:
            return None
        if isinstance(v, bool) or not isinstance(v, int) or v < 0:
            raise ValueError(f"record_count must be a non-negative integer, got {v}")
        return int(v)

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, v: str) -> str:
        cleaned = v.strip()
        if not re.fullmatch(r"^[0-9a-f]{64}$", cleaned):
            raise ValueError(
                f"sha256 must be a lowercase 64-character hex digest, got '{v}'"
            )
        return cleaned

    @model_validator(mode="after")
    def validate_schema_version(self) -> ArtifactManifestEntry:
        if self.schema_version != SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported artifact entry schema_version: '{self.schema_version}'. "
                f"Expected '{SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION}'"
            )
        return self


REQUIRED_RELATIVE_DIGESTED_ARTIFACT_PATHS: tuple[str, ...] = (
    "experiments/phase3/task_3_9/comparison_manifest.json",
    "experiments/phase3/task_3_9/prophet_experiment.json",
    "experiments/phase3/task_3_9/isolation_forest_experiment.json",
    "experiments/phase3/task_3_9/autoencoder_experiment.json",
    "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl",
    "results/processed/phase3/task_3_9/model_comparison.json",
    "results/processed/phase3/task_3_9/model_comparison.csv",
    "results/processed/phase3/task_3_9/scenario_comparison.csv",
    "results/figures/phase3/task_3_9/model_quality_metrics.svg",
    "results/figures/phase3/task_3_9/detection_latency_by_scenario.svg",
    "reports/phase3/task_3_9/evaluation_report.md",
)

EXPECTED_ARTIFACT_MEDIA_TYPES: dict[str, str] = {
    "experiments/phase3/task_3_9/comparison_manifest.json": "application/json",
    "experiments/phase3/task_3_9/prophet_experiment.json": "application/json",
    "experiments/phase3/task_3_9/isolation_forest_experiment.json": "application/json",
    "experiments/phase3/task_3_9/autoencoder_experiment.json": "application/json",
    "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl": "application/x-ndjson",
    "results/processed/phase3/task_3_9/model_comparison.json": "application/json",
    "results/processed/phase3/task_3_9/model_comparison.csv": "text/csv",
    "results/processed/phase3/task_3_9/scenario_comparison.csv": "text/csv",
    "results/figures/phase3/task_3_9/model_quality_metrics.svg": "image/svg+xml",
    "results/figures/phase3/task_3_9/detection_latency_by_scenario.svg": "image/svg+xml",
    "reports/phase3/task_3_9/evaluation_report.md": "text/markdown",
}

EXPECTED_RECORD_COUNT_REQUIRED_SUBPATHS: tuple[str, ...] = (
    "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl",
    "results/processed/phase3/task_3_9/model_comparison.json",
    "results/processed/phase3/task_3_9/model_comparison.csv",
    "results/processed/phase3/task_3_9/scenario_comparison.csv",
)


class EvaluationPackageManifest(BaseModel):
    """Package manifest declaring experiment conditions, artifacts, and SHA-256 digests."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    package_version: str = Field(min_length=1)
    code_revision: str = Field(min_length=1)
    created_at: AwareDatetime
    environment: str = Field(min_length=1)
    calibration_seed: int = Field(ge=0, le=MAX_UINT64)
    evaluation_seed: int = Field(ge=0, le=MAX_UINT64)
    decision_threshold: float = Field(ge=0.0, le=1.0)
    models: list[str] = Field(min_length=1)
    scenarios: list[str] = Field(min_length=1)
    artifact_manifest_path: str = Field(
        default="research/results/processed/phase3/task_3_9/artifact_manifest.json",
        min_length=1,
    )
    self_digest_policy: Literal["excluded_from_manifest_digest"] = (
        "excluded_from_manifest_digest"
    )
    artifacts: list[ArtifactManifestEntry] = Field(default_factory=list)

    @field_validator("schema_version")
    @classmethod
    def validate_manifest_schema_version(cls, v: str) -> str:
        if v != SUPPORTED_EVALUATION_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported manifest schema_version: '{v}'. Expected '{SUPPORTED_EVALUATION_SCHEMA_VERSION}'"
            )
        return v

    @field_validator("artifact_manifest_path")
    @classmethod
    def validate_manifest_path(cls, v: str) -> str:
        cleaned = v.strip().replace("\\", "/")
        if not cleaned:
            raise ValueError(
                "artifact_manifest_path cannot be empty or whitespace-only"
            )
        if re.match(r"^[a-zA-Z]:[\\/]", cleaned) or cleaned.startswith(
            ("//", "\\\\", "/")
        ):
            raise ValueError(
                f"artifact_manifest_path must be project-relative: '{cleaned}'"
            )
        if ".." in cleaned.split("/"):
            raise ValueError(
                f"artifact_manifest_path cannot contain traversal: '{cleaned}'"
            )
        return cleaned

    @field_validator("decision_threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if (
            isinstance(v, bool)
            or not isinstance(v, (int, float))
            or not math.isfinite(v)
            or v < 0.0
            or v > 1.0
        ):
            raise ValueError(
                f"decision_threshold must be a finite float in [0.0, 1.0], got {v}"
            )
        return float(v)

    @model_validator(mode="after")
    def validate_manifest_invariants(self) -> EvaluationPackageManifest:
        if self.calibration_seed == self.evaluation_seed:
            raise ValueError("calibration_seed and evaluation_seed must differ")
        if tuple(self.scenarios) != CANONICAL_SCENARIO_ORDER:
            raise ValueError(
                f"scenarios must match canonical order: {CANONICAL_SCENARIO_ORDER}"
            )
        if tuple(self.models) != CANONICAL_MODEL_ORDER:
            raise ValueError(
                f"models must match canonical order: {CANONICAL_MODEL_ORDER}"
            )
        if self.self_digest_policy != "excluded_from_manifest_digest":
            raise ValueError(
                f"self_digest_policy must be 'excluded_from_manifest_digest', got '{self.self_digest_policy}'"
            )

        suffix = "results/processed/phase3/task_3_9/artifact_manifest.json"
        if self.artifact_manifest_path.endswith("/" + suffix):
            root = self.artifact_manifest_path[: -len("/" + suffix)]
        elif self.artifact_manifest_path == suffix:
            root = ""
        else:
            raise ValueError(
                f"artifact_manifest_path '{self.artifact_manifest_path}' must end with '{suffix}'"
            )

        # Enforce exact 11 digested artifacts topology
        if len(self.artifacts) != len(REQUIRED_RELATIVE_DIGESTED_ARTIFACT_PATHS):
            raise ValueError(
                f"EvaluationPackageManifest requires exactly {len(REQUIRED_RELATIVE_DIGESTED_ARTIFACT_PATHS)} "
                f"digested artifacts, got {len(self.artifacts)}"
            )

        paths = [a.path for a in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("Duplicate artifact paths in manifest")

        if self.artifact_manifest_path in paths:
            raise ValueError(
                f"Self-digest violation: artifact_manifest_path '{self.artifact_manifest_path}' "
                "cannot be present in digested artifacts list."
            )

        expected_full_paths = {
            f"{root}/{sub}" if root else sub
            for sub in REQUIRED_RELATIVE_DIGESTED_ARTIFACT_PATHS
        }
        actual_paths = set(paths)
        if actual_paths != expected_full_paths:
            missing = expected_full_paths - actual_paths
            unexpected = actual_paths - expected_full_paths
            errs: list[str] = []
            if missing:
                errs.append(f"Missing required artifact(s): {sorted(missing)}")
            if unexpected:
                errs.append(f"Unexpected artifact(s): {sorted(unexpected)}")
            raise ValueError("; ".join(errs))

        for entry in self.artifacts:
            subpath = (
                entry.path[len(root) + 1 :]
                if root and entry.path.startswith(root + "/")
                else entry.path
            )
            expected_media = EXPECTED_ARTIFACT_MEDIA_TYPES.get(subpath)
            if entry.media_type != expected_media:
                raise ValueError(
                    f"Invalid media_type for '{entry.path}': expected '{expected_media}', got '{entry.media_type}'"
                )
            if subpath in EXPECTED_RECORD_COUNT_REQUIRED_SUBPATHS:
                if (
                    entry.record_count is None
                    or isinstance(entry.record_count, bool)
                    or not isinstance(entry.record_count, int)
                    or entry.record_count < 0
                ):
                    raise ValueError(
                        f"Artifact '{entry.path}' requires a non-negative integer record_count, got {entry.record_count}"
                    )
            else:
                if entry.record_count is not None:
                    raise ValueError(
                        f"Artifact '{entry.path}' must have record_count=None, got {entry.record_count}"
                    )

        return self


class EvaluationUnit(BaseModel):
    """Atomic evaluation window preserving ground truth alignment and window bounds."""

    model_config = ConfigDict(frozen=True)

    unit_id: str = Field(min_length=1)
    unit_index: int = Field(ge=0)
    scenario_id: str = Field(min_length=1)
    run_id: uuid.UUID
    window_start: AwareDatetime
    window_end: AwareDatetime
    observation_timestamp: AwareDatetime
    ground_truth_positive: bool
    truth_activation_time: AwareDatetime
    truth_recovery_time: AwareDatetime | None = None
    service: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_temporal_bounds(self) -> EvaluationUnit:
        if self.window_start >= self.window_end:
            raise ValueError(
                f"window_start ({self.window_start}) must be before window_end ({self.window_end})"
            )
        if (
            self.observation_timestamp < self.window_start
            or self.observation_timestamp > self.window_end
        ):
            raise ValueError(
                f"observation_timestamp ({self.observation_timestamp}) must be within window [{self.window_start}, {self.window_end}]"
            )
        if (
            self.truth_recovery_time is not None
            and self.truth_activation_time >= self.truth_recovery_time
        ):
            raise ValueError(
                f"truth_activation_time ({self.truth_activation_time}) must be before truth_recovery_time ({self.truth_recovery_time})"
            )
        return self


class EvaluationOutcome(BaseModel):
    """Deterministic evaluation outcome for a single (unit, model) decision."""

    model_config = ConfigDict(frozen=True)

    outcome_id: uuid.UUID
    unit_id: str = Field(min_length=1)
    unit_index: int = Field(ge=0)
    scenario_id: str = Field(min_length=1)
    run_id: uuid.UUID
    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    event_time: AwareDatetime
    ground_truth_positive: bool
    status: EvaluationOutcomeStatus
    raw_score: float | None = None
    raw_score_type: str | None = None
    baseline_normalized_score: float | None = None
    calibrated_score: float | None = None
    severity: EventSeverity | None = None
    predicted_positive: bool = False
    is_true_positive: bool = False
    is_false_positive: bool = False
    is_true_negative: bool = False
    is_false_negative: bool = False
    source_signal_id: uuid.UUID | None = None
    source_event_ids: list[uuid.UUID] = Field(default_factory=list)
    error_category: str | None = None
    error_message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("raw_score", mode="before")
    @classmethod
    def validate_raw_score(cls, v: Any) -> Any:
        if v is not None:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("raw_score must be numeric and not bool")
            if not math.isfinite(v):
                raise ValueError(f"raw_score must be a finite float, got {v}")
        return v

    @field_validator("baseline_normalized_score", "calibrated_score", mode="before")
    @classmethod
    def validate_bounded_scores(cls, v: Any) -> Any:
        if v is not None:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("score must be numeric and not bool")
            if not math.isfinite(v) or v < 0.0 or v > 1.0:
                raise ValueError(f"Score must be a finite float in [0.0, 1.0], got {v}")
        return v

    @model_validator(mode="after")
    def validate_confusion_consistency(self) -> EvaluationOutcome:
        active_counts = sum(
            [
                int(self.is_true_positive),
                int(self.is_false_positive),
                int(self.is_true_negative),
                int(self.is_false_negative),
            ]
        )
        if active_counts != 1:
            raise ValueError(
                f"Outcome must be assigned exactly one confusion category, got {active_counts}"
            )
        if self.status == EvaluationOutcomeStatus.SUCCESS:
            if self.calibrated_score is None:
                raise ValueError("Successful outcome must carry calibrated_score")
        else:
            if self.predicted_positive:
                raise ValueError(
                    f"Non-success outcome ({self.status}) cannot be predicted_positive"
                )
            if self.calibrated_score is not None:
                raise ValueError(
                    f"Non-success outcome ({self.status}) must have calibrated_score=None"
                )
        if self.ground_truth_positive:
            if self.predicted_positive and not self.is_true_positive:
                raise ValueError("Inconsistent TP classification")
            if not self.predicted_positive and not self.is_false_negative:
                raise ValueError("Inconsistent FN classification")
        else:
            if self.predicted_positive and not self.is_false_positive:
                raise ValueError("Inconsistent FP classification")
            if not self.predicted_positive and not self.is_true_negative:
                raise ValueError("Inconsistent TN classification")
        return self


class ConfusionCounts(BaseModel):
    """Integer confusion matrix totals and execution status counters."""

    model_config = ConfigDict(frozen=True)

    true_positives: int = Field(ge=0)
    false_positives: int = Field(ge=0)
    true_negatives: int = Field(ge=0)
    false_negatives: int = Field(ge=0)
    total_units: int = Field(ge=0)
    evaluable_units: int = Field(ge=0)
    success_units: int = Field(ge=0)
    insufficient_units: int = Field(ge=0)
    failure_units: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_totals(self) -> ConfusionCounts:
        matrix_sum = (
            self.true_positives
            + self.false_positives
            + self.true_negatives
            + self.false_negatives
        )
        if matrix_sum != self.total_units:
            raise ValueError(
                f"Sum of confusion matrix ({matrix_sum}) does not match total_units ({self.total_units})"
            )
        status_sum = self.success_units + self.insufficient_units + self.failure_units
        if status_sum != self.total_units:
            raise ValueError(
                f"Sum of status counters ({status_sum}) does not match total_units ({self.total_units})"
            )
        if self.evaluable_units != self.total_units:
            raise ValueError(
                f"evaluable_units ({self.evaluable_units}) does not match total_units ({self.total_units})"
            )
        return self


class MetricResult(BaseModel):
    """Typed evaluation metric result distinguishing defined values from undefined cases."""

    model_config = ConfigDict(frozen=True)

    metric_name: str = Field(min_length=1)
    value: float | None = None
    status: Literal["defined", "undefined"]
    reason: str | None = None
    numerator: float | None = None
    denominator: float | None = None

    @field_validator("value", "numerator", "denominator", mode="before")
    @classmethod
    def validate_numbers(cls, v: Any) -> Any:
        if v is not None:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("Metric numbers must be numeric and not bool")
            if not math.isfinite(v):
                raise ValueError(f"Metric numbers must be finite, got {v}")
        return v

    @field_validator("numerator", "denominator")
    @classmethod
    def validate_non_negative_parts(cls, v: float | None) -> float | None:
        if v is not None and v < 0.0:
            raise ValueError(
                f"Metric numerator and denominator must be non-negative, got {v}"
            )
        return v

    @model_validator(mode="after")
    def validate_defined_consistency(self) -> MetricResult:
        if self.status == "defined":
            if (
                self.value is None
                or not math.isfinite(self.value)
                or self.value < 0.0
                or self.value > 1.0
            ):
                raise ValueError(
                    f"Defined metric must carry a finite value in [0.0, 1.0], got {self.value}"
                )
            if self.reason is not None:
                raise ValueError(
                    f"Defined metric must have reason=None, got '{self.reason}'"
                )
            if self.numerator is None or self.denominator is None:
                raise ValueError("Defined metric must carry numerator and denominator")
            if self.denominator <= 0.0:
                raise ValueError(
                    f"Defined metric cannot have denominator <= 0, got {self.denominator}"
                )
            if (
                self.metric_name in ("precision", "recall", "f1", "false_positive_rate")
                and self.numerator > self.denominator
            ):
                raise ValueError(
                    f"Numerator cannot exceed denominator for {self.metric_name}"
                )
        elif self.status == "undefined":
            if self.value is not None:
                raise ValueError(
                    f"Undefined metric must have value None, got {self.value}"
                )
            if not self.reason or not self.reason.strip():
                raise ValueError(
                    "Undefined metric must disclose an explicit non-empty reason"
                )
        return self


class DetectionLatencyResult(BaseModel):
    """Event-time detection latency result for a single scenario and model."""

    model_config = ConfigDict(frozen=True)

    scenario_id: str = Field(min_length=1)
    model_name: str = Field(min_length=1)
    status: Literal["detected", "not_detected", "insufficient_data", "failed"]
    latency_seconds: float | None = None
    first_true_positive_time: AwareDatetime | None = None
    truth_activation_time: AwareDatetime | None = None
    details: str | None = None

    @field_validator("latency_seconds", mode="before")
    @classmethod
    def validate_latency(cls, v: Any) -> Any:
        if v is not None:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("latency_seconds must be numeric and not bool")
            if not math.isfinite(v) or v < 0.0:
                raise ValueError(
                    f"Detection latency must be finite and non-negative, got {v}"
                )
        return v

    @model_validator(mode="after")
    def validate_status_consistency(self) -> DetectionLatencyResult:
        if self.status == "detected":
            if (
                self.latency_seconds is None
                or self.first_true_positive_time is None
                or self.truth_activation_time is None
            ):
                raise ValueError(
                    "Detected latency must carry latency_seconds, first_true_positive_time, and truth_activation_time"
                )
            if self.first_true_positive_time < self.truth_activation_time:
                raise ValueError(
                    "first_true_positive_time must be >= truth_activation_time"
                )
            expected_latency = round(
                (
                    self.first_true_positive_time - self.truth_activation_time
                ).total_seconds(),
                6,
            )
            if abs(self.latency_seconds - expected_latency) > 1e-6:
                raise ValueError(
                    "latency_seconds must exactly match rounded event-time difference"
                )
        else:
            if (
                self.latency_seconds is not None
                or self.first_true_positive_time is not None
            ):
                raise ValueError(
                    f"Non-detected status '{self.status}' must have latency_seconds=None and first_true_positive_time=None"
                )
        return self


class ScoreDistributionSummary(BaseModel):
    """Descriptive statistics for a calibrated score distribution."""

    model_config = ConfigDict(frozen=True)

    distribution_name: str = Field(min_length=1)
    count: int = Field(ge=0)
    status: Literal["defined", "empty"]

    @field_validator("count", mode="before")
    @classmethod
    def validate_count(cls, v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("count must be integer and not bool")
        return v

    @field_validator("min", "max", "mean", "median", "p25", "p75", "p95", mode="before")
    @classmethod
    def validate_stats_before(cls, v: Any) -> Any:
        if v is not None:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("statistics must be numeric and not bool")
        return v

    min: float | None = None
    max: float | None = None
    mean: float | None = None
    median: float | None = None
    p25: float | None = None
    p75: float | None = None
    p95: float | None = None

    @model_validator(mode="after")
    def validate_distribution_invariants(self) -> ScoreDistributionSummary:
        if self.status == "empty":
            if self.count != 0:
                raise ValueError(
                    f"Empty distribution must have count 0, got {self.count}"
                )
            for field_name in ("min", "max", "mean", "median", "p25", "p75", "p95"):
                if getattr(self, field_name) is not None:
                    raise ValueError(f"Empty distribution must have {field_name}=None")
        else:
            if self.count <= 0:
                raise ValueError(
                    f"Defined distribution must have count > 0, got {self.count}"
                )
            for field_name in ("min", "max", "mean", "median", "p25", "p75", "p95"):
                v = getattr(self, field_name)
                if v is None or not math.isfinite(v):
                    raise ValueError(
                        f"Defined distribution must carry finite {field_name}"
                    )
                if v < 0.0 or v > 1.0:
                    raise ValueError(
                        f"Defined distribution statistic {field_name} must be in [0.0, 1.0]"
                    )
            assert (
                self.min is not None
                and self.p25 is not None
                and self.median is not None
            )
            assert (
                self.p75 is not None and self.p95 is not None and self.max is not None
            )
            if not (
                self.min <= self.p25 <= self.median <= self.p75 <= self.p95 <= self.max
            ):
                raise ValueError(
                    f"Distribution statistics must be ordered: min({self.min}) <= p25({self.p25}) <= "
                    f"median({self.median}) <= p75({self.p75}) <= p95({self.p95}) <= max({self.max})"
                )
            assert self.mean is not None
            if not (self.min <= self.mean <= self.max):
                raise ValueError("min <= mean <= max")
        return self


class ScenarioCoverageSummary(BaseModel):
    """Coverage breakdown of canonical scenario execution and evaluability."""

    model_config = ConfigDict(frozen=True)

    requested_count: int = 8
    requested_scenario_ids: list[str]
    executed_count: int = Field(ge=0)
    executed_scenario_ids: list[str]
    feature_ready_count: int = Field(ge=0)
    feature_ready_scenario_ids: list[str]
    scored_count: int = Field(ge=0)
    scored_scenario_ids: list[str]
    evaluable_count: int = Field(ge=0)
    evaluable_scenario_ids: list[str]
    detected_count: int = Field(ge=0)
    detected_scenario_ids: list[str]
    insufficient_count: int = Field(ge=0)
    insufficient_scenario_ids: list[str]
    failed_count: int = Field(ge=0)
    failed_scenario_ids: list[str]

    @field_validator(
        "requested_count",
        "executed_count",
        "feature_ready_count",
        "scored_count",
        "evaluable_count",
        "detected_count",
        "insufficient_count",
        "failed_count",
        mode="before",
    )
    @classmethod
    def validate_counts_before(cls, v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("counts must be integer and not bool")
        return v

    @model_validator(mode="after")
    def validate_coverage_invariants(self) -> ScenarioCoverageSummary:
        if self.requested_count != 8:
            raise ValueError("requested_count must be exactly 8")
        if tuple(self.requested_scenario_ids) != tuple(CANONICAL_SCENARIO_ORDER):
            raise ValueError(
                "requested_scenario_ids must exactly equal CANONICAL_SCENARIO_ORDER"
            )

        for name, count, id_list in [
            ("requested", self.requested_count, self.requested_scenario_ids),
            ("executed", self.executed_count, self.executed_scenario_ids),
            (
                "feature_ready",
                self.feature_ready_count,
                self.feature_ready_scenario_ids,
            ),
            ("scored", self.scored_count, self.scored_scenario_ids),
            ("evaluable", self.evaluable_count, self.evaluable_scenario_ids),
            ("detected", self.detected_count, self.detected_scenario_ids),
            ("insufficient", self.insufficient_count, self.insufficient_scenario_ids),
            ("failed", self.failed_count, self.failed_scenario_ids),
        ]:
            if count != len(id_list):
                raise ValueError(
                    f"{name}_count ({count}) does not match list length ({len(id_list)})"
                )
            if len(id_list) != len(set(id_list)):
                raise ValueError(f"{name}_scenario_ids contains duplicate entries")
            if name != "requested":
                for sid in id_list:
                    if sid not in CANONICAL_SCENARIO_ORDER:
                        raise ValueError(f"Unknown scenario_id: {sid}")
                # check canonical order
                expected_order = [
                    sid for sid in CANONICAL_SCENARIO_ORDER if sid in id_list
                ]
                if id_list != expected_order:
                    raise ValueError(f"{name}_scenario_ids must follow canonical order")

        exec_set = set(self.executed_scenario_ids)
        for sub in (
            "detected",
            "insufficient",
            "failed",
            "feature_ready",
            "scored",
            "evaluable",
        ):
            sub_set = set(getattr(self, f"{sub}_scenario_ids"))
            if not sub_set.issubset(exec_set):
                raise ValueError(
                    f"{sub}_scenario_ids must be a subset of executed_scenario_ids"
                )
        return self


class LatencySummary(BaseModel):
    """Aggregate statistics across detected scenario latencies."""

    model_config = ConfigDict(frozen=True)

    total_scenario_count: int = Field(ge=0)
    contributing_scenario_count: int = Field(ge=0)
    missing_detection_count: int = Field(ge=0)
    min_seconds: float | None = None
    mean_seconds: float | None = None
    median_seconds: float | None = None
    p95_seconds: float | None = None
    max_seconds: float | None = None

    @field_validator(
        "contributing_scenario_count",
        "missing_detection_count",
        "total_scenario_count",
        mode="before",
    )
    @classmethod
    def validate_counts(cls, v: Any) -> int:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError("counts must be non-negative integers and reject booleans")
        return int(v)

    @field_validator(
        "min_seconds",
        "mean_seconds",
        "median_seconds",
        "p95_seconds",
        "max_seconds",
        mode="before",
    )
    @classmethod
    def validate_stats(cls, v: Any) -> Any:
        if v is not None:
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError("statistics must be numeric and not bool")
            if not math.isfinite(v) or v < 0.0:
                raise ValueError(
                    "every statistic is finite and non-negative when present"
                )
        return v

    @model_validator(mode="after")
    def validate_invariants(self) -> LatencySummary:
        if (
            self.contributing_scenario_count + self.missing_detection_count
            != self.total_scenario_count
        ):
            raise ValueError(
                "contributing_scenario_count + missing_detection_count must equal total_scenario_count"
            )

        stat_fields = [
            self.min_seconds,
            self.mean_seconds,
            self.median_seconds,
            self.p95_seconds,
            self.max_seconds,
        ]
        if self.contributing_scenario_count == 0:
            if any(s is not None for s in stat_fields):
                raise ValueError(
                    "contributing_scenario_count == 0 requires every statistic None"
                )
        else:
            if any(s is None for s in stat_fields):
                raise ValueError(
                    "contributing_scenario_count > 0 requires every statistic present"
                )

            assert self.min_seconds is not None and self.median_seconds is not None
            assert (
                self.p95_seconds is not None
                and self.max_seconds is not None
                and self.mean_seconds is not None
            )
            if not (
                self.min_seconds
                <= self.median_seconds
                <= self.p95_seconds
                <= self.max_seconds
            ):
                raise ValueError("min <= median <= p95 <= max")
            if not (self.min_seconds <= self.mean_seconds <= self.max_seconds):
                raise ValueError("min <= mean <= max")
        return self


class ScenarioModelEvaluation(BaseModel):
    """Evaluation metrics and score distribution for one (scenario, model) pair."""

    model_config = ConfigDict(frozen=True)

    scenario_id: str
    model_name: str
    model_version: str
    confusion_counts: ConfusionCounts
    precision: MetricResult
    recall: MetricResult
    f1: MetricResult
    false_positive_rate: MetricResult
    detection_latency: DetectionLatencyResult
    truth_positive_distribution: ScoreDistributionSummary
    truth_negative_distribution: ScoreDistributionSummary
    predicted_positive_distribution: ScoreDistributionSummary
    predicted_negative_distribution: ScoreDistributionSummary
    status: str


class ModelComparisonSummary(BaseModel):
    """Complete aggregated quality, latency, and coverage summary for one model baseline."""

    model_config = ConfigDict(frozen=True)

    model_name: str
    model_version: str
    micro_confusion_counts: ConfusionCounts
    micro_precision: MetricResult
    micro_recall: MetricResult
    micro_f1: MetricResult
    micro_false_positive_rate: MetricResult
    macro_precision: MetricResult
    macro_recall: MetricResult
    macro_f1: MetricResult
    macro_false_positive_rate: MetricResult
    macro_contributing_scenario_count: int = Field(ge=0)
    latency_summary: LatencySummary
    scenario_coverage: ScenarioCoverageSummary
    truth_positive_distribution: ScoreDistributionSummary
    truth_negative_distribution: ScoreDistributionSummary
    predicted_positive_distribution: ScoreDistributionSummary
    predicted_negative_distribution: ScoreDistributionSummary
    scenario_evaluations: list[ScenarioModelEvaluation]


def calculate_confusion_counts(
    outcomes: Sequence[EvaluationOutcome],
) -> ConfusionCounts:
    tp = sum(1 for o in outcomes if o.is_true_positive)
    fp = sum(1 for o in outcomes if o.is_false_positive)
    tn = sum(1 for o in outcomes if o.is_true_negative)
    fn = sum(1 for o in outcomes if o.is_false_negative)
    tot = len(outcomes)
    succ = sum(1 for o in outcomes if o.status == EvaluationOutcomeStatus.SUCCESS)
    insuf = sum(
        1
        for o in outcomes
        if o.status
        in (
            EvaluationOutcomeStatus.INSUFFICIENT_DATA,
            EvaluationOutcomeStatus.NOT_APPLICABLE,
        )
    )
    fail = sum(
        1
        for o in outcomes
        if o.status
        in (
            EvaluationOutcomeStatus.FIT_FAILURE,
            EvaluationOutcomeStatus.MODEL_FAILURE,
            EvaluationOutcomeStatus.NON_CONVERGENCE,
            EvaluationOutcomeStatus.INVALID_INPUT,
            EvaluationOutcomeStatus.MISSING_CALIBRATION,
        )
    )
    return ConfusionCounts(
        true_positives=tp,
        false_positives=fp,
        true_negatives=tn,
        false_negatives=fn,
        total_units=tot,
        evaluable_units=tot,
        success_units=succ,
        insufficient_units=insuf,
        failure_units=fail,
    )


def calculate_precision(tp: int, fp: int) -> MetricResult:
    if isinstance(tp, bool) or not isinstance(tp, int) or tp < 0:
        raise ValueError("tp must be a non-negative integer")
    if isinstance(fp, bool) or not isinstance(fp, int) or fp < 0:
        raise ValueError("fp must be a non-negative integer")
    denom = tp + fp
    if denom == 0:
        return MetricResult(
            metric_name="precision",
            value=None,
            status="undefined",
            reason="zero_denominator: TP + FP == 0",
            numerator=float(tp),
            denominator=0.0,
        )
    val = round(tp / denom, 6)
    return MetricResult(
        metric_name="precision",
        value=val,
        status="defined",
        numerator=float(tp),
        denominator=float(denom),
    )


def calculate_recall(tp: int, fn: int) -> MetricResult:
    if isinstance(tp, bool) or not isinstance(tp, int) or tp < 0:
        raise ValueError("tp must be a non-negative integer")
    if isinstance(fn, bool) or not isinstance(fn, int) or fn < 0:
        raise ValueError("fn must be a non-negative integer")
    denom = tp + fn
    if denom == 0:
        return MetricResult(
            metric_name="recall",
            value=None,
            status="undefined",
            reason="zero_denominator: TP + FN == 0",
            numerator=float(tp),
            denominator=0.0,
        )
    val = round(tp / denom, 6)
    return MetricResult(
        metric_name="recall",
        value=val,
        status="defined",
        numerator=float(tp),
        denominator=float(denom),
    )


def calculate_f1(precision: MetricResult, recall: MetricResult) -> MetricResult:
    if precision.metric_name != "precision" or recall.metric_name != "recall":
        raise ValueError(
            "calculate_f1 requires correctly named precision and recall MetricResult inputs"
        )
    if (
        precision.status == "undefined"
        or recall.status == "undefined"
        or precision.value is None
        or recall.value is None
    ):
        return MetricResult(
            metric_name="f1",
            value=None,
            status="undefined",
            reason="undefined_component: precision or recall is undefined",
        )
    denom = precision.value + recall.value
    if denom == 0.0:
        return MetricResult(
            metric_name="f1",
            value=None,
            status="undefined",
            reason="zero_denominator: precision + recall == 0.0",
            numerator=0.0,
            denominator=0.0,
        )
    val = round(2 * precision.value * recall.value / denom, 6)
    return MetricResult(
        metric_name="f1",
        value=val,
        status="defined",
        numerator=round(2 * precision.value * recall.value, 6),
        denominator=round(denom, 6),
    )


def calculate_false_positive_rate(fp: int, tn: int) -> MetricResult:
    if isinstance(fp, bool) or not isinstance(fp, int) or fp < 0:
        raise ValueError("fp must be a non-negative integer")
    if isinstance(tn, bool) or not isinstance(tn, int) or tn < 0:
        raise ValueError("tn must be a non-negative integer")
    denom = fp + tn
    if denom == 0:
        return MetricResult(
            metric_name="false_positive_rate",
            value=None,
            status="undefined",
            reason="zero_denominator: FP + TN == 0",
            numerator=float(fp),
            denominator=0.0,
        )
    val = round(fp / denom, 6)
    return MetricResult(
        metric_name="false_positive_rate",
        value=val,
        status="defined",
        numerator=float(fp),
        denominator=float(denom),
    )


def calculate_score_distribution(
    scores: Sequence[float], name: str
) -> ScoreDistributionSummary:
    for s in scores:
        if isinstance(s, bool) or not isinstance(s, (int, float)):
            raise ValueError(
                f"Reject boolean or non-numeric value in score distribution '{name}'"
            )
        if not math.isfinite(s):
            raise ValueError(
                f"Non-finite value '{s}' rejected in score distribution '{name}'"
            )
        if s < 0.0 or s > 1.0:
            raise ValueError(
                f"Values must be in [0.0, 1.0] for score distribution '{name}'"
            )

    valid_scores = [float(s) for s in scores]
    if not valid_scores:
        return ScoreDistributionSummary(
            distribution_name=name,
            count=0,
            status="empty",
        )
    sorted_scores = sorted(valid_scores)
    return ScoreDistributionSummary(
        distribution_name=name,
        count=len(sorted_scores),
        status="defined",
        min=round(sorted_scores[0], 6),
        max=round(sorted_scores[-1], 6),
        mean=round(sum(sorted_scores) / len(sorted_scores), 6),
        median=round(compute_quantile(sorted_scores, 0.50), 6),
        p25=round(compute_quantile(sorted_scores, 0.25), 6),
        p75=round(compute_quantile(sorted_scores, 0.75), 6),
        p95=round(compute_quantile(sorted_scores, 0.95), 6),
    )


def calculate_detection_latency(
    outcomes: Sequence[EvaluationOutcome],
    truth_activation_time: datetime,
    scenario_id: str,
    model_name: str,
) -> DetectionLatencyResult:
    tp_outcomes = [o for o in outcomes if o.is_true_positive]
    if not tp_outcomes:
        has_success = any(o.status == EvaluationOutcomeStatus.SUCCESS for o in outcomes)
        status: Literal["detected", "not_detected", "insufficient_data", "failed"]
        if not outcomes:
            status = "insufficient_data"
        elif not has_success:
            status = "failed"
        else:
            status = "not_detected"
        return DetectionLatencyResult(
            scenario_id=scenario_id,
            model_name=model_name,
            status=status,
            latency_seconds=None,
            first_true_positive_time=None,
            truth_activation_time=truth_activation_time,
            details="No true positive detections recorded during active scenario window",
        )

    # Sort by event_time to find first true positive
    sorted_tp = sorted(tp_outcomes, key=lambda o: o.event_time)
    first_tp = sorted_tp[0]

    if first_tp.event_time < truth_activation_time:
        raise EvaluationExecutionError(
            f"True positive event_time {first_tp.event_time} precedes activation_time {truth_activation_time}"
        )

    lat_sec = round((first_tp.event_time - truth_activation_time).total_seconds(), 6)

    return DetectionLatencyResult(
        scenario_id=scenario_id,
        model_name=model_name,
        status="detected",
        latency_seconds=lat_sec,
        first_true_positive_time=first_tp.event_time,
        truth_activation_time=truth_activation_time,
        details=f"First true positive detected at {first_tp.event_time.isoformat()}",
    )


class Phase3EvaluationHarness:
    """Deterministic orchestrator for Phase 3 baseline evaluation across canonical scenarios."""

    def __init__(
        self,
        config: EvaluationHarnessConfig,
        observability: ObservabilityCollector | None = None,
    ) -> None:
        self.config = config
        self.observability = observability or ObservabilityCollector()

    def _observability_context(
        self,
        run_result: ScenarioRunResult | None = None,
        *,
        model_name: str | None = None,
        model_version: str | None = None,
        signal_id: uuid.UUID | None = None,
    ) -> ObservabilityContext:
        return ObservabilityContext(
            run_id=run_result.run_id if run_result else None,
            scenario_id=run_result.scenario_id if run_result else None,
            scenario_version=run_result.scenario_version if run_result else None,
            seed=run_result.seed if run_result else None,
            reproducibility_key=(
                run_result.reproducibility_key if run_result else None
            ),
            package_id=self.config.package_id,
            package_version=self.config.package_version,
            code_revision=self.config.code_revision,
            model_name=model_name,
            model_version=model_version,
            signal_id=signal_id,
        )

    def _record_semantic_failure(
        self,
        *,
        category: FailureCategory,
        failure_type: str,
        failure_message: str,
        context: ObservabilityContext,
    ) -> None:
        self.observability.record_failure(
            FailureRecord(
                stage=OperationalStage.EVALUATION,
                category=category,
                failure_type=failure_type,
                failure_message=failure_message,
                observed_at=datetime.now(timezone.utc),
                context=context,
            )
        )

    async def run_evaluation(
        self,
    ) -> tuple[
        list[EvaluationOutcome],
        list[ModelComparisonSummary],
        EvaluationRunMetadata,
    ]:
        """Execute full evaluation pipeline: simulator runs -> features -> models -> calibration -> metrics."""
        runner = ScenarioRunner()

        # 1. Calibration Phase (Seed 41)
        t_calib_start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        calib_cutoff = t_calib_start + timedelta(
            seconds=self.config.calibration_cutoff_seconds
        )

        prophet_calib_samples: list[CalibrationReferenceSample] = []
        iforest_calib_samples: list[CalibrationReferenceSample] = []
        ae_calib_samples: list[CalibrationReferenceSample] = []

        calibration_runs_records: list[ScenarioRunLineage] = []

        for scen_id in self.config.scenario_ids:
            run_id = derive_run_id(
                package_id=self.config.package_id,
                package_version=self.config.package_version,
                partition_role="calibration",
                scenario_id=scen_id,
                seed=self.config.calibration_seed,
            )
            calib_run_result = await runner.run(
                scenario=scen_id,
                run_id=run_id,
                run_start_time=t_calib_start,
                seed=self.config.calibration_seed,
            )
            calibration_runs_records.append(
                _build_scenario_run_lineage(
                    scenario_id=scen_id,
                    run_id=run_id,
                    seed=self.config.calibration_seed,
                )
            )

            # Extract features for calibration
            calib_context = self._observability_context(calib_run_result)
            calib_feat_res = self.observability.sync_measured_call(
                OperationalStage.FEATURE_EXTRACTION,
                FailureCategory.FEATURE_EXTRACTION_FAILURE,
                calib_context,
                action=lambda: extract_features(
                    calib_run_result,
                    config=self.config.feature_window,
                ),
            )

            # Score Prophet on calibration
            p_baseline = ProphetBaseline(config=self.config.prophet_config)
            p_context = self._observability_context(
                calib_run_result,
                model_name="prophet",
                model_version=self.config.prophet_config.model_version,
            )
            p_scores = self.observability.sync_measured_call(
                OperationalStage.MODEL_EXECUTION,
                FailureCategory.MODEL_FAILURE,
                p_context,
                action=lambda: p_baseline.score_series(calib_feat_res, start_index=0),
            )
            for idx, p_res in enumerate(p_scores):
                if (
                    p_res.status == ProphetScoreStatus.SUCCESS
                    and p_res.signal
                    and p_res.anomaly_score is not None
                ):
                    sample_id = derive_calibration_sample_id(
                        package_id=self.config.package_id,
                        package_version=self.config.package_version,
                        model_name="prophet",
                        model_version=self.config.prophet_config.model_version,
                        run_id=run_id,
                        scenario_id=scen_id,
                        scenario_version=calib_run_result.scenario_version,
                        window_index=idx,
                        event_time=p_res.signal.event_time,
                        source_event_ids=p_res.signal.source_event_ids,
                    )
                    prophet_calib_samples.append(
                        CalibrationReferenceSample(
                            sample_id=sample_id,
                            model_name="prophet",
                            model_version=self.config.prophet_config.model_version,
                            baseline_normalized_score=p_res.anomaly_score,
                            event_time=p_res.signal.event_time,
                            run_id=run_id,
                            scenario_id=scen_id,
                            scenario_version=calib_run_result.scenario_version,
                            seed=self.config.calibration_seed,
                            reproducibility_key=calib_run_result.reproducibility_key,
                            partition_role="calibration",
                        )
                    )

            # Score Isolation Forest on calibration
            if_baseline = IsolationForestBaseline(
                config=self.config.isolation_forest_config
            )
            if_context = self._observability_context(
                calib_run_result,
                model_name="isolation_forest",
                model_version=self.config.isolation_forest_config.model_version,
            )
            if_scores = self.observability.sync_measured_call(
                OperationalStage.MODEL_EXECUTION,
                FailureCategory.MODEL_FAILURE,
                if_context,
                action=lambda: if_baseline.score_series(calib_feat_res, start_index=0),
            )
            for idx, if_res in enumerate(if_scores):
                if (
                    if_res.status == IsolationForestScoreStatus.SUCCESS
                    and if_res.signal
                    and if_res.anomaly_score is not None
                ):
                    sample_id = derive_calibration_sample_id(
                        package_id=self.config.package_id,
                        package_version=self.config.package_version,
                        model_name="isolation_forest",
                        model_version=self.config.isolation_forest_config.model_version,
                        run_id=run_id,
                        scenario_id=scen_id,
                        scenario_version=calib_run_result.scenario_version,
                        window_index=idx,
                        event_time=if_res.signal.event_time,
                        source_event_ids=if_res.signal.source_event_ids,
                    )
                    iforest_calib_samples.append(
                        CalibrationReferenceSample(
                            sample_id=sample_id,
                            model_name="isolation_forest",
                            model_version=self.config.isolation_forest_config.model_version,
                            baseline_normalized_score=if_res.anomaly_score,
                            event_time=if_res.signal.event_time,
                            run_id=run_id,
                            scenario_id=scen_id,
                            scenario_version=calib_run_result.scenario_version,
                            seed=self.config.calibration_seed,
                            reproducibility_key=calib_run_result.reproducibility_key,
                            partition_role="calibration",
                        )
                    )

            # Score Autoencoder on calibration
            ae_baseline = AutoencoderBaseline(config=self.config.autoencoder_config)
            ae_context = self._observability_context(
                calib_run_result,
                model_name="autoencoder",
                model_version=self.config.autoencoder_config.model_version,
            )
            ae_scores = self.observability.sync_measured_call(
                OperationalStage.MODEL_EXECUTION,
                FailureCategory.MODEL_FAILURE,
                ae_context,
                action=lambda: ae_baseline.score_series(calib_feat_res, start_index=0),
            )
            for idx, ae_res in enumerate(ae_scores):
                if (
                    ae_res.status == AutoencoderScoreStatus.SUCCESS
                    and ae_res.signal
                    and ae_res.anomaly_score is not None
                ):
                    sample_id = derive_calibration_sample_id(
                        package_id=self.config.package_id,
                        package_version=self.config.package_version,
                        model_name="autoencoder",
                        model_version=self.config.autoencoder_config.model_version,
                        run_id=run_id,
                        scenario_id=scen_id,
                        scenario_version=calib_run_result.scenario_version,
                        window_index=idx,
                        event_time=ae_res.signal.event_time,
                        source_event_ids=ae_res.signal.source_event_ids,
                    )
                    ae_calib_samples.append(
                        CalibrationReferenceSample(
                            sample_id=sample_id,
                            model_name="autoencoder",
                            model_version=self.config.autoencoder_config.model_version,
                            baseline_normalized_score=ae_res.anomaly_score,
                            event_time=ae_res.signal.event_time,
                            run_id=run_id,
                            scenario_id=scen_id,
                            scenario_version=calib_run_result.scenario_version,
                            seed=self.config.calibration_seed,
                            reproducibility_key=calib_run_result.reproducibility_key,
                            partition_role="calibration",
                        )
                    )

        # Build CalibrationReferenceData
        ref_datasets = [
            CalibrationReferenceData(
                reference_id="calib-ref-prophet-v1",
                reference_version="1.0.0",
                model_name="prophet",
                model_version=self.config.prophet_config.model_version,
                samples=prophet_calib_samples,
                calibration_cutoff_time=calib_cutoff,
                allowed_partition_role="calibration",
                created_at=t_calib_start,
            ),
            CalibrationReferenceData(
                reference_id="calib-ref-iforest-v1",
                reference_version="1.0.0",
                model_name="isolation_forest",
                model_version=self.config.isolation_forest_config.model_version,
                samples=iforest_calib_samples,
                calibration_cutoff_time=calib_cutoff,
                allowed_partition_role="calibration",
                created_at=t_calib_start,
            ),
            CalibrationReferenceData(
                reference_id="calib-ref-autoencoder-v1",
                reference_version="1.0.0",
                model_name="autoencoder",
                model_version=self.config.autoencoder_config.model_version,
                samples=ae_calib_samples,
                calibration_cutoff_time=calib_cutoff,
                allowed_partition_role="calibration",
                created_at=t_calib_start,
            ),
        ]

        calibrator = CommonScoreCalibrator(config=self.config.calibration_config)
        self.observability.sync_measured_call(
            OperationalStage.CALIBRATION,
            FailureCategory.CALIBRATION_FAILURE,
            self._observability_context(),
            action=lambda: calibrator.fit_reference_data(ref_datasets),
        )

        # 2. Evaluation Phase (Seed 42)
        t_eval_start = t_calib_start + timedelta(
            seconds=self.config.calibration_cutoff_seconds + 60.0
        )

        all_outcomes: list[EvaluationOutcome] = []
        evaluation_runs_records: list[ScenarioRunLineage] = []

        scenario_model_evaluations: dict[str, list[ScenarioModelEvaluation]] = {
            m: [] for m in self.config.model_names
        }

        seen_unit_ids: set[str] = set()

        for scen_id in self.config.scenario_ids:
            eval_run_id = derive_run_id(
                package_id=self.config.package_id,
                package_version=self.config.package_version,
                partition_role="evaluation",
                scenario_id=scen_id,
                seed=self.config.evaluation_seed,
            )
            eval_run_result = await runner.run(
                scenario=scen_id,
                run_id=eval_run_id,
                run_start_time=t_eval_start,
                seed=self.config.evaluation_seed,
            )
            evaluation_runs_records.append(
                _build_scenario_run_lineage(
                    scenario_id=scen_id,
                    run_id=eval_run_id,
                    seed=self.config.evaluation_seed,
                )
            )

            truth = eval_run_result.scenario_truth
            truth_start, truth_end = compute_truth_interval(
                run_start_time=t_eval_start,
                activation_time_seconds=truth.activation_time_seconds,
                recovery_time_seconds=truth.recovery_time_seconds,
                total_duration_seconds=truth.run.total_duration_seconds,
            )

            # Common extracted features across all 3 models
            eval_context = self._observability_context(eval_run_result)
            feat_res = self.observability.sync_measured_call(
                OperationalStage.FEATURE_EXTRACTION,
                FailureCategory.FEATURE_EXTRACTION_FAILURE,
                eval_context,
                action=lambda: extract_features(
                    eval_run_result,
                    config=self.config.feature_window,
                ),
            )

            # Generate EvaluationUnits
            units: list[EvaluationUnit] = []
            for w in feat_res.windows:
                unit_id = f"{scen_id}:{w.window_index}"
                if unit_id in seen_unit_ids:
                    raise EvaluationExecutionError(
                        f"Duplicate evaluation unit ID detected: '{unit_id}'"
                    )
                seen_unit_ids.add(unit_id)

                is_gt_positive = evaluate_ground_truth_label(
                    w.window_start, truth_start, truth_end
                )
                unit = EvaluationUnit(
                    unit_id=unit_id,
                    unit_index=w.window_index,
                    scenario_id=scen_id,
                    run_id=eval_run_id,
                    window_start=w.window_start,
                    window_end=w.window_end,
                    observation_timestamp=w.observation_timestamp,
                    ground_truth_positive=is_gt_positive,
                    truth_activation_time=truth_start,
                    truth_recovery_time=truth_end,
                    service=w.service,
                )
                units.append(unit)

            # Run and evaluate each of the 3 models on the common windows
            # Model 1: Prophet
            p_baseline = ProphetBaseline(config=self.config.prophet_config)
            p_context = self._observability_context(
                eval_run_result,
                model_name="prophet",
                model_version=self.config.prophet_config.model_version,
            )
            p_scores = self.observability.sync_measured_call(
                OperationalStage.MODEL_EXECUTION,
                FailureCategory.MODEL_FAILURE,
                p_context,
                action=lambda: p_baseline.score_series(feat_res, start_index=0),
            )
            p_outcomes = self._evaluate_model_scores(
                model_name="prophet",
                model_version=self.config.prophet_config.model_version,
                units=units,
                model_scores=p_scores,
                calibrator=calibrator,
                score_converter=lambda res: calibration_input_from_prophet(
                    res, self.config.prophet_config
                ),
                context=p_context,
            )
            all_outcomes.extend(p_outcomes)
            scenario_model_evaluations["prophet"].append(
                self._compute_scenario_model_evaluation(
                    scenario_id=scen_id,
                    model_name="prophet",
                    model_version=self.config.prophet_config.model_version,
                    outcomes=p_outcomes,
                    truth_activation_time=truth_start,
                )
            )

            # Model 2: Isolation Forest
            if_baseline = IsolationForestBaseline(
                config=self.config.isolation_forest_config
            )
            if_context = self._observability_context(
                eval_run_result,
                model_name="isolation_forest",
                model_version=self.config.isolation_forest_config.model_version,
            )
            if_scores = self.observability.sync_measured_call(
                OperationalStage.MODEL_EXECUTION,
                FailureCategory.MODEL_FAILURE,
                if_context,
                action=lambda: if_baseline.score_series(feat_res, start_index=0),
            )
            if_outcomes = self._evaluate_model_scores(
                model_name="isolation_forest",
                model_version=self.config.isolation_forest_config.model_version,
                units=units,
                model_scores=if_scores,
                calibrator=calibrator,
                score_converter=lambda res: calibration_input_from_isolation_forest(
                    res, self.config.isolation_forest_config
                ),
                context=if_context,
            )
            all_outcomes.extend(if_outcomes)
            scenario_model_evaluations["isolation_forest"].append(
                self._compute_scenario_model_evaluation(
                    scenario_id=scen_id,
                    model_name="isolation_forest",
                    model_version=self.config.isolation_forest_config.model_version,
                    outcomes=if_outcomes,
                    truth_activation_time=truth_start,
                )
            )

            # Model 3: Autoencoder
            ae_baseline = AutoencoderBaseline(config=self.config.autoencoder_config)
            ae_context = self._observability_context(
                eval_run_result,
                model_name="autoencoder",
                model_version=self.config.autoencoder_config.model_version,
            )
            ae_scores = self.observability.sync_measured_call(
                OperationalStage.MODEL_EXECUTION,
                FailureCategory.MODEL_FAILURE,
                ae_context,
                action=lambda: ae_baseline.score_series(feat_res, start_index=0),
            )
            ae_outcomes = self._evaluate_model_scores(
                model_name="autoencoder",
                model_version=self.config.autoencoder_config.model_version,
                units=units,
                model_scores=ae_scores,
                calibrator=calibrator,
                score_converter=lambda res: calibration_input_from_autoencoder(
                    res, self.config.autoencoder_config
                ),
                context=ae_context,
            )
            all_outcomes.extend(ae_outcomes)
            scenario_model_evaluations["autoencoder"].append(
                self._compute_scenario_model_evaluation(
                    scenario_id=scen_id,
                    model_name="autoencoder",
                    model_version=self.config.autoencoder_config.model_version,
                    outcomes=ae_outcomes,
                    truth_activation_time=truth_start,
                )
            )

        # 3. Model Summaries across scenarios
        model_summaries: list[ModelComparisonSummary] = []
        for model_name in self.config.model_names:
            model_outcomes = [o for o in all_outcomes if o.model_name == model_name]
            scen_evals = scenario_model_evaluations[model_name]
            summary = self._compute_model_comparison_summary(
                model_name=model_name,
                outcomes=model_outcomes,
                scenario_evaluations=scen_evals,
            )
            model_summaries.append(summary)

        run_metadata = _build_evaluation_run_metadata(
            calibration_cutoff=calib_cutoff,
            evaluation_start=t_eval_start,
            calibration_runs=tuple(calibration_runs_records),
            evaluation_runs=tuple(evaluation_runs_records),
        )

        return all_outcomes, model_summaries, run_metadata

    def _evaluate_model_scores(
        self,
        model_name: str,
        model_version: str,
        units: list[EvaluationUnit],
        model_scores: list[Any],
        calibrator: CommonScoreCalibrator,
        score_converter: Any,
        context: ObservabilityContext,
    ) -> list[EvaluationOutcome]:
        return self.observability.sync_measured_call(
            OperationalStage.EVALUATION,
            FailureCategory.EVALUATION_FAILURE,
            context,
            action=lambda: self._evaluate_model_scores_internal(
                model_name=model_name,
                model_version=model_version,
                units=units,
                model_scores=model_scores,
                calibrator=calibrator,
                score_converter=score_converter,
                context=context,
            ),
        )

    def _evaluate_model_scores_internal(
        self,
        model_name: str,
        model_version: str,
        units: list[EvaluationUnit],
        model_scores: list[Any],
        calibrator: CommonScoreCalibrator,
        score_converter: Any,
        context: ObservabilityContext,
    ) -> list[EvaluationOutcome]:
        outcomes: list[EvaluationOutcome] = []
        threshold = self.config.decision_policy.decision_threshold

        for idx, unit in enumerate(units):
            score_res = model_scores[idx] if idx < len(model_scores) else None
            outcome_status: EvaluationOutcomeStatus
            raw_score: float | None = None
            raw_score_type: str | None = None
            base_norm_score: float | None = None
            calibrated_score: float | None = None
            severity: EventSeverity | None = None
            source_sig_id: uuid.UUID | None = None
            source_ev_ids: list[uuid.UUID] = []
            err_cat: str | None = None
            err_msg: str | None = None

            score_status = getattr(score_res, "status", None)
            status_value = getattr(score_status, "value", score_status)

            if score_res is None:
                outcome_status = EvaluationOutcomeStatus.INSUFFICIENT_DATA
                err_cat = "missing_score"
                err_msg = "No score produced for window index"
                self._record_semantic_failure(
                    category=FailureCategory.MISSING_WINDOW,
                    failure_type="MissingWindow",
                    failure_message=err_msg,
                    context=context,
                )
            elif status_value != "success":
                stat = status_value if isinstance(status_value, str) else "unknown"
                if "insufficient" in stat:
                    outcome_status = EvaluationOutcomeStatus.INSUFFICIENT_DATA
                elif "non_convergence" in stat:
                    outcome_status = EvaluationOutcomeStatus.NON_CONVERGENCE
                elif "fit_failure" in stat:
                    outcome_status = EvaluationOutcomeStatus.FIT_FAILURE
                elif "invalid" in stat:
                    outcome_status = EvaluationOutcomeStatus.INVALID_INPUT
                else:
                    outcome_status = EvaluationOutcomeStatus.MODEL_FAILURE
                err_cat = stat
                err_msg = (
                    getattr(score_res, "error_message", None) or f"Model status: {stat}"
                )
                if stat in {"missing_target_value", "missing_features"}:
                    self._record_semantic_failure(
                        category=FailureCategory.MISSING_WINDOW,
                        failure_type="MissingWindow",
                        failure_message=err_msg,
                        context=context,
                    )
                elif stat in {
                    "fit_failure",
                    "non_convergence",
                    "invalid_input",
                    "degenerate_series",
                }:
                    self._record_semantic_failure(
                        category=FailureCategory.MODEL_FAILURE,
                        failure_type="ModelFailure",
                        failure_message=err_msg,
                        context=context,
                    )
                elif "insufficient" not in stat:
                    self._record_semantic_failure(
                        category=FailureCategory.MALFORMED_OUTPUT,
                        failure_type="MalformedOutput",
                        failure_message="Model returned an unsupported status",
                        context=context,
                    )
            else:
                score = getattr(score_res, "anomaly_score", None)
                signal = getattr(score_res, "signal", None)
                if (
                    isinstance(score, bool)
                    or not isinstance(score, (int, float))
                    or not math.isfinite(float(score))
                    or float(score) < 0.0
                    or float(score) > 1.0
                    or not isinstance(signal, AnomalySignal)
                ):
                    outcome_status = EvaluationOutcomeStatus.MODEL_FAILURE
                    err_cat = "malformed_output"
                    err_msg = "Successful model result violates the score contract"
                    signal_id = (
                        signal.signal_id if isinstance(signal, AnomalySignal) else None
                    )
                    self._record_semantic_failure(
                        category=FailureCategory.MALFORMED_OUTPUT,
                        failure_type="MalformedOutput",
                        failure_message=err_msg,
                        context=context.model_copy(update={"signal_id": signal_id}),
                    )
                    score_res = None

            if score_res is not None and status_value == "success" and err_cat is None:
                try:
                    signal = score_res.signal
                    calibration_context = context.model_copy(
                        update={"signal_id": signal.signal_id if signal else None}
                    )

                    def apply_calibration() -> CalibratedScoreResult:
                        calib_inp: CalibrationInput = score_converter(score_res)
                        return calibrator.calibrate_input(calib_inp)

                    cal_res = self.observability.sync_measured_call(
                        OperationalStage.CALIBRATION,
                        FailureCategory.CALIBRATION_FAILURE,
                        calibration_context,
                        action=apply_calibration,
                    )
                    raw_score = cal_res.raw_score
                    raw_score_type = cal_res.raw_score_type
                    base_norm_score = cal_res.baseline_normalized_score
                    source_sig_id = cal_res.source_signal_id
                    if cal_res.calibrated_signal:
                        source_ev_ids = list(cal_res.calibrated_signal.source_event_ids)

                    if cal_res.status == CalibratedScoreStatus.SUCCESS:
                        outcome_status = EvaluationOutcomeStatus.SUCCESS
                        calibrated_score = cal_res.calibrated_score
                        severity = cal_res.severity
                    elif cal_res.status == CalibratedScoreStatus.MISSING_CALIBRATION:
                        outcome_status = EvaluationOutcomeStatus.MISSING_CALIBRATION
                        err_cat = "missing_calibration"
                        err_msg = cal_res.error_message
                    else:
                        outcome_status = EvaluationOutcomeStatus.INVALID_INPUT
                        err_cat = str(cal_res.status)
                        err_msg = cal_res.error_message
                except Exception as exc:
                    outcome_status = EvaluationOutcomeStatus.MODEL_FAILURE
                    err_cat = "calibration_exception"
                    err_msg = str(exc)

            predicted_pos = compute_binary_decision(
                status=outcome_status,
                calibrated_score=calibrated_score,
                decision_threshold=threshold,
            )

            is_tp, is_fp, is_tn, is_fn = classify_confusion_category(
                ground_truth_positive=unit.ground_truth_positive,
                predicted_positive=predicted_pos,
            )

            outcome_id = derive_outcome_id(
                package_id=self.config.package_id,
                package_version=self.config.package_version,
                model_name=model_name,
                run_id=unit.run_id,
                scenario_id=unit.scenario_id,
                unit_id=unit.unit_id,
            )

            outcome = EvaluationOutcome(
                outcome_id=outcome_id,
                unit_id=unit.unit_id,
                unit_index=unit.unit_index,
                scenario_id=unit.scenario_id,
                run_id=unit.run_id,
                model_name=model_name,
                model_version=model_version,
                event_time=unit.observation_timestamp,
                ground_truth_positive=unit.ground_truth_positive,
                status=outcome_status,
                raw_score=raw_score,
                raw_score_type=raw_score_type,
                baseline_normalized_score=base_norm_score,
                calibrated_score=calibrated_score,
                severity=severity,
                predicted_positive=predicted_pos,
                is_true_positive=is_tp,
                is_false_positive=is_fp,
                is_true_negative=is_tn,
                is_false_negative=is_fn,
                source_signal_id=source_sig_id,
                source_event_ids=source_ev_ids,
                error_category=err_cat,
                error_message=err_msg,
                details={
                    "window_start": unit.window_start.isoformat(),
                    "window_end": unit.window_end.isoformat(),
                },
            )
            outcomes.append(outcome)

        return outcomes

    def _compute_scenario_model_evaluation(
        self,
        scenario_id: str,
        model_name: str,
        model_version: str,
        outcomes: list[EvaluationOutcome],
        truth_activation_time: datetime,
    ) -> ScenarioModelEvaluation:
        counts = calculate_confusion_counts(outcomes)
        prec = calculate_precision(counts.true_positives, counts.false_positives)
        rec = calculate_recall(counts.true_positives, counts.false_negatives)
        f1 = calculate_f1(precision=prec, recall=rec)
        fpr = calculate_false_positive_rate(
            counts.false_positives, counts.true_negatives
        )
        latency = calculate_detection_latency(
            outcomes=outcomes,
            truth_activation_time=truth_activation_time,
            scenario_id=scenario_id,
            model_name=model_name,
        )

        gt_pos_scores = [
            o.calibrated_score
            for o in outcomes
            if o.ground_truth_positive and o.calibrated_score is not None
        ]
        gt_neg_scores = [
            o.calibrated_score
            for o in outcomes
            if not o.ground_truth_positive and o.calibrated_score is not None
        ]
        pred_pos_scores = [
            o.calibrated_score
            for o in outcomes
            if o.predicted_positive and o.calibrated_score is not None
        ]
        pred_neg_scores = [
            o.calibrated_score
            for o in outcomes
            if not o.predicted_positive and o.calibrated_score is not None
        ]

        return ScenarioModelEvaluation(
            scenario_id=scenario_id,
            model_name=model_name,
            model_version=model_version,
            confusion_counts=counts,
            precision=prec,
            recall=rec,
            f1=f1,
            false_positive_rate=fpr,
            detection_latency=latency,
            truth_positive_distribution=calculate_score_distribution(
                gt_pos_scores, "truth_positive"
            ),
            truth_negative_distribution=calculate_score_distribution(
                gt_neg_scores, "truth_negative"
            ),
            predicted_positive_distribution=calculate_score_distribution(
                pred_pos_scores, "predicted_positive"
            ),
            predicted_negative_distribution=calculate_score_distribution(
                pred_neg_scores, "predicted_negative"
            ),
            status="completed",
        )

    def _compute_model_comparison_summary(
        self,
        model_name: str,
        outcomes: list[EvaluationOutcome],
        scenario_evaluations: list[ScenarioModelEvaluation],
    ) -> ModelComparisonSummary:
        version = (
            scenario_evaluations[0].model_version if scenario_evaluations else "1.0.0"
        )
        micro_counts = calculate_confusion_counts(outcomes)
        micro_prec = calculate_precision(
            micro_counts.true_positives, micro_counts.false_positives
        )
        micro_rec = calculate_recall(
            micro_counts.true_positives, micro_counts.false_negatives
        )
        micro_f1 = calculate_f1(precision=micro_prec, recall=micro_rec)
        micro_fpr = calculate_false_positive_rate(
            micro_counts.false_positives, micro_counts.true_negatives
        )

        def_prec_vals = [
            se.precision.value
            for se in scenario_evaluations
            if se.precision.status == "defined" and se.precision.value is not None
        ]
        def_rec_vals = [
            se.recall.value
            for se in scenario_evaluations
            if se.recall.status == "defined" and se.recall.value is not None
        ]
        def_f1_vals = [
            se.f1.value
            for se in scenario_evaluations
            if se.f1.status == "defined" and se.f1.value is not None
        ]
        def_fpr_vals = [
            se.false_positive_rate.value
            for se in scenario_evaluations
            if se.false_positive_rate.status == "defined"
            and se.false_positive_rate.value is not None
        ]

        macro_prec = (
            MetricResult(
                metric_name="macro_precision",
                value=round(sum(def_prec_vals) / len(def_prec_vals), 6),
                status="defined",
                numerator=sum(def_prec_vals),
                denominator=float(len(def_prec_vals)),
            )
            if def_prec_vals
            else MetricResult(
                metric_name="macro_precision",
                value=None,
                status="undefined",
                reason="no_defined_scenario_precisions",
            )
        )

        macro_rec = (
            MetricResult(
                metric_name="macro_recall",
                value=round(sum(def_rec_vals) / len(def_rec_vals), 6),
                status="defined",
                numerator=sum(def_rec_vals),
                denominator=float(len(def_rec_vals)),
            )
            if def_rec_vals
            else MetricResult(
                metric_name="macro_recall",
                value=None,
                status="undefined",
                reason="no_defined_scenario_recalls",
            )
        )

        macro_f1 = (
            MetricResult(
                metric_name="macro_f1",
                value=round(sum(def_f1_vals) / len(def_f1_vals), 6),
                status="defined",
                numerator=sum(def_f1_vals),
                denominator=float(len(def_f1_vals)),
            )
            if def_f1_vals
            else MetricResult(
                metric_name="macro_f1",
                value=None,
                status="undefined",
                reason="no_defined_scenario_f1s",
            )
        )

        macro_fpr = (
            MetricResult(
                metric_name="macro_false_positive_rate",
                value=round(sum(def_fpr_vals) / len(def_fpr_vals), 6),
                status="defined",
                numerator=sum(def_fpr_vals),
                denominator=float(len(def_fpr_vals)),
            )
            if def_fpr_vals
            else MetricResult(
                metric_name="macro_false_positive_rate",
                value=None,
                status="undefined",
                reason="no_defined_scenario_fprs",
            )
        )

        detected_latencies = [
            se.detection_latency.latency_seconds
            for se in scenario_evaluations
            if se.detection_latency.status == "detected"
            and se.detection_latency.latency_seconds is not None
        ]
        missing_count = len(scenario_evaluations) - len(detected_latencies)

        if detected_latencies:
            lat_summary = LatencySummary(
                total_scenario_count=len(scenario_evaluations),
                contributing_scenario_count=len(detected_latencies),
                missing_detection_count=missing_count,
                min_seconds=round(min(detected_latencies), 6),
                mean_seconds=round(
                    sum(detected_latencies) / len(detected_latencies), 6
                ),
                median_seconds=round(
                    compute_quantile(sorted(detected_latencies), 0.50), 6
                ),
                p95_seconds=round(
                    compute_quantile(sorted(detected_latencies), 0.95), 6
                ),
                max_seconds=round(max(detected_latencies), 6),
            )
        else:
            lat_summary = LatencySummary(
                total_scenario_count=len(scenario_evaluations),
                contributing_scenario_count=0,
                missing_detection_count=missing_count,
            )

        req_scenarios = list(self.config.scenario_ids)
        exec_scenarios = [se.scenario_id for se in scenario_evaluations]
        feat_scenarios = exec_scenarios
        scored_scenarios = [
            se.scenario_id
            for se in scenario_evaluations
            if se.confusion_counts.success_units > 0
        ]
        eval_scenarios = exec_scenarios
        det_scenarios = [
            se.scenario_id
            for se in scenario_evaluations
            if se.detection_latency.status == "detected"
        ]
        insuf_scenarios = [
            se.scenario_id
            for se in scenario_evaluations
            if se.confusion_counts.insufficient_units > 0
        ]
        fail_scenarios = [
            se.scenario_id
            for se in scenario_evaluations
            if se.confusion_counts.failure_units > 0
        ]

        coverage = ScenarioCoverageSummary(
            requested_count=len(req_scenarios),
            requested_scenario_ids=req_scenarios,
            executed_count=len(exec_scenarios),
            executed_scenario_ids=exec_scenarios,
            feature_ready_count=len(feat_scenarios),
            feature_ready_scenario_ids=feat_scenarios,
            scored_count=len(scored_scenarios),
            scored_scenario_ids=scored_scenarios,
            evaluable_count=len(eval_scenarios),
            evaluable_scenario_ids=eval_scenarios,
            detected_count=len(det_scenarios),
            detected_scenario_ids=det_scenarios,
            insufficient_count=len(insuf_scenarios),
            insufficient_scenario_ids=insuf_scenarios,
            failed_count=len(fail_scenarios),
            failed_scenario_ids=fail_scenarios,
        )

        gt_pos_scores = [
            o.calibrated_score
            for o in outcomes
            if o.ground_truth_positive and o.calibrated_score is not None
        ]
        gt_neg_scores = [
            o.calibrated_score
            for o in outcomes
            if not o.ground_truth_positive and o.calibrated_score is not None
        ]
        pred_pos_scores = [
            o.calibrated_score
            for o in outcomes
            if o.predicted_positive and o.calibrated_score is not None
        ]
        pred_neg_scores = [
            o.calibrated_score
            for o in outcomes
            if not o.predicted_positive and o.calibrated_score is not None
        ]

        return ModelComparisonSummary(
            model_name=model_name,
            model_version=version,
            micro_confusion_counts=micro_counts,
            micro_precision=micro_prec,
            micro_recall=micro_rec,
            micro_f1=micro_f1,
            micro_false_positive_rate=micro_fpr,
            macro_precision=macro_prec,
            macro_recall=macro_rec,
            macro_f1=macro_f1,
            macro_false_positive_rate=macro_fpr,
            macro_contributing_scenario_count=len(def_prec_vals),
            latency_summary=lat_summary,
            scenario_coverage=coverage,
            truth_positive_distribution=calculate_score_distribution(
                gt_pos_scores, "truth_positive"
            ),
            truth_negative_distribution=calculate_score_distribution(
                gt_neg_scores, "truth_negative"
            ),
            predicted_positive_distribution=calculate_score_distribution(
                pred_pos_scores, "predicted_positive"
            ),
            predicted_negative_distribution=calculate_score_distribution(
                pred_neg_scores, "predicted_negative"
            ),
            scenario_evaluations=scenario_evaluations,
        )


def _compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def generate_model_quality_svg(summaries: Sequence[ModelComparisonSummary]) -> str:
    """Generate deterministic standalone SVG visualization of model quality metrics."""
    width = 750
    height = 420
    margin_top = 70
    margin_bottom = 60
    margin_left = 80
    margin_right = 40
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    models = [s.model_name for s in summaries]
    metrics = ["Precision", "Recall", "F1", "FPR"]
    num_metrics = len(metrics)
    group_width = plot_width / num_metrics

    colors = {
        "prophet": "#2b5c8f",
        "isolation_forest": "#2e7d32",
        "autoencoder": "#d84315",
    }
    bar_width = min(35.0, (group_width - 30) / max(1, len(models)))

    lines: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}" font-family="monospace, sans-serif">',
        "  <title>AegisOps Phase 3 Anomaly Detection Model Quality Metrics</title>",
        f"  <desc>Comparison of micro Precision, Recall, F1, and False Positive Rate across {len(models)} baseline models.</desc>",
        '  <rect width="100%" height="100%" fill="#ffffff"/>',
        f'  <text x="{width / 2}" y="35" text-anchor="middle" font-size="16" font-weight="bold" fill="#111827">Phase 3 Baseline Model Quality Metrics (Micro Aggregations)</text>',
        f'  <text x="{width / 2}" y="55" text-anchor="middle" font-size="11" fill="#4b5563">Canonical Acceptance Evaluation Suite</text>',
    ]

    for i in range(6):
        grid_val = i * 0.20
        y = margin_top + plot_height * (1.0 - grid_val)
        lines.append(
            f'  <line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" stroke="#e5e7eb" stroke-dasharray="3,3" />'
        )
        lines.append(
            f'  <text x="{margin_left - 10}" y="{y + 4:.1f}" text-anchor="end" font-size="11" fill="#6b7280">{grid_val:.2f}</text>'
        )

    lines.append(
        f'  <line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" stroke="#111827" stroke-width="1.5" />'
    )
    lines.append(
        f'  <line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" stroke="#111827" stroke-width="1.5" />'
    )

    for m_idx, m_name in enumerate(metrics):
        group_center_x = margin_left + m_idx * group_width + group_width / 2
        lines.append(
            f'  <text x="{group_center_x:.1f}" y="{margin_top + plot_height + 25}" text-anchor="middle" font-size="12" font-weight="bold" fill="#111827">{m_name}</text>'
        )

        for mod_idx, summary in enumerate(summaries):
            mod_name = summary.model_name
            color = colors.get(mod_name, "#6b7280")
            metric_val: float | None = None
            if m_name == "Precision":
                metric_val = summary.micro_precision.value
            elif m_name == "Recall":
                metric_val = summary.micro_recall.value
            elif m_name == "F1":
                metric_val = summary.micro_f1.value
            elif m_name == "FPR":
                metric_val = summary.micro_false_positive_rate.value

            bar_x = (
                group_center_x - (len(summaries) * bar_width) / 2 + mod_idx * bar_width
            )
            if metric_val is not None and math.isfinite(metric_val):
                bar_h = metric_val * plot_height
                bar_y = margin_top + plot_height - bar_h
                lines.append(
                    f'  <rect x="{bar_x:.1f}" y="{bar_y:.1f}" width="{bar_width - 4:.1f}" height="{bar_h:.1f}" fill="{color}" rx="2" />'
                )
                label_y = bar_y - 5 if bar_y - 5 > margin_top + 12 else bar_y + 14
                label_fill = "#111827" if bar_y - 5 > margin_top + 12 else "#ffffff"
                lines.append(
                    f'  <text x="{bar_x + (bar_width - 4) / 2:.1f}" y="{label_y:.1f}" text-anchor="middle" font-size="10" font-weight="bold" fill="{label_fill}">{metric_val:.2f}</text>'
                )
            else:
                lines.append(
                    f'  <rect x="{bar_x:.1f}" y="{margin_top + plot_height - 15}" width="{bar_width - 4:.1f}" height="15" fill="#f3f4f6" stroke="#9ca3af" stroke-dasharray="2,2" rx="2" />'
                )
                lines.append(
                    f'  <text x="{bar_x + (bar_width - 4) / 2:.1f}" y="{margin_top + plot_height - 3:.1f}" text-anchor="middle" font-size="9" fill="#9ca3af">N/A</text>'
                )

    leg_x = margin_left
    leg_y = height - 15
    for mod_idx, summary in enumerate(summaries):
        mod_name = summary.model_name
        color = colors.get(mod_name, "#6b7280")
        lx = leg_x + mod_idx * 210
        display_name = mod_name.replace("_", " ").title()
        lines.append(
            f'  <rect x="{lx}" y="{leg_y - 10}" width="14" height="10" fill="{color}" rx="2" />'
        )
        lines.append(
            f'  <text x="{lx + 20}" y="{leg_y}" font-size="11" fill="#374151">{display_name}</text>'
        )

    lines.append("</svg>")
    return "\n".join(lines)


def generate_detection_latency_svg(
    summaries: Sequence[ModelComparisonSummary], scenarios: Sequence[str]
) -> str:
    """Generate deterministic standalone SVG visualization of detection latency by scenario."""
    width = 900
    height = 460
    margin_top = 70
    margin_bottom = 110
    margin_left = 80
    margin_right = 40
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    num_scenarios = len(scenarios)
    group_width = plot_width / max(1, num_scenarios)
    bar_width = min(22.0, (group_width - 15) / max(1, len(summaries)))

    colors = {
        "prophet": "#2b5c8f",
        "isolation_forest": "#2e7d32",
        "autoencoder": "#d84315",
    }

    all_lats: list[float] = []
    for s in summaries:
        for se in s.scenario_evaluations:
            if (
                se.detection_latency.status == "detected"
                and se.detection_latency.latency_seconds is not None
            ):
                all_lats.append(se.detection_latency.latency_seconds)
    max_lat = max(all_lats) if all_lats else 10.0
    y_max = math.ceil(max_lat + 2.0)

    lines: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}" font-family="monospace, sans-serif">',
        "  <title>AegisOps Phase 3 Detection Latency by Scenario</title>",
        f"  <desc>Event-time detection latency (seconds from fault activation) across {num_scenarios} canonical scenarios for baseline models.</desc>",
        '  <rect width="100%" height="100%" fill="#ffffff"/>',
        f'  <text x="{width / 2}" y="35" text-anchor="middle" font-size="16" font-weight="bold" fill="#111827">Detection Latency by Scenario (Event-Time Seconds)</text>',
        f'  <text x="{width / 2}" y="55" text-anchor="middle" font-size="11" fill="#4b5563">Time from Scenario Truth Activation to First True-Positive Warning</text>',
    ]

    steps = 5
    for i in range(steps + 1):
        grid_val = (i / steps) * y_max
        y = margin_top + plot_height * (1.0 - grid_val / y_max)
        lines.append(
            f'  <line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" stroke="#e5e7eb" stroke-dasharray="3,3" />'
        )
        lines.append(
            f'  <text x="{margin_left - 10}" y="{y + 4:.1f}" text-anchor="end" font-size="11" fill="#6b7280">{grid_val:.1f}s</text>'
        )

    lines.append(
        f'  <line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{width - margin_right}" y2="{margin_top + plot_height}" stroke="#111827" stroke-width="1.5" />'
    )
    lines.append(
        f'  <line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" stroke="#111827" stroke-width="1.5" />'
    )

    for sc_idx, scen_id in enumerate(scenarios):
        group_center_x = margin_left + sc_idx * group_width + group_width / 2
        lines.append(
            f'  <text x="{group_center_x:.1f}" y="{margin_top + plot_height + 15}" transform="rotate(35, {group_center_x:.1f}, {margin_top + plot_height + 15})" text-anchor="start" font-size="10" fill="#111827">{scen_id}</text>'
        )

        for mod_idx, summary in enumerate(summaries):
            mod_name = summary.model_name
            color = colors.get(mod_name, "#6b7280")
            scen_eval: ScenarioModelEvaluation | None = next(
                (e for e in summary.scenario_evaluations if e.scenario_id == scen_id),
                None,
            )
            bar_x = (
                group_center_x - (len(summaries) * bar_width) / 2 + mod_idx * bar_width
            )

            if (
                scen_eval
                and scen_eval.detection_latency.status == "detected"
                and scen_eval.detection_latency.latency_seconds is not None
            ):
                lat = scen_eval.detection_latency.latency_seconds
                bar_h = (lat / y_max) * plot_height
                bar_y = margin_top + plot_height - bar_h
                lines.append(
                    f'  <rect x="{bar_x:.1f}" y="{bar_y:.1f}" width="{bar_width - 3:.1f}" height="{bar_h:.1f}" fill="{color}" rx="2" />'
                )
                lines.append(
                    f'  <text x="{bar_x + (bar_width - 3) / 2:.1f}" y="{bar_y - 4:.1f}" text-anchor="middle" font-size="8.5" font-weight="bold" fill="#111827">{lat:.1f}s</text>'
                )
            else:
                lines.append(
                    f'  <rect x="{bar_x:.1f}" y="{margin_top + plot_height - 12}" width="{bar_width - 3:.1f}" height="12" fill="#f9fafb" stroke="#d1d5db" stroke-dasharray="2,2" rx="1" />'
                )
                lines.append(
                    f'  <text x="{bar_x + (bar_width - 3) / 2:.1f}" y="{margin_top + plot_height - 3:.1f}" text-anchor="middle" font-size="7.5" fill="#9ca3af">ND</text>'
                )

    leg_x = margin_left
    leg_y = height - 15
    for mod_idx, summary in enumerate(summaries):
        mod_name = summary.model_name
        color = colors.get(mod_name, "#6b7280")
        lx = leg_x + mod_idx * 210
        display_name = mod_name.replace("_", " ").title()
        lines.append(
            f'  <rect x="{lx}" y="{leg_y - 10}" width="14" height="10" fill="{color}" rx="2" />'
        )
        lines.append(
            f'  <text x="{lx + 20}" y="{leg_y}" font-size="11" fill="#374151">{display_name}</text>'
        )

    lines.append(
        f'  <rect x="{leg_x + len(summaries) * 210}" y="{leg_y - 10}" width="14" height="10" fill="#f9fafb" stroke="#d1d5db" stroke-dasharray="2,2" rx="1" />'
    )
    lines.append(
        f'  <text x="{leg_x + len(summaries) * 210 + 20}" y="{leg_y}" font-size="11" fill="#6b7280">ND = Not Detected</text>'
    )

    lines.append("</svg>")
    return "\n".join(lines)


_LogicalUnitKey = tuple[str, uuid.UUID, str, int]
_LogicalSlotKey = tuple[str, uuid.UUID, str, int, str]


class _OutcomeSlotAccounting(NamedTuple):
    unique_unit_keys: set[_LogicalUnitKey]
    unique_slot_keys: set[_LogicalSlotKey]
    unique_units: int
    expected_slots: int
    recorded_outcomes: int
    duplicate_slots: int
    missing_slots: int


def _extract_logical_unit_key(outcome: EvaluationOutcome) -> _LogicalUnitKey:
    return (
        outcome.scenario_id,
        outcome.run_id,
        outcome.unit_id,
        outcome.unit_index,
    )


def _extract_logical_slot_key(outcome: EvaluationOutcome) -> _LogicalSlotKey:
    return (
        outcome.scenario_id,
        outcome.run_id,
        outcome.unit_id,
        outcome.unit_index,
        outcome.model_name,
    )


def _calculate_outcome_slot_accounting(
    outcomes: Sequence[EvaluationOutcome],
    config: EvaluationHarnessConfig,
) -> _OutcomeSlotAccounting:
    unique_unit_keys = {_extract_logical_unit_key(o) for o in outcomes}
    unique_slot_keys = {_extract_logical_slot_key(o) for o in outcomes}
    unique_units = len(unique_unit_keys)
    expected_slots = unique_units * len(config.model_names)
    recorded_outcomes = len(outcomes)
    duplicate_slots = recorded_outcomes - len(unique_slot_keys)
    missing_slots = expected_slots - len(unique_slot_keys)
    return _OutcomeSlotAccounting(
        unique_unit_keys=unique_unit_keys,
        unique_slot_keys=unique_slot_keys,
        unique_units=unique_units,
        expected_slots=expected_slots,
        recorded_outcomes=recorded_outcomes,
        duplicate_slots=duplicate_slots,
        missing_slots=missing_slots,
    )


def validate_evaluation_outcomes(
    outcomes: Sequence[EvaluationOutcome],
    config: EvaluationHarnessConfig,
) -> None:
    """Validate outcome matrix uniqueness, completeness, and boundary constraints before serialization."""
    seen_outcome_ids: set[uuid.UUID] = set()

    for o in outcomes:
        if o.outcome_id in seen_outcome_ids:
            raise EvaluationArtifactError(
                f"Duplicate outcome_id detected: {o.outcome_id}"
            )
        seen_outcome_ids.add(o.outcome_id)

        if o.scenario_id not in config.scenario_ids:
            raise EvaluationArtifactError(
                f"Outcome contains unknown scenario_id '{o.scenario_id}' not in configured scenario_ids"
            )

        if o.model_name not in config.model_names:
            raise EvaluationArtifactError(
                f"Outcome contains unknown model_name '{o.model_name}' not in configured model_names"
            )

    accounting = _calculate_outcome_slot_accounting(outcomes, config)
    if accounting.duplicate_slots > 0:
        raise EvaluationArtifactError("Duplicate logical model-outcome slot detected")


def generate_evaluation_report(
    manifest: EvaluationPackageManifest,
    summaries: Sequence[ModelComparisonSummary],
    outcomes: Sequence[EvaluationOutcome],
    config: EvaluationHarnessConfig,
    run_metadata: EvaluationRunMetadata,
) -> str:
    """Generate comprehensive human-readable evaluation report in Markdown format."""
    now_str = manifest.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")

    dec_policy = config.decision_policy
    lbl_policy = config.label_policy
    feat_win = config.feature_window
    calib_cfg = config.calibration_config

    calib_run_map = {
        r.scenario_id: str(r.run_id) for r in run_metadata.calibration_runs
    }
    eval_run_map = {r.scenario_id: str(r.run_id) for r in run_metadata.evaluation_runs}

    run_lineage_lines = [
        "| Scenario ID | Calibration Run ID | Evaluation Run ID |",
        "|---|---|---|",
    ]
    for scen_id in config.scenario_ids:
        c_rid = calib_run_map[scen_id]
        e_rid = eval_run_map[scen_id]
        run_lineage_lines.append(f"| `{scen_id}` | `{c_rid}` | `{e_rid}` |")

    model_version_lines = []
    for s in summaries:
        disp = s.model_name.replace("_", " ").title()
        model_version_lines.append(
            f"- **{disp} Baseline** (`{s.model_name}`, version `{s.model_version}`)"
        )

    lines: list[str] = [
        "# AegisOps Phase 3 Baseline Model Comparison Report",
        "",
        f"**Package ID:** `{config.package_id}`  ",
        f"**Package Version:** `{config.package_version}`  ",
        f"**Code Revision:** `{config.code_revision}`  ",
        f"**Generated:** {now_str}  ",
        f"**Environment:** `{config.environment}`  ",
        f"**Artifact Root:** `{config.artifact_root}`  ",
        "",
        "---",
        "",
        "## 1. Executive Summary and Evaluation Scope",
        "",
        "This report presents empirical comparison evidence for the approved AegisOps Phase 3 anomaly detection baselines:",
        *model_version_lines,
        "",
        f"Evaluation was performed across {len(config.scenario_ids)} canonical simulator scenarios under controlled, reproducible conditions using disjoint calibration (seed `{config.calibration_seed}`) and evaluation (seed `{config.evaluation_seed}`) partitions.",
        "",
        "---",
        "",
        "## 2. Partition, Policy, and Feature Lineage",
        "",
        "### 2.1 Partition and Reproducibility Lineage",
        f"- **Calibration Seed:** `{config.calibration_seed}` (Partition ID: `{config.calibration_partition_id}`)",
        f"- **Evaluation Seed:** `{config.evaluation_seed}` (Partition ID: `{config.evaluation_partition_id}`)",
        f"- **Calibration Cutoff:** {config.calibration_cutoff_seconds:.1f}s (Cutoff Timestamp: `{run_metadata.calibration_cutoff.isoformat()}`)",
        f"- **Evaluation Start:** `{run_metadata.evaluation_start.isoformat()}`",
        f"- **Calibration Scenario Runs Recorded:** {len(run_metadata.calibration_runs)}",
        f"- **Evaluation Scenario Runs Recorded:** {len(run_metadata.evaluation_runs)}",
        "",
        "#### Scenario Run Lineage",
        *run_lineage_lines,
        "",
        "### 2.2 Operational Policy Lineage",
        f"- **Decision Policy:** `{dec_policy.policy_name}` (v`{dec_policy.policy_version}`)",
        f"- **Decision Threshold:** `{config.decision_policy.decision_threshold:.2f}` ({dec_policy.description})",
        f"- **Label Policy:** `{lbl_policy.policy_name}` (v`{lbl_policy.policy_version}`, overlap rule: `{lbl_policy.overlap_rule}`)",
        f"- **Label Description:** {lbl_policy.description}",
        "",
        "### 2.3 Feature and Windowing Lineage",
        f"- **Feature Configuration ID:** `{feat_win.feature_config_id}`",
        f"- **Feature Names:** {', '.join(f'`{f}`' for f in feat_win.feature_names)}",
        f"- **Window Size / Step Size:** {feat_win.window_size_seconds:.1f}s / {feat_win.step_size_seconds:.1f}s",
        f"- **Aggregation Methods:** {', '.join(feat_win.aggregation_methods)}",
        f"- **Imputation Strategy:** `{feat_win.imputation_strategy}`",
        "",
        "### 2.4 Model and Calibration Lineage",
        f"- **Calibration Method Name:** `{calib_cfg.calibration_method}`",
        f"- **Calibration Method Version:** `{calib_cfg.calibration_method_version}`",
        f"- **Calibration Reference ID:** `{calib_cfg.calibration_reference_id}`",
        f"- **Calibration Reference Version:** `{calib_cfg.calibration_reference_version}`",
        f"- **Calibration Split Rule:** `{calib_cfg.calibration_split_rule}`",
        f"- **Calibration Split Version:** `{calib_cfg.calibration_split_version}`",
        f"- **Severity Mapping Version:** `{calib_cfg.severity_mapping_version}`",
        "",
        "---",
        "",
        "## 3. Common Evaluation Matrix and Micro Summary",
        "",
        "| Model | Total Units | TP | FP | TN | FN | Precision | Recall | F1 | FPR |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for s in summaries:
        c = s.micro_confusion_counts
        p_str = (
            f"{s.micro_precision.value:.4f}"
            if s.micro_precision.value is not None
            else "undefined"
        )
        r_str = (
            f"{s.micro_recall.value:.4f}"
            if s.micro_recall.value is not None
            else "undefined"
        )
        f1_str = (
            f"{s.micro_f1.value:.4f}" if s.micro_f1.value is not None else "undefined"
        )
        fpr_str = (
            f"{s.micro_false_positive_rate.value:.4f}"
            if s.micro_false_positive_rate.value is not None
            else "undefined"
        )
        display_name = s.model_name.replace("_", " ").title()
        lines.append(
            f"| {display_name} | {c.total_units} | {c.true_positives} | {c.false_positives} | {c.true_negatives} | {c.false_negatives} | {p_str} | {r_str} | {f1_str} | {fpr_str} |"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## 4. Detection Latency (Event Time)",
            "",
            f"Detection latency measures the event-time duration (in seconds) between ground-truth fault activation and the first true-positive calibrated alert (`score >= {config.decision_policy.decision_threshold:.2f}`).",
            "",
            "| Model | Detected Scenarios | Missing Detections | Min Latency | Mean Latency | Median Latency | P95 Latency | Max Latency |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for s in summaries:
        lat_sum = s.latency_summary
        display_name = s.model_name.replace("_", " ").title()
        min_str = (
            f"{lat_sum.min_seconds:.2f}s" if lat_sum.min_seconds is not None else "N/A"
        )
        mean_str = (
            f"{lat_sum.mean_seconds:.2f}s"
            if lat_sum.mean_seconds is not None
            else "N/A"
        )
        med_str = (
            f"{lat_sum.median_seconds:.2f}s"
            if lat_sum.median_seconds is not None
            else "N/A"
        )
        p95_str = (
            f"{lat_sum.p95_seconds:.2f}s" if lat_sum.p95_seconds is not None else "N/A"
        )
        max_str = (
            f"{lat_sum.max_seconds:.2f}s" if lat_sum.max_seconds is not None else "N/A"
        )
        lines.append(
            f"| {display_name} | {lat_sum.contributing_scenario_count} | {lat_sum.missing_detection_count} | {min_str} | {mean_str} | {med_str} | {p95_str} | {max_str} |"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## 5. Scenario Coverage and Non-Success Breakdown",
            "",
            "| Model | Requested | Executed | Scored | Evaluable | Detected | Insufficient Data | Failed |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for s in summaries:
        cov = s.scenario_coverage
        display_name = s.model_name.replace("_", " ").title()
        lines.append(
            f"| {display_name} | {cov.requested_count} | {cov.executed_count} | {cov.scored_count} | {cov.evaluable_count} | {cov.detected_count} | {cov.insufficient_count} | {cov.failed_count} |"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## 6. Detailed Per-Scenario Performance",
            "",
            "| Scenario ID | Model | TP | FP | TN | FN | Precision | Recall | F1 | Latency | Status |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )

    for scen_id in config.scenario_ids:
        for s in summaries:
            scen_eval = next(
                (e for e in s.scenario_evaluations if e.scenario_id == scen_id), None
            )
            if not scen_eval:
                continue
            c = scen_eval.confusion_counts
            p_str = (
                f"{scen_eval.precision.value:.2f}"
                if scen_eval.precision.value is not None
                else "und"
            )
            r_str = (
                f"{scen_eval.recall.value:.2f}"
                if scen_eval.recall.value is not None
                else "und"
            )
            f1_str = (
                f"{scen_eval.f1.value:.2f}" if scen_eval.f1.value is not None else "und"
            )
            lat_str = (
                f"{scen_eval.detection_latency.latency_seconds:.1f}s"
                if scen_eval.detection_latency.latency_seconds is not None
                else "ND"
            )
            display_name = s.model_name.replace("_", " ").title()
            lines.append(
                f"| `{scen_id}` | {display_name} | {c.true_positives} | {c.false_positives} | {c.true_negatives} | {c.false_negatives} | {p_str} | {r_str} | {f1_str} | {lat_str} | {scen_eval.status} |"
            )

    lines.extend(
        [
            "",
            "---",
            "",
            "## 7. Score Distributions and Calibration Lineage",
            "",
        ]
    )

    for s in summaries:
        display_name = s.model_name.replace("_", " ").title()
        tp_dist = s.truth_positive_distribution
        tn_dist = s.truth_negative_distribution
        lines.extend(
            [
                f"### {display_name}",
                f"- **Truth-Positive Distribution (N={tp_dist.count}):** "
                f"Min={tp_dist.min}, Mean={tp_dist.mean}, Median={tp_dist.median}, P95={tp_dist.p95}, Max={tp_dist.max}",
                f"- **Truth-Negative Distribution (N={tn_dist.count}):** "
                f"Min={tn_dist.min}, Mean={tn_dist.mean}, Median={tn_dist.median}, P95={tn_dist.p95}, Max={tn_dist.max}",
                "",
            ]
        )

    fps = [o for o in outcomes if o.is_false_positive]
    fns = [o for o in outcomes if o.is_false_negative]
    succs = [o for o in outcomes if o.status == EvaluationOutcomeStatus.SUCCESS]
    insufs = [
        o for o in outcomes if o.status == EvaluationOutcomeStatus.INSUFFICIENT_DATA
    ]
    miss_cals = [
        o for o in outcomes if o.status == EvaluationOutcomeStatus.MISSING_CALIBRATION
    ]
    non_convs = [
        o for o in outcomes if o.status == EvaluationOutcomeStatus.NON_CONVERGENCE
    ]
    fit_fails = [o for o in outcomes if o.status == EvaluationOutcomeStatus.FIT_FAILURE]
    mod_fails = [
        o for o in outcomes if o.status == EvaluationOutcomeStatus.MODEL_FAILURE
    ]
    inv_inputs = [
        o for o in outcomes if o.status == EvaluationOutcomeStatus.INVALID_INPUT
    ]
    not_apps = [
        o for o in outcomes if o.status == EvaluationOutcomeStatus.NOT_APPLICABLE
    ]

    accounting = _calculate_outcome_slot_accounting(outcomes, config)
    unique_units = accounting.unique_units
    expected_slots = accounting.expected_slots
    recorded_outcomes = accounting.recorded_outcomes
    duplicate_slots = accounting.duplicate_slots
    missing_slots = accounting.missing_slots

    status_table_lines = [
        "| Model | Success | Insufficient Data | Missing Calibration | Non-Convergence | Fit Failure | Invalid Input | Model Failure | Not Applicable | Total Recorded |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s in summaries:
        mod_outs = [o for o in outcomes if o.model_name == s.model_name]
        disp = s.model_name.replace("_", " ").title()
        n_succ = sum(1 for o in mod_outs if o.status == EvaluationOutcomeStatus.SUCCESS)
        n_insuf = sum(
            1 for o in mod_outs if o.status == EvaluationOutcomeStatus.INSUFFICIENT_DATA
        )
        n_miss = sum(
            1
            for o in mod_outs
            if o.status == EvaluationOutcomeStatus.MISSING_CALIBRATION
        )
        n_nonconv = sum(
            1 for o in mod_outs if o.status == EvaluationOutcomeStatus.NON_CONVERGENCE
        )
        n_fit = sum(
            1 for o in mod_outs if o.status == EvaluationOutcomeStatus.FIT_FAILURE
        )
        n_inv = sum(
            1 for o in mod_outs if o.status == EvaluationOutcomeStatus.INVALID_INPUT
        )
        n_mod = sum(
            1 for o in mod_outs if o.status == EvaluationOutcomeStatus.MODEL_FAILURE
        )
        n_notapp = sum(
            1 for o in mod_outs if o.status == EvaluationOutcomeStatus.NOT_APPLICABLE
        )
        status_table_lines.append(
            f"| {disp} | {n_succ} | {n_insuf} | {n_miss} | {n_nonconv} | {n_fit} | {n_inv} | {n_mod} | {n_notapp} | {len(mod_outs)} |"
        )
    status_table_lines.append(
        f"| **Total** | **{len(succs)}** | **{len(insufs)}** | **{len(miss_cals)}** | **{len(non_convs)}** | **{len(fit_fails)}** | **{len(inv_inputs)}** | **{len(mod_fails)}** | **{len(not_apps)}** | **{recorded_outcomes}** |"
    )

    lines.extend(
        [
            "---",
            "",
            "## 8. Negative, False Positive, and Failure Inventory",
            "",
            f"- **Unique Evaluation Units:** {unique_units}",
            f"- **Expected Model-Outcome Slots:** {expected_slots} ({unique_units} units × {len(config.model_names)} models)",
            f"- **Recorded Model Outcomes:** {recorded_outcomes}",
            f"- **Duplicate Model Outcomes:** {duplicate_slots}",
            f"- **Missing Model-Outcome Slots:** {missing_slots}",
            f"- **Total True Positives:** {sum(s.micro_confusion_counts.true_positives for s in summaries)} across all models and scenarios.",
            f"- **Total False Positives:** {len(fps)} across all models and scenarios.",
            f"- **Total False Negatives:** {len(fns)} across all models and scenarios.",
            f"- **Total True Negatives:** {sum(s.micro_confusion_counts.true_negatives for s in summaries)} across all models and scenarios.",
            "",
            "### 8.1 Detailed Outcome Status Breakdown",
            *status_table_lines,
            "",
            "---",
            "",
            "## 9. Limitations and Research Scope",
            "",
            "1. **Controlled Simulation Environment:** Results reflect synthetic telemetry generated by the AegisOps canonical microservice simulator and should not be construed as universal production benchmarks.",
            f"2. **Single Seed Split Evaluation:** Calibration and evaluation were run with fixed seeds (`{config.calibration_seed}` and `{config.evaluation_seed}`); statistical confidence bounds across multi-seed Monte Carlo runs remain future research.",
            f"3. **Operational Decision Threshold:** Threshold `{config.decision_policy.decision_threshold:.2f}` represents an operational standard corresponding to WARNING severity, not an analytically optimized decision point.",
            "4. **Enterprise Scope & Boundaries:** Production SLAs, horizontal scalability, causal diagnosis, and automated remediation are not evaluated in this benchmark and remain unverified.",
            "",
            "---",
            "",
            "## 10. Machine-Readable Artifact Index",
            "",
            "| Artifact Path | Format | Description |",
            "|---|---|---|",
        ]
    )

    has_rep = any(
        art.path.endswith("evaluation_report.md") for art in manifest.artifacts
    )
    for art in manifest.artifacts:
        lines.append(
            f"| `{art.path}` | `{art.media_type}` | Schema `{art.schema_version}` (SHA-256: `{art.sha256[:12]}...`) |"
        )
    if not has_rep:
        root_prefix = manifest.artifact_manifest_path.split("/results/")[0]
        rep_rel = f"{root_prefix}/reports/phase3/task_3_9/evaluation_report.md"
        lines.append(
            f"| `{rep_rel}` | `text/markdown` | Schema `1.0` (generated report) |"
        )
    lines.append(
        f"| `{manifest.artifact_manifest_path}` | `application/json` | Schema `{manifest.schema_version}` (`self_digest_policy: {manifest.self_digest_policy}`) |"
    )

    lines.extend(
        [
            "",
            "**Manifest Self-Digest Policy:**",
            f"The artifact manifest (`{manifest.artifact_manifest_path}`) digests all 11 other final artifacts in the package. Self-digestion of the manifest is intentionally excluded (`self_digest_policy: {manifest.self_digest_policy}`) because calculating a SHA-256 digest over a file that contains its own digest is mathematically recursive. The manifest digest is verified externally after generation.",
            "",
        ]
    )

    return "\n".join(lines)


def _guard_finite_serialization(data: Any, path: str = "$") -> None:
    if len(path) > 80:
        path = path[:77] + "..."
    if isinstance(data, dict):
        for k, v in data.items():
            k_str = str(k)
            if len(k_str) > 30:
                k_str = k_str[:27] + "..."
            _guard_finite_serialization(v, f"{path}.{k_str}")
    elif isinstance(data, (list, tuple)):
        for i, v in enumerate(data):
            _guard_finite_serialization(v, f"{path}[{i}]")
    elif hasattr(data, "model_dump") and callable(getattr(data, "model_dump")):
        _guard_finite_serialization(data.model_dump(), path)
    elif isinstance(data, float):
        if not math.isfinite(data):
            raise EvaluationArtifactError(f"Non-finite float detected at {path}")


def _encode_text_payload(text: str) -> bytes:
    """Encode text exactly as Path.write_text(..., encoding='utf-8') would."""
    buffer = io.BytesIO()
    wrapper = io.TextIOWrapper(
        buffer,
        encoding="utf-8",
        newline=None,
        write_through=True,
    )
    wrapper.write(text)
    wrapper.flush()
    payload = buffer.getvalue()
    wrapper.detach()
    return payload


async def run_phase3_evaluation(
    config: EvaluationHarnessConfig,
    base_dir: Path | None = None,
    package_created_at: datetime | None = None,
) -> tuple[EvaluationPackageManifest, list[ModelComparisonSummary]]:
    """High-level runner executing full Phase 3 evaluation and generating all required artifacts."""
    # ==================================================================
    # PHASE A — PREPARE AND VALIDATE EVERYTHING IN MEMORY
    # ==================================================================
    root_path = base_dir or Path.cwd()
    paths = resolve_evaluation_package_paths(root_path, config.artifact_root)

    harness = Phase3EvaluationHarness(config=config)
    outcomes, summaries, run_metadata = await harness.run_evaluation()

    # 1. Pre-write validation of outcome matrix and runtime lineage
    validate_evaluation_outcomes(outcomes, config)
    validate_evaluation_run_metadata(run_metadata, config)

    now_utc = package_created_at or datetime.now(timezone.utc)

    # 2. Finite-serialization guards on inputs and summaries
    _guard_finite_serialization(config, "$")
    _guard_finite_serialization(run_metadata, "$")
    for s_idx, s in enumerate(summaries):
        _guard_finite_serialization(s, f"summaries[{s_idx}]")

    # 3. Canonical sorting and finite checks for outcomes
    scen_order_map = {sid: i for i, sid in enumerate(CANONICAL_SCENARIO_ORDER)}
    mod_order_map = {m: i for i, m in enumerate(CANONICAL_MODEL_ORDER)}
    sorted_outcomes = sorted(
        outcomes,
        key=lambda o: (
            scen_order_map.get(o.scenario_id, 999),
            mod_order_map.get(o.model_name, 999),
            o.unit_index,
            o.event_time,
            str(o.outcome_id),
        ),
    )
    for i, out in enumerate(sorted_outcomes):
        _guard_finite_serialization(out, f"outcomes[{i}]")

    # 4. In-memory construction and serialization of experiment configs
    p_exp = AnomalyExperimentConfig(
        schema_version="1.0",
        experiment_id=uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{config.package_id}:prophet:{config.code_revision}",
        ),
        name="prophet-canonical-evaluation",
        description="Phase 3 Prophet baseline evaluation across canonical simulator scenarios",
        dataset=DatasetReference(
            dataset_id="canonical-simulator-v1",
            dataset_version="1.0.0",
            dataset_split="evaluation",
            location_reference=paths.relative_artifact_path(
                "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl"
            ),
        ),
        scenario=ScenarioReference(
            scenario_id="all-canonical-scenarios",
            scenario_version="1.0.0",
            config_reference="app/simulator/scenarios/catalogue.py",
        ),
        reproducibility=ExperimentReproducibility(
            seed=config.evaluation_seed,
            code_revision=config.code_revision,
            environment=config.environment,
            conditions={
                "calibration_seed": config.calibration_seed,
                "evaluation_seed": config.evaluation_seed,
            },
        ),
        feature_window=config.feature_window,
        model=ModelSpecification(
            model_name="prophet",
            model_version=config.prophet_config.model_version,
            calibration_method="empirical_cdf_percentile",
            calibration_version=config.calibration_config.calibration_method_version,
        ),
        evaluation=EvaluationDeclaration(
            requested_metrics=config.requested_metrics,
            target_service="canonical-topology",
            ground_truth_reference="app/simulator/ground_truth",
        ),
        outputs=OutputDeclaration(
            artifact_root=paths.relative_artifact_path(
                "results/processed/phase3/task_3_9"
            ),
            requested_outputs=["model_comparison.json", "model_comparison.csv"],
        ),
    )
    if_exp = AnomalyExperimentConfig(
        schema_version="1.0",
        experiment_id=uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{config.package_id}:isolation_forest:{config.code_revision}",
        ),
        name="isolation-forest-canonical-evaluation",
        description="Phase 3 Isolation Forest baseline evaluation across canonical simulator scenarios",
        dataset=DatasetReference(
            dataset_id="canonical-simulator-v1",
            dataset_version="1.0.0",
            dataset_split="evaluation",
            location_reference=paths.relative_artifact_path(
                "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl"
            ),
        ),
        scenario=ScenarioReference(
            scenario_id="all-canonical-scenarios",
            scenario_version="1.0.0",
            config_reference="app/simulator/scenarios/catalogue.py",
        ),
        reproducibility=ExperimentReproducibility(
            seed=config.evaluation_seed,
            code_revision=config.code_revision,
            environment=config.environment,
            conditions={
                "calibration_seed": config.calibration_seed,
                "evaluation_seed": config.evaluation_seed,
            },
        ),
        feature_window=config.feature_window,
        model=ModelSpecification(
            model_name="isolation_forest",
            model_version=config.isolation_forest_config.model_version,
            calibration_method="empirical_cdf_percentile",
            calibration_version=config.calibration_config.calibration_method_version,
        ),
        evaluation=EvaluationDeclaration(
            requested_metrics=config.requested_metrics,
            target_service="canonical-topology",
            ground_truth_reference="app/simulator/ground_truth",
        ),
        outputs=OutputDeclaration(
            artifact_root=paths.relative_artifact_path(
                "results/processed/phase3/task_3_9"
            ),
            requested_outputs=["model_comparison.json", "model_comparison.csv"],
        ),
    )
    ae_exp = AnomalyExperimentConfig(
        schema_version="1.0",
        experiment_id=uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{config.package_id}:autoencoder:{config.code_revision}",
        ),
        name="autoencoder-canonical-evaluation",
        description="Phase 3 Autoencoder baseline evaluation across canonical simulator scenarios",
        dataset=DatasetReference(
            dataset_id="canonical-simulator-v1",
            dataset_version="1.0.0",
            dataset_split="evaluation",
            location_reference=paths.relative_artifact_path(
                "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl"
            ),
        ),
        scenario=ScenarioReference(
            scenario_id="all-canonical-scenarios",
            scenario_version="1.0.0",
            config_reference="app/simulator/scenarios/catalogue.py",
        ),
        reproducibility=ExperimentReproducibility(
            seed=config.evaluation_seed,
            code_revision=config.code_revision,
            environment=config.environment,
            conditions={
                "calibration_seed": config.calibration_seed,
                "evaluation_seed": config.evaluation_seed,
            },
        ),
        feature_window=config.feature_window,
        model=ModelSpecification(
            model_name="autoencoder",
            model_version=config.autoencoder_config.model_version,
            calibration_method="empirical_cdf_percentile",
            calibration_version=config.calibration_config.calibration_method_version,
        ),
        evaluation=EvaluationDeclaration(
            requested_metrics=config.requested_metrics,
            target_service="canonical-topology",
            ground_truth_reference="app/simulator/ground_truth",
        ),
        outputs=OutputDeclaration(
            artifact_root=paths.relative_artifact_path(
                "results/processed/phase3/task_3_9"
            ),
            requested_outputs=["model_comparison.json", "model_comparison.csv"],
        ),
    )

    prophet_exp_bytes = serialize_experiment_config(p_exp)
    iforest_exp_bytes = serialize_experiment_config(if_exp)
    autoencoder_exp_bytes = serialize_experiment_config(ae_exp)
    comp_manifest_bytes = _encode_text_payload(config.model_dump_json(indent=2))

    # 5. Raw outcomes JSONL payload
    raw_jsonl_bytes = _encode_text_payload(
        "\n".join(out.model_dump_json() for out in sorted_outcomes) + "\n"
    )

    # 6. Processed model_comparison.json payload
    metadata_payload: dict[str, Any] = {
        **run_metadata.model_dump(mode="json"),
        "package_id": config.package_id,
        "package_version": config.package_version,
        "code_revision": config.code_revision,
        "created_at": now_utc.isoformat(),
        "environment": config.environment,
        "artifact_root": paths.normalized_root,
        "calibration_seed": config.calibration_seed,
        "evaluation_seed": config.evaluation_seed,
        "calibration_partition_id": config.calibration_partition_id,
        "evaluation_partition_id": config.evaluation_partition_id,
        "calibration_cutoff_seconds": config.calibration_cutoff_seconds,
        "decision_policy": config.decision_policy.model_dump(mode="json"),
        "label_policy": config.label_policy.model_dump(mode="json"),
        "feature_window": config.feature_window.model_dump(mode="json"),
        "calibration_config": config.calibration_config.model_dump(mode="json"),
        "scenario_ids": list(config.scenario_ids),
        "model_names": list(config.model_names),
        "canonical_scenario_order": list(CANONICAL_SCENARIO_ORDER),
        "canonical_model_order": list(CANONICAL_MODEL_ORDER),
    }
    comp_data = {
        "schema_version": SUPPORTED_EVALUATION_SCHEMA_VERSION,
        "package_id": config.package_id,
        "package_version": config.package_version,
        "code_revision": config.code_revision,
        "created_at": now_utc.isoformat(),
        "decision_threshold": config.decision_policy.decision_threshold,
        "models": [s.model_dump(mode="json") for s in summaries],
        "metadata": metadata_payload,
    }
    _guard_finite_serialization(comp_data, "$")
    model_comp_json_bytes = _encode_text_payload(
        json.dumps(comp_data, indent=2, allow_nan=False)
    )

    # 7. Processed model_comparison.csv payload
    model_csv_buf = io.StringIO()
    model_csv_writer = csv.writer(model_csv_buf, lineterminator="\r\n")
    model_csv_writer.writerow(
        [
            "model_name",
            "model_version",
            "total_units",
            "true_positives",
            "false_positives",
            "true_negatives",
            "false_negatives",
            "precision",
            "recall",
            "f1",
            "false_positive_rate",
            "scenarios_detected",
            "detection_latency_mean_sec",
            "detection_latency_median_sec",
            "detection_latency_p95_sec",
            "success_units",
            "insufficient_units",
            "failure_units",
        ]
    )
    for s in summaries:
        c = s.micro_confusion_counts
        lat_sum = s.latency_summary
        model_csv_writer.writerow(
            [
                s.model_name,
                s.model_version,
                c.total_units,
                c.true_positives,
                c.false_positives,
                c.true_negatives,
                c.false_negatives,
                s.micro_precision.value
                if s.micro_precision.value is not None
                else "undefined",
                s.micro_recall.value
                if s.micro_recall.value is not None
                else "undefined",
                s.micro_f1.value if s.micro_f1.value is not None else "undefined",
                s.micro_false_positive_rate.value
                if s.micro_false_positive_rate.value is not None
                else "undefined",
                lat_sum.contributing_scenario_count,
                lat_sum.mean_seconds if lat_sum.mean_seconds is not None else "",
                lat_sum.median_seconds if lat_sum.median_seconds is not None else "",
                lat_sum.p95_seconds if lat_sum.p95_seconds is not None else "",
                c.success_units,
                c.insufficient_units,
                c.failure_units,
            ]
        )
    model_comp_csv_bytes = model_csv_buf.getvalue().encode("utf-8")

    # 8. Processed scenario_comparison.csv payload
    scen_csv_buf = io.StringIO()
    scen_csv_writer = csv.writer(scen_csv_buf, lineterminator="\r\n")
    scen_csv_writer.writerow(
        [
            "scenario_id",
            "model_name",
            "total_units",
            "true_positives",
            "false_positives",
            "true_negatives",
            "false_negatives",
            "precision",
            "recall",
            "f1",
            "false_positive_rate",
            "detection_status",
            "detection_latency_sec",
        ]
    )
    for scen_id in config.scenario_ids:
        for s in summaries:
            scen_eval = next(
                (e for e in s.scenario_evaluations if e.scenario_id == scen_id),
                None,
            )
            if not scen_eval:
                continue
            c = scen_eval.confusion_counts
            scen_csv_writer.writerow(
                [
                    scen_id,
                    s.model_name,
                    c.total_units,
                    c.true_positives,
                    c.false_positives,
                    c.true_negatives,
                    c.false_negatives,
                    scen_eval.precision.value
                    if scen_eval.precision.value is not None
                    else "undefined",
                    scen_eval.recall.value
                    if scen_eval.recall.value is not None
                    else "undefined",
                    scen_eval.f1.value
                    if scen_eval.f1.value is not None
                    else "undefined",
                    scen_eval.false_positive_rate.value
                    if scen_eval.false_positive_rate.value is not None
                    else "undefined",
                    scen_eval.detection_latency.status,
                    scen_eval.detection_latency.latency_seconds
                    if scen_eval.detection_latency.latency_seconds is not None
                    else "",
                ]
            )
    scen_comp_csv_bytes = scen_csv_buf.getvalue().encode("utf-8")

    # 9. Figures payloads
    fig_quality_bytes = _encode_text_payload(generate_model_quality_svg(summaries))
    fig_latency_bytes = _encode_text_payload(
        generate_detection_latency_svg(summaries, config.scenario_ids)
    )

    # 10. Initial 10 Artifact Manifest Entries (computed from exact bytes)
    artifact_entries: list[ArtifactManifestEntry] = [
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "experiments/phase3/task_3_9/comparison_manifest.json"
            ),
            media_type="application/json",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=None,
            sha256=hashlib.sha256(comp_manifest_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "experiments/phase3/task_3_9/prophet_experiment.json"
            ),
            media_type="application/json",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=None,
            sha256=hashlib.sha256(prophet_exp_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "experiments/phase3/task_3_9/isolation_forest_experiment.json"
            ),
            media_type="application/json",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=None,
            sha256=hashlib.sha256(iforest_exp_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "experiments/phase3/task_3_9/autoencoder_experiment.json"
            ),
            media_type="application/json",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=None,
            sha256=hashlib.sha256(autoencoder_exp_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "results/raw/phase3/task_3_9/evaluation_outcomes.jsonl"
            ),
            media_type="application/x-ndjson",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=len(sorted_outcomes),
            sha256=hashlib.sha256(raw_jsonl_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "results/processed/phase3/task_3_9/model_comparison.json"
            ),
            media_type="application/json",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=len(summaries),
            sha256=hashlib.sha256(model_comp_json_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "results/processed/phase3/task_3_9/model_comparison.csv"
            ),
            media_type="text/csv",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=len(summaries),
            sha256=hashlib.sha256(model_comp_csv_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "results/processed/phase3/task_3_9/scenario_comparison.csv"
            ),
            media_type="text/csv",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=len(config.scenario_ids) * len(config.model_names),
            sha256=hashlib.sha256(scen_comp_csv_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "results/figures/phase3/task_3_9/model_quality_metrics.svg"
            ),
            media_type="image/svg+xml",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=None,
            sha256=hashlib.sha256(fig_quality_bytes).hexdigest(),
        ),
        ArtifactManifestEntry(
            path=paths.relative_artifact_path(
                "results/figures/phase3/task_3_9/detection_latency_by_scenario.svg"
            ),
            media_type="image/svg+xml",
            schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
            record_count=None,
            sha256=hashlib.sha256(fig_latency_bytes).hexdigest(),
        ),
    ]

    manifest_rel_path = paths.relative_artifact_path(
        "results/processed/phase3/task_3_9/artifact_manifest.json"
    )
    manifest_for_report = EvaluationPackageManifest.model_construct(
        schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
        package_id=config.package_id,
        package_version=config.package_version,
        code_revision=config.code_revision,
        created_at=now_utc,
        environment=config.environment,
        calibration_seed=config.calibration_seed,
        evaluation_seed=config.evaluation_seed,
        decision_threshold=config.decision_policy.decision_threshold,
        models=list(config.model_names),
        scenarios=list(config.scenario_ids),
        artifact_manifest_path=manifest_rel_path,
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=artifact_entries,
    )

    # 11. Report generation and encoding
    report_text = generate_evaluation_report(
        manifest_for_report,
        summaries,
        sorted_outcomes,
        config,
        run_metadata,
    )
    report_bytes = _encode_text_payload(report_text)
    report_sha256 = hashlib.sha256(report_bytes).hexdigest()

    # 12. Final 11-Entry Package Manifest
    rep_entry = ArtifactManifestEntry(
        path=paths.relative_artifact_path(
            "reports/phase3/task_3_9/evaluation_report.md"
        ),
        media_type="text/markdown",
        schema_version=SUPPORTED_ARTIFACT_ENTRY_SCHEMA_VERSION,
        record_count=None,
        sha256=report_sha256,
    )
    final_manifest = EvaluationPackageManifest(
        schema_version=SUPPORTED_EVALUATION_SCHEMA_VERSION,
        package_id=config.package_id,
        package_version=config.package_version,
        code_revision=config.code_revision,
        created_at=now_utc,
        environment=config.environment,
        calibration_seed=config.calibration_seed,
        evaluation_seed=config.evaluation_seed,
        decision_threshold=config.decision_policy.decision_threshold,
        models=list(config.model_names),
        scenarios=list(config.scenario_ids),
        artifact_manifest_path=manifest_rel_path,
        self_digest_policy="excluded_from_manifest_digest",
        artifacts=[*artifact_entries, rep_entry],
    )
    _guard_finite_serialization(final_manifest, "$")
    manifest_json_bytes = _encode_text_payload(final_manifest.model_dump_json(indent=2))

    # ==================================================================
    # PHASE B — PERSISTENCE (Only after 100% of preparation & validation)
    # ==================================================================
    exp_dir = paths.experiments_dir
    raw_dir = paths.raw_dir
    proc_dir = paths.processed_dir
    fig_dir = paths.figures_dir
    rep_dir = paths.reports_dir

    for d in (exp_dir, raw_dir, proc_dir, fig_dir, rep_dir):
        d.mkdir(parents=True, exist_ok=True)

    (exp_dir / "comparison_manifest.json").write_bytes(comp_manifest_bytes)
    (exp_dir / "prophet_experiment.json").write_bytes(prophet_exp_bytes)
    (exp_dir / "isolation_forest_experiment.json").write_bytes(iforest_exp_bytes)
    (exp_dir / "autoencoder_experiment.json").write_bytes(autoencoder_exp_bytes)
    (raw_dir / "evaluation_outcomes.jsonl").write_bytes(raw_jsonl_bytes)
    (proc_dir / "model_comparison.json").write_bytes(model_comp_json_bytes)
    (proc_dir / "model_comparison.csv").write_bytes(model_comp_csv_bytes)
    (proc_dir / "scenario_comparison.csv").write_bytes(scen_comp_csv_bytes)
    (fig_dir / "model_quality_metrics.svg").write_bytes(fig_quality_bytes)
    (fig_dir / "detection_latency_by_scenario.svg").write_bytes(fig_latency_bytes)
    (rep_dir / "evaluation_report.md").write_bytes(report_bytes)
    (proc_dir / "artifact_manifest.json").write_bytes(manifest_json_bytes)

    return final_manifest, summaries
