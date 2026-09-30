from unittest.mock import MagicMock
import uuid

import pytest
from pydantic import ValidationError

from app.anomaly.errors import (
    AnomalyExperimentDeserializationError,
    AnomalyExperimentSerializationError,
)
from app.anomaly.experiment import (
    SUPPORTED_EXPERIMENT_SCHEMA_VERSION,
    AnomalyExperimentConfig,
    DatasetReference,
    EvaluationDeclaration,
    ExperimentReproducibility,
    FeatureWindowConfig,
    ModelSpecification,
    OutputDeclaration,
    ScenarioReference,
    deserialize_experiment_config,
    serialize_experiment_config,
)
from app.anomaly.models import MAX_UINT64


def _sample_dataset() -> DatasetReference:
    return DatasetReference(
        dataset_id="aegis-telemetry-corpus",
        dataset_version="v1.0.0",
        dataset_split="evaluation",
        location_reference="datasets/v1/telemetry_corpus.parquet",
    )


def _sample_scenario() -> ScenarioReference:
    return ScenarioReference(
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        config_reference="configs/scenarios/cpu_saturation.json",
    )


def _sample_reproducibility() -> ExperimentReproducibility:
    return ExperimentReproducibility(
        seed=42,
        code_revision="517551f6f5f37c03ccc6bb54608bd12e1f78e8ac",
        environment="simulation",
        reproducibility_key="rep-key-42-cpu",
        conditions={"warmup_seconds": 30, "sample_rate_hz": 1.0},
    )


def _sample_feature_window() -> FeatureWindowConfig:
    return FeatureWindowConfig(
        feature_config_id="multivariate-latency-cpu",
        feature_names=["http_request_latency_ms", "cpu_usage_pct", "queue_depth"],
        window_size_seconds=60.0,
        step_size_seconds=10.0,
        aggregation_methods=["mean", "p95", "std"],
        imputation_strategy="forward_fill",
    )


def _sample_model(model_name: str = "isolation_forest") -> ModelSpecification:
    if model_name == "prophet":
        hyperparameters = {
            "changepoint_prior_scale": 0.05,
            "seasonality_prior_scale": 10.0,
            "seasonality_mode": "additive",
        }
    elif model_name == "autoencoder":
        hyperparameters = {
            "encoder_layers": [32, 16],
            "latent_dim": 8,
            "learning_rate": 0.001,
            "epochs": 50,
        }
    else:
        hyperparameters = {
            "n_estimators": 100,
            "contamination": 0.01,
            "max_samples": "auto",
        }

    return ModelSpecification(
        model_name=model_name,
        model_version="1.0.0",
        hyperparameters=hyperparameters,
        calibration_method="quantile_calibration",
        calibration_version="v1.0.0",
        calibration_parameters={"alpha": 0.05, "target_fpr": 0.01},
    )


def _sample_evaluation() -> EvaluationDeclaration:
    return EvaluationDeclaration(
        requested_metrics=["precision", "recall", "f1_score", "detection_delay_seconds"],
        target_service="order-service",
        ground_truth_reference="research/ground_truth/cpu_saturation_truth.json",
    )


def _sample_outputs() -> OutputDeclaration:
    return OutputDeclaration(
        artifact_root="research/results/phase3/exp_cpu_saturation_01",
        requested_outputs=["anomaly_signals", "evaluation_summary", "residual_timeseries"],
        save_intermediate_features=False,
    )


def _sample_config_kwargs(model_name: str = "isolation_forest") -> dict:
    return {
        "name": f"experiment-{model_name}-cpu-saturation",
        "description": "Baseline comparative anomaly detection experiment on cpu-saturation",
        "dataset": _sample_dataset(),
        "scenario": _sample_scenario(),
        "reproducibility": _sample_reproducibility(),
        "feature_window": _sample_feature_window(),
        "model": _sample_model(model_name),
        "evaluation": _sample_evaluation(),
        "outputs": _sample_outputs(),
        "metadata": {"researcher": "aegis-benchmark-suite", "phase": "3"},
    }


def test_valid_minimum_configuration() -> None:
    kwargs = _sample_config_kwargs()
    del kwargs["description"]
    del kwargs["metadata"]

    config = AnomalyExperimentConfig(**kwargs)

    assert config.schema_version == SUPPORTED_EXPERIMENT_SCHEMA_VERSION
    assert isinstance(config.experiment_id, uuid.UUID)
    assert config.name == "experiment-isolation_forest-cpu-saturation"
    assert config.description == ""
    assert config.metadata == {}
    assert config.dataset.dataset_id == "aegis-telemetry-corpus"
    assert config.model.model_name == "isolation_forest"


@pytest.mark.parametrize("model_name", ["prophet", "isolation_forest", "autoencoder"])
def test_valid_fully_populated_configuration_across_models(model_name: str) -> None:
    kwargs = _sample_config_kwargs(model_name=model_name)
    exp_id = uuid.uuid4()
    kwargs["experiment_id"] = exp_id

    config = AnomalyExperimentConfig(**kwargs)

    assert config.experiment_id == exp_id
    assert config.model.model_name == model_name
    assert len(config.feature_window.feature_names) == 3
    assert len(config.evaluation.requested_metrics) == 4
    assert config.outputs.save_intermediate_features is False
    assert config.reproducibility.seed == 42


@pytest.mark.parametrize(
    "missing_section",
    [
        "name",
        "dataset",
        "scenario",
        "reproducibility",
        "feature_window",
        "model",
        "evaluation",
        "outputs",
    ],
)
def test_missing_required_sections_rejection(missing_section: str) -> None:
    kwargs = _sample_config_kwargs()
    del kwargs[missing_section]
    with pytest.raises(ValidationError):
        AnomalyExperimentConfig(**kwargs)


@pytest.mark.parametrize("invalid_version", ["2.0", "0.9", "v1", "", " "])
def test_invalid_schema_version_rejection(invalid_version: str) -> None:
    kwargs = _sample_config_kwargs()
    kwargs["schema_version"] = invalid_version
    with pytest.raises(ValidationError):
        AnomalyExperimentConfig(**kwargs)


@pytest.mark.parametrize(
    "empty_val",
    ["", "   ", "\t\n"],
)
def test_empty_identity_fields_rejection(empty_val: str) -> None:
    # Empty name
    kwargs = _sample_config_kwargs()
    kwargs["name"] = empty_val
    with pytest.raises(ValidationError):
        AnomalyExperimentConfig(**kwargs)

    # Empty dataset ID
    with pytest.raises(ValidationError):
        DatasetReference(
            dataset_id=empty_val,
            dataset_version="v1",
            location_reference="datasets/v1/test.parquet",
        )

    # Empty scenario ID
    with pytest.raises(ValidationError):
        ScenarioReference(
            scenario_id=empty_val,
            scenario_version="v1",
            config_reference="configs/scenarios/test.json",
        )


@pytest.mark.parametrize("valid_seed", [0, 42, 1000, MAX_UINT64])
def test_valid_seed_bounds(valid_seed: int) -> None:
    repro = ExperimentReproducibility(
        seed=valid_seed,
        code_revision="commit123",
        environment="simulation",
    )
    assert repro.seed == valid_seed


@pytest.mark.parametrize("invalid_seed", [-1, -100, MAX_UINT64 + 1])
def test_invalid_seed_bounds_rejection(invalid_seed: int) -> None:
    with pytest.raises(ValidationError):
        ExperimentReproducibility(
            seed=invalid_seed,
            code_revision="commit123",
            environment="simulation",
        )


def test_feature_window_validation() -> None:
    # Valid window
    fw = FeatureWindowConfig(
        feature_config_id="features-v1",
        feature_names=["latency", "cpu"],
        window_size_seconds=60.0,
        step_size_seconds=10.0,
    )
    assert fw.window_size_seconds == 60.0
    assert fw.step_size_seconds == 10.0

    # step_size > window_size rejected
    with pytest.raises(ValidationError, match="step_size_seconds .* cannot be greater than window_size_seconds"):
        FeatureWindowConfig(
            feature_config_id="features-v1",
            feature_names=["latency"],
            window_size_seconds=10.0,
            step_size_seconds=30.0,
        )

    # Non-positive or non-finite window rejected
    with pytest.raises(ValidationError):
        FeatureWindowConfig(
            feature_config_id="features-v1",
            feature_names=["latency"],
            window_size_seconds=0.0,
            step_size_seconds=10.0,
        )
    with pytest.raises(ValidationError):
        FeatureWindowConfig(
            feature_config_id="features-v1",
            feature_names=["latency"],
            window_size_seconds=float("nan"),
            step_size_seconds=10.0,
        )
    with pytest.raises(ValidationError):
        FeatureWindowConfig(
            feature_config_id="features-v1",
            feature_names=["latency"],
            window_size_seconds=float("inf"),
            step_size_seconds=10.0,
        )

    # Empty feature names rejected
    with pytest.raises(ValidationError):
        FeatureWindowConfig(
            feature_config_id="features-v1",
            feature_names=[],
            window_size_seconds=60.0,
            step_size_seconds=10.0,
        )
    with pytest.raises(ValidationError):
        FeatureWindowConfig(
            feature_config_id="features-v1",
            feature_names=["  "],
            window_size_seconds=60.0,
            step_size_seconds=10.0,
        )


def test_evaluation_and_output_declaration_validation() -> None:
    # Empty requested metrics rejected
    with pytest.raises(ValidationError):
        EvaluationDeclaration(
            requested_metrics=[],
            target_service="order-service",
            ground_truth_reference="research/ground_truth/truth.json",
        )

    # Metric containing embedded measured values rejected (e.g. "f1=0.92")
    with pytest.raises(ValidationError, match="must declare metric names, not measured values"):
        EvaluationDeclaration(
            requested_metrics=["f1_score=0.95"],
            target_service="order-service",
            ground_truth_reference="research/ground_truth/truth.json",
        )

    # Empty requested outputs rejected
    with pytest.raises(ValidationError):
        OutputDeclaration(
            artifact_root="research/results/phase3/exp1",
            requested_outputs=[],
        )


@pytest.mark.parametrize(
    "absolute_path",
    [
        "C:\\Users\\User\\datasets\\data.parquet",
        "D:/AegisOps/research/configs/scenario.json",
        "c:/test/data",
        "/home/user/datasets/data.parquet",
        "/var/log/telemetry",
        "/tmp/results",
        "\\\\server\\share\\data",
    ],
)
def test_machine_specific_absolute_path_rejection(absolute_path: str) -> None:
    # In DatasetReference
    with pytest.raises(ValidationError, match="absolute path"):
        DatasetReference(
            dataset_id="corpus",
            dataset_version="v1",
            location_reference=absolute_path,
        )

    # In ScenarioReference
    with pytest.raises(ValidationError, match="absolute path"):
        ScenarioReference(
            scenario_id="cpu",
            scenario_version="v1",
            config_reference=absolute_path,
        )

    # In EvaluationDeclaration
    with pytest.raises(ValidationError, match="absolute path"):
        EvaluationDeclaration(
            requested_metrics=["f1_score"],
            target_service="order-service",
            ground_truth_reference=absolute_path,
        )

    # In OutputDeclaration
    with pytest.raises(ValidationError, match="absolute path"):
        OutputDeclaration(
            artifact_root=absolute_path,
            requested_outputs=["signals"],
        )


@pytest.mark.parametrize(
    "secret_sample",
    [
        "https://user:password123@storage.com/data",
        "api_key=sk-1234567890abcdef",
        "access_token=bearer xyz987",
        "private_key=MIIEvgIBADANBgkqhki",
        "client_secret=secret999",
    ],
)
def test_secret_bearing_value_rejection(secret_sample: str) -> None:
    # In Dataset location
    with pytest.raises(ValidationError, match="secret credential pattern"):
        DatasetReference(
            dataset_id="corpus",
            dataset_version="v1",
            location_reference=f"datasets/v1/?{secret_sample}",
        )

    # In Reproducibility conditions
    with pytest.raises(ValidationError, match="secret credential pattern"):
        ExperimentReproducibility(
            seed=42,
            code_revision="commit1",
            environment="sim",
            conditions={"auth": secret_sample},
        )

    # In Config Metadata
    kwargs = _sample_config_kwargs()
    kwargs["metadata"] = {"api_key": "12345"}
    with pytest.raises(ValidationError, match="secret credential pattern"):
        AnomalyExperimentConfig(**kwargs)


@pytest.mark.parametrize(
    "forbidden_key",
    [
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
    ],
)
def test_measured_results_rejection(forbidden_key: str) -> None:
    # In Config metadata
    kwargs = _sample_config_kwargs()
    kwargs["metadata"] = {forbidden_key: {"f1": 0.95}}
    with pytest.raises(ValidationError, match="measured result field"):
        AnomalyExperimentConfig(**kwargs)

    # In Model hyperparameters
    with pytest.raises(ValidationError, match="measured result field"):
        ModelSpecification(
            model_name="prophet",
            model_version="1.0",
            hyperparameters={forbidden_key: [0.1, 0.9]},
            calibration_method="static",
            calibration_version="v1",
        )

    # In Reproducibility conditions
    with pytest.raises(ValidationError, match="measured result field"):
        ExperimentReproducibility(
            seed=42,
            code_revision="commit1",
            environment="sim",
            conditions={forbidden_key: 10},
        )


def test_config_immutability() -> None:
    config = AnomalyExperimentConfig(**_sample_config_kwargs())
    with pytest.raises(ValidationError):
        config.name = "new-name"  # type: ignore[misc]


def test_deterministic_serialization_and_round_trip() -> None:
    exp_id = uuid.UUID("44444444-4444-4444-4444-444444444444")
    kwargs = _sample_config_kwargs(model_name="autoencoder")
    kwargs["experiment_id"] = exp_id

    config1 = AnomalyExperimentConfig(**kwargs)
    config2 = AnomalyExperimentConfig(**kwargs)

    # Serialization round trip
    raw_bytes = serialize_experiment_config(config1)
    assert isinstance(raw_bytes, bytes)
    assert len(raw_bytes) > 0

    restored = deserialize_experiment_config(raw_bytes)
    assert restored == config1
    assert restored.experiment_id == exp_id
    assert restored.model.model_name == "autoencoder"
    assert restored.dataset.dataset_id == "aegis-telemetry-corpus"
    assert restored.feature_window.window_size_seconds == 60.0
    assert restored.evaluation.requested_metrics == ["precision", "recall", "f1_score", "detection_delay_seconds"]

    # Determinism
    raw_bytes2 = serialize_experiment_config(config2)
    assert raw_bytes == raw_bytes2


def test_deserialization_unsupported_version_rejected() -> None:
    config = AnomalyExperimentConfig(**_sample_config_kwargs())
    data = config.model_dump(mode="json")
    data["schema_version"] = "2.0"

    import json
    raw = json.dumps(data).encode("utf-8")
    with pytest.raises(AnomalyExperimentDeserializationError, match="Unsupported schema_version"):
        deserialize_experiment_config(raw)


@pytest.mark.parametrize("invalid_raw", [b"INVALID_JSON", b"12345", b'"just_a_string"', b"[1, 2, 3]"])
def test_deserialization_malformed_json_rejected(invalid_raw: bytes) -> None:
    with pytest.raises(AnomalyExperimentDeserializationError):
        deserialize_experiment_config(invalid_raw)


def test_serialization_failure_wraps_in_anomaly_experiment_serialization_error() -> None:
    mock_config = MagicMock(spec=AnomalyExperimentConfig)
    mock_config.model_dump_json.side_effect = RuntimeError("Mock dump failure")
    mock_config.experiment_id = uuid.uuid4()

    with pytest.raises(AnomalyExperimentSerializationError, match="Failed to serialize AnomalyExperimentConfig"):
        serialize_experiment_config(mock_config)  # type: ignore[arg-type]
