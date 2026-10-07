import uuid


class AnomalyError(Exception):
    """Base exception for anomaly module errors."""


class AnomalyValidationError(AnomalyError):
    """Raised when an anomaly signal or configuration fails contract validation."""


class AnomalySerializationError(AnomalyError):
    """Raised when an anomaly entity fails serialization."""


class AnomalyDeserializationError(AnomalyError):
    """Raised when deserializing an anomaly entity fails."""


class AnomalyExperimentError(AnomalyError):
    """Base exception for anomaly experiment configuration errors."""


class AnomalyExperimentValidationError(AnomalyValidationError, AnomalyExperimentError):
    """Raised when an AnomalyExperimentConfig fails validation."""


class AnomalyExperimentSerializationError(
    AnomalySerializationError, AnomalyExperimentError
):
    """Raised when serializing an AnomalyExperimentConfig fails."""


class AnomalyExperimentDeserializationError(
    AnomalyDeserializationError, AnomalyExperimentError
):
    """Raised when deserializing an AnomalyExperimentConfig fails."""


class AnomalyFeatureError(AnomalyError):
    """Base exception for feature extraction and windowing errors."""


class FeatureExtractionError(AnomalyFeatureError):
    """Raised when feature extraction fails unexpectedly."""


class InvalidFeatureInputError(AnomalyFeatureError, AnomalyValidationError):
    """Raised when input observations or telemetry records are malformed or invalid."""


class InsufficientDataError(AnomalyFeatureError):
    """Raised when input observations do not satisfy minimum data requirements."""


class WindowingError(AnomalyFeatureError):
    """Raised when observation windowing parameters or slicing fail."""


class ProphetError(AnomalyError):
    """Base exception for Prophet baseline model errors."""


class ProphetConfigurationError(ProphetError, AnomalyValidationError):
    """Raised when Prophet baseline configuration fails contract validation."""


class ProphetFitError(ProphetError):
    """Raised when Prophet model fitting or forecasting fails."""


class ProphetInsufficientHistoryError(ProphetError, InsufficientDataError):
    """Raised when training history is insufficient to fit Prophet baseline."""


class IsolationForestError(AnomalyError):
    """Base exception for Isolation Forest baseline model errors."""


class IsolationForestConfigurationError(IsolationForestError, AnomalyValidationError):
    """Raised when Isolation Forest baseline configuration fails contract validation."""


class IsolationForestFitError(IsolationForestError):
    """Raised when Isolation Forest model fitting or scoring fails."""


class IsolationForestInsufficientHistoryError(
    IsolationForestError, InsufficientDataError
):
    """Raised when training history is insufficient to fit Isolation Forest baseline."""


class AutoencoderError(AnomalyError):
    """Base exception for Autoencoder baseline model errors."""


class AutoencoderConfigurationError(AutoencoderError, AnomalyValidationError):
    """Raised when Autoencoder baseline configuration fails contract validation."""


class AutoencoderFitError(AutoencoderError):
    """Raised when Autoencoder model fitting or reconstruction fails."""


class AutoencoderInsufficientHistoryError(AutoencoderError, InsufficientDataError):
    """Raised when training history is insufficient to fit Autoencoder baseline."""


class AutoencoderArtifactError(AutoencoderError):
    """Raised when Autoencoder artifact validation, serialization, or loading fails."""


class CalibrationError(AnomalyError):
    """Base exception for calibration and severity mapping errors."""


class CalibrationConfigurationError(CalibrationError, AnomalyValidationError):
    """Raised when calibration configuration fails contract validation."""


class CalibrationReferenceError(CalibrationError):
    """Raised when calibration reference data is malformed, invalid, or insufficient."""


class MissingCalibrationError(CalibrationError):
    """Raised when calibration is requested for a model lacking fitted calibration parameters."""


class EvaluationError(AnomalyError):
    """Base exception for anomaly evaluation errors."""


class EvaluationConfigurationError(EvaluationError, AnomalyValidationError):
    """Raised when evaluation configuration fails contract validation."""


class EvaluationExecutionError(EvaluationError):
    """Raised when evaluation pipeline execution fails."""


class EvaluationArtifactError(EvaluationError):
    """Raised when evaluation artifacts cannot be read or written."""


class AnomalyPublicationError(AnomalyError):
    """Raised when an anomaly signal cannot be published."""

    def __init__(
        self,
        *,
        signal_id: uuid.UUID,
        run_id: uuid.UUID,
        scenario_id: str,
        cause: Exception,
    ) -> None:
        super().__init__("Failed to publish anomaly signal")
        self.signal_id = signal_id
        self.run_id = run_id
        self.scenario_id = scenario_id
        self.cause = cause


class AnomalyHandoffError(AnomalyError):
    """Raised when a consumed anomaly envelope violates the Phase 4 handoff contract."""
