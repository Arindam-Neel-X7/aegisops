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


class AnomalyExperimentSerializationError(AnomalySerializationError, AnomalyExperimentError):
    """Raised when serializing an AnomalyExperimentConfig fails."""


class AnomalyExperimentDeserializationError(AnomalyDeserializationError, AnomalyExperimentError):
    """Raised when deserializing an AnomalyExperimentConfig fails."""
