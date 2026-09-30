class AnomalyError(Exception):
    """Base exception for anomaly module errors."""


class AnomalyValidationError(AnomalyError):
    """Raised when an AnomalySignal fails contract validation."""


class AnomalySerializationError(AnomalyError):
    """Raised when an AnomalySignal fails serialization."""


class AnomalyDeserializationError(AnomalyError):
    """Raised when deserializing an AnomalySignal fails."""
