class CorrelationError(Exception):
    """Base exception for correlation module errors."""


class IncidentContractError(CorrelationError):
    """Raised when an incident contract fails validation."""


class IncidentIdentityError(CorrelationError):
    """Raised when incident identity derivation fails."""


class IncidentSerializationError(CorrelationError):
    """Raised when incident serialization or deserialization fails."""


class IncidentDeserializationError(IncidentSerializationError, IncidentContractError):
    """Raised when deserializing an incident contract fails."""
