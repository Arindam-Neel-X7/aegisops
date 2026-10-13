class CorrelationError(Exception):
    """Base exception for correlation module errors."""


class CorrelationConfigurationError(CorrelationError):
    """Raised when correlation configuration validation or serialization fails."""


class IncidentContractError(CorrelationError):
    """Raised when an incident contract fails validation."""


class IncidentIdentityError(CorrelationError):
    """Raised when incident identity derivation fails."""


class IncidentSerializationError(CorrelationError):
    """Raised when incident serialization or deserialization fails."""


class IncidentDeserializationError(IncidentSerializationError, IncidentContractError):
    """Raised when deserializing an incident contract fails."""


# Topology errors
class TopologyError(CorrelationError):
    """Base exception for topology errors."""


class TopologyContractError(TopologyError):
    """Raised when a topology contract fails validation."""


class TopologyNodeNotFoundError(TopologyError):
    """Raised when a service node is not found in the topology."""


class TopologyVersionMismatchError(TopologyError):
    """Raised when the topology version does not match the expected version."""


class TopologyProviderError(TopologyError):
    """Raised when a topology provider fails."""


class TopologyQueryLimitError(TopologyError):
    """Raised when a topology query exceeds a configured limit."""
