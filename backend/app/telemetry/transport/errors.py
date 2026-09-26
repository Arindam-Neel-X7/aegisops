class TelemetryTransportError(Exception):
    """Base exception for all telemetry transport and serialization errors."""


class TelemetrySerializationError(TelemetryTransportError):
    """Raised when serialization of a TelemetryEvent fails."""


class TelemetryDeserializationError(TelemetryTransportError):
    """Raised when deserialization or schema validation of raw bytes fails."""


class ProducerError(TelemetryTransportError):
    """Raised when publishing a TelemetryEvent to Kafka fails."""


class ProducerNotStartedError(ProducerError):
    """Raised when attempting to publish before the producer is started or after it is closed."""
