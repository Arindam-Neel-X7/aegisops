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


class ConsumerError(TelemetryTransportError):
    """Base exception for all consumer and routing errors."""


class ConsumerNotStartedError(ConsumerError):
    """Raised when attempting to consume or process records before consumer is started or after it is closed."""


class ConsumerRecordValidationError(ConsumerError):
    """Raised when validating a consumed Kafka record or its headers fails."""


class MissingRequiredHeaderError(ConsumerRecordValidationError):
    """Raised when a required execution header is missing."""


class DuplicateHeaderError(ConsumerRecordValidationError):
    """Raised when a required execution header is duplicated."""


class InvalidHeaderError(ConsumerRecordValidationError):
    """Raised when an execution header value is invalid (malformed UUID, invalid integer/seed, non-UTF8, empty)."""


class TopicEventTypeMismatchError(ConsumerRecordValidationError):
    """Raised when the record topic does not match EVENT_TYPE_TO_TOPIC[event.event_type] or worker subscription."""


class ConsumerHandlerError(ConsumerError):
    """Raised when an injected downstream handler fails while processing an envelope."""

