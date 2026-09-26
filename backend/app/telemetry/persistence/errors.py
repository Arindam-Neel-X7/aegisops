class TelemetryPersistenceError(Exception):
    """Base exception for all telemetry persistence errors."""


class InvalidMetricError(TelemetryPersistenceError):
    """Raised when a metric payload fails validation prior to transmission."""


class VictoriaMetricsPersistenceError(TelemetryPersistenceError):
    """Raised when VictoriaMetrics HTTP ingestion fails."""


class VictoriaMetricsRetryExhaustedError(VictoriaMetricsPersistenceError):
    """Raised when transient VictoriaMetrics HTTP ingestion failures exhaust retry attempts."""


class VictoriaMetricsConfigurationError(TelemetryPersistenceError):
    """Raised when VictoriaMetrics adapter configuration or connection settings are invalid."""


class InvalidEvidenceEventError(TelemetryPersistenceError):
    """Raised when an evidence event is invalid or not allowed for OpenSearch persistence."""


class OpenSearchPersistenceError(TelemetryPersistenceError):
    """Raised when OpenSearch HTTP document indexing fails."""


class OpenSearchRetryExhaustedError(OpenSearchPersistenceError):
    """Raised when transient OpenSearch HTTP indexing failures exhaust retry attempts."""


class OpenSearchConfigurationError(TelemetryPersistenceError):
    """Raised when OpenSearch connection or cluster configuration is invalid."""


class EvidenceIndexBootstrapError(TelemetryPersistenceError):
    """Raised when bootstrapping index templates, concrete indices, or aliases fails."""
