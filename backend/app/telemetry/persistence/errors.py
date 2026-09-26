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
