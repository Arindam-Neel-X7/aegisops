class TelemetryQueryError(Exception):
    """Base exception for all telemetry query errors."""


class InvalidMetricQueryError(TelemetryQueryError):
    """Raised when a MetricQuery request fails validation."""


class MetricQueryBackendError(TelemetryQueryError):
    """Raised when VictoriaMetrics query execution fails or returns an error response."""


class InvalidEvidenceQueryError(TelemetryQueryError):
    """Raised when an EvidenceQuery request fails validation."""


class EvidenceQueryBackendError(TelemetryQueryError):
    """Raised when OpenSearch search execution fails or returns an error response."""


class QueryResponseValidationError(TelemetryQueryError):
    """Raised when a backend response structure cannot be validated or parsed into domain models."""
