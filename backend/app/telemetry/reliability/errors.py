class TelemetryReliabilityError(Exception):
    """Base exception for telemetry reliability, quarantine, replay, and readiness errors."""


class QuarantineError(TelemetryReliabilityError):
    """Base exception for quarantine operations."""


class QuarantineWriteError(QuarantineError):
    """Raised when appending a failed record to quarantine storage fails."""


class QuarantineReadError(QuarantineError):
    """Raised when reading records from quarantine storage fails."""


class QuarantineRecordValidationError(QuarantineError):
    """Raised when a quarantine record structure or field is invalid."""


class ReplayError(TelemetryReliabilityError):
    """Base exception for quarantine replay operations."""


class ReplayRecordError(ReplayError):
    """Raised when a specific quarantine record cannot be replayed."""


class TelemetryReadinessError(TelemetryReliabilityError):
    """Raised when probing telemetry subsystem readiness fails."""
