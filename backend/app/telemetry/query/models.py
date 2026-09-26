from datetime import timedelta
import re
from typing import Any
import uuid

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.telemetry.query.errors import (
    InvalidEvidenceQueryError,
    InvalidMetricQueryError,
)
from app.telemetry.schemas import EventSeverity, EventType
from app.telemetry.transport.serialization import MAX_UINT64

METRIC_NAME_REGEX = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


class MetricQuery(BaseModel):
    """Query model for searching and retrieving metric samples from VictoriaMetrics."""

    model_config = ConfigDict(frozen=True)

    metric: str
    service: str | None = None
    tenant_id: uuid.UUID | None = None
    environment: str | None = None
    run_id: uuid.UUID | None = None
    seed: int | None = None
    event_id: uuid.UUID | None = None
    start_time: AwareDatetime | None = None
    end_time: AwareDatetime | None = None
    step: str | int | float | timedelta | None = None

    @model_validator(mode="after")
    def validate_query(self) -> "MetricQuery":
        if not self.metric or not self.metric.strip():
            raise InvalidMetricQueryError("Field 'metric' must be a non-empty string")

        if not METRIC_NAME_REGEX.match(self.metric.strip()):
            raise InvalidMetricQueryError(
                f"Field 'metric' contains invalid characters: '{self.metric}'. "
                "Must match [a-zA-Z_:][a-zA-Z0-9_:]*"
            )

        if self.seed is not None:
            if self.seed < 0 or self.seed > MAX_UINT64:
                raise InvalidMetricQueryError(
                    f"Field 'seed' must be in range [0, 2^64 - 1], got {self.seed}"
                )

        if self.start_time is not None and self.end_time is not None:
            if self.start_time > self.end_time:
                raise InvalidMetricQueryError(
                    f"start_time ({self.start_time}) cannot be greater than end_time ({self.end_time})"
                )

        if self.step is not None:
            if isinstance(self.step, (int, float)) and self.step <= 0:
                raise InvalidMetricQueryError(f"step must be positive, got {self.step}")
            if isinstance(self.step, timedelta) and self.step.total_seconds() <= 0:
                raise InvalidMetricQueryError(f"step timedelta must be positive, got {self.step}")
            if isinstance(self.step, str) and not self.step.strip():
                raise InvalidMetricQueryError("step string cannot be empty")

        return self


class MetricSample(BaseModel):
    """Normalized metric sample retrieved from VictoriaMetrics."""

    model_config = ConfigDict(frozen=True)

    metric: str
    event_id: uuid.UUID
    run_id: uuid.UUID
    service: str
    tenant_id: uuid.UUID
    environment: str
    seed: int
    timestamp: AwareDatetime
    value: float
    scenario_id: str | None = None
    status_code: str | None = None
    outcome: str | None = None


class MetricQueryResult(BaseModel):
    """Result envelope containing normalized metric samples from VictoriaMetrics."""

    model_config = ConfigDict(frozen=True)

    samples: list[MetricSample] = Field(default_factory=list)
    count: int = 0


class EvidenceQuery(BaseModel):
    """Query model for searching and retrieving LOG and SYSTEM evidence from OpenSearch."""

    model_config = ConfigDict(frozen=True)

    run_id: uuid.UUID | None = None
    seed: int | None = None
    start_time: AwareDatetime | None = None
    end_time: AwareDatetime | None = None
    service: str | None = None
    event_type: EventType | None = None
    severity: EventSeverity | None = None
    trace_id: str | None = None
    event_id: uuid.UUID | None = None
    marker: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)

    @model_validator(mode="after")
    def validate_query(self) -> "EvidenceQuery":
        if self.seed is not None:
            if self.seed < 0 or self.seed > MAX_UINT64:
                raise InvalidEvidenceQueryError(
                    f"Field 'seed' must be in range [0, 2^64 - 1], got {self.seed}"
                )

        if self.start_time is not None and self.end_time is not None:
            if self.start_time > self.end_time:
                raise InvalidEvidenceQueryError(
                    f"start_time ({self.start_time}) cannot be greater than end_time ({self.end_time})"
                )

        if self.event_type is not None and self.event_type not in (EventType.LOG, EventType.SYSTEM):
            raise InvalidEvidenceQueryError(
                f"event_type '{self.event_type}' is not allowed for evidence queries. "
                "Only LOG and SYSTEM are supported in Phase 2."
            )

        if self.service is not None and not self.service.strip():
            raise InvalidEvidenceQueryError("Field 'service' cannot be empty string")

        if self.trace_id is not None and not self.trace_id.strip():
            raise InvalidEvidenceQueryError("Field 'trace_id' cannot be empty string")

        if self.marker is not None and not self.marker.strip():
            raise InvalidEvidenceQueryError("Field 'marker' cannot be empty string")

        return self


class EvidenceRecord(BaseModel):
    """Normalized evidence record retrieved from OpenSearch."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = "1.0"
    event_id: uuid.UUID
    event_time: AwareDatetime
    tenant_id: uuid.UUID
    environment: str
    service: str
    event_type: EventType
    severity: EventSeverity
    trace_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    run_id: uuid.UUID
    scenario_id: str
    scenario_version: str
    reproducibility_key: str
    seed: int
    ingested_at: AwareDatetime


class EvidenceQueryResult(BaseModel):
    """Result envelope containing normalized evidence records from OpenSearch."""

    model_config = ConfigDict(frozen=True)

    records: list[EvidenceRecord] = Field(default_factory=list)
    count: int = 0
