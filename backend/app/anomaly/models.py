from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any
import uuid

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.telemetry.schemas import EventSeverity

MAX_UINT64: int = 18446744073709551615  # 2**64 - 1
SUPPORTED_ANOMALY_SCHEMA_VERSION = "1.0"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class EventTimeWindow(BaseModel):
    """Time window spanning source telemetry events analyzed by an anomaly model."""

    model_config = ConfigDict(frozen=True)

    start_time: AwareDatetime
    end_time: AwareDatetime

    @model_validator(mode="after")
    def validate_window(self) -> EventTimeWindow:
        if self.start_time > self.end_time:
            raise ValueError(
                f"start_time ({self.start_time.isoformat()}) cannot be greater than "
                f"end_time ({self.end_time.isoformat()})"
            )
        return self


class AnomalyEvidence(BaseModel):
    """Machine-readable structured evidence item supporting an anomaly detection signal."""

    model_config = ConfigDict(frozen=True)

    evidence_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    evidence_type: str = Field(min_length=1)
    metric_or_feature: str | None = None
    observed_value: float | None = None
    expected_value: float | None = None
    deviation: float | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    timestamp: AwareDatetime | None = None

    @field_validator("evidence_type")
    @classmethod
    def validate_non_blank_evidence_type(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field 'evidence_type' cannot be empty or whitespace-only")
        return v

    @field_validator("observed_value", "expected_value", "deviation")
    @classmethod
    def validate_finite_numeric_evidence(cls, v: float | None) -> float | None:
        if v is not None and not math.isfinite(v):
            raise ValueError(f"Numeric evidence value must be finite, got {v}")
        return v


class CalibrationMetadata(BaseModel):
    """Structured, versioned threshold and calibration metadata for anomaly scoring."""

    model_config = ConfigDict(frozen=True)

    schema_version: str = Field(default=SUPPORTED_ANOMALY_SCHEMA_VERSION, min_length=1)
    method: str = Field(min_length=1)
    threshold_value: float
    calibration_version: str = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    calibrated_at: AwareDatetime | None = None

    @field_validator("schema_version")
    @classmethod
    def validate_calibration_schema_version(cls, v: str) -> str:
        if v != SUPPORTED_ANOMALY_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported calibration schema_version: '{v}'. "
                f"Expected '{SUPPORTED_ANOMALY_SCHEMA_VERSION}'"
            )
        return v

    @field_validator("method", "calibration_version")
    @classmethod
    def validate_non_blank_strings(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v

    @field_validator("threshold_value")
    @classmethod
    def validate_finite_threshold(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"threshold_value must be a finite numeric value, got {v}")
        return v


class AnomalySignal(BaseModel):
    """Typed, versioned output contract for Phase 3 anomaly signals.

    Preserves signal detection details, model metadata, structured evidence,
    and complete research/execution provenance for downstream correlation.
    """

    model_config = ConfigDict(frozen=True)

    # --- Minimum Signal Contract ---
    signal_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_time: AwareDatetime = Field(default_factory=_now_utc)
    service: str = Field(min_length=1)
    metric_or_feature: str = Field(min_length=1)
    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    anomaly_score: float
    severity: EventSeverity
    evidence: list[AnomalyEvidence] = Field(default_factory=list)
    threshold_or_calibration: CalibrationMetadata

    # --- Applicable Execution and Research Context ---
    schema_version: str = Field(default=SUPPORTED_ANOMALY_SCHEMA_VERSION, min_length=1)
    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)
    reproducibility_key: str = Field(min_length=1)
    source_event_ids: list[uuid.UUID] = Field(default_factory=list)
    source_event_time_window: EventTimeWindow | None = None
    environment: str = Field(min_length=1)
    tenant_id: uuid.UUID

    # --- Optional Diagnostic Metadata ---
    trace_id: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v != SUPPORTED_ANOMALY_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported anomaly schema_version: '{v}'. "
                f"Expected '{SUPPORTED_ANOMALY_SCHEMA_VERSION}'"
            )
        return v

    @field_validator(
        "service",
        "metric_or_feature",
        "model_name",
        "model_version",
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "environment",
    )
    @classmethod
    def validate_non_blank_string_fields(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v

    @field_validator("anomaly_score")
    @classmethod
    def validate_anomaly_score(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"anomaly_score must be a finite float, got {v}")
        if v < 0.0 or v > 1.0:
            raise ValueError(f"anomaly_score must be bounded in range [0.0, 1.0], got {v}")
        return v
