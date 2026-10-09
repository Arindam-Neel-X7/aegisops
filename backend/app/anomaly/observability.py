from __future__ import annotations

import math
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from enum import StrEnum
from typing import TypeVar

import structlog
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.anomaly.models import MAX_UINT64

SUPPORTED_OBSERVABILITY_SCHEMA_VERSION = "1.0"

_FAILURE_TYPE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_UNSAFE_MESSAGE_PATTERNS = (
    re.compile(r"\btraceback\b", re.IGNORECASE),
    re.compile(
        r"\b(?:password|secret|token|api[_-]?key|credential)s?\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b[A-Za-z]:[\\/][^\s]*"),
    re.compile(r"\\\\[^\\/\s]+[\\/][^\s]+"),
    re.compile(r"(?<![\w:])/(?:[^/\s]+/)*[^/\s]+"),
    re.compile(r"<[^>]*(?:object|instance)[^>]*>", re.IGNORECASE),
    re.compile(r"\b0x[0-9A-Fa-f]+\b"),
    re.compile(r"^[\[{].*[\]}]$"),
)
_SAFE_FAILURE_MESSAGE = "Operation failed"
_MAX_FAILURE_MESSAGE_LENGTH = 500

T = TypeVar("T")


class OperationalStage(StrEnum):
    FEATURE_EXTRACTION = "feature_extraction"
    MODEL_EXECUTION = "model_execution"
    CALIBRATION = "calibration"
    EVALUATION = "evaluation"
    PUBLICATION = "publication"


class OperationStatus(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"


class FailureCategory(StrEnum):
    MODEL_FAILURE = "model_failure"
    MALFORMED_OUTPUT = "malformed_output"
    MISSING_WINDOW = "missing_window"
    PUBLICATION_FAILURE = "publication_failure"
    FEATURE_EXTRACTION_FAILURE = "feature_extraction_failure"
    CALIBRATION_FAILURE = "calibration_failure"
    EVALUATION_FAILURE = "evaluation_failure"


class ObservabilityContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    run_id: uuid.UUID | None = None
    scenario_id: str | None = None
    scenario_version: str | None = None
    seed: int | None = None
    reproducibility_key: str | None = None
    package_id: str | None = None
    package_version: str | None = None
    code_revision: str | None = None
    model_name: str | None = None
    model_version: str | None = None
    signal_id: uuid.UUID | None = None
    trace_id: str | None = None
    semantic_fingerprint: str | None = None

    @field_validator("semantic_fingerprint", mode="before")
    @classmethod
    def validate_semantic_fingerprint(cls, value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, str):
            raise ValueError("semantic_fingerprint must be a string")
        if len(value) != 64:
            raise ValueError("semantic_fingerprint must be exactly 64 characters")
        if not all(c in "0123456789abcdef" for c in value):
            raise ValueError("semantic_fingerprint must be lowercase hexadecimal")
        return value

    @field_validator(
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "package_id",
        "package_version",
        "code_revision",
        "model_name",
        "model_version",
        "trace_id",
        mode="before",
    )
    @classmethod
    def validate_non_blank_string(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        stripped = value.strip()
        if not stripped:
            raise ValueError("String field cannot be blank")
        return stripped

    @field_validator("seed", mode="before")
    @classmethod
    def validate_seed(cls, value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("Seed must be an exact integer")
        if value < 0 or value > MAX_UINT64:
            raise ValueError("Seed must be between 0 and MAX_UINT64")
        return value

    @field_validator("run_id", "signal_id", mode="before")
    @classmethod
    def validate_uuid(cls, value: object, info: ValidationInfo) -> uuid.UUID | None:
        if value is None:
            return None
        if info.mode == "json" and isinstance(value, str):
            try:
                return uuid.UUID(value)
            except ValueError as exc:
                raise ValueError("Invalid UUID string") from exc
        if not isinstance(value, uuid.UUID):
            raise ValueError("UUID fields require UUID objects in Python mode")
        return value

    @model_validator(mode="after")
    def validate_provenance_groups(self) -> ObservabilityContext:
        if (self.model_name is None) != (self.model_version is None):
            raise ValueError(
                "model_name and model_version must both be present or both absent"
            )
        package_values = (self.package_id, self.package_version, self.code_revision)
        if any(value is None for value in package_values) and any(
            value is not None for value in package_values
        ):
            raise ValueError(
                "package_id, package_version, and code_revision must all be present or all absent"
            )
        return self


class OperationRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    schema_version: str = Field(default=SUPPORTED_OBSERVABILITY_SCHEMA_VERSION)
    stage: OperationalStage
    status: OperationStatus
    started_at: AwareDatetime
    completed_at: AwareDatetime
    latency_ms: float
    context: ObservabilityContext | None = None

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != SUPPORTED_OBSERVABILITY_SCHEMA_VERSION:
            raise ValueError("Unsupported observability schema version")
        return value

    @field_validator("stage", mode="before")
    @classmethod
    def validate_stage(cls, value: object, info: ValidationInfo) -> object:
        if info.mode == "json" and isinstance(value, str):
            return OperationalStage(value)
        return value

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, value: object, info: ValidationInfo) -> object:
        if info.mode == "json" and isinstance(value, str):
            return OperationStatus(value)
        return value

    @field_validator("started_at", "completed_at", mode="before")
    @classmethod
    def validate_timestamp(cls, value: object, info: ValidationInfo) -> datetime:
        if info.mode == "json" and isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("Invalid ISO-8601 timestamp") from exc
        if not isinstance(value, datetime):
            raise ValueError("Timestamps require datetime objects in Python mode")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamps must be timezone-aware")
        return value

    @field_validator("latency_ms", mode="before")
    @classmethod
    def validate_latency(cls, value: object) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("Latency must be numeric and non-boolean")
        latency = float(value)
        if not math.isfinite(latency) or latency < 0:
            raise ValueError("Latency must be finite and non-negative")
        return latency

    @model_validator(mode="after")
    def validate_temporal_order(self) -> OperationRecord:
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must be greater than or equal to started_at")
        return self


class FailureRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    schema_version: str = Field(default=SUPPORTED_OBSERVABILITY_SCHEMA_VERSION)
    stage: OperationalStage
    category: FailureCategory
    failure_type: str
    failure_message: str
    observed_at: AwareDatetime
    context: ObservabilityContext | None = None

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != SUPPORTED_OBSERVABILITY_SCHEMA_VERSION:
            raise ValueError("Unsupported observability schema version")
        return value

    @field_validator("stage", mode="before")
    @classmethod
    def validate_stage(cls, value: object, info: ValidationInfo) -> object:
        if info.mode == "json" and isinstance(value, str):
            return OperationalStage(value)
        return value

    @field_validator("category", mode="before")
    @classmethod
    def validate_category(cls, value: object, info: ValidationInfo) -> object:
        if info.mode == "json" and isinstance(value, str):
            return FailureCategory(value)
        return value

    @field_validator("failure_type", mode="before")
    @classmethod
    def validate_failure_type(cls, value: object) -> str:
        if not isinstance(value, str) or not _FAILURE_TYPE_PATTERN.fullmatch(value):
            raise ValueError(
                "failure_type must be a simple type name of 1-128 characters"
            )
        return value

    @field_validator("failure_message", mode="before")
    @classmethod
    def validate_failure_message(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("failure_message must be a string")
        sanitized = _sanitize_message(value)
        if not sanitized:
            raise ValueError("failure_message cannot be blank")
        return sanitized

    @field_validator("observed_at", mode="before")
    @classmethod
    def validate_timestamp(cls, value: object, info: ValidationInfo) -> datetime:
        if info.mode == "json" and isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("Invalid ISO-8601 timestamp") from exc
        if not isinstance(value, datetime):
            raise ValueError("Timestamps require datetime objects in Python mode")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamps must be timezone-aware")
        return value


def _sanitize_message(raw: str) -> str:
    text = re.sub(r"\s+", " ", raw).strip()
    if not text:
        return ""
    if any(pattern.search(text) for pattern in _UNSAFE_MESSAGE_PATTERNS):
        return _SAFE_FAILURE_MESSAGE
    if len(text) > _MAX_FAILURE_MESSAGE_LENGTH:
        return f"{text[: _MAX_FAILURE_MESSAGE_LENGTH - 3]}..."
    return text


def _safe_exception_type(exc: Exception) -> str:
    failure_type = type(exc).__name__
    if _FAILURE_TYPE_PATTERN.fullmatch(failure_type):
        return failure_type
    return "Exception"


def _safe_exception_message(exc: Exception) -> str:
    try:
        message = str(exc)
    except Exception:
        return _SAFE_FAILURE_MESSAGE
    return message or _SAFE_FAILURE_MESSAGE


class ObservabilityCollector:
    def __init__(self) -> None:
        self._operations: list[OperationRecord] = []
        self._failures: list[FailureRecord] = []
        self._logger = structlog.get_logger(__name__)

    def record_operation(self, record: OperationRecord) -> None:
        self._operations.append(record)
        try:
            self._logger.info(
                "anomaly_operation",
                schema_version=record.schema_version,
                stage=record.stage.value,
                status=record.status.value,
                started_at=record.started_at.isoformat(),
                completed_at=record.completed_at.isoformat(),
                latency_ms=record.latency_ms,
                context=(
                    record.context.model_dump(mode="json") if record.context else None
                ),
            )
        except Exception:
            pass

    def record_failure(self, record: FailureRecord) -> None:
        self._failures.append(record)
        try:
            self._logger.error(
                "anomaly_failure",
                schema_version=record.schema_version,
                stage=record.stage.value,
                category=record.category.value,
                failure_type=record.failure_type,
                failure_message=record.failure_message,
                observed_at=record.observed_at.isoformat(),
                context=(
                    record.context.model_dump(mode="json") if record.context else None
                ),
            )
        except Exception:
            pass

    def snapshot_operations(self) -> tuple[OperationRecord, ...]:
        return tuple(self._operations)

    def snapshot_failures(self) -> tuple[FailureRecord, ...]:
        return tuple(self._failures)

    def sync_measured_call(
        self,
        stage: OperationalStage,
        failure_category: FailureCategory,
        context: ObservabilityContext | None = None,
        *,
        action: Callable[[], T],
    ) -> T:
        _validate_measurement_metadata(stage, failure_category, context)
        started_at = datetime.now(timezone.utc)
        started_monotonic = time.monotonic()
        try:
            result = action()
        except Exception as exc:
            completed_at = datetime.now(timezone.utc)
            self._record_measured_failure(
                stage=stage,
                failure_category=failure_category,
                context=context,
                started_at=started_at,
                completed_at=completed_at,
                latency_ms=(time.monotonic() - started_monotonic) * 1000.0,
                exc=exc,
            )
            raise
        completed_at = datetime.now(timezone.utc)
        self.record_operation(
            OperationRecord(
                stage=stage,
                status=OperationStatus.SUCCESS,
                started_at=started_at,
                completed_at=completed_at,
                latency_ms=(time.monotonic() - started_monotonic) * 1000.0,
                context=context,
            )
        )
        return result

    async def async_measured_call(
        self,
        stage: OperationalStage,
        failure_category: FailureCategory,
        context: ObservabilityContext | None = None,
        *,
        action: Callable[[], Awaitable[T]],
    ) -> T:
        _validate_measurement_metadata(stage, failure_category, context)
        started_at = datetime.now(timezone.utc)
        started_monotonic = time.monotonic()
        try:
            result = await action()
        except Exception as exc:
            completed_at = datetime.now(timezone.utc)
            self._record_measured_failure(
                stage=stage,
                failure_category=failure_category,
                context=context,
                started_at=started_at,
                completed_at=completed_at,
                latency_ms=(time.monotonic() - started_monotonic) * 1000.0,
                exc=exc,
            )
            raise
        completed_at = datetime.now(timezone.utc)
        self.record_operation(
            OperationRecord(
                stage=stage,
                status=OperationStatus.SUCCESS,
                started_at=started_at,
                completed_at=completed_at,
                latency_ms=(time.monotonic() - started_monotonic) * 1000.0,
                context=context,
            )
        )
        return result

    def _record_measured_failure(
        self,
        *,
        stage: OperationalStage,
        failure_category: FailureCategory,
        context: ObservabilityContext | None,
        started_at: datetime,
        completed_at: datetime,
        latency_ms: float,
        exc: Exception,
    ) -> None:
        self.record_operation(
            OperationRecord(
                stage=stage,
                status=OperationStatus.FAILURE,
                started_at=started_at,
                completed_at=completed_at,
                latency_ms=latency_ms,
                context=context,
            )
        )
        self.record_failure(
            FailureRecord(
                stage=stage,
                category=failure_category,
                failure_type=_safe_exception_type(exc),
                failure_message=_safe_exception_message(exc),
                observed_at=completed_at,
                context=context,
            )
        )


def _validate_measurement_metadata(
    stage: OperationalStage,
    failure_category: FailureCategory,
    context: ObservabilityContext | None,
) -> None:
    if not isinstance(stage, OperationalStage):
        raise TypeError("stage must be an OperationalStage")
    if not isinstance(failure_category, FailureCategory):
        raise TypeError("failure_category must be a FailureCategory")
    if context is not None and not isinstance(context, ObservabilityContext):
        raise TypeError("context must be an ObservabilityContext or None")
