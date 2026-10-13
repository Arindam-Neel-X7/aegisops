from __future__ import annotations

import hashlib
import json
import math
import re
from enum import StrEnum
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    ValidationInfo,
    model_validator,
)

from app.correlation.errors import CorrelationConfigurationError
from app.correlation.models import MAX_SIGNAL_REFS, MAX_POLICY_LEN
from app.correlation.topology import TOPOLOGY_MAX_HOPS

SUPPORTED_CORRELATION_CONFIG_SCHEMA_VERSION = "1.0.0"

MIN_WINDOW_SECONDS = 1
MAX_WINDOW_SECONDS = 86400
MIN_LATENESS_SECONDS = 0
MAX_LATENESS_SECONDS = 86400
MIN_QUIET_PERIOD_SECONDS = 0
MAX_QUIET_PERIOD_SECONDS = 86400
MIN_HOP_LIMIT = 0
MAX_HOP_LIMIT = TOPOLOGY_MAX_HOPS
MIN_GROUP_SIZE = 1
MAX_GROUP_SIZE = MAX_SIGNAL_REFS
MIN_ACCEPTANCE_THRESHOLD = 0.0
MAX_ACCEPTANCE_THRESHOLD = 1.0

WEIGHT_SUM_TOLERANCE = 1e-12


class SeverityAggregationPolicy(StrEnum):
    MAXIMUM = "maximum"


class LateSignalDisposition(StrEnum):
    REJECT = "reject"
    ACCEPT = "accept"
    DELAY = "delay"


class DuplicateSignalDisposition(StrEnum):
    IGNORE = "ignore"
    REJECT = "reject"


class ConflictingSignalDisposition(StrEnum):
    REJECT = "reject"
    ISOLATE = "isolate"


class DisconnectedSignalDisposition(StrEnum):
    ISOLATE = "isolate"
    CORRELATE_TEMPORAL = "correlate_temporal"
    REJECT = "reject"


class InsufficientContextDisposition(StrEnum):
    IGNORE = "ignore"
    ISOLATE = "isolate"
    ENRICH = "enrich"


_SECRET_KEYWORDS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "api-key",
    "apikey",
    "credential",
    "access_key",
    "private_key",
    "bearer",
)

_DRIVE_PATH_REGEX = re.compile(r"^[a-zA-Z]:[/\\]")


def _validate_safe_text(value: Any, max_len: int, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError("invalid type")
    trimmed = value.strip()
    if not trimmed:
        raise ValueError("invalid text")
    if len(trimmed) > max_len:
        raise ValueError("exceeds maximum length")
    if any(ord(c) < 32 or ord(c) == 127 for c in trimmed):
        raise ValueError("contains control characters")

    lower_val = trimmed.lower()
    for kw in _SECRET_KEYWORDS:
        if kw in lower_val:
            raise ValueError("contains forbidden pattern")

    if "://" in lower_val or "file:" in lower_val:
        raise ValueError("contains forbidden pattern")
    if trimmed.startswith("\\\\") or trimmed.startswith("//"):
        raise ValueError("contains forbidden pattern")
    if _DRIVE_PATH_REGEX.match(trimmed):
        raise ValueError("contains forbidden pattern")
    if trimmed.startswith("/"):
        raise ValueError("contains forbidden pattern")

    if "=" in trimmed and any(
        kw in lower_val
        for kw in ("key", "token", "secret", "password", "passwd", "credential")
    ):
        raise ValueError("contains forbidden pattern")

    return str(trimmed)


def _validate_strict_float(value: Any, info: ValidationInfo) -> float:
    if isinstance(value, bool) or not isinstance(value, float):
        raise ValueError("must be a strict float")
    if math.isnan(value) or math.isinf(value):
        raise ValueError("must be finite")
    return float(value)


def _validate_schema_version(value: Any, info: ValidationInfo) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError("unsupported schema version")
    trimmed = value.strip()
    if (
        not trimmed
        or len(trimmed) > 32
        or any(ord(c) < 32 or ord(c) == 127 for c in trimmed)
    ):
        raise ValueError("unsupported schema version")
    if trimmed != SUPPORTED_CORRELATION_CONFIG_SCHEMA_VERSION:
        raise ValueError("unsupported schema version")
    return str(trimmed)


class CorrelationScoreWeights(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        hide_input_in_errors=True,
    )

    temporal: float
    service: float
    topology: float
    symptom: float
    context: float

    @field_validator(
        "temporal", "service", "topology", "symptom", "context", mode="before"
    )
    @classmethod
    def _check_weight(cls, v: Any, info: ValidationInfo) -> float:
        val = _validate_strict_float(v, info)
        if val < 0.0 or val > 1.0:
            raise ValueError("weight must be between 0.0 and 1.0 inclusive")
        return val

    @model_validator(mode="after")
    def _validate_weights_sum(self) -> CorrelationScoreWeights:
        weights = [
            self.temporal,
            self.service,
            self.topology,
            self.symptom,
            self.context,
        ]
        if all(w == 0.0 for w in weights):
            raise ValueError("weights cannot all be zero")
        total = math.fsum(weights)
        if abs(total - 1.0) > WEIGHT_SUM_TOLERANCE:
            raise ValueError("weights must sum to 1.0")
        return self


class CorrelationConfig(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        hide_input_in_errors=True,
    )

    schema_version: str
    policy_version: str
    window_seconds: int = Field(..., ge=MIN_WINDOW_SECONDS, le=MAX_WINDOW_SECONDS)
    allowed_lateness_seconds: int = Field(
        ..., ge=MIN_LATENESS_SECONDS, le=MAX_LATENESS_SECONDS
    )
    quiet_period_seconds: int = Field(
        ..., ge=MIN_QUIET_PERIOD_SECONDS, le=MAX_QUIET_PERIOD_SECONDS
    )
    hop_limit: int = Field(..., ge=MIN_HOP_LIMIT, le=MAX_HOP_LIMIT)
    score_weights: CorrelationScoreWeights
    acceptance_threshold: float
    severity_aggregation: SeverityAggregationPolicy
    maximum_group_size: int = Field(..., ge=MIN_GROUP_SIZE, le=MAX_GROUP_SIZE)
    late_signal_disposition: LateSignalDisposition
    duplicate_signal_disposition: DuplicateSignalDisposition
    conflicting_signal_disposition: ConflictingSignalDisposition
    disconnected_signal_disposition: DisconnectedSignalDisposition
    insufficient_context_disposition: InsufficientContextDisposition

    @field_validator("schema_version", mode="before")
    @classmethod
    def _check_schema_version(cls, v: Any, info: ValidationInfo) -> str:
        return _validate_schema_version(v, info)

    @field_validator("policy_version", mode="before")
    @classmethod
    def _check_policy_version(cls, v: Any, info: ValidationInfo) -> str:
        return _validate_safe_text(v, MAX_POLICY_LEN, "policy_version")

    @field_validator("acceptance_threshold", mode="before")
    @classmethod
    def _check_threshold(cls, v: Any, info: ValidationInfo) -> float:
        val = _validate_strict_float(v, info)
        if val < MIN_ACCEPTANCE_THRESHOLD or val > MAX_ACCEPTANCE_THRESHOLD:
            raise ValueError(
                "acceptance_threshold must be between 0.0 and 1.0 inclusive"
            )
        return val


def serialize_correlation_config(config: CorrelationConfig) -> bytes:
    try:
        if not isinstance(config, CorrelationConfig):
            raise TypeError("config must be a CorrelationConfig instance")
        payload = config.model_dump(mode="json")
        if not isinstance(payload, dict):
            raise CorrelationConfigurationError("Invalid model payload")
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except CorrelationConfigurationError:
        raise
    except Exception as exc:
        raise CorrelationConfigurationError(
            "Failed to serialize CorrelationConfig"
        ) from exc


def deserialize_correlation_config(
    raw: bytes | bytearray | memoryview,
) -> CorrelationConfig:
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise CorrelationConfigurationError("Payload must be bytes-like")
    try:
        text = bytes(raw).decode("utf-8")
    except Exception as exc:
        raise CorrelationConfigurationError("Invalid UTF-8 payload") from exc

    trimmed_text = text.strip()
    if not trimmed_text.startswith("{"):
        raise CorrelationConfigurationError("Payload must be a JSON object")

    try:
        parsed = json.loads(trimmed_text)
    except Exception as exc:
        raise CorrelationConfigurationError("Invalid JSON payload") from exc
    if not isinstance(parsed, dict):
        raise CorrelationConfigurationError("Payload must be a JSON object")

    try:
        return CorrelationConfig.model_validate_json(trimmed_text)
    except CorrelationConfigurationError:
        raise
    except ValidationError as exc:
        raise CorrelationConfigurationError(
            "CorrelationConfig validation failed"
        ) from exc
    except Exception as exc:
        raise CorrelationConfigurationError(
            "CorrelationConfig validation failed"
        ) from exc


def compute_correlation_config_fingerprint(config: CorrelationConfig) -> str:
    try:
        if not isinstance(config, CorrelationConfig):
            raise TypeError("config must be a CorrelationConfig instance")
        canonical_bytes = serialize_correlation_config(config)
        digest = hashlib.sha256(canonical_bytes).hexdigest()
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise CorrelationConfigurationError("Invalid fingerprint generated")
        return digest
    except CorrelationConfigurationError:
        raise
    except Exception as exc:
        raise CorrelationConfigurationError(
            "Failed to compute configuration fingerprint"
        ) from exc
