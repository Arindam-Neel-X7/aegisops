from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.anomaly.errors import AnomalyValidationError
from app.anomaly.models import AnomalySignal, EventTimeWindow, MAX_UINT64

SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION = "1.0"


class AnomalyReproducibilityLineage(BaseModel):
    """Immutable, strictly validated anomaly reproducibility lineage."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    schema_version: str = Field(default=SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION)
    signal_id: uuid.UUID
    run_id: uuid.UUID
    scenario_id: str
    scenario_version: str
    seed: int
    reproducibility_key: str
    model_name: str
    model_version: str
    source_event_ids: tuple[uuid.UUID, ...]
    source_event_time_window: EventTimeWindow | None = None
    semantic_fingerprint: str

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v != SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported anomaly lineage schema_version: '{v}'. "
                f"Expected '{SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION}'"
            )
        return v

    @field_validator(
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "model_name",
        "model_version",
        mode="before",
    )
    @classmethod
    def validate_non_blank_string(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError("field must be a string")
        stripped = v.strip()
        if not stripped:
            raise ValueError("field cannot be empty or whitespace-only")
        return stripped

    @field_validator("seed", mode="before")
    @classmethod
    def validate_seed(cls, v: object) -> int:
        if isinstance(v, bool) or v is None or not isinstance(v, int):
            raise ValueError("seed must be an exact integer and not boolean")
        if v < 0 or v > MAX_UINT64:
            raise ValueError(f"seed must be within [0, MAX_UINT64], got {v}")
        return v

    @field_validator("signal_id", "run_id", mode="before")
    @classmethod
    def validate_uuid_fields(cls, v: object, info: ValidationInfo) -> uuid.UUID:
        if info.mode == "json" and isinstance(v, str):
            try:
                return uuid.UUID(v)
            except Exception as exc:
                raise ValueError("field must be a valid UUID string") from exc
        if not isinstance(v, uuid.UUID) or isinstance(v, bool):
            raise ValueError("field must be a UUID instance")
        return v

    @field_validator("source_event_ids", mode="before")
    @classmethod
    def validate_source_event_ids(
        cls, v: object, info: ValidationInfo
    ) -> tuple[uuid.UUID, ...]:
        if info.mode == "json":
            if isinstance(v, (list, tuple)):
                result: list[uuid.UUID] = []
                for item in v:
                    if isinstance(item, str):
                        try:
                            result.append(uuid.UUID(item))
                        except Exception as exc:
                            raise ValueError(
                                "source_event_ids must contain valid UUID strings"
                            ) from exc
                    elif type(item) is uuid.UUID:
                        result.append(item)
                    else:
                        raise ValueError(
                            "source_event_ids must contain UUID objects or strings"
                        )
                return tuple(result)
        if isinstance(v, tuple) and all(type(item) is uuid.UUID for item in v):
            return v
        raise ValueError("source_event_ids must be a tuple of UUID objects")

    @field_validator("source_event_time_window", mode="before")
    @classmethod
    def validate_time_window(
        cls, v: object, info: ValidationInfo
    ) -> EventTimeWindow | None:
        if v is None:
            return None
        if isinstance(v, EventTimeWindow):
            return v
        if info.mode == "json" and isinstance(v, dict):
            try:
                return EventTimeWindow.model_validate(v)
            except Exception as exc:
                raise ValueError(
                    "source_event_time_window must be a valid EventTimeWindow"
                ) from exc
        raise ValueError(
            "source_event_time_window must be an EventTimeWindow instance in Python mode"
        )

    @field_validator("semantic_fingerprint", mode="before")
    @classmethod
    def validate_semantic_fingerprint(cls, v: object) -> str:
        if isinstance(v, bool) or not isinstance(v, str):
            raise ValueError("semantic_fingerprint must be a string")
        if len(v) != 64:
            raise ValueError("semantic_fingerprint must be exactly 64 characters")
        if not all(c in "0123456789abcdef" for c in v):
            raise ValueError("semantic_fingerprint must be lowercase hexadecimal")
        return v

    @model_validator(mode="after")
    def validate_frozen_immutability(self) -> AnomalyReproducibilityLineage:
        return self


def _canonicalize_for_fingerprint(signal: AnomalySignal) -> dict[str, Any]:
    """Convert AnomalySignal to canonical JSON-compatible dict for fingerprinting."""
    data = signal.model_dump(mode="json", exclude_none=False)
    return data


def compute_anomaly_semantic_fingerprint(signal: AnomalySignal) -> str:
    """Compute SHA-256 semantic fingerprint of an anomaly signal.

    The fingerprint covers the complete semantic signal including all fields,
    with deterministic serialization (sorted keys, compact separators, UTF-8).

    Args:
        signal: The AnomalySignal to fingerprint.

    Returns:
        Lowercase SHA-256 hex digest (64 characters).

    Raises:
        TypeError: If input is not an AnomalySignal.
        AnomalyValidationError: If canonicalization fails.
    """
    if not isinstance(signal, AnomalySignal):
        raise TypeError(f"Expected AnomalySignal, got {type(signal).__name__}")

    try:
        data = _canonicalize_for_fingerprint(signal)
        canonical_json = json.dumps(
            data,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    except Exception as exc:
        raise AnomalyValidationError(
            "Failed to compute anomaly semantic fingerprint"
        ) from exc


def build_anomaly_reproducibility_lineage(
    signal: AnomalySignal,
) -> AnomalyReproducibilityLineage:
    """Build the complete reproducibility lineage for an anomaly signal.

    Args:
        signal: The AnomalySignal to build lineage for.

    Returns:
        AnomalyReproducibilityLineage containing all reproducibility fields
        and the computed semantic fingerprint.
    """
    if not isinstance(signal, AnomalySignal):
        raise TypeError(f"Expected AnomalySignal, got {type(signal).__name__}")

    fingerprint = compute_anomaly_semantic_fingerprint(signal)

    return AnomalyReproducibilityLineage(
        schema_version=SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION,
        signal_id=signal.signal_id,
        run_id=signal.run_id,
        scenario_id=signal.scenario_id,
        scenario_version=signal.scenario_version,
        seed=signal.seed,
        reproducibility_key=signal.reproducibility_key,
        model_name=signal.model_name,
        model_version=signal.model_version,
        source_event_ids=tuple(signal.source_event_ids),
        source_event_time_window=signal.source_event_time_window,
        semantic_fingerprint=fingerprint,
    )


def require_same_anomaly_semantic_identity(
    expected: AnomalySignal,
    candidate: AnomalySignal,
) -> AnomalyReproducibilityLineage:
    """Verify that two anomaly signals have identical semantic identity.

    Args:
        expected: The expected anomaly signal.
        candidate: The candidate anomaly signal to compare.

    Returns:
        The candidate's lineage if identities match.

    Raises:
        TypeError: If either input is not an AnomalySignal.
        AnomalyValidationError: If semantic identities differ (message: "Anomaly semantic identity mismatch").
    """
    if not isinstance(expected, AnomalySignal):
        raise TypeError(f"Expected AnomalySignal, got {type(expected).__name__}")
    if not isinstance(candidate, AnomalySignal):
        raise TypeError(f"Expected AnomalySignal, got {type(candidate).__name__}")

    expected_lineage = build_anomaly_reproducibility_lineage(expected)
    candidate_lineage = build_anomaly_reproducibility_lineage(candidate)

    if expected_lineage != candidate_lineage:
        raise AnomalyValidationError("Anomaly semantic identity mismatch")

    return candidate_lineage
