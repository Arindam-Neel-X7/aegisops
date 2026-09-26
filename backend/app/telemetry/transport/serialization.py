import json
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.telemetry.schemas import TelemetryEvent
from app.telemetry.transport.errors import (
    TelemetryDeserializationError,
    TelemetrySerializationError,
)

MAX_UINT64: int = 18446744073709551615  # 2**64 - 1


class TelemetryExecutionContext(BaseModel):
    """External execution and research context accompanying transported telemetry events.

    This context is transport metadata carried in Kafka headers and storage fields;
    it is strictly separate from the canonical TelemetryEvent envelope.
    """

    model_config = ConfigDict(frozen=True)

    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    reproducibility_key: str = Field(min_length=1)
    seed: int = Field(ge=0, le=MAX_UINT64)


def serialize_event(event: TelemetryEvent) -> bytes:
    """Serialize a canonical TelemetryEvent to UTF-8 JSON bytes."""
    try:
        return event.model_dump_json().encode("utf-8")
    except Exception as exc:
        raise TelemetrySerializationError(
            f"Failed to serialize TelemetryEvent {getattr(event, 'event_id', 'unknown')}: {exc}"
        ) from exc


def deserialize_event(raw: bytes | bytearray | memoryview) -> TelemetryEvent:
    """Deserialize UTF-8 JSON bytes into a canonical TelemetryEvent.

    - Missing schema_version is accepted and defaults to '1.0'.
    - Explicitly declared unsupported schema_version is rejected.
    """
    try:
        raw_bytes = bytes(raw)
        data: Any = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(data, dict):
            raise TelemetryDeserializationError("Deserialized JSON root must be an object")

        if "schema_version" in data:
            declared_version = data["schema_version"]
            if declared_version != "1.0":
                raise TelemetryDeserializationError(
                    f"Unsupported schema_version: {declared_version}"
                )

        return TelemetryEvent.model_validate(data)
    except TelemetryDeserializationError:
        raise
    except (json.JSONDecodeError, ValidationError, ValueError, TypeError, UnicodeDecodeError) as exc:
        raise TelemetryDeserializationError(
            f"Failed to deserialize TelemetryEvent: {exc}"
        ) from exc


def construct_kafka_key(event: TelemetryEvent) -> bytes:
    """Construct partition routing key from tenant_id:environment:service.

    Ensures all events for a service within a tenant and environment route
    to the same partition, preserving per-service causal ordering.
    """
    return f"{event.tenant_id}:{event.environment}:{event.service}".encode("utf-8")


def construct_kafka_headers(
    context: TelemetryExecutionContext,
    producer_version: str | None = None,
) -> list[tuple[str, bytes]]:
    """Construct required Kafka record headers from TelemetryExecutionContext."""
    headers: list[tuple[str, bytes]] = [
        ("run_id", str(context.run_id).encode("utf-8")),
        ("scenario_id", context.scenario_id.encode("utf-8")),
        ("scenario_version", context.scenario_version.encode("utf-8")),
        ("reproducibility_key", context.reproducibility_key.encode("utf-8")),
        ("seed", str(context.seed).encode("utf-8")),
    ]
    if producer_version is not None:
        headers.append(("producer_version", producer_version.encode("utf-8")))
    return headers
