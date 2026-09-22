import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, AwareDatetime


class EventType(StrEnum):
    METRIC = "metric"
    LOG = "log"
    SYSTEM = "system"
    ANOMALY = "anomaly"
    INCIDENT = "incident"
    AGENT = "agent"


class EventSeverity(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class TelemetryEvent(BaseModel):
    schema_version: str = Field(default="1.0", min_length=1)
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_time: AwareDatetime = Field(default_factory=_now_utc)
    tenant_id: uuid.UUID
    environment: str = Field(min_length=1)
    service: str = Field(min_length=1)
    event_type: EventType
    severity: EventSeverity
    trace_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
