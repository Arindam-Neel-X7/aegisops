from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TelemetryRunPublishResult(BaseModel):
    """Immutable result of a detached telemetry pipeline publication."""

    model_config = ConfigDict(frozen=True)

    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    total_events: int = Field(ge=0)
    published_events: int = Field(ge=0)
    metric_events: int = Field(ge=0)
    log_events: int = Field(ge=0)
    system_events: int = Field(ge=0)
    publish_results: list = Field(default_factory=list)
    started_at: datetime
    completed_at: datetime
    duration_ms: float = Field(ge=0.0)
