from __future__ import annotations

import uuid


class TelemetryPipelineError(Exception):
    """Base exception for telemetry pipeline errors."""


class TelemetryPipelinePublishError(TelemetryPipelineError):
    """Raised when publishing a ScenarioRunResult through the telemetry pipeline fails."""

    def __init__(
        self,
        message: str,
        *,
        run_id: uuid.UUID,
        scenario_id: str,
        published_count: int,
        failing_event_id: uuid.UUID | None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.run_id = run_id
        self.scenario_id = scenario_id
        self.published_count = published_count
        self.failing_event_id = failing_event_id
        self.__cause__ = cause