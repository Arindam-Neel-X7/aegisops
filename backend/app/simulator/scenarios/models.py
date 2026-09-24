from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.simulator.interfaces import FaultSpec
from app.simulator.topology import (
    CANONICAL_SERVICE_NAMES,
    CANONICAL_TOPOLOGY_VERSION,
)
from app.simulator.workload import WorkloadConfig, WorkloadProfile


class ScenarioSystemMarker(BaseModel):
    """Declarative metadata for a system-level lifecycle/deployment marker."""

    model_config = ConfigDict(frozen=True)

    marker_name: str = Field(min_length=1)
    service: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_service_name(self) -> "ScenarioSystemMarker":
        if self.service not in CANONICAL_SERVICE_NAMES:
            raise ValueError(
                f"Marker service {self.service!r} is not a valid canonical service name"
            )
        return self


class ScheduledFault(BaseModel):
    """Declarative container associating a canonical FaultSpec with scenario execution."""

    model_config = ConfigDict(frozen=True)

    fault: FaultSpec


class ScenarioDefinition(BaseModel):
    """Immutable declarative scenario specification for research experiments."""

    model_config = ConfigDict(frozen=True)

    scenario_id: str = Field(min_length=1)
    version: str = Field(default="1.0.0", min_length=1)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)

    default_seed: int = Field(default=42, ge=0)

    topology_reference: str = Field(default="canonical", min_length=1)
    topology_version: str = Field(default=CANONICAL_TOPOLOGY_VERSION, min_length=1)

    workload: WorkloadConfig

    primary_fault: ScheduledFault | None = None

    activation_time_seconds: float = Field(default=10.0, ge=0.0)

    expected_symptoms: tuple[str, ...] = Field(min_length=1)
    expected_affected_services: tuple[str, ...] = Field(min_length=1)
    expected_root_cause: str = Field(min_length=1)

    observation_window_seconds: float = Field(default=10.0, gt=0.0)

    recovery_time_seconds: float | None = Field(default=20.0, ge=0.0)
    recovery_condition: str = Field(min_length=1)

    post_recovery_observation_window_seconds: float = Field(default=10.0, ge=0.0)

    total_duration_seconds: float = Field(default=30.0, gt=0.0)

    system_marker: ScenarioSystemMarker | None = None

    @model_validator(mode="after")
    def validate_scenario(self) -> "ScenarioDefinition":
        # Validate topology version matches source of truth
        if self.topology_version != CANONICAL_TOPOLOGY_VERSION:
            raise ValueError(
                f"topology_version ({self.topology_version!r}) must match CANONICAL_TOPOLOGY_VERSION ({CANONICAL_TOPOLOGY_VERSION!r})"
            )

        # Validate affected service names
        for svc in self.expected_affected_services:
            if svc not in CANONICAL_SERVICE_NAMES:
                raise ValueError(
                    f"Expected affected service {svc!r} is not a valid canonical service name"
                )

        # Validate timing intervals
        if self.activation_time_seconds >= self.total_duration_seconds:
            raise ValueError("activation_time_seconds must be strictly less than total_duration_seconds")

        if self.recovery_time_seconds is not None:
            if self.recovery_time_seconds <= self.activation_time_seconds:
                raise ValueError("recovery_time_seconds must be strictly greater than activation_time_seconds")
            if self.recovery_time_seconds > self.total_duration_seconds:
                raise ValueError("recovery_time_seconds cannot exceed total_duration_seconds")

        # Validate traffic surge special case & timing consistency
        if self.workload.profile_name == WorkloadProfile.TRAFFIC_SURGE:
            if self.primary_fault is not None:
                raise ValueError("Traffic surge scenario must not define a primary_fault")
            if self.recovery_time_seconds is None:
                raise ValueError("Traffic surge scenario must define a non-None recovery_time_seconds")
            surge_start = self.workload.surge_start_seconds or 0.0
            if self.activation_time_seconds != surge_start:
                raise ValueError(
                    f"Traffic surge activation_time_seconds ({self.activation_time_seconds}) must equal "
                    f"workload.surge_start_seconds ({surge_start})"
                )
            expected_surge_end = surge_start + (self.workload.surge_duration_seconds or 0.0)
            if self.recovery_time_seconds != expected_surge_end:
                raise ValueError(
                    f"Traffic surge recovery_time_seconds ({self.recovery_time_seconds}) must equal "
                    f"surge window end (surge_start_seconds + surge_duration_seconds = {expected_surge_end})"
                )
        else:
            if self.primary_fault is None:
                raise ValueError("Non-traffic-surge fault scenario must define a primary_fault")

        return self
