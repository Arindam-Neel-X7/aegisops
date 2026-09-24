import hashlib
from enum import StrEnum
from pydantic import BaseModel, ConfigDict, Field, model_validator


class WorkloadProfile(StrEnum):
    HEALTHY_BASELINE = "healthy_baseline"
    TRAFFIC_SURGE = "traffic_surge"


class WorkloadConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    profile_name: WorkloadProfile = Field(default=WorkloadProfile.HEALTHY_BASELINE)
    base_requests_per_second: float = Field(default=10.0, ge=0.0)
    tick_seconds: float = Field(default=1.0, gt=0.0)
    total_duration_seconds: float = Field(default=60.0, gt=0.0)

    # Traffic surge profile parameters
    surge_start_seconds: float | None = Field(default=None, ge=0.0)
    surge_duration_seconds: float | None = Field(default=None, gt=0.0)
    surge_requests_per_second: float | None = Field(default=None, ge=0.0)

    @model_validator(mode="after")
    def validate_profile_parameters(self) -> "WorkloadConfig":
        if self.profile_name == WorkloadProfile.TRAFFIC_SURGE:
            if self.surge_start_seconds is None:
                raise ValueError("surge_start_seconds is required for traffic_surge profile")
            if self.surge_duration_seconds is None:
                raise ValueError("surge_duration_seconds is required for traffic_surge profile")
            if self.surge_requests_per_second is None:
                raise ValueError("surge_requests_per_second is required for traffic_surge profile")
            if self.surge_requests_per_second <= self.base_requests_per_second:
                raise ValueError("surge_requests_per_second must be strictly greater than base_requests_per_second")
            if self.surge_start_seconds >= self.total_duration_seconds:
                raise ValueError("surge_start_seconds must be strictly less than total_duration_seconds")
            if self.surge_start_seconds + self.surge_duration_seconds > self.total_duration_seconds:
                raise ValueError("surge window (start + duration) cannot exceed total_duration_seconds")
        return self

    def config_identity(self) -> str:
        """Derive a deterministic 8-character hex identity from the canonical config representation."""
        canonical_json = self.model_dump_json()
        digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        return digest[:8]


class SyntheticRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str = Field(min_length=1)
    trace_id: str = Field(min_length=1)
    tick_index: int = Field(ge=0)
    sequence_in_tick: int = Field(ge=0)
    simulation_offset_seconds: float = Field(ge=0.0)
    source_service: str = Field(min_length=1)
    target_service: str = Field(min_length=1)
    route: list[str] = Field(min_length=1)
    accumulated_latency_ms: float = Field(ge=0.0)
    outcome: str = Field(default="success")
    status_code: int = Field(default=200)
    error: str | None = Field(default=None)
