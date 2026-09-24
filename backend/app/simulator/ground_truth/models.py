import uuid
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.simulator.ground_truth.deterministic import build_reproducibility_key
from app.simulator.interfaces import GroundTruthRecord
from app.simulator.scenarios.models import ScenarioDefinition, ScenarioSystemMarker
from app.simulator.workload.models import WorkloadConfig


class ResearchRunContext(BaseModel):
    """Immutable execution context and provenance metadata for a simulator run."""

    model_config = ConfigDict(frozen=True)

    run_id: uuid.UUID
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)
    seed: int = Field(ge=0)

    topology_reference: str = Field(min_length=1)
    topology_version: str = Field(min_length=1)

    workload_identity: str = Field(min_length=1)
    workload_config: WorkloadConfig

    run_start_time: AwareDatetime

    total_duration_seconds: float = Field(gt=0.0)
    tick_seconds: float = Field(gt=0.0)

    reproducibility_key: str = Field(min_length=64, max_length=64)
    config_reference: str | None = None

    @model_validator(mode="after")
    def validate_run_context(self) -> "ResearchRunContext":
        if self.run_start_time.tzinfo is None:
            raise ValueError("run_start_time must be timezone-aware")
        if self.workload_identity != self.workload_config.config_identity():
            raise ValueError(
                f"workload_identity ({self.workload_identity!r}) does not match "
                f"workload_config.config_identity() ({self.workload_config.config_identity()!r})"
            )
        return self

    @classmethod
    def from_scenario(
        cls,
        run_id: uuid.UUID,
        scenario: ScenarioDefinition,
        run_start_time: AwareDatetime,
        seed: int | None = None,
        config_reference: str | None = None,
    ) -> "ResearchRunContext":
        """Construct a valid ResearchRunContext for a scenario and execution run."""
        effective_seed = seed if seed is not None else scenario.default_seed
        reproducibility_key = build_reproducibility_key(scenario, effective_seed)

        return cls(
            run_id=run_id,
            scenario_id=scenario.scenario_id,
            scenario_version=scenario.version,
            seed=effective_seed,
            topology_reference=scenario.topology_reference,
            topology_version=scenario.topology_version,
            workload_identity=scenario.workload.config_identity(),
            workload_config=scenario.workload,
            run_start_time=run_start_time,
            total_duration_seconds=scenario.total_duration_seconds,
            tick_seconds=scenario.workload.tick_seconds,
            reproducibility_key=reproducibility_key,
            config_reference=config_reference,
        )


class ScenarioRunTruth(BaseModel):
    """Machine-readable known ground truth for a completed simulator scenario run."""

    model_config = ConfigDict(frozen=True)

    run: ResearchRunContext
    scenario_id: str = Field(min_length=1)
    scenario_version: str = Field(min_length=1)

    expected_root_cause: str = Field(min_length=1)
    expected_symptoms: tuple[str, ...] = Field(min_length=1)
    expected_affected_services: tuple[str, ...] = Field(min_length=1)

    activation_time_seconds: float = Field(ge=0.0)
    recovery_time_seconds: float | None = Field(default=None, ge=0.0)
    recovery_condition: str = Field(min_length=1)

    system_marker: ScenarioSystemMarker | None = None

    fault_ground_truth_records: tuple[GroundTruthRecord, ...] = Field(default_factory=tuple)
