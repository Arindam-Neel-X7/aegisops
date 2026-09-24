import uuid
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class TelemetrySynthesisContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: uuid.UUID = Field(
        default_factory=lambda: uuid.uuid5(uuid.NAMESPACE_DNS, "aegisops:tenant:default")
    )
    environment: str = Field(default="simulation", min_length=1)
    run_start_time: AwareDatetime

    @model_validator(mode="after")
    def validate_context(self) -> "TelemetrySynthesisContext":
        if self.run_start_time.tzinfo is None:
            raise ValueError("run_start_time must be timezone-aware")
        return self
