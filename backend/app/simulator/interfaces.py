import uuid
from enum import StrEnum
from typing import Any, Protocol

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from app.telemetry.schemas import TelemetryEvent


class ServiceType(StrEnum):
    API = "api"
    WORKER = "worker"
    DATABASE = "database"
    CACHE = "cache"
    QUEUE = "queue"
    EXTERNAL = "external"
    GENERIC = "generic"


class DependencyType(StrEnum):
    SYNC = "sync"
    ASYNC = "async"
    DATA = "data"
    CONTROL = "control"
    GENERIC = "generic"


class ServiceNode(BaseModel):
    service_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str = Field(min_length=1)
    service_type: ServiceType = Field(default=ServiceType.GENERIC)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ServiceDependency(BaseModel):
    upstream_service_id: uuid.UUID
    downstream_service_id: uuid.UUID
    dependency_type: DependencyType = Field(default=DependencyType.GENERIC)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_endpoints(self) -> "ServiceDependency":
        if self.upstream_service_id == self.downstream_service_id:
            raise ValueError("Self-dependency is not allowed")
        return self


class ServiceTopology(BaseModel):
    topology_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    services: list[ServiceNode] = Field(default_factory=list)
    dependencies: list[ServiceDependency] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_topology(self) -> "ServiceTopology":
        # Duplicate service IDs rejected
        service_ids = [s.service_id for s in self.services]
        if len(service_ids) != len(set(service_ids)):
            raise ValueError("Duplicate service IDs found in topology")

        # Dependency endpoints must reference existing services inside ServiceTopology
        known_services = set(service_ids)
        for dep in self.dependencies:
            if dep.upstream_service_id not in known_services:
                raise ValueError(f"Upstream service_id {dep.upstream_service_id} not found in topology")
            if dep.downstream_service_id not in known_services:
                raise ValueError(f"Downstream service_id {dep.downstream_service_id} not found in topology")

        # Duplicate identical dependency edges rejected
        edge_signatures = set()
        for dep in self.dependencies:
            sig = (dep.upstream_service_id, dep.downstream_service_id, dep.dependency_type)
            if sig in edge_signatures:
                raise ValueError(f"Duplicate dependency edge found: {sig}")
            edge_signatures.add(sig)

        return self


class FaultType(StrEnum):
    LATENCY = "latency"
    ERROR = "error"
    TIMEOUT = "timeout"
    CRASH = "crash"
    RESOURCE = "resource"
    NETWORK = "network"


class FaultSpec(BaseModel):
    fault_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    target_service_id: uuid.UUID
    fault_type: FaultType
    duration_seconds: float | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_duration(self) -> "FaultSpec":
        if self.duration_seconds is not None and self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be > 0 if provided")
        return self


class FaultInjectionResult(BaseModel):
    fault_id: uuid.UUID
    accepted: bool
    message: str | None = None


class FaultInjector(Protocol):
    async def inject(self, fault: FaultSpec) -> FaultInjectionResult:
        ...

    async def recover(self, fault_id: uuid.UUID) -> FaultInjectionResult:
        ...


class GroundTruthRecord(BaseModel):
    record_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    fault_id: uuid.UUID
    target_service_id: uuid.UUID
    fault_type: FaultType
    injected_at: AwareDatetime
    expected_root_cause: str = Field(min_length=1)
    expected_affected_service_ids: list[uuid.UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_unique_affected_services(self) -> "GroundTruthRecord":
        if len(self.expected_affected_service_ids) != len(set(self.expected_affected_service_ids)):
            raise ValueError("Duplicate service IDs found in expected_affected_service_ids")
        return self


class TelemetryEmitter(Protocol):
    async def emit(self, event: TelemetryEvent) -> None:
        ...
