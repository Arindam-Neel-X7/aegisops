import math
import random

from app.simulator.interfaces import ServiceTopology
from app.simulator.topology import CANONICAL_SERVICE_NAMES, build_canonical_topology
from app.simulator.workload.deterministic import derive_child_seed
from app.simulator.workload.models import (
    SyntheticRequest,
    WorkloadConfig,
    WorkloadProfile,
)

CANONICAL_REQUEST_ROUTE: list[str] = [
    "client",
    "api-gateway",
    "order-service",
    "payment-service",
    "database",
    "inventory-service",
    "database",
    "notification-service",
]

DEFAULT_HOP_LATENCIES_MS: dict[str, float] = {
    "client->api-gateway": 5.0,
    "api-gateway->order-service": 5.0,
    "order-service->payment-service": 10.0,
    "payment-service->database": 15.0,
    "order-service->inventory-service": 10.0,
    "inventory-service->database": 15.0,
    "order-service->notification-service": 5.0,
}


class DeterministicWorkloadGenerator:
    """Generates synthetic, deterministic request flows for the canonical topology."""

    def __init__(
        self,
        config: WorkloadConfig,
        topology: ServiceTopology | None = None,
        seed: int = 42,
    ) -> None:
        if seed < 0:
            raise ValueError(f"seed must be non-negative, got {seed}")

        self.config = config
        self.topology = topology if topology is not None else build_canonical_topology()
        self.seed = seed

        self._validate_topology()

        self._workload_seed = derive_child_seed(self.seed, "workload")
        self._config_id = self.config.config_identity()
        self._rng = random.Random(self._workload_seed)
        self._credit: float = 0.0

    def _validate_topology(self) -> None:
        known_service_names = {s.name for s in self.topology.services}
        missing = set(CANONICAL_SERVICE_NAMES) - known_service_names
        if missing:
            raise ValueError(f"Supplied topology is missing required canonical services: {sorted(missing)}")

    def effective_rps(self, simulation_offset_seconds: float) -> float:
        """Return the target request rate at the given simulation time offset."""
        if self.config.profile_name == WorkloadProfile.TRAFFIC_SURGE:
            start = self.config.surge_start_seconds or 0.0
            duration = self.config.surge_duration_seconds or 0.0
            if start <= simulation_offset_seconds < start + duration:
                return self.config.surge_requests_per_second or 0.0
        return self.config.base_requests_per_second

    def requests_for_tick(
        self,
        tick_index: int,
        simulation_offset_seconds: float | None = None,
    ) -> list[SyntheticRequest]:
        """Deterministically produce synthetic requests for a specific simulation tick."""
        if tick_index < 0:
            raise ValueError(f"tick_index must be non-negative, got {tick_index}")

        sim_offset = (
            simulation_offset_seconds
            if simulation_offset_seconds is not None
            else float(tick_index) * self.config.tick_seconds
        )
        if sim_offset < 0.0:
            raise ValueError(f"simulation_offset_seconds must be non-negative, got {sim_offset}")

        rps = self.effective_rps(sim_offset)
        self._credit += rps * self.config.tick_seconds
        count = int(math.floor(self._credit + 1e-9))
        self._credit -= count

        base_latency = sum(DEFAULT_HOP_LATENCIES_MS.values())
        requests: list[SyntheticRequest] = []

        for seq in range(count):
            req_id = f"req-{self._config_id}-{self.seed:08x}-{tick_index:06d}-{seq:04d}"
            trace_id = f"trace-{self._config_id}-{self.seed:08x}-{tick_index:06d}-{seq:04d}"
            jitter = round(self._rng.uniform(0.0, 5.0), 3)
            accumulated_latency = round(base_latency + jitter, 3)

            req = SyntheticRequest(
                request_id=req_id,
                trace_id=trace_id,
                tick_index=tick_index,
                sequence_in_tick=seq,
                simulation_offset_seconds=sim_offset,
                source_service="client",
                target_service="api-gateway",
                route=list(CANONICAL_REQUEST_ROUTE),
                accumulated_latency_ms=accumulated_latency,
                outcome="success",
                status_code=200,
                error=None,
            )
            requests.append(req)

        return requests

    def reset(self) -> None:
        """Reset the generator PRNG stream and fractional request accumulator."""
        self._rng = random.Random(self._workload_seed)
        self._credit = 0.0
