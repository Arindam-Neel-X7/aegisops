import hashlib
import json
from typing import Any
import uuid

from app.simulator.scenarios.models import ScenarioDefinition

GROUND_TRUTH_NAMESPACE: uuid.UUID = uuid.uuid5(
    uuid.NAMESPACE_DNS, "aegisops:simulator:ground-truth"
)


def build_reproducibility_key(
    scenario: ScenarioDefinition,
    seed: int,
) -> str:
    """Derive a stable 64-character SHA-256 hex digest representing the effective simulator configuration.

    Includes:
    - scenario identity and version
    - seed
    - topology reference and version
    - workload configuration
    - primary fault configuration (or workload-only marker)
    - timing configuration

    Excludes:
    - run_id (execution provenance)
    - run_start_time (wall-clock/execution origin)
    """
    if seed < 0:
        raise ValueError(f"seed must be non-negative, got {seed}")

    fault_payload: dict[str, Any]
    if scenario.primary_fault is not None:
        fault = scenario.primary_fault.fault
        fault_payload = {
            "fault_id": str(fault.fault_id),
            "target_service_id": str(fault.target_service_id),
            "fault_type": str(fault.fault_type),
            "duration_seconds": fault.duration_seconds,
            "parameters": fault.parameters,
        }
    else:
        fault_payload = {"marker": "no-primary-fault:workload-condition"}

    marker_payload: dict[str, Any] | None = None
    if scenario.system_marker is not None:
        marker_payload = {
            "marker_name": scenario.system_marker.marker_name,
            "service": scenario.system_marker.service,
            "payload": scenario.system_marker.payload,
        }

    canonical_obj = {
        "scenario_id": scenario.scenario_id,
        "scenario_version": scenario.version,
        "seed": seed,
        "topology_reference": scenario.topology_reference,
        "topology_version": scenario.topology_version,
        "workload_config": scenario.workload.model_dump(),
        "fault_config": fault_payload,
        "timing_config": {
            "activation_time_seconds": scenario.activation_time_seconds,
            "observation_window_seconds": scenario.observation_window_seconds,
            "recovery_time_seconds": scenario.recovery_time_seconds,
            "post_recovery_observation_window_seconds": scenario.post_recovery_observation_window_seconds,
            "total_duration_seconds": scenario.total_duration_seconds,
            "tick_seconds": scenario.workload.tick_seconds,
        },
        "system_marker": marker_payload,
    }

    canonical_json = json.dumps(canonical_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def deterministic_ground_truth_record_id(
    reproducibility_key: str, fault_id: uuid.UUID
) -> uuid.UUID:
    """Derive deterministic UUIDv5 identifier for a GroundTruthRecord."""
    return uuid.uuid5(GROUND_TRUTH_NAMESPACE, f"{reproducibility_key}:{fault_id}")
