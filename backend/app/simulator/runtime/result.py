from typing import Any
import uuid
from pydantic import BaseModel, ConfigDict

from app.simulator.ground_truth.models import ScenarioRunTruth
from app.telemetry.schemas import TelemetryEvent


class ScenarioLifecycleTransition(BaseModel):
    """Execution lifecycle boundary marker."""

    model_config = ConfigDict(frozen=True)

    phase: str
    simulation_offset_seconds: float
    details: str = ""


class ScenarioRunResult(BaseModel):
    """Detached execution outcome and captured evidence for a completed scenario run."""

    model_config = ConfigDict(frozen=True)

    run_id: uuid.UUID
    scenario_id: str
    scenario_version: str
    seed: int
    reproducibility_key: str

    scenario_truth: ScenarioRunTruth
    telemetry_events: tuple[TelemetryEvent, ...]
    request_count: int
    transitions: tuple[ScenarioLifecycleTransition, ...]


def normalize_run_result(result: ScenarioRunResult) -> dict[str, Any]:
    """Normalize a ScenarioRunResult for deterministic research replay comparison.

    - Excludes execution-specific run_id.
    - Converts absolute timestamps into relative simulation offsets.
    - Preserves all deterministic semantic fields, event IDs, request IDs, trace IDs, and payloads.
    """
    start_time = result.scenario_truth.run.run_start_time

    normalized_telemetry = []
    for ev in result.telemetry_events:
        offset_sec = round((ev.event_time - start_time).total_seconds(), 6)
        normalized_telemetry.append(
            {
                "event_id": str(ev.event_id),
                "offset_seconds": offset_sec,
                "service": ev.service,
                "event_type": str(ev.event_type),
                "severity": str(ev.severity),
                "trace_id": ev.trace_id,
                "payload": ev.payload,
            }
        )

    normalized_ground_truth = []
    for rec in result.scenario_truth.fault_ground_truth_records:
        injected_offset = round((rec.injected_at - start_time).total_seconds(), 6)
        normalized_ground_truth.append(
            {
                "record_id": str(rec.record_id),
                "fault_id": str(rec.fault_id),
                "target_service_id": str(rec.target_service_id),
                "fault_type": str(rec.fault_type),
                "injected_offset_seconds": injected_offset,
                "expected_root_cause": rec.expected_root_cause,
                "expected_affected_service_ids": [
                    str(uid) for uid in rec.expected_affected_service_ids
                ],
                "metadata": rec.metadata,
            }
        )

    normalized_transitions = [
        {"phase": t.phase, "offset_seconds": t.simulation_offset_seconds, "details": t.details}
        for t in result.transitions
    ]

    return {
        "scenario_id": result.scenario_id,
        "scenario_version": result.scenario_version,
        "seed": result.seed,
        "reproducibility_key": result.reproducibility_key,
        "request_count": result.request_count,
        "expected_root_cause": result.scenario_truth.expected_root_cause,
        "expected_symptoms": list(result.scenario_truth.expected_symptoms),
        "expected_affected_services": list(result.scenario_truth.expected_affected_services),
        "activation_time_seconds": result.scenario_truth.activation_time_seconds,
        "recovery_time_seconds": result.scenario_truth.recovery_time_seconds,
        "recovery_condition": result.scenario_truth.recovery_condition,
        "ground_truth_records": normalized_ground_truth,
        "telemetry_events": normalized_telemetry,
        "transitions": normalized_transitions,
    }
