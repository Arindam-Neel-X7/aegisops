"""Task 4.12-C1 unit tests: incident publication contract, serialization & validation.

Pure contract verification without starting Kafka, PostgreSQL, Neo4j, or the
runtime coordinator. Embeds the frozen A1-R1 wire oracles (FIX-01-REV1,
FIX-01-REV1-NO-PRODUCER-VERSION, FIX-01-REV2) for standalone CI portability.
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any
from uuid import NAMESPACE_DNS, UUID, uuid5

from pydantic import ValidationError
import pytest

from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import AnomalySignal
from app.anomaly.serialization import to_canonical_telemetry_event
from app.correlation.aggregate import create_or_update_incident
from app.correlation.blast_radius import (
    BlastRadiusPolicy,
    compute_context_fingerprint,
)
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
    serialize_correlation_config,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.grouping import group_candidate_signals
from app.correlation.handoff import (
    build_phase5_handoff,
    compute_handoff_fingerprint,
    derive_enriched_handoff_id,
    serialize_phase5_handoff,
)
from app.correlation.models import (
    CorrelationWindowState,
    IncidentAggregate,
    IncidentSignalReference,
    IncidentStatus,
    TopologySnapshotReference,
)
from app.correlation.persistence_repository import PersistedPublicationIntent
import app.correlation.publication as publication_mod
from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    ENVELOPE_SCHEMA_VERSION,
    HANDOFF_SCHEMA_VERSION,
    INCIDENT_PUBLICATION_EVENT_NAMESPACE,
    INCIDENT_PUBLICATION_EVENT_NAMESPACE_NAME,
    INCIDENT_PUBLICATION_EVENT_TYPE,
    INCIDENT_PUBLICATION_TOPIC,
    INCIDENT_SCHEMA_VERSION,
    MAX_KAFKA_HEADERS_TOTAL_BYTES,
    MAX_KAFKA_HEADER_VALUE_BYTES,
    MAX_KAFKA_KEY_BYTES,
    MAX_KAFKA_RECORD_ESTIMATED_BYTES,
    MAX_KAFKA_VALUE_BYTES,
    PAYLOAD_TOP_LEVEL_KEYS,
    PUBLICATION_CONTRACT_VERSION,
    PUBLICATION_FINGERPRINT_INPUT_KEYS,
    TRANSPORT_FIELDS_EXCLUDED_FROM_SEMANTIC_EQUALITY,
    IncidentPublicationContractError,
    IncidentPublicationContractResult,
    IncidentPublicationHeaderError,
    IncidentPublicationPayload,
    IncidentPublicationRecord,
    IncidentPublicationSizeLimitError,
    IncidentPublicationValidationError,
    IncidentPublicationValidationOutcome,
    IncidentPublicationVersionError,
    PublicationExecutionLineage,
    PublicationTransportMetadata,
    build_incident_publication,
    build_incident_publication_from_handoff,
    build_incident_publication_from_intent,
    build_publication_fingerprint_projection,
    build_publication_kafka_headers,
    build_publication_kafka_key,
    compute_incident_publication_fingerprint,
    decode_incident_publication_from_envelope,
    decode_incident_publication_structural,
    derive_incident_publication_event_id,
    incident_publication_to_telemetry_event,
    parse_and_validate_publication_headers,
    publications_semantically_equal,
    try_build_incident_publication,
    try_decode_incident_publication,
    try_validate_incident_publication,
    validate_incident_publication,
    validate_incident_publication_from_envelope,
)
from app.correlation.scoring import score_candidate_pair
from app.correlation.serialization import serialize_incident_aggregate
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import TelemetryEnvelope, parse_kafka_headers
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
    construct_kafka_key,
    deserialize_event,
    serialize_event,
)

# ============================================================================
# FROZEN A1-R1 PINNED INPUTS AND WIRE ORACLES (PORTABLE STANDALONE COPY)
# ============================================================================

PINNED_TOPOLOGY_ID = "77bb041d-fb4c-5370-9b99-45bef0082479"
PINNED_TOPOLOGY_VERSION = "1.0.0"
PINNED_TOPOLOGY_CAPTURED_AT = "2026-11-27T00:00:00Z"
PINNED_TOPOLOGY_FINGERPRINT = (
    "2d9f8e9a54a8d1ce103de39be842b4fcfa8e36b32b41f4f0d7abc1820d6779dd"
)
PINNED_CONFIG_FINGERPRINT = (
    "b5c8773d088faf9d643149d7236df24069867529e0597580341e1d90c2f57aa5"
)

PINNED_BLAST_RADIUS_POLICY: dict[str, Any] = {
    "allowed_edge_types": ["async", "control", "data", "generic", "sync"],
    "direction": "downstream",
    "max_edges": 1024,
    "max_examined_arcs": 65536,
    "max_hops": 2,
    "max_services": 512,
    "max_snapshot_age_seconds": None,
    "policy_version": "blast-radius-policy/1.0",
}

PINNED_CORRELATION_CONFIG: dict[str, Any] = {
    "acceptance_threshold": 0.75,
    "allowed_lateness_seconds": 30,
    "conflicting_signal_disposition": "reject",
    "disconnected_signal_disposition": "isolate",
    "duplicate_signal_disposition": "ignore",
    "hop_limit": 3,
    "insufficient_context_disposition": "ignore",
    "late_signal_disposition": "reject",
    "maximum_group_size": 100,
    "policy_version": "v1.0",
    "quiet_period_seconds": 10,
    "schema_version": "1.0.0",
    "score_weights": {
        "context": 0.1,
        "service": 0.2,
        "symptom": 0.2,
        "temporal": 0.3,
        "topology": 0.2,
    },
    "severity_aggregation": "maximum",
    "window_seconds": 300,
}

PINNED_EXECUTION_CONTEXT: dict[str, Any] = {
    "reproducibility_key": "task-4.12-a1-primary-v1",
    "run_id": "71331f12-256e-52cf-950e-a27a59451741",
    "scenario_id": "task-4.12-controlled-runtime",
    "scenario_version": "1.0.0",
    "seed": 42,
}

PINNED_SIGNALS: dict[str, dict[str, Any]] = {
    "S1": {
        "anomaly_score": 0.88,
        "environment": "production",
        "event_time": "2026-11-27T00:01:00Z",
        "evidence": [
            {
                "details": {},
                "deviation": 0.23,
                "evidence_id": "b3ff3f0b-04fb-596c-980d-17b6c462f87f",
                "evidence_type": "metric_spike",
                "expected_value": 0.01,
                "metric_or_feature": "http_error_rate",
                "observed_value": 0.24,
                "timestamp": "2026-11-27T00:01:00Z",
            }
        ],
        "metric_or_feature": "http_error_rate",
        "model_name": "isolation_forest",
        "model_version": "1.0.0",
        "reproducibility_key": "task-4.12-a1-primary-v1",
        "run_id": "71331f12-256e-52cf-950e-a27a59451741",
        "scenario_id": "task-4.12-controlled-runtime",
        "scenario_version": "1.0.0",
        "schema_version": "1.0",
        "seed": 42,
        "service": "order-service",
        "severity": "error",
        "signal_id": "9ebb03b0-57f7-5c80-a7f1-e1c212016268",
        "source_event_ids": ["3d2e15b5-dbf6-5156-9862-213b9d6427a2"],
        "source_event_time_window": None,
        "tags": {},
        "tenant_id": "1dfc0469-7644-5efe-8b07-d1c7c31254f5",
        "threshold_or_calibration": {
            "calibrated_at": "2026-11-27T00:00:00Z",
            "calibration_version": "cal-v1",
            "method": "zscore-threshold",
            "parameters": {"k": 3.0},
            "schema_version": "1.0",
            "threshold_value": 0.75,
        },
        "trace_id": None,
    },
    "S2": {
        "anomaly_score": 0.95,
        "environment": "production",
        "event_time": "2026-11-27T00:01:10Z",
        "evidence": [
            {
                "details": {},
                "deviation": 0.61,
                "evidence_id": "1721404c-9d6f-541d-add4-c0eefd5fcc0a",
                "evidence_type": "metric_spike",
                "expected_value": 0.01,
                "metric_or_feature": "http_error_rate",
                "observed_value": 0.62,
                "timestamp": "2026-11-27T00:01:10Z",
            }
        ],
        "metric_or_feature": "http_error_rate",
        "model_name": "isolation_forest",
        "model_version": "1.0.0",
        "reproducibility_key": "task-4.12-a1-primary-v1",
        "run_id": "71331f12-256e-52cf-950e-a27a59451741",
        "scenario_id": "task-4.12-controlled-runtime",
        "scenario_version": "1.0.0",
        "schema_version": "1.0",
        "seed": 42,
        "service": "order-service",
        "severity": "critical",
        "signal_id": "c34041f1-3bbe-5fcc-8219-734e3abab88a",
        "source_event_ids": ["73c1d1f9-565c-59c1-80c1-db883bb2756d"],
        "source_event_time_window": None,
        "tags": {},
        "tenant_id": "1dfc0469-7644-5efe-8b07-d1c7c31254f5",
        "threshold_or_calibration": {
            "calibrated_at": "2026-11-27T00:00:00Z",
            "calibration_version": "cal-v1",
            "method": "zscore-threshold",
            "parameters": {"k": 3.0},
            "schema_version": "1.0",
            "threshold_value": 0.75,
        },
        "trace_id": None,
    },
}

EXPECTED_REACHABLE_SERVICES = [
    {
        "minimum_depth": 1,
        "name": "notification-service",
        "service_id": "10ed11a8-4faf-50f5-9903-2417c842278c",
    },
    {
        "minimum_depth": 1,
        "name": "payment-service",
        "service_id": "2b8e9c49-61b0-5a53-821d-6193136b1db2",
    },
    {
        "minimum_depth": 1,
        "name": "inventory-service",
        "service_id": "936fb446-a6f1-5cf9-88d2-65789b6e6eae",
    },
    {
        "minimum_depth": 2,
        "name": "database",
        "service_id": "a50eab12-f3a8-544a-be74-060c56b3e254",
    },
    {
        "minimum_depth": 0,
        "name": "order-service",
        "service_id": "fdface04-c1df-5daa-ab47-23debcebb2fd",
    },
]

WIRE_ORACLES: dict[str, dict[str, Any]] = {
    "FIX-01-REV1": {
        "aiokafka_estimated_record_bytes": 8075,
        "correlation_state": "collecting",
        "environment": "production",
        "event_id": "888578ec-eac1-52f8-9184-83ac1ae2c78a",
        "handoff_bytes": 6536,
        "handoff_fingerprint": "28c61a966837b2996e7a5dc655e60b688be2e02b0acc37ca104d97f12210aba8",
        "handoff_id": "1bd0689d-a88c-5e26-8b37-42136a4c93b2",
        "handoff_sha256": "0a651b615a4f33e5404fcd673877ace4286fff0aabf40f1d937952958fde62b9",
        "incident_id": "1b914870-81d6-50eb-beae-9f332d3aae69",
        "incident_number": "INC-1B91487081D650EBBEAE9F332D3AAE69",
        "intent_time": "2026-11-27T00:01:05Z",
        "kafka_headers": [
            ["run_id", "71331f12-256e-52cf-950e-a27a59451741"],
            ["scenario_id", "task-4.12-controlled-runtime"],
            ["scenario_version", "1.0.0"],
            ["reproducibility_key", "task-4.12-a1-primary-v1"],
            ["seed", "42"],
            ["producer_version", "phase4-incident-publisher/1.0"],
        ],
        "kafka_headers_total_bytes": 195,
        "kafka_key_bytes": 61,
        "kafka_key_utf8": "1dfc0469-7644-5efe-8b07-d1c7c31254f5:production:order-service",
        "kafka_topic": "aegis.incidents",
        "kafka_value_bytes": 7721,
        "kafka_value_sha256": "9c9e20c228ee31b1cfe5e75b41caa8dcfb5cfb38a15dd908d3fbd39901c45d5a",
        "membership_fingerprint": "1aca41082b21800de48e03941a889b83ca82c5ecc9415364043c4b5d204012eb",
        "primary_service": "order-service",
        "producer_version": "phase4-incident-publisher/1.0",
        "publication_fingerprint": "c9d5c7a97f9458183dee754c9b74d7057880350a4c02e7772245de17fa9aa06c",
        "revision": 1,
        "revision_fingerprint": "df43523c859542baa0196d3e21eebeb40803fd620963f2b852af7fa11fbe323b",
        "severity": "error",
        "signal_ids": ["9ebb03b0-57f7-5c80-a7f1-e1c212016268"],
        "tenant_id": "1dfc0469-7644-5efe-8b07-d1c7c31254f5",
    },
    "FIX-01-REV1-NO-PRODUCER-VERSION": {
        "aiokafka_estimated_record_bytes": 8001,
        "correlation_state": "collecting",
        "environment": "production",
        "event_id": "888578ec-eac1-52f8-9184-83ac1ae2c78a",
        "handoff_bytes": 6536,
        "handoff_fingerprint": "28c61a966837b2996e7a5dc655e60b688be2e02b0acc37ca104d97f12210aba8",
        "handoff_id": "1bd0689d-a88c-5e26-8b37-42136a4c93b2",
        "handoff_sha256": "0a651b615a4f33e5404fcd673877ace4286fff0aabf40f1d937952958fde62b9",
        "incident_id": "1b914870-81d6-50eb-beae-9f332d3aae69",
        "incident_number": "INC-1B91487081D650EBBEAE9F332D3AAE69",
        "intent_time": "2026-11-27T00:01:05Z",
        "kafka_headers": [
            ["run_id", "71331f12-256e-52cf-950e-a27a59451741"],
            ["scenario_id", "task-4.12-controlled-runtime"],
            ["scenario_version", "1.0.0"],
            ["reproducibility_key", "task-4.12-a1-primary-v1"],
            ["seed", "42"],
        ],
        "kafka_headers_total_bytes": 150,
        "kafka_key_bytes": 61,
        "kafka_key_utf8": "1dfc0469-7644-5efe-8b07-d1c7c31254f5:production:order-service",
        "kafka_topic": "aegis.incidents",
        "kafka_value_bytes": 7694,
        "kafka_value_sha256": "ee290d4063d96d8215a5dba5b1197339dd5443917b6789fca553e93f3ff8c5c1",
        "membership_fingerprint": "1aca41082b21800de48e03941a889b83ca82c5ecc9415364043c4b5d204012eb",
        "primary_service": "order-service",
        "producer_version": None,
        "publication_fingerprint": "1cf7a4c7ccd7c7432aac3bd03479c209676367ca4061b80511306d9a4986f919",
        "revision": 1,
        "revision_fingerprint": "df43523c859542baa0196d3e21eebeb40803fd620963f2b852af7fa11fbe323b",
        "severity": "error",
        "signal_ids": ["9ebb03b0-57f7-5c80-a7f1-e1c212016268"],
        "tenant_id": "1dfc0469-7644-5efe-8b07-d1c7c31254f5",
    },
    "FIX-01-REV2": {
        "aiokafka_estimated_record_bytes": 8777,
        "correlation_state": "settling",
        "environment": "production",
        "event_id": "a63b7cb2-9100-526f-9a6f-cd91122c2b18",
        "handoff_bytes": 7234,
        "handoff_fingerprint": "502a5823268d6c5136bd0b54888e728d483f135f14136209f627de8d9c4c290a",
        "handoff_id": "2902f2d3-c0e2-5571-b411-4182ebf60195",
        "handoff_sha256": "89dfeb91ebb2e5c22bcadc06aec3719c8c4ea03650c739a2da09d768a59edc30",
        "incident_id": "1b914870-81d6-50eb-beae-9f332d3aae69",
        "incident_number": "INC-1B91487081D650EBBEAE9F332D3AAE69",
        "intent_time": "2026-11-27T00:01:25Z",
        "kafka_headers": [
            ["run_id", "71331f12-256e-52cf-950e-a27a59451741"],
            ["scenario_id", "task-4.12-controlled-runtime"],
            ["scenario_version", "1.0.0"],
            ["reproducibility_key", "task-4.12-a1-primary-v1"],
            ["seed", "42"],
            ["producer_version", "phase4-incident-publisher/1.0"],
        ],
        "kafka_headers_total_bytes": 195,
        "kafka_key_bytes": 61,
        "kafka_key_utf8": "1dfc0469-7644-5efe-8b07-d1c7c31254f5:production:order-service",
        "kafka_topic": "aegis.incidents",
        "kafka_value_bytes": 8422,
        "kafka_value_sha256": "39fd8aa6ea2fa9ba068500eec2cd690a9aeef453e5a649e2cdbea14fb7ad0e86",
        "membership_fingerprint": "602272bb0f0fa63b020ce42f525a17587dfdb7438a7f65c00efee164804f524c",
        "primary_service": "order-service",
        "producer_version": "phase4-incident-publisher/1.0",
        "publication_fingerprint": "ab40a770bafef6f42a429e0fd13bb858dd7f118e371d8f51739f390fe21b3193",
        "revision": 2,
        "revision_fingerprint": "38180583cd9b5d67795e1b6a7cba0e6dc93c200196267d963360da05323ae8ed",
        "severity": "critical",
        "signal_ids": [
            "9ebb03b0-57f7-5c80-a7f1-e1c212016268",
            "c34041f1-3bbe-5fcc-8219-734e3abab88a",
        ],
        "tenant_id": "1dfc0469-7644-5efe-8b07-d1c7c31254f5",
    },
}


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture(scope="module")
def fix01_bundle() -> dict[str, Any]:
    """Assemble FIX-01 rev 1 and rev 2 aggregates using frozen production constructors."""
    snapshot = asyncio.run(CanonicalTopologyProvider().get_snapshot())
    assert snapshot.snapshot_fingerprint == PINNED_TOPOLOGY_FINGERPRINT

    topology_ref = TopologySnapshotReference(
        topology_id=UUID(PINNED_TOPOLOGY_ID),
        topology_version=PINNED_TOPOLOGY_VERSION,
        snapshot_fingerprint=snapshot.snapshot_fingerprint,
        captured_at=_utc(PINNED_TOPOLOGY_CAPTURED_AT),
        provider="canonical",
    )
    policy = BlastRadiusPolicy.model_validate_json(
        json.dumps(PINNED_BLAST_RADIUS_POLICY)
    )
    config = CorrelationConfig.model_validate_json(
        json.dumps(PINNED_CORRELATION_CONFIG)
    )
    assert compute_correlation_config_fingerprint(config) == PINNED_CONFIG_FINGERPRINT
    context = TelemetryExecutionContext.model_validate(PINNED_EXECUTION_CONTEXT)

    inputs: dict[str, AnomalyCorrelationInput] = {}
    for name in ("S1", "S2"):
        signal = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS[name]))
        inputs[name] = AnomalyCorrelationInput(
            signal=signal,
            lineage=build_anomaly_reproducibility_lineage(signal),
        )

    t1 = _utc("2026-11-27T00:01:05Z")
    grouped_1 = group_candidate_signals(
        [inputs["S1"]],
        [],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=t1,
    )
    assert len(grouped_1.groups) == 1
    res_1 = create_or_update_incident(
        group=grouped_1.groups[0],
        signal_inputs={inputs["S1"].signal.signal_id: inputs["S1"]},
        processing_time=t1,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    )

    t2 = _utc("2026-11-27T00:01:25Z")
    pair_evidence = score_candidate_pair(
        inputs["S1"], inputs["S2"], topology_snapshot=snapshot, config=config
    )
    grouped_2 = group_candidate_signals(
        [inputs["S1"], inputs["S2"]],
        [pair_evidence],
        topology_snapshot=snapshot,
        config=config,
        event_time_watermark=t1,
    )
    assert len(grouped_2.groups) == 1
    res_2 = create_or_update_incident(
        group=grouped_2.groups[0],
        signal_inputs={
            inputs["S1"].signal.signal_id: inputs["S1"],
            inputs["S2"].signal.signal_id: inputs["S2"],
        },
        processing_time=t2,
        desired_correlation_state=CorrelationWindowState.SETTLING,
        existing_aggregate=res_1.aggregate,
    )

    return {
        "snapshot": snapshot,
        "topology_ref": topology_ref,
        "policy": policy,
        "config": config,
        "context": context,
        "rev1": res_1.aggregate,
        "rev2": res_2.aggregate,
    }


def _assert_matches_oracle(
    record: IncidentPublicationRecord, oracle: dict[str, Any]
) -> None:
    assert record.topic == KafkaTopic.INCIDENTS
    assert record.topic.value == oracle["kafka_topic"]
    assert str(record.event_id) == oracle["event_id"]
    assert record.publication_fingerprint == oracle["publication_fingerprint"]
    assert record.kafka_key.decode("utf-8") == oracle["kafka_key_utf8"]
    assert len(record.kafka_key) == oracle["kafka_key_bytes"]
    assert record.kafka_headers_json == oracle["kafka_headers"]
    assert record.kafka_headers_total_bytes == oracle["kafka_headers_total_bytes"]
    assert len(record.kafka_value) == oracle["kafka_value_bytes"]
    assert record.value_sha256 == oracle["kafka_value_sha256"]
    assert record.estimated_record_bytes == oracle["aiokafka_estimated_record_bytes"]
    assert record.producer_version == oracle["producer_version"]

    handoff = record.handoff
    handoff_bytes = serialize_phase5_handoff(handoff)
    assert len(handoff_bytes) == oracle["handoff_bytes"]
    assert _sha256(handoff_bytes) == oracle["handoff_sha256"]
    assert str(handoff.handoff_id) == oracle["handoff_id"]
    assert handoff.handoff_fingerprint == oracle["handoff_fingerprint"]
    assert handoff.membership_fingerprint == oracle["membership_fingerprint"]

    incident = record.incident
    assert str(incident.tenant_id) == oracle["tenant_id"]
    assert str(incident.incident_id) == oracle["incident_id"]
    assert incident.incident_number == oracle["incident_number"]
    assert incident.revision == oracle["revision"]
    assert incident.revision_fingerprint == oracle["revision_fingerprint"]
    assert incident.severity.value == oracle["severity"]
    assert incident.correlation_state.value == oracle["correlation_state"]
    assert incident.environment == oracle["environment"]
    assert incident.primary_service == oracle["primary_service"]
    assert [str(s.signal_id) for s in incident.signal_references] == oracle[
        "signal_ids"
    ]
    assert (
        record.payload.intent_time.isoformat().replace("+00:00", "Z")
        == oracle["intent_time"]
    )

    reachable = [
        {
            "minimum_depth": s.minimum_depth,
            "name": s.name,
            "service_id": str(s.service_id),
        }
        for s in handoff.topology_context.reachable_services
    ]
    assert reachable == EXPECTED_REACHABLE_SERVICES


def _resign_raw_envelope(
    envelope_dict: dict[str, Any],
) -> tuple[bytes, bytes, list[tuple[str, bytes]]]:
    """Re-sign a mutated envelope JSON dict so only the targeted invariant changes."""
    payload = envelope_dict["payload"]
    handoff = payload["handoff"]
    topo_ctx = handoff["topology_context"]
    topo_ctx["context_fingerprint"] = compute_context_fingerprint(topo_ctx)
    handoff["handoff_fingerprint"] = compute_handoff_fingerprint(handoff)
    handoff["handoff_id"] = str(
        derive_enriched_handoff_id(handoff["handoff_fingerprint"])
    )

    topo_ref = topo_ctx["topology_snapshot"]
    projection = {
        "correlation_config_fingerprint": payload["correlation_config_fingerprint"],
        "correlation_policy_version": payload["correlation_policy_version"],
        "envelope_schema_version": envelope_dict["schema_version"],
        "environment": envelope_dict["environment"],
        "event_id": payload["event_id"],
        "event_time": payload["intent_time"],
        "event_type": envelope_dict["event_type"],
        "execution_lineage": payload["execution_lineage"],
        "handoff_fingerprint": handoff["handoff_fingerprint"],
        "handoff_id": handoff["handoff_id"],
        "incident_id": payload["incident_id"],
        "incident_number": payload["incident_number"],
        "intent_time": payload["intent_time"],
        "membership_fingerprint": handoff["membership_fingerprint"],
        "producer_version": payload["producer_version"],
        "publication_contract_version": payload["publication_contract_version"],
        "revision": payload["revision"],
        "revision_fingerprint": payload["revision_fingerprint"],
        "service": envelope_dict["service"],
        "severity": envelope_dict["severity"],
        "tenant_id": payload["tenant_id"],
        "topology_id": topo_ref["topology_id"],
        "topology_snapshot_fingerprint": topo_ref["snapshot_fingerprint"],
        "topology_version": topo_ref["topology_version"],
        "trace_id": envelope_dict["trace_id"],
    }
    payload["publication_fingerprint"] = compute_incident_publication_fingerprint(
        projection
    )
    canonical_payload = json.loads(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    )
    event = TelemetryEvent(
        schema_version=envelope_dict["schema_version"],
        event_id=UUID(envelope_dict["event_id"]),
        event_time=_utc(envelope_dict["event_time"]),
        tenant_id=UUID(envelope_dict["tenant_id"]),
        environment=envelope_dict["environment"],
        service=envelope_dict["service"],
        event_type=EventType(envelope_dict["event_type"]),
        severity=EventSeverity(envelope_dict["severity"]),
        trace_id=envelope_dict["trace_id"],
        payload=canonical_payload,
    )
    ctx = TelemetryExecutionContext.model_validate(payload["execution_lineage"])
    headers = list(build_publication_kafka_headers(ctx, payload["producer_version"]))
    return serialize_event(event), construct_kafka_key(event), headers


# ============================================================================
# C1-01: EXACT KNOWN BYTES, KEY, ORDERED HEADERS, EVENT ID & FINGERPRINT
# ============================================================================


def test_frozen_constants_and_namespace_pin() -> None:
    assert ENVELOPE_SCHEMA_VERSION == "1.0"
    assert INCIDENT_SCHEMA_VERSION == "1.2"
    assert HANDOFF_SCHEMA_VERSION == "phase5-investigation-handoff/1.0"
    assert PUBLICATION_CONTRACT_VERSION == "incident-publication/1.0"
    assert DEFAULT_PUBLICATION_PRODUCER_VERSION == "phase4-incident-publisher/1.0"
    assert (
        INCIDENT_PUBLICATION_EVENT_NAMESPACE_NAME
        == "aegisops.phase4.incident-publication-event/v1"
    )
    assert str(INCIDENT_PUBLICATION_EVENT_NAMESPACE) == (
        "73e228d0-585c-5708-9ec8-3f83ec2b98e1"
    )
    assert INCIDENT_PUBLICATION_EVENT_NAMESPACE == uuid5(
        NAMESPACE_DNS, "aegisops.phase4.incident-publication-event/v1"
    )
    assert INCIDENT_PUBLICATION_EVENT_TYPE == EventType.INCIDENT
    assert INCIDENT_PUBLICATION_TOPIC == KafkaTopic.INCIDENTS
    assert MAX_KAFKA_KEY_BYTES == 512
    assert MAX_KAFKA_HEADER_VALUE_BYTES == 256
    assert MAX_KAFKA_HEADERS_TOTAL_BYTES == 1024
    assert MAX_KAFKA_VALUE_BYTES == 983_040
    assert MAX_KAFKA_RECORD_ESTIMATED_BYTES == 1_048_576
    assert len(PAYLOAD_TOP_LEVEL_KEYS) == 14
    assert len(PUBLICATION_FINGERPRINT_INPUT_KEYS) == 25
    assert set(TRANSPORT_FIELDS_EXCLUDED_FROM_SEMANTIC_EQUALITY) == set(
        PublicationTransportMetadata.model_fields.keys()
    )


@pytest.mark.parametrize(
    ("oracle_name", "revision_key", "producer_version"),
    [
        ("FIX-01-REV1", "rev1", DEFAULT_PUBLICATION_PRODUCER_VERSION),
        ("FIX-01-REV1-NO-PRODUCER-VERSION", "rev1", None),
        ("FIX-01-REV2", "rev2", DEFAULT_PUBLICATION_PRODUCER_VERSION),
    ],
)
def test_exact_wire_oracles_match_byte_for_byte(
    fix01_bundle: dict[str, Any],
    oracle_name: str,
    revision_key: str,
    producer_version: str | None,
) -> None:
    oracle = WIRE_ORACLES[oracle_name]
    incident: IncidentAggregate = fix01_bundle[revision_key]

    record = build_incident_publication(
        incident=incident,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
        producer_version=producer_version,
    )
    _assert_matches_oracle(record, oracle)

    # Conversion to TelemetryEvent reproduces exact wire bytes and key.
    event = incident_publication_to_telemetry_event(record)
    assert serialize_event(event) == record.kafka_value
    assert construct_kafka_key(event) == record.kafka_key
    assert event.schema_version == "1.0"
    assert event.event_type == EventType.INCIDENT
    assert event.trace_id is None
    assert tuple(event.payload.keys()) == PAYLOAD_TOP_LEVEL_KEYS

    # Distinct four-layer versions are preserved without conflation.
    assert event.schema_version == "1.0"
    assert event.payload["publication_contract_version"] == "incident-publication/1.0"
    assert (
        event.payload["handoff"]["schema_version"] == "phase5-investigation-handoff/1.0"
    )
    assert event.payload["handoff"]["incident"]["schema_version"] == "1.2"

    # Build from PersistedPublicationIntent produces byte-identical wire record.
    intent = PersistedPublicationIntent(
        tenant_id=incident.tenant_id,
        incident_id=incident.incident_id,
        revision=incident.revision,
        schema_version=incident.schema_version,
        revision_fingerprint=incident.revision_fingerprint,
        intent_time=incident.updated_at,
        aggregate=incident,
    )
    from_intent = build_incident_publication_from_intent(
        intent=intent,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
        producer_version=producer_version,
    )
    assert from_intent == record
    assert from_intent.kafka_value == record.kafka_value

    # Build from pre-assembled Phase5InvestigationHandoff is byte-identical.
    handoff = build_phase5_handoff(
        incident=incident,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        policy=fix01_bundle["policy"],
        as_of=incident.updated_at,
    )
    from_handoff = build_incident_publication_from_handoff(
        handoff=handoff,
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
        producer_version=producer_version,
        expected_topology=fix01_bundle["topology_ref"],
        snapshot=fix01_bundle["snapshot"],
        expected_blast_radius_policy=fix01_bundle["policy"],
    )
    assert from_handoff == record
    assert from_handoff.kafka_value == record.kafka_value


# ============================================================================
# C1-02: FULL HANDOFF EQUALITY, UNCHANGED INCIDENT & STRUCTURAL/SEMANTIC DECODE
# ============================================================================


@pytest.mark.parametrize(
    ("revision_key", "producer_version"),
    [
        ("rev1", DEFAULT_PUBLICATION_PRODUCER_VERSION),
        ("rev1", None),
        ("rev2", DEFAULT_PUBLICATION_PRODUCER_VERSION),
    ],
)
def test_full_handoff_equality_and_decode_roundtrip(
    fix01_bundle: dict[str, Any],
    revision_key: str,
    producer_version: str | None,
) -> None:
    incident: IncidentAggregate = fix01_bundle[revision_key]
    original_incident_bytes = serialize_incident_aggregate(incident)
    original_handoff = build_phase5_handoff(
        incident=incident,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        policy=fix01_bundle["policy"],
        as_of=incident.updated_at,
    )
    original_handoff_bytes = serialize_phase5_handoff(original_handoff)

    record = build_incident_publication(
        incident=incident,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
        producer_version=producer_version,
    )

    decoded = decode_incident_publication_structural(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=record.kafka_headers,
        require_canonical_bytes=True,
    )
    assert decoded == record
    assert decoded.kafka_value == record.kafka_value
    assert decoded.handoff == original_handoff
    assert serialize_phase5_handoff(decoded.handoff) == original_handoff_bytes
    assert decoded.incident == incident
    assert serialize_incident_aggregate(decoded.incident) == original_incident_bytes
    assert decoded.incident.topology_snapshot is None
    assert decoded.incident.blast_radius is None
    assert decoded.identity == record.identity

    validated = validate_incident_publication(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=record.kafka_headers,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
        expected_correlation_config=fix01_bundle["config"],
        expected_execution_context=fix01_bundle["context"],
        expected_producer_version=producer_version,
        expected_tenant_id=incident.tenant_id,
        require_canonical_bytes=True,
    )
    assert validated == record
    assert validated.handoff == original_handoff

    # Pretty-printed valid JSON succeeds structurally when require_canonical_bytes=False
    # and fails closed when require_canonical_bytes=True.
    pretty_bytes = json.dumps(
        json.loads(record.kafka_value.decode("utf-8")), indent=2
    ).encode("utf-8")
    decoded_pretty = decode_incident_publication_structural(
        raw_value=pretty_bytes,
        raw_key=record.kafka_key,
        headers=record.kafka_headers,
        require_canonical_bytes=False,
    )
    assert decoded_pretty == record
    assert decoded_pretty.kafka_value == record.kafka_value

    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=pretty_bytes,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            require_canonical_bytes=True,
        )


def test_non_raising_typed_result_contracts(fix01_bundle: dict[str, Any]) -> None:
    built = try_build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    assert isinstance(built, IncidentPublicationContractResult)
    assert built.ok is True
    assert built.outcome == IncidentPublicationValidationOutcome.VALID
    assert built.record is not None
    assert built.error_code is None
    assert built.error_type is None
    assert built.error_message is None

    decoded = try_decode_incident_publication(
        raw_value=built.record.kafka_value,
        raw_key=built.record.kafka_key,
        headers=built.record.kafka_headers,
    )
    assert decoded.ok is True
    assert decoded.record == built.record

    validated = try_validate_incident_publication(
        raw_value=built.record.kafka_value,
        raw_key=built.record.kafka_key,
        headers=built.record.kafka_headers,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
        expected_correlation_config=fix01_bundle["config"],
        expected_execution_context=fix01_bundle["context"],
        expected_producer_version=DEFAULT_PUBLICATION_PRODUCER_VERSION,
        expected_tenant_id=fix01_bundle["rev1"].tenant_id,
    )
    assert validated.ok is True
    assert validated.record == built.record

    # Typed error classifications
    bad_size = try_decode_incident_publication(
        raw_value=b"{" + b" " * MAX_KAFKA_VALUE_BYTES + b"}",
        raw_key=built.record.kafka_key,
        headers=built.record.kafka_headers,
    )
    assert bad_size.ok is False
    assert bad_size.outcome == IncidentPublicationValidationOutcome.SIZE_LIMIT_EXCEEDED
    assert bad_size.error_code == "INCIDENT_PUBLICATION_SIZE_LIMIT_ERROR"
    assert bad_size.error_type == "IncidentPublicationSizeLimitError"
    assert bad_size.record is None

    bad_hdr = try_decode_incident_publication(
        raw_value=built.record.kafka_value,
        raw_key=built.record.kafka_key,
        headers=[],
    )
    assert bad_hdr.ok is False
    assert bad_hdr.outcome == IncidentPublicationValidationOutcome.INVALID_HEADERS
    assert bad_hdr.error_code == "INCIDENT_PUBLICATION_HEADER_ERROR"

    wrong_env = json.loads(built.record.kafka_value.decode("utf-8"))
    wrong_env["schema_version"] = "2.0"
    bad_ver = try_decode_incident_publication(
        raw_value=json.dumps(wrong_env).encode("utf-8"),
        raw_key=built.record.kafka_key,
        headers=built.record.kafka_headers,
    )
    assert bad_ver.ok is False
    assert bad_ver.outcome == IncidentPublicationValidationOutcome.UNSUPPORTED_VERSION
    assert bad_ver.error_code == "INCIDENT_PUBLICATION_VERSION_ERROR"

    wrong_tenant = try_validate_incident_publication(
        raw_value=built.record.kafka_value,
        raw_key=built.record.kafka_key,
        headers=built.record.kafka_headers,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
        expected_tenant_id=UUID("d8526eee-5633-5644-b721-9d5a678684dc"),
    )
    assert wrong_tenant.ok is False
    assert (
        wrong_tenant.outcome == IncidentPublicationValidationOutcome.SEMANTIC_MISMATCH
    )
    assert wrong_tenant.error_code == "INCIDENT_PUBLICATION_VALIDATION_ERROR"


# ============================================================================
# C1-03: STABLE IDENTITY ON REPLAY AND STRICT REVISION SEPARATION
# ============================================================================


def test_stable_identity_on_replay_and_revision_separation(
    fix01_bundle: dict[str, Any],
) -> None:
    rev1_a = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    rev1_b = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    assert rev1_a == rev1_b
    assert rev1_a.event_id == rev1_b.event_id
    assert rev1_a.publication_fingerprint == rev1_b.publication_fingerprint
    assert rev1_a.kafka_value == rev1_b.kafka_value
    assert rev1_a.kafka_key == rev1_b.kafka_key
    assert rev1_a.kafka_headers == rev1_b.kafka_headers

    rev1_no_pv = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
        producer_version=None,
    )
    # Optional producer_version keeps event_id stable while separating wire fingerprint.
    assert rev1_no_pv.event_id == rev1_a.event_id
    assert rev1_no_pv.publication_fingerprint != rev1_a.publication_fingerprint
    assert len(rev1_no_pv.kafka_headers) == 5
    assert len(rev1_a.kafka_headers) == 6
    assert rev1_no_pv.kafka_key == rev1_a.kafka_key

    rev2 = build_incident_publication(
        incident=fix01_bundle["rev2"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    # Same incident & routing key, strictly distinct event_id & publication_fingerprint.
    assert rev2.incident.incident_id == rev1_a.incident.incident_id
    assert rev2.incident.incident_number == rev1_a.incident.incident_number
    assert rev2.kafka_key == rev1_a.kafka_key
    assert rev2.payload.revision == 2
    assert rev1_a.payload.revision == 1
    assert rev2.event_id != rev1_a.event_id
    assert rev2.publication_fingerprint != rev1_a.publication_fingerprint
    assert rev2.handoff.handoff_id != rev1_a.handoff.handoff_id
    assert rev2.handoff.handoff_fingerprint != rev1_a.handoff.handoff_fingerprint
    assert rev2.kafka_value != rev1_a.kafka_value

    # Event ID derivation separates every identity component.
    base_id = derive_incident_publication_event_id(
        tenant_id=rev1_a.incident.tenant_id,
        incident_id=rev1_a.incident.incident_id,
        revision=1,
        revision_fingerprint=rev1_a.incident.revision_fingerprint,
    )
    assert base_id == rev1_a.event_id
    assert (
        derive_incident_publication_event_id(
            tenant_id=UUID("d8526eee-5633-5644-b721-9d5a678684dc"),
            incident_id=rev1_a.incident.incident_id,
            revision=1,
            revision_fingerprint=rev1_a.incident.revision_fingerprint,
        )
        != base_id
    )
    assert (
        derive_incident_publication_event_id(
            tenant_id=rev1_a.incident.tenant_id,
            incident_id=UUID("00000000-0000-5000-8000-000000000001"),
            revision=1,
            revision_fingerprint=rev1_a.incident.revision_fingerprint,
        )
        != base_id
    )
    assert (
        derive_incident_publication_event_id(
            tenant_id=rev1_a.incident.tenant_id,
            incident_id=rev1_a.incident.incident_id,
            revision=2,
            revision_fingerprint=rev1_a.incident.revision_fingerprint,
        )
        != base_id
    )
    assert (
        derive_incident_publication_event_id(
            tenant_id=rev1_a.incident.tenant_id,
            incident_id=rev1_a.incident.incident_id,
            revision=1,
            revision_fingerprint="f" * 64,
        )
        != base_id
    )


# ============================================================================
# C1-04: MALFORMED, UNSUPPORTED VERSION, OVERSIZED, NON-FINITE & MISMATCHES
# ============================================================================


@pytest.mark.parametrize(
    "raw_value",
    [
        b"",
        b"\xff\xfe",
        b"{not-json",
        b"[]",
        b'"scalar"',
        b"123",
        b'{"a":1,"a":2}',
        b'{"schema_version":"1.0","payload":{"x":NaN}}',
        b'{"schema_version":"1.0","payload":{"x":Infinity}}',
        b'{"schema_version":"1.0","payload":{"x":-Infinity}}',
        b"{" + b'"a":{' * 70 + b"1" + b"}" * 70 + b"}",
    ],
)
def test_reject_malformed_duplicate_nonfinite_and_deep_json(
    fix01_bundle: dict[str, Any], raw_value: bytes
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=raw_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )


def test_reject_duplicate_nested_json_key_in_valid_envelope(
    fix01_bundle: dict[str, Any],
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    text = record.kafka_value.decode("utf-8")
    # Inject a duplicate key inside payload without breaking JSON syntax.
    duplicated = text.replace(
        '"revision":1',
        '"revision":1,"revision":1',
        1,
    ).encode("utf-8")
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=duplicated,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )


def test_reject_nonfinite_inputs_across_python_and_json_boundaries(
    fix01_bundle: dict[str, Any],
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    # Non-finite float inside raw JSON anomaly_score
    text = record.kafka_value.decode("utf-8")
    nan_raw = text.replace('"anomaly_score":0.88', '"anomaly_score":NaN').encode(
        "utf-8"
    )
    inf_raw = text.replace('"anomaly_score":0.88', '"anomaly_score":Infinity').encode(
        "utf-8"
    )
    for bad in (nan_raw, inf_raw):
        with pytest.raises(IncidentPublicationContractError):
            decode_incident_publication_structural(
                raw_value=bad,
                raw_key=record.kafka_key,
                headers=record.kafka_headers,
            )

    # Non-finite float inside fingerprint projection
    proj = build_publication_fingerprint_projection(
        event_id=record.event_id,
        handoff=record.handoff,
        correlation_policy_version=record.payload.correlation_policy_version,
        correlation_config_fingerprint=record.payload.correlation_config_fingerprint,
        execution_lineage=record.payload.execution_lineage,
        producer_version=record.producer_version,
        intent_time=record.payload.intent_time,
    )
    proj["trace_id"] = float("nan")
    with pytest.raises(IncidentPublicationContractError):
        compute_incident_publication_fingerprint(proj)

    # Non-finite / boolean / negative latency_ms in transport metadata
    for bad_latency in (float("nan"), float("inf"), float("-inf"), -0.1, True):
        with pytest.raises(ValidationError):
            PublicationTransportMetadata(latency_ms=bad_latency)  # type: ignore[arg-type]

    # Unsafe model_copy injecting NaN anomaly_score into IncidentAggregate
    bad_ref = record.incident.signal_references[0].model_copy(
        update={"anomaly_score": float("nan")}
    )
    bad_incident = record.incident.model_copy(update={"signal_references": (bad_ref,)})
    with pytest.raises(IncidentPublicationContractError):
        build_incident_publication(
            incident=bad_incident,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_envelope_version",
        "bad_envelope_version",
        "missing_publication_version",
        "bad_publication_version",
        "bad_handoff_version",
        "bad_incident_version",
        "bad_context_version",
        "bad_policy_version",
        "bad_provenance_producer_version",
    ],
)
def test_reject_unsupported_versions_at_all_four_layers(
    fix01_bundle: dict[str, Any], mutation: str
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    obj = json.loads(record.kafka_value.decode("utf-8"))
    if mutation == "missing_envelope_version":
        del obj["schema_version"]
    elif mutation == "bad_envelope_version":
        obj["schema_version"] = "2.0"
    elif mutation == "missing_publication_version":
        del obj["payload"]["publication_contract_version"]
    elif mutation == "bad_publication_version":
        obj["payload"]["publication_contract_version"] = "incident-publication/2.0"
    elif mutation == "bad_handoff_version":
        obj["payload"]["handoff"]["schema_version"] = "phase5-investigation-handoff/2.0"
    elif mutation == "bad_incident_version":
        obj["payload"]["handoff"]["incident"]["schema_version"] = "1.1"
    elif mutation == "bad_context_version":
        obj["payload"]["handoff"]["topology_context"]["schema_version"] = (
            "blast-radius-context/2.0"
        )
    elif mutation == "bad_policy_version":
        obj["payload"]["handoff"]["topology_context"]["policy"]["policy_version"] = (
            "blast-radius-policy/2.0"
        )
    elif mutation == "bad_provenance_producer_version":
        obj["payload"]["handoff"]["provenance"]["producer_version"] = (
            "phase4-blast-handoff/2.0"
        )

    with pytest.raises(IncidentPublicationVersionError):
        decode_incident_publication_structural(
            raw_value=json.dumps(obj).encode("utf-8"),
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )


def test_reject_unsupported_versions_on_build_and_derivation(
    fix01_bundle: dict[str, Any],
) -> None:
    incident: IncidentAggregate = fix01_bundle["rev1"]

    with pytest.raises(IncidentPublicationVersionError):
        derive_incident_publication_event_id(
            tenant_id=incident.tenant_id,
            incident_id=incident.incident_id,
            revision=incident.revision,
            revision_fingerprint=incident.revision_fingerprint,
            publication_contract_version="incident-publication/2.0",
        )

    bad_intent = PersistedPublicationIntent(
        tenant_id=incident.tenant_id,
        incident_id=incident.incident_id,
        revision=incident.revision,
        schema_version="1.1",
        revision_fingerprint=incident.revision_fingerprint,
        intent_time=incident.updated_at,
        aggregate=incident,
    )
    with pytest.raises(IncidentPublicationVersionError):
        build_incident_publication_from_intent(
            intent=bad_intent,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )

    bad_incident = incident.model_copy(update={"schema_version": "2.0"})
    with pytest.raises(IncidentPublicationVersionError):
        build_incident_publication(
            incident=bad_incident,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )


def test_reject_oversized_key_value_headers_and_estimated_record(
    fix01_bundle: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )

    # 1. Raw value exceeding 983,040 bytes rejected before parsing
    oversized_value = b"{" + (b" " * MAX_KAFKA_VALUE_BYTES) + b"}"
    assert len(oversized_value) > MAX_KAFKA_VALUE_BYTES
    with pytest.raises(IncidentPublicationSizeLimitError):
        decode_incident_publication_structural(
            raw_value=oversized_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )

    # 2. Oversized Kafka key (> 512 bytes) at decode and build boundaries
    with pytest.raises(IncidentPublicationSizeLimitError):
        decode_incident_publication_structural(
            raw_value=record.kafka_value,
            raw_key=b"k" * (MAX_KAFKA_KEY_BYTES + 1),
            headers=record.kafka_headers,
        )
    huge_key_event = record.event.model_copy(
        update={"service": "s" * MAX_KAFKA_KEY_BYTES}
    )
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_publication_kafka_key(huge_key_event)

    # 3. Oversized single header (> 256 bytes) & per-field header byte limits
    ctx = fix01_bundle["context"]
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_publication_kafka_headers(
            TelemetryExecutionContext(
                run_id=ctx.run_id,
                scenario_id="a" * 129,
                scenario_version=ctx.scenario_version,
                reproducibility_key=ctx.reproducibility_key,
                seed=ctx.seed,
            )
        )
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_publication_kafka_headers(
            TelemetryExecutionContext(
                run_id=ctx.run_id,
                scenario_id=ctx.scenario_id,
                scenario_version="v" * 65,
                reproducibility_key=ctx.reproducibility_key,
                seed=ctx.seed,
            )
        )
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_publication_kafka_headers(
            TelemetryExecutionContext(
                run_id=ctx.run_id,
                scenario_id=ctx.scenario_id,
                scenario_version=ctx.scenario_version,
                reproducibility_key="r" * (MAX_KAFKA_HEADER_VALUE_BYTES + 1),
                seed=ctx.seed,
            )
        )
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_publication_kafka_headers(ctx, producer_version="p" * 129)
    # Multi-byte UTF-8: 43 x 3-byte chars = 129 UTF-8 bytes (> 128 byte cap)
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_publication_kafka_headers(ctx, producer_version="€" * 43)

    # 4. Oversized total headers (> 1024 bytes)
    bloated_headers = [
        ("h0", b"x" * 250),
        ("h1", b"x" * 250),
        ("h2", b"x" * 250),
        ("h3", b"x" * 250),
        ("h4", b"x" * 50),
    ]
    assert (
        sum(len(k.encode("utf-8")) + len(v) for k, v in bloated_headers)
        > MAX_KAFKA_HEADERS_TOTAL_BYTES
    )
    with pytest.raises(IncidentPublicationSizeLimitError):
        parse_and_validate_publication_headers(bloated_headers)

    # 5. Oversized incident revision at build time (exceeds 983,040 value bytes)
    t_now = _utc("2026-11-27T00:01:05Z")
    many_refs = tuple(
        IncidentSignalReference(
            signal_id=uuid5(NAMESPACE_DNS, f"oversized-sig-{i}"),
            semantic_fingerprint=f"{i:064x}",
            tenant_id=UUID("1dfc0469-7644-5efe-8b07-d1c7c31254f5"),
            environment="production",
            service="order-service",
            event_time=t_now,
            severity=EventSeverity.ERROR,
            anomaly_score=0.9,
        )
        for i in range(60)
    )
    from app.correlation.models import CorrelationEvidence

    many_evidence = tuple(
        CorrelationEvidence(
            left_signal_id=many_refs[i].signal_id,
            right_signal_id=many_refs[j].signal_id,
            temporal_score=0.9,
            service_score=1.0,
            topology_score=1.0,
            symptom_score=0.9,
            context_score=0.9,
            weighted_score=0.94,
            accepted=True,
            policy_version="v1.0",
            reason="Oversized Kafka wire-limit verification " + ("x" * 440),
        )
        for i in range(60)
        for j in range(i + 1, 60)
    )
    assert len(many_evidence) == 1770
    oversized_incident = IncidentAggregate.create(
        incident_id=UUID("1b914870-81d6-50eb-beae-9f332d3aae69"),
        incident_number="INC-OVERSIZED",
        tenant_id=UUID("1dfc0469-7644-5efe-8b07-d1c7c31254f5"),
        environment="production",
        status=IncidentStatus.OPEN,
        correlation_state=CorrelationWindowState.COLLECTING,
        severity=EventSeverity.ERROR,
        severity_policy_version="max-member-severity/v1",
        started_at=t_now,
        last_seen_at=t_now,
        created_at=t_now,
        updated_at=t_now,
        revision=1,
        primary_service="order-service",
        affected_services=("order-service",),
        signal_references=many_refs,
        correlation_evidence=many_evidence,
    )
    assert len(serialize_incident_aggregate(oversized_incident)) > MAX_KAFKA_VALUE_BYTES
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_incident_publication(
            incident=oversized_incident,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )
    res_oversized = try_build_incident_publication(
        incident=oversized_incident,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    assert res_oversized.ok is False
    assert (
        res_oversized.outcome
        == IncidentPublicationValidationOutcome.SIZE_LIMIT_EXCEEDED
    )
    assert res_oversized.error_code == "INCIDENT_PUBLICATION_SIZE_LIMIT_ERROR"

    # 5b. Incident under cap where envelope/handoff overhead crosses MAX_KAFKA_VALUE_BYTES
    with monkeypatch.context() as mp:
        mp.setattr(
            publication_mod,
            "MAX_KAFKA_VALUE_BYTES",
            len(record.kafka_value) - 1,
        )
        with pytest.raises(IncidentPublicationSizeLimitError):
            build_incident_publication(
                incident=fix01_bundle["rev1"],
                snapshot=fix01_bundle["snapshot"],
                expected_topology=fix01_bundle["topology_ref"],
                blast_radius_policy=fix01_bundle["policy"],
                correlation_config=fix01_bundle["config"],
                execution_context=fix01_bundle["context"],
            )

    # 6. Estimated Kafka record cap (> 1,048,576) enforced at build and decode
    monkeypatch.setattr(
        publication_mod,
        "_estimate_kafka_record_bytes",
        lambda k, v, h: MAX_KAFKA_RECORD_ESTIMATED_BYTES + 1,
    )
    with pytest.raises(IncidentPublicationSizeLimitError):
        build_incident_publication(
            incident=fix01_bundle["rev1"],
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )
    with pytest.raises(IncidentPublicationSizeLimitError):
        decode_incident_publication_structural(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )


def test_bounded_header_validation_edge_cases(fix01_bundle: dict[str, Any]) -> None:
    ctx: TelemetryExecutionContext = fix01_bundle["context"]
    valid_headers = list(build_publication_kafka_headers(ctx))
    assert len(valid_headers) == 6

    # Valid boundary seeds: 0 and 2^64 - 1
    for boundary_seed in (0, MAX_UINT64):
        b_ctx = TelemetryExecutionContext(
            run_id=ctx.run_id,
            scenario_id=ctx.scenario_id,
            scenario_version=ctx.scenario_version,
            reproducibility_key=ctx.reproducibility_key,
            seed=boundary_seed,
        )
        hdrs = build_publication_kafka_headers(b_ctx, producer_version=None)
        parsed_ctx, parsed_pv, _, _ = parse_and_validate_publication_headers(hdrs)
        assert parsed_ctx == b_ctx
        assert parsed_pv is None

    # Exact rejection cases
    bad_header_cases: list[Any] = [
        None,
        "not-a-sequence",
        valid_headers[:-2],  # missing seed
        valid_headers[1:],  # missing run_id
        list(reversed(valid_headers)),  # wrong order
        valid_headers + [("run_id", valid_headers[0][1])],  # duplicate required
        valid_headers + [("producer_version", b"v1")],  # duplicate optional
        valid_headers + [("extra_header", b"value")],  # unknown extra header
        valid_headers[:-1] + [("producer_version", b"")],  # empty producer_version
        [("run_id", valid_headers[0][1].upper()), *valid_headers[1:]],  # uppercase UUID
        [
            ("run_id", b"71331f12256e52cf950ea27a59451741"),
            *valid_headers[1:],
        ],  # non-hyphenated
        [*valid_headers[:1], ("scenario_id", b" padded "), *valid_headers[2:]],
        [*valid_headers[:1], ("scenario_id", b"bad\nline"), *valid_headers[2:]],
        [*valid_headers[:1], ("scenario_id", b"\xff\xfe"), *valid_headers[2:]],
        [*valid_headers[:4], ("seed", b"+42"), valid_headers[5]],
        [*valid_headers[:4], ("seed", b"042"), valid_headers[5]],
        [*valid_headers[:4], ("seed", b"-1"), valid_headers[5]],
        [*valid_headers[:4], ("seed", str(MAX_UINT64 + 1).encode()), valid_headers[5]],
        [*valid_headers[:4], ("seed", "٤٢".encode("utf-8")), valid_headers[5]],
        [*valid_headers[:5], ("producer_version", b"v1\x7f")],
        [("run_id", None)],  # type: ignore[list-item]
    ]
    for bad in bad_header_cases:
        with pytest.raises(IncidentPublicationHeaderError):
            parse_and_validate_publication_headers(bad)

    # Unsafe TelemetryExecutionContext with bool seed rejected
    bool_seed_ctx = ctx.model_copy(update={"seed": True})
    with pytest.raises(IncidentPublicationHeaderError):
        build_publication_kafka_headers(bool_seed_ctx)


def test_reject_envelope_payload_key_and_topic_mismatches(
    fix01_bundle: dict[str, Any],
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )

    # Wrong topic
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            topic=KafkaTopic.ANOMALIES,
        )

    # Wrong routing key
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=record.kafka_value,
            raw_key=b"1dfc0469-7644-5efe-8b07-d1c7c31254f5:production:payment-service",
            headers=record.kafka_headers,
        )

    # Mutate envelope / payload fields
    base_obj = json.loads(record.kafka_value.decode("utf-8"))
    mutations: list[tuple[str, Any]] = [
        ("event_type", "anomaly"),
        ("trace_id", "trace-123"),
        ("service", "payment-service"),
        ("environment", "staging"),
        ("severity", "critical"),
        ("event_id", "00000000-0000-5000-8000-000000000099"),
        ("extra_envelope_key", "nope"),
    ]
    for key, val in mutations:
        mutated = json.loads(json.dumps(base_obj))
        mutated[key] = val
        with pytest.raises(IncidentPublicationContractError):
            decode_incident_publication_structural(
                raw_value=json.dumps(mutated).encode("utf-8"),
                raw_key=record.kafka_key,
                headers=record.kafka_headers,
            )

    # Uppercase UUID in JSON envelope rejected
    upper_uuid = json.loads(json.dumps(base_obj))
    upper_uuid["event_id"] = upper_uuid["event_id"].upper()
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=json.dumps(upper_uuid).encode("utf-8"),
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )

    # Tampered publication_fingerprint rejected
    tampered_fp = json.loads(json.dumps(base_obj))
    tampered_fp["payload"]["publication_fingerprint"] = "0" * 64
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=json.dumps(tampered_fp).encode("utf-8"),
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )

    # Extra top-level payload key rejected
    extra_payload = json.loads(json.dumps(base_obj))
    extra_payload["payload"]["unexpected"] = 1
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_structural(
            raw_value=json.dumps(extra_payload).encode("utf-8"),
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )


def test_reject_topology_policy_config_and_lineage_mismatches(
    fix01_bundle: dict[str, Any],
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )

    # 1. Topology snapshot pin mismatch at build & semantic validation (FIX-07)
    wrong_topo_ref = fix01_bundle["topology_ref"].model_copy(
        update={"snapshot_fingerprint": "f" * 64}
    )
    with pytest.raises(IncidentPublicationValidationError):
        build_incident_publication(
            incident=fix01_bundle["rev1"],
            snapshot=fix01_bundle["snapshot"],
            expected_topology=wrong_topo_ref,
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )
    with pytest.raises(IncidentPublicationValidationError):
        validate_incident_publication(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=wrong_topo_ref,
            expected_blast_radius_policy=fix01_bundle["policy"],
        )

    # 2. Stale snapshot age policy mismatch
    strict_age_policy = BlastRadiusPolicy(max_snapshot_age_seconds=10)
    with pytest.raises(IncidentPublicationValidationError):
        build_incident_publication(
            incident=fix01_bundle["rev1"],
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=strict_age_policy,
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )
    with pytest.raises(IncidentPublicationValidationError):
        validate_incident_publication(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            expected_blast_radius_policy=strict_age_policy,
        )

    # 3. Semantically forged blast-radius graph (passes structural decode, fails semantic validation!)
    forged_obj = json.loads(record.kafka_value.decode("utf-8"))
    # Shrink policy max_hops to 0 and keep only root order-service so handoff is
    # 100% structurally self-consistent, yet differs from expected_policy (max_hops=2)!
    topo_ctx = forged_obj["payload"]["handoff"]["topology_context"]
    root_node = [n for n in topo_ctx["reachable_services"] if n["minimum_depth"] == 0]
    root_path = [p for p in topo_ctx["paths"] if p["hop_count"] == 0]
    topo_ctx["policy"]["max_hops"] = 0
    topo_ctx["reachable_services"] = root_node
    topo_ctx["edges"] = []
    topo_ctx["paths"] = root_path
    topo_ctx["potentially_affected_service_ids"] = [root_node[0]["service_id"]]
    topo_ctx["truncated"] = False
    topo_ctx["truncation_reasons"] = []
    topo_ctx["omitted_within_hop_services"] = 0
    topo_ctx["omitted_eligible_edges"] = 0
    topo_ctx["examined_arcs"] = 0
    forged_value, forged_key, forged_headers = _resign_raw_envelope(forged_obj)

    # Structural decode passes (self-consistent wire artifact)...
    struct_decoded = decode_incident_publication_structural(
        raw_value=forged_value,
        raw_key=forged_key,
        headers=forged_headers,
    )
    assert struct_decoded.payload.handoff.topology_context.policy.max_hops == 0
    # ...but semantic validation against pinned BlastRadiusPolicy(max_hops=2) MUST reject!
    with pytest.raises(IncidentPublicationValidationError):
        validate_incident_publication(
            raw_value=forged_value,
            raw_key=forged_key,
            headers=forged_headers,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            expected_blast_radius_policy=fix01_bundle["policy"],
        )

    # 4. Header-to-payload execution lineage mismatch (each field)
    ctx: TelemetryExecutionContext = fix01_bundle["context"]
    mismatched_contexts = [
        TelemetryExecutionContext(
            run_id=UUID("00000000-0000-5000-8000-000000000009"),
            scenario_id=ctx.scenario_id,
            scenario_version=ctx.scenario_version,
            reproducibility_key=ctx.reproducibility_key,
            seed=ctx.seed,
        ),
        TelemetryExecutionContext(
            run_id=ctx.run_id,
            scenario_id="other-scenario",
            scenario_version=ctx.scenario_version,
            reproducibility_key=ctx.reproducibility_key,
            seed=ctx.seed,
        ),
        TelemetryExecutionContext(
            run_id=ctx.run_id,
            scenario_id=ctx.scenario_id,
            scenario_version="2.0.0",
            reproducibility_key=ctx.reproducibility_key,
            seed=ctx.seed,
        ),
        TelemetryExecutionContext(
            run_id=ctx.run_id,
            scenario_id=ctx.scenario_id,
            scenario_version=ctx.scenario_version,
            reproducibility_key="other-repro-key",
            seed=ctx.seed,
        ),
        TelemetryExecutionContext(
            run_id=ctx.run_id,
            scenario_id=ctx.scenario_id,
            scenario_version=ctx.scenario_version,
            reproducibility_key=ctx.reproducibility_key,
            seed=99,
        ),
    ]
    for bad_ctx in mismatched_contexts:
        with pytest.raises(IncidentPublicationValidationError):
            decode_incident_publication_structural(
                raw_value=record.kafka_value,
                raw_key=record.kafka_key,
                headers=build_publication_kafka_headers(bad_ctx),
            )
        with pytest.raises(IncidentPublicationValidationError):
            validate_incident_publication(
                raw_value=record.kafka_value,
                raw_key=record.kafka_key,
                headers=record.kafka_headers,
                snapshot=fix01_bundle["snapshot"],
                expected_topology=fix01_bundle["topology_ref"],
                expected_blast_radius_policy=fix01_bundle["policy"],
                expected_execution_context=bad_ctx,
            )

    # 5. Header-to-payload producer_version mismatch & expected_producer_version mismatch
    with pytest.raises(IncidentPublicationValidationError):
        decode_incident_publication_structural(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=build_publication_kafka_headers(ctx, producer_version=None),
        )
    with pytest.raises(IncidentPublicationValidationError):
        decode_incident_publication_structural(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=build_publication_kafka_headers(
                ctx, producer_version="phase4-incident-publisher/9.9"
            ),
        )
    with pytest.raises(IncidentPublicationValidationError):
        validate_incident_publication(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            expected_blast_radius_policy=fix01_bundle["policy"],
            expected_producer_version=None,
        )

    # 6. CorrelationConfig pin mismatch
    other_cfg = CorrelationConfig.model_validate_json(
        json.dumps({**PINNED_CORRELATION_CONFIG, "window_seconds": 600})
    )
    with pytest.raises(IncidentPublicationValidationError):
        validate_incident_publication(
            raw_value=record.kafka_value,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            expected_blast_radius_policy=fix01_bundle["policy"],
            expected_correlation_config=other_cfg,
        )

    # 7. intent_time mismatch & PersistedPublicationIntent mirror mismatch
    with pytest.raises(IncidentPublicationValidationError):
        build_incident_publication(
            incident=fix01_bundle["rev1"],
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
            intent_time=_utc("2026-11-27T00:01:06Z"),
        )
    mismatched_intent = PersistedPublicationIntent(
        tenant_id=fix01_bundle["rev1"].tenant_id,
        incident_id=fix01_bundle["rev1"].incident_id,
        revision=99,
        schema_version="1.2",
        revision_fingerprint=fix01_bundle["rev1"].revision_fingerprint,
        intent_time=fix01_bundle["rev1"].updated_at,
        aggregate=fix01_bundle["rev1"],
    )
    with pytest.raises(IncidentPublicationValidationError):
        build_incident_publication_from_intent(
            intent=mismatched_intent,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )


def test_input_and_output_immutability(fix01_bundle: dict[str, Any]) -> None:
    incident: IncidentAggregate = fix01_bundle["rev1"]
    snapshot = fix01_bundle["snapshot"]
    topo_ref = fix01_bundle["topology_ref"]
    policy = fix01_bundle["policy"]
    config = fix01_bundle["config"]
    context = fix01_bundle["context"]

    incident_before = serialize_incident_aggregate(incident)
    config_before = serialize_correlation_config(config)
    context_before = context.model_dump_json()
    policy_before = policy.model_dump_json()
    topo_before = topo_ref.model_dump_json()

    record = build_incident_publication(
        incident=incident,
        snapshot=snapshot,
        expected_topology=topo_ref,
        blast_radius_policy=policy,
        correlation_config=config,
        execution_context=context,
    )

    # None of the input objects were mutated.
    assert serialize_incident_aggregate(incident) == incident_before
    assert serialize_correlation_config(config) == config_before
    assert context.model_dump_json() == context_before
    assert policy.model_dump_json() == policy_before
    assert topo_ref.model_dump_json() == topo_before

    # Mutating caller-owned bytearrays and header lists after decode cannot affect record.
    mutable_value = bytearray(record.kafka_value)
    mutable_key = bytearray(record.kafka_key)
    mutable_headers = [(k, bytearray(v)) for k, v in record.kafka_headers]

    decoded = decode_incident_publication_structural(
        raw_value=mutable_value,
        raw_key=mutable_key,
        headers=mutable_headers,  # type: ignore[arg-type]
    )
    mutable_value[:] = b"X" * len(mutable_value)
    mutable_key[:] = b"Y" * len(mutable_key)
    for _, v in mutable_headers:
        v[:] = b"Z" * len(v)
    mutable_headers.clear()

    assert decoded == record
    assert decoded.kafka_value == record.kafka_value
    assert decoded.kafka_key == record.kafka_key
    assert decoded.kafka_headers == record.kafka_headers

    # Mutating returned TelemetryEvent cannot affect record or subsequent event copies.
    ev = record.event
    ev.service = "mutated-service"
    ev.payload["tampered"] = True
    ev.payload["handoff"]["tampered"] = True

    fresh_ev = record.to_telemetry_event()
    assert fresh_ev.service == "order-service"
    assert "tampered" not in fresh_ev.payload
    assert "tampered" not in fresh_ev.payload["handoff"]
    assert serialize_event(fresh_ev) == record.kafka_value

    # Output record and nested contracts are frozen.
    with pytest.raises(FrozenInstanceError):
        record.kafka_value = b"tampered"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        record.payload.revision = 99  # type: ignore[misc]
    with pytest.raises(ValidationError):
        record.payload.execution_lineage.seed = 999  # type: ignore[misc]


# ============================================================================
# C1-05: TRANSPORT METADATA EXPLICITLY EXCLUDED FROM SEMANTIC EQUALITY
# ============================================================================


def test_transport_metadata_excluded_from_semantic_equality(
    fix01_bundle: dict[str, Any],
) -> None:
    base = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )

    meta_a = PublicationTransportMetadata(
        partition=0,
        offset=101,
        broker_timestamp_ms=1764201665100,
        kafka_timestamp_ms=1764201665100,
        latency_ms=4.25,
        consumed_at=_utc("2026-11-27T00:01:06Z"),
        attempt_count=1,
        claim_token=UUID("11111111-1111-5111-8111-111111111111"),
        claimed_by="worker-a",
        claimed_at=_utc("2026-11-27T00:01:05.100000Z"),
        acknowledged_at=_utc("2026-11-27T00:01:05.200000Z"),
    )
    meta_b = PublicationTransportMetadata(
        partition=2,
        offset=999,
        broker_timestamp_ms=1764201700555,
        kafka_timestamp_ms=1764201700555,
        latency_ms=48.75,
        consumed_at=_utc("2026-11-27T00:02:10Z"),
        attempt_count=2,
        claim_token=UUID("22222222-2222-5222-8222-222222222222"),
        claimed_by="worker-b",
        claimed_at=_utc("2026-11-27T00:02:05.100000Z"),
        acknowledged_at=_utc("2026-11-27T00:02:05.900000Z"),
    )

    rec_a = base.with_transport_metadata(meta_a)
    rec_b = decode_incident_publication_structural(
        raw_value=base.kafka_value,
        raw_key=base.kafka_key,
        headers=base.kafka_headers,
        transport_metadata=meta_b,
    )

    assert rec_a.transport_metadata == meta_a
    assert rec_b.transport_metadata == meta_b
    assert rec_a.transport_metadata != rec_b.transport_metadata
    assert base.transport_metadata is None

    # Semantic equality, __eq__, and __hash__ ignore all 11 transport fields.
    assert rec_a == base
    assert rec_b == base
    assert rec_a == rec_b
    assert hash(rec_a) == hash(rec_b) == hash(base)
    assert rec_a.semantic_equals(rec_b) is True
    assert publications_semantically_equal(rec_a, rec_b) is True
    assert publications_semantically_equal(rec_a, base) is True

    # Envelope decoding preserves transport metadata while preserving semantic equality.
    env_1 = TelemetryEnvelope(
        event=base.event,
        context=base.execution_context,
        kafka_timestamp_ms=1764201665001,
        topic=KafkaTopic.INCIDENTS.value,
        partition=0,
        offset=10,
        key=base.kafka_key,
        consumed_at=_utc("2026-11-27T00:01:06Z"),
        producer_version=base.producer_version,
    )
    env_2 = TelemetryEnvelope(
        event=base.event,
        context=base.execution_context,
        kafka_timestamp_ms=1764201700999,
        topic=KafkaTopic.INCIDENTS.value,
        partition=1,
        offset=77,
        key=base.kafka_key,
        consumed_at=_utc("2026-11-27T00:03:00Z"),
        producer_version=base.producer_version,
    )
    dec_env_1 = decode_incident_publication_from_envelope(
        env_1,
        raw_value=base.kafka_value,
        raw_headers=base.kafka_headers,
        require_canonical_bytes=True,
    )
    dec_env_2 = validate_incident_publication_from_envelope(
        env_2,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
        raw_value=base.kafka_value,
        raw_headers=base.kafka_headers,
        expected_correlation_config=fix01_bundle["config"],
        expected_execution_context=fix01_bundle["context"],
        expected_producer_version=base.producer_version,
        expected_tenant_id=fix01_bundle["rev1"].tenant_id,
        require_canonical_bytes=True,
    )
    assert dec_env_1 == dec_env_2 == base
    assert dec_env_1.transport_metadata is not None
    assert dec_env_2.transport_metadata is not None
    assert dec_env_1.transport_metadata.offset == 10
    assert dec_env_2.transport_metadata.offset == 77
    assert dec_env_1.transport_metadata.partition == 0
    assert dec_env_2.transport_metadata.partition == 1
    assert dec_env_1.transport_metadata != dec_env_2.transport_metadata
    assert publications_semantically_equal(dec_env_1, dec_env_2) is True


def test_publication_payload_rejects_non_utc_and_bool_revision(
    fix01_bundle: dict[str, Any],
) -> None:
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    non_utc = datetime(
        2026, 11, 27, 5, 31, 5, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    with pytest.raises(IncidentPublicationContractError):
        build_incident_publication(
            incident=fix01_bundle["rev1"],
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
            intent_time=non_utc,
        )

    raw_payload = record.payload.model_dump()
    raw_payload["revision"] = True
    with pytest.raises(ValidationError):
        IncidentPublicationPayload.model_validate(raw_payload)

    with pytest.raises(ValidationError):
        PublicationExecutionLineage(
            run_id=fix01_bundle["context"].run_id,
            scenario_id="s",
            scenario_version="1",
            reproducibility_key="k",
            seed=True,  # type: ignore[arg-type]
        )

    # Boolean revision on PersistedPublicationIntent rejected even when aggregate.revision == 1
    bool_intent = PersistedPublicationIntent(
        tenant_id=fix01_bundle["rev1"].tenant_id,
        incident_id=fix01_bundle["rev1"].incident_id,
        revision=True,  # type: ignore[arg-type]
        schema_version="1.2",
        revision_fingerprint=fix01_bundle["rev1"].revision_fingerprint,
        intent_time=fix01_bundle["rev1"].updated_at,
        aggregate=fix01_bundle["rev1"],
    )
    with pytest.raises(IncidentPublicationContractError):
        build_incident_publication_from_intent(
            intent=bool_intent,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            blast_radius_policy=fix01_bundle["policy"],
            correlation_config=fix01_bundle["config"],
            execution_context=fix01_bundle["context"],
        )


def test_microsecond_timestamp_canonical_roundtrip(
    fix01_bundle: dict[str, Any],
) -> None:
    base_inc: IncidentAggregate = fix01_bundle["rev1"]
    micro_dt = datetime(2026, 11, 27, 0, 1, 5, 123456, tzinfo=timezone.utc)
    micro_inc = IncidentAggregate.create(
        incident_id=base_inc.incident_id,
        incident_number=base_inc.incident_number,
        tenant_id=base_inc.tenant_id,
        environment=base_inc.environment,
        status=base_inc.status,
        correlation_state=base_inc.correlation_state,
        severity=base_inc.severity,
        severity_policy_version=base_inc.severity_policy_version,
        started_at=base_inc.started_at,
        last_seen_at=base_inc.last_seen_at,
        created_at=micro_dt,
        updated_at=micro_dt,
        revision=1,
        primary_service=base_inc.primary_service,
        affected_services=base_inc.affected_services,
        signal_references=base_inc.signal_references,
        correlation_evidence=base_inc.correlation_evidence,
    )
    record = build_incident_publication(
        incident=micro_inc,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    decoded = validate_incident_publication(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=record.kafka_headers,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
        expected_correlation_config=fix01_bundle["config"],
        expected_execution_context=fix01_bundle["context"],
        require_canonical_bytes=True,
    )
    assert decoded == record
    assert decoded.payload.intent_time == micro_dt


# ============================================================================
# TASK 4.12-C1-R1 RETAINED REPRODUCTION REGRESSION TESTS (GAPS 1, 2, 3)
# ============================================================================


def test_c1_r1_gap1_envelope_adapters_preserve_and_enforce_wire_evidence(
    fix01_bundle: dict[str, Any],
) -> None:
    """Gap 1: envelope adapters must not launder noncanonical headers or pretty JSON."""
    base = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )

    canonical_env = TelemetryEnvelope(
        event=base.event,
        context=base.execution_context,
        kafka_timestamp_ms=1764201665001,
        topic=KafkaTopic.INCIDENTS.value,
        partition=0,
        offset=10,
        key=base.kafka_key,
        consumed_at=_utc("2026-11-27T00:01:06Z"),
        producer_version=base.producer_version,
    )

    # Omitting raw_value or raw_headers fails closed under both strict and non-strict modes.
    for strict_mode in (False, True):
        with pytest.raises(IncidentPublicationContractError):
            decode_incident_publication_from_envelope(
                canonical_env, require_canonical_bytes=strict_mode
            )
        with pytest.raises(IncidentPublicationContractError):
            decode_incident_publication_from_envelope(
                canonical_env,
                raw_value=base.kafka_value,
                require_canonical_bytes=strict_mode,
            )
        with pytest.raises(IncidentPublicationContractError):
            decode_incident_publication_from_envelope(
                canonical_env,
                raw_headers=base.kafka_headers,
                require_canonical_bytes=strict_mode,
            )
        with pytest.raises(IncidentPublicationContractError):
            validate_incident_publication_from_envelope(
                canonical_env,
                snapshot=fix01_bundle["snapshot"],
                expected_topology=fix01_bundle["topology_ref"],
                expected_blast_radius_policy=fix01_bundle["policy"],
                require_canonical_bytes=strict_mode,
            )

    # Noncanonical seed headers (+42, 042), reversed headers, and extra headers are
    # accepted by frozen parse_kafka_headers but MUST be rejected by both envelope adapters.
    base_hdrs = list(base.kafka_headers)
    noncanonical_header_variants: list[list[tuple[str, bytes]]] = [
        [*base_hdrs[:4], ("seed", b"+42"), base_hdrs[5]],
        [*base_hdrs[:4], ("seed", b"042"), base_hdrs[5]],
        list(reversed(base_hdrs)),
        [*base_hdrs, ("x-extra-header", b"extra")],
    ]
    for bad_hdrs in noncanonical_header_variants:
        parsed_ctx, parsed_pv = parse_kafka_headers(bad_hdrs)
        env_from_bad_hdrs = TelemetryEnvelope(
            event=deserialize_event(base.kafka_value),
            context=parsed_ctx,
            kafka_timestamp_ms=1764201665001,
            topic=KafkaTopic.INCIDENTS.value,
            partition=0,
            offset=10,
            key=base.kafka_key,
            consumed_at=_utc("2026-11-27T00:01:06Z"),
            producer_version=parsed_pv,
        )
        for strict_mode in (False, True):
            with pytest.raises(IncidentPublicationHeaderError):
                decode_incident_publication_from_envelope(
                    env_from_bad_hdrs,
                    raw_value=base.kafka_value,
                    raw_headers=bad_hdrs,
                    require_canonical_bytes=strict_mode,
                )
            with pytest.raises(IncidentPublicationHeaderError):
                validate_incident_publication_from_envelope(
                    env_from_bad_hdrs,
                    snapshot=fix01_bundle["snapshot"],
                    expected_topology=fix01_bundle["topology_ref"],
                    expected_blast_radius_policy=fix01_bundle["policy"],
                    raw_value=base.kafka_value,
                    raw_headers=bad_hdrs,
                    require_canonical_bytes=strict_mode,
                )

    # Pretty-printed JSON parsed into a TelemetryEnvelope MUST be rejected when
    # require_canonical_bytes=True, and accepted only when require_canonical_bytes=False.
    pretty_bytes = json.dumps(
        json.loads(base.kafka_value.decode("utf-8")), indent=2
    ).encode("utf-8")
    env_from_pretty = TelemetryEnvelope(
        event=deserialize_event(pretty_bytes),
        context=base.execution_context,
        kafka_timestamp_ms=1764201665001,
        topic=KafkaTopic.INCIDENTS.value,
        partition=0,
        offset=10,
        key=base.kafka_key,
        consumed_at=_utc("2026-11-27T00:01:06Z"),
        producer_version=base.producer_version,
    )
    with pytest.raises(IncidentPublicationContractError):
        decode_incident_publication_from_envelope(
            env_from_pretty,
            raw_value=pretty_bytes,
            raw_headers=base.kafka_headers,
            require_canonical_bytes=True,
        )
    with pytest.raises(IncidentPublicationContractError):
        validate_incident_publication_from_envelope(
            env_from_pretty,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            expected_blast_radius_policy=fix01_bundle["policy"],
            raw_value=pretty_bytes,
            raw_headers=base.kafka_headers,
            require_canonical_bytes=True,
        )

    decoded_non_strict = decode_incident_publication_from_envelope(
        env_from_pretty,
        raw_value=pretty_bytes,
        raw_headers=base.kafka_headers,
        require_canonical_bytes=False,
    )
    assert decoded_non_strict == base
    assert decoded_non_strict.kafka_value == base.kafka_value

    # Mismatched TelemetryEnvelope event/context vs raw wire evidence is rejected.
    mismatched_env = canonical_env.model_copy(
        update={
            "context": TelemetryExecutionContext(
                run_id=base.execution_context.run_id,
                scenario_id=base.execution_context.scenario_id,
                scenario_version=base.execution_context.scenario_version,
                reproducibility_key=base.execution_context.reproducibility_key,
                seed=999,
            )
        }
    )
    with pytest.raises(IncidentPublicationValidationError):
        decode_incident_publication_from_envelope(
            mismatched_env,
            raw_value=base.kafka_value,
            raw_headers=base.kafka_headers,
            require_canonical_bytes=True,
        )


def test_c1_r1_gap2_replaced_or_forged_record_cannot_convert_to_anomaly_event(
    fix01_bundle: dict[str, Any],
) -> None:
    """Gap 2: replaced/forged record declaring aegis.incidents cannot yield an anomaly event."""
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    anomaly_signal = AnomalySignal.model_validate_json(json.dumps(PINNED_SIGNALS["S1"]))
    anomaly_event = to_canonical_telemetry_event(anomaly_signal)
    assert anomaly_event.event_type == EventType.ANOMALY
    anomaly_bytes = serialize_event(anomaly_event)

    # 1. dataclasses.replace with anomaly bytes while keeping topic=aegis.incidents fails immediately.
    with pytest.raises(IncidentPublicationContractError):
        replace(record, kafka_value=anomaly_bytes)

    # 2. dataclasses.replace with wrong topic fails immediately.
    with pytest.raises(IncidentPublicationContractError):
        replace(record, topic=KafkaTopic.ANOMALIES)

    # 3. Even bypassing __post_init__ via object.__new__/object.__setattr__ cannot produce
    # an anomaly TelemetryEvent via to_telemetry_event(), .event, or incident_publication_to_telemetry_event().
    forged = object.__new__(IncidentPublicationRecord)
    for attr in (
        "topic",
        "event_id",
        "publication_fingerprint",
        "payload",
        "execution_context",
        "producer_version",
        "kafka_key",
        "kafka_headers",
        "kafka_headers_total_bytes",
        "estimated_record_bytes",
        "transport_metadata",
    ):
        object.__setattr__(forged, attr, getattr(record, attr))
    object.__setattr__(forged, "kafka_value", anomaly_bytes)
    assert forged.topic == KafkaTopic.INCIDENTS

    with pytest.raises(IncidentPublicationContractError):
        forged.to_telemetry_event()
    with pytest.raises(IncidentPublicationContractError):
        _ = forged.event
    with pytest.raises(IncidentPublicationContractError):
        incident_publication_to_telemetry_event(forged)

    # 4. Tampering key, headers, or fingerprint post-construction is also rejected on conversion.
    for attr, bad_val in (
        ("kafka_key", b"wrong:key:value"),
        ("publication_fingerprint", "0" * 64),
        ("estimated_record_bytes", 1),
    ):
        tampered = object.__new__(IncidentPublicationRecord)
        for field_name in (
            "topic",
            "event_id",
            "publication_fingerprint",
            "payload",
            "execution_context",
            "producer_version",
            "kafka_key",
            "kafka_value",
            "kafka_headers",
            "kafka_headers_total_bytes",
            "estimated_record_bytes",
            "transport_metadata",
        ):
            object.__setattr__(tampered, field_name, getattr(record, field_name))
        object.__setattr__(tampered, attr, bad_val)
        with pytest.raises(IncidentPublicationContractError):
            incident_publication_to_telemetry_event(tampered)


def test_c1_r1_gap3_malformed_unicode_never_escapes_typed_outcomes(
    fix01_bundle: dict[str, Any],
) -> None:
    """Gap 3: lone UTF-16 surrogates must return typed errors/outcomes, never UnicodeEncodeError."""
    record = build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=fix01_bundle["context"],
    )
    surrogate = "\ud800"
    ctx: TelemetryExecutionContext = fix01_bundle["context"]

    # Case 1: Malformed Unicode in producer_version & execution_context via non-raising try_build
    res_pv = try_build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=ctx,
        producer_version=surrogate,
    )
    assert res_pv.ok is False
    assert res_pv.outcome == IncidentPublicationValidationOutcome.INVALID_HEADERS
    assert res_pv.error_type == "IncidentPublicationHeaderError"

    bad_ctx = ctx.model_copy(update={"scenario_id": f"scen-{surrogate}"})
    res_ctx = try_build_incident_publication(
        incident=fix01_bundle["rev1"],
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        blast_radius_policy=fix01_bundle["policy"],
        correlation_config=fix01_bundle["config"],
        execution_context=bad_ctx,
    )
    assert res_ctx.ok is False
    assert res_ctx.outcome == IncidentPublicationValidationOutcome.INVALID_HEADERS
    assert res_ctx.error_type == "IncidentPublicationHeaderError"

    # Case 2: Malformed Unicode in header name via non-raising try_decode & try_validate
    bad_name_headers = [(surrogate, b"value")]
    res_hdr_decode = try_decode_incident_publication(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=bad_name_headers,
    )
    assert res_hdr_decode.ok is False
    assert (
        res_hdr_decode.outcome == IncidentPublicationValidationOutcome.INVALID_HEADERS
    )
    assert res_hdr_decode.error_type == "IncidentPublicationHeaderError"

    res_hdr_validate = try_validate_incident_publication(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=bad_name_headers,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
    )
    assert res_hdr_validate.ok is False
    assert (
        res_hdr_validate.outcome == IncidentPublicationValidationOutcome.INVALID_HEADERS
    )
    assert res_hdr_validate.error_type == "IncidentPublicationHeaderError"

    # Case 3: Escaped lone UTF-16 surrogate in raw JSON (UUID, envelope string, nested string)
    for bad_json_bytes in (
        record.kafka_value.replace(
            f'"{record.event_id}"'.encode("utf-8"),
            b'"\\ud800"',
            1,
        ),
        record.kafka_value.replace(
            b'"order-service"',
            b'"\\ud800"',
            1,
        ),
        record.kafka_value.replace(
            b'"task-4.12-controlled-runtime"',
            b'"\\udfff"',
            1,
        ),
    ):
        res_json_decode = try_decode_incident_publication(
            raw_value=bad_json_bytes,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
        )
        assert res_json_decode.ok is False
        assert (
            res_json_decode.outcome
            == IncidentPublicationValidationOutcome.INVALID_CONTRACT
        )
        assert res_json_decode.error_type == "IncidentPublicationContractError"

        res_json_validate = try_validate_incident_publication(
            raw_value=bad_json_bytes,
            raw_key=record.kafka_key,
            headers=record.kafka_headers,
            snapshot=fix01_bundle["snapshot"],
            expected_topology=fix01_bundle["topology_ref"],
            expected_blast_radius_policy=fix01_bundle["policy"],
        )
        assert res_json_validate.ok is False
        assert (
            res_json_validate.outcome
            == IncidentPublicationValidationOutcome.INVALID_CONTRACT
        )
        assert res_json_validate.error_type == "IncidentPublicationContractError"

    # Additional malformed-Unicode boundaries: expected_producer_version, topic, routing key
    res_exp_pv = try_validate_incident_publication(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=record.kafka_headers,
        snapshot=fix01_bundle["snapshot"],
        expected_topology=fix01_bundle["topology_ref"],
        expected_blast_radius_policy=fix01_bundle["policy"],
        expected_producer_version=surrogate,
    )
    assert res_exp_pv.ok is False
    assert res_exp_pv.outcome == IncidentPublicationValidationOutcome.INVALID_HEADERS
    assert res_exp_pv.error_type == "IncidentPublicationHeaderError"

    res_topic = try_decode_incident_publication(
        raw_value=record.kafka_value,
        raw_key=record.kafka_key,
        headers=record.kafka_headers,
        topic=surrogate,
    )
    assert res_topic.ok is False
    assert res_topic.outcome == IncidentPublicationValidationOutcome.INVALID_CONTRACT

    bad_key_event = record.event.model_copy(update={"service": surrogate})
    with pytest.raises(IncidentPublicationContractError):
        build_publication_kafka_key(bad_key_event)
