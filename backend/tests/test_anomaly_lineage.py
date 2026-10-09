from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import uuid

import aiokafka
from aiokafka.structs import RecordMetadata
import pytest
from pydantic import ValidationError

import app.anomaly.lineage as lineage_module
from app.anomaly.errors import AnomalyValidationError
from app.anomaly.lineage import (
    SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION,
    AnomalyReproducibilityLineage,
    build_anomaly_reproducibility_lineage,
    compute_anomaly_semantic_fingerprint,
    require_same_anomaly_semantic_identity,
)
from app.anomaly.models import (
    MAX_UINT64,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.serialization import to_canonical_telemetry_event
from app.telemetry.reliability.quarantine import (
    QuarantineRecord,
    encode_bytes,
    encode_headers,
)
from app.telemetry.reliability.replay import (
    REPLAY_MARKER_HEADER,
    QuarantineReplayService,
)
from app.telemetry.schemas import EventSeverity
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    deserialize_event,
    serialize_event,
)

SIGNAL_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
RUN_ID = uuid.UUID("99999999-8888-4777-8666-555555555555")
TENANT_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")
SOURCE_EVENT_ID_1 = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
SOURCE_EVENT_ID_2 = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
EVIDENCE_ID_1 = uuid.UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
EVIDENCE_ID_2 = uuid.UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
START_TIME = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
END_TIME = datetime(2026, 10, 1, 12, 5, tzinfo=timezone.utc)


def _sample_signal() -> AnomalySignal:
    return AnomalySignal(
        signal_id=SIGNAL_ID,
        event_time=END_TIME,
        tenant_id=TENANT_ID,
        environment="simulation",
        service="order-service-東京",
        metric_or_feature="http_request_latency_ms",
        model_name="prophet",
        model_version="1.0.0",
        anomaly_score=0.92,
        severity=EventSeverity.ERROR,
        evidence=[
            AnomalyEvidence(
                evidence_id=EVIDENCE_ID_1,
                evidence_type="residual_threshold_exceeded",
                metric_or_feature="latency",
                observed_value=450.0,
                expected_value=120.0,
                deviation=330.0,
                details={"window_size": 300, "label": "測定"},
                timestamp=END_TIME,
            ),
            AnomalyEvidence(
                evidence_id=EVIDENCE_ID_2,
                evidence_type="forecast_interval_exceeded",
                metric_or_feature="latency",
                observed_value=430.0,
                expected_value=125.0,
                deviation=305.0,
                details={"upper": 200.0, "lower": 80.0},
                timestamp=END_TIME,
            ),
        ],
        threshold_or_calibration=CalibrationMetadata(
            schema_version="1.0",
            method="quantile",
            threshold_value=0.9,
            calibration_version="1.0",
            parameters={"alpha": 0.05, "nested": {"upper": 0.9, "lower": 0.1}},
            calibrated_at=START_TIME,
        ),
        run_id=RUN_ID,
        scenario_id="latency-spike",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-key-123",
        source_event_ids=[SOURCE_EVENT_ID_1, SOURCE_EVENT_ID_2],
        source_event_time_window=EventTimeWindow(
            start_time=START_TIME,
            end_time=END_TIME,
        ),
        trace_id="trace-abc-123",
        tags={"region": "東京", "tier": "critical"},
    )


def _lineage_python_payload(
    signal: AnomalySignal,
    **updates: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": SUPPORTED_ANOMALY_LINEAGE_SCHEMA_VERSION,
        "signal_id": signal.signal_id,
        "run_id": signal.run_id,
        "scenario_id": signal.scenario_id,
        "scenario_version": signal.scenario_version,
        "seed": signal.seed,
        "reproducibility_key": signal.reproducibility_key,
        "model_name": signal.model_name,
        "model_version": signal.model_version,
        "source_event_ids": tuple(signal.source_event_ids),
        "source_event_time_window": signal.source_event_time_window,
        "semantic_fingerprint": compute_anomaly_semantic_fingerprint(signal),
    }
    payload.update(updates)
    return payload


def _replace_signal(signal: AnomalySignal, **updates: object) -> AnomalySignal:
    payload = signal.model_dump(mode="python")
    payload.update(updates)
    return AnomalySignal.model_validate(payload)


def test_lineage_maps_complete_signal_contract() -> None:
    signal = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(signal)

    assert lineage.schema_version == "1.0"
    assert lineage.signal_id == signal.signal_id
    assert lineage.run_id == signal.run_id
    assert lineage.scenario_id == signal.scenario_id
    assert lineage.scenario_version == signal.scenario_version
    assert lineage.seed == signal.seed
    assert lineage.reproducibility_key == signal.reproducibility_key
    assert lineage.model_name == signal.model_name
    assert lineage.model_version == signal.model_version
    assert lineage.source_event_ids == tuple(signal.source_event_ids)
    assert lineage.source_event_time_window == signal.source_event_time_window
    assert lineage.semantic_fingerprint == compute_anomaly_semantic_fingerprint(signal)


def test_lineage_json_round_trip_restores_complete_equality() -> None:
    lineage = build_anomaly_reproducibility_lineage(_sample_signal())

    assert (
        AnomalyReproducibilityLineage.model_validate_json(lineage.model_dump_json())
        == lineage
    )


def test_lineage_accepts_seed_boundaries_through_validation() -> None:
    signal = _sample_signal()

    assert (
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(signal, seed=0)
        ).seed
        == 0
    )
    assert (
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(signal, seed=MAX_UINT64)
        ).seed
        == MAX_UINT64
    )


@pytest.mark.parametrize("seed", [-1, MAX_UINT64 + 1, 41.0, "41", True, False, None])
def test_lineage_rejects_invalid_seed(seed: object) -> None:
    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(_sample_signal(), seed=seed)
        )


@pytest.mark.parametrize("field", ["signal_id", "run_id"])
def test_lineage_rejects_uuid_strings_in_python_mode(field: str) -> None:
    signal = _sample_signal()

    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(signal, **{field: str(getattr(signal, field))})
        )


def test_lineage_json_mode_restores_uuid_strings_and_tuple() -> None:
    lineage = build_anomaly_reproducibility_lineage(_sample_signal())

    restored = AnomalyReproducibilityLineage.model_validate_json(
        lineage.model_dump_json()
    )

    assert isinstance(restored.signal_id, uuid.UUID)
    assert isinstance(restored.run_id, uuid.UUID)
    assert isinstance(restored.source_event_ids, tuple)
    assert all(isinstance(item, uuid.UUID) for item in restored.source_event_ids)


def test_lineage_python_mode_rejects_non_tuple_ids_and_window_dictionary() -> None:
    signal = _sample_signal()
    assert signal.source_event_time_window is not None

    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(
                signal, source_event_ids=list(signal.source_event_ids)
            )
        )
    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(
                signal,
                source_event_ids=signal.source_event_ids[0],
            )
        )
    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(
                signal,
                source_event_time_window=signal.source_event_time_window.model_dump(),
            )
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "2.0"),
        ("scenario_id", "   "),
        ("scenario_version", ""),
        ("reproducibility_key", "\t"),
        ("model_name", None),
        ("model_version", False),
    ],
)
def test_lineage_rejects_invalid_version_and_required_strings(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(_sample_signal(), **{field: value})
        )


@pytest.mark.parametrize(
    "fingerprint",
    [
        "",
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        True,
        42,
        ["a" * 64],
        {"value": "a" * 64},
    ],
)
def test_lineage_rejects_malformed_fingerprint(fingerprint: object) -> None:
    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            _lineage_python_payload(
                _sample_signal(),
                semantic_fingerprint=fingerprint,
            )
        )


def test_lineage_rejects_unknown_fields_and_mutation() -> None:
    lineage = build_anomaly_reproducibility_lineage(_sample_signal())

    with pytest.raises(ValidationError):
        AnomalyReproducibilityLineage.model_validate(
            {**_lineage_python_payload(_sample_signal()), "unexpected": "value"}
        )
    with pytest.raises(ValidationError):
        lineage.seed = 100  # type: ignore[misc]


def test_lineage_source_event_snapshot_is_isolated() -> None:
    signal = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(signal)
    snapshot = lineage.source_event_ids

    signal.source_event_ids.append(uuid.UUID("12121212-1212-4212-8212-121212121212"))

    assert lineage.source_event_ids == snapshot
    assert tuple(signal.source_event_ids) != snapshot


def test_fingerprint_is_deterministic_after_json_round_trip_and_unicode() -> None:
    signal = _sample_signal()
    restored = AnomalySignal.model_validate_json(signal.model_dump_json())

    assert compute_anomaly_semantic_fingerprint(signal) == (
        compute_anomaly_semantic_fingerprint(signal)
    )
    assert compute_anomaly_semantic_fingerprint(restored) == (
        compute_anomaly_semantic_fingerprint(signal)
    )


def test_fingerprint_ignores_nested_dictionary_insertion_order() -> None:
    signal = _sample_signal()
    reordered_evidence = [
        item.model_copy(update={"details": dict(reversed(item.details.items()))})
        for item in signal.evidence
    ]
    calibration = signal.threshold_or_calibration.model_copy(
        update={
            "parameters": dict(
                reversed(signal.threshold_or_calibration.parameters.items())
            )
        }
    )
    reordered = _replace_signal(
        signal,
        tags=dict(reversed(signal.tags.items())),
        evidence=reordered_evidence,
        threshold_or_calibration=calibration,
    )

    assert compute_anomaly_semantic_fingerprint(reordered) == (
        compute_anomaly_semantic_fingerprint(signal)
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "signal_id",
        "run_id",
        "scenario",
        "seed",
        "reproducibility_key",
        "model",
        "score",
        "severity",
        "evidence",
        "calibration",
        "source_event_ids",
        "source_event_window",
        "tenant_environment_service_metric",
        "event_time",
        "trace_id",
        "tags",
    ],
)
def test_fingerprint_changes_for_every_semantic_identity_group(mutation: str) -> None:
    signal = _sample_signal()
    updates: dict[str, object]
    if mutation == "signal_id":
        updates = {"signal_id": uuid.UUID("10101010-1010-4010-8010-101010101010")}
    elif mutation == "run_id":
        updates = {"run_id": uuid.UUID("20202020-2020-4020-8020-202020202020")}
    elif mutation == "scenario":
        updates = {"scenario_id": "other", "scenario_version": "2.0.0"}
    elif mutation == "seed":
        updates = {"seed": 43}
    elif mutation == "reproducibility_key":
        updates = {"reproducibility_key": "different-key"}
    elif mutation == "model":
        updates = {"model_name": "autoencoder", "model_version": "2.0.0"}
    elif mutation == "score":
        updates = {"anomaly_score": 0.5}
    elif mutation == "severity":
        updates = {"severity": EventSeverity.WARNING}
    elif mutation == "evidence":
        updates = {"evidence": list(reversed(signal.evidence))}
    elif mutation == "calibration":
        updates = {
            "threshold_or_calibration": signal.threshold_or_calibration.model_copy(
                update={"threshold_value": 0.8}
            )
        }
    elif mutation == "source_event_ids":
        updates = {"source_event_ids": list(reversed(signal.source_event_ids))}
    elif mutation == "source_event_window":
        updates = {
            "source_event_time_window": EventTimeWindow(
                start_time=START_TIME,
                end_time=datetime(2026, 10, 1, 12, 6, tzinfo=timezone.utc),
            )
        }
    elif mutation == "tenant_environment_service_metric":
        updates = {
            "tenant_id": uuid.UUID("30303030-3030-4030-8030-303030303030"),
            "environment": "staging",
            "service": "billing-service",
            "metric_or_feature": "error_rate",
        }
    elif mutation == "event_time":
        updates = {"event_time": datetime(2026, 10, 1, 12, 6, tzinfo=timezone.utc)}
    elif mutation == "trace_id":
        updates = {"trace_id": "different-trace"}
    else:
        updates = {"tags": {"region": "大阪", "tier": "critical"}}

    mutated = _replace_signal(signal, **updates)

    assert compute_anomaly_semantic_fingerprint(mutated) != (
        compute_anomaly_semantic_fingerprint(signal)
    )


def test_fingerprint_rejects_non_signal() -> None:
    with pytest.raises(TypeError, match="Expected AnomalySignal"):
        compute_anomaly_semantic_fingerprint("not a signal")  # type: ignore[arg-type]


def test_fingerprint_translates_canonicalization_failure_with_exact_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = RuntimeError("private payload text")

    def fail_json_dump(*args: object, **kwargs: object) -> str:
        raise original

    monkeypatch.setattr(lineage_module.json, "dumps", fail_json_dump)

    with pytest.raises(AnomalyValidationError) as exc_info:
        compute_anomaly_semantic_fingerprint(_sample_signal())

    assert str(exc_info.value) == "Failed to compute anomaly semantic fingerprint"
    assert "private payload text" not in str(exc_info.value)
    assert exc_info.value.__cause__ is original
    assert exc_info.value.__suppress_context__ is True


def test_same_identity_accepts_same_copy_and_json_restored_signal() -> None:
    signal = _sample_signal()
    expected = build_anomaly_reproducibility_lineage(signal)

    assert require_same_anomaly_semantic_identity(signal, signal) == expected
    assert (
        require_same_anomaly_semantic_identity(signal, signal.model_copy()) == expected
    )
    restored = AnomalySignal.model_validate_json(signal.model_dump_json())
    assert require_same_anomaly_semantic_identity(signal, restored) == expected


@pytest.mark.parametrize("different_id", [False, True])
def test_same_identity_rejects_semantic_mismatch_with_static_message(
    different_id: bool,
) -> None:
    signal = _sample_signal()
    changed_value = "private-changed-value"
    candidate = (
        _replace_signal(
            signal,
            signal_id=uuid.UUID("40404040-4040-4040-8040-404040404040"),
        )
        if different_id
        else _replace_signal(signal, trace_id=changed_value)
    )

    with pytest.raises(AnomalyValidationError) as exc_info:
        require_same_anomaly_semantic_identity(signal, candidate)

    assert str(exc_info.value) == "Anomaly semantic identity mismatch"
    assert changed_value not in str(exc_info.value)


@pytest.mark.parametrize("invalid_side", ["expected", "candidate"])
def test_same_identity_rejects_non_signal_inputs(invalid_side: str) -> None:
    signal = _sample_signal()

    with pytest.raises(TypeError, match="Expected AnomalySignal"):
        if invalid_side == "expected":
            require_same_anomaly_semantic_identity("invalid", signal)  # type: ignore[arg-type]
        else:
            require_same_anomaly_semantic_identity(signal, "invalid")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_replay_preserves_anomaly_semantic_identity_and_transport_contract() -> (
    None
):
    signal = _sample_signal()
    event = to_canonical_telemetry_event(signal)
    raw_value = serialize_event(event)
    raw_key = construct_kafka_key(event)
    context = TelemetryExecutionContext(
        run_id=signal.run_id,
        scenario_id=signal.scenario_id,
        scenario_version=signal.scenario_version,
        reproducibility_key=signal.reproducibility_key,
        seed=signal.seed,
    )
    original_headers = construct_kafka_headers(context, "v3.11-b")
    record = QuarantineRecord(
        quarantine_id=uuid.UUID("50505050-5050-4050-8050-505050505050"),
        failure_stage="publication",
        failure_type="ProducerError",
        failure_message_sanitized="Broker unavailable",
        topic=KafkaTopic.ANOMALIES.value,
        partition=0,
        offset=100,
        kafka_timestamp_ms=1770000000000,
        raw_key_base64=encode_bytes(raw_key),
        raw_value_base64=encode_bytes(raw_value) or "",
        headers=encode_headers(original_headers),
        event_id=signal.signal_id,
        run_id=signal.run_id,
        scenario_id=signal.scenario_id,
        service=signal.service,
        event_type="anomaly",
    )
    producer = MagicMock(spec=aiokafka.AIOKafkaProducer)
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock(
        return_value=RecordMetadata(
            topic=KafkaTopic.ANOMALIES.value,
            partition=0,
            topic_partition=None,
            offset=101,
            timestamp=1770000050000,
            timestamp_type=0,
            log_start_offset=0,
        )
    )

    result = await QuarantineReplayService(producer=producer).replay_record(record)

    producer.send_and_wait.assert_awaited_once()
    replayed = producer.send_and_wait.call_args.kwargs
    assert replayed["value"] == raw_value
    assert replayed["key"] == raw_key
    assert replayed["headers"] == [
        *original_headers,
        (REPLAY_MARKER_HEADER, b"1"),
    ]
    assert replayed["headers"].count((REPLAY_MARKER_HEADER, b"1")) == 1
    reconstructed_event = deserialize_event(replayed["value"])
    reconstructed_signal = AnomalySignal.model_validate(reconstructed_event.payload)
    assert reconstructed_signal == signal
    assert reconstructed_signal.signal_id == signal.signal_id
    assert compute_anomaly_semantic_fingerprint(reconstructed_signal) == (
        compute_anomaly_semantic_fingerprint(signal)
    )
    assert result.original_offset == 100
    assert result.replay_offset == 101
    assert result.original_offset != result.replay_offset
