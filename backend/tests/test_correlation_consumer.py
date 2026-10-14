"""Task 4.5 — anomaly intake, validation, and deduplication tests.

Unit tests run without infrastructure. Only the two real-Kafka tests are
marked ``@pytest.mark.integration``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
import uuid

from aiokafka.structs import ConsumerRecord
import pytest

import app.correlation.consumer as consumer_module
from app.anomaly.errors import AnomalyHandoffError
from app.anomaly.handoff import anomaly_signal_from_envelope
from app.anomaly.lineage import (
    build_anomaly_reproducibility_lineage,
    compute_anomaly_semantic_fingerprint,
)
from app.anomaly.models import (
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.serialization import (
    ANOMALY_KAFKA_TOPIC,
    construct_anomaly_kafka_key,
    to_canonical_telemetry_event,
)
from app.core.config import settings
from app.correlation.consumer import (
    DEFAULT_INTAKE_CONSUMER_GROUP,
    AnomalyCorrelationInput,
    AnomalyDedupeStore,
    AnomalyDownstreamHandler,
    AnomalyIntakeConsumer,
    AnomalyIntakeDisposition,
    AnomalyIntakeProcessor,
    AnomalyIntakeResult,
    DedupeReservationOutcome,
    InMemoryAnomalyDedupeStore,
)
from app.correlation.errors import (
    AnomalyDedupeCapacityError,
    AnomalyDedupeStoreError,
    AnomalyEvidenceLimitError,
    AnomalyIdentityConflictError,
)
from app.correlation.models import MAX_EVIDENCE_ENTRIES, MAX_SIGNAL_REFS
from app.telemetry.reliability.errors import QuarantineWriteError
from app.telemetry.reliability.quarantine import (
    FileQuarantineSink,
    QuarantineRecord,
    decode_bytes,
)
from app.telemetry.reliability.replay import REPLAY_MARKER_HEADER
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import TelemetryEnvelope
from app.telemetry.transport.errors import (
    ConsumerNotStartedError,
    TopicEventTypeMismatchError,
)
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    serialize_event,
)
from app.telemetry.transport.consumer import parse_kafka_headers

SIGNAL_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
EVIDENCE_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
SOURCE_ID_1 = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
SOURCE_ID_2 = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
TENANT_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
RUN_ID = uuid.UUID("99999999-8888-7777-6666-555555555555")
FIXED_EVENT_TIME = datetime(2026, 10, 1, 12, 5, 0, tzinfo=timezone.utc)
FIXED_START_TIME = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
FIXED_KAFKA_TS = 1790800000000


def _det_uuid(name: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"aegis-task45-{name}")


def _sample_calibration() -> CalibrationMetadata:
    return CalibrationMetadata(
        schema_version="1.0",
        method="quantile_calibration",
        threshold_value=0.85,
        calibration_version="v1.0.0",
        parameters={"alpha": 0.05, "contamination": 0.01},
        calibrated_at=datetime(2026, 9, 30, 10, 0, 0, tzinfo=timezone.utc),
    )


def _sample_signal(
    signal_id: uuid.UUID = SIGNAL_ID,
    **overrides: Any,
) -> AnomalySignal:
    fields: dict[str, Any] = {
        "signal_id": signal_id,
        "event_time": FIXED_EVENT_TIME,
        "tenant_id": TENANT_ID,
        "environment": "simulation",
        "service": "order-service",
        "metric_or_feature": "http_request_latency_ms",
        "model_name": "isolation_forest",
        "model_version": "1.0.0",
        "anomaly_score": 0.92,
        "severity": EventSeverity.ERROR,
        "evidence": [
            AnomalyEvidence(
                evidence_id=EVIDENCE_ID,
                evidence_type="residual_threshold_exceeded",
                metric_or_feature="http_request_latency_ms",
                observed_value=450.0,
                expected_value=120.0,
                deviation=330.0,
                details={"window_size_sec": 300},
                timestamp=FIXED_EVENT_TIME,
            )
        ],
        "threshold_or_calibration": _sample_calibration(),
        "run_id": RUN_ID,
        "scenario_id": "latency-spike",
        "scenario_version": "1.0.0",
        "seed": 42,
        "reproducibility_key": "rep-key-12345",
        "source_event_ids": [SOURCE_ID_1, SOURCE_ID_2],
        "source_event_time_window": EventTimeWindow(
            start_time=FIXED_START_TIME,
            end_time=FIXED_EVENT_TIME,
        ),
        "trace_id": "trace-abc-123",
        "tags": {"region": "us-west-2", "tier": "critical"},
    }
    fields.update(overrides)
    return AnomalySignal(**fields)


def _sample_envelope(
    signal: AnomalySignal | None = None,
    topic: str = ANOMALY_KAFKA_TOPIC.value,
    partition: int = 0,
    offset: int = 42,
    kafka_timestamp_ms: int = FIXED_KAFKA_TS,
    key: bytes | None = None,
    event_override: TelemetryEvent | None = None,
    context_override: TelemetryExecutionContext | None = None,
) -> TelemetryEnvelope:
    sig = signal or _sample_signal()
    event = event_override or to_canonical_telemetry_event(sig)
    context = context_override or TelemetryExecutionContext(
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        reproducibility_key=sig.reproducibility_key,
        seed=sig.seed,
    )
    return TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=kafka_timestamp_ms,
        topic=topic,
        partition=partition,
        offset=offset,
        key=key if key is not None else construct_anomaly_kafka_key(sig),
        producer_version="v3.10-c",
    )


def _make_record(
    signal: AnomalySignal | None = None,
    topic: str = ANOMALY_KAFKA_TOPIC.value,
    partition: int = 0,
    offset: int = 42,
    timestamp: int = FIXED_KAFKA_TS,
    key: bytes | None = None,
    raw_value: bytes | None = None,
    raw_headers: list[tuple[str, bytes]] | None = None,
    event_override: TelemetryEvent | None = None,
    context_override: TelemetryExecutionContext | None = None,
) -> ConsumerRecord:
    sig = signal or _sample_signal()
    event = event_override or to_canonical_telemetry_event(sig)
    context = context_override or TelemetryExecutionContext(
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        reproducibility_key=sig.reproducibility_key,
        seed=sig.seed,
    )
    value = raw_value if raw_value is not None else serialize_event(event)
    headers = (
        raw_headers
        if raw_headers is not None
        else construct_kafka_headers(context, producer_version="v3.10-c")
    )
    record_key = key if key is not None else construct_anomaly_kafka_key(sig)
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        timestamp_type=0,
        key=record_key,
        value=value,
        checksum=None,
        serialized_key_size=len(record_key),
        serialized_value_size=len(value),
        headers=headers,
    )


class RecordingDownstream:
    """Recording stand-in for the future membership/timeline boundary."""

    def __init__(self, failures: list[Exception] | None = None) -> None:
        self.calls: list[AnomalyCorrelationInput] = []
        self._failures = list(failures or [])

    async def handle(self, correlation_input: AnomalyCorrelationInput) -> None:
        self.calls.append(correlation_input)
        if self._failures:
            raise self._failures.pop(0)


class ListQuarantineSink:
    """In-memory QuarantineSink recording every structured failure record."""

    def __init__(self) -> None:
        self.records: list[QuarantineRecord] = []

    async def quarantine(self, record: QuarantineRecord) -> None:
        self.records.append(record)


class FailingQuarantineSink:
    """QuarantineSink that always fails with the given error."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.calls = 0

    async def quarantine(self, record: QuarantineRecord) -> None:
        self.calls += 1
        raise self._error


def _make_mock_kafka() -> MagicMock:
    mock_aiokafka = MagicMock()
    mock_aiokafka.start = AsyncMock()
    mock_aiokafka.stop = AsyncMock()
    mock_aiokafka.commit = AsyncMock()
    return mock_aiokafka


def _make_started_consumer(
    processor: AnomalyIntakeProcessor,
    mock_aiokafka: MagicMock,
    quarantine_sink: Any = None,
    group_id: str = "aegis-test-group",
) -> AnomalyIntakeConsumer:
    return AnomalyIntakeConsumer(
        processor=processor,
        bootstrap_servers="localhost:9092",
        group_id=group_id,
        quarantine_sink=quarantine_sink,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka,
    )


# ---------------------------------------------------------------------------
# 8.1 Contract tests
# ---------------------------------------------------------------------------


def test_contract_valid_strict_python_construction() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    correlation_input = AnomalyCorrelationInput(signal=sig, lineage=lineage)
    assert correlation_input.signal == sig
    assert correlation_input.lineage == lineage
    result = AnomalyIntakeResult(
        disposition=AnomalyIntakeDisposition.ACCEPTED,
        correlation_input=correlation_input,
    )
    assert result.disposition == AnomalyIntakeDisposition.ACCEPTED
    assert result.correlation_input == correlation_input
    dup = AnomalyIntakeResult(disposition=AnomalyIntakeDisposition.DUPLICATE)
    assert dup.correlation_input is None


def test_contract_json_round_trip_complete_equality() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    correlation_input = AnomalyCorrelationInput(signal=sig, lineage=lineage)
    assert (
        AnomalyCorrelationInput.model_validate_json(correlation_input.model_dump_json())
        == correlation_input
    )
    result = AnomalyIntakeResult(
        disposition=AnomalyIntakeDisposition.ACCEPTED,
        correlation_input=correlation_input,
    )
    assert AnomalyIntakeResult.model_validate_json(result.model_dump_json()) == result
    dup = AnomalyIntakeResult(disposition=AnomalyIntakeDisposition.DUPLICATE)
    assert AnomalyIntakeResult.model_validate_json(dup.model_dump_json()) == dup


def test_contract_frozen_behavior() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    correlation_input = AnomalyCorrelationInput(signal=sig, lineage=lineage)
    with pytest.raises(Exception):
        correlation_input.signal = _sample_signal(signal_id=_det_uuid("other"))  # type: ignore[misc]
    result = AnomalyIntakeResult(disposition=AnomalyIntakeDisposition.DUPLICATE)
    with pytest.raises(Exception):
        result.disposition = AnomalyIntakeDisposition.ACCEPTED  # type: ignore[misc]


def test_contract_unknown_field_rejection() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    with pytest.raises(Exception):
        AnomalyCorrelationInput.model_validate(
            {"signal": sig, "lineage": lineage, "bogus_field": 1}
        )
    with pytest.raises(Exception):
        AnomalyIntakeResult.model_validate(
            {"disposition": AnomalyIntakeDisposition.ACCEPTED, "bogus_field": 1}
        )


def test_contract_strict_enum_python_mode() -> None:
    # Enum instances are accepted in Python mode.
    ok = AnomalyIntakeResult(disposition=AnomalyIntakeDisposition.ACCEPTED)
    assert ok.disposition is AnomalyIntakeDisposition.ACCEPTED
    # Raw strings, booleans, ints, wrong case, and None are rejected in
    # Python mode under strict=True.
    for bad in ("accepted", "bogus", True, 1, "ACCEPTED", None):
        with pytest.raises(Exception):
            AnomalyIntakeResult.model_validate({"disposition": bad})
    # JSON strings still decode (strict enum from JSON is allowed).
    from_json = AnomalyIntakeResult.model_validate_json(
        '{"disposition": "duplicate", "correlation_input": null}'
    )
    assert from_json.disposition is AnomalyIntakeDisposition.DUPLICATE


def test_contract_canonical_semantic_fingerprint_format() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    fingerprint = lineage.semantic_fingerprint
    assert len(fingerprint) == 64
    assert all(c in "0123456789abcdef" for c in fingerprint)
    assert fingerprint == compute_anomaly_semantic_fingerprint(sig)
    correlation_input = AnomalyCorrelationInput(signal=sig, lineage=lineage)
    assert correlation_input.semantic_fingerprint == fingerprint


def test_contract_input_contains_complete_signal_and_lineage() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    correlation_input = AnomalyCorrelationInput(signal=sig, lineage=lineage)
    assert correlation_input.signal == sig
    assert correlation_input.signal.evidence == sig.evidence
    assert correlation_input.signal.source_event_ids == sig.source_event_ids
    assert correlation_input.lineage == lineage
    assert correlation_input.lineage.signal_id == sig.signal_id
    assert correlation_input.lineage.run_id == sig.run_id


def test_contract_input_collections_isolated_from_caller() -> None:
    source_ids = [SOURCE_ID_1, SOURCE_ID_2]
    sig = _sample_signal(source_event_ids=list(source_ids))
    lineage = build_anomaly_reproducibility_lineage(sig)
    correlation_input = AnomalyCorrelationInput(signal=sig, lineage=lineage)
    source_ids.append(_det_uuid("late"))
    assert len(correlation_input.signal.source_event_ids) == 2
    assert _det_uuid("late") not in correlation_input.signal.source_event_ids
    assert isinstance(correlation_input.lineage.source_event_ids, tuple)


def test_contract_malformed_fingerprint_and_version_rejected() -> None:
    sig = _sample_signal()
    lineage = build_anomaly_reproducibility_lineage(sig)
    lineage_dict = lineage.model_dump(mode="json")
    lineage_dict["semantic_fingerprint"] = "ZZZ-not-hex"
    with pytest.raises(Exception):
        AnomalyCorrelationInput.model_validate({"signal": sig, "lineage": lineage_dict})
    lineage_dict["semantic_fingerprint"] = "abc"
    with pytest.raises(Exception):
        AnomalyCorrelationInput.model_validate({"signal": sig, "lineage": lineage_dict})
    lineage_dict2 = lineage.model_dump(mode="json")
    lineage_dict2["schema_version"] = "9.9"
    with pytest.raises(Exception):
        AnomalyCorrelationInput.model_validate(
            {"signal": sig, "lineage": lineage_dict2}
        )


# ---------------------------------------------------------------------------
# 8.2 Handoff reuse and context validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handoff_invoked_exactly_once() -> None:
    envelope = _sample_envelope()
    calls: list[TelemetryEnvelope] = []
    original = consumer_module.anomaly_signal_from_envelope

    def counting(envelope_arg: TelemetryEnvelope) -> AnomalySignal:
        calls.append(envelope_arg)
        return original(envelope_arg)

    consumer_module.anomaly_signal_from_envelope = counting  # type: ignore[assignment]
    try:
        downstream = RecordingDownstream()
        processor = AnomalyIntakeProcessor(
            dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
            downstream=downstream,
        )
        await processor.process(envelope)
    finally:
        consumer_module.anomaly_signal_from_envelope = original  # type: ignore[method-assign]
    assert len(calls) == 1
    assert calls[0] is envelope


def test_no_second_payload_parser_in_task45() -> None:
    source = Path(consumer_module.__file__).read_text(encoding="utf-8")
    assert "anomaly_signal_from_envelope" in source
    assert "AnomalySignal.model_validate" not in source
    assert source.count("anomaly_signal_from_envelope") == 2  # import + one call


@pytest.mark.asyncio
async def test_valid_context_succeeds() -> None:
    envelope = _sample_envelope()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    result = await processor.process(envelope)
    assert result.disposition == AnomalyIntakeDisposition.ACCEPTED
    assert len(downstream.calls) == 1


@pytest.mark.asyncio
async def test_handoff_mismatch_wrong_topic_fails_before_downstream() -> None:
    sig = _sample_signal()
    envelope = _sample_envelope(sig, topic=KafkaTopic.METRICS.value)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError):
        await processor.process(envelope)
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_mismatch_wrong_event_type_fails() -> None:
    sig = _sample_signal()
    event = to_canonical_telemetry_event(sig).model_copy(
        update={"event_type": EventType.METRIC}
    )
    envelope = _sample_envelope(sig, event_override=event)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError):
        await processor.process(envelope)
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_mismatch_wrong_key_fails() -> None:
    envelope = _sample_envelope(key=b"wrong-key")
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError, match="key"):
        await processor.process(envelope)
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_mismatch_tenant_fails() -> None:
    sig = _sample_signal()
    event = to_canonical_telemetry_event(sig).model_copy(
        update={"tenant_id": _det_uuid("other-tenant")}
    )
    envelope = _sample_envelope(sig, event_override=event)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError):
        await processor.process(envelope)
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_mismatch_context_run_fails() -> None:
    sig = _sample_signal()
    context = TelemetryExecutionContext(
        run_id=_det_uuid("other-run"),
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        reproducibility_key=sig.reproducibility_key,
        seed=sig.seed,
    )
    envelope = _sample_envelope(sig, context_override=context)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError, match="context"):
        await processor.process(envelope)
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_mismatch_seed_fails() -> None:
    sig = _sample_signal()
    context = TelemetryExecutionContext(
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        reproducibility_key=sig.reproducibility_key,
        seed=sig.seed + 1,
    )
    envelope = _sample_envelope(sig, context_override=context)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError, match="context"):
        await processor.process(envelope)
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_unsupported_schema_fails_with_cause() -> None:
    sig = _sample_signal()
    payload = sig.model_dump(mode="json")
    payload["schema_version"] = "9.9"
    event = to_canonical_telemetry_event(sig).model_copy(update={"payload": payload})
    envelope = _sample_envelope(sig, event_override=event)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError) as exc_info:
        await processor.process(envelope)
    assert exc_info.value.__cause__ is not None
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_handoff_malformed_payload_fails_with_cause() -> None:
    sig = _sample_signal()
    payload = sig.model_dump(mode="json")
    del payload["severity"]
    event = to_canonical_telemetry_event(sig).model_copy(update={"payload": payload})
    envelope = _sample_envelope(sig, event_override=event)
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(AnomalyHandoffError) as exc_info:
        await processor.process(envelope)
    assert exc_info.value.__cause__ is not None
    assert type(exc_info.value.__cause__).__name__ == "ValidationError"
    assert downstream.calls == []
    assert store.entry_count() == 0


@pytest.mark.asyncio
async def test_processor_rejects_non_envelope() -> None:
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    with pytest.raises(TypeError):
        await processor.process(object())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 8.3 Source-evidence bounds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bounds_zero_source_and_evidence_accepted() -> None:
    sig = _sample_signal(source_event_ids=[], evidence=[])
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    result = await processor.process(_sample_envelope(sig))
    assert result.disposition == AnomalyIntakeDisposition.ACCEPTED


@pytest.mark.asyncio
async def test_bounds_exact_max_signal_refs_accepted() -> None:
    sig = _sample_signal(
        source_event_ids=[_det_uuid(f"src-{i}") for i in range(MAX_SIGNAL_REFS)]
    )
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    result = await processor.process(_sample_envelope(sig))
    assert result.disposition == AnomalyIntakeDisposition.ACCEPTED


@pytest.mark.asyncio
async def test_bounds_max_signal_refs_plus_one_rejected() -> None:
    sig = _sample_signal(
        source_event_ids=[_det_uuid(f"src-{i}") for i in range(MAX_SIGNAL_REFS + 1)]
    )
    envelope = _sample_envelope(sig)
    before_envelope = envelope.model_dump_json()
    before_signal = sig.model_dump_json()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    with pytest.raises(AnomalyEvidenceLimitError):
        await processor.process(envelope)
    assert downstream.calls == []
    assert envelope.model_dump_json() == before_envelope
    assert sig.model_dump_json() == before_signal


@pytest.mark.asyncio
async def test_bounds_exact_max_evidence_accepted() -> None:
    sig = _sample_signal(
        evidence=[
            AnomalyEvidence(
                evidence_id=_det_uuid(f"ev-{i}"),
                evidence_type="threshold",
            )
            for i in range(MAX_EVIDENCE_ENTRIES)
        ]
    )
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    result = await processor.process(_sample_envelope(sig))
    assert result.disposition == AnomalyIntakeDisposition.ACCEPTED


@pytest.mark.asyncio
async def test_bounds_max_evidence_plus_one_rejected() -> None:
    sig = _sample_signal(
        evidence=[
            AnomalyEvidence(
                evidence_id=_det_uuid(f"ev-{i}"),
                evidence_type="threshold",
            )
            for i in range(MAX_EVIDENCE_ENTRIES + 1)
        ]
    )
    envelope = _sample_envelope(sig)
    before_envelope = envelope.model_dump_json()
    before_signal = sig.model_dump_json()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    with pytest.raises(AnomalyEvidenceLimitError):
        await processor.process(envelope)
    assert downstream.calls == []
    assert envelope.model_dump_json() == before_envelope
    assert sig.model_dump_json() == before_signal


@pytest.mark.asyncio
async def test_bounds_duplicate_source_event_id_rejected() -> None:
    sig = _sample_signal(source_event_ids=[SOURCE_ID_1, SOURCE_ID_1])
    envelope = _sample_envelope(sig)
    before_envelope = envelope.model_dump_json()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    with pytest.raises(AnomalyEvidenceLimitError, match="unique"):
        await processor.process(envelope)
    assert downstream.calls == []
    assert envelope.model_dump_json() == before_envelope


@pytest.mark.asyncio
async def test_bounds_duplicate_evidence_id_rejected() -> None:
    first = AnomalyEvidence(evidence_id=EVIDENCE_ID, evidence_type="a")
    second = AnomalyEvidence(evidence_id=EVIDENCE_ID, evidence_type="b")
    sig = _sample_signal(evidence=[first, second])
    envelope = _sample_envelope(sig)
    before_envelope = envelope.model_dump_json()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=downstream,
    )
    with pytest.raises(AnomalyEvidenceLimitError, match="unique"):
        await processor.process(envelope)
    assert downstream.calls == []
    assert envelope.model_dump_json() == before_envelope


# ---------------------------------------------------------------------------
# 8.4 Dedupe behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedupe_first_accepted_retry_duplicate_downstream_once() -> None:
    envelope = _sample_envelope()
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    first = await processor.process(envelope)
    assert first.disposition == AnomalyIntakeDisposition.ACCEPTED
    assert first.correlation_input is not None
    second = await processor.process(envelope)
    assert second.disposition == AnomalyIntakeDisposition.DUPLICATE
    assert second.correlation_input is None
    assert len(downstream.calls) == 1
    assert store.entry_count() == 1
    assert store.completed_count() == 1


@pytest.mark.asyncio
async def test_dedupe_transport_coordinates_do_not_affect_identity() -> None:
    sig = _sample_signal()
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    first = await processor.process(_sample_envelope(sig))
    assert first.disposition == AnomalyIntakeDisposition.ACCEPTED
    retry = _sample_envelope(
        sig, partition=3, offset=999, kafka_timestamp_ms=1800000000000
    )
    second = await processor.process(retry)
    assert second.disposition == AnomalyIntakeDisposition.DUPLICATE
    assert len(downstream.calls) == 1
    assert store.entry_count() == 1


@pytest.mark.asyncio
async def test_dedupe_conflict_same_id_different_fingerprint() -> None:
    sig = _sample_signal()
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    await processor.process(_sample_envelope(sig))
    accepted_fp = compute_anomaly_semantic_fingerprint(sig)
    other = _sample_signal(model_name="autoencoder")
    assert compute_anomaly_semantic_fingerprint(other) != accepted_fp
    with pytest.raises(AnomalyIdentityConflictError):
        await processor.process(_sample_envelope(other))
    assert len(downstream.calls) == 1
    assert store.entry_count() == 1
    # Accepted fingerprint is preserved.
    assert store._store[sig.signal_id][0] == accepted_fp
    assert store._store[sig.signal_id][1] is True


@pytest.mark.asyncio
async def test_dedupe_different_signal_id_accepted_independently() -> None:
    sig = _sample_signal()
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    first = await processor.process(_sample_envelope(sig))
    assert first.disposition == AnomalyIntakeDisposition.ACCEPTED
    other = _sample_signal(signal_id=_det_uuid("independent"))
    second = await processor.process(_sample_envelope(other))
    assert second.disposition == AnomalyIntakeDisposition.ACCEPTED
    assert len(downstream.calls) == 2
    assert store.entry_count() == 2


@pytest.mark.asyncio
async def test_dedupe_capacity_exact_max_then_reject() -> None:
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=2)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    for i in range(2):
        sig = _sample_signal(signal_id=_det_uuid(f"cap-{i}"))
        result = await processor.process(_sample_envelope(sig))
        assert result.disposition == AnomalyIntakeDisposition.ACCEPTED
    overflow = _sample_signal(signal_id=_det_uuid("cap-overflow"))
    with pytest.raises(AnomalyDedupeCapacityError):
        await processor.process(_sample_envelope(overflow))
    # No accepted identity was evicted.
    assert store.entry_count() == 2
    assert store.completed_count() == 2
    assert len(downstream.calls) == 2


@pytest.mark.asyncio
async def test_dedupe_store_rejects_nonpositive_max_size() -> None:
    with pytest.raises(ValueError):
        InMemoryAnomalyDedupeStore(max_size=0)
    with pytest.raises(ValueError):
        InMemoryAnomalyDedupeStore(max_size=-1)
    with pytest.raises(ValueError):
        InMemoryAnomalyDedupeStore(max_size=True)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_dedupe_store_protocol_conformance() -> None:
    store = InMemoryAnomalyDedupeStore(max_size=5)
    assert isinstance(store, AnomalyDedupeStore)
    first = await store.reserve(SIGNAL_ID, "f" * 64)
    assert first == DedupeReservationOutcome.RESERVED_NEW
    # In-flight second reservation with same fingerprint waits;
    # after complete it becomes ALREADY_COMPLETED.
    reserve_task = asyncio.create_task(store.reserve(SIGNAL_ID, "f" * 64))
    await asyncio.sleep(0.05)
    assert not reserve_task.done()
    await store.complete(SIGNAL_ID)
    again = await asyncio.wait_for(reserve_task, timeout=2.0)
    assert again == DedupeReservationOutcome.ALREADY_COMPLETED
    with pytest.raises(AnomalyIdentityConflictError):
        await store.reserve(SIGNAL_ID, "e" * 64)
    with pytest.raises(AnomalyDedupeStoreError):
        await store.complete(_det_uuid("unknown"))


# ---------------------------------------------------------------------------
# 8.4b Atomic concurrency (genuine overlapping delivery)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_atomic_overlapping_identical_success() -> None:
    sig = _sample_signal()
    sig_bytes_before = sig.model_dump_json()
    rec_a = _make_record(sig, offset=91)
    rec_b = _make_record(sig, offset=92)
    rec_a_bytes = rec_a.value
    rec_b_bytes = rec_b.value

    entered = asyncio.Event()
    release = asyncio.Event()
    concurrent = 0
    max_concurrent = 0
    call_count = 0

    async def blocking_downstream(ci: AnomalyCorrelationInput) -> None:
        nonlocal concurrent, max_concurrent, call_count
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        call_count += 1
        entered.set()
        await release.wait()
        concurrent -= 1

    class ObservingStore(InMemoryAnomalyDedupeStore):
        def __init__(self, *a: Any, **kw: Any) -> None:
            super().__init__(*a, **kw)
            self.reserve_count = 0
            self.second_reserve_entered = asyncio.Event()

        async def reserve(
            self, signal_id: uuid.UUID, fingerprint: str
        ) -> DedupeReservationOutcome:
            self.reserve_count += 1
            if self.reserve_count == 2:
                self.second_reserve_entered.set()
            return await super().reserve(signal_id, fingerprint)

    store = ObservingStore(max_size=10)
    processor = AnomalyIntakeProcessor(
        dedupe_store=store, downstream=blocking_downstream
    )
    # Capture actual envelopes passed to handoff and their serialized bytes
    captured_envelopes: list[TelemetryEnvelope] = []
    captured_env_bytes: list[str] = []
    original_handoff = consumer_module.anomaly_signal_from_envelope

    def observing_handoff(envelope: TelemetryEnvelope) -> Any:
        captured_envelopes.append(envelope)
        captured_env_bytes.append(envelope.model_dump_json())
        return original_handoff(envelope)

    consumer_module.anomaly_signal_from_envelope = observing_handoff  # type: ignore[assignment]
    mock_aiokafka = _make_mock_kafka()
    consumer = _make_started_consumer(processor, mock_aiokafka)
    await consumer.start()
    t_owner = None
    t_waiter = None
    t_o2 = None
    t_w2 = None
    try:
        t_owner = asyncio.create_task(consumer.process_record(rec_a))
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        t_waiter = asyncio.create_task(consumer.process_record(rec_b))
        # Deterministic proof waiter reached real reservation boundary
        await asyncio.wait_for(store.second_reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        assert not t_waiter.done(), "waiter completed prematurely while owner in-flight"
        assert max_concurrent == 1
        assert call_count == 1
        assert mock_aiokafka.commit.await_count == 0
        release.set()
        await asyncio.wait_for(t_owner, timeout=2.0)
        await asyncio.wait_for(t_waiter, timeout=2.0)
        assert call_count == 1
        assert max_concurrent == 1
        assert mock_aiokafka.commit.await_count == 2
        from aiokafka import TopicPartition

        tp = TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0)
        assert mock_aiokafka.commit.await_args_list[-1][0][0][tp] == 93
        # Owner ACCEPTED, waiter DUPLICATE via store state and downstream count
        # Directly verify via processor on same store would be ALREADY_COMPLETED;
        # consumer path proves via exactly 2 commits and 1 downstream call
        assert store.entry_count() == 1
        assert store.completed_count() == 1
        # Verify actual envelopes processed are byte-identical before/after
        assert len(captured_envelopes) == 2
        for idx, env in enumerate(captured_envelopes):
            assert env.model_dump_json() == captured_env_bytes[idx]
        assert rec_a.value == rec_a_bytes
        assert rec_b.value == rec_b_bytes
        assert sig.model_dump_json() == sig_bytes_before
        # Also prove processor-level dispositions deterministically
        sig2 = _sample_signal(signal_id=_det_uuid("atomic-success-proc"))
        env_proc = _sample_envelope(sig2, offset=191)
        store2 = InMemoryAnomalyDedupeStore(max_size=10)
        proc2 = AnomalyIntakeProcessor(
            dedupe_store=store2, downstream=blocking_downstream
        )
        # Use fresh blocking for proc2
        entered2 = asyncio.Event()
        release2 = asyncio.Event()

        async def blocking2(ci: AnomalyCorrelationInput) -> None:
            entered2.set()
            await release2.wait()

        proc2.downstream = blocking2  # type: ignore[assignment]
        # Need ObservingStore for proc2 as well
        store2_obs = ObservingStore(max_size=10)
        proc2.dedupe_store = store2_obs
        t_o2 = asyncio.create_task(proc2.process(env_proc))
        await asyncio.wait_for(entered2.wait(), timeout=2.0)
        t_w2 = asyncio.create_task(proc2.process(env_proc))
        await asyncio.wait_for(store2_obs.second_reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        assert not t_w2.done()
        release2.set()
        owner_res = await asyncio.wait_for(t_o2, timeout=2.0)
        waiter_res = await asyncio.wait_for(t_w2, timeout=2.0)
        assert isinstance(owner_res, AnomalyIntakeResult)
        assert owner_res.disposition == AnomalyIntakeDisposition.ACCEPTED
        assert isinstance(waiter_res, AnomalyIntakeResult)
        assert waiter_res.disposition == AnomalyIntakeDisposition.DUPLICATE
    finally:
        consumer_module.anomaly_signal_from_envelope = original_handoff  # type: ignore[assignment]
        if t_owner is not None and not t_owner.done():
            t_owner.cancel()
            try:
                await t_owner
            except asyncio.CancelledError:
                pass
        if t_waiter is not None and not t_waiter.done():
            t_waiter.cancel()
            try:
                await t_waiter
            except asyncio.CancelledError:
                pass
        if t_o2 is not None and not t_o2.done():
            t_o2.cancel()
            try:
                await t_o2
            except asyncio.CancelledError:
                pass
        if t_w2 is not None and not t_w2.done():
            t_w2.cancel()
            try:
                await t_w2
            except asyncio.CancelledError:
                pass
        assert t_owner is None or t_owner.done()
        assert t_waiter is None or t_waiter.done()
        assert t_o2 is None or t_o2.done()
        assert t_w2 is None or t_w2.done()
        await consumer.stop()


@pytest.mark.asyncio
async def test_atomic_owner_failure_waiter_takeover() -> None:
    sig = _sample_signal()
    sentinel = ValueError("sentinel-owner-failure-c1r1")
    entered = asyncio.Event()
    release = asyncio.Event()
    concurrent = 0
    max_concurrent = 0

    async def failing_then_ok(ci: AnomalyCorrelationInput) -> None:
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        try:
            if not entered.is_set():
                entered.set()
                await release.wait()
                raise sentinel
            return None
        finally:
            concurrent -= 1

    class ObservingStore(InMemoryAnomalyDedupeStore):
        def __init__(self, *a: Any, **kw: Any) -> None:
            super().__init__(*a, **kw)
            self.reserve_count = 0
            self.second_reserve_entered = asyncio.Event()

        async def reserve(
            self, signal_id: uuid.UUID, fingerprint: str
        ) -> DedupeReservationOutcome:
            self.reserve_count += 1
            if self.reserve_count == 2:
                self.second_reserve_entered.set()
            return await super().reserve(signal_id, fingerprint)

    store = ObservingStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=failing_then_ok)
    mock_aiokafka = _make_mock_kafka()
    consumer = _make_started_consumer(processor, mock_aiokafka)
    await consumer.start()
    t_owner = None
    t_waiter = None
    t_o2 = None
    t_w2 = None
    try:
        rec_a = _make_record(sig, offset=101)
        rec_b = _make_record(sig, offset=102)
        t_owner = asyncio.create_task(consumer.process_record(rec_a))
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        t_waiter = asyncio.create_task(consumer.process_record(rec_b))
        await asyncio.wait_for(store.second_reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        assert not t_waiter.done()
        assert max_concurrent == 1
        assert mock_aiokafka.commit.await_count == 0
        release.set()
        from app.telemetry.transport.errors import ConsumerHandlerError

        with pytest.raises(ConsumerHandlerError) as exc_info:
            await asyncio.wait_for(t_owner, timeout=2.0)
        assert exc_info.value.__cause__ is sentinel
        await asyncio.wait_for(t_waiter, timeout=2.0)
        assert max_concurrent == 1
        assert mock_aiokafka.commit.await_count == 1
        from aiokafka import TopicPartition

        tp = TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0)
        assert mock_aiokafka.commit.await_args_list[0][0][0][tp] == 103
        assert store.entry_count() == 1
        assert store.completed_count() == 1
        # Also prove processor-level waiter result is ACCEPTED
        sig2 = _sample_signal(signal_id=_det_uuid("atomic-fail-proc"))
        env2 = _sample_envelope(sig2, offset=201)
        entered2 = asyncio.Event()
        release2 = asyncio.Event()

        async def fail_then_ok2(ci: AnomalyCorrelationInput) -> None:
            if not entered2.is_set():
                entered2.set()
                await release2.wait()
                raise sentinel
            return None

        store2 = ObservingStore(max_size=10)
        proc2 = AnomalyIntakeProcessor(dedupe_store=store2, downstream=fail_then_ok2)
        t_o2 = asyncio.create_task(proc2.process(env2))
        await asyncio.wait_for(entered2.wait(), timeout=2.0)
        t_w2 = asyncio.create_task(proc2.process(env2))
        await asyncio.wait_for(store2.second_reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        assert not t_w2.done()
        release2.set()
        with pytest.raises(ValueError) as exc2:
            await asyncio.wait_for(t_o2, timeout=2.0)
        assert exc2.value is sentinel
        res_w2 = await asyncio.wait_for(t_w2, timeout=2.0)
        assert isinstance(res_w2, AnomalyIntakeResult)
        assert res_w2.disposition == AnomalyIntakeDisposition.ACCEPTED
    finally:
        if t_owner is not None and not t_owner.done():
            t_owner.cancel()
            try:
                await t_owner
            except asyncio.CancelledError:
                pass
        if t_waiter is not None and not t_waiter.done():
            t_waiter.cancel()
            try:
                await t_waiter
            except asyncio.CancelledError:
                pass
        if t_o2 is not None and not t_o2.done():
            t_o2.cancel()
            try:
                await t_o2
            except asyncio.CancelledError:
                pass
        if t_w2 is not None and not t_w2.done():
            t_w2.cancel()
            try:
                await t_w2
            except asyncio.CancelledError:
                pass
        assert t_owner is None or t_owner.done()
        assert t_waiter is None or t_waiter.done()
        assert t_o2 is None or t_o2.done()
        assert t_w2 is None or t_w2.done()
        await consumer.stop()


@pytest.mark.asyncio
async def test_atomic_inflight_conflict() -> None:
    sig = _sample_signal()
    other = AnomalySignal(
        signal_id=sig.signal_id,
        event_time=sig.event_time,
        tenant_id=sig.tenant_id,
        environment=sig.environment,
        service=sig.service,
        metric_or_feature=sig.metric_or_feature,
        model_name="autoencoder",
        model_version=sig.model_version,
        anomaly_score=sig.anomaly_score,
        severity=sig.severity,
        evidence=[
            AnomalyEvidence(
                evidence_id=_det_uuid("c1r1-conflict-ev"),
                evidence_type="autoencoder_reconstruction_error",
                metric_or_feature=sig.metric_or_feature,
                observed_value=999.0,
            )
        ],
        threshold_or_calibration=sig.threshold_or_calibration,
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        seed=sig.seed,
        reproducibility_key=sig.reproducibility_key,
        source_event_ids=[_det_uuid("c1r1-conflict-src")],
        source_event_time_window=sig.source_event_time_window,
        trace_id=sig.trace_id,
        tags=dict(sig.tags),
    )
    assert compute_anomaly_semantic_fingerprint(
        sig
    ) != compute_anomaly_semantic_fingerprint(other)
    original_fp = compute_anomaly_semantic_fingerprint(sig)
    entered = asyncio.Event()
    release = asyncio.Event()
    downstream_calls: list[uuid.UUID] = []

    async def blocking_ok(ci: AnomalyCorrelationInput) -> None:
        downstream_calls.append(ci.signal.signal_id)
        entered.set()
        await release.wait()

    class ObservingStore(InMemoryAnomalyDedupeStore):
        def __init__(self, *a: Any, **kw: Any) -> None:
            super().__init__(*a, **kw)
            self.reserve_count = 0
            self.reserve_entered = asyncio.Event()

        async def reserve(
            self, signal_id: uuid.UUID, fingerprint: str
        ) -> DedupeReservationOutcome:
            self.reserve_count += 1
            # Signal on second call (conflict attempt) immediately
            if self.reserve_count == 2:
                self.reserve_entered.set()
            return await super().reserve(signal_id, fingerprint)

    store = ObservingStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=blocking_ok)
    mock_aiokafka = _make_mock_kafka()
    consumer = _make_started_consumer(processor, mock_aiokafka)
    await consumer.start()
    t_owner = None
    conflict_task = None
    try:
        rec_owner = _make_record(sig, offset=111)
        rec_conflict = _make_record(other, offset=112)
        t_owner = asyncio.create_task(consumer.process_record(rec_owner))
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        conflict_task = asyncio.create_task(consumer.process_record(rec_conflict))
        await asyncio.wait_for(store.reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        from app.telemetry.transport.errors import ConsumerHandlerError

        with pytest.raises(ConsumerHandlerError) as exc_info:
            await asyncio.wait_for(conflict_task, timeout=2.0)
        assert isinstance(exc_info.value.__cause__, AnomalyIdentityConflictError)
        assert downstream_calls == [sig.signal_id]
        assert mock_aiokafka.commit.await_count == 0
        assert store.entry_count() == 1
        assert store._store[sig.signal_id][0] == original_fp
        assert store._store[sig.signal_id][1] is False
        release.set()
        await asyncio.wait_for(t_owner, timeout=2.0)
        t_owner = None
        assert store.completed_count() == 1
        assert store._store[sig.signal_id][0] == original_fp
        assert mock_aiokafka.commit.await_count == 1
        sig3 = _sample_signal(signal_id=_det_uuid("atomic-conflict-proc"))
        other3 = AnomalySignal(
            signal_id=sig3.signal_id,
            event_time=sig3.event_time,
            tenant_id=sig3.tenant_id,
            environment=sig3.environment,
            service=sig3.service,
            metric_or_feature=sig3.metric_or_feature,
            model_name="autoencoder",
            model_version=sig3.model_version,
            anomaly_score=sig3.anomaly_score,
            severity=sig3.severity,
            evidence=[
                AnomalyEvidence(
                    evidence_id=_det_uuid("c1r1-c2-ev"),
                    evidence_type="autoencoder_reconstruction_error",
                    metric_or_feature=sig3.metric_or_feature,
                    observed_value=999.0,
                )
            ],
            threshold_or_calibration=sig3.threshold_or_calibration,
            run_id=sig3.run_id,
            scenario_id=sig3.scenario_id,
            scenario_version=sig3.scenario_version,
            seed=sig3.seed,
            reproducibility_key=sig3.reproducibility_key,
            source_event_ids=[_det_uuid("c1r1-c2-src")],
            source_event_time_window=sig3.source_event_time_window,
            trace_id=sig3.trace_id,
            tags=dict(sig3.tags),
        )
        entered3 = asyncio.Event()
        release3 = asyncio.Event()

        async def blocking3(ci: AnomalyCorrelationInput) -> None:
            entered3.set()
            await release3.wait()

        store3 = ObservingStore(max_size=10)
        proc3 = AnomalyIntakeProcessor(dedupe_store=store3, downstream=blocking3)
        env_owner3 = _sample_envelope(sig3, offset=212)
        env_conf3 = _sample_envelope(other3, offset=213)
        t_o3 = None
        t_c3 = None
        try:
            t_o3 = asyncio.create_task(proc3.process(env_owner3))
            await asyncio.wait_for(entered3.wait(), timeout=2.0)
            t_c3 = asyncio.create_task(proc3.process(env_conf3))
            await asyncio.wait_for(store3.reserve_entered.wait(), timeout=2.0)
            with pytest.raises(AnomalyIdentityConflictError):
                await asyncio.wait_for(t_c3, timeout=2.0)
            release3.set()
            res_o3 = await asyncio.wait_for(t_o3, timeout=2.0)
            assert isinstance(res_o3, AnomalyIntakeResult)
            assert res_o3.disposition == AnomalyIntakeDisposition.ACCEPTED
        finally:
            if t_o3 is not None and not t_o3.done():
                t_o3.cancel()
                try:
                    await t_o3
                except asyncio.CancelledError:
                    pass
            if t_c3 is not None and not t_c3.done():
                t_c3.cancel()
                try:
                    await t_c3
                except asyncio.CancelledError:
                    pass
            assert t_o3 is None or t_o3.done()
            assert t_c3 is None or t_c3.done()
    finally:
        if conflict_task is not None and not conflict_task.done():
            conflict_task.cancel()
            try:
                await conflict_task
            except asyncio.CancelledError:
                pass

        if t_owner is not None and not t_owner.done():
            t_owner.cancel()
            try:
                await t_owner
            except asyncio.CancelledError:
                pass

        assert conflict_task is None or conflict_task.done()
        assert t_owner is None or t_owner.done()
        await consumer.stop()


@pytest.mark.asyncio
async def test_atomic_cancellation_safety() -> None:
    sig = _sample_signal()
    entered = asyncio.Event()
    concurrent = 0
    max_concurrent = 0

    async def blocking(ci: AnomalyCorrelationInput) -> None:
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        try:
            entered.set()
            await asyncio.Event().wait()  # never completes unless cancelled
        finally:
            concurrent -= 1

    async def quick_ok(ci: AnomalyCorrelationInput) -> None:
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        concurrent -= 1

    class ObservingStore(InMemoryAnomalyDedupeStore):
        def __init__(self, *a: Any, **kw: Any) -> None:
            super().__init__(*a, **kw)
            self.reserve_count = 0
            self.second_reserve_entered = asyncio.Event()

        async def reserve(
            self, signal_id: uuid.UUID, fingerprint: str
        ) -> DedupeReservationOutcome:
            self.reserve_count += 1
            if self.reserve_count == 2:
                self.second_reserve_entered.set()
            return await super().reserve(signal_id, fingerprint)

    store = ObservingStore(max_size=10)
    processor_owner = AnomalyIntakeProcessor(dedupe_store=store, downstream=blocking)
    mock_owner = _make_mock_kafka()
    consumer_owner = _make_started_consumer(processor_owner, mock_owner)
    await consumer_owner.start()
    processor_waiter = AnomalyIntakeProcessor(dedupe_store=store, downstream=quick_ok)
    mock_waiter = _make_mock_kafka()
    consumer_waiter = _make_started_consumer(processor_waiter, mock_waiter)
    await consumer_waiter.start()
    t_owner = None
    t_waiter = None
    t_oc = None
    t_wc = None
    try:
        rec_a = _make_record(sig, offset=121)
        rec_b = _make_record(sig, offset=122)
        t_owner = asyncio.create_task(consumer_owner.process_record(rec_a))
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        # Start waiter while owner is blocked; waiter must reach reservation boundary
        t_waiter = asyncio.create_task(consumer_waiter.process_record(rec_b))
        await asyncio.wait_for(store.second_reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        assert not t_waiter.done(), "waiter should be pending on in-flight reservation"
        assert max_concurrent == 1
        assert mock_owner.commit.await_count == 0
        assert mock_waiter.commit.await_count == 0
        t_owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(t_owner, timeout=2.0)
        t_owner = None
        await asyncio.wait_for(t_waiter, timeout=2.0)
        assert max_concurrent == 1
        assert mock_waiter.commit.await_count == 1
        from aiokafka import TopicPartition

        tp = TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0)
        assert mock_waiter.commit.await_args_list[0][0][0][tp] == 123
        assert store.entry_count() == 1
        assert store.completed_count() == 1
        assert t_waiter.done() and not t_waiter.cancelled()
        # Prove processor-level cancellation path also returns ACCEPTED for waiter
        entered2 = asyncio.Event()

        async def blocking2(ci: AnomalyCorrelationInput) -> None:
            entered2.set()
            await asyncio.Event().wait()

        async def quick2(ci: AnomalyCorrelationInput) -> None:
            return None

        store2 = ObservingStore(max_size=10)
        proc_o2 = AnomalyIntakeProcessor(dedupe_store=store2, downstream=blocking2)
        proc_w2 = AnomalyIntakeProcessor(dedupe_store=store2, downstream=quick2)
        sig_c = _sample_signal(signal_id=_det_uuid("atomic-cancel-proc2"))
        env_c1 = _sample_envelope(sig_c, offset=221)
        env_c2 = _sample_envelope(sig_c, offset=222)
        t_oc = None
        t_wc = None
        t_oc = asyncio.create_task(proc_o2.process(env_c1))
        await asyncio.wait_for(entered2.wait(), timeout=2.0)
        t_wc = asyncio.create_task(proc_w2.process(env_c2))
        await asyncio.wait_for(store2.second_reserve_entered.wait(), timeout=2.0)
        await asyncio.sleep(0)
        assert not t_wc.done()
        t_oc.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(t_oc, timeout=2.0)
        res_wc = await asyncio.wait_for(t_wc, timeout=2.0)
        assert isinstance(res_wc, AnomalyIntakeResult)
        assert res_wc.disposition == AnomalyIntakeDisposition.ACCEPTED
    finally:
        if t_oc is not None and not t_oc.done():
            t_oc.cancel()
            try:
                await t_oc
            except asyncio.CancelledError:
                pass
        if t_wc is not None and not t_wc.done():
            t_wc.cancel()
            try:
                await t_wc
            except asyncio.CancelledError:
                pass
        if t_owner is not None and not t_owner.done():
            t_owner.cancel()
            try:
                await t_owner
            except asyncio.CancelledError:
                pass
        if t_waiter is not None and not t_waiter.done():
            t_waiter.cancel()
            try:
                await t_waiter
            except asyncio.CancelledError:
                pass
        assert t_owner is None or t_owner.done()
        assert t_waiter is None or t_waiter.done()
        assert t_oc is None or t_oc.done()
        assert t_wc is None or t_wc.done()
        await consumer_owner.stop()
        await consumer_waiter.stop()


# ---------------------------------------------------------------------------
# 8.5 Processing and offset boundary (real TelemetryConsumer path + mocks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_offset_ordering_reserve_downstream_complete_commit() -> None:
    sig = _sample_signal()
    record = _make_record(sig, offset=7)
    order: list[str] = []
    store = InMemoryAnomalyDedupeStore(max_size=10)
    orig_reserve = store.reserve
    orig_complete = store.complete

    async def spy_reserve(
        signal_id: uuid.UUID, fingerprint: str
    ) -> DedupeReservationOutcome:
        order.append("reserve")
        return await orig_reserve(signal_id, fingerprint)

    async def spy_complete(signal_id: uuid.UUID) -> None:
        order.append("complete")
        await orig_complete(signal_id)

    store.reserve = spy_reserve  # type: ignore[method-assign]
    store.complete = spy_complete  # type: ignore[method-assign]

    async def spy_downstream(correlation_input: AnomalyCorrelationInput) -> None:
        order.append("downstream")

    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=spy_downstream)
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(processor, mock_aiokafka)
    await intake_consumer.start()
    try:
        await intake_consumer.process_record(record)
    finally:
        await intake_consumer.stop()
    assert order == ["reserve", "downstream", "complete"]
    from aiokafka import TopicPartition

    mock_aiokafka.commit.assert_awaited_once_with(
        {TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0): 8}
    )


@pytest.mark.asyncio
async def test_offset_downstream_failure_releases_and_no_commit() -> None:
    sig = _sample_signal()
    record = _make_record(sig, offset=11)
    business_error = ValueError("downstream-business-failure")
    downstream = RecordingDownstream(failures=[business_error])
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    with pytest.raises(Exception) as exc_info:
        await processor.process(_sample_envelope(sig))
    # Same business exception object propagates (processor does not translate).
    assert exc_info.value is business_error
    assert store.entry_count() == 0
    # Retry after the uncommitted failure can be accepted.
    retry_result = await processor.process(_sample_envelope(sig))
    assert retry_result.disposition == AnomalyIntakeDisposition.ACCEPTED
    assert len(downstream.calls) == 2

    # Without quarantine the consumer path does not commit on failure.
    failing = RecordingDownstream(failures=[ValueError("boom")])
    processor2 = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10), downstream=failing
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(processor2, mock_aiokafka)
    await intake_consumer.start()
    try:
        with pytest.raises(Exception):
            await intake_consumer.process_record(record)
    finally:
        await intake_consumer.stop()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_offset_duplicate_commits_without_downstream() -> None:
    sig = _sample_signal()
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(processor, mock_aiokafka)
    await intake_consumer.start()
    try:
        await intake_consumer.process_record(_make_record(sig, offset=20))
        await intake_consumer.process_record(_make_record(sig, offset=21))
    finally:
        await intake_consumer.stop()
    assert len(downstream.calls) == 1
    from aiokafka import TopicPartition

    assert mock_aiokafka.commit.await_count == 2
    mock_aiokafka.commit.assert_awaited_with(
        {TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0): 22}
    )


@pytest.mark.asyncio
async def test_offset_commit_failure_does_not_rerun_downstream() -> None:
    sig = _sample_signal()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10), downstream=downstream
    )
    mock_aiokafka = _make_mock_kafka()
    mock_aiokafka.commit = AsyncMock(side_effect=RuntimeError("commit-boom"))
    intake_consumer = _make_started_consumer(processor, mock_aiokafka)
    await intake_consumer.start()
    try:
        with pytest.raises(RuntimeError, match="commit-boom"):
            await intake_consumer.process_record(_make_record(sig, offset=30))
    finally:
        await intake_consumer.stop()
    assert len(downstream.calls) == 1


@pytest.mark.asyncio
async def test_offset_consumer_not_started_is_typed() -> None:
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    intake_consumer = AnomalyIntakeConsumer(
        processor=processor,
        bootstrap_servers="localhost:9092",
        group_id="aegis-test-not-started",
        consumer_factory=lambda *args, **kwargs: _make_mock_kafka(),
    )
    with pytest.raises(ConsumerNotStartedError):
        await intake_consumer.process_record(_make_record(offset=1))


@pytest.mark.asyncio
async def test_offset_constructor_performs_no_network_io() -> None:
    calls: list[tuple[Any, ...]] = []

    def factory(*args: Any, **kwargs: Any) -> MagicMock:
        calls.append((args, kwargs))
        return _make_mock_kafka()

    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    intake_consumer = AnomalyIntakeConsumer(
        processor=processor,
        bootstrap_servers="localhost:9092",
        group_id="aegis-test-no-io",
        consumer_factory=factory,
    )
    assert calls == []
    assert intake_consumer._consumer is None  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_offset_start_stop_idempotent() -> None:
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(processor, mock_aiokafka)
    await intake_consumer.start()
    await intake_consumer.start()
    await intake_consumer.stop()
    await intake_consumer.stop()


@pytest.mark.asyncio
async def test_offset_only_anomaly_topic_and_type_accepted() -> None:
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(processor, mock_aiokafka)
    assert intake_consumer.topics == [KafkaTopic.ANOMALIES.value]
    assert intake_consumer.allowed_event_types == {EventType.ANOMALY}
    assert intake_consumer.allowed_topics == {KafkaTopic.ANOMALIES.value}
    assert intake_consumer.group_id == "aegis-test-group"
    assert DEFAULT_INTAKE_CONSUMER_GROUP == "aegis-correlation-anomaly-intake-v1"
    await intake_consumer.start()
    try:
        metric_event = TelemetryEvent(
            schema_version="1.0",
            event_id=_det_uuid("metric"),
            event_time=FIXED_EVENT_TIME,
            tenant_id=TENANT_ID,
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            payload={"metric_name": "x", "value": 1.0},
        )
        context = TelemetryExecutionContext(
            run_id=RUN_ID,
            scenario_id="latency-spike",
            scenario_version="1.0.0",
            reproducibility_key="rep-key-12345",
            seed=42,
        )
        metric_record = ConsumerRecord(
            topic=KafkaTopic.METRICS.value,
            partition=0,
            offset=5,
            timestamp=FIXED_KAFKA_TS,
            timestamp_type=0,
            key=b"k",
            value=serialize_event(metric_event),
            checksum=None,
            serialized_key_size=1,
            serialized_value_size=1,
            headers=construct_kafka_headers(context),
        )
        with pytest.raises(TopicEventTypeMismatchError):
            await intake_consumer.process_record(metric_record)
    finally:
        await intake_consumer.stop()
    mock_aiokafka.commit.assert_not_called()


# ---------------------------------------------------------------------------
# 8.6 Quarantine and replay policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quarantine_malformed_without_sink_uncommitted() -> None:
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(
        processor, mock_aiokafka, quarantine_sink=None
    )
    await intake_consumer.start()
    try:
        with pytest.raises(Exception):
            await intake_consumer.process_record(_make_record(raw_value=b"NOT-JSON{{{"))
    finally:
        await intake_consumer.stop()
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_quarantine_malformed_with_sink_single_record_then_commit() -> None:
    raw_value = b"NOT-JSON{{{"
    raw_key = b"poison-key"
    sig = _sample_signal()
    raw_headers = [
        ("run_id", str(sig.run_id).encode()),
        ("scenario_id", sig.scenario_id.encode()),
        ("scenario_version", sig.scenario_version.encode()),
        ("reproducibility_key", sig.reproducibility_key.encode()),
        ("seed", str(sig.seed).encode()),
        ("custom", b"kept"),
    ]
    record = _make_record(
        sig,
        offset=50,
        timestamp=1790900000000,
        key=raw_key,
        raw_value=raw_value,
        raw_headers=raw_headers,
    )
    sink = ListQuarantineSink()
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10), downstream=downstream
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(
        processor, mock_aiokafka, quarantine_sink=sink
    )
    await intake_consumer.start()
    try:
        outcome = await intake_consumer.process_record(record)
    finally:
        await intake_consumer.stop()
    assert isinstance(outcome, QuarantineRecord)
    assert len(sink.records) == 1
    stored = sink.records[0]
    assert decode_bytes(stored.raw_value_base64) == raw_value
    assert decode_bytes(stored.raw_key_base64) == raw_key
    assert stored.topic == ANOMALY_KAFKA_TOPIC.value
    assert stored.partition == 0
    assert stored.offset == 50
    assert stored.kafka_timestamp_ms == 1790900000000
    header_pairs = [
        (h.name, decode_bytes(h.value_base64) or b"") for h in stored.headers
    ]
    assert header_pairs == raw_headers
    assert downstream.calls == []
    from aiokafka import TopicPartition

    mock_aiokafka.commit.assert_awaited_once_with(
        {TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0): 51}
    )


@pytest.mark.asyncio
async def test_quarantine_conflict_single_record_then_commit() -> None:
    sig = _sample_signal()
    other = _sample_signal(model_name="autoencoder")
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=10)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    sink = ListQuarantineSink()
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(
        processor, mock_aiokafka, quarantine_sink=sink
    )
    await intake_consumer.start()
    try:
        await intake_consumer.process_record(_make_record(sig, offset=60))
        outcome = await intake_consumer.process_record(_make_record(other, offset=61))
    finally:
        await intake_consumer.stop()
    assert isinstance(outcome, QuarantineRecord)
    assert outcome.failure_stage == "persistence"
    # Exactly one structured failure record; no second record produced.
    assert len(sink.records) == 1
    assert len(downstream.calls) == 1
    from aiokafka import TopicPartition

    assert mock_aiokafka.commit.await_count == 2
    mock_aiokafka.commit.assert_awaited_with(
        {TopicPartition(ANOMALY_KAFKA_TOPIC.value, 0): 62}
    )


@pytest.mark.asyncio
async def test_quarantine_failure_propagates_uncommitted() -> None:
    sink = FailingQuarantineSink(QuarantineWriteError("sink-boom"))
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(
        processor, mock_aiokafka, quarantine_sink=sink
    )
    await intake_consumer.start()
    try:
        with pytest.raises(QuarantineWriteError):
            await intake_consumer.process_record(_make_record(raw_value=b"BROKEN{{{"))
    finally:
        await intake_consumer.stop()
    assert sink.calls == 1
    mock_aiokafka.commit.assert_not_called()


@pytest.mark.asyncio
async def test_quarantine_conflict_without_sink_uncommitted() -> None:
    sig = _sample_signal()
    other = _sample_signal(model_name="autoencoder")
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10), downstream=downstream
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(
        processor, mock_aiokafka, quarantine_sink=None
    )
    await intake_consumer.start()
    try:
        await intake_consumer.process_record(_make_record(sig, offset=70))
        with pytest.raises(Exception):
            await intake_consumer.process_record(_make_record(other, offset=71))
    finally:
        await intake_consumer.stop()
    # Only the accepted delivery committed.
    assert mock_aiokafka.commit.await_count == 1
    assert len(downstream.calls) == 1


@pytest.mark.asyncio
async def test_replay_marker_header_preserved_and_duplicate() -> None:
    sig = _sample_signal()
    context = TelemetryExecutionContext(
        run_id=sig.run_id,
        scenario_id=sig.scenario_id,
        scenario_version=sig.scenario_version,
        reproducibility_key=sig.reproducibility_key,
        seed=sig.seed,
    )
    headers = construct_kafka_headers(context, producer_version="v3.10-c")
    headers = list(headers) + [(REPLAY_MARKER_HEADER, b"1")]
    # Existing header parser tolerates the extra replay marker.
    parsed_context, _ = parse_kafka_headers(headers)
    assert parsed_context.run_id == sig.run_id
    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10), downstream=downstream
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(processor, mock_aiokafka)
    await intake_consumer.start()
    try:
        await intake_consumer.process_record(_make_record(sig, offset=80))
        replayed = await intake_consumer.process_record(
            _make_record(sig, offset=81, raw_headers=headers)
        )
    finally:
        await intake_consumer.stop()
    # Replay of an accepted semantic signal resolves to duplicate.
    assert len(downstream.calls) == 1
    assert mock_aiokafka.commit.await_count == 2
    assert replayed is not None
    # No recursive replay-loop-prevention beyond the frozen reliability
    # contract is claimed; dedupe only classifies the semantic retry.


@pytest.mark.asyncio
async def test_quarantine_store_failure_paths_preserve_cause() -> None:
    class AlwaysBoomStore:
        async def reserve(self, signal_id: uuid.UUID, fingerprint: str) -> Any:
            raise AnomalyDedupeStoreError("reserve-boom")

        async def complete(self, signal_id: uuid.UUID) -> None:
            raise AnomalyDedupeStoreError("complete-boom")

        async def release(self, signal_id: uuid.UUID) -> None:
            return None

    downstream = RecordingDownstream()
    processor = AnomalyIntakeProcessor(
        dedupe_store=AlwaysBoomStore(),
        downstream=downstream,  # type: ignore[arg-type]
    )
    with pytest.raises(AnomalyDedupeStoreError, match="reserve-boom"):
        await processor.process(_sample_envelope())
    assert downstream.calls == []


# ---------------------------------------------------------------------------
# 8.7 Immutability (complete serialized equality)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_immutability_accepted() -> None:
    envelope = _sample_envelope()
    before_envelope = envelope.model_dump_json()
    sig_before = anomaly_signal_from_envelope(envelope).model_dump_json()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    await processor.process(envelope)
    assert envelope.model_dump_json() == before_envelope
    assert anomaly_signal_from_envelope(envelope).model_dump_json() == sig_before


@pytest.mark.asyncio
async def test_immutability_duplicate() -> None:
    envelope = _sample_envelope()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    await processor.process(envelope)
    before_envelope = envelope.model_dump_json()
    sig_before = anomaly_signal_from_envelope(envelope).model_dump_json()
    await processor.process(envelope)
    assert envelope.model_dump_json() == before_envelope
    assert anomaly_signal_from_envelope(envelope).model_dump_json() == sig_before


@pytest.mark.asyncio
async def test_immutability_conflict() -> None:
    sig = _sample_signal()
    other = _sample_signal(model_name="autoencoder")
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    await processor.process(_sample_envelope(sig))
    conflict_envelope = _sample_envelope(other)
    before_envelope = conflict_envelope.model_dump_json()
    before_signal = other.model_dump_json()
    with pytest.raises(AnomalyIdentityConflictError):
        await processor.process(conflict_envelope)
    assert conflict_envelope.model_dump_json() == before_envelope
    assert other.model_dump_json() == before_signal


@pytest.mark.asyncio
async def test_immutability_bound_rejection() -> None:
    sig = _sample_signal(
        source_event_ids=[_det_uuid(f"src-{i}") for i in range(MAX_SIGNAL_REFS + 1)]
    )
    envelope = _sample_envelope(sig)
    before_envelope = envelope.model_dump_json()
    before_signal = sig.model_dump_json()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    with pytest.raises(AnomalyEvidenceLimitError):
        await processor.process(envelope)
    assert envelope.model_dump_json() == before_envelope
    assert sig.model_dump_json() == before_signal


@pytest.mark.asyncio
async def test_immutability_downstream_failure() -> None:
    envelope = _sample_envelope()
    before_envelope = envelope.model_dump_json()
    sig = anomaly_signal_from_envelope(envelope)
    before_signal = sig.model_dump_json()
    downstream = RecordingDownstream(failures=[RuntimeError("boom")])
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10), downstream=downstream
    )
    with pytest.raises(RuntimeError):
        await processor.process(envelope)
    assert envelope.model_dump_json() == before_envelope
    assert sig.model_dump_json() == before_signal


@pytest.mark.asyncio
async def test_immutability_store_failure() -> None:
    envelope = _sample_envelope()
    before_envelope = envelope.model_dump_json()

    class BoomStore:  # type: ignore[no-redef]
        async def reserve(self, signal_id: uuid.UUID, fingerprint: str) -> Any:
            raise AnomalyDedupeStoreError("boom")

        async def complete(self, signal_id: uuid.UUID) -> None:
            return None

        async def release(self, signal_id: uuid.UUID) -> None:
            return None

    processor = AnomalyIntakeProcessor(
        dedupe_store=BoomStore(),
        downstream=RecordingDownstream(),  # type: ignore[arg-type]
    )
    with pytest.raises(AnomalyDedupeStoreError):
        await processor.process(envelope)
    assert envelope.model_dump_json() == before_envelope


@pytest.mark.asyncio
async def test_immutability_quarantine_success_and_failure() -> None:
    record = _make_record(raw_value=b"BROKEN{{{")
    # Success path: envelope bytes captured via quarantine record.
    sink = ListQuarantineSink()
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_ok = _make_mock_kafka()
    consumer_ok = _make_started_consumer(processor, mock_ok, quarantine_sink=sink)
    await consumer_ok.start()
    try:
        outcome = await consumer_ok.process_record(record)
    finally:
        await consumer_ok.stop()
    assert isinstance(outcome, QuarantineRecord)
    raw_again = decode_bytes(outcome.raw_value_base64)
    assert raw_again == b"BROKEN{{{"
    # Failure path: quarantine write fails; original bytes still intact.
    failing_sink = FailingQuarantineSink(QuarantineWriteError("boom"))
    processor2 = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_fail = _make_mock_kafka()
    consumer_fail = _make_started_consumer(
        processor2, mock_fail, quarantine_sink=failing_sink
    )
    await consumer_fail.start()
    try:
        with pytest.raises(QuarantineWriteError):
            await consumer_fail.process_record(record)
    finally:
        await consumer_fail.stop()
    assert record.value == b"BROKEN{{{"
    mock_fail.commit.assert_not_called()


def test_public_surface_exports() -> None:
    import app.correlation as corr

    for name in (
        "AnomalyCorrelationInput",
        "AnomalyDedupeStore",
        "AnomalyDownstreamHandler",
        "AnomalyIntakeConsumer",
        "AnomalyIntakeDisposition",
        "AnomalyIntakeProcessor",
        "AnomalyIntakeResult",
        "DedupeReservationOutcome",
        "InMemoryAnomalyDedupeStore",
        "AnomalyDedupeCapacityError",
        "AnomalyDedupeStoreError",
        "AnomalyEvidenceLimitError",
        "AnomalyIdentityConflictError",
        "AnomalyIntakeError",
        "DEFAULT_INTAKE_CONSUMER_GROUP",
        "DEFAULT_INTAKE_DEDUPE_MAX_SIZE",
    ):
        assert name in corr.__all__
        assert hasattr(corr, name)
    assert issubclass(AnomalyDownstreamHandler, object)


# ---------------------------------------------------------------------------
# Section 9 — Mandatory real-Kafka tests (integration only)
# ---------------------------------------------------------------------------


def _integration_signal(suffix: str) -> AnomalySignal:
    return _sample_signal(
        signal_id=_det_uuid(f"it-{suffix}"),
        run_id=_det_uuid(f"it-run-{suffix}"),
        reproducibility_key=f"rep-key-it-{suffix}",
        trace_id=f"trace-it-{suffix}",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_kafka_accepted_then_duplicate() -> None:
    from app.anomaly.publisher import AnomalySignalPublisher
    from app.telemetry.transport.producer import KafkaTelemetryProducer

    group_id = "aegis-task-4-5-c1-accepted-duplicate-v1"
    signal = _integration_signal("c1-accept")
    signal_snapshot = signal.model_dump_json()

    producer = KafkaTelemetryProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id="test-45-accept-dup-producer",
        producer_version="v4.5-it",
    )
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=100)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    intake_consumer = AnomalyIntakeConsumer(
        processor=processor,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="latest",
    )
    await producer.start()
    await intake_consumer.start()
    # Wait for assignment, then seek to end so retained broker history cannot satisfy the test.
    deadline = asyncio.get_event_loop().time() + 30.0
    raw_consumer1 = intake_consumer._consumer  # type: ignore[attr-defined]
    assert raw_consumer1 is not None
    while not raw_consumer1.assignment() and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.3)
    assigned = list(raw_consumer1.assignment())
    assert (
        assigned
    ), f"consumer did not get assignment before publish, assignment={assigned}"
    await raw_consumer1.seek_to_end(*assigned)
    try:
        from app.anomaly.observability import ObservabilityCollector

        publisher = AnomalySignalPublisher(
            producer=producer, observability=ObservabilityCollector()
        )
        ack1 = await publisher.publish(signal)
        ack2 = await publisher.publish(signal)
        assert ack1.topic == KafkaTopic.ANOMALIES.value
        assert ack2.topic == KafkaTopic.ANOMALIES.value

        # Poll for both deliveries via consume_one
        deadline = asyncio.get_event_loop().time() + 60.0
        consumed = 0
        while consumed < 2 and asyncio.get_event_loop().time() < deadline:
            try:
                outcome = await asyncio.wait_for(
                    intake_consumer.consume_one(timeout_ms=2000), timeout=5.0
                )
                if outcome is not None:
                    consumed += 1
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(0.2)
        assert consumed == 2, f"expected exactly 2 consumed deliveries, got {consumed}"

        # Verify: first accepted, exact retry duplicate -> downstream ran once
        assert (
            len(downstream.calls) == 1
        ), f"expected 1 downstream call, got {len(downstream.calls)}"
        assert store.entry_count() == 1
        assert store.completed_count() == 1
        assert signal.model_dump_json() == signal_snapshot

        # Verify offset committed only after both consumed
        inner = intake_consumer._consumer  # type: ignore[attr-defined]
        assert inner is not None
        from aiokafka import TopicPartition as _TP2

        tp2 = _TP2(KafkaTopic.ANOMALIES.value, int(ack2.partition))
        committed_now = await inner.committed(tp2)
        assert (
            committed_now is not None and committed_now >= ack2.offset + 1
        ), f"committed offset {committed_now} before ack2 {ack2.offset}"

        # Verify offsets committed via separate consumer for historical record inspection
        from aiokafka import AIOKafkaConsumer, TopicPartition

        verify_consumer = AIOKafkaConsumer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="aegis-task-4-5-c1-verify-v1",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await verify_consumer.start()
        try:
            tp = TopicPartition(KafkaTopic.ANOMALIES.value, 0)
            verify_consumer.assign([tp])
            # Seek to earliest and count messages for this run's signal_id
            verify_consumer.seek(tp, 0)
            count = 0
            deadline = asyncio.get_event_loop().time() + 10.0
            while asyncio.get_event_loop().time() < deadline:
                records = await verify_consumer.getmany(timeout_ms=1000, max_records=10)
                for recs in records.values():
                    for r in recs:
                        if r.offset >= ack1.offset and r.offset <= ack2.offset:
                            count += 1
                if count >= 2:
                    break
            assert count >= 2
            # Verify committed offset for our group
            inner = intake_consumer._consumer  # type: ignore[attr-defined]
            assert inner is not None
            committed = await inner.committed(tp)
            assert committed is not None and committed >= ack2.offset + 1
        finally:
            await verify_consumer.stop()
    finally:
        await intake_consumer.stop()
        await producer.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_kafka_conflicting_identity_quarantined() -> None:
    from app.anomaly.publisher import AnomalySignalPublisher
    from app.telemetry.transport.producer import KafkaTelemetryProducer

    group_id = "aegis-task-4-5-c1-conflicting-reuse-v1"
    _c1_sig = _integration_signal("c1-conflict")
    _c1_other = AnomalySignal(
        signal_id=_c1_sig.signal_id,
        event_time=_c1_sig.event_time,
        tenant_id=_c1_sig.tenant_id,
        environment=_c1_sig.environment,
        service=_c1_sig.service,
        metric_or_feature=_c1_sig.metric_or_feature,
        model_name="autoencoder",
        model_version=_c1_sig.model_version,
        anomaly_score=_c1_sig.anomaly_score,
        severity=_c1_sig.severity,
        evidence=[
            AnomalyEvidence(
                evidence_id=_det_uuid("c1-conflict-ev"),
                evidence_type="autoencoder_reconstruction_error",
                metric_or_feature=_c1_sig.metric_or_feature,
                observed_value=999.0,
            )
        ],
        threshold_or_calibration=_c1_sig.threshold_or_calibration,
        run_id=_c1_sig.run_id,
        scenario_id=_c1_sig.scenario_id,
        scenario_version=_c1_sig.scenario_version,
        seed=_c1_sig.seed,
        reproducibility_key=_c1_sig.reproducibility_key,
        source_event_ids=[_det_uuid("c1-conflict-src")],
        source_event_time_window=_c1_sig.source_event_time_window,
        trace_id=_c1_sig.trace_id,
        tags=dict(_c1_sig.tags),
    )
    signal = _c1_sig
    other = _c1_other
    assert signal.signal_id == other.signal_id, "conflict test requires same signal_id"
    assert compute_anomaly_semantic_fingerprint(
        other
    ) != compute_anomaly_semantic_fingerprint(signal)

    producer = KafkaTelemetryProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id="test-45-conflict-producer",
        producer_version="v4.5-it",
    )
    downstream = RecordingDownstream()
    store = InMemoryAnomalyDedupeStore(max_size=100)
    processor = AnomalyIntakeProcessor(dedupe_store=store, downstream=downstream)
    sink = ListQuarantineSink()
    intake_consumer = AnomalyIntakeConsumer(
        processor=processor,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="latest",
        quarantine_sink=sink,
    )
    await producer.start()
    await intake_consumer.start()
    raw_consumer2 = intake_consumer._consumer  # type: ignore[attr-defined]
    assert raw_consumer2 is not None
    deadline = asyncio.get_event_loop().time() + 30.0
    while not raw_consumer2.assignment() and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.3)
    assigned = list(raw_consumer2.assignment())
    assert (
        assigned
    ), f"consumer did not get assignment before publish, assignment={assigned}"
    await raw_consumer2.seek_to_end(*assigned)
    try:
        from app.anomaly.observability import ObservabilityCollector

        publisher = AnomalySignalPublisher(
            producer=producer, observability=ObservabilityCollector()
        )
        ack1 = await publisher.publish(signal)
        ack2 = await publisher.publish(other)
        assert ack1.topic == KafkaTopic.ANOMALIES.value
        assert ack2.topic == KafkaTopic.ANOMALIES.value

        # Wait for intake consumer to process both deliveries
        deadline = asyncio.get_event_loop().time() + 60.0
        while (
            len(downstream.calls) < 1 or len(sink.records) < 1
        ) and asyncio.get_event_loop().time() < deadline:
            try:
                await asyncio.wait_for(
                    intake_consumer.consume_one(timeout_ms=2000), timeout=5.0
                )
                # Outcomes are accumulated via sink/downstream; no direct assertion here
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(0.2)

        # First accepted, second quarantined as conflict
        assert (
            len(downstream.calls) == 1
        ), f"expected 1 downstream call, got {len(downstream.calls)}"
        assert (
            len(sink.records) == 1
        ), f"expected 1 quarantine record, got {len(sink.records)}"
        assert sink.records[0].failure_stage == "persistence"
        accepted_fp = compute_anomaly_semantic_fingerprint(signal)
        assert store._store[signal.signal_id][0] == accepted_fp
    finally:
        await intake_consumer.stop()
        await producer.close()


@pytest.mark.asyncio
async def test_file_quarantine_sink_round_trip(tmp_path: Path) -> None:
    sink = FileQuarantineSink(file_path=tmp_path / "q.jsonl")
    record = _make_record(offset=99)
    processor = AnomalyIntakeProcessor(
        dedupe_store=InMemoryAnomalyDedupeStore(max_size=10),
        downstream=RecordingDownstream(),
    )
    mock_aiokafka = _make_mock_kafka()
    intake_consumer = _make_started_consumer(
        processor, mock_aiokafka, quarantine_sink=sink
    )
    await intake_consumer.start()
    try:
        broken = _make_record(raw_value=b"BROKEN{{{", offset=99)
        outcome = await intake_consumer.process_record(broken)
    finally:
        await intake_consumer.stop()
    assert isinstance(outcome, QuarantineRecord)
    stored = await sink.read_records()
    assert len(stored) == 1
    assert stored[0].offset == 99
    assert record.offset == 99
