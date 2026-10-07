from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock
import uuid

from aiokafka.structs import ConsumerRecord
import pydantic
import pytest

import app.anomaly
from app.anomaly.errors import AnomalyHandoffError
from app.anomaly.handoff import anomaly_signal_from_envelope
from app.anomaly.models import (
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.serialization import (
    ANOMALY_EVENT_TYPE,
    ANOMALY_KAFKA_TOPIC,
    construct_anomaly_kafka_key,
    to_canonical_telemetry_event,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import TelemetryConsumer, TelemetryEnvelope
from app.telemetry.transport.errors import ConsumerHandlerError
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    serialize_event,
)


def _sample_calibration() -> CalibrationMetadata:
    return CalibrationMetadata(
        schema_version="1.0",
        method="quantile_calibration",
        threshold_value=0.85,
        calibration_version="v1.0.0",
        parameters={"alpha": 0.05, "contamination": 0.01},
        calibrated_at=datetime(2026, 9, 30, 10, 0, 0, tzinfo=timezone.utc),
    )


def _sample_signal() -> AnomalySignal:
    start_time = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 10, 1, 12, 5, 0, tzinfo=timezone.utc)
    source_id_1 = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    source_id_2 = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")

    evidence = [
        AnomalyEvidence(
            evidence_id=uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            evidence_type="residual_threshold_exceeded",
            metric_or_feature="http_request_latency_ms",
            observed_value=450.0,
            expected_value=120.0,
            deviation=330.0,
            details={"window_size_sec": 300},
            timestamp=end_time,
        )
    ]

    return AnomalySignal(
        signal_id=uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        event_time=datetime(2026, 10, 1, 12, 5, 0, tzinfo=timezone.utc),
        tenant_id=uuid.UUID("11111111-2222-3333-4444-555555555555"),
        environment="simulation",
        service="order-service",
        metric_or_feature="http_request_latency_ms",
        model_name="isolation_forest",
        model_version="1.0.0",
        anomaly_score=0.92,
        severity=EventSeverity.ERROR,
        evidence=evidence,
        threshold_or_calibration=_sample_calibration(),
        run_id=uuid.UUID("99999999-8888-7777-6666-555555555555"),
        scenario_id="latency-spike",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="rep-key-12345",
        source_event_ids=[source_id_1, source_id_2],
        source_event_time_window=EventTimeWindow(
            start_time=start_time,
            end_time=end_time,
        ),
        trace_id="trace-abc-123",
        tags={"region": "us-west-2", "tier": "critical"},
    )


def _sample_envelope(
    signal: AnomalySignal | None = None,
    topic: str = ANOMALY_KAFKA_TOPIC.value,
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
    envelope_key = key if key is not None else construct_anomaly_kafka_key(sig)
    return TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1790800000000,
        topic=topic,
        partition=0,
        offset=42,
        key=envelope_key,
        producer_version="v3.10-c",
    )


def test_valid_handoff() -> None:
    signal = _sample_signal()
    envelope = _sample_envelope(signal)

    envelope_snapshot = envelope.model_dump_json()
    signal_snapshot = signal.model_dump_json()

    extracted = anomaly_signal_from_envelope(envelope)

    assert extracted == signal
    assert extracted.signal_id == signal.signal_id
    assert extracted.event_time == signal.event_time
    assert extracted.tenant_id == signal.tenant_id
    assert extracted.environment == signal.environment
    assert extracted.service == signal.service
    assert extracted.model_name == signal.model_name
    assert extracted.model_version == signal.model_version
    assert extracted.anomaly_score == signal.anomaly_score
    assert extracted.severity == signal.severity
    assert extracted.evidence == signal.evidence
    assert extracted.threshold_or_calibration == signal.threshold_or_calibration
    assert extracted.run_id == signal.run_id
    assert extracted.scenario_id == signal.scenario_id
    assert extracted.scenario_version == signal.scenario_version
    assert extracted.seed == signal.seed
    assert extracted.reproducibility_key == signal.reproducibility_key
    assert extracted.source_event_ids == signal.source_event_ids
    assert extracted.source_event_time_window == signal.source_event_time_window
    assert extracted.trace_id == signal.trace_id
    assert extracted.tags == signal.tags

    # Verify immutability
    assert envelope.model_dump_json() == envelope_snapshot
    assert signal.model_dump_json() == signal_snapshot


def test_public_api_exports() -> None:
    assert hasattr(app.anomaly, "AnomalyHandoffError")
    assert hasattr(app.anomaly, "anomaly_signal_from_envelope")
    assert "AnomalyHandoffError" in app.anomaly.__all__
    assert "anomaly_signal_from_envelope" in app.anomaly.__all__

    # Verify no private helpers in __all__
    for export in app.anomaly.__all__:
        assert not export.startswith("_")


def test_runtime_type_boundary() -> None:
    with pytest.raises(TypeError, match="^Expected TelemetryEnvelope$"):
        anomaly_signal_from_envelope(None)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="^Expected TelemetryEnvelope$"):
        anomaly_signal_from_envelope({})  # type: ignore[arg-type]

    signal = _sample_signal()
    raw_event = to_canonical_telemetry_event(signal)
    with pytest.raises(TypeError, match="^Expected TelemetryEnvelope$"):
        anomaly_signal_from_envelope(raw_event)  # type: ignore[arg-type]


def test_route_boundary_wrong_topic() -> None:
    signal = _sample_signal()
    envelope = _sample_envelope(signal, topic="aegis.telemetry.metrics")

    with pytest.raises(AnomalyHandoffError, match="^Invalid anomaly handoff route$"):
        anomaly_signal_from_envelope(envelope)


def test_route_boundary_wrong_event_type() -> None:
    signal = _sample_signal()
    event = TelemetryEvent(
        schema_version="1.0",
        event_id=signal.signal_id,
        event_time=signal.event_time,
        tenant_id=signal.tenant_id,
        environment=signal.environment,
        service=signal.service,
        event_type=EventType.METRIC,
        severity=signal.severity,
        trace_id=signal.trace_id,
        payload=signal.model_dump(mode="json"),
    )
    envelope = _sample_envelope(signal, event_override=event)

    with pytest.raises(AnomalyHandoffError, match="^Invalid anomaly handoff route$"):
        anomaly_signal_from_envelope(envelope)


def test_invalid_payload_and_cause_semantics() -> None:
    signal = _sample_signal()
    malformed_payload = signal.model_dump(mode="json")
    malformed_payload["anomaly_score"] = 2.5  # Invalid score > 1.0

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=signal.signal_id,
        event_time=signal.event_time,
        tenant_id=signal.tenant_id,
        environment=signal.environment,
        service=signal.service,
        event_type=ANOMALY_EVENT_TYPE,
        severity=signal.severity,
        trace_id=signal.trace_id,
        payload=malformed_payload,
    )
    envelope = _sample_envelope(signal, event_override=event)

    with pytest.raises(AnomalyHandoffError) as exc_info:
        anomaly_signal_from_envelope(envelope)

    err = exc_info.value
    assert str(err) == "Invalid anomaly handoff payload"
    assert isinstance(err.__cause__, pydantic.ValidationError)
    assert err.__suppress_context__ is True


@pytest.mark.parametrize(
    ("field", "modified_value"),
    [
        ("event_id", uuid.UUID("99999999-9999-4999-8999-999999999999")),
        ("event_time", datetime(2026, 10, 1, 13, 0, 0, tzinfo=timezone.utc)),
        ("tenant_id", uuid.UUID("22222222-2222-3222-4222-222222222222")),
        ("environment", "production"),
        ("service", "payment-service"),
        ("severity", EventSeverity.CRITICAL),
        ("trace_id", "trace-diff-999"),
    ],
)
def test_envelope_coherence_mismatch(field: str, modified_value: object) -> None:
    signal = _sample_signal()
    event_dict = to_canonical_telemetry_event(signal).model_dump()
    event_dict[field] = modified_value
    event = TelemetryEvent.model_validate(event_dict)
    envelope = _sample_envelope(signal, event_override=event)

    with pytest.raises(
        AnomalyHandoffError,
        match="^Anomaly handoff envelope does not match payload$",
    ):
        anomaly_signal_from_envelope(envelope)


@pytest.mark.parametrize(
    ("field", "modified_value"),
    [
        ("run_id", uuid.UUID("88888888-8888-4888-8888-888888888888")),
        ("scenario_id", "memory-exhaustion"),
        ("scenario_version", "2.0.0"),
        ("reproducibility_key", "rep-diff-key"),
        ("seed", 9999),
    ],
)
def test_context_coherence_mismatch(field: str, modified_value: object) -> None:
    signal = _sample_signal()
    context_dict: dict[str, Any] = {
        "run_id": signal.run_id,
        "scenario_id": signal.scenario_id,
        "scenario_version": signal.scenario_version,
        "reproducibility_key": signal.reproducibility_key,
        "seed": signal.seed,
    }
    context_dict[field] = modified_value
    context = TelemetryExecutionContext(**context_dict)
    envelope = _sample_envelope(signal, context_override=context)

    with pytest.raises(
        AnomalyHandoffError,
        match="^Anomaly handoff context does not match payload$",
    ):
        anomaly_signal_from_envelope(envelope)


def test_key_coherence_mismatch_none() -> None:
    signal = _sample_signal()
    envelope = _sample_envelope(signal, key=b"")
    envelope_no_key = TelemetryEnvelope(
        event=envelope.event,
        context=envelope.context,
        kafka_timestamp_ms=envelope.kafka_timestamp_ms,
        topic=envelope.topic,
        partition=envelope.partition,
        offset=envelope.offset,
        key=None,
        producer_version=envelope.producer_version,
    )

    with pytest.raises(
        AnomalyHandoffError, match="^Anomaly handoff key does not match payload$"
    ):
        anomaly_signal_from_envelope(envelope_no_key)


def test_key_coherence_mismatch_wrong_key() -> None:
    signal = _sample_signal()
    envelope = _sample_envelope(signal, key=b"wrong-partition-key")

    with pytest.raises(
        AnomalyHandoffError, match="^Anomaly handoff key does not match payload$"
    ):
        anomaly_signal_from_envelope(envelope)


@pytest.mark.asyncio
async def test_existing_generic_consumer_handoff() -> None:
    signal = _sample_signal()
    event = to_canonical_telemetry_event(signal)
    context = TelemetryExecutionContext(
        run_id=signal.run_id,
        scenario_id=signal.scenario_id,
        scenario_version=signal.scenario_version,
        reproducibility_key=signal.reproducibility_key,
        seed=signal.seed,
    )

    raw_value = serialize_event(event)
    raw_key = construct_anomaly_kafka_key(signal)
    raw_headers = construct_kafka_headers(context, producer_version="v3.10-c")

    consumer_record = ConsumerRecord(
        topic=KafkaTopic.ANOMALIES.value,
        partition=0,
        offset=100,
        timestamp=1790800000000,
        timestamp_type=0,
        key=raw_key,
        value=raw_value,
        checksum=None,
        serialized_key_size=len(raw_key),
        serialized_value_size=len(raw_value),
        headers=raw_headers,
    )

    mock_aiokafka_consumer = MagicMock()
    mock_aiokafka_consumer.start = AsyncMock()
    mock_aiokafka_consumer.stop = AsyncMock()
    mock_aiokafka_consumer.commit = AsyncMock()

    handled_signal: AnomalySignal | None = None
    handled_envelope: TelemetryEnvelope | None = None
    handler_invocations = 0

    async def anomaly_handler(envelope: TelemetryEnvelope) -> None:
        nonlocal handled_signal, handled_envelope, handler_invocations
        handler_invocations += 1
        handled_envelope = envelope
        handled_signal = anomaly_signal_from_envelope(envelope)

    consumer = TelemetryConsumer(
        topics=[KafkaTopic.ANOMALIES.value],
        allowed_event_types={EventType.ANOMALY},
        allowed_topics={KafkaTopic.ANOMALIES.value},
        handler=anomaly_handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka_consumer,
    )

    await consumer.start()
    try:
        await consumer.process_record(consumer_record)

        # Prove handler was invoked exactly once
        assert handler_invocations == 1

        assert handled_envelope is not None
        assert handled_envelope.topic == KafkaTopic.ANOMALIES.value
        assert handled_envelope.partition == 0
        assert handled_envelope.offset == 100
        assert handled_envelope.kafka_timestamp_ms == 1790800000000
        assert handled_envelope.key == raw_key
        assert handled_envelope.producer_version == "v3.10-c"

        assert handled_signal is not None
        assert handled_signal == signal

        # Prove commit of offset + 1 after successful handling
        mock_aiokafka_consumer.commit.assert_awaited_once()
        commit_arg = mock_aiokafka_consumer.commit.call_args.args[0]
        tp = list(commit_arg.keys())[0]
        assert tp.topic == KafkaTopic.ANOMALIES.value
        assert tp.partition == 0
        assert commit_arg[tp] == 101

    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_failure_before_commit_on_incoherent_handoff() -> None:
    signal = _sample_signal()
    event = to_canonical_telemetry_event(signal)
    # Context run_id differs from event payload run_id
    context = TelemetryExecutionContext(
        run_id=uuid.UUID("77777777-7777-4777-8777-777777777777"),
        scenario_id=signal.scenario_id,
        scenario_version=signal.scenario_version,
        reproducibility_key=signal.reproducibility_key,
        seed=signal.seed,
    )

    raw_value = serialize_event(event)
    raw_key = construct_anomaly_kafka_key(signal)
    raw_headers = construct_kafka_headers(context, producer_version="v3.10-c")

    consumer_record = ConsumerRecord(
        topic=KafkaTopic.ANOMALIES.value,
        partition=0,
        offset=100,
        timestamp=1790800000000,
        timestamp_type=0,
        key=raw_key,
        value=raw_value,
        checksum=None,
        serialized_key_size=len(raw_key),
        serialized_value_size=len(raw_value),
        headers=raw_headers,
    )

    mock_aiokafka_consumer = MagicMock()
    mock_aiokafka_consumer.start = AsyncMock()
    mock_aiokafka_consumer.stop = AsyncMock()
    mock_aiokafka_consumer.commit = AsyncMock()

    handler_invocations = 0

    async def anomaly_handler(envelope: TelemetryEnvelope) -> None:
        nonlocal handler_invocations
        handler_invocations += 1
        anomaly_signal_from_envelope(envelope)

    consumer = TelemetryConsumer(
        topics=[KafkaTopic.ANOMALIES.value],
        allowed_event_types={EventType.ANOMALY},
        allowed_topics={KafkaTopic.ANOMALIES.value},
        handler=anomaly_handler,
        consumer_factory=lambda *args, **kwargs: mock_aiokafka_consumer,
    )

    await consumer.start()
    try:
        with pytest.raises(ConsumerHandlerError) as exc_info:
            await consumer.process_record(consumer_record)

        # Prove handler was invoked exactly once
        assert handler_invocations == 1

        assert isinstance(exc_info.value.__cause__, AnomalyHandoffError)
        assert (
            str(exc_info.value.__cause__)
            == "Anomaly handoff context does not match payload"
        )

        # Prove NO commit occurred on failure
        mock_aiokafka_consumer.commit.assert_not_awaited()

    finally:
        await consumer.stop()
