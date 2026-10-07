from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import uuid

import pytest

from app.anomaly.errors import AnomalyPublicationError
from app.anomaly.models import (
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.publisher import AnomalySignalPublisher
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic
from app.telemetry.transport.errors import ProducerError, ProducerNotStartedError
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_key,
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


def _sample_signal(
    model_name: str = "isolation_forest",
    model_version: str = "1.0.0",
) -> AnomalySignal:
    start_time = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 10, 1, 12, 5, 0, tzinfo=timezone.utc)
    source_id_1 = uuid.uuid4()
    source_id_2 = uuid.uuid4()

    evidence = [
        AnomalyEvidence(
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
        tenant_id=uuid.UUID("11111111-2222-3333-4444-555555555555"),
        environment="simulation",
        service="order-service",
        metric_or_feature="http_request_latency_ms",
        model_name=model_name,
        model_version=model_version,
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


def _sample_publish_result() -> PublishResult:
    return PublishResult(
        topic=KafkaTopic.ANOMALIES.value,
        partition=2,
        offset=105,
        timestamp_ms=1790800000000,
        latency_ms=1.45,
        serialized_bytes=1024,
    )


@pytest.mark.asyncio
async def test_successful_publication() -> None:
    signal = _sample_signal()
    expected_result = _sample_publish_result()

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=expected_result)

    publisher = AnomalySignalPublisher(producer=mock_producer)
    result = await publisher.publish(signal)

    assert result is expected_result
    mock_producer.publish.assert_awaited_once()

    call_args = mock_producer.publish.call_args
    passed_event: TelemetryEvent = call_args.args[0]
    passed_context: TelemetryExecutionContext = call_args.args[1]

    # Verify canonical TelemetryEvent envelope
    assert isinstance(passed_event, TelemetryEvent)
    assert passed_event.event_id == signal.signal_id
    assert passed_event.event_time == signal.event_time
    assert passed_event.tenant_id == signal.tenant_id
    assert passed_event.environment == signal.environment
    assert passed_event.service == signal.service
    assert passed_event.event_type == EventType.ANOMALY
    assert passed_event.severity == signal.severity
    assert passed_event.trace_id == signal.trace_id

    # Verify TelemetryExecutionContext
    assert isinstance(passed_context, TelemetryExecutionContext)
    assert passed_context.run_id == signal.run_id
    assert passed_context.scenario_id == signal.scenario_id
    assert passed_context.scenario_version == signal.scenario_version
    assert passed_context.reproducibility_key == signal.reproducibility_key
    assert passed_context.seed == signal.seed


@pytest.mark.asyncio
async def test_complete_payload_and_provenance_adaptation() -> None:
    signal = _sample_signal()
    expected_result = _sample_publish_result()

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=expected_result)

    publisher = AnomalySignalPublisher(producer=mock_producer)
    await publisher.publish(signal)

    passed_event: TelemetryEvent = mock_producer.publish.call_args.args[0]

    # Reconstruct AnomalySignal from the canonical event payload
    reconstructed = AnomalySignal.model_validate(passed_event.payload)

    assert reconstructed == signal
    assert reconstructed.signal_id == signal.signal_id
    assert reconstructed.run_id == signal.run_id
    assert reconstructed.scenario_id == signal.scenario_id
    assert reconstructed.scenario_version == signal.scenario_version
    assert reconstructed.seed == signal.seed
    assert reconstructed.reproducibility_key == signal.reproducibility_key
    assert reconstructed.source_event_ids == signal.source_event_ids
    assert reconstructed.source_event_time_window == signal.source_event_time_window
    assert reconstructed.evidence == signal.evidence
    assert reconstructed.threshold_or_calibration == signal.threshold_or_calibration
    assert reconstructed.trace_id == signal.trace_id
    assert reconstructed.tags == signal.tags


@pytest.mark.parametrize("model_name", ["prophet", "isolation_forest", "autoencoder"])
@pytest.mark.asyncio
async def test_model_independence(model_name: str) -> None:
    signal = _sample_signal(model_name=model_name, model_version="2.1.0")
    expected_result = _sample_publish_result()

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=expected_result)

    publisher = AnomalySignalPublisher(producer=mock_producer)
    result = await publisher.publish(signal)

    assert result is expected_result
    mock_producer.publish.assert_awaited_once()

    passed_event: TelemetryEvent = mock_producer.publish.call_args.args[0]
    assert passed_event.payload["model_name"] == model_name
    assert passed_event.payload["model_version"] == "2.1.0"
    assert passed_event.event_type == EventType.ANOMALY


@pytest.mark.asyncio
async def test_immutability_of_signal() -> None:
    signal = _sample_signal()
    signal_snapshot_json = signal.model_dump_json()

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=_sample_publish_result())

    publisher = AnomalySignalPublisher(producer=mock_producer)
    await publisher.publish(signal)

    # Prove signal is unchanged
    assert signal.model_dump_json() == signal_snapshot_json


@pytest.mark.asyncio
async def test_input_boundary_non_anomaly_signal_rejected() -> None:
    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock()

    publisher = AnomalySignalPublisher(producer=mock_producer)

    with pytest.raises(TypeError, match="Expected AnomalySignal"):
        await publisher.publish({"invalid": "payload"})  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="Expected AnomalySignal"):
        await publisher.publish(None)  # type: ignore[arg-type]

    mock_producer.publish.assert_not_called()


@pytest.mark.asyncio
async def test_failure_translation_producer_error() -> None:
    signal = _sample_signal()
    signal_before = signal.model_dump_json()
    original_exc = ProducerError("Kafka broker connection timeout")

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(side_effect=original_exc)

    publisher = AnomalySignalPublisher(producer=mock_producer)

    with pytest.raises(AnomalyPublicationError) as exc_info:
        await publisher.publish(signal)

    assert signal.model_dump_json() == signal_before
    mock_producer.publish.assert_awaited_once()

    err = exc_info.value
    # Static, bounded public message
    assert str(err) == "Failed to publish anomaly signal"
    assert "broker" not in str(err).lower()
    assert "timeout" not in str(err).lower()

    # Structured attributes preserved
    assert err.signal_id == signal.signal_id
    assert err.run_id == signal.run_id
    assert err.scenario_id == signal.scenario_id
    assert err.cause is original_exc

    # Exception chaining
    assert err.__cause__ is original_exc
    assert err.__suppress_context__ is True


@pytest.mark.asyncio
async def test_failure_translation_producer_not_started_error() -> None:
    signal = _sample_signal()
    signal_before = signal.model_dump_json()
    original_exc = ProducerNotStartedError("Producer is not started")

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(side_effect=original_exc)

    publisher = AnomalySignalPublisher(producer=mock_producer)

    with pytest.raises(AnomalyPublicationError) as exc_info:
        await publisher.publish(signal)

    assert signal.model_dump_json() == signal_before
    mock_producer.publish.assert_awaited_once()

    err = exc_info.value
    assert str(err) == "Failed to publish anomaly signal"
    assert err.signal_id == signal.signal_id
    assert err.run_id == signal.run_id
    assert err.scenario_id == signal.scenario_id
    assert err.cause is original_exc
    assert err.__cause__ is original_exc


@pytest.mark.asyncio
async def test_lifecycle_boundary_does_not_manage_producer_lifecycle() -> None:
    signal = _sample_signal()
    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=_sample_publish_result())
    mock_producer.start = AsyncMock()
    mock_producer.flush = AsyncMock()
    mock_producer.close = AsyncMock()

    publisher = AnomalySignalPublisher(producer=mock_producer)
    await publisher.publish(signal)

    mock_producer.start.assert_not_called()
    mock_producer.flush.assert_not_called()
    mock_producer.close.assert_not_called()


@pytest.mark.asyncio
async def test_existing_routing_and_key_compatibility() -> None:
    signal = _sample_signal()
    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=_sample_publish_result())

    publisher = AnomalySignalPublisher(producer=mock_producer)
    await publisher.publish(signal)

    passed_event: TelemetryEvent = mock_producer.publish.call_args.args[0]

    assert passed_event.event_type == EventType.ANOMALY
    assert EVENT_TYPE_TO_TOPIC[passed_event.event_type] == KafkaTopic.ANOMALIES
    assert EVENT_TYPE_TO_TOPIC[passed_event.event_type].value == "aegis.ml.anomalies"

    # Key construction matches tenant_id:environment:service
    expected_key = f"{signal.tenant_id}:{signal.environment}:{signal.service}".encode(
        "utf-8"
    )
    assert construct_kafka_key(passed_event) == expected_key
