from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest

from app.anomaly.errors import AnomalyPublicationError
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
from app.anomaly.publisher import AnomalySignalPublisher
from app.anomaly.observability import (
    FailureCategory,
    ObservabilityCollector,
    OperationalStage,
    OperationStatus,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic
from app.telemetry.transport.errors import ProducerError, ProducerNotStartedError
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)

SIGNAL_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
EVIDENCE_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
SOURCE_EVENT_ID_1 = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
SOURCE_EVENT_ID_2 = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
FIXED_EVENT_TIME = datetime(2026, 10, 1, 12, 5, tzinfo=timezone.utc)


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
    end_time = FIXED_EVENT_TIME

    evidence = [
        AnomalyEvidence(
            evidence_id=EVIDENCE_ID,
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
        signal_id=SIGNAL_ID,
        event_time=FIXED_EVENT_TIME,
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
        source_event_ids=[SOURCE_EVENT_ID_1, SOURCE_EVENT_ID_2],
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

    collector = ObservabilityCollector()
    publisher = AnomalySignalPublisher(
        producer=mock_producer,
        observability=collector,
    )
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

    operations = collector.snapshot_operations()
    assert len(operations) == 1
    operation = operations[0]
    assert operation.stage is OperationalStage.PUBLICATION
    assert operation.status is OperationStatus.SUCCESS
    assert operation.latency_ms >= 0.0
    assert operation.context is not None
    assert operation.context.run_id == signal.run_id
    assert operation.context.scenario_id == signal.scenario_id
    assert operation.context.scenario_version == signal.scenario_version
    assert operation.context.seed == signal.seed
    assert operation.context.reproducibility_key == signal.reproducibility_key
    assert operation.context.model_name == signal.model_name
    assert operation.context.model_version == signal.model_version
    assert operation.context.signal_id == signal.signal_id
    assert operation.context.semantic_fingerprint == (
        compute_anomaly_semantic_fingerprint(signal)
    )
    assert collector.snapshot_failures() == ()


def test_publisher_observability_defaults_and_injection() -> None:
    producer = MagicMock(spec=KafkaTelemetryProducer)
    default_publisher = AnomalySignalPublisher(producer=producer)
    collector = ObservabilityCollector()
    injected_publisher = AnomalySignalPublisher(
        producer=producer,
        observability=collector,
    )

    assert isinstance(default_publisher.observability, ObservabilityCollector)
    assert default_publisher.observability is not collector
    assert injected_publisher.observability is collector


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

    collector = ObservabilityCollector()
    publisher = AnomalySignalPublisher(
        producer=mock_producer,
        observability=collector,
    )

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

    operations = collector.snapshot_operations()
    assert len(operations) == 1
    assert operations[0].stage is OperationalStage.PUBLICATION
    assert operations[0].status is OperationStatus.FAILURE
    failures = collector.snapshot_failures()
    assert len(failures) == 1
    assert failures[0].category is FailureCategory.PUBLICATION_FAILURE
    assert failures[0].failure_type == "ProducerError"
    assert failures[0].context is not None
    assert failures[0].context.signal_id == signal.signal_id
    assert failures[0].context.run_id == signal.run_id
    assert failures[0].context.semantic_fingerprint == (
        compute_anomaly_semantic_fingerprint(signal)
    )


@pytest.mark.asyncio
async def test_failure_translation_producer_not_started_error() -> None:
    signal = _sample_signal()
    signal_before = signal.model_dump_json()
    original_exc = ProducerNotStartedError("Producer is not started")

    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(side_effect=original_exc)

    collector = ObservabilityCollector()
    publisher = AnomalySignalPublisher(
        producer=mock_producer,
        observability=collector,
    )

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
    assert err.__suppress_context__ is True

    operations = collector.snapshot_operations()
    assert len(operations) == 1
    assert operations[0].stage is OperationalStage.PUBLICATION
    assert operations[0].status is OperationStatus.FAILURE
    failures = collector.snapshot_failures()
    assert len(failures) == 1
    assert failures[0].category is FailureCategory.PUBLICATION_FAILURE
    assert failures[0].failure_type == "ProducerNotStartedError"
    assert failures[0].context is not None
    assert failures[0].context.signal_id == signal.signal_id
    assert failures[0].context.run_id == signal.run_id
    assert failures[0].context.semantic_fingerprint == (
        compute_anomaly_semantic_fingerprint(signal)
    )


@pytest.mark.asyncio
async def test_retry_same_signal_preserves_semantic_identity_without_hidden_attempt() -> (
    None
):
    signal = _sample_signal()
    signal_before = signal.model_dump_json()
    first_result = _sample_publish_result()
    second_result = first_result.model_copy(
        update={"offset": first_result.offset + 1, "timestamp_ms": 1790800000001}
    )
    producer = MagicMock(spec=KafkaTelemetryProducer)
    producer.publish = AsyncMock(side_effect=[first_result, second_result])
    collector = ObservabilityCollector()
    publisher = AnomalySignalPublisher(producer, collector)

    with patch(
        "app.anomaly.publisher.build_anomaly_reproducibility_lineage",
        wraps=build_anomaly_reproducibility_lineage,
    ) as lineage_builder:
        actual_first = await publisher.publish(signal)
        actual_second = await publisher.publish(signal)

    assert actual_first is first_result
    assert actual_second is second_result
    assert actual_first is not actual_second
    assert producer.publish.await_count == 2
    assert lineage_builder.call_count == 2
    first_call, second_call = producer.publish.await_args_list
    first_event, first_context = first_call.args
    second_event, second_context = second_call.args
    assert first_event == second_event
    assert first_context == second_context
    assert first_event.event_id == second_event.event_id == signal.signal_id
    assert serialize_event(first_event) == serialize_event(second_event)
    assert construct_kafka_key(first_event) == construct_kafka_key(second_event)
    assert construct_kafka_headers(first_context) == construct_kafka_headers(
        second_context
    )
    assert {name for name, _ in construct_kafka_headers(first_context)} == {
        "run_id",
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "seed",
    }
    operations = collector.snapshot_operations()
    assert len(operations) == 2
    expected_fingerprint = compute_anomaly_semantic_fingerprint(signal)
    assert all(record.status is OperationStatus.SUCCESS for record in operations)
    assert all(record.context is not None for record in operations)
    assert all(
        record.context.semantic_fingerprint == expected_fingerprint
        for record in operations
        if record.context is not None
    )
    assert collector.snapshot_failures() == ()
    assert signal.model_dump_json() == signal_before


@pytest.mark.asyncio
async def test_publisher_sink_failure_preserves_result_and_record() -> None:
    signal = _sample_signal()
    expected_result = _sample_publish_result()
    signal_before = signal.model_dump_json()
    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(return_value=expected_result)
    collector = ObservabilityCollector()
    collector._logger = MagicMock()  # type: ignore[assignment]
    collector._logger.info.side_effect = RuntimeError("logging sink unavailable")
    publisher = AnomalySignalPublisher(mock_producer, collector)

    result = await publisher.publish(signal)

    assert result is expected_result
    assert signal.model_dump_json() == signal_before
    mock_producer.publish.assert_awaited_once()
    operations = collector.snapshot_operations()
    assert len(operations) == 1
    assert operations[0].stage is OperationalStage.PUBLICATION
    assert operations[0].status is OperationStatus.SUCCESS
    assert collector.snapshot_failures() == ()


@pytest.mark.asyncio
async def test_publisher_sink_failure_preserves_business_exception_and_record() -> None:
    signal = _sample_signal()
    signal_before = signal.model_dump_json()
    original_exc = ProducerError("Kafka broker connection timeout")
    mock_producer = MagicMock(spec=KafkaTelemetryProducer)
    mock_producer.publish = AsyncMock(side_effect=original_exc)
    collector = ObservabilityCollector()
    collector._logger = MagicMock()  # type: ignore[assignment]
    collector._logger.info.side_effect = RuntimeError("logging sink unavailable")
    collector._logger.error.side_effect = RuntimeError("logging sink unavailable")
    publisher = AnomalySignalPublisher(mock_producer, collector)

    with pytest.raises(AnomalyPublicationError) as exc_info:
        await publisher.publish(signal)

    assert exc_info.value.cause is original_exc
    assert exc_info.value.__cause__ is original_exc
    assert signal.model_dump_json() == signal_before
    mock_producer.publish.assert_awaited_once()
    operations = collector.snapshot_operations()
    assert len(operations) == 1
    assert operations[0].status is OperationStatus.FAILURE
    failures = collector.snapshot_failures()
    assert len(failures) == 1
    assert failures[0].category is FailureCategory.PUBLICATION_FAILURE


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
