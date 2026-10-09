from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import math
import uuid

from aiokafka import AIOKafkaConsumer, TopicPartition
import pytest

from app.anomaly.models import (
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
    EventTimeWindow,
)
from app.anomaly.handoff import anomaly_signal_from_envelope
from app.anomaly.lineage import compute_anomaly_semantic_fingerprint
from app.anomaly.observability import (
    ObservabilityCollector,
    OperationalStage,
    OperationStatus,
)
from app.anomaly.publisher import AnomalySignalPublisher
from app.core.config import settings
from app.telemetry.schemas import EventSeverity, EventType
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport import TelemetryEnvelope, parse_kafka_headers
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.serialization import deserialize_event

pytestmark = pytest.mark.integration

SIGNAL_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
EVIDENCE_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
SOURCE_EVENT_ID_1 = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
SOURCE_EVENT_ID_2 = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
FIXED_EVENT_TIME = datetime(2026, 10, 1, 12, 5, 0, tzinfo=timezone.utc)


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
        source_event_ids=[SOURCE_EVENT_ID_1, SOURCE_EVENT_ID_2],
        source_event_time_window=EventTimeWindow(
            start_time=start_time,
            end_time=end_time,
        ),
        trace_id="trace-abc-123",
        tags={"region": "us-west-2", "tier": "critical"},
    )


@pytest.mark.asyncio
async def test_controlled_real_kafka_anomaly_publication() -> None:
    signal = _sample_signal()
    signal_snapshot_json = signal.model_dump_json()

    producer = KafkaTelemetryProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id="test-anomaly-integration-producer",
        producer_version="v3.10-b",
    )

    await producer.start()
    try:
        collector = ObservabilityCollector()
        publisher = AnomalySignalPublisher(
            producer=producer,
            observability=collector,
        )
        publish_result = await publisher.publish(signal)

        # Assert PublishResult evidence
        assert publish_result.topic == KafkaTopic.ANOMALIES.value
        assert publish_result.partition >= 0
        assert publish_result.offset >= 0
        assert publish_result.timestamp_ms is not None
        assert math.isfinite(publish_result.latency_ms)
        assert publish_result.latency_ms >= 0.0
        assert publish_result.serialized_bytes > 0

        # Consume and verify exact acknowledged record
        consumer = AIOKafkaConsumer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        tp = TopicPartition(publish_result.topic, publish_result.partition)
        await consumer.start()
        try:
            consumer.assign([tp])
            consumer.seek(tp, publish_result.offset)

            raw_msg = await asyncio.wait_for(consumer.getone(), timeout=10.0)

            # Assert broker identity
            assert raw_msg.topic == publish_result.topic
            assert raw_msg.partition == publish_result.partition
            assert raw_msg.offset == publish_result.offset
            assert raw_msg.timestamp == publish_result.timestamp_ms
            assert (
                raw_msg.timestamp_type == 0
            )  # 0 corresponds to CreateTime in Kafka wire protocol
            assert raw_msg.timestamp != int(signal.event_time.timestamp() * 1000)

            # Assert Kafka key
            expected_key = (
                f"{signal.tenant_id}:{signal.environment}:{signal.service}".encode(
                    "utf-8"
                )
            )
            assert raw_msg.key == expected_key

            # Assert required headers
            header_dict = dict(raw_msg.headers)
            assert set(header_dict) == {
                "run_id",
                "scenario_id",
                "scenario_version",
                "reproducibility_key",
                "seed",
                "producer_version",
            }
            assert header_dict["run_id"] == str(signal.run_id).encode("utf-8")
            assert header_dict["scenario_id"] == signal.scenario_id.encode("utf-8")
            assert header_dict["scenario_version"] == signal.scenario_version.encode(
                "utf-8"
            )
            assert header_dict[
                "reproducibility_key"
            ] == signal.reproducibility_key.encode("utf-8")
            assert header_dict["seed"] == str(signal.seed).encode("utf-8")
            assert header_dict["producer_version"] == b"v3.10-b"

            # Deserialize canonical envelope
            event = deserialize_event(raw_msg.value)
            assert event.event_id == signal.signal_id
            assert event.event_time == signal.event_time
            assert event.tenant_id == signal.tenant_id
            assert event.environment == signal.environment
            assert event.service == signal.service
            assert event.event_type == EventType.ANOMALY
            assert event.severity == signal.severity
            assert event.trace_id == signal.trace_id

            # Reconstruct AnomalySignal from payload and assert equality
            reconstructed = AnomalySignal.model_validate(event.payload)
            assert reconstructed == signal
            assert compute_anomaly_semantic_fingerprint(reconstructed) == (
                compute_anomaly_semantic_fingerprint(signal)
            )
            assert reconstructed.signal_id == SIGNAL_ID
            assert reconstructed.event_time == FIXED_EVENT_TIME
            assert reconstructed.evidence[0].evidence_id == EVIDENCE_ID
            assert reconstructed.source_event_ids == [
                SOURCE_EVENT_ID_1,
                SOURCE_EVENT_ID_2,
            ]
            assert (
                reconstructed.source_event_time_window
                == signal.source_event_time_window
            )
            assert reconstructed.evidence == signal.evidence
            assert (
                reconstructed.threshold_or_calibration
                == signal.threshold_or_calibration
            )
            assert reconstructed.tags == signal.tags

            # Test anomaly_signal_from_envelope handoff closure contract
            context, producer_version = parse_kafka_headers(raw_msg.headers)
            envelope = TelemetryEnvelope(
                event=event,
                context=context,
                kafka_timestamp_ms=raw_msg.timestamp
                if raw_msg.timestamp is not None
                else 0,
                topic=raw_msg.topic,
                partition=raw_msg.partition,
                offset=raw_msg.offset,
                key=raw_msg.key,
                consumed_at=datetime.now(timezone.utc),
                producer_version=producer_version,
            )
            envelope_before = envelope.model_dump_json()
            handoff_signal = anomaly_signal_from_envelope(envelope)
            assert envelope.model_dump_json() == envelope_before
            assert handoff_signal == signal
            assert compute_anomaly_semantic_fingerprint(
                handoff_signal
            ) == compute_anomaly_semantic_fingerprint(signal)
            assert handoff_signal.signal_id == signal.signal_id
            assert handoff_signal.run_id == signal.run_id
            assert handoff_signal.scenario_id == signal.scenario_id
            assert handoff_signal.seed == signal.seed
            assert handoff_signal.reproducibility_key == signal.reproducibility_key
            assert handoff_signal.source_event_ids == signal.source_event_ids
            assert handoff_signal.evidence == signal.evidence
            assert (
                handoff_signal.threshold_or_calibration
                == signal.threshold_or_calibration
            )
            assert handoff_signal.tags == signal.tags
            assert (
                handoff_signal.source_event_time_window
                == signal.source_event_time_window
            )

            # Assert byte count equality
            assert publish_result.serialized_bytes == len(raw_msg.value)

        finally:
            await consumer.stop()

        # Assert original signal immutability
        assert signal.model_dump_json() == signal_snapshot_json

        # Assert real publication observability and complete available lineage.
        operations = collector.snapshot_operations()
        assert len(operations) == 1
        operation = operations[0]
        assert operation.stage is OperationalStage.PUBLICATION
        assert operation.status is OperationStatus.SUCCESS
        assert math.isfinite(operation.latency_ms)
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

    finally:
        await producer.close()
