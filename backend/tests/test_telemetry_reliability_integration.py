from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock, MagicMock
import uuid

import aiokafka
from aiokafka import TopicPartition
import httpx
import pytest

from app.core.config import settings
from app.main import app
from app.telemetry.persistence.metrics import (
    VictoriaMetricsPersistenceAdapter,
    check_victoriametrics_health,
)
from app.telemetry.query.metrics import MetricQueryAdapter
from app.telemetry.query.models import MetricQuery
from app.telemetry.reliability.errors import QuarantineWriteError
from app.telemetry.reliability.metrics import (
    InstrumentedMetricHandler,
    InstrumentedTelemetryPublisher,
    pipeline_metrics,
    sample_consumer_lag,
)
from app.telemetry.reliability.quarantine import (
    FileQuarantineSink,
    QuarantineRecord,
    decode_bytes,
    encode_bytes,
)
from app.telemetry.reliability.replay import (
    REPLAY_MARKER_HEADER,
    QuarantineReplayService,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    MetricsConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.serialization import (
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)


@pytest.mark.asyncio
async def test_real_quarantine_malformed_record() -> None:
    """Verify that a malformed raw Kafka record on an ACTIVE Phase 2 topic is quarantined and committed."""
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step29-it-quarantine-malformed-{test_id}"
    topic = KafkaTopic.METRICS.value  # Active Phase 2 topic

    # 1. Publish malformed raw bytes directly to Kafka
    raw_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()
    try:
        raw_poison_payload = b"MALFORMED_NON_JSON_{{{_POISON_RECORD_" + test_id.encode("utf-8")
        raw_headers = [("raw_h1", b"val1"), ("raw_h2", b"val2"), ("duplicate_h", b"d1"), ("duplicate_h", b"d2")]
        metadata = await raw_producer.send_and_wait(
            topic=topic,
            value=raw_poison_payload,
            key=b"poison-key",
            headers=raw_headers,
        )
    finally:
        await raw_producer.stop()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_q_path = Path(temp_dir) / "test_quarantine.jsonl"
        sink = FileQuarantineSink(file_path=temp_q_path)

        handler = AsyncMock()
        consumer = MetricsConsumer(
            handler=handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            quarantine_sink=sink,
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            tp = TopicPartition(topic, metadata.partition)
            outcome = None
            for _ in range(50):
                res = await consumer.consume_one(timeout_ms=1000)
                if isinstance(res, QuarantineRecord) and decode_bytes(res.raw_value_base64) == raw_poison_payload:
                    outcome = res
                    break

            assert outcome is not None, "Failed to consume and quarantine poison record"
            assert outcome.failure_stage == "deserialization"
            assert decode_bytes(outcome.raw_value_base64) == raw_poison_payload
            assert decode_bytes(outcome.raw_key_base64) == b"poison-key"
            assert outcome.offset == metadata.offset
            assert outcome.topic == topic

            # Verify ordered headers and duplicate headers preserved
            header_tuples = [(h.name, decode_bytes(h.value_base64)) for h in outcome.headers]
            assert ("duplicate_h", b"d1") in header_tuples
            assert ("duplicate_h", b"d2") in header_tuples

            # Handler was NOT invoked for the poison record
            poison_handler_calls = [
                call for call in handler.call_args_list
                if hasattr(call[0][0], "offset") and call[0][0].offset == metadata.offset
            ]
            assert len(poison_handler_calls) == 0

            # Offset committed only after quarantine success
            committed_offset = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            assert committed_offset is not None and committed_offset >= metadata.offset + 1

            # Verify quarantine file contains valid JSONL
            records = await sink.read_records()
            assert len(records) >= 1
            matching = [r for r in records if decode_bytes(r.raw_value_base64) == raw_poison_payload]
            assert len(matching) == 1

        finally:
            await consumer.stop()


@pytest.mark.asyncio
async def test_real_quarantine_persistence_failure() -> None:
    """Verify that a valid record failing downstream persistence is quarantined and its offset committed."""
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step29-it-quarantine-persistence-{test_id}"
    topic = KafkaTopic.METRICS.value

    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        tenant_id=tenant_id,
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": f"test_fail_{test_id}", "value": 42.0},
    )
    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"sc-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rk-{test_id}",
        seed=100,
    )

    producer = KafkaTelemetryProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        pub_result = await producer.publish(event, context)
    finally:
        await producer.close()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_q_path = Path(temp_dir) / "test_quarantine.jsonl"
        sink = FileQuarantineSink(file_path=temp_q_path)

        # Injected failing handler simulating persistence failure
        failing_handler = AsyncMock(side_effect=RuntimeError("VictoriaMetrics outage"))

        consumer = MetricsConsumer(
            handler=failing_handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            quarantine_sink=sink,
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            tp = TopicPartition(topic, pub_result.partition)
            outcome = None
            for _ in range(50):
                res = await consumer.consume_one(timeout_ms=1000)
                if isinstance(res, QuarantineRecord) and res.event_id == event_id:
                    outcome = res
                    break

            assert outcome is not None, "Failed to consume and quarantine failed record"
            assert outcome.failure_stage == "persistence"
            assert outcome.event_id == event_id
            assert outcome.run_id == run_id
            assert outcome.service == "order-service"
            assert outcome.event_type == "metric"
            assert "VictoriaMetrics outage" in outcome.failure_message_sanitized

            # Offset committed after quarantine success
            committed_offset = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            assert committed_offset is not None and committed_offset >= pub_result.offset + 1

            records = await sink.read_records()
            assert len(records) >= 1
            matching = [r for r in records if r.event_id == event_id]
            assert len(matching) == 1

        finally:
            await consumer.stop()


@pytest.mark.asyncio
async def test_real_quarantine_failure_leaves_offset_uncommitted() -> None:
    """Verify that when quarantine write fails, the Kafka offset is NOT committed."""
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step29-it-quarantine-fail-{test_id}"
    topic = KafkaTopic.METRICS.value

    raw_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()
    try:
        metadata = await raw_producer.send_and_wait(
            topic=topic,
            value=b"MALFORMED_CORRUPT_BYTES_" + test_id.encode("utf-8"),
        )
    finally:
        await raw_producer.stop()

    # Mock sink that fails writes
    mock_sink = MagicMock()
    mock_sink.quarantine = AsyncMock(side_effect=QuarantineWriteError("Storage disk I/O failure"))

    consumer = MetricsConsumer(
        handler=AsyncMock(),
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        quarantine_sink=mock_sink,
        auto_offset_reset="earliest",
    )

    await consumer.start()
    try:
        tp = TopicPartition(topic, metadata.partition)
        initial_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]

        with pytest.raises(QuarantineWriteError, match="Storage disk I/O failure"):
            for _ in range(50):
                await consumer.consume_one(timeout_ms=1000)

        # Verify offset was NOT committed
        after_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
        if initial_committed is None:
            assert after_committed is None or after_committed <= metadata.offset
        else:
            assert after_committed == initial_committed

    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_real_recoverable_replay() -> None:
    """Verify that a valid quarantined record is replayed to Kafka and persisted to storage."""
    is_vm_healthy = await check_victoriametrics_health()
    if not is_vm_healthy:
        pytest.skip(f"VictoriaMetrics not reachable at {settings.VICTORIAMETRICS_URL}")

    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step29-it-replay-success-{test_id}"
    topic = KafkaTopic.METRICS.value

    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    metric_name = f"replay_metric_{test_id}"
    now = datetime(2026, 9, 26, 17, 0, 0, tzinfo=timezone.utc)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=now,
        tenant_id=tenant_id,
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"metric_name": metric_name, "value": 99.5},
    )
    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"sc-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rk-{test_id}",
        seed=100,
    )

    raw_val = serialize_event(event)
    raw_key = construct_kafka_key(event)
    headers = construct_kafka_headers(context)

    # 1. Create a QuarantineRecord representing a recoverable event
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_q_path = Path(temp_dir) / "replay_quarantine.jsonl"
        sink = FileQuarantineSink(file_path=temp_q_path)

        header_list = [
            {"name": h[0], "value_base64": encode_bytes(h[1]) or ""}
            for h in headers
        ]

        q_record = QuarantineRecord(
            failure_stage="persistence",
            failure_type="TemporaryNetworkOutage",
            failure_message_sanitized="Simulated outage",
            topic=topic,
            partition=0,
            offset=1000,
            kafka_timestamp_ms=1770000000000,
            raw_key_base64=encode_bytes(raw_key),
            raw_value_base64=encode_bytes(raw_val) or "",
            headers=header_list,  # type: ignore[arg-type]
            consumer_group=group_id,
            event_id=event_id,
            run_id=run_id,
            service="order-service",
            event_type="metric",
        )
        await sink.quarantine(q_record)

        # 2. Replay record via QuarantineReplayService
        replay_service = QuarantineReplayService(sink=sink, bootstrap_servers=bootstrap_servers)
        replay_results = await replay_service.replay_from_file()

        assert len(replay_results) == 1
        rep_res = replay_results[0]
        assert rep_res.success is True
        assert rep_res.replay_topic == topic
        assert rep_res.replay_offset > 0

        # Verify source quarantine file is NOT deleted
        records_in_file = await sink.read_records()
        assert len(records_in_file) == 1

        # 3. Consume replayed record with real persistence adapter
        vm_adapter = VictoriaMetricsPersistenceAdapter()
        consumer = MetricsConsumer(
            handler=vm_adapter,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            quarantine_sink=sink,
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            outcome = None
            for _ in range(50):
                res = await consumer.consume_one(timeout_ms=1000)
                if isinstance(res, TelemetryEnvelope) and res.event.event_id == event_id:
                    outcome = res
                    break

            assert outcome is not None, "Failed to consume replayed record"
            assert outcome.event.event_id == event_id

            # Verify persisted in VictoriaMetrics and retrievable via Query adapter
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")

            query_adapter = MetricQueryAdapter()
            q_res = await query_adapter.query(
                MetricQuery(
                    metric=metric_name,
                    run_id=run_id,
                    end_time=now + timedelta(seconds=60),
                )
            )
            assert q_res.count == 1
            assert q_res.samples[0].event_id == event_id
            assert q_res.samples[0].value == 99.5
            await query_adapter.close()

        finally:
            await consumer.stop()
            await vm_adapter.close()
            await replay_service.close()


@pytest.mark.asyncio
async def test_real_poison_replay_quarantines_again_without_loop() -> None:
    """Verify that replaying a malformed poison record re-quarantines safely."""
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step29-it-poison-replay-{test_id}"
    topic = KafkaTopic.METRICS.value

    raw_poison = b"POISON_REPLAY_TEST_BYTES_" + test_id.encode("utf-8")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_q_path = Path(temp_dir) / "poison_quarantine.jsonl"
        sink = FileQuarantineSink(file_path=temp_q_path)

        q_record = QuarantineRecord(
            failure_stage="deserialization",
            failure_type="MalformedJSON",
            failure_message_sanitized="Malformed poison",
            topic=topic,
            partition=0,
            offset=500,
            kafka_timestamp_ms=1770000000000,
            raw_value_base64=encode_bytes(raw_poison) or "",
        )
        await sink.quarantine(q_record)

        # Replay poison record
        replay_service = QuarantineReplayService(sink=sink, bootstrap_servers=bootstrap_servers)
        replay_results = await replay_service.replay_from_file()
        assert len(replay_results) == 1
        assert replay_results[0].success is True
        rep_res = replay_results[0]
        assert rep_res.replay_topic == topic

        # Consume replayed poison record
        consumer = MetricsConsumer(
            handler=AsyncMock(),
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            quarantine_sink=sink,
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            outcome = None
            for _ in range(50):
                res = await consumer.consume_one(timeout_ms=1000)
                if isinstance(res, QuarantineRecord) and decode_bytes(res.raw_value_base64) == raw_poison:
                    outcome = res
                    break

            assert outcome is not None, "Failed to consume and re-quarantine replayed poison"

            # Re-quarantined with replay marker header present
            headers_dict = {h.name: h.value_base64 for h in outcome.headers}
            assert REPLAY_MARKER_HEADER in headers_dict

            # Check that 2 quarantine lines exist (original + re-quarantined)
            records_in_file = await sink.read_records()
            matching_poison = [r for r in records_in_file if decode_bytes(r.raw_value_base64) == raw_poison]
            assert len(matching_poison) == 2

        finally:
            await consumer.stop()
            await replay_service.close()


@pytest.mark.asyncio
async def test_real_telemetry_readiness_endpoints() -> None:
    """Verify live /ready/telemetry and core /ready endpoint contracts."""
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Core /ready is healthy
        core_res = await client.get("/ready")
        assert core_res.status_code == 200
        assert core_res.json() == {"status": "ready"}

        # 2. Telemetry /ready/telemetry is healthy
        t_res = await client.get("/ready/telemetry")
        assert t_res.status_code == 200
        t_data = t_res.json()
        assert t_data["status"] == "ready"
        assert t_data["components"]["kafka"]["ready"] is True
        assert t_data["components"]["victoriametrics"]["ready"] is True
        assert t_data["components"]["opensearch"]["ready"] is True
        assert t_data["components"]["metric_query"]["ready"] is True
        assert t_data["components"]["evidence_query"]["ready"] is True

        # 3. Telemetry metrics snapshot
        m_res = await client.get("/telemetry/metrics")
        assert m_res.status_code == 200
        m_data = m_res.json()
        assert "throughput_per_second" in m_data


@pytest.mark.asyncio
async def test_real_pipeline_observability_metrics() -> None:
    """Verify automatic pipeline operational metric recording through production instrumentation wrappers."""
    is_vm_healthy = await check_victoriametrics_health()
    if not is_vm_healthy:
        pytest.skip(f"VictoriaMetrics not reachable at {settings.VICTORIAMETRICS_URL}")

    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step29-it-metrics-flow-{test_id}"
    topic = KafkaTopic.METRICS.value

    # Reset metrics before test
    pipeline_metrics.reset()

    # 1. Production-path instrumented publisher
    raw_producer = KafkaTelemetryProducer(bootstrap_servers=bootstrap_servers)
    publisher = InstrumentedTelemetryPublisher(raw_producer, metrics=pipeline_metrics)
    await publisher.start()

    events_to_publish = []
    for i in range(3):
        ev = TelemetryEvent(
            schema_version="1.0",
            event_id=uuid.uuid4(),
            event_time=datetime.now(timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            payload={"metric_name": f"auto_obs_{test_id}_{i}", "value": float(10 + i)},
        )
        ctx = TelemetryExecutionContext(
            run_id=uuid.uuid4(),
            scenario_id=f"sc-obs-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-obs-{test_id}",
            seed=100 + i,
        )
        events_to_publish.append((ev, ctx))

    # Publish 3 events through instrumented publisher (automatically records published_count & latencies)
    publish_results = []
    for ev, ctx in events_to_publish:
        res = await publisher.publish(ev, ctx)
        publish_results.append(res)

    await publisher.close()

    # 2. Production-path instrumented persistence handler & consumer with quarantine sink
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_q_path = Path(temp_dir) / "obs_quarantine.jsonl"
        sink = FileQuarantineSink(file_path=temp_q_path)

        vm_adapter = VictoriaMetricsPersistenceAdapter()
        instrumented_handler = InstrumentedMetricHandler(vm_adapter, metrics=pipeline_metrics)

        consumer = MetricsConsumer(
            handler=instrumented_handler,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            quarantine_sink=sink,
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            tp = TopicPartition(topic, 0)

            # Sample lag before consumption from Kafka offsets
            initial_lag = await sample_consumer_lag(consumer._consumer, tp, metrics=pipeline_metrics)  # type: ignore[union-attr]
            assert initial_lag >= 3

            # Process 2 events successfully through instrumented handler
            for i in range(2):
                outcome = await consumer.consume_one(timeout_ms=3000)
                assert isinstance(outcome, TelemetryEnvelope)

            # Introduce simulated persistence failure for 3rd event to trigger quarantine path
            instrumented_handler._adapter = MagicMock()
            instrumented_handler._adapter.persist = AsyncMock(side_effect=RuntimeError("Storage failure"))

            fail_outcome = await consumer.consume_one(timeout_ms=3000)
            assert isinstance(fail_outcome, QuarantineRecord)

            # Sample lag after consumption
            final_lag = await sample_consumer_lag(consumer._consumer, tp, metrics=pipeline_metrics)  # type: ignore[union-attr]
            assert final_lag <= initial_lag

            # Obtain snapshot WITHOUT any direct test calls to record_publish or record_persistence
            snap = pipeline_metrics.snapshot()

            assert snap.published_count == 3
            assert snap.persisted_count == 2
            assert snap.quarantined_count == 1
            assert snap.failed_count == 1
            assert snap.publish_latency_p50_ms > 0.0
            assert snap.persistence_latency_p50_ms > 0.0
            assert snap.e2e_latency_p50_ms > 0.0
            assert snap.throughput_per_second > 0.0

        finally:
            await consumer.stop()
            await vm_adapter.close()
