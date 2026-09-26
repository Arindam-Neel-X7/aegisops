from datetime import datetime, timezone, timedelta
import json
import uuid

from aiokafka import TopicPartition
import httpx
import pytest

from app.core.config import settings
from app.telemetry.persistence.metrics import (
    VictoriaMetricsPersistenceAdapter,
    check_victoriametrics_health,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    MetricsConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.errors import ConsumerHandlerError
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
)


@pytest.mark.asyncio
async def test_direct_victoriametrics_persistence_and_query_verification() -> None:
    """Verify direct metric persistence into real VictoriaMetrics (localhost:8428).

    Validates:
    - HTTP 200/204 ingestion success
    - Preservation of required labels
    - Preservation of full uint64 seed (18446744073709551615)
    - Preservation of exact domain event_time instant
    - Direct export and query verification (/api/v1/export)
    """
    vm_url = settings.VICTORIAMETRICS_URL
    is_healthy = await check_victoriametrics_health(base_url=vm_url)
    if not is_healthy:
        pytest.skip(f"VictoriaMetrics not reachable or unhealthy at {vm_url}")

    test_id = uuid.uuid4().hex[:8]
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    metric_name = f"http_request_latency_ms_{test_id}"
    metric_value = 245.75

    # Non-UTC timezone-aware datetime (+05:30) pointing to 14:00:00 UTC
    tz_offset = timezone(timedelta(hours=5, minutes=30))
    event_time = datetime(2026, 9, 26, 19, 30, 0, 0, tzinfo=tz_offset)
    expected_epoch_ms = int(event_time.timestamp() * 1000)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=event_time,
        tenant_id=tenant_id,
        environment="simulation",
        service="order-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=f"trace-{test_id}",
        payload={
            "metric_name": metric_name,
            "value": metric_value,
            "status_code": 200,
            "outcome": "success",
        },
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-key-{test_id}",
        seed=MAX_UINT64,
    )

    envelope = TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=1,
        producer_version="step2.4",
    )

    adapter = VictoriaMetricsPersistenceAdapter(base_url=vm_url)
    result = await adapter.persist(envelope)

    assert result.event_id == event_id
    assert result.run_id == run_id
    assert result.metric_name == metric_name
    assert result.timestamp_ms == expected_epoch_ms
    assert result.attempts == 1
    assert result.status_code in (200, 204)
    assert result.latency_ms > 0.0

    # Query back directly from VictoriaMetrics after force flush
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(f"{vm_url.rstrip('/')}/internal/force_flush")

        export_res = await client.get(
            f"{vm_url.rstrip('/')}/api/v1/export",
            params={"match[]": f'{{__name__="{metric_name}"}}'},
        )
        assert export_res.status_code == 200
        lines = [line.strip() for line in export_res.text.strip().split("\n") if line.strip()]
        assert len(lines) == 1, f"Expected 1 series exported, got {len(lines)}"

        exported_sample = json.loads(lines[0])
        metric_labels = exported_sample["metric"]

        # Validate all required labels
        assert metric_labels["__name__"] == metric_name
        assert metric_labels["service"] == "order-service"
        assert metric_labels["tenant_id"] == str(tenant_id)
        assert metric_labels["environment"] == "simulation"
        assert metric_labels["run_id"] == str(run_id)
        assert metric_labels["scenario_id"] == f"scenario-{test_id}"
        assert metric_labels["seed"] == "18446744073709551615"
        assert metric_labels["event_id"] == str(event_id)
        assert metric_labels["status_code"] == "200"
        assert metric_labels["outcome"] == "success"

        # Validate forbidden labels absent
        assert "trace_id" not in metric_labels
        assert "request_id" not in metric_labels
        assert "scenario_version" not in metric_labels
        assert "reproducibility_key" not in metric_labels

        # Validate values and timestamp
        assert exported_sample["values"] == [metric_value]
        assert exported_sample["timestamps"] == [expected_epoch_ms]

    await adapter.close()


@pytest.mark.asyncio
async def test_real_kafka_to_metrics_consumer_to_victoriametrics() -> None:
    """End-to-end integration test: Kafka -> MetricsConsumer -> VictoriaMetrics."""
    bootstrap_servers = "localhost:9092"
    vm_url = settings.VICTORIAMETRICS_URL

    is_healthy = await check_victoriametrics_health(base_url=vm_url)
    if not is_healthy:
        pytest.skip(f"VictoriaMetrics not reachable at {vm_url}")

    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step26-it-metrics-{test_id}"
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    metric_name = f"queue_depth_{test_id}"
    metric_value = 75.0
    event_time = datetime(2026, 9, 26, 14, 0, 0, tzinfo=timezone.utc)
    expected_epoch_ms = int(event_time.timestamp() * 1000)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=event_time,
        tenant_id=tenant_id,
        environment="simulation",
        service="inventory-service",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=f"trace-e2e-{test_id}",
        payload={"metric_name": metric_name, "value": metric_value},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-e2e-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-e2e-{test_id}",
        seed=555666777,
    )

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=f"producer-e2e-{test_id}",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        adapter = VictoriaMetricsPersistenceAdapter(base_url=vm_url)
        consumer = MetricsConsumer(
            handler=adapter,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-e2e-{test_id}",
            auto_offset_reset="latest",
        )

        await consumer.start()
        try:
            pub_result = await producer.publish(event, context)
            assert pub_result.topic == KafkaTopic.METRICS.value

            envelope = await consumer.consume_one(timeout_ms=5000)
            assert envelope is not None, "Failed to consume published metric event"
            assert envelope.event.event_id == event_id
            assert envelope.topic == KafkaTopic.METRICS.value
            assert envelope.offset == pub_result.offset

            # Verify committed Kafka offset is consumed offset + 1
            tp = TopicPartition(pub_result.topic, pub_result.partition)
            committed_offset = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            assert committed_offset is not None
            assert committed_offset == pub_result.offset + 1

            # Force flush and verify sample persisted in VictoriaMetrics
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.get(f"{vm_url.rstrip('/')}/internal/force_flush")
                res = await client.get(
                    f"{vm_url.rstrip('/')}/api/v1/export",
                    params={"match[]": f'{{__name__="{metric_name}"}}'},
                )
                assert res.status_code == 200
                lines = [line.strip() for line in res.text.strip().split("\n") if line.strip()]
                assert len(lines) == 1
                data = json.loads(lines[0])
                assert data["metric"]["__name__"] == metric_name
                assert data["values"] == [metric_value]
                assert data["timestamps"] == [expected_epoch_ms]

        finally:
            await consumer.stop()
            await adapter.close()

    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_real_persistence_failure_leaves_offset_uncommitted() -> None:
    """Verify that failure during VictoriaMetrics persistence prevents Kafka offset commit."""
    bootstrap_servers = "localhost:9092"
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step26-it-fail-{test_id}"

    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        tenant_id=tenant_id,
        environment="simulation",
        service="database",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=f"trace-fail-{test_id}",
        payload={"metric_name": f"fail_metric_{test_id}", "value": 999.0},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-fail-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-fail-{test_id}",
        seed=123,
    )

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=f"producer-fail-{test_id}",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        # Injected adapter pointing to an unreachable port to simulate storage outage
        failing_adapter = VictoriaMetricsPersistenceAdapter(
            base_url="http://127.0.0.1:59999",
            timeout_seconds=0.5,
            max_attempts=2,
        )

        consumer = MetricsConsumer(
            handler=failing_adapter,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-fail-{test_id}",
            auto_offset_reset="latest",
        )

        await consumer.start()
        try:
            pub_result = await producer.publish(event, context)
            tp = TopicPartition(pub_result.topic, pub_result.partition)
            initial_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]

            with pytest.raises(ConsumerHandlerError, match="Downstream handler failed"):
                await consumer.consume_one(timeout_ms=5000)

            # Check that failed record offset was NOT committed
            after_failure_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            assert after_failure_committed == initial_committed

        finally:
            await consumer.stop()
            await failing_adapter.close()

    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_query_visible_duplicate_suppression() -> None:
    """Verify query-visible duplicate suppression under -dedup.minScrapeInterval=1ms."""
    vm_url = settings.VICTORIAMETRICS_URL
    is_healthy = await check_victoriametrics_health(base_url=vm_url)
    if not is_healthy:
        pytest.skip(f"VictoriaMetrics not reachable at {vm_url}")

    test_id = uuid.uuid4().hex[:8]
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    metric_name = f"dedup_test_metric_{test_id}"
    metric_value = 88.0
    event_time = datetime(2026, 9, 26, 14, 0, 0, tzinfo=timezone.utc)
    expected_epoch_ms = int(event_time.timestamp() * 1000)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=event_time,
        tenant_id=tenant_id,
        environment="simulation",
        service="api-gateway",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id=f"trace-dedup-{test_id}",
        payload={"metric_name": metric_name, "value": metric_value},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-dedup-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-dedup-{test_id}",
        seed=42,
    )

    envelope = TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=100,
        producer_version="step2.4",
    )

    adapter = VictoriaMetricsPersistenceAdapter(base_url=vm_url)

    # Ingest the EXACT same sample twice (simulating at-least-once duplicate)
    res1 = await adapter.persist(envelope)
    res2 = await adapter.persist(envelope)
    assert res1.status_code in (200, 204)
    assert res2.status_code in (200, 204)

    # Force flush and verify query-visible duplicate suppression under -dedup.minScrapeInterval=1ms
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(f"{vm_url.rstrip('/')}/internal/force_flush")
        export_res = await client.get(
            f"{vm_url.rstrip('/')}/api/v1/export",
            params={"match[]": f'{{__name__="{metric_name}"}}'},
        )
        assert export_res.status_code == 200
        lines = [line.strip() for line in export_res.text.strip().split("\n") if line.strip()]
        assert len(lines) == 1, f"Expected exactly 1 series returned after dedup, got {len(lines)}"
        data = json.loads(lines[0])
        assert len(data["timestamps"]) == 1
        assert data["timestamps"] == [expected_epoch_ms]
        assert data["values"] == [metric_value]

    await adapter.close()
