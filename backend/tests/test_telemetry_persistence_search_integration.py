import asyncio
from datetime import datetime, timezone, timedelta
import subprocess
from typing import Any
import uuid

from aiokafka import TopicPartition
import httpx
import pytest

from app.core.config import settings
from app.telemetry.persistence.search import (
    CONCRETE_INDEX_V1,
    EVIDENCE_ALIAS,
    TEMPLATE_NAME,
    OpenSearchPersistenceAdapter,
    check_opensearch_health,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    EvidenceConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.errors import ConsumerHandlerError
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
)


@pytest.mark.asyncio
async def test_real_opensearch_bootstrap_and_mapping() -> None:
    """Verify OpenSearch bootstrap installs template, concrete index, alias, and is idempotent."""
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    adapter = OpenSearchPersistenceAdapter()

    # 1. Bootstrap
    await adapter.bootstrap()

    # Verify template, concrete index, and alias via HTTP API
    async with httpx.AsyncClient(timeout=5.0) as client:
        # Check template
        tpl_res = await client.get(f"{settings.OPENSEARCH_URL}/_index_template/{TEMPLATE_NAME}")
        assert tpl_res.status_code == 200
        tpl_data = tpl_res.json()
        assert len(tpl_data.get("index_templates", [])) > 0

        # Check concrete index
        idx_res = await client.get(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}")
        assert idx_res.status_code == 200
        idx_data = idx_res.json()
        assert CONCRETE_INDEX_V1 in idx_data

        # Check alias
        alias_res = await client.get(f"{settings.OPENSEARCH_URL}/_alias/{EVIDENCE_ALIAS}")
        assert alias_res.status_code == 200
        alias_data = alias_res.json()
        assert CONCRETE_INDEX_V1 in alias_data
        assert EVIDENCE_ALIAS in alias_data[CONCRETE_INDEX_V1]["aliases"]

        # Check payload dynamic mapping is false
        mapping = idx_data[CONCRETE_INDEX_V1]["mappings"]
        assert mapping["properties"]["payload"]["dynamic"] == "false" or mapping["properties"]["payload"]["dynamic"] is False
        assert mapping["properties"]["seed"]["type"] == "keyword"
        assert mapping["properties"]["event_time"]["type"] == "date"
        assert mapping["properties"]["ingested_at"]["type"] == "date"

    # 2. Repeated bootstrap should be idempotent and safe
    await adapter.bootstrap()
    await adapter.close()


@pytest.mark.asyncio
async def test_direct_real_log_persistence_and_unknown_payload() -> None:
    """Verify direct LOG event persistence, uint64 seed keyword, and payload.dynamic=false."""
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    adapter = OpenSearchPersistenceAdapter()
    await adapter.bootstrap()

    test_id = uuid.uuid4().hex[:8]
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    # Non-UTC timezone-aware event_time (+02:00)
    tz_offset = timezone(timedelta(hours=2))
    event_time = datetime(2026, 9, 26, 18, 30, 0, 0, tzinfo=tz_offset)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=event_time,
        tenant_id=tenant_id,
        environment="simulation",
        service="payment-service",
        event_type=EventType.LOG,
        severity=EventSeverity.WARNING,
        trace_id=f"trace-{test_id}",
        payload={
            "message": "Payment timeout occurred",
            "status_code": 504,
            "latency_ms": 5002.5,
            "route": "/api/v1/charge",
            "unknown_research_field": {"arbitrary_key": "arbitrary_val", "nested": 999},
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
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=10,
        producer_version="step2.4",
    )

    res = await adapter.persist(envelope)
    assert res.status_code in (200, 201)
    assert res.result in ("created", "updated")
    assert res.document_id == f"{run_id}:{event_id}"

    # Verify retrieval from OpenSearch
    async with httpx.AsyncClient(timeout=5.0) as client:
        doc_res = await client.get(
            f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{res.document_id}"
        )
        assert doc_res.status_code == 200
        doc_body = doc_res.json()
        assert doc_body["found"] is True
        source = doc_body["_source"]

        # Validate canonical fields
        assert source["event_id"] == str(event_id)
        assert source["service"] == "payment-service"
        assert source["event_type"] == "log"
        assert source["severity"] == "warning"
        assert source["trace_id"] == f"trace-{test_id}"
        assert source["run_id"] == str(run_id)
        assert source["scenario_id"] == f"scenario-{test_id}"
        assert source["seed"] == "18446744073709551615"

        # Validate event_time instant preservation
        parsed_dt = datetime.fromisoformat(source["event_time"])
        assert parsed_dt.timestamp() == event_time.timestamp()

        # Validate ingested_at is UTC
        ingested_at = datetime.fromisoformat(source["ingested_at"])
        assert ingested_at.tzinfo is not None

        # Validate unknown payload field is preserved in _source
        assert "unknown_research_field" in source["payload"]
        assert source["payload"]["unknown_research_field"] == {"arbitrary_key": "arbitrary_val", "nested": 999}

        # Validate mapping was NOT dynamically polluted with unknown_research_field
        map_res = await client.get(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_mapping")
        assert map_res.status_code == 200
        map_body = map_res.json()
        payload_props = map_body[CONCRETE_INDEX_V1]["mappings"]["properties"]["payload"]["properties"]
        assert "unknown_research_field" not in payload_props
        assert "message" in payload_props

    await adapter.close()


@pytest.mark.asyncio
async def test_direct_real_system_persistence() -> None:
    """Verify direct SYSTEM event persistence into OpenSearch."""
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    adapter = OpenSearchPersistenceAdapter()
    await adapter.bootstrap()

    test_id = uuid.uuid4().hex[:8]
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
        event_type=EventType.SYSTEM,
        severity=EventSeverity.CRITICAL,
        trace_id=f"trace-sys-{test_id}",
        payload={
            "marker": "fault_injected",
            "fault_id": str(uuid.uuid4()),
            "fault_type": "crash",
            "target_service_id": str(uuid.uuid4()),
            "deployment_version": "v1.2.3",
        },
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-sys-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-key-{test_id}",
        seed=42,
    )

    envelope = TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.SYSTEM_EVENTS.value,
        partition=0,
        offset=20,
        producer_version="step2.4",
    )

    res = await adapter.persist(envelope)
    assert res.status_code in (200, 201)
    assert res.document_id == f"{run_id}:{event_id}"

    # Verify retrieval
    async with httpx.AsyncClient(timeout=5.0) as client:
        doc_res = await client.get(
            f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{res.document_id}"
        )
        assert doc_res.status_code == 200
        source = doc_res.json()["_source"]
        assert source["event_type"] == "system"
        assert source["payload"]["marker"] == "fault_injected"
        assert source["payload"]["fault_type"] == "crash"
        assert source["payload"]["deployment_version"] == "v1.2.3"

    await adapter.close()


@pytest.mark.asyncio
async def test_real_idempotency_and_overwrite() -> None:
    """Verify physical document idempotency and overwrite semantics."""
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    adapter = OpenSearchPersistenceAdapter()
    await adapter.bootstrap()

    test_id = uuid.uuid4().hex[:8]
    event_id = uuid.uuid4()
    run_id1 = uuid.uuid4()
    run_id2 = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="order-service",
        event_type=EventType.LOG,
        severity=EventSeverity.INFO,
        trace_id=f"trace-{test_id}",
        payload={"message": "Initial message", "status_code": 200},
    )

    context1 = TelemetryExecutionContext(
        run_id=run_id1,
        scenario_id=f"scenario-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-{test_id}",
        seed=100,
    )

    env1 = TelemetryEnvelope(
        event=event,
        context=context1,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=1,
    )

    # First write -> 201 created
    res1 = await adapter.persist(env1)
    assert res1.document_id == f"{run_id1}:{event_id}"

    # Second write with same run_id & event_id (updated payload) -> 200 updated
    event.payload["message"] = "Updated message"
    res2 = await adapter.persist(env1)
    assert res2.document_id == f"{run_id1}:{event_id}"
    assert res2.result in ("updated", "created")

    # Verify single document exists for id1 with updated payload
    async with httpx.AsyncClient(timeout=5.0) as client:
        doc1_res = await client.get(
            f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{res1.document_id}"
        )
        assert doc1_res.status_code == 200
        assert doc1_res.json()["_source"]["payload"]["message"] == "Updated message"

    # Write same event_id under different run_id2 -> creates separate document
    context2 = TelemetryExecutionContext(
        run_id=run_id2,
        scenario_id=f"scenario-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-{test_id}",
        seed=100,
    )
    env2 = TelemetryEnvelope(
        event=event,
        context=context2,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=2,
    )
    res3 = await adapter.persist(env2)
    assert res3.document_id == f"{run_id2}:{event_id}"
    assert res3.document_id != res1.document_id

    async with httpx.AsyncClient(timeout=5.0) as client:
        doc2_res = await client.get(
            f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{res3.document_id}"
        )
        assert doc2_res.status_code == 200
        assert doc2_res.json()["found"] is True

    await adapter.close()


@pytest.mark.asyncio
async def test_real_kafka_to_evidence_consumer_to_opensearch() -> None:
    """End-to-end integration test: Kafka -> EvidenceConsumer -> OpenSearch."""
    bootstrap_servers = "localhost:9092"
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step27-it-evidence-{test_id}"

    producer = KafkaTelemetryProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=f"producer-evidence-{test_id}",
        producer_version="step2.4",
    )

    try:
        await producer.start()
    except Exception as exc:
        pytest.skip(f"Kafka broker not reachable on {bootstrap_servers}: {exc}")

    try:
        adapter = OpenSearchPersistenceAdapter()
        await adapter.bootstrap()

        # 1. Publish LOG event
        log_event_id = uuid.uuid4()
        log_run_id = uuid.uuid4()
        log_event = TelemetryEvent(
            schema_version="1.0",
            event_id=log_event_id,
            event_time=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="order-service",
            event_type=EventType.LOG,
            severity=EventSeverity.INFO,
            trace_id=f"trace-log-{test_id}",
            payload={"message": "Order created successfully", "status_code": 201},
        )
        log_context = TelemetryExecutionContext(
            run_id=log_run_id,
            scenario_id=f"scenario-e2e-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rep-log-{test_id}",
            seed=111,
        )
        pub_log = await producer.publish(log_event, log_context)
        assert pub_log.topic == KafkaTopic.LOGS.value

        # 2. Publish SYSTEM event
        sys_event_id = uuid.uuid4()
        sys_run_id = uuid.uuid4()
        sys_event = TelemetryEvent(
            schema_version="1.0",
            event_id=sys_event_id,
            event_time=datetime(2026, 9, 26, 12, 5, 0, tzinfo=timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="notification-service",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.WARNING,
            trace_id=f"trace-sys-{test_id}",
            payload={"marker": "memory_threshold_exceeded"},
        )
        sys_context = TelemetryExecutionContext(
            run_id=sys_run_id,
            scenario_id=f"scenario-e2e-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rep-sys-{test_id}",
            seed=222,
        )
        pub_sys = await producer.publish(sys_event, sys_context)
        assert pub_sys.topic == KafkaTopic.SYSTEM_EVENTS.value

        consumer = EvidenceConsumer(
            handler=adapter,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-evidence-{test_id}",
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            consumed_log: TelemetryEnvelope | None = None
            consumed_sys: TelemetryEnvelope | None = None

            for _ in range(100):
                if consumed_log is not None and consumed_sys is not None:
                    break
                env = await consumer.consume_one(timeout_ms=1000)
                if env is not None:
                    if env.event.event_id == log_event_id:
                        consumed_log = env
                    elif env.event.event_id == sys_event_id:
                        consumed_sys = env

            assert consumed_log is not None, "Failed to consume LOG event"
            assert consumed_sys is not None, "Failed to consume SYSTEM event"

            tp_log = TopicPartition(pub_log.topic, pub_log.partition)
            committed_log = await consumer._consumer.committed(tp_log)  # type: ignore[union-attr]
            assert committed_log is not None
            assert committed_log >= pub_log.offset + 1

            tp_sys = TopicPartition(pub_sys.topic, pub_sys.partition)
            committed_sys = await consumer._consumer.committed(tp_sys)  # type: ignore[union-attr]
            assert committed_sys is not None
            assert committed_sys >= pub_sys.offset + 1

            # 3. Direct document retrieval from OpenSearch
            async with httpx.AsyncClient(timeout=5.0) as client:
                # LOG document
                log_doc_res = await client.get(
                    f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{log_run_id}:{log_event_id}"
                )
                assert log_doc_res.status_code == 200
                assert log_doc_res.json()["found"] is True

                # SYSTEM document
                sys_doc_res = await client.get(
                    f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{sys_run_id}:{sys_event_id}"
                )
                assert sys_doc_res.status_code == 200
                assert sys_doc_res.json()["found"] is True

        finally:
            await consumer.stop()
            await adapter.close()

    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_real_opensearch_persistence_failure_leaves_offset_uncommitted() -> None:
    """Verify that OpenSearch persistence failure leaves Kafka offset uncommitted."""
    bootstrap_servers = "localhost:9092"
    test_id = uuid.uuid4().hex[:8]
    group_id = f"aegis-step27-it-fail-{test_id}"

    event_id = uuid.uuid4()
    run_id = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="order-service",
        event_type=EventType.LOG,
        severity=EventSeverity.INFO,
        trace_id=f"trace-fail-{test_id}",
        payload={"message": "Failure test"},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-fail-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-fail-{test_id}",
        seed=999,
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
        pub_result = await producer.publish(event, context)

        # Injected adapter pointing to unreachable port
        failing_adapter = OpenSearchPersistenceAdapter(
            base_url="http://127.0.0.1:59998",
            timeout_seconds=0.5,
            max_attempts=2,
        )

        consumer = EvidenceConsumer(
            handler=failing_adapter,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=f"consumer-fail-{test_id}",
            auto_offset_reset="earliest",
        )

        await consumer.start()
        try:
            tp = TopicPartition(pub_result.topic, pub_result.partition)
            initial_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]

            with pytest.raises(ConsumerHandlerError, match="Downstream handler failed"):
                for _ in range(50):
                    await consumer.consume_one(timeout_ms=1000)

            after_failure_committed = await consumer._consumer.committed(tp)  # type: ignore[union-attr]
            if initial_committed is None:
                assert after_failure_committed is None or after_failure_committed <= pub_result.offset
            else:
                assert after_failure_committed == initial_committed

        finally:
            await consumer.stop()
            await failing_adapter.close()

    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_opensearch_persistence_across_container_restart() -> None:
    """Verify OpenSearch volume persistence across container restart."""
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    adapter = OpenSearchPersistenceAdapter()
    await adapter.bootstrap()

    test_id = uuid.uuid4().hex[:8]
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id,
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="order-service",
        event_type=EventType.LOG,
        severity=EventSeverity.INFO,
        trace_id=f"trace-restart-{test_id}",
        payload={"message": "Restart persistence verification"},
    )

    context = TelemetryExecutionContext(
        run_id=run_id,
        scenario_id=f"scenario-restart-{test_id}",
        scenario_version="1.0",
        reproducibility_key=f"rep-restart-{test_id}",
        seed=777,
    )

    envelope = TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=1,
    )

    res = await adapter.persist(envelope)
    doc_id = res.document_id
    await adapter.close()

    # Restart OpenSearch container preserving named volume
    subprocess.run(
        ["docker", "restart", "aegisops-opensearch"],
        check=True,
        capture_output=True,
    )

    # Wait for container to become healthy again
    healthy_after_restart = False
    for _ in range(30):
        await asyncio.sleep(2)
        h, status = await check_opensearch_health()
        if h and status in ("green", "yellow"):
            healthy_after_restart = True
            break

    assert healthy_after_restart is True, "OpenSearch failed to recover after restart"

    # Verify document and index exist after restart
    async with httpx.AsyncClient(timeout=5.0) as client:
        doc_res: httpx.Response | None = None
        for _ in range(10):
            fetch_res = await client.get(
                f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_doc/{doc_id}"
            )
            if fetch_res.status_code == 200:
                doc_res = fetch_res
                break
            await asyncio.sleep(1)

        assert doc_res is not None and doc_res.status_code == 200
        doc_data: dict[str, Any] = doc_res.json()
        assert doc_data["found"] is True

        alias_res = await client.get(f"{settings.OPENSEARCH_URL}/_alias/{EVIDENCE_ALIAS}")
        assert alias_res.status_code == 200
        alias_dict: dict[str, Any] = alias_res.json()
        assert CONCRETE_INDEX_V1 in alias_dict
