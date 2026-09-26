from datetime import datetime, timedelta, timezone
import uuid

import httpx
import pytest

from app.core.config import settings
from app.telemetry.persistence.metrics import (
    VictoriaMetricsPersistenceAdapter,
    check_victoriametrics_health,
)
from app.telemetry.persistence.search import (
    CONCRETE_INDEX_V1,
    OpenSearchPersistenceAdapter,
    check_opensearch_health,
)
from app.telemetry.query.errors import (
    EvidenceQueryBackendError,
    MetricQueryBackendError,
)
from app.telemetry.query.evidence import EvidenceQueryAdapter
from app.telemetry.query.metrics import MetricQueryAdapter
from app.telemetry.query.models import (
    EvidenceQuery,
    MetricQuery,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import TelemetryEnvelope
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
)


@pytest.mark.asyncio
async def test_real_metric_query_integration() -> None:
    """Verify MetricQueryAdapter against real VictoriaMetrics (localhost:8428)."""
    is_healthy = await check_victoriametrics_health()
    if not is_healthy:
        pytest.skip(f"VictoriaMetrics not reachable at {settings.VICTORIAMETRICS_URL}")

    test_id = uuid.uuid4().hex[:8]
    metric_name = f"http_latency_ms_{test_id}"
    run_id1 = uuid.uuid4()
    run_id2 = uuid.uuid4()
    tenant_id = uuid.uuid4()
    event_id1 = uuid.uuid4()
    event_id2 = uuid.uuid4()
    event_id3 = uuid.uuid4()

    t0 = datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=10)
    t2 = t0 + timedelta(seconds=20)

    # 1. Persist 3 metric occurrences
    vm_adapter = VictoriaMetricsPersistenceAdapter()

    # Event 1: service="order-service", run_id=run_id1, seed=42, val=100.0, time=t0
    e1 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id1,
            event_time=t0,
            tenant_id=tenant_id,
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            payload={"metric_name": metric_name, "value": 100.0},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id1,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=42,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=1,
    )

    # Event 2: service="order-service", run_id=run_id1, seed=MAX_UINT64, val=150.0, time=t1
    e2 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id2,
            event_time=t1,
            tenant_id=tenant_id,
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            payload={"metric_name": metric_name, "value": 150.0},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id1,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=MAX_UINT64,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=2,
    )

    # Event 3: service="payment-service", run_id=run_id2, seed=99, val=200.0, time=t2
    e3 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id3,
            event_time=t2,
            tenant_id=tenant_id,
            environment="simulation",
            service="payment-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            payload={"metric_name": metric_name, "value": 200.0},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id2,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=99,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=3,
    )

    await vm_adapter.persist(e1)
    await vm_adapter.persist(e2)
    await vm_adapter.persist(e3)
    await vm_adapter.close()

    # Flush storage
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")

    query_adapter = MetricQueryAdapter()
    eval_time = t2 + timedelta(seconds=60)

    # Test Filter 1: metric + service (instant query returns 2 matching series)
    res_service = await query_adapter.query(
        MetricQuery(
            metric=metric_name,
            service="order-service",
            end_time=eval_time,
        )
    )
    assert res_service.count == 2
    assert {s.event_id for s in res_service.samples} == {event_id1, event_id2}
    assert res_service.samples[0].timestamp.timestamp() <= res_service.samples[1].timestamp.timestamp()

    # Test Filter 2: run_id
    res_run = await query_adapter.query(
        MetricQuery(
            metric=metric_name,
            run_id=run_id2,
            end_time=eval_time,
        )
    )
    assert res_run.count == 1
    assert res_run.samples[0].event_id == event_id3
    assert res_run.samples[0].service == "payment-service"
    assert res_run.samples[0].seed == 99

    # Test Filter 3: seed = MAX_UINT64
    res_seed = await query_adapter.query(
        MetricQuery(
            metric=metric_name,
            seed=MAX_UINT64,
            end_time=eval_time,
        )
    )
    assert res_seed.count == 1
    assert res_seed.samples[0].event_id == event_id2
    assert res_seed.samples[0].seed == MAX_UINT64

    # Test Filter 4: event_id
    res_eid = await query_adapter.query(
        MetricQuery(
            metric=metric_name,
            event_id=event_id1,
            end_time=eval_time,
        )
    )
    assert res_eid.count == 1
    assert res_eid.samples[0].event_id == event_id1
    assert res_eid.samples[0].value == 100.0

    # Test Range Query with step
    res_range = await query_adapter.query(
        MetricQuery(
            metric=metric_name,
            run_id=run_id1,
            start_time=t0,
            end_time=t1,
            step="10s",
        )
    )
    assert res_range.count >= 2
    assert {s.event_id for s in res_range.samples} == {event_id1, event_id2}

    # Test Filter 5: Non-matching filter returns empty result
    res_none = await query_adapter.query(
        MetricQuery(
            metric=metric_name,
            service="non-existent-service",
            end_time=eval_time,
        )
    )
    assert res_none.count == 0
    assert len(res_none.samples) == 0

    await query_adapter.close()


@pytest.mark.asyncio
async def test_real_evidence_query_integration() -> None:
    """Verify EvidenceQueryAdapter against real OpenSearch (localhost:9200)."""
    is_healthy, _ = await check_opensearch_health()
    if not is_healthy:
        pytest.skip(f"OpenSearch not reachable at {settings.OPENSEARCH_URL}")

    test_id = uuid.uuid4().hex[:8]
    run_id1 = uuid.uuid4()
    run_id2 = uuid.uuid4()
    tenant_id = uuid.uuid4()
    event_id1 = uuid.uuid4()
    event_id2 = uuid.uuid4()
    event_id3 = uuid.uuid4()
    event_id4 = uuid.uuid4()

    t0 = datetime(2026, 9, 26, 14, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=10)
    t2 = t0 + timedelta(seconds=20)
    t3 = t0 + timedelta(seconds=30)

    # Bootstrap and persist evidence records
    os_adapter = OpenSearchPersistenceAdapter()
    await os_adapter.bootstrap()

    # Record 1: LOG, service="order-service", severity=INFO, run_id=run_id1, trace="tr-1", seed=42, t0
    e1 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id1,
            event_time=t0,
            tenant_id=tenant_id,
            environment="simulation",
            service="order-service",
            event_type=EventType.LOG,
            severity=EventSeverity.INFO,
            trace_id="tr-1",
            payload={"message": "Order placed", "order_id": 101},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id1,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=42,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=1,
    )

    # Record 2: LOG, service="payment-service", severity=ERROR, run_id=run_id1, trace=None, seed=MAX_UINT64, t1
    e2 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id2,
            event_time=t1,
            tenant_id=tenant_id,
            environment="simulation",
            service="payment-service",
            event_type=EventType.LOG,
            severity=EventSeverity.ERROR,
            trace_id=None,
            payload={"message": "Payment gateway timeout", "status_code": 504},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id1,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=MAX_UINT64,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=2,
    )

    # Record 3: SYSTEM, service="database", severity=CRITICAL, run_id=run_id2, marker="crash", t2
    e3 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id3,
            event_time=t2,
            tenant_id=tenant_id,
            environment="simulation",
            service="database",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.CRITICAL,
            trace_id="tr-2",
            payload={"marker": "crash", "fault_type": "oom"},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id2,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=100,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.SYSTEM_EVENTS.value,
        partition=0,
        offset=3,
    )

    # Record 4: LOG, service="order-service", severity=INFO, run_id=run_id1, trace="tr-1", seed=42, t3
    e4 = TelemetryEnvelope(
        event=TelemetryEvent(
            schema_version="1.0",
            event_id=event_id4,
            event_time=t3,
            tenant_id=tenant_id,
            environment="simulation",
            service="order-service",
            event_type=EventType.LOG,
            severity=EventSeverity.INFO,
            trace_id="tr-1",
            payload={"message": "Order fulfilled", "order_id": 101},
        ),
        context=TelemetryExecutionContext(
            run_id=run_id1,
            scenario_id=f"sc-{test_id}",
            scenario_version="1.0",
            reproducibility_key=f"rk-{test_id}",
            seed=42,
        ),
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.LOGS.value,
        partition=0,
        offset=4,
    )

    await os_adapter.persist(e1)
    await os_adapter.persist(e2)
    await os_adapter.persist(e3)
    await os_adapter.persist(e4)
    await os_adapter.close()

    # Refresh OpenSearch index
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_refresh")

    query_adapter = EvidenceQueryAdapter()

    # Test 1: Query by run_id
    res_run = await query_adapter.query(EvidenceQuery(run_id=run_id1))
    assert res_run.count == 3
    assert [r.event_id for r in res_run.records] == [event_id1, event_id2, event_id4]
    # Check deterministic sort (event_time ASC)
    assert res_run.records[0].event_time.timestamp() < res_run.records[1].event_time.timestamp() < res_run.records[2].event_time.timestamp()

    # Test 2: Query by seed = MAX_UINT64
    res_seed = await query_adapter.query(EvidenceQuery(seed=MAX_UINT64))
    assert res_seed.count >= 1
    matching = [r for r in res_seed.records if r.event_id == event_id2]
    assert len(matching) == 1
    assert matching[0].seed == MAX_UINT64

    # Test 3: Query by event_type (SYSTEM)
    res_sys = await query_adapter.query(EvidenceQuery(run_id=run_id2, event_type=EventType.SYSTEM))
    assert res_sys.count == 1
    assert res_sys.records[0].event_id == event_id3
    assert res_sys.records[0].payload["marker"] == "crash"

    # Test 4: Query by marker
    res_marker = await query_adapter.query(EvidenceQuery(marker="crash"))
    assert res_marker.count >= 1
    assert any(r.event_id == event_id3 for r in res_marker.records)

    # Test 5: Query by severity (ERROR)
    res_sev = await query_adapter.query(EvidenceQuery(run_id=run_id1, severity=EventSeverity.ERROR))
    assert res_sev.count == 1
    assert res_sev.records[0].event_id == event_id2

    # Test 6: Query by trace_id
    res_trace = await query_adapter.query(EvidenceQuery(run_id=run_id1, trace_id="tr-1"))
    assert res_trace.count == 2
    assert [r.event_id for r in res_trace.records] == [event_id1, event_id4]

    # Test 7: Query with limit (limit=2 on run_id1 which has 3 records)
    res_limit = await query_adapter.query(EvidenceQuery(run_id=run_id1, limit=2))
    assert res_limit.count == 2
    assert [r.event_id for r in res_limit.records] == [event_id1, event_id2]

    # Test 8: Non-matching filter returns empty result
    res_empty = await query_adapter.query(EvidenceQuery(service="non-existent-service"))
    assert res_empty.count == 0
    assert len(res_empty.records) == 0

    await query_adapter.close()


@pytest.mark.asyncio
async def test_cross_store_identity_compatibility() -> None:
    """Demonstrate cross-store identity compatibility (shared run_id / event_id representation)."""
    is_vm_healthy = await check_victoriametrics_health()
    is_os_healthy, _ = await check_opensearch_health()
    if not is_vm_healthy or not is_os_healthy:
        pytest.skip("VictoriaMetrics or OpenSearch not reachable")

    test_id = uuid.uuid4().hex[:8]
    shared_run_id = uuid.uuid4()
    shared_event_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    now = datetime(2026, 9, 26, 15, 0, 0, tzinfo=timezone.utc)

    # Persist metric with shared identity
    vm_adapter = VictoriaMetricsPersistenceAdapter()
    await vm_adapter.persist(
        TelemetryEnvelope(
            event=TelemetryEvent(
                schema_version="1.0",
                event_id=shared_event_id,
                event_time=now,
                tenant_id=tenant_id,
                environment="simulation",
                service="order-service",
                event_type=EventType.METRIC,
                severity=EventSeverity.INFO,
                payload={"metric_name": f"cross_store_{test_id}", "value": 42.0},
            ),
            context=TelemetryExecutionContext(
                run_id=shared_run_id,
                scenario_id=f"sc-{test_id}",
                scenario_version="1.0",
                reproducibility_key=f"rk-{test_id}",
                seed=123,
            ),
            kafka_timestamp_ms=1770000000000,
            topic=KafkaTopic.METRICS.value,
            partition=0,
            offset=1,
        )
    )
    await vm_adapter.close()

    # Persist log with shared identity
    os_adapter = OpenSearchPersistenceAdapter()
    await os_adapter.bootstrap()
    await os_adapter.persist(
        TelemetryEnvelope(
            event=TelemetryEvent(
                schema_version="1.0",
                event_id=shared_event_id,
                event_time=now,
                tenant_id=tenant_id,
                environment="simulation",
                service="order-service",
                event_type=EventType.LOG,
                severity=EventSeverity.INFO,
                payload={"message": "Order processed"},
            ),
            context=TelemetryExecutionContext(
                run_id=shared_run_id,
                scenario_id=f"sc-{test_id}",
                scenario_version="1.0",
                reproducibility_key=f"rk-{test_id}",
                seed=123,
            ),
            kafka_timestamp_ms=1770000000000,
            topic=KafkaTopic.LOGS.value,
            partition=0,
            offset=1,
        )
    )
    await os_adapter.close()

    # Flush storage
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")
        await client.post(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_refresh")

    metric_adapter = MetricQueryAdapter()
    evidence_adapter = EvidenceQueryAdapter()

    # Query metric by run_id
    m_res = await metric_adapter.query(
        MetricQuery(
            metric=f"cross_store_{test_id}",
            run_id=shared_run_id,
            end_time=now + timedelta(seconds=60),
        )
    )
    assert m_res.count == 1
    assert m_res.samples[0].run_id == shared_run_id
    assert m_res.samples[0].event_id == shared_event_id

    # Query evidence by run_id
    e_res = await evidence_adapter.query(EvidenceQuery(run_id=shared_run_id))
    assert e_res.count == 1
    assert e_res.records[0].run_id == shared_run_id
    assert e_res.records[0].event_id == shared_event_id

    # Verify identical identity types
    assert isinstance(m_res.samples[0].run_id, uuid.UUID)
    assert isinstance(e_res.records[0].run_id, uuid.UUID)
    assert m_res.samples[0].run_id == e_res.records[0].run_id

    await metric_adapter.close()
    await evidence_adapter.close()


@pytest.mark.asyncio
async def test_max_uint64_cross_backend() -> None:
    """Verify uint64 maximum seed (18446744073709551615) round-trips correctly on both backends."""
    is_vm_healthy = await check_victoriametrics_health()
    is_os_healthy, _ = await check_opensearch_health()
    if not is_vm_healthy or not is_os_healthy:
        pytest.skip("VictoriaMetrics or OpenSearch not reachable")

    test_id = uuid.uuid4().hex[:8]
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    event_id_metric = uuid.uuid4()
    event_id_evidence = uuid.uuid4()
    now = datetime(2026, 9, 26, 16, 0, 0, tzinfo=timezone.utc)

    # 1. Persist Metric with max seed
    vm_adapter = VictoriaMetricsPersistenceAdapter()
    await vm_adapter.persist(
        TelemetryEnvelope(
            event=TelemetryEvent(
                schema_version="1.0",
                event_id=event_id_metric,
                event_time=now,
                tenant_id=tenant_id,
                environment="simulation",
                service="auth-service",
                event_type=EventType.METRIC,
                severity=EventSeverity.INFO,
                payload={"metric_name": f"max_seed_{test_id}", "value": 99.0},
            ),
            context=TelemetryExecutionContext(
                run_id=run_id,
                scenario_id=f"sc-{test_id}",
                scenario_version="1.0",
                reproducibility_key=f"rk-{test_id}",
                seed=MAX_UINT64,
            ),
            kafka_timestamp_ms=1770000000000,
            topic=KafkaTopic.METRICS.value,
            partition=0,
            offset=1,
        )
    )
    await vm_adapter.close()

    # 2. Persist Evidence with max seed
    os_adapter = OpenSearchPersistenceAdapter()
    await os_adapter.bootstrap()
    await os_adapter.persist(
        TelemetryEnvelope(
            event=TelemetryEvent(
                schema_version="1.0",
                event_id=event_id_evidence,
                event_time=now,
                tenant_id=tenant_id,
                environment="simulation",
                service="auth-service",
                event_type=EventType.LOG,
                severity=EventSeverity.INFO,
                payload={"message": "User authenticated"},
            ),
            context=TelemetryExecutionContext(
                run_id=run_id,
                scenario_id=f"sc-{test_id}",
                scenario_version="1.0",
                reproducibility_key=f"rk-{test_id}",
                seed=MAX_UINT64,
            ),
            kafka_timestamp_ms=1770000000000,
            topic=KafkaTopic.LOGS.value,
            partition=0,
            offset=1,
        )
    )
    await os_adapter.close()

    # Flush storage
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")
        await client.post(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_refresh")

    metric_adapter = MetricQueryAdapter()
    evidence_adapter = EvidenceQueryAdapter()

    # Query metric
    m_res = await metric_adapter.query(
        MetricQuery(
            metric=f"max_seed_{test_id}",
            seed=MAX_UINT64,
            end_time=now + timedelta(seconds=60),
        )
    )
    assert m_res.count == 1
    assert m_res.samples[0].seed == MAX_UINT64
    assert isinstance(m_res.samples[0].seed, int)

    # Query evidence
    e_res = await evidence_adapter.query(EvidenceQuery(run_id=run_id, seed=MAX_UINT64))
    assert e_res.count == 1
    assert e_res.records[0].seed == MAX_UINT64
    assert isinstance(e_res.records[0].seed, int)

    await metric_adapter.close()
    await evidence_adapter.close()


@pytest.mark.asyncio
async def test_non_utc_time_range_query() -> None:
    """Verify that timezone-aware non-UTC ranges correctly match equivalent instants."""
    is_vm_healthy = await check_victoriametrics_health()
    is_os_healthy, _ = await check_opensearch_health()
    if not is_vm_healthy or not is_os_healthy:
        pytest.skip("VictoriaMetrics or OpenSearch not reachable")

    test_id = uuid.uuid4().hex[:8]
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    # Domain instant: 2026-09-26 17:00:00 UTC represented in +05:30 (22:30:00)
    tz_offset = timezone(timedelta(hours=5, minutes=30))
    event_time_non_utc = datetime(2026, 9, 26, 22, 30, 0, tzinfo=tz_offset)
    instant_utc = datetime(2026, 9, 26, 17, 0, 0, tzinfo=timezone.utc)

    # Persist metric
    vm_adapter = VictoriaMetricsPersistenceAdapter()
    await vm_adapter.persist(
        TelemetryEnvelope(
            event=TelemetryEvent(
                schema_version="1.0",
                event_id=uuid.uuid4(),
                event_time=event_time_non_utc,
                tenant_id=tenant_id,
                environment="simulation",
                service="order-service",
                event_type=EventType.METRIC,
                severity=EventSeverity.INFO,
                payload={"metric_name": f"non_utc_{test_id}", "value": 77.0},
            ),
            context=TelemetryExecutionContext(
                run_id=run_id,
                scenario_id=f"sc-{test_id}",
                scenario_version="1.0",
                reproducibility_key=f"rk-{test_id}",
                seed=10,
            ),
            kafka_timestamp_ms=1770000000000,
            topic=KafkaTopic.METRICS.value,
            partition=0,
            offset=1,
        )
    )
    await vm_adapter.close()

    # Persist log
    os_adapter = OpenSearchPersistenceAdapter()
    await os_adapter.bootstrap()
    await os_adapter.persist(
        TelemetryEnvelope(
            event=TelemetryEvent(
                schema_version="1.0",
                event_id=uuid.uuid4(),
                event_time=event_time_non_utc,
                tenant_id=tenant_id,
                environment="simulation",
                service="order-service",
                event_type=EventType.LOG,
                severity=EventSeverity.INFO,
                payload={"message": "Non-UTC test"},
            ),
            context=TelemetryExecutionContext(
                run_id=run_id,
                scenario_id=f"sc-{test_id}",
                scenario_version="1.0",
                reproducibility_key=f"rk-{test_id}",
                seed=10,
            ),
            kafka_timestamp_ms=1770000000000,
            topic=KafkaTopic.LOGS.value,
            partition=0,
            offset=1,
        )
    )
    await os_adapter.close()

    # Flush storage
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")
        await client.post(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_refresh")

    metric_adapter = MetricQueryAdapter()
    evidence_adapter = EvidenceQueryAdapter()

    # Query with instant query at instant_utc
    m_res = await metric_adapter.query(
        MetricQuery(
            metric=f"non_utc_{test_id}",
            end_time=instant_utc,
        )
    )
    assert m_res.count == 1
    assert m_res.samples[0].timestamp.timestamp() == instant_utc.timestamp()

    e_res = await evidence_adapter.query(
        EvidenceQuery(
            run_id=run_id,
            start_time=instant_utc - timedelta(minutes=1),
            end_time=instant_utc + timedelta(minutes=1),
        )
    )
    assert e_res.count == 1
    assert e_res.records[0].event_time.timestamp() == instant_utc.timestamp()

    await metric_adapter.close()
    await evidence_adapter.close()


@pytest.mark.asyncio
async def test_backend_failure_contract() -> None:
    """Verify that backend unavailability surfaces as typed query errors."""
    unreachable_url = "http://127.0.0.1:59997"

    metric_adapter = MetricQueryAdapter(base_url=unreachable_url, timeout_seconds=0.5)
    with pytest.raises(MetricQueryBackendError):
        await metric_adapter.query(MetricQuery(metric="cpu"))
    await metric_adapter.close()

    evidence_adapter = EvidenceQueryAdapter(base_url=unreachable_url, timeout_seconds=0.5)
    with pytest.raises(EvidenceQueryBackendError):
        await evidence_adapter.query(EvidenceQuery())
    await evidence_adapter.close()
