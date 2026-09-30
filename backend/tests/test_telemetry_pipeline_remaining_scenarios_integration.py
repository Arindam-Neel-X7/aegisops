import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile
import time
import uuid

import aiokafka
from aiokafka import TopicPartition
import httpx
import pytest

from app.core.config import settings
from app.simulator.runtime.result import ScenarioRunResult, normalize_run_result
from app.simulator.runtime.runner import ScenarioRunner
from app.simulator.scenarios import scenario_ids
from app.telemetry.persistence.metrics import (
    VictoriaMetricsPersistenceAdapter,
    check_victoriametrics_health,
)
from app.telemetry.persistence.search import (
    CONCRETE_INDEX_V1,
    OpenSearchPersistenceAdapter,
    check_opensearch_health,
)
from app.telemetry.pipeline.publisher import TelemetryPipelinePublisher
from app.telemetry.query.evidence import EvidenceQuery, EvidenceQueryAdapter
from app.telemetry.query.metrics import MetricQuery, MetricQueryAdapter
from app.telemetry.reliability.metrics import (
    InstrumentedEvidenceHandler,
    InstrumentedMetricHandler,
    InstrumentedTelemetryPublisher,
    TelemetryPipelineMetrics,
    sample_consumer_lag,
)
from app.telemetry.reliability.quarantine import FileQuarantineSink
from app.telemetry.schemas import EventType
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import EvidenceConsumer, MetricsConsumer
from app.telemetry.transport.producer import KafkaTelemetryProducer

pytestmark = pytest.mark.integration

STEP_210C_SCENARIOS = (
    "memory-exhaustion",
    "connection-exhaustion",
    "dependency-latency",
    "dependency-failure",
    "error-rate-spike",
    "traffic-surge",
    "bad-deployment-config",
)


async def _wait_for_assignment(
    consumer: aiokafka.AIOKafkaConsumer,
    expected_partitions: set[TopicPartition],
    timeout_seconds: float = 10.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if expected_partitions.issubset(consumer.assignment()):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(
        f"Consumer failed to be assigned expected partitions {expected_partitions} within {timeout_seconds}s. "
        f"Current assignment: {consumer.assignment()}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario_id", STEP_210C_SCENARIOS)
async def test_remaining_scenario_through_real_telemetry_pipeline(scenario_id: str) -> None:
    # --- Coverage guard ---
    assert ("cpu-saturation",) + STEP_210C_SCENARIOS == scenario_ids()

    # --- Pre-task: confirm infrastructure health ---
    is_vm_healthy = await check_victoriametrics_health()
    is_os_healthy, _ = await check_opensearch_health()
    assert is_vm_healthy, "VictoriaMetrics not healthy"
    assert is_os_healthy, "OpenSearch not healthy"

    # --- Phase timing A: simulator (detached boundary) ---
    t0_sim = time.perf_counter()
    runner = ScenarioRunner()
    run_id = uuid.uuid4()
    run_start_time = datetime(2026, 9, 29, 12, 0, 0, 0, tzinfo=timezone.utc)
    seed = 42
    result = await runner.run(scenario_id, run_id=run_id, run_start_time=run_start_time, seed=seed)
    t1_sim = time.perf_counter()
    sim_ms = (t1_sim - t0_sim) * 1000.0

    # --- Verify detached boundary & run_id ---
    assert isinstance(result, ScenarioRunResult)
    assert isinstance(result.run_id, uuid.UUID)
    assert uuid.UUID(str(result.run_id)) == result.run_id
    assert str(run_id) == str(result.run_id)

    # Before-publication snapshot for immutability verification
    norm_before = normalize_run_result(result)
    telemetry_events = result.telemetry_events
    metric_events = [e for e in telemetry_events if e.event_type == EventType.METRIC]
    log_events = [e for e in telemetry_events if e.event_type == EventType.LOG]
    system_events = [e for e in telemetry_events if e.event_type == EventType.SYSTEM]
    total_events = len(telemetry_events)

    assert total_events == len(metric_events) + len(log_events) + len(system_events)

    expected_metric_ids = {str(e.event_id) for e in metric_events}
    expected_log_ids = {str(e.event_id) for e in log_events}
    expected_system_ids = {str(e.event_id) for e in system_events}
    expected_evidence_ids = expected_log_ids | expected_system_ids

    source_system_markers = [
        str(e.payload.get("marker"))
        for e in system_events
        if "marker" in e.payload and e.payload.get("marker") is not None
    ]

    # Universal lifecycle markers check
    assert "scenario_started" in source_system_markers
    assert "scenario_completed" in source_system_markers

    # Scenario-specific source marker checks
    if scenario_id == "traffic-surge":
        assert "traffic_surge_started" in source_system_markers
        assert "traffic_surge_ended" in source_system_markers

    if scenario_id == "bad-deployment-config":
        assert "deployment_changed" in source_system_markers
        source_dept_record = next(
            e for e in system_events if e.payload.get("marker") == "deployment_changed"
        )
        assert source_dept_record.payload.get("deployment_version") == "bad-config-v1"
        assert source_dept_record.payload.get("change_type") == "configuration"

    # --- Phase timing B: consumer startup + assignment ---
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    raw_producer = KafkaTelemetryProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()

    scenario_metrics = TelemetryPipelineMetrics()
    instrumented_pub = InstrumentedTelemetryPublisher(raw_producer, metrics=scenario_metrics)
    pipeline_pub = TelemetryPipelinePublisher(producer=instrumented_pub)

    vm_adapter = VictoriaMetricsPersistenceAdapter(timeout_seconds=30.0)
    os_adapter = OpenSearchPersistenceAdapter(timeout_seconds=30.0)
    await os_adapter.bootstrap()

    metric_handler = InstrumentedMetricHandler(adapter=vm_adapter, metrics=scenario_metrics)
    evidence_handler = InstrumentedEvidenceHandler(adapter=os_adapter, metrics=scenario_metrics)

    mq_adapter = MetricQueryAdapter()
    eq_adapter = EvidenceQueryAdapter()

    tp_metrics = TopicPartition(KafkaTopic.METRICS.value, 0)
    tp_logs = TopicPartition(KafkaTopic.LOGS.value, 0)
    tp_sys = TopicPartition(KafkaTopic.SYSTEM_EVENTS.value, 0)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_m_q = Path(temp_dir) / f"metrics_q_{scenario_id}.jsonl"
        temp_e_q = Path(temp_dir) / f"evidence_q_{scenario_id}.jsonl"
        m_sink = FileQuarantineSink(file_path=temp_m_q)
        e_sink = FileQuarantineSink(file_path=temp_e_q)

        metrics_consumer = MetricsConsumer(
            handler=metric_handler,
            bootstrap_servers=bootstrap_servers,
            group_id=f"step210c-m-{scenario_id}-{run_id.hex[:8]}",
            quarantine_sink=m_sink,
            auto_offset_reset="latest",
            client_id=f"step210c-m-c-{scenario_id}-{run_id.hex[:8]}",
        )
        evidence_consumer = EvidenceConsumer(
            handler=evidence_handler,
            bootstrap_servers=bootstrap_servers,
            group_id=f"step210c-e-{scenario_id}-{run_id.hex[:8]}",
            quarantine_sink=e_sink,
            auto_offset_reset="latest",
            client_id=f"step210c-e-c-{scenario_id}-{run_id.hex[:8]}",
        )

        try:
            await metrics_consumer.start()
            await evidence_consumer.start()

            assert metrics_consumer._consumer is not None
            assert evidence_consumer._consumer is not None

            # Await group partition assignments
            await _wait_for_assignment(metrics_consumer._consumer, {tp_metrics})
            await _wait_for_assignment(evidence_consumer._consumer, {tp_logs, tp_sys})

            # Seek assigned partitions to current end offsets BEFORE publishing
            await metrics_consumer._consumer.seek_to_end(tp_metrics)
            await evidence_consumer._consumer.seek_to_end(tp_logs, tp_sys)

            # --- Phase timing C: publication (detached) ---
            t2_pub_start = time.perf_counter()
            pub_res = await pipeline_pub.publish_run(result)
            t3_pub_end = time.perf_counter()
            pub_duration_ms = (t3_pub_end - t2_pub_start) * 1000.0

            # Verify publication result counts
            assert pub_res.published_events == total_events
            assert pub_res.total_events == total_events
            assert pub_res.metric_events == len(metric_events)
            assert pub_res.log_events == len(log_events)
            assert pub_res.system_events == len(system_events)

            # Drain both consumers concurrently within a bounded deadline
            drain_tasks = []
            if metric_events:
                drain_tasks.append(metrics_consumer.run(max_records=len(metric_events)))
            if log_events or system_events:
                drain_tasks.append(evidence_consumer.run(max_records=len(log_events) + len(system_events)))
            if drain_tasks:
                await asyncio.wait_for(asyncio.gather(*drain_tasks), timeout=240.0)

            # --- Phase timing F/G: real terminal lag verification ---
            metrics_lag = await sample_consumer_lag(metrics_consumer._consumer, tp_metrics)
            logs_lag = await sample_consumer_lag(evidence_consumer._consumer, tp_logs)
            system_lag = await sample_consumer_lag(evidence_consumer._consumer, tp_sys)

            assert metrics_lag == 0, f"Metrics lag not zero: {metrics_lag}"
            assert logs_lag == 0, f"Logs lag not zero: {logs_lag}"
            assert system_lag == 0, f"System lag not zero: {system_lag}"

            evidence_lag = logs_lag + system_lag
            assert evidence_lag == 0, f"Evidence aggregate lag not zero: {evidence_lag}"

            # --- Phase timing H/I: storage visibility & query verification ---
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")
                await client.post(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_refresh")

            # Exhaustive metric identity in VictoriaMetrics
            eval_time = run_start_time + timedelta(seconds=30.0 + 60)
            vm_query_ids = set()
            distinct_metric_names = {
                str(e.payload.get("metric_name"))
                for e in metric_events
                if e.payload.get("metric_name") is not None
            }
            for metric_name in distinct_metric_names:
                m_res = await mq_adapter.query(
                    MetricQuery(metric=metric_name, run_id=run_id, end_time=eval_time)
                )
                for s in m_res.samples:
                    assert s.run_id == run_id
                    vm_query_ids.add(str(s.event_id))

            # Exhaustive LOG identity in OpenSearch
            os_log_ids = set()
            log_res = await eq_adapter.query(
                EvidenceQuery(run_id=run_id, event_type=EventType.LOG, limit=1000)
            )
            for r in log_res.records:
                assert r.run_id == run_id
                os_log_ids.add(str(r.event_id))

            # Exhaustive SYSTEM identity in OpenSearch
            os_system_ids = set()
            sys_res = await eq_adapter.query(
                EvidenceQuery(run_id=run_id, event_type=EventType.SYSTEM, limit=1000)
            )
            for r in sys_res.records:
                assert r.run_id == run_id
                os_system_ids.add(str(r.event_id))

            os_evidence_ids = os_log_ids | os_system_ids

            # Assert exact ID matching (0 missing, 0 unexpected)
            missing_metric_ids = expected_metric_ids - vm_query_ids
            unexpected_metric_ids = vm_query_ids - expected_metric_ids
            assert not missing_metric_ids, f"VM missing metric IDs: {missing_metric_ids}"
            assert not unexpected_metric_ids, f"VM unexpected metric IDs: {unexpected_metric_ids}"

            missing_log_ids = expected_log_ids - os_log_ids
            unexpected_log_ids = os_log_ids - expected_log_ids
            assert not missing_log_ids, f"OS missing log IDs: {missing_log_ids}"
            assert not unexpected_log_ids, f"OS unexpected log IDs: {unexpected_log_ids}"

            missing_sys_ids = expected_system_ids - os_system_ids
            unexpected_sys_ids = os_system_ids - expected_system_ids
            assert not missing_sys_ids, f"OS missing system IDs: {missing_sys_ids}"
            assert not unexpected_sys_ids, f"OS unexpected system IDs: {unexpected_sys_ids}"

            assert os_evidence_ids == expected_evidence_ids

            # System marker persistence verification via OpenSearch records
            assert len(sys_res.records) == len(system_events)
            persisted_system_markers = [
                str(r.payload.get("marker"))
                for r in sys_res.records
                if "marker" in r.payload and r.payload.get("marker") is not None
            ]
            assert sorted(source_system_markers) == sorted(persisted_system_markers), "System markers mismatch"

            # Scenario-specific persisted marker checks
            if scenario_id == "traffic-surge":
                assert "traffic_surge_started" in persisted_system_markers
                assert "traffic_surge_ended" in persisted_system_markers

            if scenario_id == "bad-deployment-config":
                assert "deployment_changed" in persisted_system_markers
                persisted_dept_record = next(
                    r for r in sys_res.records if r.payload.get("marker") == "deployment_changed"
                )
                assert persisted_dept_record.payload.get("deployment_version") == "bad-config-v1"
                assert persisted_dept_record.payload.get("change_type") == "configuration"

            # --- Phase timing J: cleanup + immutability ---
            t4_cleanup_start = time.perf_counter()
            await metrics_consumer.stop()
            await evidence_consumer.stop()
            await raw_producer.close()
            await mq_adapter.close()
            await eq_adapter.close()
            await vm_adapter.close()
            await os_adapter.close()
            t5_cleanup_end = time.perf_counter()
            cleanup_duration_ms = (t5_cleanup_end - t4_cleanup_start) * 1000.0

            # Immutability: before == after
            norm_after = normalize_run_result(result)
            assert norm_before == norm_after, "ScenarioRunResult mutated by pipeline"

            # --- Quarantine check ---
            quarantine_metric_records = await m_sink.read_records()
            quarantine_evidence_records = await e_sink.read_records()
            quarantine_for_run = [
                r for r in (quarantine_metric_records + quarantine_evidence_records)
                if getattr(r, "run_id", None) == run_id
            ]
            quarantine_count = len(quarantine_for_run)
            assert quarantine_count == 0, f"Quarantine not zero: {quarantine_count}"

            _ = (sim_ms, tp_sys, pub_duration_ms, cleanup_duration_ms)

        finally:
            await asyncio.wait_for(metrics_consumer.stop(), timeout=10.0)
            await asyncio.wait_for(evidence_consumer.stop(), timeout=10.0)
            await asyncio.wait_for(raw_producer.close(), timeout=10.0)
            await asyncio.wait_for(mq_adapter.close(), timeout=5.0)
            await asyncio.wait_for(eq_adapter.close(), timeout=5.0)
            await asyncio.wait_for(vm_adapter.close(), timeout=5.0)
            await asyncio.wait_for(os_adapter.close(), timeout=5.0)
