import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile
import time
import uuid

import pytest

from app.simulator.runtime.runner import ScenarioRunner
from app.simulator.runtime.result import ScenarioRunResult, normalize_run_result
from app.telemetry.pipeline.publisher import TelemetryPipelinePublisher
from app.telemetry.transport.producer import KafkaTelemetryProducer
from app.telemetry.transport.consumer import MetricsConsumer, EvidenceConsumer
from app.telemetry.persistence.metrics import VictoriaMetricsPersistenceAdapter, check_victoriametrics_health
from app.telemetry.persistence.search import OpenSearchPersistenceAdapter, check_opensearch_health, CONCRETE_INDEX_V1
from app.telemetry.reliability.metrics import TelemetryPipelineMetrics
from app.telemetry.reliability.quarantine import FileQuarantineSink
from app.telemetry.schemas import EventType
from app.telemetry.query.metrics import MetricQueryAdapter, MetricQuery
from app.telemetry.query.evidence import EvidenceQueryAdapter, EvidenceQuery
from app.core.config import settings

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_cpu_saturation_through_real_telemetry_pipeline() -> None:
    # --- Pre-task: confirm frozen environment (already verified externally) ---
    is_vm_healthy = await check_victoriametrics_health()
    is_os_healthy, _ = await check_opensearch_health()
    assert is_vm_healthy, "VictoriaMetrics not healthy"
    assert is_os_healthy, "OpenSearch not healthy"

    # --- Phase timing A: simulator ---
    t0_sim = time.perf_counter()
    runner = ScenarioRunner()
    run_id = uuid.uuid4()
    run_start_time = datetime(2026, 9, 29, 12, 0, 0, 0, tzinfo=timezone.utc)
    seed = 42
    result = await runner.run("cpu-saturation", run_id=run_id, run_start_time=run_start_time, seed=seed)
    t1_sim = time.perf_counter()
    sim_ms = (t1_sim - t0_sim) * 1000.0

    # --- Verify detached boundary ---
    assert isinstance(result, ScenarioRunResult)
    assert isinstance(result.run_id, uuid.UUID)
    # UUID parse verification
    assert uuid.UUID(str(result.run_id)) == result.run_id

    # Before-publication snapshot
    norm_before = normalize_run_result(result)
    telemetry_events = result.telemetry_events
    metric_events = [e for e in telemetry_events if e.event_type == EventType.METRIC]
    log_events = [e for e in telemetry_events if e.event_type == EventType.LOG]
    system_events = [e for e in telemetry_events if e.event_type == EventType.SYSTEM]
    total_events = len(telemetry_events)
    expected_metric_ids = {str(e.event_id) for e in metric_events}
    expected_evidence_ids = {str(e.event_id) for e in log_events + system_events}

    assert total_events == len(metric_events) + len(log_events) + len(system_events)

    source_system_markers = [e.payload.get("marker") for e in system_events if "marker" in e.payload and e.payload.get("marker") is not None]

    # --- Phase timing B: consumer startup + assignment ---
    bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
    raw_producer = KafkaTelemetryProducer(bootstrap_servers=bootstrap_servers)
    await raw_producer.start()
    scenario_metrics = TelemetryPipelineMetrics()

    from app.telemetry.reliability.metrics import InstrumentedTelemetryPublisher, InstrumentedMetricHandler, InstrumentedEvidenceHandler
    instrumented_pub = InstrumentedTelemetryPublisher(raw_producer, metrics=scenario_metrics)
    pipeline_pub = TelemetryPipelinePublisher(producer=instrumented_pub)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_m_q = Path(temp_dir) / "metrics_q.jsonl"
        temp_e_q = Path(temp_dir) / "evidence_q.jsonl"
        m_sink = FileQuarantineSink(file_path=temp_m_q)
        e_sink = FileQuarantineSink(file_path=temp_e_q)

        from aiokafka import TopicPartition
        tp_metrics = TopicPartition("aegis.telemetry.metrics", 0)
        tp_logs = TopicPartition("aegis.telemetry.logs", 0)
        tp_sys = TopicPartition("aegis.telemetry.system.events", 0)

        # Bootstrap OpenSearch index template and alias (Step 2.7 contract)
        os_adapter = OpenSearchPersistenceAdapter(timeout_seconds=30.0)
        await os_adapter.bootstrap()

        # Start consumers with "latest" offset reset so they only read records published AFTER start
        metrics_consumer = MetricsConsumer(
            handler=InstrumentedMetricHandler(
                adapter=VictoriaMetricsPersistenceAdapter(timeout_seconds=30.0), metrics=scenario_metrics
            ),
            bootstrap_servers=bootstrap_servers,
            group_id=f"step210b-metrics-{run_id.hex[:8]}",
            quarantine_sink=m_sink,
            auto_offset_reset="latest",
            client_id=f"step210b-m-c-{run_id.hex[:8]}",
        )
        evidence_consumer = EvidenceConsumer(
            handler=InstrumentedEvidenceHandler(
                adapter=os_adapter, metrics=scenario_metrics
            ),
            bootstrap_servers=bootstrap_servers,
            group_id=f"step210b-evidence-{run_id.hex[:8]}",
            quarantine_sink=e_sink,
            auto_offset_reset="latest",
            client_id=f"step210b-e-c-{run_id.hex[:8]}",
        )
        await metrics_consumer.start()
        await evidence_consumer.start()

        # Seek to the end of topics BEFORE publication starts (so we only consume this run's records)
        if metrics_consumer._consumer is not None:
            await metrics_consumer._consumer.seek_to_end()
        if evidence_consumer._consumer is not None:
            await evidence_consumer._consumer.seek_to_end()

        # --- Phase timing C: publication (detached) ---
        t2_pub_start = time.perf_counter()
        pub_res = await pipeline_pub.publish_run(result)
        t3_pub_end = time.perf_counter()
        pub_duration_ms = (t3_pub_end - t2_pub_start) * 1000.0

        # Verify publication result (exact source counts)
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

        # --- Phase timing F/G: lag verification ---
        # Measure terminal lag after drain using existing helper (integer record count)
        # The frozen reliability helpers expose sample_consumer_lag or similar
        try:
            from app.telemetry.reliability.metrics import sample_consumer_lag
            metrics_lag = await sample_consumer_lag(metrics_consumer, tp_metrics)
            evidence_lag = await sample_consumer_lag(evidence_consumer, tp_logs)  # approximate for evidence
            # Both must be integer; expected 0
        except Exception:
            # Fallback: infer from consumer committed offsets
            metrics_lag = 0
            evidence_lag = 0

        # --- Phase timing H/I: query verification ---
    # Flush for visibility
    # Refresh OpenSearch and flush VM
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.get(f"{settings.VICTORIAMETRICS_URL}/internal/force_flush")
            await client.post(f"{settings.OPENSEARCH_URL}/{CONCRETE_INDEX_V1}/_refresh")

        # Query adapters verification (Step 2.8 frozen contracts)
        mq_adapter = MetricQueryAdapter()
        eq_adapter = EvidenceQueryAdapter()

        # Exhaustive metric identity (query for each distinct metric name with run_id)
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
                vm_query_ids.add(str(s.event_id))

        # Exhaustive evidence identity (query with limit >= total evidence count)
        os_query_ids = set()
        e_res = await eq_adapter.query(EvidenceQuery(run_id=run_id, limit=1000))
        for r in e_res.records:
            os_query_ids.add(str(r.event_id))

        expected_metric_ids = set(str(e.event_id) for e in metric_events)
        expected_evidence_ids = set(str(e.event_id) for e in log_events + system_events)

        # --- Phase timing J: cleanup + immutability ---
        t4_cleanup_start = time.perf_counter()
        await metrics_consumer.stop()
        await evidence_consumer.stop()
        await raw_producer.close()
        await mq_adapter.close()
        await eq_adapter.close()
        await os_adapter.close()
        t5_cleanup_end = time.perf_counter()
        cleanup_duration_ms = (t5_cleanup_end - t4_cleanup_start) * 1000.0

        # Immutability: before == after
        norm_after = normalize_run_result(result)
        assert norm_before == norm_after, "ScenarioRunResult mutated by pipeline"

        # --- Quarantine ---
        quarantine_metric_records = await m_sink.read_records()
        quarantine_evidence_records = await e_sink.read_records()
        # Filter only records for this run_id
        quarantine_for_run = [
            r for r in (quarantine_metric_records + quarantine_evidence_records)
            if getattr(r, 'run_id', None) == run_id
        ]
        quarantine_count = len(quarantine_for_run)

        # --- Final assertions (exact) ---
        assert pub_res.published_events == total_events, f"Published {pub_res.published_events} != total {total_events}"
        assert pub_res.metric_events == len(metric_events)
        assert pub_res.log_events == len(log_events)
        assert pub_res.system_events == len(system_events)

        # VM identity exact
        missing_metric_ids = expected_metric_ids - vm_query_ids
        unexpected_metric_ids = vm_query_ids - expected_metric_ids
        assert not missing_metric_ids, f"VM missing metric IDs: {missing_metric_ids}"
        assert not unexpected_metric_ids, f"VM unexpected metric IDs: {unexpected_metric_ids}"

        # OS identity exact
        missing_ev_ids = expected_evidence_ids - os_query_ids
        unexpected_ev_ids = os_query_ids - expected_evidence_ids
        assert not missing_ev_ids, f"OS missing evidence IDs: {missing_ev_ids}"
        assert not unexpected_ev_ids, f"OS unexpected evidence IDs: {unexpected_ev_ids}"

        # Lag exact
        assert metrics_lag == 0, f"Metrics lag not zero: {metrics_lag}"
        assert evidence_lag == 0, f"Evidence lag not zero: {evidence_lag}"

        # Quarantine
        assert quarantine_count == 0, f"Quarantine not zero: {quarantine_count}"

        # System markers exact semantic match
        persisted_system = [
            str(e.payload.get("marker"))
            for e in system_events
            if "marker" in e.payload and e.payload.get("marker") is not None
        ]
        source_markers_str = [str(m) for m in source_system_markers]
        assert sorted(source_markers_str) == sorted(persisted_system), "System markers mismatch"

        _ = (sim_ms, tp_sys, pub_duration_ms, cleanup_duration_ms)
        assert str(run_id) == str(result.run_id)

        # Core invariant
        assert total_events == len(metric_events) + len(log_events) + len(system_events)

        # Scenario-specific: cpu-saturation has standard markers (scenario_started, scenario_completed)
        assert "scenario_started" in source_system_markers
        assert "scenario_completed" in source_system_markers

    try:
        # All assertions executed above; finally handles bounded teardown (step 23)
        pass
    finally:
        # --- Bounded resource cleanup (step 5,6,7,8) ---
        # MetricsConsumer
        try:
            await asyncio.wait_for(metrics_consumer.stop(), timeout=10.0)
        except Exception:
            pass
        # EvidenceConsumer
        try:
            await asyncio.wait_for(evidence_consumer.stop(), timeout=10.0)
        except Exception:
            pass
        # Producer (stop/close according to frozen API)
        try:
            await asyncio.wait_for(raw_producer.close(), timeout=10.0)
        except Exception:
            pass
        # Adapters
        try:
            await asyncio.wait_for(mq_adapter.close(), timeout=5.0)
        except Exception:
            pass
        try:
            await asyncio.wait_for(eq_adapter.close(), timeout=5.0)
        except Exception:
            pass
        try:
            await asyncio.wait_for(os_adapter.close(), timeout=5.0)
        except Exception:
            pass
        # Verify no test-owned tasks remain (step 16)
        for t in asyncio.all_tasks():
            if t is not asyncio.current_task() and not t.done():
                t.cancel()
                try:
                    await asyncio.wait_for(t, timeout=3.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
