import json
import uuid
from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from app.simulator.interfaces import TelemetryEmitter
from app.simulator.telemetry import (
    AEGISOPS_TELEMETRY_NAMESPACE,
    InMemoryTelemetryEmitter,
    ROOT_NAMESPACE,
    SimulatorTelemetryAdapter,
    TelemetrySynthesisContext,
    deterministic_telemetry_event_id,
)
from app.simulator.workload import (
    DeterministicWorkloadGenerator,
    SyntheticRequest,
    WorkloadConfig,
    WorkloadProfile,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent


def test_telemetry_synthesis_context_valid() -> None:
    now_utc = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
    tenant_id = uuid.uuid4()
    context = TelemetrySynthesisContext(
        tenant_id=tenant_id,
        environment="staging",
        run_start_time=now_utc,
    )
    assert context.tenant_id == tenant_id
    assert context.environment == "staging"
    assert context.run_start_time == now_utc


def test_telemetry_synthesis_context_rejects_naive_datetime() -> None:
    naive_now = datetime(2026, 9, 24, 12, 0, 0)
    with pytest.raises(ValidationError, match="timezone"):
        TelemetrySynthesisContext(run_start_time=naive_now)


def test_deterministic_telemetry_event_id() -> None:
    assert isinstance(ROOT_NAMESPACE, uuid.UUID)
    assert isinstance(AEGISOPS_TELEMETRY_NAMESPACE, uuid.UUID)
    assert AEGISOPS_TELEMETRY_NAMESPACE == uuid.uuid5(ROOT_NAMESPACE, "aegisops:simulator:telemetry")

    id1 = deterministic_telemetry_event_id("test:event:1")
    id2 = deterministic_telemetry_event_id("test:event:1")
    id3 = deterministic_telemetry_event_id("test:event:2")

    assert isinstance(id1, uuid.UUID)
    assert id1 == id2
    assert id1 != id3
    assert id1 == uuid.uuid5(AEGISOPS_TELEMETRY_NAMESPACE, "test:event:1")

    with pytest.raises(ValueError, match="identity_key must not be empty"):
        deterministic_telemetry_event_id("")


def test_request_telemetry_synthesis_mapping() -> None:
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time, environment="test-env")
    adapter = SimulatorTelemetryAdapter(context=context)

    req = SyntheticRequest(
        request_id="req-test-001",
        trace_id="trace-test-001",
        tick_index=2,
        sequence_in_tick=0,
        simulation_offset_seconds=2.0,
        source_service="client",
        target_service="api-gateway",
        route=["client", "api-gateway", "order-service", "database"],
        accumulated_latency_ms=67.5,
        outcome="success",
        status_code=200,
        error=None,
    )

    events = adapter.events_for_request(req)
    assert len(events) == 3

    # Event 1: Count Metric
    ev_count = events[0]
    assert ev_count.event_type == EventType.METRIC
    assert ev_count.severity == EventSeverity.INFO
    assert ev_count.service == "api-gateway"
    assert ev_count.trace_id == "trace-test-001"
    assert ev_count.environment == "test-env"
    assert ev_count.event_time == datetime(2026, 9, 24, 0, 0, 2, tzinfo=timezone.utc)
    assert ev_count.payload["metric_name"] == "http_requests_total"
    assert ev_count.payload["value"] == 1
    assert ev_count.payload["request_id"] == "req-test-001"

    # Event 2: Duration Metric
    ev_dur = events[1]
    assert ev_dur.event_type == EventType.METRIC
    assert ev_dur.severity == EventSeverity.INFO
    assert ev_dur.service == "api-gateway"
    assert ev_dur.trace_id == "trace-test-001"
    assert ev_dur.event_time == datetime(2026, 9, 24, 0, 0, 2, tzinfo=timezone.utc)
    assert ev_dur.payload["metric_name"] == "http_request_duration_ms"
    assert ev_dur.payload["value"] == 67.5

    # Event 3: Access Log
    ev_log = events[2]
    assert ev_log.event_type == EventType.LOG
    assert ev_log.severity == EventSeverity.INFO
    assert ev_log.service == "api-gateway"
    assert ev_log.trace_id == "trace-test-001"
    assert ev_log.payload["request_id"] == "req-test-001"
    assert ev_log.payload["route"] == ["client", "api-gateway", "order-service", "database"]
    assert ev_log.payload["status_code"] == 200
    assert ev_log.payload["latency_ms"] == 67.5


def test_batch_synthesis_deterministic_order() -> None:
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time)
    adapter = SimulatorTelemetryAdapter(context=context)

    config = WorkloadConfig(base_requests_per_second=2.0)
    gen = DeterministicWorkloadGenerator(config=config, seed=42)
    reqs = gen.requests_for_tick(0)

    events = adapter.events_for_requests(reqs)
    # 2 requests * 3 events = 6 events
    assert len(events) == 6

    # Verify per-request event order: metric_count, metric_duration, log
    assert events[0].payload["metric_name"] == "http_requests_total"
    assert events[1].payload["metric_name"] == "http_request_duration_ms"
    assert events[2].event_type == EventType.LOG

    assert events[3].payload["metric_name"] == "http_requests_total"
    assert events[4].payload["metric_name"] == "http_request_duration_ms"
    assert events[5].event_type == EventType.LOG


@pytest.mark.asyncio
async def test_in_memory_telemetry_emitter_protocol() -> None:
    concrete_emitter = InMemoryTelemetryEmitter()
    protocol_emitter: TelemetryEmitter = concrete_emitter
    assert hasattr(protocol_emitter, "emit")
    assert callable(protocol_emitter.emit)
    assert len(concrete_emitter) == 0

    event = TelemetryEvent(
        tenant_id=uuid.uuid4(),
        environment="test",
        service="api-gateway",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        payload={"count": 1},
    )

    await protocol_emitter.emit(event)
    assert len(concrete_emitter) == 1

    snap = concrete_emitter.snapshot()
    assert isinstance(snap, tuple)
    assert len(snap) == 1
    assert snap[0] is event

    concrete_emitter.clear()
    assert len(concrete_emitter) == 0
    assert len(concrete_emitter.snapshot()) == 0


@pytest.mark.asyncio
async def test_adapter_async_emit_helpers() -> None:
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time)
    adapter = SimulatorTelemetryAdapter(context=context)
    emitter = InMemoryTelemetryEmitter()

    config = WorkloadConfig(base_requests_per_second=3.0)
    gen = DeterministicWorkloadGenerator(config=config, seed=10)
    reqs = gen.requests_for_tick(0)

    # Emit single request
    await adapter.emit_request(emitter, reqs[0])
    assert len(emitter) == 3

    # Emit remaining requests
    await adapter.emit_requests(emitter, reqs[1:])
    assert len(emitter) == 9


def test_system_lifecycle_marker_creation() -> None:
    start_time = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time, environment="prod")
    adapter = SimulatorTelemetryAdapter(context=context)

    event = adapter.create_system_marker(
        marker_name="workload_started",
        service="api-gateway",
        simulation_offset_seconds=5.0,
        severity=EventSeverity.INFO,
        payload={"profile": "healthy_baseline", "rps": 10.0},
    )

    assert event.event_type == EventType.SYSTEM
    assert event.severity == EventSeverity.INFO
    assert event.service == "api-gateway"
    assert event.environment == "prod"
    assert event.event_time == datetime(2026, 9, 24, 10, 0, 5, tzinfo=timezone.utc)
    assert event.payload["marker"] == "workload_started"
    assert event.payload["rps"] == 10.0


def test_system_marker_validation() -> None:
    start_time = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time)
    adapter = SimulatorTelemetryAdapter(context=context)

    with pytest.raises(ValueError, match="marker_name must not be empty"):
        adapter.create_system_marker(marker_name="", service="api-gateway", simulation_offset_seconds=0.0)

    with pytest.raises(ValueError, match="service must not be empty"):
        adapter.create_system_marker(marker_name="test", service="", simulation_offset_seconds=0.0)

    with pytest.raises(ValueError, match="simulation_offset_seconds must be non-negative"):
        adapter.create_system_marker(marker_name="test", service="api-gateway", simulation_offset_seconds=-1.0)


def test_same_input_exact_telemetry_replay() -> None:
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time)

    config = WorkloadConfig(
        profile_name=WorkloadProfile.TRAFFIC_SURGE,
        base_requests_per_second=2.0,
        surge_start_seconds=1.0,
        surge_duration_seconds=1.0,
        surge_requests_per_second=6.0,
        total_duration_seconds=5.0,
    )

    # Run 1
    gen1 = DeterministicWorkloadGenerator(config=config, seed=777)
    adapter1 = SimulatorTelemetryAdapter(context=context)
    batch1: list[TelemetryEvent] = []
    for t in range(3):
        batch1.extend(adapter1.events_for_requests(gen1.requests_for_tick(t)))

    # Run 2
    gen2 = DeterministicWorkloadGenerator(config=config, seed=777)
    adapter2 = SimulatorTelemetryAdapter(context=context)
    batch2: list[TelemetryEvent] = []
    for t in range(3):
        batch2.extend(adapter2.events_for_requests(gen2.requests_for_tick(t)))

    assert len(batch1) == len(batch2)
    for e1, e2 in zip(batch1, batch2):
        assert e1.event_id == e2.event_id
        assert e1.event_time == e2.event_time
        assert e1.tenant_id == e2.tenant_id
        assert e1.environment == e2.environment
        assert e1.service == e2.service
        assert e1.event_type == e2.event_type
        assert e1.severity == e2.severity
        assert e1.trace_id == e2.trace_id
        assert e1.payload == e2.payload
        assert e1.model_dump_json() == e2.model_dump_json()


def test_different_seed_and_config_telemetry_variance() -> None:
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time)
    adapter = SimulatorTelemetryAdapter(context=context)

    config_a = WorkloadConfig(base_requests_per_second=2.0)
    config_b = WorkloadConfig(base_requests_per_second=4.0)

    # Different seeds
    gen1 = DeterministicWorkloadGenerator(config=config_a, seed=1)
    gen2 = DeterministicWorkloadGenerator(config=config_a, seed=2)
    evs1 = adapter.events_for_requests(gen1.requests_for_tick(0))
    evs2 = adapter.events_for_requests(gen2.requests_for_tick(0))

    assert len(evs1) == len(evs2)
    assert evs1[0].event_id != evs2[0].event_id
    assert evs1[0].trace_id != evs2[0].trace_id

    # Different config
    gen3 = DeterministicWorkloadGenerator(config=config_b, seed=1)
    evs3 = adapter.events_for_requests(gen3.requests_for_tick(0))
    assert len(evs3) == 12  # 4 reqs * 3 events
    assert evs1[0].event_id != evs3[0].event_id


def test_emitter_isolation() -> None:
    emitter1 = InMemoryTelemetryEmitter()
    emitter2 = InMemoryTelemetryEmitter()

    event = TelemetryEvent(
        tenant_id=uuid.uuid4(),
        environment="test",
        service="api-gateway",
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
    )

    import asyncio

    asyncio.run(emitter1.emit(event))
    assert len(emitter1) == 1
    assert len(emitter2) == 0

    emitter1.clear()
    assert len(emitter1) == 0
    assert len(emitter2) == 0


def test_telemetry_serialization_and_json_safety() -> None:
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    context = TelemetrySynthesisContext(run_start_time=start_time)
    adapter = SimulatorTelemetryAdapter(context=context)

    config = WorkloadConfig(base_requests_per_second=1.0)
    gen = DeterministicWorkloadGenerator(config=config, seed=42)
    reqs = gen.requests_for_tick(0)
    events = adapter.events_for_requests(reqs)

    for event in events:
        serialized = event.model_dump_json()
        data = json.loads(serialized)
        assert data["schema_version"] == "1.0"
        assert isinstance(data["event_id"], str)
        assert isinstance(data["event_time"], str)
        assert data["service"] == "api-gateway"

        reconstructed = TelemetryEvent.model_validate_json(serialized)
        assert reconstructed == event


def test_telemetry_package_exports() -> None:
    import app.simulator.telemetry as sim_telemetry_pkg

    expected = [
        "AEGISOPS_TELEMETRY_NAMESPACE",
        "InMemoryTelemetryEmitter",
        "ROOT_NAMESPACE",
        "SimulatorTelemetryAdapter",
        "TelemetrySynthesisContext",
        "deterministic_telemetry_event_id",
    ]
    assert hasattr(sim_telemetry_pkg, "__all__")
    assert sorted(sim_telemetry_pkg.__all__) == sorted(expected)
    for sym in expected:
        assert hasattr(sim_telemetry_pkg, sym)
