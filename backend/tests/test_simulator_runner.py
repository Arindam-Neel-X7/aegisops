from datetime import datetime, timezone
import uuid
import pytest

from app.simulator.interfaces import FaultType
from app.simulator.runtime.clock import SimulationClock
from app.simulator.runtime.result import (
    ScenarioRunResult,
    normalize_run_result,
)
from app.simulator.runtime.runner import ScenarioRunner
from app.simulator.scenarios import scenario_ids
from app.telemetry.schemas import EventType


def test_simulation_clock_contract() -> None:
    start = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
    clock = SimulationClock(start_time=start, tick_seconds=1.0)

    assert clock.current_tick == 0
    assert clock.current_offset_seconds == 0.0
    assert clock.current_time == start

    # Advance 1 tick
    new_offset = clock.advance()
    assert new_offset == 1.0
    assert clock.current_tick == 1
    assert clock.current_offset_seconds == 1.0
    assert clock.current_time == datetime(2026, 9, 24, 12, 0, 1, tzinfo=timezone.utc)

    # Advance 4 more ticks
    for _ in range(4):
        clock.advance()
    assert clock.current_tick == 5
    assert clock.current_offset_seconds == 5.0

    # Reset
    clock.reset()
    assert clock.current_tick == 0
    assert clock.current_offset_seconds == 0.0
    assert clock.current_time == start


def test_simulation_clock_validation() -> None:
    naive_start = datetime(2026, 9, 24, 12, 0, 0)
    with pytest.raises(ValueError, match="start_time must be timezone-aware"):
        SimulationClock(start_time=naive_start)

    start = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="tick_seconds must be positive"):
        SimulationClock(start_time=start, tick_seconds=0.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("scen_id", scenario_ids())
async def test_all_eight_scenarios_execute_successfully(scen_id: str) -> None:
    runner = ScenarioRunner()
    run_id = uuid.uuid4()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    result = await runner.run(
        scenario=scen_id,
        run_id=run_id,
        run_start_time=start_time,
        seed=42,
    )

    assert isinstance(result, ScenarioRunResult)
    assert result.run_id == run_id
    assert result.scenario_id == scen_id
    assert result.scenario_version == "1.0.0"
    assert result.seed == 42
    assert len(result.reproducibility_key) == 64
    assert len(result.telemetry_events) > 0

    # Check transitions
    phases = [t.phase for t in result.transitions]
    assert phases == ["initialized", "activated", "recovered", "completed"]

    # Check truth cardinality
    if scen_id == "traffic-surge":
        assert len(result.scenario_truth.fault_ground_truth_records) == 0
        assert result.request_count == 700  # 10s@10 + 10s@50 + 10s@10 = 100+500+100
    else:
        assert len(result.scenario_truth.fault_ground_truth_records) == 1
        assert result.request_count == 300  # 30s @ 10 RPS = 300


@pytest.mark.asyncio
async def test_cpu_saturation_telemetry_evidence() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="cpu-saturation",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )

    # Filter CPU metric events on order-service
    cpu_events = [
        ev
        for ev in result.telemetry_events
        if ev.service == "order-service"
        and ev.event_type == EventType.METRIC
        and ev.payload.get("metric_name") == "simulated_cpu_utilization_pct"
    ]
    assert len(cpu_events) == 30  # 30 ticks

    # Ticks 0..9: baseline CPU (15%)
    for ev in cpu_events[:10]:
        assert ev.payload["value"] == 15.0

    # Ticks 10..19: saturated CPU (95%)
    for ev in cpu_events[10:20]:
        assert ev.payload["value"] == 95.0

    # Ticks 20..29: recovered CPU (15%)
    for ev in cpu_events[20:]:
        assert ev.payload["value"] == 15.0


@pytest.mark.asyncio
async def test_traffic_surge_execution() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="traffic-surge",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )

    assert result.request_count == 700
    assert len(result.scenario_truth.fault_ground_truth_records) == 0

    # System markers check
    system_markers = [
        ev.payload["marker"]
        for ev in result.telemetry_events
        if ev.event_type == EventType.SYSTEM and "marker" in ev.payload
    ]
    assert "traffic_surge_started" in system_markers
    assert "traffic_surge_ended" in system_markers


@pytest.mark.asyncio
async def test_bad_deployment_execution() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="bad-deployment-config",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )

    assert len(result.scenario_truth.fault_ground_truth_records) == 1
    record = result.scenario_truth.fault_ground_truth_records[0]
    assert record.fault_type == FaultType.ERROR

    # Deployment marker check
    deploy_events = [
        ev
        for ev in result.telemetry_events
        if ev.event_type == EventType.SYSTEM
        and ev.payload.get("marker") == "deployment_changed"
    ]
    assert len(deploy_events) == 1
    assert deploy_events[0].payload["deployment_version"] == "bad-config-v1"


@pytest.mark.asyncio
async def test_memory_exhaustion_telemetry_evidence() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="memory-exhaustion",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    mem_events = [
        ev
        for ev in result.telemetry_events
        if ev.service == "notification-service"
        and ev.event_type == EventType.METRIC
        and ev.payload.get("metric_name") == "simulated_memory_utilization_pct"
    ]
    assert len(mem_events) == 30
    for ev in mem_events[:10]:
        assert ev.payload["value"] == 25.0
    for ev in mem_events[10:20]:
        assert ev.payload["value"] == 95.0
    for ev in mem_events[20:]:
        assert ev.payload["value"] == 25.0


@pytest.mark.asyncio
async def test_connection_exhaustion_telemetry_evidence() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="connection-exhaustion",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    conn_events = [
        ev
        for ev in result.telemetry_events
        if ev.service == "database"
        and ev.event_type == EventType.METRIC
        and ev.payload.get("metric_name") == "simulated_connection_pool_used"
    ]
    assert len(conn_events) == 30
    for ev in conn_events[:10]:
        assert ev.payload["value"] == 5.0
    for ev in conn_events[10:20]:
        assert ev.payload["value"] == 100.0  # 100% capacity saturated
    for ev in conn_events[20:]:
        assert ev.payload["value"] == 5.0


@pytest.mark.asyncio
async def test_dependency_latency_telemetry_evidence() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="dependency-latency",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    lat_events = [
        ev
        for ev in result.telemetry_events
        if ev.service == "payment-service"
        and ev.event_type == EventType.METRIC
        and ev.payload.get("metric_name") == "simulated_effective_latency_ms"
    ]
    assert len(lat_events) == 30
    for ev in lat_events[:10]:
        assert ev.payload["value"] == 0.0
    for ev in lat_events[10:20]:
        assert ev.payload["value"] == 150.0  # +150ms injected latency
    for ev in lat_events[20:]:
        assert ev.payload["value"] == 0.0


@pytest.mark.asyncio
async def test_dependency_failure_telemetry_evidence() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="dependency-failure",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    reach_events = [
        ev
        for ev in result.telemetry_events
        if ev.service == "database"
        and ev.event_type == EventType.METRIC
        and ev.payload.get("metric_name") == "simulated_network_reachable"
    ]
    assert len(reach_events) == 30
    for ev in reach_events[:10]:
        assert ev.payload["value"] == 1.0
    for ev in reach_events[10:20]:
        assert ev.payload["value"] == 0.0  # network unreachable
    for ev in reach_events[20:]:
        assert ev.payload["value"] == 1.0


@pytest.mark.asyncio
async def test_error_rate_spike_telemetry_evidence() -> None:
    runner = ScenarioRunner()
    result = await runner.run(
        scenario="error-rate-spike",
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    err_events = [
        ev
        for ev in result.telemetry_events
        if ev.service == "order-service"
        and ev.event_type == EventType.METRIC
        and ev.payload.get("metric_name") == "simulated_error_rate"
    ]
    assert len(err_events) == 30
    for ev in err_events[:10]:
        assert ev.payload["value"] == 0.0
    for ev in err_events[10:20]:
        assert ev.payload["value"] == 0.45  # error rate spike
    for ev in err_events[20:]:
        assert ev.payload["value"] == 0.0


@pytest.mark.asyncio
async def test_fresh_vs_reused_runner_equivalence() -> None:
    reused_runner = ScenarioRunner()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    # Reused runner runs scenario X, then Y, then X
    await reused_runner.run(scenario="memory-exhaustion", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    res_reused = await reused_runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)

    # Fresh runner runs scenario X
    fresh_runner = ScenarioRunner()
    res_fresh = await fresh_runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)

    assert normalize_run_result(res_reused) == normalize_run_result(res_fresh)


@pytest.mark.asyncio
async def test_same_seed_deterministic_replay_equivalence() -> None:
    runner = ScenarioRunner()
    start_time1 = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    start_time2 = datetime(2026, 9, 24, 18, 45, 0, tzinfo=timezone.utc)

    # Two runs with same scenario + seed, but DIFFERENT run_id and DIFFERENT start_time
    result1 = await runner.run(
        scenario="dependency-latency",
        run_id=uuid.uuid4(),
        run_start_time=start_time1,
        seed=100,
    )
    result2 = await runner.run(
        scenario="dependency-latency",
        run_id=uuid.uuid4(),
        run_start_time=start_time2,
        seed=100,
    )

    assert result1.run_id != result2.run_id
    assert result1.reproducibility_key == result2.reproducibility_key

    norm1 = normalize_run_result(result1)
    norm2 = normalize_run_result(result2)

    assert norm1 == norm2


@pytest.mark.asyncio
async def test_different_seed_behavior() -> None:
    runner = ScenarioRunner()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    result_seed1 = await runner.run(
        scenario="error-rate-spike",
        run_id=uuid.uuid4(),
        run_start_time=start_time,
        seed=1,
    )
    result_seed2 = await runner.run(
        scenario="error-rate-spike",
        run_id=uuid.uuid4(),
        run_start_time=start_time,
        seed=2,
    )

    assert result_seed1.reproducibility_key != result_seed2.reproducibility_key

    norm1 = normalize_run_result(result_seed1)
    norm2 = normalize_run_result(result_seed2)

    assert norm1["reproducibility_key"] != norm2["reproducibility_key"]
    # Jitter and request IDs differ
    assert norm1["telemetry_events"] != norm2["telemetry_events"]


@pytest.mark.asyncio
async def test_runner_reuse_and_state_isolation() -> None:
    runner = ScenarioRunner()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    # 1. Run Scenario A (CPU)
    res_a1 = await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)

    # 2. Run Scenario B (Connection)
    res_b = await runner.run(scenario="connection-exhaustion", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)

    # 3. Run Scenario A again on the same runner instance
    res_a2 = await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)

    norm_a1 = normalize_run_result(res_a1)
    norm_a2 = normalize_run_result(res_a2)

    assert norm_a1 == norm_a2
    assert res_b.scenario_id == "connection-exhaustion"


@pytest.mark.asyncio
async def test_runner_validation_errors() -> None:
    runner = ScenarioRunner()
    valid_id = uuid.uuid4()
    valid_start = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    # Invalid run_id
    with pytest.raises(ValueError, match="run_id must be a UUID"):
        await runner.run(scenario="cpu-saturation", run_id="invalid-id", run_start_time=valid_start, seed=42)  # type: ignore

    # Naive start time
    with pytest.raises(ValueError, match="run_start_time must be timezone-aware"):
        await runner.run(scenario="cpu-saturation", run_id=valid_id, run_start_time=datetime(2026, 9, 24, 0, 0, 0), seed=42)

    # Negative seed
    with pytest.raises(ValueError, match="seed must be non-negative"):
        await runner.run(scenario="cpu-saturation", run_id=valid_id, run_start_time=valid_start, seed=-5)

    # Omitted seed raises TypeError
    with pytest.raises(TypeError):
        await runner.run(scenario="cpu-saturation", run_id=valid_id, run_start_time=valid_start)  # type: ignore


@pytest.mark.asyncio
async def test_explicit_scenario_default_seed_accepted() -> None:
    from app.simulator.scenarios import get_scenario

    runner = ScenarioRunner()
    scenario = get_scenario("cpu-saturation")
    result = await runner.run(
        scenario=scenario,
        run_id=uuid.uuid4(),
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        seed=scenario.default_seed,
    )
    assert result.seed == 42


@pytest.mark.asyncio
async def test_reset_on_rejected_injection() -> None:
    from app.simulator.faults.injector import ConcreteFaultInjector
    from app.simulator.interfaces import FaultInjectionResult

    orig_inject = ConcreteFaultInjector.inject

    async def mock_inject(self: ConcreteFaultInjector, fault: object) -> FaultInjectionResult:
        return FaultInjectionResult(fault_id=uuid.uuid4(), accepted=False, message="Target busy")

    setattr(ConcreteFaultInjector, "inject", mock_inject)

    runner = ScenarioRunner()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    try:
        with pytest.raises(RuntimeError, match="Fault injection failed"):
            await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    finally:
        setattr(ConcreteFaultInjector, "inject", orig_inject)

    res = await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    assert len(res.telemetry_events) > 0


@pytest.mark.asyncio
async def test_reset_on_rejected_recovery() -> None:
    from app.simulator.faults.injector import ConcreteFaultInjector
    from app.simulator.interfaces import FaultInjectionResult

    orig_recover = ConcreteFaultInjector.recover

    async def mock_recover(self: ConcreteFaultInjector, fault_id: uuid.UUID) -> FaultInjectionResult:
        return FaultInjectionResult(fault_id=fault_id, accepted=False, message="Recovery lock failure")

    setattr(ConcreteFaultInjector, "recover", mock_recover)

    runner = ScenarioRunner()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    try:
        with pytest.raises(RuntimeError, match="Fault recovery failed"):
            await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    finally:
        setattr(ConcreteFaultInjector, "recover", orig_recover)

    res = await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    assert len(res.telemetry_events) > 0


@pytest.mark.asyncio
async def test_reset_on_controlled_execution_exception() -> None:
    from app.simulator.faults.injector import ConcreteFaultInjector

    orig_inject = ConcreteFaultInjector.inject

    async def mock_inject(self: ConcreteFaultInjector, fault: object) -> object:
        raise RuntimeError("Controlled execution exception")

    setattr(ConcreteFaultInjector, "inject", mock_inject)

    runner = ScenarioRunner()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    try:
        with pytest.raises(RuntimeError, match="Controlled execution exception"):
            await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    finally:
        setattr(ConcreteFaultInjector, "inject", orig_inject)

    res = await runner.run(scenario="cpu-saturation", run_id=uuid.uuid4(), run_start_time=start_time, seed=42)
    assert len(res.telemetry_events) > 0


def test_package_exports() -> None:
    import app.simulator.runtime.clock as clock_mod
    import app.simulator.runtime.engine as engine_mod
    import app.simulator.runtime.observability as obs_mod
    import app.simulator.runtime.result as result_mod
    import app.simulator.runtime.runner as runner_mod

    assert hasattr(clock_mod, "SimulationClock")
    assert hasattr(engine_mod, "SimulationEngine")
    assert hasattr(obs_mod, "synthesize_service_state_events")
    assert hasattr(result_mod, "ScenarioLifecycleTransition")
    assert hasattr(result_mod, "ScenarioRunResult")
    assert hasattr(result_mod, "normalize_run_result")
    assert hasattr(runner_mod, "ScenarioRunner")
