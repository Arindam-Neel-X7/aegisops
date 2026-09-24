from datetime import datetime, timezone
import uuid
import pytest
from pydantic import ValidationError

from app.simulator.ground_truth import (
    GROUND_TRUTH_NAMESPACE,
    GroundTruthBuilder,
    ResearchRunContext,
    ScenarioRunTruth,
    build_reproducibility_key,
    deterministic_ground_truth_record_id,
)
from app.simulator.interfaces import FaultInjectionResult, FaultType, GroundTruthRecord
from app.simulator.scenarios import get_scenario, list_scenarios
from app.simulator.topology import canonical_service_id


def test_research_run_context_creation() -> None:
    run_id = uuid.uuid4()
    scenario = get_scenario("cpu-saturation")
    start_time = datetime(2026, 9, 24, 10, 0, 0, tzinfo=timezone.utc)

    ctx = ResearchRunContext.from_scenario(
        run_id=run_id,
        scenario=scenario,
        run_start_time=start_time,
        seed=42,
    )

    assert ctx.run_id == run_id
    assert ctx.scenario_id == "cpu-saturation"
    assert ctx.scenario_version == "1.0.0"
    assert ctx.seed == 42
    assert ctx.topology_reference == "canonical"
    assert ctx.topology_version == "1.0.0"
    assert ctx.run_start_time == start_time
    assert len(ctx.reproducibility_key) == 64


def test_run_context_validation_rejects_naive_datetime() -> None:
    scenario = get_scenario("cpu-saturation")
    naive_start = datetime(2026, 9, 24, 10, 0, 0)

    with pytest.raises(ValidationError, match="timezone"):
        ResearchRunContext.from_scenario(
            run_id=uuid.uuid4(),
            scenario=scenario,
            run_start_time=naive_start,
        )


def test_reproducibility_key_determinism() -> None:
    scenario = get_scenario("error-rate-spike")

    key1 = build_reproducibility_key(scenario, seed=42)
    key2 = build_reproducibility_key(scenario, seed=42)
    assert key1 == key2

    # Different seeds produce different keys
    key_other_seed = build_reproducibility_key(scenario, seed=99)
    assert key1 != key_other_seed


def test_run_id_and_start_time_excluded_from_reproducibility_key() -> None:
    scenario = get_scenario("dependency-latency")

    ctx1 = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 1, 0, 0, tzinfo=timezone.utc),
        seed=42,
    )
    ctx2 = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 18, 30, 0, tzinfo=timezone.utc),
        seed=42,
    )

    assert ctx1.run_id != ctx2.run_id
    assert ctx1.run_start_time != ctx2.run_start_time
    assert ctx1.reproducibility_key == ctx2.reproducibility_key


def test_deterministic_ground_truth_record_id() -> None:
    assert isinstance(GROUND_TRUTH_NAMESPACE, uuid.UUID)
    fault_id = uuid.uuid4()
    rep_key = "a" * 64

    id1 = deterministic_ground_truth_record_id(rep_key, fault_id)
    id2 = deterministic_ground_truth_record_id(rep_key, fault_id)
    assert id1 == id2
    assert isinstance(id1, uuid.UUID)


def test_builder_constructs_fault_ground_truth_record() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("cpu-saturation")
    assert scenario.primary_fault is not None

    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)
    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=start_time,
        seed=42,
    )

    inj_result = FaultInjectionResult(
        fault_id=scenario.primary_fault.fault.fault_id,
        accepted=True,
        message="Injected",
    )

    record = builder.build_fault_ground_truth(
        scenario=scenario,
        run_context=run_ctx,
        injection_result=inj_result,
    )

    assert isinstance(record, GroundTruthRecord)
    assert record.fault_id == scenario.primary_fault.fault.fault_id
    assert record.target_service_id == canonical_service_id("order-service")
    assert record.fault_type == FaultType.RESOURCE
    assert record.injected_at == datetime(2026, 9, 24, 0, 0, 10, tzinfo=timezone.utc)
    assert record.expected_root_cause == "simulated CPU saturation on order-service"
    assert record.expected_affected_service_ids == [
        canonical_service_id("order-service"),
        canonical_service_id("api-gateway"),
        canonical_service_id("payment-service"),
        canonical_service_id("inventory-service"),
        canonical_service_id("notification-service"),
    ]
    assert record.metadata["duration_seconds"] == 10.0
    assert record.metadata["parameters"] == {"resource_kind": "cpu", "utilization_pct": 95.0}


def test_builder_rejects_failed_injection() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("cpu-saturation")
    assert scenario.primary_fault is not None

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    inj_failed = FaultInjectionResult(
        fault_id=scenario.primary_fault.fault.fault_id,
        accepted=False,
        message="Target service busy",
    )

    with pytest.raises(ValueError, match="Cannot build GroundTruthRecord for rejected fault injection"):
        builder.build_fault_ground_truth(
            scenario=scenario,
            run_context=run_ctx,
            injection_result=inj_failed,
        )


def test_builder_rejects_fault_id_mismatch() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("cpu-saturation")

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    inj_mismatched = FaultInjectionResult(
        fault_id=uuid.uuid4(),
        accepted=True,
    )

    with pytest.raises(ValueError, match="does not match scenario fault_id"):
        builder.build_fault_ground_truth(
            scenario=scenario,
            run_context=run_ctx,
            injection_result=inj_mismatched,
        )


def test_builder_validates_reproducibility_key() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("cpu-saturation")
    assert scenario.primary_fault is not None

    inj_res = FaultInjectionResult(
        fault_id=scenario.primary_fault.fault.fault_id,
        accepted=True,
    )

    # 1. Arbitrary key -> rejected
    bad_ctx1 = ResearchRunContext(
        run_id=uuid.uuid4(),
        scenario_id=scenario.scenario_id,
        scenario_version=scenario.version,
        seed=scenario.default_seed,
        topology_reference=scenario.topology_reference,
        topology_version=scenario.topology_version,
        workload_identity=scenario.workload.config_identity(),
        workload_config=scenario.workload,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        total_duration_seconds=scenario.total_duration_seconds,
        tick_seconds=scenario.workload.tick_seconds,
        reproducibility_key="f" * 64,  # arbitrary
    )
    with pytest.raises(ValueError, match="reproducibility_key .* does not match expected key"):
        builder.build_fault_ground_truth(scenario, bad_ctx1, inj_res)

    # 2. Key generated for another seed -> rejected
    key_seed99 = build_reproducibility_key(scenario, seed=99)
    bad_ctx2 = ResearchRunContext(
        run_id=uuid.uuid4(),
        scenario_id=scenario.scenario_id,
        scenario_version=scenario.version,
        seed=42,  # seed is 42, but key is for 99
        topology_reference=scenario.topology_reference,
        topology_version=scenario.topology_version,
        workload_identity=scenario.workload.config_identity(),
        workload_config=scenario.workload,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        total_duration_seconds=scenario.total_duration_seconds,
        tick_seconds=scenario.workload.tick_seconds,
        reproducibility_key=key_seed99,
    )
    with pytest.raises(ValueError, match="reproducibility_key .* does not match expected key"):
        builder.build_fault_ground_truth(scenario, bad_ctx2, inj_res)

    # 3. Key generated for another scenario -> rejected
    key_other_scen = build_reproducibility_key(get_scenario("memory-exhaustion"), seed=42)
    bad_ctx3 = ResearchRunContext(
        run_id=uuid.uuid4(),
        scenario_id=scenario.scenario_id,
        scenario_version=scenario.version,
        seed=42,
        topology_reference=scenario.topology_reference,
        topology_version=scenario.topology_version,
        workload_identity=scenario.workload.config_identity(),
        workload_config=scenario.workload,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
        total_duration_seconds=scenario.total_duration_seconds,
        tick_seconds=scenario.workload.tick_seconds,
        reproducibility_key=key_other_scen,
    )
    with pytest.raises(ValueError, match="reproducibility_key .* does not match expected key"):
        builder.build_fault_ground_truth(scenario, bad_ctx3, inj_res)


def test_builder_validates_workload_identity_match() -> None:
    scenario = get_scenario("dependency-latency")

    # Mismatched workload identity raises validation error
    with pytest.raises(ValidationError, match="workload_identity .* does not match"):
        ResearchRunContext(
            run_id=uuid.uuid4(),
            scenario_id=scenario.scenario_id,
            scenario_version=scenario.version,
            seed=42,
            topology_reference=scenario.topology_reference,
            topology_version=scenario.topology_version,
            workload_identity="bad_identity",
            workload_config=scenario.workload,
            run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
            total_duration_seconds=scenario.total_duration_seconds,
            tick_seconds=scenario.workload.tick_seconds,
            reproducibility_key=build_reproducibility_key(scenario, 42),
        )


def test_builder_injected_at_timestamp_validation() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("cpu-saturation")
    assert scenario.primary_fault is not None

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    inj_res = FaultInjectionResult(
        fault_id=scenario.primary_fault.fault.fault_id,
        accepted=True,
    )

    # 1. Exact matching explicit timestamp -> PASS
    matching_time = datetime(2026, 9, 24, 0, 0, 10, tzinfo=timezone.utc)
    rec1 = builder.build_fault_ground_truth(scenario, run_ctx, inj_res, injected_at=matching_time)
    assert rec1.injected_at == matching_time

    # 2. Mismatched explicit timestamp -> rejected
    mismatch_time = datetime(2026, 9, 24, 0, 0, 15, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="does not match expected activation timestamp"):
        builder.build_fault_ground_truth(scenario, run_ctx, inj_res, injected_at=mismatch_time)

    # 3. Naive explicit timestamp -> rejected
    naive_time = datetime(2026, 9, 24, 0, 0, 10)
    with pytest.raises(ValueError, match="injected_at must be timezone-aware"):
        builder.build_fault_ground_truth(scenario, run_ctx, inj_res, injected_at=naive_time)


def test_traffic_surge_rejects_injection_result() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("traffic-surge")

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    unwanted_inj_res = FaultInjectionResult(
        fault_id=uuid.uuid4(),
        accepted=True,
    )

    with pytest.raises(ValueError, match="cannot accept an injection_result"):
        builder.build_run_truth(
            scenario=scenario,
            run_context=run_ctx,
            injection_result=unwanted_inj_res,
        )


def test_traffic_surge_run_truth_has_zero_fault_records() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("traffic-surge")

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    truth = builder.build_run_truth(
        scenario=scenario,
        run_context=run_ctx,
        injection_result=None,
    )

    assert isinstance(truth, ScenarioRunTruth)
    assert truth.scenario_id == "traffic-surge"
    assert truth.fault_ground_truth_records == ()
    assert truth.expected_root_cause == "deterministic ingress traffic surge"
    assert len(truth.expected_affected_services) == 6


def test_bad_deployment_run_truth_contains_error_record_and_system_marker() -> None:
    builder = GroundTruthBuilder()
    scenario = get_scenario("bad-deployment-config")
    assert scenario.primary_fault is not None

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    inj_res = FaultInjectionResult(
        fault_id=scenario.primary_fault.fault.fault_id,
        accepted=True,
    )

    truth = builder.build_run_truth(
        scenario=scenario,
        run_context=run_ctx,
        injection_result=inj_res,
    )

    assert len(truth.fault_ground_truth_records) == 1
    record = truth.fault_ground_truth_records[0]
    assert record.fault_type == FaultType.ERROR
    assert truth.system_marker is not None
    assert truth.system_marker.marker_name == "deployment_changed"


def test_all_eight_scenarios_truth_generation() -> None:
    builder = GroundTruthBuilder()
    start_time = datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    for scenario in list_scenarios():
        run_ctx = ResearchRunContext.from_scenario(
            run_id=uuid.uuid4(),
            scenario=scenario,
            run_start_time=start_time,
        )

        inj_res = None
        if scenario.primary_fault is not None:
            inj_res = FaultInjectionResult(
                fault_id=scenario.primary_fault.fault.fault_id,
                accepted=True,
            )

        truth = builder.build_run_truth(
            scenario=scenario,
            run_context=run_ctx,
            injection_result=inj_res,
        )

        assert truth.scenario_id == scenario.scenario_id
        if scenario.scenario_id == "traffic-surge":
            assert len(truth.fault_ground_truth_records) == 0
        else:
            assert len(truth.fault_ground_truth_records) == 1


def test_serialization_and_reconstruction() -> None:
    scenario = get_scenario("memory-exhaustion")
    assert scenario.primary_fault is not None

    run_ctx = ResearchRunContext.from_scenario(
        run_id=uuid.uuid4(),
        scenario=scenario,
        run_start_time=datetime(2026, 9, 24, 0, 0, 0, tzinfo=timezone.utc),
    )

    ctx_json = run_ctx.model_dump_json()
    reconstructed_ctx = ResearchRunContext.model_validate_json(ctx_json)
    assert reconstructed_ctx == run_ctx

    builder = GroundTruthBuilder()
    inj_res = FaultInjectionResult(
        fault_id=scenario.primary_fault.fault.fault_id,
        accepted=True,
    )
    truth = builder.build_run_truth(scenario, run_ctx, inj_res)

    truth_json = truth.model_dump_json()
    reconstructed_truth = ScenarioRunTruth.model_validate_json(truth_json)
    assert reconstructed_truth == truth


def test_package_exports() -> None:
    import app.simulator.ground_truth as gt_pkg

    expected = [
        "GROUND_TRUTH_NAMESPACE",
        "GroundTruthBuilder",
        "ResearchRunContext",
        "ScenarioRunTruth",
        "build_reproducibility_key",
        "deterministic_ground_truth_record_id",
    ]
    assert hasattr(gt_pkg, "__all__")
    assert sorted(gt_pkg.__all__) == sorted(expected)
    for sym in expected:
        assert hasattr(gt_pkg, sym)
