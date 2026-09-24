import json
import pytest
from pydantic import ValidationError

from app.simulator.interfaces import ServiceNode, ServiceTopology, ServiceType
from app.simulator.topology import build_canonical_topology
from app.simulator.workload import (
    CANONICAL_REQUEST_ROUTE,
    DEFAULT_HOP_LATENCIES_MS,
    DeterministicWorkloadGenerator,
    SyntheticRequest,
    WorkloadConfig,
    WorkloadProfile,
    derive_child_seed,
)


def test_hop_latencies_and_custom_topology() -> None:
    assert len(DEFAULT_HOP_LATENCIES_MS) == 7
    assert sum(DEFAULT_HOP_LATENCIES_MS.values()) == 65.0

    # Test generator with explicitly passed canonical topology instance
    explicit_topology = build_canonical_topology()
    config = WorkloadConfig(base_requests_per_second=2.0)
    gen = DeterministicWorkloadGenerator(config=config, topology=explicit_topology, seed=10)
    reqs = gen.requests_for_tick(0)
    assert len(reqs) == 2


def test_child_seed_derivation_deterministic() -> None:
    seed1 = derive_child_seed(42, "workload")
    seed2 = derive_child_seed(42, "workload")
    assert seed1 == seed2
    assert isinstance(seed1, int)
    assert seed1 >= 0

    # Different stream names produce different child seeds
    seed_fault = derive_child_seed(42, "faults")
    assert seed1 != seed_fault

    # Different master seeds produce different child seeds
    seed_other = derive_child_seed(43, "workload")
    assert seed1 != seed_other


def test_child_seed_derivation_validation() -> None:
    with pytest.raises(ValueError, match="master_seed must be non-negative"):
        derive_child_seed(-1, "workload")

    with pytest.raises(ValueError, match="stream_name must not be empty"):
        derive_child_seed(42, "")


def test_workload_config_baseline_defaults() -> None:
    config = WorkloadConfig()
    assert config.profile_name == WorkloadProfile.HEALTHY_BASELINE
    assert config.base_requests_per_second == 10.0
    assert config.tick_seconds == 1.0
    assert config.total_duration_seconds == 60.0
    assert config.surge_start_seconds is None


def test_workload_config_surge_validation() -> None:
    # Valid surge configuration
    config = WorkloadConfig(
        profile_name=WorkloadProfile.TRAFFIC_SURGE,
        base_requests_per_second=10.0,
        surge_start_seconds=10.0,
        surge_duration_seconds=20.0,
        surge_requests_per_second=100.0,
        total_duration_seconds=60.0,
    )
    assert config.surge_start_seconds == 10.0
    assert config.surge_duration_seconds == 20.0
    assert config.surge_requests_per_second == 100.0

    # Missing surge_start_seconds
    with pytest.raises(ValidationError, match="surge_start_seconds is required"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            surge_duration_seconds=10.0,
            surge_requests_per_second=100.0,
        )

    # Missing surge_duration_seconds
    with pytest.raises(ValidationError, match="surge_duration_seconds is required"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            surge_start_seconds=10.0,
            surge_requests_per_second=100.0,
        )

    # Missing surge_requests_per_second
    with pytest.raises(ValidationError, match="surge_requests_per_second is required"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            surge_start_seconds=10.0,
            surge_duration_seconds=10.0,
        )

    # Surge start exceeds total duration
    with pytest.raises(ValidationError, match="surge_start_seconds must be strictly less than total_duration_seconds"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            surge_start_seconds=60.0,
            surge_duration_seconds=10.0,
            surge_requests_per_second=100.0,
            total_duration_seconds=60.0,
        )

    # Surge window exceeds total duration
    with pytest.raises(ValidationError, match="surge window .* cannot exceed total_duration_seconds"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            surge_start_seconds=50.0,
            surge_duration_seconds=20.0,
            surge_requests_per_second=100.0,
            total_duration_seconds=60.0,
        )


def test_workload_config_negative_numbers_rejected() -> None:
    with pytest.raises(ValidationError):
        WorkloadConfig(base_requests_per_second=-5.0)

    with pytest.raises(ValidationError):
        WorkloadConfig(tick_seconds=0.0)

    with pytest.raises(ValidationError):
        WorkloadConfig(total_duration_seconds=-10.0)


def test_generator_topology_validation() -> None:
    config = WorkloadConfig()
    # Topology missing database
    broken_topology = ServiceTopology(
        services=[
            ServiceNode(name="client", service_type=ServiceType.EXTERNAL),
            ServiceNode(name="api-gateway", service_type=ServiceType.API),
        ]
    )
    with pytest.raises(ValueError, match="missing required canonical services"):
        DeterministicWorkloadGenerator(config=config, topology=broken_topology, seed=42)


def test_generator_negative_seed_rejected() -> None:
    config = WorkloadConfig()
    with pytest.raises(ValueError, match="seed must be non-negative"):
        DeterministicWorkloadGenerator(config=config, seed=-1)


def test_healthy_baseline_request_generation() -> None:
    config = WorkloadConfig(base_requests_per_second=5.0, tick_seconds=1.0)
    generator = DeterministicWorkloadGenerator(config=config, seed=123)
    config_id = config.config_identity()

    requests = generator.requests_for_tick(tick_index=0)
    assert len(requests) == 5

    for seq, req in enumerate(requests):
        assert req.tick_index == 0
        assert req.sequence_in_tick == seq
        assert req.request_id == f"req-{config_id}-0000007b-000000-{seq:04d}"
        assert req.trace_id == f"trace-{config_id}-0000007b-000000-{seq:04d}"
        assert req.source_service == "client"
        assert req.target_service == "api-gateway"
        assert req.route == CANONICAL_REQUEST_ROUTE
        assert req.outcome == "success"
        assert req.status_code == 200
        assert req.error is None
        assert req.accumulated_latency_ms >= 65.0
        assert req.accumulated_latency_ms <= 70.0


def test_traffic_surge_rate_schedule() -> None:
    config = WorkloadConfig(
        profile_name=WorkloadProfile.TRAFFIC_SURGE,
        base_requests_per_second=10.0,
        surge_start_seconds=5.0,
        surge_duration_seconds=3.0,
        surge_requests_per_second=50.0,
        total_duration_seconds=20.0,
    )
    generator = DeterministicWorkloadGenerator(config=config, seed=42)

    # Ticks 0..4: base rate (10 RPS)
    for tick in range(5):
        reqs = generator.requests_for_tick(tick_index=tick)
        assert len(reqs) == 10

    # Ticks 5..7: surge rate (50 RPS)
    for tick in range(5, 8):
        reqs = generator.requests_for_tick(tick_index=tick)
        assert len(reqs) == 50

    # Ticks 8..10: return to base rate (10 RPS)
    for tick in range(8, 11):
        reqs = generator.requests_for_tick(tick_index=tick)
        assert len(reqs) == 10


def test_fractional_rate_accumulation() -> None:
    # 2.5 RPS -> Ticks should produce: 2, 3, 2, 3, ...
    config = WorkloadConfig(base_requests_per_second=2.5, tick_seconds=1.0)
    generator = DeterministicWorkloadGenerator(config=config, seed=42)

    counts = [len(generator.requests_for_tick(tick_index=t)) for t in range(6)]
    assert counts == [2, 3, 2, 3, 2, 3]
    assert sum(counts) == 15


def test_same_seed_exact_replayability() -> None:
    config = WorkloadConfig(
        profile_name=WorkloadProfile.TRAFFIC_SURGE,
        base_requests_per_second=4.0,
        surge_start_seconds=2.0,
        surge_duration_seconds=2.0,
        surge_requests_per_second=12.0,
        total_duration_seconds=10.0,
    )
    gen_a = DeterministicWorkloadGenerator(config=config, seed=999)
    gen_b = DeterministicWorkloadGenerator(config=config, seed=999)

    for tick in range(6):
        reqs_a = gen_a.requests_for_tick(tick_index=tick)
        reqs_b = gen_b.requests_for_tick(tick_index=tick)

        assert len(reqs_a) == len(reqs_b)
        for r_a, r_b in zip(reqs_a, reqs_b):
            assert r_a.request_id == r_b.request_id
            assert r_a.trace_id == r_b.trace_id
            assert r_a.accumulated_latency_ms == r_b.accumulated_latency_ms
            assert r_a.route == r_b.route
            assert r_a.outcome == r_b.outcome
            assert r_a.status_code == r_b.status_code


def test_generator_reset() -> None:
    config = WorkloadConfig(base_requests_per_second=2.5)
    gen = DeterministicWorkloadGenerator(config=config, seed=555)

    batch_1 = [gen.requests_for_tick(t) for t in range(4)]

    # Reset generator
    gen.reset()

    batch_2 = [gen.requests_for_tick(t) for t in range(4)]

    assert [[r.model_dump() for r in b] for b in batch_1] == [[r.model_dump() for r in b] for b in batch_2]


def test_different_seed_variation() -> None:
    config = WorkloadConfig(base_requests_per_second=5.0)
    gen1 = DeterministicWorkloadGenerator(config=config, seed=1)
    gen2 = DeterministicWorkloadGenerator(config=config, seed=2)

    reqs1 = gen1.requests_for_tick(tick_index=0)
    reqs2 = gen2.requests_for_tick(tick_index=0)

    # Counts and structural routes match
    assert len(reqs1) == len(reqs2) == 5
    assert reqs1[0].route == reqs2[0].route

    # Request IDs and jitter latencies differ
    assert reqs1[0].request_id != reqs2[0].request_id
    latencies1 = [r.accumulated_latency_ms for r in reqs1]
    latencies2 = [r.accumulated_latency_ms for r in reqs2]
    assert latencies1 != latencies2


def test_serialization_and_reconstruction() -> None:
    config = WorkloadConfig(
        profile_name=WorkloadProfile.TRAFFIC_SURGE,
        base_requests_per_second=15.0,
        surge_start_seconds=5.0,
        surge_duration_seconds=10.0,
        surge_requests_per_second=150.0,
    )
    dumped_json = config.model_dump_json()
    reconstructed_config = WorkloadConfig.model_validate_json(dumped_json)
    assert reconstructed_config == config

    gen = DeterministicWorkloadGenerator(config=config, seed=77)
    reqs = gen.requests_for_tick(tick_index=0)
    req = reqs[0]

    req_json = req.model_dump_json()
    reconstructed_req = SyntheticRequest.model_validate_json(req_json)
    assert reconstructed_req == req
    assert json.loads(req_json)["outcome"] == "success"


def test_generator_invalid_tick_arguments() -> None:
    config = WorkloadConfig()
    gen = DeterministicWorkloadGenerator(config=config, seed=42)

    with pytest.raises(ValueError, match="tick_index must be non-negative"):
        gen.requests_for_tick(tick_index=-1)

    with pytest.raises(ValueError, match="simulation_offset_seconds must be non-negative"):
        gen.requests_for_tick(tick_index=0, simulation_offset_seconds=-0.5)


def test_workload_config_surge_strictly_greater_than_baseline() -> None:
    # Greater than baseline -> PASS
    config = WorkloadConfig(
        profile_name=WorkloadProfile.TRAFFIC_SURGE,
        base_requests_per_second=10.0,
        surge_start_seconds=5.0,
        surge_duration_seconds=5.0,
        surge_requests_per_second=20.0,
        total_duration_seconds=30.0,
    )
    assert config.surge_requests_per_second == 20.0

    # Equal to baseline -> rejected
    with pytest.raises(ValidationError, match="surge_requests_per_second must be strictly greater than base_requests_per_second"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            base_requests_per_second=10.0,
            surge_start_seconds=5.0,
            surge_duration_seconds=5.0,
            surge_requests_per_second=10.0,
            total_duration_seconds=30.0,
        )

    # Below baseline -> rejected
    with pytest.raises(ValidationError, match="surge_requests_per_second must be strictly greater than base_requests_per_second"):
        WorkloadConfig(
            profile_name=WorkloadProfile.TRAFFIC_SURGE,
            base_requests_per_second=10.0,
            surge_start_seconds=5.0,
            surge_duration_seconds=5.0,
            surge_requests_per_second=5.0,
            total_duration_seconds=30.0,
        )


def test_workload_identity_stability_and_determinism() -> None:
    config1 = WorkloadConfig(base_requests_per_second=15.0)
    config2 = WorkloadConfig(base_requests_per_second=15.0)
    assert config1.config_identity() == config2.config_identity()
    assert len(config1.config_identity()) == 8

    # Stable across serialization & reconstruction
    reconstructed = WorkloadConfig.model_validate_json(config1.model_dump_json())
    assert reconstructed.config_identity() == config1.config_identity()

    # Different config produces different identity
    config3 = WorkloadConfig(base_requests_per_second=25.0)
    assert config3.config_identity() != config1.config_identity()


def test_request_identity_differs_by_config() -> None:
    config_a = WorkloadConfig(base_requests_per_second=5.0)
    config_b = WorkloadConfig(base_requests_per_second=10.0)

    gen_a = DeterministicWorkloadGenerator(config=config_a, seed=42)
    gen_b = DeterministicWorkloadGenerator(config=config_b, seed=42)

    reqs_a = gen_a.requests_for_tick(0)
    reqs_b = gen_b.requests_for_tick(0)

    # Different configs with same seed produce distinct request and trace IDs
    assert reqs_a[0].request_id != reqs_b[0].request_id
    assert reqs_a[0].trace_id != reqs_b[0].trace_id

    # Same config with same seed produces identical request and trace IDs
    gen_a2 = DeterministicWorkloadGenerator(config=config_a, seed=42)
    reqs_a2 = gen_a2.requests_for_tick(0)
    assert reqs_a[0].request_id == reqs_a2[0].request_id
    assert reqs_a[0].trace_id == reqs_a2[0].trace_id


def test_workload_package_exports() -> None:
    import app.simulator.workload as wl_pkg

    expected = [
        "CANONICAL_REQUEST_ROUTE",
        "DEFAULT_HOP_LATENCIES_MS",
        "DeterministicWorkloadGenerator",
        "SyntheticRequest",
        "WorkloadConfig",
        "WorkloadProfile",
        "derive_child_seed",
    ]
    assert hasattr(wl_pkg, "__all__")
    assert sorted(wl_pkg.__all__) == sorted(expected)
    for sym in expected:
        assert hasattr(wl_pkg, sym)
