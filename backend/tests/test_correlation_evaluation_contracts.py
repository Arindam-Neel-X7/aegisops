"""C1 contracts/fixtures only: no runtime grouping or benchmark execution."""

from __future__ import annotations

import copy
import hashlib
import inspect
import json
from pathlib import Path
from uuid import uuid5

import pytest
from pydantic import ValidationError

from app.correlation import evaluation as e
from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import SUPPORTED_ANOMALY_SCHEMA_VERSION
from app.correlation.grouping import (
    CorrelationGroup,
    GroupingEdgeRejectionReason,
    GroupingMemberReference,
    GroupingSignalDisposition,
    GroupingSignalRecord,
    RejectedGroupingEdge,
    compute_group_fingerprint,
    compute_result_fingerprint,
)

REPO = Path(__file__).resolve().parents[2]
DATA = REPO / "research/datasets/phase4_correlation_evaluation"
CONFIG = REPO / "research/configs/phase4_correlation_evaluation.json"


def encode(value):
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode()


@pytest.fixture(scope="module")
def package():
    return e.load_evaluation_inputs(
        CONFIG.read_bytes(),
        (DATA / "runtime.json").read_bytes(),
        (DATA / "truth.json").read_bytes(),
        (DATA / "topologies.json").read_bytes(),
        expected_config_sha256=hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
    )


@pytest.fixture(scope="module")
def oracles():
    return json.loads((DATA / "oracles.json").read_bytes())


def test_frozen_population_and_pins(package, oracles):
    config, runtime, truth = package
    assert config.protocol_fingerprint == e.PROTOCOL_FINGERPRINT
    assert len(runtime.cases) == len(truth.cases) == 16
    assert [len(c.units) for c in runtime.cases] == [
        5,
        1,
        5,
        3,
        5,
        2,
        6,
        2,
        4,
        4,
        3,
        2,
        2,
        2,
        2,
        0,
    ]
    assert len(runtime.cases[10].deliveries) == 5
    assert runtime.cases[11].decisions[-1].watermark_offset_seconds == 41
    assert all(u.event_offset_seconds < 41 - 30 for u in runtime.cases[11].units)
    assert [i for i, c in enumerate(truth.cases) if c.blast_truth] == [8, 12]
    assert len(oracles["examples"]) == 9 and len(oracles["availability_controls"]) == 4
    assert oracles["availability_contract_revision"] == "A1-R1"
    assert (
        len(truth.cases[6].truth_groups) == 1
    )  # Traffic surge is an episode, not an assumed healthy case.


@pytest.mark.parametrize(
    "filename,model",
    [
        ("runtime.json", e.RuntimeInputs),
        ("truth.json", e.TruthInputs),
        ("topologies.json", e.TopologyInputs),
    ],
)
def test_canonical_fixture_round_trip(filename, model):
    raw = (DATA / filename).read_bytes()
    value = e.deserialize_evaluation(model, raw)
    assert e.serialize_evaluation(value) == raw
    assert model.model_validate_json(raw) == value
    with pytest.raises((ValidationError, ValueError)):
        value.schema_version = "other"


def test_config_round_trip():
    raw = CONFIG.read_bytes()
    assert (
        e.serialize_evaluation(e.deserialize_evaluation(e.EvaluationConfig, raw)) == raw
    )


@pytest.mark.parametrize("index", range(16))
def test_runtime_projection_uses_only_observations(package, index):
    _, runtime, _ = package
    case = runtime.cases[index]
    projected = e.project_runtime_case(case)
    run = uuid5(e.NAMESPACE, f"case:{index + 1}:seed:42")
    assert len(projected) == len(case.units)
    for i, (unit, item) in enumerate(zip(case.units, projected), start=1):
        signal = item.signal
        assert signal.signal_id == uuid5(run, f"unit:{i}")
        assert signal.run_id == run
        assert signal.tenant_id == uuid5(e.NAMESPACE, "tenant")
        assert signal.source_event_ids == [uuid5(run, f"event:{i}")]
        assert (
            signal.service == unit.service and signal.metric_or_feature == unit.metric
        )
        assert (
            signal.tags == {}
            and signal.trace_id is None
            and signal.source_event_time_window is None
        )
        assert (
            signal.evidence[0].details == {}
            and signal.evidence[0].observed_value == unit.observed_value
        )
        assert signal.event_time == e.EPOCH + e.timedelta(
            seconds=unit.event_offset_seconds
        )
        assert item.lineage == build_anomaly_reproducibility_lineage(signal)
        assert signal.scenario_id == "controlled-correlation"
        assert signal.schema_version == SUPPORTED_ANOMALY_SCHEMA_VERSION == "1.0"
    assert projected == e.project_runtime_case(case)


def test_truth_permutation_cannot_change_projection(package):
    _, runtime, truth = package
    before = tuple(
        i.model_dump(mode="json") for i in e.project_runtime_case(runtime.cases[8])
    )
    changed = json.loads(e.serialize_evaluation(truth))
    changed["cases"][8]["truth_groups"] = [
        dict(group="other-a", members=["u001", "u003"]),
        dict(group="other-b", members=["u002", "u004"]),
    ]
    changed["cases"][8]["blast_truth"] = None
    changed["cases"][8]["blast_unavailable_reason"] = "unsupported_truth"
    e.deserialize_evaluation(e.TruthInputs, encode(changed))
    assert (
        tuple(
            i.model_dump(mode="json") for i in e.project_runtime_case(runtime.cases[8])
        )
        == before
    )
    assert list(inspect.signature(e.project_runtime_case).parameters) == ["case"]
    with pytest.raises(TypeError):
        e.project_runtime_case(runtime.cases[8], truth=truth)


@pytest.mark.parametrize(
    "metadata",
    [
        "truth_groups",
        "expected_root_cause",
        "tags",
        "evidence",
        "details",
        "scenario_truth",
    ],
)
def test_nested_truth_metadata_rejected(package, metadata):
    _, runtime, _ = package
    data = json.loads(e.serialize_evaluation(runtime))
    data["cases"][0]["units"][0][metadata] = {"truth": "leak"}
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeInputs, encode(data))


@pytest.mark.parametrize("bad", [True, False, 1.0, "1", -1, None])
def test_schedule_integers_are_strict(package, bad):
    data = json.loads(e.serialize_evaluation(package[1]))
    data["cases"][0]["units"][0]["event_offset_seconds"] = bad
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeInputs, encode(data))


@pytest.mark.parametrize(
    "change",
    [
        "duplicate_unit",
        "unknown_delivery",
        "missing_delivery",
        "early_arrival",
        "duplicate_decision",
        "backward_watermark",
        "short_horizon",
        "unknown_field",
    ],
)
def test_runtime_reference_failures(package, change):
    data = json.loads(e.serialize_evaluation(package[1]))
    case = data["cases"][9]  # Two decisions and four units.
    if change == "duplicate_unit":
        case["units"][1]["unit"] = case["units"][0]["unit"]
    if change == "unknown_delivery":
        case["deliveries"][0]["unit"] = "absent"
    if change == "missing_delivery":
        case["deliveries"].pop()
    if change == "early_arrival":
        case["deliveries"][0]["arrival_offset_seconds"] = 0
    if change == "duplicate_decision":
        case["decisions"][1]["decision"] = case["decisions"][0]["decision"]
    if change == "backward_watermark":
        case["decisions"][1]["watermark_offset_seconds"] = 0
    if change == "short_horizon":
        case["decisions"][-1]["arrival_cutoff_seconds"] = 16
    if change == "unknown_field":
        case["truth"] = {}
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeInputs, encode(data))


@pytest.mark.parametrize(
    "change",
    [
        "missing_unit",
        "overlap",
        "unknown_unit",
        "duplicate_group",
        "bad_blast_group",
        "duplicate_blast",
        "missing_reason",
    ],
)
def test_truth_partitions_and_annotations(package, change):
    data = json.loads(e.serialize_evaluation(package[2]))
    case = data["cases"][8]
    if change == "missing_unit":
        case["truth_groups"][0]["members"].pop()
    if change == "overlap":
        case["truth_groups"][1]["members"][0] = "u001"
    if change == "unknown_unit":
        case["truth_groups"][0]["members"][0] = "absent"
    if change == "duplicate_group":
        case["truth_groups"][1]["group"] = "t001"
    if change == "bad_blast_group":
        case["blast_truth"][0]["group"] = "absent"
    if change == "duplicate_blast":
        case["blast_truth"][1]["group"] = "t001"
    if change == "missing_reason":
        case["blast_truth"] = None
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.TruthInputs, encode(data))


@pytest.mark.parametrize(
    "raw",
    [
        b'{"unit":"x","unit":"y"}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":1e999}',
        b"\xff",
        b"{",
    ],
)
def test_malformed_json_fails_before_model(raw):
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeUnit, raw)


def test_bounds_cycles_and_unsafe_instance_revalidation(package):
    case = package[1].cases[0]
    for bad in [
        case.model_copy(update={"truth": []}),
        case.model_copy(
            update={
                "units": (
                    case.units[0].model_copy(update={"event_offset_seconds": True}),
                )
            }
        ),
    ]:
        with pytest.raises((ValueError, ValidationError)):
            e.project_runtime_case(bad)
        with pytest.raises(e.EvaluationContractError):
            e.serialize_evaluation(bad)
    forged = e.RuntimeUnit.model_construct(
        unit="x",
        service="s",
        metric="m",
        observed_value=float("nan"),
        event_offset_seconds=1,
    )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(forged)
    cyclic = {}
    cyclic["cycle"] = cyclic
    with pytest.raises(ValueError):
        e.RuntimeCase.model_validate(cyclic)
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(package[1], limit=8)
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(
            e.RuntimeInputs, (DATA / "runtime.json").read_bytes(), limit=8
        )


def test_pair_budget_boundary_has_no_silent_pruning():
    def payload(n):
        return dict(
            case_id="case-999",
            topology_key="canonical",
            units=[
                dict(
                    unit=f"u{i:03}",
                    service="order-service",
                    metric="m",
                    observed_value=1.0,
                    event_offset_seconds=1,
                )
                for i in range(n)
            ],
            deliveries=[
                dict(delivery=f"d{i:03}", unit=f"u{i:03}", arrival_offset_seconds=2)
                for i in range(n)
            ],
            decisions=[
                dict(
                    decision="q001",
                    arrival_cutoff_seconds=2,
                    watermark_offset_seconds=2,
                )
            ],
        )

    accepted = e.deserialize_evaluation(e.RuntimeCase, encode(payload(91)))
    assert len(accepted.units) * (len(accepted.units) - 1) // 2 == 4095
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeCase, encode(payload(92)))


@pytest.mark.parametrize(
    "field",
    [
        "protocol_fingerprint",
        "source_revision",
        "seed",
        "epoch",
        "timing_rules",
        "target_rules",
    ],
)
def test_config_identity_decisions_are_frozen(field):
    data = json.loads(CONFIG.read_bytes())
    data[field] = True if field == "seed" else "changed"
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.EvaluationConfig, encode(data))


@pytest.mark.parametrize(
    "change",
    [
        "policy_hash",
        "policy_threshold",
        "topology_version",
        "topology_hash",
        "nested_cap_bool",
        "nested_extra",
    ],
)
def test_nested_config_pins_and_types(change):
    data = json.loads(CONFIG.read_bytes())
    if change == "policy_hash":
        data["policies"][0]["configuration_fingerprint"] = "0" * 64
    if change == "policy_threshold":
        data["policies"][0]["config"]["acceptance_threshold"] = 0.9
    if change == "topology_version":
        data["topologies"][0]["pin"]["topology_version"] = "stale"
    if change == "topology_hash":
        data["topologies"][0]["pin"]["snapshot_fingerprint"] = "0" * 64
    if change == "nested_cap_bool":
        data["budgets"]["byte_caps"]["raw"] = True
    if change == "nested_extra":
        data["policies"][0]["config"]["truth"] = {}
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.EvaluationConfig, encode(data))


@pytest.mark.parametrize(
    "path",
    [
        "../truth.json",
        "C:/truth.json",
        "\\\\host\\share",
        "a:b",
        "/absolute",
        "a//b",
        "a/./b",
        "a/../b",
    ],
)
def test_paths_are_not_logical_labels(path):
    with pytest.raises(ValueError):
        e.PortableIdentity(path=path, bytes=1, sha256="0" * 64)
    assert (
        e.PlannedRun.model_fields["run"].annotation is str
    )  # A token, not a path resolver.


def test_input_hash_drift_is_rejected_before_parse():
    with pytest.raises(e.EvaluationContractError, match="identity"):
        e.load_evaluation_inputs(
            CONFIG.read_bytes(),
            b"{broken",
            (DATA / "truth.json").read_bytes(),
            (DATA / "topologies.json").read_bytes(),
            expected_config_sha256=hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
        )


def test_config_needs_an_independent_hash_before_parsing():
    with pytest.raises(e.EvaluationContractError, match="config identity"):
        e.load_evaluation_inputs(
            b"{invalid",
            b"{invalid",
            b"{invalid",
            b"{invalid",
            expected_config_sha256="0" * 64,
        )


def test_planned_population_without_runtime_calls(package, monkeypatch):
    import app.correlation.grouping as grouping
    import app.correlation.scoring as scoring

    def forbidden(*args, **kwargs):
        raise AssertionError("E1 runtime dispatch is not authorized")

    monkeypatch.setattr(grouping, "group_candidate_signals", forbidden)
    monkeypatch.setattr(scoring, "score_candidate_pair", forbidden)
    plans = e.plan_evaluation(CONFIG.read_bytes(), package[1])
    assert len(plans) == len({p.run for p in plans}) == 352
    assert (
        sum(not p.warmup for p in plans) == 320 and sum(p.warmup for p in plans) == 32
    )
    assert plans == e.plan_evaluation(CONFIG.read_bytes(), package[1])


def sidecar_for_control(control, oracles):
    source = next(
        x for x in oracles["examples"] if x["example"] == control["oracle_example"]
    )
    return e.deserialize_evaluation(
        e.TruthCase,
        encode(
            dict(
                case_id=control["case_metrics"]["case_id"],
                truth_status=control["truth_status"],
                universe=source["universe"],
                truth_groups=[
                    dict(group=f"t{i+1:03}", members=g)
                    for i, g in enumerate(source["truth_groups"])
                ],
                background_units=[],
                blast_truth=None,
                blast_unavailable_reason="unsupported_truth",
            )
        ),
    )


@pytest.mark.parametrize("index", range(4))
def test_r1_controls_in_actual_contracts(oracles, index):
    control = oracles["availability_controls"][index]
    record = e.deserialize_evaluation(e.CaseMetrics, encode(control["case_metrics"]))
    truth = sidecar_for_control(control, oracles)
    bound = e.bind_case_metrics(
        record, truth, final_grouping_available=control["name"] != "known_failure"
    )
    assert (
        e.deserialize_evaluation(e.CaseMetrics, e.serialize_evaluation(bound)) == record
    )
    assert (record.TP, record.FP, record.FN) == (
        (0, 0, 0) if index == 2 else (0, 0, 2) if index == 3 else (None, None, None)
    )


R1_MUTATIONS = [
    (0, ["TP"], 0),
    (1, ["FN"], 0),
    (0, ["quality_reason"], "DELETE"),
    (1, ["quality_reason"], "unknown_truth"),
    (2, ["TP"], None),
    (2, ["FP"], None),
    (3, ["FN"], None),
    (2, ["TP"], True),
    (2, ["FP"], 0.0),
    (3, ["FN"], -1),
    (3, ["FN"], 0),
    (0, ["metrics", 0, "numerator"], 0),
    (0, ["metrics", 0, "value"], "0.000000000000"),
    (0, ["matching"], []),
    (1, ["missed_truth_groups"], []),
    (0, ["background_prediction_groups"], []),
    (2, ["quality_reason"], "unknown_truth"),
    (2, ["truth_status"], "unknown"),
    (3, ["missed_truth_groups"], None),
    (2, ["metrics", 0, "numerator"], True),
]


@pytest.mark.parametrize("index,path,value", R1_MUTATIONS)
def test_all_twenty_r1_regressions_in_actual_contracts(oracles, index, path, value):
    control = oracles["availability_controls"][index]
    mutant = copy.deepcopy(control["case_metrics"])
    parent = mutant
    for part in path[:-1]:
        parent = parent[part]
    if value == "DELETE":
        del parent[path[-1]]
    else:
        parent[path[-1]] = value
    assert encode(mutant) != encode(control["case_metrics"])
    with pytest.raises((e.EvaluationContractError, ValidationError)):
        parsed = e.deserialize_evaluation(e.CaseMetrics, encode(mutant))
        e.bind_case_metrics(
            parsed,
            sidecar_for_control(control, oracles),
            final_grouping_available=index != 3,
        )


@pytest.mark.parametrize(
    "stamp",
    [
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00.000000Z\n",
        "2026-02-30T00:00:00.000000Z",
        "2026-01-01T00:00:00.000000+00:00",
    ],
)
def test_raw_timestamp_grammar(stamp):
    data = dict(
        deadline_ns=30_000_000_000,
        elapsed_ns=30_000_000_001,
        last_stage="grouping",
        pid=1,
        terminated=True,
    )
    assert e.deserialize_evaluation(e.TimeoutEvidence, encode(data)).terminated
    with pytest.raises(ValueError):
        e._stamp(stamp)


def raw_failure(package):
    plan = e.plan_evaluation(CONFIG.read_bytes(), package[1])[0]
    return e.RawRun(
        plan=plan,
        utc_start="2026-01-01T00:00:00.000000Z",
        utc_end="2026-01-01T00:00:01.000000Z",
        elapsed_ns=1_000_000_000,
        terminal_status="RUNTIME_ERROR",
        final_grouping_available=False,
        decisions=(),
        enrichments=(),
        timings=(
            e.StageTiming(
                stage="worker_wall",
                decision=None,
                group=None,
                elapsed_ns=1_000_000_000,
                status="observed",
            ),
        ),
        errors=(
            e.SafeError(
                stage="projection",
                exception_type="ValueError",
                message="injected development control",
            ),
        ),
        timeout=None,
        environment=e.Environment(
            python_version="3.11.9",
            platform="development",
            processor="development",
            dependencies=(),
            timer="perf_counter_ns",
            execution_host="development",
        ),
        sources_before=package[0].source_identities,
        sources_after=package[0].source_identities,
    )


def test_raw_failure_round_trip_preserves_status(package):
    record = raw_failure(package)
    assert e.deserialize_evaluation(e.RawRun, e.serialize_evaluation(record)) == record
    for update in [
        dict(terminal_status="SUCCESS"),
        dict(final_grouping_available=True),
        dict(utc_end="2025-01-01T00:00:00.000000Z"),
        dict(elapsed_ns=True),
        dict(sources_after=()),
        dict(timings=()),
    ]:
        with pytest.raises(e.EvaluationContractError):
            e.serialize_evaluation(record.model_copy(update=update))


def test_raw_population_rejects_missing_records(package):
    plan = e.plan_evaluation(CONFIG.read_bytes(), package[1])
    with pytest.raises(ValueError):
        e.RawOutcomes(
            schema_version=e.SCHEMA_VERSION,
            config_sha256=hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
            planned=plan,
            runs=(),
        )


def test_bounded_recursion_rejects_deep_metadata():
    value = {}
    for _ in range(34):
        value = {"nested": value}
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeCase, encode(value))


def argument_payload(case, config):
    projected = e.project_runtime_case(case)
    ids = sorted({i.signal.signal_id for i in projected}, key=lambda u: u.bytes)
    body = dict(
        decision=case.decisions[-1].decision,
        runtime_inputs=[i.model_dump(mode="json") for i in projected],
        pair_order=[
            [str(a), str(b)] for index, a in enumerate(ids) for b in ids[index + 1 :]
        ],
        configuration_fingerprint=config.policies[0].configuration_fingerprint,
        topology_fingerprint=config.topologies[0].pin.snapshot_fingerprint,
        watermark=(
            e.EPOCH + e.timedelta(seconds=case.decisions[-1].watermark_offset_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    )
    body["argument_fingerprint"] = hashlib.sha256(
        encode(body).rstrip(b"\n")
    ).hexdigest()
    return body


def test_actual_projection_can_round_trip_through_capture_contract(package):
    config, runtime, _ = package
    value = e.deserialize_evaluation(
        e.CapturedArguments, encode(argument_payload(runtime.cases[0], config))
    )
    assert len(value.runtime_inputs) == 5 and len(value.pair_order) == 10
    assert (
        e.deserialize_evaluation(e.CapturedArguments, e.serialize_evaluation(value))
        == value
    )


@pytest.mark.parametrize(
    "change",
    [
        "tags",
        "details",
        "extra_signal",
        "lineage",
        "pair_omission",
        "argument_hash",
        "numeric_bool",
    ],
)
def test_capture_rejects_truth_routes_and_forged_payloads(package, change):
    body = argument_payload(package[1].cases[0], package[0])
    signal = body["runtime_inputs"][0]["signal"]
    if change == "tags":
        signal["tags"] = {"truth": "incident-a"}
    if change == "details":
        signal["evidence"][0]["details"] = {"expected_root_cause": "x"}
    if change == "extra_signal":
        signal["truth_group"] = "a"
    if change == "lineage":
        body["runtime_inputs"][0]["lineage"]["semantic_fingerprint"] = "0" * 64
    if change == "pair_omission":
        body["pair_order"].pop()
    if change == "numeric_bool":
        signal["anomaly_score"] = True
    without_hash = {k: v for k, v in body.items() if k != "argument_fingerprint"}
    body["argument_fingerprint"] = (
        "0" * 64
        if change == "argument_hash"
        else hashlib.sha256(encode(without_hash).rstrip(b"\n")).hexdigest()
    )
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.CapturedArguments, encode(body))


def test_empty_success_is_a_real_typed_result_not_a_failure(package):
    config, runtime, _ = package
    case = runtime.cases[-1]
    args = e.deserialize_evaluation(
        e.CapturedArguments, encode(argument_payload(case, config))
    )
    now = e.EPOCH + e.timedelta(seconds=1)
    payload = dict(
        schema_version="1.0.0",
        policy_version="v1.0",
        configuration_fingerprint=config.policies[0].configuration_fingerprint,
        topology_snapshot_fingerprint=config.topologies[0].pin.snapshot_fingerprint,
        event_time_watermark=now.isoformat(),
        groups=[],
        rejected_edges=[],
        signal_records=[],
    )
    fingerprint = hashlib.sha256(encode(payload).rstrip(b"\n")).hexdigest()
    result = e.CorrelationGroupingResult(
        schema_version="1.0.0",
        policy_version="v1.0",
        configuration_fingerprint=config.policies[0].configuration_fingerprint,
        topology_snapshot_fingerprint=config.topologies[0].pin.snapshot_fingerprint,
        event_time_watermark=now,
        groups=(),
        rejected_edges=(),
        signal_records=(),
        result_fingerprint=fingerprint,
    )
    outcome = e.DecisionOutcome(
        decision="q001",
        arguments=args,
        pair_evidence=(),
        grouping_result=result,
        status="success",
        errors=(),
        timings=tuple(
            e.StageTiming(
                stage=stage,
                decision="q001",
                group=None,
                elapsed_ns=1,
                status="observed",
            )
            for stage in ("projection", "scoring", "grouping")
        ),
    )
    plan = next(
        p
        for p in e.plan_evaluation(CONFIG.read_bytes(), runtime)
        if p.case_id == case.case_id
    )
    record = raw_failure(package).model_copy(
        update=dict(
            plan=plan,
            decisions=(outcome,),
            terminal_status="SUCCESS",
            final_grouping_available=True,
            errors=(),
        )
    )
    parsed = e.deserialize_evaluation(e.RawRun, e.serialize_evaluation(record))
    assert (
        parsed.final_grouping_available
        and parsed.decisions[0].grouping_result.groups == ()
    )


def test_complete_raw_population_can_round_trip_without_dispatch(package):
    config, runtime, _ = package
    plans = e.plan_evaluation(CONFIG.read_bytes(), runtime)
    base = raw_failure(package)
    records = tuple(base.model_copy(update={"plan": p}) for p in plans)
    outcomes = e.RawOutcomes(
        schema_version=e.SCHEMA_VERSION,
        config_sha256=hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
        planned=plans,
        runs=records,
    )
    raw = e.serialize_evaluation(outcomes, limit=e.MAX_RAW_BYTES)
    parsed = e.deserialize_evaluation(e.RawOutcomes, raw, limit=e.MAX_RAW_BYTES)
    assert e.bind_raw_outcomes(parsed, CONFIG.read_bytes(), runtime) == outcomes
    bad = outcomes.model_copy(update={"runs": outcomes.runs[:-1] + (outcomes.runs[0],)})
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(bad, limit=e.MAX_RAW_BYTES)


def test_success_status_cannot_override_observed_failure(oracles):
    control = oracles["availability_controls"][2]
    record = e.deserialize_evaluation(e.CaseMetrics, encode(control["case_metrics"]))
    with pytest.raises(e.EvaluationContractError):
        e.bind_case_metrics(
            record,
            sidecar_for_control(control, oracles),
            final_grouping_available=False,
        )


RAW_REPRODUCTIONS = [
    (
        "result_watermark_mismatch",
        ("decisions", 0, "grouping_result", "event_time_watermark"),
    ),
    ("result_policy_mismatch", ("decisions", 0, "grouping_result", "policy_version")),
    ("success_with_decision_error", ("decisions", 0, "errors")),
    ("success_with_run_error", ("terminal_status",)),
    ("dropped_signal_records", ("decisions", 0, "grouping_result", "signal_records")),
    ("missing_group_enrichment", ("enrichments",)),
]


def structural_result(
    arguments, groups=(), records=(), evidence=(), *, watermark=None, policy="v1.0"
):
    """Construct native development data, never execute a grouping algorithm."""
    included = {record.signal_id for record in records if record.included}
    rejections = tuple(
        RejectedGroupingEdge(
            left_signal_id=p.left_signal_id,
            right_signal_id=p.right_signal_id,
            reason=GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED,
            source_evidence=p,
            detail="development structural rejection",
        )
        for p in evidence
        if p.left_signal_id in included and p.right_signal_id in included
    )
    data = dict(
        schema_version="1.0.0",
        policy_version=policy,
        configuration_fingerprint=arguments.configuration_fingerprint,
        topology_snapshot_fingerprint=arguments.topology_fingerprint,
        event_time_watermark=watermark
        or e.datetime.strptime(arguments.watermark, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=e.timezone.utc
        ),
        groups=groups,
        rejected_edges=rejections,
        signal_records=records,
    )
    temporary = e.CorrelationGroupingResult.model_construct(
        **data, result_fingerprint="0" * 64
    )
    return e.CorrelationGroupingResult(
        **data, result_fingerprint=compute_result_fingerprint(temporary)
    )


def structural_success(package, index):
    config, runtime, _ = package
    case = runtime.cases[index]
    arguments = e.deserialize_evaluation(
        e.CapturedArguments, encode(argument_payload(case, config))
    )
    groups = []
    records = []
    for item in sorted(
        arguments.runtime_inputs, key=lambda i: i.signal.signal_id.bytes
    ):
        signal = item.signal
        member = GroupingMemberReference(
            signal_id=signal.signal_id,
            semantic_fingerprint=item.semantic_fingerprint,
            service=signal.service,
            event_time=signal.event_time,
        )
        body = dict(
            schema_version="1.0.0",
            policy_version="v1.0",
            configuration_fingerprint=arguments.configuration_fingerprint,
            topology_snapshot_fingerprint=arguments.topology_fingerprint,
            members=(member,),
            accepted_evidence=(),
            started_at=signal.event_time,
            last_seen_at=signal.event_time,
            time_span_seconds=0.0,
            topology_diameter_hops=0,
        )
        temp = CorrelationGroup.model_construct(**body, group_fingerprint="0" * 64)
        group = CorrelationGroup(
            **body, group_fingerprint=compute_group_fingerprint(temp)
        )
        groups.append(group)
        records.append(
            GroupingSignalRecord(
                signal_id=signal.signal_id,
                semantic_fingerprint=item.semantic_fingerprint,
                included=True,
                duplicate_count=1,
                late=False,
                disposition=GroupingSignalDisposition.GROUPED,
                group_fingerprint=group.group_fingerprint,
            )
        )
    evidence = tuple(
        e.CorrelationEvidence(
            left_signal_id=a,
            right_signal_id=b,
            temporal_score=0.0,
            service_score=0.0,
            topology_score=0.0,
            symptom_score=0.0,
            context_score=0.0,
            weighted_score=0.0,
            accepted=False,
            policy_version="v1.0",
            reason="development structural pair",
        )
        for a, b in arguments.pair_order
    )
    result = structural_result(arguments, tuple(groups), tuple(records), evidence)
    outcome = e.DecisionOutcome(
        decision=arguments.decision,
        arguments=arguments,
        pair_evidence=evidence,
        grouping_result=result,
        status="success",
        errors=(),
        timings=tuple(
            e.StageTiming(
                stage=stage,
                decision=arguments.decision,
                group=None,
                elapsed_ns=1,
                status="observed",
            )
            for stage in ("projection", "scoring", "grouping")
        ),
    )
    enrichments = tuple(
        e.EnrichmentOutcome(
            group=g.group_fingerprint,
            aggregate=None,
            blast_context=None,
            handoff=None,
            status="failed",
            errors=(
                e.SafeError(
                    stage="aggregate",
                    exception_type="ValueError",
                    message="development enrichment failure",
                ),
            ),
            timings=(),
        )
        for g in groups
    )
    plan = next(
        p
        for p in e.plan_evaluation(CONFIG.read_bytes(), runtime)
        if p.case_id == case.case_id
    )
    return raw_failure(package).model_copy(
        update=dict(
            plan=plan,
            decisions=(outcome,),
            enrichments=enrichments,
            terminal_status="PARTIAL_FAILURE" if enrichments else "SUCCESS",
            final_grouping_available=True,
            errors=(),
        )
    )


def raw_reproduction(package, name):
    index = (
        0
        if name == "dropped_signal_records"
        else 1
        if name == "missing_group_enrichment"
        else 15
    )
    clean = structural_success(package, index)
    decision = clean.decisions[0]
    error = e.SafeError(
        stage="grouping",
        exception_type="RuntimeError",
        message="development native error, not a warning",
    )
    if name == "result_watermark_mismatch":
        result = structural_result(
            decision.arguments, watermark=e.EPOCH + e.timedelta(seconds=600)
        )
        mutant = clean.model_copy(
            update={
                "decisions": (decision.model_copy(update={"grouping_result": result}),)
            }
        )
    elif name == "result_policy_mismatch":
        result = structural_result(decision.arguments, policy="wrong-policy")
        mutant = clean.model_copy(
            update={
                "decisions": (decision.model_copy(update={"grouping_result": result}),)
            }
        )
    elif name == "success_with_decision_error":
        mutant = clean.model_copy(
            update={"decisions": (decision.model_copy(update={"errors": (error,)}),)}
        )
    elif name == "success_with_run_error":
        mutant = clean.model_copy(update={"errors": (error,)})
    elif name == "dropped_signal_records":
        result = structural_result(decision.arguments)
        mutant = clean.model_copy(
            update={
                "decisions": (decision.model_copy(update={"grouping_result": result}),),
                "enrichments": (),
                "terminal_status": "SUCCESS",
            }
        )
    elif name == "missing_group_enrichment":
        mutant = clean.model_copy(
            update={"enrichments": (), "terminal_status": "SUCCESS"}
        )
    else:
        raise AssertionError(name)
    return clean, mutant


def bind_development_record(record, package):
    """Exercise the same serialize/decode/full-population binding as the review."""
    parsed = e.deserialize_evaluation(e.RawRun, e.serialize_evaluation(record))
    plans = e.plan_evaluation(CONFIG.read_bytes(), package[1])
    base = raw_failure(package)
    records = tuple(
        parsed if p == parsed.plan else base.model_copy(update={"plan": p})
        for p in plans
    )
    root = e.RawOutcomes(
        schema_version=e.SCHEMA_VERSION,
        config_sha256=hashlib.sha256(CONFIG.read_bytes()).hexdigest(),
        planned=plans,
        runs=records,
    )
    decoded = e.deserialize_evaluation(
        e.RawOutcomes,
        e.serialize_evaluation(root, limit=e.MAX_RAW_BYTES),
        limit=e.MAX_RAW_BYTES,
    )
    return e.bind_raw_outcomes(decoded, CONFIG.read_bytes(), package[1])


def diagnostic_locations(error):
    current = error
    while current is not None:
        if isinstance(current, ValidationError):
            return [tuple(r["loc"]) for r in current.errors()]
        current = current.__cause__
    return []


@pytest.mark.parametrize("name,field", RAW_REPRODUCTIONS)
def test_review_raw_contract_regressions(package, name, field):
    clean, mutant = raw_reproduction(package, name)
    bind_development_record(clean, package)
    with pytest.raises((e.EvaluationContractError, ValidationError)) as rejected:
        bind_development_record(mutant, package)
    assert field in diagnostic_locations(rejected.value)


def rehashed_result(result, **updates):
    data = {
        key: getattr(result, key)
        for key in type(result).model_fields
        if key != "result_fingerprint"
    }
    data.update(updates)
    temp = e.CorrelationGroupingResult.model_construct(
        **data, result_fingerprint="0" * 64
    )
    return e.CorrelationGroupingResult(
        **data, result_fingerprint=compute_result_fingerprint(temp)
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("duplicate_count", 2),
        ("late", True),
        ("disposition", GroupingSignalDisposition.DUPLICATE_IGNORED),
    ],
)
def test_signal_records_cannot_change_captured_dispositions(package, field, value):
    clean = structural_success(package, 1)
    decision = clean.decisions[0]
    result = decision.grouping_result
    record = result.signal_records[0].model_copy(update={field: value})
    changed = rehashed_result(result, signal_records=(record,))
    mutant = clean.model_copy(
        update={
            "decisions": (decision.model_copy(update={"grouping_result": changed}),)
        }
    )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(mutant)


def test_recorded_scorer_evidence_cannot_be_dropped_from_rejections(package):
    clean = structural_success(package, 0)
    decision = clean.decisions[0]
    changed = rehashed_result(decision.grouping_result, rejected_edges=())
    mutant = clean.model_copy(
        update={
            "decisions": (decision.model_copy(update={"grouping_result": changed}),)
        }
    )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(mutant)


def test_run_errors_are_preserved_as_partial_failure(package):
    clean = structural_success(package, 15)
    error = e.SafeError(
        stage="load",
        exception_type="RuntimeError",
        message="development earlier-stage error",
    )
    partial = clean.model_copy(
        update={"errors": (error,), "terminal_status": "PARTIAL_FAILURE"}
    )
    parsed = e.deserialize_evaluation(e.RawRun, e.serialize_evaluation(partial))
    assert parsed.errors == (error,) and parsed.final_grouping_available


@pytest.mark.parametrize("change", ["duplicate", "orphan", "missing_partial"])
def test_enrichments_must_cover_exactly_the_final_group_keys(package, change):
    clean = structural_success(package, 1)
    item = clean.enrichments[0]
    if change == "duplicate":
        mutant = clean.model_copy(update={"enrichments": (item, item)})
    elif change == "orphan":
        mutant = clean.model_copy(
            update={"enrichments": (item.model_copy(update={"group": "0" * 64}),)}
        )
    else:
        mutant = clean.model_copy(
            update={"enrichments": (), "terminal_status": "PARTIAL_FAILURE"}
        )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(mutant)


def timeout_control(package, deadline=30_000_000_000):
    clean = structural_success(package, 1)
    elapsed = 31_000_000_000
    return clean.model_copy(
        update=dict(
            enrichments=(),
            terminal_status="TIMEOUT",
            elapsed_ns=elapsed,
            utc_end="2026-01-01T00:00:31.000000Z",
            timeout=e.TimeoutEvidence(
                deadline_ns=deadline,
                elapsed_ns=elapsed,
                last_stage="aggregate",
                pid=1,
                terminated=True,
            ),
            timings=(
                e.StageTiming(
                    stage="worker_wall",
                    decision=None,
                    group=None,
                    elapsed_ns=elapsed,
                    status="observed",
                ),
            ),
        )
    )


def test_explicit_timeout_retains_censored_enrichment_without_success(package):
    run = timeout_control(package)
    parsed = e.deserialize_evaluation(e.RawRun, e.serialize_evaluation(run))
    assert (
        parsed.terminal_status == "TIMEOUT"
        and parsed.final_grouping_available
        and not parsed.enrichments
    )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(run.model_copy(update={"terminal_status": "SUCCESS"}))


def test_timeout_cannot_override_the_frozen_worker_deadline(package):
    with pytest.raises(ValidationError) as rejected:
        bind_development_record(timeout_control(package, deadline=1), package)
    assert ("timeout", "deadline_ns") in diagnostic_locations(rejected.value)


def complete_enrichment_record(package):
    # These existing pure APIs construct unit-test data; no evaluation adapter runs.
    from app.correlation.aggregate import create_or_update_incident
    from app.correlation.blast_radius import BlastRadiusPolicy
    from app.correlation.handoff import build_phase5_handoff
    from app.correlation.models import CorrelationWindowState, TopologySnapshotReference
    from app.correlation.topology import TraversalDirection

    clean = structural_success(package, 1)
    decision = clean.decisions[0]
    group = decision.grouping_result.groups[0]
    as_of = decision.grouping_result.event_time_watermark
    snapshot = package[0].topologies[0].snapshot
    inputs = {i.signal.signal_id: i for i in decision.arguments.runtime_inputs}
    aggregate = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=as_of,
        desired_correlation_state=CorrelationWindowState.COLLECTING,
        existing_aggregate=None,
    ).aggregate
    ref = TopologySnapshotReference(
        topology_id=snapshot.version.topology_id,
        topology_version=snapshot.version.topology_version,
        snapshot_fingerprint=snapshot.snapshot_fingerprint,
        captured_at=e.EPOCH,
        provider="canonical",
    )
    handoff = build_phase5_handoff(
        incident=aggregate,
        snapshot=snapshot,
        expected_topology=ref,
        policy=BlastRadiusPolicy(
            direction=TraversalDirection.BOTH,
            max_hops=3,
            max_services=64,
            max_edges=128,
        ),
        as_of=as_of,
    )
    item = e.EnrichmentOutcome(
        group=group.group_fingerprint,
        aggregate=aggregate,
        blast_context=handoff.topology_context,
        handoff=handoff,
        status="success",
        errors=(),
        timings=(),
    )
    return clean.model_copy(
        update={"enrichments": (item,), "terminal_status": "SUCCESS"}
    )


def test_late_delayed_records_do_not_require_ineligible_pair_rejections(package):
    """Native grouping excludes late endpoints from its rejected-edge ledger."""
    decision = structural_success(package, 0).decisions[0]
    body = decision.arguments.model_dump(mode="json")
    body["watermark"] = "2026-01-01T00:10:00.000000Z"
    body.pop("argument_fingerprint")
    body["argument_fingerprint"] = hashlib.sha256(
        encode(body).rstrip(b"\n")
    ).hexdigest()
    arguments = e.deserialize_evaluation(e.CapturedArguments, encode(body))
    records = tuple(
        record.model_copy(
            update=dict(
                late=True,
                included=False,
                disposition=GroupingSignalDisposition.LATE_DELAYED,
                group_fingerprint=None,
            )
        )
        for record in decision.grouping_result.signal_records
    )
    result = structural_result(
        arguments, records=records, evidence=decision.pair_evidence
    )
    assert len(result.signal_records) == 5 and len(decision.pair_evidence) == 10
    assert not result.groups and not result.rejected_edges
    value = decision.model_copy(
        update=dict(arguments=arguments, grouping_result=result)
    )
    parsed = e.deserialize_evaluation(e.DecisionOutcome, e.serialize_evaluation(value))
    assert parsed == value


def test_valid_complete_enrichment_round_trips_and_binds(package):
    clean = complete_enrichment_record(package)
    bound = bind_development_record(clean, package)
    observed = next(r for r in bound.runs if r.plan == clean.plan)
    assert observed.terminal_status == "SUCCESS" and len(observed.enrichments) == 1


def test_orphaned_context_is_rejected_even_when_a_failure_is_reported(package):
    clean = complete_enrichment_record(package)
    item = clean.enrichments[0]
    changed = item.model_copy(
        update={
            "aggregate": None,
            "handoff": None,
            "status": "failed",
            "errors": (
                e.SafeError(
                    stage="aggregate",
                    exception_type="ValueError",
                    message="development orphan",
                ),
            ),
        }
    )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(
            clean.model_copy(
                update={"enrichments": (changed,), "terminal_status": "PARTIAL_FAILURE"}
            )
        )
