"""M1 oracle, independence and complete-population development reductions."""

from __future__ import annotations

import copy
from fractions import Fraction
import hashlib
import itertools
import json
from pathlib import Path
import random
import runpy

import pytest

from app.correlation import evaluation as e
from app.correlation import evaluation_metrics as m

REPO = Path(__file__).resolve().parents[2]
DATA = REPO / "research/datasets/phase4_correlation_evaluation"
CONFIG = REPO / "research/configs/phase4_correlation_evaluation.json"
WORKSPACE = REPO.parent


@pytest.fixture(scope="module")
def independent():
    return runpy.run_path(str(REPO / "scripts/verify_phase4_correlation_evaluation.py"))


@pytest.fixture(scope="module")
def oracles():
    return json.loads((DATA / "oracles.json").read_bytes())["examples"]


def oracle_arguments(example):
    t = [
        dict(group=f"t{i}", members=tuple(group))
        for i, group in enumerate(example["truth_groups"])
    ]
    p = [
        dict(
            fingerprint=hashlib.sha256(",".join(sorted(group)).encode()).hexdigest(),
            members=tuple(group),
            exposure=None,
        )
        for group in example["predicted_groups"]
    ]
    return dict(
        case_id="case-901",
        policy="reference",
        universe=tuple(example["universe"]),
        services=example["services"],
        truth_status=example["truth_status"],
        truth_groups=t,
        background_units=tuple(
            sorted(
                set(example["universe"])
                - {u for g in example["truth_groups"] for u in g}
            )
        ),
        blast_truth=None,
        predictions=p,
        status=example["terminal_status"],
        final_available=example["final_grouping_available"],
    )


@pytest.mark.parametrize("index", range(9))
def test_all_frozen_oracles_and_independent_case_reduction(oracles, independent, index):
    example = oracles[index]
    args = oracle_arguments(example)
    result = m.case_quality(**args)
    assert result.model_dump(mode="json") == independent["case_quality"](
        **json.loads(json.dumps(args))
    )
    rows = {row.name: row for row in result.metrics}
    for name, expected in example["expected_metrics"].items():
        actual = rows[name].model_dump()
        assert {
            k: actual[k] for k in ["numerator", "denominator", "value", "reason"]
        } == expected
    if example["hand_counts"] is not None:
        assert (result.TP, result.FP, result.FN) == tuple(
            example["hand_counts"][k] for k in ["TP", "FP", "FN"]
        )
        for field, metric in [
            ("exact_matches", "exact_matches"),
            ("over_groups", "over_groups"),
            ("under_groups", "under_groups"),
            ("missed_truth_groups", "missed_truth_group_count"),
        ]:
            assert rows[metric].numerator == example["hand_counts"][field]
        assert [
            ("".join(row.truth_members), "".join(row.prediction_members))
            for row in result.matching
        ] == [tuple(pair) for pair in example["expected_matching"]]
    else:
        assert result.TP is result.FP is result.FN is None
        assert (
            result.matching
            is result.missed_truth_groups
            is result.background_prediction_groups
            is None
        )


@pytest.mark.parametrize("truth_status", ["unknown", "ambiguous"])
def test_unknown_and_ambiguous_do_not_erase_observed_fields(
    oracles, independent, truth_status
):
    args = oracle_arguments(oracles[-1])
    args["truth_status"] = truth_status
    result = m.case_quality(**args)
    assert result.model_dump(mode="json") == independent["case_quality"](
        **json.loads(json.dumps(args))
    )
    assert all(
        row.value is None
        and row.numerator is None
        and row.reason == truth_status + "_truth"
        for row in result.metrics
    )
    assert result.status == "SUCCESS" and result.unassigned_units == ()


@pytest.mark.parametrize(
    "truth",
    [[("a", "b"), ("c", "d")], [("a", "b", "c"), ("d", "e")], [("a",), ("b",), ("c",)]],
)
def test_matching_is_exact_and_label_order_independent(truth, independent):
    predictions = (
        [("a", "c"), ("b", "d")]
        if len(set().union(*map(set, truth))) >= 4
        else [("a", "b"), ("c",)]
    )
    expected = m.matching(truth, predictions)
    assert expected == independent["matching"](truth, predictions)
    for t in itertools.permutations(truth):
        for p in itertools.permutations(predictions):
            assert m.matching(list(t), list(p)) == expected


def test_matching_tie_break_uses_unit_tuples_not_ids():
    assert m.matching([("a", "b"), ("c", "d")], [("a", "c"), ("b", "d")]) == (
        (("a", "b"), ("a", "c")),
        (("c", "d"), ("b", "d")),
    )


@pytest.mark.parametrize("seed", range(20))
def test_dynamic_matching_matches_exhaustive_small_assignment(seed, independent):
    rng = random.Random(seed)
    units = list("abcdef")
    truth = [tuple(u for u in units if (ord(u) + seed) % 3 == i) for i in range(3)]
    buckets = [[] for _ in range(3)]
    for u in units:
        if rng.randrange(4):
            buckets[rng.randrange(3)].append(u)
    predictions = [tuple(bucket) for bucket in buckets if bucket]
    options = [(Fraction(), ())]
    for size in range(1, min(len(truth), len(predictions)) + 1):
        for selected in itertools.combinations(truth, size):
            for assigned in itertools.permutations(predictions, size):
                pairs = tuple(sorted(zip(selected, assigned)))
                if all(set(a) & set(b) for a, b in pairs):
                    options.append(
                        (
                            sum(
                                (
                                    Fraction(len(set(a) & set(b)), len(set(a) | set(b)))
                                    for a, b in pairs
                                ),
                                Fraction(),
                            ),
                            pairs,
                        )
                    )
    expected = min(options, key=lambda value: (-value[0], value[1]))[1]
    assert (
        m.matching(truth, predictions)
        == independent["matching"](truth, predictions)
        == expected
    )


def empty_development_success():
    helpers = runpy.run_path(
        str(REPO / "backend/tests/test_correlation_evaluation_contracts.py")
    )
    package = helpers["package"].__wrapped__()
    raw = helpers["structural_success"](package, 15)
    plan = next(
        p
        for p in e.plan_evaluation(CONFIG.read_bytes(), package[1])
        if p.case_id == "case-016" and not p.warmup
    )
    return raw.model_copy(update={"plan": plan})


@pytest.mark.parametrize("scope", ["controlled_compute", "worker_wall", "enrichment"])
def test_latency_exact_statistics_censoring_and_failure(independent, scope):
    base = empty_development_success()
    runs = []
    for duration in range(10, 30):
        d = base.decisions[0]
        timings = tuple(
            t.model_copy(
                update={"elapsed_ns": duration - 2 if t.stage == "projection" else 1}
            )
            for t in d.timings
        )
        runs.append(
            base.model_copy(
                update={
                    "decisions": (d.model_copy(update={"timings": timings}),),
                    "elapsed_ns": duration,
                    "timings": (
                        base.timings[0].model_copy(update={"elapsed_ns": duration}),
                    ),
                }
            )
        )
    timeout = base.model_copy(
        update={
            "terminal_status": "TIMEOUT",
            "elapsed_ns": 30_000_000_000,
            "timings": (
                base.timings[0].model_copy(
                    update={"elapsed_ns": 30_000_000_000, "status": "censored"}
                ),
            ),
            "timeout": e.TimeoutEvidence(
                deadline_ns=30_000_000_000,
                elapsed_ns=30_000_000_000,
                last_stage="grouping",
                pid=123,
                terminated=True,
            ),
        }
    )
    runs.append(timeout)
    result = m.latency_summary("case-016", "reference", scope, runs)
    expected = independent["latency_summary"](
        "case-016", "reference", scope, [run.model_dump(mode="json") for run in runs]
    )
    assert result.model_dump(mode="json") == expected
    assert result.censored == 1 and result.n == 20
    if scope != "enrichment":
        assert (
            result.minimum_ns,
            result.median_ns,
            result.p95_ns,
            result.maximum_ns,
        ) == (10, "19.500000000000", 28, 29)


def test_enrichment_failure_does_not_erase_grouping_latency(independent):
    base = empty_development_success()
    degraded = base.model_copy(
        update={
            "terminal_status": "PARTIAL_FAILURE",
            "errors": (
                e.SafeError(
                    stage="aggregate",
                    exception_type="RuntimeError",
                    message="development only",
                ),
            ),
        }
    )
    assert (
        m.latency_summary("case-016", "reference", "controlled_compute", [degraded]).n
        == 1
    )
    assert (
        m.latency_summary("case-016", "reference", "worker_wall", [degraded]).failed
        == 1
    )


def test_latency_does_not_count_warmups(independent):
    raw = empty_development_success()
    plan = raw.plan.model_copy(
        update={"warmup": True, "run": "case-016.reference.warmup.000"}
    )
    warmup = raw.model_copy(update={"plan": plan})
    result = m.latency_summary("case-016", "reference", "controlled_compute", [warmup])
    assert result.n == result.failed == result.censored == 0
    assert result.model_dump(mode="json") == independent["latency_summary"](
        "case-016", "reference", "controlled_compute", [warmup.model_dump(mode="json")]
    )


def blast_arguments():
    return dict(
        case_id="case-901",
        policy="reference",
        universe=("a", "b", "c"),
        services={"a": "s1", "b": "s2", "c": "s3"},
        truth_status="complete",
        truth_groups=[
            dict(group="t1", members=("a", "b")),
            dict(group="t2", members=("c",)),
        ],
        background_units=(),
        blast_truth=[
            dict(
                group="t1",
                services=("s1", "s2", "s4"),
                topology_fingerprint="a" * 64,
                direction="both",
                max_hops=3,
                annotation_source="development independent enumeration",
            )
        ],
        predictions=[
            dict(fingerprint="1" * 64, members=("a", "b"), exposure=("s1", "s5")),
            dict(fingerprint="2" * 64, members=("c",), exposure=("s3",)),
        ],
        status="SUCCESS",
        final_available=True,
    )


@pytest.mark.parametrize(
    "change",
    [
        "partial_support",
        "unmatched_prediction",
        "failed_enrichment",
        "grouping_failure",
        "full_support",
    ],
)
def test_blast_matching_failure_and_partial_truth_coverage(independent, change):
    args = blast_arguments()
    if change == "unmatched_prediction":
        args["predictions"] = [
            dict(fingerprint="1" * 64, members=("a",), exposure=("s1",)),
            dict(fingerprint="2" * 64, members=("b",), exposure=("s2",)),
        ]
    if change == "failed_enrichment":
        args["predictions"][0]["exposure"] = None
        args["status"] = "PARTIAL_FAILURE"
    if change == "grouping_failure":
        args.update(predictions=[], status="RUNTIME_ERROR", final_available=False)
    if change == "full_support":
        args["blast_truth"].append(
            {**args["blast_truth"][0], "group": "t2", "services": ("s3",)}
        )
    result = m.case_quality(**args)
    assert result.model_dump(mode="json") == independent["case_quality"](
        **json.loads(json.dumps(args))
    )
    rows = {row.name: row for row in result.metrics}
    if change == "unmatched_prediction":
        assert rows["blast_precision"].reason == "incomplete_blast_truth"
    if change == "grouping_failure":
        assert (
            rows["blast_FN"].numerator == 3
            and rows["blast_recall"].value == "0.000000000000"
        )


@pytest.mark.parametrize(
    "change",
    [
        "unknown_member",
        "overlap",
        "duplicate_member",
        "duplicate_fingerprint",
        "output_without_final",
    ],
)
def test_invalid_predictions_fail_before_metric_serialization(change):
    args = blast_arguments()
    if change == "unknown_member":
        args["predictions"][0]["members"] = ("absent",)
    if change == "overlap":
        args["predictions"][1]["members"] = ("a",)
    if change == "duplicate_member":
        args["predictions"][0]["members"] = ("a", "a")
    if change == "duplicate_fingerprint":
        args["predictions"][1]["fingerprint"] = "1" * 64
    if change == "output_without_final":
        args["final_available"] = False
    with pytest.raises((ValueError, e.EvaluationContractError)):
        m.case_quality(**args)


@pytest.mark.parametrize(
    "numerator,denominator,expected",
    [
        (1, 3, "0.333333333333"),
        (1, 2000000000000, "0.000000000000"),
        (3, 2000000000000, "0.000000000002"),
        (900000000001, 1000000000000, "0.900000000001"),
    ],
)
def test_exact_half_even_formatting(independent, numerator, denominator, expected):
    value = Fraction(numerator, denominator)
    assert m.decimal12(value) == independent["decimal12"](value) == expected


@pytest.mark.parametrize(
    "field,value", [("threshold", "x" * 129), ("scope", "x" * 257), ("measured", "NaN")]
)
def test_target_wire_bounds_and_finite_values(field, value):
    data = dict(
        policy="reference",
        target="live_grouping_latency",
        threshold="<5s",
        scope="live_orchestration",
        measured=None,
        status="NOT_MEASURED",
        reason="outside_task_4.11_scope",
    )
    data[field] = value
    with pytest.raises(ValueError):
        m.TargetRow.model_validate(data)


@pytest.fixture(scope="module")
def development_raw():
    """Portable, synthetic failure records: no host evidence or runtime dispatch."""
    config_raw = CONFIG.read_bytes()
    runtime_raw = (DATA / "runtime.json").read_bytes()
    runtime = e.deserialize_evaluation(e.RuntimeInputs, runtime_raw)
    plans = e.plan_evaluation(config_raw, runtime)
    runs = []
    config = e.deserialize_evaluation(e.EvaluationConfig, config_raw)
    environment = e.Environment(
        python_version="3.11.9",
        platform="unit-test",
        processor="unit-test",
        dependencies=(),
        timer="perf_counter_ns",
        execution_host="unit-test",
    )
    for plan in plans:
        runs.append(
            e.RawRun(
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
                        stage="load",
                        exception_type="ValueError",
                        message="Explicit unit-test failure fixture",
                    ),
                ),
                timeout=None,
                environment=environment,
                sources_before=config.source_identities,
                sources_after=config.source_identities,
            )
        )
    value = e.RawOutcomes(
        schema_version=e.SCHEMA_VERSION,
        config_sha256=hashlib.sha256(config_raw).hexdigest(),
        planned=plans,
        runs=tuple(runs),
    )
    raw = e.serialize_evaluation(value, limit=e.MAX_RAW_BYTES)
    return (
        config_raw,
        runtime_raw,
        (DATA / "truth.json").read_bytes(),
        (DATA / "topologies.json").read_bytes(),
        raw,
    )


@pytest.fixture(scope="module")
def comparison(development_raw):
    config, runtime, truth, topology, raw = development_raw
    return m.calculate_comparison(
        config,
        runtime,
        truth,
        topology,
        raw,
        expected_config_sha256=hashlib.sha256(config).hexdigest(),
        expected_raw_sha256=hashlib.sha256(raw).hexdigest(),
    )


def test_complete_population_and_targets_stay_explicit(comparison):
    assert len(comparison.cases) == 32 and len(comparison.targets) == 6
    for policy in ["reference", "threshold_sensitivity"]:
        assert (
            sum(
                row.count
                for row in comparison.statuses
                if row.policy == policy and row.scope == "measured"
            )
            == 160
        )
        assert (
            sum(
                row.count
                for row in comparison.statuses
                if row.policy == policy and row.scope == "warmup"
            )
            == 16
        )
        assert (
            sum(
                row.count
                for row in comparison.statuses
                if row.policy == policy and row.scope == "case"
            )
            == 16
        )
        live = next(
            row
            for row in comparison.targets
            if row.policy == policy and row.target == "live_grouping_latency"
        )
        assert live.measured is None and live.status == "NOT_MEASURED"
        controlled = next(
            row
            for row in comparison.targets
            if row.policy == policy and row.target == "controlled_budget"
        )
        assert (
            controlled.status == "NOT_DEMONSTRATED"
        )  # both missing-context fixtures remain


def test_case_quality_is_not_multiplied_by_repetitions(comparison):
    for policy in ["reference", "threshold_sensitivity"]:
        numerator = sum(
            next(row.numerator for row in case.metrics if row.name == "exact_partition")
            for case in comparison.cases
            if case.policy == policy
        )
        row = next(
            row
            for row in comparison.policy_micro
            if row.scope == f"policy.{policy}.micro" and row.name == "exact_partition"
        )
        assert (row.numerator, row.denominator) == (numerator, 16)


def test_full_independent_recomputation_is_byte_exact(
    independent, development_raw, comparison
):
    config, runtime, truth, _, raw = development_raw
    actual = independent["recompute"](
        json.loads(config), json.loads(runtime), json.loads(truth), json.loads(raw)
    )
    assert e.serialize_evaluation(comparison, limit=8 * 1024 * 1024) == independent[
        "canonical"
    ](actual)


def test_raw_identity_drift_rejects_before_reduction(development_raw):
    config, runtime, truth, topology, raw = development_raw
    with pytest.raises(e.EvaluationContractError, match="raw identity"):
        m.calculate_comparison(
            config,
            runtime,
            truth,
            topology,
            raw + b" ",
            expected_config_sha256=hashlib.sha256(config).hexdigest(),
            expected_raw_sha256=hashlib.sha256(raw).hexdigest(),
        )


def test_nonfinite_and_unsafe_instances_fail_serialization(comparison):
    altered = comparison.model_copy(
        update={
            "coverage": (comparison.coverage[0].model_copy(update={"numerator": True}),)
        }
    )
    with pytest.raises(e.EvaluationContractError):
        e.serialize_evaluation(altered)
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(m.ProcessedComparison, b'{"schema_version":NaN}')


def test_semantic_repeat_drift_is_rejected(development_raw):
    config, runtime, truth, topology, raw = development_raw
    value = e.deserialize_evaluation(e.RawOutcomes, raw, limit=e.MAX_RAW_BYTES)
    index = next(
        i
        for i, r in enumerate(value.runs)
        if not r.plan.warmup and r.plan.repetition == 1
    )
    candidate = value.runs[index]
    changed = candidate.model_copy(
        update={
            "errors": (
                e.SafeError(
                    stage="load",
                    exception_type="RuntimeError",
                    message="development semantic alteration",
                ),
            ),
        }
    )
    runs = list(value.runs)
    runs[index] = changed
    mutated = e.serialize_evaluation(
        value.model_copy(update={"runs": tuple(runs)}), limit=e.MAX_RAW_BYTES
    )
    with pytest.raises(e.EvaluationContractError, match="semantically inconsistent"):
        m.calculate_comparison(
            config,
            runtime,
            truth,
            topology,
            mutated,
            expected_config_sha256=hashlib.sha256(config).hexdigest(),
            expected_raw_sha256=hashlib.sha256(mutated).hexdigest(),
        )


def test_complete_raw_timeout_keeps_committed_final_grouping(
    development_raw, independent
):
    config, runtime, truth, topology, raw = development_raw
    value = e.deserialize_evaluation(e.RawOutcomes, raw, limit=e.MAX_RAW_BYTES)
    success = empty_development_success()
    runs = []
    for run in value.runs:
        if run.plan.case_id == "case-016" and run.plan.policy == "reference":
            run = success.model_copy(
                update={
                    "plan": run.plan,
                    "terminal_status": "TIMEOUT",
                    "elapsed_ns": 30_000_000_000,
                    "utc_end": "2026-01-01T00:00:30.000000Z",
                    "timings": (
                        e.StageTiming(
                            stage="worker_wall",
                            decision=None,
                            group=None,
                            elapsed_ns=30_000_000_000,
                            status="censored",
                        ),
                    ),
                    "timeout": e.TimeoutEvidence(
                        deadline_ns=30_000_000_000,
                        elapsed_ns=30_000_000_000,
                        last_stage="handoff",
                        pid=1,
                        terminated=True,
                    ),
                }
            )
        runs.append(run)
    changed = e.serialize_evaluation(
        value.model_copy(update={"runs": tuple(runs)}), limit=e.MAX_RAW_BYTES
    )
    processed = m.calculate_comparison(
        config,
        runtime,
        truth,
        topology,
        changed,
        expected_config_sha256=hashlib.sha256(config).hexdigest(),
        expected_raw_sha256=hashlib.sha256(changed).hexdigest(),
    )
    expected = independent["recompute"](
        json.loads(config), json.loads(runtime), json.loads(truth), json.loads(changed)
    )
    assert e.serialize_evaluation(processed, limit=8 * 1024 * 1024) == independent[
        "canonical"
    ](expected)
    case = next(
        case
        for case in processed.cases
        if case.case_id == "case-016" and case.policy == "reference"
    )
    assert (
        case.status == "TIMEOUT"
        and next(row.value for row in case.metrics if row.name == "exact_partition")
        == "1.000000000000"
    )
    latency = next(
        row
        for row in processed.latency
        if row.case_id == "case-016"
        and row.policy == "reference"
        and row.scope == "controlled_compute"
    )
    assert latency.censored == 10 and latency.n == 0
    conditioned = next(
        row
        for row in processed.policy_micro
        if row.scope == "policy.reference.grouping_success.micro"
        and row.name == "exact_partition"
    )
    assert (conditioned.numerator, conditioned.denominator) == (1, 1)


@pytest.mark.parametrize(
    "change,pointer",
    [
        ("raw_source_drift", r"\$\.runs\[1\]\.sources_after\[0\]\.sha256"),
        ("raw_coordinated_source_drift", r"\$\.runs\[1\]\.sources_before\[0\]\.sha256"),
        ("raw_invalid_repetition", r"\$\.planned\[1\]\.repetition"),
        ("raw_coordinated_repetition", r"\$\.planned\[1\]\.repetition"),
    ],
)
def test_review_false_accepts_reject_at_raw_field(
    development_raw, independent, change, pointer
):
    config, runtime, truth, _, raw = map(json.loads, development_raw)
    if "source" in change:
        raw["runs"][1]["sources_after"][0]["sha256"] = "0" * 64
        if change == "raw_coordinated_source_drift":
            raw["runs"][1]["sources_before"][0]["sha256"] = "0" * 64
    else:
        for plan in [raw["planned"][1], raw["runs"][1]["plan"]]:
            plan["repetition"] = 99
            if change == "raw_coordinated_repetition":
                plan["run"] = "case-001.reference.measured.099"
    with pytest.raises(ValueError, match=pointer):
        independent["recompute"](config, runtime, truth, raw)


@pytest.mark.parametrize(
    "change",
    [
        "bool_bytes",
        "bad_hash",
        "unsafe_path",
        "duplicate",
        "missing_field",
        "extra_field",
    ],
)
def test_source_identity_fields_are_strict(independent, change):
    source = dict(
        path="aegisops/backend/app/correlation/scoring.py", bytes=123, sha256="a" * 64
    )
    values = [source]
    if change == "bool_bytes":
        source["bytes"] = True
    if change == "bad_hash":
        source["sha256"] = "A" * 64
    if change == "unsafe_path":
        source["path"] = "../source.py"
    if change == "duplicate":
        values.append(copy.deepcopy(source))
    if change == "missing_field":
        source.pop("bytes")
    if change == "extra_field":
        source["trusted"] = True
    with pytest.raises(ValueError, match=r"sources\[\d\]"):
        independent["_source_identities"](values, "sources")


@pytest.mark.parametrize(
    "change",
    ["label", "warmup_index", "warmup_bool", "reordered", "missing", "run_plan_only"],
)
def test_manifest_is_derived_not_trusted(development_raw, independent, change):
    config, runtime, truth, _, raw = map(json.loads, development_raw)
    if change == "label":
        raw["planned"][1]["run"] = raw["runs"][1]["plan"]["run"] = "wrong-label"
    if change == "warmup_index":
        raw["planned"][0]["repetition"] = raw["runs"][0]["plan"]["repetition"] = 1
    if change == "warmup_bool":
        raw["planned"][0]["warmup"] = raw["runs"][0]["plan"]["warmup"] = 1
    if change == "reordered":
        raw["planned"][1:3] = raw["planned"][1:3][::-1]
        raw["runs"][1:3] = raw["runs"][1:3][::-1]
    if change == "missing":
        raw["planned"].pop()
        raw["runs"].pop()
    if change == "run_plan_only":
        raw["runs"][1]["plan"]["repetition"] = 99
    with pytest.raises(ValueError, match=r"\$\.(planned|runs)"):
        independent["recompute"](config, runtime, truth, raw)


def test_success_conditioned_metrics_have_own_denominators(oracles, independent):
    rows = []
    for index in range(16):
        args = oracle_arguments(oracles[0 if index < 2 else 1 if index < 15 else 4])
        args["case_id"] = f"case-{901 + index}"
        rows.append(m.case_quality(**args))
    successful = rows[:-1]
    weights = {
        r.case_id: max(
            next(v.denominator for v in r.metrics if v.name == "exact_group_precision"),
            next(v.denominator for v in r.metrics if v.name == "exact_group_recall"),
        )
        for r in rows
    }
    independent_micro, independent_macro = independent["_aggregate"](
        [r.model_dump(mode="json") for r in successful],
        "reference",
        weights,
        population=16,
        success_conditioned=True,
    )
    for macro, independent_rows in [
        (False, independent_micro),
        (True, independent_macro),
    ]:
        actual = [
            m._aggregate(
                successful,
                name,
                "reference",
                macro,
                population=16,
                success_conditioned=True,
            )
            for name in m.METRICS
        ]
        assert [v.model_dump(mode="json") for v in actual] == independent_rows
        exact = next(v for v in actual if v.name == "exact_partition")
        assert Fraction(exact.numerator, exact.denominator) == Fraction(2, 15)
        assert (exact.planned, exact.eligible, exact.observed, exact.failed) == (
            16,
            15,
            15,
            1,
        )
    operational = m._aggregate(rows, "exact_partition", "reference", False)
    assert (operational.numerator, operational.denominator) == (2, 16)
    assert operational.scope == "policy.reference.micro"
    conditional_f1 = m._aggregate(
        successful,
        "pair_f1",
        "reference",
        False,
        population=16,
        success_conditioned=True,
    )
    assert (conditional_f1.numerator, conditional_f1.denominator) == (60, 112)
    assert (
        m._aggregate(rows, "pair_f1", "reference", False).numerator,
        m._aggregate(rows, "pair_f1", "reference", False).denominator,
    ) == (60, 114)


def test_no_successful_grouping_is_unavailable_not_perfect(independent):
    micro, macro = independent["_aggregate"](
        [], "reference", {}, population=16, success_conditioned=True
    )
    for is_macro, expected in [(False, micro), (True, macro)]:
        actual = [
            m._aggregate(
                [], name, "reference", is_macro, population=16, success_conditioned=True
            )
            for name in m.METRICS
        ]
        assert [v.model_dump(mode="json") for v in actual] == expected
        assert all(
            v.value is None
            and v.reason == "no_successful_grouping_cases"
            and v.planned == v.failed == 16
            and v.eligible == v.observed == 0
            for v in actual
        )


def test_conditioned_unknown_truth_still_has_no_quality(oracles, independent):
    rows = [m.case_quality(**oracle_arguments(oracles[-1]))]
    micro, macro = independent["_aggregate"](
        [r.model_dump(mode="json") for r in rows],
        "reference",
        {rows[0].case_id: 1},
        population=1,
        success_conditioned=True,
    )
    for is_macro, expected in [(False, micro), (True, macro)]:
        actual = [
            m._aggregate(
                rows,
                name,
                "reference",
                is_macro,
                population=1,
                success_conditioned=True,
            )
            for name in m.METRICS
        ]
        assert [v.model_dump(mode="json") for v in actual] == expected
        assert all(v.value is None and v.eligible == v.observed == 0 for v in actual)


def test_grouping_condition_is_independent_of_enrichment_status():
    base = empty_development_success()
    assert m._grouping_succeeded(base)
    assert m._grouping_succeeded(
        base.model_copy(update={"terminal_status": "PARTIAL_FAILURE"})
    )
    assert m._grouping_succeeded(base.model_copy(update={"terminal_status": "TIMEOUT"}))
    assert not m._grouping_succeeded(
        base.model_copy(update={"final_grouping_available": False})
    )
    failed = base.decisions[0].model_copy(
        update={"status": "failed", "grouping_result": None}
    )
    assert not m._grouping_succeeded(
        base.model_copy(update={"decisions": (failed, *base.decisions)})
    )
