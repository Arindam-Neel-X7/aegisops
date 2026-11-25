"""Pure A1-R1 reductions over authenticated raw outcomes; no runtime dispatch."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
import hashlib
from itertools import combinations
from typing import Annotated, Any, Literal

from pydantic import Field, StringConstraints, model_validator

from app.correlation import evaluation as e

COUNT_METRICS = (
    "exact_matches",
    "over_groups",
    "under_groups",
    "extra_truth_intersections",
    "partially_covered_truth_groups",
    "background_group_count",
    "unassigned_unit_count",
    "missed_truth_group_count",
    "blast_TP",
    "blast_FP",
    "blast_FN",
)
METRICS = tuple(
    sorted(
        (
            *COUNT_METRICS,
            "pair_precision",
            "pair_recall",
            "pair_f1",
            "exact_group_precision",
            "exact_group_recall",
            "exact_partition",
            "over_group_rate",
            "under_group_rate",
            "service_jaccard",
            "blast_precision",
            "blast_recall",
        )
    )
)
STATUSES: tuple[e.RunStatus, ...] = (
    "SUCCESS",
    "PARTIAL_FAILURE",
    "CONTEXT_UNAVAILABLE",
    "RUNTIME_ERROR",
    "CAPACITY_REJECTED",
    "TIMEOUT",
)
Fixed12 = Annotated[
    str, StringConstraints(strict=True, pattern=r"^[0-9]+\.[0-9]{12}$", max_length=128)
]


def decimal12(value: Fraction) -> str:
    with localcontext() as context:
        context.prec = (
            max(len(str(abs(value.numerator))), len(str(value.denominator))) + 40
        )
        return format(
            (Decimal(value.numerator) / Decimal(value.denominator)).quantize(
                Decimal("0.000000000001"), rounding=ROUND_HALF_EVEN
            ),
            "f",
        )


def _metric(
    name: str,
    numerator: int | None,
    denominator: int | None,
    *,
    reason: str | None = None,
    components: tuple[e.RationalComponent, ...] | None = None,
    planned: int = 1,
    eligible: int = 1,
    observed: int = 1,
    failed: int = 0,
    scope: str = "case.operational",
) -> e.MetricValue:
    if denominator == 0:
        reason = "empty_denominator"
    if reason is None and (numerator is None or denominator is None):
        raise e.EvaluationContractError(
            "Observed metric needs exact integer components"
        )
    value = None
    if reason is None:
        assert numerator is not None and denominator is not None
        value = decimal12(Fraction(numerator, denominator))
    return e.MetricValue(
        name=name,
        numerator=numerator,
        denominator=denominator,
        rational_components=components,
        value=value,
        reason=reason,
        planned=planned,
        eligible=eligible,
        observed=observed,
        failed=failed,
        scope=scope,
    )


def matching(
    truth_groups: list[Any], prediction_groups: list[Any]
) -> tuple[tuple[tuple[str, ...], tuple[str, ...]], ...]:
    """Exact bitmask DP; optimize member overlap, then the canonical pair sequence."""
    truth = sorted(tuple(sorted(group)) for group in truth_groups)
    predictions = sorted(tuple(sorted(group)) for group in prediction_groups)
    for partition in [truth, predictions]:
        flattened = [unit for group in partition for unit in group]
        if any(not group for group in partition) or len(flattened) != len(
            set(flattened)
        ):
            raise e.EvaluationContractError(
                "Matching needs disjoint nonempty member sets"
            )
    if len(truth) > 8 or len(predictions) > 91:
        raise e.EvaluationContractError("Matching capacity exceeded")
    # mask -> (unrounded score, sorted matching ledger)
    states: dict[int, tuple[Fraction, tuple[Any, ...]]] = {0: (Fraction(), ())}
    for prediction in predictions:
        updated = states.copy()
        for mask, (score, ledger) in states.items():
            for index, group in enumerate(truth):
                intersection = len(set(group) & set(prediction))
                if mask & (1 << index) or not intersection:
                    continue
                candidate = (
                    score + Fraction(intersection, len(set(group) | set(prediction))),
                    tuple(sorted((*ledger, (group, prediction)))),
                )
                target = mask | (1 << index)
                previous = updated.get(target)
                if (
                    previous is None
                    or candidate[0] > previous[0]
                    or (candidate[0] == previous[0] and candidate[1] < previous[1])
                ):
                    updated[target] = candidate
        states = updated
    return min(states.values(), key=lambda pair: (-pair[0], pair[1]))[1]


class Prediction(e.EvaluationModel):
    fingerprint: e.Hash
    members: tuple[e.Token, ...] = Field(min_length=1, max_length=91)
    exposure: tuple[e.Token, ...] | None = Field(max_length=64)

    @model_validator(mode="after")
    def unique(self) -> Prediction:
        e._unique(self.members)
        if self.exposure is not None:
            e._unique(self.exposure)
        return self


def case_quality(
    case_id: str,
    policy: str,
    universe: Any,
    services: dict[str, str],
    truth_status: e.TruthStatus,
    truth_groups: list[dict[str, Any]],
    background_units: Any,
    blast_truth: Any,
    predictions: list[dict[str, Any]],
    status: e.RunStatus,
    final_available: bool,
    earlier_failure: bool = False,
) -> e.CaseMetrics:
    """One case/policy, one quality weight; failed final calls have no output."""
    if type(final_available) is not bool or type(earlier_failure) is not bool:  # noqa: E721
        raise e.EvaluationContractError("Observed flags must be booleans")
    truth = e.TruthCase.model_validate(
        dict(
            case_id=case_id,
            truth_status=truth_status,
            universe=tuple(universe),
            truth_groups=tuple(
                e.TruthGroup.model_validate({**g, "members": tuple(g["members"])})
                for g in truth_groups
            ),
            background_units=tuple(background_units),
            blast_truth=None
            if blast_truth is None
            else tuple(
                e.BlastTruth.model_validate({**b, "services": tuple(b["services"])})
                for b in blast_truth
            ),
            blast_unavailable_reason="unsupported_truth"
            if blast_truth is None
            else None,
        )
    )
    values = tuple(
        Prediction.model_validate(
            {
                **p,
                "members": tuple(p["members"]),
                "exposure": None if p["exposure"] is None else tuple(p["exposure"]),
            }
        )
        for p in predictions
    )
    e._unique(values, lambda p: p.fingerprint)
    assigned = [unit for p in values for unit in p.members]
    e._unique(assigned)
    if (
        not set(assigned) <= set(universe)
        or set(services) != set(universe)
        or any(type(v) is not str or not v for v in services.values())  # noqa: E721 - services are strings, no coercion
    ):
        raise e.EvaluationContractError(
            "Prediction/service mapping differs from logical universe"
        )
    if not final_available and values:
        raise e.EvaluationContractError(
            "No predictions without committed final grouping"
        )
    unassigned = tuple(sorted(set(universe) - set(assigned)))
    failed = int(not final_available or earlier_failure)
    if truth_status != "complete":
        reason: Literal["unknown_truth", "ambiguous_truth"] = (
            "unknown_truth" if truth_status == "unknown" else "ambiguous_truth"
        )
        unavailable_rows = tuple(
            _metric(
                name, None, None, reason=reason, eligible=0, observed=0, failed=failed
            )
            for name in METRICS
        )
        return e.bind_case_metrics(
            e.CaseMetrics(
                case_id=case_id,
                policy=policy,
                truth_status=truth_status,
                quality_reason=reason,
                TP=None,
                FP=None,
                FN=None,
                metrics=unavailable_rows,
                matching=None,
                status=status,
                unassigned_units=unassigned,
                missed_truth_groups=None,
                background_prediction_groups=None,
            ),
            truth,
            final_grouping_available=final_available,
        )
    t = {tuple(sorted(group.members)): group for group in truth.truth_groups}
    p = {tuple(sorted(pred.members)): pred for pred in values}
    truth_pairs = {pair for group in t for pair in combinations(group, 2)}
    predicted_pairs = {pair for group in p for pair in combinations(group, 2)}
    tp, fp, fn = (
        len(truth_pairs & predicted_pairs),
        len(predicted_pairs - truth_pairs),
        len(truth_pairs - predicted_pairs),
    )
    ledger = matching(list(t), list(p))
    matched = []
    components = []
    for left, right in ledger:
        ls, rs = {services[u] for u in left}, {services[u] for u in right}
        si, su = len(ls & rs), len(ls | rs)
        matched.append(
            e.MatchingRow(
                case_id=case_id,
                policy=policy,
                truth_members=left,
                prediction_members=right,
                intersection=len(set(left) & set(right)),
                union=len(set(left) | set(right)),
                service_intersection=si,
                service_union=su,
            )
        )
        components.append(e.RationalComponent(numerator=si, denominator=su))
    intersections_t = {
        group: [pred for pred in p if set(group) & set(pred)] for group in t
    }
    intersections_p = {
        pred: [group for group in t if set(group) & set(pred)] for pred in p
    }
    missed = tuple(sorted(t[group].group for group in t if not intersections_t[group]))
    background = tuple(
        sorted(
            pred.fingerprint
            for pred in values
            if set(pred.members) & set(background_units)
        )
    )
    exact = len(set(t) & set(p))
    over = sum(len(groups) >= 2 for groups in intersections_p.values())
    under = sum(len(groups) >= 2 for groups in intersections_t.values())
    ratios: dict[str, tuple[int, int]] = dict(
        pair_precision=(tp, tp + fp),
        pair_recall=(tp, tp + fn),
        pair_f1=(2 * tp, 2 * tp + fp + fn),
        exact_group_precision=(exact, len(p)),
        exact_group_recall=(exact, len(t)),
        exact_partition=(
            int(final_available and not earlier_failure and set(t) == set(p)),
            1,
        ),
        over_group_rate=(over, len(p)),
        under_group_rate=(under, len(t)),
        exact_matches=(exact, 1),
        over_groups=(over, 1),
        under_groups=(under, 1),
        extra_truth_intersections=(
            sum(max(0, len(groups) - 1) for groups in intersections_p.values()),
            1,
        ),
        partially_covered_truth_groups=(
            sum(
                bool(intersections_t[group]) and not set(group) <= set(assigned)
                for group in t
            ),
            1,
        ),
        background_group_count=(len(background), 1),
        unassigned_unit_count=(len(unassigned), 1),
        missed_truth_group_count=(len(missed), 1),
    )
    service = (
        sum((Fraction(c.numerator, c.denominator) for c in components), Fraction())
        / max(len(t), len(p))
        if t or p
        else None
    )
    ratios["service_jaccard"] = (
        (service.numerator, service.denominator) if service is not None else (0, 0)
    )
    rows = {
        name: _metric(
            name,
            a,
            b,
            components=tuple(components) if name == "service_jaccard" else None,
            observed=int(final_available),
            failed=failed,
        )
        for name, (a, b) in ratios.items()
    }
    if not truth.blast_truth:
        for name in [
            "blast_precision",
            "blast_recall",
            "blast_TP",
            "blast_FP",
            "blast_FN",
        ]:
            rows[name] = _metric(
                name,
                None,
                None,
                reason="unsupported_truth",
                eligible=0,
                observed=0,
                failed=failed,
            )
    else:
        support = {b.group: set(b.services) for b in truth.blast_truth}
        paired = {t[left].group: p[right] for left, right in ledger}
        btp = bfp = bfn = 0
        for group, expected in support.items():
            prediction = paired.get(group)
            emitted = (
                set(prediction.exposure or ()) if prediction is not None else set()
            )
            btp += len(emitted & expected)
            bfp += len(emitted - expected)
            bfn += len(expected - emitted)
        unmatched = [
            pred
            for members, pred in p.items()
            if members not in {right for _, right in ledger}
        ]
        incomplete = bool(unmatched) and len(support) != len(t)
        if not incomplete:
            bfp += sum(len(pred.exposure or ()) for pred in unmatched)
        observed = int(
            final_available and all(pred.exposure is not None for pred in values)
        )
        for name, (a, b) in {
            "blast_recall": (btp, btp + bfn),
            "blast_TP": (btp, 1),
            "blast_FN": (bfn, 1),
        }.items():
            rows[name] = _metric(name, a, b, observed=observed, failed=failed)
        for name, (a, b) in {
            "blast_precision": (btp, btp + bfp),
            "blast_FP": (bfp, 1),
        }.items():
            rows[name] = _metric(
                name,
                None if incomplete else a,
                None if incomplete else b,
                reason="incomplete_blast_truth" if incomplete else None,
                observed=observed,
                failed=failed,
            )
    return e.bind_case_metrics(
        e.CaseMetrics(
            case_id=case_id,
            policy=policy,
            truth_status=truth_status,
            quality_reason=None,
            TP=tp,
            FP=fp,
            FN=fn,
            metrics=tuple(rows[name] for name in METRICS),
            matching=tuple(matched),
            status=status,
            unassigned_units=unassigned,
            missed_truth_groups=missed,
            background_prediction_groups=background,
        ),
        truth,
        final_grouping_available=final_available,
    )


class StatusCount(e.EvaluationModel):
    scope: Literal["measured", "warmup", "case"]
    policy: e.Token
    status: e.RunStatus
    count: e.Count


class LatencySummary(e.EvaluationModel):
    case_id: e.Token | None
    policy: e.Token
    scope: Literal["controlled_compute", "worker_wall", "enrichment"]
    samples_ns: tuple[e.Count, ...]
    n: e.Count
    failed: e.Count
    censored: e.Count
    minimum_ns: e.Count | None
    median_ns: Fixed12 | None
    p95_ns: e.Count | None
    maximum_ns: e.Count | None

    @model_validator(mode="after")
    def exact(self) -> LatencySummary:
        ordered = sorted(self.samples_ns)
        if self.n != len(ordered):
            raise ValueError("Latency sample count mismatch")
        expected = (
            (None, None, None, None)
            if not ordered
            else (
                ordered[0],
                decimal12(
                    Fraction(
                        ordered[(len(ordered) - 1) // 2] + ordered[len(ordered) // 2], 2
                    )
                ),
                ordered[(95 * len(ordered) + 99) // 100 - 1],
                ordered[-1],
            )
        )
        if (self.minimum_ns, self.median_ns, self.p95_ns, self.maximum_ns) != expected:
            raise ValueError("Latency statistics mismatch")
        return self


class TargetRow(e.EvaluationModel):
    policy: e.Token
    target: Literal[
        "controlled_budget", "known_pattern_quality_proxy", "live_grouping_latency"
    ]
    threshold: Annotated[
        str, StringConstraints(strict=True, pattern=r"\S", min_length=1, max_length=128)
    ]
    scope: Annotated[
        str, StringConstraints(strict=True, pattern=r"\S", min_length=1, max_length=256)
    ]
    measured: Fixed12 | None
    status: Literal["MET", "MISSED", "NOT_MEASURED", "NOT_DEMONSTRATED"]
    reason: e.Token | None

    @model_validator(mode="after")
    def availability(self) -> TargetRow:
        if self.status in {"MET", "MISSED"} and (
            self.measured is None or self.reason is not None
        ):
            raise ValueError("Measured target needs a value and no unavailable reason")
        if self.status in {"NOT_MEASURED", "NOT_DEMONSTRATED"} and self.reason is None:
            raise ValueError("Unavailable target needs its reason")
        if self.status == "NOT_MEASURED" and self.measured is not None:
            raise ValueError("Unmeasured target must not invent a value")
        return self


class ProcessedComparison(e.EvaluationModel):
    schema_version: Literal["correlation-evaluation/1.0"]
    metric_version: Literal["correlation-metrics/1.0"]
    raw_sha256: e.Hash
    runtime_sha256: e.Hash
    truth_sha256: e.Hash
    config_sha256: e.Hash
    cases: tuple[e.CaseMetrics, ...] = Field(min_length=32, max_length=32)
    policy_micro: tuple[e.MetricValue, ...]
    policy_macro: tuple[e.MetricValue, ...]
    statuses: tuple[StatusCount, ...]
    latency: tuple[LatencySummary, ...]
    targets: tuple[TargetRow, ...] = Field(min_length=6, max_length=6)
    coverage: tuple[e.MetricValue, ...]

    @model_validator(mode="after")
    def unique_rows(self) -> ProcessedComparison:
        e._unique(self.cases, lambda row: (row.case_id, row.policy))
        for rows in [self.policy_micro, self.policy_macro, self.coverage]:
            e._unique(rows, lambda row: (row.scope, row.name))
        e._unique(self.statuses, lambda row: (row.policy, row.scope, row.status))
        e._unique(self.latency, lambda row: (row.policy, row.case_id, row.scope))
        e._unique(self.targets, lambda row: (row.policy, row.target))
        return self


def _fraction(metric: e.MetricValue) -> Fraction:
    if metric.value is None or metric.numerator is None or metric.denominator is None:
        raise e.EvaluationContractError("Undefined metric has no rational value")
    return Fraction(metric.numerator, metric.denominator)


def _num(metric: e.MetricValue) -> int:
    if metric.numerator is None:
        raise e.EvaluationContractError("Unavailable numerator")
    return metric.numerator


def _den(metric: e.MetricValue) -> int:
    if metric.denominator is None:
        raise e.EvaluationContractError("Unavailable denominator")
    return metric.denominator


def _semantic(run: e.RawRun) -> Any:
    return dict(
        status=run.terminal_status,
        available=run.final_grouping_available,
        decisions=[
            d.model_dump(mode="json", exclude={"timings", "errors"})
            | {"errors": [(v.stage, v.exception_type) for v in d.errors]}
            for d in run.decisions
        ],
        enrichments=[
            item.model_dump(mode="json", exclude={"timings", "errors"})
            | {"errors": [(v.stage, v.exception_type) for v in item.errors]}
            for item in run.enrichments
        ],
        errors=[(v.stage, v.exception_type) for v in run.errors],
    )


def latency_summary(
    case_id: str | None,
    policy: str,
    scope: Literal["controlled_compute", "worker_wall", "enrichment"],
    runs: list[e.RawRun],
) -> LatencySummary:
    samples = []
    failed = censored = 0
    for run in runs:
        if run.plan.warmup:
            continue
        if run.terminal_status == "TIMEOUT":
            censored += 1
            continue
        if scope == "worker_wall":
            valid = run.terminal_status == "SUCCESS"
            elapsed = run.elapsed_ns
        elif scope == "controlled_compute":
            valid = (
                bool(run.decisions)
                and len(run.decisions) == len(run.plan.decision_ids)
                and all(d.status == "success" for d in run.decisions)
            )
            elapsed = sum(
                t.elapsed_ns
                for d in run.decisions
                for t in d.timings
                if t.stage in {"projection", "scoring", "grouping"}
                and t.status == "observed"
            )
        elif scope == "enrichment":
            valid = run.final_grouping_available and all(
                item.status == "success" for item in run.enrichments
            )
            elapsed = sum(
                t.elapsed_ns
                for item in run.enrichments
                for t in item.timings
                if t.stage in {"aggregate", "blast", "handoff"}
                and t.status == "observed"
            )
            valid = valid and all(
                sum(t.stage == stage and t.status == "observed" for t in item.timings)
                == 1
                for item in run.enrichments
                for stage in ["aggregate", "blast", "handoff"]
            )
        else:
            raise e.EvaluationContractError("Unknown latency scope")
        if valid:
            samples.append(elapsed)
        else:
            failed += 1
    ordered = sorted(samples)
    n = len(ordered)
    return LatencySummary(
        case_id=case_id,
        policy=policy,
        scope=scope,
        samples_ns=tuple(samples),
        n=n,
        failed=failed,
        censored=censored,
        minimum_ns=ordered[0] if n else None,
        median_ns=decimal12(Fraction(ordered[(n - 1) // 2] + ordered[n // 2], 2))
        if n
        else None,
        p95_ns=ordered[(95 * n + 99) // 100 - 1] if n else None,
        maximum_ns=ordered[-1] if n else None,
    )


def _aggregate(
    cases: list[e.CaseMetrics],
    name: str,
    policy: str,
    macro: bool,
    *,
    population: int | None = None,
    success_conditioned: bool = False,
) -> e.MetricValue:
    rows = [next(m for m in case.metrics if m.name == name) for case in cases]
    defined = [m for m in rows if m.value is not None]
    coverage: dict[str, Any] = dict(
        planned=len(cases) if population is None else population,
        eligible=sum(m.eligible for m in rows),
        observed=len(defined) if macro else sum(m.observed for m in rows),
        failed=sum(m.failed for m in rows)
        + (0 if population is None else population - len(cases)),
        scope=f"policy.{policy}.{'grouping_success.' if success_conditioned else ''}{'macro' if macro else 'micro'}",
    )
    if not cases and success_conditioned:
        return _metric(
            name, None, None, reason="no_successful_grouping_cases", **coverage
        )
    if (
        not macro
        and name == "blast_precision"
        and any(
            m.reason
            in {
                "unsupported_truth",
                "incomplete_blast_truth",
                "unknown_truth",
                "ambiguous_truth",
            }
            for m in rows
        )
    ):
        return _metric(name, None, None, reason="incomplete_blast_truth", **coverage)
    if not defined:
        known = [
            m for m in rows if m.numerator is not None and m.denominator is not None
        ]
        if not macro and known:
            return _metric(
                name,
                sum(_num(m) for m in known),
                sum(_den(m) for m in known),
                components=tuple(
                    c for m in known for c in (m.rational_components or ())
                )
                if name == "service_jaccard"
                else None,
                **coverage,
            )
        reason = (
            "unsupported_truth"
            if success_conditioned
            and not macro
            and all(m.reason == "unsupported_truth" for m in rows)
            else "no_defined_cases"
        )
        return _metric(name, None, None, reason=reason, **coverage)
    if macro:
        parts = tuple(
            e.RationalComponent(numerator=_num(m), denominator=_den(m)) for m in defined
        )
        value = sum((_fraction(m) for m in defined), Fraction()) / len(defined)
        return _metric(
            name, value.numerator, value.denominator, components=parts, **coverage
        )
    if name == "service_jaccard":
        parts = tuple(
            component for m in defined for component in (m.rational_components or ())
        )
        # Exact case group population is carried by exact-group metric denominators.
        weight = sum(
            max(
                next(
                    v.denominator
                    for v in case.metrics
                    if v.name == "exact_group_precision"
                )
                or 0,
                next(
                    v.denominator
                    for v in case.metrics
                    if v.name == "exact_group_recall"
                )
                or 0,
            )
            for case in cases
            if case.truth_status == "complete"
        )
        value = (
            sum((Fraction(c.numerator, c.denominator) for c in parts), Fraction())
            / weight
        )
        return _metric(
            name, value.numerator, value.denominator, components=parts, **coverage
        )
    if name in COUNT_METRICS:
        return _metric(name, sum(_num(m) for m in defined), 1, **coverage)
    return _metric(
        name,
        sum(_num(m) for m in rows if m.numerator is not None),
        sum(_den(m) for m in rows if m.denominator is not None),
        **coverage,
    )


def _coverage(
    name: str, numerator: int, denominator: int, policy: str
) -> e.MetricValue:
    return _metric(
        name,
        numerator,
        denominator,
        planned=denominator,
        eligible=denominator,
        observed=numerator,
        failed=denominator - numerator,
        scope=f"policy.{policy}.coverage",
    )


def _grouping_succeeded(run: e.RawRun) -> bool:
    """Condition grouping quality on grouping, independently of enrichment/wall status."""
    return run.final_grouping_available and all(
        d.status == "success" for d in run.decisions
    )


def calculate_comparison(
    config_raw: bytes,
    runtime_raw: bytes,
    truth_raw: bytes,
    topology_raw: bytes,
    raw_outcomes: bytes,
    *,
    expected_config_sha256: str,
    expected_raw_sha256: str,
) -> ProcessedComparison:
    if hashlib.sha256(raw_outcomes).hexdigest() != expected_raw_sha256:
        raise e.EvaluationContractError("Independent raw identity mismatch")
    config, runtime, truth = e.load_evaluation_inputs(
        config_raw,
        runtime_raw,
        truth_raw,
        topology_raw,
        expected_config_sha256=expected_config_sha256,
    )
    raw = e.bind_raw_outcomes(
        e.deserialize_evaluation(e.RawOutcomes, raw_outcomes, limit=e.MAX_RAW_BYTES),
        config_raw,
        runtime,
    )
    by_case = {case.case_id: case for case in runtime.cases}
    truths = {case.case_id: case for case in truth.cases}
    selected = {}
    measured = {}
    for policy in config.policies:
        for case in runtime.cases:
            runs = [
                r
                for r in raw.runs
                if r.plan.policy == policy.name
                and r.plan.case_id == case.case_id
                and not r.plan.warmup
            ]
            if len(runs) != 10 or any(
                _semantic(r) != _semantic(runs[0]) for r in runs[1:]
            ):
                raise e.EvaluationContractError(
                    "Measured repetitions are semantically inconsistent"
                )
            selected[(case.case_id, policy.name)] = runs[0]
            measured[(case.case_id, policy.name)] = runs
    cases = []
    for case in runtime.cases:
        for policy in config.policies:
            run = selected[(case.case_id, policy.name)]
            sidecar = truths[case.case_id]
            mapping = {
                v.signal.signal_id: u.unit
                for u, v in zip(case.units, e.project_runtime_case(case))
            }
            result = (
                run.decisions[-1].grouping_result
                if run.final_grouping_available
                else None
            )
            enrichments = {item.group: item for item in run.enrichments}
            predictions = []
            for group in result.groups if result is not None else ():
                item = enrichments.get(group.group_fingerprint)
                exposure = (
                    tuple(sorted(s.name for s in item.blast_context.reachable_services))
                    if item is not None
                    and item.status == "success"
                    and item.blast_context is not None
                    else None
                )
                predictions.append(
                    dict(
                        fingerprint=group.group_fingerprint,
                        members=tuple(
                            sorted(
                                mapping[member.signal_id] for member in group.members
                            )
                        ),
                        exposure=exposure,
                    )
                )
            cases.append(
                case_quality(
                    case.case_id,
                    policy.name,
                    sidecar.universe,
                    {unit.unit: unit.service for unit in case.units},
                    sidecar.truth_status,
                    [g.model_dump() for g in sidecar.truth_groups],
                    sidecar.background_units,
                    [b.model_dump() for b in sidecar.blast_truth]
                    if sidecar.blast_truth is not None
                    else None,
                    predictions,
                    run.terminal_status,
                    run.final_grouping_available,
                    any(d.status == "failed" for d in run.decisions[:-1]),
                )
            )
    micro: list[e.MetricValue] = []
    macro: list[e.MetricValue] = []
    statuses: list[StatusCount] = []
    latency: list[LatencySummary] = []
    targets: list[TargetRow] = []
    coverage: list[e.MetricValue] = []
    for policy in config.policies:
        rows = [case for case in cases if case.policy == policy.name]
        micro_start = len(micro)
        coverage_start = len(coverage)
        for name in METRICS:
            micro.append(_aggregate(rows, name, policy.name, False))
            macro.append(_aggregate(rows, name, policy.name, True))
        # Observed-only blast precision explicitly declares its smaller eligibility.
        btp = [next(m for m in case.metrics if m.name == "blast_TP") for case in rows]
        bfp = [next(m for m in case.metrics if m.name == "blast_FP") for case in rows]
        usable = [
            (a, b)
            for a, b in zip(btp, bfp)
            if a.value is not None and b.value is not None
        ]
        a = sum(_num(x) for x, _ in usable)
        b = sum(_num(y) for _, y in usable)
        micro.append(
            _metric(
                "blast_precision_observed",
                a,
                a + b,
                planned=16,
                eligible=len(usable),
                observed=len(usable),
                failed=sum(m.failed for m, _ in usable),
                scope=f"policy.{policy.name}.micro",
            )
        )
        micro[micro_start:] = sorted(micro[micro_start:], key=lambda m: m.name)
        successful = [
            case
            for case in rows
            if _grouping_succeeded(selected[(case.case_id, policy.name)])
        ]
        for name in METRICS:
            micro.append(
                _aggregate(
                    successful,
                    name,
                    policy.name,
                    False,
                    population=len(rows),
                    success_conditioned=True,
                )
            )
            macro.append(
                _aggregate(
                    successful,
                    name,
                    policy.name,
                    True,
                    population=len(rows),
                    success_conditioned=True,
                )
            )
        status_scopes: tuple[Literal["measured", "warmup", "case"], ...] = (
            "measured",
            "warmup",
            "case",
        )
        for scope in status_scopes:
            runs = (
                [
                    r
                    for r in raw.runs
                    if r.plan.policy == policy.name
                    and (r.plan.warmup if scope == "warmup" else not r.plan.warmup)
                ]
                if scope != "case"
                else [selected[(case.case_id, policy.name)] for case in runtime.cases]
            )
            counts = Counter(r.terminal_status for r in runs)
            statuses.extend(
                StatusCount(
                    scope=scope, policy=policy.name, status=status, count=counts[status]
                )
                for status in STATUSES
            )
        for case_id in [*(case.case_id for case in runtime.cases), None]:
            runs = (
                measured[(case_id, policy.name)]
                if case_id is not None
                else [
                    r
                    for r in raw.runs
                    if r.plan.policy == policy.name and not r.plan.warmup
                ]
            )
            latency_scopes: tuple[
                Literal["controlled_compute", "enrichment", "worker_wall"], ...
            ] = ("controlled_compute", "enrichment", "worker_wall")
            latency.extend(
                latency_summary(case_id, policy.name, scope, runs)
                for scope in latency_scopes
            )
        aggregate_latency = next(
            row
            for row in latency
            if row.policy == policy.name
            and row.case_id is None
            and row.scope == "controlled_compute"
        )
        canonical = [case for case in rows if case.case_id in config.cases[:8]]
        known = all(case.truth_status == "complete" for case in canonical)
        numerator = (
            sum(
                next(_num(m) for m in case.metrics if m.name == "exact_partition")
                for case in canonical
            )
            if known
            else 0
        )
        quality = Fraction(numerator, 8)
        controlled = (
            aggregate_latency.n == 160
            and not aggregate_latency.failed
            and not aggregate_latency.censored
        )
        targets.extend(
            [
                TargetRow(
                    policy=policy.name,
                    target="controlled_budget",
                    threshold="<5000000000ns",
                    scope="controlled_compute_p95",
                    measured=decimal12(Fraction(aggregate_latency.p95_ns))
                    if aggregate_latency.p95_ns is not None
                    else None,
                    status=(
                        "MET"
                        if aggregate_latency.p95_ns is not None
                        and aggregate_latency.p95_ns < 5_000_000_000
                        else "MISSED"
                    )
                    if controlled
                    else "NOT_DEMONSTRATED",
                    reason=None
                    if controlled
                    else "incomplete_successful_grouping_coverage",
                ),
                TargetRow(
                    policy=policy.name,
                    target="known_pattern_quality_proxy",
                    threshold=">9/10",
                    scope="eight_canonical_exact_partition",
                    measured=decimal12(quality) if known else None,
                    status=("MET" if quality > Fraction(9, 10) else "MISSED")
                    if known
                    else "NOT_DEMONSTRATED",
                    reason=None if known else "incomplete_canonical_truth",
                ),
                TargetRow(
                    policy=policy.name,
                    target="live_grouping_latency",
                    threshold="<5s",
                    scope="live_orchestration",
                    measured=None,
                    status="NOT_MEASURED",
                    reason="outside_task_4.11_scope",
                ),
            ]
        )
        chosen = [selected[(case.case_id, policy.name)] for case in runtime.cases]
        total_groups = sum(len(run.enrichments) for run in chosen)
        coverage.extend(
            [
                _coverage(
                    "grouping_available",
                    sum(run.final_grouping_available for run in chosen),
                    16,
                    policy.name,
                ),
                _coverage(
                    "enrichment_available",
                    sum(
                        item.status == "success"
                        for run in chosen
                        for item in run.enrichments
                    ),
                    total_groups,
                    policy.name,
                ),
                *(
                    _coverage(
                        "truth_" + status,
                        sum(case.truth_status == status for case in rows),
                        16,
                        policy.name,
                    )
                    for status in ["complete", "unknown", "ambiguous"]
                ),
                _coverage(
                    "zero_output",
                    sum(
                        run.final_grouping_available
                        and run.decisions[-1].grouping_result is not None
                        and not run.decisions[-1].grouping_result.groups
                        for run in chosen
                    ),
                    16,
                    policy.name,
                ),
                _coverage(
                    "unassigned_units",
                    sum(len(case.unassigned_units) for case in rows),
                    sum(len(case.units) for case in runtime.cases),
                    policy.name,
                ),
            ]
        )
        timing_runs = [
            r for r in raw.runs if r.plan.policy == policy.name and not r.plan.warmup
        ]
        planned_deliveries = sum(
            len(by_case[r.plan.case_id].deliveries) for r in timing_runs
        )
        final_arguments = [
            r.decisions[-1].arguments
            for r in timing_runs
            if r.decisions and r.decisions[-1].decision == r.plan.decision_ids[-1]
        ]
        captured_deliveries = sum(len(a.runtime_inputs) for a in final_arguments)
        coverage.append(
            _coverage(
                "deliveries_captured",
                captured_deliveries,
                planned_deliveries,
                policy.name,
            )
        )
        all_decisions = [d for r in timing_runs for d in r.decisions]
        records = [
            record
            for d in all_decisions
            if d.grouping_result is not None
            for record in d.grouping_result.signal_records
        ]
        for disposition in e.GroupingSignalDisposition:
            coverage.append(
                _coverage(
                    "disposition." + disposition.value,
                    sum(r.disposition == disposition for r in records),
                    len(records),
                    policy.name,
                )
            )
        rejected = [
            edge
            for d in all_decisions
            if d.grouping_result is not None
            for edge in d.grouping_result.rejected_edges
        ]
        for reason in e.GroupingEdgeRejectionReason:
            coverage.append(
                _coverage(
                    "rejection." + reason.value,
                    sum(edge.reason == reason for edge in rejected),
                    len(rejected),
                    policy.name,
                )
            )
        coverage[coverage_start:] = sorted(
            coverage[coverage_start:], key=lambda m: m.name
        )
    return ProcessedComparison(
        schema_version="correlation-evaluation/1.0",
        metric_version="correlation-metrics/1.0",
        raw_sha256=expected_raw_sha256,
        runtime_sha256=hashlib.sha256(runtime_raw).hexdigest(),
        truth_sha256=hashlib.sha256(truth_raw).hexdigest(),
        config_sha256=expected_config_sha256,
        cases=tuple(cases),
        policy_micro=tuple(micro),
        policy_macro=tuple(macro),
        statuses=tuple(statuses),
        latency=tuple(latency),
        targets=tuple(targets),
        coverage=tuple(coverage),
    )
