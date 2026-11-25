"""Independent Task 4.11-M1 reductions, using only the Python standard library.

Authority: approved A1 sections 7-8, A1-R1 record schemas/amendment, and
M1 implementation notes. No production evaluation, matching or rendering code
is imported. This module does not execute an evaluation or publish artifacts.
"""

# A1-R1 requires exact builtin types; isinstance would admit bool/subclasses.
# ruff: noqa: E721

from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
import argparse
import csv
from html import escape
import hashlib
import io
from itertools import chain
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
from uuid import NAMESPACE_URL, UUID, uuid5


STATUSES = (
    "SUCCESS",
    "PARTIAL_FAILURE",
    "CONTEXT_UNAVAILABLE",
    "RUNTIME_ERROR",
    "CAPACITY_REJECTED",
    "TIMEOUT",
)
METRIC_NAMES = tuple(
    sorted(
        (
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
    )
)
_TOKEN = re.compile(r"[A-Za-z0-9_.:-]{1,128}", re.ASCII)
SCHEMA_VERSION = "correlation-evaluation/1.0"
METRIC_VERSION = "correlation-metrics/1.0"
_COUNT_METRICS = frozenset(
    (
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
)
_FIELDS = {
    "Config": "schema_version protocol_fingerprint source_revision policies topologies cases seed epoch budgets repetitions timing_rules metric_version target_rules source_identities runtime_identity truth_identity",
    "RuntimeInputs": "schema_version cases",
    "TruthInputs": "schema_version cases",
    "RawOutcomes": "schema_version config_sha256 planned runs",
    "RuntimeCase": "case_id topology_key units deliveries decisions",
    "RuntimeUnit": "unit service metric observed_value event_offset_seconds",
    "Delivery": "delivery unit arrival_offset_seconds",
    "Decision": "decision arrival_cutoff_seconds watermark_offset_seconds",
    "TruthCase": "case_id truth_status universe truth_groups background_units blast_truth blast_unavailable_reason",
    "PlannedRun": "run case_id policy configuration_fingerprint repetition warmup source_revision config_sha256 runtime_sha256 truth_sha256 topology_fingerprint decision_ids",
    "RawRun": "plan utc_start utc_end elapsed_ns terminal_status final_grouping_available decisions enrichments timings errors timeout environment sources_before sources_after",
    "DecisionOutcome": "decision arguments pair_evidence grouping_result status errors timings",
    "EnrichmentOutcome": "group aggregate blast_context handoff status errors timings",
    "StageTiming": "stage decision group elapsed_ns status",
    "SafeError": "stage exception_type message",
    "TimeoutEvidence": "deadline_ns elapsed_ns last_stage pid terminated",
}


def _record(value, name):
    if type(value) is not dict or set(value) != set(_FIELDS[name].split()):
        raise ValueError(f"{name}: unexpected or missing fields")
    return value


def _sha256(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("expected lowercase SHA-256")
    return value


def _digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _integer(value, label):
    if type(value) is not int or value < 0:
        raise ValueError(f"{label}: expected nonnegative integer, never bool")
    return value


def _source_identities(items, label):
    """Validate portable observations without consulting a different host tree."""
    if type(items) is not list or not items:
        raise ValueError(f"{label}: expected nonempty source identity list")
    result = {}
    for index, item in enumerate(items):
        where = f"{label}[{index}]"
        if type(item) is not dict or set(item) != {"path", "bytes", "sha256"}:
            raise ValueError(f"{where}: expected exact portable identity fields")
        path = item["path"]
        if (
            type(path) is not str
            or not path.strip()
            or len(path) > 1024
            or PurePosixPath(path).is_absolute()
            or PurePosixPath(path).as_posix() != path
            or any(p in (".", "..") for p in path.split("/"))
            or any(c in path for c in "\\:\x00\r\n")
        ):
            raise ValueError(f"{where}.path: expected safe exact relative path")
        _integer(item["bytes"], f"{where}.bytes")
        if (
            type(item["sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
        ):
            raise ValueError(f"{where}.sha256: expected lowercase SHA-256")
        if path in result:
            raise ValueError(f"{where}.path: duplicate source path")
        result[path] = item
    return result


def _boolean(value, label):
    if type(value) is not bool:
        raise ValueError(f"{label}: expected bool")
    return value


def _token(value):
    if type(value) is not str or _TOKEN.fullmatch(value) is None:
        raise ValueError("invalid logical token")
    return value


def _text(value):
    if type(value) is not str or not value.strip() or len(value) > 128:
        raise ValueError("expected nonblank service text of at most 128 characters")
    return value


def _unique(values, label, *, tokens=True):
    if type(values) not in (list, tuple, set, frozenset):
        raise ValueError(f"{label}: expected member collection")
    checked = [(_token(v) if tokens else _text(v)) for v in values]
    if len(set(checked)) != len(checked):
        raise ValueError(f"{label}: duplicate member")
    return frozenset(checked)


def _json_value(value, depth=0, ancestors=frozenset()):
    """Validate Python types before JSON can coerce keys, tuples or subclasses."""
    if depth > 32:
        raise ValueError("JSON depth exceeds 32")
    if type(value) in (list, dict):
        if id(value) in ancestors:
            raise ValueError("cyclic JSON value")
        ancestors = ancestors | {id(value)}
    if value is None or type(value) in (bool, int, str):
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for item in value:
            _json_value(item, depth + 1, ancestors)
        return
    if type(value) is dict and all(type(key) is str for key in value):
        for item in value.values():
            _json_value(item, depth + 1, ancestors)
        return
    raise ValueError("expected finite JSON with string keys and strict builtin types")


def canonical(value):
    """Return compact, sorted, finite UTF-8 JSON with exactly one final LF."""
    _json_value(value)
    return (
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def strict_loads(value):
    """Reject duplicate keys and nonfinite values, including exponent overflow."""

    def object_pairs(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    def constant(value):
        raise ValueError(f"nonfinite JSON constant: {value}")

    result = json.loads(value, object_pairs_hook=object_pairs, parse_constant=constant)
    _json_value(result)
    return result


def decimal12(value):
    """Format a nonnegative exact rational, with exact half-even rounding.

    Integer division determines rounding before Decimal formatting. This avoids
    double rounding even for fractions arbitrarily close to a half-way point.
    """
    if type(value) is int:
        value = Fraction(value)
    if type(value) is not Fraction or value < 0:
        raise ValueError("decimal12 requires a nonnegative Fraction or integer")
    scaled, remainder = divmod(value.numerator * 10**12, value.denominator)
    if 2 * remainder > value.denominator or (
        2 * remainder == value.denominator and scaled % 2
    ):
        scaled += 1
    with localcontext() as context:
        context.prec = max(28, len(str(scaled)) + 13)
        rounded = (
            Decimal(scaled)
            .scaleb(-12)
            .quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_EVEN)
        )
        return format(rounded, ".12f")


def _groups(groups, label):
    """Accept member collections or the documented group/prediction records."""
    if type(groups) not in (list, tuple):
        raise ValueError(f"{label}: expected groups")
    result = []
    seen = set()
    for group in groups:
        members = _unique(group["members"] if type(group) is dict else group, label)
        if not members or seen.intersection(members):
            raise ValueError(f"{label}: empty or overlapping groups")
        seen.update(members)
        result.append(tuple(sorted(members)))
    return tuple(sorted(result))


def matching(T, P):
    """Exact maximum member-Jaccard matching with canonical lexicographic ties.

    Each state retains the best exact score and sorted pair sequence for a
    truth mask. Equal-mask sequences have equal length, so lexicographic order
    is preserved when identical future pairs are inserted. At most 256 masks
    are needed for the approved eight-truth-group ceiling.
    """
    truth, predictions = _groups(T, "truth"), _groups(P, "prediction")
    if len(truth) > 8:
        raise ValueError("truth group capacity exceeds eight")
    states = {0: (Fraction(0), ())}
    for prediction in predictions:
        following = dict(states)
        pred_set = frozenset(prediction)
        for mask, (score, pairs) in states.items():
            for index, members in enumerate(truth):
                if mask & (1 << index):
                    continue
                truth_set = frozenset(members)
                overlap = len(truth_set & pred_set)
                if not overlap:
                    continue
                candidate = (
                    score + Fraction(overlap, len(truth_set | pred_set)),
                    tuple(sorted(pairs + ((members, prediction),))),
                )
                next_mask = mask | (1 << index)
                previous = following.get(next_mask)
                if (
                    previous is None
                    or candidate[0] > previous[0]
                    or (candidate[0] == previous[0] and candidate[1] < previous[1])
                ):
                    following[next_mask] = candidate
        states = following
    return min(states.values(), key=lambda item: (-item[0], item[1]))[1]


def _metric(
    name,
    numerator,
    denominator,
    *,
    reason=None,
    components=None,
    eligible=1,
    observed=1,
    failed=0,
):
    value = None
    if reason is None:
        _integer(numerator, "numerator")
        _integer(denominator, "denominator")
        if denominator:
            value = decimal12(Fraction(numerator, denominator))
        else:
            reason = "empty_denominator"
    return {
        "name": name,
        "numerator": numerator,
        "denominator": denominator,
        "rational_components": components,
        "value": value,
        "reason": reason,
        "planned": 1,
        "eligible": eligible,
        "observed": observed,
        "failed": failed,
        "scope": "case.operational",
    }


def case_quality(
    case_id,
    policy,
    universe,
    services,
    truth_status,
    truth_groups,
    background_units,
    blast_truth,
    predictions,
    status,
    final_available,
    earlier_failure=False,
):
    """Reduce authenticated logical truth and final predictions to CaseMetrics."""
    _token(case_id)
    _token(policy)
    _boolean(final_available, "final_available")
    _boolean(earlier_failure, "earlier_failure")
    if status not in STATUSES or truth_status not in (
        "complete",
        "unknown",
        "ambiguous",
    ):
        raise ValueError("unknown status")
    if (not final_available and status in ("SUCCESS", "PARTIAL_FAILURE")) or (
        final_available
        and status in ("RUNTIME_ERROR", "CONTEXT_UNAVAILABLE", "CAPACITY_REJECTED")
    ):
        raise ValueError("terminal status contradicts final grouping availability")
    if earlier_failure and status == "SUCCESS":
        raise ValueError("earlier failure requires degraded terminal status")
    units = _unique(universe, "universe")
    if len(units) > 91 or type(services) is not dict or set(services) != units:
        raise ValueError("invalid universe capacity or unit-service mapping")
    for service in services.values():
        _text(service)
    background = _unique(background_units, "background")
    truth = _groups(truth_groups, "truth")
    predicted = _groups(predictions, "prediction")
    if len(truth) > 8:
        raise ValueError("truth group capacity exceeds eight")
    truth_ids = {}
    for group in truth_groups:
        if type(group) is not dict or set(group) != {"group", "members"}:
            raise ValueError("invalid truth group fields")
        group_id = _token(group["group"])
        if group_id in truth_ids.values():
            raise ValueError("duplicate truth group ID")
        truth_ids[tuple(sorted(group["members"]))] = group_id
    truth_units = {unit for group in truth for unit in group}
    predicted_units = {unit for group in predicted for unit in group}
    if (
        not (truth_units | background | predicted_units) <= units
        or truth_units & background
    ):
        raise ValueError("unknown or overlapping truth/background/prediction member")
    if truth_status == "complete" and truth_units | background != units:
        raise ValueError("complete truth must partition universe")
    exposures, fingerprints = {}, {}
    for group in predictions:
        if type(group) is not dict or set(group) != {
            "fingerprint",
            "members",
            "exposure",
        }:
            raise ValueError("invalid prediction fields")
        fingerprint = group["fingerprint"]
        if (
            type(fingerprint) is not str
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
        ):
            raise ValueError("invalid prediction fingerprint")
        if fingerprint in fingerprints.values():
            raise ValueError("duplicate prediction fingerprint")
        key = tuple(sorted(group["members"]))
        fingerprints[key] = fingerprint
        exposure = group["exposure"]
        if exposure is not None:
            checked = _unique(exposure, "exposure", tokens=False)
            if (
                type(exposure) is not list
                or exposure != sorted(checked)
                or len(checked) > 64
            ):
                raise ValueError("exposure must be a sorted unique service list")
            exposure = checked
        exposures[key] = exposure
    # No partial groups from an unavailable final call may masquerade as output.
    if not final_available and predictions:
        raise ValueError("unavailable final grouping contains committed predictions")
    failed = int(not final_available or earlier_failure)
    result = {
        "case_id": case_id,
        "policy": policy,
        "metrics": [],
        "matching": None,
        "TP": None,
        "FP": None,
        "FN": None,
        "status": status,
        "unassigned_units": sorted(units - predicted_units),
        "missed_truth_groups": None,
        "background_prediction_groups": None,
        "truth_status": truth_status,
        "quality_reason": None
        if truth_status == "complete"
        else truth_status + "_truth",
    }
    if truth_status != "complete":
        result["metrics"] = [
            _metric(
                name,
                None,
                None,
                reason=result["quality_reason"],
                eligible=0,
                observed=0,
                failed=failed,
            )
            for name in METRIC_NAMES
        ]
        return result

    tsets = [frozenset(group) for group in truth]
    psets = [frozenset(group) for group in predicted]

    def choose2(n):
        return n * (n - 1) // 2

    tp = sum(choose2(len(t & p)) for t in tsets for p in psets)
    fp = sum(choose2(len(p)) for p in psets) - tp
    fn = sum(choose2(len(t)) for t in tsets) - tp
    exact = len(set(truth) & set(predicted))
    over = [sum(bool(t & p) for t in tsets) for p in psets]
    under = [sum(bool(t & p) for p in psets) for t in tsets]
    missed = sorted(truth_ids[t] for t, hits in zip(truth, under) if hits == 0)
    bg_predictions = sorted(fingerprints[p] for p in predicted if set(p) & background)
    pairs = matching(truth, predicted)
    ledger, components = [], []
    for t, p in pairs:
        ts, ps = {services[u] for u in t}, {services[u] for u in p}
        intersection, union = len(ts & ps), len(ts | ps)
        ledger.append(
            {
                "case_id": case_id,
                "policy": policy,
                "truth_members": list(t),
                "prediction_members": list(p),
                "intersection": len(set(t) & set(p)),
                "union": len(set(t) | set(p)),
                "service_intersection": intersection,
                "service_union": union,
            }
        )
        components.append({"numerator": intersection, "denominator": union})
    result.update(
        TP=tp,
        FP=fp,
        FN=fn,
        matching=ledger,
        missed_truth_groups=missed,
        background_prediction_groups=bg_predictions,
    )
    counts = {
        "pair_precision": (tp, tp + fp),
        "pair_recall": (tp, tp + fn),
        "pair_f1": (2 * tp, 2 * tp + fp + fn),
        "exact_group_precision": (exact, len(predicted)),
        "exact_group_recall": (exact, len(truth)),
        "exact_partition": (
            int(final_available and not earlier_failure and truth == predicted),
            1,
        ),
        "over_group_rate": (sum(n >= 2 for n in over), len(predicted)),
        "under_group_rate": (sum(n >= 2 for n in under), len(truth)),
        "exact_matches": (exact, 1),
        "over_groups": (sum(n >= 2 for n in over), 1),
        "under_groups": (sum(n >= 2 for n in under), 1),
        "extra_truth_intersections": (sum(max(0, n - 1) for n in over), 1),
        "partially_covered_truth_groups": (
            sum(bool(t & predicted_units) and not t <= predicted_units for t in tsets),
            1,
        ),
        "background_group_count": (len(bg_predictions), 1),
        "unassigned_unit_count": (len(units - predicted_units), 1),
        "missed_truth_group_count": (len(missed), 1),
    }
    metrics = {
        name: _metric(name, n, d, observed=int(final_available), failed=failed)
        for name, (n, d) in counts.items()
    }
    total_groups = max(len(truth), len(predicted))
    service_sum = sum(
        (Fraction(c["numerator"], c["denominator"]) for c in components), Fraction(0)
    )
    service_value = service_sum / total_groups if total_groups else Fraction(0)
    metrics["service_jaccard"] = _metric(
        "service_jaccard",
        service_value.numerator if total_groups else 0,
        service_value.denominator if total_groups else 0,
        components=components,
        observed=int(final_available),
        failed=failed,
    )

    support = {}
    if blast_truth is not None:
        if type(blast_truth) is not list:
            raise ValueError("blast_truth must be null or list")
        for annotation in blast_truth:
            if (
                type(annotation) is not dict
                or not {"group", "services"} <= annotation.keys()
            ):
                raise ValueError("invalid blast annotation")
            group_id = annotation["group"]
            if group_id not in truth_ids.values() or group_id in support:
                raise ValueError("unknown or repeated blast truth group")
            exposed = _unique(annotation["services"], "blast truth", tokens=False)
            if (
                not exposed
                or len(exposed) > 64
                or type(annotation["services"]) is not list
                or annotation["services"] != sorted(exposed)
            ):
                raise ValueError("blast services must be a nonempty sorted unique list")
            support[group_id] = exposed
    blast_observed = int(
        bool(support)
        and final_available
        and all(exposure is not None for exposure in exposures.values())
    )
    blast_kwargs = {
        "eligible": int(bool(support)),
        "observed": blast_observed,
        "failed": failed,
    }
    if not support:
        for name in (
            "blast_precision",
            "blast_recall",
            "blast_TP",
            "blast_FP",
            "blast_FN",
        ):
            metrics[name] = _metric(
                name, None, None, reason="unsupported_truth", **blast_kwargs
            )
    else:
        matched = dict(pairs)
        btp = bfp = bfn = 0
        for t, group_id in truth_ids.items():
            if group_id not in support:
                continue
            expected = support[group_id]
            p = matched.get(t)
            actual = (exposures[p] or frozenset()) if p is not None else frozenset()
            btp += len(expected & actual)
            bfp += len(actual - expected)
            bfn += len(expected - actual)
        assigned = {p for _, p in pairs}
        unmatched = [p for p in predicted if p not in assigned]
        # Section 7 excludes matches to unsupported truth. It makes only
        # *unmatched* prediction contributions unavailable under partial truth.
        precision_available = len(support) == len(truth) or not unmatched
        if len(support) == len(truth):
            bfp += sum(len(exposures[p] or ()) for p in unmatched)
        for name, n, d in (
            ("blast_TP", btp, 1),
            ("blast_FN", bfn, 1),
            ("blast_recall", btp, btp + bfn),
        ):
            metrics[name] = _metric(name, n, d, **blast_kwargs)
        for name, n, d in (("blast_FP", bfp, 1), ("blast_precision", btp, btp + bfp)):
            metrics[name] = (
                _metric(name, n, d, **blast_kwargs)
                if precision_available
                else _metric(
                    name, None, None, reason="incomplete_blast_truth", **blast_kwargs
                )
            )
    result["metrics"] = [metrics[name] for name in METRIC_NAMES]
    return result


def latency_summary(case_id, policy, scope, runs):
    """Summarize measured raw runs; samples retain input execution order.

    Primary stages live in DecisionOutcome.timings; enrichment stages live in
    EnrichmentOutcome.timings. RawRun.timings contains load/wall observations.
    A timeout censors every scope, even if some earlier stages were observed.
    """
    if case_id is not None:
        _token(case_id)
    _token(policy)
    if scope not in ("controlled_compute", "enrichment", "worker_wall"):
        raise ValueError("unknown latency scope")
    if type(runs) not in (list, tuple):
        raise ValueError("runs must be a sequence")
    samples, failed, censored = [], 0, 0
    for run in runs:
        plan = run["plan"]
        if plan["policy"] != policy or (
            case_id is not None and plan["case_id"] != case_id
        ):
            raise ValueError("run does not belong to latency scope")
        if _boolean(plan["warmup"], "warmup"):
            continue
        status = run["terminal_status"]
        if status not in STATUSES:
            raise ValueError("unknown terminal status")
        _integer(run["elapsed_ns"], "elapsed_ns")
        available = _boolean(
            run["final_grouping_available"], "final_grouping_available"
        )
        timings = (
            run["timings"]
            + [t for d in run["decisions"] for t in d["timings"]]
            + [t for e in run["enrichments"] for t in e["timings"]]
        )
        for timing in timings:
            _integer(timing["elapsed_ns"], "stage elapsed_ns")
            if timing["status"] not in ("observed", "failed", "censored"):
                raise ValueError("unknown timing status")
        if status == "TIMEOUT":
            censored += 1
            continue
        duration = None
        if scope == "worker_wall":
            if status == "SUCCESS":
                duration = run["elapsed_ns"]
        elif scope == "controlled_compute":
            ids = plan["decision_ids"]
            decisions = run["decisions"]
            if len(set(ids)) != len(ids) or not ids:
                raise ValueError("invalid planned decision IDs")
            if (
                len(decisions) == len(ids)
                and {d["decision"] for d in decisions} == set(ids)
                and all(d["status"] == "success" for d in decisions)
            ):
                selected = [
                    t
                    for d in decisions
                    for t in d["timings"]
                    if t["stage"] in ("projection", "scoring", "grouping")
                ]
                expected = {
                    (decision, stage)
                    for decision in ids
                    for stage in ("projection", "scoring", "grouping")
                }
                if (
                    len(selected) == len(expected)
                    and {(t["decision"], t["stage"]) for t in selected} == expected
                    and all(t["status"] == "observed" for t in selected)
                ):
                    duration = sum(t["elapsed_ns"] for t in selected)
        else:
            enrichments = run["enrichments"]
            if available and all(e["status"] == "success" for e in enrichments):
                selected = [
                    t
                    for e in enrichments
                    for t in e["timings"]
                    if t["stage"] in ("aggregate", "blast", "handoff")
                ]
                groups = [e["group"] for e in enrichments]
                expected = {
                    (group, stage)
                    for group in groups
                    for stage in ("aggregate", "blast", "handoff")
                }
                if (
                    len(groups) == len(set(groups))
                    and len(selected) == len(expected)
                    and {(t["group"], t["stage"]) for t in selected} == expected
                    and all(t["status"] == "observed" for t in selected)
                ):
                    duration = sum(t["elapsed_ns"] for t in selected)
        if duration is None:
            failed += 1
        else:
            samples.append(duration)
    ordered = sorted(samples)
    n = len(ordered)
    median = Fraction(ordered[(n - 1) // 2] + ordered[n // 2], 2) if n else None
    return {
        "case_id": case_id,
        "policy": policy,
        "scope": scope,
        "samples_ns": samples,
        "n": n,
        "failed": failed,
        "censored": censored,
        "minimum_ns": ordered[0] if n else None,
        "median_ns": decimal12(median) if n else None,
        "p95_ns": ordered[(95 * n + 99) // 100 - 1] if n else None,
        "maximum_ns": ordered[-1] if n else None,
    }


def _semantic(run):
    """Exclude only execution diagnostics; retain all embedded runtime payloads."""

    def errors(items):
        return [
            {"stage": item["stage"], "exception_type": item["exception_type"]}
            for item in items
        ]

    def outcome(item):
        return {
            key: errors(value) if key == "errors" else value
            for key, value in item.items()
            if key != "timings"
        }

    return canonical(
        {
            "terminal_status": run["terminal_status"],
            "final_grouping_available": run["final_grouping_available"],
            "decisions": [outcome(item) for item in run["decisions"]],
            "enrichments": [outcome(item) for item in run["enrichments"]],
            "errors": errors(run["errors"]),
        }
    )


def _fraction(metric):
    """Use exact integer components, never the rounded presentation value."""
    if metric["reason"] is not None:
        return None
    return Fraction(metric["numerator"], metric["denominator"])


def _aggregate(
    cases, policy, group_weights, *, population=None, success_conditioned=False
):
    micro, macro = [], []
    planned = len(cases) if population is None else population
    prefix = f"policy.{policy}." + ("grouping_success." if success_conditioned else "")
    by_name = {
        name: [next(m for m in case["metrics"] if m["name"] == name) for case in cases]
        for name in METRIC_NAMES
    }
    for name in METRIC_NAMES:
        rows = by_name[name]
        known = [row for row in rows if row["numerator"] is not None]
        defined = [row for row in rows if _fraction(row) is not None]
        coverage = {
            key: sum(row[key] for row in rows)
            for key in ("eligible", "observed", "failed")
        }
        coverage["failed"] += planned - len(cases)
        reason, components = None, None
        n = d = None
        if not cases and success_conditioned:
            reason = "no_successful_grouping_cases"
        elif name == "blast_precision" and len(known) != len(rows):
            reason = "incomplete_blast_truth"
        elif not known:
            reason = (
                "unsupported_truth"
                if (
                    all(r["reason"] == "unsupported_truth" for r in rows)
                    if success_conditioned
                    else name.startswith("blast_")
                )
                else "no_defined_cases"
            )
        elif name == "service_jaccard":
            components = [
                component for row in known for component in row["rational_components"]
            ]
            weight = sum(
                group_weights[case["case_id"]]
                for case in cases
                if case["truth_status"] == "complete"
            )
            exact = sum(
                (Fraction(c["numerator"], c["denominator"]) for c in components),
                Fraction(0),
            )
            if weight:
                value = exact / weight
                n, d = value.numerator, value.denominator
            else:
                n = d = 0
        elif name in _COUNT_METRICS:
            n, d = sum(row["numerator"] for row in known), 1
        else:
            n = sum(row["numerator"] for row in known)
            d = sum(row["denominator"] for row in known)
        row = _metric(name, n, d, reason=reason, components=components, **coverage)
        row.update(planned=planned, scope=prefix + "micro")
        micro.append(row)

        components = [
            {"numerator": r["numerator"], "denominator": r["denominator"]}
            for r in defined
        ]
        if defined:
            value = sum((_fraction(r) for r in defined), Fraction(0)) / len(defined)
            row = _metric(
                name,
                value.numerator,
                value.denominator,
                components=components,
                eligible=coverage["eligible"],
                observed=len(defined),
                failed=coverage["failed"],
            )
        else:
            row = _metric(
                name,
                None,
                None,
                reason="no_successful_grouping_cases"
                if not cases and success_conditioned
                else "no_defined_cases",
                components=None,
                eligible=coverage["eligible"],
                observed=0,
                failed=coverage["failed"],
            )
        row.update(planned=planned, scope=prefix + "macro")
        macro.append(row)

    if success_conditioned:
        return sorted(micro, key=lambda m: m["name"]), macro

    usable = [
        (tp, fp)
        for tp, fp in zip(by_name["blast_TP"], by_name["blast_FP"])
        if tp["numerator"] is not None and fp["numerator"] is not None
    ]
    tp_sum = sum(tp["numerator"] for tp, _ in usable)
    fp_sum = sum(fp["numerator"] for _, fp in usable)
    row = _metric(
        "blast_precision_observed",
        tp_sum,
        tp_sum + fp_sum,
        eligible=len(usable),
        observed=len(usable),
        failed=sum(tp["failed"] for tp, _ in usable),
    )
    row.update(planned=len(cases), scope=f"policy.{policy}.micro")
    micro.append(row)
    return sorted(micro, key=lambda m: m["name"]), macro


_DISPOSITIONS = (
    "grouped",
    "duplicate_ignored",
    "duplicate_rejected",
    "late_accepted",
    "late_rejected",
    "late_delayed",
)
_REJECTIONS = (
    "source_pair_rejected",
    "missing_pair_evidence",
    "ineligible_signal",
    "maximum_group_size_exceeded",
    "tenant_mismatch",
    "environment_mismatch",
    "run_context_mismatch",
    "time_span_exceeded",
    "topology_service_missing",
    "topology_service_ambiguous",
    "topology_disconnected",
    "topology_diameter_exceeded",
    "bridge_chain_blocked",
)


def _coverage(policy, cases, first, measured, runtime_by_id):
    counts = {}
    enrichments = [e for run in first for e in run["enrichments"]]
    counts["grouping_available"] = (
        sum(r["final_grouping_available"] for r in first),
        len(first),
    )
    counts["enrichment_available"] = (
        sum(e["status"] == "success" for e in enrichments),
        len(enrichments),
    )
    for status in ("complete", "unknown", "ambiguous"):
        counts[f"truth_{status}"] = (
            sum(c["truth_status"] == status for c in cases),
            len(cases),
        )
    counts["zero_output"] = (
        sum(
            r["final_grouping_available"]
            and not r["decisions"][-1]["grouping_result"]["groups"]
            for r in first
        ),
        len(first),
    )
    counts["unassigned_units"] = (
        sum(len(c["unassigned_units"]) for c in cases),
        sum(len(runtime_by_id[c["case_id"]]["units"]) for c in cases),
    )
    captured = sum(
        len(r["decisions"][-1]["arguments"]["runtime_inputs"])
        for r in measured
        if r["decisions"]
        and r["decisions"][-1]["decision"] == r["plan"]["decision_ids"][-1]
    )
    counts["deliveries_captured"] = (
        captured,
        sum(len(runtime_by_id[r["plan"]["case_id"]]["deliveries"]) for r in measured),
    )
    records, edges = [], []
    for run in measured:
        for decision in run["decisions"]:
            result = decision["grouping_result"]
            if result is not None:
                records.extend(result["signal_records"])
                edges.extend(result["rejected_edges"])
    if any(r["disposition"] not in _DISPOSITIONS for r in records):
        raise ValueError("unknown signal disposition")
    if any(e["reason"] not in _REJECTIONS for e in edges):
        raise ValueError("unknown edge rejection reason")
    for disposition in _DISPOSITIONS:
        counts[f"disposition.{disposition}"] = (
            sum(r["disposition"] == disposition for r in records),
            len(records),
        )
    for reason in _REJECTIONS:
        counts[f"rejection.{reason}"] = (
            sum(e["reason"] == reason for e in edges),
            len(edges),
        )
    result = []
    for name, (n, d) in sorted(counts.items()):
        if n > d:
            raise ValueError(f"coverage {name} exceeds its authenticated population")
        row = _metric(name, n, d, eligible=d, observed=n, failed=d - n)
        row.update(planned=d, scope=f"policy.{policy}.coverage")
        result.append(row)
    return result


def _targets(policy, cases, controlled):
    n = controlled["p95_ns"]
    complete = (
        controlled["n"] == 160 and controlled["failed"] == controlled["censored"] == 0
    )
    budget = {
        "policy": policy,
        "target": "controlled_budget",
        "threshold": "<5000000000ns",
        "scope": "controlled_compute_p95",
        "measured": decimal12(n) if n is not None else None,
        "status": ("MET" if n < 5_000_000_000 else "MISSED")
        if complete
        else "NOT_DEMONSTRATED",
        "reason": None if complete else "incomplete_successful_grouping_coverage",
    }
    canonical_cases = cases[:8]
    complete = len(canonical_cases) == 8 and all(
        c["truth_status"] == "complete" for c in canonical_cases
    )
    rate = (
        sum(
            (
                next(
                    _fraction(m) for m in c["metrics"] if m["name"] == "exact_partition"
                )
                for c in canonical_cases
            ),
            Fraction(0),
        )
        / 8
        if complete
        else None
    )
    quality = {
        "policy": policy,
        "target": "known_pattern_quality_proxy",
        "threshold": ">9/10",
        "scope": "eight_canonical_exact_partition",
        "measured": decimal12(rate) if rate is not None else None,
        "status": ("MET" if rate > Fraction(9, 10) else "MISSED")
        if complete
        else "NOT_DEMONSTRATED",
        "reason": None if complete else "incomplete_canonical_truth",
    }
    live = {
        "policy": policy,
        "target": "live_grouping_latency",
        "threshold": "<5s",
        "scope": "live_orchestration",
        "measured": None,
        "status": "NOT_MEASURED",
        "reason": "outside_task_4.11_scope",
    }
    return [budget, quality, live]


def _difference(expected, actual, path="$"):
    """Return the first differing field, including strict type/array/key checks."""
    if type(expected) is not type(actual):
        return f"{path}: type mismatch"
    if type(expected) is dict:
        if set(expected) != set(actual):
            return f"{path}: field mismatch"
        for key in sorted(expected):
            difference = _difference(expected[key], actual[key], f"{path}.{key}")
            if difference:
                return difference
    elif type(expected) is list:
        if len(expected) != len(actual):
            return f"{path}: array length mismatch"
        for index, (left, right) in enumerate(zip(expected, actual)):
            difference = _difference(left, right, f"{path}[{index}]")
            if difference:
                return difference
    elif expected != actual:
        return f"{path}: value mismatch"
    return None


def _timestamp(value):
    if (
        type(value) is not str
        or re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", value, re.ASCII)
        is None
    ):
        raise ValueError("expected exact six-place UTC timestamp")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")


def _index(records, field):
    result = {}
    if type(records) is not list:
        raise ValueError("expected record list")
    for record in records:
        key = _token(record[field])
        if key in result:
            raise ValueError(f"duplicate {field}: {key}")
        result[key] = record
    return result


def _validate_errors(items):
    if type(items) is not list:
        raise ValueError("expected error list")
    for error in items:
        _record(error, "SafeError")
        for key, limit in (("stage", 128), ("exception_type", 256), ("message", 1024)):
            text = error[key]
            if type(text) is not str or not text.strip() or len(text) > limit:
                raise ValueError(f"invalid error {key}")


def _validate_timings(items):
    if type(items) is not list:
        raise ValueError("expected timing list")
    for timing in items:
        _record(timing, "StageTiming")
        _integer(timing["elapsed_ns"], "elapsed_ns")
        if timing["stage"] not in (
            "projection",
            "scoring",
            "grouping",
            "aggregate",
            "blast",
            "handoff",
            "load",
            "worker_wall",
        ):
            raise ValueError("unknown timing stage")
        if timing["status"] not in ("observed", "failed", "censored"):
            raise ValueError("unknown timing status")
        if timing["decision"] is not None:
            _token(timing["decision"])
        if timing["group"] is not None:
            _sha256(timing["group"])


def _validate_timeout(run):
    evidence = run["timeout"]
    if run["terminal_status"] != "TIMEOUT":
        if evidence is not None:
            raise ValueError("non-timeout run contains timeout evidence")
        return
    _record(evidence, "TimeoutEvidence")
    for key in ("deadline_ns", "elapsed_ns", "pid"):
        _integer(evidence[key], f"timeout {key}")
    if not evidence["deadline_ns"] or not evidence["pid"]:
        raise ValueError("timeout deadline and PID must be positive")
    _boolean(evidence["terminated"], "timeout terminated")
    _text(evidence["last_stage"])


def _validate_inputs(config, runtime, truth, raw):
    """Authenticate record links and the complete fixed planned population."""
    for value, name in (
        (config, "Config"),
        (runtime, "RuntimeInputs"),
        (truth, "TruthInputs"),
        (raw, "RawOutcomes"),
    ):
        _json_value(value)
        _record(value, name)
        if value["schema_version"] != SCHEMA_VERSION:
            raise ValueError(f"{name}: wrong schema version")
    if (
        config["metric_version"] != METRIC_VERSION
        or type(config["seed"]) is not int
        or config["seed"] != 42
    ):
        raise ValueError("wrong metric version or seed")
    if config["epoch"] != "2026-01-01T00:00:00.000000Z":
        raise ValueError("wrong fixed epoch")
    _sha256(config["protocol_fingerprint"])
    if (
        type(config["source_revision"]) is not str
        or re.fullmatch(r"[0-9a-f]{40}", config["source_revision"]) is None
    ):
        raise ValueError("invalid source revision")
    source_identities = _source_identities(
        config["source_identities"], "$.config.source_identities"
    )
    repetitions = config["repetitions"]
    if (
        type(repetitions) is not dict
        or set(repetitions) != {"warmups", "measured"}
        or type(repetitions["warmups"]) is not int
        or type(repetitions["measured"]) is not int
        or repetitions != {"warmups": 1, "measured": 10}
    ):
        raise ValueError("$.config.repetitions: expected one warmup and ten measured")
    ids = config["cases"]
    if type(ids) is not list or len(ids) != 16 or len(_unique(ids, "case IDs")) != 16:
        raise ValueError("exactly sixteen unique cases required")
    if any(re.fullmatch(r"case-[0-9]{3}", c) is None for c in ids):
        raise ValueError("invalid opaque case ID")
    policies = _index(config["policies"], "name")
    if len(policies) != 2:
        raise ValueError("exactly two policies required")
    for policy in policies.values():
        if set(policy) != {"name", "config", "configuration_fingerprint"}:
            raise ValueError("unexpected policy fields")
        _sha256(policy["configuration_fingerprint"])
    topologies = _index(config["topologies"], "name")
    for topology in topologies.values():
        if set(topology) != {"name", "snapshot", "pin"}:
            raise ValueError("unexpected topology fields")
        pin, snapshot = topology["pin"], topology["snapshot"]
        if type(pin) is not dict or set(pin) != {
            "snapshot_fingerprint",
            "topology_version",
        }:
            raise ValueError("invalid independent topology pin")
        if (
            _sha256(pin["snapshot_fingerprint"]) != snapshot["snapshot_fingerprint"]
            or pin["topology_version"] != snapshot["version"]["topology_version"]
        ):
            raise ValueError("topology snapshot does not match its independent pin")
    runtime_by_id, truth_by_id = (
        _index(runtime["cases"], "case_id"),
        _index(truth["cases"], "case_id"),
    )
    if list(runtime_by_id) != ids or list(truth_by_id) != ids:
        raise ValueError("runtime/truth case order or population mismatch")
    for case_id in ids:
        case, annotation = runtime_by_id[case_id], truth_by_id[case_id]
        _record(case, "RuntimeCase")
        _record(annotation, "TruthCase")
        units = _index(case["units"], "unit")
        if len(units) > 91 or set(annotation["universe"]) != set(units):
            raise ValueError("truth/runtime universe mismatch or capacity excess")
        _unique(annotation["universe"], "truth universe")
        if case["topology_key"] not in topologies:
            raise ValueError("unknown topology key")
        for unit in units.values():
            _record(unit, "RuntimeUnit")
            _text(unit["service"])
            _text(unit["metric"])
            if type(unit["observed_value"]) not in (int, float):
                raise ValueError("observed value must be finite number, never bool")
            _integer(unit["event_offset_seconds"], "event_offset_seconds")
        deliveries, decisions = (
            _index(case["deliveries"], "delivery"),
            _index(case["decisions"], "decision"),
        )
        if len(deliveries) > 182 or not 1 <= len(decisions) <= 8:
            raise ValueError("delivery/decision capacity violation")
        for delivery in deliveries.values():
            _record(delivery, "Delivery")
            if delivery["unit"] not in units:
                raise ValueError("delivery references unknown unit")
            _integer(delivery["arrival_offset_seconds"], "arrival_offset_seconds")
        for decision in decisions.values():
            _record(decision, "Decision")
            _integer(decision["arrival_cutoff_seconds"], "arrival_cutoff_seconds")
            _integer(decision["watermark_offset_seconds"], "watermark_offset_seconds")
    digests = {
        name: _digest(value)
        for name, value in (
            ("config", config),
            ("runtime", runtime),
            ("truth", truth),
            ("raw", raw),
        )
    }
    if raw["config_sha256"] != digests["config"]:
        raise ValueError("raw/config digest mismatch")
    for name in ("runtime", "truth"):
        identity = config[f"{name}_identity"]
        if identity["sha256"] != digests[name] or _integer(
            identity["bytes"], "identity bytes"
        ) != len(canonical(runtime if name == "runtime" else truth)):
            raise ValueError(f"config/{name} identity mismatch")
    expected_plans = []
    for policy in policies.values():
        for case in runtime_by_id.values():
            pin = topologies[case["topology_key"]]["pin"]["snapshot_fingerprint"]
            for warmup, count in ((True, 1), (False, 10)):
                for repetition in range(count):
                    kind = "warmup" if warmup else "measured"
                    expected_plans.append(
                        dict(
                            run=f"{case['case_id']}.{policy['name']}.{kind}.{repetition:03}",
                            case_id=case["case_id"],
                            policy=policy["name"],
                            configuration_fingerprint=policy[
                                "configuration_fingerprint"
                            ],
                            repetition=repetition,
                            warmup=warmup,
                            source_revision=config["source_revision"],
                            config_sha256=digests["config"],
                            runtime_sha256=digests["runtime"],
                            truth_sha256=digests["truth"],
                            topology_fingerprint=pin,
                            decision_ids=[d["decision"] for d in case["decisions"]],
                        )
                    )
    difference = _difference(expected_plans, raw["planned"], "$.planned")
    if difference:
        raise ValueError(f"expected planned manifest: {difference}")
    plans = _index(raw["planned"], "run")
    runs = {}
    # A raw run carries its identity inside plan; never flatten or repair it.
    if type(raw["runs"]) is not list:
        raise ValueError("expected raw run list")
    for run in raw["runs"]:
        _record(run, "RawRun")
        label = _token(run["plan"]["run"])
        if label in runs:
            raise ValueError("duplicate terminal raw run")
        runs[label] = run
    if len(plans) != 352 or list(runs) != list(plans):
        raise ValueError("missing, extra or reordered planned/terminal runs")
    grouped = {(case, policy): [] for case in ids for policy in policies}
    for run_index, (label, plan) in enumerate(plans.items()):
        _record(plan, "PlannedRun")
        run = runs[label]
        difference = _difference(plan, run["plan"], f"$.runs[{run_index}].plan")
        if difference:
            raise ValueError(f"raw run does not match authenticated plan: {difference}")
        where = f"$.runs[{run_index}]"
        before = _source_identities(run["sources_before"], where + ".sources_before")
        _source_identities(run["sources_after"], where + ".sources_after")
        difference = _difference(
            run["sources_before"], run["sources_after"], where + ".sources_after"
        )
        if difference:
            raise ValueError(f"source mutated during run: {difference}")
        for path, expected in source_identities.items():
            if path not in before:
                raise ValueError(
                    f"{where}.sources_before: missing configured source {path}"
                )
            index = next(
                i
                for i, item in enumerate(run["sources_before"])
                if item["path"] == path
            )
            difference = _difference(
                expected, before[path], f"{where}.sources_before[{index}]"
            )
            if difference:
                raise ValueError(f"configured source identity mismatch: {difference}")
        pair = (plan["case_id"], plan["policy"])
        if pair not in grouped:
            raise ValueError("unplanned case or policy")
        _integer(plan["repetition"], "repetition")
        _boolean(plan["warmup"], "warmup")
        for name in ("config", "runtime", "truth"):
            if plan[f"{name}_sha256"] != digests[name]:
                raise ValueError(f"planned {name} digest mismatch")
        case = runtime_by_id[plan["case_id"]]
        pin = topologies[case["topology_key"]]["pin"]["snapshot_fingerprint"]
        if (
            plan["source_revision"] != config["source_revision"]
            or plan["configuration_fingerprint"]
            != policies[plan["policy"]]["configuration_fingerprint"]
            or plan["topology_fingerprint"] != _sha256(pin)
            or plan["decision_ids"] != [d["decision"] for d in case["decisions"]]
        ):
            raise ValueError("planned source/policy/topology/decision mismatch")
        _integer(run["elapsed_ns"], "elapsed_ns")
        if _timestamp(run["utc_end"]) < _timestamp(run["utc_start"]):
            raise ValueError("negative UTC runtime interval")
        if run["terminal_status"] not in STATUSES:
            raise ValueError("unknown terminal status")
        available = _boolean(
            run["final_grouping_available"], "final_grouping_available"
        )
        if (
            not available and run["terminal_status"] in ("SUCCESS", "PARTIAL_FAILURE")
        ) or (
            available
            and run["terminal_status"]
            in ("RUNTIME_ERROR", "CONTEXT_UNAVAILABLE", "CAPACITY_REJECTED")
        ):
            raise ValueError("raw terminal status contradicts grouping availability")
        _validate_timeout(run)
        _validate_errors(run["errors"])
        _validate_timings(run["timings"])
        decision_ids = []
        for decision in run["decisions"]:
            _record(decision, "DecisionOutcome")
            decision_ids.append(decision["decision"])
            if decision["status"] not in ("success", "failed"):
                raise ValueError("unknown decision status")
            if (decision["status"] == "success") != (
                decision["grouping_result"] is not None
            ):
                raise ValueError("decision status/grouping result mismatch")
            _validate_errors(decision["errors"])
            _validate_timings(decision["timings"])
            if decision["grouping_result"] is not None:
                _logical_groups(case, decision["grouping_result"])
        expected_ids = plan["decision_ids"]
        if decision_ids != expected_ids[: len(decision_ids)]:
            raise ValueError("unknown, duplicated or reordered captured decisions")
        if available and (
            decision_ids != expected_ids or run["decisions"][-1]["status"] != "success"
        ):
            raise ValueError(
                "available final grouping missing successful final decision"
            )
        for enrichment in run["enrichments"]:
            _record(enrichment, "EnrichmentOutcome")
            _sha256(enrichment["group"])
            if enrichment["status"] not in ("success", "failed"):
                raise ValueError("unknown enrichment status")
            _validate_errors(enrichment["errors"])
            _validate_timings(enrichment["timings"])
        if run["terminal_status"] == "SUCCESS" and (
            any(d["status"] != "success" for d in run["decisions"])
            or any(e["status"] != "success" for e in run["enrichments"])
        ):
            raise ValueError("successful run contains failed decision/enrichment")
        # Validate every run, including warmups, before repetition comparison.
        _predictions(case, run)
        grouped[pair].append(run)
    for pair, items in grouped.items():
        warmup = [r for r in items if r["plan"]["warmup"]]
        measured = [r for r in items if not r["plan"]["warmup"]]
        if (
            len(warmup) != 1
            or len(measured) != 10
            or len({r["plan"]["repetition"] for r in measured}) != 10
        ):
            raise ValueError(
                "case/policy requires one warmup and ten unique measured repetitions"
            )
        semantic = _semantic(measured[0])
        if any(_semantic(run) != semantic for run in measured[1:]):
            raise ValueError(f"semantic repetition mismatch: {pair[0]}/{pair[1]}")
    return runtime_by_id, truth_by_id, policies, grouped, digests


def _logical_groups(case, grouping_result):
    """Validate all decision groups against independently generated UUIDs."""
    namespace = uuid5(NAMESPACE_URL, "aegisops:correlation-evaluation:1.0")
    run_id = uuid5(namespace, f"case:{int(case['case_id'][5:])}:seed:42")
    by_uuid = {
        str(uuid5(run_id, f"unit:{index}")): unit
        for index, unit in enumerate(case["units"], 1)
    }
    result, emitted = [], set()
    groups = grouping_result["groups"]
    if type(groups) is not list or len(groups) > 91:
        raise ValueError("invalid emitted group collection")
    for group in groups:
        fingerprint = _sha256(group["group_fingerprint"])
        if fingerprint in emitted:
            raise ValueError("duplicate emitted group fingerprint")
        emitted.add(fingerprint)
        members = []
        if type(group["members"]) is not list or not 1 <= len(group["members"]) <= 91:
            raise ValueError("invalid emitted group member collection")
        for member in group["members"]:
            signal_id = member["signal_id"]
            if (
                type(signal_id) is not str
                or str(UUID(signal_id)) != signal_id
                or signal_id not in by_uuid
            ):
                raise ValueError("unknown or noncanonical member signal UUID")
            unit = by_uuid[signal_id]
            if member["service"] != unit["service"]:
                raise ValueError(
                    "member service differs from authenticated logical unit"
                )
            members.append(unit["unit"])
        if not members or len(_unique(members, "prediction members")) != len(members):
            raise ValueError("empty or repeated group member")
        result.append(
            {"fingerprint": fingerprint, "members": sorted(members), "exposure": None}
        )
    _groups(result, "prediction")
    return result


def _predictions(case, run):
    """Project embedded wire payloads using an independently generated UUID map.

    Field paths are from the approved 4.8 A1 group contract and 4.10 A1
    BlastRadiusContext contract, not production metric or matching code.
    """
    if not run["final_grouping_available"]:
        if run["enrichments"]:
            raise ValueError("unavailable final grouping has committed enrichments")
        return []
    result = _logical_groups(case, run["decisions"][-1]["grouping_result"])
    enrichments = _index(run["enrichments"], "group")
    for prediction in result:
        fingerprint = prediction["fingerprint"]
        enrichment = enrichments.get(fingerprint)
        if enrichment is None:
            raise ValueError("missing emitted-group enrichment outcome")
        exposure = None
        if enrichment["status"] == "success":
            if any(
                enrichment[key] is None
                for key in ("aggregate", "blast_context", "handoff")
            ):
                raise ValueError("successful enrichment lacks a committed payload")
            reachable = enrichment["blast_context"]["reachable_services"]
            names = [service["name"] for service in reachable]
            _unique(names, "reachable services", tokens=False)
            exposure = sorted(names)
        prediction["exposure"] = exposure
    if set(enrichments) != {p["fingerprint"] for p in result}:
        raise ValueError("enrichment population differs from emitted groups")
    _groups(result, "prediction")
    return result


def recompute(config, runtime, truth, raw):
    """Independently derive the entire ProcessedComparison from raw records.

    Input identities are SHA-256 of canonical source bytes including final LF.
    No processed value, production reducer, matching function or renderer is
    consulted. All population and repetition checks precede quality reduction.
    """
    runtime_by_id, truth_by_id, policies, grouped, digests = _validate_inputs(
        config, runtime, truth, raw
    )
    first, cases, weights = {}, [], {}
    for case_id in config["cases"]:
        for policy in policies:
            items = grouped[(case_id, policy)]
            run = next(r for r in items if not r["plan"]["warmup"])
            first[(case_id, policy)] = run
            annotation, source = truth_by_id[case_id], runtime_by_id[case_id]
            predictions = _predictions(source, run)
            earlier_failure = any(
                d["status"] != "success" for d in run["decisions"][:-1]
            )
            cases.append(
                case_quality(
                    case_id,
                    policy,
                    annotation["universe"],
                    {unit["unit"]: unit["service"] for unit in source["units"]},
                    annotation["truth_status"],
                    annotation["truth_groups"],
                    annotation["background_units"],
                    annotation["blast_truth"],
                    predictions,
                    run["terminal_status"],
                    run["final_grouping_available"],
                    earlier_failure,
                )
            )
            weights[(case_id, policy)] = max(
                len(annotation["truth_groups"]), len(predictions)
            )
    micro, macro, statuses, latency, targets, coverage = [], [], [], [], [], []
    for policy in policies:
        policy_cases = [c for c in cases if c["policy"] == policy]
        policy_runs = [r for r in raw["runs"] if r["plan"]["policy"] == policy]
        measured = [r for r in policy_runs if not r["plan"]["warmup"]]
        warmup = [r for r in policy_runs if r["plan"]["warmup"]]
        first_runs = [first[(case_id, policy)] for case_id in config["cases"]]
        p_micro, p_macro = _aggregate(
            policy_cases,
            policy,
            {case: weights[(case, policy)] for case in config["cases"]},
        )
        micro.extend(p_micro)
        macro.extend(p_macro)
        successful = [
            case
            for case in policy_cases
            if first[(case["case_id"], policy)]["final_grouping_available"]
            and all(
                d["status"] == "success"
                for d in first[(case["case_id"], policy)]["decisions"]
            )
        ]
        success_micro, success_macro = _aggregate(
            successful,
            policy,
            {case: weights[(case, policy)] for case in config["cases"]},
            population=len(policy_cases),
            success_conditioned=True,
        )
        micro.extend(success_micro)
        macro.extend(success_macro)
        for scope, runs in (
            ("measured", measured),
            ("warmup", warmup),
            ("case", first_runs),
        ):
            statuses.extend(
                {
                    "scope": scope,
                    "policy": policy,
                    "status": status,
                    "count": sum(r["terminal_status"] == status for r in runs),
                }
                for status in STATUSES
            )
        controlled = None
        for case_id in config["cases"] + [None]:
            runs = [
                r
                for r in measured
                if case_id is None or r["plan"]["case_id"] == case_id
            ]
            for scope in ("controlled_compute", "enrichment", "worker_wall"):
                row = latency_summary(case_id, policy, scope, runs)
                if row["n"] + row["failed"] + row["censored"] != len(runs):
                    raise ValueError("latency population does not reconcile")
                latency.append(row)
                if case_id is None and scope == "controlled_compute":
                    controlled = row
        targets.extend(_targets(policy, policy_cases, controlled))
        coverage.extend(
            _coverage(policy, policy_cases, first_runs, measured, runtime_by_id)
        )
    result = {
        "schema_version": SCHEMA_VERSION,
        "metric_version": METRIC_VERSION,
        **{f"{name}_sha256": digest for name, digest in digests.items()},
        "cases": cases,
        "policy_micro": micro,
        "policy_macro": macro,
        "statuses": statuses,
        "latency": latency,
        "targets": targets,
        "coverage": coverage,
    }
    canonical(result)
    return result


PACKAGE_FILES = (
    "config.json",
    "runtime_inputs.json",
    "truth.json",
    "raw_outcomes.json",
    "processed_comparison.json",
    "processed_comparison.csv",
    "figures/grouping_quality.svg",
    "figures/status_latency.svg",
    "report.md",
    "manifest.json",
)
PACKAGE_LIMITS = dict(
    zip(
        PACKAGE_FILES,
        (
            2097152,
            2097152,
            2097152,
            67108864,
            8388608,
            8388608,
            1048576,
            1048576,
            2097152,
            1048576,
        ),
    )
)
CSV_FIELDS = (
    "scope",
    "case_id",
    "policy",
    "metric",
    "numerator",
    "denominator",
    "value",
    "reason",
    "planned",
    "eligible",
    "observed",
    "failed",
    "raw_sha256",
    "truth_sha256",
    "runtime_sha256",
    "metric_version",
)


def _artifact_buffer(chunks, limit):
    data = bytearray()
    for text in chunks:
        raw = text.encode("utf-8")
        if len(data) + len(raw) > limit:
            raise ValueError("artifact projection exceeds byte limit")
        data.extend(raw)
    return bytes(data)


def _artifact_json(obj, limit):
    _json_value(obj)
    return _artifact_buffer(
        chain(
            json.JSONEncoder(
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).iterencode(obj),
            ("\n",),
        ),
        limit,
    )


def _project_scalar(
    scope, name, rational, planned, observed, failed, reason=None, eligible=None
):
    return dict(
        scope=scope,
        name=name,
        numerator=None if rational is None else rational.numerator,
        denominator=None if rational is None else rational.denominator,
        value=None if rational is None else decimal12(rational),
        reason=reason,
        planned=planned,
        eligible=planned if eligible is None else eligible,
        observed=observed,
        failed=failed,
    )


def _projection_rows(config, processed):
    def record(metric, case, policy):
        values = [metric["scope"], case, policy, metric["name"]]
        values.extend(
            metric[key]
            for key in (
                "numerator",
                "denominator",
                "value",
                "reason",
                "planned",
                "eligible",
                "observed",
                "failed",
            )
        )
        values.extend(
            processed[key]
            for key in (
                "raw_sha256",
                "truth_sha256",
                "runtime_sha256",
                "metric_version",
            )
        )
        return [str(value) if value is not None else "" for value in values]

    for case in processed["cases"]:
        for metric in case["metrics"]:
            yield record(metric, case["case_id"], case["policy"])
    for collection in ("policy_micro", "policy_macro", "coverage"):
        for metric in processed[collection]:
            labels = [
                policy["name"]
                for policy in config["policies"]
                if metric["scope"].startswith("policy." + policy["name"] + ".")
            ]
            if len(labels) != 1:
                raise ValueError("metric scope does not identify one configured policy")
            yield record(metric, "", labels[0])
    for item in processed["statuses"]:
        population = sum(
            v["count"]
            for v in processed["statuses"]
            if (v["policy"], v["scope"]) == (item["policy"], item["scope"])
        )
        metric = _project_scalar(
            "status." + item["scope"],
            item["status"],
            Fraction(item["count"], population),
            population,
            population,
            0,
        )
        metric["numerator"] = item["count"]
        metric["denominator"] = population
        yield record(metric, "", item["policy"])
    for item in processed["latency"]:
        values = {
            key: None if item[key] is None else Fraction(item[key])
            for key in ("n", "failed", "censored", "minimum_ns", "p95_ns", "maximum_ns")
        }
        ordered = sorted(item["samples_ns"])
        n = len(ordered)
        values["median_ns"] = (
            Fraction(ordered[(n - 1) // 2] + ordered[n // 2], 2) if n else None
        )
        names = [
            "n",
            "failed",
            "censored",
            "minimum_ns",
            "median_ns",
            "p95_ns",
            "maximum_ns",
        ]
        names.extend(f"sample.{index:03}" for index in range(n))
        values.update(
            {
                f"sample.{index:03}": Fraction(v)
                for index, v in enumerate(item["samples_ns"])
            }
        )
        for name in names:
            rational = values[name]
            metric = _project_scalar(
                "latency." + item["scope"],
                name,
                rational,
                item["n"] + item["failed"] + item["censored"],
                item["n"],
                item["failed"],
                "no_successful_samples" if rational is None else None,
            )
            yield record(
                metric,
                item["case_id"] if item["case_id"] is not None else "",
                item["policy"],
            )
    for target in processed["targets"]:
        rational = None
        planned = eligible = observed = failed = 0
        if target["target"] == "known_pattern_quality_proxy":
            population = [
                case
                for case in processed["cases"]
                if case["policy"] == target["policy"]
                and case["case_id"] in config["cases"][:8]
            ]
            planned = 8
            eligible = observed = sum(
                case["truth_status"] == "complete" for case in population
            )
            partitions = [
                next(m for m in case["metrics"] if m["name"] == "exact_partition")
                for case in population
            ]
            failed = sum(m["failed"] for m in partitions)
            if target["measured"] is not None:
                rational = Fraction(sum(m["numerator"] for m in partitions), 8)
        elif target["target"] == "controlled_budget":
            item = next(
                row
                for row in processed["latency"]
                if row["case_id"] is None
                and row["policy"] == target["policy"]
                and row["scope"] == "controlled_compute"
            )
            planned = item["n"] + item["failed"] + item["censored"]
            eligible = observed = item["n"]
            failed = item["failed"]
            if item["p95_ns"] is not None:
                rational = Fraction(item["p95_ns"])
        metric = _project_scalar(
            "target." + target["scope"],
            target["target"] + "." + target["status"],
            rational,
            planned,
            observed,
            failed,
            target["reason"],
            eligible,
        )
        if (
            target["target"] == "known_pattern_quality_proxy"
            and target["measured"] is not None
        ):
            metric.update(
                numerator=sum(m["numerator"] for m in partitions), denominator=8
            )
        yield record(metric, "", target["policy"])


def _expected_csv(config, processed):
    def pieces():
        for row in chain((CSV_FIELDS,), _projection_rows(config, processed)):
            stream = io.StringIO(newline="")
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(row)
            yield stream.getvalue()

    return _artifact_buffer(pieces(), 8388608)


def _markdown_atom(value):
    text = "null" if value is None else str(value)
    return (
        escape(text)
        .replace("|", "&#124;")
        .replace("`", "&#96;")
        .replace("\r", "")
        .replace("\n", "<br>")
    )


def _markdown_table(header, records):
    header = tuple(header)
    yield "| " + " | ".join(header) + " |\n"
    yield "| " + " | ".join(["---"] * len(header)) + " |\n"
    for record in records:
        yield "| " + " | ".join(_markdown_atom(v) for v in record) + " |\n"
    yield "\n"


def _expected_report(config, raw, p):
    def chunks():
        yield "# Correlation evaluation report\n\n"
        yield "Declared synthetic anomaly observations; supplied signals condition these grouping results. Collection completeness does not certify independent executions, production accuracy, causality or a live SLA. Failures and unavailable values remain in the population. No evaluation-partition tuning is permitted.\n\n"
        yield "## Provenance and pinned inputs\n\n"
        provenance = [
            (field, config[field])
            for field in (
                "protocol_fingerprint",
                "source_revision",
                "seed",
                "metric_version",
            )
        ]
        provenance.extend(
            (field, p[field])
            for field in (
                "raw_sha256",
                "truth_sha256",
                "runtime_sha256",
                "config_sha256",
            )
        )
        yield from _markdown_table(("Field", "Value"), provenance)
        yield from _markdown_table(
            ("Policy", "Fingerprint"),
            ((v["name"], v["configuration_fingerprint"]) for v in config["policies"]),
        )
        yield from _markdown_table(
            ("Topology", "Version", "Fingerprint"),
            (
                (
                    v["name"],
                    v["pin"]["topology_version"],
                    v["pin"]["snapshot_fingerprint"],
                )
                for v in config["topologies"]
            ),
        )
        yield "## Declared targets beside measured results\n\nThe >90% proxy uses eight canonical cases (8/8 required). Controlled p95 <5000000000ns is supplemental; live <5s remains NOT_MEASURED. Negative results do not fail artifact correctness.\n\n"
        yield from _markdown_table(
            ("Policy", "Target", "Threshold", "Scope", "Measured", "Status", "Reason"),
            (
                [
                    v[key]
                    for key in (
                        "policy",
                        "target",
                        "threshold",
                        "scope",
                        "measured",
                        "status",
                        "reason",
                    )
                ]
                for v in p["targets"]
            ),
        )
        yield "## Case outcomes and grouping errors\n\n"

        def cases():
            for case in p["cases"]:
                metrics_by_key = {v["name"]: v for v in case["metrics"]}
                f1 = metrics_by_key["pair_f1"]
                yield [
                    case["case_id"],
                    case["policy"],
                    case["status"],
                    case["truth_status"],
                    case["TP"],
                    case["FP"],
                    case["FN"],
                    f1["value"] if f1["value"] is not None else f1["reason"],
                    metrics_by_key["exact_partition"]["value"],
                    metrics_by_key["over_groups"]["numerator"],
                    metrics_by_key["under_groups"]["numerator"],
                    *[
                        json.dumps(case[key], separators=(",", ":"))
                        for key in (
                            "unassigned_units",
                            "missed_truth_groups",
                            "background_prediction_groups",
                        )
                    ],
                ]

        yield from _markdown_table(
            (
                "Case",
                "Policy",
                "Status",
                "Truth",
                "TP",
                "FP",
                "FN",
                "Pair F1/reason",
                "Exact partition",
                "False merges",
                "False splits",
                "Unassigned",
                "Missed",
                "Background groups",
            ),
            cases(),
        )
        yield "## Membership matching ledger\n\n"

        def matches():
            for case in p["cases"]:
                for pair in case["matching"] or []:
                    yield [
                        case["case_id"],
                        case["policy"],
                        json.dumps(pair["truth_members"], separators=(",", ":")),
                        json.dumps(pair["prediction_members"], separators=(",", ":")),
                        *[
                            pair[key]
                            for key in (
                                "intersection",
                                "union",
                                "service_intersection",
                                "service_union",
                            )
                        ],
                    ]

        yield from _markdown_table(
            (
                "Case",
                "Policy",
                "Truth members",
                "Prediction members",
                "Intersection",
                "Union",
                "Service intersection",
                "Service union",
            ),
            matches(),
        )
        yield "## Raw run census (warmups retained separately)\n\n"

        def census():
            for run in raw["runs"]:
                errors = list(run["errors"])
                for item in chain(run["decisions"], run["enrichments"]):
                    errors.extend(item["errors"])
                yield [
                    run["plan"]["run"],
                    run["plan"]["warmup"],
                    run["terminal_status"],
                    run["final_grouping_available"],
                    run["utc_start"],
                    run["utc_end"],
                    run["elapsed_ns"],
                    json.dumps(
                        errors,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                ]

        yield from _markdown_table(
            (
                "Run",
                "Warmup",
                "Status",
                "Final grouping",
                "UTC start",
                "UTC end",
                "Worker wall ns",
                "Errors",
            ),
            census(),
        )
        yield "## Complete scalar projection\n\nRows and first twelve columns match processed_comparison.csv. The four repeated provenance constants are listed above. Latency statistics/samples use nanoseconds; failed/censored counts remain explicit. Operational and grouping-success scopes retain distinct denominators. Full native payloads and rational-component ledgers remain in raw_outcomes.json and processed_comparison.json.\n\n"
        yield from _markdown_table(
            CSV_FIELDS[:12], (row[:12] for row in _projection_rows(config, p))
        )

    return _artifact_buffer(chunks(), 2097152)


def _expected_svg(config, p, quality):
    entries = []
    if quality:
        for policy in config["policies"]:
            for family in ("micro", "grouping_success.micro"):
                for name in (
                    "pair_precision",
                    "pair_recall",
                    "pair_f1",
                    "exact_partition",
                    "service_jaccard",
                ):
                    metric = next(
                        row
                        for row in p["policy_micro"]
                        if row["scope"] == "policy." + policy["name"] + "." + family
                        and row["name"] == name
                    )
                    rational = (
                        Fraction(metric["numerator"], metric["denominator"])
                        if metric["value"] is not None
                        else None
                    )
                    entries.append(
                        (
                            policy["name"] + " / " + family + " / " + name,
                            rational,
                            metric["value"]
                            if rational is not None
                            else metric["reason"],
                        )
                    )
        notes = [
            f"{r['policy']} / {r['target']}: target {r['threshold']}; measured {r['measured']}; {r['status']}; {r['reason']}"
            for r in p["targets"]
        ]
        title = "Grouping quality: operational and grouping-success populations"
    else:
        for status in p["statuses"]:
            count = sum(
                r["count"]
                for r in p["statuses"]
                if (r["scope"], r["policy"]) == (status["scope"], status["policy"])
            )
            entries.append(
                (
                    status["policy"]
                    + " / "
                    + status["scope"]
                    + " / "
                    + status["status"],
                    Fraction(status["count"], count),
                    str(status["count"]) + "/" + str(count),
                )
            )
        notes = [
            f"{r['policy']} / {r['scope']}: n={r['n']}, failed={r['failed']}, censored={r['censored']}, min={r['minimum_ns']}, median={r['median_ns']}, p95={r['p95_ns']}, max={r['maximum_ns']} ns"
            for r in p["latency"]
            if r["case_id"] is None
        ]
        title = "Status coverage and scoped controlled latency (not a live SLA)"
    height = 100 + len(entries) * 32 + len(notes) * 24

    def chunks():
        yield f'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="{height}" viewBox="0 0 1200 {height}" role="img">\n<title>{escape(title)}</title>\n<rect width="1200" height="{height}" fill="white"/>\n'
        yield f'<text x="20" y="26" font-family="sans-serif" font-size="16">{escape(title)}</text>\n<text x="20" y="48" font-family="sans-serif" font-size="11">raw SHA-256: {p["raw_sha256"]}</text>\n'
        for i, (text, value, display) in enumerate(entries):
            y = 80 + i * 32
            width = 0 if value is None else round(value * 200)
            if width < 0 or width > 200:
                raise ValueError("figure fraction outside unit interval")
            yield f'<text x="20" y="{y}" font-family="sans-serif" font-size="11">{escape(text)}</text><rect x="640" y="{y-12}" width="{width}" height="14" fill="#285b87"/><text x="850" y="{y}" font-family="sans-serif" font-size="11">{escape(display)}</text>\n'
        for i, note in enumerate(notes):
            yield f'<text x="20" y="{80+len(entries)*32+i*24}" font-family="sans-serif" font-size="10">{escape(note)}</text>\n'
        yield "</svg>\n"

    return _artifact_buffer(chunks(), 1048576)


def verify_artifact_bytes(files, expected_config_sha256, expected_raw_sha256):
    """Recompute all nine projections and the manifest before any publication."""
    if type(files) is not dict or set(files) != set(PACKAGE_FILES):
        raise ValueError("exact ten artifact paths required")
    total = 0
    for name in PACKAGE_FILES:
        if type(files[name]) is not bytes or len(files[name]) > PACKAGE_LIMITS[name]:
            raise ValueError(f"{name}: artifact byte/type limit exceeded")
        total += len(files[name])
        if total > 100663296:
            raise ValueError("package byte cap exceeded")
    for name, digest in [
        ("config.json", expected_config_sha256),
        ("raw_outcomes.json", expected_raw_sha256),
    ]:
        if hashlib.sha256(files[name]).hexdigest() != _sha256(digest):
            raise ValueError(f"{name}: independently supplied digest mismatch")
    decoded = {}
    for key, name in [
        ("config", "config.json"),
        ("runtime", "runtime_inputs.json"),
        ("truth", "truth.json"),
        ("raw", "raw_outcomes.json"),
    ]:
        decoded[key] = strict_loads(files[name])
        if canonical(decoded[key]) != files[name]:
            raise ValueError(f"{name}: input canonical bytes mismatch")
    processed = recompute(**decoded)
    expected_processed = _artifact_json(processed, 8388608)
    if files["processed_comparison.json"] != expected_processed:
        raise ValueError(
            "processed_comparison.json differs from independent raw reduction"
        )
    projections = {
        "processed_comparison.csv": _expected_csv(decoded["config"], processed),
        "figures/grouping_quality.svg": _expected_svg(
            decoded["config"], processed, True
        ),
        "figures/status_latency.svg": _expected_svg(
            decoded["config"], processed, False
        ),
        "report.md": _expected_report(decoded["config"], decoded["raw"], processed),
    }
    for name, expected in projections.items():
        if files[name] != expected:
            raise ValueError(f"{name} differs from independent raw-derived projection")
    counts = [
        sum(not run["plan"]["warmup"] for run in decoded["raw"]["runs"]),
        sum(run["plan"]["warmup"] for run in decoded["raw"]["runs"]),
    ]
    if counts != [320, 32]:
        raise ValueError(
            "manifest population differs from approved measured/warmup counts"
        )
    manifest = dict(
        schema_version=SCHEMA_VERSION,
        protocol_fingerprint=decoded["config"]["protocol_fingerprint"],
        source_revision=decoded["config"]["source_revision"],
        artifacts=[
            dict(
                path=name,
                bytes=len(files[name]),
                sha256=hashlib.sha256(files[name]).hexdigest(),
            )
            for name in PACKAGE_FILES[:-1]
        ],
        measured_runs=counts[0],
        warmup_runs=counts[1],
        complete=True,
        receipt="external, not embedded",
    )
    if files["manifest.json"] != _artifact_json(manifest, 1048576):
        raise ValueError(
            "manifest.json differs from exact nine-file identity/population projection"
        )
    return dict(
        verified=True,
        scope="package",
        artifacts=10,
        measured_runs=320,
        warmup_runs=32,
        manifest_sha256=hashlib.sha256(files["manifest.json"]).hexdigest(),
    )


def verify_package(
    root, expected_config_sha256, expected_raw_sha256, expected_manifest_sha256
):
    """Non-writing exact topology/file/permission verification of a frozen package."""
    root = Path(root)
    for directory in (root, root / "figures"):
        stat = directory.lstat()
        if (
            not directory.is_dir()
            or directory.is_symlink()
            or getattr(stat, "st_file_attributes", 0) & 0x400
        ):
            raise ValueError("package directory is not regular/non-reparse")
    if {p.name for p in root.iterdir()} != {
        p for p in PACKAGE_FILES if "/" not in p
    } | {"figures"}:
        raise ValueError("package root has missing/extra entries")
    if {p.name for p in (root / "figures").iterdir()} != {
        "grouping_quality.svg",
        "status_latency.svg",
    }:
        raise ValueError("figures have missing/extra entries")
    files = {}
    for name in PACKAGE_FILES:
        path = root / name
        stat = path.lstat()
        if (
            not path.is_file()
            or path.is_symlink()
            or getattr(stat, "st_file_attributes", 0) & 0x400
        ):
            raise ValueError(f"{name}: expected regular non-reparse file")
        readonly = (
            bool(stat.st_file_attributes & 1)
            if os.name == "nt"
            else not bool(stat.st_mode & 0o222)
        )
        if not readonly:
            raise ValueError(f"{name}: file is not read-only")
        files[name] = _read(path, PACKAGE_LIMITS[name])
    if hashlib.sha256(files["manifest.json"]).hexdigest() != _sha256(
        expected_manifest_sha256
    ):
        raise ValueError("manifest.json: independently supplied digest mismatch")
    return verify_artifact_bytes(files, expected_config_sha256, expected_raw_sha256)


def _read(path, limit):
    """Bounded binary read; this verifier never opens a file for writing."""
    with Path(path).open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("input exceeds approved byte cap")
    return data


def main(argv=None):
    """Authenticate four source digests and compare every processed field/byte."""
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "runtime", "truth", "raw"):
        parser.add_argument(f"--{name}")
        parser.add_argument(
            f"--{name}-sha256",
            help="independently supplied SHA-256 of exact input bytes",
        )
    parser.add_argument("--processed")
    parser.add_argument("--package", type=Path)
    parser.add_argument("--manifest-sha256")
    args = parser.parse_args(argv)
    if args.package is not None:
        if not all((args.config_sha256, args.raw_sha256, args.manifest_sha256)) or any(
            (
                args.config,
                args.runtime,
                args.truth,
                args.raw,
                args.processed,
                args.runtime_sha256,
                args.truth_sha256,
            )
        ):
            parser.error(
                "package mode requires only package and independently pinned config/raw/manifest digests"
            )
    elif (
        not all(
            getattr(args, key)
            for key in (
                "config",
                "runtime",
                "truth",
                "raw",
                "processed",
                "config_sha256",
                "runtime_sha256",
                "truth_sha256",
                "raw_sha256",
            )
        )
        or args.manifest_sha256 is not None
    ):
        parser.error(
            "raw-reduction mode requires four input files/digests and processed output"
        )
    try:
        if args.package is not None:
            result = verify_package(
                args.package, args.config_sha256, args.raw_sha256, args.manifest_sha256
            )
            print(canonical(result).decode(), end="")
            return 0
        sources = {}
        for name in ("config", "runtime", "truth", "raw"):
            data = _read(
                getattr(args, name), 67_108_864 if name == "raw" else 2_097_152
            )
            expected_digest = _sha256(getattr(args, f"{name}_sha256"))
            if hashlib.sha256(data).hexdigest() != expected_digest:
                raise ValueError(
                    f"{name}: independently supplied input digest mismatch"
                )
            value = strict_loads(data)
            if data != canonical(value):
                raise ValueError(f"{name}: input is not canonical JSON bytes")
            sources[name] = value
        expected = recompute(**sources)
        # Open processed output only after independent recomputation is complete.
        observed_bytes = _read(args.processed, 8_388_608)
        actual = strict_loads(observed_bytes)
        difference = _difference(expected, actual)
        if difference:
            print(
                canonical({"verified": False, "error": difference}).decode(),
                end="",
                file=sys.stderr,
            )
            return 1
        if observed_bytes != canonical(expected):
            print(
                canonical(
                    {"verified": False, "error": "processed canonical bytes mismatch"}
                ).decode(),
                end="",
                file=sys.stderr,
            )
            return 1
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        OverflowError,
        RecursionError,
    ) as error:
        print(
            canonical({"verified": False, "error": str(error)}).decode(),
            end="",
            file=sys.stderr,
        )
        return 2
    print(canonical({"verified": True}).decode(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
