"""Bounded evaluation artifacts and native Windows no-replace publication."""
# ruff: noqa: E721 -- strict builtin types are part of the evaluation boundary

from __future__ import annotations

import csv
from datetime import datetime, timezone
from fractions import Fraction
from html import escape
import io
import hashlib
import json
from itertools import chain
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable

from app.correlation import evaluation as e
from app.correlation import evaluation_metrics as metrics

ARTIFACTS = (
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
CSV_COLUMNS = (
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
CAPS = dict(
    zip(
        ARTIFACTS,
        (
            2 << 20,
            2 << 20,
            2 << 20,
            64 << 20,
            8 << 20,
            8 << 20,
            1 << 20,
            1 << 20,
            2 << 20,
            1 << 20,
        ),
    )
)
PACKAGE_CAP = 96 << 20


def _bounded(parts: Iterable[str | bytes], limit: int) -> bytes:
    output = bytearray()
    for part in parts:
        data = part.encode("utf-8") if type(part) is str else part
        if type(data) is not bytes or len(output) + len(data) > limit:
            raise e.EvaluationContractError("Artifact byte/type bound exceeded")
        output.extend(data)
    return bytes(output)


def _json(value: Any, limit: int) -> bytes:
    e._plain(value)  # reject cycles/nonfinite/unsupported objects before encoding
    encoder = json.JSONEncoder(
        sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    return _bounded(chain(encoder.iterencode(value), ("\n",)), limit)


def _verifier() -> dict[str, Any]:
    return runpy.run_path(
        str(
            Path(__file__).resolve().parents[3]
            / "scripts/verify_phase4_correlation_evaluation.py"
        )
    )


def _row(p: dict, value: dict, case: str, policy: str) -> list[str]:
    vals = [
        value["scope"],
        case,
        policy,
        value["name"],
        *(
            value[k]
            for k in (
                "numerator",
                "denominator",
                "value",
                "reason",
                "planned",
                "eligible",
                "observed",
                "failed",
            )
        ),
        *(
            p[k]
            for k in ("raw_sha256", "truth_sha256", "runtime_sha256", "metric_version")
        ),
    ]
    return ["" if v is None else str(v) for v in vals]


def _scalar(
    name: str,
    scope: str,
    number: Fraction | None,
    planned: int,
    observed: int,
    failed: int,
    *,
    reason: str | None = None,
    eligible: int | None = None,
) -> dict:
    return dict(
        name=name,
        scope=scope,
        numerator=None if number is None else number.numerator,
        denominator=None if number is None else number.denominator,
        value=None if number is None else metrics.decimal12(number),
        reason=reason,
        planned=planned,
        eligible=planned if eligible is None else eligible,
        observed=observed,
        failed=failed,
    )


def _csv_rows(config: dict, p: dict) -> Iterable[list[str]]:
    for case in p["cases"]:
        for value in case["metrics"]:
            yield _row(p, value, case["case_id"], case["policy"])
    for field in ("policy_micro", "policy_macro", "coverage"):
        for value in p[field]:
            policy = next(
                v["name"]
                for v in config["policies"]
                if value["scope"].startswith(f"policy.{v['name']}.")
            )
            yield _row(p, value, "", policy)
    for status in p["statuses"]:
        total = sum(
            r["count"]
            for r in p["statuses"]
            if r["scope"] == status["scope"] and r["policy"] == status["policy"]
        )
        value = _scalar(
            status["status"],
            "status." + status["scope"],
            Fraction(status["count"], total),
            total,
            total,
            0,
        )
        # Preserve the original integer frequency components, not a reduced fraction.
        value.update(numerator=status["count"], denominator=total)
        yield _row(p, value, "", status["policy"])
    for summary in p["latency"]:
        samples = sorted(summary["samples_ns"])
        n = summary["n"]
        middle = Fraction(samples[(n - 1) // 2] + samples[n // 2], 2) if n else None
        numbers = [
            (key, None if summary[key] is None else Fraction(summary[key]))
            for key in ("n", "failed", "censored", "minimum_ns")
        ]
        numbers += [
            ("median_ns", middle),
            *(
                (key, None if summary[key] is None else Fraction(summary[key]))
                for key in ("p95_ns", "maximum_ns")
            ),
        ]
        numbers += [
            (f"sample.{i:03}", Fraction(number))
            for i, number in enumerate(summary["samples_ns"])
        ]
        for name, number in numbers:
            value = _scalar(
                name,
                "latency." + summary["scope"],
                number,
                n + summary["failed"] + summary["censored"],
                n,
                summary["failed"],
                reason="no_successful_samples" if number is None else None,
            )
            yield _row(p, value, summary["case_id"] or "", summary["policy"])
    for target in p["targets"]:
        number = None
        planned = eligible = observed = failed = 0
        if target["target"] == "known_pattern_quality_proxy":
            cases = [
                c
                for c in p["cases"]
                if c["policy"] == target["policy"]
                and c["case_id"] in config["cases"][:8]
            ]
            planned = 8
            eligible = observed = sum(c["truth_status"] == "complete" for c in cases)
            failed = sum(
                next(
                    v["failed"] for v in c["metrics"] if v["name"] == "exact_partition"
                )
                for c in cases
            )
            if target["measured"] is not None:
                number = Fraction(
                    sum(
                        next(
                            v["numerator"]
                            for v in c["metrics"]
                            if v["name"] == "exact_partition"
                        )
                        for c in cases
                    ),
                    8,
                )
        elif target["target"] == "controlled_budget":
            latency = next(
                v
                for v in p["latency"]
                if v["case_id"] is None
                and v["policy"] == target["policy"]
                and v["scope"] == "controlled_compute"
            )
            planned = latency["n"] + latency["failed"] + latency["censored"]
            eligible = observed = latency["n"]
            failed = latency["failed"]
            if latency["p95_ns"] is not None:
                number = Fraction(latency["p95_ns"])
        value = _scalar(
            target["target"] + "." + target["status"],
            "target." + target["scope"],
            number,
            planned,
            observed,
            failed,
            reason=target["reason"],
            eligible=eligible,
        )
        if (
            target["target"] == "known_pattern_quality_proxy"
            and target["measured"] is not None
        ):
            value.update(
                numerator=sum(
                    next(
                        v["numerator"]
                        for v in c["metrics"]
                        if v["name"] == "exact_partition"
                    )
                    for c in cases
                ),
                denominator=8,
            )
        yield _row(p, value, "", target["policy"])


def _csv(config: dict, p: dict) -> bytes:
    def lines():
        for row in chain((CSV_COLUMNS,), _csv_rows(config, p)):
            buffer = io.StringIO(newline="")
            csv.writer(buffer, lineterminator="\n").writerow(row)
            yield buffer.getvalue()

    return _bounded(lines(), CAPS["processed_comparison.csv"])


def _md(value: Any) -> str:
    return (
        escape("null" if value is None else str(value))
        .replace("|", "&#124;")
        .replace("`", "&#96;")
        .replace("\r", "")
        .replace("\n", "<br>")
    )


def _table(headers: Iterable[str], rows: Iterable[Iterable[Any]]) -> Iterable[str]:
    header = tuple(headers)
    yield "| " + " | ".join(header) + " |\n"
    yield "| " + " | ".join("---" for _ in header) + " |\n"
    for row in rows:
        yield "| " + " | ".join(map(_md, row)) + " |\n"
    yield "\n"


def _report(config: dict, raw: dict, p: dict) -> bytes:
    def lines():
        yield "# Correlation evaluation report\n\n"
        yield "Declared synthetic anomaly observations; supplied signals condition these grouping results. Collection completeness does not certify independent executions, production accuracy, causality or a live SLA. Failures and unavailable values remain in the population. No evaluation-partition tuning is permitted.\n\n"
        yield "## Provenance and pinned inputs\n\n"
        yield from _table(
            ("Field", "Value"),
            [
                (key, config[key])
                for key in (
                    "protocol_fingerprint",
                    "source_revision",
                    "seed",
                    "metric_version",
                )
            ]
            + [
                (key, p[key])
                for key in (
                    "raw_sha256",
                    "truth_sha256",
                    "runtime_sha256",
                    "config_sha256",
                )
            ],
        )
        yield from _table(
            ("Policy", "Fingerprint"),
            ((v["name"], v["configuration_fingerprint"]) for v in config["policies"]),
        )
        yield from _table(
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
        yield from _table(
            ("Policy", "Target", "Threshold", "Scope", "Measured", "Status", "Reason"),
            (
                (
                    v[k]
                    for k in (
                        "policy",
                        "target",
                        "threshold",
                        "scope",
                        "measured",
                        "status",
                        "reason",
                    )
                )
                for v in p["targets"]
            ),
        )
        yield "## Case outcomes and grouping errors\n\n"
        rows = []
        for c in p["cases"]:
            metrics_by_name = {v["name"]: v for v in c["metrics"]}
            f1 = metrics_by_name["pair_f1"]
            rows.append(
                [
                    c["case_id"],
                    c["policy"],
                    c["status"],
                    c["truth_status"],
                    c["TP"],
                    c["FP"],
                    c["FN"],
                    f1["value"] if f1["value"] is not None else f1["reason"],
                    metrics_by_name["exact_partition"]["value"],
                    metrics_by_name["over_groups"]["numerator"],
                    metrics_by_name["under_groups"]["numerator"],
                    json.dumps(c["unassigned_units"], separators=(",", ":")),
                    json.dumps(c["missed_truth_groups"], separators=(",", ":")),
                    json.dumps(
                        c["background_prediction_groups"], separators=(",", ":")
                    ),
                ]
            )
        yield from _table(
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
            rows,
        )
        yield "## Membership matching ledger\n\n"
        yield from _table(
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
            (
                (
                    c["case_id"],
                    c["policy"],
                    json.dumps(r["truth_members"], separators=(",", ":")),
                    json.dumps(r["prediction_members"], separators=(",", ":")),
                    r["intersection"],
                    r["union"],
                    r["service_intersection"],
                    r["service_union"],
                )
                for c in p["cases"]
                for r in (c["matching"] or [])
            ),
        )
        yield "## Raw run census (warmups retained separately)\n\n"

        def runs():
            for r in raw["runs"]:
                errors = [
                    *r["errors"],
                    *(error for d in r["decisions"] for error in d["errors"]),
                    *(error for v in r["enrichments"] for error in v["errors"]),
                ]
                yield (
                    r["plan"]["run"],
                    r["plan"]["warmup"],
                    r["terminal_status"],
                    r["final_grouping_available"],
                    r["utc_start"],
                    r["utc_end"],
                    r["elapsed_ns"],
                    json.dumps(
                        errors,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )

        yield from _table(
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
            runs(),
        )
        yield "## Complete scalar projection\n\nRows and first twelve columns match processed_comparison.csv. The four repeated provenance constants are listed above. Latency statistics/samples use nanoseconds; failed/censored counts remain explicit. Operational and grouping-success scopes retain distinct denominators. Full native payloads and rational-component ledgers remain in raw_outcomes.json and processed_comparison.json.\n\n"
        yield from _table(CSV_COLUMNS[:12], (r[:12] for r in _csv_rows(config, p)))

    return _bounded(lines(), CAPS["report.md"])


def _svg(config: dict, p: dict, quality: bool) -> bytes:
    rows: list[tuple[str, Fraction | None, str]] = []
    if quality:
        for policy in config["policies"]:
            for suffix in ("micro", "grouping_success.micro"):
                for name in (
                    "pair_precision",
                    "pair_recall",
                    "pair_f1",
                    "exact_partition",
                    "service_jaccard",
                ):
                    r = next(
                        v
                        for v in p["policy_micro"]
                        if v["scope"] == f"policy.{policy['name']}.{suffix}"
                        and v["name"] == name
                    )
                    rows.append(
                        (
                            f"{policy['name']} / {suffix} / {name}",
                            None
                            if r["value"] is None
                            else Fraction(r["numerator"], r["denominator"]),
                            r["reason"] if r["value"] is None else r["value"],
                        )
                    )
        notes = [
            f"{r['policy']} / {r['target']}: target {r['threshold']}; measured {r['measured']}; {r['status']}; {r['reason']}"
            for r in p["targets"]
        ]
        title = "Grouping quality: operational and grouping-success populations"
    else:
        for r in p["statuses"]:
            total = sum(
                v["count"]
                for v in p["statuses"]
                if v["policy"] == r["policy"] and v["scope"] == r["scope"]
            )
            rows.append(
                (
                    f"{r['policy']} / {r['scope']} / {r['status']}",
                    Fraction(r["count"], total),
                    f"{r['count']}/{total}",
                )
            )
        notes = [
            f"{r['policy']} / {r['scope']}: n={r['n']}, failed={r['failed']}, censored={r['censored']}, min={r['minimum_ns']}, median={r['median_ns']}, p95={r['p95_ns']}, max={r['maximum_ns']} ns"
            for r in p["latency"]
            if r["case_id"] is None
        ]
        title = "Status coverage and scoped controlled latency (not a live SLA)"
    height = 100 + 32 * len(rows) + 24 * len(notes)

    def parts():
        yield f'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="{height}" viewBox="0 0 1200 {height}" role="img">\n<title>{escape(title)}</title>\n<rect width="1200" height="{height}" fill="white"/>\n'
        yield f'<text x="20" y="26" font-family="sans-serif" font-size="16">{escape(title)}</text>\n<text x="20" y="48" font-family="sans-serif" font-size="11">raw SHA-256: {p["raw_sha256"]}</text>\n'
        for i, (label, value, display) in enumerate(rows):
            y = 80 + 32 * i
            width = 0 if value is None else round(value * 200)
            if not 0 <= width <= 200:
                raise e.EvaluationContractError("Figure fraction outside unit interval")
            yield f'<text x="20" y="{y}" font-family="sans-serif" font-size="11">{escape(label)}</text><rect x="640" y="{y-12}" width="{width}" height="14" fill="#285b87"/><text x="850" y="{y}" font-family="sans-serif" font-size="11">{escape(display)}</text>\n'
        for i, note in enumerate(notes):
            yield f'<text x="20" y="{80+32*len(rows)+24*i}" font-family="sans-serif" font-size="10">{escape(note)}</text>\n'
        yield "</svg>\n"

    return _bounded(parts(), 1 << 20)


def render_artifacts(
    config_raw: bytes,
    runtime_raw: bytes,
    truth_raw: bytes,
    topology_raw: bytes,
    raw_outcomes: bytes,
    *,
    expected_config_sha256: str,
    expected_raw_sha256: str,
) -> dict[str, bytes]:
    comparison = metrics.calculate_comparison(
        config_raw,
        runtime_raw,
        truth_raw,
        topology_raw,
        raw_outcomes,
        expected_config_sha256=expected_config_sha256,
        expected_raw_sha256=expected_raw_sha256,
    )
    processed = e.serialize_evaluation(comparison, limit=8 << 20)
    config = json.loads(config_raw)
    raw = json.loads(raw_outcomes)
    p = json.loads(processed)
    files = dict(
        zip(
            ARTIFACTS[:5], (config_raw, runtime_raw, truth_raw, raw_outcomes, processed)
        )
    )
    files["processed_comparison.csv"] = _csv(config, p)
    files["figures/grouping_quality.svg"] = _svg(config, p, True)
    files["figures/status_latency.svg"] = _svg(config, p, False)
    files["report.md"] = _report(config, raw, p)
    manifest = dict(
        schema_version=e.SCHEMA_VERSION,
        protocol_fingerprint=config["protocol_fingerprint"],
        source_revision=config["source_revision"],
        artifacts=[
            dict(
                path=name,
                bytes=len(files[name]),
                sha256=hashlib.sha256(files[name]).hexdigest(),
            )
            for name in ARTIFACTS[:-1]
        ],
        measured_runs=sum(not r["plan"]["warmup"] for r in raw["runs"]),
        warmup_runs=sum(r["plan"]["warmup"] for r in raw["runs"]),
        complete=True,
        receipt="external, not embedded",
    )
    files["manifest.json"] = _json(manifest, 1 << 20)
    _verifier()["verify_artifact_bytes"](
        files, expected_config_sha256, expected_raw_sha256
    )
    return files


def read_bounded(path: Path, limit: int) -> bytes:
    with Path(path).open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise e.EvaluationContractError("Input exceeds approved byte cap")
    return raw


def native_identity(path: Path) -> dict:
    stat = path.lstat()
    if (
        not path.is_file()
        or getattr(stat, "st_file_attributes", 0) & 0x400
        or path.is_symlink()
    ):
        raise e.EvaluationContractError("Expected regular non-reparse file")
    raw = path.read_bytes()
    seconds, ns = divmod(stat.st_mtime_ns, 1_000_000_000)
    return dict(
        bytes=len(raw),
        lines=len(raw.splitlines()),
        sha256=hashlib.sha256(raw).hexdigest().upper(),
        timestamp_utc=datetime.fromtimestamp(seconds, timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        + f".{ns//100:07d}Z",
        read_only=bool(stat.st_file_attributes & 1)
        if os.name == "nt"
        else not bool(stat.st_mode & 0o222),
    )


def _fresh_output(path: Path) -> None:
    if (
        not path.name
        or ".." in path.parts
        or not path.parent.is_dir()
        or os.path.lexists(path)
    ):
        raise FileExistsError(f"Fresh output and existing parent required: {path}")
    for parent in (path.parent, *path.parent.parents):
        if (
            parent.is_symlink()
            or getattr(parent.lstat(), "st_file_attributes", 0) & 0x400
        ):
            raise e.EvaluationContractError("Output ancestor is a reparse point")


def _write(path: Path, raw: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(raw)


def publish_artifacts(
    files: dict[str, bytes],
    destination: Path,
    receipt_path: Path,
    *,
    expected_config_sha256: str,
    expected_raw_sha256: str,
) -> dict:
    if os.name != "nt":
        raise e.EvaluationContractError(
            "Publication requires native Windows no-replace directory rename"
        )
    if type(files) is not dict:
        raise e.EvaluationContractError("Expected exact artifact dictionary")
    files = (
        files.copy()
    )  # bytes are immutable; do not publish a caller's changing mapping
    verification = _verifier()
    summary = verification["verify_artifact_bytes"](
        files, expected_config_sha256, expected_raw_sha256
    )
    # A direct caller must pass the approved native schemas too, even if it
    # assembled a mutually consistent set of raw-derived projections itself.
    config = e.deserialize_evaluation(e.EvaluationConfig, files["config.json"])
    topology_raw = e.serialize_evaluation(
        e.TopologyInputs(
            schema_version="correlation-evaluation/1.0", topologies=config.topologies
        )
    )
    _, runtime, _ = e.load_evaluation_inputs(
        files["config.json"],
        files["runtime_inputs.json"],
        files["truth.json"],
        topology_raw,
        expected_config_sha256=expected_config_sha256,
    )
    e.bind_raw_outcomes(
        e.deserialize_evaluation(
            e.RawOutcomes, files["raw_outcomes.json"], limit=e.MAX_RAW_BYTES
        ),
        files["config.json"],
        runtime,
    )
    destination = Path(destination).absolute()
    receipt_path = Path(receipt_path).absolute()
    streams_paths = {
        name: receipt_path.with_name(receipt_path.stem + "_" + name + ".bin")
        for name in ("stdout", "stderr")
    }
    for path in (destination, receipt_path, *streams_paths.values()):
        _fresh_output(path)
    if any(
        path.is_relative_to(destination)
        for path in (receipt_path, *streams_paths.values())
    ):
        raise e.EvaluationContractError(
            "Receipt and streams must be outside the package"
        )
    stage = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.attempt-", dir=destination.parent)
    )
    (stage / "figures").mkdir()
    # Manifest is last. On any failure, preserve this private attempt for inspection.
    for name in ARTIFACTS:
        _write(stage / name, files[name])
    for name in ARTIFACTS:
        os.chmod(stage / name, 0o444)
    verification["verify_package"](
        stage, expected_config_sha256, expected_raw_sha256, summary["manifest_sha256"]
    )
    intended = {name: native_identity(stage / name) for name in ARTIFACTS}
    os.rename(
        stage, destination
    )  # Windows refuses even a late empty-directory collision.
    if intended != {name: native_identity(destination / name) for name in ARTIFACTS}:
        raise e.EvaluationContractError("Publication changed an intended file identity")
    script = (
        Path(__file__).resolve().parents[3]
        / "scripts/verify_phase4_correlation_evaluation.py"
    )
    argv = [
        sys.executable,
        "-B",
        str(script),
        "--package",
        str(destination),
        "--config-sha256",
        expected_config_sha256,
        "--raw-sha256",
        expected_raw_sha256,
        "--manifest-sha256",
        summary["manifest_sha256"],
    ]
    start = datetime.now(timezone.utc).isoformat()
    tick = time.perf_counter_ns()
    child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    timed_out = False
    try:
        out, err = child.communicate(timeout=180)
    except subprocess.TimeoutExpired:
        timed_out = True
        child.kill()
        out, err = child.communicate()
    end = datetime.now(timezone.utc).isoformat()
    elapsed = time.perf_counter_ns() - tick
    streams = {}
    for name, data in (("stdout", out), ("stderr", err)):
        _write(streams_paths[name], data)
        os.chmod(streams_paths[name], 0o444)
        streams[name] = dict(
            path=str(streams_paths[name]), identity=native_identity(streams_paths[name])
        )
    unchanged = intended == {
        name: native_identity(destination / name) for name in ARTIFACTS
    }
    try:
        observed = json.loads(out)
    except (ValueError, UnicodeDecodeError):
        observed = None
    passed = (
        child.returncode == 0
        and not timed_out
        and not err
        and observed == summary
        and unchanged
    )
    receipt = dict(
        schema_version=e.SCHEMA_VERSION,
        kind="correlation-evaluation-publication-receipt/1.0",
        root=str(destination),
        config_sha256=expected_config_sha256,
        raw_sha256=expected_raw_sha256,
        manifest_sha256=summary["manifest_sha256"],
        files=intended,
        non_writing=unchanged,
        verified=passed,
        verifier=dict(
            argv=argv,
            pid=child.pid,
            utc_start=start,
            utc_end=end,
            elapsed_ns=elapsed,
            exit_code=child.returncode,
            timed_out=timed_out,
            streams=streams,
        ),
    )
    _write(receipt_path, _json(receipt, 1 << 20))
    os.chmod(receipt_path, 0o444)
    if not passed:
        raise e.EvaluationContractError(
            "Published package failed post validation; package and failure receipt preserved"
        )
    return receipt
