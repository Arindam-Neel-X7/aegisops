"""P1 portable projections, fail-before-write and native Windows publication."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest

from app.correlation import evaluation as e
from app.correlation import evaluation_artifacts as a

REPO = Path(__file__).resolve().parents[2]
WINDOWS = pytest.mark.skipif(
    os.name != "nt", reason="A1 requires actual Windows no-replace directory semantics"
)


@pytest.fixture(scope="module")
def inputs():
    helpers = runpy.run_path(
        str(REPO / "backend/tests/test_correlation_evaluation_metrics.py")
    )
    return helpers["development_raw"].__wrapped__()


@pytest.fixture(scope="module")
def package(inputs):
    config, runtime, truth, topology, raw = inputs
    return a.render_artifacts(
        config,
        runtime,
        truth,
        topology,
        raw,
        expected_config_sha256=hashlib.sha256(config).hexdigest(),
        expected_raw_sha256=hashlib.sha256(raw).hexdigest(),
    )


@pytest.fixture(scope="module")
def independent():
    return runpy.run_path(str(REPO / "scripts/verify_phase4_correlation_evaluation.py"))


def digests(files):
    return dict(
        expected_config_sha256=hashlib.sha256(files["config.json"]).hexdigest(),
        expected_raw_sha256=hashlib.sha256(files["raw_outcomes.json"]).hexdigest(),
    )


def verify(independent, files):
    return independent["verify_artifact_bytes"](files, **digests(files))


def test_exact_artifacts_manifest_and_negative_census(package, independent):
    assert tuple(package) == a.ARTIFACTS and len(package) == 10
    manifest = json.loads(package["manifest.json"])
    assert set(manifest) == {
        "schema_version",
        "protocol_fingerprint",
        "source_revision",
        "artifacts",
        "measured_runs",
        "warmup_runs",
        "complete",
        "receipt",
    }
    assert (
        manifest["complete"] is True
        and manifest["measured_runs"] == 320
        and manifest["warmup_runs"] == 32
    )
    assert manifest["receipt"] == "external, not embedded"
    assert manifest["artifacts"] == [
        dict(
            path=p, bytes=len(package[p]), sha256=hashlib.sha256(package[p]).hexdigest()
        )
        for p in a.ARTIFACTS[:-1]
    ]
    result = verify(independent, package)
    assert result["verified"] is True and result["artifacts"] == 10
    assert (
        all(len(data) <= a.CAPS[path] for path, data in package.items())
        and sum(map(len, package.values())) <= a.PACKAGE_CAP
    )
    processed = json.loads(package["processed_comparison.json"])
    assert len(processed["cases"]) == 32 and any(
        c["status"] == "RUNTIME_ERROR" for c in processed["cases"]
    )
    assert all(
        t["status"] == "NOT_MEASURED"
        for t in processed["targets"]
        if t["target"] == "live_grouping_latency"
    )


def test_generation_is_deterministic_and_inputs_unchanged(inputs, package):
    before = [hashlib.sha256(data).hexdigest() for data in inputs]
    config, runtime, truth, topology, raw = inputs
    again = a.render_artifacts(
        config, runtime, truth, topology, raw, **digests(package)
    )
    assert again == package and before == [
        hashlib.sha256(data).hexdigest() for data in inputs
    ]


def test_csv_headers_all_rows_and_reasons(package):
    rows = list(
        csv.DictReader(
            io.StringIO(package["processed_comparison.csv"].decode(), newline="")
        )
    )
    assert tuple(rows[0]) == (
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
    assert all(row["value"] or row["reason"] for row in rows)
    assert len(
        {(r["scope"], r["case_id"], r["policy"], r["metric"]) for r in rows}
    ) == len(rows)
    assert (
        sum(int(r["numerator"]) for r in rows if r["scope"] == "status.measured") == 320
    )
    assert sum(int(r["numerator"]) for r in rows if r["scope"] == "status.warmup") == 32
    assert any("grouping_success" in r["scope"] for r in rows)
    rawhash = hashlib.sha256(package["raw_outcomes.json"]).hexdigest()
    assert all(r["raw_sha256"] == rawhash for r in rows)


def test_report_census_and_figures_are_actual_bounded_xml(package):
    report = package["report.md"].decode()
    raw = json.loads(package["raw_outcomes.json"])
    assert all(r["plan"]["run"] in report for r in raw["runs"])
    assert (
        "RUNTIME_ERROR" in report
        and "empty_denominator" in report
        and "NOT_MEASURED" in report
    )
    assert (
        "independent executions" in report
        and "No evaluation-partition tuning" in report
    )
    for name in ["figures/grouping_quality.svg", "figures/status_latency.svg"]:
        svg = ET.fromstring(package[name])
        assert svg.tag == "{http://www.w3.org/2000/svg}svg"
        assert not any(
            node.tag.endswith("script") or "href" in node.attrib or "src" in node.attrib
            for node in svg.iter()
        )
        assert (
            int(svg.attrib["height"]) > 0
            and b"NOT_MEASURED" in package["figures/grouping_quality.svg"]
        )


@pytest.mark.parametrize("text", ["<script>x</script>", "a|b`c\nd", "<>&\"'"])
def test_markdown_text_is_escaped(text):
    value = a._md(text)
    assert (
        "<script>" not in value
        and "|" not in value
        and "`" not in value
        and "\n" not in value
    )


def mutant(package, name):
    files = package.copy()
    if name == "processed":
        value = json.loads(files["processed_comparison.json"])
        value["cases"][0]["TP"] += 1
        files["processed_comparison.json"] = (
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        return files, "processed_comparison.json"
    path = {
        "csv_cell": "processed_comparison.csv",
        "csv_header": "processed_comparison.csv",
        "quality_text": "figures/grouping_quality.svg",
        "status_coordinate": "figures/status_latency.svg",
        "report_cell": "report.md",
        "report_header": "report.md",
        "manifest_hash": "manifest.json",
        "manifest_bytes": "manifest.json",
        "manifest_bool": "manifest.json",
    }[name]
    if name.startswith("manifest"):
        value = json.loads(files[path])
        if name == "manifest_hash":
            value["artifacts"][0]["sha256"] = "0" * 64
        if name == "manifest_bytes":
            value["artifacts"][0]["bytes"] += 1
        if name == "manifest_bool":
            value["measured_runs"] = True
        files[path] = (
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    else:
        old, new = {
            "csv_cell": (b"case.operational", b"case.forged"),
            "csv_header": (b"scope,case_id", b"wrong,case_id"),
            "quality_text": (b"Grouping quality:", b"Invented quality:"),
            "status_coordinate": (b'x="640"', b'x="641"'),
            "report_cell": (b"RUNTIME_ERROR", b"SUCCESS"),
            "report_header": (b"## Case outcomes", b"## Forged outcomes"),
        }[name]
        assert old in files[path]
        files[path] = files[path].replace(old, new, 1)
        # Rebase the manifest identity: a hash comparison alone cannot pass this test.
        manifest = json.loads(files["manifest.json"])
        row = next(r for r in manifest["artifacts"] if r["path"] == path)
        row.update(
            bytes=len(files[path]), sha256=hashlib.sha256(files[path]).hexdigest()
        )
        files["manifest.json"] = (
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    assert files != package
    return files, path


@pytest.mark.parametrize(
    "name",
    [
        "processed",
        "csv_cell",
        "csv_header",
        "quality_text",
        "status_coordinate",
        "report_cell",
        "report_header",
        "manifest_hash",
        "manifest_bytes",
        "manifest_bool",
    ],
)
def test_independent_whole_projection_rejects_coordinated_mutations(
    package, independent, name
):
    files, path = mutant(package, name)
    with pytest.raises(ValueError, match=path.replace(".", "\\.")):
        verify(independent, files)


@WINDOWS
@pytest.mark.parametrize(
    "name", ["processed", "csv_cell", "quality_text", "report_cell", "manifest_hash"]
)
def test_invalid_artifacts_fail_before_staging(package, tmp_path, name):
    files, _ = mutant(package, name)
    destination = tmp_path / "official"
    with pytest.raises(ValueError):
        a.publish_artifacts(
            files, destination, tmp_path / "receipt.json", **digests(files)
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "mode", ["duplicate_key", "nonfinite", "deep", "cycle", "bool_bytes"]
)
def test_recursive_and_typed_guards(independent, mode):
    if mode == "duplicate_key":
        with pytest.raises(ValueError, match="duplicate"):
            independent["strict_loads"](b'{"x":1,"x":2}')
    elif mode == "nonfinite":
        with pytest.raises(ValueError, match="nonfinite"):
            independent["strict_loads"](b'{"x":NaN}')
    elif mode == "deep":
        with pytest.raises(ValueError, match="depth"):
            independent["strict_loads"](b"[" * 34 + b"0" + b"]" * 34)
    elif mode == "cycle":
        value = []
        value.append(value)
        with pytest.raises(ValueError, match="cyclic"):
            independent["canonical"](value)
    else:
        with pytest.raises(ValueError, match="integer"):
            independent["_source_identities"](
                [dict(path="source.py", bytes=True, sha256="0" * 64)], "sources"
            )


def test_bounded_encoder_stops_before_consuming_later_chunks():
    def chunks():
        yield "too long"
        pytest.fail("encoder consumed data after its bound failed")

    with pytest.raises(e.EvaluationContractError, match="bound"):
        a._bounded(chunks(), 2)


@WINDOWS
@pytest.mark.parametrize("field,value", [("processor", True), ("timer", "fake-clock")])
def test_consistent_projection_cannot_bypass_native_raw_schema(
    package, independent, tmp_path, field, value
):
    files = package.copy()
    raw = json.loads(files["raw_outcomes.json"])
    raw["runs"][1]["environment"][field] = value
    files["raw_outcomes.json"] = independent["canonical"](raw)
    config = json.loads(files["config.json"])
    processed = independent["recompute"](
        config,
        json.loads(files["runtime_inputs.json"]),
        json.loads(files["truth.json"]),
        raw,
    )
    files["processed_comparison.json"] = independent["canonical"](processed)
    files["processed_comparison.csv"] = independent["_expected_csv"](config, processed)
    files["figures/grouping_quality.svg"] = independent["_expected_svg"](
        config, processed, True
    )
    files["figures/status_latency.svg"] = independent["_expected_svg"](
        config, processed, False
    )
    files["report.md"] = independent["_expected_report"](config, raw, processed)
    manifest = json.loads(files["manifest.json"])
    manifest["artifacts"] = [
        dict(
            path=name,
            bytes=len(files[name]),
            sha256=hashlib.sha256(files[name]).hexdigest(),
        )
        for name in a.ARTIFACTS[:-1]
    ]
    files["manifest.json"] = independent["canonical"](manifest)
    with pytest.raises(e.EvaluationContractError):
        a.publish_artifacts(
            files, tmp_path / "official", tmp_path / "receipt.json", **digests(files)
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("change", ["missing", "extra", "wrong_type", "oversized"])
def test_package_topology_type_and_caps_before_recompute(package, independent, change):
    files = package.copy()
    if change == "missing":
        files.pop("report.md")
    if change == "extra":
        files["../escape"] = b""
    if change == "wrong_type":
        files["report.md"] = bytearray(files["report.md"])
    if change == "oversized":
        files["report.md"] = b"x" * ((2 << 20) + 1)
    with pytest.raises(ValueError):
        verify(independent, files)


@pytest.fixture(scope="module")
def published(package, tmp_path_factory):
    if os.name != "nt":
        pytest.skip("native Windows publication")
    root = tmp_path_factory.mktemp("p1_native")
    destination = root / "published"
    receipt_path = root / "receipt.json"
    receipt = a.publish_artifacts(
        package, destination, receipt_path, **digests(package)
    )
    return destination, receipt_path, receipt


@WINDOWS
def test_native_publication_receipt_and_non_writing_post(
    package, published, independent
):
    destination, receipt_path, receipt = published
    assert receipt["verified"] is True and receipt["non_writing"] is True
    assert (
        receipt["verifier"]["exit_code"] == 0
        and receipt["verifier"]["pid"] > 0
        and not receipt["verifier"]["timed_out"]
    )
    assert set(receipt["files"]) == set(a.ARTIFACTS)
    assert all(
        a.native_identity(destination / name) == meta
        for name, meta in receipt["files"].items()
    )
    assert (
        all(meta["read_only"] for meta in receipt["files"].values())
        and a.native_identity(receipt_path)["read_only"]
    )
    assert not Path(receipt["verifier"]["streams"]["stderr"]["path"]).read_bytes()
    before = {name: a.native_identity(destination / name) for name in a.ARTIFACTS}
    independent["verify_package"](
        destination,
        **digests(package),
        expected_manifest_sha256=receipt["manifest_sha256"],
    )
    assert before == {
        name: a.native_identity(destination / name) for name in a.ARTIFACTS
    }
    assert not any(
        p.name.startswith(".published.attempt-") for p in destination.parent.iterdir()
    )


@WINDOWS
def test_existing_collision_does_not_replace_or_stage(package, tmp_path):
    destination = tmp_path / "official"
    destination.mkdir()
    (destination / "keep").write_bytes(b"approved")
    with pytest.raises(FileExistsError):
        a.publish_artifacts(
            package, destination, tmp_path / "receipt.json", **digests(package)
        )
    assert (destination / "keep").read_bytes() == b"approved"
    assert {p.name for p in tmp_path.iterdir()} == {"official"}


@WINDOWS
def test_late_native_collision_refuses_even_empty_directory(
    package, tmp_path, monkeypatch
):
    native = os.rename
    destination = tmp_path / "official"

    def collide(source, target):
        Path(target).mkdir()
        native(source, target)

    monkeypatch.setattr(a.os, "rename", collide)
    with pytest.raises(OSError):
        a.publish_artifacts(
            package, destination, tmp_path / "receipt.json", **digests(package)
        )
    assert destination.is_dir() and list(destination.iterdir()) == []
    attempts = list(tmp_path.glob(".official.attempt-*"))
    assert len(attempts) == 1
    assert (attempts[0] / "manifest.json").read_bytes() == package["manifest.json"]


@WINDOWS
def test_stop_before_rename_preserves_frozen_attempt(package, tmp_path, monkeypatch):
    def stop(source, target):
        raise RuntimeError("controlled stop before rename")

    monkeypatch.setattr(a.os, "rename", stop)
    with pytest.raises(RuntimeError, match="controlled stop"):
        a.publish_artifacts(
            package,
            tmp_path / "official",
            tmp_path / "receipt.json",
            **digests(package),
        )
    assert (
        not (tmp_path / "official").exists()
        and not (tmp_path / "receipt.json").exists()
    )
    attempts = list(tmp_path.glob(".official.attempt-*"))
    assert len(attempts) == 1
    assert all(
        a.native_identity(attempts[0] / name)["read_only"] for name in a.ARTIFACTS
    )


@WINDOWS
def test_staging_io_failure_keeps_official_root_absent(package, tmp_path, monkeypatch):
    native = a._write

    def fail(path, data):
        if path.name == "truth.json":
            raise OSError("controlled staging IO failure")
        native(path, data)

    monkeypatch.setattr(a, "_write", fail)
    with pytest.raises(OSError, match="controlled staging"):
        a.publish_artifacts(
            package,
            tmp_path / "official",
            tmp_path / "receipt.json",
            **digests(package),
        )
    assert not (tmp_path / "official").exists()
    attempts = list(tmp_path.glob(".official.attempt-*"))
    assert len(attempts) == 1
    assert not (attempts[0] / "manifest.json").exists()


@WINDOWS
def test_manifest_is_written_last(package, tmp_path, monkeypatch):
    native = a._write
    names = []

    def observed(path, data):
        if ".attempt-" in str(path):
            names.append(
                path.relative_to(
                    path.parents[1] if path.parent.name == "figures" else path.parent
                ).as_posix()
            )
        native(path, data)

    monkeypatch.setattr(a, "_write", observed)
    a.publish_artifacts(
        package, tmp_path / "official", tmp_path / "receipt.json", **digests(package)
    )
    assert names == list(a.ARTIFACTS)


@WINDOWS
def test_defensive_mapping_copy_survives_caller_change(package, tmp_path, monkeypatch):
    caller = package.copy()
    native = a._write
    touched = False

    def alter(path, data):
        nonlocal touched
        if not touched:
            caller["report.md"] = b"forged"
            touched = True
        native(path, data)

    monkeypatch.setattr(a, "_write", alter)
    a.publish_artifacts(
        caller, tmp_path / "official", tmp_path / "receipt.json", **digests(package)
    )
    assert (tmp_path / "official/report.md").read_bytes() == package["report.md"]


@WINDOWS
def test_external_receipt_cannot_be_in_package(package, tmp_path):
    # The absent package parent also rejects before any staging writes.
    with pytest.raises((FileExistsError, e.EvaluationContractError)):
        a.publish_artifacts(
            package,
            tmp_path / "official",
            tmp_path / "official/receipt.json",
            **digests(package),
        )
    assert list(tmp_path.iterdir()) == []


@WINDOWS
@pytest.mark.parametrize("change", ["extra", "writable", "manifest_pin"])
def test_disk_package_guards(package, published, tmp_path, independent, change):
    destination, _, receipt = published
    copy_path = tmp_path / "copy"
    shutil.copytree(destination, copy_path)
    expected = receipt["manifest_sha256"]
    if change == "extra":
        (copy_path / "undeclared.txt").write_bytes(b"extra")
    if change == "writable":
        os.chmod(copy_path / "report.md", 0o666)
    if change == "manifest_pin":
        expected = "0" * 64
    with pytest.raises(ValueError):
        independent["verify_package"](
            copy_path, **digests(package), expected_manifest_sha256=expected
        )


@pytest.mark.parametrize(
    "args",
    [
        ["--worker", "--publish-from", "not-opened"],
        [
            "--development",
            "--publish-from",
            "not-opened",
            "--publish-to",
            "never-created",
            "--receipt",
            "no.json",
        ],
        ["--publish-to", "never-created"],
    ],
)
def test_cli_modes_reject_before_worker_or_output(args, tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(REPO / "scripts/run_phase4_correlation_evaluation.py"),
            *args,
        ],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    assert result.returncode == 2 and not (tmp_path / "never-created").exists()


@WINDOWS
def test_package_verifier_cli_uses_installed_entry_point(package, published):
    destination, _, receipt = published
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(REPO / "scripts/verify_phase4_correlation_evaluation.py"),
            "--package",
            str(destination),
            "--config-sha256",
            digests(package)["expected_config_sha256"],
            "--raw-sha256",
            digests(package)["expected_raw_sha256"],
            "--manifest-sha256",
            receipt["manifest_sha256"],
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    assert (
        result.returncode == 0
        and not result.stderr
        and json.loads(result.stdout)["verified"] is True
    )
