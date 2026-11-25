"""E1: real production dispatch on development inputs; no quality tuning."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path

import pytest

from app.correlation import evaluation as e
from app.correlation import scoring, grouping, aggregate, blast_radius, handoff

REPO = Path(__file__).resolve().parents[2]
WORKSPACE = REPO.parent
CONFIG = REPO / "research/configs/phase4_correlation_evaluation.json"
RUNTIME = REPO / "research/datasets/phase4_correlation_evaluation/runtime.json"
SCRIPT = REPO / "scripts/run_phase4_correlation_evaluation.py"


@pytest.fixture(scope="module")
def package():
    config = e.deserialize_evaluation(e.EvaluationConfig, CONFIG.read_bytes())
    runtime = e.deserialize_evaluation(e.RuntimeInputs, RUNTIME.read_bytes())
    plans = e.plan_evaluation(CONFIG.read_bytes(), runtime)
    return config, runtime, plans


def _runtime_sources(
    config: e.EvaluationConfig,
) -> tuple[Path, tuple[e.PortableIdentity, ...]]:
    """Use full workspace pins when available; otherwise verify all 15 repo sources."""
    if all(
        (WORKSPACE / source.path).is_file()
        and hashlib.sha256((WORKSPACE / source.path).read_bytes()).hexdigest()
        == source.sha256
        for source in config.source_identities
    ):
        return WORKSPACE, config.source_identities
    repo_sources = []
    for source in config.source_identities:
        if not source.path.startswith("aegisops/"):
            continue
        relative = source.path.removeprefix("aegisops/")
        raw = (REPO / relative).read_bytes()
        repo_sources.append(
            e.PortableIdentity(
                path=relative,
                bytes=len(raw),
                sha256=hashlib.sha256(raw).hexdigest(),
            )
        )
    assert len(repo_sources) == 15
    return REPO, tuple(repo_sources)


def request(package, index=0):
    config, runtime, plans = package
    case = runtime.cases[index]
    plan = next(p for p in plans if p.case_id == case.case_id and p.warmup)
    workspace, sources = _runtime_sources(config)
    return e.RuntimeRequest(
        plan=plan,
        case=case,
        policy=config.policies[0],
        topology=next(t for t in config.topologies if t.name == case.topology_key),
        sources=sources,
        workspace=str(workspace),
        environment=e.runtime_environment(),
    )


def execute(value):
    frames = []
    result = asyncio.run(e.execute_runtime_request(value, frames.append))
    parsed = e.deserialize_evaluation(
        e.RawRun,
        e.serialize_evaluation(result, limit=e.MAX_RAW_BYTES),
        limit=e.MAX_RAW_BYTES,
    )
    assert parsed == result
    assert result.sources_before == result.sources_after
    return result, frames


def semantic(result):
    return dict(
        available=result.final_grouping_available,
        status=result.terminal_status,
        decisions=[
            d.model_dump(mode="json", exclude={"timings", "errors"})
            for d in result.decisions
        ],
        enrichments=[
            item.model_dump(mode="json", exclude={"timings", "errors"})
            for item in result.enrichments
        ],
    )


@pytest.mark.parametrize("index", range(16))
def test_actual_calls_capture_every_case_without_changing_sources(package, index):
    value = request(package, index)
    before = e.serialize_evaluation(value)
    result, frames = execute(value)
    assert e.serialize_evaluation(value) == before
    assert len(result.decisions) == len(value.case.decisions)
    assert sum(f["kind"] == "delivery" for f in frames) == len(value.case.deliveries)
    assert result.terminal_status == (
        "CONTEXT_UNAVAILABLE" if index == 14 else "SUCCESS"
    )
    assert all(t.elapsed_ns >= 0 for d in result.decisions for t in d.timings)
    assert len(
        {f["value"]["decision"] for f in frames if f["kind"] == "decision"}
    ) == len(result.decisions)


def test_real_scorer_and_grouping_receive_exact_recorded_arguments(
    package, monkeypatch
):
    score = scoring.score_candidate_pair
    group = grouping.group_candidate_signals
    scored, grouped = [], []

    def score_spy(left, right, **kwargs):
        value = score(left, right, **kwargs)
        scored.append((left, right, kwargs, value))
        return value

    def group_spy(inputs, evidence, **kwargs):
        grouped.append((tuple(inputs), tuple(evidence), kwargs))
        return group(inputs, evidence, **kwargs)

    monkeypatch.setattr(scoring, "score_candidate_pair", score_spy)
    monkeypatch.setattr(grouping, "group_candidate_signals", group_spy)
    result, _ = execute(request(package))
    assert len(scored) == 10 and len(grouped) == 1
    captured = result.decisions[0]
    assert grouped[0][0] == captured.arguments.runtime_inputs
    assert grouped[0][1] == captured.pair_evidence == tuple(v[3] for v in scored)
    assert (
        tuple(
            (left.signal.signal_id, right.signal.signal_id)
            for left, right, _, _ in scored
        )
        == captured.arguments.pair_order
    )


def test_truth_sidecar_permutation_cannot_reach_actual_dispatch(package):
    sidecar = json.loads(
        (
            REPO / "research/datasets/phase4_correlation_evaluation/truth.json"
        ).read_bytes()
    )
    first, _ = execute(request(package, 8))
    altered = copy.deepcopy(sidecar)
    altered["cases"][8]["truth_groups"].reverse()
    for group in altered["cases"][8]["truth_groups"]:
        group["group"] = "altered-" + group["group"]
    assert altered != sidecar
    second, _ = execute(request(package, 8))
    assert semantic(first) == semantic(second)
    assert "truth" not in e.RuntimeRequest.model_fields


@pytest.mark.parametrize("index", [9, 10, 11])
def test_window_replay_and_late_records_match_frozen_schedule(package, index):
    value = request(package, index)
    result, frames = execute(value)
    projected = dict(
        zip((u.unit for u in value.case.units), e.project_runtime_case(value.case))
    )
    for expected, captured in zip(value.case.decisions, result.decisions):
        assert captured.arguments.runtime_inputs == tuple(
            projected[d.unit]
            for d in value.case.deliveries
            if d.arrival_offset_seconds <= expected.arrival_cutoff_seconds
        )
        assert captured.grouping_result.event_time_watermark == e.EPOCH + e.timedelta(
            seconds=expected.watermark_offset_seconds
        )
    if index == 10:
        assert any(f.get("disposition") == "duplicate" for f in frames)
        assert any(
            r.duplicate_count > 1
            for r in result.decisions[-1].grouping_result.signal_records
        )
    if index == 11:
        assert any(
            r.late and not r.included
            for r in result.decisions[-1].grouping_result.signal_records
        )


@pytest.mark.parametrize(
    "boundary,stage",
    [
        (scoring, "scoring"),
        (grouping, "grouping"),
        (aggregate, "aggregate"),
        (blast_radius, "blast"),
        (handoff, "handoff"),
    ],
)
def test_native_failures_remain_visible_without_internal_message(
    package, monkeypatch, boundary, stage
):
    functions = {
        "scoring": "score_candidate_pair",
        "grouping": "group_candidate_signals",
        "aggregate": "create_or_update_incident",
        "blast": "calculate_blast_radius",
        "handoff": "build_phase5_handoff",
    }

    def fail(*args, **kwargs):
        raise RuntimeError("secret DSN sql password internal.example:5432")

    monkeypatch.setattr(boundary, functions[stage], fail)
    result, _ = execute(request(package, 0))
    errors = [error for d in result.decisions for error in d.errors] + [
        error for item in result.enrichments for error in item.errors
    ]
    assert errors and all(
        error.stage == stage and error.exception_type == "RuntimeError"
        for error in errors
    )
    assert (
        "secret" not in e.serialize_evaluation(result, limit=e.MAX_RAW_BYTES).decode()
    )
    assert result.terminal_status == (
        "RUNTIME_ERROR" if stage in {"scoring", "grouping"} else "PARTIAL_FAILURE"
    )
    assert result.final_grouping_available == (stage not in {"scoring", "grouping"})


def test_fresh_intake_state_for_each_run(package):
    first, frames_a = execute(request(package, 10))
    second, frames_b = execute(request(package, 10))
    assert [f for f in frames_a if f["kind"] == "delivery"] == [
        f for f in frames_b if f["kind"] == "delivery"
    ]
    assert semantic(first) == semantic(second)


def test_stale_topology_and_source_drift_fail_before_dispatch(package, monkeypatch):
    value = request(package)
    with pytest.raises(ValueError):
        e.RuntimeRequest.model_validate(
            e._plain(
                value.model_copy(
                    update={
                        "plan": value.plan.model_copy(
                            update={"topology_fingerprint": "0" * 64}
                        )
                    }
                )
            )
        )
    source = value.sources[0].model_copy(update={"sha256": "0" * 64})
    with pytest.raises(e.EvaluationContractError):
        e.observe_runtime_sources(value.workspace, (source,))


def test_real_isolated_worker_and_capture(package):
    result, out, err, pid = e.capture_runtime_run(request(package, 1), str(SCRIPT))
    assert pid > 0 and result.terminal_status == "SUCCESS"
    assert (
        len(
            [
                json.loads(line)
                for line in out.splitlines()
                if json.loads(line)["kind"] == "result"
            ]
        )
        == 1
    )
    assert err.splitlines() in (
        [],
        [b"Importing plotly failed. Interactive plots will not work."],
    )
    assert any(t.stage == "load" for t in result.timings)
    assert result.timings[-1].stage == "worker_wall"


def test_actual_worker_exit_is_retained(package, tmp_path):
    script = tmp_path / "crash.py"
    script.write_text("raise SystemExit(9)\n", encoding="utf-8")
    result, _, _, pid = e.capture_runtime_run(request(package), str(script))
    assert (
        result.terminal_status == "RUNTIME_ERROR"
        and result.errors[0].exception_type == "WorkerExit"
    )
    assert pid > 0 and not result.final_grouping_available


def test_actual_30_second_timeout_kills_child_and_retains_pid(package, tmp_path):
    script = tmp_path / "timeout.py"
    script.write_text(
        'import time\nprint(\'{"kind":"stage","stage":"scoring"}\', flush=True)\ntime.sleep(120)\n',
        encoding="utf-8",
    )
    result, out, _, pid = e.capture_runtime_run(request(package), str(script))
    assert result.terminal_status == "TIMEOUT"
    assert result.timeout.terminated and result.timeout.pid == pid
    assert result.timeout.elapsed_ns >= result.timeout.deadline_ns == 30_000_000_000
    assert result.timeout.elapsed_ns < 45_000_000_000
    assert result.timeout.last_stage == "scoring" and out


def capacity_case(package, count=91):
    body = request(package).case.model_dump(mode="json")
    unit = body["units"][0]
    body["units"] = [
        {**unit, "unit": f"u{index:03}", "event_offset_seconds": 12}
        for index in range(count)
    ]
    body["deliveries"] = [
        dict(delivery=f"d{index:03}", unit=f"u{index:03}", arrival_offset_seconds=13)
        for index in range(count)
    ]
    return body


def test_91_units_score_all_4095_pairs_without_pruning(package, monkeypatch):
    value = request(package)
    case = e.deserialize_evaluation(
        e.RuntimeCase, json.dumps(capacity_case(package)).encode()
    )
    native = scoring.score_candidate_pair
    pairs = []

    def observed(left, right, **kwargs):
        pairs.append((left.signal.signal_id, right.signal.signal_id))
        return native(left, right, **kwargs)

    def stopped_enrichment(**kwargs):
        raise RuntimeError("qualification stops after the complete real grouping")

    monkeypatch.setattr(scoring, "score_candidate_pair", observed)
    monkeypatch.setattr(aggregate, "create_or_update_incident", stopped_enrichment)
    result, _ = execute(value.model_copy(update={"case": case}))
    assert len(pairs) == len(set(pairs)) == 4095
    assert len(result.decisions[0].grouping_result.signal_records) == 91
    assert (
        result.final_grouping_available and result.terminal_status == "PARTIAL_FAILURE"
    )


@pytest.mark.parametrize("field", ["units", "deliveries", "decisions"])
def test_capacity_rejection_is_terminal_before_any_child_or_scoring(
    package, monkeypatch, field
):
    value = request(package)
    body = capacity_case(package, 92 if field == "units" else 1)
    if field == "deliveries":
        body["deliveries"] = [
            dict(delivery=f"d{index:03}", unit="u000", arrival_offset_seconds=13)
            for index in range(183)
        ]
    if field == "decisions":
        body["decisions"] = [
            dict(
                decision=f"q{index:03}",
                arrival_cutoff_seconds=13 + index,
                watermark_offset_seconds=13 + index,
            )
            for index in range(9)
        ]

    def forbidden(*args, **kwargs):
        raise AssertionError("over-capacity input must not dispatch")

    monkeypatch.setattr(e, "capture_runtime_run", forbidden)
    monkeypatch.setattr(scoring, "score_candidate_pair", forbidden)
    result, _, _, pid = e.capture_runtime_attempt(
        value.plan,
        json.dumps(body).encode(),
        value.policy,
        value.topology,
        value.sources,
        value.workspace,
        str(SCRIPT),
        value.environment,
    )
    assert (
        result.terminal_status == "CAPACITY_REJECTED"
        and not result.decisions
        and pid == 0
    )
    assert result.errors[0].exception_type == "RuntimeCapacityError"


def test_earlier_failure_final_success_remains_partial(package, monkeypatch):
    native = grouping.group_candidate_signals
    calls = []

    def first_fails(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("qualification earlier decision failure")
        return native(*args, **kwargs)

    monkeypatch.setattr(grouping, "group_candidate_signals", first_fails)
    result, _ = execute(request(package, 9))
    assert (
        result.decisions[0].status == "failed"
        and result.decisions[-1].status == "success"
    )
    assert (
        result.final_grouping_available and result.terminal_status == "PARTIAL_FAILURE"
    )


def test_nested_truth_fields_cannot_enter_worker(package):
    value = request(package).model_dump(mode="json")
    value["case"]["units"][0]["truth_partition"] = "hidden-answer"
    with pytest.raises(e.EvaluationContractError):
        e.deserialize_evaluation(e.RuntimeRequest, json.dumps(value).encode())


def test_malformed_worker_pipes_are_saved_before_rejection(package, tmp_path):
    script = tmp_path / "invalid.py"
    script.write_text('print(\'{"kind":"result","kind":"stage"}\')\n', encoding="utf-8")
    folder = tmp_path / "capture"
    folder.mkdir()
    with pytest.raises(e.EvaluationContractError):
        e.capture_runtime_run(request(package), str(script), capture_folder=str(folder))
    assert (folder / "stdout.bin").read_bytes()
    command = json.loads((folder / "command.json").read_bytes())
    assert type(command["exit_code"]) is int and command["exit_code"] == 0  # noqa: E721 - typed exit


def test_wrong_worker_result_plan_is_rejected(package, tmp_path):
    value, _ = execute(request(package, 15))
    frame = json.dumps(dict(kind="result", value=value.model_dump(mode="json")))
    script = tmp_path / "wrong.py"
    script.write_text(f"print({frame!r})\n", encoding="utf-8")
    with pytest.raises(e.EvaluationContractError, match="another request"):
        e.capture_runtime_run(request(package, 1), str(script))


def test_worker_result_cannot_change_its_matching_plans_watermark(package, tmp_path):
    # Empty case allows an internally valid rehashed result with the wrong clock;
    # the supervisor must still bind it to the authenticated request schedule.
    value, _ = execute(request(package, 15))
    decision = value.decisions[0]
    args = decision.arguments.model_dump(mode="json")
    args["watermark"] = "2026-01-01T00:10:00.000000Z"
    args.pop("argument_fingerprint")
    import hashlib

    args["argument_fingerprint"] = hashlib.sha256(
        json.dumps(
            args, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()
    arguments = e.deserialize_evaluation(e.CapturedArguments, json.dumps(args).encode())
    result = decision.grouping_result.model_copy(
        update={"event_time_watermark": e.EPOCH + e.timedelta(seconds=600)}
    )
    result = result.model_copy(
        update={"result_fingerprint": e.compute_result_fingerprint(result)}
    )
    altered = value.model_copy(
        update={
            "decisions": (
                decision.model_copy(
                    update={"arguments": arguments, "grouping_result": result}
                ),
            )
        }
    )
    altered = e.deserialize_evaluation(e.RawRun, e.serialize_evaluation(altered))
    frames = [
        dict(kind="decision", value=d.model_dump(mode="json"))
        for d in altered.decisions
    ]
    frames.append(dict(kind="result", value=altered.model_dump(mode="json")))
    script = tmp_path / "clock.py"
    script.write_text(
        "\n".join(f"print({json.dumps(frame)!r})" for frame in frames), encoding="utf-8"
    )
    with pytest.raises(e.EvaluationContractError, match="different runtime schedule"):
        e.capture_runtime_run(request(package, 15), str(script))


def disposable_worker(tmp_path, frames, exit_code=0, *, sleep=False):
    script = tmp_path / "reproduction.py"
    source = "\n".join(f"print({json.dumps(frame)!r}, flush=True)" for frame in frames)
    if sleep:
        source += "\nimport time\ntime.sleep(120)"
    script.write_text(source + f"\nraise SystemExit({exit_code})\n", encoding="utf-8")
    return str(script)


def complete_worker_frames(package):
    result, frames = execute(request(package, 15))
    return frames + [dict(kind="result", value=result.model_dump(mode="json"))]


@pytest.mark.parametrize(
    "position", ["before_result", "after_result", "repeated_failure"]
)
def test_retained_failure_frame_cannot_be_hidden_by_success(
    package, tmp_path, position
):
    frames = complete_worker_frames(package)
    failure = dict(kind="failure", exception_type="RuntimeError")
    if position == "before_result":
        frames.insert(-1, failure)
    elif position == "after_result":
        frames.append(failure)
    else:
        frames = [failure, failure]
    with pytest.raises(e.EvaluationContractError, match="terminal result or failure"):
        e.capture_runtime_run(request(package, 15), disposable_worker(tmp_path, frames))


@pytest.mark.parametrize("exit_code,sleep", [(9, False), (0, True)])
def test_retained_wrong_clock_prefix_rejects_on_crash_and_timeout(
    package, tmp_path, exit_code, sleep
):
    import hashlib

    frames = complete_worker_frames(package)
    captured = next(frame for frame in frames if frame["kind"] == "decision")["value"]
    native = e.deserialize_evaluation(
        e.DecisionOutcome, json.dumps(captured).encode()
    ).grouping_result
    arguments = captured["arguments"]
    arguments["watermark"] = "2026-01-01T00:10:00.000000Z"
    arguments.pop("argument_fingerprint")
    arguments["argument_fingerprint"] = hashlib.sha256(
        json.dumps(
            arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()
    native = native.model_copy(
        update={"event_time_watermark": e.EPOCH + e.timedelta(seconds=600)}
    )
    native = native.model_copy(
        update={"result_fingerprint": e.compute_result_fingerprint(native)}
    )
    captured["grouping_result"] = native.model_dump(mode="json")
    prefix = [frame for frame in frames if frame["kind"] != "result"]
    folder = tmp_path / "capture"
    folder.mkdir()
    with pytest.raises(e.EvaluationContractError, match="different runtime schedule"):
        e.capture_runtime_run(
            request(package, 15),
            disposable_worker(tmp_path, prefix, exit_code, sleep=sleep),
            capture_folder=str(folder),
        )
    assert (folder / "stdout.bin").read_bytes()
    command = json.loads((folder / "command.json").read_bytes())
    assert command["timed_out"] is sleep


def test_valid_completed_prefix_survives_worker_crash(package, tmp_path):
    frames = [
        frame for frame in complete_worker_frames(package) if frame["kind"] != "result"
    ]
    value, _, _, _ = e.capture_runtime_run(
        request(package, 15), disposable_worker(tmp_path, frames, 9)
    )
    assert value.terminal_status == "PARTIAL_FAILURE" and value.final_grouping_available
    expected = e.EPOCH + e.timedelta(
        seconds=request(package, 15).case.decisions[0].watermark_offset_seconds
    )
    assert value.decisions[0].arguments.watermark == expected.strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def test_valid_failure_terminal_is_retained(package, tmp_path):
    frames = [dict(kind="failure", exception_type="RuntimeError")]
    value, _, _, _ = e.capture_runtime_run(
        request(package, 15), disposable_worker(tmp_path, frames, 1)
    )
    assert (
        value.terminal_status == "RUNTIME_ERROR"
        and value.errors[0].exception_type == "RuntimeError"
    )


def test_interrupted_delivery_journal_is_bound_to_request(package, tmp_path):
    value = request(package, 10)
    _, frames = execute(value)
    first = next(frame for frame in frames if frame["kind"] == "delivery")
    accepted, _, _, _ = e.capture_runtime_run(
        value, disposable_worker(tmp_path, [first], 9)
    )
    assert accepted.terminal_status == "RUNTIME_ERROR" and not accepted.decisions
    changed = {**first, "signal_id": "00000000-0000-0000-0000-000000000000"}
    with pytest.raises(e.EvaluationContractError, match="intake journal"):
        e.capture_runtime_run(value, disposable_worker(tmp_path, [changed], 9))
