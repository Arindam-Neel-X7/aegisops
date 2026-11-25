"""Truth-free worker/raw capture and explicit evaluator-only publication mode.

E1 --development runs one labelled attempt per case/policy. Full 352-record
evaluation remains a separately authorized V1 operation.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "backend"))

from app.correlation import evaluation as e  # noqa: E402


def worker() -> None:
    load_tick = time.perf_counter_ns()
    request = e.deserialize_evaluation(
        e.RuntimeRequest, sys.stdin.buffer.read(e.MAX_INPUT_BYTES + 1)
    )

    def emit(frame: dict) -> None:
        print(
            json.dumps(
                frame,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ),
            flush=True,
        )

    loaded = time.perf_counter_ns() - load_tick
    try:
        result = asyncio.run(e.execute_runtime_request(request, emit))
    except Exception as error:
        emit(dict(kind="failure", exception_type=type(error).__name__))
        raise SystemExit(1) from None
    result = result.model_copy(
        update={
            "timings": (
                e.StageTiming(
                    stage="load",
                    decision=None,
                    group=None,
                    elapsed_ns=loaded,
                    status="observed",
                ),
            )
            + result.timings
        }
    )
    # Revalidate before committing the terminal result to the pipe.
    payload = json.loads(e.serialize_evaluation(result, limit=e.MAX_RAW_BYTES))
    emit(dict(kind="result", value=payload))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--development", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--workspace", type=Path, default=REPO.parent)
    parser.add_argument("--config-sha256")
    parser.add_argument(
        "--publish-from",
        type=Path,
        help="authenticated complete raw_outcomes.json; never dispatches workers",
    )
    parser.add_argument("--publish-to", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--raw-sha256")
    args = parser.parse_args()
    publication = any(
        (args.publish_from, args.publish_to, args.receipt, args.raw_sha256)
    )
    if publication:
        if (
            not all(
                (
                    args.publish_from,
                    args.publish_to,
                    args.receipt,
                    args.config_sha256,
                    args.raw_sha256,
                )
            )
            or args.worker
            or args.development
            or args.output is not None
        ):
            parser.error(
                "publication requires raw input/digest, config digest, fresh destination/receipt; worker/capture flags cannot be mixed"
            )
        from app.correlation import evaluation_artifacts as artifacts

        config_raw = artifacts.read_bounded(
            REPO / "research/configs/phase4_correlation_evaluation.json",
            e.MAX_INPUT_BYTES,
        )
        if hashlib.sha256(config_raw).hexdigest() != args.config_sha256:
            raise e.EvaluationContractError("Independent config hash mismatch")
        config = e.deserialize_evaluation(e.EvaluationConfig, config_raw)
        runtime_raw = artifacts.read_bounded(
            REPO / config.runtime_identity.path, e.MAX_INPUT_BYTES
        )
        truth_raw = artifacts.read_bounded(
            REPO / config.truth_identity.path, e.MAX_INPUT_BYTES
        )
        topology_raw = artifacts.read_bounded(
            REPO / "research/datasets/phase4_correlation_evaluation/topologies.json",
            e.MAX_INPUT_BYTES,
        )
        raw = artifacts.read_bounded(args.publish_from, e.MAX_RAW_BYTES)
        files = artifacts.render_artifacts(
            config_raw,
            runtime_raw,
            truth_raw,
            topology_raw,
            raw,
            expected_config_sha256=args.config_sha256,
            expected_raw_sha256=args.raw_sha256,
        )
        receipt = artifacts.publish_artifacts(
            files,
            args.publish_to,
            args.receipt,
            expected_config_sha256=args.config_sha256,
            expected_raw_sha256=args.raw_sha256,
        )
        print(
            json.dumps(
                dict(
                    published=receipt["root"],
                    receipt=str(args.receipt),
                    verified=receipt["verified"],
                ),
                sort_keys=True,
            )
        )
        return
    if args.worker:
        worker()
        return
    if args.output is None or args.config_sha256 is None:
        parser.error("--output and independently approved --config-sha256 are required")
    config_raw = (
        REPO / "research/configs/phase4_correlation_evaluation.json"
    ).read_bytes()
    if hashlib.sha256(config_raw).hexdigest() != args.config_sha256:
        raise e.EvaluationContractError("Independent config hash mismatch")
    config = e.deserialize_evaluation(e.EvaluationConfig, config_raw)
    runtime_raw = (REPO / config.runtime_identity.path).read_bytes()
    e._check_identity(runtime_raw, config.runtime_identity)
    runtime = e.deserialize_evaluation(e.RuntimeInputs, runtime_raw)
    plans = e.plan_evaluation(config_raw, runtime)
    cases = {case.case_id: case for case in runtime.cases}
    topologies = {topology.name: topology for topology in config.topologies}
    policies = {policy.name: policy for policy in config.policies}
    sources = {source.path: source for source in config.source_identities}
    # Runtime/implementation bytes are additional observed sources. Truth is never
    # opened here or sent to a worker; only its approved opaque plan hash remains.
    for path in [
        Path(__file__),
        REPO / "backend/app/correlation/evaluation.py",
        REPO / config.runtime_identity.path,
        REPO / "research/configs/phase4_correlation_evaluation.json",
    ]:
        raw = path.read_bytes()
        relative = path.resolve().relative_to(args.workspace.resolve()).as_posix()
        sources[relative] = e.PortableIdentity(
            path=relative, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()
        )
    observed = e.observe_runtime_sources(
        str(args.workspace), tuple(sources[key] for key in sorted(sources))
    )
    environment = e.runtime_environment()
    selected = (
        tuple(plan for plan in plans if plan.warmup) if args.development else plans
    )
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.mkdir(parents=True, exist_ok=False)
    completed = []
    for plan in selected:
        folder = args.output / plan.run
        folder.mkdir(exist_ok=False)
        case = cases[plan.case_id]
        request = e.RuntimeRequest(
            plan=plan,
            case=case,
            policy=policies[plan.policy],
            topology=topologies[case.topology_key],
            sources=observed,
            workspace=str(args.workspace),
            environment=environment,
        )
        request_raw = e.serialize_evaluation(request)
        with (folder / "request.json").open("xb") as handle:
            handle.write(request_raw)
        record, out, err, pid = e.capture_runtime_run(
            request, str(Path(__file__)), capture_folder=str(folder)
        )
        for name, raw in [
            ("raw_run.json", e.serialize_evaluation(record, limit=e.MAX_RAW_BYTES)),
        ]:
            with (folder / name).open("xb") as handle:
                handle.write(raw)
        completed.append(
            dict(
                run=plan.run,
                pid=pid,
                status=record.terminal_status,
                request_sha256=hashlib.sha256(request_raw).hexdigest(),
                raw_sha256=hashlib.sha256(
                    (folder / "raw_run.json").read_bytes()
                ).hexdigest(),
            )
        )
        print(json.dumps(completed[-1]), flush=True)
    if not args.development:
        runs = tuple(
            e.deserialize_evaluation(
                e.RawRun,
                (args.output / plan.run / "raw_run.json").read_bytes(),
                limit=e.MAX_RAW_BYTES,
            )
            for plan in plans
        )
        value = e.RawOutcomes(
            schema_version=e.SCHEMA_VERSION,
            config_sha256=args.config_sha256,
            planned=plans,
            runs=runs,
        )
        # Only runtime bindings, never metrics/truth parsing. M1/V1 independently
        # bind raw against the evaluator's separate authenticated truth sidecar.
        e.bind_raw_outcomes(value, config_raw, runtime)
        with (args.output / "raw_outcomes.json").open("xb") as handle:
            handle.write(e.serialize_evaluation(value, limit=e.MAX_RAW_BYTES))
    with (args.output / "development_capture.json").open(
        "x", encoding="utf-8", newline="\n"
    ) as handle:
        json.dump(
            dict(
                scope="E1_DEVELOPMENT" if args.development else "V1_RAW_EVALUATION",
                planned=len(selected),
                completed=completed,
                metrics_calculated=False,
                M1_started=False,
            ),
            handle,
            sort_keys=True,
            indent=2,
        )
        handle.write("\n")
    for path in args.output.rglob("*"):
        if path.is_file():
            os.chmod(path, 0o444)


if __name__ == "__main__":
    main()
