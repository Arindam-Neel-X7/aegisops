#!/usr/bin/env python3
"""AegisOps Phase 3 Task 3.9 - Evaluation Harness & Comparison Reports Entry Point."""

from __future__ import annotations

import argparse
import asyncio
import math
from pathlib import Path
import subprocess
import sys
import time

# Ensure backend directory is in sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = REPO_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from pydantic import ValidationError  # noqa: E402

from app.anomaly.errors import (  # noqa: E402
    EvaluationArtifactError,
    EvaluationConfigurationError,
    EvaluationExecutionError,
)
from app.anomaly.evaluation import (  # noqa: E402
    _compute_sha256,
    _validate_safe_artifact_root,
    create_default_evaluation_harness_config,
    run_phase3_evaluation,
)

APPROVED_CLI_PAIR_ALLOWLIST: frozenset[tuple[type[Exception], str]] = frozenset(
    [
        (
            EvaluationConfigurationError,
            "lineage.scenario_id.type: scenario_id must be a string",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario_id.empty: scenario_id cannot be empty or whitespace-only",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.type: seed must be an exact integer",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.range: seed must be within unsigned 64-bit range",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.type: run_id must be a UUID instance",
        ),
        (
            EvaluationConfigurationError,
            "lineage.timestamp.naive: runtime lineage timestamps must be timezone-aware UTC",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.missing: configured scenario lineage is incomplete",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.unknown: lineage contains an unconfigured scenario",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.duplicate: lineage contains duplicate scenario entries",
        ),
        (
            EvaluationConfigurationError,
            "lineage.scenario.order: lineage does not follow canonical scenario order",
        ),
        (
            EvaluationConfigurationError,
            "lineage.seed.mismatch: lineage seed does not match configured partition seed",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.mismatch: lineage run_id does not match deterministic derivation",
        ),
        (
            EvaluationConfigurationError,
            "lineage.run_id.collision: calibration and evaluation partition run IDs overlap",
        ),
        (
            EvaluationConfigurationError,
            "lineage.temporal.order: evaluation start must be strictly after calibration cutoff",
        ),
        (
            EvaluationExecutionError,
            "Evaluation lineage validation failed",
        ),
        (
            EvaluationExecutionError,
            "Evaluation validation failed",
        ),
        (
            EvaluationConfigurationError,
            "Failed to discover git revision from repository",
        ),
        (
            EvaluationConfigurationError,
            "Git rev-parse returned empty revision",
        ),
    ]
)


def _safe_cli_error_message(exc: BaseException) -> str:
    if not isinstance(exc, Exception):
        raise exc

    exc_type = type(exc)
    if (
        exc_type in (EvaluationConfigurationError, EvaluationExecutionError)
        and len(exc.args) == 1
        and type(exc.args[0]) is str  # noqa: E721
    ):
        candidate_pair = (exc_type, exc.args[0])
        if candidate_pair in APPROVED_CLI_PAIR_ALLOWLIST:
            return exc.args[0]

    if exc_type is EvaluationConfigurationError:
        return "Evaluation configuration failed"
    if exc_type is EvaluationArtifactError:
        return "Evaluation artifact processing failed"
    if exc_type is EvaluationExecutionError:
        return "Evaluation execution failed"
    if exc_type is ValidationError:
        return "Evaluation configuration validation failed"
    return "Evaluation failed due to an unexpected internal error"


def _emit_cli_error(body: str) -> None:
    print(f"Error: {body}", file=sys.stderr)


def _get_git_revision() -> str:
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        sha = res.stdout.strip()
        if sha:
            return sha
    except Exception:
        raise EvaluationConfigurationError(
            "Failed to discover git revision from repository"
        ) from None
    raise EvaluationConfigurationError(
        "Git rev-parse returned empty revision"
    ) from None


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run AegisOps Phase 3 baseline anomaly detection evaluation across canonical scenarios."
    )
    parser.add_argument(
        "--artifact-root",
        type=str,
        default="research",
        help="Project-relative artifact root directory (default: 'research')",
    )
    parser.add_argument(
        "--calibration-seed",
        type=int,
        default=41,
        help="Deterministic random seed for calibration runs (default: 41)",
    )
    parser.add_argument(
        "--evaluation-seed",
        type=int,
        default=42,
        help="Deterministic random seed for evaluation runs (default: 42)",
    )
    parser.add_argument(
        "--decision-threshold",
        type=float,
        default=0.50,
        help="Binary decision threshold on calibrated scores in [0.0, 1.0] (default: 0.50)",
    )
    return parser.parse_args(args)


async def main_async(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        if (
            not (REPO_ROOT / "backend").exists()
            or not (REPO_ROOT / "research").exists()
        ):
            _emit_cli_error("Invalid repository root structure")
            return 1

        try:
            artifact_root = _validate_safe_artifact_root(args.artifact_root)
        except ValueError:
            _emit_cli_error("Invalid artifact root path")
            return 1

        if args.calibration_seed == args.evaluation_seed:
            _emit_cli_error("Calibration seed and evaluation seed must differ")
            return 1

        dt = args.decision_threshold
        if type(dt) is int:  # noqa: E721
            threshold_invalid = dt < 0 or dt > 1
        elif type(dt) is float:  # noqa: E721
            threshold_invalid = not math.isfinite(dt) or dt < 0.0 or dt > 1.0
        else:
            threshold_invalid = True

        if threshold_invalid:
            _emit_cli_error("Decision threshold must be a finite float in [0.0, 1.0]")
            return 1
        decision_threshold = float(dt)

        code_rev = _get_git_revision()

        config = create_default_evaluation_harness_config(
            code_revision=code_rev,
            calibration_seed=args.calibration_seed,
            evaluation_seed=args.evaluation_seed,
            artifact_root=artifact_root,
            decision_threshold=decision_threshold,
        )

        print("======================================================================")
        print("AEGISOPS PHASE 3 TASK 3.9 - EVALUATION HARNESS & COMPARISON")
        print("======================================================================")
        print(f"Code revision:       {config.code_revision}")
        print(f"Calibration seed:    {config.calibration_seed}")
        print(f"Evaluation seed:     {config.evaluation_seed}")
        print(f"Decision threshold:  {config.decision_policy.decision_threshold:.2f}")
        print(
            f"Canonical scenarios: {len(config.scenario_ids)} ({', '.join(config.scenario_ids)})"
        )
        print(
            f"Baseline models:     {len(config.model_names)} ({', '.join(config.model_names)})"
        )
        print("----------------------------------------------------------------------")
        print(
            f"Running evaluation across all {len(config.scenario_ids)} canonical scenarios..."
        )

        start_time = time.perf_counter()
        manifest, summaries = await run_phase3_evaluation(config, base_dir=REPO_ROOT)
        elapsed = time.perf_counter() - start_time

        print(f"Evaluation completed in {elapsed:.2f}s")
        print("----------------------------------------------------------------------")
        print(
            f"SUMMARY OF RESULTS (Micro Aggregations across {len(config.scenario_ids)} Canonical Scenarios):"
        )
        print(
            f"{'Model':<20} {'TP':<5} {'FP':<5} {'TN':<5} {'FN':<5} {'Precision':<10} {'Recall':<10} {'F1':<10} {'FPR':<10} {'Detected':<10}"
        )
        print("-" * 95)
        for s in summaries:
            c = s.micro_confusion_counts
            p_str = (
                f"{s.micro_precision.value:.4f}"
                if s.micro_precision.value is not None
                else "undefined"
            )
            r_str = (
                f"{s.micro_recall.value:.4f}"
                if s.micro_recall.value is not None
                else "undefined"
            )
            f1_str = (
                f"{s.micro_f1.value:.4f}"
                if s.micro_f1.value is not None
                else "undefined"
            )
            fpr_str = (
                f"{s.micro_false_positive_rate.value:.4f}"
                if s.micro_false_positive_rate.value is not None
                else "undefined"
            )
            det_str = f"{s.latency_summary.contributing_scenario_count}/{len(config.scenario_ids)}"
            disp = s.model_name.replace("_", " ").title()
            print(
                f"{disp:<20} {c.true_positives:<5} {c.false_positives:<5} {c.true_negatives:<5} {c.false_negatives:<5} {p_str:<10} {r_str:<10} {f1_str:<10} {fpr_str:<10} {det_str:<10}"
            )

        manifest_file = REPO_ROOT / manifest.artifact_manifest_path
        if not manifest_file.is_file():
            raise EvaluationArtifactError(
                "Artifact manifest file is missing or inaccessible"
            )
        manifest_sha = _compute_sha256(manifest_file)

        print("----------------------------------------------------------------------")
        print(
            "Generated complete 12-file evaluation package (11 manifest-digested + manifest):"
        )
        print(f"Manifest-digested artifacts ({len(manifest.artifacts)} files):")
        for art in manifest.artifacts:
            print(f"  - {art.path} (SHA256: {art.sha256[:12]}...)")
        print(f"Artifact manifest ({manifest.self_digest_policy}):")
        print(f"  - {manifest.artifact_manifest_path} (SHA256: {manifest_sha[:12]}...)")

        print("======================================================================")
        print("PHASE 3 TASK 3.9 EVALUATION HARNESS COMPLETE — SUCCESS")
        print("======================================================================")
        return 0
    except Exception as exc:
        _emit_cli_error(_safe_cli_error_message(exc))
        return 1


def main() -> None:
    sys.exit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
