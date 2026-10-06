# AegisOps Phase 3 Baseline Model Comparison Report

**Package ID:** `phase3-task-3-9-comparison`  
**Package Version:** `1.0.0`  
**Code Revision:** `481652c2047a59e697c3008115da0faefcbbde01`  
**Generated:** 2026-10-05 14:28:57 UTC  
**Environment:** `simulation`  
**Artifact Root:** `research`  

---

## 1. Executive Summary and Evaluation Scope

This report presents empirical comparison evidence for the approved AegisOps Phase 3 anomaly detection baselines:
- **Prophet Baseline** (`prophet`, version `1.0.0`)
- **Isolation Forest Baseline** (`isolation_forest`, version `1.0.0`)
- **Autoencoder Baseline** (`autoencoder`, version `1.0.0`)

Evaluation was performed across 8 canonical simulator scenarios under controlled, reproducible conditions using disjoint calibration (seed `41`) and evaluation (seed `42`) partitions.

---

## 2. Partition, Policy, and Feature Lineage

### 2.1 Partition and Reproducibility Lineage
- **Calibration Seed:** `41` (Partition ID: `partition-calibration-seed-41`)
- **Evaluation Seed:** `42` (Partition ID: `partition-evaluation-seed-42`)
- **Calibration Cutoff:** 300.0s (Cutoff Timestamp: `2026-01-01T00:05:00+00:00`)
- **Evaluation Start:** `2026-01-01T00:06:00+00:00`
- **Calibration Scenario Runs Recorded:** 8
- **Evaluation Scenario Runs Recorded:** 8

#### Scenario Run Lineage
| Scenario ID | Calibration Run ID | Evaluation Run ID |
|---|---|---|
| `cpu-saturation` | `f183354c-b40f-50c4-aa53-7686a8061bb0` | `11f3e022-a80d-5004-a515-3ec253f4d6c8` |
| `memory-exhaustion` | `05d76f46-f154-5965-bae8-c03cab8177ce` | `6255f42c-6b1e-5c7e-8df0-df20e70a974d` |
| `connection-exhaustion` | `fcc68e7e-d3d2-55b4-aa22-088f48ef4980` | `6d70071a-6cd2-59f0-92b2-297404ca655c` |
| `dependency-latency` | `bcfc6cff-994e-5ece-8c3c-64b059368314` | `c3d2ae76-0170-57d5-8986-e05391c7c9cd` |
| `dependency-failure` | `73505a0c-18c3-5e74-be48-896b4c9f8f1c` | `3a575a9c-62ab-5a97-895f-8e81cdebe35f` |
| `error-rate-spike` | `48faf76f-e2cf-5dfb-ad20-604bf4b05711` | `40c85108-992a-5b8c-8ccf-aaa867df118d` |
| `traffic-surge` | `e1243bef-c393-5599-b1e6-bf8e0b005f6f` | `761159f6-5722-549e-aa87-80b0ced0e580` |
| `bad-deployment-config` | `50d6320a-4c50-57fa-89b3-dde3b9a554dd` | `dee90965-327d-536b-b1e2-1850979314ac` |

### 2.2 Operational Policy Lineage
- **Decision Policy:** `warning_threshold_binary_decision` (v`1.0.0`)
- **Decision Threshold:** `0.50` (Calibrated anomaly score >= decision_threshold (WARNING, ERROR, or CRITICAL) is comparison-positive)
- **Label Policy:** `half_open_active_interval` (v`1.0.0`, overlap rule: `window_start_in_active_interval`)
- **Label Description:** Ground truth positive when window_start falls in [truth_start, truth_end), pre-activation and post-recovery are ground truth negative

### 2.3 Feature and Windowing Lineage
- **Feature Configuration ID:** `feat-cfg-canonical-v1`
- **Feature Names:** `http_request_duration_ms:mean`, `http_requests_total:mean`
- **Window Size / Step Size:** 1.0s / 1.0s
- **Aggregation Methods:** mean, p95, std
- **Imputation Strategy:** `forward_fill`

### 2.4 Model and Calibration Lineage
- **Calibration Method Name:** `empirical_cdf_percentile`
- **Calibration Method Version:** `1.0.0`
- **Calibration Reference ID:** `calib-ref-v1`
- **Calibration Reference Version:** `1.0.0`
- **Calibration Split Rule:** `independent_reference_set`
- **Calibration Split Version:** `1.0.0`
- **Severity Mapping Version:** `1.0.0`

---

## 3. Common Evaluation Matrix and Micro Summary

| Model | Total Units | TP | FP | TN | FN | Precision | Recall | F1 | FPR |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Prophet | 240 | 39 | 89 | 71 | 41 | 0.3047 | 0.4875 | 0.3750 | 0.5563 |
| Isolation Forest | 240 | 53 | 74 | 86 | 27 | 0.4173 | 0.6625 | 0.5121 | 0.4625 |
| Autoencoder | 240 | 52 | 72 | 88 | 28 | 0.4194 | 0.6500 | 0.5098 | 0.4500 |

---

## 4. Detection Latency (Event Time)

Detection latency measures the event-time duration (in seconds) between ground-truth fault activation and the first true-positive calibrated alert (`score >= 0.50`).

| Model | Detected Scenarios | Missing Detections | Min Latency | Mean Latency | Median Latency | P95 Latency | Max Latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| Prophet | 8 | 0 | 2.00s | 2.00s | 2.00s | 2.00s | 2.00s |
| Isolation Forest | 8 | 0 | 2.00s | 2.00s | 2.00s | 2.00s | 2.00s |
| Autoencoder | 8 | 0 | 1.00s | 1.00s | 1.00s | 1.00s | 1.00s |

---

## 5. Scenario Coverage and Non-Success Breakdown

| Model | Requested | Executed | Scored | Evaluable | Detected | Insufficient Data | Failed |
|---|---:|---:|---:|---:|---:|---:|---:|
| Prophet | 8 | 8 | 8 | 8 | 8 | 8 | 0 |
| Isolation Forest | 8 | 8 | 8 | 8 | 8 | 8 | 0 |
| Autoencoder | 8 | 8 | 8 | 8 | 8 | 8 | 0 |

---

## 6. Detailed Per-Scenario Performance

| Scenario ID | Model | TP | FP | TN | FN | Precision | Recall | F1 | Latency | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `cpu-saturation` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `cpu-saturation` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `cpu-saturation` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |
| `memory-exhaustion` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `memory-exhaustion` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `memory-exhaustion` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |
| `connection-exhaustion` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `connection-exhaustion` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `connection-exhaustion` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |
| `dependency-latency` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `dependency-latency` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `dependency-latency` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |
| `dependency-failure` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `dependency-failure` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `dependency-failure` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |
| `error-rate-spike` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `error-rate-spike` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `error-rate-spike` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |
| `traffic-surge` | Prophet | 4 | 12 | 8 | 6 | 0.25 | 0.40 | 0.31 | 2.0s | completed |
| `traffic-surge` | Isolation Forest | 4 | 11 | 9 | 6 | 0.27 | 0.40 | 0.32 | 2.0s | completed |
| `traffic-surge` | Autoencoder | 3 | 9 | 11 | 7 | 0.25 | 0.30 | 0.27 | 1.0s | completed |
| `bad-deployment-config` | Prophet | 5 | 11 | 9 | 5 | 0.31 | 0.50 | 0.38 | 2.0s | completed |
| `bad-deployment-config` | Isolation Forest | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 2.0s | completed |
| `bad-deployment-config` | Autoencoder | 7 | 9 | 11 | 3 | 0.44 | 0.70 | 0.54 | 1.0s | completed |

---

## 7. Score Distributions and Calibration Lineage

### Prophet
- **Truth-Positive Distribution (N=80):** Min=0.0, Mean=0.480564, Median=0.389943, P95=0.924108, Max=0.924108
- **Truth-Negative Distribution (N=120):** Min=0.0, Mean=0.64363, Median=0.732506, P95=1.0, Max=1.0

### Isolation Forest
- **Truth-Positive Distribution (N=80):** Min=0.077409, Mean=0.615741, Median=0.685766, P95=0.943651, Max=0.943651
- **Truth-Negative Distribution (N=120):** Min=0.078411, Mean=0.617016, Median=0.688194, P95=0.963373, Max=0.963373

### Autoencoder
- **Truth-Positive Distribution (N=80):** Min=0.0, Mean=0.575672, Median=0.67795, P95=0.9449, Max=0.9449
- **Truth-Negative Distribution (N=120):** Min=0.0, Mean=0.593379, Median=0.794717, P95=1.0, Max=1.0

---

## 8. Negative, False Positive, and Failure Inventory

- **Unique Evaluation Units:** 240
- **Expected Model-Outcome Slots:** 720 (240 units × 3 models)
- **Recorded Model Outcomes:** 720
- **Duplicate Model Outcomes:** 0
- **Missing Model-Outcome Slots:** 0
- **Total True Positives:** 144 across all models and scenarios.
- **Total False Positives:** 235 across all models and scenarios.
- **Total False Negatives:** 96 across all models and scenarios.
- **Total True Negatives:** 245 across all models and scenarios.

### 8.1 Detailed Outcome Status Breakdown
| Model | Success | Insufficient Data | Missing Calibration | Non-Convergence | Fit Failure | Invalid Input | Model Failure | Not Applicable | Total Recorded |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Prophet | 200 | 40 | 0 | 0 | 0 | 0 | 0 | 0 | 240 |
| Isolation Forest | 200 | 40 | 0 | 0 | 0 | 0 | 0 | 0 | 240 |
| Autoencoder | 200 | 40 | 0 | 0 | 0 | 0 | 0 | 0 | 240 |
| **Total** | **600** | **120** | **0** | **0** | **0** | **0** | **0** | **0** | **720** |

---

## 9. Limitations and Research Scope

1. **Controlled Simulation Environment:** Results reflect synthetic telemetry generated by the AegisOps canonical microservice simulator and should not be construed as universal production benchmarks.
2. **Single Seed Split Evaluation:** Calibration and evaluation were run with fixed seeds (`41` and `42`); statistical confidence bounds across multi-seed Monte Carlo runs remain future research.
3. **Operational Decision Threshold:** Threshold `0.50` represents an operational standard corresponding to WARNING severity, not an analytically optimized decision point.
4. **Enterprise Scope & Boundaries:** Production SLAs, horizontal scalability, causal diagnosis, and automated remediation are not evaluated in this benchmark and remain unverified.

---

## 10. Machine-Readable Artifact Index

| Artifact Path | Format | Description |
|---|---|---|
| `research/experiments/phase3/task_3_9/comparison_manifest.json` | `application/json` | Schema `1.0` (SHA-256: `058ba566ab9a...`) |
| `research/experiments/phase3/task_3_9/prophet_experiment.json` | `application/json` | Schema `1.0` (SHA-256: `4d6255e8175e...`) |
| `research/experiments/phase3/task_3_9/isolation_forest_experiment.json` | `application/json` | Schema `1.0` (SHA-256: `e807ef3613f2...`) |
| `research/experiments/phase3/task_3_9/autoencoder_experiment.json` | `application/json` | Schema `1.0` (SHA-256: `d863a3858fad...`) |
| `research/results/raw/phase3/task_3_9/evaluation_outcomes.jsonl` | `application/x-ndjson` | Schema `1.0` (SHA-256: `dbce32f53227...`) |
| `research/results/processed/phase3/task_3_9/model_comparison.json` | `application/json` | Schema `1.0` (SHA-256: `96e286a54f94...`) |
| `research/results/processed/phase3/task_3_9/model_comparison.csv` | `text/csv` | Schema `1.0` (SHA-256: `6db8df2f2654...`) |
| `research/results/processed/phase3/task_3_9/scenario_comparison.csv` | `text/csv` | Schema `1.0` (SHA-256: `3cd1e3ae323d...`) |
| `research/results/figures/phase3/task_3_9/model_quality_metrics.svg` | `image/svg+xml` | Schema `1.0` (SHA-256: `8c52780cff6d...`) |
| `research/results/figures/phase3/task_3_9/detection_latency_by_scenario.svg` | `image/svg+xml` | Schema `1.0` (SHA-256: `95ef62163405...`) |
| `research/reports/phase3/task_3_9/evaluation_report.md` | `text/markdown` | Schema `1.0` (generated report) |
| `research/results/processed/phase3/task_3_9/artifact_manifest.json` | `application/json` | Schema `1.0` (`self_digest_policy: excluded_from_manifest_digest`) |

**Manifest Self-Digest Policy:**
The artifact manifest (`research/results/processed/phase3/task_3_9/artifact_manifest.json`) digests all 11 other final artifacts in the package. Self-digestion of the manifest is intentionally excluded (`self_digest_policy: excluded_from_manifest_digest`) because calculating a SHA-256 digest over a file that contains its own digest is mathematically recursive. The manifest digest is verified externally after generation.
