# AegisOps Research Artifacts

## Purpose

This README is the canonical Phase 0 guide to the AegisOps research artifact structure. It defines how research artifacts are organized, the purpose of each directory, the roles of configuration and ground-truth artifacts, and the methodologies for ensuring reproducible and objective comparative experiments. 

This is a methodology guide, NOT an operational, model-training, or destructive chaos engineering manual.

## Directory Structure

The current AegisOps research directory is organized as follows:

```text
research/
├── configs/
├── datasets/
├── experiments/
├── notebooks/
├── reports/
└── results/
```

## Research Artifact Roles

### `configs/`
Intended for declarative research configuration and validation contracts. Currently stores canonical JSON schema contracts.

### `configs/faults/`
Reserved for fault-related configuration/ground-truth documentation and schema artifacts.

### `datasets/`
Intended for research dataset copies, references, derived datasets, or versioned dataset manifests as appropriate.

### `experiments/`
Intended for experiment-specific definitions, scenario selection records, and configuration packages.

### `notebooks/`
Reserved for exploratory data analysis, visualization, offline investigation, and research prototyping. Notebooks should NOT become the sole authoritative source of experiment configuration or ground truth. Canonical configuration remains machine-readable and version-controlled.

### `results/`
Intended for machine-generated or experiment-generated raw result artifacts (e.g., future metric results, timing measurements, predictions). 

### `reports/`
Intended for human-readable research summaries, plots, tables, benchmark summaries, and thesis-ready analysis outputs.

## Artifact Roles

### Experiment Manifest
The experiment manifest (`configs/experiment_manifest.schema.json`) defines the contract for a declarative experiment configuration. 
Conceptually, it defines:
- Experiment identity
- Dataset name and version
- Random seed
- Scenario configuration reference
- Requested metrics (metric names requested for later evaluation)
- Output artifact root
- Fixed reproducibility conditions

**Manifest vs Results Boundary:** The manifest specifies *what* to run and *where* to store outputs. It does NOT contain actual measured values, latency outputs, anomaly scores, RCA accuracy, false-positive counts, or remediation outcomes. Those belong under `research/results/`.

### Scenario / Fault Configuration
The `scenario.config_reference` inside the experiment manifest points to the external scenario/fault CONFIGURATION. It does NOT point directly to ground-truth records, result files, or evaluation reports. 

### Ground Truth
Ground-truth records (`configs/faults/ground_truth.schema.json`) record KNOWN injected fault truth. 
- Conceptually, it includes `record_id`, `fault_id`, `target_service_id`, `fault_type`, `injected_at`, `expected_root_cause`, `expected_affected_service_ids`, and `metadata`.
- **One record = one injected fault**. If multiple faults are injected, multiple records are produced.
- Ground truth is strictly for objective evaluation; it is not a model prediction or a measured result.

### Fault Catalogue
The Fault Catalogue (`configs/faults/README.md`) defines the canonical fault taxonomy and semantic guidance for the six approved fault types (`latency`, `error`, `timeout`, `crash`, `resource`, `network`).

## Reproducibility Requirements

Reproducibility is paramount for AegisOps research:

### Random Seed
A random seed is required in the experiment manifest to enable deterministic or repeatable conditions where supported. Note that the seed alone does not guarantee full determinism.

### Reproducibility Conditions
Because the seed is insufficient alone, `reproducibility.conditions` must be recorded. Examples conceptually include observation windows, warm-up periods, execution environment, and algorithm settings. `code_version` and `environment` may also be recorded.

### Datasets and Versioning
- Experiments should identify dataset name/version through the manifest.
- Datasets should be versioned or unambiguously referenced.
- Machine-specific absolute paths should be avoided.
- Secret-bearing storage references must not be stored in manifests.
- Derived datasets should preserve provenance where feasible.

### Naming Guidance
When naming artifacts, use descriptive names and stable identifiers for machine-readable records. Avoid spaces, do not use timestamp-only names if identity would be ambiguous, and use relative/project-portable paths.

## Comparative Experiment Methodology

When comparing methods or pipelines, the Engineering Baseline dictates that identical evaluation conditions and fault scenarios should be preserved where possible.

Comparisons should conceptually preserve (where relevant):
- Same scenario/fault configuration
- Same target service
- Same fault type
- Same relevant parameters
- Same dataset and version
- Same random seed
- Same observation/evaluation conditions
- Same requested metrics

## Result and Evidence Discipline

- **Negative Results / False Positives:** The research methodology treats negative results, failed fault scenarios, and false positives as valid, essential evidence. They must not be silently discarded and belong in results/reports.
- **Performance / Latency Measurement:** Latency-sensitive pipeline stages should capture performance measurements where applicable. While metric names may be requested in the manifest, actual measured values strictly belong in result artifacts.
- **Research Claim Discipline:** Do not present target performance numbers as measured results. Distinguish model confidence from empirical accuracy, retain negative evidence, and preserve comparable experimental conditions.

## Conceptual Experiment Lifecycle

1. `experiment manifest`
2. `scenario/fault configuration`
3. `experiment execution`
4. `injected fault(s)`
5. `ground-truth record(s)`
6. `collected evaluation results`
7. `analysis/report`

## Validation Guidance

Experiment manifests should validate against `configs/experiment_manifest.schema.json`. Ground-truth records should validate against `configs/faults/ground_truth.schema.json`.

*Note on Format Validation:* Validating `format: uuid` and `format: date-time` fields strictly requires JSON Schema validators (e.g., `jsonschema` in Python) that actively assert format checks. Full automated validation tooling may require these dependencies to be explicitly installed in the execution environment.

## Safety and Sensitive Data

Research artifacts MUST NOT store:
- Passwords
- API keys
- Access tokens
- Production credentials
- Secret-bearing connection strings
- Unnecessary PII

Fault research remains exclusively within controlled simulator/research environments. This structure provides NO operational disruption instructions.

## Extension Policy

Future changes to research contracts (e.g., new manifest fields, new fault types, result schemas, scenario schemas) must be explicitly coordinated. Future extensions should:
1. Be explicitly designed.
2. Update machine-readable contracts.
3. Update relevant documentation.
4. Update validation/tests.
5. Preserve backward compatibility where practical.

## Related Artifacts

- [Experiment Manifest Schema](configs/experiment_manifest.schema.json)
- [Ground-Truth Record Schema](configs/faults/ground_truth.schema.json)
- [Fault Catalogue](configs/faults/README.md)
