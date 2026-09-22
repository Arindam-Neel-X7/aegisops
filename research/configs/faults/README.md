# AegisOps Fault Catalogue

## Purpose

This README serves as the canonical Phase 0 fault catalogue and usage guide for AegisOps research experiments. It defines the taxonomy of faults, how faults are specified conceptually in scenarios, and how known injected faults are recorded as ground truth for deterministic research evaluation.

## Canonical Fault Types

The canonical AegisOps Step 9 fault types are exactly:
- `latency`
- `error`
- `timeout`
- `crash`
- `resource`
- `network`

## Fault Catalogue Table

| Fault Type | Meaning | Typical Research Intent | Example Parameter Concepts |
|---|---|---|---|
| `latency` | Added response delay | Evaluate predictive detection of slow degradation | Delay-related configuration |
| `error` | Forced application-level error response | Evaluate root-cause analysis logic for explicit application failures | Error mode/code behavior |
| `timeout` | Request exceeds configured completion window | Evaluate system boundary timeouts and cascade detection | Timeout-window behavior |
| `crash` | Simulated service/process unavailability | Evaluate fast-fail detection and topology impact | Simulated unavailability settings |
| `resource` | Constrained CPU/memory/resource availability | Evaluate subtle hardware/container pressure | Resource-pressure configuration |
| `network` | Simulated connectivity degradation or interruption | Evaluate communication isolation and network segmentation faults | Network degradation characteristics |

## Fault Specification Contract

The `FaultSpec` semantically defines an intended fault injection for an experiment scenario. It includes the following fields:

- `fault_id`: Uniquely identifies the intended injected fault.
- `target_service_id`: UUID of the targeted service.
- `fault_type`: One of the six canonical values.
- `duration_seconds`: Optional positive duration when supplied. It is optional / unspecified when omitted or `None`.
- `parameters`: Fault-specific configuration dictionary.

## Ground-Truth Contract

The `GroundTruthRecord` captures the known injected truth of an executed experiment fault. It is NOT predicted RCA, anomaly detection output, evaluation result data, or remediation outcome data.

Fields include:
- `record_id`: UUID identity of the ground-truth record.
- `fault_id`: Identity of the corresponding injected fault.
- `target_service_id`: UUID identity of the service targeted by the injected fault.
- `fault_type`: Classification of the fault (the six canonical values).
- `injected_at`: Timezone-aware RFC 3339 injection timestamp.
- `expected_root_cause`: The known cause intentionally introduced by the experiment. It is NOT a generated hypothesis, model prediction, or post-incident analyst result.
- `expected_affected_service_ids`: Known services expected to be affected by the injected fault.
- `metadata`: Optional freeform research annotations.

Each ground truth record strictly observes the **one-fault-per-record rule**: one `GroundTruthRecord` corresponds to exactly one injected fault (`fault_id`). If an experiment injects multiple faults, multiple ground-truth records must be produced.

## Experiment / Scenario / Ground-Truth Relationship

The conceptual research flow in AegisOps is strictly modeled as:
`experiment manifest` → `scenario/fault configuration` → `injected fault` → `ground-truth record` → `later evaluation results`

The `scenario.config_reference` from the experiment manifest points strictly to the external scenario/fault CONFIGURATION used to execute the experiment. It does NOT point to the ground-truth record. The ground truth is produced as a separate research artifact capturing the injected fault occurrence.

## Identifier Semantics

Explicit identifier distinctions:
- `fault_id`: UUID identity for the injected fault.
- `target_service_id`: UUID identity of the targeted service.
- `record_id`: UUID identity of the ground-truth record.

Important Note: `scenario_id != fault_id`. A scenario may conceptually cause one or more faults during an experiment. However, one ground-truth record exactly maps to one `fault_id`.

## Expected Affected Services

The `expected_affected_service_ids` field lists known services expected to be affected by the injected fault.
- It is a UUID list.
- Duplicate IDs are not allowed.
- An empty list is valid.
- The field may be omitted externally (representing an empty list).
- The target service MAY appear in this list, but it is NOT required to appear.
- The order of IDs has no semantic meaning.

This field represents declared research ground truth, NOT a dynamically computed topology traversal.

## Metadata Guidance

The `metadata` field acts as optional freeform research annotations (e.g., labels, notes, non-sensitive experiment annotations).

It MUST NOT be used for:
- Secrets, API keys, passwords, or credentials.
- Production connection strings.
- Personal Identifiable Information (PII).
- Model predictions or measured experiment results.

## Reproducibility Guidance

For comparative experiments to remain scientifically reproducible, the following properties should be preserved across comparisons where applicable:
- Identical fault scenario configuration.
- Identical target service selection.
- Identical fault type.
- Identical relevant fault parameters.
- Identical evaluation conditions.
- Stable dataset/version references.
- Stable random seed.
- Comparable observation windows/conditions.

These conditions are conceptually declared within the reproducibility section of the experiment manifest.

## Safety Boundary

This catalogue defines research semantics and data contracts ONLY.

It does NOT provide:
- Production fault injection instructions.
- Destructive host commands.
- Network disruption commands.
- Database corruption procedures.
- Privileged shell commands.
- Deployment-specific sabotage steps.

Future runtime implementations must execute only within the controlled AegisOps research simulator/environment and adhere to the project's approval and safety model.

## Extension Policy

Future changes to `FaultType` values, ground-truth core fields, ID semantics, or timestamp semantics must be treated as contract changes. Arbitrary additions are discouraged. 

The suggested extension policy is:
1. Update the approved Python contract first or through coordinated design.
2. Update the external JSON schema.
3. Update this documentation.
4. Update compatibility tests.
5. Preserve backward compatibility where possible.

## Related Artifacts

- `ground_truth.schema.json`: Defines the external JSON validation contract for ground-truth records.
- `../experiment_manifest.schema.json`: The research experiment manifest schema. The manifest defines experiment configuration (via `scenario.config_reference`), keeping ground-truth records separate.
