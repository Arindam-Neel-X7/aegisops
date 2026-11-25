"""Evaluation contracts and truth-free runtime capture (A1-R1 / C1-R1 / E1).

Truth is an evaluator sidecar, never a runtime argument. No metric calculation
or comparison/report publication occurs here.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import PurePosixPath
from types import UnionType
from typing import (
    Annotated,
    Any,
    Literal,
    NoReturn,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.anomaly.models import (
    SUPPORTED_ANOMALY_SCHEMA_VERSION,
    AnomalyEvidence,
    AnomalySignal,
    CalibrationMetadata,
)
from app.correlation.blast_radius import (
    BlastRadiusContext,
    BlastRadiusPolicy,
    calculate_blast_radius,
)
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.grouping import (
    CorrelationGroupingResult,
    GroupingEdgeRejectionReason,
    GroupingSignalDisposition,
    compute_group_fingerprint,
    compute_result_fingerprint,
)
from app.correlation.handoff import Phase5InvestigationHandoff
from app.correlation.models import CorrelationEvidence, IncidentAggregate
from app.correlation.topology import TopologySnapshot, TraversalDirection
from app.telemetry.schemas import EventSeverity

SCHEMA_VERSION = "correlation-evaluation/1.0"
PROTOCOL_FINGERPRINT = (
    "23a576aec8f6665724b5b074ddf4b3c9c35aaf1c43dffbbc0c2c463c9ba80b60"
)
SOURCE_REVISION = "526f75150e22decef938eac7b59b85661ede457a"
TIMING_RULES = "One warmup and ten measured repeats per case/policy; serial isolated workers; sum projection/lineage, scoring and grouping; nearest-rank p95; retain failed/censored samples; live orchestration not measured."
TARGET_RULES = "Known-pattern exact partition proxy strictly >9/10 on eight canonical cases; live <5s NOT_MEASURED; controlled p95 <5000000000ns only with complete successful grouping coverage."
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)
NAMESPACE = uuid5(NAMESPACE_URL, "aegisops:correlation-evaluation:1.0")
MAX_INPUT_BYTES = 2 * 1024 * 1024
MAX_RAW_BYTES = 64 * 1024 * 1024
POLICY_PINS = (
    ("reference", "6c5b3957a84bc720b22052e1dbe556701d425dfee067f0cec79e7e8fa47c0052"),
    (
        "threshold_sensitivity",
        "481905696901576eb41a150b5dc3a424196d6814247dde609ca4b14fe88e4497",
    ),
)
TOPOLOGY_PINS = (
    (
        "canonical",
        "1.0.0",
        "2d9f8e9a54a8d1ce103de39be842b4fcfa8e36b32b41f4f0d7abc1820d6779dd",
    ),
    (
        "disconnected",
        "eval-disconnected/1.0",
        "36cd8c3b8ee824155f334b8dab2411a54ac769eb3110757ea72a9e819c2e7a24",
    ),
)
Token = Annotated[
    str,
    StringConstraints(
        strict=True, pattern=r"^[A-Za-z0-9_.:-]+$", min_length=1, max_length=128
    ),
]
Text = Annotated[
    str, StringConstraints(strict=True, pattern=r"\S", min_length=1, max_length=1024)
]
Hash = Annotated[
    str,
    StringConstraints(
        strict=True, pattern=r"^[0-9a-f]{64}$", min_length=64, max_length=64
    ),
]
Count = Annotated[StrictInt, Field(ge=0)]
Positive = Annotated[StrictInt, Field(gt=0)]
TruthStatus = Literal["complete", "unknown", "ambiguous"]
RunStatus = Literal[
    "SUCCESS",
    "PARTIAL_FAILURE",
    "CONTEXT_UNAVAILABLE",
    "RUNTIME_ERROR",
    "CAPACITY_REJECTED",
    "TIMEOUT",
]
Stage = Literal[
    "projection",
    "scoring",
    "grouping",
    "aggregate",
    "blast",
    "handoff",
    "load",
    "worker_wall",
]
Stamp = Annotated[str, StringConstraints(strict=True, min_length=27, max_length=27)]


class EvaluationContractError(ValueError):
    """Evaluation inputs cannot be represented by the frozen contract."""


def _plain(value: Any, ancestors: frozenset[int] = frozenset(), depth: int = 0) -> Any:
    """Expose real instance fields before serialization can normalize bad types."""
    if depth > 32:
        raise ValueError("JSON depth exceeds 32")
    if isinstance(value, (BaseModel, dict, list, tuple)):
        if id(value) in ancestors:
            raise ValueError("Cyclic input")
        ancestors = ancestors | {id(value)}
        if isinstance(value, BaseModel):
            fields = dict(value.__dict__)
            fields.update(value.__pydantic_extra__ or {})
            if set(fields) - set(type(value).model_fields):
                raise ValueError("Undeclared instance fields")
            return _plain(fields, ancestors, depth + 1)
        if isinstance(value, dict):
            if any(type(key) is not str for key in value):  # noqa: E721 - exact wire-key type
                raise ValueError("Object keys must be strings")
            return {k: _plain(v, ancestors, depth + 1) for k, v in value.items()}
        items = [_plain(v, ancestors, depth + 1) for v in value]
        return tuple(items) if isinstance(value, tuple) else items
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite number")
    if value is not None and not isinstance(
        value, (str, int, float, bool, UUID, datetime, Enum)
    ):
        raise ValueError("Unsupported value type")
    return value


def _check_fields(value: Any, annotation: Any) -> None:
    """Reject nested unknown fields and numeric coercion in legacy payloads too."""
    if value is None:
        return
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Annotated:
        return _check_fields(value, args[0])
    if origin in (Union, UnionType):
        branches = [a for a in args if a is not type(None)]
        if len(branches) == 1:
            _check_fields(value, branches[0])
        return
    if origin is Literal:
        if value not in args or not any(type(value) is type(a) for a in args):
            raise ValueError("Literal has the wrong value or type")
    elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
        if isinstance(value, dict):
            if set(value) - set(annotation.model_fields):
                raise ValueError("Unknown nested model field")
            for key, val in value.items():
                _check_fields(val, annotation.model_fields[key].annotation)
    elif origin in (list, tuple) and isinstance(value, (tuple, list)) and args:
        for i, item in enumerate(value):
            _check_fields(
                item,
                args[0]
                if len(args) == 1 or args[-1] is Ellipsis
                else args[min(i, len(args) - 1)],
            )
    elif origin is dict and isinstance(value, dict) and len(args) == 2:
        for item in value.values():
            _check_fields(item, args[1])
    elif annotation is int and type(value) is not int:  # noqa: E721 - bool is not a count
        raise ValueError("Integer required; bool/float/coercion forbidden")
    elif annotation is float and type(value) not in (int, float):
        raise ValueError("Finite number required")
    elif annotation is bool and type(value) is not bool:  # noqa: E721 - no coercion
        raise ValueError("Boolean required")


def _unique(items: Any, key: Any = lambda x: x) -> None:
    keys = [key(item) for item in items]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate stable key")


def _stamp(text: str) -> str:
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", text):
        raise ValueError("Exact UTC timestamp required")
    datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ")
    return text


def _json_native(value: Any, annotation: Any) -> Any:
    """Restore typed containers after a model-before validator leaves JSON mode.

    Only representation types (arrays/UUIDs/enums/dates) are reconstructed;
    numeric types are checked before this step and are never coerced here.
    """
    if value is None:
        return None
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Annotated:
        return _json_native(value, args[0])
    if origin in (Union, UnionType):
        branches = [a for a in args if a is not type(None)]
        return _json_native(value, branches[0]) if len(branches) == 1 else value
    if (
        isinstance(annotation, type)
        and issubclass(annotation, BaseModel)
        and isinstance(value, dict)
    ):
        if not issubclass(annotation, EvaluationModel):
            # Native validators distinguish JSON dictionaries from Python instances
            # (notably UUID-bearing topology references). Preserve that boundary.
            return annotation.model_validate_json(
                json.dumps(
                    value,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            )
        return {
            k: _json_native(v, annotation.model_fields[k].annotation)
            for k, v in value.items()
        }
    if origin in (tuple, list) and isinstance(value, (list, tuple)):
        parts = [
            _json_native(
                v,
                args[0]
                if len(args) == 1 or args[-1] is Ellipsis
                else args[min(i, len(args) - 1)],
            )
            for i, v in enumerate(value)
        ]
        return tuple(parts) if origin is tuple else parts
    if origin is dict and isinstance(value, dict):
        return {k: _json_native(v, args[1]) for k, v in value.items()}
    if annotation is UUID and isinstance(value, str):
        parsed = UUID(value)
        if str(parsed) != value:
            raise ValueError("Canonical UUID text required")
        return parsed
    if annotation is datetime and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation(value)
    return value


class EvaluationModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        revalidate_instances="always",
        allow_inf_nan=False,
        hide_input_in_errors=True,
    )

    @model_validator(mode="before")
    @classmethod
    def validate_real_fields(cls, value: Any, info: ValidationInfo) -> Any:
        plain = _plain(value)
        data = plain if info.mode == "python" else value
        _check_fields(data, cls)
        return _json_native(data, cls) if info.mode == "json" else data


class RuntimeUnit(EvaluationModel):
    unit: Token
    service: Annotated[
        str, StringConstraints(min_length=1, max_length=128, strip_whitespace=False)
    ]
    metric: Token
    observed_value: float
    event_offset_seconds: Count

    @field_validator("service")
    @classmethod
    def exact_service(cls, value: str) -> str:
        if value != value.strip() or any(ord(c) < 32 for c in value):
            raise ValueError("Exact nonblank service required")
        return value


class Delivery(EvaluationModel):
    delivery: Token
    unit: Token
    arrival_offset_seconds: Count


class Decision(EvaluationModel):
    decision: Token
    arrival_cutoff_seconds: Count
    watermark_offset_seconds: Count


class RuntimeCase(EvaluationModel):
    case_id: Annotated[
        str, StringConstraints(pattern=r"^case-[0-9]{3}$", min_length=8, max_length=8)
    ]
    topology_key: Token
    units: tuple[RuntimeUnit, ...] = Field(max_length=91)
    deliveries: tuple[Delivery, ...] = Field(max_length=182)
    decisions: tuple[Decision, ...] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def references(self) -> RuntimeCase:
        if int(self.case_id[5:]) == 0:
            raise ValueError("Case ordinal starts at one")
        _unique(self.units, lambda u: u.unit)
        _unique(self.deliveries, lambda d: d.delivery)
        _unique(self.decisions, lambda q: q.decision)
        events = {u.unit: u.event_offset_seconds for u in self.units}
        if {d.unit for d in self.deliveries} != set(events):
            raise ValueError(
                "Every logical unit must be delivered; no unknown delivery"
            )
        if any(d.arrival_offset_seconds < events[d.unit] for d in self.deliveries):
            raise ValueError("Arrival cannot precede event time")
        if list(self.deliveries) != sorted(
            self.deliveries, key=lambda d: (d.arrival_offset_seconds, d.delivery)
        ):
            raise ValueError("Deliveries must be in schedule order")
        for earlier, later in zip(self.decisions, self.decisions[1:]):
            if (
                later.arrival_cutoff_seconds <= earlier.arrival_cutoff_seconds
                or later.watermark_offset_seconds < earlier.watermark_offset_seconds
            ):
                raise ValueError(
                    "Decisions must advance arrival cutoffs and watermarks"
                )
        if self.decisions[-1].arrival_cutoff_seconds < max(
            (d.arrival_offset_seconds for d in self.deliveries), default=0
        ):
            raise ValueError("Final horizon omits a delivery")
        # Check datetime representability without consulting a wall clock.
        for seconds in [
            *(u.event_offset_seconds for u in self.units),
            *(q.watermark_offset_seconds for q in self.decisions),
        ]:
            try:
                EPOCH + timedelta(seconds=seconds)
            except OverflowError as exc:
                raise ValueError("Schedule timestamp overflow") from exc
        return self


class TruthGroup(EvaluationModel):
    group: Token
    members: tuple[Token, ...] = Field(min_length=1, max_length=91)

    @model_validator(mode="after")
    def unique_members(self) -> TruthGroup:
        _unique(self.members)
        return self


class BlastTruth(EvaluationModel):
    group: Token
    services: tuple[Token, ...] = Field(min_length=1, max_length=64)
    topology_fingerprint: Hash
    direction: Literal["both"]
    max_hops: Literal[3]
    annotation_source: Text

    @model_validator(mode="after")
    def canonical_services(self) -> BlastTruth:
        if tuple(sorted(set(self.services))) != self.services:
            raise ValueError("Sorted unique exposure services required")
        return self


class TruthCase(EvaluationModel):
    case_id: Annotated[
        str, StringConstraints(pattern=r"^case-[0-9]{3}$", min_length=8, max_length=8)
    ]
    truth_status: TruthStatus
    universe: tuple[Token, ...] = Field(max_length=91)
    truth_groups: tuple[TruthGroup, ...] = Field(max_length=8)
    background_units: tuple[Token, ...] = Field(max_length=91)
    blast_truth: tuple[BlastTruth, ...] | None = Field(max_length=8)
    blast_unavailable_reason: Token | None

    @model_validator(mode="after")
    def partition(self) -> TruthCase:
        _unique(self.universe)
        _unique(self.truth_groups, lambda g: g.group)
        members = [u for g in self.truth_groups for u in g.members] + list(
            self.background_units
        )
        _unique(members)
        if not set(members) <= set(self.universe):
            raise ValueError("Truth references unknown units")
        if self.truth_status == "complete" and set(members) != set(self.universe):
            raise ValueError("Complete truth must partition the entire universe")
        if self.blast_truth is None:
            if self.blast_unavailable_reason is None:
                raise ValueError("Unsupported exposure truth needs a reason")
        else:
            if not self.blast_truth or self.blast_unavailable_reason is not None:
                raise ValueError(
                    "Exposure annotations or an unavailable reason required"
                )
            _unique(self.blast_truth, lambda b: b.group)
            if not {b.group for b in self.blast_truth} <= {
                g.group for g in self.truth_groups
            }:
                raise ValueError("Exposure truth references an unknown group")
        return self


class RuntimeInputs(EvaluationModel):
    schema_version: Literal["correlation-evaluation/1.0"]
    cases: tuple[RuntimeCase, ...] = Field(min_length=16, max_length=16)

    @model_validator(mode="after")
    def case_order(self) -> RuntimeInputs:
        if tuple(c.case_id for c in self.cases) != tuple(
            f"case-{i:03}" for i in range(1, 17)
        ):
            raise ValueError("Exact ordered 16-case population required")
        return self


class TruthInputs(EvaluationModel):
    schema_version: Literal["correlation-evaluation/1.0"]
    cases: tuple[TruthCase, ...] = Field(min_length=16, max_length=16)

    @model_validator(mode="after")
    def case_order(self) -> TruthInputs:
        if tuple(c.case_id for c in self.cases) != tuple(
            f"case-{i:03}" for i in range(1, 17)
        ):
            raise ValueError("Exact ordered truth population required")
        return self


class PortableIdentity(EvaluationModel):
    path: Text
    bytes: Count
    sha256: Hash

    @field_validator("path")
    @classmethod
    def relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or path.as_posix() != value
            or any(p in (".", "..") for p in value.split("/"))
            or any(c in value for c in "\\:\x00\r\n")
        ):
            raise ValueError("Safe exact relative path required")
        return value


class EvaluationPolicy(EvaluationModel):
    name: Token
    config: CorrelationConfig
    configuration_fingerprint: Hash

    @model_validator(mode="after")
    def fingerprint(self) -> EvaluationPolicy:
        if (
            compute_correlation_config_fingerprint(self.config)
            != self.configuration_fingerprint
        ):
            raise ValueError("Policy fingerprint mismatch")
        return self


class TopologyPin(EvaluationModel):
    topology_version: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    snapshot_fingerprint: Hash


class EvaluationTopology(EvaluationModel):
    name: Token
    snapshot: TopologySnapshot
    pin: TopologyPin

    @model_validator(mode="after")
    def independent_pin(self) -> EvaluationTopology:
        if (
            self.snapshot.version.topology_version != self.pin.topology_version
            or self.snapshot.snapshot_fingerprint != self.pin.snapshot_fingerprint
        ):
            raise ValueError("Topology pin mismatch")
        if len(self.snapshot.nodes) > 64 or len(self.snapshot.edges) > 128:
            raise ValueError("Evaluation topology capacity exceeded")
        return self


class TopologyInputs(EvaluationModel):
    schema_version: Literal["correlation-evaluation/1.0"]
    topologies: tuple[EvaluationTopology, ...] = Field(min_length=2, max_length=2)


class Repetitions(EvaluationModel):
    warmups: Literal[1]
    measured: Literal[10]


class ByteCaps(EvaluationModel):
    config: Literal[2097152]
    runtime: Literal[2097152]
    truth: Literal[2097152]
    raw: Literal[67108864]
    processed_each: Literal[8388608]
    figure_each: Literal[1048576]
    report: Literal[2097152]
    manifest: Literal[1048576]
    package: Literal[100663296]


class Budgets(EvaluationModel):
    unique_units: Literal[91]
    deliveries: Literal[182]
    truth_groups: Literal[8]
    decisions: Literal[8]
    nodes: Literal[64]
    edges: Literal[128]
    pairs_per_decision: Literal[4095]
    worker_deadline_seconds: Literal[30]
    json_depth: Literal[32]
    byte_caps: ByteCaps


class EvaluationConfig(EvaluationModel):
    schema_version: Literal["correlation-evaluation/1.0"]
    protocol_fingerprint: Hash
    source_revision: Annotated[
        str, StringConstraints(pattern=r"^[0-9a-f]{40}$", min_length=40, max_length=40)
    ]
    policies: tuple[EvaluationPolicy, ...] = Field(min_length=2, max_length=2)
    topologies: tuple[EvaluationTopology, ...] = Field(min_length=2, max_length=2)
    cases: tuple[Token, ...] = Field(min_length=16, max_length=16)
    seed: Literal[42]
    epoch: Literal["2026-01-01T00:00:00.000000Z"]
    budgets: Budgets
    repetitions: Repetitions
    timing_rules: Text
    metric_version: Literal["correlation-metrics/1.0"]
    target_rules: Text
    source_identities: tuple[PortableIdentity, ...]
    runtime_identity: PortableIdentity
    truth_identity: PortableIdentity

    @model_validator(mode="after")
    def approved_population(self) -> EvaluationConfig:
        if (
            self.protocol_fingerprint != PROTOCOL_FINGERPRINT
            or self.source_revision != SOURCE_REVISION
            or self.timing_rules != TIMING_RULES
            or self.target_rules != TARGET_RULES
        ):
            raise ValueError(
                "Approved protocol/source/timing/target decisions required"
            )
        if (
            tuple((p.name, p.configuration_fingerprint) for p in self.policies)
            != POLICY_PINS
        ):
            raise ValueError("Approved policy bytes/order required")
        if (
            tuple(
                (t.name, t.pin.topology_version, t.pin.snapshot_fingerprint)
                for t in self.topologies
            )
            != TOPOLOGY_PINS
        ):
            raise ValueError("Approved topology pins/order required")
        if self.cases != tuple(f"case-{i:03}" for i in range(1, 17)):
            raise ValueError("Exact 16-case population required")
        _unique(self.source_identities, lambda p: p.path)
        if not self.source_identities:
            raise ValueError("Source identities required")
        return self


def project_runtime_case(case: RuntimeCase) -> tuple[AnomalyCorrelationInput, ...]:
    """Project only observable fields; truth has no parameter or metadata route."""
    case = RuntimeCase.model_validate(_plain(case))
    ordinal = int(case.case_id[5:])
    run = uuid5(NAMESPACE, f"case:{ordinal}:seed:42")
    results = []
    for index, unit in enumerate(case.units, start=1):
        event_time = EPOCH + timedelta(seconds=unit.event_offset_seconds)
        signal = AnomalySignal(
            signal_id=uuid5(run, f"unit:{index}"),
            event_time=event_time,
            service=unit.service,
            metric_or_feature=unit.metric,
            model_name="controlled-observation",
            model_version="1.0.0",
            anomaly_score=0.95,
            severity=EventSeverity.WARNING,
            evidence=[
                AnomalyEvidence(
                    evidence_id=uuid5(run, f"evidence:{index}"),
                    evidence_type="metric_deviation",
                    metric_or_feature=unit.metric,
                    observed_value=unit.observed_value,
                    expected_value=0.0,
                    deviation=unit.observed_value,
                    details={},
                    timestamp=event_time,
                )
            ],
            threshold_or_calibration=CalibrationMetadata(
                method="fixed",
                threshold_value=0.8,
                calibration_version="evaluation/1.0",
                parameters={},
                calibrated_at=None,
            ),
            schema_version=SUPPORTED_ANOMALY_SCHEMA_VERSION,
            run_id=run,
            scenario_id="controlled-correlation",
            scenario_version="1.0.0",
            seed=42,
            reproducibility_key=f"evaluation/1.0/case/{ordinal}/seed/42",
            source_event_ids=[uuid5(run, f"event:{index}")],
            source_event_time_window=None,
            environment="evaluation",
            tenant_id=uuid5(NAMESPACE, "tenant"),
            trace_id=None,
            tags={},
        )
        results.append(
            AnomalyCorrelationInput(
                signal=signal, lineage=build_anomaly_reproducibility_lineage(signal)
            )
        )
    return tuple(results)


T = TypeVar("T", bound=EvaluationModel)


def serialize_evaluation(
    value: EvaluationModel, *, limit: int = MAX_INPUT_BYTES
) -> bytes:
    """Revalidate actual Python fields, then encode a bounded canonical payload."""
    if (
        not isinstance(value, EvaluationModel)
        or type(limit) is not int  # noqa: E721 - bool is not a byte limit
        or not 0 < limit <= MAX_RAW_BYTES
    ):
        raise EvaluationContractError(
            "Evaluation model and bounded byte limit required"
        )
    try:
        fresh = type(value).model_validate(_plain(value))
        payload = fresh.model_dump(mode="json")
        encoded = bytearray()
        for part in json.JSONEncoder(
            sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).iterencode(payload):
            data = part.encode("utf-8")
            if len(encoded) + len(data) + 1 > limit:
                raise ValueError("Evaluation byte limit exceeded")
            encoded.extend(data)
        encoded.extend(b"\n")
        return bytes(encoded)
    except (ValueError, TypeError, RecursionError) as exc:
        raise EvaluationContractError("Invalid evaluation serialization") from exc


def deserialize_evaluation(
    cls: type[T], raw: bytes, *, limit: int = MAX_INPUT_BYTES
) -> T:
    if (
        type(raw) is not bytes  # noqa: E721 - immutable wire bytes
        or type(limit) is not int  # noqa: E721 - bool is not a byte limit
        or not 0 < limit <= MAX_RAW_BYTES
        or len(raw) > limit
    ):
        raise EvaluationContractError("Bounded UTF-8 bytes required")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise ValueError("Non-finite JSON constant")

    try:
        data = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=invalid_constant,
        )
        _plain(data)
        return cls.model_validate_json(raw)
    except (ValueError, TypeError, RecursionError) as exc:
        raise EvaluationContractError("Invalid evaluation payload") from exc


def _check_identity(raw: bytes, declared: PortableIdentity) -> None:
    declared = PortableIdentity.model_validate(_plain(declared))
    if len(raw) != declared.bytes or hashlib.sha256(raw).hexdigest() != declared.sha256:
        raise EvaluationContractError("Declared artifact identity mismatch")


def load_evaluation_inputs(
    config_raw: bytes,
    runtime_raw: bytes,
    truth_raw: bytes,
    topology_raw: bytes,
    *,
    expected_config_sha256: str,
) -> tuple[EvaluationConfig, RuntimeInputs, TruthInputs]:
    """Authenticate runtime/truth bytes before parsing either sidecar."""
    if (
        type(config_raw) is not bytes  # noqa: E721 - exact immutable wire bytes
        or len(config_raw) > MAX_INPUT_BYTES
        or not isinstance(expected_config_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_config_sha256)
        or hashlib.sha256(config_raw).hexdigest() != expected_config_sha256
    ):  # noqa: E721 - exact wire bytes
        raise EvaluationContractError("Independent config identity mismatch")
    config = deserialize_evaluation(EvaluationConfig, config_raw)
    _check_identity(runtime_raw, config.runtime_identity)
    _check_identity(truth_raw, config.truth_identity)
    runtime = deserialize_evaluation(RuntimeInputs, runtime_raw)
    truth = deserialize_evaluation(TruthInputs, truth_raw)
    topology = deserialize_evaluation(TopologyInputs, topology_raw)
    if topology.topologies != config.topologies:
        raise EvaluationContractError("Topology artifact differs from pinned config")
    by_topology = {t.name: t for t in config.topologies}
    for case, sidecar in zip(runtime.cases, truth.cases):
        if (
            case.case_id != sidecar.case_id
            or tuple(u.unit for u in case.units) != sidecar.universe
            or case.topology_key not in by_topology
        ):
            raise EvaluationContractError("Case/truth/topology references disagree")
        for annotation in sidecar.blast_truth or ():
            if (
                annotation.topology_fingerprint
                != by_topology[case.topology_key].pin.snapshot_fingerprint
            ):
                raise EvaluationContractError("Exposure truth topology mismatch")
            if not set(annotation.services) <= {
                n.name for n in by_topology[case.topology_key].snapshot.nodes
            }:
                raise EvaluationContractError("Exposure truth service missing")
    return config, runtime, truth


class PlannedRun(EvaluationModel):
    run: Token
    case_id: Token
    policy: Token
    configuration_fingerprint: Hash
    repetition: Count
    warmup: bool
    source_revision: Annotated[
        str, StringConstraints(pattern=r"^[0-9a-f]{40}$", min_length=40, max_length=40)
    ]
    config_sha256: Hash
    runtime_sha256: Hash
    truth_sha256: Hash
    topology_fingerprint: Hash
    decision_ids: tuple[Token, ...] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def run_identity(self) -> PlannedRun:
        _unique(self.decision_ids)
        kind = "warmup" if self.warmup else "measured"
        if self.run != f"{self.case_id}.{self.policy}.{kind}.{self.repetition:03}":
            raise ValueError("Run token disagrees with plan fields")
        if self.repetition >= (1 if self.warmup else 10):
            raise ValueError("Repetition exceeds approved population")
        if (self.policy, self.configuration_fingerprint) not in POLICY_PINS:
            raise ValueError("Unapproved planned policy")
        return self


def plan_evaluation(
    config_raw: bytes, runtime: RuntimeInputs
) -> tuple[PlannedRun, ...]:
    """Build the complete planned manifest without executing any run."""
    config = deserialize_evaluation(EvaluationConfig, config_raw)
    runtime = RuntimeInputs.model_validate(_plain(runtime))
    _check_identity(serialize_evaluation(runtime), config.runtime_identity)
    topologies = {t.name: t for t in config.topologies}
    result = []
    for policy in config.policies:
        for case in runtime.cases:
            for warmup, count in [(True, 1), (False, 10)]:
                for repetition in range(count):
                    kind = "warmup" if warmup else "measured"
                    result.append(
                        PlannedRun(
                            run=f"{case.case_id}.{policy.name}.{kind}.{repetition:03}",
                            case_id=case.case_id,
                            policy=policy.name,
                            configuration_fingerprint=policy.configuration_fingerprint,
                            repetition=repetition,
                            warmup=warmup,
                            source_revision=config.source_revision,
                            config_sha256=hashlib.sha256(config_raw).hexdigest(),
                            runtime_sha256=config.runtime_identity.sha256,
                            truth_sha256=config.truth_identity.sha256,
                            topology_fingerprint=topologies[
                                case.topology_key
                            ].pin.snapshot_fingerprint,
                            decision_ids=tuple(q.decision for q in case.decisions),
                        )
                    )
    return tuple(result)


class StageTiming(EvaluationModel):
    stage: Stage
    decision: Token | None
    group: Hash | None
    elapsed_ns: Count
    status: Literal["observed", "failed", "censored"]


class SafeError(EvaluationModel):
    stage: Stage
    exception_type: Annotated[str, StringConstraints(min_length=1, max_length=256)]
    message: Text


def _observable_input(value: AnomalyCorrelationInput) -> None:
    signal = value.signal
    if value.lineage != build_anomaly_reproducibility_lineage(signal):
        raise ValueError("Runtime lineage mismatch")
    if (
        signal.tags
        or signal.trace_id is not None
        or signal.source_event_time_window is not None
    ):
        raise ValueError("Runtime projection metadata must be empty")
    if (
        signal.model_name,
        signal.model_version,
        signal.scenario_id,
        signal.scenario_version,
        signal.seed,
        signal.environment,
        signal.anomaly_score,
        signal.severity,
    ) != (
        "controlled-observation",
        "1.0.0",
        "controlled-correlation",
        "1.0.0",
        42,
        "evaluation",
        0.95,
        EventSeverity.WARNING,
    ):
        raise ValueError("Runtime projection constants disagree")
    calibration = signal.threshold_or_calibration
    if (
        calibration.method,
        calibration.threshold_value,
        calibration.calibration_version,
        calibration.parameters,
        calibration.calibrated_at,
    ) != ("fixed", 0.8, "evaluation/1.0", {}, None):
        raise ValueError("Runtime calibration projection disagrees")
    if len(signal.evidence) != 1 or len(signal.source_event_ids) != 1:
        raise ValueError("Runtime evidence projection disagrees")
    e = signal.evidence[0]
    if (
        e.details
        or e.evidence_type != "metric_deviation"
        or e.metric_or_feature != signal.metric_or_feature
        or e.timestamp != signal.event_time
        or e.expected_value != 0.0
        or e.deviation != e.observed_value
    ):
        raise ValueError("Unallowlisted runtime evidence")


class CapturedArguments(EvaluationModel):
    decision: Token
    runtime_inputs: tuple[AnomalyCorrelationInput, ...] = Field(max_length=182)
    pair_order: tuple[tuple[UUID, UUID], ...] = Field(max_length=4095)
    configuration_fingerprint: Hash
    topology_fingerprint: Hash
    watermark: Stamp
    argument_fingerprint: Hash

    _watermark = field_validator("watermark")(_stamp)

    @model_validator(mode="after")
    def projection(self) -> CapturedArguments:
        for value in self.runtime_inputs:
            _observable_input(value)
        ids = {i.signal.signal_id for i in self.runtime_inputs}
        if len(ids) > 91:
            raise ValueError("Logical-unit capacity exceeded")
        expected = tuple(
            (a, b)
            for i, a in enumerate(sorted(ids, key=lambda u: u.bytes))
            for b in sorted(ids, key=lambda u: u.bytes)[i + 1 :]
        )
        if self.pair_order != expected:
            raise ValueError("All unordered unique pairs in UUID order required")
        contexts = {
            (
                s.signal.run_id,
                s.signal.scenario_id,
                s.signal.scenario_version,
                s.signal.seed,
                s.signal.reproducibility_key,
                s.signal.tenant_id,
                s.signal.environment,
            )
            for s in self.runtime_inputs
        }
        if len(contexts) > 1:
            raise ValueError("Case provenance must be common to all inputs")
        payload = self.model_dump(mode="json", exclude={"argument_fingerprint"})
        if (
            hashlib.sha256(
                json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            != self.argument_fingerprint
        ):
            raise ValueError("Argument fingerprint mismatch")
        return self


def _raw_error(location: tuple[str | int, ...], message: str) -> NoReturn:
    """Keep cross-record rejections attached to their actual offending field."""
    raise ValidationError.from_exception_data(
        "RawContract",
        [
            dict(
                type="value_error",
                loc=location,
                input=None,
                ctx={"error": ValueError(message)},
            )
        ],
    )


def _bind_grouping_result(
    arguments: CapturedArguments,
    evidence: tuple[CorrelationEvidence, ...],
    result: CorrelationGroupingResult,
) -> None:
    # Both approved config fingerprints use policy v1.0, 30s lateness, delay/ignore.
    if arguments.configuration_fingerprint not in dict(POLICY_PINS).values():
        _raw_error(
            ("arguments", "configuration_fingerprint"),
            "Unknown approved policy fingerprint",
        )
    if result.policy_version != "v1.0":
        _raw_error(
            ("grouping_result", "policy_version"),
            "Result policy differs from approved configuration",
        )
    watermark = datetime.strptime(arguments.watermark, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    )
    if result.event_time_watermark != watermark:
        _raw_error(
            ("grouping_result", "event_time_watermark"),
            "Result watermark differs from recorded arguments",
        )
    inputs: dict[UUID, AnomalyCorrelationInput] = {}
    counts: dict[UUID, int] = {}
    for value in arguments.runtime_inputs:
        uid = value.signal.signal_id
        if uid in inputs and value != inputs[uid]:
            _raw_error(
                ("arguments", "runtime_inputs"),
                "Successful grouping cannot contain conflicting duplicate identities",
            )
        inputs[uid] = value
        counts[uid] = counts.get(uid, 0) + 1
    records = {r.signal_id: r for r in result.signal_records}
    if set(records) != set(inputs):
        _raw_error(
            ("grouping_result", "signal_records"),
            "Result must account for every supplied logical signal exactly once",
        )
    for index, record in enumerate(result.signal_records):
        value = inputs[record.signal_id]
        late = value.signal.event_time < watermark - timedelta(seconds=30)
        disposition = (
            GroupingSignalDisposition.LATE_DELAYED
            if late
            else GroupingSignalDisposition.DUPLICATE_IGNORED
            if counts[record.signal_id] > 1
            else GroupingSignalDisposition.GROUPED
        )
        for name, expected in dict(
            semantic_fingerprint=value.semantic_fingerprint,
            duplicate_count=counts[record.signal_id],
            late=late,
            included=not late,
            disposition=disposition,
        ).items():
            if getattr(record, name) != expected:
                _raw_error(
                    ("grouping_result", "signal_records", index, name),
                    "Signal disposition/identity disagrees with captured inputs and policy",
                )
    source = {(ev.left_signal_id, ev.right_signal_id): ev for ev in evidence}
    for index, ev in enumerate(evidence):
        if ev.policy_version != result.policy_version:
            _raw_error(
                ("pair_evidence", index, "policy_version"),
                "Scorer evidence uses a different policy",
            )
    for index, group in enumerate(result.groups):
        for member_index, member in enumerate(group.members):
            value = inputs[member.signal_id]
            for name, expected in dict(
                semantic_fingerprint=value.semantic_fingerprint,
                service=value.signal.service,
                event_time=value.signal.event_time,
            ).items():
                if getattr(member, name) != expected:
                    _raw_error(
                        (
                            "grouping_result",
                            "groups",
                            index,
                            "members",
                            member_index,
                            name,
                        ),
                        "Group member differs from captured signal",
                    )
        members = {m.signal_id for m in group.members}
        expected_evidence = {
            pair: ev for pair, ev in source.items() if set(pair) <= members
        }
        actual_evidence = {
            (ev.left_signal_id, ev.right_signal_id): ev
            for ev in group.accepted_evidence
        }
        if actual_evidence != expected_evidence or len(actual_evidence) != len(
            group.accepted_evidence
        ):
            _raw_error(
                ("grouping_result", "groups", index, "accepted_evidence"),
                "Group evidence differs from recorded scorer evidence",
            )
        if group.group_fingerprint != compute_group_fingerprint(group):
            _raw_error(
                ("grouping_result", "groups", index, "group_fingerprint"),
                "Group fingerprint must match the actual payload",
            )
    expected_rejections = {
        pair
        for pair, ev in source.items()
        if (
            records[pair[0]].included
            and records[pair[1]].included
            and records[pair[0]].group_fingerprint != records[pair[1]].group_fingerprint
        )
        or (
            ev.accepted
            and (not records[pair[0]].included or not records[pair[1]].included)
        )
    }
    actual_rejections = {
        (r.left_signal_id, r.right_signal_id) for r in result.rejected_edges
    }
    # C1 permits omitted rejected-scorer pairs whose endpoints were excluded.
    # Native grouping additionally emits INELIGIBLE_SIGNAL for those pairs;
    # preserve these real diagnostics without invalidating C1's representation.
    allowed_rejections = expected_rejections | {
        pair
        for pair in source
        if not records[pair[0]].included or not records[pair[1]].included
    }
    if not expected_rejections <= actual_rejections <= allowed_rejections:
        _raw_error(
            ("grouping_result", "rejected_edges"),
            "Rejected pairs must match native eligible/ineligible evidence accounting",
        )
    for index, rejected in enumerate(result.rejected_edges):
        if (
            not records[rejected.left_signal_id].included
            or not records[rejected.right_signal_id].included
        ) and rejected.reason != GroupingEdgeRejectionReason.INELIGIBLE_SIGNAL:
            _raw_error(
                ("grouping_result", "rejected_edges", index, "reason"),
                "Accepted evidence with excluded endpoints requires the native ineligible reason",
            )
        if (
            rejected.source_evidence
            != source[(rejected.left_signal_id, rejected.right_signal_id)]
        ):
            _raw_error(
                ("grouping_result", "rejected_edges", index, "source_evidence"),
                "Rejected edge differs from recorded scorer evidence",
            )
    if result.result_fingerprint != compute_result_fingerprint(result):
        _raw_error(
            ("grouping_result", "result_fingerprint"),
            "Result fingerprint must match the actual payload",
        )


class DecisionOutcome(EvaluationModel):
    decision: Token
    arguments: CapturedArguments
    pair_evidence: tuple[CorrelationEvidence, ...] = Field(max_length=4095)
    grouping_result: CorrelationGroupingResult | None
    status: Literal["success", "failed"]
    errors: tuple[SafeError, ...] = Field(max_length=16)
    timings: tuple[StageTiming, ...] = Field(max_length=256)

    @model_validator(mode="after")
    def consistency(self) -> DecisionOutcome:
        if self.decision != self.arguments.decision:
            raise ValueError("Decision/argument identity mismatch")
        if (self.status == "success") != (self.grouping_result is not None) or (
            self.status == "failed" and not self.errors
        ):
            raise ValueError("Decision availability/status mismatch")
        if self.status == "success" and self.errors:
            _raw_error(
                ("errors",), "A successful decision cannot contain native errors"
            )
        pairs = tuple((e.left_signal_id, e.right_signal_id) for e in self.pair_evidence)
        _unique(pairs)
        if self.status == "success" and pairs != self.arguments.pair_order:
            raise ValueError("Missing or reordered scorer evidence")
        if self.grouping_result is not None and (
            self.grouping_result.configuration_fingerprint
            != self.arguments.configuration_fingerprint
            or self.grouping_result.topology_snapshot_fingerprint
            != self.arguments.topology_fingerprint
        ):
            raise ValueError("Grouping result pin mismatch")
        if self.grouping_result is not None:
            _bind_grouping_result(
                self.arguments, self.pair_evidence, self.grouping_result
            )
        if self.status == "success":
            for stage in ("projection", "scoring", "grouping"):
                observed = [
                    t
                    for t in self.timings
                    if t.stage == stage
                    and t.decision == self.decision
                    and t.status == "observed"
                ]
                if len(observed) != 1:
                    raise ValueError(
                        "Successful decision needs one real timing for each primary stage"
                    )
        return self


class EnrichmentOutcome(EvaluationModel):
    group: Hash
    aggregate: IncidentAggregate | None
    blast_context: BlastRadiusContext | None
    handoff: Phase5InvestigationHandoff | None
    status: Literal["success", "failed"]
    errors: tuple[SafeError, ...] = Field(max_length=16)
    timings: tuple[StageTiming, ...] = Field(max_length=256)

    @model_validator(mode="after")
    def availability(self) -> EnrichmentOutcome:
        if self.status == "success" and (
            self.aggregate is None
            or self.blast_context is None
            or self.handoff is None
            or self.errors
        ):
            raise ValueError("Successful enrichment must be complete")
        if self.status == "failed" and not self.errors:
            raise ValueError("Failed enrichment needs diagnostics")
        if self.blast_context is not None and self.aggregate is None:
            _raw_error(
                ("blast_context",), "Topology context requires its source aggregate"
            )
        if self.handoff is not None:
            if self.aggregate is None or self.handoff.incident != self.aggregate:
                _raw_error(
                    ("handoff", "incident"),
                    "Handoff must contain this enrichment's aggregate",
                )
            if (
                self.blast_context is None
                or self.handoff.topology_context != self.blast_context
            ):
                _raw_error(
                    ("handoff", "topology_context"),
                    "Handoff must contain this enrichment's topology context",
                )
        return self


class TimeoutEvidence(EvaluationModel):
    deadline_ns: Positive
    elapsed_ns: Count
    last_stage: Token
    pid: Positive
    terminated: bool

    @model_validator(mode="after")
    def terminated_child(self) -> TimeoutEvidence:
        if not self.terminated or self.elapsed_ns < self.deadline_ns:
            raise ValueError("Timeout needs elapsed deadline and terminated child")
        return self


class DependencyVersion(EvaluationModel):
    name: Token
    version: Annotated[str, StringConstraints(min_length=1, max_length=128)]


class Environment(EvaluationModel):
    python_version: Text
    platform: Text
    processor: Text
    dependencies: tuple[DependencyVersion, ...]
    timer: Literal["perf_counter_ns"]
    execution_host: Token

    @model_validator(mode="after")
    def versions(self) -> Environment:
        if tuple(d.name for d in self.dependencies) != tuple(
            sorted({d.name for d in self.dependencies})
        ):
            raise ValueError("Dependency names must be sorted and unique")
        return self


class RawRun(EvaluationModel):
    plan: PlannedRun
    utc_start: Stamp
    utc_end: Stamp
    elapsed_ns: Count
    terminal_status: RunStatus
    final_grouping_available: bool
    decisions: tuple[DecisionOutcome, ...] = Field(max_length=8)
    enrichments: tuple[EnrichmentOutcome, ...] = Field(max_length=91)
    timings: tuple[StageTiming, ...] = Field(max_length=2048)
    errors: tuple[SafeError, ...] = Field(max_length=64)
    timeout: TimeoutEvidence | None
    environment: Environment
    sources_before: tuple[PortableIdentity, ...] = Field(min_length=1)
    sources_after: tuple[PortableIdentity, ...] = Field(min_length=1)

    _timestamps = field_validator("utc_start", "utc_end")(_stamp)

    @model_validator(mode="after")
    def accounting(self) -> RawRun:
        if self.utc_end < self.utc_start:
            raise ValueError("UTC boundaries reversed")
        wall = [t for t in self.timings if t.stage == "worker_wall"]
        if len(wall) != 1 or wall[0].elapsed_ns != self.elapsed_ns:
            raise ValueError("Raw run needs its observed worker-wall interval")
        if self.timeout is not None and self.timeout.elapsed_ns > self.elapsed_ns:
            _raw_error(
                ("timeout", "elapsed_ns"),
                "Timeout interval exceeds observed worker wall time",
            )
        if self.sources_before != self.sources_after:
            raise ValueError("Source mutated during run")
        _unique(self.sources_before, lambda p: p.path)
        _unique(self.decisions, lambda d: d.decision)
        _unique(self.enrichments, lambda e: e.group)
        ids = tuple(d.decision for d in self.decisions)
        if ids != self.plan.decision_ids[: len(ids)]:
            raise ValueError("Unexpected decision sequence")
        final = bool(
            self.decisions
            and self.decisions[-1].decision == self.plan.decision_ids[-1]
            and self.decisions[-1].grouping_result is not None
        )
        if final != self.final_grouping_available:
            raise ValueError("Final grouping availability must be observed")
        final_result = self.decisions[-1].grouping_result if final else None
        expected_groups = (
            {g.group_fingerprint: g for g in final_result.groups}
            if final_result
            else {}
        )
        actual_groups = {item.group for item in self.enrichments}
        if not actual_groups <= set(expected_groups) or (
            self.timeout is None and actual_groups != set(expected_groups)
        ):
            _raw_error(
                ("enrichments",),
                "Every final group needs exactly one enrichment outcome; only explicit timeout may leave censored groups",
            )
        final_inputs = (
            {
                i.signal.signal_id: i.signal
                for i in self.decisions[-1].arguments.runtime_inputs
            }
            if final
            else {}
        )
        for index, item in enumerate(self.enrichments):
            group = expected_groups[item.group]
            expected_members = {member.signal_id: member for member in group.members}
            if item.aggregate is not None:
                references = {
                    ref.signal_id: ref for ref in item.aggregate.signal_references
                }
                if set(references) != set(expected_members):
                    _raw_error(
                        ("enrichments", index, "aggregate", "signal_references"),
                        "Aggregate membership differs from its final group",
                    )
                for uid, ref in references.items():
                    signal = final_inputs[uid]
                    shared_fields = (
                        set(type(ref).model_fields) & set(type(signal).model_fields)
                    ) - {"schema_version"}
                    if (
                        any(
                            getattr(ref, name) != getattr(signal, name)
                            for name in shared_fields
                        )
                        or ref.semantic_fingerprint
                        != expected_members[uid].semantic_fingerprint
                    ):
                        _raw_error(
                            ("enrichments", index, "aggregate", "signal_references"),
                            "Aggregate signal identity differs from captured input",
                        )
                aggregate_evidence = {
                    (ev.left_signal_id, ev.right_signal_id): ev
                    for ev in item.aggregate.correlation_evidence
                }
                group_evidence = {
                    (ev.left_signal_id, ev.right_signal_id): ev
                    for ev in group.accepted_evidence
                }
                if aggregate_evidence != group_evidence or len(
                    aggregate_evidence
                ) != len(item.aggregate.correlation_evidence):
                    _raw_error(
                        ("enrichments", index, "aggregate", "correlation_evidence"),
                        "Aggregate evidence differs from final group",
                    )
            if item.blast_context is not None:
                if (
                    {s.name for s in item.blast_context.member_services}
                    != {s.service for s in group.members}
                    or item.blast_context.topology_snapshot.snapshot_fingerprint
                    != self.plan.topology_fingerprint
                ):
                    _raw_error(
                        ("enrichments", index, "blast_context"),
                        "Enrichment context belongs to a different group or snapshot",
                    )
        failed = (
            bool(self.errors)
            or any(d.status == "failed" or d.errors for d in self.decisions)
            or any(e.status == "failed" for e in self.enrichments)
        )
        if self.timeout is not None:
            expected = "TIMEOUT"
        elif self.terminal_status == "CAPACITY_REJECTED":
            if self.decisions or not self.errors:
                raise ValueError("Capacity rejection must precede dispatch")
            expected = "CAPACITY_REJECTED"
        elif not final:
            if not self.errors and not any(d.errors for d in self.decisions):
                raise ValueError("Unavailable grouping needs diagnostics")
            expected = (
                "CONTEXT_UNAVAILABLE"
                if self.terminal_status == "CONTEXT_UNAVAILABLE"
                else "RUNTIME_ERROR"
            )
        else:
            expected = "PARTIAL_FAILURE" if failed else "SUCCESS"
        if self.terminal_status != expected:
            _raw_error(
                ("terminal_status",),
                "Terminal status disagrees with observed errors/outcomes",
            )
        for decision in self.decisions:
            if (
                decision.arguments.configuration_fingerprint
                != self.plan.configuration_fingerprint
                or decision.arguments.topology_fingerprint
                != self.plan.topology_fingerprint
            ):
                raise ValueError("Raw decision uses different plan pins")
        return self


class RawOutcomes(EvaluationModel):
    schema_version: Literal["correlation-evaluation/1.0"]
    config_sha256: Hash
    planned: tuple[PlannedRun, ...] = Field(min_length=352, max_length=352)
    runs: tuple[RawRun, ...] = Field(min_length=352, max_length=352)

    @model_validator(mode="after")
    def complete_population(self) -> RawOutcomes:
        _unique(self.planned, lambda p: p.run)
        if tuple(run.plan for run in self.runs) != self.planned:
            raise ValueError("Missing, repeated or reordered terminal raw record")
        if any(p.config_sha256 != self.config_sha256 for p in self.planned):
            raise ValueError("Raw config identity mismatch")
        if sum(not p.warmup for p in self.planned) != 320:
            raise ValueError("Measured/warmup population mismatch")
        return self


def bind_raw_outcomes(
    value: RawOutcomes, config_raw: bytes, runtime: RuntimeInputs
) -> RawOutcomes:
    """Bind recorded identities/population to the authenticated planned inputs."""
    value = RawOutcomes.model_validate(_plain(value))
    config = deserialize_evaluation(EvaluationConfig, config_raw)
    if value.planned != plan_evaluation(config_raw, runtime):
        raise EvaluationContractError("Raw population differs from frozen plan")
    cases = {case.case_id: case for case in runtime.cases}
    topologies = {t.name: t for t in config.topologies}
    for run in value.runs:
        actual = {p.path: p for p in run.sources_before}
        if (
            run.timeout is not None
            and run.timeout.deadline_ns
            != config.budgets.worker_deadline_seconds * 1_000_000_000
        ):
            _raw_error(
                ("timeout", "deadline_ns"),
                "Timeout deadline differs from authenticated configuration",
            )
        if any(actual.get(p.path) != p for p in config.source_identities):
            raise EvaluationContractError(
                "Raw source identities omit configured sources"
            )
        case = cases[run.plan.case_id]
        if run.decisions:
            projected = dict(
                zip((u.unit for u in case.units), project_runtime_case(case))
            )
            for recorded in run.decisions:
                decision = next(
                    q for q in case.decisions if q.decision == recorded.decision
                )
                expected_inputs = tuple(
                    projected[d.unit]
                    for d in case.deliveries
                    if d.arrival_offset_seconds <= decision.arrival_cutoff_seconds
                )
                expected_watermark = (
                    EPOCH + timedelta(seconds=decision.watermark_offset_seconds)
                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                if (
                    recorded.arguments.runtime_inputs != expected_inputs
                    or recorded.arguments.watermark != expected_watermark
                ):
                    raise EvaluationContractError(
                        "Captured inputs/watermark differ from frozen runtime schedule"
                    )
        if run.terminal_status == "CONTEXT_UNAVAILABLE":
            nodes = {n.name for n in topologies[case.topology_key].snapshot.nodes}
            if not {unit.service for unit in case.units} - nodes:
                raise EvaluationContractError(
                    "Context failure lacks an observable missing service"
                )
        for index, item in enumerate(run.enrichments):
            if item.blast_context is not None:
                snapshot = topologies[case.topology_key].snapshot
                ref = item.blast_context.topology_snapshot
                if (
                    ref.topology_id,
                    ref.topology_version,
                    ref.snapshot_fingerprint,
                ) != (
                    snapshot.version.topology_id,
                    snapshot.version.topology_version,
                    snapshot.snapshot_fingerprint,
                ):
                    _raw_error(
                        ("enrichments", index, "blast_context", "topology_snapshot"),
                        "Context differs from independently pinned topology",
                    )
                final_result = run.decisions[-1].grouping_result
                if final_result is None or item.aggregate is None:
                    _raw_error(
                        ("enrichments", index),
                        "Context requires a final result and its source aggregate",
                    )
                as_of = final_result.event_time_watermark
                expected_context = calculate_blast_radius(
                    incident=item.aggregate,
                    snapshot=snapshot,
                    expected_topology=ref,
                    policy=BlastRadiusPolicy(
                        direction=TraversalDirection.BOTH,
                        max_hops=3,
                        max_services=64,
                        max_edges=128,
                    ),
                    as_of=as_of,
                )
                if expected_context != item.blast_context:
                    _raw_error(
                        ("enrichments", index, "blast_context"),
                        "Context differs from approved bounded calculation on pinned topology",
                    )
                if item.handoff is not None and item.handoff.generated_at != as_of:
                    _raw_error(
                        ("enrichments", index, "handoff", "generated_at"),
                        "Handoff time differs from fixed final watermark",
                    )
    return value


class RationalComponent(EvaluationModel):
    numerator: Count
    denominator: Positive


class MetricValue(EvaluationModel):
    name: Token
    numerator: Count | None
    denominator: Count | None
    rational_components: tuple[RationalComponent, ...] | None
    value: Annotated[str, StringConstraints(pattern=r"^[0-9]+\.[0-9]{12}$")] | None
    reason: Token | None
    planned: Count
    eligible: Count
    observed: Count
    failed: Count
    scope: Token

    @model_validator(mode="after")
    def availability(self) -> MetricValue:
        if (self.value is None) != (self.reason is not None):
            raise ValueError(
                "Unavailable metric needs a reason; observed value cannot have one"
            )
        if self.denominator == 0 and (
            self.value is not None or self.reason != "empty_denominator"
        ):
            raise ValueError("Zero denominator must stay undefined")
        if (
            self.value is not None
            and (self.denominator is None or self.numerator is None)
            and not self.rational_components
        ):
            raise ValueError("Observed quality needs auditable rational components")
        if (
            self.eligible > self.planned
            or self.observed > self.planned
            or self.failed > self.planned
        ):
            raise ValueError("Metric coverage exceeds planned population")
        return self


class MatchingRow(EvaluationModel):
    case_id: Token
    policy: Token
    truth_members: tuple[Token, ...]
    prediction_members: tuple[Token, ...]
    intersection: Positive
    union: Positive
    service_intersection: Count
    service_union: Positive

    @model_validator(mode="after")
    def overlap(self) -> MatchingRow:
        if self.truth_members != tuple(
            sorted(set(self.truth_members))
        ) or self.prediction_members != tuple(sorted(set(self.prediction_members))):
            raise ValueError("Sorted unique member keys required")
        if (
            self.intersection
            != len(set(self.truth_members) & set(self.prediction_members))
            or self.union != len(set(self.truth_members) | set(self.prediction_members))
            or self.service_intersection > self.service_union
        ):
            raise ValueError("Matching counts disagree with members")
        return self


class CaseMetrics(EvaluationModel):
    case_id: Token
    policy: Token
    truth_status: TruthStatus
    quality_reason: Literal["unknown_truth", "ambiguous_truth"] | None
    TP: Count | None
    FP: Count | None
    FN: Count | None
    metrics: tuple[MetricValue, ...]
    matching: tuple[MatchingRow, ...] | None
    status: RunStatus
    unassigned_units: tuple[Token, ...]
    missed_truth_groups: tuple[Token, ...] | None
    background_prediction_groups: tuple[Hash, ...] | None

    @field_validator("quality_reason")
    @classmethod
    def availability_reason(cls, value: Any, info: ValidationInfo) -> Any:
        status = info.data.get("truth_status")
        if status is not None and value != (
            None if status == "complete" else status + "_truth"
        ):
            raise ValueError("Truth availability reason mismatch")
        return value

    @field_validator(
        "TP",
        "FP",
        "FN",
        "matching",
        "missed_truth_groups",
        "background_prediction_groups",
    )
    @classmethod
    def available_fields(cls, value: Any, info: ValidationInfo) -> Any:
        status = info.data.get("truth_status")
        if status is not None and ((status == "complete") == (value is None)):
            raise ValueError("Truth-dependent field availability mismatch")
        return value

    @model_validator(mode="after")
    def quality(self) -> CaseMetrics:
        _unique(self.metrics, lambda m: m.name)
        _unique(self.unassigned_units)
        if self.truth_status != "complete":
            reason = self.truth_status + "_truth"
            for index, metric in enumerate(self.metrics):
                for field in [
                    "numerator",
                    "denominator",
                    "rational_components",
                    "value",
                ]:
                    if getattr(metric, field) is not None:
                        raise ValidationError.from_exception_data(
                            type(self).__name__,
                            [
                                dict(
                                    type="value_error",
                                    loc=("metrics", index, field),
                                    input=getattr(metric, field),
                                    ctx={
                                        "error": ValueError(
                                            "Unavailable truth must not manufacture quality"
                                        )
                                    },
                                )
                            ],
                        )
                if metric.reason != reason:
                    raise ValueError("Truth-dependent metric reason mismatch")
        return self


def bind_case_metrics(
    record: CaseMetrics, truth: TruthCase, *, final_grouping_available: bool
) -> CaseMetrics:
    """Authenticate availability against a sidecar, including known-failure counts."""
    record = CaseMetrics.model_validate(_plain(record))
    truth = TruthCase.model_validate(_plain(truth))
    if type(final_grouping_available) is not bool:  # noqa: E721 - observed boolean only
        raise EvaluationContractError("Observed final availability must be boolean")
    if record.case_id != truth.case_id or record.truth_status != truth.truth_status:
        raise EvaluationContractError(
            "truth_status/case disagrees with authenticated sidecar"
        )
    if not final_grouping_available and record.status in ("SUCCESS", "PARTIAL_FAILURE"):
        raise EvaluationContractError(
            "Successful grouping status requires an observed final result"
        )
    if final_grouping_available and record.status in (
        "RUNTIME_ERROR",
        "CONTEXT_UNAVAILABLE",
        "CAPACITY_REJECTED",
    ):
        raise EvaluationContractError(
            "Unavailable grouping status cannot have a final result"
        )
    if not set(record.unassigned_units) <= set(truth.universe):
        raise EvaluationContractError("Unknown unassigned unit")
    if truth.truth_status == "complete" and not final_grouping_available:
        missed_pairs = sum(
            len(g.members) * (len(g.members) - 1) // 2 for g in truth.truth_groups
        )
        if (record.TP, record.FP, record.FN) != (0, 0, missed_pairs):
            raise EvaluationContractError(
                "Known-failure TP/FP/FN must retain truth pairs"
            )
        if set(record.missed_truth_groups or ()) != {
            g.group for g in truth.truth_groups
        } or set(record.unassigned_units) != set(truth.universe):
            raise EvaluationContractError(
                "Known-failure ledgers must retain missed truth"
            )
    return record


class RuntimeRequest(EvaluationModel):
    """Worker wire contract: no truth sidecar, metrics or expected partition."""

    plan: PlannedRun
    case: RuntimeCase
    policy: EvaluationPolicy
    topology: EvaluationTopology
    sources: tuple[PortableIdentity, ...]
    workspace: str
    environment: Environment

    @model_validator(mode="after")
    def pins(self) -> RuntimeRequest:
        if (
            self.case.case_id != self.plan.case_id
            or self.policy.name != self.plan.policy
            or self.policy.configuration_fingerprint
            != self.plan.configuration_fingerprint
            or self.topology.name != self.case.topology_key
            or self.topology.pin.snapshot_fingerprint != self.plan.topology_fingerprint
            or tuple(d.decision for d in self.case.decisions) != self.plan.decision_ids
        ):
            raise ValueError("Worker inputs differ from the authenticated plan")
        _unique(self.sources, lambda source: source.path)
        return self


class RuntimeAttemptCase(RuntimeCase):
    """Validate an unpruned attempted case before applying dispatch capacities.

    The existing RuntimeCase remains bounded; only a rejected attempt may exceed
    its limits. The JSON byte/depth guards still apply to this input.
    """

    units: tuple[RuntimeUnit, ...]
    deliveries: tuple[Delivery, ...]
    decisions: tuple[Decision, ...] = Field(min_length=1)


def capture_runtime_attempt(
    plan: PlannedRun,
    case_raw: bytes,
    policy: EvaluationPolicy,
    topology: EvaluationTopology,
    sources: tuple[PortableIdentity, ...],
    workspace: str,
    worker_script: str,
    environment: Environment,
    *,
    capture_folder: str | None = None,
) -> tuple[RawRun, bytes, bytes, int]:
    """Retain over-capacity attempts without pruning or invoking production."""
    import time

    start = datetime.now(timezone.utc)
    tick = time.perf_counter_ns()
    case = deserialize_evaluation(RuntimeAttemptCase, case_raw)
    if (
        case.case_id != plan.case_id
        or topology.name != case.topology_key
        or policy.configuration_fingerprint != plan.configuration_fingerprint
    ):
        raise EvaluationContractError("Attempt pins differ from assigned plan")
    if (
        len(case.units) <= 91
        and len(case.deliveries) <= 182
        and len(case.decisions) <= 8
    ):
        request = RuntimeRequest(
            plan=plan,
            case=RuntimeCase.model_validate(_plain(case)),
            policy=policy,
            topology=topology,
            sources=sources,
            workspace=workspace,
            environment=environment,
        )
        return capture_runtime_run(
            request, worker_script, capture_folder=capture_folder
        )
    observed = observe_runtime_sources(workspace, sources)
    elapsed = time.perf_counter_ns() - tick
    result = RawRun(
        plan=plan,
        utc_start=start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        utc_end=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        elapsed_ns=elapsed,
        terminal_status="CAPACITY_REJECTED",
        final_grouping_available=False,
        decisions=(),
        enrichments=(),
        timings=(
            StageTiming(
                stage="worker_wall",
                decision=None,
                group=None,
                elapsed_ns=elapsed,
                status="observed",
            ),
        ),
        errors=(
            SafeError(
                stage="load",
                exception_type="RuntimeCapacityError",
                message="Attempt exceeds frozen runtime capacity; no production dispatch occurred.",
            ),
        ),
        timeout=None,
        environment=environment,
        sources_before=observed,
        sources_after=observe_runtime_sources(workspace, sources),
    )
    return result, b"", b"", 0


def observe_runtime_sources(
    workspace: str, sources: tuple[PortableIdentity, ...]
) -> tuple[PortableIdentity, ...]:
    """Reopen every declared source; do not substitute a retained hash."""
    from pathlib import Path

    root = Path(workspace).resolve(strict=True)
    result = []
    for source in sources:
        path = (root / source.path).resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file() or path.is_symlink():
            raise EvaluationContractError("Source escaped workspace or is not a file")
        raw = path.read_bytes()
        observed = PortableIdentity(
            path=source.path, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()
        )
        if observed != source:
            raise EvaluationContractError("Runtime source identity changed")
        result.append(observed)
    return tuple(result)


def runtime_environment() -> Environment:
    import platform
    import sys
    from importlib.metadata import version

    return Environment(
        python_version=platform.python_version(),
        platform=platform.platform(),
        processor=platform.processor() or platform.machine(),
        dependencies=(DependencyVersion(name="pydantic", version=version("pydantic")),),
        timer="perf_counter_ns",
        execution_host=f"{platform.node()}:{sys.platform}",
    )


def _runtime_error(stage: Stage, error: Exception) -> SafeError:
    return SafeError(
        stage=stage,
        exception_type=type(error).__name__,
        message="Production call failed; native exception type and stage are retained.",
    )


async def execute_runtime_request(request: RuntimeRequest, emit: Any) -> RawRun:
    """Execute existing pure APIs with fresh state; caller owns process isolation."""
    import time

    from app.anomaly.serialization import (
        ANOMALY_KAFKA_TOPIC,
        construct_anomaly_kafka_key,
        to_canonical_telemetry_event,
    )
    from app.correlation.aggregate import create_or_update_incident
    from app.correlation.blast_radius import calculate_blast_radius as capture_blast
    from app.correlation.consumer import (
        AnomalyIntakeProcessor,
        InMemoryAnomalyDedupeStore,
    )
    from app.correlation.grouping import group_candidate_signals
    from app.correlation.handoff import build_phase5_handoff
    from app.correlation.models import CorrelationWindowState, TopologySnapshotReference
    from app.correlation.scoring import score_candidate_pair
    from app.telemetry.transport.consumer import TelemetryEnvelope
    from app.telemetry.transport.serialization import TelemetryExecutionContext

    request = RuntimeRequest.model_validate(_plain(request))
    start = datetime.now(timezone.utc)
    tick = time.perf_counter_ns()
    sources = observe_runtime_sources(request.workspace, request.sources)
    decisions: list[DecisionOutcome] = []
    enrichments: list[EnrichmentOutcome] = []
    timings: list[StageTiming] = []

    async def accepted(value: AnomalyCorrelationInput) -> None:
        # Intake validates/deduplicates; grouping still receives every delivery.
        return None

    processor = AnomalyIntakeProcessor(
        InMemoryAnomalyDedupeStore(max_size=182), accepted
    )
    delivered: set[str] = set()

    def timed(
        stage: Stage,
        decision: str | None,
        group: str | None,
        began: int,
        failed: bool = False,
    ) -> StageTiming:
        return StageTiming(
            stage=stage,
            decision=decision,
            group=group,
            elapsed_ns=time.perf_counter_ns() - began,
            status="failed" if failed else "observed",
        )

    for scheduled in request.case.decisions:
        emit(dict(kind="stage", stage="projection", decision=scheduled.decision))
        began = time.perf_counter_ns()
        stage: Stage = "projection"
        arguments = None
        evidence: list[CorrelationEvidence] = []
        measured: list[StageTiming] = []
        try:
            projected = dict(
                zip(
                    (u.unit for u in request.case.units),
                    project_runtime_case(request.case),
                )
            )
            cumulative = []
            for offset, delivery in enumerate(request.case.deliveries):
                if delivery.arrival_offset_seconds > scheduled.arrival_cutoff_seconds:
                    continue
                value = projected[delivery.unit]
                if delivery.delivery not in delivered:
                    signal = value.signal
                    outcome = await processor.process(
                        TelemetryEnvelope(
                            event=to_canonical_telemetry_event(signal),
                            context=TelemetryExecutionContext(
                                run_id=signal.run_id,
                                scenario_id=signal.scenario_id,
                                scenario_version=signal.scenario_version,
                                seed=signal.seed,
                                reproducibility_key=signal.reproducibility_key,
                            ),
                            kafka_timestamp_ms=int(
                                signal.event_time.timestamp() * 1000
                            ),
                            topic=ANOMALY_KAFKA_TOPIC.value,
                            partition=0,
                            offset=offset,
                            key=construct_anomaly_kafka_key(signal),
                            consumed_at=EPOCH
                            + timedelta(seconds=delivery.arrival_offset_seconds),
                        )
                    )
                    if (
                        outcome.correlation_input is not None
                        and outcome.correlation_input != value
                    ):
                        raise EvaluationContractError("Intake changed projected input")
                    emit(
                        dict(
                            kind="delivery",
                            delivery=delivery.delivery,
                            disposition=outcome.disposition.value,
                            signal_id=str(signal.signal_id),
                        )
                    )
                    delivered.add(delivery.delivery)
                cumulative.append(value)
            unique = {v.signal.signal_id: v for v in cumulative}
            ids = sorted(unique, key=lambda uid: uid.bytes)
            pairs = tuple(
                (a, b) for index, a in enumerate(ids) for b in ids[index + 1 :]
            )
            body = dict(
                decision=scheduled.decision,
                runtime_inputs=[v.model_dump(mode="json") for v in cumulative],
                pair_order=[[str(a), str(b)] for a, b in pairs],
                configuration_fingerprint=request.policy.configuration_fingerprint,
                topology_fingerprint=request.topology.pin.snapshot_fingerprint,
                watermark=(
                    EPOCH + timedelta(seconds=scheduled.watermark_offset_seconds)
                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            )
            body["argument_fingerprint"] = hashlib.sha256(
                json.dumps(
                    body,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            arguments = deserialize_evaluation(
                CapturedArguments, json.dumps(body).encode()
            )
            measured.append(timed(stage, scheduled.decision, None, began))
            stage = "scoring"
            emit(dict(kind="stage", stage=stage, decision=scheduled.decision))
            began = time.perf_counter_ns()
            for a, b in pairs:
                evidence.append(
                    score_candidate_pair(
                        unique[a],
                        unique[b],
                        topology_snapshot=request.topology.snapshot,
                        config=request.policy.config,
                    )
                )
            measured.append(timed(stage, scheduled.decision, None, began))
            stage = "grouping"
            emit(dict(kind="stage", stage=stage, decision=scheduled.decision))
            began = time.perf_counter_ns()
            result = group_candidate_signals(
                cumulative,
                evidence,
                topology_snapshot=request.topology.snapshot,
                config=request.policy.config,
                event_time_watermark=EPOCH
                + timedelta(seconds=scheduled.watermark_offset_seconds),
            )
            measured.append(timed(stage, scheduled.decision, None, began))
            decision = DecisionOutcome(
                decision=scheduled.decision,
                arguments=arguments,
                pair_evidence=tuple(evidence),
                grouping_result=result,
                status="success",
                errors=(),
                timings=tuple(measured),
            )
        except Exception as error:
            measured.append(timed(stage, scheduled.decision, None, began, True))
            # Projection failures have no trustworthy capture to invent. Abort this
            # worker; the supervisor retains its completed prefix and a raw error.
            if arguments is None:
                raise
            decision = DecisionOutcome(
                decision=scheduled.decision,
                arguments=arguments,
                pair_evidence=tuple(evidence),
                grouping_result=None,
                status="failed",
                errors=(_runtime_error(stage, error),),
                timings=tuple(measured),
            )
        decisions.append(decision)
        emit(dict(kind="decision", value=decision.model_dump(mode="json")))
    final = decisions[-1].grouping_result
    if final is not None:
        inputs = {v.signal.signal_id: v for v in decisions[-1].arguments.runtime_inputs}
        as_of = final.event_time_watermark
        ref = TopologySnapshotReference(
            topology_id=request.topology.snapshot.version.topology_id,
            topology_version=request.topology.snapshot.version.topology_version,
            snapshot_fingerprint=request.topology.pin.snapshot_fingerprint,
            captured_at=EPOCH,
            provider="canonical",
        )
        policy = BlastRadiusPolicy(
            direction=TraversalDirection.BOTH,
            max_hops=3,
            max_services=64,
            max_edges=128,
        )
        for group in final.groups:
            aggregate = context = handoff = None
            measured = []
            errors: tuple[SafeError, ...] = ()
            stage = "aggregate"
            emit(dict(kind="stage", stage=stage, group=group.group_fingerprint))
            began = time.perf_counter_ns()
            try:
                aggregate = create_or_update_incident(
                    group=group,
                    signal_inputs={
                        member.signal_id: inputs[member.signal_id]
                        for member in group.members
                    },
                    processing_time=as_of,
                    desired_correlation_state=CorrelationWindowState.COLLECTING,
                    existing_aggregate=None,
                ).aggregate
                measured.append(timed(stage, None, group.group_fingerprint, began))
                stage = "blast"
                emit(dict(kind="stage", stage=stage, group=group.group_fingerprint))
                began = time.perf_counter_ns()
                context = capture_blast(
                    incident=aggregate,
                    snapshot=request.topology.snapshot,
                    expected_topology=ref,
                    policy=policy,
                    as_of=as_of,
                )
                measured.append(timed(stage, None, group.group_fingerprint, began))
                stage = "handoff"
                emit(dict(kind="stage", stage=stage, group=group.group_fingerprint))
                began = time.perf_counter_ns()
                handoff = build_phase5_handoff(
                    incident=aggregate,
                    snapshot=request.topology.snapshot,
                    expected_topology=ref,
                    policy=policy,
                    as_of=as_of,
                )
                if handoff.topology_context != context:
                    raise EvaluationContractError("Handoff context changed")
                measured.append(timed(stage, None, group.group_fingerprint, began))
            except Exception as error:
                measured.append(
                    timed(stage, None, group.group_fingerprint, began, True)
                )
                errors = (_runtime_error(stage, error),)
            item = EnrichmentOutcome(
                group=group.group_fingerprint,
                aggregate=aggregate,
                blast_context=context,
                handoff=handoff,
                status="failed" if errors else "success",
                errors=errors,
                timings=tuple(measured),
            )
            enrichments.append(item)
            emit(dict(kind="enrichment", value=item.model_dump(mode="json")))
    failed = any(d.status == "failed" for d in decisions) or any(
        item.status == "failed" for item in enrichments
    )
    missing = {u.service for u in request.case.units} - {
        n.name for n in request.topology.snapshot.nodes
    }
    status: RunStatus = (
        ("CONTEXT_UNAVAILABLE" if missing else "RUNTIME_ERROR")
        if final is None
        else ("PARTIAL_FAILURE" if failed else "SUCCESS")
    )
    elapsed = time.perf_counter_ns() - tick
    timings.append(
        StageTiming(
            stage="worker_wall",
            decision=None,
            group=None,
            elapsed_ns=elapsed,
            status="observed",
        )
    )
    return RawRun(
        plan=request.plan,
        utc_start=start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        utc_end=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        elapsed_ns=elapsed,
        terminal_status=status,
        final_grouping_available=final is not None,
        decisions=tuple(decisions),
        enrichments=tuple(enrichments),
        timings=tuple(timings),
        errors=(),
        timeout=None,
        environment=request.environment,
        sources_before=sources,
        sources_after=observe_runtime_sources(request.workspace, request.sources),
    )


def capture_runtime_run(
    request: RuntimeRequest,
    worker_script: str,
    *,
    deadline_seconds: int = 30,
    capture_folder: str | None = None,
) -> tuple[RawRun, bytes, bytes, int]:
    """One fresh child per repetition, actual pipes/timeout/PID, no truth loader."""
    import subprocess
    import sys
    import tempfile
    import time

    if type(deadline_seconds) is not int or deadline_seconds != 30:  # noqa: E721 - bool is not a deadline
        raise EvaluationContractError("Frozen worker deadline is 30 seconds")
    request = RuntimeRequest.model_validate(_plain(request))
    sources = observe_runtime_sources(request.workspace, request.sources)
    start = datetime.now(timezone.utc)
    tick = time.perf_counter_ns()
    # On Windows communicate(input=...) may block writing stdin before its
    # timeout starts if a child never reads. A finite seekable input avoids that
    # unbounded pipe write while stdout/stderr remain captured as raw pipes.
    with tempfile.TemporaryFile() as worker_input:
        worker_input.write(serialize_evaluation(request))
        worker_input.seek(0)
        process = subprocess.Popen(
            [sys.executable, "-B", worker_script, "--worker"],
            stdin=worker_input,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    timed_out = False
    try:
        out, err = process.communicate(
            timeout=max(
                0.001,
                deadline_seconds - (time.perf_counter_ns() - tick) / 1_000_000_000,
            )
        )
    except subprocess.TimeoutExpired:
        timed_out = True
        process.kill()
        out, err = process.communicate()
    elapsed = time.perf_counter_ns() - tick
    if capture_folder is not None:
        from pathlib import Path

        folder = Path(capture_folder)
        # Preserve raw pipes before parsing or validating any child claim.
        command = dict(
            argv=process.args,
            pid=process.pid,
            exit_code=process.returncode,
            timed_out=timed_out,
            elapsed_ns=elapsed,
            utc_start=start.isoformat(),
            utc_end=datetime.now(timezone.utc).isoformat(),
            request_sha256=hashlib.sha256(serialize_evaluation(request)).hexdigest(),
        )
        for filename, raw in [
            ("stdout.bin", out),
            ("stderr.bin", err),
            (
                "command.json",
                (json.dumps(command, sort_keys=True, indent=2) + "\n").encode(),
            ),
        ]:
            with (folder / filename).open("xb") as handle:
                handle.write(raw)
    decisions: list[DecisionOutcome] = []
    enrichments: list[EnrichmentOutcome] = []
    returned = None
    stage: Stage = "load"
    failure_type = "WorkerExit"
    failure_seen = False
    deliveries: list[dict[str, Any]] = []
    for line in out.splitlines():
        if len(line) > MAX_RAW_BYTES:
            raise EvaluationContractError("Worker frame exceeds byte cap")

        # Same duplicate-key and finite guards as the approved JSON decoder.
        def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            value: dict[str, Any] = {}
            for k, v in pairs:
                if k in value:
                    raise EvaluationContractError("Duplicate worker frame key")
                value[k] = v
            return value

        frame = json.loads(
            line,
            object_pairs_hook=unique,
            parse_constant=lambda value: (_ for _ in ()).throw(
                EvaluationContractError("Nonfinite worker frame")
            ),
        )
        if returned is not None or failure_seen:
            raise EvaluationContractError(
                "Worker emitted frames after its terminal result or failure"
            )
        if frame["kind"] == "stage":
            stage = frame["stage"]
        elif frame["kind"] == "decision":
            decisions.append(
                deserialize_evaluation(
                    DecisionOutcome,
                    json.dumps(frame["value"]).encode(),
                    limit=MAX_RAW_BYTES,
                )
            )
        elif frame["kind"] == "enrichment":
            enrichments.append(
                deserialize_evaluation(
                    EnrichmentOutcome,
                    json.dumps(frame["value"]).encode(),
                    limit=MAX_RAW_BYTES,
                )
            )
        elif frame["kind"] == "result":
            if returned is not None:
                raise EvaluationContractError("Repeated worker result")
            returned = deserialize_evaluation(
                RawRun, json.dumps(frame["value"]).encode(), limit=MAX_RAW_BYTES
            )
        elif frame["kind"] == "failure":
            failure_type = frame["exception_type"]
            failure_seen = True
        elif frame["kind"] == "delivery":
            deliveries.append(frame)
        else:
            raise EvaluationContractError("Unknown worker frame")
    # Authenticate the same journal on every exit path, before constructing
    # success, exception or timeout outcomes. A failed child is not a trusted
    # source of alternative inputs, clocks or enrichment identities.
    if returned is not None:
        if returned.plan != request.plan or returned.sources_before != sources:
            raise EvaluationContractError("Worker returned another request")
        if (
            tuple(decisions) != returned.decisions
            or tuple(enrichments) != returned.enrichments
        ):
            raise EvaluationContractError("Worker journal differs from final outcome")
    if (
        tuple(d.decision for d in decisions)
        != request.plan.decision_ids[: len(decisions)]
    ):
        raise EvaluationContractError("Worker decision journal is not a request prefix")
    projected = dict(
        zip(
            (unit.unit for unit in request.case.units),
            project_runtime_case(request.case),
        )
    )
    for captured in decisions:
        scheduled = next(
            d for d in request.case.decisions if d.decision == captured.decision
        )
        expected_inputs = tuple(
            projected[d.unit]
            for d in request.case.deliveries
            if d.arrival_offset_seconds <= scheduled.arrival_cutoff_seconds
        )
        expected_watermark = (
            EPOCH + timedelta(seconds=scheduled.watermark_offset_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if (
            captured.arguments.runtime_inputs != expected_inputs
            or captured.arguments.watermark != expected_watermark
            or captured.arguments.configuration_fingerprint
            != request.policy.configuration_fingerprint
            or captured.arguments.topology_fingerprint
            != request.topology.pin.snapshot_fingerprint
        ):
            raise EvaluationContractError(
                "Worker captured a different runtime schedule"
            )
    expected_deliveries = []
    seen = set()
    for delivery in request.case.deliveries:
        uid = projected[delivery.unit].signal.signal_id
        expected_deliveries.append(
            dict(
                kind="delivery",
                delivery=delivery.delivery,
                signal_id=str(uid),
                disposition="duplicate" if uid in seen else "accepted",
            )
        )
        seen.add(uid)
    required = (
        sum(
            d.arrival_offset_seconds
            <= request.case.decisions[len(decisions) - 1].arrival_cutoff_seconds
            for d in request.case.deliveries
        )
        if decisions
        else 0
    )
    if (
        deliveries != expected_deliveries[: len(deliveries)]
        or len(deliveries) < required
        or (returned is not None and len(deliveries) != len(expected_deliveries))
    ):
        raise EvaluationContractError(
            "Worker intake journal omitted or changed deliveries"
        )
    final_result = (
        decisions[-1].grouping_result
        if decisions and decisions[-1].decision == request.plan.decision_ids[-1]
        else None
    )
    final_groups = (
        {g.group_fingerprint for g in final_result.groups}
        if final_result is not None
        else set()
    )
    enrichment_groups = [item.group for item in enrichments]
    if (
        len(set(enrichment_groups)) != len(enrichment_groups)
        or not set(enrichment_groups) <= final_groups
    ):
        raise EvaluationContractError(
            "Worker enrichment journal differs from final request groups"
        )
    wall = StageTiming(
        stage="worker_wall",
        decision=None,
        group=None,
        elapsed_ns=elapsed,
        status="censored" if timed_out else "observed",
    )
    if returned is not None and not timed_out and process.returncode == 0:
        value = returned.model_copy(
            update=dict(
                timings=tuple(t for t in returned.timings if t.stage != "worker_wall")
                + (wall,),
                elapsed_ns=elapsed,
                utc_start=start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                utc_end=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            )
        )
    else:
        final = bool(
            decisions
            and decisions[-1].decision == request.plan.decision_ids[-1]
            and decisions[-1].grouping_result is not None
        )
        # A crash without an explicit timeout cannot invent missing enrichments.
        if final and not timed_out:
            completed = {item.group for item in enrichments}
            result = decisions[-1].grouping_result
            assert result is not None
            for group in result.groups:
                if group.group_fingerprint not in completed:
                    enrichments.append(
                        EnrichmentOutcome(
                            group=group.group_fingerprint,
                            aggregate=None,
                            blast_context=None,
                            handoff=None,
                            status="failed",
                            errors=(
                                SafeError(
                                    stage="load",
                                    exception_type="WorkerExit",
                                    message="Worker exited before enrichment completed.",
                                ),
                            ),
                            timings=(),
                        )
                    )
        value = RawRun(
            plan=request.plan,
            utc_start=start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            utc_end=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            elapsed_ns=elapsed,
            terminal_status="TIMEOUT"
            if timed_out
            else "PARTIAL_FAILURE"
            if final
            else "RUNTIME_ERROR",
            final_grouping_available=final,
            decisions=tuple(decisions),
            enrichments=tuple(enrichments),
            timings=(wall,),
            errors=(
                SafeError(
                    stage=stage,
                    exception_type="TimeoutExpired" if timed_out else failure_type,
                    message="Worker exceeded deadline."
                    if timed_out
                    else "Worker exited without a committed result.",
                ),
            ),
            timeout=TimeoutEvidence(
                deadline_ns=deadline_seconds * 1_000_000_000,
                elapsed_ns=elapsed,
                last_stage=stage,
                pid=process.pid,
                terminated=True,
            )
            if timed_out
            else None,
            environment=request.environment,
            sources_before=sources,
            sources_after=sources,
        )
    value = value.model_copy(
        update=dict(
            sources_after=observe_runtime_sources(request.workspace, request.sources)
        )
    )
    value = RawRun.model_validate(_plain(value))
    if err.splitlines() not in (
        [],
        [b"Importing plotly failed. Interactive plots will not work."],
    ):
        raise EvaluationContractError(
            "Unexpected worker stderr; raw pipes must be retained by caller"
        )
    return value, out, err, process.pid
