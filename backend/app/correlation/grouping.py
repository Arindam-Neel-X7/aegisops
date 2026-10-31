from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping, Sequence

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
    ValidationError,
)

from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
    DuplicateSignalDisposition,
    LateSignalDisposition,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    GroupingContractError,
    GroupingInputError,
    GroupingCapacityError,
)
from app.correlation.models import (
    CorrelationEvidence,
    FINGERPRINT_LEN,
    MAX_SIGNAL_REFS,
    MAX_EVIDENCE_ENTRIES,
    MAX_ENV_SERVICE_LEN,
)
from app.correlation.topology import (
    TopologySnapshot,
    TraversalDirection,
    find_snapshot_paths,
    TOPOLOGY_MAX_HOPS,
)

SUPPORTED_GROUPING_SCHEMA_VERSION = "1.0.0"

_HEX_CHARS = frozenset("0123456789abcdef")
_UTC_OFFSET = timezone.utc.utcoffset(None)


def _validate_fingerprint(value: Any, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise GroupingContractError(f"{field_name} must be a string")
    if len(value) != FINGERPRINT_LEN:
        raise GroupingContractError(f"{field_name} must be exactly 64 characters")
    if any(c not in _HEX_CHARS for c in value):
        raise GroupingContractError(f"{field_name} must be lowercase hexadecimal")
    return str(value)


def _validate_bounded_text(value: Any, max_len: int, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise GroupingContractError(f"{field_name} must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise GroupingContractError(
            f"{field_name} must not be empty or whitespace-only"
        )
    if len(trimmed) > max_len:
        raise GroupingContractError(f"{field_name} exceeds maximum length")
    return str(trimmed)


def _validate_utc(value: Any, info: ValidationInfo) -> datetime:
    if isinstance(value, str):
        if info.mode == "python":
            raise GroupingContractError(
                "datetime object required in python validation mode"
            )
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception as exc:
            raise GroupingContractError("invalid ISO datetime string") from exc
        value = dt
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise GroupingContractError("timestamp must be timezone-aware")
        if value.tzinfo.utcoffset(value) != _UTC_OFFSET:
            raise GroupingContractError("timestamp must be UTC")
        return value
    raise GroupingContractError("timestamp must be a datetime or ISO string")


def _validate_not_bool(value: Any) -> Any:
    if isinstance(value, bool):
        raise GroupingContractError(
            "boolean is not accepted where a numeric value is required"
        )
    return value


class GroupingSignalDisposition(StrEnum):
    GROUPED = "grouped"
    DUPLICATE_IGNORED = "duplicate_ignored"
    DUPLICATE_REJECTED = "duplicate_rejected"
    LATE_ACCEPTED = "late_accepted"
    LATE_REJECTED = "late_rejected"
    LATE_DELAYED = "late_delayed"


class GroupingEdgeRejectionReason(StrEnum):
    SOURCE_PAIR_REJECTED = "source_pair_rejected"
    MISSING_PAIR_EVIDENCE = "missing_pair_evidence"
    INELIGIBLE_SIGNAL = "ineligible_signal"
    MAXIMUM_GROUP_SIZE_EXCEEDED = "maximum_group_size_exceeded"
    TENANT_MISMATCH = "tenant_mismatch"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    RUN_CONTEXT_MISMATCH = "run_context_mismatch"
    TIME_SPAN_EXCEEDED = "time_span_exceeded"
    TOPOLOGY_SERVICE_MISSING = "topology_service_missing"
    TOPOLOGY_SERVICE_AMBIGUOUS = "topology_service_ambiguous"
    TOPOLOGY_DISCONNECTED = "topology_disconnected"
    TOPOLOGY_DIAMETER_EXCEEDED = "topology_diameter_exceeded"
    BRIDGE_CHAIN_BLOCKED = "bridge_chain_blocked"


class GroupingMemberReference(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    signal_id: uuid.UUID
    semantic_fingerprint: str
    service: str
    event_time: AwareDatetime

    @field_validator("semantic_fingerprint", mode="after")
    @classmethod
    def _check_fp(cls, v: str) -> str:
        return _validate_fingerprint(v, "semantic_fingerprint")

    @field_validator("service", mode="after")
    @classmethod
    def _check_svc(cls, v: str) -> str:
        return _validate_bounded_text(v, MAX_ENV_SERVICE_LEN, "service")

    @field_validator("event_time", mode="before")
    @classmethod
    def _check_time(cls, v: Any, info: ValidationInfo) -> datetime:
        return _validate_utc(v, info)


class GroupingSignalRecord(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    signal_id: uuid.UUID
    semantic_fingerprint: str
    included: bool
    duplicate_count: int = Field(..., ge=1)
    late: bool
    disposition: GroupingSignalDisposition
    group_fingerprint: str | None = None

    @field_validator("semantic_fingerprint", mode="after")
    @classmethod
    def _check_fp(cls, v: str) -> str:
        return _validate_fingerprint(v, "semantic_fingerprint")

    @field_validator("duplicate_count", mode="before")
    @classmethod
    def _check_dup_cnt(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("group_fingerprint", mode="after")
    @classmethod
    def _check_group_fp(cls, v: str | None) -> str | None:
        if v is not None:
            return _validate_fingerprint(v, "group_fingerprint")
        return None

    @model_validator(mode="after")
    def _check_consistency(self) -> GroupingSignalRecord:
        if not self.included and self.group_fingerprint is not None:
            raise GroupingContractError("Excluded signal cannot have group_fingerprint")
        if self.included and self.group_fingerprint is None:
            raise GroupingContractError("Included signal must have group_fingerprint")
        return self


class RejectedGroupingEdge(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    left_signal_id: uuid.UUID
    right_signal_id: uuid.UUID
    reason: GroupingEdgeRejectionReason
    source_evidence: CorrelationEvidence | None = None
    detail: str

    @model_validator(mode="before")
    @classmethod
    def _canonicalize_edge(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        left = data.get("left_signal_id")
        right = data.get("right_signal_id")
        if left is None or right is None:
            return data
        try:
            left_bytes = (
                left.bytes
                if isinstance(left, uuid.UUID)
                else uuid.UUID(str(left)).bytes
            )
            right_bytes = (
                right.bytes
                if isinstance(right, uuid.UUID)
                else uuid.UUID(str(right)).bytes
            )
        except Exception:
            return data
        if left_bytes > right_bytes:
            swapped = dict(data)
            swapped["left_signal_id"] = right
            swapped["right_signal_id"] = left
            return swapped
        return data

    @field_validator("detail", mode="after")
    @classmethod
    def _check_detail(cls, v: str) -> str:
        return _validate_bounded_text(v, 256, "detail")

    @model_validator(mode="after")
    def _check_endpoints(self) -> RejectedGroupingEdge:
        if self.left_signal_id == self.right_signal_id:
            raise GroupingContractError("Rejected edge cannot be self-referential")
        return self


class CorrelationGroup(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    schema_version: str = SUPPORTED_GROUPING_SCHEMA_VERSION
    policy_version: str
    configuration_fingerprint: str
    topology_snapshot_fingerprint: str
    members: tuple[GroupingMemberReference, ...]
    accepted_evidence: tuple[CorrelationEvidence, ...]
    started_at: AwareDatetime
    last_seen_at: AwareDatetime
    time_span_seconds: float = Field(..., ge=0.0)
    topology_diameter_hops: int = Field(..., ge=0, le=TOPOLOGY_MAX_HOPS)
    group_fingerprint: str

    @field_validator("schema_version", mode="after")
    @classmethod
    def _check_schema(cls, v: str) -> str:
        if v != SUPPORTED_GROUPING_SCHEMA_VERSION:
            raise GroupingContractError("Unsupported grouping schema version")
        return v

    @field_validator("policy_version", mode="after")
    @classmethod
    def _check_policy(cls, v: str) -> str:
        return _validate_bounded_text(v, 128, "policy_version")

    @field_validator(
        "configuration_fingerprint",
        "topology_snapshot_fingerprint",
        "group_fingerprint",
        mode="after",
    )
    @classmethod
    def _check_fps(cls, v: str) -> str:
        return _validate_fingerprint(v, "fingerprint")

    @field_validator("started_at", "last_seen_at", mode="before")
    @classmethod
    def _check_times(cls, v: Any, info: ValidationInfo) -> datetime:
        return _validate_utc(v, info)

    @field_validator("time_span_seconds", mode="before")
    @classmethod
    def _check_span(cls, v: Any) -> Any:
        val = _validate_not_bool(v)
        if isinstance(val, (int, float)):
            if not math.isfinite(val) or val < 0.0:
                raise GroupingContractError(
                    "time_span_seconds must be finite and non-negative"
                )
            return float(val)
        raise GroupingContractError("time_span_seconds must be numeric")

    @field_validator("topology_diameter_hops", mode="before")
    @classmethod
    def _check_hops(cls, v: Any) -> Any:
        return _validate_not_bool(v)

    @field_validator("members", mode="before")
    @classmethod
    def _check_members(cls, v: Any) -> Any:
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise GroupingContractError("members must be a list or tuple")
        return tuple(v)

    @field_validator("accepted_evidence", mode="before")
    @classmethod
    def _check_ev(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise GroupingContractError("accepted_evidence must be a list or tuple")
        return tuple(v)

    @model_validator(mode="after")
    def _check_invariants(self) -> CorrelationGroup:
        if self.started_at > self.last_seen_at:
            raise GroupingContractError("started_at must be <= last_seen_at")
        if len(self.members) < 1:
            raise GroupingContractError("Group must contain at least one member")
        if len(self.members) > MAX_SIGNAL_REFS:
            raise GroupingContractError("Group members exceed maximum limit")

        sig_ids = [m.signal_id for m in self.members]
        if len(set(sig_ids)) != len(sig_ids):
            raise GroupingContractError("Group member signal IDs must be unique")

        sorted_members = tuple(sorted(self.members, key=lambda m: m.signal_id.bytes))
        if sorted_members != self.members:
            raise GroupingContractError(
                "Group members must be canonically sorted by signal_id bytes"
            )

        n = len(self.members)
        expected_edge_count = n * (n - 1) // 2
        if len(self.accepted_evidence) != expected_edge_count:
            raise GroupingContractError(
                "Accepted evidence count must match complete clique size"
            )

        member_set = set(sig_ids)
        seen_pairs: set[tuple[bytes, bytes]] = set()
        for ev in self.accepted_evidence:
            if (
                ev.left_signal_id not in member_set
                or ev.right_signal_id not in member_set
            ):
                raise GroupingContractError(
                    "Accepted evidence must reference only group members"
                )
            if not ev.accepted:
                raise GroupingContractError("Group evidence must be accepted")
            pair = (ev.left_signal_id.bytes, ev.right_signal_id.bytes)
            if pair[0] >= pair[1]:
                raise GroupingContractError(
                    "Accepted evidence pair not canonically ordered"
                )
            if pair in seen_pairs:
                raise GroupingContractError("Duplicate accepted evidence pair")
            seen_pairs.add(pair)

        times = [m.event_time for m in self.members]
        min_t = min(times)
        max_t = max(times)
        expected_span = (max_t - min_t).total_seconds()
        if abs(self.time_span_seconds - expected_span) > 1e-6:
            raise GroupingContractError(
                "time_span_seconds does not match member event times"
            )
        if self.started_at != min_t or self.last_seen_at != max_t:
            raise GroupingContractError(
                "started_at / last_seen_at must match min/max member times"
            )

        if self.group_fingerprint != "0" * 64:
            expected_fp = compute_group_fingerprint(self)
            if expected_fp != self.group_fingerprint:
                raise GroupingContractError(
                    "group_fingerprint does not match canonical projection"
                )

        return self


class CorrelationGroupingResult(BaseModel):
    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", hide_input_in_errors=True
    )

    schema_version: str = SUPPORTED_GROUPING_SCHEMA_VERSION
    policy_version: str
    configuration_fingerprint: str
    topology_snapshot_fingerprint: str
    event_time_watermark: AwareDatetime
    groups: tuple[CorrelationGroup, ...]
    rejected_edges: tuple[RejectedGroupingEdge, ...]
    signal_records: tuple[GroupingSignalRecord, ...]
    result_fingerprint: str

    @field_validator("schema_version", mode="after")
    @classmethod
    def _check_schema(cls, v: str) -> str:
        if v != SUPPORTED_GROUPING_SCHEMA_VERSION:
            raise GroupingContractError("Unsupported grouping schema version")
        return v

    @field_validator("policy_version", mode="after")
    @classmethod
    def _check_policy(cls, v: str) -> str:
        return _validate_bounded_text(v, 128, "policy_version")

    @field_validator(
        "configuration_fingerprint",
        "topology_snapshot_fingerprint",
        "result_fingerprint",
        mode="after",
    )
    @classmethod
    def _check_fps(cls, v: str) -> str:
        return _validate_fingerprint(v, "fingerprint")

    @field_validator("event_time_watermark", mode="before")
    @classmethod
    def _check_watermark(cls, v: Any, info: ValidationInfo) -> datetime:
        return _validate_utc(v, info)

    @field_validator("groups", "rejected_edges", "signal_records", mode="before")
    @classmethod
    def _check_collections(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, (str, bytes)) or not isinstance(v, (list, tuple)):
            raise GroupingContractError("Collection must be a list or tuple")
        return tuple(v)

    @model_validator(mode="after")
    def _check_result_invariants(self) -> CorrelationGroupingResult:
        if len(self.groups) > MAX_SIGNAL_REFS:
            raise GroupingContractError("Groups count exceeds maximum")
        if len(self.rejected_edges) > MAX_EVIDENCE_ENTRIES:
            raise GroupingContractError("Rejected edges count exceeds maximum")
        if len(self.signal_records) > MAX_SIGNAL_REFS:
            raise GroupingContractError("Signal records count exceeds maximum")

        sorted_groups = tuple(
            sorted(self.groups, key=lambda g: g.members[0].signal_id.bytes)
        )
        if sorted_groups != self.groups:
            raise GroupingContractError(
                "Groups must be canonically sorted by first member signal_id bytes"
            )

        sorted_rejections = tuple(
            sorted(
                self.rejected_edges,
                key=lambda e: (
                    e.left_signal_id.bytes,
                    e.right_signal_id.bytes,
                    e.reason.value,
                ),
            )
        )
        if sorted_rejections != self.rejected_edges:
            raise GroupingContractError("Rejected edges must be canonically sorted")

        sorted_records = tuple(
            sorted(self.signal_records, key=lambda r: r.signal_id.bytes)
        )
        if sorted_records != self.signal_records:
            raise GroupingContractError(
                "Signal records must be canonically sorted by signal_id bytes"
            )

        # 1. Unique signal record IDs
        record_ids = [r.signal_id for r in self.signal_records]
        if len(set(record_ids)) != len(record_ids):
            raise GroupingContractError("Signal record IDs must be unique")

        # 2. Unique group fingerprints
        group_fps = [g.group_fingerprint for g in self.groups]
        if len(set(group_fps)) != len(group_fps):
            raise GroupingContractError("Group fingerprints must be unique")

        # 3. Globally unique member signal IDs across all groups
        assigned_ids: set[uuid.UUID] = set()
        group_member_sets: list[set[uuid.UUID]] = []
        for g in self.groups:
            g_set = set()
            for m in g.members:
                if m.signal_id in assigned_ids:
                    raise GroupingContractError("Signal appears in multiple groups")
                assigned_ids.add(m.signal_id)
                g_set.add(m.signal_id)
            group_member_sets.append(g_set)

        record_map = {r.signal_id: r for r in self.signal_records}

        # 4 & 5 & 6 & 7 & 8. Signal record consistency with groups
        included_record_ids = {r.signal_id for r in self.signal_records if r.included}
        if included_record_ids != assigned_ids:
            raise GroupingContractError(
                "Included signal records must equal exactly group members"
            )

        for g in self.groups:
            if g.policy_version != self.policy_version:
                raise GroupingContractError(
                    "Group policy_version must match result policy_version"
                )
            if g.configuration_fingerprint != self.configuration_fingerprint:
                raise GroupingContractError(
                    "Group configuration_fingerprint must match result configuration_fingerprint"
                )
            if g.topology_snapshot_fingerprint != self.topology_snapshot_fingerprint:
                raise GroupingContractError(
                    "Group topology_snapshot_fingerprint must match result topology_snapshot_fingerprint"
                )

            for m in g.members:
                rec = record_map.get(m.signal_id)
                if rec is None or not rec.included:
                    raise GroupingContractError(
                        "Group member must have an included signal record"
                    )
                if rec.group_fingerprint != g.group_fingerprint:
                    raise GroupingContractError(
                        "Included record group_fingerprint must match containing group"
                    )
                if m.semantic_fingerprint != rec.semantic_fingerprint:
                    raise GroupingContractError(
                        "Member semantic fingerprint must match signal record"
                    )

        for r in self.signal_records:
            if not r.included:
                if r.group_fingerprint is not None:
                    raise GroupingContractError(
                        "Excluded signal record cannot have group_fingerprint"
                    )
            else:
                if r.signal_id not in assigned_ids:
                    raise GroupingContractError(
                        "Included signal record must belong to a group"
                    )

        # 12 & 13 & 14 & 15 & 16. Rejected edge invariants (uniqueness by canonical endpoints only)
        seen_rej_pairs: set[tuple[bytes, bytes]] = set()
        for rej in self.rejected_edges:
            if (
                rej.left_signal_id not in record_map
                or rej.right_signal_id not in record_map
            ):
                raise GroupingContractError(
                    "Rejected edge endpoints must exist in signal records"
                )

            l_b = rej.left_signal_id.bytes
            r_b = rej.right_signal_id.bytes
            if l_b >= r_b:
                raise GroupingContractError(
                    "Rejected edge endpoints must be canonically ordered"
                )
            pair_key = (l_b, r_b)
            if pair_key in seen_rej_pairs:
                raise GroupingContractError(
                    "Duplicate rejected edge for unordered pair"
                )
            seen_rej_pairs.add(pair_key)

            # Cannot be internal to the same group
            g_l = record_map[rej.left_signal_id].group_fingerprint
            g_r = record_map[rej.right_signal_id].group_fingerprint
            if g_l is not None and g_l == g_r:
                raise GroupingContractError(
                    "Rejected edge cannot be internal to an emitted group"
                )

            if rej.source_evidence is not None:
                ev = rej.source_evidence
                ev_l = ev.left_signal_id.bytes
                ev_r = ev.right_signal_id.bytes
                ev_pair = (ev_l, ev_r) if ev_l < ev_r else (ev_r, ev_l)
                if ev_pair != pair_key:
                    raise GroupingContractError(
                        "Source evidence on rejected edge must reference that exact unordered pair"
                    )

        if self.result_fingerprint != "0" * 64:
            expected_result_fp = compute_result_fingerprint(self)
            if expected_result_fp != self.result_fingerprint:
                raise GroupingContractError(
                    "result_fingerprint does not match canonical projection"
                )

        return self


def compute_group_fingerprint(group: CorrelationGroup) -> str:
    payload = {
        "schema_version": group.schema_version,
        "policy_version": group.policy_version,
        "configuration_fingerprint": group.configuration_fingerprint,
        "topology_snapshot_fingerprint": group.topology_snapshot_fingerprint,
        "members": [m.model_dump(mode="json") for m in group.members],
        "accepted_evidence": [
            e.model_dump(mode="json") for e in group.accepted_evidence
        ],
        "started_at": group.started_at.isoformat(),
        "last_seen_at": group.last_seen_at.isoformat(),
        "time_span_seconds": group.time_span_seconds,
        "topology_diameter_hops": group.topology_diameter_hops,
    }
    canonical_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def compute_result_fingerprint(result: CorrelationGroupingResult) -> str:
    payload = {
        "schema_version": result.schema_version,
        "policy_version": result.policy_version,
        "configuration_fingerprint": result.configuration_fingerprint,
        "topology_snapshot_fingerprint": result.topology_snapshot_fingerprint,
        "event_time_watermark": result.event_time_watermark.isoformat(),
        "groups": [g.model_dump(mode="json") for g in result.groups],
        "rejected_edges": [e.model_dump(mode="json") for e in result.rejected_edges],
        "signal_records": [r.model_dump(mode="json") for r in result.signal_records],
    }
    canonical_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def serialize_grouping_result(result: CorrelationGroupingResult) -> bytes:
    try:
        if not isinstance(result, CorrelationGroupingResult):
            raise GroupingContractError(
                "result must be a CorrelationGroupingResult instance"
            )
        payload = result.model_dump(mode="json")
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except GroupingContractError:
        raise
    except Exception as exc:
        raise GroupingContractError(
            "Failed to serialize CorrelationGroupingResult"
        ) from exc


def deserialize_grouping_result(
    raw: bytes | bytearray | memoryview,
) -> CorrelationGroupingResult:
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise GroupingContractError("Payload must be bytes-like")
    try:
        text = bytes(raw).decode("utf-8")
    except Exception as exc:
        raise GroupingContractError("Invalid UTF-8 payload") from exc

    trimmed_text = text.strip()
    if not trimmed_text.startswith("{"):
        raise GroupingContractError("Payload must be a JSON object")

    try:
        parsed = json.loads(trimmed_text)
    except Exception as exc:
        raise GroupingContractError("Invalid JSON payload") from exc
    if not isinstance(parsed, dict):
        raise GroupingContractError("Payload must be a JSON object")

    try:
        return CorrelationGroupingResult.model_validate_json(trimmed_text)
    except GroupingContractError:
        raise
    except ValidationError as exc:
        raise GroupingContractError(
            "CorrelationGroupingResult validation failed"
        ) from exc
    except Exception as exc:
        raise GroupingContractError(
            "CorrelationGroupingResult validation failed"
        ) from exc


def _resolve_service_nodes(service_name: str, snapshot: TopologySnapshot) -> list[Any]:
    return [node for node in snapshot.nodes if node.name == service_name]


def group_candidate_signals(
    inputs: Sequence[AnomalyCorrelationInput],
    pair_evidence: Sequence[CorrelationEvidence],
    *,
    topology_snapshot: TopologySnapshot,
    config: CorrelationConfig,
    event_time_watermark: datetime,
) -> CorrelationGroupingResult:
    if not isinstance(inputs, (list, tuple)):
        raise GroupingInputError("inputs must be a list or tuple")
    if not isinstance(pair_evidence, (list, tuple)):
        raise GroupingInputError("pair_evidence must be a list or tuple")
    if not isinstance(topology_snapshot, TopologySnapshot):
        raise GroupingInputError(
            "topology_snapshot must be a TopologySnapshot instance"
        )
    if not isinstance(config, CorrelationConfig):
        raise GroupingInputError("config must be a CorrelationConfig instance")
    if not isinstance(event_time_watermark, datetime):
        raise GroupingInputError("event_time_watermark must be a datetime instance")
    if (
        event_time_watermark.tzinfo is None
        or event_time_watermark.tzinfo.utcoffset(event_time_watermark) != _UTC_OFFSET
    ):
        raise GroupingInputError("event_time_watermark must be timezone-aware UTC")

    if len(inputs) > MAX_SIGNAL_REFS:
        raise GroupingCapacityError("Inputs exceed MAX_SIGNAL_REFS")
    if len(pair_evidence) > MAX_EVIDENCE_ENTRIES:
        raise GroupingCapacityError("Pair evidence exceeds MAX_EVIDENCE_ENTRIES")

    config_fp = compute_correlation_config_fingerprint(config)
    topo_fp = topology_snapshot.snapshot_fingerprint

    by_sig_id: dict[uuid.UUID, list[AnomalyCorrelationInput]] = {}
    for inp in inputs:
        if not isinstance(inp, AnomalyCorrelationInput):
            raise GroupingInputError(
                "Every input must be an AnomalyCorrelationInput instance"
            )
        by_sig_id.setdefault(inp.signal.signal_id, []).append(inp)

    raw_signal_meta: dict[
        uuid.UUID,
        tuple[AnomalyCorrelationInput, bool, int, bool, GroupingSignalDisposition],
    ] = {}
    active_inputs: dict[uuid.UUID, AnomalyCorrelationInput] = {}

    for sig_id, group_inputs in by_sig_id.items():
        first = group_inputs[0]
        for other in group_inputs[1:]:
            if (
                other.semantic_fingerprint != first.semantic_fingerprint
                or other.signal != first.signal
            ):
                raise GroupingInputError("Conflicting input signals detected")

        dup_count = len(group_inputs)
        is_duplicate = dup_count > 1

        late_cutoff = event_time_watermark.timestamp() - config.allowed_lateness_seconds
        is_late = first.signal.event_time.timestamp() < late_cutoff

        disposition: GroupingSignalDisposition
        included = True

        if is_duplicate:
            if config.duplicate_signal_disposition == DuplicateSignalDisposition.REJECT:
                included = False
                disposition = GroupingSignalDisposition.DUPLICATE_REJECTED
            else:
                if is_late:
                    if config.late_signal_disposition == LateSignalDisposition.REJECT:
                        included = False
                        disposition = GroupingSignalDisposition.LATE_REJECTED
                    elif config.late_signal_disposition == LateSignalDisposition.DELAY:
                        included = False
                        disposition = GroupingSignalDisposition.LATE_DELAYED
                    else:
                        disposition = GroupingSignalDisposition.DUPLICATE_IGNORED
                else:
                    disposition = GroupingSignalDisposition.DUPLICATE_IGNORED
        else:
            if is_late:
                if config.late_signal_disposition == LateSignalDisposition.REJECT:
                    included = False
                    disposition = GroupingSignalDisposition.LATE_REJECTED
                elif config.late_signal_disposition == LateSignalDisposition.DELAY:
                    included = False
                    disposition = GroupingSignalDisposition.LATE_DELAYED
                else:
                    included = True
                    disposition = GroupingSignalDisposition.LATE_ACCEPTED
            else:
                included = True
                disposition = GroupingSignalDisposition.GROUPED

        raw_signal_meta[sig_id] = (first, included, dup_count, is_late, disposition)
        if included:
            active_inputs[sig_id] = first

    eligible_signal_ids = sorted(active_inputs.keys(), key=lambda u: u.bytes)
    supplied_signal_ids = set(by_sig_id.keys())

    # R1-C: Canonical topology prevalidation precedence (missing first, ambiguous second over sorted active services)
    active_services = sorted({inp.signal.service for inp in active_inputs.values()})
    for svc in active_services:
        nodes = _resolve_service_nodes(svc, topology_snapshot)
        if not nodes:
            raise GroupingInputError("Service missing in topology snapshot")
    for svc in active_services:
        nodes = _resolve_service_nodes(svc, topology_snapshot)
        if len(nodes) > 1:
            raise GroupingInputError("Service ambiguous in topology snapshot")

    # R1-A: Validate pair evidence and maintain rejections_by_pair keyed only by canonical unordered endpoints
    validated_evidence_map: dict[tuple[bytes, bytes], CorrelationEvidence] = {}
    rejections_by_pair: dict[tuple[bytes, bytes], RejectedGroupingEdge] = {}
    seen_evidence_pairs: set[tuple[bytes, bytes]] = set()

    for ev in pair_evidence:
        if not isinstance(ev, CorrelationEvidence):
            raise GroupingInputError(
                "Every edge must be a CorrelationEvidence instance"
            )
        if ev.left_signal_id == ev.right_signal_id:
            raise GroupingInputError("Self-referential evidence edge")

        if (
            ev.left_signal_id not in supplied_signal_ids
            or ev.right_signal_id not in supplied_signal_ids
        ):
            raise GroupingInputError("Evidence references unknown signal endpoint")

        left_bytes = ev.left_signal_id.bytes
        right_bytes = ev.right_signal_id.bytes
        if left_bytes > right_bytes:
            left_bytes, right_bytes = right_bytes, left_bytes
        pair_key = (left_bytes, right_bytes)

        if pair_key in seen_evidence_pairs:
            raise GroupingInputError("Duplicate entry for unordered pair evidence")
        seen_evidence_pairs.add(pair_key)

        if ev.policy_version != config.policy_version:
            raise GroupingInputError("Evidence policy_version does not match config")

        if ev.accepted:
            if not ev.reason.startswith("accepted"):
                raise GroupingInputError(
                    "Accepted evidence must have reason starting with accepted"
                )
            if ev.weighted_score < config.acceptance_threshold:
                raise GroupingInputError(
                    "Accepted evidence weighted_score below acceptance threshold"
                )
            validated_evidence_map[pair_key] = ev
        else:
            if ev.reason.startswith("accepted"):
                raise GroupingInputError(
                    "Rejected evidence cannot have reason starting with accepted"
                )
            if (
                ev.left_signal_id in active_inputs
                and ev.right_signal_id in active_inputs
            ):
                l_id = ev.left_signal_id
                r_id = ev.right_signal_id
                if l_id.bytes > r_id.bytes:
                    l_id, r_id = r_id, l_id
                rej = RejectedGroupingEdge(
                    left_signal_id=l_id,
                    right_signal_id=r_id,
                    reason=GroupingEdgeRejectionReason.SOURCE_PAIR_REJECTED,
                    source_evidence=ev,
                    detail="Source pair explicitly rejected by scorer",
                )
                if pair_key not in rejections_by_pair:
                    rejections_by_pair[pair_key] = rej

    # 4. Canonical Complete-Link Partitioning Algorithm
    assigned_signals: set[uuid.UUID] = set()
    raw_groups: list[list[uuid.UUID]] = []

    for seed_id in eligible_signal_ids:
        if seed_id in assigned_signals:
            continue

        current_group = [seed_id]
        assigned_signals.add(seed_id)

        for cand_id in eligible_signal_ids:
            if cand_id in assigned_signals:
                continue

            can_join = True
            for member_id in current_group:
                mb = member_id.bytes
                cb = cand_id.bytes
                pkey = (mb, cb) if mb < cb else (cb, mb)
                edge = validated_evidence_map.get(pkey)
                if edge is None or not edge.accepted:
                    can_join = False
                    break

            if not can_join:
                continue

            proposed_group = current_group + [cand_id]
            guard_reason = _evaluate_group_guards(
                proposed_group, active_inputs, config, topology_snapshot
            )
            if guard_reason is not None:
                continue

            current_group.append(cand_id)
            assigned_signals.add(cand_id)

        raw_groups.append(current_group)

    raw_groups.sort(key=lambda g: min(g, key=lambda u: u.bytes).bytes)

    final_groups: list[CorrelationGroup] = []
    signal_to_group_fp: dict[uuid.UUID, str] = {}

    for g_member_ids in raw_groups:
        sorted_m_ids = sorted(g_member_ids, key=lambda u: u.bytes)
        members_tuple = tuple(
            GroupingMemberReference(
                signal_id=uid,
                semantic_fingerprint=active_inputs[uid].semantic_fingerprint,
                service=active_inputs[uid].signal.service,
                event_time=active_inputs[uid].signal.event_time,
            )
            for uid in sorted_m_ids
        )

        evidence_tuple: list[CorrelationEvidence] = []
        n = len(sorted_m_ids)
        for i in range(n):
            for j in range(i + 1, n):
                mi = sorted_m_ids[i]
                mj = sorted_m_ids[j]
                mib = mi.bytes
                mjb = mj.bytes
                pkey = (mib, mjb) if mib < mjb else (mjb, mib)
                ev = validated_evidence_map[pkey]
                evidence_tuple.append(ev)

        sorted_evidence = tuple(
            sorted(
                evidence_tuple,
                key=lambda e: (e.left_signal_id.bytes, e.right_signal_id.bytes),
            )
        )

        times = [m.event_time for m in members_tuple]
        started_at = min(times)
        last_seen_at = max(times)
        span_seconds = float((last_seen_at - started_at).total_seconds())
        diameter = _compute_group_topology_diameter(members_tuple, topology_snapshot)

        prov_group = CorrelationGroup(
            policy_version=config.policy_version,
            configuration_fingerprint=config_fp,
            topology_snapshot_fingerprint=topo_fp,
            members=members_tuple,
            accepted_evidence=sorted_evidence,
            started_at=started_at,
            last_seen_at=last_seen_at,
            time_span_seconds=span_seconds,
            topology_diameter_hops=diameter,
            group_fingerprint="0" * 64,
        )
        g_fp = compute_group_fingerprint(prov_group)

        final_group = CorrelationGroup(
            policy_version=config.policy_version,
            configuration_fingerprint=config_fp,
            topology_snapshot_fingerprint=topo_fp,
            members=members_tuple,
            accepted_evidence=sorted_evidence,
            started_at=started_at,
            last_seen_at=last_seen_at,
            time_span_seconds=span_seconds,
            topology_diameter_hops=diameter,
            group_fingerprint=g_fp,
        )
        final_groups.append(final_group)
        for uid in sorted_m_ids:
            signal_to_group_fp[uid] = g_fp

    group_map = {g.group_fingerprint: g for g in final_groups}

    # Construct final signal records via normal validated construction (no model_copy bypass)
    final_signal_records: list[GroupingSignalRecord] = []
    for sig_id, (
        first,
        included,
        dup_count,
        is_late,
        disposition,
    ) in raw_signal_meta.items():
        sig_group_fp = signal_to_group_fp.get(sig_id)
        rec = GroupingSignalRecord(
            signal_id=sig_id,
            semantic_fingerprint=first.semantic_fingerprint,
            included=included,
            duplicate_count=dup_count,
            late=is_late,
            disposition=disposition,
            group_fingerprint=sig_group_fp,
        )
        final_signal_records.append(rec)

    final_signal_records.sort(key=lambda r: r.signal_id.bytes)

    # R1-A: Determine rejected edges for all eligible pairs not internal to the same group (strictly one per canonical pair)
    n_elig = len(eligible_signal_ids)
    for i in range(n_elig):
        for j in range(i + 1, n_elig):
            uid_a = eligible_signal_ids[i]
            uid_b = eligible_signal_ids[j]
            ab = uid_a.bytes
            bb = uid_b.bytes
            left_b, right_b = (ab, bb) if ab < bb else (bb, ab)
            left_id, right_id = (uid_a, uid_b) if ab < bb else (uid_b, uid_a)
            pkey = (left_b, right_b)

            if pkey in rejections_by_pair:
                continue

            g_a = signal_to_group_fp.get(left_id)
            g_b = signal_to_group_fp.get(right_id)

            if g_a is not None and g_a == g_b:
                continue

            ev = validated_evidence_map.get(pkey)
            reason: GroupingEdgeRejectionReason
            detail: str
            source_ev = ev

            if ev is None:
                reason = GroupingEdgeRejectionReason.MISSING_PAIR_EVIDENCE
                detail = "Missing pair evidence between eligible signals"
            else:
                members_a = (
                    [m.signal_id for m in group_map[g_a].members]
                    if g_a in group_map
                    else [left_id]
                )
                members_b = (
                    [m.signal_id for m in group_map[g_b].members]
                    if g_b in group_map
                    else [right_id]
                )
                union_ids = sorted(set(members_a + members_b), key=lambda u: u.bytes)
                guard_r = _evaluate_group_guards(
                    union_ids, active_inputs, config, topology_snapshot
                )
                if guard_r is not None:
                    reason = guard_r
                    detail = f"Group union violated guard {guard_r.value}"
                else:
                    reason = GroupingEdgeRejectionReason.BRIDGE_CHAIN_BLOCKED
                    detail = "Complete-link rule prevented group merge across bridge"

            rejections_by_pair[pkey] = RejectedGroupingEdge(
                left_signal_id=left_id,
                right_signal_id=right_id,
                reason=reason,
                source_evidence=source_ev,
                detail=detail,
            )

    # Ineligible signal rejections
    for ev in pair_evidence:
        if ev.left_signal_id in by_sig_id and ev.right_signal_id in by_sig_id:
            rec_l = {r.signal_id: r for r in final_signal_records}[ev.left_signal_id]
            rec_r = {r.signal_id: r for r in final_signal_records}[ev.right_signal_id]
            if not rec_l.included or not rec_r.included:
                lb = ev.left_signal_id.bytes
                rb = ev.right_signal_id.bytes
                l_id, r_id = (
                    (ev.left_signal_id, ev.right_signal_id)
                    if lb < rb
                    else (ev.right_signal_id, ev.left_signal_id)
                )
                pkey = (l_id.bytes, r_id.bytes)
                if pkey not in rejections_by_pair:
                    rejections_by_pair[pkey] = RejectedGroupingEdge(
                        left_signal_id=l_id,
                        right_signal_id=r_id,
                        reason=GroupingEdgeRejectionReason.INELIGIBLE_SIGNAL,
                        source_evidence=ev,
                        detail="One or both endpoint signals were excluded (late/duplicate)",
                    )

    rejected_edges_list = list(rejections_by_pair.values())
    if len(rejected_edges_list) > MAX_EVIDENCE_ENTRIES:
        raise GroupingCapacityError("Rejected edges exceed MAX_EVIDENCE_ENTRIES")

    rejected_edges_tuple = tuple(
        sorted(
            rejected_edges_list,
            key=lambda e: (
                e.left_signal_id.bytes,
                e.right_signal_id.bytes,
                e.reason.value,
            ),
        )
    )
    groups_tuple = tuple(final_groups)
    signal_records_tuple = tuple(final_signal_records)

    prov_result = CorrelationGroupingResult(
        policy_version=config.policy_version,
        configuration_fingerprint=config_fp,
        topology_snapshot_fingerprint=topo_fp,
        event_time_watermark=event_time_watermark,
        groups=groups_tuple,
        rejected_edges=rejected_edges_tuple,
        signal_records=signal_records_tuple,
        result_fingerprint="0" * 64,
    )
    result_fp = compute_result_fingerprint(prov_result)

    return CorrelationGroupingResult(
        policy_version=config.policy_version,
        configuration_fingerprint=config_fp,
        topology_snapshot_fingerprint=topo_fp,
        event_time_watermark=event_time_watermark,
        groups=groups_tuple,
        rejected_edges=rejected_edges_tuple,
        signal_records=signal_records_tuple,
        result_fingerprint=result_fp,
    )


def _evaluate_group_guards(
    group_signal_ids: Sequence[uuid.UUID],
    active_inputs: dict[uuid.UUID, AnomalyCorrelationInput],
    config: CorrelationConfig,
    topology_snapshot: TopologySnapshot,
) -> GroupingEdgeRejectionReason | None:
    if len(group_signal_ids) > config.maximum_group_size:
        return GroupingEdgeRejectionReason.MAXIMUM_GROUP_SIZE_EXCEEDED

    first_inp = active_inputs[group_signal_ids[0]]
    t_id = first_inp.signal.tenant_id
    env = first_inp.signal.environment
    run_id = first_inp.signal.run_id
    scen_id = first_inp.signal.scenario_id
    scen_ver = first_inp.signal.scenario_version
    seed = first_inp.signal.seed
    rep_key = first_inp.signal.reproducibility_key

    event_times = [first_inp.signal.event_time]

    for uid in group_signal_ids[1:]:
        inp = active_inputs[uid]
        sig = inp.signal
        if sig.tenant_id != t_id:
            return GroupingEdgeRejectionReason.TENANT_MISMATCH
        if sig.environment != env:
            return GroupingEdgeRejectionReason.ENVIRONMENT_MISMATCH
        if (
            sig.run_id != run_id
            or sig.scenario_id != scen_id
            or sig.scenario_version != scen_ver
            or sig.seed != seed
            or sig.reproducibility_key != rep_key
        ):
            return GroupingEdgeRejectionReason.RUN_CONTEXT_MISMATCH
        event_times.append(sig.event_time)

    span = (max(event_times) - min(event_times)).total_seconds()
    if span > config.window_seconds:
        return GroupingEdgeRejectionReason.TIME_SPAN_EXCEEDED

    services = [active_inputs[uid].signal.service for uid in group_signal_ids]
    sorted_services = sorted(set(services))
    service_nodes_map: dict[str, list[Any]] = {}

    for svc in sorted_services:
        nodes = _resolve_service_nodes(svc, topology_snapshot)
        if not nodes:
            return GroupingEdgeRejectionReason.TOPOLOGY_SERVICE_MISSING

    for svc in sorted_services:
        nodes = _resolve_service_nodes(svc, topology_snapshot)
        if len(nodes) > 1:
            return GroupingEdgeRejectionReason.TOPOLOGY_SERVICE_AMBIGUOUS
        service_nodes_map[svc] = nodes

    n = len(group_signal_ids)
    for i in range(n):
        for j in range(i + 1, n):
            s_i = active_inputs[group_signal_ids[i]].signal.service
            s_j = active_inputs[group_signal_ids[j]].signal.service
            if s_i == s_j:
                continue
            id_i = service_nodes_map[s_i][0].service_id
            id_j = service_nodes_map[s_j][0].service_id

            paths = find_snapshot_paths(
                topology_snapshot,
                source_service_id=id_i,
                target_service_id=id_j,
                max_hops=TOPOLOGY_MAX_HOPS,
                direction=TraversalDirection.BOTH,
            )
            if not paths:
                return GroupingEdgeRejectionReason.TOPOLOGY_DISCONNECTED

            min_hops = min(len(p.service_ids) - 1 for p in paths)
            if min_hops > config.hop_limit:
                return GroupingEdgeRejectionReason.TOPOLOGY_DIAMETER_EXCEEDED

    return None


def _compute_group_topology_diameter(
    members: tuple[GroupingMemberReference, ...],
    topology_snapshot: TopologySnapshot,
) -> int:
    services = [m.service for m in members]
    service_ids: list[uuid.UUID] = []
    for svc in sorted(set(services)):
        nodes = _resolve_service_nodes(svc, topology_snapshot)
        if not nodes:
            raise GroupingInputError("Service missing in topology snapshot")
        if len(nodes) > 1:
            raise GroupingInputError("Service ambiguous in topology snapshot")
        service_ids.append(nodes[0].service_id)

    if len(service_ids) <= 1:
        return 0

    max_d = 0
    n = len(service_ids)
    for i in range(n):
        for j in range(i + 1, n):
            if service_ids[i] == service_ids[j]:
                continue
            paths = find_snapshot_paths(
                topology_snapshot,
                source_service_id=service_ids[i],
                target_service_id=service_ids[j],
                max_hops=TOPOLOGY_MAX_HOPS,
                direction=TraversalDirection.BOTH,
            )
            if not paths:
                raise GroupingInputError(
                    "Services are disconnected in topology snapshot"
                )
            hops = min(len(p.service_ids) - 1 for p in paths)
            if hops > max_d:
                max_d = hops
    return max_d


__all__ = [
    "SUPPORTED_GROUPING_SCHEMA_VERSION",
    "GroupingSignalDisposition",
    "GroupingEdgeRejectionReason",
    "GroupingMemberReference",
    "GroupingSignalRecord",
    "RejectedGroupingEdge",
    "CorrelationGroup",
    "CorrelationGroupingResult",
    "group_candidate_signals",
    "serialize_grouping_result",
    "deserialize_grouping_result",
    "compute_group_fingerprint",
    "compute_result_fingerprint",
]
