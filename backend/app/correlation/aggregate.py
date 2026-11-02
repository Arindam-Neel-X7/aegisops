"""Deterministic incident aggregate service for correlation groups."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Self

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from app.anomaly.models import EventSeverity
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    AggregateInputError,
    AggregateStateError,
    IncidentIdentityError,
)
from app.correlation.grouping import CorrelationGroup
from app.correlation.identity import derive_incident_id, derive_timeline_event_id
from app.correlation.models import (
    MAX_REVISION,
    SUPPORTED_INCIDENT_SCHEMA_VERSION,
    SUPPORTED_INCIDENT_SEVERITY_POLICY_VERSION,
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentSignalReference,
    IncidentStatus,
    IncidentTimelineEvent,
    IncidentTimelineEventType,
)

_UTC_OFFSET = timezone.utc.utcoffset(None)

SEVERITY_ORDER = (
    EventSeverity.DEBUG,
    EventSeverity.INFO,
    EventSeverity.WARNING,
    EventSeverity.ERROR,
    EventSeverity.CRITICAL,
)
SEVERITY_RANK = {severity: index for index, severity in enumerate(SEVERITY_ORDER)}

STATE_ORDER = {
    CorrelationWindowState.COLLECTING: 0,
    CorrelationWindowState.SETTLING: 1,
    CorrelationWindowState.FINALIZED: 2,
}


class IncidentAggregationResult(BaseModel):
    """Immutable result contract for deterministic incident aggregation."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    aggregate: IncidentAggregate
    timeline_events: tuple[IncidentTimelineEvent, ...]
    material_update: bool

    @model_validator(mode="after")
    def _validate_events_consistency(self) -> Self:
        if not self.material_update and len(self.timeline_events) != 0:
            raise ValueError(
                "Non-material aggregation result must not contain timeline events"
            )
        return self


def _raise_evidence_error(is_existing: bool) -> None:
    if is_existing:
        raise AggregateStateError(
            "Existing aggregate correlation evidence is not canonical"
        )
    raise AggregateInputError("Correlation group evidence is not canonical")


def _canonical_evidence_map(
    evidence_entries: tuple[CorrelationEvidence, ...],
    member_ids: set[uuid.UUID],
    *,
    is_existing: bool,
) -> dict[tuple[uuid.UUID, uuid.UUID], CorrelationEvidence]:
    """Map and strictly validate correlation evidence pairs as a complete clique."""
    n = len(member_ids)
    expected_count = n * (n - 1) // 2
    if len(evidence_entries) != expected_count:
        _raise_evidence_error(is_existing)

    pair_map: dict[tuple[uuid.UUID, uuid.UUID], CorrelationEvidence] = {}
    for ev in evidence_entries:
        if not ev.accepted:
            _raise_evidence_error(is_existing)
        left = ev.left_signal_id
        right = ev.right_signal_id
        if left not in member_ids or right not in member_ids:
            _raise_evidence_error(is_existing)
        if left == right:
            _raise_evidence_error(is_existing)
        if left.bytes >= right.bytes:
            _raise_evidence_error(is_existing)
        key = (left, right)
        if key in pair_map:
            _raise_evidence_error(is_existing)
        pair_map[key] = ev

    if len(pair_map) != expected_count:
        _raise_evidence_error(is_existing)

    sorted_ids = sorted(member_ids, key=lambda u: u.bytes)
    for i in range(len(sorted_ids)):
        for j in range(i + 1, len(sorted_ids)):
            expected_key = (sorted_ids[i], sorted_ids[j])
            if expected_key not in pair_map:
                _raise_evidence_error(is_existing)

    return pair_map


def _create_timeline_event(
    *,
    incident_id: uuid.UUID,
    revision: int,
    event_type: IncidentTimelineEventType,
    event_time: datetime,
    source_signal_id: uuid.UUID | None,
    summary: str,
    revision_fingerprint: str,
    is_update: bool,
) -> IncidentTimelineEvent:
    """Derive timeline event identity and construct event model with typed error translation."""
    try:
        event_id = derive_timeline_event_id(
            incident_id=incident_id,
            revision=revision,
            event_type=event_type,
            event_time=event_time,
            source_signal_id=source_signal_id,
            revision_fingerprint=revision_fingerprint,
        )
        return IncidentTimelineEvent(
            schema_version=SUPPORTED_INCIDENT_SCHEMA_VERSION,
            event_id=event_id,
            incident_id=incident_id,
            revision=revision,
            event_time=event_time,
            event_type=event_type,
            source_signal_id=source_signal_id,
            summary=summary,
            revision_fingerprint=revision_fingerprint,
        )
    except (IncidentIdentityError, ValidationError, ValueError, TypeError) as exc:
        if is_update:
            raise AggregateStateError(
                "Failed to construct incident timeline event"
            ) from exc
        raise AggregateInputError(
            "Failed to construct incident timeline event"
        ) from exc


def create_or_update_incident(
    *,
    group: CorrelationGroup,
    signal_inputs: Mapping[uuid.UUID, AnomalyCorrelationInput],
    processing_time: datetime,
    desired_correlation_state: CorrelationWindowState,
    existing_aggregate: IncidentAggregate | None = None,
) -> IncidentAggregationResult:
    """Create or update a deterministic incident aggregate from a correlation group."""
    if not isinstance(group, CorrelationGroup):
        raise AggregateInputError(
            "Correlation group must be a CorrelationGroup instance"
        )
    if not isinstance(desired_correlation_state, CorrelationWindowState):
        raise AggregateInputError(
            "Desired correlation state must be a CorrelationWindowState instance"
        )
    if not isinstance(signal_inputs, Mapping):
        raise AggregateInputError("Signal inputs must be a Mapping")
    if isinstance(processing_time, bool) or not isinstance(processing_time, datetime):
        raise AggregateInputError("Processing time must be a datetime instance")
    if (
        processing_time.tzinfo is None
        or processing_time.tzinfo.utcoffset(processing_time) != _UTC_OFFSET
    ):
        raise AggregateInputError(
            "Processing time must be timezone-aware UTC with zero offset"
        )
    if existing_aggregate is not None and not isinstance(
        existing_aggregate, IncidentAggregate
    ):
        raise AggregateInputError(
            "Existing aggregate must be an IncidentAggregate instance or None"
        )

    if len(group.members) == 0:
        raise AggregateInputError("Correlation group members cannot be empty")

    group_signal_ids = {m.signal_id for m in group.members}
    if len(group_signal_ids) != len(group.members):
        raise AggregateInputError(
            "Correlation group members must have unique signal IDs"
        )

    for member in group.members:
        if member.signal_id not in signal_inputs:
            raise AggregateInputError(
                "Signal input missing for correlation group member"
            )
        authoritative = signal_inputs[member.signal_id]
        if not isinstance(authoritative, AnomalyCorrelationInput):
            raise AggregateInputError(
                "Signal input must be an AnomalyCorrelationInput instance"
            )
        sig = authoritative.signal
        if sig.signal_id != member.signal_id:
            raise AggregateInputError("Signal input signal_id mismatch with member")
        if authoritative.semantic_fingerprint != member.semantic_fingerprint:
            raise AggregateInputError(
                "Signal input semantic fingerprint mismatch with member"
            )
        if sig.service != member.service:
            raise AggregateInputError("Signal input service mismatch with member")
        if sig.event_time != member.event_time:
            raise AggregateInputError("Signal input event_time mismatch with member")

    first_input = signal_inputs[group.members[0].signal_id]
    group_tenant_id = first_input.signal.tenant_id
    group_environment = first_input.signal.environment

    for member in group.members:
        inp = signal_inputs[member.signal_id]
        if (
            inp.signal.tenant_id != group_tenant_id
            or inp.signal.environment != group_environment
        ):
            raise AggregateInputError(
                "All group members must share a single tenant and environment"
            )

    incoming_evidence_map = _canonical_evidence_map(
        group.accepted_evidence,
        group_signal_ids,
        is_existing=False,
    )

    candidate_references_by_id: dict[uuid.UUID, IncidentSignalReference] = {}
    for member in group.members:
        sig = signal_inputs[member.signal_id].signal
        try:
            ref = IncidentSignalReference(
                signal_id=sig.signal_id,
                semantic_fingerprint=member.semantic_fingerprint,
                tenant_id=sig.tenant_id,
                environment=sig.environment,
                service=sig.service,
                event_time=sig.event_time,
                severity=sig.severity,
                anomaly_score=sig.anomaly_score,
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise AggregateInputError(
                "Signal input cannot be projected into incident membership"
            ) from exc
        candidate_references_by_id[sig.signal_id] = ref

    all_references = tuple(candidate_references_by_id.values())
    candidate_severity = max(
        (ref.severity for ref in all_references),
        key=SEVERITY_RANK.__getitem__,
    )

    if existing_aggregate is not None:
        if (
            existing_aggregate.tenant_id != group_tenant_id
            or existing_aggregate.environment != group_environment
        ):
            raise AggregateInputError(
                "Aggregation inputs tenant or environment does not match existing aggregate"
            )
        if existing_aggregate.schema_version != SUPPORTED_INCIDENT_SCHEMA_VERSION:
            raise AggregateStateError(
                "Existing aggregate schema version is unsupported"
            )
        if (
            existing_aggregate.severity_policy_version
            != SUPPORTED_INCIDENT_SEVERITY_POLICY_VERSION
        ):
            raise AggregateStateError(
                "Existing aggregate severity policy version is unsupported"
            )

        expected_existing_services = tuple(
            sorted(
                {
                    reference.service
                    for reference in existing_aggregate.signal_references
                }
            )
        )
        if existing_aggregate.affected_services != expected_existing_services:
            raise AggregateStateError(
                "Existing aggregate affected services are not canonical"
            )

        existing_member_times = [
            ref.event_time for ref in existing_aggregate.signal_references
        ]
        min_existing_time = min(existing_member_times)
        max_existing_time = max(existing_member_times)
        if (
            existing_aggregate.started_at != min_existing_time
            or existing_aggregate.last_seen_at != max_existing_time
        ):
            raise AggregateStateError(
                "Existing aggregate time bounds do not match members"
            )

        existing_policy_severity = max(
            (ref.severity for ref in existing_aggregate.signal_references),
            key=SEVERITY_RANK.__getitem__,
        )
        if existing_aggregate.severity != existing_policy_severity:
            raise AggregateStateError(
                "Existing aggregate severity is inconsistent with severity policy"
            )

        founding_existing = min(
            existing_aggregate.signal_references,
            key=lambda ref: (ref.event_time, ref.signal_id.bytes),
        )
        try:
            derived_existing_id = derive_incident_id(
                tenant_id=existing_aggregate.tenant_id,
                environment=existing_aggregate.environment,
                anchor_signal_fingerprint=founding_existing.semantic_fingerprint,
            )
        except (IncidentIdentityError, ValueError, TypeError) as exc:
            raise AggregateStateError(
                "Failed to validate existing incident identity"
            ) from exc

        if derived_existing_id != existing_aggregate.incident_id:
            raise AggregateStateError(
                "Existing aggregate incident id does not match founding signal"
            )
        if (
            existing_aggregate.incident_number
            != f"INC-{derived_existing_id.hex.upper()}"
        ):
            raise AggregateStateError(
                "Existing aggregate incident number does not match founding signal"
            )
        if existing_aggregate.primary_service != founding_existing.service:
            raise AggregateStateError(
                "Existing aggregate primary service does not match founding signal"
            )

        existing_member_ids = {
            ref.signal_id for ref in existing_aggregate.signal_references
        }
        existing_evidence_map = _canonical_evidence_map(
            existing_aggregate.correlation_evidence,
            existing_member_ids,
            is_existing=True,
        )

        for pair_key, existing_ev in existing_evidence_map.items():
            if (
                pair_key not in incoming_evidence_map
                or incoming_evidence_map[pair_key] != existing_ev
            ):
                raise AggregateStateError(
                    "Existing correlation evidence cannot be removed or modified"
                )

        if (
            STATE_ORDER[desired_correlation_state]
            < STATE_ORDER[existing_aggregate.correlation_state]
        ):
            raise AggregateStateError("Correlation window state cannot regress")

        existing_ref_map = {
            ref.signal_id: ref for ref in existing_aggregate.signal_references
        }
        for existing_sig_id, existing_ref in existing_ref_map.items():
            if existing_sig_id not in candidate_references_by_id:
                raise AggregateStateError(
                    "Existing aggregate members cannot be removed"
                )
            candidate_ref = candidate_references_by_id[existing_sig_id]
            if candidate_ref != existing_ref:
                raise AggregateStateError(
                    "Retained aggregate member attributes cannot be modified"
                )

        if (
            SEVERITY_RANK[candidate_severity]
            < SEVERITY_RANK[existing_aggregate.severity]
        ):
            raise AggregateStateError("Incident aggregate severity cannot regress")

        membership_expanded = len(candidate_references_by_id) > len(
            existing_aggregate.signal_references
        )
        evidence_expanded = len(group.accepted_evidence) > len(
            existing_aggregate.correlation_evidence
        )
        candidate_started_at = min(ref.event_time for ref in all_references)
        candidate_last_seen_at = max(ref.event_time for ref in all_references)
        timespan_expanded = (
            candidate_started_at < existing_aggregate.started_at
            or candidate_last_seen_at > existing_aggregate.last_seen_at
        )
        severity_increased = (
            SEVERITY_RANK[candidate_severity]
            > SEVERITY_RANK[existing_aggregate.severity]
        )
        state_advanced = (
            STATE_ORDER[desired_correlation_state]
            > STATE_ORDER[existing_aggregate.correlation_state]
        )

        is_material_update = (
            membership_expanded
            or evidence_expanded
            or timespan_expanded
            or severity_increased
            or state_advanced
        )

        if not is_material_update:
            return IncidentAggregationResult(
                aggregate=existing_aggregate,
                timeline_events=(),
                material_update=False,
            )

        if processing_time < existing_aggregate.updated_at:
            raise AggregateStateError(
                "Processing time cannot precede existing aggregate updated time"
            )
        if existing_aggregate.revision >= MAX_REVISION:
            raise AggregateStateError(
                "Incident aggregate revision exceeds maximum allowed value"
            )

        new_revision = existing_aggregate.revision + 1
        incident_id = existing_aggregate.incident_id
        incident_number = existing_aggregate.incident_number
        primary_service = existing_aggregate.primary_service
        created_at = existing_aggregate.created_at
        status = existing_aggregate.status
        topology_snapshot = existing_aggregate.topology_snapshot
        blast_radius = existing_aggregate.blast_radius

        affected_services = tuple(sorted({ref.service for ref in all_references}))
        sorted_signal_refs = tuple(
            sorted(all_references, key=lambda r: (r.event_time, r.signal_id.bytes))
        )
        sorted_evidence = tuple(group.accepted_evidence)

        try:
            aggregate = IncidentAggregate.create(
                schema_version=SUPPORTED_INCIDENT_SCHEMA_VERSION,
                incident_id=incident_id,
                incident_number=incident_number,
                tenant_id=group_tenant_id,
                environment=group_environment,
                status=status,
                correlation_state=desired_correlation_state,
                severity=candidate_severity,
                severity_policy_version=SUPPORTED_INCIDENT_SEVERITY_POLICY_VERSION,
                started_at=candidate_started_at,
                last_seen_at=candidate_last_seen_at,
                created_at=created_at,
                updated_at=processing_time,
                revision=new_revision,
                primary_service=primary_service,
                affected_services=affected_services,
                signal_references=sorted_signal_refs,
                correlation_evidence=sorted_evidence,
                topology_snapshot=topology_snapshot,
                blast_radius=blast_radius,
            )
        except (IncidentIdentityError, ValidationError, ValueError, TypeError) as exc:
            raise AggregateStateError(
                "Aggregate update violated model invariants"
            ) from exc

        events: list[IncidentTimelineEvent] = []
        newly_added_members = [
            m for m in group.members if m.signal_id not in existing_ref_map
        ]
        newly_added_members.sort(key=lambda m: m.signal_id.bytes)
        for m in newly_added_members:
            events.append(
                _create_timeline_event(
                    incident_id=incident_id,
                    revision=new_revision,
                    event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
                    event_time=processing_time,
                    source_signal_id=m.signal_id,
                    summary="Correlated signal attached to incident aggregate",
                    revision_fingerprint=aggregate.revision_fingerprint,
                    is_update=True,
                )
            )

        if severity_increased:
            escalating_candidates = [
                m
                for m in newly_added_members
                if signal_inputs[m.signal_id].signal.severity == candidate_severity
            ]
            if escalating_candidates:
                source_sig: uuid.UUID | None = min(
                    escalating_candidates, key=lambda m: m.signal_id.bytes
                ).signal_id
            else:
                source_sig = None

            events.append(
                _create_timeline_event(
                    incident_id=incident_id,
                    revision=new_revision,
                    event_type=IncidentTimelineEventType.SEVERITY_CHANGED,
                    event_time=processing_time,
                    source_signal_id=source_sig,
                    summary="Incident aggregate severity escalated",
                    revision_fingerprint=aggregate.revision_fingerprint,
                    is_update=True,
                )
            )

        if (
            existing_aggregate.correlation_state != CorrelationWindowState.FINALIZED
            and desired_correlation_state == CorrelationWindowState.FINALIZED
        ):
            events.append(
                _create_timeline_event(
                    incident_id=incident_id,
                    revision=new_revision,
                    event_type=IncidentTimelineEventType.CORRELATION_FINALIZED,
                    event_time=processing_time,
                    source_signal_id=None,
                    summary="Correlation aggregation window finalized",
                    revision_fingerprint=aggregate.revision_fingerprint,
                    is_update=True,
                )
            )
        elif (
            existing_aggregate.correlation_state == CorrelationWindowState.COLLECTING
            and desired_correlation_state == CorrelationWindowState.SETTLING
        ):
            events.append(
                _create_timeline_event(
                    incident_id=incident_id,
                    revision=new_revision,
                    event_type=IncidentTimelineEventType.CORRELATION_STATE_CHANGED,
                    event_time=processing_time,
                    source_signal_id=None,
                    summary="Correlation aggregation window advanced to settling",
                    revision_fingerprint=aggregate.revision_fingerprint,
                    is_update=True,
                )
            )

        return IncidentAggregationResult(
            aggregate=aggregate,
            timeline_events=tuple(events),
            material_update=True,
        )

    # Initial creation
    anchor_member = min(
        group.members,
        key=lambda member: (member.event_time, member.signal_id.bytes),
    )
    try:
        incident_id = derive_incident_id(
            tenant_id=group_tenant_id,
            environment=group_environment,
            anchor_signal_fingerprint=anchor_member.semantic_fingerprint,
        )
    except (IncidentIdentityError, ValueError, TypeError) as exc:
        raise AggregateInputError("Failed to derive incident identity") from exc

    incident_number = f"INC-{incident_id.hex.upper()}"
    primary_service = anchor_member.service

    candidate_started_at = min(ref.event_time for ref in all_references)
    candidate_last_seen_at = max(ref.event_time for ref in all_references)
    affected_services = tuple(sorted({ref.service for ref in all_references}))
    sorted_signal_refs = tuple(
        sorted(all_references, key=lambda r: (r.event_time, r.signal_id.bytes))
    )
    sorted_evidence = tuple(group.accepted_evidence)

    try:
        aggregate = IncidentAggregate.create(
            schema_version=SUPPORTED_INCIDENT_SCHEMA_VERSION,
            incident_id=incident_id,
            incident_number=incident_number,
            tenant_id=group_tenant_id,
            environment=group_environment,
            status=IncidentStatus.OPEN,
            correlation_state=desired_correlation_state,
            severity=candidate_severity,
            severity_policy_version=SUPPORTED_INCIDENT_SEVERITY_POLICY_VERSION,
            started_at=candidate_started_at,
            last_seen_at=candidate_last_seen_at,
            created_at=processing_time,
            updated_at=processing_time,
            revision=1,
            primary_service=primary_service,
            affected_services=affected_services,
            signal_references=sorted_signal_refs,
            correlation_evidence=sorted_evidence,
            topology_snapshot=None,
            blast_radius=None,
        )
    except (IncidentIdentityError, ValidationError, ValueError, TypeError) as exc:
        raise AggregateInputError(
            "Aggregate creation violated model invariants"
        ) from exc

    events = []
    events.append(
        _create_timeline_event(
            incident_id=incident_id,
            revision=1,
            event_type=IncidentTimelineEventType.CREATED,
            event_time=processing_time,
            source_signal_id=anchor_member.signal_id,
            summary="Incident aggregate initialized from founding correlation anchor signal",
            revision_fingerprint=aggregate.revision_fingerprint,
            is_update=False,
        )
    )

    non_anchor_members = [
        m for m in group.members if m.signal_id != anchor_member.signal_id
    ]
    non_anchor_members.sort(key=lambda m: m.signal_id.bytes)
    for m in non_anchor_members:
        events.append(
            _create_timeline_event(
                incident_id=incident_id,
                revision=1,
                event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
                event_time=processing_time,
                source_signal_id=m.signal_id,
                summary="Correlated signal attached to incident aggregate",
                revision_fingerprint=aggregate.revision_fingerprint,
                is_update=False,
            )
        )

    if desired_correlation_state == CorrelationWindowState.FINALIZED:
        events.append(
            _create_timeline_event(
                incident_id=incident_id,
                revision=1,
                event_type=IncidentTimelineEventType.CORRELATION_FINALIZED,
                event_time=processing_time,
                source_signal_id=None,
                summary="Correlation aggregation window finalized",
                revision_fingerprint=aggregate.revision_fingerprint,
                is_update=False,
            )
        )

    return IncidentAggregationResult(
        aggregate=aggregate,
        timeline_events=tuple(events),
        material_update=True,
    )
