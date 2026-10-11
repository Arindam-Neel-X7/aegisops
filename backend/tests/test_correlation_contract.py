from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.correlation.models import (
    SUPPORTED_INCIDENT_SCHEMA_VERSION,
    BlastRadiusSummary,
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentHandoff,
    IncidentSignalReference,
    IncidentStatus,
    IncidentTimelineEvent,
    IncidentTimelineEventType,
    TopologySnapshotReference,
)

# ruff: noqa: F401 - aggregate_semantic_projection and compute_revision_fingerprint kept for spec coverage
from app.correlation.models import aggregate_semantic_projection  # noqa: F401
from app.correlation.identity import (  # noqa: F401
    compute_revision_fingerprint,
    derive_handoff_id,
    derive_incident_id,
    derive_timeline_event_id,
)
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    deserialize_incident_handoff,
    deserialize_incident_timeline_event,
    serialize_incident_aggregate,
    serialize_incident_handoff,
    serialize_incident_timeline_event,
)
from app.telemetry.schemas import EventSeverity

_UTC_NOW = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
_FP = "a" * 64
_FP2 = "b" * 64
_FP3 = "c" * 64


# ---------- helpers ----------


def _tenant_id() -> uuid.UUID:
    return uuid.UUID("11111111-1111-1111-1111-111111111111")


def _signal_ref(**overrides) -> IncidentSignalReference:
    base: dict[str, object] = {
        "signal_id": uuid.UUID("22222222-2222-2222-2222-222222222222"),
        "semantic_fingerprint": _FP,
        "tenant_id": _tenant_id(),
        "environment": "prod",
        "service": "order-service",
        "event_time": _UTC_NOW,
        "severity": EventSeverity.ERROR,
    }
    base.update(overrides)
    return IncidentSignalReference.model_validate(base)


def _topology_ref(**overrides) -> TopologySnapshotReference:
    base: dict[str, object] = {
        "topology_id": uuid.UUID("33333333-3333-3333-3333-333333333333"),
        "topology_version": "1.0.0",
        "snapshot_fingerprint": _FP2,
        "captured_at": _UTC_NOW,
        "provider": "canonical",
    }
    base.update(overrides)
    return TopologySnapshotReference.model_validate(base)


def _evidence(**overrides) -> CorrelationEvidence:
    base: dict[str, object] = {
        "left_signal_id": uuid.UUID("44444444-4444-4444-4444-444444444444"),
        "right_signal_id": uuid.UUID("55555555-5555-5555-5555-555555555555"),
        "temporal_score": 0.7,
        "service_score": 0.8,
        "topology_score": 0.6,
        "symptom_score": 0.9,
        "context_score": 0.5,
        "weighted_score": 0.72,
        "accepted": True,
        "policy_version": "1.0",
        "reason": "temporal proximity",
    }
    base.update(overrides)
    return CorrelationEvidence.model_validate(base)


def _blast_radius(**overrides) -> BlastRadiusSummary:
    base: dict[str, object] = {
        "primary_service": "order-service",
        "affected_services": ["order-service", "payment-service"],
        "max_hops": 2,
        "truncated": False,
        "path_summaries": ("order->payment",),
        "topology_snapshot": _topology_ref(),
    }
    base.update(overrides)
    return BlastRadiusSummary.model_validate(base)


def _make_aggregate(**overrides) -> IncidentAggregate:
    sig_a = _signal_ref(
        signal_id=uuid.UUID("66666666-6666-6666-6666-666666666666"),
        semantic_fingerprint="0" * 63 + "1",
    )
    sig_b = _signal_ref(
        signal_id=uuid.UUID("77777777-7777-7777-7777-777777777777"),
        semantic_fingerprint="0" * 63 + "2",
        service="payment-service",
    )
    topo = _topology_ref()
    br = BlastRadiusSummary(
        primary_service="order-service",
        affected_services=("order-service", "payment-service"),  # type: ignore[arg-type]
        max_hops=2,
        truncated=False,
        path_summaries=("order->payment",),
        topology_snapshot=topo,
    )
    base: dict[str, object] = {
        "incident_id": uuid.UUID("88888888-8888-8888-8888-888888888888"),
        "incident_number": "INC-0001",
        "tenant_id": _tenant_id(),
        "environment": "prod",
        "status": IncidentStatus.OPEN,
        "correlation_state": CorrelationWindowState.COLLECTING,
        "severity": EventSeverity.ERROR,
        "started_at": _UTC_NOW,
        "last_seen_at": _UTC_NOW + timedelta(seconds=60),
        "created_at": _UTC_NOW,
        "updated_at": _UTC_NOW + timedelta(seconds=10),
        "revision": 1,
        "primary_service": "order-service",
        "affected_services": ["order-service", "payment-service"],
        "signal_references": [sig_a, sig_b],
        "correlation_evidence": [
            CorrelationEvidence(
                left_signal_id=sig_a.signal_id,
                right_signal_id=sig_b.signal_id,
                temporal_score=0.7,
                service_score=0.8,
                topology_score=0.6,
                symptom_score=0.9,
                context_score=0.5,
                weighted_score=0.72,
                accepted=True,
                policy_version="1.0",
                reason="close in time",
            )
        ],
        "topology_snapshot": topo,
        "blast_radius": br,
    }
    base.update(overrides)
    # If revision_fingerprint is not overridden, use create() path
    if "revision_fingerprint" not in overrides:
        return IncidentAggregate.create(**base)  # type: ignore[arg-type]
    return IncidentAggregate.model_validate(base)


# ---------- 1. valid construction for every required model ----------


def test_valid_incident_signal_reference() -> None:
    r = _signal_ref()
    assert r.semantic_fingerprint == _FP
    assert r.service == "order-service"
    assert r.environment == "prod"
    assert r.severity == EventSeverity.ERROR


def test_valid_correlation_evidence_canonical_order() -> None:
    left = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    right = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    # pass in reverse order — should be canonicalized
    ev = _evidence(left_signal_id=right, right_signal_id=left)
    assert ev.left_signal_id == left
    assert ev.right_signal_id == right


def test_valid_topology_snapshot_reference() -> None:
    t = _topology_ref()
    assert t.topology_version == "1.0.0"
    assert t.provider == "canonical"


def test_valid_blast_radius_summary() -> None:
    b = _blast_radius()
    assert "order-service" in b.affected_services
    assert b.truncated is False


def test_valid_incident_timeline_event() -> None:
    agg = _make_aggregate()
    event_id = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=agg.revision,
        event_type=IncidentTimelineEventType.CREATED,
        event_time=_UTC_NOW,
        source_signal_id=None,
        revision_fingerprint=agg.revision_fingerprint,
    )
    ev = IncidentTimelineEvent.model_validate(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "incident_id": agg.incident_id,
            "revision": 1,
            "event_time": _UTC_NOW,
            "event_type": IncidentTimelineEventType.CREATED,
            "summary": "incident created",
            "revision_fingerprint": agg.revision_fingerprint,
        }
    )
    assert ev.event_type == IncidentTimelineEventType.CREATED


def test_valid_incident_aggregate() -> None:
    agg = _make_aggregate()
    assert agg.revision == 1
    assert len(agg.signal_references) == 2
    assert agg.primary_service in agg.affected_services
    assert agg.schema_version == SUPPORTED_INCIDENT_SCHEMA_VERSION


def test_valid_incident_handoff() -> None:
    agg = _make_aggregate()
    handoff = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=agg.revision,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": agg.revision,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": list(agg.affected_services),
            "signal_references": list(agg.signal_references),
            "topology_snapshot": agg.topology_snapshot,
            "blast_radius": agg.blast_radius,
            "generated_at": _UTC_NOW,
        }
    )
    assert handoff.incident_id == agg.incident_id


# ---------- 2. Python and JSON equality round trips ----------


def test_python_and_json_round_trip_signal_reference() -> None:
    original = _signal_ref()
    python_copy = IncidentSignalReference.model_validate(original.model_dump())
    assert python_copy == original
    json_copy = IncidentSignalReference.model_validate_json(
        IncidentSignalReference.model_dump_json(original)
    )
    assert json_copy == original


def test_python_and_json_round_trip_aggregate() -> None:
    original = _make_aggregate()
    python_copy = IncidentAggregate.model_validate(original.model_dump())
    assert python_copy == original
    assert serialize_incident_aggregate(original) is not None
    json_raw = serialize_incident_aggregate(original)
    json_copy = deserialize_incident_aggregate(json_raw)
    assert json_copy == original


def test_python_and_json_round_trip_timeline_event() -> None:
    agg = _make_aggregate()
    event_id = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=1,
        event_type=IncidentTimelineEventType.TOPOLOGY_ENRICHED,
        event_time=_UTC_NOW,
        source_signal_id=None,
        revision_fingerprint=agg.revision_fingerprint,
    )
    event = IncidentTimelineEvent.model_validate(
        {
            "schema_version": "1.0",
            "event_id": event_id,
            "incident_id": agg.incident_id,
            "revision": 1,
            "event_time": _UTC_NOW,
            "event_type": IncidentTimelineEventType.TOPOLOGY_ENRICHED,
            "summary": "topology enriched",
            "revision_fingerprint": agg.revision_fingerprint,
        }
    )
    assert IncidentTimelineEvent.model_validate(event.model_dump()) == event
    assert (
        deserialize_incident_timeline_event(serialize_incident_timeline_event(event))
        == event
    )


def test_python_and_json_round_trip_handoff() -> None:
    agg = _make_aggregate()
    handoff = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=agg.revision,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": agg.revision,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": list(agg.affected_services),
            "signal_references": list(agg.signal_references),
            "generated_at": _UTC_NOW,
        }
    )
    assert IncidentHandoff.model_validate(handoff.model_dump()) == handoff
    assert deserialize_incident_handoff(serialize_incident_handoff(handoff)) == handoff


def test_serialization_round_trip_byte_stability() -> None:
    agg = _make_aggregate()
    raw1 = serialize_incident_aggregate(agg)
    raw2 = serialize_incident_aggregate(agg)
    assert raw1 == raw2


# ---------- 3. frozen-model mutation rejection ----------


def test_frozen_signal_reference_mutation_rejected() -> None:
    ref = _signal_ref()
    with pytest.raises(ValidationError):
        ref.service = "tampered"  # type: ignore[misc]


def test_frozen_evidence_mutation_rejected() -> None:
    ev = _evidence()
    with pytest.raises(ValidationError):
        ev.accepted = False  # type: ignore[misc]


def test_frozen_aggregate_mutation_rejected() -> None:
    agg = _make_aggregate()
    with pytest.raises(ValidationError):
        agg.revision = 99  # type: ignore[misc]


def test_frozen_handoff_mutation_rejected() -> None:
    agg = _make_aggregate()
    handoff = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=agg.revision,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": agg.revision,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": list(agg.affected_services),
            "signal_references": list(agg.signal_references),
            "generated_at": _UTC_NOW,
        }
    )
    with pytest.raises(ValidationError):
        handoff.incident_revision = 2  # type: ignore[misc]


# ---------- 4. unknown-field rejection at every model level ----------


def test_unknown_field_rejected_signal_reference() -> None:
    raw = _signal_ref().model_dump(mode="json")
    raw["unknown_field"] = "evil"
    with pytest.raises(ValidationError):
        IncidentSignalReference.model_validate(raw)


def test_unknown_field_rejected_evidence() -> None:
    raw = _evidence().model_dump(mode="json")
    raw["unknown_field"] = "evil"
    with pytest.raises(ValidationError):
        CorrelationEvidence.model_validate(raw)


def test_unknown_field_rejected_topology_snapshot() -> None:
    raw = _topology_ref().model_dump(mode="json")
    raw["unknown_field"] = "evil"
    with pytest.raises(ValidationError):
        TopologySnapshotReference.model_validate(raw)


def test_unknown_field_rejected_blast_radius() -> None:
    raw = _blast_radius().model_dump(mode="json")
    raw["unknown_field"] = "evil"
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(raw)


def test_unknown_field_rejected_aggregate() -> None:
    agg = _make_aggregate()
    raw = agg.model_dump(mode="json")
    raw["unknown_field"] = "evil"
    with pytest.raises(ValidationError):
        IncidentAggregate.model_validate(raw)


def test_unknown_field_rejected_timeline_event() -> None:
    agg = _make_aggregate()
    event_id = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=1,
        event_type=IncidentTimelineEventType.CREATED,
        event_time=_UTC_NOW,
        source_signal_id=None,
        revision_fingerprint=agg.revision_fingerprint,
    )
    raw = {
        "schema_version": "1.0",
        "event_id": str(event_id),
        "incident_id": str(agg.incident_id),
        "revision": 1,
        "event_time": _UTC_NOW.isoformat(),
        "event_type": "created",
        "summary": "test",
        "revision_fingerprint": agg.revision_fingerprint,
        "unknown_field": "evil",
    }
    with pytest.raises(ValidationError):
        IncidentTimelineEvent.model_validate(raw)


def test_unknown_field_rejected_handoff() -> None:
    agg = _make_aggregate()
    handoff = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=agg.revision,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": agg.revision,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": list(agg.affected_services),
            "signal_references": list(agg.signal_references),
            "generated_at": _UTC_NOW,
        }
    )
    raw = handoff.model_dump(mode="json")
    raw["unknown_field"] = "evil"
    with pytest.raises(ValidationError):
        IncidentHandoff.model_validate(raw)


# ---------- 5. naive/non-UTC datetime rejection and JSON-mode UTC acceptance ----------


def test_naive_datetime_rejected() -> None:
    naive = datetime(2026, 9, 30, 12, 0, 0)  # naive
    with pytest.raises(ValidationError):
        IncidentSignalReference.model_validate(
            {
                "signal_id": uuid.uuid4(),
                "semantic_fingerprint": _FP,
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "service": "svc",
                "event_time": naive,
                "severity": EventSeverity.ERROR,
            }
        )


def test_non_utc_datetime_rejected() -> None:
    plus5 = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    with pytest.raises(ValidationError):
        IncidentSignalReference.model_validate(
            {
                "signal_id": uuid.uuid4(),
                "semantic_fingerprint": _FP,
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "service": "svc",
                "event_time": plus5,
                "severity": EventSeverity.ERROR,
            }
        )


def test_json_mode_utc_acceptance() -> None:
    iso_utc = "2026-09-30T12:00:00+00:00"
    raw = {
        "signal_id": str(uuid.uuid4()),
        "semantic_fingerprint": _FP,
        "tenant_id": str(_tenant_id()),
        "environment": "prod",
        "service": "svc",
        "event_time": iso_utc,
        "severity": "error",
    }
    # model_validate_json should accept the ISO string
    obj = IncidentSignalReference.model_validate_json(json.dumps(raw))
    assert obj.event_time.tzinfo is not None


def test_python_string_not_accepted_in_model_validate() -> None:
    # Passing a raw ISO string to model_validate (not model_validate_json) should fail for datetime
    with pytest.raises(ValidationError):
        IncidentSignalReference.model_validate(
            {
                "signal_id": uuid.uuid4(),
                "semantic_fingerprint": _FP,
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "service": "svc",
                "event_time": "2026-09-30T12:00:00+00:00",  # string, not datetime in Python mode
                "severity": EventSeverity.ERROR,
            }
        )


# ---------- 6. boolean rejection for every numeric field ----------


@pytest.mark.parametrize(
    "field",
    [
        "temporal_score",
        "service_score",
        "topology_score",
        "symptom_score",
        "context_score",
        "weighted_score",
    ],
)
def test_boolean_rejected_for_score(field: str) -> None:
    kwa = {field: True}
    with pytest.raises(ValidationError):
        CorrelationEvidence.model_validate(
            {
                "left_signal_id": uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                "right_signal_id": uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                "temporal_score": 0.5,
                "service_score": 0.5,
                "topology_score": 0.5,
                "symptom_score": 0.5,
                "context_score": 0.5,
                "weighted_score": 0.5,
                "accepted": True,
                "policy_version": "1.0",
                "reason": "test",
                **kwa,
            }
        )


def test_boolean_rejected_for_revision() -> None:
    with pytest.raises(ValidationError):
        IncidentTimelineEvent.model_validate(
            {
                "schema_version": "1.0",
                "event_id": uuid.uuid4(),
                "incident_id": uuid.uuid4(),
                "revision": True,  # bool
                "event_time": _UTC_NOW,
                "event_type": IncidentTimelineEventType.CREATED,
                "summary": "test",
                "revision_fingerprint": _FP,
            }
        )


def test_boolean_rejected_for_max_hops() -> None:
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc",
                "affected_services": ["svc"],
                "max_hops": True,  # bool
                "truncated": False,
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )


# ---------- 7. NaN, positive infinity, negative infinity rejection ----------


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_score_rejected(bad: float) -> None:
    with pytest.raises(ValidationError):
        CorrelationEvidence.model_validate(
            {
                "left_signal_id": uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                "right_signal_id": uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                "temporal_score": bad,
                "service_score": 0.5,
                "topology_score": 0.5,
                "symptom_score": 0.5,
                "context_score": 0.5,
                "weighted_score": 0.5,
                "accepted": True,
                "policy_version": "1.0",
                "reason": "test",
            }
        )


# ---------- 8. oversized/blank strings and empty/duplicate/oversized collections ----------


def test_blank_environment_rejected() -> None:
    with pytest.raises(ValidationError):
        _signal_ref(environment="  ")


def test_oversized_environment_rejected() -> None:
    with pytest.raises(ValidationError):
        _signal_ref(environment="x" * 129)


def test_text_boundary_environment_max_and_adjacent() -> None:
    from app.correlation.models import MAX_ENV_SERVICE_LEN

    assert _signal_ref(environment="x" * MAX_ENV_SERVICE_LEN).environment == (
        "x" * MAX_ENV_SERVICE_LEN
    )
    with pytest.raises(ValidationError):
        _signal_ref(environment="x" * (MAX_ENV_SERVICE_LEN + 1))
    with pytest.raises(ValidationError):
        _signal_ref(environment="")
    with pytest.raises(ValidationError):
        _signal_ref(environment="   ")
    assert _signal_ref(environment="  prod  ").environment == "prod"


def test_text_boundary_service_max_and_adjacent() -> None:
    from app.correlation.models import MAX_ENV_SERVICE_LEN

    assert _signal_ref(service="x" * MAX_ENV_SERVICE_LEN).service == (
        "x" * MAX_ENV_SERVICE_LEN
    )
    with pytest.raises(ValidationError):
        _signal_ref(service="x" * (MAX_ENV_SERVICE_LEN + 1))
    with pytest.raises(ValidationError):
        _signal_ref(service="   ")
    assert _signal_ref(service="  svc  ").service == "svc"


def test_text_boundary_primary_service_max_and_adjacent() -> None:
    from app.correlation.models import MAX_ENV_SERVICE_LEN

    model = BlastRadiusSummary.model_validate(
        {
            "primary_service": "x" * MAX_ENV_SERVICE_LEN,
            "affected_services": ["x" * MAX_ENV_SERVICE_LEN],
            "max_hops": 1,
            "truncated": False,
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert model.primary_service == "x" * MAX_ENV_SERVICE_LEN
    trimmed = BlastRadiusSummary.model_validate(
        {
            "primary_service": "  value  ",
            "affected_services": ["value"],
            "max_hops": 1,
            "truncated": False,
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert trimmed.primary_service == "value"
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "x" * (MAX_ENV_SERVICE_LEN + 1),
                "affected_services": ["svc"],
                "max_hops": 1,
                "truncated": False,
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "   ",
                "affected_services": ["svc"],
                "max_hops": 1,
                "truncated": False,
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )


def test_text_boundary_provider_max_and_adjacent() -> None:
    from app.correlation.models import MAX_PROVIDER_LEN

    assert (
        _topology_ref(provider="x" * MAX_PROVIDER_LEN).provider
        == "x" * MAX_PROVIDER_LEN
    )
    assert _topology_ref(provider="  value  ").provider == "value"
    with pytest.raises(ValidationError):
        _topology_ref(provider="x" * (MAX_PROVIDER_LEN + 1))
    with pytest.raises(ValidationError):
        _topology_ref(provider="  ")


def test_text_boundary_policy_version_max_and_adjacent() -> None:
    from app.correlation.models import MAX_POLICY_LEN

    assert (
        _evidence(policy_version="x" * MAX_POLICY_LEN).policy_version
        == "x" * MAX_POLICY_LEN
    )
    assert _evidence(policy_version="  value  ").policy_version == "value"
    with pytest.raises(ValidationError):
        _evidence(policy_version="x" * (MAX_POLICY_LEN + 1))
    with pytest.raises(ValidationError):
        _evidence(policy_version="  ")


def test_text_boundary_reason_max_and_adjacent() -> None:
    from app.correlation.models import MAX_REASON_LEN

    assert _evidence(reason="x" * MAX_REASON_LEN).reason == "x" * MAX_REASON_LEN
    with pytest.raises(ValidationError):
        _evidence(reason="x" * (MAX_REASON_LEN + 1))
    with pytest.raises(ValidationError):
        _evidence(reason="   ")
    assert _evidence(reason="  trimmed  ").reason == "trimmed"


def test_text_boundary_topology_version_max_and_adjacent() -> None:
    from app.correlation.models import MAX_TOPOLOGY_VER_LEN

    assert (
        _topology_ref(topology_version="x" * MAX_TOPOLOGY_VER_LEN).topology_version
        == "x" * MAX_TOPOLOGY_VER_LEN
    )
    assert _topology_ref(topology_version="  value  ").topology_version == "value"
    with pytest.raises(ValidationError):
        _topology_ref(topology_version="x" * (MAX_TOPOLOGY_VER_LEN + 1))
    with pytest.raises(ValidationError):
        _topology_ref(topology_version="  ")


def test_text_boundary_incident_number_max_and_adjacent() -> None:
    from app.correlation.models import MAX_INCIDENT_NUMBER_LEN

    agg = _make_aggregate(
        incident_number="x" * MAX_INCIDENT_NUMBER_LEN,
    )
    assert agg.incident_number == "x" * MAX_INCIDENT_NUMBER_LEN
    assert _make_aggregate(incident_number="  value  ").incident_number == "value"
    with pytest.raises(ValidationError):
        _make_aggregate(incident_number="x" * (MAX_INCIDENT_NUMBER_LEN + 1))
    with pytest.raises(ValidationError):
        _make_aggregate(incident_number="   ")


def test_text_boundary_timeline_summary_max_and_adjacent() -> None:
    from app.correlation.models import MAX_SUMMARY_LEN

    agg = _make_aggregate()
    ok = IncidentTimelineEvent.model_validate(
        {
            "schema_version": "1.0",
            "event_id": uuid.uuid4(),
            "incident_id": agg.incident_id,
            "revision": 1,
            "event_time": _UTC_NOW,
            "event_type": IncidentTimelineEventType.CREATED,
            "summary": "x" * MAX_SUMMARY_LEN,
            "revision_fingerprint": _FP,
        }
    )
    assert ok.summary == "x" * MAX_SUMMARY_LEN
    trimmed_summary = IncidentTimelineEvent.model_validate(
        {
            "schema_version": "1.0",
            "event_id": uuid.uuid4(),
            "incident_id": agg.incident_id,
            "revision": 1,
            "event_time": _UTC_NOW,
            "event_type": IncidentTimelineEventType.CREATED,
            "summary": "  value  ",
            "revision_fingerprint": _FP,
        }
    )
    assert trimmed_summary.summary == "value"
    with pytest.raises(ValidationError):
        IncidentTimelineEvent.model_validate(
            {
                "schema_version": "1.0",
                "event_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "revision": 1,
                "event_time": _UTC_NOW,
                "event_type": IncidentTimelineEventType.CREATED,
                "summary": "x" * (MAX_SUMMARY_LEN + 1),
                "revision_fingerprint": _FP,
            }
        )
    with pytest.raises(ValidationError):
        IncidentTimelineEvent.model_validate(
            {
                "schema_version": "1.0",
                "event_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "revision": 1,
                "event_time": _UTC_NOW,
                "event_type": IncidentTimelineEventType.CREATED,
                "summary": "   ",
                "revision_fingerprint": _FP,
            }
        )


def test_text_boundary_path_summary_max_and_adjacent() -> None:
    from app.correlation.models import MAX_PATH_SUMMARY_LEN

    ok = BlastRadiusSummary.model_validate(
        {
            "primary_service": "svc",
            "affected_services": ["svc"],
            "max_hops": 1,
            "truncated": False,
            "path_summaries": ["x" * MAX_PATH_SUMMARY_LEN],
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert ok.path_summaries[0] == "x" * MAX_PATH_SUMMARY_LEN
    trimmed_paths = BlastRadiusSummary.model_validate(
        {
            "primary_service": "svc",
            "affected_services": ["svc"],
            "max_hops": 1,
            "truncated": False,
            "path_summaries": ["  value  "],
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert trimmed_paths.path_summaries == ("value",)
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc",
                "affected_services": ["svc"],
                "max_hops": 1,
                "truncated": False,
                "path_summaries": ["x" * (MAX_PATH_SUMMARY_LEN + 1)],
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc",
                "affected_services": ["svc"],
                "max_hops": 1,
                "truncated": False,
                "path_summaries": ["   "],
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )


def test_collection_boundary_signal_references() -> None:
    from app.correlation.models import MAX_SIGNAL_REFS

    def _unique_ref(index: int) -> IncidentSignalReference:
        return IncidentSignalReference.model_validate(
            {
                "signal_id": uuid.uuid5(uuid.NAMESPACE_DNS, f"signal-{index}"),
                "semantic_fingerprint": f"{index:016x}" + "a" * 48,
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "service": "svc",
                "event_time": _UTC_NOW,
                "severity": EventSeverity.ERROR,
            }
        )

    sig = _unique_ref(0)
    ok = _make_aggregate(signal_references=[sig], correlation_evidence=[])
    assert len(ok.signal_references) == 1
    with pytest.raises(ValidationError):
        _make_aggregate(signal_references=[], correlation_evidence=[])
    full = [_unique_ref(index) for index in range(MAX_SIGNAL_REFS)]
    full_ok = _make_aggregate(signal_references=full, correlation_evidence=[])
    assert len(full_ok.signal_references) == MAX_SIGNAL_REFS
    with pytest.raises(ValidationError):
        _make_aggregate(
            signal_references=full + [_unique_ref(MAX_SIGNAL_REFS)],
            correlation_evidence=[],
        )


def test_collection_boundary_correlation_evidence() -> None:
    from app.correlation.models import MAX_EVIDENCE_ENTRIES

    agg = _make_aggregate(correlation_evidence=[])
    assert len(agg.correlation_evidence) == 0
    ev = CorrelationEvidence.model_validate(
        {
            "left_signal_id": agg.signal_references[0].signal_id,
            "right_signal_id": agg.signal_references[1].signal_id,
            "temporal_score": 0.5,
            "service_score": 0.5,
            "topology_score": 0.5,
            "symptom_score": 0.5,
            "context_score": 0.5,
            "weighted_score": 0.5,
            "accepted": True,
            "policy_version": "1.0",
            "reason": "x",
        }
    )
    full = _make_aggregate(correlation_evidence=[ev] * MAX_EVIDENCE_ENTRIES)
    assert len(full.correlation_evidence) == MAX_EVIDENCE_ENTRIES
    with pytest.raises(ValidationError):
        _make_aggregate(correlation_evidence=[ev] * (MAX_EVIDENCE_ENTRIES + 1))


def test_collection_boundary_affected_services() -> None:
    from app.correlation.models import MAX_AFFECTED_SERVICES

    ok = BlastRadiusSummary.model_validate(
        {
            "primary_service": "svc-0000",
            "affected_services": [f"svc-{i:04d}" for i in range(MAX_AFFECTED_SERVICES)],
            "max_hops": 1,
            "truncated": False,
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert len(ok.affected_services) == MAX_AFFECTED_SERVICES
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc",
                "affected_services": [],
                "max_hops": 1,
                "truncated": False,
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc-000",
                "affected_services": [
                    f"svc-{i:04d}" for i in range(MAX_AFFECTED_SERVICES + 1)
                ],
                "max_hops": 1,
                "truncated": False,
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )


def test_collection_boundary_path_summaries() -> None:
    from app.correlation.models import MAX_PATH_SUMMARIES

    ok = BlastRadiusSummary.model_validate(
        {
            "primary_service": "svc",
            "affected_services": ["svc"],
            "max_hops": 1,
            "truncated": False,
            "path_summaries": [],
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert ok.path_summaries == ()
    full = BlastRadiusSummary.model_validate(
        {
            "primary_service": "svc",
            "affected_services": ["svc"],
            "max_hops": 1,
            "truncated": False,
            "path_summaries": [f"p-{i:04d}" for i in range(MAX_PATH_SUMMARIES)],
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    assert len(full.path_summaries) == MAX_PATH_SUMMARIES
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc",
                "affected_services": ["svc"],
                "max_hops": 1,
                "truncated": False,
                "path_summaries": ["x"] * (MAX_PATH_SUMMARIES + 1),
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )


def test_numeric_boundary_revision() -> None:
    from app.correlation.models import MAX_REVISION, MIN_REVISION

    agg = _make_aggregate(revision=MIN_REVISION)
    assert agg.revision == MIN_REVISION
    agg2 = _make_aggregate(revision=MAX_REVISION)
    assert agg2.revision == MAX_REVISION
    with pytest.raises(ValidationError):
        _make_aggregate(revision=MIN_REVISION - 1)
    with pytest.raises(ValidationError):
        _make_aggregate(revision=MAX_REVISION + 1)
    with pytest.raises(ValidationError):
        _make_aggregate(revision=True)


def test_numeric_boundary_handoff_revision() -> None:
    from app.correlation.models import MAX_REVISION, MIN_REVISION

    agg = _make_aggregate()
    ok = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=MIN_REVISION,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": MIN_REVISION,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": list(agg.affected_services),
            "signal_references": list(agg.signal_references),
            "generated_at": _UTC_NOW,
        }
    )
    assert ok.incident_revision == MIN_REVISION
    ok2 = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=MAX_REVISION,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": MAX_REVISION,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": list(agg.affected_services),
            "signal_references": list(agg.signal_references),
            "generated_at": _UTC_NOW,
        }
    )
    assert ok2.incident_revision == MAX_REVISION
    with pytest.raises(ValidationError):
        IncidentHandoff.model_validate(
            {
                "schema_version": "1.0",
                "handoff_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "incident_revision": MIN_REVISION - 1,
                "incident_revision_fingerprint": agg.revision_fingerprint,
                "tenant_id": agg.tenant_id,
                "environment": agg.environment,
                "status": IncidentStatus.OPEN,
                "severity": EventSeverity.ERROR,
                "primary_service": agg.primary_service,
                "affected_services": list(agg.affected_services),
                "signal_references": list(agg.signal_references),
                "generated_at": _UTC_NOW,
            }
        )
    with pytest.raises(ValidationError):
        IncidentHandoff.model_validate(
            {
                "schema_version": "1.0",
                "handoff_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "incident_revision": MAX_REVISION + 1,
                "incident_revision_fingerprint": agg.revision_fingerprint,
                "tenant_id": agg.tenant_id,
                "environment": agg.environment,
                "status": IncidentStatus.OPEN,
                "severity": EventSeverity.ERROR,
                "primary_service": agg.primary_service,
                "affected_services": list(agg.affected_services),
                "signal_references": list(agg.signal_references),
                "generated_at": _UTC_NOW,
            }
        )
    with pytest.raises(ValidationError):
        IncidentHandoff.model_validate(
            {
                "schema_version": "1.0",
                "handoff_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "incident_revision": True,
                "incident_revision_fingerprint": agg.revision_fingerprint,
                "tenant_id": agg.tenant_id,
                "environment": agg.environment,
                "status": IncidentStatus.OPEN,
                "severity": EventSeverity.ERROR,
                "primary_service": agg.primary_service,
                "affected_services": list(agg.affected_services),
                "signal_references": list(agg.signal_references),
                "generated_at": _UTC_NOW,
            }
        )


def test_numeric_boundary_max_hops() -> None:
    from app.correlation.models import MAX_HOPS, MIN_HOPS

    assert _blast_radius(max_hops=MIN_HOPS).max_hops == MIN_HOPS
    assert _blast_radius(max_hops=MAX_HOPS).max_hops == MAX_HOPS
    with pytest.raises(ValidationError):
        _blast_radius(max_hops=MIN_HOPS - 1)
    with pytest.raises(ValidationError):
        _blast_radius(max_hops=MAX_HOPS + 1)
    with pytest.raises(ValidationError):
        _blast_radius(max_hops=True)


def test_numeric_boundary_scores_exact_edges() -> None:
    for field in (
        "temporal_score",
        "service_score",
        "topology_score",
        "symptom_score",
        "context_score",
        "weighted_score",
    ):
        assert _evidence(**{field: 0.0}).__getattribute__(field) == 0.0
        assert _evidence(**{field: 1.0}).__getattribute__(field) == 1.0
        with pytest.raises(ValidationError):
            _evidence(**{field: -0.01})
        with pytest.raises(ValidationError):
            _evidence(**{field: 1.01})
        with pytest.raises(ValidationError):
            _evidence(**{field: 1})
        with pytest.raises(ValidationError):
            _evidence(**{field: True})
        for bad in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValidationError):
                _evidence(**{field: bad})


def test_fingerprint_boundary_exact_edges() -> None:
    agg = _make_aggregate()
    assert (
        IncidentTimelineEvent.model_validate(
            {
                "schema_version": "1.0",
                "event_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "revision": 1,
                "event_time": _UTC_NOW,
                "event_type": IncidentTimelineEventType.CREATED,
                "summary": "ok",
                "revision_fingerprint": "a" * 64,
            }
        ).revision_fingerprint
        == "a" * 64
    )
    for bad in ("a" * 63, "a" * 65, "A" * 64, "g" * 64, "", 123, True):
        with pytest.raises(ValidationError):
            IncidentTimelineEvent.model_validate(
                {
                    "schema_version": "1.0",
                    "event_id": uuid.uuid4(),
                    "incident_id": agg.incident_id,
                    "revision": 1,
                    "event_time": _UTC_NOW,
                    "event_type": IncidentTimelineEventType.CREATED,
                    "summary": "ok",
                    "revision_fingerprint": bad,
                }
            )


def _schema_version_cases() -> list[object]:
    return ["2.0", "", "   ", "x" * 129, 123, True, None]


def test_schema_version_boundary_timeline_event() -> None:
    from app.correlation.models import SUPPORTED_INCIDENT_SCHEMA_VERSION

    agg = _make_aggregate()
    ok = IncidentTimelineEvent.model_validate(
        {
            "schema_version": SUPPORTED_INCIDENT_SCHEMA_VERSION,
            "event_id": uuid.uuid4(),
            "incident_id": agg.incident_id,
            "revision": 1,
            "event_time": _UTC_NOW,
            "event_type": IncidentTimelineEventType.CREATED,
            "summary": "ok",
            "revision_fingerprint": _FP,
        }
    )
    assert ok.schema_version == SUPPORTED_INCIDENT_SCHEMA_VERSION
    for bad in _schema_version_cases():
        with pytest.raises(ValidationError) as exc_info:
            IncidentTimelineEvent.model_validate(
                {
                    "schema_version": bad,
                    "event_id": uuid.uuid4(),
                    "incident_id": agg.incident_id,
                    "revision": 1,
                    "event_time": _UTC_NOW,
                    "event_type": IncidentTimelineEventType.CREATED,
                    "summary": "ok",
                    "revision_fingerprint": _FP,
                }
            )
        assert any(
            "schema_version" in str(error.get("loc", ()))
            for error in exc_info.value.errors()
        )


def test_schema_version_boundary_incident_aggregate() -> None:
    from app.correlation.models import SUPPORTED_INCIDENT_SCHEMA_VERSION

    agg = _make_aggregate()
    assert agg.schema_version == SUPPORTED_INCIDENT_SCHEMA_VERSION
    base = agg.model_dump()
    base["schema_version"] = SUPPORTED_INCIDENT_SCHEMA_VERSION
    assert IncidentAggregate.model_validate(base).schema_version == (
        SUPPORTED_INCIDENT_SCHEMA_VERSION
    )
    for bad in _schema_version_cases():
        candidate = dict(agg.model_dump())
        candidate["schema_version"] = bad
        with pytest.raises(ValidationError) as exc_info:
            IncidentAggregate.model_validate(candidate)
        assert any(
            "schema_version" in str(error.get("loc", ()))
            for error in exc_info.value.errors()
        )


def test_schema_version_boundary_incident_handoff() -> None:
    from app.correlation.models import SUPPORTED_INCIDENT_SCHEMA_VERSION

    agg = _make_aggregate()
    base = {
        "schema_version": SUPPORTED_INCIDENT_SCHEMA_VERSION,
        "handoff_id": derive_handoff_id(
            incident_id=agg.incident_id,
            incident_revision=agg.revision,
            incident_revision_fingerprint=agg.revision_fingerprint,
        ),
        "incident_id": agg.incident_id,
        "incident_revision": agg.revision,
        "incident_revision_fingerprint": agg.revision_fingerprint,
        "tenant_id": agg.tenant_id,
        "environment": agg.environment,
        "status": IncidentStatus.OPEN,
        "severity": EventSeverity.ERROR,
        "primary_service": agg.primary_service,
        "affected_services": list(agg.affected_services),
        "signal_references": list(agg.signal_references),
        "generated_at": _UTC_NOW,
    }
    assert IncidentHandoff.model_validate(base).schema_version == (
        SUPPORTED_INCIDENT_SCHEMA_VERSION
    )
    for bad in _schema_version_cases():
        candidate = dict(base)
        candidate["schema_version"] = bad
        with pytest.raises(ValidationError) as exc_info:
            IncidentHandoff.model_validate(candidate)
        assert any(
            "schema_version" in str(error.get("loc", ()))
            for error in exc_info.value.errors()
        )


def test_snapshot_isolation_blast_radius_collections() -> None:
    topo = _topology_ref()
    affected = ["order-service", "payment-service"]
    paths = ["order->payment"]
    br = BlastRadiusSummary.model_validate(
        {
            "primary_service": "order-service",
            "affected_services": affected,
            "max_hops": 1,
            "truncated": False,
            "path_summaries": paths,
            "topology_snapshot": topo,
        }
    )
    before = br.model_dump_json()
    affected.append("evil-service")
    paths.append("evil-path")
    assert br.affected_services == ("order-service", "payment-service")
    assert br.path_summaries == ("order->payment",)
    assert br.model_dump_json() == before


def test_snapshot_isolation_aggregate_collections() -> None:
    affected = ["order-service", "payment-service"]
    ref_a = _signal_ref()
    ref_b = _signal_ref(signal_id=uuid.uuid4(), semantic_fingerprint="f" * 64)
    refs = [ref_a, ref_b]
    ev = CorrelationEvidence.model_validate(
        {
            "left_signal_id": ref_a.signal_id,
            "right_signal_id": ref_b.signal_id,
            "temporal_score": 0.5,
            "service_score": 0.5,
            "topology_score": 0.5,
            "symptom_score": 0.5,
            "context_score": 0.5,
            "weighted_score": 0.5,
            "accepted": True,
            "policy_version": "1.0",
            "reason": "x",
        }
    )
    evidence = [ev]
    agg = _make_aggregate(
        affected_services=affected,
        signal_references=refs,
        correlation_evidence=evidence,
    )
    before_dump = agg.model_dump()
    before_bytes = serialize_incident_aggregate(agg)
    before_fingerprint = agg.revision_fingerprint
    affected.append("evil-service")
    refs.append(_signal_ref(signal_id=uuid.uuid4(), semantic_fingerprint="e" * 64))
    evidence.append(ev)
    assert agg.affected_services == ("order-service", "payment-service")
    assert agg.signal_references == (ref_a, ref_b)
    assert agg.correlation_evidence == (ev,)
    assert agg.model_dump() == before_dump
    assert agg.revision_fingerprint == before_fingerprint
    assert serialize_incident_aggregate(agg) == before_bytes


def test_snapshot_isolation_handoff_collections() -> None:
    agg = _make_aggregate()
    affected = ["order-service", "payment-service"]
    refs = list(agg.signal_references)
    handoff = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg.incident_id,
                incident_revision=agg.revision,
                incident_revision_fingerprint=agg.revision_fingerprint,
            ),
            "incident_id": agg.incident_id,
            "incident_revision": agg.revision,
            "incident_revision_fingerprint": agg.revision_fingerprint,
            "tenant_id": agg.tenant_id,
            "environment": agg.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg.primary_service,
            "affected_services": affected,
            "signal_references": refs,
            "generated_at": _UTC_NOW,
        }
    )
    before_dump = handoff.model_dump()
    before_bytes = serialize_incident_handoff(handoff)
    affected.append("evil-service")
    refs.append(_signal_ref(signal_id=uuid.uuid4(), semantic_fingerprint="d" * 64))
    assert handoff.affected_services == ("order-service", "payment-service")
    assert handoff.signal_references == tuple(agg.signal_references)
    assert handoff.model_dump() == before_dump
    assert serialize_incident_handoff(handoff) == before_bytes


def test_empty_signal_references_rejected() -> None:
    with pytest.raises(ValidationError):
        IncidentAggregate.model_validate(
            {
                "incident_id": uuid.uuid4(),
                "incident_number": "INC-1",
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "status": IncidentStatus.OPEN,
                "correlation_state": CorrelationWindowState.COLLECTING,
                "severity": EventSeverity.ERROR,
                "started_at": _UTC_NOW,
                "last_seen_at": _UTC_NOW,
                "created_at": _UTC_NOW,
                "updated_at": _UTC_NOW,
                "revision": 1,
                "revision_fingerprint": _FP,
                "primary_service": "svc",
                "affected_services": ["svc"],
                "signal_references": [],  # empty
                "correlation_evidence": [],
            }
        )


def test_duplicate_affected_services_rejected() -> None:
    with pytest.raises(ValidationError):
        BlastRadiusSummary.model_validate(
            {
                "primary_service": "svc",
                "affected_services": ["svc", "svc"],  # duplicate
                "max_hops": 1,
                "truncated": False,
                "topology_snapshot": _topology_ref().model_dump(),
            }
        )


def test_duplicate_signal_fingerprints_rejected() -> None:
    sig_a = _signal_ref()
    sig_b = _signal_ref(
        signal_id=uuid.UUID("99999999-9999-9999-9999-999999999999"),
        semantic_fingerprint=_FP,  # same fingerprint as sig_a
    )
    with pytest.raises(ValidationError):
        _make_aggregate(signal_references=[sig_a, sig_b])


def test_oversized_signal_references_rejected() -> None:
    # build 1025 references (one over the 1024 limit)
    many = [
        IncidentSignalReference.model_validate(
            {
                "signal_id": uuid.uuid4(),
                "semantic_fingerprint": f"{i:016x}" + "a" * 48,
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "service": "svc",
                "event_time": _UTC_NOW,
                "severity": EventSeverity.ERROR,
            }
        )
        for i in range(1025)
    ]
    # need to provide correct fingerprint for aggregate
    with pytest.raises(ValidationError):
        IncidentAggregate.model_validate(
            {
                "incident_id": uuid.uuid4(),
                "incident_number": "INC-1",
                "tenant_id": _tenant_id(),
                "environment": "prod",
                "status": IncidentStatus.OPEN,
                "correlation_state": CorrelationWindowState.COLLECTING,
                "severity": EventSeverity.ERROR,
                "started_at": _UTC_NOW,
                "last_seen_at": _UTC_NOW,
                "created_at": _UTC_NOW,
                "updated_at": _UTC_NOW,
                "revision": 1,
                "revision_fingerprint": _FP,
                "primary_service": "svc",
                "affected_services": ["svc"],
                "signal_references": many,  # oversized
                "correlation_evidence": [],
            }
        )


def test_oversized_summary_rejected() -> None:
    agg = _make_aggregate()
    with pytest.raises(ValidationError):
        IncidentTimelineEvent.model_validate(
            {
                "schema_version": "1.0",
                "event_id": uuid.uuid4(),
                "incident_id": agg.incident_id,
                "revision": 1,
                "event_time": _UTC_NOW,
                "event_type": IncidentTimelineEventType.CREATED,
                "summary": "x" * 513,  # over 512
                "revision_fingerprint": _FP,
            }
        )


# ---------- 9. caller collection mutation cannot alter stored models ----------


def test_caller_list_mutation_does_not_affect_model() -> None:
    raw_list = ["order-service", "payment-service"]
    br = BlastRadiusSummary.model_validate(
        {
            "primary_service": "order-service",
            "affected_services": raw_list,
            "max_hops": 1,
            "truncated": False,
            "topology_snapshot": _topology_ref().model_dump(),
        }
    )
    raw_list.append("evil-service")
    assert "evil-service" not in br.affected_services


# ---------- 10. aggregate temporal, tenancy, membership, evidence, service, fingerprint invariants ----------


def test_aggregate_started_after_last_seen_rejected() -> None:
    with pytest.raises(ValidationError):
        _make_aggregate(started_at=_UTC_NOW + timedelta(hours=1), last_seen_at=_UTC_NOW)


def test_aggregate_created_after_updated_rejected() -> None:
    with pytest.raises(ValidationError):
        _make_aggregate(created_at=_UTC_NOW + timedelta(hours=1), updated_at=_UTC_NOW)


def test_aggregate_signal_tenant_mismatch_rejected() -> None:
    other_tenant = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    bad_ref = _signal_ref(tenant_id=other_tenant)
    with pytest.raises(ValidationError):
        _make_aggregate(signal_references=[bad_ref])


def test_aggregate_evidence_references_non_member_signal() -> None:
    agg = _make_aggregate()
    bogus = CorrelationEvidence.model_validate(
        {
            "left_signal_id": agg.signal_references[0].signal_id,
            "right_signal_id": uuid.uuid4(),  # not a member
            "temporal_score": 0.5,
            "service_score": 0.5,
            "topology_score": 0.5,
            "symptom_score": 0.5,
            "context_score": 0.5,
            "weighted_score": 0.5,
            "accepted": True,
            "policy_version": "1.0",
            "reason": "test",
        }
    )
    with pytest.raises(ValidationError):
        _make_aggregate(correlation_evidence=[bogus])


def test_aggregate_primary_not_in_affected_services_rejected() -> None:
    with pytest.raises(ValidationError):
        _make_aggregate(primary_service="ghost-service")


def test_aggregate_wrong_revision_fingerprint_rejected() -> None:
    agg = _make_aggregate()
    bad = agg.model_dump(mode="json")
    bad["revision_fingerprint"] = _FP  # wrong fingerprint
    with pytest.raises(ValidationError):
        IncidentAggregate.model_validate(bad)


# ---------- 11. business status and window state are independent ----------


def test_status_and_window_state_independent() -> None:
    agg1 = _make_aggregate(
        status=IncidentStatus.OPEN, correlation_state=CorrelationWindowState.COLLECTING
    )
    agg2 = _make_aggregate(
        status=IncidentStatus.OPEN, correlation_state=CorrelationWindowState.FINALIZED
    )
    assert agg1.status == agg2.status
    assert agg1.correlation_state != agg2.correlation_state
    assert IncidentStatus.OPEN != CorrelationWindowState.COLLECTING  # type is distinct


# ---------- 12. forbidden later-phase fields and transport-coordinate fields are rejected ----------


@pytest.mark.parametrize(
    "forbidden",
    [
        "root_cause",
        "root_cause_confidence",
        "rca_confidence",
        "causal_confidence",
        "causal_rank",
        "rag",
        "retrieval",
        "embedding",
        "agent",
        "risk_score",
        "approval",
        "remediation",
        "verification",
        "rollback_plan",
        "kafka_partition",
        "kafka_offset",
        "broker_timestamp",
    ],
)
def test_forbidden_fields_rejected(forbidden: str) -> None:
    raw = _signal_ref().model_dump(mode="json")
    raw[forbidden] = "evil"
    with pytest.raises(ValidationError):
        IncidentSignalReference.model_validate(raw)
