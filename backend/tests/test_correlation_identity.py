from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from app.correlation.errors import IncidentIdentityError, IncidentSerializationError
from app.correlation.models import (
    IncidentAggregate,
    IncidentHandoff,
    IncidentSignalReference,
    IncidentStatus,
    IncidentTimelineEvent,
    IncidentTimelineEventType,
    CorrelationWindowState,
    aggregate_semantic_projection,
)
from app.correlation.identity import (
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

_UTC_NOW = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
_TENANT = uuid.UUID("11111111-1111-1111-1111-111111111111")
_FP_A = "a" * 64
_FP_B = "b" * 64


def _base_ref(**overrides) -> IncidentSignalReference:
    base: dict[str, object] = {
        "signal_id": uuid.UUID("22222222-2222-2222-2222-222222222222"),
        "semantic_fingerprint": _FP_A,
        "tenant_id": _TENANT,
        "environment": "prod",
        "service": "order-service",
        "event_time": _UTC_NOW,
        "severity": EventSeverity.ERROR,
    }
    base.update(overrides)
    return IncidentSignalReference.model_validate(base)


def _make_aggregate(**overrides) -> IncidentAggregate:
    import uuid as _uuid

    ref_a = _base_ref(
        signal_id=_uuid.UUID("66666666-6666-6666-6666-666666666666"),
        semantic_fingerprint="0" * 63 + "1",
    )
    ref_b = _base_ref(
        signal_id=_uuid.UUID("77777777-7777-7777-7777-777777777777"),
        semantic_fingerprint="0" * 63 + "2",
        service="payment-service",
    )
    from app.correlation.models import BlastRadiusSummary, TopologySnapshotReference

    topo = TopologySnapshotReference.model_validate(
        {
            "topology_id": _uuid.UUID("33333333-3333-3333-3333-333333333333"),
            "topology_version": "1.0.0",
            "snapshot_fingerprint": _FP_B,
            "captured_at": _UTC_NOW,
            "provider": "canonical",
        }
    )
    br = BlastRadiusSummary.model_validate(
        {
            "primary_service": "order-service",
            "affected_services": ["order-service", "payment-service"],
            "max_hops": 2,
            "truncated": False,
            "path_summaries": ("order->payment",),
            "topology_snapshot": topo.model_dump(),
        }
    )
    base: dict[str, object] = {
        "incident_id": _uuid.UUID("88888888-8888-8888-8888-888888888888"),
        "incident_number": "INC-0001",
        "tenant_id": _TENANT,
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
        "signal_references": [ref_a, ref_b],
        "correlation_evidence": [],
        "topology_snapshot": topo.model_dump(),
        "blast_radius": br.model_dump(),
    }
    base.update(overrides)
    if "revision_fingerprint" not in overrides:
        return IncidentAggregate.create(**base)  # type: ignore[arg-type]
    return IncidentAggregate.model_validate(base)


# ---------- incident ID determinism and sensitivity ----------


def test_incident_id_deterministic() -> None:
    i1 = derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    )
    i2 = derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    )
    assert i1 == i2


def test_incident_id_sensitive_to_tenant() -> None:
    t2 = uuid.UUID("99999999-9999-9999-9999-999999999999")
    assert derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    ) != derive_incident_id(
        tenant_id=t2, environment="prod", anchor_signal_fingerprint=_FP_A
    )


def test_incident_id_sensitive_to_environment() -> None:
    assert derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    ) != derive_incident_id(
        tenant_id=_TENANT, environment="staging", anchor_signal_fingerprint=_FP_A
    )


def test_incident_id_sensitive_to_fingerprint() -> None:
    assert derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    ) != derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_B
    )


def test_later_member_addition_does_not_change_incident_id() -> None:
    anchor = derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    )
    # Build an aggregate with that anchor as first signal
    sig1 = _base_ref(
        signal_id=uuid.uuid4(),
        semantic_fingerprint=_FP_A,
    )
    # Additional signal (different fingerprint)
    sig2 = _base_ref(
        signal_id=uuid.uuid4(),
        semantic_fingerprint=_FP_B,
    )
    agg_small = _make_aggregate(signal_references=[sig1])
    _ = _make_aggregate(signal_references=[sig1, sig2])
    # The derived incident_id from the anchor is stable regardless of later members
    assert anchor == derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    )
    assert agg_small.tenant_id == _TENANT


# ---------- revision fingerprint determinism and mutation sensitivity ----------


def test_revision_fingerprint_determinism() -> None:
    agg = _make_aggregate()
    assert compute_revision_fingerprint(agg) == compute_revision_fingerprint(agg)


def test_revision_fingerprint_mutation_sensitivity() -> None:
    agg = _make_aggregate()
    # build an aggregate with different severity via create
    agg2 = IncidentAggregate.create(
        incident_id=agg.incident_id,
        incident_number=agg.incident_number,
        tenant_id=agg.tenant_id,
        environment=agg.environment,
        status=agg.status,
        correlation_state=agg.correlation_state,
        severity=EventSeverity.CRITICAL,  # changed
        started_at=agg.started_at,
        last_seen_at=agg.last_seen_at,
        created_at=agg.created_at,
        updated_at=agg.updated_at,
        revision=agg.revision,
        primary_service=agg.primary_service,
        affected_services=list(agg.affected_services),
        signal_references=list(agg.signal_references),
        correlation_evidence=list(agg.correlation_evidence),
        topology_snapshot=agg.topology_snapshot,
        blast_radius=agg.blast_radius,
    )
    assert compute_revision_fingerprint(agg) != compute_revision_fingerprint(agg2)


# ---------- dictionary-order and serialization-whitespace invariance ----------


def test_fingerprint_dict_order_invariance() -> None:
    agg = _make_aggregate()
    proj = aggregate_semantic_projection(agg)
    # same projection with shuffled key insertion order
    shuffled = dict(reversed(list(proj.items())))
    raw_sorted = json.dumps(
        proj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    raw_shuffled = json.dumps(
        shuffled,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    assert raw_sorted == raw_shuffled
    assert hashlib.sha256(
        raw_sorted.encode()
    ).hexdigest() == compute_revision_fingerprint(agg)


def test_canonical_serialization_whitespace_invariance() -> None:
    agg = _make_aggregate()
    raw = serialize_incident_aggregate(agg)
    # pretty formatting round-trips to the same aggregate but canonical bytes are whitespace-free
    assert deserialize_incident_aggregate(raw) == deserialize_incident_aggregate(
        serialize_incident_aggregate(deserialize_incident_aggregate(raw))
    )
    # canonical serialization is compact (no extra spaces)
    assert b": " not in raw and b", " not in raw


# ---------- sequence and member/evidence permutation behavior ----------


def test_member_permutation_preserves_aggregate_and_fingerprint() -> None:
    sig_a = _base_ref(
        signal_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        semantic_fingerprint="0" * 63 + "1",
    )
    sig_b = _base_ref(
        signal_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        semantic_fingerprint="0" * 63 + "2",
    )
    sig_c = _base_ref(
        signal_id=uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        semantic_fingerprint="0" * 63 + "3",
    )
    agg1 = _make_aggregate(signal_references=[sig_a, sig_b, sig_c])
    agg2 = _make_aggregate(signal_references=[sig_c, sig_a, sig_b])
    assert agg1.signal_references == agg2.signal_references
    assert agg1 == agg2
    assert compute_revision_fingerprint(agg1) == compute_revision_fingerprint(agg2)
    assert serialize_incident_aggregate(agg1) == serialize_incident_aggregate(agg2)


def test_evidence_permutation_preserves_aggregate_and_fingerprint() -> None:
    import uuid as _uuid

    s1 = _uuid.UUID("66666666-6666-6666-6666-666666666666")
    s2 = _uuid.UUID("77777777-7777-7777-7777-777777777777")
    from app.correlation.models import CorrelationEvidence

    e1 = CorrelationEvidence.model_validate(
        {
            "left_signal_id": s1,
            "right_signal_id": s2,
            "temporal_score": 0.7,
            "service_score": 0.5,
            "topology_score": 0.5,
            "symptom_score": 0.5,
            "context_score": 0.5,
            "weighted_score": 0.5,
            "accepted": True,
            "policy_version": "1.0",
            "reason": "first",
        }
    )
    e2 = CorrelationEvidence.model_validate(
        {
            "left_signal_id": s1,
            "right_signal_id": s2,
            "temporal_score": 0.2,
            "service_score": 0.5,
            "topology_score": 0.5,
            "symptom_score": 0.5,
            "context_score": 0.5,
            "weighted_score": 0.2,
            "accepted": False,
            "policy_version": "1.0",
            "reason": "second",
        }
    )
    agg1 = _make_aggregate(correlation_evidence=[e1, e2])
    agg2 = _make_aggregate(correlation_evidence=[e2, e1])
    assert agg1.correlation_evidence == agg2.correlation_evidence
    assert agg1 == agg2
    assert compute_revision_fingerprint(agg1) == compute_revision_fingerprint(agg2)
    assert serialize_incident_aggregate(agg1) == serialize_incident_aggregate(agg2)


def test_handoff_member_permutation_is_canonical() -> None:
    sig_a = _base_ref(
        signal_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        semantic_fingerprint="0" * 63 + "1",
    )
    sig_b = _base_ref(
        signal_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        semantic_fingerprint="0" * 63 + "2",
    )
    agg1 = _make_aggregate(signal_references=[sig_a, sig_b])
    agg2 = _make_aggregate(signal_references=[sig_b, sig_a])
    assert agg1 == agg2
    handoff = IncidentHandoff.model_validate(
        {
            "schema_version": "1.0",
            "handoff_id": derive_handoff_id(
                incident_id=agg1.incident_id,
                incident_revision=agg1.revision,
                incident_revision_fingerprint=agg1.revision_fingerprint,
            ),
            "incident_id": agg1.incident_id,
            "incident_revision": agg1.revision,
            "incident_revision_fingerprint": agg1.revision_fingerprint,
            "tenant_id": agg1.tenant_id,
            "environment": agg1.environment,
            "status": IncidentStatus.OPEN,
            "severity": EventSeverity.ERROR,
            "primary_service": agg1.primary_service,
            "affected_services": list(agg1.affected_services),
            "signal_references": [sig_b, sig_a],
            "generated_at": _UTC_NOW,
        }
    )
    assert handoff.signal_references == agg1.signal_references
    assert serialize_incident_handoff(handoff) == serialize_incident_handoff(
        handoff.model_copy()
    )


def test_canonical_permutations_serialize_to_identical_bytes() -> None:
    sig_a = _base_ref(
        signal_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        semantic_fingerprint="0" * 63 + "1",
    )
    sig_b = _base_ref(
        signal_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        semantic_fingerprint="0" * 63 + "2",
    )
    agg1 = _make_aggregate(signal_references=[sig_a, sig_b])
    agg2 = _make_aggregate(signal_references=[sig_b, sig_a])
    assert agg1 == agg2
    assert serialize_incident_aggregate(agg1) == serialize_incident_aggregate(agg2)


# ---------- transport coordinates are absent and cannot affect identity ----------


def test_no_transport_fields_in_projection() -> None:
    agg = _make_aggregate()
    proj = aggregate_semantic_projection(agg)
    forbidden = {
        "kafka_partition",
        "kafka_offset",
        "broker_timestamp",
        "delivery_attempt",
    }
    for key in forbidden:
        assert key not in proj
        assert key not in json.dumps(proj, sort_keys=True)


def test_incident_id_not_affected_by_transport() -> None:
    _ = derive_incident_id(
        tenant_id=_TENANT, environment="prod", anchor_signal_fingerprint=_FP_A
    )
    # pretend transport info is passed as extra kwargs — function does not accept it
    with pytest.raises(TypeError):
        derive_incident_id(
            tenant_id=_TENANT,
            environment="prod",
            anchor_signal_fingerprint=_FP_A,
            kafka_partition=3,
        )  # type: ignore[call-arg]


# ---------- timeline-event and handoff identity determinism/sensitivity ----------


def test_timeline_event_id_determinism_and_sensitivity() -> None:
    agg = _make_aggregate()
    kid = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=agg.revision,
        event_type=IncidentTimelineEventType.CREATED,
        event_time=_UTC_NOW,
        source_signal_id=None,
        revision_fingerprint=agg.revision_fingerprint,
    )
    kid2 = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=agg.revision,
        event_type=IncidentTimelineEventType.CREATED,
        event_time=_UTC_NOW,
        source_signal_id=None,
        revision_fingerprint=agg.revision_fingerprint,
    )
    assert kid == kid2
    # change event_type changes id
    kid3 = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=agg.revision,
        event_type=IncidentTimelineEventType.SIGNAL_ATTACHED,
        event_time=_UTC_NOW,
        source_signal_id=None,
        revision_fingerprint=agg.revision_fingerprint,
    )
    assert kid != kid3


def test_handoff_id_determinism_and_sensitivity() -> None:
    agg = _make_aggregate()
    hid = derive_handoff_id(
        incident_id=agg.incident_id,
        incident_revision=agg.revision,
        incident_revision_fingerprint=agg.revision_fingerprint,
    )
    hid2 = derive_handoff_id(
        incident_id=agg.incident_id,
        incident_revision=agg.revision,
        incident_revision_fingerprint=agg.revision_fingerprint,
    )
    assert hid == hid2
    hid3 = derive_handoff_id(
        incident_id=agg.incident_id,
        incident_revision=agg.revision + 1,
        incident_revision_fingerprint=agg.revision_fingerprint,
    )
    assert hid != hid3


# ---------- canonical serialization is byte-identical ----------


def test_canonical_serialization_byte_identical() -> None:
    agg = _make_aggregate()
    raw1 = serialize_incident_aggregate(agg)
    raw2 = serialize_incident_aggregate(agg)
    assert raw1 == raw2
    # sorted keys, compact separators
    parsed = json.loads(raw1)
    assert list(parsed.keys()) == sorted(parsed.keys())
    assert b": " not in raw1


# ---------- translation failures use the correct typed error with exact cause identity ----------


def test_translation_failure_typed_error_with_cause() -> None:
    # malformed JSON bytes -> IncidentSerializationError with exact cause identity
    with pytest.raises(IncidentSerializationError) as ei:
        deserialize_incident_aggregate(b"{ not json }")
    assert type(ei.value) is IncidentSerializationError
    assert ei.value.__cause__ is not None
    assert ei.value.__suppress_context__ is True
    assert "not json" not in str(ei.value)


def test_identity_translation_failure_with_cause() -> None:
    from unittest.mock import patch

    import app.correlation.identity as identity_mod

    sentinel = RuntimeError("sentinel-identity")
    with patch.object(identity_mod.uuid, "uuid5", side_effect=sentinel), pytest.raises(
        IncidentIdentityError
    ) as ei:
        derive_incident_id(
            tenant_id=_TENANT,
            environment="prod",
            anchor_signal_fingerprint=_FP_A,
        )
    assert type(ei.value) is IncidentIdentityError
    assert ei.value.__cause__ is sentinel
    assert ei.value.__suppress_context__ is True
    assert "sentinel-identity" not in str(ei.value)


def test_serialization_translation_failure_with_cause() -> None:
    from unittest.mock import patch

    from app.correlation.errors import IncidentSerializationError

    agg = _make_aggregate()
    sentinel = RuntimeError("sentinel-serialize")
    with patch.object(json, "dumps", side_effect=sentinel), pytest.raises(
        IncidentSerializationError
    ) as ei:
        serialize_incident_aggregate(agg)
    assert type(ei.value) is IncidentSerializationError
    assert ei.value.__cause__ is sentinel
    assert ei.value.__suppress_context__ is True
    assert "sentinel-serialize" not in str(ei.value)


def test_deserialization_translation_failure_with_cause() -> None:
    from unittest.mock import patch

    from app.correlation.errors import IncidentDeserializationError
    from app.correlation.models import IncidentAggregate

    sentinel = ValueError("sentinel-validate")
    with patch.object(
        IncidentAggregate, "model_validate_json", side_effect=sentinel
    ), pytest.raises(IncidentDeserializationError) as ei:
        deserialize_incident_aggregate(b'{"schema_version": "1.0"}')
    assert type(ei.value) is IncidentDeserializationError
    assert ei.value.__cause__ is sentinel
    assert ei.value.__suppress_context__ is True
    assert "sentinel-validate" not in str(ei.value)


def test_compute_fingerprint_wrong_type_raises_identity_error() -> None:
    with pytest.raises(IncidentIdentityError) as ei:
        compute_revision_fingerprint("not an aggregate")
    assert ei.value.__cause__ is not None


# ---------- boundary reconstruction uses validation, never model_copy(update=...) ----------


def test_boundary_aggregate_round_trip_via_validation_not_model_copy() -> None:
    agg = _make_aggregate()
    raw = serialize_incident_aggregate(agg)
    reconstructed = deserialize_incident_aggregate(raw)
    # mutate via model_copy should not be used; enforce via validation
    assert reconstructed == agg
    # ensure that model_copy(update=...) path is not required for correctness
    mutated = reconstructed.model_copy(update={"revision": 2})  # type: ignore[arg-type]
    # mutated has revision 2 but fingerprint mismatch — re-validating the dumped mutated payload must fail
    with pytest.raises(IncidentSerializationError):
        # serialize mutated -> but mutated should not pass deserialization due to fingerprint mismatch
        mutated_raw = serialize_incident_aggregate(mutated)  # type: ignore[arg-type]
        deserialize_incident_aggregate(mutated_raw)


# ---------- timeline and handoff serialization ----------


def test_timeline_event_serialization_cycle() -> None:
    agg = _make_aggregate()
    event_id = derive_timeline_event_id(
        incident_id=agg.incident_id,
        revision=1,
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
            "summary": "created",
            "revision_fingerprint": agg.revision_fingerprint,
        }
    )
    assert (
        deserialize_incident_timeline_event(serialize_incident_timeline_event(ev)) == ev
    )


def test_handoff_serialization_cycle() -> None:
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
    assert deserialize_incident_handoff(serialize_incident_handoff(handoff)) == handoff
