"""Receiving-envelope contracts, H1 adapters and independent semantic validation."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from app.correlation.blast_radius import MAX_HANDOFF_BYTES, compute_context_fingerprint
from app.correlation.handoff import (
    Phase5HandoffContractError,
    build_phase5_handoff,
    compute_handoff_fingerprint,
    compute_membership_fingerprint,
    decode_phase5_handoff_structural,
    derive_enriched_handoff_id,
    serialize_phase5_handoff,
    prepare_phase5_handoff,
    validate_phase5_handoff,
)
from app.correlation.identity import derive_handoff_id
from app.correlation.models import (
    CorrelationEvidence,
    CorrelationWindowState,
    IncidentAggregate,
    IncidentHandoff,
    IncidentSignalReference,
    IncidentStatus,
)
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    deserialize_incident_handoff,
    serialize_incident_aggregate,
    serialize_incident_handoff,
)
from app.telemetry.schemas import EventSeverity
from test_correlation_blast_radius import context_payload, encode


def incident_fixture():
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    refs = tuple(
        IncidentSignalReference(
            signal_id=UUID(int=i),
            semantic_fingerprint=str(i) * 64,
            tenant_id=UUID(int=1),
            environment="prod",
            service="order-service",
            event_time=now,
            severity=EventSeverity.ERROR,
            anomaly_score=0.85,
        )
        for i in (2, 3)
    )
    evidence = CorrelationEvidence(
        left_signal_id=refs[0].signal_id,
        right_signal_id=refs[1].signal_id,
        temporal_score=0.7,
        service_score=0.8,
        topology_score=0.6,
        symptom_score=0.9,
        context_score=0.5,
        weighted_score=0.72,
        accepted=True,
        policy_version="1.0",
        reason="Phase 4 membership evidence",
    )
    return IncidentAggregate.create(
        incident_id=UUID(int=8),
        incident_number="INC-0001",
        tenant_id=UUID(int=1),
        environment="prod",
        status=IncidentStatus.OPEN,
        correlation_state=CorrelationWindowState.COLLECTING,
        severity=EventSeverity.ERROR,
        severity_policy_version="max-member-severity/v1",
        started_at=now,
        last_seen_at=now,
        created_at=now,
        updated_at=now,
        revision=1,
        primary_service="order-service",
        affected_services=("order-service",),
        signal_references=refs,
        correlation_evidence=(evidence,),
        topology_snapshot=None,
        blast_radius=None,
    )


def handoff_payload():
    incident = incident_fixture()
    context = context_payload()
    p = dict(
        schema_version="phase5-investigation-handoff/1.0",
        incident=json.loads(serialize_incident_aggregate(incident)),
        membership_fingerprint=compute_membership_fingerprint(incident),
        topology_context=context,
        provenance=dict(
            producer_version="phase4-blast-handoff/1.0",
            source_kind="provided_snapshot",
            provider_class=None,
            incident_revision_fingerprint=incident.revision_fingerprint,
            topology_snapshot_fingerprint=context["topology_snapshot"][
                "snapshot_fingerprint"
            ],
            policy_version=context["policy"]["policy_version"],
        ),
        generated_at="2026-11-21T00:01:00Z",
    )
    return resign(p)


def resign(p):
    p["topology_context"]["context_fingerprint"] = compute_context_fingerprint(
        p["topology_context"]
    )
    p["handoff_fingerprint"] = compute_handoff_fingerprint(p)
    p["handoff_id"] = str(derive_enriched_handoff_id(p["handoff_fingerprint"]))
    return p


def test_envelope_roundtrip_preserves_original_incident_bytes_and_fields():
    incident = incident_fixture()
    before = serialize_incident_aggregate(incident)
    p = handoff_payload()
    raw = encode(p)
    model = decode_phase5_handoff_structural(raw)
    assert serialize_phase5_handoff(model) == raw
    assert (
        serialize_incident_aggregate(model.incident)
        == before
        == serialize_incident_aggregate(incident)
    )
    assert (
        model.incident.topology_snapshot is None and model.incident.blast_radius is None
    )
    assert model.incident.correlation_evidence == incident.correlation_evidence
    assert (
        model.membership_fingerprint
        == hashlib.sha256(encode(json.loads(before)["signal_references"])).hexdigest()
    )
    assert (
        model.handoff_fingerprint
        == hashlib.sha256(
            encode(
                {
                    k: v
                    for k, v in p.items()
                    if k not in ("handoff_id", "handoff_fingerprint")
                }
            )
        ).hexdigest()
    )
    assert deserialize_incident_aggregate(before) == incident


@pytest.mark.parametrize("field", ["generated_at", "provider", "snapshot", "policy"])
def test_meaningful_context_provenance_and_timestamp_changes_change_identity(field):
    p = handoff_payload()
    original = p["handoff_id"]
    if field == "generated_at":
        p[field] = "2026-11-21T00:01:01Z"
    elif field == "provider":
        p["provenance"].update(
            source_kind="canonical",
            provider_class="app.correlation.topology_canonical.CanonicalTopologyProvider",
        )
    elif field == "snapshot":
        p["topology_context"]["topology_snapshot"]["topology_version"] = "1.0.1"
    elif field == "policy":
        p["topology_context"]["policy"]["max_hops"] = 3
    model = decode_phase5_handoff_structural(encode(resign(p)))
    assert str(model.handoff_id) != original


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("provenance",),
        ("incident",),
        ("incident", "signal_references", 0),
        ("incident", "correlation_evidence", 0),
        ("topology_context",),
        ("topology_context", "policy"),
        ("topology_context", "reachable_services", 0),
    ],
)
def test_no_rca_or_remediation_fields_at_nested_boundaries(path):
    p = handoff_payload()
    obj = p
    for key in path:
        obj = obj[key]
    obj["remediation"] = "not authorized"
    with pytest.raises(Phase5HandoffContractError):
        decode_phase5_handoff_structural(encode(resign(p)))


@pytest.mark.parametrize(
    "change",
    [
        "member",
        "primary",
        "tenant",
        "revision_fp",
        "membership_fp",
        "provenance_fp",
        "topology_mirror",
        "provider_label",
        "provider_class",
        "future_capture",
        "expired",
        "non_utc",
        "bad_timestamp",
        "bad_id",
        "bad_hash",
        "future_schema",
        "bad_context_hash",
        "counter_bool",
        "legacy_unsafe",
    ],
)
def test_rehashed_inconsistent_envelopes_rejected(change):
    p = handoff_payload()
    if change == "member":
        p["topology_context"]["member_services"][0]["name"] = "other-service"
    elif change == "primary":
        p["incident"]["primary_service"] = "other-service"
    elif change == "tenant":
        p["incident"]["signal_references"][0]["tenant_id"] = str(UUID(int=99))
    elif change == "revision_fp":
        p["incident"]["revision_fingerprint"] = "a" * 64
    elif change == "membership_fp":
        p["membership_fingerprint"] = "a" * 64
    elif change == "provenance_fp":
        p["provenance"]["incident_revision_fingerprint"] = "a" * 64
    elif change == "topology_mirror":
        p["provenance"]["topology_snapshot_fingerprint"] = "a" * 64
    elif change == "provider_label":
        p["provenance"]["source_kind"] = "unknown"
    elif change == "provider_class":
        p["provenance"]["provider_class"] = "not.a.real.Provider"
    elif change == "future_capture":
        p["topology_context"]["topology_snapshot"]["captured_at"] = (
            "2026-11-22T00:00:00Z"
        )
    elif change == "expired":
        p["topology_context"]["policy"]["max_snapshot_age_seconds"] = 59
    elif change == "non_utc":
        p["generated_at"] = "2026-11-21T05:31:00+05:30"
    elif change == "bad_timestamp":
        p["generated_at"] = "2026-11-21T00:01:00"
    elif change == "bad_id":
        p["handoff_id"] = str(UUID(int=99))
    elif change == "bad_hash":
        p["handoff_fingerprint"] = "a" * 64
    elif change == "future_schema":
        p["schema_version"] = "phase5-investigation-handoff/2.0"
    elif change == "bad_context_hash":
        p["topology_context"]["context_fingerprint"] = "a" * 64
    elif change == "counter_bool":
        p["topology_context"]["examined_arcs"] = True
    elif change == "legacy_unsafe":
        p["incident"]["revision"] = True
    if change not in ("bad_id", "bad_hash", "bad_context_hash"):
        resign(p)
    with pytest.raises(Phase5HandoffContractError):
        decode_phase5_handoff_structural(encode(p))


def test_age_equality_is_accepted():
    p = handoff_payload()
    p["topology_context"]["policy"]["max_snapshot_age_seconds"] = 60
    assert (
        decode_phase5_handoff_structural(
            encode(resign(p))
        ).topology_context.policy.max_snapshot_age_seconds
        == 60
    )


@pytest.mark.parametrize(
    "raw",
    [
        b'{"incident":{},"incident":{}}',
        b'{"x":{"a":1,"a":2}}',
        b'{"x":Infinity}',
        b"[]",
        b"\xff",
        b"bad",
    ],
)
def test_structural_decoder_rejects_malformed_nested_duplicate_and_nonfinite_json(raw):
    with pytest.raises(Phase5HandoffContractError):
        decode_phase5_handoff_structural(raw)


def test_handoff_raw_byte_limit_precedes_json_parse():
    with pytest.raises(Phase5HandoffContractError) as caught:
        decode_phase5_handoff_structural(b" " * (MAX_HANDOFF_BYTES + 1))
    assert caught.value.__cause__ is not None
    assert "byte limit" in str(caught.value.__cause__)


def test_unsafe_instances_cannot_bypass_serialization_checks():
    model = decode_phase5_handoff_structural(encode(handoff_payload()))
    with pytest.raises(ValidationError):
        model.generated_at = datetime.now(timezone.utc)
    bad = model.model_copy(
        update={
            "provenance": model.provenance.model_copy(update={"provider_class": "fake"})
        }
    )
    with pytest.raises(Phase5HandoffContractError):
        serialize_phase5_handoff(bad)
    bad = model.model_copy(
        update={"incident": model.incident.model_copy(update={"revision": True})}
    )
    with pytest.raises(Phase5HandoffContractError):
        serialize_phase5_handoff(bad)


def test_legacy_handoff_roundtrip_and_identity_remain_unchanged():
    incident = incident_fixture()
    legacy = IncidentHandoff(
        handoff_id=derive_handoff_id(
            incident_id=incident.incident_id,
            incident_revision=incident.revision,
            incident_revision_fingerprint=incident.revision_fingerprint,
        ),
        incident_id=incident.incident_id,
        incident_revision=incident.revision,
        incident_revision_fingerprint=incident.revision_fingerprint,
        tenant_id=incident.tenant_id,
        environment=incident.environment,
        status=incident.status,
        severity=incident.severity,
        severity_policy_version=incident.severity_policy_version,
        primary_service=incident.primary_service,
        affected_services=incident.affected_services,
        signal_references=incident.signal_references,
        topology_snapshot=None,
        blast_radius=None,
        generated_at=datetime(2026, 11, 21, 0, 1, tzinfo=timezone.utc),
    )
    before = serialize_incident_handoff(legacy)
    decode_phase5_handoff_structural(encode(handoff_payload()))
    assert (
        serialize_incident_handoff(deserialize_incident_handoff(before))
        == before
        == serialize_incident_handoff(legacy)
    )


@pytest.mark.parametrize("location", ["envelope", "incident", "signal", "provenance"])
def test_unknown_unsafe_copy_fields_never_disappear_during_envelope_encoding(location):
    model = decode_phase5_handoff_structural(encode(handoff_payload()))
    extra = {"root_cause_confidence": 0.9}
    if location == "envelope":
        bad = model.model_copy(update=extra)
    elif location == "incident":
        bad = model.model_copy(
            update={"incident": model.incident.model_copy(update=extra)}
        )
    elif location == "signal":
        refs = model.incident.signal_references
        incident = model.incident.model_copy(
            update={"signal_references": (refs[0].model_copy(update=extra), *refs[1:])}
        )
        bad = model.model_copy(update={"incident": incident})
    else:
        bad = model.model_copy(
            update={"provenance": model.provenance.model_copy(update=extra)}
        )
    with pytest.raises(Phase5HandoffContractError):
        serialize_phase5_handoff(bad)


def test_exact_handoff_raw_ceiling_then_one_byte_over():
    raw = encode(handoff_payload())
    exact = raw + b" " * (MAX_HANDOFF_BYTES - len(raw))
    assert serialize_phase5_handoff(decode_phase5_handoff_structural(exact)) == raw
    with pytest.raises(Phase5HandoffContractError):
        decode_phase5_handoff_structural(exact + b" ")


def semantic_validate(raw, args):
    return validate_phase5_handoff(
        raw=raw,
        snapshot=args["snapshot"],
        expected_topology=args["expected_topology"],
        expected_policy=args["policy"],
    )


@pytest.mark.parametrize("case_id", [f"F{i:02d}" for i in range(1, 17)])
def test_h1_static_fixture_roundtrip_and_determinism(case_id, monkeypatch):
    from test_correlation_blast_radius import _calculator_inputs
    from app.correlation import handoff as module
    from unittest.mock import Mock

    args = _calculator_inputs(case_id)
    before_incident = serialize_incident_aggregate(args["incident"])
    before_snapshot = args["snapshot"].model_dump_json()
    before_pin = args["expected_topology"].model_dump_json()
    before_policy = args["policy"].model_dump_json()
    calculator = Mock(wraps=module.calculate_blast_radius)
    monkeypatch.setattr(module, "calculate_blast_radius", calculator)
    first = build_phase5_handoff(**args)
    assert calculator.call_count == 1
    raw = serialize_phase5_handoff(first)
    assert first.topology_context.model_dump(mode="json") == context_payload(case_id)
    assert first.provenance.source_kind == "provided_snapshot"
    assert first.provenance.provider_class is None
    assert first.generated_at == args["as_of"]
    assert serialize_phase5_handoff(build_phase5_handoff(**args)) == raw
    assert serialize_phase5_handoff(semantic_validate(raw, args)) == raw
    assert calculator.call_count == 3  # one per assembly and consumer recomputation
    assert serialize_incident_aggregate(first.incident) == before_incident
    assert serialize_incident_aggregate(args["incident"]) == before_incident
    assert args["snapshot"].model_dump_json() == before_snapshot
    assert args["expected_topology"].model_dump_json() == before_pin
    assert args["policy"].model_dump_json() == before_policy
    assert first.incident.topology_snapshot is None
    assert first.incident.blast_radius is None


@pytest.mark.asyncio
async def test_h1_real_canonical_provider_single_fetch_and_membership_evidence(
    monkeypatch,
):
    from app.correlation import handoff as module
    from app.correlation.models import TopologySnapshotReference
    from app.correlation.topology_canonical import CanonicalTopologyProvider
    from app.correlation.blast_radius import BlastRadiusPolicy
    from unittest.mock import Mock

    provider = CanonicalTopologyProvider()
    snapshot = provider._snapshot
    now = datetime(2026, 11, 21, tzinfo=timezone.utc)
    pin = TopologySnapshotReference(
        topology_id=snapshot.version.topology_id,
        topology_version=snapshot.version.topology_version,
        snapshot_fingerprint=snapshot.snapshot_fingerprint,
        captured_at=now,
        provider="untrusted observation label",
    )
    calls = []
    real_fetch = CanonicalTopologyProvider.get_snapshot

    async def fetch(self, *, expected_version):
        calls.append(expected_version)
        return await real_fetch(self, expected_version=expected_version)

    async def forbidden(*args, **kwargs):
        pytest.fail("Adapter must not fetch paths/neighborhoods or close provider")

    monkeypatch.setattr(CanonicalTopologyProvider, "get_snapshot", fetch)
    for name in ("close", "get_neighborhood", "find_paths"):
        monkeypatch.setattr(CanonicalTopologyProvider, name, forbidden)
    calculator = Mock(wraps=module.calculate_blast_radius)
    monkeypatch.setattr(module, "calculate_blast_radius", calculator)
    incident = incident_fixture()
    before = serialize_incident_aggregate(incident)
    policy = BlastRadiusPolicy()
    handoff = await prepare_phase5_handoff(
        incident=incident,
        provider=provider,
        expected_topology=pin,
        policy=policy,
        as_of=now,
    )
    assert calls == [pin.topology_version] and calculator.call_count == 1
    assert handoff.provenance.source_kind == "canonical"
    assert handoff.provenance.provider_class == (
        "app.correlation.topology_canonical.CanonicalTopologyProvider"
    )
    assert handoff.topology_context.topology_snapshot.provider == pin.provider
    assert serialize_incident_aggregate(handoff.incident) == before
    assert serialize_incident_aggregate(incident) == before
    assert provider._snapshot is snapshot
    raw = serialize_phase5_handoff(handoff)
    assert (
        validate_phase5_handoff(
            raw=raw,
            snapshot=snapshot,
            expected_topology=pin,
            expected_policy=policy,
        )
        == handoff
    )


@pytest.mark.asyncio
async def test_h1_neo4j_dispatch_unit_double_no_resource_ownership(monkeypatch):
    from app.correlation.topology_neo4j import Neo4jTopologyProvider
    from test_correlation_blast_radius import _calculator_inputs
    from unittest.mock import Mock

    args = _calculator_inputs()
    snapshot = args.pop("snapshot")
    driver = Mock()
    provider = Neo4jTopologyProvider(
        driver=driver,
        topology_id=args["expected_topology"].topology_id,
        topology_version=args["expected_topology"].topology_version,
    )
    calls = []

    async def fetch(self, *, expected_version):
        assert self is provider
        calls.append(expected_version)
        return snapshot

    monkeypatch.setattr(Neo4jTopologyProvider, "get_snapshot", fetch)
    result = await prepare_phase5_handoff(provider=provider, **args)
    assert calls == [args["expected_topology"].topology_version]
    assert result.provenance.source_kind == "neo4j"
    assert result.provenance.provider_class == (
        "app.correlation.topology_neo4j.Neo4jTopologyProvider"
    )
    assert driver.mock_calls == []  # mocked fetch; real Neo4j belongs to V1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "missing_pin",
        "pin_version_space",
        "incident_pin",
        "bool_revision",
        "unknown_field",
        "name_space",
        "bad_policy",
        "naive_time",
        "nonutc_time",
        "future_pin",
        "expired_pin",
        "too_many_refs",
        "missing_incident",
        "missing_policy",
        "invalid_pin_hash",
    ],
)
async def test_h1_invalid_inputs_rejected_before_provider_dispatch(
    mutation, monkeypatch
):
    from app.correlation.topology_canonical import CanonicalTopologyProvider
    from test_correlation_blast_radius import _calculator_inputs
    from datetime import timedelta
    from unittest.mock import AsyncMock

    args = _calculator_inputs()
    args.pop("snapshot")
    provider = CanonicalTopologyProvider()
    fetch = AsyncMock(side_effect=AssertionError("Provider must not be dispatched"))
    monkeypatch.setattr(CanonicalTopologyProvider, "get_snapshot", fetch)
    if mutation == "missing_pin":
        args["expected_topology"] = None
    elif mutation == "missing_incident":
        args["incident"] = None
    elif mutation == "missing_policy":
        args["policy"] = None
    elif mutation == "pin_version_space":
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"topology_version": " 1.0.0 "}
        )
    elif mutation == "invalid_pin_hash":
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"snapshot_fingerprint": "invalid"}
        )
    elif mutation == "incident_pin":
        pin = args["expected_topology"].model_copy(update={"topology_version": "stale"})
        args["incident"] = args["incident"].model_copy(
            update={"topology_snapshot": pin}
        )
    elif mutation in ("bool_revision", "unknown_field"):
        update = (
            {"revision": True} if mutation == "bool_revision" else {"root_cause": "x"}
        )
        args["incident"] = args["incident"].model_copy(update=update)
    elif mutation == "name_space":
        refs = args["incident"].signal_references
        args["incident"] = args["incident"].model_copy(
            update={
                "signal_references": (
                    refs[0].model_copy(update={"service": " order-service "}),
                    *refs[1:],
                )
            }
        )
    elif mutation == "too_many_refs":
        args["incident"] = args["incident"].model_copy(
            update={"signal_references": args["incident"].signal_references * 1025}
        )
    elif mutation == "bad_policy":
        args["policy"] = args["policy"].model_copy(update={"max_hops": True})
    elif mutation == "naive_time":
        args["as_of"] = args["as_of"].replace(tzinfo=None)
    elif mutation == "nonutc_time":
        args["as_of"] = args["as_of"].astimezone(timezone(timedelta(hours=1)))
    elif mutation == "future_pin":
        args["as_of"] = args["expected_topology"].captured_at - timedelta(seconds=1)
    elif mutation == "expired_pin":
        args["policy"] = args["policy"].model_copy(
            update={"max_snapshot_age_seconds": 0}
        )
    with pytest.raises(Phase5HandoffContractError, match="Cannot prepare") as error:
        await prepare_phase5_handoff(provider=provider, **args)
    assert error.value.__cause__ is not None
    fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["id", "version", "hash", "corrupt", "provider_error", "cancel"]
)
async def test_h1_single_fetch_failures_and_mismatches_preserve_ownership(
    mutation, monkeypatch
):
    from app.correlation.topology_canonical import CanonicalTopologyProvider
    from test_correlation_blast_radius import _calculator_inputs
    from unittest.mock import AsyncMock
    from asyncio import CancelledError

    args = _calculator_inputs()
    snapshot = args.pop("snapshot")
    if mutation in ("id", "version"):
        update = (
            {"topology_id": UUID(int=999)}
            if mutation == "id"
            else {"topology_version": "stale"}
        )
        args["expected_topology"] = args["expected_topology"].model_copy(update=update)
    elif mutation == "hash":
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"snapshot_fingerprint": "f" * 64}
        )
    elif mutation == "corrupt":
        snapshot = snapshot.model_copy(update={"snapshot_fingerprint": "f" * 64})
    failure = RuntimeError("secret-host:5432 SELECT internal_password")
    fetch = AsyncMock(return_value=snapshot)
    if mutation == "provider_error":
        fetch.side_effect = failure
    elif mutation == "cancel":
        fetch.side_effect = CancelledError()
    close = AsyncMock()
    monkeypatch.setattr(CanonicalTopologyProvider, "get_snapshot", fetch)
    monkeypatch.setattr(CanonicalTopologyProvider, "close", close)
    expected_error = (
        CancelledError if mutation == "cancel" else Phase5HandoffContractError
    )
    with pytest.raises(expected_error) as error:
        await prepare_phase5_handoff(provider=CanonicalTopologyProvider(), **args)
    fetch.assert_awaited_once_with(
        expected_version=args["expected_topology"].topology_version
    )
    close.assert_not_awaited()
    if mutation == "provider_error":
        assert error.value.__cause__ is failure
        assert "secret" not in str(error.value) and "SELECT" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["protocol", "subclass"])
async def test_h1_rejects_arbitrary_provider_dispatch(kind):
    from app.correlation.topology_canonical import CanonicalTopologyProvider
    from test_correlation_blast_radius import _calculator_inputs
    from unittest.mock import AsyncMock

    class ArbitraryProvider:
        get_snapshot = AsyncMock()

    class Subclass(CanonicalTopologyProvider):
        get_snapshot = AsyncMock()

    provider = ArbitraryProvider() if kind == "protocol" else Subclass()
    args = _calculator_inputs()
    args.pop("snapshot")
    with pytest.raises(Phase5HandoffContractError):
        await prepare_phase5_handoff(provider=provider, **args)
    provider.get_snapshot.assert_not_awaited()


@pytest.mark.parametrize(
    "mutation",
    ["arcs", "alternate_path", "wrong_depth", "omission", "name", "pin_metadata"],
)
def test_h1_consumer_rejects_rehashed_semantic_forgeries(mutation):
    from test_correlation_blast_radius import _calculator_inputs

    args = _calculator_inputs("F10" if mutation == "omission" else "F06")
    payload = build_phase5_handoff(**args).model_dump(mode="json")
    context = payload["topology_context"]
    ids = {n["name"]: n["service_id"] for n in context["reachable_services"]}
    if mutation == "arcs":
        context["examined_arcs"] += 1
    elif mutation == "alternate_path":
        path = next(p for p in context["paths"] if p["target_service_id"] == ids["D"])
        path["service_ids"] = [ids["A"], ids["C"], ids["D"]]
        path["edges"] = [
            e
            for a, b in [("A", "C"), ("C", "D")]
            for e in context["edges"]
            if (e["upstream_service_id"], e["downstream_service_id"])
            == (ids[a], ids[b])
        ]
    elif mutation == "wrong_depth":
        # Replace D's incoming graph edges and witness with a fictitious direct A->D.
        edge = dict(
            upstream_service_id=ids["A"],
            downstream_service_id=ids["D"],
            dependency_type="sync",
        )
        context["edges"] = [
            e for e in context["edges"] if e["downstream_service_id"] != ids["D"]
        ] + [edge]
        path = next(p for p in context["paths"] if p["target_service_id"] == ids["D"])
        path.update(service_ids=[ids["A"], ids["D"]], edges=[edge], hop_count=1)
        next(n for n in context["reachable_services"] if n["name"] == "D")[
            "minimum_depth"
        ] = 1
    elif mutation == "omission":
        context["omitted_within_hop_services"] += 1
    elif mutation == "name":
        next(n for n in context["reachable_services"] if n["name"] == "D")["name"] = (
            "forged-service"
        )
    else:
        context["topology_snapshot"]["provider"] = "changed metadata"
    raw = encode(resign(payload))
    assert decode_phase5_handoff_structural(raw)  # internally coherent hashes/shape
    with pytest.raises(Phase5HandoffContractError, match="semantics") as error:
        semantic_validate(raw, args)
    assert "independently calculated" in str(error.value.__cause__)


@pytest.mark.parametrize(
    "mutation", ["policy", "id", "version", "hash", "missing_pin", "bad_snapshot"]
)
def test_h1_consumer_requires_external_pin_policy_and_validated_snapshot(mutation):
    from test_correlation_blast_radius import _calculator_inputs

    args = _calculator_inputs()
    raw = serialize_phase5_handoff(build_phase5_handoff(**args))
    if mutation == "policy":
        args["policy"] = args["policy"].model_copy(update={"max_hops": 1})
    elif mutation == "missing_pin":
        args["expected_topology"] = None
    elif mutation == "bad_snapshot":
        args["snapshot"] = args["snapshot"].model_copy(
            update={"snapshot_fingerprint": "f" * 64}
        )
    else:
        updates = {
            "id": {"topology_id": UUID(int=999)},
            "version": {"topology_version": "stale"},
            "hash": {"snapshot_fingerprint": "f" * 64},
        }
        args["expected_topology"] = args["expected_topology"].model_copy(
            update=updates[mutation]
        )
    with pytest.raises(Phase5HandoffContractError):
        semantic_validate(raw, args)


def test_h1_existing_incident_pin_and_phase4_scores_are_carried_unchanged():
    from test_correlation_blast_radius import _calculator_inputs

    args = _calculator_inputs()
    original = incident_fixture()
    fields = original.model_dump(mode="python", exclude={"revision_fingerprint"})
    fields["topology_snapshot"] = args["expected_topology"]
    incident = IncidentAggregate.create(**fields)
    args["incident"] = incident
    before = serialize_incident_aggregate(incident)
    result = semantic_validate(
        serialize_phase5_handoff(build_phase5_handoff(**args)), args
    )
    assert serialize_incident_aggregate(result.incident) == before
    assert result.incident.correlation_evidence == original.correlation_evidence
    assert result.incident.correlation_evidence[0].weighted_score == 0.72
    assert result.incident.blast_radius is None
    assert serialize_incident_aggregate(incident) == before


def test_h1_assembly_is_invariant_to_graph_input_order():
    from test_correlation_blast_radius import _calculator_inputs
    from app.correlation.topology import TopologySnapshot

    args = _calculator_inputs("F06")
    raw = serialize_phase5_handoff(build_phase5_handoff(**args))
    snapshot = args["snapshot"]
    args["snapshot"] = TopologySnapshot.create(
        version=snapshot.version,
        nodes=tuple(reversed(snapshot.nodes)),
        edges=tuple(reversed(snapshot.edges)),
    )
    assert serialize_phase5_handoff(build_phase5_handoff(**args)) == raw


@pytest.mark.parametrize("field", ["policy", "as_of", "membership"])
def test_h1_assembly_meaningful_changes_bind_new_identity(field):
    from test_correlation_blast_radius import _calculator_inputs, _incident_for_services
    from datetime import timedelta

    args = _calculator_inputs()
    before = build_phase5_handoff(**args)
    if field == "policy":
        args["policy"] = args["policy"].model_copy(update={"max_hops": 1})
    elif field == "as_of":
        args["as_of"] += timedelta(seconds=1)
    else:
        args["incident"] = _incident_for_services(["order-service", "payment-service"])
    after = build_phase5_handoff(**args)
    assert after.handoff_fingerprint != before.handoff_fingerprint
    assert after.handoff_id != before.handoff_id


@pytest.mark.parametrize(
    "mutation", ["duplicate", "unknown_field", "bad_hash", "bad_version", "not_bytes"]
)
def test_h1_consumer_structural_checks_precede_graph_dispatch(mutation, monkeypatch):
    from test_correlation_blast_radius import _calculator_inputs
    from app.correlation import handoff as module
    from unittest.mock import Mock

    args = _calculator_inputs()
    p = build_phase5_handoff(**args).model_dump(mode="json")
    raw = encode(p)
    if mutation == "duplicate":
        raw = raw[:-1] + b',"schema_version":"phase5-investigation-handoff/1.0"}'
    elif mutation == "unknown_field":
        p["root_cause_candidate"] = "forbidden"
        raw = encode(p)
    elif mutation == "bad_hash":
        p["handoff_fingerprint"] = "f" * 64
        raw = encode(p)
    elif mutation == "bad_version":
        p["schema_version"] = "phase5-investigation-handoff/2.0"
        raw = encode(p)
    else:
        raw = raw.decode()
    calculator = Mock(side_effect=AssertionError("Must fail structural check first"))
    monkeypatch.setattr(module, "calculate_blast_radius", calculator)
    with pytest.raises(Phase5HandoffContractError):
        semantic_validate(raw, args)
    calculator.assert_not_called()


@pytest.mark.parametrize("entry", ["build", "consumer"])
def test_h1_unexpected_calculator_failure_sanitized_with_original_cause(
    entry, monkeypatch
):
    from test_correlation_blast_radius import _calculator_inputs
    from app.correlation import handoff as module
    from unittest.mock import Mock

    args = _calculator_inputs()
    raw = serialize_phase5_handoff(build_phase5_handoff(**args))
    failure = RuntimeError("SELECT secret_password FROM secret_host")
    monkeypatch.setattr(module, "calculate_blast_radius", Mock(side_effect=failure))
    with pytest.raises(Phase5HandoffContractError) as error:
        if entry == "build":
            build_phase5_handoff(**args)
        else:
            semantic_validate(raw, args)
    assert error.value.__cause__ is failure
    assert "secret" not in str(error.value) and "SELECT" not in str(error.value)
