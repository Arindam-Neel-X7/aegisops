"""Bounded Phase 5 receiving envelope and topology-pinned handoff adapters.

Structural decoding does not authenticate topology reachability or historical
provider dispatch. Consumers use validate_phase5_handoff for graph semantics.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_DNS, UUID, uuid5

from pydantic import AwareDatetime, ValidationInfo, field_validator, model_validator

from app.correlation.blast_radius import (
    MAX_CONTEXT_BYTES,
    MAX_HANDOFF_BYTES,
    BlastRadiusContext,
    BlastRadiusPolicy,
    Fingerprint,
    _canonical_bytes,
    _Contract,
    _decode_object,
    _fresh,
    calculate_blast_radius,
    serialize_blast_radius_context,
)
from app.correlation.models import IncidentAggregate, TopologySnapshotReference
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    serialize_incident_aggregate,
)
from app.correlation.topology import TopologySnapshot
from app.correlation.topology_canonical import CanonicalTopologyProvider
from app.correlation.topology_neo4j import Neo4jTopologyProvider

HANDOFF_NAMESPACE = uuid5(NAMESPACE_DNS, "aegisops.phase4.blast-radius-handoff/v1")
_PROVIDERS = {
    "provided_snapshot": None,
    "canonical": "app.correlation.topology_canonical.CanonicalTopologyProvider",
    "neo4j": "app.correlation.topology_neo4j.Neo4jTopologyProvider",
}


class Phase5HandoffContractError(ValueError):
    """Invalid, oversized or structurally inconsistent receiving envelope."""


def compute_membership_fingerprint(incident: IncidentAggregate) -> str:
    # Use the existing serializer/deserializer as the sole incident contract.
    import json

    payload = json.loads(serialize_incident_aggregate(incident))
    return hashlib.sha256(
        _canonical_bytes(payload["signal_references"], MAX_HANDOFF_BYTES)
    ).hexdigest()


def compute_handoff_fingerprint(payload: dict[str, Any]) -> str:
    projection = {
        k: v
        for k, v in payload.items()
        if k not in ("handoff_id", "handoff_fingerprint")
    }
    return hashlib.sha256(_canonical_bytes(projection, MAX_HANDOFF_BYTES)).hexdigest()


def derive_enriched_handoff_id(fingerprint: str) -> UUID:
    if (
        not isinstance(fingerprint, str)
        or len(fingerprint) != 64
        or any(c not in "0123456789abcdef" for c in fingerprint)
    ):
        raise Phase5HandoffContractError("Invalid handoff fingerprint")
    return uuid5(HANDOFF_NAMESPACE, fingerprint)


class HandoffProvenance(_Contract):
    producer_version: Literal["phase4-blast-handoff/1.0"] = "phase4-blast-handoff/1.0"
    source_kind: Literal["provided_snapshot", "canonical", "neo4j"]
    provider_class: str | None
    incident_revision_fingerprint: Fingerprint
    topology_snapshot_fingerprint: Fingerprint
    policy_version: Literal["blast-radius-policy/1.0"]

    @model_validator(mode="after")
    def _provider(self) -> HandoffProvenance:
        if self.provider_class != _PROVIDERS[self.source_kind]:
            raise ValueError("Provider attribution mismatch")
        return self


class Phase5InvestigationHandoff(_Contract):
    schema_version: Literal["phase5-investigation-handoff/1.0"] = (
        "phase5-investigation-handoff/1.0"
    )
    handoff_id: UUID
    handoff_fingerprint: Fingerprint
    incident: IncidentAggregate
    membership_fingerprint: Fingerprint
    topology_context: BlastRadiusContext
    provenance: HandoffProvenance
    generated_at: AwareDatetime

    @field_validator("incident", mode="before")
    @classmethod
    def _incident(cls, value: Any, info: ValidationInfo) -> Any:
        if isinstance(value, IncidentAggregate):
            value = _fresh(value, IncidentAggregate)
            return deserialize_incident_aggregate(serialize_incident_aggregate(value))
        if isinstance(value, dict):
            if info.mode == "python":
                return IncidentAggregate.model_validate(value)
            return deserialize_incident_aggregate(
                _canonical_bytes(value, MAX_HANDOFF_BYTES)
            )
        return value

    @field_validator("generated_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("UTC timestamp required")
        return value

    @model_validator(mode="after")
    def _reconcile(self) -> Phase5InvestigationHandoff:
        # Also revalidate legacy objects reached via JSON or unsafe copied instances.
        incident = deserialize_incident_aggregate(
            serialize_incident_aggregate(self.incident)
        )
        object.__setattr__(self, "incident", incident)
        context = self.topology_context
        serialize_blast_radius_context(context)
        if {s.service for s in incident.signal_references} != {
            s.name for s in context.member_services
        }:
            raise ValueError("Incident/context membership mismatch")
        if incident.primary_service not in {s.name for s in context.member_services}:
            raise ValueError("Primary service is not a member")
        ref = context.topology_snapshot
        if incident.topology_snapshot is not None:
            old = incident.topology_snapshot
            if (old.topology_id, old.topology_version, old.snapshot_fingerprint) != (
                ref.topology_id,
                ref.topology_version,
                ref.snapshot_fingerprint,
            ):
                raise ValueError("Incident/context topology pin mismatch")
        age = self.generated_at - ref.captured_at
        if age < timedelta(0):
            raise ValueError("Snapshot captured in the future")
        maximum_age = context.policy.max_snapshot_age_seconds
        if maximum_age is not None and age > timedelta(seconds=maximum_age):
            raise ValueError("Snapshot exceeds age policy")
        if self.membership_fingerprint != compute_membership_fingerprint(incident):
            raise ValueError("Membership fingerprint mismatch")
        if (
            self.provenance.incident_revision_fingerprint
            != incident.revision_fingerprint
        ):
            raise ValueError("Incident provenance mirror mismatch")
        if (
            self.provenance.topology_snapshot_fingerprint != ref.snapshot_fingerprint
            or self.provenance.policy_version != context.policy.policy_version
        ):
            raise ValueError("Topology/policy provenance mirror mismatch")
        if self.handoff_fingerprint != compute_handoff_fingerprint(
            self.model_dump(mode="json")
        ):
            raise ValueError("Handoff fingerprint mismatch")
        if self.handoff_id != derive_enriched_handoff_id(self.handoff_fingerprint):
            raise ValueError("Handoff ID mismatch")
        return self


def serialize_phase5_handoff(handoff: Phase5InvestigationHandoff) -> bytes:
    try:
        if type(handoff) is not Phase5InvestigationHandoff:
            raise ValueError("Wrong handoff type")
        fresh = _fresh(handoff, Phase5InvestigationHandoff)
        payload = fresh.model_dump(mode="json")
        # Retain the legacy serializer's exact incident representation.
        import json

        payload["incident"] = json.loads(serialize_incident_aggregate(fresh.incident))
        _canonical_bytes(payload["topology_context"], MAX_CONTEXT_BYTES)
        return _canonical_bytes(payload, MAX_HANDOFF_BYTES)
    except Exception as exc:
        raise Phase5HandoffContractError(
            "Invalid or oversized Phase 5 handoff"
        ) from exc


def decode_phase5_handoff_structural(raw: bytes) -> Phase5InvestigationHandoff:
    """Structural/identity check only; not the H1 graph-semantic trust boundary."""
    try:
        payload = _decode_object(raw, MAX_HANDOFF_BYTES)
        _canonical_bytes(payload.get("topology_context"), MAX_CONTEXT_BYTES)
        model = Phase5InvestigationHandoff.model_validate_json(
            _canonical_bytes(payload, MAX_HANDOFF_BYTES)
        )
        serialize_phase5_handoff(model)
        return model
    except Exception as exc:
        raise Phase5HandoffContractError("Invalid Phase 5 handoff payload") from exc


def _assemble_handoff(
    incident: IncidentAggregate,
    context: BlastRadiusContext,
    as_of: datetime,
    source_kind: Literal["provided_snapshot", "canonical", "neo4j"],
    provider_class: str | None,
) -> Phase5InvestigationHandoff:
    # Context is calculated privately; no public precomputed-context input.
    incident = _fresh(incident, IncidentAggregate)
    provenance = HandoffProvenance(
        source_kind=source_kind,
        provider_class=provider_class,
        incident_revision_fingerprint=incident.revision_fingerprint,
        topology_snapshot_fingerprint=context.topology_snapshot.snapshot_fingerprint,
        policy_version=context.policy.policy_version,
    )
    payload = dict(
        schema_version="phase5-investigation-handoff/1.0",
        incident=incident.model_dump(mode="json"),
        membership_fingerprint=compute_membership_fingerprint(incident),
        topology_context=context.model_dump(mode="json"),
        provenance=provenance.model_dump(mode="json"),
        generated_at=as_of.isoformat().replace("+00:00", "Z"),
    )
    fingerprint = compute_handoff_fingerprint(payload)
    payload.update(
        handoff_fingerprint=fingerprint,
        handoff_id=str(derive_enriched_handoff_id(fingerprint)),
    )
    # The existing structural boundary checks nested contracts and full byte caps.
    return decode_phase5_handoff_structural(
        _canonical_bytes(payload, MAX_HANDOFF_BYTES)
    )


def build_phase5_handoff(
    *,
    incident: IncidentAggregate,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    policy: BlastRadiusPolicy,
    as_of: datetime,
) -> Phase5InvestigationHandoff:
    """Pure assembly from one independently pinned snapshot; no resource ownership."""
    try:
        context = calculate_blast_radius(
            incident=incident,
            snapshot=snapshot,
            expected_topology=expected_topology,
            policy=policy,
            as_of=as_of,
        )
        return _assemble_handoff(incident, context, as_of, "provided_snapshot", None)
    except Exception as exc:
        raise Phase5HandoffContractError("Cannot build Phase 5 handoff") from exc


async def prepare_phase5_handoff(
    *,
    incident: IncidentAggregate,
    provider: CanonicalTopologyProvider | Neo4jTopologyProvider,
    expected_topology: TopologySnapshotReference,
    policy: BlastRadiusPolicy,
    as_of: datetime,
) -> Phase5InvestigationHandoff:
    """Validate inputs before dispatch, fetch once, and retain caller ownership."""
    try:
        if type(provider) not in (CanonicalTopologyProvider, Neo4jTopologyProvider):
            raise ValueError("Unsupported topology provider")
        if (
            not isinstance(incident, IncidentAggregate)
            or not isinstance(expected_topology, TopologySnapshotReference)
            or not isinstance(policy, BlastRadiusPolicy)
        ):
            raise ValueError("Incident, independent pin and policy are required")
        # Bound unsafe copied inputs before recursively validating them.
        for values, limit in [
            (incident.signal_references, 1024),
            (incident.correlation_evidence, 4096),
        ]:
            if not isinstance(values, (tuple, list)) or len(values) > limit:
                raise ValueError("Invalid source collection")
        texts = [expected_topology.topology_version]
        texts.extend(ref.service for ref in incident.signal_references)
        if any(not isinstance(s, str) or not s or s != s.strip() for s in texts):
            raise ValueError("Noncanonical source name or version")
        fresh = _fresh(incident, IncidentAggregate)
        if serialize_incident_aggregate(fresh) != serialize_incident_aggregate(
            incident
        ):
            raise ValueError("Noncanonical incident")
        incident = fresh
        expected_topology = _fresh(expected_topology, TopologySnapshotReference)
        policy = _fresh(policy, BlastRadiusPolicy)
        if not isinstance(as_of, datetime) or as_of.utcoffset() != timedelta(0):
            raise ValueError("Explicit UTC as_of is required")
        age = as_of - expected_topology.captured_at
        if age < timedelta(0) or (
            policy.max_snapshot_age_seconds is not None
            and age > timedelta(seconds=policy.max_snapshot_age_seconds)
        ):
            raise ValueError("Invalid topology capture age")
        members = {ref.service for ref in incident.signal_references}
        if incident.primary_service not in members or not 1 <= len(members) <= min(
            512, policy.max_services
        ):
            raise ValueError("Invalid incident members")
        ref = incident.topology_snapshot
        if ref is not None and (
            ref.topology_id,
            ref.topology_version,
            ref.snapshot_fingerprint,
        ) != (
            expected_topology.topology_id,
            expected_topology.topology_version,
            expected_topology.snapshot_fingerprint,
        ):
            raise ValueError("Incident topology pin mismatch")
        snapshot = await provider.get_snapshot(
            expected_version=expected_topology.topology_version
        )
        context = calculate_blast_radius(
            incident=incident,
            snapshot=snapshot,
            expected_topology=expected_topology,
            policy=policy,
            as_of=as_of,
        )
        actual_class = type(provider)
        qualified_name = f"{actual_class.__module__}.{actual_class.__qualname__}"
        source_kind: Literal["canonical", "neo4j"] = (
            "canonical" if actual_class is CanonicalTopologyProvider else "neo4j"
        )
        return _assemble_handoff(incident, context, as_of, source_kind, qualified_name)
    except Exception as exc:
        raise Phase5HandoffContractError("Cannot prepare Phase 5 handoff") from exc


def validate_phase5_handoff(
    *,
    raw: bytes,
    snapshot: TopologySnapshot,
    expected_topology: TopologySnapshotReference,
    expected_policy: BlastRadiusPolicy,
) -> Phase5InvestigationHandoff:
    """Consumer trust boundary: recompute every context field from external pins.

    Recorded provider provenance remains historical attribution, not proof of a
    remote dispatch. The independent graph and policy supply semantic authority.
    """
    try:
        handoff = decode_phase5_handoff_structural(raw)
        expected = calculate_blast_radius(
            incident=handoff.incident,
            snapshot=snapshot,
            expected_topology=expected_topology,
            policy=expected_policy,
            as_of=handoff.generated_at,
        )
        if serialize_blast_radius_context(expected) != serialize_blast_radius_context(
            handoff.topology_context
        ):
            raise ValueError(
                "Handoff context differs from independently calculated context"
            )
        return handoff
    except Exception as exc:
        raise Phase5HandoffContractError("Invalid Phase 5 handoff semantics") from exc
