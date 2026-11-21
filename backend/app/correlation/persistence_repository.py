"""Tenant-scoped async persistence repository for PostgreSQL-backed incidents.

Provides a caller-session-owned async SQLAlchemy repository adapter that:
1. binds and verifies transaction-local tenant context (app.current_tenant_id);
2. resolves incident membership only within that tenant;
3. loads and locks current incident state;
4. reconstructs and validates canonical aggregate, member, evidence, timeline, and publication intents;
5. inserts append-only child records and immutable publication intents;
6. performs parent compare-and-swap (CAS) updates without owning transactions;
7. translates database failures into typed persistence errors; and
8. never commits, rolls back, begins transactions, or leaks ORM record models.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Any, Collection, Protocol, Sequence, runtime_checkable
from uuid import UUID

from sqlalchemy import func, insert, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.correlation.errors import (
    IncidentPersistenceConflictError,
    IncidentPersistenceCorruptionError,
    IncidentPersistenceError,
    IncidentPersistenceInputError,
    IncidentPersistenceStoreError,
    IncidentPersistenceTenantError,
)
from app.correlation.identity import compute_revision_fingerprint
from app.correlation.models import (
    CorrelationEvidence,
    IncidentAggregate,
    IncidentSignalReference,
    IncidentTimelineEvent,
    IncidentTimelineEventType,
)
from app.correlation.serialization import (
    deserialize_incident_aggregate,
    serialize_incident_aggregate,
)
from app.models.incident import (
    IncidentCorrelationEvidenceRecord,
    IncidentPublicationIntentRecord,
    IncidentRecord,
    IncidentSignalLinkRecord,
    IncidentTimelineEventRecord,
)

MAX_PAYLOAD_BYTES = 67108864
MAX_REVISION = 18446744073709551615


# ============================================================================
# IMMUTABLE DOMAIN & PERSISTENCE TYPES
# ============================================================================


@dataclass(frozen=True)
class IncidentVersion:
    """Lightweight immutable revision identity."""

    revision: int
    revision_fingerprint: str


@dataclass(frozen=True)
class PersistedPublicationIntent:
    """Immutable outbox intent record for downstream publication."""

    tenant_id: UUID
    incident_id: UUID
    revision: int
    schema_version: str
    revision_fingerprint: str
    intent_time: datetime
    aggregate: IncidentAggregate


@dataclass(frozen=True)
class PersistedIncidentState:
    """Complete canonically reconstructed and validated incident state."""

    aggregate: IncidentAggregate
    timeline_events: tuple[IncidentTimelineEvent, ...]
    publication_intents: tuple[PersistedPublicationIntent, ...]


# ============================================================================
# REPOSITORY PROTOCOL
# ============================================================================


@runtime_checkable
class IncidentPersistenceRepository(Protocol):
    """Protocol defining the tenant-bound persistence repository contract."""

    @property
    def tenant_id(self) -> UUID: ...

    async def bind_tenant_context(self) -> None:
        """Bind session-local tenant context and verify readback."""
        ...

    async def resolve_incident_ids(
        self, signal_ids: Collection[UUID]
    ) -> tuple[UUID, ...]:
        """Resolve member incident IDs for the given signal IDs within the tenant."""
        ...

    async def load_incident(
        self, incident_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        """Load and canonically validate full incident state."""
        ...

    async def load_incident_version(
        self, incident_id: UUID, *, for_update: bool = False
    ) -> IncidentVersion | None:
        """Load current incident revision and fingerprint."""
        ...

    async def insert_incident(self, aggregate: IncidentAggregate) -> None:
        """Insert the durable parent incident record."""
        ...

    async def append_signal_links(
        self, incident_id: UUID, links: Sequence[IncidentSignalReference]
    ) -> None:
        """Append signal membership records."""
        ...

    async def append_correlation_evidence(
        self, incident_id: UUID, evidence: Sequence[CorrelationEvidence]
    ) -> None:
        """Append pairwise correlation evidence records."""
        ...

    async def append_timeline_events(
        self, events: Sequence[IncidentTimelineEvent]
    ) -> None:
        """Append timeline event audit records."""
        ...

    async def append_publication_intent(
        self, aggregate: IncidentAggregate, *, intent_time: datetime
    ) -> None:
        """Append immutable publication intent for this revision."""
        ...

    async def update_incident_cas(
        self,
        aggregate: IncidentAggregate,
        *,
        expected_revision: int,
        expected_revision_fingerprint: str,
    ) -> bool:
        """Atomically update parent incident with compare-and-swap predicate."""
        ...

    async def load_publication_intent(
        self, incident_id: UUID, revision: int
    ) -> PersistedPublicationIntent | None:
        """Load a specific publication intent by incident and revision."""
        ...


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _validate_revision_int(rev: Any, param_name: str = "revision") -> int:
    """Validate revision is a positive integer in [1, 2^64-1]."""
    if (
        not isinstance(rev, int)
        or isinstance(rev, bool)
        or rev < 1
        or rev > MAX_REVISION
    ):
        raise IncidentPersistenceInputError(
            f"Invalid {param_name}: must be an integer between 1 and {MAX_REVISION}, got {rev!r}"
        )
    return rev


def _validate_decimal_revision(val: Any) -> int:
    """Validate and convert database Numeric revision to canonical integer revision."""
    if (
        isinstance(val, Decimal)
        and not isinstance(val, bool)
        and val.is_finite()
        and val == val.to_integral_value()
    ):
        int_val = int(val)
        if 1 <= int_val <= MAX_REVISION:
            return int_val
    raise IncidentPersistenceCorruptionError(
        f"Stored revision must be a finite integral Decimal in [1, {MAX_REVISION}], got {val!r}"
    )


_decimal_to_revision = _validate_decimal_revision


def _validate_utc_datetime(dt: Any, param_name: str = "datetime") -> datetime:
    """Validate datetime is timezone-aware UTC."""
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        raise IncidentPersistenceInputError(
            f"{param_name} must be a timezone-aware datetime"
        )
    offset = dt.utcoffset()
    if offset is not None and offset != timezone.utc.utcoffset(None):
        raise IncidentPersistenceInputError(
            f"{param_name} must have UTC offset (+00:00), got {offset}"
        )
    return dt


def _validate_fingerprint(fp: Any, param_name: str = "revision_fingerprint") -> str:
    """Validate fingerprint is a 64-character lowercase hexadecimal string."""
    if not isinstance(fp, str) or len(fp) != 64 or not re.match(r"^[0-9a-f]{64}$", fp):
        raise IncidentPersistenceInputError(
            f"{param_name} must be a 64-character lowercase hex string, got {fp!r}"
        )
    return fp


def _translate_database_error(exc: Exception) -> IncidentPersistenceError:
    """Translate lower-level database exceptions to the typed persistence error hierarchy."""
    if isinstance(exc, IncidentPersistenceError):
        return exc

    if isinstance(exc, IntegrityError):
        orig = getattr(exc, "orig", None)
        sqlstate = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
        if sqlstate == "23505":
            return IncidentPersistenceConflictError(
                f"Persistence unique constraint conflict: {exc}"
            )
        if sqlstate in ("23503", "23514", "23502"):
            return IncidentPersistenceInputError(
                f"Persistence constraint violation ({sqlstate}): {exc}"
            )
        return IncidentPersistenceStoreError(
            f"Persistence integrity error ({sqlstate}): {exc}"
        )

    if isinstance(exc, (DBAPIError, SQLAlchemyError)):
        orig = getattr(exc, "orig", None)
        sqlstate = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
        if sqlstate == "42501":
            return IncidentPersistenceTenantError(
                f"Persistence tenant permission / RLS violation ({sqlstate}): {exc}"
            )
        return IncidentPersistenceStoreError(f"Database persistence store error: {exc}")

    return IncidentPersistenceStoreError(f"Unexpected persistence store error: {exc}")


# ============================================================================
# CONCRETE SQLALCHEMY REPOSITORY IMPLEMENTATION
# ============================================================================


class SqlAlchemyIncidentPersistenceRepository(IncidentPersistenceRepository):
    """Caller-session-owned async SQLAlchemy repository implementation."""

    def __init__(self, session: AsyncSession, tenant_id: UUID) -> None:
        if not isinstance(session, AsyncSession):
            raise IncidentPersistenceInputError(
                "session must be an AsyncSession instance"
            )
        if not isinstance(tenant_id, UUID):
            raise IncidentPersistenceInputError("tenant_id must be a UUID instance")
        self._session = session
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    @property
    def session(self) -> AsyncSession:
        return self._session

    def _ensure_active_transaction(self) -> None:
        """Enforce that caller owns an active database transaction."""
        if not self._session.in_transaction():
            raise IncidentPersistenceStoreError(
                "AsyncSession must be in an active transaction owned by the caller"
            )

    async def _verify_tenant_context(self) -> None:
        """Verify transaction-local tenant context matches repository tenant_id."""
        self._ensure_active_transaction()
        try:
            val = await self._session.scalar(
                select(func.current_setting("app.current_tenant_id", True))
            )
        except Exception as exc:
            raise _translate_database_error(exc) from exc

        if val is None or val == "":
            raise IncidentPersistenceTenantError(
                "Transaction-local tenant context 'app.current_tenant_id' is not set"
            )
        try:
            ctx_uuid = UUID(val)
        except (ValueError, TypeError) as exc:
            raise IncidentPersistenceTenantError(
                f"Invalid tenant context value: {val!r}"
            ) from exc

        if ctx_uuid != self._tenant_id:
            raise IncidentPersistenceTenantError(
                f"Active tenant context mismatch: expected {self._tenant_id}, got {ctx_uuid}"
            )

    def _validate_incident_aggregate(self, aggregate: IncidentAggregate) -> bytes:
        """Validate aggregate model, tenant ownership, datetimes, and canonical bytes."""
        if not isinstance(aggregate, IncidentAggregate):
            raise IncidentPersistenceInputError(
                f"aggregate must be an IncidentAggregate instance, got {type(aggregate).__name__}"
            )
        if aggregate.tenant_id != self._tenant_id:
            raise IncidentPersistenceTenantError(
                f"Incident tenant_id {aggregate.tenant_id} does not match repository tenant_id {self._tenant_id}"
            )
        _validate_revision_int(aggregate.revision, "aggregate.revision")
        _validate_fingerprint(
            aggregate.revision_fingerprint, "aggregate.revision_fingerprint"
        )
        _validate_utc_datetime(aggregate.started_at, "aggregate.started_at")
        _validate_utc_datetime(aggregate.last_seen_at, "aggregate.last_seen_at")
        _validate_utc_datetime(aggregate.created_at, "aggregate.created_at")
        _validate_utc_datetime(aggregate.updated_at, "aggregate.updated_at")

        # Revalidate revision fingerprint matches aggregate state
        try:
            expected_fp = compute_revision_fingerprint(aggregate)
        except Exception as exc:
            raise IncidentPersistenceInputError(
                f"Failed to compute revision fingerprint for aggregate: {exc}"
            ) from exc
        if aggregate.revision_fingerprint != expected_fp:
            raise IncidentPersistenceInputError(
                f"Aggregate revision_fingerprint mismatch: expected {expected_fp}, got {aggregate.revision_fingerprint}"
            )

        # Canonical serialization & deserialization check
        try:
            payload_bytes = serialize_incident_aggregate(aggregate)
        except Exception as exc:
            raise IncidentPersistenceInputError(
                f"Failed to serialize aggregate: {exc}"
            ) from exc

        if not (2 <= len(payload_bytes) <= MAX_PAYLOAD_BYTES):
            raise IncidentPersistenceInputError(
                f"Aggregate payload size {len(payload_bytes)} bytes exceeds 64 MiB limit: out of bounds [2, {MAX_PAYLOAD_BYTES}]"
            )

        try:
            deserialized = deserialize_incident_aggregate(payload_bytes)
        except Exception as exc:
            raise IncidentPersistenceInputError(
                f"Failed to deserialize serialized aggregate: {exc}"
            ) from exc

        if deserialized != aggregate:
            raise IncidentPersistenceInputError(
                "Deserialized aggregate does not match input aggregate"
            )

        try:
            reserialized = serialize_incident_aggregate(deserialized)
        except Exception as exc:
            raise IncidentPersistenceInputError(
                f"Failed to reserialize deserialized aggregate: {exc}"
            ) from exc

        if reserialized != payload_bytes:
            raise IncidentPersistenceInputError(
                "Reserialized aggregate payload does not match canonical payload bytes"
            )

        return payload_bytes

    async def bind_tenant_context(self) -> None:
        """Bind session-local tenant context and verify readback."""
        self._ensure_active_transaction()
        try:
            await self._session.execute(
                select(
                    func.set_config(
                        "app.current_tenant_id",
                        str(self._tenant_id),
                        True,  # is_local = true (transaction-scoped)
                    )
                )
            )
            readback = await self._session.scalar(
                select(func.current_setting("app.current_tenant_id", True))
            )
        except Exception as exc:
            raise _translate_database_error(exc) from exc

        if readback is None or UUID(readback) != self._tenant_id:
            raise IncidentPersistenceTenantError(
                f"Failed to bind tenant context: readback mismatch for {self._tenant_id}"
            )

    async def resolve_incident_ids(
        self, signal_ids: Collection[UUID]
    ) -> tuple[UUID, ...]:
        """Resolve member incident IDs for the given signal IDs within the tenant."""
        if isinstance(signal_ids, (str, bytes)) or not isinstance(
            signal_ids, Collection
        ):
            raise IncidentPersistenceInputError(
                "signal_ids must be a collection of UUIDs"
            )

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        if not signal_ids:
            return ()

        uuid_list: list[UUID] = []
        for sid in signal_ids:
            if not isinstance(sid, UUID):
                raise IncidentPersistenceInputError(
                    f"Invalid signal_id: expected UUID, got {type(sid).__name__}"
                )
            uuid_list.append(sid)

        stmt = (
            select(IncidentSignalLinkRecord.incident_id)
            .where(
                IncidentSignalLinkRecord.tenant_id == self._tenant_id,
                IncidentSignalLinkRecord.signal_id.in_(uuid_list),
            )
            .distinct()
            .order_by(IncidentSignalLinkRecord.incident_id)
        )
        try:
            result = await self._session.execute(stmt)
            rows = result.scalars().all()
            return tuple(rows)
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def load_incident_version(
        self, incident_id: UUID, *, for_update: bool = False
    ) -> IncidentVersion | None:
        """Load current incident revision and fingerprint."""
        if not isinstance(incident_id, UUID):
            raise IncidentPersistenceInputError("incident_id must be a UUID")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = select(
            IncidentRecord.revision, IncidentRecord.revision_fingerprint
        ).where(
            IncidentRecord.tenant_id == self._tenant_id,
            IncidentRecord.incident_id == incident_id,
        )
        if for_update:
            stmt = stmt.with_for_update()

        try:
            result = await self._session.execute(stmt)
            row = result.first()
            if row is None:
                return None
            rev_int = _validate_decimal_revision(row[0])
            fp = _validate_fingerprint(row[1])
            return IncidentVersion(revision=rev_int, revision_fingerprint=fp)
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def load_incident(
        self, incident_id: UUID, *, for_update: bool = False
    ) -> PersistedIncidentState | None:
        """Load and canonically validate full incident state."""
        if not isinstance(incident_id, UUID):
            raise IncidentPersistenceInputError("incident_id must be a UUID")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = select(IncidentRecord).where(
            IncidentRecord.tenant_id == self._tenant_id,
            IncidentRecord.incident_id == incident_id,
        )
        if for_update:
            stmt = stmt.with_for_update()

        try:
            res = await self._session.execute(stmt)
            row = res.scalar_one_or_none()
            if row is None:
                return None

            # Canonical payload deserialization
            raw_payload = bytes(row.aggregate_payload)
            if len(raw_payload) > MAX_PAYLOAD_BYTES:
                raise IncidentPersistenceCorruptionError(
                    f"Aggregate payload exceeds byte limit: {len(raw_payload)}"
                )

            try:
                agg = deserialize_incident_aggregate(raw_payload)
            except Exception as exc:
                raise IncidentPersistenceCorruptionError(
                    f"Failed to deserialize aggregate payload: {exc}"
                ) from exc

            # Canonical payload re-serialization bit-for-bit check
            reserialized = serialize_incident_aggregate(agg)
            if reserialized != raw_payload:
                raise IncidentPersistenceCorruptionError(
                    "Persisted aggregate payload is not canonically serialized"
                )

            # Reconcile parent columns with aggregate state
            stored_rev = _validate_decimal_revision(row.revision)
            if row.tenant_id != agg.tenant_id or row.tenant_id != self._tenant_id:
                raise IncidentPersistenceCorruptionError("Parent tenant_id mismatch")
            if row.incident_id != agg.incident_id or row.incident_id != incident_id:
                raise IncidentPersistenceCorruptionError("Parent incident_id mismatch")
            if row.schema_version != agg.schema_version:
                raise IncidentPersistenceCorruptionError(
                    "Parent schema_version mismatch"
                )
            if row.incident_number != agg.incident_number:
                raise IncidentPersistenceCorruptionError(
                    "Parent incident_number mismatch"
                )
            if row.environment != agg.environment:
                raise IncidentPersistenceCorruptionError("Parent environment mismatch")
            if row.status != agg.status:
                raise IncidentPersistenceCorruptionError("Parent status mismatch")
            if row.correlation_state != agg.correlation_state:
                raise IncidentPersistenceCorruptionError(
                    "Parent correlation_state mismatch"
                )
            if row.severity != agg.severity:
                raise IncidentPersistenceCorruptionError("Parent severity mismatch")
            if row.severity_policy_version != agg.severity_policy_version:
                raise IncidentPersistenceCorruptionError(
                    "Parent severity_policy_version mismatch"
                )
            if row.started_at != agg.started_at:
                raise IncidentPersistenceCorruptionError("Parent started_at mismatch")
            if row.last_seen_at != agg.last_seen_at:
                raise IncidentPersistenceCorruptionError("Parent last_seen_at mismatch")
            if row.created_at != agg.created_at:
                raise IncidentPersistenceCorruptionError("Parent created_at mismatch")
            if row.updated_at != agg.updated_at:
                raise IncidentPersistenceCorruptionError("Parent updated_at mismatch")
            if stored_rev != agg.revision:
                raise IncidentPersistenceCorruptionError("Parent revision mismatch")
            if row.primary_service != agg.primary_service:
                raise IncidentPersistenceCorruptionError(
                    "Parent primary_service mismatch"
                )
            if list(row.affected_services) != list(agg.affected_services):
                raise IncidentPersistenceCorruptionError(
                    "Parent affected_services mismatch"
                )
            if row.revision_fingerprint != agg.revision_fingerprint:
                raise IncidentPersistenceCorruptionError(
                    "Parent revision_fingerprint mismatch"
                )
            if compute_revision_fingerprint(agg) != agg.revision_fingerprint:
                raise IncidentPersistenceCorruptionError(
                    "Parent aggregate revision_fingerprint mismatch with computed fingerprint"
                )

            # Load and verify signal links
            link_stmt = (
                select(IncidentSignalLinkRecord)
                .where(
                    IncidentSignalLinkRecord.tenant_id == self._tenant_id,
                    IncidentSignalLinkRecord.incident_id == incident_id,
                )
                .order_by(
                    IncidentSignalLinkRecord.event_time,
                    IncidentSignalLinkRecord.signal_id,
                )
            )
            link_rows = (await self._session.execute(link_stmt)).scalars().all()
            if len(link_rows) != len(agg.signal_references):
                raise IncidentPersistenceCorruptionError(
                    f"Member signal count mismatch: DB has {len(link_rows)}, aggregate has {len(agg.signal_references)}"
                )

            # The members in aggregate must match link rows
            member_dict = {m.signal_id: m for m in agg.signal_references}
            for lk in link_rows:
                mem = member_dict.get(lk.signal_id)
                if mem is None:
                    raise IncidentPersistenceCorruptionError(
                        f"Signal link {lk.signal_id} not found in aggregate members"
                    )
                if (
                    lk.tenant_id != self._tenant_id
                    or lk.semantic_fingerprint != mem.semantic_fingerprint
                    or lk.environment != mem.environment
                    or lk.service != mem.service
                    or lk.event_time != mem.event_time
                    or lk.severity != mem.severity
                    or lk.anomaly_score != mem.anomaly_score
                ):
                    raise IncidentPersistenceCorruptionError(
                        f"Member signal attributes mismatch for {lk.signal_id}"
                    )

            # Load and verify correlation evidence
            ev_stmt = (
                select(IncidentCorrelationEvidenceRecord)
                .where(
                    IncidentCorrelationEvidenceRecord.tenant_id == self._tenant_id,
                    IncidentCorrelationEvidenceRecord.incident_id == incident_id,
                )
                .order_by(
                    IncidentCorrelationEvidenceRecord.left_signal_id,
                    IncidentCorrelationEvidenceRecord.right_signal_id,
                )
            )
            ev_rows = (await self._session.execute(ev_stmt)).scalars().all()
            if len(ev_rows) != len(agg.correlation_evidence):
                raise IncidentPersistenceCorruptionError(
                    f"Evidence count mismatch: DB has {len(ev_rows)}, aggregate has {len(agg.correlation_evidence)}"
                )

            ev_dict = {
                (e.left_signal_id, e.right_signal_id): e
                for e in agg.correlation_evidence
            }
            for er in ev_rows:
                ev_key = (er.left_signal_id, er.right_signal_id)
                ev = ev_dict.get(ev_key)
                if ev is None:
                    raise IncidentPersistenceCorruptionError(
                        f"Evidence pair {ev_key} not found in aggregate"
                    )
                if (
                    er.tenant_id != self._tenant_id
                    or er.temporal_score != ev.temporal_score
                    or er.service_score != ev.service_score
                    or er.topology_score != ev.topology_score
                    or er.symptom_score != ev.symptom_score
                    or er.context_score != ev.context_score
                    or er.weighted_score != ev.weighted_score
                    or er.accepted != ev.accepted
                    or er.policy_version != ev.policy_version
                    or er.reason != ev.reason
                ):
                    raise IncidentPersistenceCorruptionError(
                        f"Correlation evidence attributes mismatch for {ev_key}"
                    )

            # Load publication intents first to cross-reference timeline events
            pi_stmt = (
                select(IncidentPublicationIntentRecord)
                .where(
                    IncidentPublicationIntentRecord.tenant_id == self._tenant_id,
                    IncidentPublicationIntentRecord.incident_id == incident_id,
                )
                .order_by(IncidentPublicationIntentRecord.revision)
            )
            pi_rows = (await self._session.execute(pi_stmt)).scalars().all()

            persisted_intents: list[PersistedPublicationIntent] = []
            intent_by_rev: dict[int, PersistedPublicationIntent] = {}
            has_current_intent = False
            for pir in pi_rows:
                pi_rev = _validate_decimal_revision(pir.revision)
                if pi_rev > agg.revision:
                    raise IncidentPersistenceCorruptionError(
                        f"Publication intent revision {pi_rev} exceeds current incident revision {agg.revision}"
                    )
                if pir.tenant_id != self._tenant_id or pir.incident_id != incident_id:
                    raise IncidentPersistenceCorruptionError(
                        f"Publication intent identity mismatch for revision {pi_rev}"
                    )

                pi_raw = bytes(pir.aggregate_payload)
                try:
                    pi_agg = deserialize_incident_aggregate(pi_raw)
                except Exception as exc:
                    raise IncidentPersistenceCorruptionError(
                        f"Failed to deserialize publication intent aggregate: {exc}"
                    ) from exc
                pi_reserialized = serialize_incident_aggregate(pi_agg)
                if pi_reserialized != pi_raw:
                    raise IncidentPersistenceCorruptionError(
                        f"Publication intent payload for revision {pi_rev} not canonical"
                    )
                if (
                    pi_agg.tenant_id != self._tenant_id
                    or pi_agg.incident_id != incident_id
                    or pir.schema_version != pi_agg.schema_version
                    or pi_rev != pi_agg.revision
                    or pir.revision_fingerprint != pi_agg.revision_fingerprint
                    or pir.revision_fingerprint != compute_revision_fingerprint(pi_agg)
                ):
                    raise IncidentPersistenceCorruptionError(
                        f"Publication intent attributes mismatch for revision {pi_rev}"
                    )

                persisted_pi = PersistedPublicationIntent(
                    tenant_id=pir.tenant_id,
                    incident_id=pir.incident_id,
                    revision=pi_rev,
                    schema_version=pir.schema_version,
                    revision_fingerprint=pir.revision_fingerprint,
                    intent_time=pir.intent_time,
                    aggregate=pi_agg,
                )
                persisted_intents.append(persisted_pi)
                intent_by_rev[pi_rev] = persisted_pi
                if pi_rev == agg.revision:
                    has_current_intent = True
                    # Must match current aggregate
                    if pi_agg != agg:
                        raise IncidentPersistenceCorruptionError(
                            "Publication intent aggregate does not match parent aggregate for current revision"
                        )

            if not has_current_intent:
                raise IncidentPersistenceCorruptionError(
                    f"Missing publication intent for current incident revision {agg.revision}"
                )

            # Load timeline events and check ordinal continuity per revision
            tl_stmt = (
                select(IncidentTimelineEventRecord)
                .where(
                    IncidentTimelineEventRecord.tenant_id == self._tenant_id,
                    IncidentTimelineEventRecord.incident_id == incident_id,
                )
                .order_by(
                    IncidentTimelineEventRecord.event_time,
                    IncidentTimelineEventRecord.revision,
                    IncidentTimelineEventRecord.event_ordinal,
                    IncidentTimelineEventRecord.event_id,
                )
            )
            tl_rows = (await self._session.execute(tl_stmt)).scalars().all()

            # Group by revision and check ordinals: must be contiguous 0, 1, 2...
            rev_ordinals: dict[int, list[int]] = {}
            timeline_events_list: list[IncidentTimelineEvent] = []
            for tr in tl_rows:
                tr_rev = _validate_decimal_revision(tr.revision)
                if tr_rev > agg.revision:
                    raise IncidentPersistenceCorruptionError(
                        f"Timeline event revision {tr_rev} exceeds current incident revision {agg.revision}"
                    )
                if tr.tenant_id != self._tenant_id or tr.incident_id != incident_id:
                    raise IncidentPersistenceCorruptionError(
                        f"Timeline event identity mismatch for event {tr.event_id}"
                    )

                matching_intent = intent_by_rev.get(tr_rev)
                if matching_intent is None:
                    raise IncidentPersistenceCorruptionError(
                        f"Timeline event revision {tr_rev} has no matching publication intent"
                    )
                if tr.revision_fingerprint != matching_intent.revision_fingerprint:
                    raise IncidentPersistenceCorruptionError(
                        f"Timeline event fingerprint mismatch with publication intent for revision {tr_rev}"
                    )

                rev_ordinals.setdefault(tr_rev, []).append(tr.event_ordinal)
                timeline_events_list.append(
                    IncidentTimelineEvent(
                        schema_version=tr.schema_version,
                        event_id=tr.event_id,
                        incident_id=tr.incident_id,
                        revision=tr_rev,
                        event_time=tr.event_time,
                        event_type=IncidentTimelineEventType(tr.event_type),
                        source_signal_id=tr.source_signal_id,
                        summary=tr.summary,
                        revision_fingerprint=tr.revision_fingerprint,
                    )
                )

            for rev, ords in rev_ordinals.items():
                expected_ords = list(range(len(ords)))
                if sorted(ords) != expected_ords:
                    raise IncidentPersistenceCorruptionError(
                        f"Timeline event ordinals not contiguous for revision {rev}: {ords}"
                    )

            return PersistedIncidentState(
                aggregate=agg,
                timeline_events=tuple(timeline_events_list),
                publication_intents=tuple(persisted_intents),
            )
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def insert_incident(self, aggregate: IncidentAggregate) -> None:
        """Insert the durable parent incident record."""
        self._ensure_active_transaction()

        payload_bytes = self._validate_incident_aggregate(aggregate)
        await self._verify_tenant_context()

        record = IncidentRecord(
            tenant_id=self._tenant_id,
            incident_id=aggregate.incident_id,
            schema_version=aggregate.schema_version,
            incident_number=aggregate.incident_number,
            environment=aggregate.environment,
            status=aggregate.status,
            correlation_state=aggregate.correlation_state,
            severity=aggregate.severity,
            severity_policy_version=aggregate.severity_policy_version,
            started_at=aggregate.started_at,
            last_seen_at=aggregate.last_seen_at,
            created_at=aggregate.created_at,
            updated_at=aggregate.updated_at,
            revision=Decimal(aggregate.revision),
            primary_service=aggregate.primary_service,
            affected_services=list(aggregate.affected_services),
            revision_fingerprint=aggregate.revision_fingerprint,
            aggregate_payload=payload_bytes,
        )

        try:
            self._session.add(record)
            await self._session.flush()
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def append_signal_links(
        self, incident_id: UUID, links: Sequence[IncidentSignalReference]
    ) -> None:
        """Append signal membership records."""
        if not isinstance(incident_id, UUID):
            raise IncidentPersistenceInputError("incident_id must be a UUID")
        if isinstance(links, (str, bytes)) or not isinstance(links, Sequence):
            raise IncidentPersistenceInputError("links must be a sequence")

        self._ensure_active_transaction()

        values = []
        foreign_tenant_link: IncidentSignalReference | None = None
        for link in links:
            if not isinstance(link, IncidentSignalReference):
                raise IncidentPersistenceInputError(
                    f"links elements must be IncidentSignalReference instances, got {type(link).__name__}"
                )
            try:
                IncidentSignalReference.model_validate(link.model_dump(mode="python"))
            except Exception as exc:
                raise IncidentPersistenceInputError(
                    f"Signal link model validation failed: {exc}"
                ) from exc

            _validate_utc_datetime(link.event_time, "link.event_time")
            _validate_fingerprint(
                link.semantic_fingerprint, "link.semantic_fingerprint"
            )
            if link.tenant_id != self._tenant_id and foreign_tenant_link is None:
                foreign_tenant_link = link

            values.append(
                {
                    "tenant_id": link.tenant_id,
                    "incident_id": incident_id,
                    "signal_id": link.signal_id,
                    "semantic_fingerprint": link.semantic_fingerprint,
                    "environment": link.environment,
                    "service": link.service,
                    "event_time": link.event_time,
                    "severity": link.severity,
                    "anomaly_score": link.anomaly_score,
                }
            )

        if foreign_tenant_link is not None:
            raise IncidentPersistenceTenantError(
                f"Signal link tenant_id {foreign_tenant_link.tenant_id} does not match repository tenant_id {self._tenant_id}"
            )

        await self._verify_tenant_context()

        if not values:
            return

        try:
            await self._session.execute(insert(IncidentSignalLinkRecord), values)
            await self._session.flush()
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def append_correlation_evidence(
        self, incident_id: UUID, evidence: Sequence[CorrelationEvidence]
    ) -> None:
        """Append pairwise correlation evidence records."""
        if not isinstance(incident_id, UUID):
            raise IncidentPersistenceInputError("incident_id must be a UUID")
        if isinstance(evidence, (str, bytes)) or not isinstance(evidence, Sequence):
            raise IncidentPersistenceInputError("evidence must be a sequence")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        if not evidence:
            return

        values = []
        for ev in evidence:
            if not isinstance(ev, CorrelationEvidence):
                raise IncidentPersistenceInputError(
                    f"evidence elements must be CorrelationEvidence instances, got {type(ev).__name__}"
                )
            try:
                CorrelationEvidence.model_validate(ev.model_dump(mode="python"))
            except Exception as exc:
                raise IncidentPersistenceInputError(
                    f"Correlation evidence model validation failed: {exc}"
                ) from exc

            if ev.left_signal_id >= ev.right_signal_id:
                raise IncidentPersistenceInputError(
                    f"Evidence pair must satisfy left_signal_id < right_signal_id: {ev.left_signal_id} >= {ev.right_signal_id}"
                )
            if not ev.accepted:
                raise IncidentPersistenceInputError("Evidence must have accepted=True")

            values.append(
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": incident_id,
                    "left_signal_id": ev.left_signal_id,
                    "right_signal_id": ev.right_signal_id,
                    "temporal_score": ev.temporal_score,
                    "service_score": ev.service_score,
                    "topology_score": ev.topology_score,
                    "symptom_score": ev.symptom_score,
                    "context_score": ev.context_score,
                    "weighted_score": ev.weighted_score,
                    "accepted": ev.accepted,
                    "policy_version": ev.policy_version,
                    "reason": ev.reason,
                }
            )

        try:
            await self._session.execute(
                insert(IncidentCorrelationEvidenceRecord), values
            )
            await self._session.flush()
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def append_timeline_events(
        self, events: Sequence[IncidentTimelineEvent]
    ) -> None:
        """Append timeline event audit records."""
        if isinstance(events, (str, bytes)) or not isinstance(events, Sequence):
            raise IncidentPersistenceInputError("events must be a sequence")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        if not events:
            return

        first = events[0]
        if not isinstance(first, IncidentTimelineEvent):
            raise IncidentPersistenceInputError(
                f"events elements must be IncidentTimelineEvent instances, got {type(first).__name__}"
            )
        batch_incident_id = first.incident_id
        batch_revision = first.revision
        batch_fingerprint = first.revision_fingerprint

        _validate_revision_int(batch_revision, "timeline revision")
        _validate_fingerprint(batch_fingerprint, "timeline revision_fingerprint")

        values = []
        for ordinal, ev in enumerate(events):
            if not isinstance(ev, IncidentTimelineEvent):
                raise IncidentPersistenceInputError(
                    f"events elements must be IncidentTimelineEvent instances, got {type(ev).__name__}"
                )
            try:
                IncidentTimelineEvent.model_validate(ev.model_dump(mode="python"))
            except Exception as exc:
                raise IncidentPersistenceInputError(
                    f"Incident timeline event model validation failed: {exc}"
                ) from exc

            if ev.incident_id != batch_incident_id:
                raise IncidentPersistenceInputError(
                    f"Mixed incident batch: expected {batch_incident_id}, got {ev.incident_id}"
                )
            if ev.revision != batch_revision:
                raise IncidentPersistenceInputError(
                    f"Mixed revision batch: expected {batch_revision}, got {ev.revision}"
                )
            if ev.revision_fingerprint != batch_fingerprint:
                raise IncidentPersistenceInputError(
                    f"Mixed fingerprint batch: expected {batch_fingerprint}, got {ev.revision_fingerprint}"
                )
            _validate_utc_datetime(ev.event_time, "event_time")

            values.append(
                {
                    "tenant_id": self._tenant_id,
                    "incident_id": ev.incident_id,
                    "event_id": ev.event_id,
                    "schema_version": ev.schema_version,
                    "revision": Decimal(ev.revision),
                    "event_ordinal": ordinal,
                    "event_time": ev.event_time,
                    "event_type": ev.event_type,
                    "source_signal_id": ev.source_signal_id,
                    "summary": ev.summary,
                    "revision_fingerprint": ev.revision_fingerprint,
                }
            )

        try:
            await self._session.execute(insert(IncidentTimelineEventRecord), values)
            await self._session.flush()
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def append_publication_intent(
        self, aggregate: IncidentAggregate, *, intent_time: datetime
    ) -> None:
        """Append immutable publication intent for this revision."""
        self._ensure_active_transaction()

        _validate_utc_datetime(intent_time, "intent_time")
        payload_bytes = self._validate_incident_aggregate(aggregate)
        await self._verify_tenant_context()

        record = IncidentPublicationIntentRecord(
            tenant_id=self._tenant_id,
            incident_id=aggregate.incident_id,
            revision=Decimal(aggregate.revision),
            schema_version=aggregate.schema_version,
            revision_fingerprint=aggregate.revision_fingerprint,
            intent_time=intent_time,
            aggregate_payload=payload_bytes,
        )

        try:
            self._session.add(record)
            await self._session.flush()
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def update_incident_cas(
        self,
        aggregate: IncidentAggregate,
        *,
        expected_revision: int,
        expected_revision_fingerprint: str,
    ) -> bool:
        """Atomically update parent incident with compare-and-swap predicate."""
        self._ensure_active_transaction()

        _validate_revision_int(expected_revision, "expected_revision")
        _validate_fingerprint(
            expected_revision_fingerprint, "expected_revision_fingerprint"
        )
        payload_bytes = self._validate_incident_aggregate(aggregate)
        await self._verify_tenant_context()

        stmt = (
            update(IncidentRecord)
            .where(
                IncidentRecord.tenant_id == self._tenant_id,
                IncidentRecord.incident_id == aggregate.incident_id,
                IncidentRecord.revision == Decimal(expected_revision),
                IncidentRecord.revision_fingerprint == expected_revision_fingerprint,
            )
            .values(
                status=aggregate.status,
                correlation_state=aggregate.correlation_state,
                severity=aggregate.severity,
                started_at=aggregate.started_at,
                last_seen_at=aggregate.last_seen_at,
                updated_at=aggregate.updated_at,
                revision=Decimal(aggregate.revision),
                affected_services=list(aggregate.affected_services),
                revision_fingerprint=aggregate.revision_fingerprint,
                aggregate_payload=payload_bytes,
            )
        )

        try:
            result = await self._session.execute(stmt)
            await self._session.flush()
            rowcount = getattr(result, "rowcount", None)
            if rowcount == 1:
                return True
            elif rowcount == 0:
                return False
            else:
                raise IncidentPersistenceCorruptionError(
                    f"CAS update affected unexpected rowcount: {rowcount}"
                )
        except Exception as exc:
            raise _translate_database_error(exc) from exc

    async def load_publication_intent(
        self, incident_id: UUID, revision: int
    ) -> PersistedPublicationIntent | None:
        """Load a specific publication intent by incident and revision."""
        if not isinstance(incident_id, UUID):
            raise IncidentPersistenceInputError("incident_id must be a UUID")
        rev_int = _validate_revision_int(revision, "revision")

        self._ensure_active_transaction()
        await self._verify_tenant_context()

        stmt = select(IncidentPublicationIntentRecord).where(
            IncidentPublicationIntentRecord.tenant_id == self._tenant_id,
            IncidentPublicationIntentRecord.incident_id == incident_id,
            IncidentPublicationIntentRecord.revision == Decimal(rev_int),
        )

        try:
            result = await self._session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                return None

            raw_payload = bytes(row.aggregate_payload)
            try:
                agg = deserialize_incident_aggregate(raw_payload)
            except Exception as exc:
                raise IncidentPersistenceCorruptionError(
                    f"Failed to deserialize publication intent aggregate payload: {exc}"
                ) from exc

            reserialized = serialize_incident_aggregate(agg)
            if reserialized != raw_payload:
                raise IncidentPersistenceCorruptionError(
                    "Publication intent aggregate payload is not canonical"
                )

            stored_rev = _validate_decimal_revision(row.revision)
            if (
                row.tenant_id != self._tenant_id
                or row.tenant_id != agg.tenant_id
                or row.incident_id != incident_id
                or row.incident_id != agg.incident_id
                or row.schema_version != agg.schema_version
                or stored_rev != agg.revision
                or row.revision_fingerprint != agg.revision_fingerprint
                or row.revision_fingerprint != compute_revision_fingerprint(agg)
            ):
                raise IncidentPersistenceCorruptionError(
                    "Publication intent metadata does not match payload"
                )

            return PersistedPublicationIntent(
                tenant_id=row.tenant_id,
                incident_id=row.incident_id,
                revision=stored_rev,
                schema_version=row.schema_version,
                revision_fingerprint=row.revision_fingerprint,
                intent_time=row.intent_time,
                aggregate=agg,
            )
        except Exception as exc:
            raise _translate_database_error(exc) from exc
