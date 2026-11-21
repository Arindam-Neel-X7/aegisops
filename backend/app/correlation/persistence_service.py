"""Atomic incident persistence transaction service.

Implements the application transaction boundary for Task 4.9 over the approved
aggregate transition service and tenant-scoped PostgreSQL repository.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from datetime import datetime, timedelta
import re
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from app.correlation.aggregate import (
    IncidentAggregationResult,
    create_or_update_incident,
)
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.errors import (
    AggregateInputError,
    AggregateStateError,
    IncidentPersistenceConflictError,
    IncidentPersistenceCorruptionError,
    IncidentPersistenceError,
    IncidentPersistenceInputError,
    IncidentPersistenceStoreError,
    IncidentPersistenceTenantError,
)
from app.correlation.grouping import CorrelationGroup, GroupingMemberReference
from app.correlation.identity import derive_incident_id
from app.correlation.models import CorrelationWindowState, IncidentAggregate
from app.correlation.persistence_repository import (
    SqlAlchemyIncidentPersistenceRepository,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_MAX_UINT64 = 18446744073709551615
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")


def _is_sqlstate_23505(exc: BaseException | None) -> bool:
    """Detect PostgreSQL SQLSTATE 23505 without relying on exception message text."""
    curr = exc
    visited: set[int] = set()
    while curr is not None and id(curr) not in visited:
        visited.add(id(curr))
        orig = getattr(curr, "orig", None)
        if orig is not None:
            code = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
            if code == "23505":
                return True
        code = getattr(curr, "pgcode", None) or getattr(curr, "sqlstate", None)
        if code == "23505":
            return True
        curr = curr.__cause__ or curr.__context__
    return False


_SANITIZED_MESSAGES: dict[type[IncidentPersistenceError], str] = {
    IncidentPersistenceInputError: "Database constraint violation during incident persistence",
    IncidentPersistenceTenantError: "Database tenant permission or isolation violation during incident persistence",
    IncidentPersistenceConflictError: "Database conflict during incident persistence",
    IncidentPersistenceCorruptionError: "Database corruption detected during incident persistence",
    IncidentPersistenceStoreError: "Database operation failed during incident persistence",
}


def _has_database_exception(exc: BaseException | None) -> bool:
    """Return True if exc or any exception in its cause/context/orig chain is a database exception."""
    queue: list[BaseException | None] = [exc]
    visited: set[int] = set()
    while queue:
        curr = queue.pop(0)
        if curr is None or id(curr) in visited:
            continue
        visited.add(id(curr))
        if isinstance(curr, (DBAPIError, SQLAlchemyError)):
            return True
        if getattr(curr, "orig", None) is not None:
            return True
        if (
            getattr(curr, "pgcode", None) is not None
            or getattr(curr, "sqlstate", None) is not None
        ):
            return True
        if curr.__class__.__module__.startswith(("psycopg", "asyncpg", "sqlalchemy")):
            return True
        queue.append(curr.__cause__)
        queue.append(curr.__context__)
        orig = getattr(curr, "orig", None)
        if isinstance(orig, BaseException):
            queue.append(orig)
    return False


def _sanitize_persistence_error(
    exc: IncidentPersistenceError,
) -> IncidentPersistenceError:
    """Return a sanitized instance of the same persistence error class if exc wraps a DB error."""
    if _has_database_exception(exc):
        exc_cls = type(exc)
        msg = _SANITIZED_MESSAGES.get(
            exc_cls, "Database operation failed during incident persistence"
        )
        new_exc = exc_cls(msg)
        new_exc.__cause__ = exc
        return new_exc
    return exc


def _validate_pre_sql_request(
    *,
    group: CorrelationGroup,
    signal_inputs: Mapping[UUID, AnomalyCorrelationInput],
    desired_state: CorrelationWindowState,
    processing_time: datetime,
    tenant_id: UUID,
    expected_revision: int | None,
    expected_revision_fingerprint: str | None,
) -> tuple[str, GroupingMemberReference, UUID]:
    """Execute all pre-SQL request validation checks before touching the database.

    Returns:
        tuple of (derived_environment, anchor_member, candidate_incident_id)
    """
    if not isinstance(group, CorrelationGroup):
        raise IncidentPersistenceInputError(
            f"group must be a CorrelationGroup instance, got {type(group).__name__}"
        )

    if not group.members:
        raise IncidentPersistenceInputError(
            "CorrelationGroup must contain at least one member"
        )

    if not isinstance(signal_inputs, Mapping):
        raise IncidentPersistenceInputError(
            f"signal_inputs must be a Mapping, got {type(signal_inputs).__name__}"
        )

    if not isinstance(desired_state, CorrelationWindowState):
        raise IncidentPersistenceInputError(
            f"desired_state must be a CorrelationWindowState, got {type(desired_state).__name__}"
        )

    if isinstance(processing_time, bool) or not isinstance(processing_time, datetime):
        raise IncidentPersistenceInputError(
            f"processing_time must be a datetime instance, got {type(processing_time).__name__}"
        )

    if processing_time.tzinfo is None or processing_time.tzinfo.utcoffset(
        processing_time
    ) != timedelta(0):
        raise IncidentPersistenceInputError(
            "processing_time must be timezone-aware UTC with zero offset"
        )

    if isinstance(tenant_id, bool) or not isinstance(tenant_id, UUID):
        raise IncidentPersistenceTenantError(
            f"tenant_id must be a UUID instance, got {type(tenant_id).__name__}"
        )

    # Expectation pair validation
    if (expected_revision is None) != (expected_revision_fingerprint is None):
        raise IncidentPersistenceInputError(
            "expected_revision and expected_revision_fingerprint must be both present or both absent"
        )

    if expected_revision is not None:
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or not (1 <= expected_revision <= _MAX_UINT64)
        ):
            raise IncidentPersistenceInputError(
                f"expected_revision must be a non-boolean integer in 1..{_MAX_UINT64}"
            )
        if not isinstance(
            expected_revision_fingerprint, str
        ) or not _FINGERPRINT_RE.fullmatch(expected_revision_fingerprint):
            raise IncidentPersistenceInputError(
                "expected_revision_fingerprint must be a 64-character lowercase hex string"
            )

    # Member duplicate check & authoritative reconciliation
    seen_member_ids: set[UUID] = set()
    for member in group.members:
        if not isinstance(member, GroupingMemberReference):
            raise IncidentPersistenceInputError(
                f"group member must be a GroupingMemberReference, got {type(member).__name__}"
            )

        if member.signal_id in seen_member_ids:
            raise IncidentPersistenceInputError(
                f"Duplicate member signal_id {member.signal_id} in group.members"
            )
        seen_member_ids.add(member.signal_id)

        if member.signal_id not in signal_inputs:
            raise IncidentPersistenceInputError(
                f"Authoritative signal input missing for member {member.signal_id}"
            )

        auth_input = signal_inputs[member.signal_id]
        if not isinstance(auth_input, AnomalyCorrelationInput):
            raise IncidentPersistenceInputError(
                f"Signal input for {member.signal_id} must be an AnomalyCorrelationInput, got {type(auth_input).__name__}"
            )

        if member.signal_id != auth_input.signal.signal_id:
            raise IncidentPersistenceInputError(
                f"Authoritative signal ID mismatch for {member.signal_id}: {auth_input.signal.signal_id}"
            )
        if member.semantic_fingerprint != auth_input.semantic_fingerprint:
            raise IncidentPersistenceInputError(
                f"Authoritative semantic fingerprint mismatch for {member.signal_id}"
            )
        if member.service != auth_input.signal.service:
            raise IncidentPersistenceInputError(
                f"Authoritative service mismatch for {member.signal_id}: {member.service} != {auth_input.signal.service}"
            )
        if member.event_time != auth_input.signal.event_time:
            raise IncidentPersistenceInputError(
                f"Authoritative event time mismatch for {member.signal_id}: {member.event_time} != {auth_input.signal.event_time}"
            )

    # Derive tenant and environment from authoritative input for group.members[0]
    first_member = group.members[0]
    first_input = signal_inputs[first_member.signal_id]
    derived_tenant = first_input.signal.tenant_id
    derived_environment = first_input.signal.environment

    if isinstance(derived_tenant, bool) or not isinstance(derived_tenant, UUID):
        raise IncidentPersistenceTenantError(
            f"Derived tenant must be a UUID, got {type(derived_tenant).__name__}"
        )
    if not isinstance(derived_environment, str) or not derived_environment.strip():
        raise IncidentPersistenceInputError(
            "Derived environment must be a non-empty string"
        )

    # Require every authoritative member input to have exactly that tenant and environment
    for member in group.members:
        inp = signal_inputs[member.signal_id]
        if inp.signal.tenant_id != derived_tenant:
            raise IncidentPersistenceTenantError(
                f"Authoritative input for member {member.signal_id} has tenant {inp.signal.tenant_id} differing from {derived_tenant}"
            )
        if inp.signal.environment != derived_environment:
            raise IncidentPersistenceInputError(
                f"Authoritative input for member {member.signal_id} has environment {inp.signal.environment} differing from {derived_environment}"
            )

    # Require derived tenant to equal the request tenant_id
    if derived_tenant != tenant_id:
        raise IncidentPersistenceTenantError(
            f"Derived tenant {derived_tenant} does not match request tenant_id {tenant_id}"
        )

    # Founding anchor selection
    anchor_member = min(
        group.members,
        key=lambda member: (member.event_time, member.signal_id.bytes),
    )

    # Candidate incident ID derivation
    candidate_id = derive_incident_id(
        tenant_id=tenant_id,
        environment=derived_environment,
        anchor_signal_fingerprint=anchor_member.semantic_fingerprint,
    )

    return derived_environment, anchor_member, candidate_id


async def _verify_role_and_isolation(session: AsyncSession) -> None:
    """Verify transaction isolation is READ COMMITTED and runtime role has no super/bypass privileges."""
    try:
        iso_res = await session.execute(text("SHOW transaction_isolation"))
        iso = iso_res.scalar_one_or_none()
        if iso is None or iso.lower() != "read committed":
            raise IncidentPersistenceStoreError(
                f"Expected transaction isolation 'read committed', observed '{iso}'"
            )

        role_res = await session.execute(
            text(
                "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
        )
        role_row = role_res.first()
        if role_row is None:
            raise IncidentPersistenceTenantError(
                "Failed to inspect role privileges: current_user not found in pg_roles"
            )

        rolsuper, rolbypassrls = role_row[0], role_row[1]
        if bool(rolsuper) is True or bool(rolbypassrls) is True:
            raise IncidentPersistenceTenantError(
                f"Runtime role cannot have superuser or bypassrls privileges (rolsuper={rolsuper}, rolbypassrls={rolbypassrls})"
            )
    except IncidentPersistenceError:
        raise
    except Exception as exc:
        raise IncidentPersistenceStoreError(
            "Failed to verify database role and transaction isolation"
        ) from exc


async def _resolve_and_lock_target(
    *,
    repo: SqlAlchemyIncidentPersistenceRepository,
    member_ids: Collection[UUID],
    candidate_id: UUID,
) -> tuple[UUID, bool, IncidentAggregate | None]:
    """Resolve incident target deterministically and lock current state."""
    resolved_ids = await repo.resolve_incident_ids(member_ids)
    if len(resolved_ids) == 0:
        target_id = candidate_id
        had_membership = False
    elif len(resolved_ids) == 1:
        resolved_id = resolved_ids[0]
        if resolved_id != candidate_id:
            raise IncidentPersistenceConflictError(
                f"Resolved incident ID {resolved_id} does not match candidate {candidate_id}"
            )
        target_id = candidate_id
        had_membership = True
    else:
        raise IncidentPersistenceConflictError(
            f"Member signals resolve to multiple distinct incidents: {resolved_ids}"
        )

    persisted_state = await repo.load_incident(target_id, for_update=True)
    if persisted_state is None:
        if had_membership:
            raise IncidentPersistenceCorruptionError(
                f"Membership resolved incident {target_id} but parent record is missing"
            )
        current_aggregate = None
    else:
        current_aggregate = persisted_state.aggregate

    return target_id, had_membership, current_aggregate


async def persist_incident_atomic(
    *,
    group: CorrelationGroup,
    signal_inputs: Mapping[UUID, AnomalyCorrelationInput],
    desired_state: CorrelationWindowState,
    processing_time: datetime,
    tenant_id: UUID,
    session_factory: async_sessionmaker[AsyncSession],
    expected_revision: int | None = None,
    expected_revision_fingerprint: str | None = None,
) -> IncidentAggregationResult:
    """Atomically persist or replay an incident within an application-owned transaction."""
    # Step 1: Pre-SQL validation before calling session_factory or touching DB
    _, _, candidate_id = _validate_pre_sql_request(
        group=group,
        signal_inputs=signal_inputs,
        desired_state=desired_state,
        processing_time=processing_time,
        tenant_id=tenant_id,
        expected_revision=expected_revision,
        expected_revision_fingerprint=expected_revision_fingerprint,
    )

    if session_factory is None or not callable(session_factory):
        raise IncidentPersistenceInputError("session_factory must be callable")

    member_ids = tuple(m.signal_id for m in group.members)
    primary_candidate_created = False
    primary_exc: BaseException | None = None

    try:
        async with session_factory() as session:
            async with session.begin():
                await _verify_role_and_isolation(session)

                repo = SqlAlchemyIncidentPersistenceRepository(session, tenant_id)
                await repo.bind_tenant_context()

                target_id, _, current_aggregate = await _resolve_and_lock_target(
                    repo=repo,
                    member_ids=member_ids,
                    candidate_id=candidate_id,
                )

                if current_aggregate is None:
                    # Absent current incident
                    if expected_revision is not None:
                        raise IncidentPersistenceConflictError(
                            "Expected revision provided but incident does not exist"
                        )

                    try:
                        pure_result = create_or_update_incident(
                            group=group,
                            signal_inputs=signal_inputs,
                            processing_time=processing_time,
                            desired_correlation_state=desired_state,
                            existing_aggregate=None,
                        )
                    except AggregateInputError as exc:
                        raise IncidentPersistenceInputError(
                            "Invalid aggregate input during incident creation"
                        ) from exc
                    except AggregateStateError as exc:
                        raise IncidentPersistenceConflictError(
                            "Aggregate state conflict during incident creation"
                        ) from exc

                    primary_candidate_created = True

                    # 10.1 Creation write order:
                    # 1. insert_incident
                    await repo.insert_incident(pure_result.aggregate)
                    # 2. append_signal_links
                    await repo.append_signal_links(
                        pure_result.aggregate.incident_id,
                        pure_result.aggregate.signal_references,
                    )
                    # 3. append_correlation_evidence
                    await repo.append_correlation_evidence(
                        pure_result.aggregate.incident_id,
                        pure_result.aggregate.correlation_evidence,
                    )
                    # 4. append_timeline_events
                    await repo.append_timeline_events(pure_result.timeline_events)
                    # 5. append_publication_intent
                    await repo.append_publication_intent(
                        pure_result.aggregate,
                        intent_time=pure_result.aggregate.updated_at,
                    )

                    return pure_result

                # Present current incident
                if expected_revision is None:
                    # Absent expectation with existing incident
                    try:
                        pure_result = create_or_update_incident(
                            group=group,
                            signal_inputs=signal_inputs,
                            processing_time=processing_time,
                            desired_correlation_state=desired_state,
                            existing_aggregate=current_aggregate,
                        )
                    except (AggregateInputError, AggregateStateError) as exc:
                        raise IncidentPersistenceConflictError(
                            "Aggregate state conflict during replay classification"
                        ) from exc

                    if not pure_result.material_update:
                        # Exact replay, zero DML
                        return IncidentAggregationResult(
                            aggregate=current_aggregate,
                            timeline_events=(),
                            material_update=False,
                        )
                    else:
                        # Material update with absent expectation -> conflict
                        raise IncidentPersistenceConflictError(
                            f"Incident {target_id} already exists; cannot perform material update without expected revision and fingerprint"
                        )

                # Present expectation with existing incident
                expected_matches = (
                    expected_revision == current_aggregate.revision
                    and expected_revision_fingerprint
                    == current_aggregate.revision_fingerprint
                )

                if expected_matches:
                    try:
                        pure_result = create_or_update_incident(
                            group=group,
                            signal_inputs=signal_inputs,
                            processing_time=processing_time,
                            desired_correlation_state=desired_state,
                            existing_aggregate=current_aggregate,
                        )
                    except AggregateInputError as exc:
                        raise IncidentPersistenceInputError(
                            "Invalid aggregate input during incident update"
                        ) from exc
                    except AggregateStateError as exc:
                        raise IncidentPersistenceConflictError(
                            "Aggregate state conflict during incident update"
                        ) from exc

                    if not pure_result.material_update:
                        # Exact replay, zero DML
                        return IncidentAggregationResult(
                            aggregate=current_aggregate,
                            timeline_events=(),
                            material_update=False,
                        )

                    # Material update write sequence:
                    existing_sig_ids = {
                        link.signal_id for link in current_aggregate.signal_references
                    }
                    new_links = [
                        link
                        for link in pure_result.aggregate.signal_references
                        if link.signal_id not in existing_sig_ids
                    ]

                    existing_evidence = {
                        (ev.left_signal_id, ev.right_signal_id)
                        for ev in current_aggregate.correlation_evidence
                    }
                    new_evidence = [
                        ev
                        for ev in pure_result.aggregate.correlation_evidence
                        if (ev.left_signal_id, ev.right_signal_id)
                        not in existing_evidence
                    ]

                    # 1. append only new signal links
                    if new_links:
                        await repo.append_signal_links(
                            pure_result.aggregate.incident_id, new_links
                        )
                    # 2. append only new evidence
                    if new_evidence:
                        await repo.append_correlation_evidence(
                            pure_result.aggregate.incident_id, new_evidence
                        )
                    # 3. append emitted timeline events
                    if pure_result.timeline_events:
                        await repo.append_timeline_events(pure_result.timeline_events)
                    # 4. append one immutable publication intent
                    await repo.append_publication_intent(
                        pure_result.aggregate,
                        intent_time=pure_result.aggregate.updated_at,
                    )
                    # 5. call update_incident_cas
                    cas_ok = await repo.update_incident_cas(
                        pure_result.aggregate,
                        expected_revision=current_aggregate.revision,
                        expected_revision_fingerprint=current_aggregate.revision_fingerprint,
                    )
                    if not cas_ok:
                        raise IncidentPersistenceConflictError(
                            f"CAS update failed for incident {pure_result.aggregate.incident_id}: predicate mismatch"
                        )

                    return pure_result

                # Stale expectation with existing incident
                try:
                    pure_result = create_or_update_incident(
                        group=group,
                        signal_inputs=signal_inputs,
                        processing_time=processing_time,
                        desired_correlation_state=desired_state,
                        existing_aggregate=current_aggregate,
                    )
                except (AggregateInputError, AggregateStateError) as exc:
                    raise IncidentPersistenceConflictError(
                        "Aggregate state conflict during replay classification"
                    ) from exc

                if not pure_result.material_update:
                    # Exact replay exemption, zero DML
                    return IncidentAggregationResult(
                        aggregate=current_aggregate,
                        timeline_events=(),
                        material_update=False,
                    )

                # Stale writer conflict
                raise IncidentPersistenceConflictError(
                    f"Stale writer: expected revision {expected_revision} (fp={expected_revision_fingerprint}) "
                    f"does not match current revision {current_aggregate.revision} (fp={current_aggregate.revision_fingerprint})"
                )

    except IncidentPersistenceError as exc:
        if primary_candidate_created and _is_sqlstate_23505(exc):
            primary_exc = exc
        else:
            raise _sanitize_persistence_error(exc)
    except (DBAPIError, SQLAlchemyError) as exc:
        if primary_candidate_created and _is_sqlstate_23505(exc):
            primary_exc = exc
        else:
            raise IncidentPersistenceStoreError(
                "Database operation failed during incident persistence"
            ) from exc
    except Exception as exc:
        raise IncidentPersistenceStoreError(
            "Unexpected error in persistence service"
        ) from exc

    # Section 11: SQLSTATE 23505 concurrent-create recovery
    # The primary transaction has rolled back completely because we exited the context managers.
    # Open exactly one new session and exactly one new READ COMMITTED transaction.
    try:
        async with session_factory() as recovery_session:
            async with recovery_session.begin():
                await _verify_role_and_isolation(recovery_session)

                rec_repo = SqlAlchemyIncidentPersistenceRepository(
                    recovery_session, tenant_id
                )
                await rec_repo.bind_tenant_context()

                # Rerun reconciliation and candidate derivation
                _validate_pre_sql_request(
                    group=group,
                    signal_inputs=signal_inputs,
                    desired_state=desired_state,
                    processing_time=processing_time,
                    tenant_id=tenant_id,
                    expected_revision=expected_revision,
                    expected_revision_fingerprint=expected_revision_fingerprint,
                )

                # Rerun membership resolution and enforce zero/one/many rules
                target_id, _, winner_aggregate = await _resolve_and_lock_target(
                    repo=rec_repo,
                    member_ids=member_ids,
                    candidate_id=candidate_id,
                )

                if winner_aggregate is None:
                    # Winner must be present
                    raise IncidentPersistenceCorruptionError(
                        f"Concurrent-create recovery expected winner {candidate_id} but parent is missing"
                    )

                # Rerun transition against locked winner using original request
                try:
                    recovery_result = create_or_update_incident(
                        group=group,
                        signal_inputs=signal_inputs,
                        processing_time=processing_time,
                        desired_correlation_state=desired_state,
                        existing_aggregate=winner_aggregate,
                    )
                except (AggregateInputError, AggregateStateError) as exc:
                    raise IncidentPersistenceConflictError(
                        f"Concurrent-create recovery rejected transition against winner {candidate_id}"
                    ) from exc

                if not recovery_result.material_update:
                    # Validated no-op replay with zero DML
                    return IncidentAggregationResult(
                        aggregate=winner_aggregate,
                        timeline_events=(),
                        material_update=False,
                    )
                else:
                    raise IncidentPersistenceConflictError(
                        f"Concurrent-create winner {candidate_id} requires material update; request rejected"
                    ) from primary_exc

    except IncidentPersistenceError as exc:
        raise _sanitize_persistence_error(exc)
    except (DBAPIError, SQLAlchemyError) as exc:
        raise IncidentPersistenceStoreError(
            "Database operation failed during concurrent-create recovery"
        ) from exc
    except Exception as exc:
        raise IncidentPersistenceStoreError(
            "Unexpected error during concurrent-create recovery"
        ) from exc
