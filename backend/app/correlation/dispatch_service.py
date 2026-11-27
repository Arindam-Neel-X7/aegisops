"""Durable incident publication dispatch and bounded recovery service.

Implements Task 4.12-D1 against approved Task 4.12-A1-R1, C1-R1, and P1
contracts without activating the runtime coordinator or Phase 5 consumers.

Key invariants enforced:
1. Separate-session isolation: reads only committed ``incident_publication_intents``
   using short ``READ COMMITTED`` transactions under a non-bypass runtime role.
2. Frozen wire immutability: builds and validates the C1 wire record once per
   committed revision, stores exact frozen bytes in ``incident_publication_dispatches``,
   and replays those exact bytes on retries/recovery without re-querying topology.
3. Controlled concurrency and ordering: atomic SQL conditional claims enforce
   server-clock leases, bounded attempt budgets, and complete predecessor-intent
   acknowledgement before admitting revision ``R``.
4. Honest fencing: pre-send active-claim checks abort known-lost tokens with zero
   publish calls; ``claim_token``-fenced completion updates surface
   ``DispatchClaimLostError`` on stale completions without modifying an
   authoritative replacement receipt.
5. Physical vs logical accounting: ordinary replay after ``acknowledged`` makes
   zero claims, zero DB row changes, and zero Kafka sends; timeout/cancellation
   retains the prior lease expiry as ``ambiguous_outcome``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json
import math
import threading
from typing import Any, Awaitable, Protocol, TypeVar, runtime_checkable
from uuid import UUID, uuid4, uuid5

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.correlation.blast_radius import BlastRadiusLimitError, BlastRadiusPolicy
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
)
from app.correlation.dispatch_repository import (
    DispatchClaimLostError,
    DispatchConfigurationError,
    DispatchCorruptionError,
    DispatchInputError,
    DispatchState,
    DispatchStoreError,
    IncidentDispatchError,
    IncidentDispatchRepository,
    PersistedPublicationDispatch,
    SqlAlchemyIncidentDispatchRepository,
    UnsendableDispatchDraft,
    decode_dispatch_publication_record,
    verify_runtime_role_and_isolation,
)
from app.correlation.errors import IncidentPersistenceTenantError
from app.correlation.handoff import (
    build_phase5_handoff,
    serialize_phase5_handoff,
)
from app.correlation.models import TopologySnapshotReference
from app.correlation.persistence_repository import PersistedPublicationIntent
from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    INCIDENT_PUBLICATION_EVENT_NAMESPACE,
    INCIDENT_PUBLICATION_TOPIC,
    MAX_CLAIMED_BY_BYTES,
    MAX_KAFKA_KEY_BYTES,
    MAX_KAFKA_RECORD_ESTIMATED_BYTES,
    MAX_KAFKA_VALUE_BYTES,
    IncidentPublicationRecord,
    IncidentPublicationSizeLimitError,
    PublicationExecutionLineage,
    build_incident_publication_from_intent,
    build_publication_fingerprint_projection,
    build_publication_kafka_headers,
    compute_incident_publication_fingerprint,
    derive_incident_publication_event_id,
    validate_publication_execution_context,
    validate_publication_producer_version,
)
from app.correlation.publisher import (
    IncidentPublishAmbiguousError,
    IncidentPublishOutcome,
    IncidentPublishStatus,
)
from app.correlation.topology import TopologySnapshot
from app.telemetry.transport.producer import PublishResult
from app.telemetry.transport.serialization import TelemetryExecutionContext


# ============================================================================
# APPROVED OPERATIONAL BOUNDS (A1-R1)
# ============================================================================

MIN_MAX_DISPATCH_ATTEMPTS: int = 1
MAX_MAX_DISPATCH_ATTEMPTS: int = 10
DEFAULT_MAX_DISPATCH_ATTEMPTS: int = 3

MIN_RETRY_BACKOFF_INITIAL_MS: int = 10
MAX_RETRY_BACKOFF_INITIAL_MS: int = 60_000
DEFAULT_RETRY_BACKOFF_INITIAL_MS: int = 250

MIN_RETRY_BACKOFF_MAX_MS: int = 10
MAX_RETRY_BACKOFF_MAX_MS: int = 300_000
DEFAULT_RETRY_BACKOFF_MAX_MS: int = 5_000

MIN_CLAIM_LEASE_SECONDS: int = 5
MAX_CLAIM_LEASE_SECONDS: int = 600
DEFAULT_CLAIM_LEASE_SECONDS: int = 30

MIN_PUBLISH_TIMEOUT_SECONDS: float = 0.1
MAX_PUBLISH_TIMEOUT_SECONDS: float = 60.0
DEFAULT_PUBLISH_TIMEOUT_SECONDS: float = 5.0

MIN_LEASE_SETTLEMENT_MARGIN_SECONDS: float = 1.0
MAX_LEASE_SETTLEMENT_MARGIN_SECONDS: float = 30.0
DEFAULT_LEASE_SETTLEMENT_MARGIN_SECONDS: float = 5.0

MIN_RECOVERY_BATCH_SIZE: int = 1
MAX_RECOVERY_BATCH_SIZE: int = 256
DEFAULT_RECOVERY_BATCH_SIZE: int = 50


# ============================================================================
# PUBLISHER PORT PROTOCOL
# ============================================================================


@runtime_checkable
class IncidentPublishPort(Protocol):
    """Minimal publish adapter protocol satisfied by ``IncidentPublisher``."""

    async def publish(
        self, record: IncidentPublicationRecord
    ) -> IncidentPublishOutcome: ...


# ============================================================================
# CONFIGURATION & STAGING AUTHORITY
# ============================================================================


def _is_strict_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _has_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in value)


@dataclass(frozen=True)
class DispatchConfig:
    """Validated finite bounds and coherent timing config for durable dispatch."""

    worker_id: str = "phase4-incident-dispatcher"
    max_dispatch_attempts: int = DEFAULT_MAX_DISPATCH_ATTEMPTS
    retry_backoff_initial_ms: int = DEFAULT_RETRY_BACKOFF_INITIAL_MS
    retry_backoff_max_ms: int = DEFAULT_RETRY_BACKOFF_MAX_MS
    claim_lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS
    publish_timeout_seconds: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS
    lease_settlement_margin_seconds: float = DEFAULT_LEASE_SETTLEMENT_MARGIN_SECONDS
    max_recovery_batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE

    def __post_init__(self) -> None:
        if (
            not isinstance(self.worker_id, str)
            or not self.worker_id
            or self.worker_id != self.worker_id.strip()
            or _has_control_chars(self.worker_id)
        ):
            raise DispatchConfigurationError(
                "worker_id must be trimmed non-empty text without control characters"
            )
        try:
            worker_bytes = self.worker_id.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise DispatchConfigurationError(
                "worker_id contains invalid Unicode"
            ) from exc
        if len(worker_bytes) > MAX_CLAIMED_BY_BYTES:
            raise DispatchConfigurationError(
                f"worker_id exceeds {MAX_CLAIMED_BY_BYTES} UTF-8 bytes"
            )

        if not _is_strict_int(self.max_dispatch_attempts) or not (
            MIN_MAX_DISPATCH_ATTEMPTS
            <= self.max_dispatch_attempts
            <= MAX_MAX_DISPATCH_ATTEMPTS
        ):
            raise DispatchConfigurationError(
                "max_dispatch_attempts must be an integer in 1..10"
            )

        if not _is_strict_int(self.retry_backoff_initial_ms) or not (
            MIN_RETRY_BACKOFF_INITIAL_MS
            <= self.retry_backoff_initial_ms
            <= MAX_RETRY_BACKOFF_INITIAL_MS
        ):
            raise DispatchConfigurationError(
                "retry_backoff_initial_ms must be an integer in 10..60000"
            )

        if not _is_strict_int(self.retry_backoff_max_ms) or not (
            MIN_RETRY_BACKOFF_MAX_MS
            <= self.retry_backoff_max_ms
            <= MAX_RETRY_BACKOFF_MAX_MS
        ):
            raise DispatchConfigurationError(
                "retry_backoff_max_ms must be an integer in 10..300000"
            )

        if self.retry_backoff_initial_ms > self.retry_backoff_max_ms:
            raise DispatchConfigurationError(
                "retry_backoff_initial_ms must not exceed retry_backoff_max_ms"
            )

        if not _is_strict_int(self.claim_lease_seconds) or not (
            MIN_CLAIM_LEASE_SECONDS
            <= self.claim_lease_seconds
            <= MAX_CLAIM_LEASE_SECONDS
        ):
            raise DispatchConfigurationError(
                "claim_lease_seconds must be an integer in 5..600"
            )

        if not _is_finite_number(self.publish_timeout_seconds) or not (
            MIN_PUBLISH_TIMEOUT_SECONDS
            <= float(self.publish_timeout_seconds)
            <= MAX_PUBLISH_TIMEOUT_SECONDS
        ):
            raise DispatchConfigurationError(
                "publish_timeout_seconds must be a finite number in 0.1..60.0"
            )

        if not _is_finite_number(self.lease_settlement_margin_seconds) or not (
            MIN_LEASE_SETTLEMENT_MARGIN_SECONDS
            <= float(self.lease_settlement_margin_seconds)
            <= MAX_LEASE_SETTLEMENT_MARGIN_SECONDS
        ):
            raise DispatchConfigurationError(
                "lease_settlement_margin_seconds must be a finite number in 1.0..30.0"
            )

        if float(self.claim_lease_seconds) < (
            float(self.publish_timeout_seconds)
            + float(self.lease_settlement_margin_seconds)
        ):
            raise DispatchConfigurationError(
                "claim_lease_seconds must be >= publish_timeout_seconds + lease_settlement_margin_seconds"
            )

        if not _is_strict_int(self.max_recovery_batch_size) or not (
            MIN_RECOVERY_BATCH_SIZE
            <= self.max_recovery_batch_size
            <= MAX_RECOVERY_BATCH_SIZE
        ):
            raise DispatchConfigurationError(
                "max_recovery_batch_size must be an integer in 1..256"
            )

    def compute_retry_backoff_ms(self, attempt_count: int) -> int:
        """Deterministic bounded exponential backoff ``min(init * 2^(k-1), max)``."""
        if not _is_strict_int(attempt_count) or attempt_count < 1:
            raise DispatchInputError("attempt_count must be a positive integer")
        delay = self.retry_backoff_initial_ms * (2 ** (attempt_count - 1))
        return int(min(delay, self.retry_backoff_max_ms))


@dataclass(frozen=True)
class DispatchStagingContext:
    """Authenticated original plan authority required before first T2 staging."""

    snapshot: TopologySnapshot
    expected_topology: TopologySnapshotReference
    blast_radius_policy: BlastRadiusPolicy
    correlation_config: CorrelationConfig
    execution_context: TelemetryExecutionContext
    producer_version: str | None = DEFAULT_PUBLICATION_PRODUCER_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, TopologySnapshot):
            raise DispatchInputError("snapshot must be a TopologySnapshot")
        if not isinstance(self.expected_topology, TopologySnapshotReference):
            raise DispatchInputError(
                "expected_topology must be a TopologySnapshotReference"
            )
        if not isinstance(self.blast_radius_policy, BlastRadiusPolicy):
            raise DispatchInputError("blast_radius_policy must be a BlastRadiusPolicy")
        if not isinstance(self.correlation_config, CorrelationConfig):
            raise DispatchInputError("correlation_config must be a CorrelationConfig")
        validate_publication_execution_context(self.execution_context)
        validate_publication_producer_version(self.producer_version)


# ============================================================================
# TYPED DISPATCH OUTCOMES & SPLIT-PHASE CLAIM ARTIFACTS
# ============================================================================


class DispatchOutcomeStatus(StrEnum):
    """Typed non-masking outcomes aligned with A1-R1 dispatch state table."""

    PUBLISHED_ACKNOWLEDGED = "PUBLISHED_ACKNOWLEDGED"
    ALREADY_ACKNOWLEDGED = "ALREADY_ACKNOWLEDGED"
    COMMITTED_PENDING_PUBLICATION = "COMMITTED_PENDING_PUBLICATION"
    COMMITTED_OUTCOME_UNKNOWN = "COMMITTED_OUTCOME_UNKNOWN"
    COMMITTED_PUBLICATION_EXHAUSTED = "COMMITTED_PUBLICATION_EXHAUSTED"
    COMMITTED_UNSENDABLE = "COMMITTED_UNSENDABLE"
    SKIPPED_ALREADY_CLAIMED = "SKIPPED_ALREADY_CLAIMED"
    BLOCKED_BY_PREDECESSOR = "BLOCKED_BY_PREDECESSOR"


@dataclass(frozen=True)
class DispatchOutcome:
    """Typed durable dispatch result with safe diagnostics and offset gate."""

    status: DispatchOutcomeStatus
    tenant_id: UUID
    incident_id: UUID
    revision: int
    dispatch: PersistedPublicationDispatch
    publish_result: PublishResult | None = None
    claim_token: UUID | None = None
    attempt_number: int | None = None
    sends_attempted: int = 0
    error_code: str | None = None
    error_message: str | None = None

    @property
    def input_offset_committable(self) -> bool:
        return self.status in (
            DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED,
            DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED,
        )


def _record_matches_dispatch_row(
    record: IncidentPublicationRecord,
    disp: PersistedPublicationDispatch,
) -> bool:
    """Verify exact identity and wire-byte parity between a C1 record and a dispatch row."""
    ident = record.identity
    expected_headers = tuple((p[0], p[1]) for p in record.kafka_headers_json)
    return (
        disp.dispatch_state is not DispatchState.REJECTED_UNSENDABLE
        and disp.kafka_value is not None
        and record.topic.value == disp.kafka_topic
        and record.event_id == disp.event_id
        and record.publication_fingerprint == disp.publication_fingerprint
        and ident.tenant_id == disp.tenant_id
        and ident.incident_id == disp.incident_id
        and ident.revision == disp.revision
        and ident.revision_fingerprint == disp.revision_fingerprint
        and ident.publication_contract_version == disp.publication_contract_version
        and ident.envelope_schema_version == disp.envelope_schema_version
        and ident.handoff_schema_version == disp.handoff_schema_version
        and ident.handoff_id == disp.handoff_id
        and ident.handoff_fingerprint == disp.handoff_fingerprint
        and ident.topology_id == disp.topology_id
        and ident.topology_version == disp.topology_version
        and ident.topology_snapshot_fingerprint == disp.topology_snapshot_fingerprint
        and ident.correlation_policy_version == disp.correlation_policy_version
        and ident.correlation_config_fingerprint == disp.correlation_config_fingerprint
        and ident.intent_time == disp.intent_time
        and record.kafka_key == disp.kafka_key
        and record.kafka_value == disp.kafka_value
        and expected_headers == disp.kafka_headers_json
        and record.estimated_record_bytes == disp.wire_record_bytes
    )


def _validate_claimed_attempt_integrity(
    attempt: ClaimedDispatchAttempt,
    *,
    verify_record_integrity: bool = True,
) -> IncidentPublicationRecord:
    """Strictly verify that a ClaimedDispatchAttempt's identity, dispatch, and record match."""
    if not isinstance(attempt, ClaimedDispatchAttempt):
        raise DispatchInputError("attempt must be a ClaimedDispatchAttempt")
    if not isinstance(attempt.tenant_id, UUID):
        raise DispatchInputError("attempt.tenant_id must be a UUID")
    if not isinstance(attempt.incident_id, UUID):
        raise DispatchInputError("attempt.incident_id must be a UUID")
    if not _is_strict_int(attempt.revision) or attempt.revision < 1:
        raise DispatchInputError("attempt.revision must be a positive integer")
    if not isinstance(attempt.claim_token, UUID):
        raise DispatchInputError("attempt.claim_token must be a UUID")
    if not isinstance(attempt.dispatch, PersistedPublicationDispatch):
        raise DispatchInputError(
            "attempt.dispatch must be a PersistedPublicationDispatch"
        )
    if not isinstance(attempt.record, IncidentPublicationRecord):
        raise DispatchInputError("attempt.record must be an IncidentPublicationRecord")

    disp = attempt.dispatch
    if (
        disp.tenant_id != attempt.tenant_id
        or disp.incident_id != attempt.incident_id
        or disp.revision != attempt.revision
        or disp.dispatch_state is not DispatchState.IN_FLIGHT
        or disp.claim_token != attempt.claim_token
    ):
        raise DispatchInputError(
            "ClaimedDispatchAttempt metadata does not match claimed dispatch row"
        )

    if verify_record_integrity:
        try:
            attempt.record.to_telemetry_event()
        except Exception as exc:
            raise DispatchInputError(
                "ClaimedDispatchAttempt record failed canonical integrity validation"
            ) from exc

    if not _record_matches_dispatch_row(attempt.record, disp):
        raise DispatchInputError(
            "ClaimedDispatchAttempt record does not match the claimed revision's frozen wire bytes"
        )
    return attempt.record


@dataclass(frozen=True)
class ClaimedDispatchAttempt:
    """Active claimed attempt owned by one unique ``claim_token``."""

    tenant_id: UUID
    incident_id: UUID
    revision: int
    claim_token: UUID
    dispatch: PersistedPublicationDispatch
    record: IncidentPublicationRecord

    def __post_init__(self) -> None:
        _validate_claimed_attempt_integrity(self)


@dataclass(frozen=True)
class ClaimAttemptResult:
    """Result of attempting to claim a staged revision."""

    claimed: bool
    attempt: ClaimedDispatchAttempt | None = None
    outcome: DispatchOutcome | None = None

    def __post_init__(self) -> None:
        if self.claimed and (self.attempt is None or self.outcome is not None):
            raise DispatchInputError("Invalid claimed ClaimAttemptResult")
        if not self.claimed and (self.attempt is not None or self.outcome is None):
            raise DispatchInputError("Invalid unclaimed ClaimAttemptResult")


@dataclass(frozen=True)
class StageIntentResult:
    """Result of staging a committed intent into ``incident_publication_dispatches``."""

    dispatch: PersistedPublicationDispatch
    created: bool
    record: IncidentPublicationRecord | None = None


@dataclass(frozen=True)
class DispatchRecoveryBatchResult:
    """Bounded recovery sweep summary with per-item typed outcomes."""

    staged_count: int
    unstaged_remaining_count: int
    outcomes: tuple[DispatchOutcome, ...]

    @property
    def acknowledged_count(self) -> int:
        return sum(
            o.status
            in (
                DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED,
                DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED,
            )
            for o in self.outcomes
        )

    @property
    def pending_count(self) -> int:
        return sum(
            o.status is DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION
            for o in self.outcomes
        )

    @property
    def unknown_count(self) -> int:
        return sum(
            o.status is DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN
            for o in self.outcomes
        )

    @property
    def exhausted_count(self) -> int:
        return sum(
            o.status is DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED
            for o in self.outcomes
        )

    @property
    def unsendable_count(self) -> int:
        return sum(
            o.status is DispatchOutcomeStatus.COMMITTED_UNSENDABLE
            for o in self.outcomes
        )

    @property
    def skipped_count(self) -> int:
        return sum(
            o.status is DispatchOutcomeStatus.SKIPPED_ALREADY_CLAIMED
            for o in self.outcomes
        )

    @property
    def blocked_count(self) -> int:
        return sum(
            o.status is DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR
            for o in self.outcomes
        )

    @property
    def total_sends_attempted(self) -> int:
        return sum(o.sends_attempted for o in self.outcomes)


# ============================================================================
# INTERNAL HELPERS
# ============================================================================


def _build_unsendable_draft(
    *,
    intent: PersistedPublicationIntent,
    staging_context: DispatchStagingContext,
    error_code: str,
    error_message: str,
) -> UnsendableDispatchDraft:
    """Assemble deterministic immutable metadata for a ``rejected_unsendable`` row."""
    agg = intent.aggregate
    topo_ref = staging_context.expected_topology
    cfg = staging_context.correlation_config
    cfg_fp = compute_correlation_config_fingerprint(cfg)
    event_id = derive_incident_publication_event_id(
        tenant_id=agg.tenant_id,
        incident_id=agg.incident_id,
        revision=agg.revision,
        revision_fingerprint=agg.revision_fingerprint,
    )
    key_bytes = f"{agg.tenant_id}:{agg.environment}:{agg.primary_service}".encode(
        "utf-8"
    )
    if len(key_bytes) > MAX_KAFKA_KEY_BYTES:
        key_bytes = key_bytes[:MAX_KAFKA_KEY_BYTES]

    try:
        headers = build_publication_kafka_headers(
            staging_context.execution_context,
            staging_context.producer_version,
        )
        headers_json = tuple((k, v.decode("utf-8")) for k, v in headers)
    except Exception:
        headers_json = ()

    lineage = PublicationExecutionLineage.from_execution_context(
        staging_context.execution_context
    )
    try:
        handoff = build_phase5_handoff(
            incident=agg,
            snapshot=staging_context.snapshot,
            expected_topology=topo_ref,
            policy=staging_context.blast_radius_policy,
            as_of=intent.intent_time,
        )
        handoff_bytes = serialize_phase5_handoff(handoff)
        projection = build_publication_fingerprint_projection(
            event_id=event_id,
            handoff=handoff,
            correlation_policy_version=cfg.policy_version,
            correlation_config_fingerprint=cfg_fp,
            execution_lineage=lineage,
            producer_version=staging_context.producer_version,
            intent_time=intent.intent_time,
        )
        pub_fp = compute_incident_publication_fingerprint(projection)
        handoff_id = handoff.handoff_id
        handoff_fp = handoff.handoff_fingerprint
        wire_record_bytes = max(
            len(handoff_bytes),
            MAX_KAFKA_VALUE_BYTES + 1,
        )
    except BlastRadiusLimitError:
        descriptor = json.dumps(
            {
                "event_id": str(event_id),
                "incident_id": str(agg.incident_id),
                "intent_time": intent.intent_time.isoformat(),
                "reason": error_code,
                "revision": agg.revision,
                "revision_fingerprint": agg.revision_fingerprint,
                "tenant_id": str(agg.tenant_id),
                "topology_snapshot_fingerprint": topo_ref.snapshot_fingerprint,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        handoff_fp = hashlib.sha256(b"unsendable-handoff:" + descriptor).hexdigest()
        handoff_id = uuid5(
            INCIDENT_PUBLICATION_EVENT_NAMESPACE,
            f"unsendable-handoff|{handoff_fp}",
        )
        pub_fp = hashlib.sha256(b"unsendable-publication:" + descriptor).hexdigest()
        wire_record_bytes = MAX_KAFKA_RECORD_ESTIMATED_BYTES + 1

    return UnsendableDispatchDraft(
        tenant_id=agg.tenant_id,
        incident_id=agg.incident_id,
        revision=agg.revision,
        revision_fingerprint=agg.revision_fingerprint,
        event_id=event_id,
        publication_fingerprint=pub_fp,
        handoff_id=handoff_id,
        handoff_fingerprint=handoff_fp,
        topology_id=topo_ref.topology_id,
        topology_version=topo_ref.topology_version,
        topology_snapshot_fingerprint=topo_ref.snapshot_fingerprint,
        correlation_policy_version=cfg.policy_version,
        correlation_config_fingerprint=cfg_fp,
        intent_time=intent.intent_time,
        kafka_topic=INCIDENT_PUBLICATION_TOPIC.value,
        kafka_key=key_bytes,
        kafka_headers_json=headers_json,
        wire_record_bytes=wire_record_bytes,
        error_code=error_code,
        error_message=error_message,
    )


def _outcome_from_non_claimed_state(
    dispatch: PersistedPublicationDispatch,
    *,
    predecessors_satisfied: bool,
) -> DispatchOutcome:
    state = dispatch.dispatch_state
    if state is DispatchState.ACKNOWLEDGED:
        return DispatchOutcome(
            status=DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED,
            tenant_id=dispatch.tenant_id,
            incident_id=dispatch.incident_id,
            revision=dispatch.revision,
            dispatch=dispatch,
            sends_attempted=0,
        )
    if state is DispatchState.REJECTED_UNSENDABLE:
        return DispatchOutcome(
            status=DispatchOutcomeStatus.COMMITTED_UNSENDABLE,
            tenant_id=dispatch.tenant_id,
            incident_id=dispatch.incident_id,
            revision=dispatch.revision,
            dispatch=dispatch,
            sends_attempted=0,
            error_code=dispatch.last_error_code,
            error_message=dispatch.last_error_message,
        )
    if state is DispatchState.EXHAUSTED:
        return DispatchOutcome(
            status=DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED,
            tenant_id=dispatch.tenant_id,
            incident_id=dispatch.incident_id,
            revision=dispatch.revision,
            dispatch=dispatch,
            sends_attempted=0,
            error_code=dispatch.last_error_code,
            error_message=dispatch.last_error_message,
        )
    if not predecessors_satisfied:
        return DispatchOutcome(
            status=DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR,
            tenant_id=dispatch.tenant_id,
            incident_id=dispatch.incident_id,
            revision=dispatch.revision,
            dispatch=dispatch,
            sends_attempted=0,
            error_code="PREDECESSOR_NOT_ACKNOWLEDGED",
            error_message="All prior committed revisions must be acknowledged before claiming this revision.",
        )
    if state is DispatchState.IN_FLIGHT:
        return DispatchOutcome(
            status=DispatchOutcomeStatus.SKIPPED_ALREADY_CLAIMED,
            tenant_id=dispatch.tenant_id,
            incident_id=dispatch.incident_id,
            revision=dispatch.revision,
            dispatch=dispatch,
            sends_attempted=0,
        )
    if state is DispatchState.AMBIGUOUS_OUTCOME:
        return DispatchOutcome(
            status=DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN,
            tenant_id=dispatch.tenant_id,
            incident_id=dispatch.incident_id,
            revision=dispatch.revision,
            dispatch=dispatch,
            sends_attempted=0,
            error_code=dispatch.last_error_code,
            error_message=dispatch.last_error_message,
        )
    return DispatchOutcome(
        status=DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION,
        tenant_id=dispatch.tenant_id,
        incident_id=dispatch.incident_id,
        revision=dispatch.revision,
        dispatch=dispatch,
        sends_attempted=0,
        error_code=dispatch.last_error_code,
        error_message=dispatch.last_error_message,
    )


def _error_code_for_publish_status(status: IncidentPublishStatus) -> tuple[str, str]:
    if status is IncidentPublishStatus.FAILED_RETRIABLE:
        return (
            "PUBLISH_RETRIABLE",
            "Incident publication received a retryable broker rejection.",
        )
    if status is IncidentPublishStatus.FAILED_UNAVAILABLE:
        return (
            "PUBLISH_UNAVAILABLE",
            "Incident publication producer is unavailable.",
        )
    if status is IncidentPublishStatus.FAILED_CONTRACT:
        return (
            "PUBLISH_CONTRACT_ERROR",
            "Incident publication contract or producer configuration validation failed.",
        )
    return (
        "PUBLISH_AMBIGUOUS",
        "Incident publication acknowledgement is unconfirmed.",
    )


# ============================================================================
# DURABLE DISPATCH SERVICE
# ============================================================================


RepositoryFactory = Callable[[AsyncSession, UUID], IncidentDispatchRepository]
_T = TypeVar("_T")
_MAX_TRACKED_CLAIM_TOKENS: int = 100_000
_GLOBAL_CLAIM_TOKEN_LOCK = threading.Lock()
_GLOBAL_STARTED_CLAIM_TOKENS: set[UUID] = set()
_GLOBAL_STARTED_CLAIM_ORDER: deque[UUID] = deque()


class IncidentDispatchService:
    """Durable dispatch and bounded recovery orchestrator over PostgreSQL and P1."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: IncidentPublishPort,
        config: DispatchConfig | None = None,
        repository_factory: RepositoryFactory | None = None,
        verify_role_fn: Callable[[AsyncSession], Any] | None = None,
    ) -> None:
        if session_factory is None or not callable(session_factory):
            raise DispatchInputError("session_factory must be callable")
        if not isinstance(publisher, IncidentPublishPort):
            raise DispatchInputError("publisher must implement IncidentPublishPort")
        effective_config = config if config is not None else DispatchConfig()
        if not isinstance(effective_config, DispatchConfig):
            raise DispatchConfigurationError("config must be a DispatchConfig instance")
        self._session_factory = session_factory
        self._publisher = publisher
        self._config = effective_config
        self._repo_factory: RepositoryFactory = (
            repository_factory
            if repository_factory is not None
            else SqlAlchemyIncidentDispatchRepository
        )
        self._verify_role_fn = (
            verify_role_fn
            if verify_role_fn is not None
            else verify_runtime_role_and_isolation
        )
        # Synchronous atomic guard guaranteeing at most one publish invocation per claim_token.
        self._started_claim_tokens: set[UUID] = set()
        self._started_claim_order: deque[UUID] = deque()
        # Track unsettled timed-out publish tasks to consume late exceptions cleanly.
        self._pending_publishes: set[asyncio.Task[IncidentPublishOutcome]] = set()

    @property
    def config(self) -> DispatchConfig:
        return self._config

    @property
    def pending_publish_count(self) -> int:
        return sum(not task.done() for task in self._pending_publishes)

    def _on_publish_task_done(self, task: asyncio.Task[IncidentPublishOutcome]) -> None:
        self._pending_publishes.discard(task)
        if not task.cancelled():
            task.exception()

    def _reserve_claim_token_once(self, claim_token: UUID) -> None:
        """Atomically reserve one unique claim_token synchronously before any await."""
        with _GLOBAL_CLAIM_TOKEN_LOCK:
            if (
                claim_token in self._started_claim_tokens
                or claim_token in _GLOBAL_STARTED_CLAIM_TOKENS
            ):
                raise DispatchClaimLostError(
                    "A publish invocation has already been started for this claim token"
                )
            self._started_claim_tokens.add(claim_token)
            self._started_claim_order.append(claim_token)
            if len(self._started_claim_order) > _MAX_TRACKED_CLAIM_TOKENS:
                evicted_local = self._started_claim_order.popleft()
                self._started_claim_tokens.discard(evicted_local)
            _GLOBAL_STARTED_CLAIM_TOKENS.add(claim_token)
            _GLOBAL_STARTED_CLAIM_ORDER.append(claim_token)
            if len(_GLOBAL_STARTED_CLAIM_ORDER) > _MAX_TRACKED_CLAIM_TOKENS:
                evicted_global = _GLOBAL_STARTED_CLAIM_ORDER.popleft()
                _GLOBAL_STARTED_CLAIM_TOKENS.discard(evicted_global)

    async def _run_in_tx(
        self,
        tenant_id: UUID,
        fn: Callable[[IncidentDispatchRepository], Awaitable[_T]],
    ) -> _T:
        if not isinstance(tenant_id, UUID):
            raise IncidentPersistenceTenantError("tenant_id must be a UUID")
        async with self._session_factory() as session:
            async with session.begin():
                await self._verify_role_fn(session)
                repo = self._repo_factory(session, tenant_id)
                await repo.bind_tenant_context()
                return await fn(repo)

    async def load_dispatch_state(
        self,
        *,
        tenant_id: UUID,
        incident_id: UUID,
        revision: int,
    ) -> PersistedPublicationDispatch | None:
        """Read current durable dispatch state for one committed revision."""
        return await self._run_in_tx(
            tenant_id,
            lambda repo: repo.load_dispatch(incident_id, revision),
        )

    async def load_incident_dispatch_states(
        self,
        *,
        tenant_id: UUID,
        incident_id: UUID,
    ) -> tuple[PersistedPublicationDispatch, ...]:
        """Read all durable dispatch rows for one incident in ascending revision order."""
        return await self._run_in_tx(
            tenant_id,
            lambda repo: repo.list_incident_dispatches(incident_id),
        )

    async def stage_committed_intent(
        self,
        *,
        tenant_id: UUID,
        incident_id: UUID,
        revision: int,
        staging_context: DispatchStagingContext,
    ) -> StageIntentResult | None:
        """Stage a committed intent into ``incident_publication_dispatches`` (TR-01/TR-02).

        Returns ``None`` if no committed intent exists in ``incident_publication_intents``
        (separate-session proof that uncommitted or rolled-back writes are invisible).
        """
        if not isinstance(staging_context, DispatchStagingContext):
            raise DispatchInputError(
                "staging_context must be a DispatchStagingContext instance"
            )

        async def _stage(repo: IncidentDispatchRepository) -> StageIntentResult | None:
            existing = await repo.load_dispatch(incident_id, revision)
            if existing is not None:
                if existing.dispatch_state is DispatchState.REJECTED_UNSENDABLE:
                    return StageIntentResult(dispatch=existing, created=False)
                frozen = decode_dispatch_publication_record(existing)
                return StageIntentResult(
                    dispatch=existing, created=False, record=frozen
                )

            intent = await repo.load_publication_intent(incident_id, revision)
            if intent is None:
                return None

            try:
                record = build_incident_publication_from_intent(
                    intent=intent,
                    snapshot=staging_context.snapshot,
                    expected_topology=staging_context.expected_topology,
                    blast_radius_policy=staging_context.blast_radius_policy,
                    correlation_config=staging_context.correlation_config,
                    execution_context=staging_context.execution_context,
                    producer_version=staging_context.producer_version,
                )
            except (IncidentPublicationSizeLimitError, BlastRadiusLimitError):
                draft = _build_unsendable_draft(
                    intent=intent,
                    staging_context=staging_context,
                    error_code="SIZE_LIMIT_EXCEEDED",
                    error_message=(
                        "Incident publication wire record exceeds Kafka size limits."
                    ),
                )
                staged_unsendable = await repo.stage_rejected_unsendable_dispatch(draft)
                return StageIntentResult(
                    dispatch=staged_unsendable.dispatch,
                    created=staged_unsendable.created,
                    record=None,
                )

            staged = await repo.stage_pending_dispatch(record)
            return StageIntentResult(
                dispatch=staged.dispatch,
                created=staged.created,
                record=record,
            )

        result: StageIntentResult | None = await self._run_in_tx(tenant_id, _stage)
        return result

    async def claim_revision(
        self,
        *,
        tenant_id: UUID,
        incident_id: UUID,
        revision: int,
    ) -> ClaimAttemptResult:
        """Atomically claim one staged revision for publish ownership (TR-03..TR-07)."""
        claim_token = uuid4()

        async def _claim(repo: IncidentDispatchRepository) -> ClaimAttemptResult:
            claimed = await repo.claim_dispatch(
                incident_id=incident_id,
                revision=revision,
                claim_token=claim_token,
                claimed_by=self._config.worker_id,
                claim_lease_seconds=self._config.claim_lease_seconds,
                max_dispatch_attempts=self._config.max_dispatch_attempts,
            )
            if claimed is not None:
                frozen = decode_dispatch_publication_record(claimed)
                return ClaimAttemptResult(
                    claimed=True,
                    attempt=ClaimedDispatchAttempt(
                        tenant_id=tenant_id,
                        incident_id=incident_id,
                        revision=revision,
                        claim_token=claim_token,
                        dispatch=claimed,
                        record=frozen,
                    ),
                )

            current = await repo.load_dispatch(incident_id, revision)
            if current is None:
                raise DispatchInputError(
                    "Cannot claim unstaged incident revision dispatch"
                )

            if (
                current.dispatch_state is DispatchState.IN_FLIGHT
                and current.attempt_count >= self._config.max_dispatch_attempts
            ):
                exhausted = await repo.mark_expired_in_flight_exhausted(
                    incident_id=incident_id,
                    revision=revision,
                    max_dispatch_attempts=self._config.max_dispatch_attempts,
                    error_code="DISPATCH_ATTEMPTS_EXHAUSTED",
                    error_message=(
                        "Dispatch claim lease expired at maximum approved attempt budget."
                    ),
                )
                if exhausted is not None:
                    current = exhausted

            predecessors = await repo.inspect_predecessors(incident_id, revision)
            return ClaimAttemptResult(
                claimed=False,
                outcome=_outcome_from_non_claimed_state(
                    current,
                    predecessors_satisfied=predecessors.satisfied,
                ),
            )

        result: ClaimAttemptResult = await self._run_in_tx(tenant_id, _claim)
        return result

    async def complete_claimed_attempt(
        self,
        attempt: ClaimedDispatchAttempt,
        publish_outcome: IncidentPublishOutcome,
    ) -> DispatchOutcome:
        """Persist fenced bookkeeping for a completed publish attempt (TR-08..TR-11)."""
        expected_record = _validate_claimed_attempt_integrity(attempt)
        return await self._complete_validated_attempt(
            attempt, expected_record, publish_outcome
        )

    async def _complete_validated_attempt(
        self,
        attempt: ClaimedDispatchAttempt,
        expected_record: IncidentPublicationRecord,
        publish_outcome: IncidentPublishOutcome,
    ) -> DispatchOutcome:
        if not isinstance(publish_outcome, IncidentPublishOutcome):
            raise DispatchInputError(
                "publish_outcome must be an IncidentPublishOutcome"
            )
        if publish_outcome.status is IncidentPublishStatus.ACKNOWLEDGED:
            if (
                publish_outcome.result is None
                or publish_outcome.result.serialized_bytes
                != len(expected_record.kafka_value)
            ):
                raise DispatchInputError(
                    "Acknowledgement serialized_bytes does not match claimed revision wire bytes"
                )

        async def _complete(repo: IncidentDispatchRepository) -> DispatchOutcome:
            # D1-R2 fix: authenticate expected_record against the authoritative stored
            # immutable dispatch row inside the completion transaction BEFORE any update.
            stored = await repo.load_dispatch(
                attempt.incident_id,
                attempt.revision,
                verify_wire=False,
            )
            if stored is None:
                raise DispatchClaimLostError(
                    "Active dispatch claim was lost before completion bookkeeping"
                )
            if not _record_matches_dispatch_row(expected_record, stored):
                raise DispatchInputError(
                    "ClaimedDispatchAttempt record does not match stored immutable dispatch wire identity"
                )
            if (
                stored.dispatch_state is not DispatchState.IN_FLIGHT
                or stored.claim_token != attempt.claim_token
            ):
                raise DispatchClaimLostError(
                    "Active dispatch claim was lost before completion bookkeeping"
                )

            if publish_outcome.status is IncidentPublishStatus.ACKNOWLEDGED:
                assert publish_outcome.result is not None
                if (
                    stored.kafka_value is None
                    or publish_outcome.result.serialized_bytes
                    != len(stored.kafka_value)
                ):
                    raise DispatchInputError(
                        "Acknowledgement serialized_bytes does not match stored immutable dispatch wire bytes"
                    )
                updated = await repo.record_acknowledged(
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    claim_token=attempt.claim_token,
                    publish_result=publish_outcome.result,
                    expected_record=expected_record,
                )
                return DispatchOutcome(
                    status=DispatchOutcomeStatus.PUBLISHED_ACKNOWLEDGED,
                    tenant_id=attempt.tenant_id,
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    dispatch=updated,
                    publish_result=publish_outcome.result,
                    claim_token=attempt.claim_token,
                    attempt_number=updated.attempt_count,
                    sends_attempted=1,
                )

            err_code, err_msg = _error_code_for_publish_status(publish_outcome.status)
            if stored.attempt_count >= self._config.max_dispatch_attempts:
                updated = await repo.record_exhausted(
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    claim_token=attempt.claim_token,
                    error_code=err_code,
                    error_message=err_msg,
                )
                return DispatchOutcome(
                    status=DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED,
                    tenant_id=attempt.tenant_id,
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    dispatch=updated,
                    claim_token=attempt.claim_token,
                    attempt_number=updated.attempt_count,
                    sends_attempted=1,
                    error_code=err_code,
                    error_message=err_msg,
                )

            backoff_ms = self._config.compute_retry_backoff_ms(stored.attempt_count)
            if publish_outcome.status is IncidentPublishStatus.FAILED_AMBIGUOUS:
                updated = await repo.record_ambiguous_outcome(
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    claim_token=attempt.claim_token,
                    max_dispatch_attempts=self._config.max_dispatch_attempts,
                    backoff_ms=backoff_ms,
                    error_code=err_code,
                    error_message=err_msg,
                )
                return DispatchOutcome(
                    status=DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN,
                    tenant_id=attempt.tenant_id,
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    dispatch=updated,
                    claim_token=attempt.claim_token,
                    attempt_number=updated.attempt_count,
                    sends_attempted=1,
                    error_code=err_code,
                    error_message=err_msg,
                )

            updated = await repo.record_retry_scheduled(
                incident_id=attempt.incident_id,
                revision=attempt.revision,
                claim_token=attempt.claim_token,
                max_dispatch_attempts=self._config.max_dispatch_attempts,
                backoff_ms=backoff_ms,
                error_code=err_code,
                error_message=err_msg,
            )
            return DispatchOutcome(
                status=DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION,
                tenant_id=attempt.tenant_id,
                incident_id=attempt.incident_id,
                revision=attempt.revision,
                dispatch=updated,
                claim_token=attempt.claim_token,
                attempt_number=updated.attempt_count,
                sends_attempted=1,
                error_code=err_code,
                error_message=err_msg,
            )

        result: DispatchOutcome = await self._run_in_tx(attempt.tenant_id, _complete)
        return result

    async def _record_cancelled_ambiguous_best_effort(
        self, attempt: ClaimedDispatchAttempt
    ) -> None:
        async def _mark(repo: IncidentDispatchRepository) -> None:
            if attempt.dispatch.attempt_count >= self._config.max_dispatch_attempts:
                await repo.record_exhausted(
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    claim_token=attempt.claim_token,
                    error_code="PUBLISH_CANCELLED",
                    error_message=(
                        "Publication was cancelled before broker acknowledgement."
                    ),
                )
            else:
                backoff_ms = self._config.compute_retry_backoff_ms(
                    attempt.dispatch.attempt_count
                )
                await repo.record_ambiguous_outcome(
                    incident_id=attempt.incident_id,
                    revision=attempt.revision,
                    claim_token=attempt.claim_token,
                    max_dispatch_attempts=self._config.max_dispatch_attempts,
                    backoff_ms=backoff_ms,
                    error_code="PUBLISH_CANCELLED",
                    error_message=(
                        "Publication was cancelled before broker acknowledgement."
                    ),
                )

        try:
            await asyncio.shield(self._run_in_tx(attempt.tenant_id, _mark))
        except Exception:
            # Best-effort only: lease expiry still guarantees durable recovery.
            return

    async def execute_claimed_attempt(
        self, attempt: ClaimedDispatchAttempt
    ) -> DispatchOutcome:
        """Verify active claim & authoritative bytes, enforce timeout, and record bookkeeping."""
        validated_record = _validate_claimed_attempt_integrity(attempt)

        # Blocker 1 fix: atomically reserve claim_token synchronously BEFORE any await
        # so concurrent executions on one token can never start duplicate sends.
        self._reserve_claim_token_once(attempt.claim_token)

        # Blocker 2 fix: verify active claim AND reload authoritative frozen bytes from
        # PostgreSQL so substituted records can never be published or acknowledged.
        active_row: PersistedPublicationDispatch | None = await self._run_in_tx(
            attempt.tenant_id,
            lambda repo: repo.load_active_claimed_dispatch(
                incident_id=attempt.incident_id,
                revision=attempt.revision,
                claim_token=attempt.claim_token,
            ),
        )
        if active_row is None:
            raise DispatchClaimLostError(
                "Dispatch claim was lost or expired prior to publication send"
            )

        if not _record_matches_dispatch_row(validated_record, active_row):
            raise DispatchInputError(
                "ClaimedDispatchAttempt record does not match authoritative database row"
            )

        # Blocker 3 fix: strictly bind effective publish wait to config.publish_timeout_seconds
        # using non-blocking asyncio.wait so hung/slower publishers cannot overrun the bound.
        publish_task: asyncio.Task[IncidentPublishOutcome] = asyncio.create_task(
            self._publisher.publish(validated_record)
        )
        self._pending_publishes.add(publish_task)
        publish_task.add_done_callback(self._on_publish_task_done)

        try:
            done, _ = await asyncio.wait(
                {publish_task},
                timeout=float(self._config.publish_timeout_seconds),
            )
            if not done:
                publish_task.cancel()
                timeout_err = IncidentPublishAmbiguousError(
                    "Incident publication acknowledgement is unconfirmed."
                )
                timeout_err.__cause__ = TimeoutError(
                    "Dispatch publication timeout expired."
                )
                publish_outcome = IncidentPublishOutcome(
                    IncidentPublishStatus.FAILED_AMBIGUOUS,
                    error=timeout_err,
                )
            else:
                publish_outcome = publish_task.result()
        except asyncio.CancelledError:
            publish_task.cancel()
            await self._record_cancelled_ambiguous_best_effort(attempt)
            raise

        authoritative_attempt = ClaimedDispatchAttempt(
            tenant_id=active_row.tenant_id,
            incident_id=active_row.incident_id,
            revision=active_row.revision,
            claim_token=attempt.claim_token,
            dispatch=active_row,
            record=validated_record,
        )
        return await self._complete_validated_attempt(
            authoritative_attempt, validated_record, publish_outcome
        )

    async def dispatch_revision(
        self,
        *,
        tenant_id: UUID,
        incident_id: UUID,
        revision: int,
        staging_context: DispatchStagingContext | None = None,
    ) -> DispatchOutcome | None:
        """Stage (if needed), claim, publish, and record durable outcome for one revision.

        Returns ``None`` if neither a dispatch row nor a committed intent exists.
        Ordinary replay after ``acknowledged`` performs zero DML and zero Kafka sends.
        """
        existing = await self.load_dispatch_state(
            tenant_id=tenant_id,
            incident_id=incident_id,
            revision=revision,
        )
        if existing is None:
            if staging_context is None:
                # Without staging context, check if committed intent exists to fail closed.
                intent = await self._run_in_tx(
                    tenant_id,
                    lambda repo: repo.load_publication_intent(incident_id, revision),
                )
                if intent is None:
                    return None
                raise DispatchInputError(
                    "staging_context is required to stage an unstaged committed intent"
                )
            staged = await self.stage_committed_intent(
                tenant_id=tenant_id,
                incident_id=incident_id,
                revision=revision,
                staging_context=staging_context,
            )
            if staged is None:
                return None
            existing = staged.dispatch

        if existing.dispatch_state is DispatchState.ACKNOWLEDGED:
            return DispatchOutcome(
                status=DispatchOutcomeStatus.ALREADY_ACKNOWLEDGED,
                tenant_id=tenant_id,
                incident_id=incident_id,
                revision=revision,
                dispatch=existing,
                sends_attempted=0,
            )
        if existing.dispatch_state is DispatchState.REJECTED_UNSENDABLE:
            return DispatchOutcome(
                status=DispatchOutcomeStatus.COMMITTED_UNSENDABLE,
                tenant_id=tenant_id,
                incident_id=incident_id,
                revision=revision,
                dispatch=existing,
                sends_attempted=0,
                error_code=existing.last_error_code,
                error_message=existing.last_error_message,
            )
        if existing.dispatch_state is DispatchState.EXHAUSTED:
            return DispatchOutcome(
                status=DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED,
                tenant_id=tenant_id,
                incident_id=incident_id,
                revision=revision,
                dispatch=existing,
                sends_attempted=0,
                error_code=existing.last_error_code,
                error_message=existing.last_error_message,
            )

        claim_result = await self.claim_revision(
            tenant_id=tenant_id,
            incident_id=incident_id,
            revision=revision,
        )
        if not claim_result.claimed:
            assert claim_result.outcome is not None
            return claim_result.outcome

        assert claim_result.attempt is not None
        return await self.execute_claimed_attempt(claim_result.attempt)

    async def recover_pending_dispatches(
        self,
        *,
        tenant_id: UUID,
        staging_context: DispatchStagingContext | None = None,
        incident_id: UUID | None = None,
        max_items: int | None = None,
    ) -> DispatchRecoveryBatchResult:
        """Bounded restart recovery sweep for unstaged intents and due dispatches.

        - If ``staging_context`` is provided, stages up to ``limit`` unstaged
          committed intents (CW-03) in ascending ``(incident_id, revision)`` order.
        - Replays already-staged recoverable rows (CW-04, CW-05, CW-06) using
          exact stored frozen wire bytes without requiring ``staging_context``
          or querying Neo4j.
        """
        if max_items is None:
            limit = self._config.max_recovery_batch_size
        else:
            if not _is_strict_int(max_items) or not (
                1 <= max_items <= self._config.max_recovery_batch_size
            ):
                raise DispatchInputError(
                    f"max_items must be an integer in 1..{self._config.max_recovery_batch_size}"
                )
            limit = max_items

        unstaged: tuple[PersistedPublicationIntent, ...] = await self._run_in_tx(
            tenant_id,
            lambda repo: repo.list_unstaged_publication_intents(
                limit=limit, incident_id=incident_id
            ),
        )

        staged_count = 0
        unstaged_remaining = 0
        outcomes: list[DispatchOutcome] = []

        if staging_context is not None:
            for intent in unstaged:
                staged = await self.stage_committed_intent(
                    tenant_id=tenant_id,
                    incident_id=intent.incident_id,
                    revision=intent.revision,
                    staging_context=staging_context,
                )
                if staged is not None and staged.created:
                    staged_count += 1
                    if (
                        staged.dispatch.dispatch_state
                        is DispatchState.REJECTED_UNSENDABLE
                    ):
                        outcomes.append(
                            DispatchOutcome(
                                status=DispatchOutcomeStatus.COMMITTED_UNSENDABLE,
                                tenant_id=tenant_id,
                                incident_id=intent.incident_id,
                                revision=intent.revision,
                                dispatch=staged.dispatch,
                                sends_attempted=0,
                                error_code=staged.dispatch.last_error_code,
                                error_message=staged.dispatch.last_error_message,
                            )
                        )
        else:
            unstaged_remaining = len(unstaged)

        remaining_budget = max(0, limit - len(outcomes))
        if remaining_budget > 0:
            recoverable: tuple[
                PersistedPublicationDispatch, ...
            ] = await self._run_in_tx(
                tenant_id,
                lambda repo: repo.list_recoverable_dispatches(
                    limit=remaining_budget,
                    incident_id=incident_id,
                ),
            )
            for row in recoverable:
                claim_result = await self.claim_revision(
                    tenant_id=tenant_id,
                    incident_id=row.incident_id,
                    revision=row.revision,
                )
                if not claim_result.claimed:
                    assert claim_result.outcome is not None
                    outcomes.append(claim_result.outcome)
                    continue
                assert claim_result.attempt is not None
                outcome = await self.execute_claimed_attempt(claim_result.attempt)
                outcomes.append(outcome)

        return DispatchRecoveryBatchResult(
            staged_count=staged_count,
            unstaged_remaining_count=unstaged_remaining,
            outcomes=tuple(outcomes),
        )


__all__ = [
    "DEFAULT_CLAIM_LEASE_SECONDS",
    "DEFAULT_LEASE_SETTLEMENT_MARGIN_SECONDS",
    "DEFAULT_MAX_DISPATCH_ATTEMPTS",
    "DEFAULT_PUBLISH_TIMEOUT_SECONDS",
    "DEFAULT_RECOVERY_BATCH_SIZE",
    "DEFAULT_RETRY_BACKOFF_INITIAL_MS",
    "DEFAULT_RETRY_BACKOFF_MAX_MS",
    "MAX_CLAIM_LEASE_SECONDS",
    "MAX_LEASE_SETTLEMENT_MARGIN_SECONDS",
    "MAX_MAX_DISPATCH_ATTEMPTS",
    "MAX_PUBLISH_TIMEOUT_SECONDS",
    "MAX_RECOVERY_BATCH_SIZE",
    "MAX_RETRY_BACKOFF_INITIAL_MS",
    "MAX_RETRY_BACKOFF_MAX_MS",
    "MIN_CLAIM_LEASE_SECONDS",
    "MIN_LEASE_SETTLEMENT_MARGIN_SECONDS",
    "MIN_MAX_DISPATCH_ATTEMPTS",
    "MIN_PUBLISH_TIMEOUT_SECONDS",
    "MIN_RECOVERY_BATCH_SIZE",
    "MIN_RETRY_BACKOFF_INITIAL_MS",
    "MIN_RETRY_BACKOFF_MAX_MS",
    "ClaimAttemptResult",
    "ClaimedDispatchAttempt",
    "DispatchClaimLostError",
    "DispatchConfigurationError",
    "DispatchCorruptionError",
    "DispatchConfig",
    "DispatchInputError",
    "DispatchOutcome",
    "DispatchOutcomeStatus",
    "DispatchRecoveryBatchResult",
    "DispatchStagingContext",
    "DispatchState",
    "DispatchStoreError",
    "IncidentDispatchError",
    "IncidentDispatchService",
    "IncidentPublishPort",
    "PersistedPublicationDispatch",
    "StageIntentResult",
]
