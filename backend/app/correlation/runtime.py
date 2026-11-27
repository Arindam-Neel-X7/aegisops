"""Controlled bounded runtime coordinator for Phase 4 incident correlation.

Implements Task 4.12-RT1 against approved Task 4.12-A1-R1, C1-R1, P1, and D1-R2
contracts without activating Phase 5 consumers or enabling an always-on daemon.

Key invariants enforced:
1. Authenticated bounded plan (``controlled-correlation-runtime/1.0``):
   validates canonical JSON, independently supplied SHA-256, pinned config,
   pinned topology reference, blast-radius policy, execution context, frozen
   ``max_dispatch_attempts``, unique source coordinates, and explicit logical
   ``ingest`` / ``evaluate`` schedule.
2. Preserved production pipeline:
   connects Kafka anomaly consumption, frozen envelope/signal/lineage decoding,
   ``InMemoryAnomalyDedupeStore`` (without suppressing durable recovery),
   pinned ``TopologySnapshot`` verification, existing ``score_candidate_pair``,
   ``group_candidate_signals``, ``create_or_update_incident``,
   ``persist_incident_atomic``, and ``IncidentDispatchService`` (D1).
3. Strict call order and zero sends before commit:
   buffered anomalies never trigger persistence, Kafka sends, or offset commits;
   D1 dispatch executes strictly after durable transaction commit.
4. A1-R1 durable historical-prefix reconstruction:
   reconstructs scheduled prefixes using pure constructors against the previous
   reconstructed aggregate (never passing an earlier group to a later head),
   classifies ``r <= H`` as ``VERIFIED_HISTORICAL_REPLAY`` (zero DML),
   continues ``r == H + 1`` with explicit CAS, and verifies/recovers all
   committed intents ``1..H`` before settling any replay delivery.
5. Honest offset settlement, readiness, and shutdown:
   tracks only consumed offsets per assigned partition, commits strictly one
   past the leading contiguous settled sequence without rewinding, separates
   malformed-input quarantine from recoverable failures, propagates
   ``asyncio.CancelledError``, never runs implicit shutdown evaluations, and
   never closes caller-owned resources.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
from itertools import combinations
import json
import math
import re
import time
from typing import Any, Protocol, TypeVar, cast, runtime_checkable
from uuid import UUID

import aiokafka
from aiokafka import TopicPartition
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.anomaly.errors import AnomalyHandoffError
from app.anomaly.handoff import anomaly_signal_from_envelope
from app.anomaly.lineage import build_anomaly_reproducibility_lineage
from app.correlation import (
    aggregate as _aggregate_mod,
    grouping as _grouping_mod,
    persistence_service as _persistence_service_mod,
    scoring as _scoring_mod,
)
from app.correlation.aggregate import (
    STATE_ORDER,
    IncidentAggregationResult,
)
from app.correlation.blast_radius import BlastRadiusPolicy
from app.correlation.config import (
    CorrelationConfig,
    compute_correlation_config_fingerprint,
)
from app.correlation.consumer import (
    DEFAULT_INTAKE_CONSUMER_GROUP,
    DEFAULT_INTAKE_DEDUPE_MAX_SIZE,
    AnomalyCorrelationInput,
    AnomalyDedupeStore,
    DedupeReservationOutcome,
    InMemoryAnomalyDedupeStore,
)
from app.correlation.dispatch_repository import (
    verify_runtime_role_and_isolation,
)
from app.correlation.dispatch_service import (
    DEFAULT_CLAIM_LEASE_SECONDS,
    DEFAULT_LEASE_SETTLEMENT_MARGIN_SECONDS,
    DEFAULT_MAX_DISPATCH_ATTEMPTS,
    DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    DEFAULT_RECOVERY_BATCH_SIZE,
    DEFAULT_RETRY_BACKOFF_INITIAL_MS,
    DEFAULT_RETRY_BACKOFF_MAX_MS,
    MAX_CLAIM_LEASE_SECONDS,
    MAX_LEASE_SETTLEMENT_MARGIN_SECONDS,
    MAX_MAX_DISPATCH_ATTEMPTS,
    MAX_PUBLISH_TIMEOUT_SECONDS,
    MAX_RECOVERY_BATCH_SIZE,
    MAX_RETRY_BACKOFF_INITIAL_MS,
    MAX_RETRY_BACKOFF_MAX_MS,
    MIN_CLAIM_LEASE_SECONDS,
    MIN_LEASE_SETTLEMENT_MARGIN_SECONDS,
    MIN_MAX_DISPATCH_ATTEMPTS,
    MIN_PUBLISH_TIMEOUT_SECONDS,
    MIN_RECOVERY_BATCH_SIZE,
    MIN_RETRY_BACKOFF_INITIAL_MS,
    MIN_RETRY_BACKOFF_MAX_MS,
    DispatchConfig,
    DispatchOutcome,
    DispatchOutcomeStatus,
    DispatchStagingContext,
    IncidentDispatchService,
)
from app.correlation.errors import (
    AnomalyEvidenceLimitError,
    AnomalyIdentityConflictError,
    CorrelationError,
    IncidentPersistenceConflictError,
    IncidentPersistenceCorruptionError,
    IncidentPersistenceError,
    IncidentPersistenceTenantError,
)
from app.correlation.grouping import (
    CorrelationGroup,
    CorrelationGroupingResult,
    GroupingSignalDisposition,
)
from app.correlation.identity import derive_incident_id
from app.correlation.models import (
    MAX_EVIDENCE_ENTRIES,
    MAX_SIGNAL_REFS,
    CorrelationWindowState,
    IncidentAggregate,
    TopologySnapshotReference,
)
from app.correlation.persistence_repository import (
    IncidentPersistenceRepository,
    PersistedIncidentState,
    SqlAlchemyIncidentPersistenceRepository,
)
from app.correlation.publication import (
    MAX_CLAIMED_BY_BYTES,
    MAX_INT64,
    MAX_JSON_DEPTH,
    validate_publication_execution_context,
    validate_publication_producer_version,
)
from app.correlation.serialization import serialize_incident_aggregate
from app.correlation.topology import TopologyProvider, TopologySnapshot
from app.telemetry.reliability.quarantine import (
    QuarantineRecord,
    QuarantineSink,
    encode_bytes,
    encode_headers,
    sanitize_failure_message,
)
from app.telemetry.schemas import EventType, TelemetryEvent
from app.telemetry.topics import EVENT_TYPE_TO_TOPIC, KafkaTopic
from app.telemetry.transport.consumer import TelemetryEnvelope, parse_kafka_headers
from app.telemetry.transport.errors import (
    ConsumerRecordValidationError,
    DuplicateHeaderError,
    InvalidHeaderError,
    MissingRequiredHeaderError,
    TelemetryDeserializationError,
    TopicEventTypeMismatchError,
)
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
    deserialize_event,
)

_T = TypeVar("_T")
_RUNTIME_OPERATIONS: set[asyncio.Future[Any]] = set()


def _consume_background_task_exception(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _retain_runtime_operation(
    task: asyncio.Future[Any], owner: set[asyncio.Future[Any]]
) -> None:
    """Keep cancellation settlement alive and retrieve every late exception."""
    _RUNTIME_OPERATIONS.add(task)
    owner.add(task)

    def settled(done: asyncio.Future[Any]) -> None:
        _RUNTIME_OPERATIONS.discard(done)
        owner.discard(done)
        _consume_background_task_exception(done)

    task.add_done_callback(settled)


async def _await_before_deadline(
    coro: Awaitable[_T],
    *,
    deadline: float,
    pending: set[asyncio.Future[Any]] | None = None,
) -> _T:
    """Request cancellation at the deadline without waiting for its bookkeeping."""
    _, result, error = await _await_task_bounded(
        coro, deadline=deadline, pending=pending
    )
    if error is not None:
        raise error
    return cast(_T, result)


async def _await_task_bounded(
    coro: Awaitable[_T],
    *,
    deadline: float,
    pending: set[asyncio.Future[Any]] | None = None,
) -> tuple[bool, _T | None, BaseException | None]:
    """Run ``coro`` inside a task bounded strictly by ``deadline`` without blocking on slow cancel."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        if isinstance(coro, asyncio.Future):
            _retain_runtime_operation(coro, pending if pending is not None else set())
            coro.cancel()
        close_fn = getattr(coro, "close", None)
        if callable(close_fn):
            close_fn()
        return False, None, TimeoutError("Bounded deadline exceeded")
    task = asyncio.ensure_future(coro)
    _retain_runtime_operation(task, pending if pending is not None else set())
    try:
        done, _ = await asyncio.wait({task}, timeout=remaining)
    except asyncio.CancelledError:
        task.cancel()
        raise
    if not done:
        task.cancel()
        return False, None, TimeoutError("Bounded deadline exceeded")
    try:
        return True, task.result(), None
    except BaseException as exc:
        return True, None, exc


# ============================================================================
# FROZEN CONTRACT VERSIONS AND APPROVED OPERATIONAL BOUNDS (A1-R1)
# ============================================================================

RUNTIME_PLAN_SCHEMA_VERSION: str = "controlled-correlation-runtime/1.0"
MAX_PLAN_BYTES: int = 16_777_216
MAX_DELIVERY_ID_BYTES: int = 128

MIN_RUN_DURATION_SECONDS: int = 1
MAX_RUN_DURATION_SECONDS: int = 3_600
DEFAULT_RUN_DURATION_SECONDS: int = 60

MIN_INPUT_RECORDS: int = 1
MAX_INPUT_RECORDS: int = 10_000
DEFAULT_INPUT_RECORDS: int = 1_000

MIN_ACTIVE_WINDOWS: int = 1
MAX_ACTIVE_WINDOWS: int = 256
DEFAULT_ACTIVE_WINDOWS: int = 64

MIN_BUFFERED_RECORDS: int = 1
MAX_BUFFERED_RECORDS: int = 4_096
DEFAULT_BUFFERED_RECORDS: int = 1_024

MIN_WINDOW_MEMBERS: int = 1
MAX_WINDOW_MEMBERS: int = 91
DEFAULT_WINDOW_MEMBERS: int = 91

MIN_IN_FLIGHT_PUBLICATIONS: int = 1
MAX_IN_FLIGHT_PUBLICATIONS: int = 64
DEFAULT_IN_FLIGHT_PUBLICATIONS: int = 8

MIN_POLL_TIMEOUT_MS: int = 50
MAX_POLL_TIMEOUT_MS: int = 5_000
DEFAULT_POLL_TIMEOUT_MS: int = 250

MIN_SHUTDOWN_GRACE_SECONDS: float = 0.5
MAX_SHUTDOWN_GRACE_SECONDS: float = 60.0
DEFAULT_SHUTDOWN_GRACE_SECONDS: float = 5.0

_PLAN_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "schema_version",
        "run_id",
        "config",
        "config_sha256",
        "topology_reference",
        "blast_radius_policy",
        "execution_context",
        "producer_version",
        "source_records",
        "steps",
        "run_end_logical_time",
        "max_dispatch_attempts",
    }
)
_SOURCE_RECORD_KEYS: frozenset[str] = frozenset(
    {
        "delivery_id",
        "topic",
        "partition",
        "offset",
        "key_sha256",
        "value_sha256",
        "headers_sha256",
        "signal_id",
        "semantic_fingerprint",
    }
)
_TOPOLOGY_REF_KEYS: frozenset[str] = frozenset(
    {
        "topology_id",
        "topology_version",
        "topology_snapshot_fingerprint",
        "topology_captured_at",
    }
)
_EXECUTION_CONTEXT_KEYS: frozenset[str] = frozenset(
    {
        "run_id",
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "seed",
    }
)
_INGEST_STEP_KEYS: frozenset[str] = frozenset({"op", "delivery_id", "logical_time"})
_EVALUATE_STEP_KEYS: frozenset[str] = frozenset({"op", "logical_time"})

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_ISO_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$")
_UTC_ZERO = timedelta(0)

# ============================================================================
# TYPED RUNTIME ERROR HIERARCHY
# ============================================================================


class CorrelationRuntimeError(CorrelationError):
    """Base exception for Task 4.12-RT1 controlled runtime errors."""


class RuntimeConfigurationError(CorrelationRuntimeError):
    """Raised when runtime bounds or timing invariants are invalid."""


class RuntimeStartupError(CorrelationRuntimeError):
    """Raised when startup pinning or dependency initialization fails closed."""


class RuntimeInputValidationError(CorrelationRuntimeError):
    """Raised on malformed Kafka input when no QuarantineSink is configured."""


class RuntimeCapacityError(CorrelationRuntimeError):
    """Raised when a finite runtime capacity bound is exceeded before commit."""


class RuntimeRecoveryInputError(CorrelationRuntimeError):
    """Raised when recovery plan authority, retained input, or durable history fails validation."""

    def __init__(
        self,
        message: str,
        *,
        pointer: str | None = None,
        reason: str | None = None,
    ) -> None:
        self.pointer = pointer
        self.reason = reason or message
        full_message = f"{pointer}: {message}" if pointer else message
        super().__init__(full_message)


class RuntimePersistenceError(CorrelationRuntimeError):
    """Raised when durable persistence fails unexpectedly during coordination."""


class RuntimeOffsetCommitError(CorrelationRuntimeError):
    """Raised when Kafka consumer offset commit fails after durable acknowledgement."""


class RuntimeRebalanceError(CorrelationRuntimeError):
    """Raised when partition assignment is revoked, cancelling commit ownership."""


# ============================================================================
# RUNTIME STATES, CONFIG, AND RESULT MODELS
# ============================================================================


class CorrelationRuntimeState(StrEnum):
    """Bounded explicit lifecycle state machine for ``CorrelationRuntimeCoordinator``."""

    CREATED = "created"
    STARTING = "starting"
    READY = "ready"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class CorrelationRuntimeRunStatus(StrEnum):
    """Explicit non-masking terminal outcome for a controlled runtime execution."""

    COMPLETED = "COMPLETED"
    COMPLETED_WITH_PENDING_DISPATCH = "COMPLETED_WITH_PENDING_DISPATCH"
    COMPLETED_WITH_TERMINAL_DISPATCH_FAILURE = (
        "COMPLETED_WITH_TERMINAL_DISPATCH_FAILURE"
    )
    INCOMPLETE_DEADLINE_EXCEEDED = "INCOMPLETE_DEADLINE_EXCEEDED"
    INCOMPLETE_DELAYED_INPUTS = "INCOMPLETE_DELAYED_INPUTS"
    STOPPED_BY_SHUTDOWN = "STOPPED_BY_SHUTDOWN"


class DeliverySettlementState(StrEnum):
    """Per-offset settlement state tracked by ``PartitionOffsetTracker``."""

    BUFFERED_UNCOMMITTED = "BUFFERED_UNCOMMITTED"
    SETTLED_ACKNOWLEDGED = "SETTLED_ACKNOWLEDGED"
    SETTLED_DUPLICATE = "SETTLED_DUPLICATE"
    SETTLED_LATE_EXCLUDED = "SETTLED_LATE_EXCLUDED"
    SETTLED_QUARANTINED = "SETTLED_QUARANTINED"


_SETTLED_STATES: frozenset[DeliverySettlementState] = frozenset(
    {
        DeliverySettlementState.SETTLED_ACKNOWLEDGED,
        DeliverySettlementState.SETTLED_DUPLICATE,
        DeliverySettlementState.SETTLED_LATE_EXCLUDED,
        DeliverySettlementState.SETTLED_QUARANTINED,
    }
)


class PersistenceDisposition(StrEnum):
    """Outcome of reconciling a reconstructed incident revision against PostgreSQL."""

    PERSISTED_CREATED = "PERSISTED_CREATED"
    PERSISTED_UPDATED = "PERSISTED_UPDATED"
    VERIFIED_HISTORICAL_REPLAY = "VERIFIED_HISTORICAL_REPLAY"
    NO_OP_REPLAY = "NO_OP_REPLAY"


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


def _validate_trimmed_utf8(
    value: Any,
    *,
    max_bytes: int,
    field_name: str,
    error_cls: type[CorrelationRuntimeError] = RuntimeConfigurationError,
) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise error_cls(f"{field_name} must be trimmed non-empty text")
    if _has_control_chars(value):
        raise error_cls(f"{field_name} must not contain control characters")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise error_cls(f"{field_name} contains invalid Unicode") from exc
    if len(encoded) > max_bytes:
        raise error_cls(f"{field_name} exceeds {max_bytes} UTF-8 bytes")
    return value


@dataclass(frozen=True)
class CorrelationRuntimeConfig:
    """Validated finite operational bounds and coherent timing config (A1-R1)."""

    worker_id: str = "phase4-correlation-runtime"
    consumer_group_id: str = DEFAULT_INTAKE_CONSUMER_GROUP
    max_run_duration_seconds: int = DEFAULT_RUN_DURATION_SECONDS
    max_input_records: int = DEFAULT_INPUT_RECORDS
    max_active_windows: int = DEFAULT_ACTIVE_WINDOWS
    max_buffered_records: int = DEFAULT_BUFFERED_RECORDS
    max_window_members: int = DEFAULT_WINDOW_MEMBERS
    max_in_flight_publications: int = DEFAULT_IN_FLIGHT_PUBLICATIONS
    max_dispatch_attempts: int = DEFAULT_MAX_DISPATCH_ATTEMPTS
    retry_backoff_initial_ms: int = DEFAULT_RETRY_BACKOFF_INITIAL_MS
    retry_backoff_max_ms: int = DEFAULT_RETRY_BACKOFF_MAX_MS
    claim_lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS
    publish_timeout_seconds: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS
    lease_settlement_margin_seconds: float = DEFAULT_LEASE_SETTLEMENT_MARGIN_SECONDS
    poll_timeout_ms: int = DEFAULT_POLL_TIMEOUT_MS
    shutdown_grace_seconds: float = DEFAULT_SHUTDOWN_GRACE_SECONDS
    max_recovery_batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE

    def __post_init__(self) -> None:
        _validate_trimmed_utf8(
            self.worker_id,
            max_bytes=MAX_CLAIMED_BY_BYTES,
            field_name="worker_id",
        )
        _validate_trimmed_utf8(
            self.consumer_group_id,
            max_bytes=MAX_CLAIMED_BY_BYTES,
            field_name="consumer_group_id",
        )

        int_bounds: tuple[tuple[str, Any, int, int], ...] = (
            (
                "max_run_duration_seconds",
                self.max_run_duration_seconds,
                MIN_RUN_DURATION_SECONDS,
                MAX_RUN_DURATION_SECONDS,
            ),
            (
                "max_input_records",
                self.max_input_records,
                MIN_INPUT_RECORDS,
                MAX_INPUT_RECORDS,
            ),
            (
                "max_active_windows",
                self.max_active_windows,
                MIN_ACTIVE_WINDOWS,
                MAX_ACTIVE_WINDOWS,
            ),
            (
                "max_buffered_records",
                self.max_buffered_records,
                MIN_BUFFERED_RECORDS,
                MAX_BUFFERED_RECORDS,
            ),
            (
                "max_window_members",
                self.max_window_members,
                MIN_WINDOW_MEMBERS,
                MAX_WINDOW_MEMBERS,
            ),
            (
                "max_in_flight_publications",
                self.max_in_flight_publications,
                MIN_IN_FLIGHT_PUBLICATIONS,
                MAX_IN_FLIGHT_PUBLICATIONS,
            ),
            (
                "max_dispatch_attempts",
                self.max_dispatch_attempts,
                MIN_MAX_DISPATCH_ATTEMPTS,
                MAX_MAX_DISPATCH_ATTEMPTS,
            ),
            (
                "retry_backoff_initial_ms",
                self.retry_backoff_initial_ms,
                MIN_RETRY_BACKOFF_INITIAL_MS,
                MAX_RETRY_BACKOFF_INITIAL_MS,
            ),
            (
                "retry_backoff_max_ms",
                self.retry_backoff_max_ms,
                MIN_RETRY_BACKOFF_MAX_MS,
                MAX_RETRY_BACKOFF_MAX_MS,
            ),
            (
                "claim_lease_seconds",
                self.claim_lease_seconds,
                MIN_CLAIM_LEASE_SECONDS,
                MAX_CLAIM_LEASE_SECONDS,
            ),
            (
                "poll_timeout_ms",
                self.poll_timeout_ms,
                MIN_POLL_TIMEOUT_MS,
                MAX_POLL_TIMEOUT_MS,
            ),
            (
                "max_recovery_batch_size",
                self.max_recovery_batch_size,
                MIN_RECOVERY_BATCH_SIZE,
                MAX_RECOVERY_BATCH_SIZE,
            ),
        )
        for name, val, lo, hi in int_bounds:
            if not _is_strict_int(val) or not (lo <= val <= hi):
                raise RuntimeConfigurationError(
                    f"{name} must be an integer in {lo}..{hi}"
                )

        if self.retry_backoff_initial_ms > self.retry_backoff_max_ms:
            raise RuntimeConfigurationError(
                "retry_backoff_initial_ms must not exceed retry_backoff_max_ms"
            )

        float_bounds: tuple[tuple[str, Any, float, float], ...] = (
            (
                "publish_timeout_seconds",
                self.publish_timeout_seconds,
                MIN_PUBLISH_TIMEOUT_SECONDS,
                MAX_PUBLISH_TIMEOUT_SECONDS,
            ),
            (
                "lease_settlement_margin_seconds",
                self.lease_settlement_margin_seconds,
                MIN_LEASE_SETTLEMENT_MARGIN_SECONDS,
                MAX_LEASE_SETTLEMENT_MARGIN_SECONDS,
            ),
            (
                "shutdown_grace_seconds",
                self.shutdown_grace_seconds,
                MIN_SHUTDOWN_GRACE_SECONDS,
                MAX_SHUTDOWN_GRACE_SECONDS,
            ),
        )
        for f_name, f_val, f_lo, f_hi in float_bounds:
            if not _is_finite_number(f_val) or not (f_lo <= float(f_val) <= f_hi):
                raise RuntimeConfigurationError(
                    f"{f_name} must be a finite number in {f_lo}..{f_hi}"
                )

        if float(self.claim_lease_seconds) < (
            float(self.publish_timeout_seconds)
            + float(self.lease_settlement_margin_seconds)
        ):
            raise RuntimeConfigurationError(
                "claim_lease_seconds must be >= publish_timeout_seconds + lease_settlement_margin_seconds"
            )

    def to_dispatch_config(
        self, *, max_dispatch_attempts: int | None = None
    ) -> DispatchConfig:
        attempts = (
            self.max_dispatch_attempts
            if max_dispatch_attempts is None
            else max_dispatch_attempts
        )
        return DispatchConfig(
            worker_id=self.worker_id,
            max_dispatch_attempts=attempts,
            retry_backoff_initial_ms=self.retry_backoff_initial_ms,
            retry_backoff_max_ms=self.retry_backoff_max_ms,
            claim_lease_seconds=self.claim_lease_seconds,
            publish_timeout_seconds=float(self.publish_timeout_seconds),
            lease_settlement_margin_seconds=float(self.lease_settlement_margin_seconds),
            max_recovery_batch_size=self.max_recovery_batch_size,
        )


# ============================================================================
# AUTHENTICATED CONTROLLED RUNTIME PLAN (A1-R1 B1)
# ============================================================================


@dataclass(frozen=True)
class PlannedTopologyPin:
    """Pinned topology identity inside ``controlled-correlation-runtime/1.0``."""

    topology_id: UUID
    topology_version: str
    topology_snapshot_fingerprint: str
    topology_captured_at: datetime

    def matches_reference(self, ref: TopologySnapshotReference) -> bool:
        return (
            self.topology_id == ref.topology_id
            and self.topology_version == ref.topology_version
            and self.topology_snapshot_fingerprint == ref.snapshot_fingerprint
            and self.topology_captured_at == ref.captured_at
        )

    def matches_snapshot(self, snapshot: TopologySnapshot) -> bool:
        return (
            self.topology_id == snapshot.version.topology_id
            and self.topology_version == snapshot.version.topology_version
            and self.topology_snapshot_fingerprint == snapshot.snapshot_fingerprint
        )


@dataclass(frozen=True)
class PlannedSourceRecord:
    """Pinned Kafka source coordinate and cryptographic digests."""

    index: int
    delivery_id: str
    topic: str
    partition: int
    offset: int
    key_sha256: str
    value_sha256: str
    headers_sha256: str
    signal_id: UUID
    semantic_fingerprint: str

    @property
    def coordinate(self) -> tuple[str, int, int]:
        return (self.topic, self.partition, self.offset)

    @property
    def topic_partition(self) -> TopicPartition:
        return TopicPartition(self.topic, self.partition)


@dataclass(frozen=True)
class PlannedStep:
    """Explicit scheduled ``ingest`` or ``evaluate`` step on logical UTC clock."""

    index: int
    op: str
    logical_time: datetime
    logical_time_iso: str
    delivery_id: str | None = None


@dataclass(frozen=True)
class AuthenticatedRuntimePlan:
    """Strictly validated and digest-authenticated controlled runtime plan."""

    schema_version: str
    plan_sha256: str
    run_id: UUID
    config: CorrelationConfig
    config_sha256: str
    topology_reference: PlannedTopologyPin
    blast_radius_policy: BlastRadiusPolicy
    execution_context: TelemetryExecutionContext
    producer_version: str | None
    source_records: tuple[PlannedSourceRecord, ...]
    steps: tuple[PlannedStep, ...]
    run_end_logical_time: datetime
    max_dispatch_attempts: int

    def source_by_delivery_id(self) -> dict[str, PlannedSourceRecord]:
        return {rec.delivery_id: rec for rec in self.source_records}

    def source_by_coordinate(
        self,
    ) -> dict[tuple[str, int, int], PlannedSourceRecord]:
        return {rec.coordinate: rec for rec in self.source_records}


@dataclass(frozen=True)
class SourceRecordDigests:
    """Observed cryptographic digests and ordered UTF-8 headers for one Kafka record."""

    key_sha256: str
    value_sha256: str
    headers_sha256: str
    ordered_headers: tuple[tuple[str, str], ...]


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _check_json_depth_and_unicode(obj: Any, *, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise RuntimeRecoveryInputError(
            f"JSON depth exceeds maximum {MAX_JSON_DEPTH}",
            pointer="/plan",
        )
    if isinstance(obj, str):
        try:
            obj.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise RuntimeRecoveryInputError(
                "Plan contains invalid Unicode", pointer="/plan"
            ) from exc
        return
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise RuntimeRecoveryInputError(
                "Non-finite floating point number is forbidden", pointer="/plan"
            )
        return
    if isinstance(obj, list):
        for item in obj:
            _check_json_depth_and_unicode(item, depth=depth + 1)
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                raise RuntimeRecoveryInputError(
                    "Plan object keys must be strings", pointer="/plan"
                )
            try:
                k.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise RuntimeRecoveryInputError(
                    "Plan key contains invalid Unicode", pointer="/plan"
                ) from exc
            _check_json_depth_and_unicode(v, depth=depth + 1)


def _strict_json_loads(raw: bytes) -> Any:
    if len(raw) > MAX_PLAN_BYTES:
        raise RuntimeRecoveryInputError(
            f"Plan payload exceeds maximum {MAX_PLAN_BYTES} bytes",
            pointer="/plan",
        )
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeRecoveryInputError(
            "Plan payload is not valid UTF-8", pointer="/plan"
        ) from exc

    def _reject_dup_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in pairs:
            if k in out:
                raise RuntimeRecoveryInputError(
                    f"Duplicate JSON key '{k}' in plan", pointer="/plan"
                )
            out[k] = v
        return out

    def _reject_non_finite(token: str) -> Any:
        raise RuntimeRecoveryInputError(
            f"Non-finite JSON constant '{token}' is forbidden", pointer="/plan"
        )

    try:
        parsed = json.loads(
            decoded,
            object_pairs_hook=_reject_dup_keys,
            parse_constant=_reject_non_finite,
        )
    except RuntimeRecoveryInputError:
        raise
    except Exception as exc:
        raise RuntimeRecoveryInputError(
            "Plan payload is not valid JSON", pointer="/plan"
        ) from exc

    _check_json_depth_and_unicode(parsed)
    return parsed


def _parse_canonical_uuid(value: Any, *, pointer: str) -> UUID:
    if not isinstance(value, str):
        raise RuntimeRecoveryInputError(
            "Expected canonical lowercase UUID", pointer=pointer
        )
    try:
        parsed = UUID(value)
    except Exception as exc:
        raise RuntimeRecoveryInputError("Invalid UUID value", pointer=pointer) from exc
    if str(parsed) != value:
        raise RuntimeRecoveryInputError(
            "UUID must be canonical lowercase format", pointer=pointer
        )
    return parsed


def _parse_hex64(value: Any, *, pointer: str) -> str:
    if not isinstance(value, str) or not _HEX64_RE.fullmatch(value):
        raise RuntimeRecoveryInputError(
            "Expected 64-character lowercase hexadecimal digest", pointer=pointer
        )
    return value


def _parse_canonical_utc_iso_z(value: Any, *, pointer: str) -> datetime:
    if not isinstance(value, str) or not _UTC_ISO_Z_RE.fullmatch(value):
        raise RuntimeRecoveryInputError(
            "Expected canonical UTC ISO-8601 timestamp ending with 'Z'",
            pointer=pointer,
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeRecoveryInputError(
            "Invalid UTC ISO-8601 timestamp", pointer=pointer
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != _UTC_ZERO:
        raise RuntimeRecoveryInputError(
            "Timestamp must have UTC offset +00:00", pointer=pointer
        )
    return parsed.astimezone(timezone.utc)


def compute_runtime_plan_sha256(plan: Mapping[str, Any]) -> str:
    """Compute canonical SHA-256 digest of a controlled runtime plan dictionary."""
    if not isinstance(plan, Mapping):
        raise RuntimeRecoveryInputError(
            "Plan must be a JSON object mapping", pointer="/plan"
        )
    raw = _canonical_json_bytes(dict(plan))
    return _sha256_hex(raw)


def compute_source_record_digests(record: Any) -> SourceRecordDigests:
    """Compute exact A1-R1 ``key_sha256``, ``value_sha256``, and ``headers_sha256``."""
    raw_key = b"" if record.key is None else bytes(record.key)
    raw_value = b"" if record.value is None else bytes(record.value)
    raw_headers = record.headers
    if raw_headers is None:
        raise MissingRequiredHeaderError("Record headers are missing")

    seen_names: set[str] = set()
    ordered_pairs: list[list[str]] = []
    ordered_tuples: list[tuple[str, str]] = []
    for item in raw_headers:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise InvalidHeaderError("Kafka header entry must be a (name, value) pair")
        name, val = item
        if not isinstance(name, str) or not name:
            raise InvalidHeaderError("Kafka header name must be a non-empty string")
        if name in seen_names:
            raise DuplicateHeaderError(f"Duplicate Kafka header: '{name}'")
        seen_names.add(name)
        if not isinstance(val, (bytes, bytearray, memoryview)):
            raise InvalidHeaderError(f"Kafka header '{name}' value must be bytes")
        try:
            decoded_val = bytes(val).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidHeaderError(
                f"Header '{name}' contains invalid UTF-8 bytes"
            ) from exc
        ordered_pairs.append([name, decoded_val])
        ordered_tuples.append((name, decoded_val))

    return SourceRecordDigests(
        key_sha256=_sha256_hex(raw_key),
        value_sha256=_sha256_hex(raw_value),
        headers_sha256=_sha256_hex(_canonical_json_bytes(ordered_pairs)),
        ordered_headers=tuple(ordered_tuples),
    )


def parse_and_authenticate_runtime_plan(
    plan_source: bytes | bytearray | memoryview | Mapping[str, Any] | None,
    *,
    expected_plan_sha256: str | None,
    max_input_records: int = DEFAULT_INPUT_RECORDS,
) -> AuthenticatedRuntimePlan:
    """Strictly parse and cryptographically authenticate a controlled runtime plan."""
    if not _is_strict_int(max_input_records) or not (
        MIN_INPUT_RECORDS <= max_input_records <= MAX_INPUT_RECORDS
    ):
        raise RuntimeConfigurationError(
            f"max_input_records must be an integer in {MIN_INPUT_RECORDS}..{MAX_INPUT_RECORDS}"
        )

    if plan_source is None:
        raise RuntimeRecoveryInputError(
            "Original authenticated runtime plan is required", pointer="/plan"
        )

    expected_digest = _parse_hex64(expected_plan_sha256, pointer="/plan_sha256")

    if isinstance(plan_source, (bytes, bytearray, memoryview)):
        raw_obj = _strict_json_loads(bytes(plan_source))
    elif isinstance(plan_source, Mapping):
        try:
            canonical_raw = _canonical_json_bytes(dict(plan_source))
        except Exception as exc:
            raise RuntimeRecoveryInputError(
                "Runtime plan is not canonical JSON-serializable", pointer="/plan"
            ) from exc
        raw_obj = _strict_json_loads(canonical_raw)
    else:
        raise RuntimeRecoveryInputError(
            "Runtime plan must be JSON bytes or a mapping", pointer="/plan"
        )

    if not isinstance(raw_obj, dict):
        raise RuntimeRecoveryInputError(
            "Runtime plan must be a JSON object", pointer="/plan"
        )

    actual_digest = _sha256_hex(_canonical_json_bytes(raw_obj))
    if actual_digest != expected_digest:
        raise RuntimeRecoveryInputError(
            "Independent runtime plan SHA-256 mismatch", pointer="/plan_sha256"
        )

    if set(raw_obj.keys()) != _PLAN_TOP_LEVEL_KEYS:
        raise RuntimeRecoveryInputError(
            "Runtime plan must contain the exact approved top-level field set",
            pointer="/plan",
        )

    if raw_obj["schema_version"] != RUNTIME_PLAN_SCHEMA_VERSION:
        raise RuntimeRecoveryInputError(
            f"Unsupported runtime plan schema_version; expected {RUNTIME_PLAN_SCHEMA_VERSION}",
            pointer="/schema_version",
        )

    run_id = _parse_canonical_uuid(raw_obj["run_id"], pointer="/run_id")

    # CorrelationConfig + config_sha256
    cfg_raw = raw_obj["config"]
    if not isinstance(cfg_raw, dict):
        raise RuntimeRecoveryInputError(
            "Plan config must be a JSON object", pointer="/config"
        )
    try:
        cfg = CorrelationConfig.model_validate_json(
            _canonical_json_bytes(cfg_raw).decode("utf-8")
        )
    except Exception as exc:
        raise RuntimeRecoveryInputError(
            "Invalid CorrelationConfig in runtime plan", pointer="/config"
        ) from exc

    config_sha256 = _parse_hex64(raw_obj["config_sha256"], pointer="/config_sha256")
    actual_cfg_fp = compute_correlation_config_fingerprint(cfg)
    if actual_cfg_fp != config_sha256:
        raise RuntimeRecoveryInputError(
            "Plan config_sha256 does not match CorrelationConfig fingerprint",
            pointer="/config_sha256",
        )

    # Topology reference
    topo_raw = raw_obj["topology_reference"]
    if not isinstance(topo_raw, dict) or set(topo_raw.keys()) != _TOPOLOGY_REF_KEYS:
        raise RuntimeRecoveryInputError(
            "Plan topology_reference must contain exact approved fields",
            pointer="/topology_reference",
        )
    topo_id = _parse_canonical_uuid(
        topo_raw["topology_id"], pointer="/topology_reference/topology_id"
    )
    topo_version = _validate_trimmed_utf8(
        topo_raw["topology_version"],
        max_bytes=128,
        field_name="topology_version",
        error_cls=RuntimeRecoveryInputError,
    )
    topo_fp = _parse_hex64(
        topo_raw["topology_snapshot_fingerprint"],
        pointer="/topology_reference/topology_snapshot_fingerprint",
    )
    topo_captured_at = _parse_canonical_utc_iso_z(
        topo_raw["topology_captured_at"],
        pointer="/topology_reference/topology_captured_at",
    )
    topo_pin = PlannedTopologyPin(
        topology_id=topo_id,
        topology_version=topo_version,
        topology_snapshot_fingerprint=topo_fp,
        topology_captured_at=topo_captured_at,
    )

    # BlastRadiusPolicy
    bp_raw = raw_obj["blast_radius_policy"]
    if not isinstance(bp_raw, dict):
        raise RuntimeRecoveryInputError(
            "Plan blast_radius_policy must be a JSON object",
            pointer="/blast_radius_policy",
        )
    try:
        blast_policy = BlastRadiusPolicy.model_validate_json(
            _canonical_json_bytes(bp_raw).decode("utf-8")
        )
    except Exception as exc:
        raise RuntimeRecoveryInputError(
            "Invalid BlastRadiusPolicy in runtime plan",
            pointer="/blast_radius_policy",
        ) from exc

    # Execution context
    ctx_raw = raw_obj["execution_context"]
    if not isinstance(ctx_raw, dict) or set(ctx_raw.keys()) != _EXECUTION_CONTEXT_KEYS:
        raise RuntimeRecoveryInputError(
            "Plan execution_context must contain exact approved fields",
            pointer="/execution_context",
        )
    ctx_run_id = _parse_canonical_uuid(
        ctx_raw["run_id"], pointer="/execution_context/run_id"
    )
    if ctx_run_id != run_id:
        raise RuntimeRecoveryInputError(
            "Plan run_id must equal execution_context.run_id", pointer="/run_id"
        )
    seed_raw = ctx_raw["seed"]
    if not _is_strict_int(seed_raw) or not (0 <= seed_raw <= MAX_UINT64):
        raise RuntimeRecoveryInputError(
            "execution_context.seed must be an integer in [0, 2^64-1]",
            pointer="/execution_context/seed",
        )
    for text_key in ("scenario_id", "scenario_version", "reproducibility_key"):
        if not isinstance(ctx_raw[text_key], str):
            raise RuntimeRecoveryInputError(
                f"execution_context.{text_key} must be a string",
                pointer=f"/execution_context/{text_key}",
            )
    try:
        exec_ctx = TelemetryExecutionContext(
            run_id=ctx_run_id,
            scenario_id=ctx_raw["scenario_id"],
            scenario_version=ctx_raw["scenario_version"],
            reproducibility_key=ctx_raw["reproducibility_key"],
            seed=seed_raw,
        )
        validate_publication_execution_context(exec_ctx)
    except Exception as exc:
        raise RuntimeRecoveryInputError(
            "Invalid execution_context in runtime plan", pointer="/execution_context"
        ) from exc

    # Producer version
    pv_raw = raw_obj["producer_version"]
    try:
        producer_version = validate_publication_producer_version(pv_raw)
    except Exception as exc:
        raise RuntimeRecoveryInputError(
            "Invalid producer_version in runtime plan", pointer="/producer_version"
        ) from exc

    # Frozen max_dispatch_attempts
    max_attempts = raw_obj["max_dispatch_attempts"]
    if not _is_strict_int(max_attempts) or not (
        MIN_MAX_DISPATCH_ATTEMPTS <= max_attempts <= MAX_MAX_DISPATCH_ATTEMPTS
    ):
        raise RuntimeRecoveryInputError(
            "max_dispatch_attempts must be an integer in 1..10",
            pointer="/max_dispatch_attempts",
        )

    # Source records
    src_raw = raw_obj["source_records"]
    if not isinstance(src_raw, list) or len(src_raw) == 0:
        raise RuntimeRecoveryInputError(
            "source_records must be a non-empty list", pointer="/source_records"
        )
    if len(src_raw) > max_input_records:
        raise RuntimeCapacityError(
            f"Plan source_records count ({len(src_raw)}) exceeds max_input_records ({max_input_records})"
        )

    parsed_sources: list[PlannedSourceRecord] = []
    seen_delivery_ids: set[str] = set()
    seen_coords: set[tuple[str, int, int]] = set()
    for idx, entry in enumerate(src_raw):
        ptr = f"/source_records/{idx}"
        if not isinstance(entry, dict) or set(entry.keys()) != _SOURCE_RECORD_KEYS:
            raise RuntimeRecoveryInputError(
                "Source record must contain the exact approved fields", pointer=ptr
            )
        delivery_id = _validate_trimmed_utf8(
            entry["delivery_id"],
            max_bytes=MAX_DELIVERY_ID_BYTES,
            field_name="delivery_id",
            error_cls=RuntimeRecoveryInputError,
        )
        topic = entry["topic"]
        if topic != KafkaTopic.ANOMALIES.value:
            raise RuntimeRecoveryInputError(
                f"Source topic must be '{KafkaTopic.ANOMALIES.value}'",
                pointer=f"{ptr}/topic",
            )
        part = entry["partition"]
        if not _is_strict_int(part) or part < 0:
            raise RuntimeRecoveryInputError(
                "Source partition must be a non-negative integer",
                pointer=f"{ptr}/partition",
            )
        offset = entry["offset"]
        if not _is_strict_int(offset) or not (0 <= offset <= MAX_INT64):
            raise RuntimeRecoveryInputError(
                "Source offset must be an integer in 0..2^63-1",
                pointer=f"{ptr}/offset",
            )
        coord = (topic, part, offset)
        if delivery_id in seen_delivery_ids or coord in seen_coords:
            raise RuntimeRecoveryInputError(
                "Duplicate delivery_id or Kafka coordinate in source_records",
                pointer=ptr,
            )
        seen_delivery_ids.add(delivery_id)
        seen_coords.add(coord)

        parsed_sources.append(
            PlannedSourceRecord(
                index=idx,
                delivery_id=delivery_id,
                topic=topic,
                partition=part,
                offset=offset,
                key_sha256=_parse_hex64(
                    entry["key_sha256"], pointer=f"{ptr}/key_sha256"
                ),
                value_sha256=_parse_hex64(
                    entry["value_sha256"], pointer=f"{ptr}/value_sha256"
                ),
                headers_sha256=_parse_hex64(
                    entry["headers_sha256"], pointer=f"{ptr}/headers_sha256"
                ),
                signal_id=_parse_canonical_uuid(
                    entry["signal_id"], pointer=f"{ptr}/signal_id"
                ),
                semantic_fingerprint=_parse_hex64(
                    entry["semantic_fingerprint"],
                    pointer=f"{ptr}/semantic_fingerprint",
                ),
            )
        )

    # Scheduled steps
    steps_raw = raw_obj["steps"]
    max_steps = 2 * max_input_records + 2
    if not isinstance(steps_raw, list) or len(steps_raw) == 0:
        raise RuntimeRecoveryInputError(
            "steps must be a non-empty list", pointer="/steps"
        )
    if len(steps_raw) > max_steps:
        raise RuntimeCapacityError(
            f"Plan steps count ({len(steps_raw)}) exceeds maximum ({max_steps})"
        )

    parsed_steps: list[PlannedStep] = []
    ingested_ids: set[str] = set()
    eval_count = 0
    last_time: datetime | None = None
    last_op: str | None = None
    last_ingest_delivery_id: str | None = None

    for idx, step in enumerate(steps_raw):
        ptr = f"/steps/{idx}"
        if not isinstance(step, dict):
            raise RuntimeRecoveryInputError("Step must be a JSON object", pointer=ptr)
        op = step.get("op")
        if op == "ingest":
            if set(step.keys()) != _INGEST_STEP_KEYS:
                raise RuntimeRecoveryInputError(
                    "ingest step must contain exact keys op, delivery_id, logical_time",
                    pointer=ptr,
                )
            d_id = step["delivery_id"]
            if (
                not isinstance(d_id, str)
                or d_id not in seen_delivery_ids
                or d_id in ingested_ids
            ):
                raise RuntimeRecoveryInputError(
                    "ingest step must reference a unique declared delivery_id",
                    pointer=ptr,
                )
            dt = _parse_canonical_utc_iso_z(
                step["logical_time"], pointer=f"{ptr}/logical_time"
            )
            if last_time is not None:
                if dt < last_time:
                    raise RuntimeRecoveryInputError(
                        "Step logical_time must be nondecreasing", pointer=ptr
                    )
                if dt == last_time:
                    if last_op == "evaluate":
                        raise RuntimeRecoveryInputError(
                            "Equal-time ingest steps must precede evaluate steps",
                            pointer=ptr,
                        )
                    if (
                        last_ingest_delivery_id is not None
                        and d_id < last_ingest_delivery_id
                    ):
                        raise RuntimeRecoveryInputError(
                            "Equal-time ingest steps must be ordered by delivery_id",
                            pointer=ptr,
                        )
            ingested_ids.add(d_id)
            last_time = dt
            last_op = "ingest"
            last_ingest_delivery_id = d_id
            parsed_steps.append(
                PlannedStep(
                    index=idx,
                    op="ingest",
                    logical_time=dt,
                    logical_time_iso=step["logical_time"],
                    delivery_id=d_id,
                )
            )
        elif op == "evaluate":
            if set(step.keys()) != _EVALUATE_STEP_KEYS:
                raise RuntimeRecoveryInputError(
                    "evaluate step must contain exact keys op, logical_time",
                    pointer=ptr,
                )
            if not ingested_ids:
                raise RuntimeRecoveryInputError(
                    "evaluate step requires at least one preceding ingest step",
                    pointer=ptr,
                )
            dt = _parse_canonical_utc_iso_z(
                step["logical_time"], pointer=f"{ptr}/logical_time"
            )
            if last_time is not None and dt < last_time:
                raise RuntimeRecoveryInputError(
                    "Step logical_time must be nondecreasing", pointer=ptr
                )
            eval_count += 1
            last_time = dt
            last_op = "evaluate"
            last_ingest_delivery_id = None
            parsed_steps.append(
                PlannedStep(
                    index=idx,
                    op="evaluate",
                    logical_time=dt,
                    logical_time_iso=step["logical_time"],
                    delivery_id=None,
                )
            )
        else:
            raise RuntimeRecoveryInputError(
                "Step op must be 'ingest' or 'evaluate'", pointer=ptr
            )

    if ingested_ids != seen_delivery_ids:
        raise RuntimeRecoveryInputError(
            "Every declared source_record must appear in exactly one ingest step",
            pointer="/steps",
        )
    if eval_count == 0:
        raise RuntimeRecoveryInputError(
            "Plan must contain at least one explicit evaluate step", pointer="/steps"
        )

    run_end = _parse_canonical_utc_iso_z(
        raw_obj["run_end_logical_time"], pointer="/run_end_logical_time"
    )
    assert last_time is not None
    if run_end < last_time:
        raise RuntimeRecoveryInputError(
            "run_end_logical_time must be >= last step logical_time",
            pointer="/run_end_logical_time",
        )

    return AuthenticatedRuntimePlan(
        schema_version=RUNTIME_PLAN_SCHEMA_VERSION,
        plan_sha256=actual_digest,
        run_id=run_id,
        config=cfg,
        config_sha256=config_sha256,
        topology_reference=topo_pin,
        blast_radius_policy=blast_policy,
        execution_context=exec_ctx,
        producer_version=producer_version,
        source_records=tuple(parsed_sources),
        steps=tuple(parsed_steps),
        run_end_logical_time=run_end,
        max_dispatch_attempts=max_attempts,
    )


# ============================================================================
# CONTIGUOUS PER-PARTITION OFFSET TRACKER (A1-R1 B1)
# ============================================================================


class PartitionOffsetTracker:
    """Tracks consumed offsets per assigned partition and computes contiguous commits.

    Rules enforced:
    - Tracks only actually observed consumed offsets (tolerates compacted gaps).
    - Advances strictly across the leading settled sequence, stopping at the
      first observed unsettled offset even if later offsets are settled.
    - Never commits below startup/confirmed committed position (zero rewind).
    - History rereads outside the assigned consumer never register here.
    - Revoked partitions immediately lose commit eligibility.
    """

    def __init__(
        self, initial_committed: Mapping[TopicPartition, int] | None = None
    ) -> None:
        self._confirmed_committed: dict[TopicPartition, int] = {}
        self._observed: dict[TopicPartition, dict[int, DeliverySettlementState]] = {}
        self._assigned: set[TopicPartition] = set()
        if initial_committed is not None:
            for tp, pos in initial_committed.items():
                self.register_partition(tp, committed_offset=pos)

    def register_partition(
        self, tp: TopicPartition, *, committed_offset: int = 0
    ) -> None:
        if not _is_strict_int(committed_offset) or committed_offset < 0:
            raise RuntimeConfigurationError(
                "committed_offset must be a non-negative integer"
            )
        self._assigned.add(tp)
        current = self._confirmed_committed.get(tp, 0)
        self._confirmed_committed[tp] = max(current, committed_offset)
        self._observed.setdefault(tp, {})

    def revoke_partitions(self, partitions: Sequence[TopicPartition]) -> None:
        for tp in partitions:
            self._assigned.discard(tp)

    @property
    def assigned_partitions(self) -> frozenset[TopicPartition]:
        return frozenset(self._assigned)

    def confirmed_committed(self, tp: TopicPartition) -> int:
        return self._confirmed_committed.get(tp, 0)

    def snapshot_confirmed_committed(self) -> dict[TopicPartition, int]:
        return dict(self._confirmed_committed)

    def observe(
        self,
        tp: TopicPartition,
        offset: int,
        *,
        state: DeliverySettlementState = DeliverySettlementState.BUFFERED_UNCOMMITTED,
    ) -> None:
        if not _is_strict_int(offset) or offset < 0:
            raise RuntimeConfigurationError("offset must be a non-negative integer")
        if tp not in self._observed:
            self._observed[tp] = {}
            self._confirmed_committed.setdefault(tp, 0)
        self._observed[tp][offset] = state

    def mark_settled(
        self, tp: TopicPartition, offset: int, state: DeliverySettlementState
    ) -> None:
        if state not in _SETTLED_STATES:
            raise RuntimeConfigurationError(f"State {state!r} is not a settled state")
        if tp not in self._observed or offset not in self._observed[tp]:
            return
        self._observed[tp][offset] = state

    def state_of(
        self, tp: TopicPartition, offset: int
    ) -> DeliverySettlementState | None:
        return self._observed.get(tp, {}).get(offset)

    def unsettled_count(self) -> int:
        total = 0
        for tp, entries in self._observed.items():
            floor = self._confirmed_committed.get(tp, 0)
            for off, st in entries.items():
                if off >= floor and st not in _SETTLED_STATES:
                    total += 1
        return total

    def buffered_offsets(self) -> tuple[tuple[TopicPartition, int], ...]:
        out: list[tuple[TopicPartition, int]] = []
        for tp, entries in self._observed.items():
            floor = self._confirmed_committed.get(tp, 0)
            for off in sorted(entries):
                if off >= floor and entries[off] not in _SETTLED_STATES:
                    out.append((tp, off))
        return tuple(out)

    def compute_committable_offsets(self) -> dict[TopicPartition, int]:
        """Return ``{tp: next_offset}`` strictly advancing past confirmed positions."""
        committable: dict[TopicPartition, int] = {}
        for tp in sorted(self._assigned, key=lambda p: (p.topic, p.partition)):
            start = self._confirmed_committed.get(tp, 0)
            target = start
            entries = self._observed.get(tp, {})
            for offset in sorted(entries):
                if offset < start:
                    continue
                if entries[offset] not in _SETTLED_STATES:
                    break
                target = offset + 1
            if target > start:
                committable[tp] = target
        return committable

    def record_committed(self, committed: Mapping[TopicPartition, int]) -> None:
        for tp, next_offset in committed.items():
            current = self._confirmed_committed.get(tp, 0)
            if next_offset < current:
                raise RuntimeOffsetCommitError(
                    "Attempted to rewind confirmed committed offset"
                )
            self._confirmed_committed[tp] = next_offset


# ============================================================================
# DIAGNOSTIC ARTIFACT TYPES
# ============================================================================


@dataclass(frozen=True)
class EvaluationTickRecord:
    """Observed evaluation step diagnostics (separating lateness vs grouping watermarks)."""

    step_index: int
    logical_time: datetime
    cohort_key: tuple[Any, ...]
    lateness_watermark: datetime
    cohort_grouping_watermark: datetime
    admitted_signal_ids: tuple[UUID, ...]
    late_rejected_signal_ids: tuple[UUID, ...]
    late_delayed_signal_ids: tuple[UUID, ...]
    grouping_result: CorrelationGroupingResult
    desired_state: CorrelationWindowState


@dataclass(frozen=True)
class RevisionExecutionRecord:
    """Observed per-revision persistence/replay reconciliation and dispatch outcome."""

    tenant_id: UUID
    incident_id: UUID
    revision: int
    revision_fingerprint: str
    persistence_disposition: PersistenceDisposition
    dispatch_outcome: DispatchOutcome | None
    logical_time: datetime


@dataclass(frozen=True)
class QuarantinedInputOutcome:
    """Typed diagnostic for a malformed Kafka record sent to QuarantineSink."""

    status: str
    topic: str
    partition: int
    offset: int
    failure_stage: str
    failure_type: str
    quarantine_record: QuarantineRecord


@dataclass(frozen=True)
class ShutdownReport:
    """Structured orderly shutdown summary with honest unresolved-send visibility."""

    state: CorrelationRuntimeState
    committed_offsets: dict[TopicPartition, int]
    unsettled_offset_count: int
    unresolved_publish_count: int
    closed_owned_consumer: bool
    closed_owned_history_reader: bool
    elapsed_seconds: float
    unresolved_operation_count: int = 0
    unclosed_owned_handle_count: int = 0


@dataclass(frozen=True)
class CorrelationRuntimeReadiness:
    """Structured readiness probe result across PostgreSQL, Kafka, and topology."""

    ready: bool
    status: str
    components: dict[str, Any]
    error_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CorrelationRuntimeRunResult:
    """Typed non-masking result of executing a bounded controlled runtime plan."""

    status: CorrelationRuntimeRunStatus
    plan_sha256: str
    run_id: UUID
    topology_snapshot_fingerprint: str
    evaluations: tuple[EvaluationTickRecord, ...]
    revisions: tuple[RevisionExecutionRecord, ...]
    dispatch_outcomes: tuple[DispatchOutcome, ...]
    quarantined_inputs: tuple[QuarantinedInputOutcome, ...]
    startup_committed_offsets: dict[TopicPartition, int]
    final_committed_offsets: dict[TopicPartition, int]
    unsettled_offsets: tuple[tuple[TopicPartition, int], ...]
    elapsed_wall_seconds: float
    unresolved_publish_count: int = 0
    phase5_activated: bool = False
    unresolved_operation_count: int = 0

    @property
    def all_settled(self) -> bool:
        return (
            self.status is CorrelationRuntimeRunStatus.COMPLETED
            and len(self.unsettled_offsets) == 0
            and self.unresolved_operation_count == 0
            and all(o.input_offset_committable for o in self.dispatch_outcomes)
        )


# ============================================================================
# PROTOCOL INTERFACES & READINESS PROBE
# ============================================================================


@runtime_checkable
class KafkaConsumerPort(Protocol):
    """Minimal async Kafka consumer surface satisfied by ``AIOKafkaConsumer``."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def assignment(self) -> set[TopicPartition]: ...
    async def committed(self, partition: TopicPartition) -> int | None: ...
    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[Any]]: ...
    async def commit(
        self, offsets: dict[TopicPartition, int] | None = None
    ) -> None: ...


PersistenceRepoFactory = Callable[[AsyncSession, UUID], IncidentPersistenceRepository]
VerifyRoleFn = Callable[[AsyncSession], Awaitable[None]]


async def _default_verify_db_readiness(session: AsyncSession) -> None:
    """Verify PostgreSQL connectivity, non-bypass role, READ COMMITTED, and dispatch grants."""
    res = await session.execute(text("SELECT 1"))
    if res.scalar_one_or_none() != 1:
        raise RuntimeStartupError("PostgreSQL readiness query failed")

    await verify_runtime_role_and_isolation(session)

    reg_res = await session.execute(
        text("SELECT to_regclass('public.incident_publication_dispatches')")
    )
    if reg_res.scalar_one_or_none() is None:
        raise RuntimeStartupError(
            "Required table 'incident_publication_dispatches' is not present"
        )

    priv_res = await session.execute(
        text(
            "SELECT "
            "has_table_privilege(current_user, 'public.incident_publication_dispatches', 'SELECT'), "
            "has_table_privilege(current_user, 'public.incident_publication_dispatches', 'INSERT'), "
            "has_column_privilege(current_user, 'public.incident_publication_dispatches', 'dispatch_state', 'UPDATE'), "
            "has_table_privilege(current_user, 'public.incident_publication_dispatches', 'DELETE')"
        )
    )
    priv_row = priv_res.first()
    if priv_row is None:
        raise RuntimeStartupError(
            "Unable to inspect runtime privileges on incident_publication_dispatches"
        )
    can_select, can_insert, can_update_state, can_delete = (
        bool(priv_row[0]),
        bool(priv_row[1]),
        bool(priv_row[2]),
        bool(priv_row[3]),
    )
    if not (can_select and can_insert and can_update_state) or can_delete:
        raise RuntimeStartupError(
            "Runtime database role does not satisfy least-privilege dispatch grants"
        )


def _verify_pinned_topology(
    snapshot: TopologySnapshot,
    expected: TopologySnapshotReference,
) -> None:
    if not isinstance(snapshot, TopologySnapshot):
        raise RuntimeStartupError("Topology provider returned invalid snapshot type")
    if (
        snapshot.version.topology_id != expected.topology_id
        or snapshot.version.topology_version != expected.topology_version
        or snapshot.snapshot_fingerprint != expected.snapshot_fingerprint
    ):
        raise RuntimeStartupError(
            "Fetched topology snapshot does not match pinned TopologySnapshotReference"
        )


async def check_correlation_runtime_readiness(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    topology_provider: TopologyProvider,
    expected_topology: TopologySnapshotReference,
    bootstrap_servers: str | None = None,
    kafka_consumer_factory: Callable[..., Any] | None = None,
    verify_db_fn: VerifyRoleFn | None = None,
    timeout_seconds: float = 5.0,
) -> CorrelationRuntimeReadiness:
    """Non-destructive readiness probe across PostgreSQL, Kafka, and pinned topology.

    Does not modify existing ``/ready`` or ``/ready/telemetry`` endpoints and
    does not activate any consumer loop or Phase 5 workflow.
    """
    if not _is_finite_number(timeout_seconds) or not (
        0.1 <= float(timeout_seconds) <= 60.0
    ):
        raise RuntimeConfigurationError(
            "timeout_seconds must be a finite number in 0.1..60.0"
        )

    errors: list[str] = []
    db_ready = False
    kafka_ready = False
    topology_ready = False
    observed_topics: tuple[str, ...] = ()
    observed_topo_fp: str | None = None

    # 1. PostgreSQL readiness + role/isolation/table privileges
    db_check = (
        verify_db_fn if verify_db_fn is not None else _default_verify_db_readiness
    )
    try:

        async def _probe_db() -> None:
            async with session_factory() as session:
                async with session.begin():
                    await db_check(session)

        await asyncio.wait_for(_probe_db(), timeout=float(timeout_seconds))
        db_ready = True
    except asyncio.CancelledError:
        raise
    except Exception:
        errors.append("POSTGRES_NOT_READY")

    # 2. Kafka connectivity + both canonical Phase 4 topics present
    required_topics = {
        KafkaTopic.ANOMALIES.value,
        KafkaTopic.INCIDENTS.value,
    }
    try:

        async def _probe_kafka() -> tuple[bool, tuple[str, ...]]:
            if kafka_consumer_factory is not None:
                probe_consumer = kafka_consumer_factory(
                    bootstrap_servers=bootstrap_servers or "localhost:9092",
                    request_timeout_ms=int(float(timeout_seconds) * 1000),
                    enable_auto_commit=False,
                )
            else:
                if not bootstrap_servers:
                    return False, ()
                probe_consumer = aiokafka.AIOKafkaConsumer(
                    bootstrap_servers=bootstrap_servers,
                    request_timeout_ms=int(float(timeout_seconds) * 1000),
                    enable_auto_commit=False,
                )
            try:
                await probe_consumer.start()
                topics = await probe_consumer.topics()
                topic_set = set(topics)
                present = tuple(sorted(required_topics.intersection(topic_set)))
                return required_topics.issubset(topic_set), present
            finally:
                try:
                    await probe_consumer.stop()
                except Exception:
                    pass

        kafka_ready, observed_topics = await asyncio.wait_for(
            _probe_kafka(), timeout=float(timeout_seconds)
        )
        if not kafka_ready:
            errors.append("KAFKA_TOPICS_MISSING")
    except asyncio.CancelledError:
        raise
    except Exception:
        errors.append("KAFKA_NOT_READY")

    # 3. Pinned topology fetch & exact verification
    try:
        snap = await asyncio.wait_for(
            topology_provider.get_snapshot(
                expected_version=expected_topology.topology_version
            ),
            timeout=float(timeout_seconds),
        )
        _verify_pinned_topology(snap, expected_topology)
        topology_ready = True
        observed_topo_fp = snap.snapshot_fingerprint
    except asyncio.CancelledError:
        raise
    except Exception:
        errors.append("TOPOLOGY_PIN_UNAVAILABLE_OR_MISMATCH")

    all_ready = db_ready and kafka_ready and topology_ready
    components: dict[str, Any] = {
        "postgresql": {"ready": db_ready},
        "kafka": {
            "ready": kafka_ready,
            "required_topics": sorted(required_topics),
            "present_required_topics": list(observed_topics),
        },
        "topology": {
            "ready": topology_ready,
            "expected_fingerprint": expected_topology.snapshot_fingerprint,
            "observed_fingerprint": observed_topo_fp,
        },
    }
    return CorrelationRuntimeReadiness(
        ready=all_ready,
        status="ready" if all_ready else "not_ready",
        components=components,
        error_codes=tuple(errors),
    )


# ============================================================================
# INTERNAL COHORT & AUTHENTICATED DELIVERY STATE
# ============================================================================


@dataclass(frozen=True)
class _AuthenticatedDelivery:
    planned: PlannedSourceRecord
    correlation_input: AnomalyCorrelationInput
    from_history_reader: bool


@dataclass
class _CohortState:
    window_key: tuple[Any, ...]
    tenant_id: UUID
    environment: str
    bucket_index: int
    observed_inputs: dict[UUID, AnomalyCorrelationInput] = field(default_factory=dict)
    observed_ingest_times: dict[UUID, datetime] = field(default_factory=dict)
    deliveries_by_signal: dict[UUID, list[_AuthenticatedDelivery]] = field(
        default_factory=dict
    )
    admitted: dict[UUID, AnomalyCorrelationInput] = field(default_factory=dict)
    classified_excluded: dict[UUID, GroupingSignalDisposition] = field(
        default_factory=dict
    )
    last_accepted_ingest_time: datetime | None = None
    grouping_watermark: datetime | None = None


@dataclass
class _IncidentPrefixTrack:
    reconstructed_aggregates: dict[int, IncidentAggregate] = field(default_factory=dict)
    reconstructed_bytes: dict[int, bytes] = field(default_factory=dict)
    latest_reconstructed: IncidentAggregate | None = None
    observed_durable_head: int = 0
    acknowledged_revisions: set[int] = field(default_factory=set)
    dependent_deliveries: list[_AuthenticatedDelivery] = field(default_factory=list)
    member_signal_ids: set[UUID] = field(default_factory=set)


class _CoordinatorRebalanceListener(aiokafka.ConsumerRebalanceListener):
    """aiokafka rebalance listener bound to coordinator-owned consumers."""

    def __init__(self, coordinator: CorrelationRuntimeCoordinator) -> None:
        self._coordinator = coordinator

    def on_partitions_revoked(self, revoked: Any) -> None:
        self._coordinator.on_partitions_revoked(tuple(revoked))

    def on_partitions_assigned(self, assigned: Any) -> None:
        self._coordinator.on_partitions_assigned(tuple(assigned))


# ============================================================================
# CONTROLLED RUNTIME COORDINATOR
# ============================================================================


class CorrelationRuntimeCoordinator:
    """Bounded controlled runtime coordinator composing Tasks 4.5â€“4.10 and D1.

    Ownership rules:
    - Caller-supplied ``session_factory``, ``dispatch_service``,
      ``topology_provider``, ``quarantine_sink``, ``consumer``, and
      ``history_reader`` are NEVER closed by this coordinator.
    - Handles created internally through ``consumer_factory`` or
      ``history_reader_factory`` ARE started and closed by this coordinator.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        dispatch_service: IncidentDispatchService,
        topology_provider: TopologyProvider,
        expected_topology: TopologySnapshotReference,
        config: CorrelationRuntimeConfig | None = None,
        consumer: Any | None = None,
        consumer_factory: Callable[..., Any] | None = None,
        history_reader: Any | None = None,
        history_reader_factory: Callable[..., Any] | None = None,
        bootstrap_servers: str | None = None,
        quarantine_sink: QuarantineSink | None = None,
        dedupe_store: AnomalyDedupeStore | None = None,
        persistence_repo_factory: PersistenceRepoFactory | None = None,
        verify_role_fn: VerifyRoleFn | None = None,
    ) -> None:
        if session_factory is None or not callable(session_factory):
            raise RuntimeConfigurationError("session_factory must be callable")
        if not isinstance(dispatch_service, IncidentDispatchService):
            raise RuntimeConfigurationError(
                "dispatch_service must be an IncidentDispatchService instance"
            )
        if not isinstance(topology_provider, TopologyProvider):
            raise RuntimeConfigurationError(
                "topology_provider must implement TopologyProvider"
            )
        if not isinstance(expected_topology, TopologySnapshotReference):
            raise RuntimeConfigurationError(
                "expected_topology must be a TopologySnapshotReference"
            )

        effective_config = config if config is not None else CorrelationRuntimeConfig()
        if not isinstance(effective_config, CorrelationRuntimeConfig):
            raise RuntimeConfigurationError(
                "config must be a CorrelationRuntimeConfig instance"
            )

        if consumer is not None and consumer_factory is not None:
            raise RuntimeConfigurationError(
                "Specify at most one of consumer or consumer_factory"
            )
        if history_reader is not None and history_reader_factory is not None:
            raise RuntimeConfigurationError(
                "Specify at most one of history_reader or history_reader_factory"
            )

        self._session_factory = session_factory
        self._dispatch_service = dispatch_service
        self._topology_provider = topology_provider
        self._expected_topology = expected_topology
        self._config = effective_config

        self._caller_consumer = consumer
        self._consumer_factory = consumer_factory
        self._caller_history_reader = history_reader
        self._history_reader_factory = history_reader_factory
        self._bootstrap_servers = bootstrap_servers
        self._quarantine_sink = quarantine_sink
        self._dedupe_store: AnomalyDedupeStore = (
            dedupe_store
            if dedupe_store is not None
            else InMemoryAnomalyDedupeStore(
                max_size=max(
                    effective_config.max_input_records,
                    DEFAULT_INTAKE_DEDUPE_MAX_SIZE,
                )
            )
        )
        self._persistence_repo_factory: PersistenceRepoFactory = (
            persistence_repo_factory
            if persistence_repo_factory is not None
            else SqlAlchemyIncidentPersistenceRepository
        )
        self._verify_role_fn: VerifyRoleFn = (
            verify_role_fn
            if verify_role_fn is not None
            else verify_runtime_role_and_isolation
        )

        self._state: CorrelationRuntimeState = CorrelationRuntimeState.CREATED
        self._pinned_snapshot: TopologySnapshot | None = None
        self._active_consumer: Any | None = None
        self._owns_consumer: bool = False
        self._active_history_reader: Any | None = None
        self._owns_history_reader: bool = False
        self._pending_operations: set[asyncio.Future[Any]] = set()
        self._cleanup_tasks: dict[str, asyncio.Task[Any]] = {}

        self._shutdown_requested: bool = False
        self._rebalance_revoked: set[TopicPartition] = set()
        self._rebalance_generation: int = 0
        self._run_rebalance_generation: int | None = None
        self._rebalance_listener = _CoordinatorRebalanceListener(self)
        self._offset_tracker: PartitionOffsetTracker | None = None
        self._publish_semaphore = asyncio.Semaphore(
            effective_config.max_in_flight_publications
        )

    @property
    def state(self) -> CorrelationRuntimeState:
        return self._state

    @property
    def pending_operation_count(self) -> int:
        return sum(not task.done() for task in self._pending_operations)

    @property
    def unclosed_owned_handle_count(self) -> int:
        return int(self._owns_consumer) + int(self._owns_history_reader)

    async def _await_before_deadline(
        self, coro: Awaitable[_T], *, deadline: float
    ) -> _T:
        return await _await_before_deadline(
            coro, deadline=deadline, pending=self._pending_operations
        )

    @property
    def config(self) -> CorrelationRuntimeConfig:
        return self._config

    @property
    def pinned_snapshot(self) -> TopologySnapshot | None:
        return self._pinned_snapshot

    @property
    def dedupe_store(self) -> AnomalyDedupeStore:
        return self._dedupe_store

    @property
    def rebalance_listener(self) -> aiokafka.ConsumerRebalanceListener:
        """ConsumerRebalanceListener bound to this coordinator's rebalance fencing."""
        return self._rebalance_listener

    def request_shutdown(self) -> None:
        """Signal the coordinator to stop polling cleanly without implicit evaluation."""
        self._shutdown_requested = True

    def on_partitions_revoked(self, revoked: Sequence[TopicPartition]) -> None:
        """Rebalance callback cancelling commit ownership for revoked partitions."""
        self._rebalance_generation += 1
        for tp in revoked:
            self._rebalance_revoked.add(tp)
        if self._offset_tracker is not None:
            self._offset_tracker.revoke_partitions(revoked)

    def on_partitions_assigned(self, assigned: Sequence[TopicPartition]) -> None:
        """Rebalance callback tracking assignment epochs (reassignment during a run fences commits)."""
        self._rebalance_generation += 1

    async def start(self, *, _deadline: float | None = None) -> TopologySnapshot:
        """Initialize pinned topology and owned Kafka handles (idempotent when READY)."""
        if self._state is CorrelationRuntimeState.READY:
            assert self._pinned_snapshot is not None
            return self._pinned_snapshot
        if self._state is not CorrelationRuntimeState.CREATED:
            raise RuntimeStartupError(
                f"Cannot start coordinator from state '{self._state.value}'"
            )

        self._state = CorrelationRuntimeState.STARTING
        try:
            topo_coro = self._topology_provider.get_snapshot(
                expected_version=self._expected_topology.topology_version
            )
            snapshot = (
                await self._await_before_deadline(topo_coro, deadline=_deadline)
                if _deadline is not None
                else await topo_coro
            )
            _verify_pinned_topology(snapshot, self._expected_topology)
            self._pinned_snapshot = snapshot

            if self._caller_consumer is not None:
                self._active_consumer = self._caller_consumer
                self._owns_consumer = False
            elif self._consumer_factory is not None:
                self._active_consumer = self._consumer_factory(
                    KafkaTopic.ANOMALIES.value,
                    bootstrap_servers=self._bootstrap_servers or "localhost:9092",
                    group_id=self._config.consumer_group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                # Mark ownership BEFORE subscribe/start so partial-start failures always invoke stop().
                self._owns_consumer = True
                if hasattr(self._active_consumer, "subscribe") and callable(
                    self._active_consumer.subscribe
                ):
                    self._active_consumer.subscribe(
                        topics=[KafkaTopic.ANOMALIES.value],
                        listener=self._rebalance_listener,
                    )
                start_coro = self._active_consumer.start()
                if _deadline is not None:
                    await self._await_before_deadline(start_coro, deadline=_deadline)
                else:
                    await start_coro
            elif self._bootstrap_servers is not None:
                self._active_consumer = aiokafka.AIOKafkaConsumer(
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._config.consumer_group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                # Mark ownership BEFORE subscribe/start so partial-start failures always invoke stop().
                self._owns_consumer = True
                self._active_consumer.subscribe(
                    topics=[KafkaTopic.ANOMALIES.value],
                    listener=self._rebalance_listener,
                )
                start_coro = self._active_consumer.start()
                if _deadline is not None:
                    await self._await_before_deadline(start_coro, deadline=_deadline)
                else:
                    await start_coro
            else:
                raise RuntimeStartupError(
                    "Either consumer, consumer_factory, or bootstrap_servers is required"
                )

            if self._caller_history_reader is not None:
                self._active_history_reader = self._caller_history_reader
                self._owns_history_reader = False
            elif self._history_reader_factory is not None:
                self._active_history_reader = self._history_reader_factory(
                    bootstrap_servers=self._bootstrap_servers or "localhost:9092",
                    group_id=None,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                # Mark ownership BEFORE start so partial-start failures always invoke stop().
                self._owns_history_reader = True
                hist_start_coro = self._active_history_reader.start()
                if _deadline is not None:
                    await self._await_before_deadline(
                        hist_start_coro, deadline=_deadline
                    )
                else:
                    await hist_start_coro

            self._state = CorrelationRuntimeState.READY
            return snapshot
        except asyncio.CancelledError:
            self._state = CorrelationRuntimeState.FAILED
            cleanup_deadline = time.monotonic() + float(
                self._config.shutdown_grace_seconds
            )
            try:
                await asyncio.shield(
                    self._close_owned_handles_best_effort(deadline=cleanup_deadline)
                )
            except Exception:
                pass
            raise
        except RuntimeStartupError:
            self._state = CorrelationRuntimeState.FAILED
            await self._close_owned_handles_best_effort(
                deadline=time.monotonic() + float(self._config.shutdown_grace_seconds)
            )
            raise
        except Exception as exc:
            self._state = CorrelationRuntimeState.FAILED
            await self._close_owned_handles_best_effort(
                deadline=time.monotonic() + float(self._config.shutdown_grace_seconds)
            )
            err = RuntimeStartupError(
                "Correlation runtime startup failed during topology pinning or consumer initialization"
            )
            err.__cause__ = exc
            raise err from exc

    async def _close_owned_handles_best_effort(
        self, *, deadline: float | None = None
    ) -> tuple[bool, bool]:
        """Start or resume owned cleanup, retaining ownership until stop succeeds."""
        effective_deadline = (
            deadline
            if deadline is not None
            else time.monotonic() + float(self._config.shutdown_grace_seconds)
        )
        tasks: dict[str, asyncio.Task[Any]] = {}

        def mark_closed(resource: str, target: Any) -> None:
            if resource == "consumer" and self._active_consumer is target:
                self._active_consumer = None
                self._owns_consumer = False
            elif resource == "history" and self._active_history_reader is target:
                self._active_history_reader = None
                self._owns_history_reader = False

        for name, handle, owned in (
            ("consumer", self._active_consumer, self._owns_consumer),
            ("history", self._active_history_reader, self._owns_history_reader),
        ):
            if not owned or handle is None:
                continue
            task = self._cleanup_tasks.get(name)
            if (
                task is not None
                and task.done()
                and not task.cancelled()
                and task.exception() is None
            ):
                # Reconcile success even if its completion callback is queued.
                mark_closed(name, handle)
                tasks[name] = task
                continue
            if task is None or task.done():
                task = asyncio.create_task(handle.stop())
                self._cleanup_tasks[name] = task
                _retain_runtime_operation(task, self._pending_operations)

                def settled(
                    done: asyncio.Task[Any],
                    *,
                    resource: str = name,
                    target: Any = handle,
                ) -> None:
                    if self._cleanup_tasks.get(resource) is done:
                        del self._cleanup_tasks[resource]
                    if done.cancelled() or done.exception() is not None:
                        return  # Failed stop remains owned and can be retried.
                    mark_closed(resource, target)

                task.add_done_callback(settled)
            tasks[name] = task

        remaining = effective_deadline - time.monotonic()
        if tasks and remaining > 0:
            # A stop that outlives grace remains owned, running and observable.
            # Cancelling it could strand a partially closed transport.
            await asyncio.wait(tasks.values(), timeout=remaining)

        def succeeded(name: str) -> bool:
            task = tasks.get(name)
            return (
                task is not None
                and task.done()
                and not task.cancelled()
                and task.exception() is None
            )

        return succeeded("consumer"), succeeded("history")

    async def shutdown(self) -> ShutdownReport:
        """Orderly bounded shutdown sharing one ``shutdown_grace_seconds`` budget across all phases.

        Never performs implicit shutdown window evaluations and never closes
        caller-owned resources.
        """
        t0 = time.monotonic()
        shutdown_deadline = t0 + float(self._config.shutdown_grace_seconds)
        self._shutdown_requested = True
        already_stopped = self._state is CorrelationRuntimeState.STOPPED
        self._state = CorrelationRuntimeState.STOPPING
        commit_error: RuntimeOffsetCommitError | None = None

        try:
            if (
                not already_stopped
                and self._offset_tracker is not None
                and self._active_consumer is not None
                and self._offset_tracker.compute_committable_offsets()
            ):
                finished, _, exc = await _await_task_bounded(
                    self._commit_settled_offsets(
                        self._offset_tracker, deadline=shutdown_deadline
                    ),
                    deadline=shutdown_deadline,
                    pending=self._pending_operations,
                )
                if not finished:
                    err = RuntimeOffsetCommitError(
                        "Offset commit timed out during runtime shutdown"
                    )
                    if exc is not None:
                        err.__cause__ = exc
                    commit_error = err
                elif exc is not None:
                    if isinstance(exc, RuntimeOffsetCommitError):
                        commit_error = exc
                    elif isinstance(exc, RuntimeRebalanceError):
                        pass
                    elif isinstance(exc, TimeoutError):
                        err = RuntimeOffsetCommitError(
                            "Offset commit timed out during runtime shutdown"
                        )
                        err.__cause__ = exc
                        commit_error = err
                    else:
                        err = RuntimeOffsetCommitError(
                            "Offset commit failed during runtime shutdown"
                        )
                        err.__cause__ = exc
                        commit_error = err

        except asyncio.CancelledError:
            await self._close_owned_handles_best_effort(deadline=time.monotonic())
            self._state = CorrelationRuntimeState.STOPPED
            raise

        closed_consumer, closed_history = await self._close_owned_handles_best_effort(
            deadline=shutdown_deadline
        )
        self._state = CorrelationRuntimeState.STOPPED

        if commit_error is not None:
            raise commit_error

        return ShutdownReport(
            state=self._state,
            committed_offsets=(
                self._offset_tracker.snapshot_confirmed_committed()
                if self._offset_tracker is not None
                else {}
            ),
            unsettled_offset_count=(
                self._offset_tracker.unsettled_count()
                if self._offset_tracker is not None
                else 0
            ),
            unresolved_publish_count=self._dispatch_service.pending_publish_count,
            closed_owned_consumer=closed_consumer,
            closed_owned_history_reader=closed_history,
            elapsed_seconds=time.monotonic() - t0,
            unresolved_operation_count=self.pending_operation_count,
            unclosed_owned_handle_count=self.unclosed_owned_handle_count,
        )

    async def __aenter__(self) -> CorrelationRuntimeCoordinator:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if exc_type is asyncio.CancelledError:
            self._shutdown_requested = True
            await self._close_owned_handles_best_effort(
                deadline=time.monotonic() + float(self._config.shutdown_grace_seconds)
            )
            self._state = CorrelationRuntimeState.STOPPED
            return
        await self.shutdown()

    # ------------------------------------------------------------------------
    # RECORD DECODING, QUARANTINE & AUTHENTICATION
    # ------------------------------------------------------------------------

    def _decode_anomaly_record(
        self, record: Any
    ) -> tuple[AnomalyCorrelationInput, TelemetryEnvelope, SourceRecordDigests]:
        """Decode and validate one Kafka record through frozen Task 4.5 contracts."""
        digests = compute_source_record_digests(record)
        event: TelemetryEvent = deserialize_event(record.value)
        context, producer_version = parse_kafka_headers(record.headers)

        expected_topic = EVENT_TYPE_TO_TOPIC[event.event_type].value
        if record.topic != expected_topic or record.topic != KafkaTopic.ANOMALIES.value:
            raise TopicEventTypeMismatchError(
                f"Record topic '{record.topic}' does not match expected anomaly topic '{KafkaTopic.ANOMALIES.value}'"
            )
        if event.event_type is not EventType.ANOMALY:
            raise TopicEventTypeMismatchError(
                f"Event type '{event.event_type.value}' is not allowed on anomaly intake"
            )

        kafka_ts = record.timestamp if record.timestamp is not None else 0
        envelope = TelemetryEnvelope(
            event=event,
            context=context,
            kafka_timestamp_ms=kafka_ts,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            consumed_at=datetime.now(timezone.utc),
            producer_version=producer_version,
        )
        signal = anomaly_signal_from_envelope(envelope)
        signal_copy = signal.model_copy(deep=True)
        lineage = build_anomaly_reproducibility_lineage(signal_copy)

        if len(signal_copy.source_event_ids) > MAX_SIGNAL_REFS:
            raise AnomalyEvidenceLimitError("Source event count exceeds maximum")
        if len(set(signal_copy.source_event_ids)) != len(signal_copy.source_event_ids):
            raise AnomalyEvidenceLimitError("Source event identities must be unique")
        if len(signal_copy.evidence) > MAX_EVIDENCE_ENTRIES:
            raise AnomalyEvidenceLimitError("Evidence count exceeds maximum")
        evidence_ids = [item.evidence_id for item in signal_copy.evidence]
        if len(set(evidence_ids)) != len(evidence_ids):
            raise AnomalyEvidenceLimitError("Evidence identities must be unique")

        return (
            AnomalyCorrelationInput(signal=signal_copy, lineage=lineage),
            envelope,
            digests,
        )

    def _authenticate_planned_record(
        self,
        record: Any,
        planned: PlannedSourceRecord,
        *,
        from_history_reader: bool,
    ) -> _AuthenticatedDelivery:
        """Verify exact key/value/headers digests and decoded signal against the pinned plan."""
        ptr = f"/source_records/{planned.index}"
        try:
            digests = compute_source_record_digests(record)
        except Exception as exc:
            raise RuntimeRecoveryInputError(
                "Pinned source record headers failed validation",
                pointer=f"{ptr}/headers_sha256",
            ) from exc

        if digests.key_sha256 != planned.key_sha256:
            raise RuntimeRecoveryInputError(
                "Authenticated source key_sha256 differs from plan",
                pointer=f"{ptr}/key_sha256",
            )
        if digests.value_sha256 != planned.value_sha256:
            raise RuntimeRecoveryInputError(
                "Authenticated source value_sha256 differs from plan",
                pointer=f"{ptr}/value_sha256",
            )
        if digests.headers_sha256 != planned.headers_sha256:
            raise RuntimeRecoveryInputError(
                "Authenticated source headers_sha256 differs from plan",
                pointer=f"{ptr}/headers_sha256",
            )

        try:
            corr_input, _, _ = self._decode_anomaly_record(record)
        except Exception as exc:
            raise RuntimeRecoveryInputError(
                "Pinned source record failed canonical anomaly decoding",
                pointer=ptr,
            ) from exc

        if corr_input.signal.signal_id != planned.signal_id:
            raise RuntimeRecoveryInputError(
                "Authenticated source signal_id differs from plan",
                pointer=f"{ptr}/signal_id",
            )
        if corr_input.semantic_fingerprint != planned.semantic_fingerprint:
            raise RuntimeRecoveryInputError(
                "Authenticated source semantic_fingerprint differs from plan",
                pointer=f"{ptr}/semantic_fingerprint",
            )

        return _AuthenticatedDelivery(
            planned=planned,
            correlation_input=corr_input,
            from_history_reader=from_history_reader,
        )

    async def _handle_unplanned_malformed_record(
        self,
        record: Any,
        exc: Exception,
        *,
        tracker: PartitionOffsetTracker,
        quarantined_out: list[QuarantinedInputOutcome],
    ) -> None:
        """Quarantine malformed unplanned input if sink is configured; otherwise fail closed."""
        failure_stage = "deserialization"
        if isinstance(
            exc,
            (MissingRequiredHeaderError, DuplicateHeaderError, InvalidHeaderError),
        ):
            failure_stage = "header_validation"
        elif isinstance(exc, TopicEventTypeMismatchError):
            failure_stage = "routing_validation"
        elif isinstance(
            exc,
            (
                AnomalyHandoffError,
                AnomalyEvidenceLimitError,
                AnomalyIdentityConflictError,
            ),
        ):
            failure_stage = "anomaly_validation"

        if self._quarantine_sink is None:
            err = RuntimeInputValidationError(
                f"Malformed anomaly record at {record.topic}[{record.partition}]@{record.offset} "
                f"rejected ({failure_stage}: {type(exc).__name__})"
            )
            err.__cause__ = exc
            raise err from exc

        kafka_ts = record.timestamp if record.timestamp is not None else 0
        q_record = QuarantineRecord(
            failure_stage=failure_stage,
            failure_type=type(exc).__name__,
            failure_message_sanitized=sanitize_failure_message(str(exc)),
            topic=str(record.topic),
            partition=int(record.partition),
            offset=int(record.offset),
            kafka_timestamp_ms=int(kafka_ts),
            raw_key_base64=encode_bytes(record.key),
            raw_value_base64=encode_bytes(record.value) or "",
            headers=encode_headers(record.headers),
            consumer_group=self._config.consumer_group_id,
        )
        await self._quarantine_sink.quarantine(q_record)
        tp = TopicPartition(record.topic, record.partition)
        tracker.observe(
            tp,
            int(record.offset),
            state=DeliverySettlementState.SETTLED_QUARANTINED,
        )
        quarantined_out.append(
            QuarantinedInputOutcome(
                status="QUARANTINED_INVALID_INPUT",
                topic=str(record.topic),
                partition=int(record.partition),
                offset=int(record.offset),
                failure_stage=failure_stage,
                failure_type=type(exc).__name__,
                quarantine_record=q_record,
            )
        )

    # ------------------------------------------------------------------------
    # BOUNDED HISTORY RETRIEVAL & ASSIGNED CONSUMER POLLING
    # ------------------------------------------------------------------------

    async def _fetch_history_coordinates(
        self,
        needed: Sequence[PlannedSourceRecord],
        *,
        deadline: float,
    ) -> dict[str, _AuthenticatedDelivery]:
        """Fetch already-committed historical coordinates without any group commits."""
        if not needed:
            return {}

        reader = self._active_history_reader
        created_ephemeral = False
        if reader is None:
            if self._history_reader_factory is not None:
                reader = self._history_reader_factory(
                    bootstrap_servers=self._bootstrap_servers or "localhost:9092",
                    group_id=None,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                created_ephemeral = True
            elif self._bootstrap_servers is not None:
                reader = aiokafka.AIOKafkaConsumer(
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=None,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                created_ephemeral = True
            else:
                raise RuntimeRecoveryInputError(
                    "History reader is required to authenticate already-committed source coordinates",
                    pointer=f"/source_records/{needed[0].index}",
                )

        fetched: dict[str, _AuthenticatedDelivery] = {}
        try:
            if created_ephemeral:
                await self._await_before_deadline(reader.start(), deadline=deadline)
            by_tp: dict[TopicPartition, list[PlannedSourceRecord]] = {}
            for rec in needed:
                by_tp.setdefault(rec.topic_partition, []).append(rec)

            for tp, targets in by_tp.items():
                targets_sorted = sorted(targets, key=lambda r: r.offset)
                min_offset = targets_sorted[0].offset
                max_offset = targets_sorted[-1].offset
                target_by_offset = {r.offset: r for r in targets_sorted}

                if hasattr(reader, "assign") and callable(reader.assign):
                    reader.assign([tp])

                if hasattr(reader, "beginning_offsets") and callable(
                    reader.beginning_offsets
                ):
                    beg_map = await self._await_before_deadline(
                        reader.beginning_offsets([tp]), deadline=deadline
                    )
                    beg = beg_map.get(tp, 0)
                    for t in targets_sorted:
                        if t.offset < beg:
                            raise RuntimeRecoveryInputError(
                                "Retained Kafka history record is no longer available (below beginning offset)",
                                pointer=f"/source_records/{t.index}",
                            )

                if hasattr(reader, "end_offsets") and callable(reader.end_offsets):
                    end_map = await self._await_before_deadline(
                        reader.end_offsets([tp]), deadline=deadline
                    )
                    end = end_map.get(tp)
                    if end is not None:
                        for t in targets_sorted:
                            if t.offset >= end:
                                raise RuntimeRecoveryInputError(
                                    "Retained Kafka history record is unavailable (beyond end offset)",
                                    pointer=f"/source_records/{t.index}",
                                )

                if hasattr(reader, "seek") and callable(reader.seek):
                    reader.seek(tp, min_offset)

                empty_polls = 0
                remaining = set(target_by_offset.keys())
                while remaining:
                    if time.monotonic() >= deadline:
                        first_missing = target_by_offset[min(remaining)]
                        raise RuntimeRecoveryInputError(
                            "Timed out fetching retained Kafka history record",
                            pointer=f"/source_records/{first_missing.index}",
                        )
                    poll_ms = min(
                        self._config.poll_timeout_ms,
                        max(10, int((deadline - time.monotonic()) * 1000)),
                    )
                    try:
                        batch = await self._await_before_deadline(
                            reader.getmany(
                                tp,
                                timeout_ms=poll_ms,
                                max_records=max(1, len(remaining) + 16),
                            ),
                            deadline=deadline,
                        )
                    except TimeoutError as exc:
                        first_missing = target_by_offset[min(remaining)]
                        raise RuntimeRecoveryInputError(
                            "Timed out fetching retained Kafka history record",
                            pointer=f"/source_records/{first_missing.index}",
                        ) from exc
                    records = batch.get(tp, [])
                    if not records:
                        empty_polls += 1
                        if empty_polls >= 3:
                            first_missing = target_by_offset[min(remaining)]
                            raise RuntimeRecoveryInputError(
                                "Retained Kafka history record unavailable",
                                pointer=f"/source_records/{first_missing.index}",
                            )
                        continue

                    empty_polls = 0
                    for raw_rec in records:
                        off = int(raw_rec.offset)
                        if off in remaining:
                            planned = target_by_offset[off]
                            fetched[planned.delivery_id] = (
                                self._authenticate_planned_record(
                                    raw_rec, planned, from_history_reader=True
                                )
                            )
                            remaining.remove(off)
                        if off > max_offset:
                            break
                    if records and int(records[-1].offset) >= max_offset and remaining:
                        first_missing = target_by_offset[min(remaining)]
                        raise RuntimeRecoveryInputError(
                            "Retained Kafka history record missing before observed higher offset",
                            pointer=f"/source_records/{first_missing.index}",
                        )
        finally:
            if created_ephemeral:
                await _await_task_bounded(
                    reader.stop(),
                    pending=self._pending_operations,
                    deadline=time.monotonic()
                    + float(self._config.shutdown_grace_seconds),
                )

        return fetched

    async def _poll_until_delivery_available(
        self,
        *,
        target: PlannedSourceRecord,
        plan_by_coord: Mapping[tuple[str, int, int], PlannedSourceRecord],
        fetched: dict[str, _AuthenticatedDelivery],
        tracker: PartitionOffsetTracker,
        quarantined_out: list[QuarantinedInputOutcome],
        deadline: float,
    ) -> bool:
        """Poll assigned consumer until ``target.delivery_id`` is authenticated.

        Returns ``False`` if bounded run deadline or explicit shutdown stops polling
        before the target arrives; raises ``RuntimeRecoveryInputError`` if the
        target is provably missing from retained Kafka input.
        """
        assert self._active_consumer is not None
        consumer = self._active_consumer

        # Fast-fail check against partition bounds if the consumer exposes them
        if hasattr(consumer, "beginning_offsets") and callable(
            consumer.beginning_offsets
        ):
            try:
                beg_map = await self._await_before_deadline(
                    consumer.beginning_offsets([target.topic_partition]),
                    deadline=deadline,
                )
            except TimeoutError:
                return False
            if target.offset < beg_map.get(target.topic_partition, 0):
                raise RuntimeRecoveryInputError(
                    "Planned source coordinate is below partition beginning offset",
                    pointer=f"/source_records/{target.index}",
                )
        if hasattr(consumer, "end_offsets") and callable(consumer.end_offsets):
            try:
                end_map = await self._await_before_deadline(
                    consumer.end_offsets([target.topic_partition]),
                    deadline=deadline,
                )
            except TimeoutError:
                return False
            end_off = end_map.get(target.topic_partition)
            if end_off is not None and target.offset >= end_off:
                raise RuntimeRecoveryInputError(
                    "Planned source coordinate is unavailable (beyond partition end offset)",
                    pointer=f"/source_records/{target.index}",
                )

        empty_polls = 0
        while target.delivery_id not in fetched:
            if self._shutdown_requested:
                return False
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                return False

            self._assert_assignment_intact(tracker)

            poll_ms = min(
                self._config.poll_timeout_ms,
                max(10, int(remaining_s * 1000)),
            )
            try:
                batch = await self._await_before_deadline(
                    consumer.getmany(
                        timeout_ms=poll_ms,
                        max_records=max(1, self._config.max_buffered_records),
                    ),
                    deadline=deadline,
                )
            except TimeoutError:
                return False
            self._assert_assignment_intact(tracker)

            any_record = False
            for tp, records in batch.items():
                for raw_rec in records:
                    any_record = True
                    coord = (
                        str(raw_rec.topic),
                        int(raw_rec.partition),
                        int(raw_rec.offset),
                    )
                    planned = plan_by_coord.get(coord)
                    if planned is not None:
                        if (
                            tracker.unsettled_count()
                            >= self._config.max_buffered_records
                        ):
                            raise RuntimeCapacityError(
                                f"Buffered unsettled records exceed max_buffered_records ({self._config.max_buffered_records})"
                            )
                        delivery = self._authenticate_planned_record(
                            raw_rec, planned, from_history_reader=False
                        )
                        fetched[planned.delivery_id] = delivery
                        tracker.observe(
                            tp,
                            int(raw_rec.offset),
                            state=DeliverySettlementState.BUFFERED_UNCOMMITTED,
                        )
                    else:
                        # Unplanned record: either malformed (quarantine/fail-closed) or unexpected valid record.
                        try:
                            self._decode_anomaly_record(raw_rec)
                        except (
                            TelemetryDeserializationError,
                            ConsumerRecordValidationError,
                            MissingRequiredHeaderError,
                            DuplicateHeaderError,
                            InvalidHeaderError,
                            TopicEventTypeMismatchError,
                            AnomalyHandoffError,
                            AnomalyEvidenceLimitError,
                            AnomalyIdentityConflictError,
                        ) as exc:
                            await self._handle_unplanned_malformed_record(
                                raw_rec,
                                exc,
                                tracker=tracker,
                                quarantined_out=quarantined_out,
                            )
                            try:
                                await self._commit_settled_offsets(
                                    tracker, deadline=deadline
                                )
                            except TimeoutError:
                                return False
                            continue
                        raise RuntimeRecoveryInputError(
                            f"Unexpected unplanned valid record at {coord[0]}[{coord[1]}]@{coord[2]}",
                            pointer="/source_records",
                        )

                    # Detect if this partition moved strictly past our target offset without yielding it
                    if (
                        tp == target.topic_partition
                        and int(raw_rec.offset) > target.offset
                        and target.delivery_id not in fetched
                    ):
                        raise RuntimeRecoveryInputError(
                            "Planned source coordinate missing before observed higher offset",
                            pointer=f"/source_records/{target.index}",
                        )

            if not any_record:
                empty_polls += 1
                if empty_polls >= 3 and (
                    hasattr(consumer, "end_offsets")
                    or getattr(consumer, "exhausted", False)
                ):
                    raise RuntimeRecoveryInputError(
                        "Planned source coordinate unavailable from consumer",
                        pointer=f"/source_records/{target.index}",
                    )
            else:
                empty_polls = 0

        return True

    def _assert_assignment_intact(self, tracker: PartitionOffsetTracker) -> None:
        assert self._active_consumer is not None
        if self._rebalance_revoked:
            tracker.revoke_partitions(tuple(self._rebalance_revoked))
            raise RuntimeRebalanceError(
                "Consumer partition assignment was revoked; commit ownership cancelled"
            )
        if (
            self._run_rebalance_generation is not None
            and self._rebalance_generation != self._run_rebalance_generation
        ):
            tracker.revoke_partitions(tuple(tracker.assigned_partitions))
            raise RuntimeRebalanceError(
                "Consumer group rebalance occurred during active run; commit ownership cancelled"
            )
        current_assignment = set(self._active_consumer.assignment())
        missing = tracker.assigned_partitions - current_assignment
        if missing:
            tracker.revoke_partitions(tuple(missing))
            raise RuntimeRebalanceError(
                "Consumer lost partition assignment; commit ownership cancelled"
            )

    async def _commit_settled_offsets(
        self,
        tracker: PartitionOffsetTracker,
        *,
        deadline: float | None = None,
    ) -> None:
        """Commit strictly advancing contiguous settled offsets on assigned partitions.

        Re-verifies rebalance epoch and broker committed position immediately before
        committing so a revoke+reassign cycle can never rewind a partition offset.
        """
        assert self._active_consumer is not None
        self._assert_assignment_intact(tracker)
        to_commit = tracker.compute_committable_offsets()
        if not to_commit:
            return

        # Defense-in-depth against silent revoke+reassign: verify live broker committed
        # position has not advanced ahead of our tracker before calling commit().
        for tp, target_offset in to_commit.items():
            committed_coro = self._active_consumer.committed(tp)
            try:
                live_committed = (
                    await self._await_before_deadline(committed_coro, deadline=deadline)
                    if deadline is not None
                    else await committed_coro
                )
            except TimeoutError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                err = RuntimeOffsetCommitError(
                    "Failed to verify broker committed position before offset commit"
                )
                err.__cause__ = exc
                raise err from exc
            if live_committed is not None:
                live_pos = int(live_committed)
                if (
                    live_pos > tracker.confirmed_committed(tp)
                    or target_offset <= live_pos
                ):
                    tracker.revoke_partitions([tp])
                    raise RuntimeRebalanceError(
                        "Broker committed offset advanced externally; refusing offset rewind"
                    )

        self._assert_assignment_intact(tracker)
        commit_coro = self._active_consumer.commit(to_commit)
        try:
            if deadline is not None:
                await self._await_before_deadline(commit_coro, deadline=deadline)
            else:
                await commit_coro
        except TimeoutError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            err = RuntimeOffsetCommitError(
                "Failed to commit contiguous settled Kafka offsets after durable acknowledgement"
            )
            err.__cause__ = exc
            raise err from exc
        tracker.record_committed(to_commit)

    # ------------------------------------------------------------------------
    # DURABLE READBACK, RECONCILIATION & POST-COMMIT DISPATCH
    # ------------------------------------------------------------------------

    async def _load_durable_incident_view(
        self,
        *,
        tenant_id: UUID,
        member_ids: Sequence[UUID],
        candidate_incident_id: UUID,
    ) -> tuple[tuple[UUID, ...], PersistedIncidentState | None]:
        async with self._session_factory() as session:
            async with session.begin():
                await self._verify_role_fn(session)
                repo = self._persistence_repo_factory(session, tenant_id)
                await repo.bind_tenant_context()
                resolved_ids = await repo.resolve_incident_ids(member_ids)
                state = await repo.load_incident(
                    candidate_incident_id, for_update=False
                )
                return resolved_ids, state

    def _verify_durable_history_against_prefix(
        self,
        *,
        durable_state: PersistedIncidentState,
        track: _IncidentPrefixTrack,
        up_to_revision: int,
    ) -> int:
        """Enforce A1-R1 exact intent completeness, head parity, and byte equality."""
        head = durable_state.aggregate.revision
        intents_by_rev = {pi.revision: pi for pi in durable_state.publication_intents}

        expected_revs = set(range(1, head + 1))
        if set(intents_by_rev.keys()) != expected_revs:
            raise RuntimeRecoveryInputError(
                "Durable incident publication intent history contains a hole or out-of-range revision",
                pointer="/durable/intents",
            )

        head_bytes = serialize_incident_aggregate(durable_state.aggregate)
        head_intent_bytes = serialize_incident_aggregate(intents_by_rev[head].aggregate)
        if head_bytes != head_intent_bytes:
            raise RuntimeRecoveryInputError(
                "Durable incident head bytes differ from immutable head publication intent",
                pointer="/durable/head",
            )

        check_limit = min(up_to_revision, head)
        for rev in range(1, check_limit + 1):
            expected_agg = track.reconstructed_aggregates.get(rev)
            expected_bytes = track.reconstructed_bytes.get(rev)
            if expected_agg is None or expected_bytes is None:
                raise RuntimeRecoveryInputError(
                    f"Cannot reconstruct durable revision {rev} from authenticated plan",
                    pointer=f"/durable/intents/{rev}",
                )
            durable_intent = intents_by_rev[rev]
            actual_bytes = serialize_incident_aggregate(durable_intent.aggregate)
            if (
                durable_intent.revision_fingerprint != expected_agg.revision_fingerprint
                or actual_bytes != expected_bytes
            ):
                raise RuntimeRecoveryInputError(
                    f"Durable historical intent revision {rev} differs from authenticated reconstruction",
                    pointer=f"/durable/intents/{rev}",
                )

        return head

    async def _reconcile_and_persist_revision(
        self,
        *,
        tenant_id: UUID,
        candidate_incident_id: UUID,
        group: CorrelationGroup,
        signal_inputs: Mapping[UUID, AnomalyCorrelationInput],
        processing_time: datetime,
        desired_state: CorrelationWindowState,
        reconstructed: IncidentAggregationResult,
        track: _IncidentPrefixTrack,
        deadline: float,
    ) -> tuple[PersistenceDisposition, int]:
        """Reconcile reconstructed revision against PostgreSQL and persist if new (H+1)."""
        member_ids = tuple(m.signal_id for m in group.members)
        resolved_ids, durable_state = await self._await_before_deadline(
            self._load_durable_incident_view(
                tenant_id=tenant_id,
                member_ids=member_ids,
                candidate_incident_id=candidate_incident_id,
            ),
            deadline=deadline,
        )

        if len(resolved_ids) > 1 or (
            len(resolved_ids) == 1 and resolved_ids[0] != candidate_incident_id
        ):
            raise RuntimeRecoveryInputError(
                "Member signals resolve to incompatible durable incident ownership",
                pointer="/durable/membership",
            )

        current_rev = reconstructed.aggregate.revision

        if durable_state is None:
            if current_rev != 1:
                raise RuntimeRecoveryInputError(
                    "Missing durable revision 1 predecessor before continuation",
                    pointer="/durable/intents",
                )
            if not reconstructed.material_update:
                return PersistenceDisposition.NO_OP_REPLAY, 0
            try:
                persisted = await self._await_before_deadline(
                    _persistence_service_mod.persist_incident_atomic(
                        group=group,
                        signal_inputs=signal_inputs,
                        desired_state=desired_state,
                        processing_time=processing_time,
                        tenant_id=tenant_id,
                        session_factory=self._session_factory,
                        expected_revision=None,
                        expected_revision_fingerprint=None,
                    ),
                    deadline=deadline,
                )
            except TimeoutError:
                raise
            except IncidentPersistenceConflictError:
                # Bounded single reread after concurrent creation race
                _, reread_state = await self._await_before_deadline(
                    self._load_durable_incident_view(
                        tenant_id=tenant_id,
                        member_ids=member_ids,
                        candidate_incident_id=candidate_incident_id,
                    ),
                    deadline=deadline,
                )
                if reread_state is None:
                    raise
                head = self._verify_durable_history_against_prefix(
                    durable_state=reread_state,
                    track=track,
                    up_to_revision=current_rev,
                )
                if head >= current_rev:
                    track.observed_durable_head = max(track.observed_durable_head, head)
                    return PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY, head
                raise
            except (IncidentPersistenceTenantError, IncidentPersistenceCorruptionError):
                raise
            except IncidentPersistenceError as exc:
                err = RuntimePersistenceError(
                    "Atomic incident persistence failed during incident creation"
                )
                err.__cause__ = exc
                raise err from exc

            if (
                serialize_incident_aggregate(persisted.aggregate)
                != track.reconstructed_bytes[current_rev]
            ):
                raise RuntimeRecoveryInputError(
                    "Persisted revision 1 bytes differ from deterministic reconstruction",
                    pointer="/durable/intents/1",
                )
            track.observed_durable_head = max(
                track.observed_durable_head, persisted.aggregate.revision
            )
            return (
                PersistenceDisposition.PERSISTED_CREATED
                if persisted.material_update
                else PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY,
                persisted.aggregate.revision,
            )

        # Durable incident exists: verify immutable intent history & head parity first
        head = self._verify_durable_history_against_prefix(
            durable_state=durable_state,
            track=track,
            up_to_revision=current_rev,
        )
        track.observed_durable_head = max(track.observed_durable_head, head)

        if current_rev <= head:
            # A1-R1 B1: VERIFIED_HISTORICAL_REPLAY â€” zero incident DML, never call persist_incident_atomic!
            return PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY, head

        if current_rev != head + 1:
            raise RuntimeRecoveryInputError(
                f"Non-contiguous revision continuation: reconstructed {current_rev} vs durable head {head}",
                pointer="/durable/continuation",
            )

        if not reconstructed.material_update:
            return PersistenceDisposition.NO_OP_REPLAY, head

        # Require exact byte equality of reconstructed predecessor with durable head
        pred_bytes = track.reconstructed_bytes.get(head)
        if pred_bytes is None or pred_bytes != serialize_incident_aggregate(
            durable_state.aggregate
        ):
            raise RuntimeRecoveryInputError(
                "Reconstructed predecessor bytes differ from durable head before CAS continuation",
                pointer="/durable/head",
            )

        try:
            persisted = await self._await_before_deadline(
                _persistence_service_mod.persist_incident_atomic(
                    group=group,
                    signal_inputs=signal_inputs,
                    desired_state=desired_state,
                    processing_time=processing_time,
                    tenant_id=tenant_id,
                    session_factory=self._session_factory,
                    expected_revision=durable_state.aggregate.revision,
                    expected_revision_fingerprint=durable_state.aggregate.revision_fingerprint,
                ),
                deadline=deadline,
            )
        except TimeoutError:
            raise
        except IncidentPersistenceConflictError:
            # Bounded single reread on CAS race
            _, reread_state = await self._await_before_deadline(
                self._load_durable_incident_view(
                    tenant_id=tenant_id,
                    member_ids=member_ids,
                    candidate_incident_id=candidate_incident_id,
                ),
                deadline=deadline,
            )
            if reread_state is None:
                raise
            reread_head = self._verify_durable_history_against_prefix(
                durable_state=reread_state,
                track=track,
                up_to_revision=current_rev,
            )
            if reread_head >= current_rev:
                track.observed_durable_head = max(
                    track.observed_durable_head, reread_head
                )
                return PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY, reread_head
            raise
        except (IncidentPersistenceTenantError, IncidentPersistenceCorruptionError):
            raise
        except IncidentPersistenceError as exc:
            err = RuntimePersistenceError(
                "Atomic incident persistence failed during material revision update"
            )
            err.__cause__ = exc
            raise err from exc

        if (
            serialize_incident_aggregate(persisted.aggregate)
            != track.reconstructed_bytes[current_rev]
        ):
            raise RuntimeRecoveryInputError(
                f"Persisted revision {current_rev} bytes differ from deterministic reconstruction",
                pointer=f"/durable/intents/{current_rev}",
            )
        track.observed_durable_head = max(
            track.observed_durable_head, persisted.aggregate.revision
        )
        return (
            PersistenceDisposition.PERSISTED_UPDATED
            if persisted.material_update
            else PersistenceDisposition.VERIFIED_HISTORICAL_REPLAY,
            persisted.aggregate.revision,
        )

    async def _dispatch_incident_revisions_up_to(
        self,
        *,
        tenant_id: UUID,
        incident_id: UUID,
        target_revision: int,
        staging_context: DispatchStagingContext,
        track: _IncidentPrefixTrack,
        deadline: float,
    ) -> tuple[DispatchOutcome | None, list[DispatchOutcome], bool]:
        """Dispatch all unacknowledged revisions ``1..target_revision`` strictly within ``deadline``."""
        outcomes: list[DispatchOutcome] = []
        target_outcome: DispatchOutcome | None = None
        hit_deadline = False

        for rev in range(1, target_revision + 1):
            if rev in track.acknowledged_revisions:
                continue
            if time.monotonic() >= deadline:
                # A1-R1: no attempt starts after the run deadline
                hit_deadline = True
                break
            try:
                await self._await_before_deadline(
                    self._publish_semaphore.acquire(), deadline=deadline
                )
            except TimeoutError:
                hit_deadline = True
                break
            try:
                if time.monotonic() >= deadline:
                    hit_deadline = True
                    break
                try:
                    outcome = await self._await_before_deadline(
                        self._dispatch_service.dispatch_revision(
                            tenant_id=tenant_id,
                            incident_id=incident_id,
                            revision=rev,
                            staging_context=staging_context,
                        ),
                        deadline=deadline,
                    )
                except TimeoutError:
                    hit_deadline = True
                    break
            finally:
                self._publish_semaphore.release()

            if outcome is None:
                raise RuntimeRecoveryInputError(
                    f"Committed publication intent missing during dispatch for revision {rev}",
                    pointer=f"/durable/intents/{rev}",
                )
            outcomes.append(outcome)
            if rev == target_revision:
                target_outcome = outcome
            if outcome.input_offset_committable:
                track.acknowledged_revisions.add(rev)
            else:
                # Strict predecessor ordering: stop advancing to later revisions if rev is unacknowledged
                break

        return target_outcome, outcomes, hit_deadline

    async def _settle_incident_deliveries_if_ready(
        self,
        *,
        track: _IncidentPrefixTrack,
        current_reconstructed_rev: int,
        tracker: PartitionOffsetTracker,
        reserved_in_memory: set[UUID],
        completed_in_memory: set[UUID],
        primary_delivery_by_signal: Mapping[UUID, str],
    ) -> None:
        """Settle dependent deliveries only when all revisions 1..observed_head are verified and acknowledged."""
        head = track.observed_durable_head
        if head <= 0 or current_reconstructed_rev < head:
            return
        if not all(rev in track.acknowledged_revisions for rev in range(1, head + 1)):
            return

        # Complete in-memory dedupe reservations ONLY after durable commit + confirmed ACK
        for sig_id in sorted(track.member_signal_ids, key=lambda u: u.bytes):
            if sig_id in reserved_in_memory and sig_id not in completed_in_memory:
                await self._dedupe_store.complete(sig_id)
                completed_in_memory.add(sig_id)

        for delivery in track.dependent_deliveries:
            if delivery.from_history_reader:
                continue
            tp = delivery.planned.topic_partition
            off = delivery.planned.offset
            is_primary = (
                primary_delivery_by_signal.get(delivery.planned.signal_id)
                == delivery.planned.delivery_id
            )
            settled_state = (
                DeliverySettlementState.SETTLED_ACKNOWLEDGED
                if is_primary
                else DeliverySettlementState.SETTLED_DUPLICATE
            )
            tracker.mark_settled(tp, off, settled_state)

    # ------------------------------------------------------------------------
    # BOUNDED PLAN EXECUTION ENTRY POINT
    # ------------------------------------------------------------------------

    async def run_plan(
        self,
        *,
        plan: bytes | bytearray | memoryview | Mapping[str, Any] | None,
        expected_plan_sha256: str | None,
        _deadline: float | None = None,
        _t_start: float | None = None,
    ) -> CorrelationRuntimeRunResult:
        """Execute one authenticated controlled runtime plan within finite bounds."""
        if self._state is not CorrelationRuntimeState.READY:
            raise RuntimeStartupError(
                f"Coordinator must be READY before run_plan (current state: '{self._state.value}')"
            )
        assert self._pinned_snapshot is not None
        assert self._active_consumer is not None

        if self.pending_operation_count:
            raise RuntimeCapacityError(
                "Previous runtime operations are still settling after cancellation"
            )

        t_start = _t_start if _t_start is not None else time.monotonic()
        deadline = (
            _deadline
            if _deadline is not None
            else (t_start + float(self._config.max_run_duration_seconds))
        )

        # 1. Authenticate plan and cross-check pinned topology + frozen attempt budget
        auth_plan = parse_and_authenticate_runtime_plan(
            plan,
            expected_plan_sha256=expected_plan_sha256,
            max_input_records=self._config.max_input_records,
        )
        if not auth_plan.topology_reference.matches_reference(self._expected_topology):
            raise RuntimeRecoveryInputError(
                "Plan topology_reference does not match pinned TopologySnapshotReference",
                pointer="/topology_reference",
            )
        if not auth_plan.topology_reference.matches_snapshot(self._pinned_snapshot):
            raise RuntimeRecoveryInputError(
                "Plan topology_reference does not match fetched TopologySnapshot",
                pointer="/topology_reference",
            )
        if (
            auth_plan.max_dispatch_attempts
            != self._dispatch_service.config.max_dispatch_attempts
        ):
            raise RuntimeRecoveryInputError(
                "Plan max_dispatch_attempts does not match frozen dispatcher budget",
                pointer="/max_dispatch_attempts",
            )

        staging_context = DispatchStagingContext(
            snapshot=self._pinned_snapshot,
            expected_topology=self._expected_topology,
            blast_radius_policy=auth_plan.blast_radius_policy,
            correlation_config=auth_plan.config,
            execution_context=auth_plan.execution_context,
            producer_version=auth_plan.producer_version,
        )

        self._state = CorrelationRuntimeState.RUNNING
        self._run_rebalance_generation = self._rebalance_generation
        reserved_in_memory: set[UUID] = set()
        completed_in_memory: set[UUID] = set()

        try:
            # 2. Record startup assigned partitions and confirmed committed positions
            assigned = set(self._active_consumer.assignment())
            tracker = PartitionOffsetTracker()
            stopped_by_deadline = False
            stopped_by_shutdown = False
            has_delayed_unsettled = False

            for tp in sorted(assigned, key=lambda p: (p.topic, p.partition)):
                try:
                    committed_pos = await self._await_before_deadline(
                        self._active_consumer.committed(tp), deadline=deadline
                    )
                except TimeoutError:
                    stopped_by_deadline = True
                    break
                tracker.register_partition(
                    tp,
                    committed_offset=0 if committed_pos is None else int(committed_pos),
                )
            self._offset_tracker = tracker
            startup_committed = tracker.snapshot_confirmed_committed()

            fetched: dict[str, _AuthenticatedDelivery] = {}
            if not stopped_by_deadline:
                # Partition source records into already-committed history vs live assigned consumer
                history_needed: list[PlannedSourceRecord] = []
                live_needed: list[PlannedSourceRecord] = []
                for src in auth_plan.source_records:
                    if src.topic_partition in assigned:
                        if src.offset < tracker.confirmed_committed(
                            src.topic_partition
                        ):
                            history_needed.append(src)
                        else:
                            live_needed.append(src)
                    else:
                        history_needed.append(src)

                # Pre-verify retained offset bounds on assigned consumer before any persistence or send (A1-R1 FIX-14)
                if live_needed:
                    live_tps = sorted(
                        {src.topic_partition for src in live_needed},
                        key=lambda p: (p.topic, p.partition),
                    )
                    if hasattr(self._active_consumer, "beginning_offsets") and callable(
                        self._active_consumer.beginning_offsets
                    ):
                        try:
                            beg_map = await self._await_before_deadline(
                                self._active_consumer.beginning_offsets(live_tps),
                                deadline=deadline,
                            )
                        except TimeoutError:
                            stopped_by_deadline = True
                        else:
                            for src in live_needed:
                                if src.offset < beg_map.get(src.topic_partition, 0):
                                    raise RuntimeRecoveryInputError(
                                        "Planned source coordinate is below partition beginning offset",
                                        pointer=f"/source_records/{src.index}",
                                    )
                    if (
                        not stopped_by_deadline
                        and hasattr(self._active_consumer, "end_offsets")
                        and callable(self._active_consumer.end_offsets)
                    ):
                        try:
                            end_map = await self._await_before_deadline(
                                self._active_consumer.end_offsets(live_tps),
                                deadline=deadline,
                            )
                        except TimeoutError:
                            stopped_by_deadline = True
                        else:
                            for src in live_needed:
                                end_off = end_map.get(src.topic_partition)
                                if end_off is not None and src.offset >= end_off:
                                    raise RuntimeRecoveryInputError(
                                        "Planned source coordinate is unavailable (beyond partition end offset)",
                                        pointer=f"/source_records/{src.index}",
                                    )

                if not stopped_by_deadline and history_needed:
                    try:
                        fetched = await self._fetch_history_coordinates(
                            history_needed, deadline=deadline
                        )
                    except TimeoutError:
                        stopped_by_deadline = True

            plan_by_coord = auth_plan.source_by_coordinate()
            source_by_id = auth_plan.source_by_delivery_id()

            cohorts: dict[tuple[Any, ...], _CohortState] = {}
            incident_tracks: dict[tuple[UUID, UUID], _IncidentPrefixTrack] = {}
            seen_signal_fingerprints: dict[UUID, str] = {}
            primary_delivery_by_signal: dict[UUID, str] = {}

            evaluations: list[EvaluationTickRecord] = []
            revision_records: list[RevisionExecutionRecord] = []
            dispatch_outcomes: list[DispatchOutcome] = []
            quarantined_inputs: list[QuarantinedInputOutcome] = []

            # 3. Process explicit ordered ingest / evaluate schedule
            for step in auth_plan.steps:
                if stopped_by_shutdown or stopped_by_deadline:
                    break
                if self._shutdown_requested:
                    stopped_by_shutdown = True
                    break
                if time.monotonic() >= deadline:
                    stopped_by_deadline = True
                    break

                if step.op == "ingest":
                    assert step.delivery_id is not None
                    planned_src = source_by_id[step.delivery_id]
                    if planned_src.delivery_id not in fetched:
                        arrived = await self._poll_until_delivery_available(
                            target=planned_src,
                            plan_by_coord=plan_by_coord,
                            fetched=fetched,
                            tracker=tracker,
                            quarantined_out=quarantined_inputs,
                            deadline=deadline,
                        )
                        if not arrived:
                            if self._shutdown_requested:
                                stopped_by_shutdown = True
                            else:
                                stopped_by_deadline = True
                            break

                    delivery = fetched[planned_src.delivery_id]
                    corr_input = delivery.correlation_input
                    sig = corr_input.signal

                    # Enforce semantic fingerprint consistency per signal_id
                    prev_fp = seen_signal_fingerprints.get(sig.signal_id)
                    if (
                        prev_fp is not None
                        and prev_fp != corr_input.semantic_fingerprint
                    ):
                        raise AnomalyIdentityConflictError(
                            "Signal identity conflicts with accepted semantic fingerprint"
                        )
                    seen_signal_fingerprints[sig.signal_id] = (
                        corr_input.semantic_fingerprint
                    )
                    primary_delivery_by_signal.setdefault(
                        sig.signal_id, delivery.planned.delivery_id
                    )

                    # Reserve in process-local InMemoryAnomalyDedupeStore without suppressing durable recovery
                    if sig.signal_id not in reserved_in_memory:
                        dedupe_outcome = await self._dedupe_store.reserve(
                            sig.signal_id, corr_input.semantic_fingerprint
                        )
                        if dedupe_outcome is DedupeReservationOutcome.RESERVED_NEW:
                            reserved_in_memory.add(sig.signal_id)

                    # Route to deterministic fixed UTC tumbling cohort
                    bucket_index = int(
                        math.floor(
                            sig.event_time.timestamp() / auth_plan.config.window_seconds
                        )
                    )
                    window_key = (
                        sig.tenant_id,
                        sig.environment,
                        (
                            sig.run_id,
                            sig.scenario_id,
                            sig.scenario_version,
                            sig.reproducibility_key,
                            sig.seed,
                        ),
                        auth_plan.config_sha256,
                        self._pinned_snapshot.snapshot_fingerprint,
                        bucket_index,
                    )
                    cohort = cohorts.get(window_key)
                    if cohort is None:
                        if len(cohorts) >= self._config.max_active_windows:
                            raise RuntimeCapacityError(
                                f"Active window count exceeds max_active_windows ({self._config.max_active_windows})"
                            )
                        cohort = _CohortState(
                            window_key=window_key,
                            tenant_id=sig.tenant_id,
                            environment=sig.environment,
                            bucket_index=bucket_index,
                        )
                        cohorts[window_key] = cohort

                    cohort.deliveries_by_signal.setdefault(sig.signal_id, []).append(
                        delivery
                    )
                    if sig.signal_id not in cohort.observed_inputs:
                        cohort.observed_inputs[sig.signal_id] = corr_input
                        cohort.observed_ingest_times[sig.signal_id] = step.logical_time
                    continue

                # step.op == "evaluate"
                tick = step.logical_time
                sorted_cohort_keys = sorted(
                    cohorts.keys(),
                    key=lambda k: (str(k[0]), str(k[1]), k[5], str(k[2])),
                )
                for wkey in sorted_cohort_keys:
                    if self._shutdown_requested:
                        stopped_by_shutdown = True
                        break
                    if time.monotonic() >= deadline:
                        stopped_by_deadline = True
                        break

                    cohort = cohorts[wkey]
                    late_rejected_ids: list[UUID] = []
                    late_delayed_ids: list[UUID] = []

                    # Classify newly observed unique signals using existing production late-disposition logic
                    newly_admitted_this_tick = False
                    for sig_id in sorted(
                        cohort.observed_inputs.keys(), key=lambda u: u.bytes
                    ):
                        if sig_id in cohort.admitted:
                            continue
                        if sig_id in cohort.classified_excluded:
                            if (
                                cohort.classified_excluded[sig_id]
                                is GroupingSignalDisposition.LATE_REJECTED
                            ):
                                # Settle any duplicate deliveries of an already late-rejected signal
                                for deliv in cohort.deliveries_by_signal.get(
                                    sig_id, ()
                                ):
                                    if not deliv.from_history_reader:
                                        tracker.mark_settled(
                                            deliv.planned.topic_partition,
                                            deliv.planned.offset,
                                            DeliverySettlementState.SETTLED_LATE_EXCLUDED,
                                        )
                            continue

                        inp = cohort.observed_inputs[sig_id]
                        probe = _grouping_mod.group_candidate_signals(
                            [inp],
                            [],
                            topology_snapshot=self._pinned_snapshot,
                            config=auth_plan.config,
                            event_time_watermark=tick,
                        )
                        rec = probe.signal_records[0]
                        if rec.included:
                            if len(cohort.admitted) >= self._config.max_window_members:
                                raise RuntimeCapacityError(
                                    f"Window member count exceeds max_window_members ({self._config.max_window_members})"
                                )
                            cohort.admitted[sig_id] = inp
                            newly_admitted_this_tick = True
                            ingest_t = cohort.observed_ingest_times[sig_id]
                            if (
                                cohort.last_accepted_ingest_time is None
                                or ingest_t > cohort.last_accepted_ingest_time
                            ):
                                cohort.last_accepted_ingest_time = ingest_t
                        elif rec.disposition is GroupingSignalDisposition.LATE_REJECTED:
                            cohort.classified_excluded[sig_id] = rec.disposition
                            late_rejected_ids.append(sig_id)
                            for deliv in cohort.deliveries_by_signal.get(sig_id, ()):
                                if not deliv.from_history_reader:
                                    tracker.mark_settled(
                                        deliv.planned.topic_partition,
                                        deliv.planned.offset,
                                        DeliverySettlementState.SETTLED_LATE_EXCLUDED,
                                    )
                            if (
                                sig_id in reserved_in_memory
                                and sig_id not in completed_in_memory
                            ):
                                await self._dedupe_store.complete(sig_id)
                                completed_in_memory.add(sig_id)
                        elif rec.disposition is GroupingSignalDisposition.LATE_DELAYED:
                            late_delayed_ids.append(sig_id)
                            has_delayed_unsettled = True
                        else:
                            cohort.classified_excluded[sig_id] = rec.disposition

                    if not cohort.admitted:
                        if late_rejected_ids:
                            try:
                                await self._commit_settled_offsets(
                                    tracker, deadline=deadline
                                )
                            except TimeoutError:
                                stopped_by_deadline = True
                                break
                        continue

                    if cohort.grouping_watermark is None:
                        cohort.grouping_watermark = tick

                    admitted_items = [
                        cohort.admitted[sid]
                        for sid in sorted(cohort.admitted.keys(), key=lambda u: u.bytes)
                    ]
                    pair_evidence = [
                        _scoring_mod.score_candidate_pair(
                            left,
                            right,
                            topology_snapshot=self._pinned_snapshot,
                            config=auth_plan.config,
                        )
                        for left, right in combinations(admitted_items, 2)
                    ]
                    grouping_result = _grouping_mod.group_candidate_signals(
                        admitted_items,
                        pair_evidence,
                        topology_snapshot=self._pinned_snapshot,
                        config=auth_plan.config,
                        event_time_watermark=cohort.grouping_watermark,
                    )

                    bucket_end = datetime.fromtimestamp(
                        (cohort.bucket_index + 1) * auth_plan.config.window_seconds,
                        timezone.utc,
                    )
                    assert cohort.last_accepted_ingest_time is not None
                    if tick >= bucket_end + timedelta(
                        seconds=auth_plan.config.allowed_lateness_seconds
                    ):
                        desired_state = CorrelationWindowState.FINALIZED
                    elif (
                        tick - cohort.last_accepted_ingest_time
                    ).total_seconds() >= auth_plan.config.quiet_period_seconds:
                        desired_state = CorrelationWindowState.SETTLING
                    else:
                        desired_state = CorrelationWindowState.COLLECTING

                    evaluations.append(
                        EvaluationTickRecord(
                            step_index=step.index,
                            logical_time=tick,
                            cohort_key=wkey,
                            lateness_watermark=tick,
                            cohort_grouping_watermark=cohort.grouping_watermark,
                            admitted_signal_ids=tuple(
                                inp.signal.signal_id for inp in admitted_items
                            ),
                            late_rejected_signal_ids=tuple(late_rejected_ids),
                            late_delayed_signal_ids=tuple(late_delayed_ids),
                            grouping_result=grouping_result,
                            desired_state=desired_state,
                        )
                    )

                    for group in grouping_result.groups:
                        group_inputs = {
                            m.signal_id: cohort.admitted[m.signal_id]
                            for m in group.members
                        }
                        anchor = min(
                            group.members,
                            key=lambda m: (m.event_time, m.signal_id.bytes),
                        )
                        incident_id = derive_incident_id(
                            tenant_id=cohort.tenant_id,
                            environment=cohort.environment,
                            anchor_signal_fingerprint=anchor.semantic_fingerprint,
                        )
                        track_key = (cohort.tenant_id, incident_id)
                        track = incident_tracks.setdefault(
                            track_key, _IncidentPrefixTrack()
                        )

                        # Enforce monotonic lifecycle non-regression
                        if (
                            track.latest_reconstructed is not None
                            and STATE_ORDER[desired_state]
                            < STATE_ORDER[track.latest_reconstructed.correlation_state]
                        ):
                            raise RuntimeRecoveryInputError(
                                "Accepted input would regress incident correlation lifecycle state",
                                pointer=f"/steps/{step.index}",
                            )

                        # Pure deterministic prefix reconstruction against previous reconstructed aggregate
                        reconstructed = _aggregate_mod.create_or_update_incident(
                            group=group,
                            signal_inputs=group_inputs,
                            processing_time=tick,
                            desired_correlation_state=desired_state,
                            existing_aggregate=track.latest_reconstructed,
                        )
                        agg = reconstructed.aggregate
                        agg_bytes = serialize_incident_aggregate(agg)

                        if reconstructed.material_update:
                            track.reconstructed_aggregates[agg.revision] = agg
                            track.reconstructed_bytes[agg.revision] = agg_bytes
                        track.latest_reconstructed = agg

                        # Associate all deliveries (including duplicates) for group members
                        seen_deliv_ids = {
                            d.planned.delivery_id for d in track.dependent_deliveries
                        }
                        for m in group.members:
                            track.member_signal_ids.add(m.signal_id)
                            for deliv in cohort.deliveries_by_signal.get(
                                m.signal_id, ()
                            ):
                                if deliv.planned.delivery_id not in seen_deliv_ids:
                                    track.dependent_deliveries.append(deliv)
                                    seen_deliv_ids.add(deliv.planned.delivery_id)

                        has_unsettled_delivery = any(
                            not d.from_history_reader
                            and tracker.state_of(
                                d.planned.topic_partition, d.planned.offset
                            )
                            is DeliverySettlementState.BUFFERED_UNCOMMITTED
                            for d in track.dependent_deliveries
                        )

                        # Always reconcile durable state (even on duplicate or non-material replay!)
                        if (
                            reconstructed.material_update
                            or newly_admitted_this_tick
                            or has_unsettled_delivery
                            or track.observed_durable_head == 0
                            or not all(
                                rev in track.acknowledged_revisions
                                for rev in range(
                                    1,
                                    max(agg.revision, track.observed_durable_head) + 1,
                                )
                            )
                        ):
                            if time.monotonic() >= deadline:
                                stopped_by_deadline = True
                                break
                            try:
                                (
                                    disposition,
                                    durable_head,
                                ) = await self._reconcile_and_persist_revision(
                                    tenant_id=cohort.tenant_id,
                                    candidate_incident_id=incident_id,
                                    group=group,
                                    signal_inputs=group_inputs,
                                    processing_time=tick,
                                    desired_state=desired_state,
                                    reconstructed=reconstructed,
                                    track=track,
                                    deadline=deadline,
                                )
                            except TimeoutError:
                                stopped_by_deadline = True
                                break

                            # Post-commit D1 dispatch: only dispatch once reconstructed prefix
                            # has caught up to observed durable head (zero sends on incomplete/drifted plan).
                            target_outcome: DispatchOutcome | None = None
                            new_outcomes: list[DispatchOutcome] = []
                            hit_deadline = False
                            if agg.revision >= durable_head and durable_head > 0:
                                (
                                    target_outcome,
                                    new_outcomes,
                                    hit_deadline,
                                ) = await self._dispatch_incident_revisions_up_to(
                                    tenant_id=cohort.tenant_id,
                                    incident_id=incident_id,
                                    target_revision=agg.revision,
                                    staging_context=staging_context,
                                    track=track,
                                    deadline=deadline,
                                )
                                dispatch_outcomes.extend(new_outcomes)

                            if reconstructed.material_update or new_outcomes:
                                revision_records.append(
                                    RevisionExecutionRecord(
                                        tenant_id=cohort.tenant_id,
                                        incident_id=incident_id,
                                        revision=agg.revision,
                                        revision_fingerprint=agg.revision_fingerprint,
                                        persistence_disposition=disposition,
                                        dispatch_outcome=target_outcome,
                                        logical_time=tick,
                                    )
                                )

                            if hit_deadline:
                                stopped_by_deadline = True
                                break

                            await self._settle_incident_deliveries_if_ready(
                                track=track,
                                current_reconstructed_rev=agg.revision,
                                tracker=tracker,
                                reserved_in_memory=reserved_in_memory,
                                completed_in_memory=completed_in_memory,
                                primary_delivery_by_signal=primary_delivery_by_signal,
                            )

                    if stopped_by_shutdown or stopped_by_deadline:
                        break

                    # Commit any newly eligible contiguous offsets after evaluation
                    try:
                        await self._commit_settled_offsets(tracker, deadline=deadline)
                    except TimeoutError:
                        stopped_by_deadline = True
                        break

                if stopped_by_shutdown or stopped_by_deadline:
                    break

            # 4. Final durable-head completeness verification (A1-R1 B1)
            if not stopped_by_shutdown and not stopped_by_deadline:
                for (t_id, inc_id), track in incident_tracks.items():
                    max_reconstructed = (
                        max(track.reconstructed_aggregates)
                        if track.reconstructed_aggregates
                        else 0
                    )
                    if track.observed_durable_head > max_reconstructed:
                        raise RuntimeRecoveryInputError(
                            f"Durable head revision {track.observed_durable_head} cannot be reconstructed "
                            f"from authenticated plan (max reconstructed: {max_reconstructed})",
                            pointer="/durable/head",
                        )
                    if track.observed_durable_head > 0:
                        (
                            _,
                            extra_outcomes,
                            hit_deadline,
                        ) = await self._dispatch_incident_revisions_up_to(
                            tenant_id=t_id,
                            incident_id=inc_id,
                            target_revision=track.observed_durable_head,
                            staging_context=staging_context,
                            track=track,
                            deadline=deadline,
                        )
                        dispatch_outcomes.extend(extra_outcomes)
                        if hit_deadline:
                            stopped_by_deadline = True
                            break
                        await self._settle_incident_deliveries_if_ready(
                            track=track,
                            current_reconstructed_rev=max_reconstructed,
                            tracker=tracker,
                            reserved_in_memory=reserved_in_memory,
                            completed_in_memory=completed_in_memory,
                            primary_delivery_by_signal=primary_delivery_by_signal,
                        )

                if not stopped_by_deadline:
                    try:
                        await self._commit_settled_offsets(tracker, deadline=deadline)
                    except TimeoutError:
                        stopped_by_deadline = True

            # Release any uncompleted in-memory reservations so retries/replays remain unblocked
            for sig_id in list(reserved_in_memory - completed_in_memory):
                await self._dedupe_store.release(sig_id)

            # 5. Classify honest non-masking terminal run status
            has_terminal_dispatch_failure = any(
                o.status
                in (
                    DispatchOutcomeStatus.COMMITTED_PUBLICATION_EXHAUSTED,
                    DispatchOutcomeStatus.COMMITTED_UNSENDABLE,
                )
                for o in dispatch_outcomes
            )
            has_pending_dispatch = any(
                o.status
                in (
                    DispatchOutcomeStatus.COMMITTED_PENDING_PUBLICATION,
                    DispatchOutcomeStatus.COMMITTED_OUTCOME_UNKNOWN,
                    DispatchOutcomeStatus.SKIPPED_ALREADY_CLAIMED,
                    DispatchOutcomeStatus.BLOCKED_BY_PREDECESSOR,
                )
                for o in dispatch_outcomes
            ) or any(
                track.observed_durable_head > 0
                and not all(
                    rev in track.acknowledged_revisions
                    for rev in range(1, track.observed_durable_head + 1)
                )
                for track in incident_tracks.values()
            )

            if stopped_by_shutdown:
                run_status = CorrelationRuntimeRunStatus.STOPPED_BY_SHUTDOWN
            elif stopped_by_deadline:
                run_status = CorrelationRuntimeRunStatus.INCOMPLETE_DEADLINE_EXCEEDED
            elif has_terminal_dispatch_failure:
                run_status = (
                    CorrelationRuntimeRunStatus.COMPLETED_WITH_TERMINAL_DISPATCH_FAILURE
                )
            elif has_pending_dispatch:
                run_status = CorrelationRuntimeRunStatus.COMPLETED_WITH_PENDING_DISPATCH
            elif has_delayed_unsettled or tracker.unsettled_count() > 0:
                run_status = CorrelationRuntimeRunStatus.INCOMPLETE_DELAYED_INPUTS
            else:
                run_status = CorrelationRuntimeRunStatus.COMPLETED

            self._state = CorrelationRuntimeState.READY
            self._run_rebalance_generation = None
            return CorrelationRuntimeRunResult(
                status=run_status,
                plan_sha256=auth_plan.plan_sha256,
                run_id=auth_plan.run_id,
                topology_snapshot_fingerprint=self._pinned_snapshot.snapshot_fingerprint,
                evaluations=tuple(evaluations),
                revisions=tuple(revision_records),
                dispatch_outcomes=tuple(dispatch_outcomes),
                quarantined_inputs=tuple(quarantined_inputs),
                startup_committed_offsets=startup_committed,
                final_committed_offsets=tracker.snapshot_confirmed_committed(),
                unsettled_offsets=tracker.buffered_offsets(),
                elapsed_wall_seconds=time.monotonic() - t_start,
                unresolved_publish_count=self._dispatch_service.pending_publish_count,
                unresolved_operation_count=self.pending_operation_count,
                phase5_activated=False,
            )

        except asyncio.CancelledError:
            self._run_rebalance_generation = None
            for sig_id in list(reserved_in_memory - completed_in_memory):
                try:
                    await asyncio.shield(self._dedupe_store.release(sig_id))
                except Exception:
                    pass
            self._state = CorrelationRuntimeState.STOPPING
            raise
        except Exception:
            self._run_rebalance_generation = None
            for sig_id in list(reserved_in_memory - completed_in_memory):
                try:
                    await self._dedupe_store.release(sig_id)
                except Exception:
                    pass
            self._state = CorrelationRuntimeState.READY
            raise


# ============================================================================
# BOUNDED EXPLICIT ENTRY POINT
# ============================================================================


async def run_controlled_correlation_runtime(
    *,
    plan: bytes | bytearray | memoryview | Mapping[str, Any] | None,
    expected_plan_sha256: str | None,
    session_factory: async_sessionmaker[AsyncSession],
    dispatch_service: IncidentDispatchService,
    topology_provider: TopologyProvider,
    expected_topology: TopologySnapshotReference,
    config: CorrelationRuntimeConfig | None = None,
    consumer: Any | None = None,
    consumer_factory: Callable[..., Any] | None = None,
    history_reader: Any | None = None,
    history_reader_factory: Callable[..., Any] | None = None,
    bootstrap_servers: str | None = None,
    quarantine_sink: QuarantineSink | None = None,
    dedupe_store: AnomalyDedupeStore | None = None,
    persistence_repo_factory: PersistenceRepoFactory | None = None,
    verify_role_fn: VerifyRoleFn | None = None,
) -> CorrelationRuntimeRunResult:
    """Bounded explicit entry point: start, execute authenticated plan, and shut down cleanly."""
    coordinator = CorrelationRuntimeCoordinator(
        session_factory=session_factory,
        dispatch_service=dispatch_service,
        topology_provider=topology_provider,
        expected_topology=expected_topology,
        config=config,
        consumer=consumer,
        consumer_factory=consumer_factory,
        history_reader=history_reader,
        history_reader_factory=history_reader_factory,
        bootstrap_servers=bootstrap_servers,
        quarantine_sink=quarantine_sink,
        dedupe_store=dedupe_store,
        persistence_repo_factory=persistence_repo_factory,
        verify_role_fn=verify_role_fn,
    )
    t_start = time.monotonic()
    run_deadline = t_start + float(coordinator.config.max_run_duration_seconds)
    await coordinator.start(_deadline=run_deadline)
    try:
        return await coordinator.run_plan(
            plan=plan,
            expected_plan_sha256=expected_plan_sha256,
            _deadline=run_deadline,
            _t_start=t_start,
        )
    except asyncio.CancelledError:
        coordinator.request_shutdown()
        await coordinator._close_owned_handles_best_effort(
            deadline=time.monotonic() + float(coordinator.config.shutdown_grace_seconds)
        )
        coordinator._state = CorrelationRuntimeState.STOPPED
        raise
    finally:
        if coordinator.state is not CorrelationRuntimeState.STOPPED:
            await coordinator.shutdown()


__all__ = [
    "DEFAULT_ACTIVE_WINDOWS",
    "DEFAULT_BUFFERED_RECORDS",
    "DEFAULT_INPUT_RECORDS",
    "DEFAULT_IN_FLIGHT_PUBLICATIONS",
    "DEFAULT_POLL_TIMEOUT_MS",
    "DEFAULT_RUN_DURATION_SECONDS",
    "DEFAULT_SHUTDOWN_GRACE_SECONDS",
    "DEFAULT_WINDOW_MEMBERS",
    "MAX_ACTIVE_WINDOWS",
    "MAX_BUFFERED_RECORDS",
    "MAX_DELIVERY_ID_BYTES",
    "MAX_INPUT_RECORDS",
    "MAX_IN_FLIGHT_PUBLICATIONS",
    "MAX_PLAN_BYTES",
    "MAX_POLL_TIMEOUT_MS",
    "MAX_RUN_DURATION_SECONDS",
    "MAX_SHUTDOWN_GRACE_SECONDS",
    "MAX_WINDOW_MEMBERS",
    "MIN_ACTIVE_WINDOWS",
    "MIN_BUFFERED_RECORDS",
    "MIN_INPUT_RECORDS",
    "MIN_IN_FLIGHT_PUBLICATIONS",
    "MIN_POLL_TIMEOUT_MS",
    "MIN_RUN_DURATION_SECONDS",
    "MIN_SHUTDOWN_GRACE_SECONDS",
    "MIN_WINDOW_MEMBERS",
    "RUNTIME_PLAN_SCHEMA_VERSION",
    "AuthenticatedRuntimePlan",
    "CorrelationRuntimeConfig",
    "CorrelationRuntimeCoordinator",
    "CorrelationRuntimeError",
    "CorrelationRuntimeReadiness",
    "CorrelationRuntimeRunResult",
    "CorrelationRuntimeRunStatus",
    "CorrelationRuntimeState",
    "DeliverySettlementState",
    "EvaluationTickRecord",
    "KafkaConsumerPort",
    "PartitionOffsetTracker",
    "PersistenceDisposition",
    "PlannedSourceRecord",
    "PlannedStep",
    "PlannedTopologyPin",
    "QuarantinedInputOutcome",
    "RevisionExecutionRecord",
    "RuntimeCapacityError",
    "RuntimeConfigurationError",
    "RuntimeInputValidationError",
    "RuntimeOffsetCommitError",
    "RuntimePersistenceError",
    "RuntimeRebalanceError",
    "RuntimeRecoveryInputError",
    "RuntimeStartupError",
    "ShutdownReport",
    "SourceRecordDigests",
    "check_correlation_runtime_readiness",
    "compute_runtime_plan_sha256",
    "compute_source_record_digests",
    "parse_and_authenticate_runtime_plan",
    "run_controlled_correlation_runtime",
]
