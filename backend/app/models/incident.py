"""SQLAlchemy record models for PostgreSQL-persisted incident aggregates."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Double,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    Uuid,
    desc,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class IncidentRecord(Base):
    """Durable parent record holding current incident state and canonical payload."""

    __tablename__ = "incidents"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "incident_id", name="pk_incidents"),
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incidents_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        UniqueConstraint(
            "tenant_id", "incident_number", name="uq_incidents_tenant_number"
        ),
        CheckConstraint("schema_version = '1.2'", name="ck_incidents_schema_version"),
        CheckConstraint(
            "incident_number = 'INC-' || upper(replace(incident_id::text, '-', ''))",
            name="ck_incidents_incident_number",
        ),
        CheckConstraint(
            "status IN ('open', 'acknowledged', 'resolved', 'closed')",
            name="ck_incidents_status",
        ),
        CheckConstraint(
            "correlation_state IN ('collecting', 'settling', 'finalized')",
            name="ck_incidents_correlation_state",
        ),
        CheckConstraint(
            "severity IN ('debug', 'info', 'warning', 'error', 'critical')",
            name="ck_incidents_severity",
        ),
        CheckConstraint(
            "severity_policy_version = 'max-member-severity/v1'",
            name="ck_incidents_severity_policy",
        ),
        CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incidents_revision",
        ),
        CheckConstraint(
            "started_at <= last_seen_at AND created_at <= updated_at",
            name="ck_incidents_time_order",
        ),
        CheckConstraint(
            "CASE WHEN jsonb_typeof(affected_services) = 'array' THEN jsonb_array_length(affected_services) BETWEEN 1 AND 512 ELSE FALSE END",
            name="ck_incidents_affected_services",
        ),
        CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incidents_revision_fp",
        ),
        CheckConstraint(
            "octet_length(aggregate_payload) BETWEEN 2 AND 67108864 AND jsonb_typeof(convert_from(aggregate_payload, 'UTF8')::jsonb) = 'object'",
            name="ck_incidents_payload",
        ),
        Index(
            "ix_incidents_tenant_status_updated",
            "tenant_id",
            "status",
            desc("updated_at"),
            "incident_id",
        ),
        Index(
            "ix_incidents_tenant_updated",
            "tenant_id",
            desc("updated_at"),
            "incident_id",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(10), nullable=False)
    incident_number: Mapped[str] = mapped_column(String(128), nullable=False)
    environment: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    correlation_state: Mapped[str] = mapped_column(String(20), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    severity_policy_version: Mapped[str] = mapped_column(String(128), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    revision: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    primary_service: Mapped[str] = mapped_column(String(128), nullable=False)
    affected_services: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    revision_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class IncidentSignalLinkRecord(Base):
    """Membership link connecting an authoritative telemetry signal to an incident."""

    __tablename__ = "incident_signal_links"
    __table_args__ = (
        PrimaryKeyConstraint(
            "tenant_id", "incident_id", "signal_id", name="pk_incident_signal_links"
        ),
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_signal_links_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "incident_id"],
            ["incidents.tenant_id", "incidents.incident_id"],
            name="fk_incident_signal_links_incident",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        UniqueConstraint(
            "tenant_id", "signal_id", name="uq_incident_signal_links_tenant_signal"
        ),
        UniqueConstraint(
            "tenant_id",
            "incident_id",
            "semantic_fingerprint",
            name="uq_incident_signal_links_incident_fp",
        ),
        CheckConstraint(
            "semantic_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_signal_links_fp",
        ),
        CheckConstraint(
            "severity IN ('debug', 'info', 'warning', 'error', 'critical')",
            name="ck_incident_signal_links_severity",
        ),
        CheckConstraint(
            "anomaly_score >= 0 AND anomaly_score <= 1",
            name="ck_incident_signal_links_anomaly_score",
        ),
        Index(
            "ix_incident_signal_links_event_time",
            "tenant_id",
            "incident_id",
            "event_time",
            "signal_id",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    signal_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    semantic_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    environment: Mapped[str] = mapped_column(String(128), nullable=False)
    service: Mapped[str] = mapped_column(String(128), nullable=False)
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    anomaly_score: Mapped[float] = mapped_column(Double, nullable=False)


class IncidentCorrelationEvidenceRecord(Base):
    """Pairwise correlation evidence record linking two member signals."""

    __tablename__ = "incident_correlation_evidence"
    __table_args__ = (
        PrimaryKeyConstraint(
            "tenant_id",
            "incident_id",
            "left_signal_id",
            "right_signal_id",
            name="pk_incident_correlation_evidence",
        ),
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_correlation_evidence_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "incident_id", "left_signal_id"],
            [
                "incident_signal_links.tenant_id",
                "incident_signal_links.incident_id",
                "incident_signal_links.signal_id",
            ],
            name="fk_incident_correlation_evidence_left_member",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "incident_id", "right_signal_id"],
            [
                "incident_signal_links.tenant_id",
                "incident_signal_links.incident_id",
                "incident_signal_links.signal_id",
            ],
            name="fk_incident_correlation_evidence_right_member",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        CheckConstraint(
            "left_signal_id < right_signal_id",
            name="ck_incident_correlation_evidence_pair_order",
        ),
        CheckConstraint(
            "accepted = TRUE",
            name="ck_incident_correlation_evidence_accepted",
        ),
        CheckConstraint(
            "temporal_score >= 0 AND temporal_score <= 1",
            name="ck_incident_correlation_evidence_temporal_score",
        ),
        CheckConstraint(
            "service_score >= 0 AND service_score <= 1",
            name="ck_incident_correlation_evidence_service_score",
        ),
        CheckConstraint(
            "topology_score >= 0 AND topology_score <= 1",
            name="ck_incident_correlation_evidence_topology_score",
        ),
        CheckConstraint(
            "symptom_score >= 0 AND symptom_score <= 1",
            name="ck_incident_correlation_evidence_symptom_score",
        ),
        CheckConstraint(
            "context_score >= 0 AND context_score <= 1",
            name="ck_incident_correlation_evidence_context_score",
        ),
        CheckConstraint(
            "weighted_score >= 0 AND weighted_score <= 1",
            name="ck_incident_correlation_evidence_weighted_score",
        ),
        Index(
            "ix_incident_correlation_evidence_right",
            "tenant_id",
            "incident_id",
            "right_signal_id",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    left_signal_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    right_signal_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    temporal_score: Mapped[float] = mapped_column(Double, nullable=False)
    service_score: Mapped[float] = mapped_column(Double, nullable=False)
    topology_score: Mapped[float] = mapped_column(Double, nullable=False)
    symptom_score: Mapped[float] = mapped_column(Double, nullable=False)
    context_score: Mapped[float] = mapped_column(Double, nullable=False)
    weighted_score: Mapped[float] = mapped_column(Double, nullable=False)
    accepted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    policy_version: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)


class IncidentTimelineEventRecord(Base):
    """Append-only domain event emitted on incident creation or revision update."""

    __tablename__ = "incident_timeline_events"
    __table_args__ = (
        PrimaryKeyConstraint(
            "tenant_id", "event_id", name="pk_incident_timeline_events"
        ),
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_timeline_events_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "incident_id"],
            ["incidents.tenant_id", "incidents.incident_id"],
            name="fk_incident_timeline_events_incident",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "incident_id", "source_signal_id"],
            [
                "incident_signal_links.tenant_id",
                "incident_signal_links.incident_id",
                "incident_signal_links.signal_id",
            ],
            name="fk_incident_timeline_events_source_member",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        UniqueConstraint(
            "tenant_id",
            "incident_id",
            "revision",
            "event_ordinal",
            name="uq_incident_timeline_events_revision_ordinal",
        ),
        CheckConstraint(
            "schema_version = '1.2'",
            name="ck_incident_timeline_events_schema_version",
        ),
        CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incident_timeline_events_revision",
        ),
        CheckConstraint(
            "event_ordinal >= 0",
            name="ck_incident_timeline_events_ordinal",
        ),
        CheckConstraint(
            "event_type IN ('created', 'signal_attached', 'severity_changed', 'topology_enriched', 'correlation_state_changed', 'correlation_finalized')",
            name="ck_incident_timeline_events_event_type",
        ),
        CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_timeline_events_revision_fp",
        ),
        Index(
            "ix_incident_timeline_events_chronological",
            "tenant_id",
            "incident_id",
            "event_time",
            "revision",
            "event_ordinal",
            "event_id",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(10), nullable=False)
    revision: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    event_ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    source_signal_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    summary: Mapped[str] = mapped_column(String(512), nullable=False)
    revision_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)


class IncidentPublicationIntentRecord(Base):
    """Outbox publication intent record for downstream Kafka incident notifications."""

    __tablename__ = "incident_publication_intents"
    __table_args__ = (
        PrimaryKeyConstraint(
            "tenant_id",
            "incident_id",
            "revision",
            name="pk_incident_publication_intents",
        ),
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_publication_intents_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "incident_id"],
            ["incidents.tenant_id", "incidents.incident_id"],
            name="fk_incident_publication_intents_incident",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incident_publication_intents_revision",
        ),
        CheckConstraint(
            "schema_version = '1.2'",
            name="ck_incident_publication_intents_schema_version",
        ),
        CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_publication_intents_revision_fp",
        ),
        CheckConstraint(
            "octet_length(aggregate_payload) BETWEEN 2 AND 67108864 AND jsonb_typeof(convert_from(aggregate_payload, 'UTF8')::jsonb) = 'object'",
            name="ck_incident_publication_intents_payload",
        ),
        Index(
            "ix_incident_publication_intents_time",
            "tenant_id",
            "intent_time",
            "incident_id",
            "revision",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    revision: Mapped[Decimal] = mapped_column(Numeric, primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(10), nullable=False)
    revision_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    intent_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    aggregate_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
