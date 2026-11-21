"""Create Phase 4 incident persistence tables.

Revision ID: 20261103_0002
Revises: 20260918_0001
Create Date: 2026-11-03
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20261103_0002"
down_revision: str | None = "20260918_0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # 1. Trigger functions
    op.execute(
        """
        CREATE OR REPLACE FUNCTION guard_incident_snapshot_change()
        RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'DELETE operation is prohibited on incidents table'
                    USING ERRCODE = '55000';
            ELSIF TG_OP = 'UPDATE' THEN
                IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
                    RAISE EXCEPTION 'Immutable field tenant_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.incident_id IS DISTINCT FROM OLD.incident_id THEN
                    RAISE EXCEPTION 'Immutable field incident_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.incident_number IS DISTINCT FROM OLD.incident_number THEN
                    RAISE EXCEPTION 'Immutable field incident_number cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.environment IS DISTINCT FROM OLD.environment THEN
                    RAISE EXCEPTION 'Immutable field environment cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.primary_service IS DISTINCT FROM OLD.primary_service THEN
                    RAISE EXCEPTION 'Immutable field primary_service cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'Immutable field created_at cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.revision IS DISTINCT FROM (OLD.revision + 1) THEN
                    RAISE EXCEPTION 'revision must increment monotonically by exactly 1 (expected %, got %)', OLD.revision + 1, NEW.revision
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.updated_at < OLD.updated_at THEN
                    RAISE EXCEPTION 'updated_at cannot regress (old %, new %)', OLD.updated_at, NEW.updated_at
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION reject_incident_child_change()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION '% operation is prohibited on append-only incident child table %', TG_OP, TG_TABLE_NAME
                USING ERRCODE = '55000';
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # 2. Table: incidents
    op.create_table(
        "incidents",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(10), nullable=False),
        sa.Column("incident_number", sa.String(128), nullable=False),
        sa.Column("environment", sa.String(128), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("correlation_state", sa.String(20), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False),
        sa.Column("severity_policy_version", sa.String(128), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revision", sa.Numeric(), nullable=False),
        sa.Column("primary_service", sa.String(128), nullable=False),
        sa.Column("affected_services", postgresql.JSONB(), nullable=False),
        sa.Column("revision_fingerprint", sa.String(64), nullable=False),
        sa.Column("aggregate_payload", postgresql.BYTEA(), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "incident_id", name="pk_incidents"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incidents_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.UniqueConstraint(
            "tenant_id", "incident_number", name="uq_incidents_tenant_number"
        ),
        sa.CheckConstraint(
            "schema_version = '1.2'", name="ck_incidents_schema_version"
        ),
        sa.CheckConstraint(
            "incident_number = 'INC-' || upper(replace(incident_id::text, '-', ''))",
            name="ck_incidents_incident_number",
        ),
        sa.CheckConstraint(
            "status IN ('open', 'acknowledged', 'resolved', 'closed')",
            name="ck_incidents_status",
        ),
        sa.CheckConstraint(
            "correlation_state IN ('collecting', 'settling', 'finalized')",
            name="ck_incidents_correlation_state",
        ),
        sa.CheckConstraint(
            "severity IN ('debug', 'info', 'warning', 'error', 'critical')",
            name="ck_incidents_severity",
        ),
        sa.CheckConstraint(
            "severity_policy_version = 'max-member-severity/v1'",
            name="ck_incidents_severity_policy",
        ),
        sa.CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incidents_revision",
        ),
        sa.CheckConstraint(
            "started_at <= last_seen_at AND created_at <= updated_at",
            name="ck_incidents_time_order",
        ),
        sa.CheckConstraint(
            "CASE WHEN jsonb_typeof(affected_services) = 'array' THEN jsonb_array_length(affected_services) BETWEEN 1 AND 512 ELSE FALSE END",
            name="ck_incidents_affected_services",
        ),
        sa.CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incidents_revision_fp",
        ),
        sa.CheckConstraint(
            "octet_length(aggregate_payload) BETWEEN 2 AND 67108864 AND jsonb_typeof(convert_from(aggregate_payload, 'UTF8')::jsonb) = 'object'",
            name="ck_incidents_payload",
        ),
    )

    op.create_index(
        "ix_incidents_tenant_status_updated",
        "incidents",
        ["tenant_id", "status", sa.text("updated_at DESC"), "incident_id"],
    )
    op.create_index(
        "ix_incidents_tenant_updated",
        "incidents",
        ["tenant_id", sa.text("updated_at DESC"), "incident_id"],
    )

    op.execute("ALTER TABLE incidents ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE incidents FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY pol_incidents_tenant ON incidents "
        "USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid) "
        "WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)"
    )

    op.execute(
        "CREATE TRIGGER trg_incidents_guard "
        "BEFORE UPDATE OR DELETE ON incidents "
        "FOR EACH ROW EXECUTE FUNCTION guard_incident_snapshot_change()"
    )

    # 3. Table: incident_signal_links
    op.create_table(
        "incident_signal_links",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("signal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("semantic_fingerprint", sa.String(64), nullable=False),
        sa.Column("environment", sa.String(128), nullable=False),
        sa.Column("service", sa.String(128), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False),
        sa.Column("anomaly_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "incident_id", "signal_id", name="pk_incident_signal_links"
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_signal_links_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "incident_id"],
            ["incidents.tenant_id", "incidents.incident_id"],
            name="fk_incident_signal_links_incident",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.UniqueConstraint(
            "tenant_id", "signal_id", name="uq_incident_signal_links_tenant_signal"
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "incident_id",
            "semantic_fingerprint",
            name="uq_incident_signal_links_incident_fp",
        ),
        sa.CheckConstraint(
            "semantic_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_signal_links_fp",
        ),
        sa.CheckConstraint(
            "severity IN ('debug', 'info', 'warning', 'error', 'critical')",
            name="ck_incident_signal_links_severity",
        ),
        sa.CheckConstraint(
            "anomaly_score >= 0 AND anomaly_score <= 1",
            name="ck_incident_signal_links_anomaly_score",
        ),
    )

    op.create_index(
        "ix_incident_signal_links_event_time",
        "incident_signal_links",
        ["tenant_id", "incident_id", "event_time", "signal_id"],
    )

    op.execute("ALTER TABLE incident_signal_links ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE incident_signal_links FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY pol_incident_signal_links_tenant ON incident_signal_links "
        "USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid) "
        "WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)"
    )

    op.execute(
        "CREATE TRIGGER trg_incident_signal_links_append "
        "BEFORE UPDATE OR DELETE ON incident_signal_links "
        "FOR EACH ROW EXECUTE FUNCTION reject_incident_child_change()"
    )

    # 4. Table: incident_correlation_evidence
    op.create_table(
        "incident_correlation_evidence",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("left_signal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("right_signal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("temporal_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("service_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("topology_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("symptom_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("context_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("weighted_score", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("accepted", sa.Boolean(), nullable=False),
        sa.Column("policy_version", sa.String(128), nullable=False),
        sa.Column("reason", sa.String(512), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "incident_id",
            "left_signal_id",
            "right_signal_id",
            name="pk_incident_correlation_evidence",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_correlation_evidence_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
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
        sa.ForeignKeyConstraint(
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
        sa.CheckConstraint(
            "left_signal_id < right_signal_id",
            name="ck_incident_correlation_evidence_pair_order",
        ),
        sa.CheckConstraint(
            "accepted = TRUE",
            name="ck_incident_correlation_evidence_accepted",
        ),
        sa.CheckConstraint(
            "temporal_score >= 0 AND temporal_score <= 1",
            name="ck_incident_correlation_evidence_temporal_score",
        ),
        sa.CheckConstraint(
            "service_score >= 0 AND service_score <= 1",
            name="ck_incident_correlation_evidence_service_score",
        ),
        sa.CheckConstraint(
            "topology_score >= 0 AND topology_score <= 1",
            name="ck_incident_correlation_evidence_topology_score",
        ),
        sa.CheckConstraint(
            "symptom_score >= 0 AND symptom_score <= 1",
            name="ck_incident_correlation_evidence_symptom_score",
        ),
        sa.CheckConstraint(
            "context_score >= 0 AND context_score <= 1",
            name="ck_incident_correlation_evidence_context_score",
        ),
        sa.CheckConstraint(
            "weighted_score >= 0 AND weighted_score <= 1",
            name="ck_incident_correlation_evidence_weighted_score",
        ),
    )

    op.create_index(
        "ix_incident_correlation_evidence_right",
        "incident_correlation_evidence",
        ["tenant_id", "incident_id", "right_signal_id"],
    )

    op.execute("ALTER TABLE incident_correlation_evidence ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE incident_correlation_evidence FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY pol_incident_correlation_evidence_tenant ON incident_correlation_evidence "
        "USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid) "
        "WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)"
    )

    op.execute(
        "CREATE TRIGGER trg_incident_correlation_evidence_append "
        "BEFORE UPDATE OR DELETE ON incident_correlation_evidence "
        "FOR EACH ROW EXECUTE FUNCTION reject_incident_child_change()"
    )

    # 5. Table: incident_timeline_events
    op.create_table(
        "incident_timeline_events",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(10), nullable=False),
        sa.Column("revision", sa.Numeric(), nullable=False),
        sa.Column("event_ordinal", sa.Integer(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("source_signal_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("summary", sa.String(512), nullable=False),
        sa.Column("revision_fingerprint", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "event_id", name="pk_incident_timeline_events"
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_timeline_events_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "incident_id"],
            ["incidents.tenant_id", "incidents.incident_id"],
            name="fk_incident_timeline_events_incident",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
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
        sa.UniqueConstraint(
            "tenant_id",
            "incident_id",
            "revision",
            "event_ordinal",
            name="uq_incident_timeline_events_revision_ordinal",
        ),
        sa.CheckConstraint(
            "schema_version = '1.2'",
            name="ck_incident_timeline_events_schema_version",
        ),
        sa.CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incident_timeline_events_revision",
        ),
        sa.CheckConstraint(
            "event_ordinal >= 0",
            name="ck_incident_timeline_events_ordinal",
        ),
        sa.CheckConstraint(
            "event_type IN ('created', 'signal_attached', 'severity_changed', 'topology_enriched', 'correlation_state_changed', 'correlation_finalized')",
            name="ck_incident_timeline_events_event_type",
        ),
        sa.CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_timeline_events_revision_fp",
        ),
    )

    op.create_index(
        "ix_incident_timeline_events_chronological",
        "incident_timeline_events",
        [
            "tenant_id",
            "incident_id",
            "event_time",
            "revision",
            "event_ordinal",
            "event_id",
        ],
    )

    op.execute("ALTER TABLE incident_timeline_events ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE incident_timeline_events FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY pol_incident_timeline_events_tenant ON incident_timeline_events "
        "USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid) "
        "WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)"
    )

    op.execute(
        "CREATE TRIGGER trg_incident_timeline_events_append "
        "BEFORE UPDATE OR DELETE ON incident_timeline_events "
        "FOR EACH ROW EXECUTE FUNCTION reject_incident_child_change()"
    )

    # 6. Table: incident_publication_intents
    op.create_table(
        "incident_publication_intents",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("revision", sa.Numeric(), nullable=False),
        sa.Column("schema_version", sa.String(10), nullable=False),
        sa.Column("revision_fingerprint", sa.String(64), nullable=False),
        sa.Column("intent_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("aggregate_payload", postgresql.BYTEA(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "incident_id",
            "revision",
            name="pk_incident_publication_intents",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_publication_intents_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "incident_id"],
            ["incidents.tenant_id", "incidents.incident_id"],
            name="fk_incident_publication_intents_incident",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incident_publication_intents_revision",
        ),
        sa.CheckConstraint(
            "schema_version = '1.2'",
            name="ck_incident_publication_intents_schema_version",
        ),
        sa.CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_publication_intents_revision_fp",
        ),
        sa.CheckConstraint(
            "octet_length(aggregate_payload) BETWEEN 2 AND 67108864 AND jsonb_typeof(convert_from(aggregate_payload, 'UTF8')::jsonb) = 'object'",
            name="ck_incident_publication_intents_payload",
        ),
    )

    op.create_index(
        "ix_incident_publication_intents_time",
        "incident_publication_intents",
        ["tenant_id", "intent_time", "incident_id", "revision"],
    )

    op.execute("ALTER TABLE incident_publication_intents ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE incident_publication_intents FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY pol_incident_publication_intents_tenant ON incident_publication_intents "
        "USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid) "
        "WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)"
    )

    op.execute(
        "CREATE TRIGGER trg_incident_publication_intents_append "
        "BEFORE UPDATE OR DELETE ON incident_publication_intents "
        "FOR EACH ROW EXECUTE FUNCTION reject_incident_child_change()"
    )


def downgrade() -> None:
    # 1. Drop child triggers
    op.execute(
        "DROP TRIGGER IF EXISTS trg_incident_publication_intents_append ON incident_publication_intents"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_incident_timeline_events_append ON incident_timeline_events"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_incident_correlation_evidence_append ON incident_correlation_evidence"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_incident_signal_links_append ON incident_signal_links"
    )

    # 2. Drop child tables
    op.drop_table("incident_publication_intents")
    op.drop_table("incident_timeline_events")
    op.drop_table("incident_correlation_evidence")
    op.drop_table("incident_signal_links")

    # 3. Drop parent trigger and table
    op.execute("DROP TRIGGER IF EXISTS trg_incidents_guard ON incidents")
    op.drop_table("incidents")

    # 4. Drop trigger functions (without CASCADE)
    op.execute("DROP FUNCTION IF EXISTS reject_incident_child_change()")
    op.execute("DROP FUNCTION IF EXISTS guard_incident_snapshot_change()")
