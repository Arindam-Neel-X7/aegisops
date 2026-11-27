"""Create Phase 4 durable incident publication dispatch table.

Revision ID: 20261127_0003
Revises: 20261103_0002
Create Date: 2026-11-27
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20261127_0003"
down_revision: str | None = "20261103_0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


_STATE_INVARIANTS_SQL = """(
    (
        dispatch_state = 'pending'
        AND attempt_count = 0
        AND claim_token IS NULL
        AND claimed_by IS NULL
        AND claimed_at IS NULL
        AND claim_expires_at IS NULL
        AND broker_topic IS NULL
        AND broker_partition IS NULL
        AND broker_offset IS NULL
        AND acknowledged_at IS NULL
    )
    OR (
        dispatch_state = 'in_flight'
        AND attempt_count >= 1
        AND claim_token IS NOT NULL
        AND claimed_by IS NOT NULL
        AND claimed_at IS NOT NULL
        AND claim_expires_at IS NOT NULL
        AND claim_expires_at > claimed_at
        AND broker_topic IS NULL
        AND broker_partition IS NULL
        AND broker_offset IS NULL
        AND acknowledged_at IS NULL
    )
    OR (
        dispatch_state = 'retry_scheduled'
        AND attempt_count >= 1
        AND claim_token IS NULL
        AND next_retry_at IS NOT NULL
        AND last_error_code IS NOT NULL
        AND broker_topic IS NULL
        AND broker_partition IS NULL
        AND broker_offset IS NULL
        AND acknowledged_at IS NULL
    )
    OR (
        dispatch_state = 'ambiguous_outcome'
        AND attempt_count >= 1
        AND claim_token IS NULL
        AND claim_expires_at IS NOT NULL
        AND next_retry_at IS NOT NULL
        AND last_error_code IS NOT NULL
        AND broker_topic IS NULL
        AND broker_partition IS NULL
        AND broker_offset IS NULL
        AND acknowledged_at IS NULL
    )
    OR (
        dispatch_state = 'acknowledged'
        AND attempt_count >= 1
        AND claim_token IS NULL
        AND broker_topic = 'aegis.incidents'
        AND broker_partition IS NOT NULL
        AND broker_partition >= 0
        AND broker_offset IS NOT NULL
        AND broker_offset >= 0
        AND acknowledged_at IS NOT NULL
    )
    OR (
        dispatch_state = 'exhausted'
        AND attempt_count >= 1
        AND claim_token IS NULL
        AND last_error_code IS NOT NULL
        AND broker_topic IS NULL
        AND broker_partition IS NULL
        AND broker_offset IS NULL
        AND acknowledged_at IS NULL
    )
    OR (
        dispatch_state = 'rejected_unsendable'
        AND attempt_count = 0
        AND claim_token IS NULL
        AND last_error_code IS NOT NULL
        AND broker_topic IS NULL
        AND broker_partition IS NULL
        AND broker_offset IS NULL
        AND acknowledged_at IS NULL
    )
)"""


def upgrade() -> None:
    # 1. Guard trigger function
    op.execute(
        """
        CREATE OR REPLACE FUNCTION guard_incident_publication_dispatch_change()
        RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'DELETE operation is prohibited on incident_publication_dispatches table'
                    USING ERRCODE = '55000';
            ELSIF TG_OP = 'UPDATE' THEN
                IF OLD.dispatch_state = 'acknowledged' THEN
                    RAISE EXCEPTION 'Terminal acknowledged dispatch row cannot be updated'
                        USING ERRCODE = '55000';
                END IF;
                IF OLD.dispatch_state = 'rejected_unsendable' THEN
                    RAISE EXCEPTION 'Terminal rejected_unsendable dispatch row cannot be updated'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
                    RAISE EXCEPTION 'Immutable field tenant_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.incident_id IS DISTINCT FROM OLD.incident_id THEN
                    RAISE EXCEPTION 'Immutable field incident_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.revision IS DISTINCT FROM OLD.revision THEN
                    RAISE EXCEPTION 'Immutable field revision cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.revision_fingerprint IS DISTINCT FROM OLD.revision_fingerprint THEN
                    RAISE EXCEPTION 'Immutable field revision_fingerprint cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.publication_contract_version IS DISTINCT FROM OLD.publication_contract_version THEN
                    RAISE EXCEPTION 'Immutable field publication_contract_version cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.envelope_schema_version IS DISTINCT FROM OLD.envelope_schema_version THEN
                    RAISE EXCEPTION 'Immutable field envelope_schema_version cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.handoff_schema_version IS DISTINCT FROM OLD.handoff_schema_version THEN
                    RAISE EXCEPTION 'Immutable field handoff_schema_version cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.event_id IS DISTINCT FROM OLD.event_id THEN
                    RAISE EXCEPTION 'Immutable field event_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.publication_fingerprint IS DISTINCT FROM OLD.publication_fingerprint THEN
                    RAISE EXCEPTION 'Immutable field publication_fingerprint cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.handoff_id IS DISTINCT FROM OLD.handoff_id THEN
                    RAISE EXCEPTION 'Immutable field handoff_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.handoff_fingerprint IS DISTINCT FROM OLD.handoff_fingerprint THEN
                    RAISE EXCEPTION 'Immutable field handoff_fingerprint cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.topology_id IS DISTINCT FROM OLD.topology_id THEN
                    RAISE EXCEPTION 'Immutable field topology_id cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.topology_version IS DISTINCT FROM OLD.topology_version THEN
                    RAISE EXCEPTION 'Immutable field topology_version cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.topology_snapshot_fingerprint IS DISTINCT FROM OLD.topology_snapshot_fingerprint THEN
                    RAISE EXCEPTION 'Immutable field topology_snapshot_fingerprint cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.correlation_policy_version IS DISTINCT FROM OLD.correlation_policy_version THEN
                    RAISE EXCEPTION 'Immutable field correlation_policy_version cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.correlation_config_fingerprint IS DISTINCT FROM OLD.correlation_config_fingerprint THEN
                    RAISE EXCEPTION 'Immutable field correlation_config_fingerprint cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.intent_time IS DISTINCT FROM OLD.intent_time THEN
                    RAISE EXCEPTION 'Immutable field intent_time cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.kafka_topic IS DISTINCT FROM OLD.kafka_topic THEN
                    RAISE EXCEPTION 'Immutable field kafka_topic cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.kafka_key IS DISTINCT FROM OLD.kafka_key THEN
                    RAISE EXCEPTION 'Immutable field kafka_key cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.kafka_value IS DISTINCT FROM OLD.kafka_value THEN
                    RAISE EXCEPTION 'Immutable field kafka_value cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.kafka_headers_json IS DISTINCT FROM OLD.kafka_headers_json THEN
                    RAISE EXCEPTION 'Immutable field kafka_headers_json cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.wire_record_bytes IS DISTINCT FROM OLD.wire_record_bytes THEN
                    RAISE EXCEPTION 'Immutable field wire_record_bytes cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'Immutable field created_at cannot be changed'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.attempt_count < OLD.attempt_count THEN
                    RAISE EXCEPTION 'attempt_count cannot regress (old %, new %)', OLD.attempt_count, NEW.attempt_count
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

    # 2. Companion table: incident_publication_dispatches
    op.create_table(
        "incident_publication_dispatches",
        # 23 immutable identity, provenance, and frozen wire columns
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("revision", sa.Numeric(), nullable=False),
        sa.Column("revision_fingerprint", sa.String(64), nullable=False),
        sa.Column("publication_contract_version", sa.String(64), nullable=False),
        sa.Column("envelope_schema_version", sa.String(16), nullable=False),
        sa.Column("handoff_schema_version", sa.String(64), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_fingerprint", sa.String(64), nullable=False),
        sa.Column("handoff_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("handoff_fingerprint", sa.String(64), nullable=False),
        sa.Column("topology_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topology_version", sa.String(128), nullable=False),
        sa.Column("topology_snapshot_fingerprint", sa.String(64), nullable=False),
        sa.Column("correlation_policy_version", sa.String(128), nullable=False),
        sa.Column("correlation_config_fingerprint", sa.String(64), nullable=False),
        sa.Column("intent_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kafka_topic", sa.String(128), nullable=False),
        sa.Column("kafka_key", postgresql.BYTEA(), nullable=False),
        sa.Column("kafka_value", postgresql.BYTEA(), nullable=True),
        sa.Column("kafka_headers_json", postgresql.JSONB(), nullable=False),
        sa.Column("wire_record_bytes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        # 16 mutable dispatch bookkeeping columns
        sa.Column("dispatch_state", sa.String(32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_by", sa.String(128), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(64), nullable=True),
        sa.Column("last_error_message", sa.String(512), nullable=True),
        sa.Column("broker_topic", sa.String(128), nullable=True),
        sa.Column("broker_partition", sa.Integer(), nullable=True),
        sa.Column("broker_offset", sa.BigInteger(), nullable=True),
        sa.Column("broker_timestamp_ms", sa.BigInteger(), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "incident_id",
            "revision",
            name="pk_incident_publication_dispatches",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_incident_pub_dispatches_tenant",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "incident_id", "revision"],
            [
                "incident_publication_intents.tenant_id",
                "incident_publication_intents.incident_id",
                "incident_publication_intents.revision",
            ],
            name="fk_incident_pub_dispatches_intent",
            ondelete="NO ACTION",
            onupdate="NO ACTION",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "event_id",
            name="uq_incident_pub_dispatches_tenant_event",
        ),
        sa.CheckConstraint(
            "revision = trunc(revision) AND revision BETWEEN 1 AND 18446744073709551615",
            name="ck_incident_pub_dispatches_revision",
        ),
        sa.CheckConstraint(
            "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_pub_dispatches_rev_fp",
        ),
        sa.CheckConstraint(
            "publication_contract_version = 'incident-publication/1.0'",
            name="ck_incident_pub_dispatches_contract_ver",
        ),
        sa.CheckConstraint(
            "envelope_schema_version = '1.0'",
            name="ck_incident_pub_dispatches_env_ver",
        ),
        sa.CheckConstraint(
            "handoff_schema_version = 'phase5-investigation-handoff/1.0'",
            name="ck_incident_pub_dispatches_handoff_ver",
        ),
        sa.CheckConstraint(
            "publication_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_pub_dispatches_pub_fp",
        ),
        sa.CheckConstraint(
            "handoff_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_pub_dispatches_handoff_fp",
        ),
        sa.CheckConstraint(
            "topology_snapshot_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_pub_dispatches_topo_fp",
        ),
        sa.CheckConstraint(
            "correlation_config_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_incident_pub_dispatches_cfg_fp",
        ),
        sa.CheckConstraint(
            "kafka_topic = 'aegis.incidents'",
            name="ck_incident_pub_dispatches_topic",
        ),
        sa.CheckConstraint(
            "octet_length(kafka_key) BETWEEN 1 AND 512",
            name="ck_incident_pub_dispatches_key",
        ),
        sa.CheckConstraint(
            "(dispatch_state = 'rejected_unsendable' AND kafka_value IS NULL) "
            "OR (dispatch_state <> 'rejected_unsendable' AND kafka_value IS NOT NULL "
            "AND octet_length(kafka_value) BETWEEN 2 AND 983040)",
            name="ck_incident_pub_dispatches_value",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(kafka_headers_json) = 'array'",
            name="ck_incident_pub_dispatches_headers",
        ),
        sa.CheckConstraint(
            "wire_record_bytes > 0 AND (dispatch_state = 'rejected_unsendable' OR wire_record_bytes <= 1048576)",
            name="ck_incident_pub_dispatches_wire_bytes",
        ),
        sa.CheckConstraint(
            "dispatch_state IN ('pending', 'in_flight', 'retry_scheduled', "
            "'ambiguous_outcome', 'acknowledged', 'exhausted', 'rejected_unsendable')",
            name="ck_incident_pub_dispatches_state",
        ),
        sa.CheckConstraint(
            "attempt_count BETWEEN 0 AND 10",
            name="ck_incident_pub_dispatches_attempts",
        ),
        sa.CheckConstraint(
            "created_at <= updated_at",
            name="ck_incident_pub_dispatches_time_order",
        ),
        sa.CheckConstraint(
            _STATE_INVARIANTS_SQL,
            name="ck_incident_pub_dispatches_state_invariants",
        ),
    )

    op.create_index(
        "ix_incident_pub_dispatches_recoverable",
        "incident_publication_dispatches",
        [
            "tenant_id",
            "dispatch_state",
            "next_retry_at",
            "claim_expires_at",
            "incident_id",
            "revision",
        ],
    )
    op.create_index(
        "ix_incident_pub_dispatches_incident_rev",
        "incident_publication_dispatches",
        ["tenant_id", "incident_id", "revision"],
    )

    op.execute("ALTER TABLE incident_publication_dispatches ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE incident_publication_dispatches FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY pol_incident_publication_dispatches_tenant "
        "ON incident_publication_dispatches "
        "USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid) "
        "WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid)"
    )

    op.execute(
        "CREATE TRIGGER trg_incident_publication_dispatches_guard "
        "BEFORE UPDATE OR DELETE ON incident_publication_dispatches "
        "FOR EACH ROW EXECUTE FUNCTION guard_incident_publication_dispatch_change()"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_incident_publication_dispatches_guard "
        "ON incident_publication_dispatches"
    )
    op.drop_table("incident_publication_dispatches")
    op.execute("DROP FUNCTION IF EXISTS guard_incident_publication_dispatch_change()")
