"""Create Phase 0 foundation tables.

Revision ID: 20260918_0001
Revises:
Create Date: 2026-09-18
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260918_0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


PLAN_ROWS = [
    (
        "00000000-0000-0000-0000-000000000001",
        "Free Trial",
        0,
        100,
        1,
        '{"custom_remediations": false, "sso": false}',
    ),
    (
        "00000000-0000-0000-0000-000000000002",
        "Professional",
        4900,
        1024,
        5,
        '{"custom_remediations": true, "sso": false}',
    ),
    (
        "00000000-0000-0000-0000-000000000003",
        "Enterprise",
        19900,
        10240,
        25,
        '{"custom_remediations": true, "sso": true}',
    ),
]


def _universal_columns() -> list[sa.Column[object]]:
    return [
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    ]


def _enable_rls(table_name: str, predicate: str) -> None:
    op.execute(f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {table_name}_tenant_isolation ON {table_name} "
        f"USING ({predicate}) WITH CHECK ({predicate})"
    )


def _add_updated_at_trigger(table_name: str) -> None:
    op.execute(
        f"CREATE TRIGGER trg_{table_name}_updated_at "
        f"BEFORE UPDATE ON {table_name} FOR EACH ROW EXECUTE FUNCTION set_updated_at()"
    )


def _verify_table(table_name: str) -> None:
    """Fail the migration early if a prerequisite table was not created."""
    op.execute(f"SELECT 1 FROM {table_name} LIMIT 1")


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute(
        """
        CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )

    op.create_table(
        "plans",
        *_universal_columns(),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("monthly_price_cents", sa.Integer(), nullable=False),
        sa.Column("telemetry_ingest_limit_mb", sa.Integer(), nullable=False),
        sa.Column("agent_concurrency_limit", sa.Integer(), nullable=False),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.UniqueConstraint("name", name="uq_plans_name"),
    )
    op.create_index("idx_plans_name", "plans", ["name"])
    _verify_table("plans")
    for plan_id, name, price, ingest_limit, concurrency_limit, features in PLAN_ROWS:
        op.execute(
            "INSERT INTO plans "
            "(id, name, monthly_price_cents, telemetry_ingest_limit_mb, "
            "agent_concurrency_limit, features) "
            f"VALUES ('{plan_id}', '{name}', {price}, {ingest_limit}, "
            f"{concurrency_limit}, '{features}'::jsonb)"
        )

    op.create_table(
        "users",
        *_universal_columns(),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text()),
        sa.Column("first_name", sa.Text(), nullable=False),
        sa.Column("last_name", sa.Text(), nullable=False),
        sa.Column("avatar_url", sa.Text()),
        sa.Column("last_login_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_index("idx_users_email", "users", ["email"], unique=True)
    _verify_table("users")

    op.create_table(
        "tenants",
        *_universal_columns(),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("plan_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("seats_limit", sa.Integer(), nullable=False, server_default="10"),
        sa.Column(
            "data_retention_days", sa.Integer(), nullable=False, server_default="90"
        ),
        sa.Column("sso_provider", sa.Text()),
        sa.Column("sso_config", postgresql.JSONB()),
        sa.Column(
            "onboarding_complete", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.ForeignKeyConstraint(["plan_id"], ["plans.id"]),
        sa.UniqueConstraint("name", name="uq_tenants_name"),
        sa.UniqueConstraint("slug", name="uq_tenants_slug"),
    )
    op.create_index("idx_tenants_plan", "tenants", ["plan_id"])
    _verify_table("tenants")

    op.create_table(
        "tenant_memberships",
        *_universal_columns(),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("max_risk_tolerance", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("invited_by", postgresql.UUID(as_uuid=True)),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["invited_by"], ["users.id"]),
        sa.CheckConstraint("role IN ('admin', 'sre', 'viewer')", name="ck_membership_role"),
        sa.CheckConstraint(
            "max_risk_tolerance >= 0 AND max_risk_tolerance <= 100",
            name="ck_membership_max_risk_tolerance",
        ),
        sa.UniqueConstraint("tenant_id", "user_id", name="uq_membership_tenant_user"),
    )
    op.create_index("idx_membership_user", "tenant_memberships", ["user_id"])
    _verify_table("tenant_memberships")

    op.create_table(
        "sessions",
        *_universal_columns(),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("ip_address", postgresql.INET(), nullable=False),
        sa.Column("user_agent", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
    )
    op.create_index("idx_sessions_user", "sessions", ["user_id"])
    op.create_index("idx_sessions_expires", "sessions", ["expires_at"])
    _verify_table("sessions")

    for table_name in ("plans", "users", "tenants", "tenant_memberships", "sessions"):
        _add_updated_at_trigger(table_name)

    _enable_rls("plans", "true")
    _enable_rls("users", "true")
    tenant_scope = "id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid"
    _enable_rls("tenants", tenant_scope)
    tenant_scope = "tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')::uuid"
    _enable_rls("tenant_memberships", tenant_scope)
    _enable_rls("sessions", tenant_scope)


def downgrade() -> None:
    op.drop_table("sessions")
    op.drop_table("tenant_memberships")
    op.drop_table("tenants")
    op.drop_table("users")
    op.drop_table("plans")
    op.execute("DROP FUNCTION set_updated_at()")
