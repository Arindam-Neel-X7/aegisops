from app.models import Base


_FOUNDATION_TABLE_NAMES = frozenset(
    {"plans", "tenants", "users", "tenant_memberships", "sessions"}
)


def test_foundation_metadata_contains_the_required_tables() -> None:
    missing_tables = _FOUNDATION_TABLE_NAMES.difference(Base.metadata.tables)
    assert not missing_tables, f"Missing foundation tables: {sorted(missing_tables)}"


def test_foundation_tables_have_the_universal_columns() -> None:
    required_columns = {"id", "created_at", "updated_at", "is_deleted"}
    for name in sorted(_FOUNDATION_TABLE_NAMES):
        assert name in Base.metadata.tables, f"Missing foundation table: {name}"
        table = Base.metadata.tables[name]
        missing_columns = required_columns.difference(table.c.keys())
        assert (
            not missing_columns
        ), f"{name} missing foundation columns: {sorted(missing_columns)}"


def test_membership_has_tenant_uniqueness_and_risk_constraints() -> None:
    membership = Base.metadata.tables["tenant_memberships"]
    constraints = {constraint.name for constraint in membership.constraints}

    assert "uq_membership_tenant_user" in constraints
    assert "ck_membership_max_risk_tolerance" in constraints
