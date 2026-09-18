from app.models import Base


def test_foundation_metadata_contains_the_required_tables() -> None:
    assert set(Base.metadata.tables) == {
        "plans",
        "tenants",
        "users",
        "tenant_memberships",
        "sessions",
    }


def test_foundation_tables_have_the_universal_columns() -> None:
    required_columns = {"id", "created_at", "updated_at", "is_deleted"}
    for table in Base.metadata.tables.values():
        assert required_columns.issubset(table.c.keys())


def test_membership_has_tenant_uniqueness_and_risk_constraints() -> None:
    membership = Base.metadata.tables["tenant_memberships"]
    constraints = {constraint.name for constraint in membership.constraints}

    assert "uq_membership_tenant_user" in constraints
    assert "ck_membership_max_risk_tolerance" in constraints
