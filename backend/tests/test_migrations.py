"""Unit tests for Alembic migration chain and head integrity."""

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_incident_persistence_migration_is_the_single_alembic_head() -> None:
    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ["20261103_0002"]
    assert len(scripts.get_heads()) == 1


def test_incident_persistence_migration_has_foundation_down_revision() -> None:
    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)

    revision = scripts.get_revision("20261103_0002")
    assert revision is not None
    assert revision.down_revision == "20260918_0001"
    assert revision.dependencies is None or len(revision.dependencies) == 0


def test_incident_persistence_migration_exports_upgrade_and_downgrade() -> None:
    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)

    revision = scripts.get_revision("20261103_0002")
    assert revision is not None
    assert callable(revision.module.upgrade)
    assert callable(revision.module.downgrade)
