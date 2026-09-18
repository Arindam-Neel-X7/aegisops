from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_foundation_migration_is_the_single_alembic_head() -> None:
    backend_root = Path(__file__).resolve().parents[1]
    config = Config(str(backend_root / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ["20260918_0001"]
    revision = scripts.get_revision("20260918_0001")
    assert revision is not None
    assert callable(revision.module.upgrade)
    assert callable(revision.module.downgrade)
