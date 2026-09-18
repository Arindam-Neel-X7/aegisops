from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Create the async engine only when database work begins in Phase 0 Step 3."""
    global _engine, _session_factory
    if _engine is None:
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=False,
            pool_pre_ping=True,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get async DB session."""
    get_engine()
    assert _session_factory is not None
    async with _session_factory() as session:
        yield session


async def check_database_connection() -> bool:
    """Return whether the configured PostgreSQL database accepts a simple query."""
    try:
        async with get_engine().connect() as connection:
            await connection.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return False
    return True


async def dispose_engine() -> None:
    """Release database connections for shutdown and test isolation."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None
