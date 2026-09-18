from typing import Any

from sqlalchemy import Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampedSoftDeleteMixin


class Plan(TimestampedSoftDeleteMixin, Base):
    """Global subscription reference data required before a tenant can exist."""

    __tablename__ = "plans"

    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    monthly_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    telemetry_ingest_limit_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    agent_concurrency_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    features: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
