from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampedSoftDeleteMixin


class Tenant(TimestampedSoftDeleteMixin, Base):
    __tablename__ = "tenants"

    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    slug: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    plan_id: Mapped[UUID] = mapped_column(ForeignKey("plans.id"), nullable=False)
    seats_limit: Mapped[int] = mapped_column(Integer, nullable=False, server_default="10")
    data_retention_days: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="90"
    )
    sso_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    sso_config: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    onboarding_complete: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )
