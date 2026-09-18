from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampedSoftDeleteMixin


class Membership(TimestampedSoftDeleteMixin, Base):
    __tablename__ = "tenant_memberships"
    __table_args__ = (
        CheckConstraint("role IN ('admin', 'sre', 'viewer')", name="ck_membership_role"),
        CheckConstraint(
            "max_risk_tolerance >= 0 AND max_risk_tolerance <= 100",
            name="ck_membership_max_risk_tolerance",
        ),
        UniqueConstraint("tenant_id", "user_id", name="uq_membership_tenant_user"),
    )

    tenant_id: Mapped[UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    max_risk_tolerance: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="50"
    )
    invited_by: Mapped[UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
