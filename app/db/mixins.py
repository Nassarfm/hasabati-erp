"""
app/db/mixins.py
══════════════════════════════════════════════════════════
Reusable SQLAlchemy mixins.
Every ERP model inherits the appropriate combination.

Standard model = Base + TenantMixin + TimestampMixin + AuditMixin
Soft-delete     = above + SoftDeleteMixin
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase


class UUIDPrimaryKeyMixin:
    """UUID primary key — never expose auto-increment integers."""
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )


class TenantMixin:
    """
    Multi-tenancy column.
    Every repository filters by tenant_id — never skips it.
    """
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )


class TimestampMixin:
    """Auto-managed created_at / updated_at."""
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class AuditMixin:
    """Who created / last updated this record."""
    created_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)


class SoftDeleteMixin:
    """
    Soft delete — mark as deleted, never physically remove.
    Required for accounting records (immutability).
    """
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)


class ERPModel(UUIDPrimaryKeyMixin, TenantMixin, TimestampMixin, AuditMixin):
    """
    Standard base for all ERP business entities.
    Inherit from this + Base:

        class MyModel(ERPModel, Base):
            __tablename__ = "my_table"
            ...
    """
    __abstract__ = True

    def to_dict(self) -> dict:
        """Serialize to plain dict (for logging / audit)."""
        result = {}
        for col in self.__table__.columns:
            val = getattr(self, col.name, None)
            if isinstance(val, uuid.UUID):
                val = str(val)
            elif isinstance(val, datetime):
                val = val.isoformat()
            result[col.name] = val
        return result
