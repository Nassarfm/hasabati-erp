"""
app/modules/dimensions/models.py
══════════════════════════════════════════════════════════
Dimensions Module — الأبعاد المحاسبية

Tables:
  dimensions        — تعريف الأبعاد (branch, cost_center...)
  dimension_values  — قيم كل بُعد
  je_line_dimensions — ربط الأبعاد بسطور القيود
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from typing import List, Optional

from sqlalchemy import (
    Boolean, DateTime, ForeignKey, Index,
    Integer, String, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.mixins import ERPModel, TenantMixin, TimestampMixin


class Dimension(ERPModel, Base):
    """
    تعريف بُعد محاسبي.
    is_system = True  → لا يمكن حذفه
    is_required = True → إجباري عند ترحيل القيد
    classification: where | who | why | expense_only
    """
    __tablename__ = "dimensions"

    code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    classification: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    is_required: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_system: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    values: Mapped[List["DimensionValue"]] = relationship(
        "DimensionValue",
        back_populates="dimension",
        cascade="all, delete-orphan",
        order_by="DimensionValue.code",
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_dim_tenant_code"),
        Index("ix_dim_tenant_active", "tenant_id", "is_active"),
    )


class DimensionValue(ERPModel, Base):
    """
    قيمة بُعد محاسبي.
    مثال: branch → الرياض، جدة، الدمام
    """
    __tablename__ = "dimension_values"

    dimension_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("dimensions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    code: Mapped[str] = mapped_column(String(50), nullable=False)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    dimension: Mapped["Dimension"] = relationship(
        "Dimension", back_populates="values"
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "dimension_id", "code",
            name="uq_dimval_tenant_dim_code",
        ),
        Index("ix_dimval_dimension", "dimension_id", "is_active"),
    )


class JELineDimension(TenantMixin, TimestampMixin, Base):
    """
    ربط سطر القيد بقيم الأبعاد.
    One row per (je_line_id, dimension_id).
    """
    __tablename__ = "je_line_dimensions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    je_line_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("je_lines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    dimension_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("dimensions.id"),
        nullable=False,
    )
    dimension_value_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("dimension_values.id"),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "je_line_id", "dimension_id",
            name="uq_jeline_dim",
        ),
        Index("ix_jeline_dim_line", "je_line_id"),
    )
