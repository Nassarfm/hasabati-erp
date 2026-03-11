"""
app/modules/assets/models.py — Fixed Assets Module
Tables: ast_assets, ast_depreciation_schedules, ast_disposal
Methods: Straight-Line (SL) — متوافق IFRS / IAS 16
"""
from __future__ import annotations
import enum, uuid
from decimal import Decimal
from typing import List, Optional
from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime, Integer,
    Numeric, String, Text, UniqueConstraint, ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.base import Base
from app.db.mixins import ERPModel, SoftDeleteMixin


class AssetStatus(str, enum.Enum):
    ACTIVE = "active"; FULLY_DEPRECIATED = "fully_depreciated"
    DISPOSED = "disposed"; UNDER_MAINTENANCE = "under_maintenance"

class DepreciationMethod(str, enum.Enum):
    STRAIGHT_LINE = "straight_line"

class AssetCategory(str, enum.Enum):
    BUILDINGS    = "buildings"
    MACHINERY    = "machinery"
    VEHICLES     = "vehicles"
    FURNITURE    = "furniture"
    COMPUTERS    = "computers"
    LAND         = "land"
    INTANGIBLE   = "intangible"
    OTHER        = "other"


class FixedAsset(ERPModel, SoftDeleteMixin, Base):
    __tablename__ = "ast_assets"

    asset_number:    Mapped[str]             = mapped_column(String(50),  nullable=False, index=True)
    name_ar:         Mapped[str]             = mapped_column(String(255), nullable=False)
    name_en:         Mapped[Optional[str]]   = mapped_column(String(255), nullable=True)
    category:        Mapped[AssetCategory]   = mapped_column(String(30),  nullable=False, index=True)
    status:          Mapped[AssetStatus]     = mapped_column(String(30),  default=AssetStatus.ACTIVE, index=True)
    depreciation_method: Mapped[DepreciationMethod] = mapped_column(String(20), default=DepreciationMethod.STRAIGHT_LINE)
    purchase_date:   Mapped[str]             = mapped_column(Date,        nullable=False)
    in_service_date: Mapped[str]             = mapped_column(Date,        nullable=False)
    purchase_cost:   Mapped[Decimal]         = mapped_column(Numeric(18, 3), nullable=False)
    salvage_value:   Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    useful_life_months: Mapped[int]          = mapped_column(Integer,     nullable=False)
    accumulated_depreciation: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    net_book_value:  Mapped[Decimal]         = mapped_column(Numeric(18, 3), nullable=False)
    last_dep_date:   Mapped[Optional[str]]   = mapped_column(Date,        nullable=True)
    location:        Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    serial_number:   Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    supplier_name:   Mapped[Optional[str]]   = mapped_column(String(255), nullable=True)
    asset_account:   Mapped[str]             = mapped_column(String(20),  default="1502")
    accum_dep_account: Mapped[str]           = mapped_column(String(20),  default="1552")
    dep_expense_account: Mapped[str]         = mapped_column(String(20),  default="6502")
    disposal_account: Mapped[str]            = mapped_column(String(20),  default="4202")
    notes:           Mapped[Optional[str]]   = mapped_column(Text,        nullable=True)

    schedules: Mapped[List["DepreciationSchedule"]] = relationship(
        "DepreciationSchedule", back_populates="asset", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "asset_number", name="uq_asset_tenant_number"),
        CheckConstraint("purchase_cost > 0",   name="ck_asset_cost_positive"),
        CheckConstraint("useful_life_months > 0", name="ck_asset_life_positive"),
        CheckConstraint("salvage_value >= 0",  name="ck_asset_salvage_positive"),
    )

    @property
    def monthly_depreciation(self) -> Decimal:
        depreciable = self.purchase_cost - self.salvage_value
        if self.useful_life_months <= 0 or depreciable <= 0:
            return Decimal("0")
        return (depreciable / self.useful_life_months).quantize(Decimal("0.001"))


class DepreciationSchedule(ERPModel, Base):
    """سجل الإهلاك الشهري لكل أصل."""
    __tablename__ = "ast_depreciation_schedules"

    asset_id:       Mapped[uuid.UUID]  = mapped_column(UUID(as_uuid=True), ForeignKey("ast_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    asset_number:   Mapped[str]        = mapped_column(String(50),  nullable=False)
    period_year:    Mapped[int]        = mapped_column(Integer,     nullable=False)
    period_month:   Mapped[int]        = mapped_column(Integer,     nullable=False)
    dep_date:       Mapped[str]        = mapped_column(Date,        nullable=False)
    dep_amount:     Mapped[Decimal]    = mapped_column(Numeric(18, 3), nullable=False)
    accum_dep_before: Mapped[Decimal]  = mapped_column(Numeric(18, 3), default=0)
    accum_dep_after:  Mapped[Decimal]  = mapped_column(Numeric(18, 3), nullable=False)
    nbv_after:      Mapped[Decimal]    = mapped_column(Numeric(18, 3), nullable=False)
    je_id:          Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial:      Mapped[Optional[str]]       = mapped_column(String(50), nullable=True)

    asset: Mapped["FixedAsset"] = relationship("FixedAsset", back_populates="schedules")

    __table_args__ = (
        UniqueConstraint("asset_id", "period_year", "period_month", name="uq_dep_asset_period"),
        CheckConstraint("period_month BETWEEN 1 AND 12", name="ck_dep_month"),
        CheckConstraint("dep_amount >= 0", name="ck_dep_amount_positive"),
    )


class AssetDisposal(ERPModel, Base):
    """التخلص من أصل ثابت (بيع / إتلاف)."""
    __tablename__ = "ast_disposals"

    asset_id:       Mapped[uuid.UUID]  = mapped_column(UUID(as_uuid=True), ForeignKey("ast_assets.id"), nullable=False)
    asset_number:   Mapped[str]        = mapped_column(String(50),  nullable=False)
    disposal_date:  Mapped[str]        = mapped_column(Date,        nullable=False)
    disposal_type:  Mapped[str]        = mapped_column(String(20),  default="sale")  # sale|scrap
    sale_price:     Mapped[Decimal]    = mapped_column(Numeric(18, 3), default=0)
    nbv_at_disposal: Mapped[Decimal]   = mapped_column(Numeric(18, 3), nullable=False)
    gain_loss:      Mapped[Decimal]    = mapped_column(Numeric(18, 3), default=0)
    je_id:          Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial:      Mapped[Optional[str]]       = mapped_column(String(50), nullable=True)
    notes:          Mapped[Optional[str]]       = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("tenant_id", "asset_id", name="uq_disposal_asset"),
    )
