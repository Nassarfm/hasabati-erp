"""
app/modules/inventory/models.py
══════════════════════════════════════════════════════════
Inventory module database models.

Tables:
  inv_products        — Product / Item master
  inv_warehouses      — Warehouse master
  inv_stock_balances  — Current stock per product/warehouse
  inv_movements       — Every stock movement (immutable log)
  inv_cost_layers     — WAC cost layers per product/warehouse
  inv_adjustments     — Stock count / adjustment documents

Design principles:
  - Movements are IMMUTABLE — never delete or update
  - Balance is always derived from movements (reconcilable)
  - WAC recalculated on every IN movement
  - Every movement that changes value → PostingEngine
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import enum
import uuid
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime, Index,
    Integer, Numeric, String, Text, UniqueConstraint,
    ForeignKey, func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.mixins import ERPModel, SoftDeleteMixin


# ══════════════════════════════════════════════════════════
# Enums
# ══════════════════════════════════════════════════════════
class ProductType(str, enum.Enum):
    STOCKABLE  = "stockable"   # بضاعة — tracked in inventory
    SERVICE    = "service"     # خدمة — no stock tracking
    CONSUMABLE = "consumable"  # مستهلكات — tracked but no valuation


class MovementType(str, enum.Enum):
    # IN movements (increase stock)
    PURCHASE_RECEIPT  = "PURCHASE_RECEIPT"   # استلام من مورد (GRN)
    SALES_RETURN      = "SALES_RETURN"       # مرتجع مبيعات
    PRODUCTION_IN     = "PRODUCTION_IN"      # إنتاج
    ADJUSTMENT_IN     = "ADJUSTMENT_IN"      # جرد — زيادة
    TRANSFER_IN       = "TRANSFER_IN"        # نقل وارد

    # OUT movements (decrease stock)
    SALES_ISSUE       = "SALES_ISSUE"        # إصدار للمبيعات
    PURCHASE_RETURN   = "PURCHASE_RETURN"    # مرتجع للمورد
    PRODUCTION_OUT    = "PRODUCTION_OUT"     # إصدار للإنتاج
    ADJUSTMENT_OUT    = "ADJUSTMENT_OUT"     # جرد — نقص
    TRANSFER_OUT      = "TRANSFER_OUT"       # نقل صادر

    # Special
    OPENING_BALANCE   = "OPENING_BALANCE"    # رصيد افتتاحي
    REVALUATION       = "REVALUATION"        # إعادة تقييم


class MovementStatus(str, enum.Enum):
    DRAFT    = "draft"
    POSTED   = "posted"    # stock balance updated + JE created
    REVERSED = "reversed"  # reversed by another movement


class CostingMethod(str, enum.Enum):
    WAC  = "WAC"   # Weighted Average Cost — الأكثر شيوعاً في السعودية
    FIFO = "FIFO"  # First In First Out
    LIFO = "LIFO"  # Last In First Out (not recommended)


class AdjustmentStatus(str, enum.Enum):
    DRAFT    = "draft"
    APPROVED = "approved"
    POSTED   = "posted"
    CANCELLED = "cancelled"


# ══════════════════════════════════════════════════════════
# 1. Product Master
# ══════════════════════════════════════════════════════════
class Product(ERPModel, SoftDeleteMixin, Base):
    """
    منتج / صنف — Product master.
    Central reference for all inventory operations.
    """
    __tablename__ = "inv_products"

    # Identity
    code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    barcode: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Classification
    product_type: Mapped[ProductType] = mapped_column(
        String(20), default=ProductType.STOCKABLE, nullable=False
    )
    category_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    unit_of_measure: Mapped[str] = mapped_column(String(20), default="قطعة")
    secondary_uom: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    uom_conversion: Mapped[Decimal] = mapped_column(
        Numeric(10, 4), default=1, nullable=False
    )

    # Costing
    costing_method: Mapped[CostingMethod] = mapped_column(
        String(10), default=CostingMethod.WAC, nullable=False
    )
    standard_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    last_purchase_price: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    average_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )

    # Pricing
    sale_price: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    vat_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=15, nullable=False
    )

    # Accounting
    inventory_account: Mapped[str] = mapped_column(
        String(20), default="1300", nullable=False
    )
    cogs_account: Mapped[str] = mapped_column(
        String(20), default="5001", nullable=False
    )
    purchase_account: Mapped[str] = mapped_column(
        String(20), default="1300", nullable=False
    )

    # Stock control
    track_stock: Mapped[bool] = mapped_column(Boolean, default=True)
    min_qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    max_qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    reorder_point: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    reorder_qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_purchasable: Mapped[bool] = mapped_column(Boolean, default=True)
    is_sellable: Mapped[bool] = mapped_column(Boolean, default=True)

    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    balances: Mapped[List["StockBalance"]] = relationship(
        "StockBalance", back_populates="product"
    )
    movements: Mapped[List["StockMovement"]] = relationship(
        "StockMovement", back_populates="product"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_product_tenant_code"),
        CheckConstraint("standard_cost >= 0", name="ck_product_cost_positive"),
        CheckConstraint("sale_price >= 0", name="ck_product_price_positive"),
    )


# ══════════════════════════════════════════════════════════
# 2. Warehouse Master
# ══════════════════════════════════════════════════════════
class Warehouse(ERPModel, Base):
    """
    مستودع — Warehouse / Storage location.
    """
    __tablename__ = "inv_warehouses"

    code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    allow_negative_stock: Mapped[bool] = mapped_column(Boolean, default=False)

    # Accounting override (optional — inherits from product if null)
    inventory_account: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    manager_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    balances: Mapped[List["StockBalance"]] = relationship(
        "StockBalance", back_populates="warehouse"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_warehouse_tenant_code"),
    )


# ══════════════════════════════════════════════════════════
# 3. Stock Balance (current snapshot)
# ══════════════════════════════════════════════════════════
class StockBalance(ERPModel, Base):
    """
    رصيد المخزون اللحظي — per product per warehouse.
    Updated atomically by InventoryService on every posted movement.
    One row per (tenant, product, warehouse).
    """
    __tablename__ = "inv_stock_balances"

    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_products.id"),
        nullable=False,
        index=True,
    )
    product_code: Mapped[str] = mapped_column(String(50), nullable=False)
    warehouse_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_warehouses.id"),
        nullable=False,
        index=True,
    )
    warehouse_code: Mapped[str] = mapped_column(String(20), nullable=False)

    # Quantities
    qty_on_hand: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    qty_reserved: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )  # reserved for open sales orders
    qty_available: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )  # on_hand - reserved
    qty_incoming: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )  # expected from open POs

    # Valuation (WAC)
    average_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    total_value: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )  # qty_on_hand × average_cost

    # Tracking
    last_movement_date: Mapped[Optional[str]] = mapped_column(Date, nullable=True)
    last_movement_type: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)

    # Relationships
    product: Mapped["Product"] = relationship("Product", back_populates="balances")
    warehouse: Mapped["Warehouse"] = relationship("Warehouse", back_populates="balances")

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "product_id", "warehouse_id",
            name="uq_stockbal_tenant_prod_wh",
        ),
        CheckConstraint("qty_on_hand >= -999999", name="ck_stockbal_qty"),
    )


# ══════════════════════════════════════════════════════════
# 4. Stock Movement (immutable ledger)
# ══════════════════════════════════════════════════════════
class StockMovement(ERPModel, Base):
    """
    حركة مخزون — Immutable movement record.
    Every IN/OUT/TRANSFER creates a new row.
    NEVER updated after posting.
    This is the inventory equivalent of journal_entries.
    """
    __tablename__ = "inv_movements"

    # Document identity
    movement_number: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )
    movement_type: Mapped[MovementType] = mapped_column(
        String(30), nullable=False, index=True
    )
    movement_date: Mapped[str] = mapped_column(Date, nullable=False, index=True)
    status: Mapped[MovementStatus] = mapped_column(
        String(20), default=MovementStatus.DRAFT, nullable=False
    )

    # Product & Warehouse
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_products.id"),
        nullable=False,
        index=True,
    )
    product_code: Mapped[str] = mapped_column(String(50), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_warehouses.id"),
        nullable=False,
    )
    warehouse_code: Mapped[str] = mapped_column(String(20), nullable=False)

    # Transfer destination (for TRANSFER_IN/OUT pairs)
    dest_warehouse_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_warehouses.id"),
        nullable=True,
    )
    dest_warehouse_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Quantity & Value
    qty: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), nullable=False
    )  # always positive — direction determined by movement_type
    unit_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), nullable=False
    )
    total_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), nullable=False
    )

    # WAC snapshot (after this movement)
    wac_before: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    wac_after: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    qty_before: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    qty_after: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # Source document link
    source_module: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    source_doc_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Accounting link
    je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    je_serial: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Reversal link
    reversed_by_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    reverses_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )

    # Posting info
    posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    product: Mapped["Product"] = relationship("Product", back_populates="movements")

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "movement_number",
            name="uq_movement_tenant_number",
        ),
        CheckConstraint("qty > 0", name="ck_movement_qty_positive"),
        CheckConstraint("unit_cost >= 0", name="ck_movement_cost_positive"),
        Index("ix_movement_tenant_date", "tenant_id", "movement_date"),
        Index("ix_movement_source", "tenant_id", "source_doc_id"),
    )


# ══════════════════════════════════════════════════════════
# 5. Stock Adjustment Document
# ══════════════════════════════════════════════════════════
class StockAdjustment(ERPModel, Base):
    """
    جرد المخزون — Stock count / adjustment document.
    Groups multiple adjustment lines under one document.
    Approved → generates ADJUSTMENT_IN/OUT movements.
    """
    __tablename__ = "inv_adjustments"

    adj_number: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    adj_date: Mapped[str] = mapped_column(Date, nullable=False)
    status: Mapped[AdjustmentStatus] = mapped_column(
        String(20), default=AdjustmentStatus.DRAFT, nullable=False
    )

    warehouse_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_warehouses.id"),
        nullable=False,
    )
    warehouse_code: Mapped[str] = mapped_column(String(20), nullable=False)

    reason: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    approved_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    approved_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    je_serial: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    lines: Mapped[List["StockAdjustmentLine"]] = relationship(
        "StockAdjustmentLine",
        back_populates="adjustment",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "adj_number", name="uq_adj_tenant_number"),
    )


class StockAdjustmentLine(ERPModel, Base):
    __tablename__ = "inv_adjustment_lines"

    adjustment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_adjustments.id", ondelete="CASCADE"),
        nullable=False,
    )
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inv_products.id"),
        nullable=False,
    )
    product_code: Mapped[str] = mapped_column(String(50), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)

    qty_system: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    qty_counted: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    qty_difference: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    # positive = overage (ADJUSTMENT_IN), negative = shortage (ADJUSTMENT_OUT)

    unit_cost: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    variance_value: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    notes: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    adjustment: Mapped["StockAdjustment"] = relationship(
        "StockAdjustment", back_populates="lines"
    )
