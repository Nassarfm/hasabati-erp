"""
app/modules/purchases/models.py
══════════════════════════════════════════════════════════
Purchases module database models.

Tables:
  pur_suppliers          — الموردون
  pur_purchase_orders    — أوامر الشراء (PO)
  pur_po_lines           — بنود أمر الشراء
  pur_grn                — إشعار استلام البضاعة (GRN)
  pur_grn_lines          — بنود GRN
  pur_vendor_invoices    — فواتير الموردين
  pur_vendor_inv_lines   — بنود فاتورة المورد

3-Way Match:
  PO ──► GRN ──► VendorInvoice
  كل GRN يرتبط بـ PO
  كل VendorInvoice يرتبط بـ GRN + PO
  المطابقة: qty_invoiced ≤ qty_received ≤ qty_ordered
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import enum
import uuid
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime,
    Index, Integer, Numeric, String, Text,
    UniqueConstraint, ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.mixins import ERPModel, SoftDeleteMixin


# ══════════════════════════════════════════════════════════
# Enums
# ══════════════════════════════════════════════════════════
class POStatus(str, enum.Enum):
    DRAFT       = "draft"
    APPROVED    = "approved"
    PARTIALLY_RECEIVED = "partially_received"
    RECEIVED    = "received"       # fully received
    INVOICED    = "invoiced"       # fully invoiced
    CLOSED      = "closed"
    CANCELLED   = "cancelled"


class GRNStatus(str, enum.Enum):
    DRAFT    = "draft"
    POSTED   = "posted"
    REVERSED = "reversed"


class VendorInvoiceStatus(str, enum.Enum):
    DRAFT    = "draft"
    MATCHED  = "matched"     # 3-way match passed
    POSTED   = "posted"
    PAID     = "paid"
    CANCELLED = "cancelled"


class PaymentTerm(str, enum.Enum):
    CASH   = "cash"
    NET_15 = "net_15"
    NET_30 = "net_30"
    NET_60 = "net_60"
    NET_90 = "net_90"


# ══════════════════════════════════════════════════════════
# 1. Supplier Master
# ══════════════════════════════════════════════════════════
class Supplier(ERPModel, SoftDeleteMixin, Base):
    """المورد — Supplier master."""
    __tablename__ = "pur_suppliers"

    code: Mapped[str]    = mapped_column(String(30), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    phone:   Mapped[Optional[str]] = mapped_column(String(30),  nullable=True)
    email:   Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    city:    Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    country: Mapped[str]           = mapped_column(String(10),  default="SA")

    vat_number: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    cr_number:  Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    payment_term: Mapped[PaymentTerm] = mapped_column(
        String(20), default=PaymentTerm.NET_30, nullable=False
    )
    credit_limit:  Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    discount_pct:  Mapped[Decimal] = mapped_column(Numeric(5, 2),  default=0)

    # Accounting
    ap_account: Mapped[str] = mapped_column(String(20), default="2101")

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    purchase_orders: Mapped[List["PurchaseOrder"]] = relationship(
        "PurchaseOrder", back_populates="supplier"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_supplier_tenant_code"),
    )


# ══════════════════════════════════════════════════════════
# 2. Purchase Order
# ══════════════════════════════════════════════════════════
class PurchaseOrder(ERPModel, Base):
    """أمر الشراء — Purchase Order header."""
    __tablename__ = "pur_purchase_orders"

    po_number:   Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    po_date:     Mapped[str] = mapped_column(Date, nullable=False, index=True)
    required_date: Mapped[Optional[str]] = mapped_column(Date, nullable=True)
    status: Mapped[POStatus] = mapped_column(
        String(30), default=POStatus.DRAFT, nullable=False, index=True
    )

    supplier_id:   Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_suppliers.id"),
        nullable=False, index=True,
    )
    supplier_code: Mapped[str] = mapped_column(String(30),  nullable=False)
    supplier_name: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_code: Mapped[str] = mapped_column(String(20), default="MAIN")

    # Amounts
    subtotal:       Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    taxable_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    vat_amount:     Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    total_amount:   Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    payment_term: Mapped[PaymentTerm] = mapped_column(String(20), default=PaymentTerm.NET_30)
    discount_pct: Mapped[Decimal]     = mapped_column(Numeric(5, 2), default=0)

    # Tracking
    qty_received_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=0)
    qty_invoiced_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=0)

    approved_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    approved_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True), nullable=True)

    notes:     Mapped[Optional[str]] = mapped_column(Text,        nullable=True)
    reference: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    supplier: Mapped["Supplier"] = relationship("Supplier", back_populates="purchase_orders")
    lines: Mapped[List["POLine"]] = relationship(
        "POLine", back_populates="po", cascade="all, delete-orphan"
    )
    grns: Mapped[List["GRN"]] = relationship("GRN", back_populates="po")

    __table_args__ = (
        UniqueConstraint("tenant_id", "po_number", name="uq_po_tenant_number"),
        Index("ix_po_tenant_date", "tenant_id", "po_date"),
    )


class POLine(ERPModel, Base):
    """بند أمر الشراء."""
    __tablename__ = "pur_po_lines"

    po_id:       Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_purchase_orders.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    line_number: Mapped[int]     = mapped_column(Integer, nullable=False)
    product_id:  Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    product_code: Mapped[str]   = mapped_column(String(50),  nullable=False)
    product_name: Mapped[str]   = mapped_column(String(255), nullable=False)
    unit_of_measure: Mapped[str] = mapped_column(String(20), default="قطعة")

    qty_ordered:  Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    qty_received: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    qty_invoiced: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    qty_pending:  Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    unit_price:      Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    discount_pct:    Mapped[Decimal] = mapped_column(Numeric(5, 2),  default=0)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    line_total:      Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    vat_rate:        Mapped[Decimal] = mapped_column(Numeric(5, 2),  default=15)
    vat_amount:      Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    line_total_with_vat: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)

    inventory_account: Mapped[str] = mapped_column(String(20), default="1301")
    notes: Mapped[Optional[str]]   = mapped_column(String(500), nullable=True)

    po: Mapped["PurchaseOrder"] = relationship("PurchaseOrder", back_populates="lines")

    __table_args__ = (
        CheckConstraint("qty_ordered > 0", name="ck_poline_qty_positive"),
        CheckConstraint("unit_price >= 0",  name="ck_poline_price_positive"),
    )


# ══════════════════════════════════════════════════════════
# 3. GRN — Goods Receipt Note
# ══════════════════════════════════════════════════════════
class GRN(ERPModel, Base):
    """إشعار استلام البضاعة — Goods Receipt Note."""
    __tablename__ = "pur_grn"

    grn_number:    Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    grn_date:      Mapped[str] = mapped_column(Date, nullable=False, index=True)
    status: Mapped[GRNStatus]  = mapped_column(
        String(20), default=GRNStatus.DRAFT, nullable=False
    )

    po_id:          Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_purchase_orders.id"),
        nullable=False, index=True,
    )
    po_number:      Mapped[str] = mapped_column(String(50),  nullable=False)
    supplier_id:    Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    supplier_code:  Mapped[str] = mapped_column(String(30),  nullable=False)
    supplier_name:  Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_code: Mapped[str] = mapped_column(String(20),  default="MAIN")

    total_cost: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    # Accounting
    je_id:     Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial: Mapped[Optional[str]]       = mapped_column(String(50), nullable=True)

    posted_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True), nullable=True)
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    delivery_note: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notes:         Mapped[Optional[str]] = mapped_column(Text,        nullable=True)

    po:    Mapped["PurchaseOrder"]   = relationship("PurchaseOrder", back_populates="grns")
    lines: Mapped[List["GRNLine"]]   = relationship(
        "GRNLine", back_populates="grn", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "grn_number", name="uq_grn_tenant_number"),
        Index("ix_grn_tenant_date", "tenant_id", "grn_date"),
    )


class GRNLine(ERPModel, Base):
    """بند GRN."""
    __tablename__ = "pur_grn_lines"

    grn_id:      Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_grn.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    po_line_id:  Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    line_number: Mapped[int]     = mapped_column(Integer,     nullable=False)
    product_id:  Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    product_code: Mapped[str]   = mapped_column(String(50),  nullable=False)
    product_name: Mapped[str]   = mapped_column(String(255), nullable=False)

    qty_received: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    unit_cost:    Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    total_cost:   Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)

    # WAC snapshot
    wac_before: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)
    wac_after:  Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)

    inventory_account: Mapped[str]         = mapped_column(String(20), default="1301")
    movement_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    notes: Mapped[Optional[str]]            = mapped_column(String(500), nullable=True)

    grn: Mapped["GRN"] = relationship("GRN", back_populates="lines")

    __table_args__ = (
        CheckConstraint("qty_received > 0", name="ck_grnline_qty_positive"),
        CheckConstraint("unit_cost >= 0",   name="ck_grnline_cost_positive"),
    )


# ══════════════════════════════════════════════════════════
# 4. Vendor Invoice
# ══════════════════════════════════════════════════════════
class VendorInvoice(ERPModel, Base):
    """فاتورة المورد — linked to PO + GRN (3-way match)."""
    __tablename__ = "pur_vendor_invoices"

    vi_number:      Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    vendor_ref:     Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    invoice_date:   Mapped[str] = mapped_column(Date, nullable=False, index=True)
    due_date:       Mapped[Optional[str]] = mapped_column(Date, nullable=True)
    status: Mapped[VendorInvoiceStatus] = mapped_column(
        String(20), default=VendorInvoiceStatus.DRAFT, nullable=False, index=True
    )

    po_id:          Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_purchase_orders.id"),
        nullable=False, index=True,
    )
    po_number:      Mapped[str] = mapped_column(String(50),  nullable=False)
    grn_id:         Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_grn.id"), nullable=True
    )
    grn_number:     Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    supplier_id:   Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    supplier_code: Mapped[str]       = mapped_column(String(30),  nullable=False)
    supplier_name: Mapped[str]       = mapped_column(String(255), nullable=False)

    # Amounts
    subtotal:        Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    taxable_amount:  Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    vat_amount:      Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    total_amount:    Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    paid_amount:     Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    balance_due:     Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    payment_term: Mapped[PaymentTerm] = mapped_column(String(20), default=PaymentTerm.NET_30)
    ap_account:   Mapped[str]         = mapped_column(String(20), default="2101")

    # 3-Way Match result
    match_status:  Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    match_notes:   Mapped[Optional[str]] = mapped_column(Text,       nullable=True)

    # Accounting
    je_id:     Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial: Mapped[Optional[str]]       = mapped_column(String(50), nullable=True)

    posted_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True), nullable=True)
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    lines: Mapped[List["VendorInvoiceLine"]] = relationship(
        "VendorInvoiceLine", back_populates="vendor_invoice", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "vi_number", name="uq_vi_tenant_number"),
        Index("ix_vi_tenant_date", "tenant_id", "invoice_date"),
    )


class VendorInvoiceLine(ERPModel, Base):
    """بند فاتورة المورد."""
    __tablename__ = "pur_vendor_inv_lines"

    vi_id:       Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pur_vendor_invoices.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    po_line_id:  Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    grn_line_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    line_number: Mapped[int]     = mapped_column(Integer,     nullable=False)

    product_code: Mapped[str]   = mapped_column(String(50),  nullable=False)
    product_name: Mapped[str]   = mapped_column(String(255), nullable=False)

    qty_ordered:  Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    qty_received: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    qty_invoiced: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)

    unit_price:      Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    discount_pct:    Mapped[Decimal] = mapped_column(Numeric(5, 2),  default=0)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    line_total:      Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    vat_rate:        Mapped[Decimal] = mapped_column(Numeric(5, 2),  default=15)
    vat_amount:      Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    line_total_with_vat: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)

    # Match result per line
    match_ok:    Mapped[bool]            = mapped_column(Boolean,     default=True)
    match_notes: Mapped[Optional[str]]   = mapped_column(String(500), nullable=True)

    inventory_account: Mapped[str] = mapped_column(String(20), default="1301")
    notes: Mapped[Optional[str]]   = mapped_column(String(500), nullable=True)

    vendor_invoice: Mapped["VendorInvoice"] = relationship(
        "VendorInvoice", back_populates="lines"
    )

    __table_args__ = (
        CheckConstraint("qty_invoiced > 0", name="ck_viline_qty_positive"),
    )
