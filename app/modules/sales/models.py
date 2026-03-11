"""
app/modules/sales/models.py
══════════════════════════════════════════════════════════
Sales module database models.

Tables:
  sal_customers         — العملاء
  sal_invoices          — فواتير المبيعات (رأس)
  sal_invoice_lines     — بنود الفاتورة
  sal_returns           — مرتجعات المبيعات (رأس)
  sal_return_lines      — بنود المرتجع

Design:
  - Invoice DRAFT → POSTED (immutable after posting)
  - Posting creates: JE (AR/Revenue/VAT/COGS) + StockMovement
  - Return creates reversal JE + SALES_RETURN movement
  - All amounts stored in SAR (no multi-currency in MVP)
  - VAT 15% default (ZATCA standard)
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
class InvoiceStatus(str, enum.Enum):
    DRAFT     = "draft"
    POSTED    = "posted"
    PARTIALLY_RETURNED = "partially_returned"
    RETURNED  = "returned"
    CANCELLED = "cancelled"


class PaymentTerm(str, enum.Enum):
    CASH       = "cash"        # نقداً
    NET_15     = "net_15"      # 15 يوم
    NET_30     = "net_30"      # 30 يوم
    NET_60     = "net_60"      # 60 يوم
    NET_90     = "net_90"      # 90 يوم


class CustomerType(str, enum.Enum):
    INDIVIDUAL = "individual"   # فرد
    COMPANY    = "company"      # شركة
    GOVERNMENT = "government"   # جهة حكومية


class ReturnStatus(str, enum.Enum):
    DRAFT    = "draft"
    POSTED   = "posted"
    CANCELLED = "cancelled"


# ══════════════════════════════════════════════════════════
# 1. Customer Master
# ══════════════════════════════════════════════════════════
class Customer(ERPModel, SoftDeleteMixin, Base):
    """
    العميل — Customer master.
    Referenced by invoices and returns.
    """
    __tablename__ = "sal_customers"

    # Identity
    code: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    customer_type: Mapped[CustomerType] = mapped_column(
        String(20), default=CustomerType.COMPANY, nullable=False
    )

    # Contact
    phone: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    country: Mapped[str] = mapped_column(String(10), default="SA")

    # ZATCA / Tax
    vat_number: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    cr_number: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Commercial
    payment_term: Mapped[PaymentTerm] = mapped_column(
        String(20), default=PaymentTerm.NET_30, nullable=False
    )
    credit_limit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    discount_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=0, nullable=False
    )

    # Accounting
    ar_account: Mapped[str] = mapped_column(
        String(20), default="1201", nullable=False
    )

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    invoices: Mapped[List["SalesInvoice"]] = relationship(
        "SalesInvoice", back_populates="customer"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_customer_tenant_code"),
        CheckConstraint("credit_limit >= 0", name="ck_customer_credit_positive"),
    )


# ══════════════════════════════════════════════════════════
# 2. Sales Invoice Header
# ══════════════════════════════════════════════════════════
class SalesInvoice(ERPModel, Base):
    """
    فاتورة المبيعات — Sales Invoice header.
    IMMUTABLE after posting (status = posted).
    """
    __tablename__ = "sal_invoices"

    # Document identity
    invoice_number: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )
    invoice_date: Mapped[str] = mapped_column(Date, nullable=False, index=True)
    due_date: Mapped[Optional[str]] = mapped_column(Date, nullable=True)
    status: Mapped[InvoiceStatus] = mapped_column(
        String(30), default=InvoiceStatus.DRAFT, nullable=False, index=True
    )

    # Customer
    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sal_customers.id"),
        nullable=False,
        index=True,
    )
    customer_code: Mapped[str] = mapped_column(String(30), nullable=False)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=False)
    customer_vat: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Warehouse
    warehouse_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    warehouse_code: Mapped[str] = mapped_column(String(20), default="MAIN")

    # Amounts (all in SAR)
    subtotal: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )   # before discount & VAT
    discount_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    taxable_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )   # subtotal - discount
    vat_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )   # 15% of taxable
    total_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )   # taxable + vat
    paid_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    balance_due: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # COGS (calculated on posting)
    total_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    gross_profit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # Commercial
    payment_term: Mapped[PaymentTerm] = mapped_column(
        String(20), default=PaymentTerm.NET_30, nullable=False
    )
    discount_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=0, nullable=False
    )

    # Accounting links
    je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    je_serial: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    ar_account: Mapped[str] = mapped_column(String(20), default="1201")

    # Posting info
    posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Return tracking
    returned_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reference: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Relationships
    customer: Mapped["Customer"] = relationship(
        "Customer", back_populates="invoices"
    )
    lines: Mapped[List["SalesInvoiceLine"]] = relationship(
        "SalesInvoiceLine",
        back_populates="invoice",
        cascade="all, delete-orphan",
    )
    returns: Mapped[List["SalesReturn"]] = relationship(
        "SalesReturn", back_populates="invoice"
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "invoice_number",
            name="uq_invoice_tenant_number",
        ),
        CheckConstraint("total_amount >= 0", name="ck_invoice_total_positive"),
        Index("ix_invoice_tenant_date", "tenant_id", "invoice_date"),
        Index("ix_invoice_tenant_customer", "tenant_id", "customer_id"),
    )


# ══════════════════════════════════════════════════════════
# 3. Sales Invoice Line
# ══════════════════════════════════════════════════════════
class SalesInvoiceLine(ERPModel, Base):
    """بند فاتورة المبيعات."""
    __tablename__ = "sal_invoice_lines"

    invoice_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sal_invoices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)

    # Product
    product_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    product_code: Mapped[str] = mapped_column(String(50), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit_of_measure: Mapped[str] = mapped_column(String(20), default="قطعة")

    # Quantities & Prices
    qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    discount_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=0, nullable=False
    )
    discount_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    line_total: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), nullable=False
    )   # qty × unit_price - discount
    vat_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=15, nullable=False
    )
    vat_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    line_total_with_vat: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), nullable=False
    )

    # COGS (filled on posting)
    unit_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), default=0, nullable=False
    )
    total_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # Accounting
    revenue_account: Mapped[str] = mapped_column(String(20), default="4001")
    cogs_account: Mapped[str] = mapped_column(String(20), default="5001")
    inventory_account: Mapped[str] = mapped_column(String(20), default="1301")

    # Return tracking
    qty_returned: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    notes: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    invoice: Mapped["SalesInvoice"] = relationship(
        "SalesInvoice", back_populates="lines"
    )

    __table_args__ = (
        CheckConstraint("qty > 0", name="ck_inv_line_qty_positive"),
        CheckConstraint("unit_price >= 0", name="ck_inv_line_price_positive"),
    )


# ══════════════════════════════════════════════════════════
# 4. Sales Return Header
# ══════════════════════════════════════════════════════════
class SalesReturn(ERPModel, Base):
    """
    مرتجع مبيعات — Sales Return (Credit Note).
    Must reference original invoice.
    """
    __tablename__ = "sal_returns"

    return_number: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )
    return_date: Mapped[str] = mapped_column(Date, nullable=False)
    status: Mapped[ReturnStatus] = mapped_column(
        String(20), default=ReturnStatus.DRAFT, nullable=False
    )

    # Original invoice
    invoice_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sal_invoices.id"),
        nullable=False,
        index=True,
    )
    invoice_number: Mapped[str] = mapped_column(String(50), nullable=False)

    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    customer_code: Mapped[str] = mapped_column(String(30), nullable=False)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_code: Mapped[str] = mapped_column(String(20), default="MAIN")

    # Amounts
    subtotal: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    vat_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    total_cost: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    # Accounting
    je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    je_serial: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    reason: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    invoice: Mapped["SalesInvoice"] = relationship(
        "SalesInvoice", back_populates="returns"
    )
    lines: Mapped[List["SalesReturnLine"]] = relationship(
        "SalesReturnLine",
        back_populates="sales_return",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "return_number",
            name="uq_return_tenant_number",
        ),
    )


# ══════════════════════════════════════════════════════════
# 5. Sales Return Line
# ══════════════════════════════════════════════════════════
class SalesReturnLine(ERPModel, Base):
    """بند مرتجع المبيعات."""
    __tablename__ = "sal_return_lines"

    return_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sal_returns.id", ondelete="CASCADE"),
        nullable=False,
    )
    invoice_line_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)

    product_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    product_code: Mapped[str] = mapped_column(String(50), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)

    qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    vat_rate: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=15)
    line_total: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    vat_amount: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    line_total_with_vat: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    unit_cost: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)
    total_cost: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)

    sales_return: Mapped["SalesReturn"] = relationship(
        "SalesReturn", back_populates="lines"
    )

    __table_args__ = (
        CheckConstraint("qty > 0", name="ck_ret_line_qty_positive"),
    )
