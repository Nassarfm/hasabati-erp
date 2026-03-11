"""
app/modules/accounting/models.py
══════════════════════════════════════════════════════════
Accounting module database models.

Tables:
  coa_accounts      — Chart of Accounts
  journal_entries   — JE headers (immutable after posting)
  je_lines          — JE lines (double-entry)
  account_balances  — Running balances per account/period
  fiscal_periods    — Year/month periods with lock state
  fiscal_locks      — Hard/soft locks on periods
  audit_log         — Immutable accounting audit trail

Immutability rules enforced at service layer:
  - Posted JE: never UPDATE, only REVERSE
  - Account balances: only updated by PostingEngine
  - Fiscal locks: only admin can unlock hard locks
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
    ForeignKey, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.mixins import ERPModel, SoftDeleteMixin, TimestampMixin, TenantMixin


# ══════════════════════════════════════════════════════════
# Enums
# ══════════════════════════════════════════════════════════
class AccountType(str, enum.Enum):
    ASSET     = "asset"       # أصول
    LIABILITY = "liability"   # التزامات
    EQUITY    = "equity"      # حقوق الملكية
    REVENUE   = "revenue"     # إيرادات
    EXPENSE   = "expense"     # مصاريف


class AccountNature(str, enum.Enum):
    DEBIT  = "debit"    # طبيعة مدينة (assets, expenses)
    CREDIT = "credit"   # طبيعة دائنة (liabilities, equity, revenue)


class JEStatus(str, enum.Enum):
    DRAFT    = "draft"      # مسودة — يمكن التعديل
    POSTED   = "posted"     # مرحَّل — لا تعديل، فقط عكس
    REVERSED = "reversed"   # معكوس — مغلق نهائياً
    VOID     = "void"       # ملغى (draft only)


class JEType(str, enum.Enum):
    GJE  = "GJE"   # General Journal Entry
    SJE  = "SJE"   # Sales Journal Entry
    PJE  = "PJE"   # Purchase Journal Entry
    PIE  = "PIE"   # Purchase Invoice Entry
    PAY  = "PAY"   # Payment Entry
    RCV  = "RCV"   # Receipt Entry
    PRV  = "PRV"   # Payroll Entry
    DEP  = "DEP"   # Depreciation Entry
    ADJ  = "ADJ"   # Adjustment Entry
    REV  = "REV"   # Reversal Entry


class FiscalLockType(str, enum.Enum):
    SOFT = "soft"   # تحذير — يمكن تجاوزه بصلاحية مدير
    HARD = "hard"   # إغلاق نهائي — لا يمكن تجاوزه أبداً


class FiscalPeriodStatus(str, enum.Enum):
    OPEN   = "open"
    CLOSED = "closed"
    LOCKED = "locked"


# ══════════════════════════════════════════════════════════
# 1. Chart of Accounts
# ══════════════════════════════════════════════════════════
class ChartOfAccount(ERPModel, Base):
    """
    دليل الحسابات — Chart of Accounts.
    Supports hierarchical structure (parent_id → children).
    Only postable=True accounts accept journal lines.
    """
    __tablename__ = "coa_accounts"

    code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    account_type: Mapped[AccountType] = mapped_column(
        String(20), nullable=False, index=True
    )
    account_nature: Mapped[AccountNature] = mapped_column(
        String(10), nullable=False
    )

    # Hierarchy
    parent_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("coa_accounts.id"),
        nullable=True,
    )
    level: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    # Flags
    postable: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    allow_direct_posting: Mapped[bool] = mapped_column(Boolean, default=True)

    # Opening balance
    opening_balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # Relationships
    children: Mapped[List["ChartOfAccount"]] = relationship(
        "ChartOfAccount", back_populates="parent"
    )
    parent: Mapped[Optional["ChartOfAccount"]] = relationship(
        "ChartOfAccount", back_populates="children", remote_side="ChartOfAccount.id"
    )
    balances: Mapped[List["AccountBalance"]] = relationship(
        "AccountBalance", back_populates="account"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_coa_tenant_code"),
    )


# ══════════════════════════════════════════════════════════
# 2. Journal Entry (Header)
# ══════════════════════════════════════════════════════════
class JournalEntry(ERPModel, Base):
    """
    قيد محاسبي — Journal Entry header.
    IMMUTABLE after posting: no updates, only reversals.
    """
    __tablename__ = "journal_entries"

    serial: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    je_type: Mapped[JEType] = mapped_column(String(10), nullable=False, index=True)
    status: Mapped[JEStatus] = mapped_column(
        String(20), default=JEStatus.DRAFT, nullable=False, index=True
    )

    entry_date: Mapped[str] = mapped_column(Date, nullable=False, index=True)
    posting_date: Mapped[Optional[str]] = mapped_column(Date, nullable=True)

    description: Mapped[str] = mapped_column(String(500), nullable=False)
    reference: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Source document link (invoice, GRN, payroll, etc.)
    source_module: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    source_doc_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Totals (denormalized for fast reporting)
    total_debit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    total_credit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # Fiscal period
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fiscal_month: Mapped[int] = mapped_column(Integer, nullable=False)

    # Dimensions
    branch_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    cost_center: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Posting info
    posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Reversal link
    reversed_by_je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    reverses_je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )

    # Idempotency
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )

    # Notes
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    lines: Mapped[List["JournalEntryLine"]] = relationship(
        "JournalEntryLine",
        back_populates="journal_entry",
        cascade="all, delete-orphan",
        order_by="JournalEntryLine.line_order",
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "serial", name="uq_je_tenant_serial"),
        Index("ix_je_tenant_date", "tenant_id", "entry_date"),
        Index("ix_je_tenant_source", "tenant_id", "source_doc_id"),
        CheckConstraint("total_debit >= 0", name="ck_je_debit_positive"),
        CheckConstraint("total_credit >= 0", name="ck_je_credit_positive"),
    )


# ══════════════════════════════════════════════════════════
# 3. Journal Entry Line
# ══════════════════════════════════════════════════════════
class JournalEntryLine(ERPModel, Base):
    """
    سطر القيد المحاسبي.
    Each line is either debit OR credit — never both.
    """
    __tablename__ = "je_lines"

    journal_entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("journal_entries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    line_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    account_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    account_name: Mapped[str] = mapped_column(String(255), nullable=False)

    description: Mapped[str] = mapped_column(String(500), nullable=False)

    debit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    credit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    # Dimensions per line
    branch_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    cost_center: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    project_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Relationships
    journal_entry: Mapped["JournalEntry"] = relationship(
        "JournalEntry", back_populates="lines"
    )

    __table_args__ = (
        CheckConstraint("debit >= 0", name="ck_jel_debit_positive"),
        CheckConstraint("credit >= 0", name="ck_jel_credit_positive"),
        CheckConstraint(
            "NOT (debit > 0 AND credit > 0)",
            name="ck_jel_not_both_sides",
        ),
        CheckConstraint(
            "debit > 0 OR credit > 0",
            name="ck_jel_at_least_one_side",
        ),
    )


# ══════════════════════════════════════════════════════════
# 4. Account Balance (running total per period)
# ══════════════════════════════════════════════════════════
class AccountBalance(ERPModel, Base):
    """
    رصيد الحساب — مُحدَّث بعد كل ترحيل.
    One row per (tenant, account, fiscal_year, fiscal_month).
    Updated atomically inside PostingEngine transaction.
    """
    __tablename__ = "account_balances"

    account_code: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )
    account_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("coa_accounts.id"),
        nullable=True,
    )

    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    fiscal_month: Mapped[int] = mapped_column(Integer, nullable=False)  # 0 = annual

    debit_total: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    credit_total: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )  # debit_total - credit_total (signed)

    opening_balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    closing_balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    last_posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    account: Mapped[Optional["ChartOfAccount"]] = relationship(
        "ChartOfAccount", back_populates="balances"
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "account_code", "fiscal_year", "fiscal_month",
            name="uq_balance_tenant_acc_period",
        ),
        Index("ix_balance_tenant_year", "tenant_id", "fiscal_year"),
    )


# ══════════════════════════════════════════════════════════
# 5. Fiscal Period
# ══════════════════════════════════════════════════════════
class FiscalPeriod(ERPModel, Base):
    """
    السنة/الشهر المالي — Fiscal Period.
    """
    __tablename__ = "fiscal_periods"

    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fiscal_month: Mapped[int] = mapped_column(Integer, nullable=False)  # 1-12 or 0 for annual
    name_ar: Mapped[str] = mapped_column(String(100), nullable=False)

    status: Mapped[FiscalPeriodStatus] = mapped_column(
        String(20), default=FiscalPeriodStatus.OPEN, nullable=False
    )

    start_date: Mapped[str] = mapped_column(Date, nullable=False)
    end_date: Mapped[str] = mapped_column(Date, nullable=False)

    is_adjustment_period: Mapped[bool] = mapped_column(Boolean, default=False)

    closed_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    closed_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    locks: Mapped[List["FiscalLock"]] = relationship(
        "FiscalLock", back_populates="period"
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "fiscal_year", "fiscal_month",
            name="uq_fp_tenant_year_month",
        ),
    )


# ══════════════════════════════════════════════════════════
# 6. Fiscal Lock
# ══════════════════════════════════════════════════════════
class FiscalLock(ERPModel, Base):
    """
    قفل الفترة المالية.
    soft = تحذير، يحتاج صلاحية للمرور.
    hard = إغلاق نهائي، لا أحد يتجاوزه.
    """
    __tablename__ = "fiscal_locks"

    period_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("fiscal_periods.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    fiscal_month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    lock_type: Mapped[FiscalLockType] = mapped_column(
        String(10), nullable=False
    )
    reason: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    locked_by: Mapped[str] = mapped_column(String(255), nullable=False)
    locked_at: Mapped[str] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    unlocked_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    unlocked_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    period: Mapped["FiscalPeriod"] = relationship("FiscalPeriod", back_populates="locks")

    __table_args__ = (
        Index("ix_lock_tenant_period", "tenant_id", "fiscal_year", "fiscal_month"),
    )


# ══════════════════════════════════════════════════════════
# 7. Accounting Audit Log (immutable)
# ══════════════════════════════════════════════════════════
class AccountingAuditLog(TenantMixin, TimestampMixin, Base):
    """
    سجل التدقيق المحاسبي — Immutable.
    No updates, no deletes, ever.
    Records every posting / reversal / lock action.
    """
    __tablename__ = "accounting_audit_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    action: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    # JE_POSTED | JE_REVERSED | JE_VOIDED | PERIOD_LOCKED | PERIOD_UNLOCKED

    # Who
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    user_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # What
    je_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    je_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    fiscal_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fiscal_month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Amounts
    total_debit: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3), nullable=True)
    total_credit: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3), nullable=True)

    # Context
    source_module: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    extra_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Request tracing
    request_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    __table_args__ = (
        Index("ix_aaudit_tenant_action", "tenant_id", "action"),
        Index("ix_aaudit_je_id", "je_id"),
    )
