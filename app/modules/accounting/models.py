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
    ASSET     = "asset"
    LIABILITY = "liability"
    EQUITY    = "equity"
    REVENUE   = "revenue"
    EXPENSE   = "expense"


class AccountNature(str, enum.Enum):
    DEBIT  = "debit"
    CREDIT = "credit"


class JEStatus(str, enum.Enum):
    DRAFT    = "draft"
    POSTED   = "posted"
    REVERSED = "reversed"
    VOID     = "void"


class JEType(str, enum.Enum):
    GJE  = "GJE"
    SJE  = "SJE"
    PJE  = "PJE"
    PIE  = "PIE"
    PAY  = "PAY"
    RCV  = "RCV"
    PRV  = "PRV"
    DEP  = "DEP"
    ADJ  = "ADJ"
    REV  = "REV"


class FiscalLockType(str, enum.Enum):
    SOFT = "soft"
    HARD = "hard"


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

    # حقول القوائم المالية
    function_type: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )  # BS | PL | BS/PL
    grp: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )  # Group
    sub_group: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )  # Sub-Group
    cash_flow_type: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # operating | investing | financing | none
    dimension_required: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )  # هل يتطلب بُعد عند الترحيل

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

    source_module: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    source_doc_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    total_debit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    total_credit: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fiscal_month: Mapped[int] = mapped_column(Integer, nullable=False)

    branch_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    cost_center: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    reversed_by_je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    reverses_je_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )

    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

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

    branch_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    cost_center: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    project_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

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
    fiscal_month: Mapped[int] = mapped_column(Integer, nullable=False)

    debit_total: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    credit_total: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    opening_balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )
    closing_balance: Mapped[Decimal] = mapped_column(
        Numeric(18, 3), default=0, nullable=False
    )

    last_posted_at: Mapped[Optional[str]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

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
    __tablename__ = "fiscal_periods"

    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fiscal_month: Mapped[int] = mapped_column(Integer, nullable=False)
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

    period: Mapped["FiscalPeriod"] = relationship("FiscalPeriod", back_populates="locks")

    __table_args__ = (
        Index("ix_lock_tenant_period", "tenant_id", "fiscal_year", "fiscal_month"),
    )


# ══════════════════════════════════════════════════════════
# 7. Accounting Audit Log (immutable)
# ══════════════════════════════════════════════════════════
class AccountingAuditLog(TenantMixin, TimestampMixin, Base):
    __tablename__ = "accounting_audit_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    action: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    user_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    je_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    je_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    fiscal_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fiscal_month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    total_debit: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3), nullable=True)
    total_credit: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3), nullable=True)

    source_module: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source_doc_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    extra_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    request_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    __table_args__ = (
        Index("ix_aaudit_tenant_action", "tenant_id", "action"),
        Index("ix_aaudit_je_id", "je_id"),
    )
