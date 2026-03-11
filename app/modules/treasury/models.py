"""
app/modules/treasury/models.py — Treasury Module
Tables: tr_bank_accounts, tr_cash_transactions, tr_bank_reconciliation
"""
from __future__ import annotations
import enum, uuid
from decimal import Decimal
from typing import List, Optional
from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime, Numeric,
    String, Text, UniqueConstraint, ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.base import Base
from app.db.mixins import ERPModel, SoftDeleteMixin


class AccountType(str, enum.Enum):
    CASH = "cash"; BANK = "bank"; PETTY_CASH = "petty_cash"

class TxType(str, enum.Enum):
    RECEIPT  = "receipt"   # قبض
    PAYMENT  = "payment"   # دفع
    TRANSFER = "transfer"  # تحويل بين حسابات

class TxStatus(str, enum.Enum):
    DRAFT  = "draft"; POSTED = "posted"; RECONCILED = "reconciled"; REVERSED = "reversed"


class BankAccount(ERPModel, SoftDeleteMixin, Base):
    __tablename__ = "tr_bank_accounts"

    account_code:  Mapped[str]            = mapped_column(String(30),  nullable=False, index=True)
    account_name:  Mapped[str]            = mapped_column(String(255), nullable=False)
    account_type:  Mapped[AccountType]    = mapped_column(String(20),  default=AccountType.BANK)
    bank_name:     Mapped[Optional[str]]  = mapped_column(String(100), nullable=True)
    iban:          Mapped[Optional[str]]  = mapped_column(String(34),  nullable=True)
    swift:         Mapped[Optional[str]]  = mapped_column(String(20),  nullable=True)
    currency:      Mapped[str]            = mapped_column(String(3),   default="SAR")
    current_balance: Mapped[Decimal]      = mapped_column(Numeric(18, 3), default=0)
    gl_account:    Mapped[str]            = mapped_column(String(20),  default="1100")
    is_active:     Mapped[bool]           = mapped_column(Boolean,     default=True)
    notes:         Mapped[Optional[str]]  = mapped_column(Text,        nullable=True)

    transactions: Mapped[List["CashTransaction"]] = relationship(
        "CashTransaction", back_populates="bank_account", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "account_code", name="uq_ba_tenant_code"),
    )


class CashTransaction(ERPModel, Base):
    __tablename__ = "tr_cash_transactions"

    tx_number:       Mapped[str]         = mapped_column(String(50),  nullable=False, index=True)
    tx_date:         Mapped[str]         = mapped_column(Date,        nullable=False, index=True)
    tx_type:         Mapped[TxType]      = mapped_column(String(20),  nullable=False)
    status:          Mapped[TxStatus]    = mapped_column(String(20),  default=TxStatus.DRAFT)
    bank_account_id: Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), ForeignKey("tr_bank_accounts.id"), nullable=False, index=True)
    account_code:    Mapped[str]         = mapped_column(String(30),  nullable=False)
    amount:          Mapped[Decimal]     = mapped_column(Numeric(18, 3), nullable=False)
    balance_after:   Mapped[Decimal]     = mapped_column(Numeric(18, 3), default=0)
    counterpart_account: Mapped[str]     = mapped_column(String(20),  nullable=False)
    description:     Mapped[str]         = mapped_column(String(500), nullable=False)
    reference:       Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    party_name:      Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    je_id:           Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial:       Mapped[Optional[str]]       = mapped_column(String(50), nullable=True)
    posted_at:       Mapped[Optional[str]]       = mapped_column(DateTime(timezone=True), nullable=True)
    reconciled:      Mapped[bool]        = mapped_column(Boolean, default=False)
    notes:           Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    bank_account: Mapped["BankAccount"] = relationship("BankAccount", back_populates="transactions")

    __table_args__ = (
        UniqueConstraint("tenant_id", "tx_number", name="uq_tx_tenant_number"),
        CheckConstraint("amount > 0", name="ck_tx_amount_positive"),
    )
