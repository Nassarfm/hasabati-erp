"""
app/modules/accounting/repository.py
══════════════════════════════════════════════════════════
Accounting data access layer.
All DB queries here — no raw SQL in services or routers.
Every query is automatically tenant-scoped.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

import structlog
from sqlalchemy import and_, desc, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.modules.accounting.models import (
    AccountBalance, AccountingAuditLog, ChartOfAccount,
    FiscalLock, FiscalPeriod, JournalEntry, JournalEntryLine,
    JEStatus, FiscalLockType,
)
from app.repositories.base_repo import BaseRepository

logger = structlog.get_logger(__name__)


# ══════════════════════════════════════════════════════════
# Chart of Accounts Repository
# ══════════════════════════════════════════════════════════
class COARepository(BaseRepository[ChartOfAccount]):
    model = ChartOfAccount

    async def get_by_code(self, code: str) -> Optional[ChartOfAccount]:
        result = await self.db.execute(
            self._base_query()
            .where(ChartOfAccount.code == code)
            .where(ChartOfAccount.is_active == True)
        )
        return result.scalar_one_or_none()

    async def get_postable_account(self, code: str) -> Optional[ChartOfAccount]:
        """Returns account only if it's active and postable."""
        result = await self.db.execute(
            self._base_query()
            .where(ChartOfAccount.code == code)
            .where(ChartOfAccount.is_active == True)
            .where(ChartOfAccount.postable == True)
        )
        return result.scalar_one_or_none()

    async def bulk_get_by_codes(self, codes: List[str]) -> dict[str, ChartOfAccount]:
        """Load multiple accounts at once — used by PostingEngine validation."""
        result = await self.db.execute(
            self._base_query()
            .where(ChartOfAccount.code.in_(codes))
            .where(ChartOfAccount.is_active == True)
        )
        return {acc.code: acc for acc in result.scalars().all()}

    async def list_active(self) -> List[ChartOfAccount]:
        result = await self.db.execute(
            self._base_query()
            .where(ChartOfAccount.is_active == True)
            .order_by(ChartOfAccount.code)
        )
        return list(result.scalars().all())


# ══════════════════════════════════════════════════════════
# Journal Entry Repository
# ══════════════════════════════════════════════════════════
class JournalEntryRepository(BaseRepository[JournalEntry]):
    model = JournalEntry

    async def get_with_lines(self, je_id: uuid.UUID) -> Optional[JournalEntry]:
        """Load JE with all lines in one query."""
        result = await self.db.execute(
            self._base_query()
            .where(JournalEntry.id == je_id)
            .options(selectinload(JournalEntry.lines))
        )
        return result.scalar_one_or_none()

    async def get_by_serial(self, serial: str) -> Optional[JournalEntry]:
        result = await self.db.execute(
            self._base_query().where(JournalEntry.serial == serial)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Optional[JournalEntry]:
        result = await self.db.execute(
            self._base_query()
            .where(JournalEntry.idempotency_key == key)
        )
        return result.scalar_one_or_none()

    async def list_paginated(
        self,
        *,
        status: Optional[str] = None,
        je_type: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        source_module: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[JournalEntry], int]:
        filters = []
        if status:
            filters.append(JournalEntry.status == status)
        if je_type:
            filters.append(JournalEntry.je_type == je_type)
        if date_from:
            filters.append(JournalEntry.entry_date >= date_from)
        if date_to:
            filters.append(JournalEntry.entry_date <= date_to)
        if source_module:
            filters.append(JournalEntry.source_module == source_module)
        if fiscal_year:
            filters.append(JournalEntry.fiscal_year == fiscal_year)

        return await self.list(
            filters=filters,
            order_by=desc(JournalEntry.entry_date),
            offset=offset,
            limit=limit,
        )

    async def get_source_je(
        self, source_doc_id: uuid.UUID
    ) -> Optional[JournalEntry]:
        """Find the posted JE for a source document (invoice, GRN, etc.)."""
        result = await self.db.execute(
            self._base_query()
            .where(JournalEntry.source_doc_id == source_doc_id)
            .where(JournalEntry.status == JEStatus.POSTED)
        )
        return result.scalar_one_or_none()


# ══════════════════════════════════════════════════════════
# Account Balance Repository
# ══════════════════════════════════════════════════════════
class AccountBalanceRepository(BaseRepository[AccountBalance]):
    model = AccountBalance

    async def get_balance(
        self,
        account_code: str,
        fiscal_year: int,
        fiscal_month: int,
    ) -> Optional[AccountBalance]:
        result = await self.db.execute(
            self._base_query()
            .where(AccountBalance.account_code == account_code)
            .where(AccountBalance.fiscal_year == fiscal_year)
            .where(AccountBalance.fiscal_month == fiscal_month)
        )
        return result.scalar_one_or_none()

    async def upsert_balance(
        self,
        account_code: str,
        fiscal_year: int,
        fiscal_month: int,
        delta_debit: Decimal,
        delta_credit: Decimal,
        account_nature: str,
    ) -> AccountBalance:
        """
        Atomically update or create balance for an account/period.
        Called inside PostingEngine transaction.
        """
        bal = await self.get_balance(account_code, fiscal_year, fiscal_month)

        if bal is None:
            bal = AccountBalance(
                tenant_id=self.tenant_id,
                account_code=account_code,
                fiscal_year=fiscal_year,
                fiscal_month=fiscal_month,
                debit_total=delta_debit,
                credit_total=delta_credit,
            )
            self.db.add(bal)
        else:
            bal.debit_total = bal.debit_total + delta_debit
            bal.credit_total = bal.credit_total + delta_credit

        # Signed balance: debit-nature = DR-CR, credit-nature = CR-DR
        if account_nature == "debit":
            bal.balance = bal.debit_total - bal.credit_total
        else:
            bal.balance = bal.credit_total - bal.debit_total

        bal.closing_balance = bal.opening_balance + bal.balance
        bal.last_posted_at = datetime.now(timezone.utc)

        await self.db.flush()
        return bal

    async def get_account_balances_for_period(
        self, fiscal_year: int, fiscal_month: Optional[int] = None
    ) -> List[AccountBalance]:
        """Used by trial balance and financial reports."""
        q = self._base_query().where(AccountBalance.fiscal_year == fiscal_year)
        if fiscal_month is not None:
            q = q.where(AccountBalance.fiscal_month == fiscal_month)
        result = await self.db.execute(q.order_by(AccountBalance.account_code))
        return list(result.scalars().all())


# ══════════════════════════════════════════════════════════
# Fiscal Lock Repository
# ══════════════════════════════════════════════════════════
class FiscalLockRepository(BaseRepository[FiscalLock]):
    model = FiscalLock

    async def get_active_lock(
        self,
        fiscal_year: int,
        fiscal_month: Optional[int] = None,
    ) -> Optional[FiscalLock]:
        """Returns the strictest active lock for a period."""
        q = (
            self._base_query()
            .where(FiscalLock.fiscal_year == fiscal_year)
            .where(FiscalLock.is_active == True)
        )
        if fiscal_month is not None:
            q = q.where(
                (FiscalLock.fiscal_month == fiscal_month) |
                (FiscalLock.fiscal_month == None)
            )
        # Hard lock takes priority
        q = q.order_by(
            FiscalLock.lock_type.desc(),  # hard > soft alphabetically
            FiscalLock.locked_at.desc(),
        ).limit(1)

        result = await self.db.execute(q)
        return result.scalar_one_or_none()

    async def list_active_locks(self) -> List[FiscalLock]:
        result = await self.db.execute(
            self._base_query()
            .where(FiscalLock.is_active == True)
            .order_by(FiscalLock.fiscal_year.desc(), FiscalLock.fiscal_month.desc())
        )
        return list(result.scalars().all())


# ══════════════════════════════════════════════════════════
# Audit Log Repository (write-only)
# ══════════════════════════════════════════════════════════
class AccountingAuditRepository:
    """Write-only — never update or delete audit records."""

    def __init__(self, db: AsyncSession, tenant_id: uuid.UUID) -> None:
        self.db = db
        self.tenant_id = tenant_id

    async def log(
        self,
        *,
        action: str,
        user_id: Optional[uuid.UUID],
        user_email: Optional[str],
        je_id: Optional[uuid.UUID] = None,
        je_serial: Optional[str] = None,
        je_type: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        fiscal_month: Optional[int] = None,
        total_debit: Optional[Decimal] = None,
        total_credit: Optional[Decimal] = None,
        source_module: Optional[str] = None,
        source_doc_number: Optional[str] = None,
        notes: Optional[str] = None,
        extra_data: Optional[dict] = None,
        request_id: Optional[str] = None,
    ) -> None:
        entry = AccountingAuditLog(
            tenant_id=self.tenant_id,
            action=action,
            user_id=user_id,
            user_email=user_email,
            je_id=je_id,
            je_serial=je_serial,
            je_type=je_type,
            fiscal_year=fiscal_year,
            fiscal_month=fiscal_month,
            total_debit=total_debit,
            total_credit=total_credit,
            source_module=source_module,
            source_doc_number=source_doc_number,
            notes=notes,
            extra_data=extra_data,
            request_id=request_id,
        )
        self.db.add(entry)
        await self.db.flush()
        logger.info("accounting_audit", action=action, je_serial=je_serial)
