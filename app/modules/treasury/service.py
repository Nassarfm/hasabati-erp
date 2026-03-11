"""
app/modules/treasury/service.py
══════════════════════════════════════════════════════════
Treasury Service.

Features:
  create_bank_account()   ← تعريف حساب بنكي/صندوق
  post_receipt()          ← قبض (DR بنك / CR ذمم أو إيرادات)
  post_payment()          ← دفع (DR ذمم أو مصروف / CR بنك)
  post_transfer()         ← تحويل بين حسابات
  get_statement()         ← كشف حساب بنكي
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

import structlog
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import (
    DuplicateError, InvalidStateError, NotFoundError, ValidationError,
)
from app.core.tenant import CurrentUser
from app.db.transactions import atomic_transaction
from app.modules.treasury.models import (
    AccountType, BankAccount, CashTransaction, TxStatus, TxType,
)
from app.repositories.base_repo import BaseRepository
from app.services.numbering.series_service import NumberSeriesService
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import ACC

logger = structlog.get_logger(__name__)
PREC = Decimal("0.001")


class BankAccountRepository(BaseRepository[BankAccount]):
    model = BankAccount

    async def get_by_code(self, code: str) -> Optional[BankAccount]:
        result = await self.db.execute(
            self._base_query().where(BankAccount.account_code == code)
        )
        return result.scalar_one_or_none()


class TreasuryService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db       = db
        self.user     = user
        self._ba_repo = BankAccountRepository(db, user.tenant_id)
        self._num     = NumberSeriesService(db, user.tenant_id)
        self._posting = PostingEngine(db, user.tenant_id)

    # ══════════════════════════════════════════════════════
    # Bank Accounts
    # ══════════════════════════════════════════════════════
    async def create_bank_account(self, data: dict) -> BankAccount:
        self.user.require("can_manage_treasury")
        code = data["account_code"]
        if await self._ba_repo.exists(account_code=code):
            raise DuplicateError("حساب بنكي", "account_code", code)
        ba = self._ba_repo.create(**data)
        ba.created_by = self.user.email
        return await self._ba_repo.save(ba)

    async def list_bank_accounts(self) -> List[BankAccount]:
        items, _ = await self._ba_repo.list(
            filters=[BankAccount.is_active == True],
            order_by=BankAccount.account_code,
            offset=0, limit=200,
        )
        return items

    async def get_bank_account(self, ba_id: uuid.UUID) -> BankAccount:
        return await self._ba_repo.get_or_raise(ba_id)

    # ══════════════════════════════════════════════════════
    # Post Receipt (قبض)
    # ══════════════════════════════════════════════════════
    async def post_receipt(
        self,
        bank_account_id: uuid.UUID,
        tx_date: date,
        amount: Decimal,
        counterpart_account: str,   # AR account or revenue account
        description: str,
        party_name: Optional[str] = None,
        reference: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> CashTransaction:
        self.user.require("can_post_treasury")
        ba = await self._ba_repo.get_or_raise(bank_account_id)
        tx_number = await self._num.next("RCP", include_month=True)

        async with atomic_transaction(self.db, label=f"receipt_{tx_number}"):
            new_balance = (ba.current_balance + amount).quantize(PREC)

            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="TJE",
                description=f"قبض — {description}",
                entry_date=tx_date,
                lines=[
                    PostingLine(account_code=ba.gl_account,
                                description=f"قبض — {description}",
                                debit=amount),
                    PostingLine(account_code=counterpart_account,
                                description=f"تسوية قبض — {description}",
                                credit=amount),
                ],
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="treasury",
                source_doc_type="receipt",
                source_doc_number=tx_number,
                idempotency_key=f"TR:RCP:{tx_number}:{self.user.tenant_id}",
                user_role=self.user.role,
            )
            je = await self._posting.post(je_req)

            tx = CashTransaction(
                tenant_id=self.user.tenant_id,
                tx_number=tx_number,
                tx_date=tx_date,
                tx_type=TxType.RECEIPT,
                status=TxStatus.POSTED,
                bank_account_id=ba.id,
                account_code=ba.account_code,
                amount=amount,
                balance_after=new_balance,
                counterpart_account=counterpart_account,
                description=description,
                reference=reference,
                party_name=party_name,
                je_id=je.je_id,
                je_serial=je.je_serial,
                posted_at=datetime.now(timezone.utc),
                notes=notes,
                created_by=self.user.email,
            )
            self.db.add(tx)
            ba.current_balance = new_balance
            await self.db.flush()

        return tx

    # ══════════════════════════════════════════════════════
    # Post Payment (دفع)
    # ══════════════════════════════════════════════════════
    async def post_payment(
        self,
        bank_account_id: uuid.UUID,
        tx_date: date,
        amount: Decimal,
        counterpart_account: str,   # AP account or expense account
        description: str,
        party_name: Optional[str] = None,
        reference: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> CashTransaction:
        self.user.require("can_post_treasury")
        ba = await self._ba_repo.get_or_raise(bank_account_id)

        if ba.current_balance < amount:
            raise ValidationError(
                f"الرصيد غير كافٍ — الرصيد الحالي: {float(ba.current_balance):,.3f} | "
                f"المبلغ المطلوب: {float(amount):,.3f}"
            )

        tx_number = await self._num.next("PMT", include_month=True)

        async with atomic_transaction(self.db, label=f"payment_{tx_number}"):
            new_balance = (ba.current_balance - amount).quantize(PREC)

            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="TJE",
                description=f"دفع — {description}",
                entry_date=tx_date,
                lines=[
                    PostingLine(account_code=counterpart_account,
                                description=f"تسوية دفع — {description}",
                                debit=amount),
                    PostingLine(account_code=ba.gl_account,
                                description=f"دفع — {description}",
                                credit=amount),
                ],
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="treasury",
                source_doc_type="payment",
                source_doc_number=tx_number,
                idempotency_key=f"TR:PMT:{tx_number}:{self.user.tenant_id}",
                user_role=self.user.role,
            )
            je = await self._posting.post(je_req)

            tx = CashTransaction(
                tenant_id=self.user.tenant_id,
                tx_number=tx_number,
                tx_date=tx_date,
                tx_type=TxType.PAYMENT,
                status=TxStatus.POSTED,
                bank_account_id=ba.id,
                account_code=ba.account_code,
                amount=amount,
                balance_after=new_balance,
                counterpart_account=counterpart_account,
                description=description,
                reference=reference,
                party_name=party_name,
                je_id=je.je_id,
                je_serial=je.je_serial,
                posted_at=datetime.now(timezone.utc),
                notes=notes,
                created_by=self.user.email,
            )
            self.db.add(tx)
            ba.current_balance = new_balance
            await self.db.flush()

        return tx

    # ══════════════════════════════════════════════════════
    # Bank Statement
    # ══════════════════════════════════════════════════════
    async def get_statement(
        self,
        bank_account_id: uuid.UUID,
        date_from: date,
        date_to: date,
    ) -> dict:
        ba = await self._ba_repo.get_or_raise(bank_account_id)
        result = await self.db.execute(
            select(CashTransaction)
            .where(CashTransaction.tenant_id == self.user.tenant_id)
            .where(CashTransaction.bank_account_id == bank_account_id)
            .where(CashTransaction.tx_date >= date_from)
            .where(CashTransaction.tx_date <= date_to)
            .where(CashTransaction.status == TxStatus.POSTED)
            .order_by(CashTransaction.tx_date)
        )
        txs = result.scalars().all()

        receipts = sum(t.amount for t in txs if t.tx_type == TxType.RECEIPT)
        payments = sum(t.amount for t in txs if t.tx_type == TxType.PAYMENT)

        return {
            "account_code":    ba.account_code,
            "account_name":    ba.account_name,
            "current_balance": float(ba.current_balance),
            "period":          f"{date_from} — {date_to}",
            "total_receipts":  float(receipts),
            "total_payments":  float(payments),
            "net_movement":    float(receipts - payments),
            "transactions": [
                {
                    "tx_number":  t.tx_number,
                    "tx_date":    str(t.tx_date),
                    "type":       t.tx_type,
                    "description": t.description,
                    "amount":     float(t.amount),
                    "balance_after": float(t.balance_after),
                    "je_serial":  t.je_serial,
                    "party_name": t.party_name,
                }
                for t in txs
            ],
        }
