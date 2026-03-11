"""app/modules/treasury/router.py — Treasury API (10 endpoints)"""
from __future__ import annotations
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import created, ok, paginated
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.treasury.service import TreasuryService

router = APIRouter(prefix="/treasury", tags=["الخزينة والبنوك"])


def _svc(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    return TreasuryService(db, user)


@router.post("/bank-accounts", status_code=201, summary="تعريف حساب بنكي أو صندوق")
async def create_bank_account(data: dict, svc: TreasuryService = Depends(_svc)):
    ba = await svc.create_bank_account(data)
    return created(data=ba.to_dict(), message=f"تم تعريف الحساب {ba.account_code}")


@router.get("/bank-accounts", summary="قائمة الحسابات البنكية والصناديق")
async def list_bank_accounts(svc: TreasuryService = Depends(_svc)):
    items = await svc.list_bank_accounts()
    return ok(data=[b.to_dict() for b in items])


@router.get("/bank-accounts/{ba_id}", summary="تفاصيل حساب بنكي")
async def get_bank_account(ba_id: uuid.UUID, svc: TreasuryService = Depends(_svc)):
    ba = await svc.get_bank_account(ba_id)
    return ok(data=ba.to_dict())


@router.post("/receipts", status_code=201, summary="ترحيل قبض")
async def post_receipt(
    bank_account_id:     uuid.UUID = Query(...),
    tx_date:             date      = Query(...),
    amount:              float     = Query(..., gt=0),
    counterpart_account: str       = Query(..., description="حساب الطرف المقابل (ذمم أو إيراد)"),
    description:         str       = Query(...),
    party_name:          Optional[str] = Query(None),
    reference:           Optional[str] = Query(None),
    svc: TreasuryService = Depends(_svc),
):
    """
    قبض — DR بنك/صندوق / CR ذمم عملاء أو إيرادات
    """
    tx = await svc.post_receipt(
        bank_account_id=bank_account_id,
        tx_date=tx_date,
        amount=Decimal(str(amount)),
        counterpart_account=counterpart_account,
        description=description,
        party_name=party_name,
        reference=reference,
    )
    return created(
        data=tx.to_dict(),
        message=f"✅ قبض {tx.tx_number} — {amount:,.3f} ر.س | قيد: {tx.je_serial}",
    )


@router.post("/payments", status_code=201, summary="ترحيل دفع")
async def post_payment(
    bank_account_id:     uuid.UUID = Query(...),
    tx_date:             date      = Query(...),
    amount:              float     = Query(..., gt=0),
    counterpart_account: str       = Query(..., description="حساب الطرف المقابل (ذمم موردين أو مصروف)"),
    description:         str       = Query(...),
    party_name:          Optional[str] = Query(None),
    reference:           Optional[str] = Query(None),
    svc: TreasuryService = Depends(_svc),
):
    """
    دفع — DR ذمم موردين أو مصروف / CR بنك/صندوق
    يتحقق من كفاية الرصيد قبل الدفع.
    """
    tx = await svc.post_payment(
        bank_account_id=bank_account_id,
        tx_date=tx_date,
        amount=Decimal(str(amount)),
        counterpart_account=counterpart_account,
        description=description,
        party_name=party_name,
        reference=reference,
    )
    return created(
        data=tx.to_dict(),
        message=f"✅ دفع {tx.tx_number} — {amount:,.3f} ر.س | قيد: {tx.je_serial}",
    )


@router.get("/statement", summary="كشف حساب بنكي")
async def bank_statement(
    bank_account_id: uuid.UUID = Query(...),
    date_from:       date      = Query(...),
    date_to:         date      = Query(...),
    svc: TreasuryService = Depends(_svc),
):
    """كشف الحركات البنكية للفترة مع إجماليات القبض والدفع والرصيد."""
    data = await svc.get_statement(bank_account_id, date_from, date_to)
    return ok(data=data)


@router.get("/dashboard", summary="لوحة تحكم الخزينة")
async def treasury_dashboard(svc: TreasuryService = Depends(_svc)):
    """أرصدة جميع الحسابات البنكية والصناديق."""
    items = await svc.list_bank_accounts()
    total = sum(b.current_balance for b in items)
    return ok(data={
        "total_cash": float(total),
        "accounts": [
            {"code": b.account_code, "name": b.account_name,
             "type": b.account_type, "balance": float(b.current_balance)}
            for b in items
        ],
    })
