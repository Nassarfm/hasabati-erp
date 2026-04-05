"""
app/modules/reports/router.py — with dimension filtering
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.reports.engine import (
    DimFilter,
    balance_sheet,
    cash_flow,
    general_ledger,
    income_statement,
    trial_balance,
    vat_return,
)

router = APIRouter(prefix="/reports", tags=["التقارير المالية"])


def _ctx(
    db:   AsyncSession  = Depends(get_db),
    user: CurrentUser   = Depends(get_current_user),
):
    return db, user


def _dim(
    branch_code:                 Optional[str] = Query(None, description="كود الفرع"),
    cost_center:                 Optional[str] = Query(None, description="كود مركز التكلفة"),
    project_code:                Optional[str] = Query(None, description="كود المشروع"),
    expense_classification_code: Optional[str] = Query(None, description="كود تصنيف المصروف"),
) -> DimFilter:
    return DimFilter(
        branch_code=branch_code,
        cost_center=cost_center,
        project_code=project_code,
        expense_classification_code=expense_classification_code,
    )


# ══════════════════════════════════════════════════════════
# 1. Trial Balance
# ══════════════════════════════════════════════════════════
@router.get("/trial-balance", summary="ميزان المراجعة")
async def get_trial_balance(
    year:  int = Query(..., ge=2020, le=2099),
    month: int = Query(..., ge=1, le=12),
    dim: DimFilter = Depends(_dim),
    ctx = Depends(_ctx),
):
    db, user = ctx
    data = await trial_balance(db, user.tenant_id, year, month, dim if dim.is_active else None)
    status = "✅ متوازن" if data["totals"]["balanced"] else "⚠️ غير متوازن"
    label  = f" | {dim.label()}" if dim.is_active else ""
    return ok(data=data, message=f"ميزان المراجعة {year}/{month:02d}{label} — {status}")


# ══════════════════════════════════════════════════════════
# 2. Income Statement
# ══════════════════════════════════════════════════════════
@router.get("/income-statement", summary="قائمة الدخل")
async def get_income_statement(
    year:       int = Query(..., ge=2020, le=2099),
    month_from: int = Query(1,  ge=1, le=12),
    month_to:   int = Query(12, ge=1, le=12),
    dim: DimFilter = Depends(_dim),
    ctx = Depends(_ctx),
):
    db, user = ctx
    data = await income_statement(
        db, user.tenant_id, year, month_from, month_to,
        dim if dim.is_active else None
    )
    ni   = data["net_income"]
    icon = "✅" if ni >= 0 else "🔴"
    label = f" | {dim.label()}" if dim.is_active else ""
    return ok(
        data=data,
        message=f"{icon} صافي الدخل: {ni:,.3f}{label}",
    )


# ══════════════════════════════════════════════════════════
# 3. Balance Sheet
# ══════════════════════════════════════════════════════════
@router.get("/balance-sheet", summary="الميزانية العمومية")
async def get_balance_sheet(
    year:  int = Query(..., ge=2020, le=2099),
    month: int = Query(..., ge=1, le=12),
    dim: DimFilter = Depends(_dim),
    ctx = Depends(_ctx),
):
    db, user = ctx
    data = await balance_sheet(
        db, user.tenant_id, year, month,
        dim if dim.is_active else None
    )
    status = "✅ متوازنة" if data["balanced"] else f"⚠️ فرق: {data['difference']:,.3f}"
    label  = f" | {dim.label()}" if dim.is_active else ""
    return ok(
        data=data,
        message=f"الميزانية {year}/{month:02d}{label} — {status}",
    )


# ══════════════════════════════════════════════════════════
# 4. Cash Flow
# ══════════════════════════════════════════════════════════
@router.get("/cash-flow", summary="قائمة التدفقات النقدية")
async def get_cash_flow(
    year:       int = Query(..., ge=2020, le=2099),
    month_from: int = Query(1,  ge=1, le=12),
    month_to:   int = Query(12, ge=1, le=12),
    dim: DimFilter = Depends(_dim),
    ctx = Depends(_ctx),
):
    db, user = ctx
    data = await cash_flow(
        db, user.tenant_id, year, month_from, month_to,
        dim if dim.is_active else None
    )
    icon  = "✅" if data["net_cash_change"] >= 0 else "🔴"
    label = f" | {dim.label()}" if dim.is_active else ""
    return ok(
        data=data,
        message=f"{icon} صافي التدفق: {data['net_cash_change']:,.3f}{label}",
    )


# ══════════════════════════════════════════════════════════
# 5. VAT Return
# ══════════════════════════════════════════════════════════
@router.get("/vat-return", summary="إقرار ضريبة القيمة المضافة")
async def get_vat_return(
    year:       int = Query(..., ge=2020, le=2099),
    month_from: int = Query(..., ge=1, le=12),
    month_to:   int = Query(..., ge=1, le=12),
    ctx = Depends(_ctx),
):
    db, user = ctx
    data = await vat_return(db, user.tenant_id, year, month_from, month_to)
    if data["payment_required"]:
        msg = f"💳 مستحق: {data['amount']:,.3f} ر.س"
    elif data["refund_due"]:
        msg = f"💰 استرداد: {data['amount']:,.3f} ر.س"
    else:
        msg = "✅ لا يوجد ضريبة"
    return ok(data=data, message=msg)


# ══════════════════════════════════════════════════════════
# 6. General Ledger
# ══════════════════════════════════════════════════════════
@router.get("/general-ledger", summary="دفتر الأستاذ")
async def get_general_ledger(
    account_code: str  = Query(...),
    date_from:    date = Query(...),
    date_to:      date = Query(...),
    ctx = Depends(_ctx),
):
    db, user = ctx
    data = await general_ledger(db, user.tenant_id, account_code, date_from, date_to)
    return ok(data=data, message=f"دفتر الأستاذ — {account_code}")


# ══════════════════════════════════════════════════════════
# 7–10: AR/AP Aging, Sales, Inventory (unchanged)
# ══════════════════════════════════════════════════════════
@router.get("/ar-aging")
async def ar_aging_report(as_of: date = Query(default=None), ctx=Depends(_ctx)):
    from datetime import date as date_type
    from sqlalchemy import select
    from app.modules.sales.models import SalesInvoice
    db, user = ctx
    today = as_of or date_type.today()
    result = await db.execute(
        select(SalesInvoice)
        .where(SalesInvoice.tenant_id == user.tenant_id)
        .where(SalesInvoice.status.in_(["posted", "partially_returned"]))
        .where(SalesInvoice.balance_due > 0)
    )
    invoices = result.scalars().all()
    buckets = {
        "current": {"label":"جارية","amount":0.0,"count":0},
        "1_30":    {"label":"1-30","amount":0.0,"count":0},
        "31_60":   {"label":"31-60","amount":0.0,"count":0},
        "61_90":   {"label":"61-90","amount":0.0,"count":0},
        "over_90": {"label":"+90","amount":0.0,"count":0},
    }
    lines = []
    for inv in invoices:
        due = inv.due_date; overdue = (today - due).days if due else 0; bal = float(inv.balance_due)
        bucket = "current" if overdue<=0 else "1_30" if overdue<=30 else "31_60" if overdue<=60 else "61_90" if overdue<=90 else "over_90"
        buckets[bucket]["amount"] += bal; buckets[bucket]["count"] += 1
        lines.append({"invoice_number":inv.invoice_number,"customer_name":inv.customer_name,"balance_due":bal,"bucket":bucket})
    return ok(data={"as_of":str(today),"total_ar":sum(b["amount"] for b in buckets.values()),"buckets":buckets,"lines":lines})


@router.get("/ap-aging")
async def ap_aging_report(as_of: date = Query(default=None), ctx=Depends(_ctx)):
    from datetime import date as date_type
    from sqlalchemy import select
    from app.modules.purchases.models import VendorInvoice
    db, user = ctx
    today = as_of or date_type.today()
    result = await db.execute(select(VendorInvoice).where(VendorInvoice.tenant_id==user.tenant_id).where(VendorInvoice.balance_due>0))
    invoices = result.scalars().all()
    buckets = {"current":{"label":"جارية","amount":0.0,"count":0},"1_30":{"label":"1-30","amount":0.0,"count":0},"31_60":{"label":"31-60","amount":0.0,"count":0},"61_90":{"label":"61-90","amount":0.0,"count":0},"over_90":{"label":"+90","amount":0.0,"count":0}}
    lines = []
    for vi in invoices:
        due=vi.due_date; overdue=(today-due).days if due else 0; bal=float(vi.balance_due)
        bucket="current" if overdue<=0 else "1_30" if overdue<=30 else "31_60" if overdue<=60 else "61_90" if overdue<=90 else "over_90"
        buckets[bucket]["amount"]+=bal; buckets[bucket]["count"]+=1
        lines.append({"vi_number":vi.vi_number,"supplier_name":vi.supplier_name,"balance_due":bal,"bucket":bucket})
    return ok(data={"as_of":str(today),"total_ap":sum(b["amount"] for b in buckets.values()),"buckets":buckets,"lines":lines})


@router.get("/sales-summary")
async def sales_summary(date_from: date=Query(...), date_to: date=Query(...), ctx=Depends(_ctx)):
    from sqlalchemy import select, func
    from app.modules.sales.models import SalesInvoice, InvoiceStatus
    db, user = ctx; tid = user.tenant_id
    result = await db.execute(select(func.count(SalesInvoice.id).label("cnt"),func.sum(SalesInvoice.taxable_amount).label("revenue"),func.sum(SalesInvoice.total_cost).label("cogs"),func.sum(SalesInvoice.gross_profit).label("gp")).where(SalesInvoice.tenant_id==tid).where(SalesInvoice.status==InvoiceStatus.POSTED).where(SalesInvoice.invoice_date>=date_from).where(SalesInvoice.invoice_date<=date_to))
    row=result.one(); revenue=float(row.revenue or 0); gp=float(row.gp or 0)
    return ok(data={"invoice_count":row.cnt or 0,"total_revenue":revenue,"total_cogs":float(row.cogs or 0),"gross_profit":gp,"gross_margin":round(gp/revenue*100,2) if revenue>0 else 0})


@router.get("/inventory-valuation")
async def inventory_valuation(ctx=Depends(_ctx)):
    from sqlalchemy import select, func
    from app.modules.inventory.models import StockBalance
    db, user = ctx
    result = await db.execute(select(StockBalance.product_code,func.sum(StockBalance.qty_on_hand).label("qty"),func.avg(StockBalance.average_cost).label("wac"),func.sum(StockBalance.total_value).label("value")).where(StockBalance.tenant_id==user.tenant_id).where(StockBalance.qty_on_hand>0).group_by(StockBalance.product_code))
    rows=[]; total=0.0
    for r in result:
        val=float(r.value or 0); total+=val; rows.append({"product_code":r.product_code,"qty":float(r.qty or 0),"wac":float(r.wac or 0),"total_value":val})
    return ok(data={"item_count":len(rows),"total_value":total,"items":rows})
