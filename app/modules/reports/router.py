"""
app/modules/reports/router.py
══════════════════════════════════════════════════════════
Reports API — 10 endpoints

Financial Statements:
  GET /reports/trial-balance         ميزان المراجعة
  GET /reports/income-statement      قائمة الدخل
  GET /reports/balance-sheet         الميزانية العمومية
  GET /reports/cash-flow             التدفقات النقدية
  GET /reports/general-ledger        دفتر الأستاذ (حساب محدد)

Tax:
  GET /reports/vat-return            إقرار ضريبة القيمة المضافة

Receivables / Payables:
  GET /reports/ar-aging              تقادم الذمم المدينة
  GET /reports/ap-aging              تقادم الذمم الدائنة

Management:
  GET /reports/sales-summary         ملخص المبيعات
  GET /reports/inventory-valuation   تقييم المخزون
══════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════
# 1. Trial Balance
# ══════════════════════════════════════════════════════════
@router.get("/trial-balance", summary="ميزان المراجعة")
async def get_trial_balance(
    year:  int = Query(..., ge=2020, le=2099, description="السنة المالية"),
    month: int = Query(..., ge=1, le=12,      description="الشهر"),
    ctx = Depends(_ctx),
):
    """
    ميزان المراجعة للشهر المحدد.

    يعرض لكل حساب قابل للترحيل:
    - الرصيد الافتتاحي (مدين / دائن)
    - حركة الشهر (مدين / دائن)
    - الرصيد الختامي (مدين / دائن)

    يتحقق من توازن الميزان: إجمالي مدين = إجمالي دائن.
    """
    db, user = ctx
    data = await trial_balance(db, user.tenant_id, year, month)
    status = "✅ متوازن" if data["totals"]["balanced"] else "⚠️ غير متوازن"
    return ok(data=data, message=f"ميزان المراجعة {year}/{month:02d} — {status}")


# ══════════════════════════════════════════════════════════
# 2. Income Statement
# ══════════════════════════════════════════════════════════
@router.get("/income-statement", summary="قائمة الدخل")
async def get_income_statement(
    year:       int = Query(..., ge=2020, le=2099),
    month_from: int = Query(1,  ge=1, le=12, description="من شهر"),
    month_to:   int = Query(12, ge=1, le=12, description="إلى شهر"),
    ctx = Depends(_ctx),
):
    """
    قائمة الدخل (Profit & Loss) للفترة المحددة.

    ```
    الإيرادات
    − تكلفة البضاعة المباعة
    = مجمل الربح
    − المصاريف التشغيلية
    = صافي الدخل
    ```

    يحسب أيضاً: هامش الربح الإجمالي % وهامش صافي الدخل %.
    """
    db, user = ctx
    data = await income_statement(db, user.tenant_id, year, month_from, month_to)
    ni = data["net_income"]
    icon = "✅" if ni >= 0 else "🔴"
    return ok(
        data=data,
        message=f"{icon} صافي الدخل: {ni:,.3f} ر.س | هامش: {data['net_margin']:.1f}%",
    )


# ══════════════════════════════════════════════════════════
# 3. Balance Sheet
# ══════════════════════════════════════════════════════════
@router.get("/balance-sheet", summary="الميزانية العمومية")
async def get_balance_sheet(
    year:  int = Query(..., ge=2020, le=2099),
    month: int = Query(..., ge=1, le=12),
    ctx = Depends(_ctx),
):
    """
    الميزانية العمومية في نهاية الشهر المحدد.

    ```
    الأصول = الالتزامات + حقوق الملكية
    ```

    - يُضيف تلقائياً صافي الدخل للسنة الحالية ضمن حقوق الملكية.
    - يتحقق من توازن المعادلة المحاسبية.
    """
    db, user = ctx
    data = await balance_sheet(db, user.tenant_id, year, month)
    status = "✅ متوازنة" if data["balanced"] else f"⚠️ فرق: {data['difference']:,.3f}"
    return ok(
        data=data,
        message=f"الميزانية في {year}/{month:02d} — {status} | إجمالي أصول: {data['total_assets']:,.3f} ر.س",
    )


# ══════════════════════════════════════════════════════════
# 4. Cash Flow
# ══════════════════════════════════════════════════════════
@router.get("/cash-flow", summary="قائمة التدفقات النقدية")
async def get_cash_flow(
    year:       int = Query(..., ge=2020, le=2099),
    month_from: int = Query(1,  ge=1, le=12),
    month_to:   int = Query(12, ge=1, le=12),
    ctx = Depends(_ctx),
):
    """
    قائمة التدفقات النقدية بالطريقة غير المباشرة.

    ```
    أنشطة التشغيل   ← صافي الدخل ± تعديلات
    أنشطة الاستثمار ← شراء/بيع الأصول الثابتة
    أنشطة التمويل   ← قروض / رأس المال
    ─────────────────────────────────────────
    صافي التغير في النقدية
    ```
    """
    db, user = ctx
    data = await cash_flow(db, user.tenant_id, year, month_from, month_to)
    icon = "✅" if data["net_cash_change"] >= 0 else "🔴"
    return ok(
        data=data,
        message=f"{icon} صافي التغير في النقدية: {data['net_cash_change']:,.3f} ر.س",
    )


# ══════════════════════════════════════════════════════════
# 5. VAT Return
# ══════════════════════════════════════════════════════════
@router.get("/vat-return", summary="إقرار ضريبة القيمة المضافة")
async def get_vat_return(
    year:       int = Query(..., ge=2020, le=2099),
    month_from: int = Query(..., ge=1, le=12, description="بداية الفترة الضريبية"),
    month_to:   int = Query(..., ge=1, le=12, description="نهاية الفترة الضريبية"),
    ctx = Depends(_ctx),
):
    """
    إقرار ضريبة القيمة المضافة (ZATCA).

    الخانات الرئيسية:
    - الخانة 1: المبيعات الخاضعة للضريبة
    - الخانة 2: ضريبة المخرجات (output VAT)
    - الخانة 3: المشتريات الخاضعة للضريبة
    - الخانة 4: ضريبة المدخلات (input VAT)
    - الخانة 5: صافي الضريبة المستحقة / القابلة للاسترداد
    """
    db, user = ctx
    data = await vat_return(db, user.tenant_id, year, month_from, month_to)
    if data["payment_required"]:
        msg = f"💳 مستحق الدفع: {data['amount']:,.3f} ر.س"
    elif data["refund_due"]:
        msg = f"💰 مستحق الاسترداد: {data['amount']:,.3f} ر.س"
    else:
        msg = "✅ لا يوجد ضريبة مستحقة"
    return ok(data=data, message=msg)


# ══════════════════════════════════════════════════════════
# 6. General Ledger
# ══════════════════════════════════════════════════════════
@router.get("/general-ledger", summary="دفتر الأستاذ")
async def get_general_ledger(
    account_code: str  = Query(..., description="كود الحساب مثل 1201"),
    date_from:    date = Query(...),
    date_to:      date = Query(...),
    ctx = Depends(_ctx),
):
    """
    دفتر الأستاذ لحساب محدد — جميع القيود بترتيب تاريخي مع الرصيد الجاري.
    """
    db, user = ctx
    data = await general_ledger(db, user.tenant_id, account_code, date_from, date_to)
    count = len(data.get("rows", []))
    return ok(data=data, message=f"دفتر الأستاذ — {account_code} | {count} حركة")


# ══════════════════════════════════════════════════════════
# 7. AR Aging
# ══════════════════════════════════════════════════════════
@router.get("/ar-aging", summary="تقادم الذمم المدينة (موحّد)")
async def ar_aging_report(
    as_of: date = Query(default=None, description="التاريخ (افتراضي: اليوم)"),
    ctx = Depends(_ctx),
):
    """
    تقرير تقادم ذمم العملاء موحّد من جميع الفواتير المستحقة.
    الفئات: جارية | 1-30 | 31-60 | 61-90 | +90 يوم.
    """
    from datetime import date as date_type
    from sqlalchemy import select
    from app.modules.sales.models import SalesInvoice, InvoiceStatus
    from sqlalchemy.ext.asyncio import AsyncSession

    db, user = ctx
    today = as_of or date_type.today()

    result = await db.execute(
        select(SalesInvoice)
        .where(SalesInvoice.tenant_id == user.tenant_id)
        .where(SalesInvoice.status.in_(["posted", "partially_returned"]))
        .where(SalesInvoice.balance_due > 0)
        .order_by(SalesInvoice.due_date)
    )
    invoices = result.scalars().all()

    buckets = {
        "current":  {"label": "جارية",      "amount": 0.0, "count": 0},
        "1_30":     {"label": "1-30 يوم",   "amount": 0.0, "count": 0},
        "31_60":    {"label": "31-60 يوم",  "amount": 0.0, "count": 0},
        "61_90":    {"label": "61-90 يوم",  "amount": 0.0, "count": 0},
        "over_90":  {"label": "+90 يوم",    "amount": 0.0, "count": 0},
    }

    lines = []
    for inv in invoices:
        due   = inv.due_date
        overdue = (today - due).days if due else 0
        bal   = float(inv.balance_due)

        if overdue <= 0:
            bucket = "current"
        elif overdue <= 30:
            bucket = "1_30"
        elif overdue <= 60:
            bucket = "31_60"
        elif overdue <= 90:
            bucket = "61_90"
        else:
            bucket = "over_90"

        buckets[bucket]["amount"] += bal
        buckets[bucket]["count"]  += 1
        lines.append({
            "invoice_number": inv.invoice_number,
            "customer_name":  inv.customer_name,
            "invoice_date":   str(inv.invoice_date),
            "due_date":       str(due) if due else None,
            "days_overdue":   max(overdue, 0),
            "balance_due":    bal,
            "bucket":         bucket,
        })

    total = sum(b["amount"] for b in buckets.values())
    return ok(data={
        "as_of":   str(today),
        "total_ar": total,
        "buckets":  buckets,
        "count":    len(invoices),
        "lines":    lines,
    })


# ══════════════════════════════════════════════════════════
# 8. AP Aging
# ══════════════════════════════════════════════════════════
@router.get("/ap-aging", summary="تقادم الذمم الدائنة (موحّد)")
async def ap_aging_report(
    as_of: date = Query(default=None),
    ctx = Depends(_ctx),
):
    """
    تقرير تقادم ذمم الموردين موحّد من جميع فواتيرهم المستحقة.
    """
    from datetime import date as date_type
    from sqlalchemy import select
    from app.modules.purchases.models import VendorInvoice

    db, user = ctx
    today = as_of or date_type.today()

    result = await db.execute(
        select(VendorInvoice)
        .where(VendorInvoice.tenant_id == user.tenant_id)
        .where(VendorInvoice.status.in_(["posted", "matched"]))
        .where(VendorInvoice.balance_due > 0)
        .order_by(VendorInvoice.due_date)
    )
    invoices = result.scalars().all()

    buckets = {
        "current": {"label": "جارية",     "amount": 0.0, "count": 0},
        "1_30":    {"label": "1-30 يوم",  "amount": 0.0, "count": 0},
        "31_60":   {"label": "31-60 يوم", "amount": 0.0, "count": 0},
        "61_90":   {"label": "61-90 يوم", "amount": 0.0, "count": 0},
        "over_90": {"label": "+90 يوم",   "amount": 0.0, "count": 0},
    }

    lines = []
    for vi in invoices:
        due     = vi.due_date
        overdue = (today - due).days if due else 0
        bal     = float(vi.balance_due)
        bucket  = (
            "current" if overdue <= 0 else
            "1_30"    if overdue <= 30 else
            "31_60"   if overdue <= 60 else
            "61_90"   if overdue <= 90 else
            "over_90"
        )
        buckets[bucket]["amount"] += bal
        buckets[bucket]["count"]  += 1
        lines.append({
            "vi_number":     vi.vi_number,
            "supplier_name": vi.supplier_name,
            "invoice_date":  str(vi.invoice_date),
            "due_date":      str(due) if due else None,
            "days_overdue":  max(overdue, 0),
            "balance_due":   bal,
            "bucket":        bucket,
        })

    total = sum(b["amount"] for b in buckets.values())
    return ok(data={
        "as_of":   str(today),
        "total_ap": total,
        "buckets":  buckets,
        "count":    len(invoices),
        "lines":    lines,
    })


# ══════════════════════════════════════════════════════════
# 9. Sales Summary
# ══════════════════════════════════════════════════════════
@router.get("/sales-summary", summary="ملخص المبيعات")
async def sales_summary(
    date_from: date = Query(...),
    date_to:   date = Query(...),
    ctx = Depends(_ctx),
):
    """ملخص المبيعات: الإيرادات + التكلفة + هامش الربح + أفضل العملاء."""
    from sqlalchemy import select, func
    from app.modules.sales.models import SalesInvoice, SalesInvoiceLine, InvoiceStatus

    db, user = ctx
    tid = user.tenant_id

    # Summary totals
    result = await db.execute(
        select(
            func.count(SalesInvoice.id).label("cnt"),
            func.sum(SalesInvoice.taxable_amount).label("revenue"),
            func.sum(SalesInvoice.vat_amount).label("vat"),
            func.sum(SalesInvoice.total_cost).label("cogs"),
            func.sum(SalesInvoice.gross_profit).label("gp"),
        )
        .where(SalesInvoice.tenant_id == tid)
        .where(SalesInvoice.status == InvoiceStatus.POSTED)
        .where(SalesInvoice.invoice_date >= date_from)
        .where(SalesInvoice.invoice_date <= date_to)
    )
    row = result.one()

    revenue = float(row.revenue or 0)
    cogs    = float(row.cogs    or 0)
    gp      = float(row.gp     or 0)
    margin  = round(gp / revenue * 100, 2) if revenue > 0 else 0

    # Top 5 customers
    top_q = await db.execute(
        select(
            SalesInvoice.customer_name,
            func.sum(SalesInvoice.taxable_amount).label("total"),
        )
        .where(SalesInvoice.tenant_id == tid)
        .where(SalesInvoice.status == InvoiceStatus.POSTED)
        .where(SalesInvoice.invoice_date >= date_from)
        .where(SalesInvoice.invoice_date <= date_to)
        .group_by(SalesInvoice.customer_name)
        .order_by(func.sum(SalesInvoice.taxable_amount).desc())
        .limit(5)
    )
    top_customers = [
        {"customer": r.customer_name, "total": float(r.total)}
        for r in top_q
    ]

    return ok(data={
        "period":         f"{date_from} — {date_to}",
        "invoice_count":  row.cnt or 0,
        "total_revenue":  revenue,
        "total_vat":      float(row.vat or 0),
        "total_cogs":     cogs,
        "gross_profit":   gp,
        "gross_margin":   margin,
        "top_customers":  top_customers,
    })


# ══════════════════════════════════════════════════════════
# 10. Inventory Valuation
# ══════════════════════════════════════════════════════════
@router.get("/inventory-valuation", summary="تقييم المخزون")
async def inventory_valuation(ctx = Depends(_ctx)):
    """
    تقييم المخزون بطريقة المتوسط المرجح (WAC).
    يعرض لكل صنف: الكمية + متوسط التكلفة + القيمة الإجمالية.
    """
    from sqlalchemy import select, func
    from app.modules.inventory.models import StockBalance, Product

    db, user = ctx
    tid = user.tenant_id

    result = await db.execute(
        select(
            StockBalance.product_code,
            StockBalance.warehouse_code,
            func.sum(StockBalance.qty_on_hand).label("qty"),
            func.avg(StockBalance.average_cost).label("wac"),
            func.sum(StockBalance.total_value).label("value"),
        )
        .where(StockBalance.tenant_id == tid)
        .where(StockBalance.qty_on_hand > 0)
        .group_by(StockBalance.product_code, StockBalance.warehouse_code)
        .order_by(StockBalance.product_code)
    )

    rows = []
    total_value = 0.0
    for r in result:
        val = float(r.value or 0)
        total_value += val
        rows.append({
            "product_code":    r.product_code,
            "warehouse_code":  r.warehouse_code,
            "qty_on_hand":     float(r.qty or 0),
            "wac":             float(r.wac or 0),
            "total_value":     val,
        })

    return ok(data={
        "valuation_method": "weighted_average_cost",
        "item_count":       len(rows),
        "total_value":      total_value,
        "items":            rows,
    })
