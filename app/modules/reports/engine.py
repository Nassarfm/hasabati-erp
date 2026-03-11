"""
app/modules/reports/engine.py
══════════════════════════════════════════════════════════
Reports Engine — يولّد القوائم المالية من جداول GL.

المصدر الوحيد للبيانات: account_balances + journal_entry_lines
لا يعتمد على أي module آخر غير accounting.

الدوال:
  trial_balance()    ← ميزان المراجعة
  income_statement() ← قائمة الدخل
  balance_sheet()    ← الميزانية العمومية
  cash_flow()        ← التدفقات النقدية (indirect method)
  vat_return()       ← إقرار ضريبة القيمة المضافة
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.accounting.models import (
    AccountBalance, AccountNature, AccountType,
    ChartOfAccount, JournalEntry, JournalEntryLine,
)

PREC = Decimal("0.001")
ZERO = Decimal("0")


# ── Helpers ───────────────────────────────────────────────
def _sign(nature: str, debit: Decimal, credit: Decimal) -> Decimal:
    """Returns signed balance: debit-nature positive when DR > CR."""
    if nature == AccountNature.DEBIT:
        return (debit - credit).quantize(PREC)
    return (credit - debit).quantize(PREC)


async def _get_coa(db: AsyncSession, tenant_id: uuid.UUID) -> Dict[str, ChartOfAccount]:
    """Load full COA into dict keyed by account_code."""
    result = await db.execute(
        select(ChartOfAccount)
        .where(ChartOfAccount.tenant_id == tenant_id)
        .where(ChartOfAccount.is_active == True)
        .order_by(ChartOfAccount.account_code)
    )
    return {a.account_code: a for a in result.scalars().all()}


async def _period_balances(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year_from: int, month_from: int,
    year_to:   int, month_to:   int,
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate debit_total / credit_total per account_code
    for all months in [year_from/month_from .. year_to/month_to].
    Returns { account_code: { 'debit': X, 'credit': Y } }
    """
    result = await db.execute(
        select(
            AccountBalance.account_code,
            func.sum(AccountBalance.debit_total).label("debit"),
            func.sum(AccountBalance.credit_total).label("credit"),
        )
        .where(AccountBalance.tenant_id == tenant_id)
        .where(
            (AccountBalance.fiscal_year * 100 + AccountBalance.fiscal_month)
            >= (year_from * 100 + month_from)
        )
        .where(
            (AccountBalance.fiscal_year * 100 + AccountBalance.fiscal_month)
            <= (year_to * 100 + month_to)
        )
        .group_by(AccountBalance.account_code)
    )
    out: Dict[str, Dict[str, Decimal]] = {}
    for row in result:
        out[row.account_code] = {
            "debit":  row.debit  or ZERO,
            "credit": row.credit or ZERO,
        }
    return out


async def _ytd_balances(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month: int,
) -> Dict[str, Dict[str, Decimal]]:
    """Cumulative balances from start of year to given month (for BS)."""
    return await _period_balances(db, tenant_id, year, 1, year, month)


# ══════════════════════════════════════════════════════════
# 1. Trial Balance
# ══════════════════════════════════════════════════════════
async def trial_balance(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month: int,
) -> Dict[str, Any]:
    """
    ميزان المراجعة للشهر المحدد.
    يعرض: opening + period movement + closing لكل حساب قابل للترحيل.
    """
    coa = await _get_coa(db, tenant_id)

    # Opening balances: all periods before this month
    open_bal = {}
    if month > 1:
        open_bal = await _period_balances(db, tenant_id, year, 1, year, month - 1)
    else:
        # Opening of year = closing of previous year (simplified: zero for MVP)
        open_bal = {}

    period_bal = await _period_balances(db, tenant_id, year, month, year, month)

    rows = []
    total_open_dr = total_open_cr = ZERO
    total_period_dr = total_period_cr = ZERO
    total_close_dr = total_close_cr = ZERO

    for code, acc in sorted(coa.items()):
        if not acc.postable:
            continue

        ob = open_bal.get(code, {"debit": ZERO, "credit": ZERO})
        pb = period_bal.get(code, {"debit": ZERO, "credit": ZERO})

        open_dr  = ob["debit"]
        open_cr  = ob["credit"]
        period_dr = pb["debit"]
        period_cr = pb["credit"]
        close_dr = (open_dr + period_dr).quantize(PREC)
        close_cr = (open_cr + period_cr).quantize(PREC)

        # Skip zero rows
        if all(v == ZERO for v in [open_dr, open_cr, period_dr, period_cr, close_dr, close_cr]):
            continue

        rows.append({
            "account_code":  code,
            "account_name":  acc.name_ar,
            "account_type":  acc.account_type,
            "open_debit":    float(open_dr),
            "open_credit":   float(open_cr),
            "period_debit":  float(period_dr),
            "period_credit": float(period_cr),
            "close_debit":   float(close_dr),
            "close_credit":  float(close_cr),
        })

        total_open_dr    += open_dr
        total_open_cr    += open_cr
        total_period_dr  += period_dr
        total_period_cr  += period_cr
        total_close_dr   += close_dr
        total_close_cr   += close_cr

    return {
        "report":  "trial_balance",
        "period":  f"{year}/{month:02d}",
        "totals": {
            "open_debit":    float(total_open_dr),
            "open_credit":   float(total_open_cr),
            "period_debit":  float(total_period_dr),
            "period_credit": float(total_period_cr),
            "close_debit":   float(total_close_dr),
            "close_credit":  float(total_close_cr),
            "balanced":      abs(total_close_dr - total_close_cr) < Decimal("0.01"),
        },
        "rows": rows,
    }


# ══════════════════════════════════════════════════════════
# 2. Income Statement (P&L)
# ══════════════════════════════════════════════════════════
async def income_statement(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month_from: int = 1,
    month_to:   int = 12,
) -> Dict[str, Any]:
    """
    قائمة الدخل للفترة من month_from إلى month_to.
    Revenue − COGS = Gross Profit − Expenses = Net Income
    """
    coa  = await _get_coa(db, tenant_id)
    bals = await _period_balances(db, tenant_id, year, month_from, year, month_to)

    revenue_rows: List[dict] = []
    cogs_rows:    List[dict] = []
    expense_rows: List[dict] = []

    total_revenue = total_cogs = total_expense = ZERO

    for code, acc in sorted(coa.items()):
        if not acc.postable:
            continue
        b = bals.get(code, {"debit": ZERO, "credit": ZERO})
        amt = _sign(acc.account_nature, b["debit"], b["credit"])
        if amt == ZERO:
            continue

        row = {
            "account_code": code,
            "account_name": acc.name_ar,
            "amount": float(amt),
        }

        if acc.account_type == AccountType.REVENUE:
            revenue_rows.append(row)
            total_revenue += amt

        elif acc.account_type == AccountType.EXPENSE:
            # Separate COGS (code starts with 5) from other expenses
            if code.startswith("5"):
                cogs_rows.append(row)
                total_cogs += amt
            else:
                expense_rows.append(row)
                total_expense += amt

    gross_profit = (total_revenue - total_cogs).quantize(PREC)
    net_income   = (gross_profit - total_expense).quantize(PREC)

    return {
        "report":   "income_statement",
        "period":   f"{year}/{month_from:02d}–{month_to:02d}",
        "sections": {
            "revenue": {
                "label":   "الإيرادات",
                "rows":    revenue_rows,
                "total":   float(total_revenue),
            },
            "cogs": {
                "label":   "تكلفة البضاعة المباعة",
                "rows":    cogs_rows,
                "total":   float(total_cogs),
            },
            "gross_profit": float(gross_profit),
            "expenses": {
                "label":   "المصاريف التشغيلية",
                "rows":    expense_rows,
                "total":   float(total_expense),
            },
        },
        "net_income":    float(net_income),
        "gross_margin":  float(
            (gross_profit / total_revenue * 100).quantize(Decimal("0.01"))
            if total_revenue > 0 else ZERO
        ),
        "net_margin":    float(
            (net_income / total_revenue * 100).quantize(Decimal("0.01"))
            if total_revenue > 0 else ZERO
        ),
    }


# ══════════════════════════════════════════════════════════
# 3. Balance Sheet
# ══════════════════════════════════════════════════════════
async def balance_sheet(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month: int,
) -> Dict[str, Any]:
    """
    الميزانية العمومية في نهاية الشهر المحدد.
    Assets = Liabilities + Equity
    """
    coa  = await _get_coa(db, tenant_id)
    bals = await _ytd_balances(db, tenant_id, year, month)

    # Net income for the year (close to retained earnings)
    ni_data  = await income_statement(db, tenant_id, year, 1, month)
    net_income = Decimal(str(ni_data["net_income"]))

    asset_rows:     List[dict] = []
    liability_rows: List[dict] = []
    equity_rows:    List[dict] = []

    total_assets = total_liabilities = total_equity = ZERO

    for code, acc in sorted(coa.items()):
        if not acc.postable:
            continue
        if acc.account_type not in (
            AccountType.ASSET, AccountType.LIABILITY, AccountType.EQUITY
        ):
            continue

        b   = bals.get(code, {"debit": ZERO, "credit": ZERO})
        amt = _sign(acc.account_nature, b["debit"], b["credit"])
        if amt == ZERO:
            continue

        row = {
            "account_code": code,
            "account_name": acc.name_ar,
            "amount": float(amt),
        }

        if acc.account_type == AccountType.ASSET:
            asset_rows.append(row)
            total_assets += amt
        elif acc.account_type == AccountType.LIABILITY:
            liability_rows.append(row)
            total_liabilities += amt
        elif acc.account_type == AccountType.EQUITY:
            equity_rows.append(row)
            total_equity += amt

    # Add current year net income to equity
    total_equity += net_income
    equity_rows.append({
        "account_code": "3202",
        "account_name": "صافي الدخل للسنة الحالية",
        "amount": float(net_income),
    })

    total_liab_equity = (total_liabilities + total_equity).quantize(PREC)
    diff = abs(total_assets - total_liab_equity)

    return {
        "report":  "balance_sheet",
        "as_of":   f"{year}/{month:02d}",
        "sections": {
            "assets": {
                "label": "الأصول",
                "rows":  asset_rows,
                "total": float(total_assets.quantize(PREC)),
            },
            "liabilities": {
                "label": "الالتزامات",
                "rows":  liability_rows,
                "total": float(total_liabilities.quantize(PREC)),
            },
            "equity": {
                "label": "حقوق الملكية",
                "rows":  equity_rows,
                "total": float(total_equity.quantize(PREC)),
            },
        },
        "total_assets":         float(total_assets.quantize(PREC)),
        "total_liab_equity":    float(total_liab_equity),
        "balanced":             diff < Decimal("1"),
        "difference":           float(diff),
    }


# ══════════════════════════════════════════════════════════
# 4. Cash Flow (Indirect Method)
# ══════════════════════════════════════════════════════════
async def cash_flow(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month_from: int = 1,
    month_to:   int = 12,
) -> Dict[str, Any]:
    """
    قائمة التدفقات النقدية — الطريقة غير المباشرة.

    Operating:
      Net Income
      + Depreciation  (non-cash expense)
      ± Changes in AR, AP, Inventory

    Investing:
      Asset purchases / disposals (account codes 15xx)

    Financing:
      Loan proceeds / repayments (account codes 25xx)
      Capital contributions (account codes 30xx)
    """
    coa  = await _get_coa(db, tenant_id)
    bals = await _period_balances(db, tenant_id, year, month_from, year, month_to)

    def _net(code: str) -> Decimal:
        b = bals.get(code, {"debit": ZERO, "credit": ZERO})
        acc = coa.get(code)
        if not acc:
            return ZERO
        return _sign(acc.account_nature, b["debit"], b["credit"])

    def _movement(code: str) -> Decimal:
        """Net DR - CR movement (positive = outflow for assets)."""
        b = bals.get(code, {"debit": ZERO, "credit": ZERO})
        return (b["debit"] - b["credit"]).quantize(PREC)

    # ── Operating ──────────────────────────────────────────
    ni_data    = await income_statement(db, tenant_id, year, month_from, month_to)
    net_income = Decimal(str(ni_data["net_income"]))

    # Depreciation: sum of all depreciation accounts (6501-6506)
    dep_total = ZERO
    for code, acc in coa.items():
        if code.startswith("65") and acc.postable:
            b = bals.get(code, {"debit": ZERO, "credit": ZERO})
            dep_total += b["debit"]

    # Working capital changes (decrease in asset = source, increase = use)
    # AR change (1201-1299): increase in AR = use of cash
    ar_change = ZERO
    inv_change = ZERO
    ap_change  = ZERO
    vat_change = ZERO

    for code, acc in coa.items():
        if not acc.postable:
            continue
        mv = _movement(code)
        if code.startswith("12"):   # AR
            ar_change += mv
        elif code.startswith("13"): # Inventory
            inv_change += mv
        elif code.startswith("21"): # AP
            ap_change += mv
        elif code.startswith("22"): # VAT payable
            vat_change += mv

    # For assets: debit movement = increase = use of cash (negative)
    # For liabilities: credit movement = increase = source (positive)
    op_ar_adj  = -ar_change
    op_inv_adj = -inv_change
    op_ap_adj  =  ap_change   # AP increase = source
    op_vat_adj =  vat_change

    op_total = (net_income + dep_total + op_ar_adj + op_inv_adj + op_ap_adj + op_vat_adj).quantize(PREC)

    # ── Investing ─────────────────────────────────────────
    fixed_asset_change = ZERO
    for code, acc in coa.items():
        if not acc.postable:
            continue
        if code.startswith("15") or code.startswith("16"):
            mv = _movement(code)
            fixed_asset_change += mv

    inv_total = (-fixed_asset_change).quantize(PREC)  # purchase = outflow

    # ── Financing ─────────────────────────────────────────
    loan_change    = ZERO
    capital_change = ZERO
    for code, acc in coa.items():
        if not acc.postable:
            continue
        mv = _movement(code)
        if code.startswith("25"):   # long-term debt
            loan_change += mv
        elif code.startswith("30"): # capital
            capital_change += mv

    fin_total = (-loan_change - capital_change).quantize(PREC)

    net_change = (op_total + inv_total + fin_total).quantize(PREC)

    # Opening cash balance
    open_cash = ZERO
    for code, acc in coa.items():
        if not acc.postable:
            continue
        if code.startswith("10") or code.startswith("11"):
            if month_from > 1:
                ob = await _period_balances(db, tenant_id, year, 1, year, month_from - 1)
                b  = ob.get(code, {"debit": ZERO, "credit": ZERO})
                open_cash += (b["debit"] - b["credit"])

    closing_cash = (open_cash + net_change).quantize(PREC)

    return {
        "report": "cash_flow",
        "period": f"{year}/{month_from:02d}–{month_to:02d}",
        "method": "indirect",
        "operating": {
            "label":        "أنشطة التشغيل",
            "net_income":   float(net_income),
            "depreciation": float(dep_total),
            "ar_change":    float(op_ar_adj),
            "inv_change":   float(op_inv_adj),
            "ap_change":    float(op_ap_adj),
            "vat_change":   float(op_vat_adj),
            "total":        float(op_total),
        },
        "investing": {
            "label":             "أنشطة الاستثمار",
            "asset_net_change":  float(inv_total),
            "total":             float(inv_total),
        },
        "financing": {
            "label":       "أنشطة التمويل",
            "loans_net":   float(-loan_change),
            "capital_net": float(-capital_change),
            "total":       float(fin_total),
        },
        "net_cash_change":  float(net_change),
        "opening_cash":     float(open_cash),
        "closing_cash":     float(closing_cash),
    }


# ══════════════════════════════════════════════════════════
# 5. VAT Return (ZATCA)
# ══════════════════════════════════════════════════════════
async def vat_return(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month_from: int,
    month_to:   int,
) -> Dict[str, Any]:
    """
    إقرار ضريبة القيمة المضافة.

    Output VAT  ← حساب 2201 (ضريبة المبيعات المستحقة)
    Input VAT   ← حساب 1401 (ضريبة المدخلات القابلة للاسترداد)
    Net VAT due = Output − Input
    """
    bals = await _period_balances(db, tenant_id, year, month_from, year, month_to)

    # Output VAT (2201): credit balance = amount collected
    vat_out_b  = bals.get("2201", {"debit": ZERO, "credit": ZERO})
    output_vat = (vat_out_b["credit"] - vat_out_b["debit"]).quantize(PREC)

    # Input VAT (1401): debit balance = amount recoverable
    vat_in_b  = bals.get("1401", {"debit": ZERO, "credit": ZERO})
    input_vat = (vat_in_b["debit"] - vat_in_b["credit"]).quantize(PREC)

    # Also collect from other VAT recoverable accounts (14xx)
    for code, b in bals.items():
        if code.startswith("14") and code != "1401":
            input_vat += (b["debit"] - b["credit"]).quantize(PREC)

    net_vat = (output_vat - input_vat).quantize(PREC)

    # Taxable sales (revenue accounts net of VAT)
    coa  = await _get_coa(db, tenant_id)
    taxable_sales = ZERO
    taxable_purchases = ZERO
    for code, acc in coa.items():
        if not acc.postable:
            continue
        b   = bals.get(code, {"debit": ZERO, "credit": ZERO})
        amt = _sign(acc.account_nature, b["debit"], b["credit"])
        if acc.account_type == AccountType.REVENUE:
            taxable_sales += amt
        elif code.startswith("13") or code.startswith("5"):
            # Approximate purchases from COGS + inventory movement
            pass

    # Approximate taxable purchases from input VAT
    taxable_purchases = (input_vat / Decimal("0.15")).quantize(PREC) if input_vat > 0 else ZERO

    return {
        "report":  "vat_return",
        "period":  f"{year}/{month_from:02d}–{month_to:02d}",
        "zatca": {
            "box_1_taxable_sales":      float(taxable_sales),
            "box_2_output_vat":         float(output_vat),
            "box_3_taxable_purchases":  float(taxable_purchases),
            "box_4_input_vat":          float(input_vat),
            "box_5_net_vat_due":        float(net_vat),
        },
        "status": "payable" if net_vat > 0 else ("refundable" if net_vat < 0 else "nil"),
        "payment_required": net_vat > 0,
        "refund_due":        net_vat < 0,
        "amount":            float(abs(net_vat)),
    }


# ══════════════════════════════════════════════════════════
# 6. General Ledger (Account drill-down)
# ══════════════════════════════════════════════════════════
async def general_ledger(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    account_code: str,
    date_from: date,
    date_to:   date,
) -> Dict[str, Any]:
    """
    دفتر الأستاذ لحساب محدد — كل القيود المؤثرة عليه.
    """
    coa = await _get_coa(db, tenant_id)
    acc = coa.get(account_code)
    if not acc:
        return {"error": f"الحساب {account_code} غير موجود"}

    result = await db.execute(
        select(JournalEntryLine, JournalEntry)
        .join(JournalEntry, JournalEntryLine.journal_entry_id == JournalEntry.id)
        .where(JournalEntryLine.tenant_id == tenant_id)
        .where(JournalEntryLine.account_code == account_code)
        .where(JournalEntry.entry_date >= date_from)
        .where(JournalEntry.entry_date <= date_to)
        .order_by(JournalEntry.entry_date, JournalEntry.serial_number)
    )

    rows_raw = result.all()
    running = ZERO
    rows = []

    for line, je in rows_raw:
        debit  = line.debit  or ZERO
        credit = line.credit or ZERO
        if acc.account_nature == AccountNature.DEBIT:
            running += debit - credit
        else:
            running += credit - debit

        rows.append({
            "date":        str(je.entry_date),
            "serial":      je.serial_number,
            "description": line.description or je.description,
            "debit":       float(debit),
            "credit":      float(credit),
            "balance":     float(running.quantize(PREC)),
            "source":      je.source_module or "",
        })

    total_dr = sum(r["debit"]  for r in rows)
    total_cr = sum(r["credit"] for r in rows)

    return {
        "report":       "general_ledger",
        "account_code": account_code,
        "account_name": acc.name_ar,
        "account_type": acc.account_type,
        "period":       f"{date_from} — {date_to}",
        "total_debit":  total_dr,
        "total_credit": total_cr,
        "net_movement": total_dr - total_cr,
        "rows":         rows,
    }
