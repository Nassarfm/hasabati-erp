"""
app/modules/reports/engine.py
══════════════════════════════════════════════════════════
Reports Engine — يولّد القوائم المالية من جداول GL.

يدعم الآن فلترة الأبعاد:
  branch_code, cost_center, project_code, expense_classification_code

عند وجود فلتر بُعد:
  → يستعلم من journal_entry_lines مباشرة
عند عدم وجود فلتر:
  → يستعلم من account_balances (أسرع)
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


# ── Dimension Filter Model ────────────────────────────────
class DimFilter:
    """فلتر الأبعاد — يُمرر لكل دوال التقارير"""
    def __init__(
        self,
        branch_code:                   Optional[str] = None,
        cost_center:                   Optional[str] = None,
        project_code:                  Optional[str] = None,
        expense_classification_code:   Optional[str] = None,
    ):
        self.branch_code                 = branch_code
        self.cost_center                 = cost_center
        self.project_code                = project_code
        self.expense_classification_code = expense_classification_code

    @property
    def is_active(self) -> bool:
        return any([
            self.branch_code,
            self.cost_center,
            self.project_code,
            self.expense_classification_code,
        ])

    def label(self) -> str:
        parts = []
        if self.branch_code:  parts.append(f"فرع: {self.branch_code}")
        if self.cost_center:  parts.append(f"م.تكلفة: {self.cost_center}")
        if self.project_code: parts.append(f"مشروع: {self.project_code}")
        if self.expense_classification_code: parts.append(f"تصنيف: {self.expense_classification_code}")
        return " | ".join(parts) if parts else "كل الأبعاد"


# ── Helpers ───────────────────────────────────────────────
def _sign(nature: str, debit: Decimal, credit: Decimal) -> Decimal:
    if nature == AccountNature.DEBIT:
        return (debit - credit).quantize(PREC)
    return (credit - debit).quantize(PREC)


async def _get_coa(db: AsyncSession, tenant_id: uuid.UUID) -> Dict[str, ChartOfAccount]:
    result = await db.execute(
        select(ChartOfAccount)
        .where(ChartOfAccount.tenant_id == tenant_id)
        .where(ChartOfAccount.is_active == True)
        .order_by(ChartOfAccount.code)
    )
    return {a.code: a for a in result.scalars().all()}


async def _period_balances(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year_from: int, month_from: int,
    year_to:   int, month_to:   int,
    dim: Optional[DimFilter] = None,
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate debit/credit per account_code للفترة المحددة.
    إذا كان dim.is_active → يستعلم من journal_entry_lines مع فلتر الأبعاد
    وإلا → يستعلم من account_balances (أسرع)
    """
    if dim and dim.is_active:
        return await _period_balances_dim(
            db, tenant_id, year_from, month_from, year_to, month_to, dim
        )

    # الاستعلام السريع من account_balances
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


async def _period_balances_dim(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year_from: int, month_from: int,
    year_to:   int, month_to:   int,
    dim: DimFilter,
) -> Dict[str, Dict[str, Decimal]]:
    """
    استعلام من journal_entry_lines مع فلتر الأبعاد.
    أبطأ لكن يدعم التحليل على مستوى الفرع/مركز التكلفة/المشروع.
    """
    from datetime import date as _date
    date_from = _date(year_from, month_from, 1)
    # آخر يوم في month_to
    import calendar
    last_day = calendar.monthrange(year_to, month_to)[1]
    date_to = _date(year_to, month_to, last_day)

    conditions = [
        JournalEntryLine.tenant_id == tenant_id,
        JournalEntry.tenant_id    == tenant_id,
        JournalEntry.status       == "posted",
        JournalEntry.entry_date   >= date_from,
        JournalEntry.entry_date   <= date_to,
    ]

    # فلاتر الأبعاد
    if dim.branch_code:
        conditions.append(JournalEntryLine.branch_code == dim.branch_code)
    if dim.cost_center:
        conditions.append(JournalEntryLine.cost_center == dim.cost_center)
    if dim.project_code:
        conditions.append(JournalEntryLine.project_code == dim.project_code)
    if dim.expense_classification_code:
        conditions.append(
            JournalEntryLine.expense_classification_code == dim.expense_classification_code
        )

    result = await db.execute(
        select(
            JournalEntryLine.account_code,
            func.sum(JournalEntryLine.debit).label("debit"),
            func.sum(JournalEntryLine.credit).label("credit"),
        )
        .join(JournalEntry, JournalEntryLine.journal_entry_id == JournalEntry.id)
        .where(and_(*conditions))
        .group_by(JournalEntryLine.account_code)
    )

    out: Dict[str, Dict[str, Decimal]] = {}
    for row in result:
        out[row.account_code] = {
            "debit":  Decimal(str(row.debit  or 0)),
            "credit": Decimal(str(row.credit or 0)),
        }
    return out


async def _ytd_balances(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month: int,
    dim: Optional[DimFilter] = None,
) -> Dict[str, Dict[str, Decimal]]:
    return await _period_balances(db, tenant_id, year, 1, year, month, dim)


# ══════════════════════════════════════════════════════════
# 1. Trial Balance
# ══════════════════════════════════════════════════════════
async def trial_balance(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month: int,
    dim: Optional[DimFilter] = None,
) -> Dict[str, Any]:
    coa = await _get_coa(db, tenant_id)

    open_bal = {}
    if month > 1:
        open_bal = await _period_balances(db, tenant_id, year, 1, year, month - 1, dim)

    period_bal = await _period_balances(db, tenant_id, year, month, year, month, dim)

    rows = []
    total_open_dr = total_open_cr = ZERO
    total_period_dr = total_period_cr = ZERO
    total_close_dr = total_close_cr = ZERO

    for code, acc in sorted(coa.items()):
        if not acc.postable:
            continue

        ob = open_bal.get(code, {"debit": ZERO, "credit": ZERO})
        pb = period_bal.get(code, {"debit": ZERO, "credit": ZERO})

        open_dr   = ob["debit"];  open_cr   = ob["credit"]
        period_dr = pb["debit"];  period_cr = pb["credit"]
        close_dr  = (open_dr + period_dr).quantize(PREC)
        close_cr  = (open_cr + period_cr).quantize(PREC)

        if all(v == ZERO for v in [open_dr, open_cr, period_dr, period_cr]):
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

        total_open_dr   += open_dr;   total_open_cr   += open_cr
        total_period_dr += period_dr; total_period_cr += period_cr
        total_close_dr  += close_dr;  total_close_cr  += close_cr

    return {
        "report":  "trial_balance",
        "period":  f"{year}/{month:02d}",
        "dimension_filter": dim.label() if dim else "كل الأبعاد",
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
# 2. Income Statement
# ══════════════════════════════════════════════════════════
async def income_statement(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month_from: int = 1,
    month_to:   int = 12,
    dim: Optional[DimFilter] = None,
) -> Dict[str, Any]:
    coa  = await _get_coa(db, tenant_id)
    bals = await _period_balances(db, tenant_id, year, month_from, year, month_to, dim)

    revenue_rows: List[dict] = []
    cogs_rows:    List[dict] = []
    expense_rows: List[dict] = []

    total_revenue = total_cogs = total_expense = ZERO

    for code, acc in sorted(coa.items()):
        if not acc.postable:
            continue
        b   = bals.get(code, {"debit": ZERO, "credit": ZERO})
        amt = _sign(acc.account_nature, b["debit"], b["credit"])
        if amt == ZERO:
            continue

        row = {
            "account_code": code,
            "account_name": acc.name_ar,
            "amount":       float(amt),
        }

        if acc.account_type == AccountType.REVENUE:
            revenue_rows.append(row)
            total_revenue += amt
        elif acc.account_type == AccountType.EXPENSE:
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
        "dimension_filter": dim.label() if dim else "كل الأبعاد",
        "sections": {
            "revenue":  {"label": "الإيرادات",   "rows": revenue_rows, "total": float(total_revenue)},
            "cogs":     {"label": "تكلفة البضاعة", "rows": cogs_rows,   "total": float(total_cogs)},
            "gross_profit": float(gross_profit),
            "expenses": {"label": "المصاريف",    "rows": expense_rows, "total": float(total_expense)},
        },
        "net_income":   float(net_income),
        "gross_margin": float(
            (gross_profit / total_revenue * 100).quantize(Decimal("0.01"))
            if total_revenue > 0 else ZERO
        ),
        "net_margin": float(
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
    dim: Optional[DimFilter] = None,
) -> Dict[str, Any]:
    coa  = await _get_coa(db, tenant_id)
    bals = await _ytd_balances(db, tenant_id, year, month, dim)

    ni_data    = await income_statement(db, tenant_id, year, 1, month, dim)
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

        row = {"account_code": code, "account_name": acc.name_ar, "amount": float(amt)}

        if acc.account_type == AccountType.ASSET:
            asset_rows.append(row); total_assets += amt
        elif acc.account_type == AccountType.LIABILITY:
            liability_rows.append(row); total_liabilities += amt
        elif acc.account_type == AccountType.EQUITY:
            equity_rows.append(row); total_equity += amt

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
        "dimension_filter": dim.label() if dim else "كل الأبعاد",
        "sections": {
            "assets":      {"label": "الأصول",      "rows": asset_rows,     "total": float(total_assets.quantize(PREC))},
            "liabilities": {"label": "الالتزامات",  "rows": liability_rows, "total": float(total_liabilities.quantize(PREC))},
            "equity":      {"label": "حقوق الملكية","rows": equity_rows,    "total": float(total_equity.quantize(PREC))},
        },
        "total_assets":      float(total_assets.quantize(PREC)),
        "total_liab_equity": float(total_liab_equity),
        "balanced":          diff < Decimal("1"),
        "difference":        float(diff),
    }


# ══════════════════════════════════════════════════════════
# 4. Cash Flow
# ══════════════════════════════════════════════════════════
async def cash_flow(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month_from: int = 1,
    month_to:   int = 12,
    dim: Optional[DimFilter] = None,
) -> Dict[str, Any]:
    coa  = await _get_coa(db, tenant_id)
    bals = await _period_balances(db, tenant_id, year, month_from, year, month_to, dim)

    def _movement(code: str) -> Decimal:
        b = bals.get(code, {"debit": ZERO, "credit": ZERO})
        return (b["debit"] - b["credit"]).quantize(PREC)

    ni_data    = await income_statement(db, tenant_id, year, month_from, month_to, dim)
    net_income = Decimal(str(ni_data["net_income"]))

    dep_total = ZERO
    for code, acc in coa.items():
        if code.startswith("65") and acc.postable:
            b = bals.get(code, {"debit": ZERO, "credit": ZERO})
            dep_total += b["debit"]

    ar_change = inv_change = ap_change = vat_change = ZERO
    for code, acc in coa.items():
        if not acc.postable: continue
        mv = _movement(code)
        if code.startswith("12"):   ar_change  += mv
        elif code.startswith("13"): inv_change += mv
        elif code.startswith("21"): ap_change  += mv
        elif code.startswith("22"): vat_change += mv

    op_total = (net_income + dep_total - ar_change - inv_change + ap_change + vat_change).quantize(PREC)

    fixed_change = ZERO
    for code, acc in coa.items():
        if not acc.postable: continue
        if code.startswith("15") or code.startswith("16"):
            fixed_change += _movement(code)
    inv_total = (-fixed_change).quantize(PREC)

    loan_change = capital_change = ZERO
    for code, acc in coa.items():
        if not acc.postable: continue
        mv = _movement(code)
        if code.startswith("25"):   loan_change    += mv
        elif code.startswith("30"): capital_change += mv
    fin_total = (-loan_change - capital_change).quantize(PREC)

    net_change = (op_total + inv_total + fin_total).quantize(PREC)

    open_cash = ZERO
    for code, acc in coa.items():
        if not acc.postable: continue
        if code.startswith("10") or code.startswith("11"):
            if month_from > 1:
                ob = await _period_balances(db, tenant_id, year, 1, year, month_from - 1, dim)
                b  = ob.get(code, {"debit": ZERO, "credit": ZERO})
                open_cash += (b["debit"] - b["credit"])

    closing_cash = (open_cash + net_change).quantize(PREC)

    return {
        "report": "cash_flow",
        "period": f"{year}/{month_from:02d}–{month_to:02d}",
        "method": "indirect",
        "dimension_filter": dim.label() if dim else "كل الأبعاد",
        "operating": {
            "net_income":   float(net_income),
            "depreciation": float(dep_total),
            "ar_change":    float(-ar_change),
            "inv_change":   float(-inv_change),
            "ap_change":    float(ap_change),
            "vat_change":   float(vat_change),
            "total":        float(op_total),
        },
        "investing": {
            "asset_net_change": float(inv_total),
            "total":            float(inv_total),
        },
        "financing": {
            "loans_net":   float(-loan_change),
            "capital_net": float(-capital_change),
            "total":       float(fin_total),
        },
        "net_cash_change": float(net_change),
        "opening_cash":    float(open_cash),
        "closing_cash":    float(closing_cash),
    }


# ══════════════════════════════════════════════════════════
# 5. VAT Return
# ══════════════════════════════════════════════════════════
async def vat_return(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    year: int,
    month_from: int,
    month_to:   int,
) -> Dict[str, Any]:
    bals = await _period_balances(db, tenant_id, year, month_from, year, month_to)

    vat_out_b  = bals.get("2201", {"debit": ZERO, "credit": ZERO})
    output_vat = (vat_out_b["credit"] - vat_out_b["debit"]).quantize(PREC)

    vat_in_b  = bals.get("1401", {"debit": ZERO, "credit": ZERO})
    input_vat = (vat_in_b["debit"] - vat_in_b["credit"]).quantize(PREC)

    for code, b in bals.items():
        if code.startswith("14") and code != "1401":
            input_vat += (b["debit"] - b["credit"]).quantize(PREC)

    net_vat = (output_vat - input_vat).quantize(PREC)

    coa = await _get_coa(db, tenant_id)
    taxable_sales = ZERO
    for code, acc in coa.items():
        if not acc.postable: continue
        b   = bals.get(code, {"debit": ZERO, "credit": ZERO})
        amt = _sign(acc.account_nature, b["debit"], b["credit"])
        if acc.account_type == AccountType.REVENUE:
            taxable_sales += amt

    taxable_purchases = (input_vat / Decimal("0.15")).quantize(PREC) if input_vat > 0 else ZERO

    return {
        "report":  "vat_return",
        "period":  f"{year}/{month_from:02d}–{month_to:02d}",
        "zatca": {
            "box_1_taxable_sales":     float(taxable_sales),
            "box_2_output_vat":        float(output_vat),
            "box_3_taxable_purchases": float(taxable_purchases),
            "box_4_input_vat":         float(input_vat),
            "box_5_net_vat_due":       float(net_vat),
        },
        "status":           "payable" if net_vat > 0 else ("refundable" if net_vat < 0 else "nil"),
        "payment_required": net_vat > 0,
        "refund_due":       net_vat < 0,
        "amount":           float(abs(net_vat)),
    }


# ══════════════════════════════════════════════════════════
# 6. General Ledger
# ══════════════════════════════════════════════════════════
async def general_ledger(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    account_code: str,
    date_from: date,
    date_to:   date,
) -> Dict[str, Any]:
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
    running  = ZERO
    rows     = []

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

    return {
        "report":       "general_ledger",
        "account_code": account_code,
        "account_name": acc.name_ar,
        "account_type": acc.account_type,
        "period":       f"{date_from} — {date_to}",
        "total_debit":  sum(r["debit"]  for r in rows),
        "total_credit": sum(r["credit"] for r in rows),
        "rows":         rows,
    }
