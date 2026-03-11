"""
app/modules/hr/gosi.py
══════════════════════════════════════════════════════════
GOSI & EOSB Calculation Engine — محرك حساب التأمينات ومكافأة نهاية الخدمة

GOSI Rates (Saudi Arabia — 2024):
  سعودي:
    موظف:        9.00%  (من الراتب الخاضع)
    صاحب عمل:   9.00%  (من الراتب الخاضع)
  أجنبي:
    موظف:        0.00%
    صاحب عمل:   2.00%  (تأمين ضد الأخطار المهنية)

EOSB (مكافأة نهاية الخدمة) — نظام العمل السعودي:
  سنة 1–5:      50%  من الراتب الأساسي × سنوات الخدمة
  سنة 5+:       100% من الراتب الأساسي × سنوات الخدمة

الراتب الخاضع للتأمينات = الأساسي + بدل السكن (عادةً)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

# ── GOSI Rates ────────────────────────────────────────────
GOSI_SAUDI_EMPLOYEE  = Decimal("9.00")    # %
GOSI_SAUDI_EMPLOYER  = Decimal("9.00")    # %
GOSI_FOREIGN_EMPLOYEE = Decimal("0.00")   # %
GOSI_FOREIGN_EMPLOYER = Decimal("2.00")   # % (Occupational Hazard)

PREC = Decimal("0.001")


@dataclass
class GOSIResult:
    subject_to:    Decimal   # الراتب الخاضع
    employee_pct:  Decimal   # نسبة الموظف
    employer_pct:  Decimal   # نسبة صاحب العمل
    employee_amount: Decimal  # اشتراك الموظف
    employer_amount: Decimal  # اشتراك صاحب العمل
    total_gosi:    Decimal   # الإجمالي


@dataclass
class EOSBResult:
    years_of_service: Decimal
    monthly_basic:    Decimal
    accrual_amount:   Decimal   # Monthly accrual (not total)
    total_entitlement: Decimal  # Full EOSB if terminated today


def calc_gosi(
    basic_salary: Decimal,
    housing_allowance: Decimal,
    nationality: str,         # "saudi" | "foreign"
) -> GOSIResult:
    """
    حساب اشتراكات GOSI.
    الراتب الخاضع = الأساسي + السكن (وفق لوائح GOSI السعودية).
    """
    subject_to = (basic_salary + housing_allowance).quantize(PREC)

    if nationality.lower() == "saudi":
        emp_pct = GOSI_SAUDI_EMPLOYEE
        er_pct  = GOSI_SAUDI_EMPLOYER
    else:
        emp_pct = GOSI_FOREIGN_EMPLOYEE
        er_pct  = GOSI_FOREIGN_EMPLOYER

    emp_amount = (subject_to * emp_pct / 100).quantize(PREC, ROUND_HALF_UP)
    er_amount  = (subject_to * er_pct  / 100).quantize(PREC, ROUND_HALF_UP)

    return GOSIResult(
        subject_to=subject_to,
        employee_pct=emp_pct,
        employer_pct=er_pct,
        employee_amount=emp_amount,
        employer_amount=er_amount,
        total_gosi=(emp_amount + er_amount).quantize(PREC),
    )


def calc_eosb(
    basic_salary: Decimal,
    hire_date: date,
    as_of_date: date,
) -> EOSBResult:
    """
    حساب مكافأة نهاية الخدمة وفق نظام العمل السعودي.

    المادة 84 من نظام العمل:
      - أقل من سنتين: لا يستحق
      - 2–5 سنوات: نصف أجر شهري عن كل سنة
      - أكثر من 5 سنوات: أجر شهري كامل عن كل سنة
    """
    # Calculate years of service
    days = (as_of_date - hire_date).days
    years = Decimal(str(days / 365.25)).quantize(Decimal("0.01"))

    if years < Decimal("2"):
        # Less than 2 years — no entitlement
        monthly_accrual = Decimal("0")
        total = Decimal("0")
    elif years <= Decimal("5"):
        # 2-5 years: half month per year
        total = (basic_salary * Decimal("0.5") * years).quantize(PREC, ROUND_HALF_UP)
        monthly_accrual = (basic_salary * Decimal("0.5") / 12).quantize(PREC, ROUND_HALF_UP)
    else:
        # Over 5 years: full month per year
        # First 5 years at half rate, rest at full rate
        first_5 = basic_salary * Decimal("0.5") * Decimal("5")
        remaining = basic_salary * (years - Decimal("5"))
        total = (first_5 + remaining).quantize(PREC, ROUND_HALF_UP)
        monthly_accrual = (basic_salary / 12).quantize(PREC, ROUND_HALF_UP)

    return EOSBResult(
        years_of_service=years,
        monthly_basic=basic_salary,
        accrual_amount=monthly_accrual,
        total_entitlement=total,
    )


def calc_payroll_line(
    basic_salary: Decimal,
    housing_allowance: Decimal,
    transport_allowance: Decimal,
    food_allowance: Decimal,
    phone_allowance: Decimal,
    other_allowances: Decimal,
    overtime_amount: Decimal,
    bonus_amount: Decimal,
    nationality: str,
    hire_date: date,
    period_date: date,
    advance_deduction: Decimal = Decimal("0"),
    absence_deduction: Decimal = Decimal("0"),
    other_deductions:  Decimal = Decimal("0"),
) -> dict:
    """
    يحسب جميع مكونات راتب موظف واحد.
    يُعيد dict يمكن تحميله مباشرة في PayrollLine.
    """
    # Gross
    gross = (
        basic_salary + housing_allowance + transport_allowance +
        food_allowance + phone_allowance + other_allowances +
        overtime_amount + bonus_amount
    ).quantize(PREC)

    # GOSI
    gosi = calc_gosi(basic_salary, housing_allowance, nationality)

    # EOSB monthly accrual
    eosb = calc_eosb(basic_salary, hire_date, period_date)

    # Total deductions (employee GOSI + other)
    total_ded = (
        gosi.employee_amount + advance_deduction +
        absence_deduction + other_deductions
    ).quantize(PREC)

    # Net
    net = (gross - total_ded).quantize(PREC)

    return {
        "basic_salary":       basic_salary,
        "housing_allowance":  housing_allowance,
        "transport_allowance": transport_allowance,
        "food_allowance":     food_allowance,
        "phone_allowance":    phone_allowance,
        "other_allowances":   other_allowances,
        "overtime_amount":    overtime_amount,
        "bonus_amount":       bonus_amount,
        "gross_salary":       gross,
        "gosi_subject_to":    gosi.subject_to,
        "gosi_employee_pct":  gosi.employee_pct,
        "gosi_employer_pct":  gosi.employer_pct,
        "gosi_employee":      gosi.employee_amount,
        "gosi_employer":      gosi.employer_amount,
        "advance_deduction":  advance_deduction,
        "absence_deduction":  absence_deduction,
        "other_deductions":   other_deductions,
        "total_deductions":   total_ded,
        "net_salary":         net,
        "years_of_service":   eosb.years_of_service,
        "eosb_accrual":       eosb.accrual_amount,
    }
