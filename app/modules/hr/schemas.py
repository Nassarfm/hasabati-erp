"""app/modules/hr/schemas.py — متوافق مع models.py الجديد (بدون Contract)"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


# ── Employee ──────────────────────────────────────────────
class EmployeeCreate(BaseModel):
    employee_number: str     = Field(..., min_length=1, max_length=30)
    first_name_ar:   str     = Field(..., min_length=1)
    last_name_ar:    str     = Field(..., min_length=1)
    first_name_en:   Optional[str] = None
    last_name_en:    Optional[str] = None
    gender:          str     = Field(default="male",  pattern="^(male|female)$")
    nationality:     str     = Field(default="saudi", pattern="^(saudi|expat)$")
    date_of_birth:   Optional[date] = None
    national_id:     Optional[str]  = None
    iqama_number:    Optional[str]  = None
    iqama_expiry:    Optional[date] = None
    hire_date:       date
    department:      Optional[str]  = None
    job_title:       Optional[str]  = None
    cost_center:     Optional[str]  = None
    # Salary (embedded — لا يوجد contract منفصل)
    basic_salary:    Decimal = Field(..., gt=0)
    housing_allow:   Decimal = Field(default=Decimal("0"), ge=0)
    transport_allow: Decimal = Field(default=Decimal("0"), ge=0)
    other_allow:     Decimal = Field(default=Decimal("0"), ge=0)
    gosi_enrolled:   bool    = True
    gosi_number:     Optional[str] = None
    bank_name:       Optional[str] = None
    bank_iban:       Optional[str] = None
    phone:           Optional[str] = None
    email:           Optional[str] = None
    notes:           Optional[str] = None


class EmployeeUpdate(BaseModel):
    first_name_ar:   Optional[str]     = None
    last_name_ar:    Optional[str]     = None
    job_title:       Optional[str]     = None
    department:      Optional[str]     = None
    basic_salary:    Optional[Decimal] = None
    housing_allow:   Optional[Decimal] = None
    transport_allow: Optional[Decimal] = None
    other_allow:     Optional[Decimal] = None
    bank_iban:       Optional[str]     = None
    status:          Optional[str]     = None
    phone:           Optional[str]     = None
    email:           Optional[str]     = None


class EmployeeResponse(BaseModel):
    id:              uuid.UUID
    employee_number: str
    first_name_ar:   str
    last_name_ar:    str
    nationality:     str
    status:          str
    hire_date:       date
    department:      Optional[str]
    job_title:       Optional[str]
    basic_salary:    Decimal
    housing_allow:   Decimal
    transport_allow: Decimal
    other_allow:     Decimal
    gosi_enrolled:   bool
    model_config = {"from_attributes": True}


# ── Payroll Run ───────────────────────────────────────────
class PayrollRunCreate(BaseModel):
    period_year:  int  = Field(..., ge=2020, le=2100)
    period_month: int  = Field(..., ge=1, le=12)
    pay_date:     date
    notes:        Optional[str] = None


class PayrollLineOverride(BaseModel):
    """تعديلات اختيارية على راتب موظف محدد في الدورة."""
    employee_id:       uuid.UUID
    overtime_amount:   Decimal = Field(default=Decimal("0"), ge=0)
    bonus_amount:      Decimal = Field(default=Decimal("0"), ge=0)
    advance_deduction: Decimal = Field(default=Decimal("0"), ge=0)
    absence_deduction: Decimal = Field(default=Decimal("0"), ge=0)
    other_deductions:  Decimal = Field(default=Decimal("0"), ge=0)
    absent_days:       int     = Field(default=0, ge=0)


class PayrollLineResponse(BaseModel):
    id:               uuid.UUID
    employee_number:  str
    employee_name:    str
    department:       Optional[str]
    nationality:      str
    basic_salary:     Decimal
    gross_salary:     Decimal
    gosi_employee:    Decimal
    gosi_employer:    Decimal
    deductions_total: Decimal
    net_salary:       Decimal
    eosb_accrual:     Decimal
    model_config = {"from_attributes": True}


class PayrollRunResponse(BaseModel):
    id:                  uuid.UUID
    run_number:          str
    period_year:         int
    period_month:        int
    pay_date:            date
    status:              str
    employee_count:      int
    total_basic:         Decimal
    total_allowances:    Decimal
    total_gross:         Decimal
    total_gosi_employee: Decimal
    total_gosi_employer: Decimal
    total_deductions:    Decimal
    total_net:           Decimal
    total_eosb_accrual:  Decimal
    je_serial:           Optional[str]
    posted_at:           Optional[datetime]
    lines: List[PayrollLineResponse] = []
    model_config = {"from_attributes": True}


# ── Leave ─────────────────────────────────────────────────
class LeaveRequestCreate(BaseModel):
    employee_id: uuid.UUID
    leave_type:  str  = Field(..., pattern="^(annual|sick|maternity|paternity|emergency|unpaid|other)$")
    start_date:  date
    end_date:    date
    reason:      Optional[str] = None
    notes:       Optional[str] = None

    @model_validator(mode="after")
    def validate_dates(self) -> "LeaveRequestCreate":
        if self.end_date < self.start_date:
            raise ValueError("تاريخ النهاية يجب أن يكون بعد تاريخ البداية")
        return self


class LeaveApprove(BaseModel):
    days_approved: int = Field(..., gt=0)
    notes:         Optional[str] = None


class LeaveResponse(BaseModel):
    id:              uuid.UUID
    leave_number:    str
    employee_name:   str
    leave_type:      str
    status:          str
    start_date:      date
    end_date:        date
    days_requested:  int
    days_approved:   int
    approved_by:     Optional[str]
    model_config = {"from_attributes": True}
