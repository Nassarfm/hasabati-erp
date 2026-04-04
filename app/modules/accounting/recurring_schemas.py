"""
app/modules/accounting/recurring_schemas.py
Pydantic schemas للقيود المتكررة
"""
from __future__ import annotations
import uuid
from datetime import date
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


# ──────────────────────────────────────────────────────────
# سطر القيد في القالب
# ──────────────────────────────────────────────────────────
class RecurringLineTemplate(BaseModel):
    account_code:               str
    account_name:               str            = ""
    description:                str            = ""
    # نسبة المدين والدائن من المبلغ الإجمالي (0-100)
    debit_pct:                  Decimal        = Decimal("0")
    credit_pct:                 Decimal        = Decimal("0")
    # أبعاد محاسبية (اختيارية)
    branch_code:                Optional[str]  = None
    branch_name:                Optional[str]  = None
    cost_center:                Optional[str]  = None
    cost_center_name:           Optional[str]  = None
    project_code:               Optional[str]  = None
    project_name:               Optional[str]  = None
    expense_classification_code: Optional[str] = None
    expense_classification_name: Optional[str] = None


# ──────────────────────────────────────────────────────────
# إنشاء قيد متكرر جديد
# ──────────────────────────────────────────────────────────
class RecurringEntryCreate(BaseModel):
    name:        str = Field(..., min_length=2, max_length=255)
    description: str = Field(..., min_length=2, max_length=500)
    total_amount: Decimal = Field(..., gt=0)

    frequency:  str = Field("monthly",  description="monthly|quarterly|semiannual|annual|weekly")
    post_day:   str = Field("start",    description="start|end")
    start_date: date
    end_date:   date
    je_type:    str = Field("JV")

    lines: List[RecurringLineTemplate] = Field(..., min_length=2)
    notes: Optional[str] = None

    @field_validator("frequency")
    @classmethod
    def validate_frequency(cls, v):
        allowed = {"monthly","quarterly","semiannual","annual","weekly"}
        if v not in allowed:
            raise ValueError(f"frequency must be one of {allowed}")
        return v

    @field_validator("post_day")
    @classmethod
    def validate_post_day(cls, v):
        if v not in {"start","end"}:
            raise ValueError("post_day must be 'start' or 'end'")
        return v

    @model_validator(mode="after")
    def validate_dates(self):
        if self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self

    @model_validator(mode="after")
    def validate_lines_balance(self):
        total_dr = sum(l.debit_pct  for l in self.lines)
        total_cr = sum(l.credit_pct for l in self.lines)
        if abs(total_dr - total_cr) > Decimal("0.01"):
            raise ValueError(f"Lines debit%={total_dr} must equal credit%={total_cr}")
        if abs(total_dr - 100) > Decimal("0.01"):
            raise ValueError(f"Total debit% must equal 100, got {total_dr}")
        return self


# ──────────────────────────────────────────────────────────
# معاينة جدول الإطفاء (قبل الإنشاء)
# ──────────────────────────────────────────────────────────
class RecurringPreviewRequest(BaseModel):
    total_amount: Decimal = Field(..., gt=0)
    frequency:    str     = "monthly"
    post_day:     str     = "start"
    start_date:   date
    end_date:     date


class RecurringPreviewItem(BaseModel):
    installment_number: int
    scheduled_date:     date
    amount:             Decimal
    cumulative:         Decimal


class RecurringPreviewResponse(BaseModel):
    total_installments:       int
    installment_amount:       Decimal
    last_installment_amount:  Decimal
    total_amount:             Decimal
    schedule:                 List[RecurringPreviewItem]


# ──────────────────────────────────────────────────────────
# استجابة القيد المتكرر
# ──────────────────────────────────────────────────────────
class RecurringInstanceOut(BaseModel):
    id:                   uuid.UUID
    installment_number:   int
    scheduled_date:       date
    amount:               Decimal
    status:               str
    journal_entry_id:     Optional[uuid.UUID] = None
    journal_entry_serial: Optional[str]       = None
    posted_at:            Optional[str]       = None
    posted_by:            Optional[str]       = None
    note:                 Optional[str]       = None

    model_config = {"from_attributes": True}


class RecurringEntryOut(BaseModel):
    id:                      uuid.UUID
    code:                    str
    name:                    str
    description:             str
    total_amount:            Decimal
    installment_amount:      Decimal
    last_installment_amount: Decimal
    total_installments:      int
    frequency:               str
    post_day:                str
    start_date:              date
    end_date:                date
    je_type:                 str
    status:                  str
    posted_count:            int
    pending_count:           int
    skipped_count:           int
    notes:                   Optional[str]              = None
    lines_template:          list
    instances:               List[RecurringInstanceOut] = []

    model_config = {"from_attributes": True}


class RecurringEntryListItem(BaseModel):
    id:                 uuid.UUID
    code:               str
    name:               str
    description:        str
    total_amount:       Decimal
    installment_amount: Decimal
    total_installments: int
    frequency:          str
    status:             str
    posted_count:       int
    pending_count:      int
    skipped_count:      int
    start_date:         date
    end_date:           date

    model_config = {"from_attributes": True}


# ──────────────────────────────────────────────────────────
# تحديث الحالة
# ──────────────────────────────────────────────────────────
class RecurringStatusUpdate(BaseModel):
    status: str  # paused | active | cancelled

class RecurringSkipInstance(BaseModel):
    note: Optional[str] = None
