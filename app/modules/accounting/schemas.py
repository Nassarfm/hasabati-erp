"""
app/modules/accounting/schemas.py
══════════════════════════════════════════════════════════
Pydantic schemas for Accounting Module API.
Complete separation between DB models and API contract.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ══════════════════════════════════════════════════════════
# Chart of Accounts
# ══════════════════════════════════════════════════════════
class COAAccountBase(BaseModel):
    code: str              = Field(..., min_length=1, max_length=20)
    name_ar: str           = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = Field(None, max_length=255)
    account_type: str      = Field(..., pattern="^(asset|liability|equity|revenue|expense)$")
    account_nature: str    = Field(..., pattern="^(debit|credit)$")
    parent_id: Optional[uuid.UUID] = None
    postable: bool         = True
    is_active: bool        = True
    opening_balance: Decimal = Decimal("0")
    function_type: Optional[str] = Field(None, pattern="^(BS|PL|BS/PL)?$")
    grp: Optional[str]           = Field(None, max_length=100)
    sub_group: Optional[str]     = Field(None, max_length=100)
    cash_flow_type: Optional[str] = Field(None, pattern="^(operating|investing|financing|none)?$")
    dimension_required: bool      = False
    dim_branch_required:    bool  = False
    dim_cc_required:        bool  = False
    dim_project_required:   bool  = False
    dim_exp_class_required: bool  = False


class COAAccountCreate(COAAccountBase):
    pass


class COAAccountUpdate(BaseModel):
    name_ar: Optional[str]         = Field(None, min_length=1, max_length=255)
    name_en: Optional[str]         = Field(None, max_length=255)
    account_type: Optional[str]    = Field(None, pattern="^(asset|liability|equity|revenue|expense)$")
    account_nature: Optional[str]  = Field(None, pattern="^(debit|credit)$")
    parent_id: Optional[uuid.UUID] = None
    postable: Optional[bool]       = None
    is_active: Optional[bool]      = None
    opening_balance: Optional[Decimal] = None
    function_type: Optional[str]      = Field(None, pattern="^(BS|PL|BS/PL)?$")
    grp: Optional[str]                = Field(None, max_length=100)
    sub_group: Optional[str]          = Field(None, max_length=100)
    cash_flow_type: Optional[str]     = Field(None, pattern="^(operating|investing|financing|none)?$")
    dimension_required:    Optional[bool] = None
    dim_branch_required:   Optional[bool] = None
    dim_cc_required:       Optional[bool] = None
    dim_project_required:  Optional[bool] = None
    dim_exp_class_required:Optional[bool] = None


class COAAccountResponse(COAAccountBase):
    id: uuid.UUID
    level: int
    created_at: datetime
    model_config = {"from_attributes": True}


class COAAccountListItem(BaseModel):
    id: uuid.UUID
    code: str
    name_ar: str
    account_type: str
    account_nature: str
    postable: bool
    is_active: bool
    level: int
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Journal Entry
# ══════════════════════════════════════════════════════════
class JELineCreate(BaseModel):
    account_code: str   = Field(..., min_length=1, max_length=20)
    description: str    = Field(..., min_length=1, max_length=500)
    debit: Decimal      = Field(default=Decimal("0"), ge=0)
    credit: Decimal     = Field(default=Decimal("0"), ge=0)
    # الأبعاد
    branch_code: Optional[str]                    = None
    branch_name: Optional[str]                    = None
    cost_center: Optional[str]                    = None
    cost_center_name: Optional[str]               = None
    project_code: Optional[str]                   = None
    project_name: Optional[str]                   = None
    expense_classification_code: Optional[str]    = None
    expense_classification_name: Optional[str]    = None
    # ── حقول ضريبة القيمة المضافة ──
    tax_type_code: Optional[str]     = None
    vat_amount:    Optional[Decimal] = Decimal("0")
    net_amount:    Optional[Decimal] = Decimal("0")

    @model_validator(mode="after")
    def validate_one_side_only(self) -> "JELineCreate":
        if self.debit > 0 and self.credit > 0:
            raise ValueError("السطر لا يمكن أن يحتوي على مدين ودائن في آنٍ واحد")
        if self.debit == 0 and self.credit == 0:
            raise ValueError("يجب أن يحتوي السطر على مدين أو دائن")
        return self


class JELineResponse(BaseModel):
    id: uuid.UUID
    line_order: int
    account_code: str
    account_name: str
    description: str
    debit: Decimal
    credit: Decimal
    branch_code: Optional[str]                 = None
    branch_name: Optional[str]                 = None
    cost_center: Optional[str]                 = None
    cost_center_name: Optional[str]            = None
    project_code: Optional[str]                = None
    project_name: Optional[str]                = None
    expense_classification_code: Optional[str] = None
    expense_classification_name: Optional[str] = None
    # ── حقول ضريبة القيمة المضافة ──
    tax_type_code: Optional[str]     = None
    vat_amount:    Optional[Decimal] = None
    net_amount:    Optional[Decimal] = None
    model_config = {"from_attributes": True}


class JournalEntryCreate(BaseModel):
    je_type: str        = Field(default="JV")
    entry_date: date
    description: str    = Field(..., min_length=1, max_length=500)
    reference: Optional[str]        = Field(None, max_length=100)
    source_module: Optional[str]    = None
    source_doc_type: Optional[str]  = None
    source_doc_id: Optional[uuid.UUID] = None
    source_doc_number: Optional[str]   = None
    branch_code: Optional[str]      = None
    cost_center: Optional[str]      = None
    notes: Optional[str]            = None
    lines: List[JELineCreate]       = Field(..., min_length=2)

    @field_validator("lines")
    @classmethod
    def must_have_two_sides(cls, v: List[JELineCreate]) -> List[JELineCreate]:
        has_dr = any(l.debit > 0 for l in v)
        has_cr = any(l.credit > 0 for l in v)
        if not has_dr:
            raise ValueError("القيد يجب أن يحتوي على سطر مدين واحد على الأقل")
        if not has_cr:
            raise ValueError("القيد يجب أن يحتوي على سطر دائن واحد على الأقل")
        return v


class JournalEntryResponse(BaseModel):
    id: uuid.UUID
    serial: str
    je_type: str
    status: str
    entry_date: date
    posting_date: Optional[date]
    description: str
    reference: Optional[str]
    source_module: Optional[str]
    source_doc_number: Optional[str]
    total_debit: Decimal
    total_credit: Decimal
    fiscal_year: int
    fiscal_month: int
    posted_at: Optional[datetime]
    posted_by: Optional[str]
    reversed_by_je_id: Optional[uuid.UUID]
    reverses_je_id: Optional[uuid.UUID]
    lines: List[JELineResponse] = []
    created_at: datetime
    created_by: Optional[str]
    model_config = {"from_attributes": True}


class JournalEntryListItem(BaseModel):
    id: uuid.UUID
    serial: str
    je_type: str
    status: str
    entry_date: date
    description: str
    total_debit: Decimal
    total_credit: Decimal
    fiscal_year: int
    posted_at: Optional[datetime]
    source_doc_number: Optional[str]
    model_config = {"from_attributes": True}


class PostJERequest(BaseModel):
    force: bool = Field(default=False)


class ReverseJERequest(BaseModel):
    reversal_date: date
    reason: str = Field(..., min_length=5, max_length=500)


# ══════════════════════════════════════════════════════════
# Fiscal Period / Lock
# ══════════════════════════════════════════════════════════
class FiscalPeriodResponse(BaseModel):
    id: uuid.UUID
    fiscal_year: int
    fiscal_month: int
    name_ar: str
    status: str
    start_date: date
    end_date: date
    model_config = {"from_attributes": True}


class LockPeriodRequest(BaseModel):
    fiscal_year: int  = Field(..., ge=2000, le=2100)
    fiscal_month: Optional[int] = Field(None, ge=1, le=12)
    lock_type: str    = Field(..., pattern="^(soft|hard)$")
    reason: Optional[str] = Field(None, max_length=500)


class LockPeriodResponse(BaseModel):
    id: uuid.UUID
    fiscal_year: int
    fiscal_month: Optional[int]
    lock_type: str
    locked_by: str
    locked_at: datetime
    is_active: bool
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Ledger / Reports
# ══════════════════════════════════════════════════════════
class LedgerLineResponse(BaseModel):
    je_serial: str
    entry_date: date
    description: str
    debit: Decimal
    credit: Decimal
    running_balance: Decimal
    source_doc_number: Optional[str]


class LedgerResponse(BaseModel):
    account_code: str
    account_name: str
    account_nature: str
    opening_balance: Decimal
    total_debit: Decimal
    total_credit: Decimal
    closing_balance: Decimal
    lines: List[LedgerLineResponse]


class TrialBalanceLine(BaseModel):
    account_code: str
    account_name: str
    account_type: str
    opening_debit: Decimal
    opening_credit: Decimal
    period_debit: Decimal
    period_credit: Decimal
    closing_debit: Decimal
    closing_credit: Decimal


class TrialBalanceResponse(BaseModel):
    fiscal_year: int
    fiscal_month: Optional[int]
    as_of_date: date
    lines: List[TrialBalanceLine]
    total_debit: Decimal
    total_credit: Decimal
    is_balanced: bool
