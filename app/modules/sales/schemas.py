"""
app/modules/sales/schemas.py
══════════════════════════════════════════════════════════
Pydantic schemas — Sales Module.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, model_validator


# ══════════════════════════════════════════════════════════
# Customer
# ══════════════════════════════════════════════════════════
class CustomerCreate(BaseModel):
    code: str      = Field(..., min_length=1, max_length=30)
    name_ar: str   = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = None
    customer_type: str = Field(default="company",
                               pattern="^(individual|company|government)$")
    phone: Optional[str]   = None
    email: Optional[str]   = None
    address: Optional[str] = None
    city: Optional[str]    = None
    vat_number: Optional[str] = None
    cr_number: Optional[str]  = None
    payment_term: str = Field(default="net_30",
                              pattern="^(cash|net_15|net_30|net_60|net_90)$")
    credit_limit: Decimal  = Field(default=Decimal("0"), ge=0)
    discount_pct: Decimal  = Field(default=Decimal("0"), ge=0, le=100)
    ar_account: str        = Field(default="1201")
    notes: Optional[str]   = None


class CustomerUpdate(BaseModel):
    name_ar: Optional[str]       = None
    phone: Optional[str]         = None
    email: Optional[str]         = None
    address: Optional[str]       = None
    payment_term: Optional[str]  = None
    credit_limit: Optional[Decimal] = None
    discount_pct: Optional[Decimal] = None
    is_active: Optional[bool]    = None


class CustomerResponse(CustomerCreate):
    id: uuid.UUID
    is_active: bool
    created_at: datetime
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Invoice Line
# ══════════════════════════════════════════════════════════
class InvoiceLineCreate(BaseModel):
    product_code: str   = Field(..., min_length=1)
    product_name: Optional[str] = None   # auto-filled from product if None
    qty: Decimal        = Field(..., gt=0)
    unit_price: Decimal = Field(..., ge=0)
    discount_pct: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    vat_rate: Decimal   = Field(default=Decimal("15"), ge=0, le=100)
    revenue_account: str = Field(default="4001")
    notes: Optional[str] = None


class InvoiceLineResponse(BaseModel):
    id: uuid.UUID
    line_number: int
    product_code: str
    product_name: str
    qty: Decimal
    unit_price: Decimal
    discount_pct: Decimal
    discount_amount: Decimal
    line_total: Decimal
    vat_rate: Decimal
    vat_amount: Decimal
    line_total_with_vat: Decimal
    unit_cost: Decimal
    total_cost: Decimal
    qty_returned: Decimal
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Sales Invoice
# ══════════════════════════════════════════════════════════
class SalesInvoiceCreate(BaseModel):
    invoice_date: date
    customer_code: str  = Field(..., min_length=1)
    warehouse_code: str = Field(default="MAIN")
    payment_term: str   = Field(default="net_30",
                                pattern="^(cash|net_15|net_30|net_60|net_90)$")
    discount_pct: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    ar_account: str     = Field(default="1201")
    notes: Optional[str]     = None
    reference: Optional[str] = None
    lines: List[InvoiceLineCreate] = Field(..., min_length=1)


class SalesInvoiceResponse(BaseModel):
    id: uuid.UUID
    invoice_number: str
    invoice_date: date
    due_date: Optional[date]
    status: str
    customer_code: str
    customer_name: str
    warehouse_code: str
    subtotal: Decimal
    discount_amount: Decimal
    taxable_amount: Decimal
    vat_amount: Decimal
    total_amount: Decimal
    total_cost: Decimal
    gross_profit: Decimal
    je_serial: Optional[str]
    posted_at: Optional[datetime]
    returned_amount: Decimal
    lines: List[InvoiceLineResponse] = []
    model_config = {"from_attributes": True}


class SalesInvoiceListItem(BaseModel):
    id: uuid.UUID
    invoice_number: str
    invoice_date: date
    status: str
    customer_name: str
    total_amount: Decimal
    balance_due: Decimal
    je_serial: Optional[str]
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Sales Return
# ══════════════════════════════════════════════════════════
class ReturnLineCreate(BaseModel):
    product_code: str = Field(..., min_length=1)
    qty: Decimal      = Field(..., gt=0)
    unit_price: Decimal = Field(..., ge=0)
    vat_rate: Decimal   = Field(default=Decimal("15"), ge=0, le=100)
    invoice_line_id: Optional[uuid.UUID] = None
    notes: Optional[str] = None


class SalesReturnCreate(BaseModel):
    return_date: date
    invoice_id: uuid.UUID
    warehouse_code: str = Field(default="MAIN")
    reason: Optional[str] = None
    notes: Optional[str]  = None
    lines: List[ReturnLineCreate] = Field(..., min_length=1)

    @model_validator(mode="after")
    def validate_lines(self) -> "SalesReturnCreate":
        if not self.lines:
            raise ValueError("يجب أن يحتوي المرتجع على بند واحد على الأقل")
        return self


class SalesReturnResponse(BaseModel):
    id: uuid.UUID
    return_number: str
    return_date: date
    status: str
    invoice_number: str
    customer_name: str
    subtotal: Decimal
    vat_amount: Decimal
    total_amount: Decimal
    je_serial: Optional[str]
    posted_at: Optional[datetime]
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Dashboard / Reports
# ══════════════════════════════════════════════════════════
class SalesDashboardResponse(BaseModel):
    period_label: str
    total_invoices: int
    total_revenue: Decimal
    total_vat: Decimal
    total_cogs: Decimal
    gross_profit: Decimal
    gross_margin_pct: Decimal
    outstanding_ar: Decimal
    top_customers: list = []
