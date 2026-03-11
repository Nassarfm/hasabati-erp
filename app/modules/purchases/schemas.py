"""app/modules/purchases/schemas.py"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class SupplierCreate(BaseModel):
    code: str    = Field(..., min_length=1, max_length=30)
    name_ar: str = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = None
    phone:   Optional[str] = None
    email:   Optional[str] = None
    address: Optional[str] = None
    city:    Optional[str] = None
    vat_number: Optional[str] = None
    cr_number:  Optional[str] = None
    payment_term: str    = Field(default="net_30", pattern="^(cash|net_15|net_30|net_60|net_90)$")
    credit_limit: Decimal = Field(default=Decimal("0"), ge=0)
    discount_pct: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    ap_account: str = Field(default="2101")
    notes: Optional[str] = None


class SupplierUpdate(BaseModel):
    name_ar: Optional[str]       = None
    phone:   Optional[str]       = None
    email:   Optional[str]       = None
    address: Optional[str]       = None
    payment_term: Optional[str]  = None
    credit_limit: Optional[Decimal] = None
    is_active: Optional[bool]    = None


class SupplierResponse(SupplierCreate):
    id: uuid.UUID
    is_active: bool
    created_at: datetime
    model_config = {"from_attributes": True}


# ── PO ────────────────────────────────────────────────────
class POLineCreate(BaseModel):
    product_code: str   = Field(..., min_length=1)
    product_name: Optional[str] = None
    qty_ordered:  Decimal = Field(..., gt=0)
    unit_price:   Decimal = Field(..., ge=0)
    discount_pct: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    vat_rate:     Decimal = Field(default=Decimal("15"), ge=0, le=100)
    inventory_account: str = Field(default="1301")
    notes: Optional[str] = None


class POCreate(BaseModel):
    po_date:       date
    required_date: Optional[date] = None
    supplier_code: str  = Field(..., min_length=1)
    warehouse_code: str = Field(default="MAIN")
    payment_term:  str  = Field(default="net_30", pattern="^(cash|net_15|net_30|net_60|net_90)$")
    discount_pct:  Decimal = Field(default=Decimal("0"), ge=0, le=100)
    notes:         Optional[str] = None
    reference:     Optional[str] = None
    lines: List[POLineCreate] = Field(..., min_length=1)


class POLineResponse(BaseModel):
    id: uuid.UUID
    line_number:  int
    product_code: str
    product_name: str
    qty_ordered:  Decimal
    qty_received: Decimal
    qty_invoiced: Decimal
    qty_pending:  Decimal
    unit_price:   Decimal
    line_total:   Decimal
    vat_amount:   Decimal
    line_total_with_vat: Decimal
    model_config = {"from_attributes": True}


class POResponse(BaseModel):
    id: uuid.UUID
    po_number:     str
    po_date:       date
    status:        str
    supplier_code: str
    supplier_name: str
    subtotal:      Decimal
    vat_amount:    Decimal
    total_amount:  Decimal
    qty_received_pct: Decimal
    qty_invoiced_pct: Decimal
    lines: List[POLineResponse] = []
    model_config = {"from_attributes": True}


# ── GRN ───────────────────────────────────────────────────
class GRNLineCreate(BaseModel):
    product_code:  str     = Field(..., min_length=1)
    qty_received:  Decimal = Field(..., gt=0)
    unit_cost:     Decimal = Field(..., ge=0)
    po_line_id:    Optional[uuid.UUID] = None
    notes:         Optional[str] = None


class GRNCreate(BaseModel):
    grn_date:       date
    po_id:          uuid.UUID
    warehouse_code: str = Field(default="MAIN")
    delivery_note:  Optional[str] = None
    notes:          Optional[str] = None
    lines: List[GRNLineCreate] = Field(..., min_length=1)


class GRNLineResponse(BaseModel):
    id: uuid.UUID
    line_number:  int
    product_code: str
    product_name: str
    qty_received: Decimal
    unit_cost:    Decimal
    total_cost:   Decimal
    wac_before:   Decimal
    wac_after:    Decimal
    model_config = {"from_attributes": True}


class GRNResponse(BaseModel):
    id: uuid.UUID
    grn_number:    str
    grn_date:      date
    status:        str
    po_number:     str
    supplier_name: str
    warehouse_code: str
    total_cost:    Decimal
    je_serial:     Optional[str]
    posted_at:     Optional[datetime]
    lines: List[GRNLineResponse] = []
    model_config = {"from_attributes": True}


# ── Vendor Invoice ────────────────────────────────────────
class VILineCreate(BaseModel):
    product_code:  str     = Field(..., min_length=1)
    product_name:  Optional[str] = None
    qty_invoiced:  Decimal = Field(..., gt=0)
    unit_price:    Decimal = Field(..., ge=0)
    discount_pct:  Decimal = Field(default=Decimal("0"), ge=0, le=100)
    vat_rate:      Decimal = Field(default=Decimal("15"), ge=0, le=100)
    po_line_id:    Optional[uuid.UUID] = None
    grn_line_id:   Optional[uuid.UUID] = None
    notes:         Optional[str] = None


class VendorInvoiceCreate(BaseModel):
    invoice_date:  date
    po_id:         uuid.UUID
    grn_id:        Optional[uuid.UUID] = None
    vendor_ref:    Optional[str] = None
    payment_term:  str = Field(default="net_30", pattern="^(cash|net_15|net_30|net_60|net_90)$")
    ap_account:    str = Field(default="2101")
    notes:         Optional[str] = None
    lines: List[VILineCreate] = Field(..., min_length=1)


class MatchResult(BaseModel):
    passed: bool
    tolerance_pct: Decimal
    issues: List[str] = []


class VendorInvoiceResponse(BaseModel):
    id: uuid.UUID
    vi_number:     str
    invoice_date:  date
    due_date:      Optional[date]
    status:        str
    po_number:     str
    grn_number:    Optional[str]
    supplier_name: str
    subtotal:      Decimal
    vat_amount:    Decimal
    total_amount:  Decimal
    balance_due:   Decimal
    match_status:  Optional[str]
    je_serial:     Optional[str]
    posted_at:     Optional[datetime]
    model_config = {"from_attributes": True}
