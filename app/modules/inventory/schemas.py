"""
app/modules/inventory/schemas.py
══════════════════════════════════════════════════════════
Pydantic schemas for Inventory Module API.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ══════════════════════════════════════════════════════════
# Product
# ══════════════════════════════════════════════════════════
class ProductCreate(BaseModel):
    code: str           = Field(..., min_length=1, max_length=50)
    name_ar: str        = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = None
    barcode: Optional[str] = None
    product_type: str   = Field(default="stockable",
                                pattern="^(stockable|service|consumable)$")
    category_code: Optional[str] = None
    unit_of_measure: str = Field(default="قطعة")
    costing_method: str  = Field(default="WAC", pattern="^(WAC|FIFO)$")
    standard_cost: Decimal = Field(default=Decimal("0"), ge=0)
    sale_price: Decimal    = Field(default=Decimal("0"), ge=0)
    vat_rate: Decimal      = Field(default=Decimal("15"), ge=0, le=100)
    inventory_account: str = Field(default="1300")
    cogs_account: str      = Field(default="5001")
    track_stock: bool      = True
    min_qty: Decimal       = Field(default=Decimal("0"), ge=0)
    reorder_point: Decimal = Field(default=Decimal("0"), ge=0)
    is_purchasable: bool   = True
    is_sellable: bool      = True


class ProductResponse(ProductCreate):
    id: uuid.UUID
    average_cost: Decimal
    last_purchase_price: Decimal
    is_active: bool
    created_at: datetime
    model_config = {"from_attributes": True}


class ProductListItem(BaseModel):
    id: uuid.UUID
    code: str
    name_ar: str
    product_type: str
    unit_of_measure: str
    average_cost: Decimal
    sale_price: Decimal
    is_active: bool
    model_config = {"from_attributes": True}


class ProductUpdate(BaseModel):
    name_ar: Optional[str]     = None
    name_en: Optional[str]     = None
    sale_price: Optional[Decimal] = None
    min_qty: Optional[Decimal] = None
    reorder_point: Optional[Decimal] = None
    is_active: Optional[bool]  = None


# ══════════════════════════════════════════════════════════
# Warehouse
# ══════════════════════════════════════════════════════════
class WarehouseCreate(BaseModel):
    code: str    = Field(..., min_length=1, max_length=20)
    name_ar: str = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str]    = None
    is_default: bool = False
    allow_negative_stock: bool = False
    inventory_account: Optional[str] = None


class WarehouseResponse(WarehouseCreate):
    id: uuid.UUID
    is_active: bool
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Stock Balance
# ══════════════════════════════════════════════════════════
class StockBalanceResponse(BaseModel):
    product_code: str
    warehouse_code: str
    qty_on_hand: Decimal
    qty_reserved: Decimal
    qty_available: Decimal
    qty_incoming: Decimal
    average_cost: Decimal
    total_value: Decimal
    last_movement_date: Optional[date]
    last_movement_type: Optional[str]
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Stock Movement
# ══════════════════════════════════════════════════════════
class StockMovementCreate(BaseModel):
    """Manual movement (opening balance, adjustment, transfer)."""
    movement_type: str  = Field(..., pattern="^(OPENING_BALANCE|ADJUSTMENT_IN|ADJUSTMENT_OUT|TRANSFER_IN|TRANSFER_OUT|PURCHASE_RETURN|SALES_RETURN)$")
    movement_date: date
    product_code: str   = Field(..., min_length=1)
    warehouse_code: str = Field(..., min_length=1)
    dest_warehouse_code: Optional[str] = None   # for transfers
    qty: Decimal        = Field(..., gt=0)
    unit_cost: Decimal  = Field(..., ge=0)
    description: Optional[str] = None
    notes: Optional[str] = None

    @model_validator(mode="after")
    def transfer_needs_dest(self) -> "StockMovementCreate":
        if "TRANSFER" in self.movement_type and not self.dest_warehouse_code:
            raise ValueError("نقل المخزون يحتاج تحديد المستودع المستهدف")
        return self


class StockMovementResponse(BaseModel):
    id: uuid.UUID
    movement_number: str
    movement_type: str
    movement_date: date
    status: str
    product_code: str
    product_name: str
    warehouse_code: str
    qty: Decimal
    unit_cost: Decimal
    total_cost: Decimal
    wac_before: Decimal
    wac_after: Decimal
    qty_before: Decimal
    qty_after: Decimal
    je_serial: Optional[str]
    source_doc_number: Optional[str]
    posted_at: Optional[datetime]
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Stock Adjustment
# ══════════════════════════════════════════════════════════
class AdjustmentLineCreate(BaseModel):
    product_code: str    = Field(..., min_length=1)
    qty_counted: Decimal = Field(..., ge=0)
    notes: Optional[str] = None


class StockAdjustmentCreate(BaseModel):
    adj_date: date
    warehouse_code: str  = Field(..., min_length=1)
    reason: Optional[str] = None
    notes: Optional[str]  = None
    lines: List[AdjustmentLineCreate] = Field(..., min_length=1)


class StockAdjustmentResponse(BaseModel):
    id: uuid.UUID
    adj_number: str
    adj_date: date
    status: str
    warehouse_code: str
    reason: Optional[str]
    je_serial: Optional[str]
    posted_at: Optional[datetime]
    lines: list = []
    model_config = {"from_attributes": True}


# ══════════════════════════════════════════════════════════
# Inventory Reports
# ══════════════════════════════════════════════════════════
class InventoryValuationLine(BaseModel):
    product_code: str
    product_name: str
    warehouse_code: str
    qty_on_hand: Decimal
    average_cost: Decimal
    total_value: Decimal


class InventoryValuationResponse(BaseModel):
    as_of_date: date
    lines: List[InventoryValuationLine]
    grand_total_value: Decimal
    total_products: int
