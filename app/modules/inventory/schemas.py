"""
app/modules/inventory/schemas.py
═══════════════════════════════════════════════════════════════════════════
Inventory Module - Pydantic Schemas
═══════════════════════════════════════════════════════════════════════════
كل الحقول معرّفة صراحة (Pydantic يحذف غير المعرّفة بصمت!)
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# ═══════════════════════════════════════════════════════════════════════════
# COMMON
# ═══════════════════════════════════════════════════════════════════════════
class BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ═══════════════════════════════════════════════════════════════════════════
# 1. UOM + UOM Conversions
# ═══════════════════════════════════════════════════════════════════════════
class UOMOut(BaseSchema):
    id: uuid.UUID
    uom_code: str
    uom_name: str
    uom_name_en: Optional[str] = None
    is_base: bool = False


class UOMCreate(BaseModel):
    uom_code: str
    uom_name: str
    uom_name_en: Optional[str] = None
    is_base: bool = False


class UOMConversionOut(BaseSchema):
    id: uuid.UUID
    from_uom_id: uuid.UUID
    from_uom_code: Optional[str] = None
    from_uom_name: Optional[str] = None
    to_uom_id: uuid.UUID
    to_uom_code: Optional[str] = None
    to_uom_name: Optional[str] = None
    factor: Decimal
    item_id: Optional[uuid.UUID] = None
    item_code: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None


class UOMConversionCreate(BaseModel):
    from_uom_id: uuid.UUID
    to_uom_id: uuid.UUID
    factor: Decimal
    item_id: Optional[uuid.UUID] = None
    notes: Optional[str] = None


class UOMConversionUpdate(BaseModel):
    factor: Optional[Decimal] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 2. Categories
# ═══════════════════════════════════════════════════════════════════════════
class CategoryOut(BaseSchema):
    id: uuid.UUID
    category_code: str
    category_name: str
    parent_id: Optional[uuid.UUID] = None
    parent_name: Optional[str] = None
    item_type: Optional[str] = None
    gl_account_code: Optional[str] = None
    cogs_account_code: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None


class CategoryCreate(BaseModel):
    category_code: str
    category_name: str
    parent_id: Optional[uuid.UUID] = None
    item_type: Optional[str] = "stock"
    gl_account_code: Optional[str] = None
    cogs_account_code: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None


class CategoryUpdate(BaseModel):
    category_name: Optional[str] = None
    parent_id: Optional[uuid.UUID] = None
    item_type: Optional[str] = None
    gl_account_code: Optional[str] = None
    cogs_account_code: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None


# ═══════════════════════════════════════════════════════════════════════════
# 3. Brands
# ═══════════════════════════════════════════════════════════════════════════
class BrandOut(BaseSchema):
    id: uuid.UUID
    brand_code: str
    brand_name: str
    brand_name_en: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None


class BrandCreate(BaseModel):
    brand_code: str
    brand_name: str
    brand_name_en: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    notes: Optional[str] = None


class BrandUpdate(BaseModel):
    brand_name: Optional[str] = None
    brand_name_en: Optional[str] = None
    logo_url: Optional[str] = None
    website: Optional[str] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 4. Reason Codes
# ═══════════════════════════════════════════════════════════════════════════
class ReasonCodeOut(BaseSchema):
    id: uuid.UUID
    reason_code: str
    reason_name: str
    reason_name_en: Optional[str] = None
    applies_to_tx_types: Optional[str] = None
    requires_expense_acc: bool = True
    expense_account_code: Optional[str] = None
    affects_cogs: bool = False
    is_increase: bool = False
    is_system: bool = False
    is_active: bool = True
    sort_order: int = 0
    notes: Optional[str] = None


class ReasonCodeCreate(BaseModel):
    reason_code: str
    reason_name: str
    reason_name_en: Optional[str] = None
    applies_to_tx_types: Optional[str] = None
    requires_expense_acc: bool = True
    expense_account_code: Optional[str] = None
    affects_cogs: bool = False
    is_increase: bool = False
    sort_order: int = 0
    notes: Optional[str] = None


class ReasonCodeUpdate(BaseModel):
    reason_name: Optional[str] = None
    reason_name_en: Optional[str] = None
    applies_to_tx_types: Optional[str] = None
    expense_account_code: Optional[str] = None
    affects_cogs: Optional[bool] = None
    is_increase: Optional[bool] = None
    is_active: Optional[bool] = None
    sort_order: Optional[int] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 5. Item Attributes
# ═══════════════════════════════════════════════════════════════════════════
class AttributeValueOut(BaseSchema):
    id: uuid.UUID
    attribute_id: uuid.UUID
    value_code: str
    value_name: str
    value_name_en: Optional[str] = None
    color_hex: Optional[str] = None
    sort_order: int = 0
    is_active: bool = True


class AttributeValueCreate(BaseModel):
    value_code: str
    value_name: str
    value_name_en: Optional[str] = None
    color_hex: Optional[str] = None
    sort_order: int = 0


class ItemAttributeOut(BaseSchema):
    id: uuid.UUID
    attribute_code: str
    attribute_name: str
    attribute_name_en: Optional[str] = None
    display_type: str = "select"
    is_active: bool = True
    sort_order: int = 0
    values: List[AttributeValueOut] = Field(default_factory=list)


class ItemAttributeCreate(BaseModel):
    attribute_code: str
    attribute_name: str
    attribute_name_en: Optional[str] = None
    display_type: str = "select"
    sort_order: int = 0


class ItemAttributeUpdate(BaseModel):
    attribute_name: Optional[str] = None
    attribute_name_en: Optional[str] = None
    display_type: Optional[str] = None
    is_active: Optional[bool] = None
    sort_order: Optional[int] = None


# ═══════════════════════════════════════════════════════════════════════════
# 6. Warehouses + Zones + Locations
# ═══════════════════════════════════════════════════════════════════════════
class WarehouseOut(BaseSchema):
    id: uuid.UUID
    warehouse_code: str
    warehouse_name: str
    warehouse_type: Optional[str] = None
    warehouse_subtype: Optional[str] = "main"
    branch_code: Optional[str] = None
    parent_warehouse_id: Optional[uuid.UUID] = None
    parent_warehouse_name: Optional[str] = None
    gl_account_code: Optional[str] = None
    transit_account: Optional[str] = None
    address: Optional[str] = None
    manager_user_id: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    allow_negative_stock: bool = False
    is_active: bool = True
    is_default: bool = False
    notes: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None
    zones_count: int = 0
    locations_count: int = 0


class WarehouseCreate(BaseModel):
    warehouse_code: str
    warehouse_name: str
    warehouse_subtype: str = "main"
    branch_code: Optional[str] = None
    parent_warehouse_id: Optional[uuid.UUID] = None
    gl_account_code: Optional[str] = None
    transit_account: Optional[str] = None
    address: Optional[str] = None
    manager_user_id: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    allow_negative_stock: bool = False
    is_default: bool = False
    notes: Optional[str] = None


class WarehouseUpdate(BaseModel):
    warehouse_name: Optional[str] = None
    warehouse_subtype: Optional[str] = None
    branch_code: Optional[str] = None
    parent_warehouse_id: Optional[uuid.UUID] = None
    gl_account_code: Optional[str] = None
    transit_account: Optional[str] = None
    address: Optional[str] = None
    manager_user_id: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    allow_negative_stock: Optional[bool] = None
    is_active: Optional[bool] = None
    is_default: Optional[bool] = None
    notes: Optional[str] = None


class ZoneOut(BaseSchema):
    id: uuid.UUID
    warehouse_id: uuid.UUID
    warehouse_name: Optional[str] = None
    zone_code: str
    zone_name: str
    zone_name_en: Optional[str] = None
    zone_type: str = "storage"
    parent_zone_id: Optional[uuid.UUID] = None
    is_active: bool = True
    notes: Optional[str] = None
    locations_count: int = 0


class ZoneCreate(BaseModel):
    warehouse_id: uuid.UUID
    zone_code: str
    zone_name: str
    zone_name_en: Optional[str] = None
    zone_type: str = "storage"
    parent_zone_id: Optional[uuid.UUID] = None
    notes: Optional[str] = None


class ZoneUpdate(BaseModel):
    zone_name: Optional[str] = None
    zone_name_en: Optional[str] = None
    zone_type: Optional[str] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class LocationOut(BaseSchema):
    id: uuid.UUID
    warehouse_id: uuid.UUID
    warehouse_name: Optional[str] = None
    zone_id: Optional[uuid.UUID] = None
    zone_name: Optional[str] = None
    location_code: str
    location_name: Optional[str] = None
    location_type: str = "storage"
    aisle: Optional[str] = None
    rack: Optional[str] = None
    shelf: Optional[str] = None
    barcode: Optional[str] = None
    max_capacity_qty: Optional[Decimal] = None
    max_capacity_volume: Optional[Decimal] = None
    max_capacity_weight: Optional[Decimal] = None
    is_active: bool = True
    is_pickable: bool = True
    notes: Optional[str] = None


class LocationCreate(BaseModel):
    warehouse_id: uuid.UUID
    zone_id: Optional[uuid.UUID] = None
    location_code: str
    location_name: Optional[str] = None
    location_type: str = "storage"
    aisle: Optional[str] = None
    rack: Optional[str] = None
    shelf: Optional[str] = None
    barcode: Optional[str] = None
    max_capacity_qty: Optional[Decimal] = None
    max_capacity_volume: Optional[Decimal] = None
    max_capacity_weight: Optional[Decimal] = None
    is_pickable: bool = True
    notes: Optional[str] = None


class LocationUpdate(BaseModel):
    zone_id: Optional[uuid.UUID] = None
    location_name: Optional[str] = None
    location_type: Optional[str] = None
    aisle: Optional[str] = None
    rack: Optional[str] = None
    shelf: Optional[str] = None
    barcode: Optional[str] = None
    max_capacity_qty: Optional[Decimal] = None
    max_capacity_volume: Optional[Decimal] = None
    max_capacity_weight: Optional[Decimal] = None
    is_active: Optional[bool] = None
    is_pickable: Optional[bool] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 7. Items + Variants
# ═══════════════════════════════════════════════════════════════════════════
class ItemVariantAttrInput(BaseModel):
    attribute_id: uuid.UUID
    attribute_value_id: uuid.UUID


class ItemVariantAttrOut(BaseSchema):
    attribute_id: uuid.UUID
    attribute_code: Optional[str] = None
    attribute_name: Optional[str] = None
    attribute_value_id: uuid.UUID
    value_code: Optional[str] = None
    value_name: Optional[str] = None
    color_hex: Optional[str] = None


class ItemOut(BaseSchema):
    id: uuid.UUID
    item_code: str
    item_name: str
    item_name_en: Optional[str] = None
    item_type: str = "stock"
    barcode: Optional[str] = None

    category_id: Optional[uuid.UUID] = None
    category_name: Optional[str] = None
    brand_id: Optional[uuid.UUID] = None
    brand_name: Optional[str] = None

    uom_id: Optional[uuid.UUID] = None
    uom_code: Optional[str] = None
    uom_name: Optional[str] = None
    purchase_uom_id: Optional[uuid.UUID] = None
    sales_uom_id: Optional[uuid.UUID] = None

    parent_item_id: Optional[uuid.UUID] = None
    parent_item_code: Optional[str] = None
    is_variant: bool = False
    has_variants: bool = False

    tracking_type: Optional[str] = "none"
    is_serialized: bool = False
    is_lot_tracked: bool = False
    is_expiry_tracked: bool = False
    shelf_life_days: Optional[int] = None

    cost_method: str = "avg"
    valuation_method: str = "avg"
    avg_cost: Decimal = Decimal("0")
    purchase_price: Decimal = Decimal("0")
    sale_price: Decimal = Decimal("0")

    min_qty: Decimal = Decimal("0")
    max_qty: Decimal = Decimal("0")
    reorder_point: Decimal = Decimal("0")
    reorder_qty: Decimal = Decimal("0")
    allow_negative: bool = False

    gl_account_code: Optional[str] = None
    cogs_account_code: Optional[str] = None
    tax_type_code: Optional[str] = None
    is_tax_exempt: bool = False

    unspsc_code: Optional[str] = None
    classification_code: Optional[str] = None
    hs_code: Optional[str] = None

    weight_kg: Optional[Decimal] = None
    volume_m3: Optional[Decimal] = None
    length_cm: Optional[Decimal] = None
    width_cm: Optional[Decimal] = None
    height_cm: Optional[Decimal] = None

    extra_data: Optional[Dict[str, Any]] = None
    lifecycle_status: str = "active"
    is_active: bool = True

    description: Optional[str] = None
    image_url: Optional[str] = None

    variant_attrs: List[ItemVariantAttrOut] = Field(default_factory=list)


class ItemCreate(BaseModel):
    item_code: str
    item_name: str
    item_name_en: Optional[str] = None
    item_type: str = "stock"
    barcode: Optional[str] = None

    category_id: Optional[uuid.UUID] = None
    brand_id: Optional[uuid.UUID] = None
    uom_id: Optional[uuid.UUID] = None
    purchase_uom_id: Optional[uuid.UUID] = None
    sales_uom_id: Optional[uuid.UUID] = None

    parent_item_id: Optional[uuid.UUID] = None
    variant_attrs: List[ItemVariantAttrInput] = Field(default_factory=list)

    is_serialized: bool = False
    is_lot_tracked: bool = False
    is_expiry_tracked: bool = False
    shelf_life_days: Optional[int] = None

    valuation_method: str = "avg"
    purchase_price: Decimal = Decimal("0")
    sale_price: Decimal = Decimal("0")

    min_qty: Decimal = Decimal("0")
    max_qty: Decimal = Decimal("0")
    reorder_point: Decimal = Decimal("0")
    reorder_qty: Decimal = Decimal("0")
    allow_negative: bool = False

    gl_account_code: Optional[str] = None
    cogs_account_code: Optional[str] = None
    tax_type_code: Optional[str] = None
    is_tax_exempt: bool = False

    unspsc_code: Optional[str] = None
    classification_code: Optional[str] = None
    hs_code: Optional[str] = None

    weight_kg: Optional[Decimal] = None
    volume_m3: Optional[Decimal] = None
    length_cm: Optional[Decimal] = None
    width_cm: Optional[Decimal] = None
    height_cm: Optional[Decimal] = None

    extra_data: Optional[Dict[str, Any]] = None
    lifecycle_status: str = "active"

    description: Optional[str] = None
    image_url: Optional[str] = None


class ItemUpdate(BaseModel):
    item_name: Optional[str] = None
    item_name_en: Optional[str] = None
    barcode: Optional[str] = None
    category_id: Optional[uuid.UUID] = None
    brand_id: Optional[uuid.UUID] = None
    uom_id: Optional[uuid.UUID] = None
    purchase_uom_id: Optional[uuid.UUID] = None
    sales_uom_id: Optional[uuid.UUID] = None
    is_serialized: Optional[bool] = None
    is_lot_tracked: Optional[bool] = None
    is_expiry_tracked: Optional[bool] = None
    shelf_life_days: Optional[int] = None
    valuation_method: Optional[str] = None
    purchase_price: Optional[Decimal] = None
    sale_price: Optional[Decimal] = None
    min_qty: Optional[Decimal] = None
    max_qty: Optional[Decimal] = None
    reorder_point: Optional[Decimal] = None
    reorder_qty: Optional[Decimal] = None
    allow_negative: Optional[bool] = None
    gl_account_code: Optional[str] = None
    cogs_account_code: Optional[str] = None
    tax_type_code: Optional[str] = None
    is_tax_exempt: Optional[bool] = None
    unspsc_code: Optional[str] = None
    classification_code: Optional[str] = None
    hs_code: Optional[str] = None
    weight_kg: Optional[Decimal] = None
    volume_m3: Optional[Decimal] = None
    length_cm: Optional[Decimal] = None
    width_cm: Optional[Decimal] = None
    height_cm: Optional[Decimal] = None
    extra_data: Optional[Dict[str, Any]] = None
    lifecycle_status: Optional[str] = None
    is_active: Optional[bool] = None
    description: Optional[str] = None
    image_url: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 8. Lots + Serials
# ═══════════════════════════════════════════════════════════════════════════
class LotOut(BaseSchema):
    id: uuid.UUID
    lot_number: str
    item_id: uuid.UUID
    item_code: Optional[str] = None
    item_name: Optional[str] = None
    manufacturing_date: Optional[date] = None
    expiry_date: Optional[date] = None
    received_date: date
    qty_received: Decimal
    qty_remaining: Decimal
    unit_cost: Decimal = Decimal("0")
    source_party_id: Optional[uuid.UUID] = None
    source_party_name: Optional[str] = None
    source_doc_id: Optional[uuid.UUID] = None
    source_doc_serial: Optional[str] = None
    quality_status: str = "approved"
    coa_number: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None


class SerialOut(BaseSchema):
    id: uuid.UUID
    serial_number: str
    item_id: uuid.UUID
    item_code: Optional[str] = None
    item_name: Optional[str] = None
    lot_id: Optional[uuid.UUID] = None
    status: str = "in_stock"
    current_warehouse_id: Optional[uuid.UUID] = None
    current_warehouse_name: Optional[str] = None
    current_location_id: Optional[uuid.UUID] = None
    purchase_date: Optional[date] = None
    purchase_doc_serial: Optional[str] = None
    purchase_party_id: Optional[uuid.UUID] = None
    purchase_cost: Optional[Decimal] = None
    sale_date: Optional[date] = None
    sale_doc_serial: Optional[str] = None
    sale_party_id: Optional[uuid.UUID] = None
    warranty_start_date: Optional[date] = None
    warranty_end_date: Optional[date] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 9. Transactions (Multi-Layer Stakeholder + Reason + Dimensions)
# ═══════════════════════════════════════════════════════════════════════════
class TransactionLineInput(BaseModel):
    """سطر حركة - يدعم Layer 2 (party + variants + lots + serials)"""
    item_id: uuid.UUID
    qty: Decimal
    unit_cost: Optional[Decimal] = Decimal("0")
    unit_price: Optional[Decimal] = Decimal("0")
    discount_pct: Optional[Decimal] = Decimal("0")
    tax_amount: Optional[Decimal] = Decimal("0")

    # Layer 2: Line-level party (override للـ consignment, multi-vendor receipts)
    party_id: Optional[uuid.UUID] = None
    party_role: Optional[str] = None

    # Variants
    variant_attrs: Optional[Dict[str, Any]] = None

    # Lots & Serials
    lot_id: Optional[uuid.UUID] = None
    lot_number: Optional[str] = None
    expiry_date: Optional[date] = None
    manufacturing_date: Optional[date] = None
    serial_id: Optional[uuid.UUID] = None
    serial_numbers: Optional[List[str]] = None  # لإنشاء serials جديدة في GRN

    # UOM & Locations
    uom_id: Optional[uuid.UUID] = None
    uom_conversion_factor: Optional[Decimal] = Decimal("1")
    from_location_id: Optional[uuid.UUID] = None
    to_location_id: Optional[uuid.UUID] = None

    # Notes
    notes: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None


class TransactionLineOut(BaseSchema):
    id: uuid.UUID
    line_no: int
    item_id: uuid.UUID
    item_code: Optional[str] = None
    item_name: Optional[str] = None
    qty: Decimal
    unit_cost: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")
    unit_price: Decimal = Decimal("0")
    total_price: Decimal = Decimal("0")
    discount_pct: Decimal = Decimal("0")
    tax_amount: Decimal = Decimal("0")

    party_id: Optional[uuid.UUID] = None
    party_role: Optional[str] = None
    party_name: Optional[str] = None

    variant_attrs: Optional[Dict[str, Any]] = None
    lot_id: Optional[uuid.UUID] = None
    lot_number: Optional[str] = None
    serial_id: Optional[uuid.UUID] = None
    serial_number: Optional[str] = None

    uom_id: Optional[uuid.UUID] = None
    uom_code: Optional[str] = None
    qty_in_base_uom: Optional[Decimal] = None
    uom_conversion_factor: Decimal = Decimal("1")
    from_location_id: Optional[uuid.UUID] = None
    from_location_code: Optional[str] = None
    to_location_id: Optional[uuid.UUID] = None
    to_location_code: Optional[str] = None

    notes: Optional[str] = None


class TransactionCreate(BaseModel):
    """
    إنشاء حركة مخزون (DRAFT أو POST مباشرة)
    
    Layer 1: Header-level party + dimensions + reason
    Layer 2: lines[].party_id (line-level override)
    """
    tx_type: str = Field(..., description="GRN|GIN|GIT|IJ|SCRAP|RETURN_IN|RETURN_OUT")
    tx_date: date
    warehouse_id: uuid.UUID
    warehouse_to_id: Optional[uuid.UUID] = None  # للـ GIT (Internal Transfer)

    # Layer 1: Header party + dimensions
    party_id: Optional[uuid.UUID] = None
    party_role: Optional[str] = None
    branch_code: Optional[str] = None
    cost_center_code: Optional[str] = None
    project_code: Optional[str] = None

    # Reason (للـ IJ, SCRAP, و non-PO transactions)
    reason_id: Optional[uuid.UUID] = None
    reason_code: Optional[str] = None

    # Source documents
    source_module: Optional[str] = None
    source_doc_type: Optional[str] = None
    source_doc_id: Optional[uuid.UUID] = None
    source_doc_serial: Optional[str] = None

    # Approval flow
    responsible_user_id: Optional[str] = None
    approved_by_user_id: Optional[str] = None

    # Stakeholders (extra parties beyond primary)
    stakeholders: Optional[Dict[str, Any]] = None

    # Reference info
    reference: Optional[str] = None
    notes: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None

    # Lines
    lines: List[TransactionLineInput]

    # Posting flag
    auto_post: bool = False  # لو true → ينشئ ويرحل في خطوة واحدة


class TransactionUpdate(BaseModel):
    tx_date: Optional[date] = None
    party_id: Optional[uuid.UUID] = None
    party_role: Optional[str] = None
    branch_code: Optional[str] = None
    cost_center_code: Optional[str] = None
    project_code: Optional[str] = None
    reason_id: Optional[uuid.UUID] = None
    reason_code: Optional[str] = None
    responsible_user_id: Optional[str] = None
    reference: Optional[str] = None
    notes: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None


class TransactionOut(BaseSchema):
    id: uuid.UUID
    tx_serial: str
    tx_type: str
    tx_date: date
    status: str = "draft"

    warehouse_id: uuid.UUID
    warehouse_code: Optional[str] = None
    warehouse_name: Optional[str] = None
    warehouse_to_id: Optional[uuid.UUID] = None
    warehouse_to_name: Optional[str] = None

    # Layer 1
    party_id: Optional[uuid.UUID] = None
    party_role: Optional[str] = None
    party_name_snapshot: Optional[str] = None
    branch_code: Optional[str] = None
    cost_center_code: Optional[str] = None
    project_code: Optional[str] = None
    reason_id: Optional[uuid.UUID] = None
    reason_code: Optional[str] = None
    reason_name: Optional[str] = None

    # Source
    source_module: Optional[str] = None
    source_doc_type: Optional[str] = None
    source_doc_id: Optional[uuid.UUID] = None
    source_doc_serial: Optional[str] = None

    # Approval & users
    responsible_user_id: Optional[str] = None
    approved_by_user_id: Optional[str] = None
    approved_at: Optional[datetime] = None

    # JE link
    je_id: Optional[uuid.UUID] = None
    je_serial: Optional[str] = None

    # Reversal
    reversed_by_id: Optional[uuid.UUID] = None
    reverses_id: Optional[uuid.UUID] = None
    reverses_serial: Optional[str] = None

    # Totals
    total_qty: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")
    total_price: Decimal = Decimal("0")
    lines_count: int = 0

    # Misc
    stakeholders: Optional[Dict[str, Any]] = None
    reference: Optional[str] = None
    notes: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None

    created_at: Optional[datetime] = None
    posted_at: Optional[datetime] = None

    # Lines (if requested)
    lines: List[TransactionLineOut] = Field(default_factory=list)


class TransactionPost(BaseModel):
    """ترحيل حركة مسوّدة"""
    posted_by: Optional[str] = None
    notes: Optional[str] = None


class TransactionReverse(BaseModel):
    """عكس حركة مرحّلة"""
    reverse_date: Optional[date] = None
    reason_code: Optional[str] = None
    reason_id: Optional[uuid.UUID] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 10. Count Sessions + Adjustments
# ═══════════════════════════════════════════════════════════════════════════
class CountSessionCreate(BaseModel):
    session_date: date
    warehouse_id: uuid.UUID
    count_type: str = "full"  # full|cycle|spot|category
    category_id: Optional[uuid.UUID] = None
    zone_id: Optional[uuid.UUID] = None
    location_id: Optional[uuid.UUID] = None
    branch_code: Optional[str] = None
    cost_center_code: Optional[str] = None
    project_code: Optional[str] = None
    notes: Optional[str] = None


class CountSessionOut(BaseSchema):
    id: uuid.UUID
    session_serial: str
    session_date: date
    warehouse_id: uuid.UUID
    warehouse_name: Optional[str] = None
    count_type: str = "full"
    status: str = "draft"
    category_id: Optional[uuid.UUID] = None
    category_name: Optional[str] = None
    zone_id: Optional[uuid.UUID] = None
    zone_name: Optional[str] = None
    location_id: Optional[uuid.UUID] = None
    location_code: Optional[str] = None
    branch_code: Optional[str] = None
    cost_center_code: Optional[str] = None
    project_code: Optional[str] = None
    items_counted: int = 0
    items_with_variance: int = 0
    total_variance_value: Decimal = Decimal("0")
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    posted_at: Optional[datetime] = None


class CountLineInput(BaseModel):
    item_id: uuid.UUID
    counted_qty: Decimal
    location_id: Optional[uuid.UUID] = None
    lot_id: Optional[uuid.UUID] = None
    notes: Optional[str] = None


class CountLineOut(BaseSchema):
    id: uuid.UUID
    item_id: uuid.UUID
    item_code: Optional[str] = None
    item_name: Optional[str] = None
    expected_qty: Decimal = Decimal("0")
    counted_qty: Decimal = Decimal("0")
    variance_qty: Decimal = Decimal("0")
    avg_cost: Decimal = Decimal("0")
    variance_value: Decimal = Decimal("0")
    location_id: Optional[uuid.UUID] = None
    lot_id: Optional[uuid.UUID] = None
    notes: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# 11. Account Settings
# ═══════════════════════════════════════════════════════════════════════════
class AccountSettingOut(BaseSchema):
    id: uuid.UUID
    tx_type: str
    debit_account: Optional[str] = None
    credit_account: Optional[str] = None
    description: Optional[str] = None


class AccountSettingUpdate(BaseModel):
    debit_account: Optional[str] = None
    credit_account: Optional[str] = None
    description: Optional[str] = None
