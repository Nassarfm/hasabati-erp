"""
app/modules/inventory/models.py
═══════════════════════════════════════════════════════════════════════════
Inventory Module - SQLAlchemy ORM Models
═══════════════════════════════════════════════════════════════════════════
يطابق DB الفعلي بالكامل (v5.0)

التصميم:
  - 3-Level Warehouse Hierarchy: Warehouse -> Zone -> Location
  - Multi-Variant Items: Parent-Child via parent_item_id + Attributes
  - Lots/Batches + Serial Numbers
  - Multi-Layer Stakeholder: party at header + line + ledger
  - Universal Item Subsidiary Ledger
  - Reason-Driven Accounting

ملاحظة: الكود الأساسي يستخدم raw SQL في router.py للأداء
هذه الـ models موجودة لأغراض type safety + بعض queries المعقدة
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime, ForeignKey,
    Index, Integer, Numeric, String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


# ═══════════════════════════════════════════════════════════════════════════
# 1. MASTER DATA - inv_uom (already exists in DB)
# ═══════════════════════════════════════════════════════════════════════════
class UOM(Base):
    """وحدات القياس"""
    __tablename__ = "inv_uom"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    uom_code: Mapped[str] = mapped_column(String(20), nullable=False)
    uom_name: Mapped[str] = mapped_column(String(100), nullable=False)
    uom_name_en: Mapped[Optional[str]] = mapped_column(String(100))
    is_base: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tenant_id", "uom_code", name="uq_uom_tenant_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. inv_uom_conversions - NEW
# ═══════════════════════════════════════════════════════════════════════════
class UOMConversion(Base):
    """تحويلات وحدات القياس"""
    __tablename__ = "inv_uom_conversions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    from_uom_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_uom.id"), nullable=False)
    to_uom_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_uom.id"), nullable=False)
    factor: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    item_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_items.id", ondelete="CASCADE"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[Optional[str]] = mapped_column(String(255))

    __table_args__ = (
        UniqueConstraint("tenant_id", "from_uom_id", "to_uom_id", "item_id", name="uq_uom_conversion"),
        CheckConstraint("from_uom_id <> to_uom_id", name="ck_different_uom"),
        CheckConstraint("factor > 0", name="ck_factor_positive"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3. inv_categories
# ═══════════════════════════════════════════════════════════════════════════
class Category(Base):
    """تصنيفات الأصناف"""
    __tablename__ = "inv_categories"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    category_code: Mapped[str] = mapped_column(String(50), nullable=False)
    category_name: Mapped[str] = mapped_column(String(255), nullable=False)
    parent_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_categories.id"))
    item_type: Mapped[Optional[str]] = mapped_column(String(20), default="stock")
    gl_account_code: Mapped[Optional[str]] = mapped_column(String(20))
    cogs_account_code: Mapped[Optional[str]] = mapped_column(String(20))
    extra_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tenant_id", "category_code", name="uq_category_tenant_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 4. inv_brands - NEW
# ═══════════════════════════════════════════════════════════════════════════
class Brand(Base):
    """العلامات التجارية"""
    __tablename__ = "inv_brands"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    brand_code: Mapped[str] = mapped_column(String(50), nullable=False)
    brand_name: Mapped[str] = mapped_column(String(255), nullable=False)
    brand_name_en: Mapped[Optional[str]] = mapped_column(String(255))
    logo_url: Mapped[Optional[str]] = mapped_column(Text)
    website: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[Optional[str]] = mapped_column(String(255))

    __table_args__ = (
        UniqueConstraint("tenant_id", "brand_code", name="uq_brand_tenant_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 5. inv_warehouses
# ═══════════════════════════════════════════════════════════════════════════
class Warehouse(Base):
    """المستودعات"""
    __tablename__ = "inv_warehouses"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    warehouse_code: Mapped[str] = mapped_column(String(20), nullable=False)
    warehouse_name: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_type: Mapped[Optional[str]] = mapped_column(String(30))
    warehouse_subtype: Mapped[Optional[str]] = mapped_column(String(30), default="main")
    branch_code: Mapped[Optional[str]] = mapped_column(String(20))
    parent_warehouse_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_warehouses.id"))
    gl_account_code: Mapped[Optional[str]] = mapped_column(String(20))
    transit_account: Mapped[Optional[str]] = mapped_column(String(20))
    address: Mapped[Optional[str]] = mapped_column(Text)
    manager_user_id: Mapped[Optional[str]] = mapped_column(String(255))
    phone: Mapped[Optional[str]] = mapped_column(String(50))
    email: Mapped[Optional[str]] = mapped_column(String(255))
    allow_negative_stock: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    extra_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[Optional[str]] = mapped_column(String(255))

    __table_args__ = (
        UniqueConstraint("tenant_id", "warehouse_code", name="uq_warehouse_tenant_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6. inv_zones - NEW (3-Level Hierarchy)
# ═══════════════════════════════════════════════════════════════════════════
class Zone(Base):
    """مناطق المستودعات - المستوى الوسيط"""
    __tablename__ = "inv_zones"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    warehouse_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_warehouses.id", ondelete="CASCADE"), nullable=False)
    zone_code: Mapped[str] = mapped_column(String(20), nullable=False)
    zone_name: Mapped[str] = mapped_column(String(255), nullable=False)
    zone_name_en: Mapped[Optional[str]] = mapped_column(String(255))
    zone_type: Mapped[str] = mapped_column(String(30), nullable=False, default="storage")
    parent_zone_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_zones.id"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("warehouse_id", "zone_code", name="uq_zone_warehouse_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 7. inv_locations (renamed from inv_bins)
# ═══════════════════════════════════════════════════════════════════════════
class Location(Base):
    """المواقع داخل المستودع"""
    __tablename__ = "inv_locations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    warehouse_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_warehouses.id"), nullable=False)
    zone_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_zones.id"))
    location_code: Mapped[str] = mapped_column(String(50), nullable=False)
    location_name: Mapped[Optional[str]] = mapped_column(String(255))
    location_type: Mapped[str] = mapped_column(String(30), default="storage")
    aisle: Mapped[Optional[str]] = mapped_column(String(20))
    rack: Mapped[Optional[str]] = mapped_column(String(20))
    shelf: Mapped[Optional[str]] = mapped_column(String(20))
    barcode: Mapped[Optional[str]] = mapped_column(String(100))
    max_capacity_qty: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3))
    max_capacity_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3))
    max_capacity_weight: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 3))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_pickable: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text)


# ═══════════════════════════════════════════════════════════════════════════
# 8. inv_items
# ═══════════════════════════════════════════════════════════════════════════
class Item(Base):
    """الأصناف - Master للمنتجات"""
    __tablename__ = "inv_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)

    # Identity
    item_code: Mapped[str] = mapped_column(String(50), nullable=False)
    item_name: Mapped[str] = mapped_column(String(255), nullable=False)
    item_name_en: Mapped[Optional[str]] = mapped_column(String(255))
    item_type: Mapped[str] = mapped_column(String(20), default="stock")
    barcode: Mapped[Optional[str]] = mapped_column(String(100))

    # Classification
    category_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_categories.id"))
    brand_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_brands.id"))
    uom_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_uom.id"))
    purchase_uom_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_uom.id"))
    sales_uom_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_uom.id"))

    # Variants
    parent_item_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_items.id", ondelete="CASCADE"))
    is_variant: Mapped[bool] = mapped_column(Boolean, default=False)
    has_variants: Mapped[bool] = mapped_column(Boolean, default=False)

    # Tracking
    tracking_type: Mapped[Optional[str]] = mapped_column(String(20), default="none")
    is_serialized: Mapped[bool] = mapped_column(Boolean, default=False)
    is_lot_tracked: Mapped[bool] = mapped_column(Boolean, default=False)
    is_expiry_tracked: Mapped[bool] = mapped_column(Boolean, default=False)
    shelf_life_days: Mapped[Optional[int]] = mapped_column(Integer)

    # Costing
    cost_method: Mapped[str] = mapped_column(String(10), default="avg")
    valuation_method: Mapped[str] = mapped_column(String(10), default="avg")
    avg_cost: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)
    purchase_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)
    sale_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)

    # Stock control
    min_qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    max_qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    reorder_point: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    reorder_qty: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    allow_negative: Mapped[bool] = mapped_column(Boolean, default=False)

    # Accounting
    gl_account_code: Mapped[Optional[str]] = mapped_column(String(20))
    cogs_account_code: Mapped[Optional[str]] = mapped_column(String(20))

    # Tax
    tax_type_code: Mapped[Optional[str]] = mapped_column(String(20))
    is_tax_exempt: Mapped[bool] = mapped_column(Boolean, default=False)

    # ZATCA Phase 2
    unspsc_code: Mapped[Optional[str]] = mapped_column(String(20))
    classification_code: Mapped[Optional[str]] = mapped_column(String(20))
    hs_code: Mapped[Optional[str]] = mapped_column(String(20))

    # Physical dimensions
    weight_kg: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3))
    volume_m3: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    length_cm: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    width_cm: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    height_cm: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))

    # Smart Hybrid + Lifecycle
    extra_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    lifecycle_status: Mapped[str] = mapped_column(String(20), default="active")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Misc
    description: Mapped[Optional[str]] = mapped_column(Text)
    image_url: Mapped[Optional[str]] = mapped_column(Text)

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[Optional[str]] = mapped_column(String(255))

    __table_args__ = (
        UniqueConstraint("tenant_id", "item_code", name="uq_item_tenant_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 9. inv_item_attributes - NEW (Variants Pool)
# ═══════════════════════════════════════════════════════════════════════════
class ItemAttribute(Base):
    """خصائص الأصناف"""
    __tablename__ = "inv_item_attributes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    attribute_code: Mapped[str] = mapped_column(String(50), nullable=False)
    attribute_name: Mapped[str] = mapped_column(String(255), nullable=False)
    attribute_name_en: Mapped[Optional[str]] = mapped_column(String(255))
    display_type: Mapped[str] = mapped_column(String(20), default="select")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tenant_id", "attribute_code", name="uq_attr_tenant_code"),
    )


class ItemAttributeValue(Base):
    """قيم الخصائص"""
    __tablename__ = "inv_item_attribute_values"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    attribute_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_item_attributes.id", ondelete="CASCADE"), nullable=False)
    value_code: Mapped[str] = mapped_column(String(50), nullable=False)
    value_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value_name_en: Mapped[Optional[str]] = mapped_column(String(255))
    color_hex: Mapped[Optional[str]] = mapped_column(String(7))
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint("attribute_id", "value_code", name="uq_attr_value"),
    )


class ItemVariantAttr(Base):
    """ربط Variants بقيم الخصائص"""
    __tablename__ = "inv_item_variant_attrs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_items.id", ondelete="CASCADE"), nullable=False)
    attribute_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_item_attributes.id"), nullable=False)
    attribute_value_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_item_attribute_values.id"), nullable=False)

    __table_args__ = (
        UniqueConstraint("item_id", "attribute_id", name="uq_item_attr"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 10. inv_lots - NEW
# ═══════════════════════════════════════════════════════════════════════════
class Lot(Base):
    """دفعات/تشغيلات الأصناف"""
    __tablename__ = "inv_lots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    lot_number: Mapped[str] = mapped_column(String(100), nullable=False)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_items.id"), nullable=False)

    manufacturing_date: Mapped[Optional[date]] = mapped_column(Date)
    expiry_date: Mapped[Optional[date]] = mapped_column(Date)
    received_date: Mapped[date] = mapped_column(Date, nullable=False, default=date.today)

    qty_received: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False, default=0)
    qty_remaining: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False, default=0)
    unit_cost: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)

    source_party_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    source_party_name: Mapped[Optional[str]] = mapped_column(String(255))
    source_doc_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    source_doc_serial: Mapped[Optional[str]] = mapped_column(String(50))

    quality_status: Mapped[str] = mapped_column(String(20), default="approved")
    coa_number: Mapped[Optional[str]] = mapped_column(String(100))

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[Optional[str]] = mapped_column(String(255))

    __table_args__ = (
        UniqueConstraint("tenant_id", "item_id", "lot_number", name="uq_lot_tenant_item"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 11. inv_serials - NEW
# ═══════════════════════════════════════════════════════════════════════════
class Serial(Base):
    """الأرقام التسلسلية"""
    __tablename__ = "inv_serials"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    serial_number: Mapped[str] = mapped_column(String(100), nullable=False)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_items.id"), nullable=False)
    lot_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_lots.id"))

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="in_stock")
    current_warehouse_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_warehouses.id"))
    current_location_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_locations.id"))

    purchase_date: Mapped[Optional[date]] = mapped_column(Date)
    purchase_doc_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    purchase_doc_serial: Mapped[Optional[str]] = mapped_column(String(50))
    purchase_party_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    purchase_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4))

    sale_date: Mapped[Optional[date]] = mapped_column(Date)
    sale_doc_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    sale_doc_serial: Mapped[Optional[str]] = mapped_column(String(50))
    sale_party_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))

    warranty_start_date: Mapped[Optional[date]] = mapped_column(Date)
    warranty_end_date: Mapped[Optional[date]] = mapped_column(Date)

    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tenant_id", "item_id", "serial_number", name="uq_serial_tenant_item"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 12. inv_reason_codes - NEW (Reason-Driven Accounting)
# ═══════════════════════════════════════════════════════════════════════════
class ReasonCode(Base):
    """أكواد الأسباب للحركات الاستثنائية"""
    __tablename__ = "inv_reason_codes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    reason_code: Mapped[str] = mapped_column(String(50), nullable=False)
    reason_name: Mapped[str] = mapped_column(String(255), nullable=False)
    reason_name_en: Mapped[Optional[str]] = mapped_column(String(255))
    applies_to_tx_types: Mapped[Optional[str]] = mapped_column(Text)
    requires_expense_acc: Mapped[bool] = mapped_column(Boolean, default=True)
    expense_account_code: Mapped[Optional[str]] = mapped_column(String(20))
    affects_cogs: Mapped[bool] = mapped_column(Boolean, default=False)
    is_increase: Mapped[bool] = mapped_column(Boolean, default=False)
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tenant_id", "reason_code", name="uq_reason_tenant_code"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 13. inv_balances - الأرصدة اللحظية
# ═══════════════════════════════════════════════════════════════════════════
class Balance(Base):
    """رصيد المخزون اللحظي per item per warehouse"""
    __tablename__ = "inv_balances"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_items.id"), nullable=False)
    warehouse_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("inv_warehouses.id"), nullable=False)
    qty_on_hand: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    qty_reserved: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    avg_cost: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)
    total_value: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    last_movement: Mapped[Optional[date]] = mapped_column(Date)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("tenant_id", "item_id", "warehouse_id", name="uq_balance_tenant_item_wh"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# 14. inv_account_settings
# ═══════════════════════════════════════════════════════════════════════════
class AccountSettings(Base):
    """إعدادات الحسابات لكل نوع حركة"""
    __tablename__ = "inv_account_settings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    tx_type: Mapped[str] = mapped_column(String(20), nullable=False)
    debit_account: Mapped[Optional[str]] = mapped_column(String(20))
    credit_account: Mapped[Optional[str]] = mapped_column(String(20))
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tenant_id", "tx_type", name="uq_acc_settings_tenant_type"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# Note: inv_transactions, inv_transaction_lines, inv_ledger, inv_fifo_layers,
# inv_count_sessions, inv_count_lines, inv_adjustments, inv_adjustment_lines
# remain accessed via raw SQL in router.py for performance.
# Models can be added here as needed for type-safety queries.
# ═══════════════════════════════════════════════════════════════════════════
