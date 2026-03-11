"""
app/modules/inventory/router.py
══════════════════════════════════════════════════════════
Inventory Module API — 20 endpoint

Products:
  POST   /inventory/products              إنشاء منتج
  GET    /inventory/products              قائمة المنتجات
  GET    /inventory/products/search       بحث
  GET    /inventory/products/{id}         تفاصيل منتج
  PUT    /inventory/products/{id}         تعديل منتج

Warehouses:
  POST   /inventory/warehouses            إنشاء مستودع
  GET    /inventory/warehouses            قائمة المستودعات
  GET    /inventory/warehouses/{id}/stock مخزون مستودع

Stock:
  GET    /inventory/stock                 رصيد مخزون (product+warehouse)
  GET    /inventory/stock/low             منتجات تحت نقطة إعادة الطلب
  GET    /inventory/stock/valuation       تقييم المخزون الكلي

Movements:
  POST   /inventory/movements             حركة يدوية
  GET    /inventory/movements/{product}   كشف حركة منتج
  GET    /inventory/movements/{id}        تفاصيل حركة

Adjustments:
  POST   /inventory/adjustments           إنشاء تسوية جرد
  GET    /inventory/adjustments           قائمة التسويات
  GET    /inventory/adjustments/{id}      تفاصيل تسوية
  POST   /inventory/adjustments/{id}/post ترحيل تسوية

Dashboard:
  GET    /inventory/dashboard             KPIs مخزون
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import created, ok, paginated
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.inventory.schemas import (
    ProductCreate, ProductUpdate,
    StockAdjustmentCreate, StockMovementCreate,
    WarehouseCreate,
)
from app.modules.inventory.service import InventoryService

router = APIRouter(prefix="/inventory", tags=["المخزون"])


def _svc(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
) -> InventoryService:
    return InventoryService(db, user)


# ══════════════════════════════════════════════════════════
# Products
# ══════════════════════════════════════════════════════════
@router.post("/products", status_code=201, summary="إنشاء منتج جديد")
async def create_product(
    data: ProductCreate,
    svc: InventoryService = Depends(_svc),
):
    product = await svc.create_product(data)
    return created(
        data=product.to_dict(),
        message=f"تم إنشاء المنتج {data.code} — {data.name_ar}",
    )


@router.get("/products", summary="قائمة المنتجات")
async def list_products(
    category:  Optional[str] = Query(None),
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    svc: InventoryService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc.list_products(
        category=category, offset=offset, limit=page_size
    )
    return paginated(
        items=[p.to_dict() for p in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/products/search", summary="بحث عن منتج")
async def search_products(
    q: str = Query(..., min_length=1),
    svc: InventoryService = Depends(_svc),
):
    """بحث بالكود أو الاسم أو الباركود."""
    results = await svc.search_products(q)
    return ok(data=[p.to_dict() for p in results])


@router.get("/products/{product_id}", summary="تفاصيل منتج")
async def get_product(
    product_id: uuid.UUID,
    svc: InventoryService = Depends(_svc),
):
    product = await svc.get_product(product_id)
    return ok(data=product.to_dict())


@router.put("/products/{product_id}", summary="تعديل منتج")
async def update_product(
    product_id: uuid.UUID,
    data: ProductUpdate,
    svc: InventoryService = Depends(_svc),
):
    product = await svc.update_product(product_id, data)
    return ok(data=product.to_dict(), message="تم التعديل")


# ══════════════════════════════════════════════════════════
# Warehouses
# ══════════════════════════════════════════════════════════
@router.post("/warehouses", status_code=201, summary="إنشاء مستودع")
async def create_warehouse(
    data: WarehouseCreate,
    svc: InventoryService = Depends(_svc),
):
    wh = await svc.create_warehouse(data)
    return created(data=wh.to_dict(), message=f"تم إنشاء المستودع {data.code}")


@router.get("/warehouses", summary="قائمة المستودعات")
async def list_warehouses(svc: InventoryService = Depends(_svc)):
    warehouses = await svc.list_warehouses()
    return ok(data=[w.to_dict() for w in warehouses])


@router.get("/warehouses/{warehouse_id}/stock", summary="مخزون مستودع")
async def warehouse_stock(
    warehouse_id: uuid.UUID,
    svc: InventoryService = Depends(_svc),
):
    balances = await svc.list_stock_by_warehouse(warehouse_id)
    return ok(
        data=[b.to_dict() for b in balances],
        message=f"{len(balances)} صنف في المستودع",
    )


# ══════════════════════════════════════════════════════════
# Stock Balances
# ══════════════════════════════════════════════════════════
@router.get("/stock", summary="رصيد مخزون منتج")
async def get_stock_balance(
    product_code:  str = Query(...),
    warehouse_code: str = Query(...),
    svc: InventoryService = Depends(_svc),
):
    """
    رصيد مخزون لحظي — الكمية المتاحة والتكلفة المتوسطة.
    """
    bal = await svc.get_stock_balance(product_code, warehouse_code)
    if bal is None:
        return ok(data={
            "product_code": product_code,
            "warehouse_code": warehouse_code,
            "qty_on_hand": 0,
            "qty_available": 0,
            "average_cost": 0,
            "total_value": 0,
        })
    return ok(data=bal.to_dict())


@router.get("/stock/low", summary="منتجات تحت نقطة إعادة الطلب")
async def low_stock_alert(svc: InventoryService = Depends(_svc)):
    items = await svc.get_low_stock_items()
    return ok(
        data=[b.to_dict() for b in items],
        message=f"⚠️ {len(items)} صنف تحت نقطة إعادة الطلب",
    )


@router.get("/stock/valuation", summary="تقييم المخزون الكلي")
async def inventory_valuation(svc: InventoryService = Depends(_svc)):
    """إجمالي قيمة المخزون (الكمية × التكلفة المتوسطة)."""
    total = await svc.get_total_inventory_value()
    return ok(data={"total_inventory_value": float(total)})


# ══════════════════════════════════════════════════════════
# Movements
# ══════════════════════════════════════════════════════════
@router.post("/movements", status_code=201, summary="حركة مخزون يدوية")
async def post_movement(
    data: StockMovementCreate,
    svc: InventoryService = Depends(_svc),
):
    """
    حركات يدوية: رصيد افتتاحي، تسوية، نقل بين مستودعات، مرتجع.
    الحركات التلقائية (GRN، فاتورة مبيعات) تنشأ من الموديولات المعنية.
    """
    movement = await svc.post_manual_movement(data)
    return created(
        data=movement.to_dict(),
        message=f"✅ تم ترحيل الحركة {movement.movement_number} | WAC بعد: {float(movement.wac_after):.4f}",
    )


@router.get("/movements/product/{product_id}", summary="كشف حركة منتج")
async def product_movements(
    product_id: uuid.UUID,
    warehouse_id: Optional[uuid.UUID] = Query(None),
    date_from:    Optional[date]      = Query(None),
    date_to:      Optional[date]      = Query(None),
    page:         int = Query(1, ge=1),
    page_size:    int = Query(20, ge=1, le=100),
    svc: InventoryService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc._movement_repo.list_for_product(
        product_id,
        warehouse_id=warehouse_id,
        date_from=date_from,
        date_to=date_to,
        offset=offset,
        limit=page_size,
    )
    return paginated(
        items=[m.to_dict() for m in items],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════
# Stock Adjustments
# ══════════════════════════════════════════════════════════
@router.post("/adjustments", status_code=201, summary="إنشاء تسوية جرد")
async def create_adjustment(
    data: StockAdjustmentCreate,
    svc: InventoryService = Depends(_svc),
):
    """
    إنشاء محضر جرد — يقارن الكمية الفعلية بالكمية النظامية.
    يبقى draft حتى يتم الترحيل.
    """
    adj = await svc.create_adjustment(data)
    return created(
        data=adj.to_dict(),
        message=f"تم إنشاء تسوية الجرد {adj.adj_number} — {len(data.lines)} صنف",
    )


@router.post("/adjustments/{adj_id}/post", summary="ترحيل تسوية جرد")
async def post_adjustment(
    adj_id: uuid.UUID,
    svc: InventoryService = Depends(_svc),
):
    """
    ترحيل محضر الجرد:
    - فروق موجبة → ADJUSTMENT_IN + قيد DR مخزون / CR فروق جرد
    - فروق سالبة → ADJUSTMENT_OUT + قيد DR فروق جرد / CR مخزون
    """
    adj = await svc.post_adjustment(adj_id)
    return ok(
        data=adj.to_dict(),
        message=f"✅ تم ترحيل تسوية الجرد {adj.adj_number}",
    )


# ══════════════════════════════════════════════════════════
# Dashboard
# ══════════════════════════════════════════════════════════
@router.get("/dashboard", summary="لوحة تحكم المخزون")
async def inventory_dashboard(svc: InventoryService = Depends(_svc)):
    """KPIs المخزون الرئيسية."""
    from sqlalchemy import func, select
    from app.modules.inventory.models import Product, StockBalance, StockMovement

    db = svc.db
    tid = svc.user.tenant_id

    # Total products
    r = await db.execute(
        select(func.count(Product.id))
        .where(Product.tenant_id == tid)
        .where(Product.is_active == True)
    )
    total_products = r.scalar_one()

    # Total inventory value
    total_value = await svc.get_total_inventory_value()

    # Low stock count
    low_stock = await svc.get_low_stock_items()

    # Movements today
    from datetime import date as date_type
    today = date_type.today()
    r2 = await db.execute(
        select(func.count(StockMovement.id))
        .where(StockMovement.tenant_id == tid)
        .where(StockMovement.movement_date == today)
        .where(StockMovement.status == "posted")
    )
    movements_today = r2.scalar_one()

    return ok(data={
        "total_products":     total_products,
        "total_inventory_value": float(total_value),
        "low_stock_count":    len(low_stock),
        "movements_today":    movements_today,
    })
