"""
app/modules/inventory/routers/items.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Items Router (with Variants)
═══════════════════════════════════════════════════════════════════════════
Endpoints:
  GET    /inventory/items-v2                          — pagination + search + filters
  GET    /inventory/items-v2/{item_id}                — تفاصيل صنف + variants
  POST   /inventory/items-v2                          — إنشاء صنف
  PUT    /inventory/items-v2/{item_id}                — تحديث
  DELETE /inventory/items-v2/{item_id}                — حذف
  POST   /inventory/items-v2/{parent_id}/generate-variants — توليد variants تلقائياً
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from itertools import product
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory", tags=["inventory-items"])


# ═══════════════════════════════════════════════════════════════════════════
# Helpers — Type-Aware Value Conversion
# ═══════════════════════════════════════════════════════════════════════════
def _clean_numeric(value):
    """يحوّل string فارغ أو whitespace إلى None، ويحفظ القيم الرقمية."""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if v == "":
            return None
        # حاول التحويل لرقم
        try:
            if "." in v:
                return float(v)
            return int(v)
        except (ValueError, TypeError):
            return None
    return value


def _clean_uuid(value):
    """يحوّل string فارغ إلى None للـ FK fields (UUID)."""
    if value is None or value == "":
        return None
    return value


def _clean_text(value):
    """يحوّل string فارغ إلى None للحقول النصية."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _clean_bool(value, default=False):
    """يحوّل أي قيمة إلى boolean صريح."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


# ═══════════════════════════════════════════════════════════════════════════
# LIST ITEMS V2 — مع pagination + search + filtering
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/items-v2")
async def list_items_v2(
    search: Optional[str] = Query(None),
    category_id: Optional[uuid.UUID] = None,
    brand_id: Optional[uuid.UUID] = None,
    is_active: Optional[bool] = None,
    has_variants: Optional[bool] = None,
    is_variant: Optional[bool] = None,
    parent_item_id: Optional[uuid.UUID] = None,
    is_serialized: Optional[bool] = None,
    is_lot_tracked: Optional[bool] = None,
    valuation_method: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    Defensive list of items v2.
    
    Step 1: نقرأ schema الفعلي من information_schema
    Step 2: نبني SELECT ديناميكياً يستخدم فقط الأعمدة الموجودة
    Step 3: نستخدم COALESCE/NULL alias للأعمدة المفقودة
    """
    tid = str(user.tenant_id)
    
    # ─── Step 1: اكتشف الأعمدة الموجودة فعلياً ──────────────
    cols_q = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='inv_items'
    """))
    existing = {row[0] for row in cols_q.fetchall()}
    
    def col_or_null(col_name, alias=None):
        """Returns 'i.col_name' if exists, else 'NULL AS alias'."""
        a = alias or col_name
        if col_name in existing:
            return f"i.{col_name}" + (f" AS {a}" if alias else "")
        return f"NULL AS {a}"
    
    # ─── Step 2: WHERE conditions (آمنة) ─────────────────────
    conds = ["i.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}

    if search and "item_code" in existing:
        conds.append("(i.item_code ILIKE :s OR i.item_name ILIKE :s OR COALESCE(i.barcode,'') ILIKE :s)")
        params["s"] = f"%{search}%"
    if category_id is not None and "category_id" in existing:
        conds.append("i.category_id=:cid")
        params["cid"] = str(category_id)
    if brand_id is not None and "brand_id" in existing:
        conds.append("i.brand_id=:bid")
        params["bid"] = str(brand_id)
    if is_active is not None and "is_active" in existing:
        conds.append("i.is_active=:act")
        params["act"] = is_active
    if has_variants is not None and "has_variants" in existing:
        conds.append("i.has_variants=:hv")
        params["hv"] = has_variants
    if is_variant is not None and "is_variant" in existing:
        conds.append("i.is_variant=:iv")
        params["iv"] = is_variant
    if parent_item_id is not None and "parent_item_id" in existing:
        conds.append("i.parent_item_id=:pid")
        params["pid"] = str(parent_item_id)
    if is_serialized is not None and "is_serialized" in existing:
        conds.append("i.is_serialized=:ser")
        params["ser"] = is_serialized
    if is_lot_tracked is not None and "is_lot_tracked" in existing:
        conds.append("i.is_lot_tracked=:lt")
        params["lt"] = is_lot_tracked

    where = " AND ".join(conds)

    # ─── Step 3: COUNT ──────────────────────────────────────
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM inv_items i WHERE {where}"), params)
    total = cnt.scalar() or 0

    # ─── Step 4: ابنِ SELECT ديناميكياً ─────────────────────
    select_cols = [
        "i.id", "i.tenant_id",
        col_or_null("item_code"),
        col_or_null("item_name"),
        col_or_null("item_name_en"),
        col_or_null("description"),
        col_or_null("barcode"),
        # ⭐ نوع الصنف (item_type)
        col_or_null("item_type"),
        col_or_null("product_type"),
        col_or_null("nature"),
        col_or_null("kind"),
        col_or_null("category_id"),
        col_or_null("brand_id"),
        col_or_null("uom_id"),
        col_or_null("purchase_uom_id"),
        col_or_null("sales_uom_id"),
        col_or_null("purchase_price"),
        col_or_null("sale_price"),
        col_or_null("standard_cost"),
        col_or_null("gl_account_code"),
        col_or_null("cogs_account_code"),
        col_or_null("income_account_code"),
        col_or_null("unspsc_code"),
        col_or_null("classification_code"),
        col_or_null("is_active"),
        col_or_null("is_purchasable"),
        col_or_null("is_sellable"),
        col_or_null("is_serialized"),
        col_or_null("is_lot_tracked"),
        col_or_null("is_expiry_tracked"),
        col_or_null("has_variants"),
        col_or_null("is_variant"),
        col_or_null("parent_item_id"),
        col_or_null("weight_kg"),
        col_or_null("volume_m3"),
        col_or_null("reorder_point"),
        col_or_null("reorder_qty"),
        col_or_null("image_url"),
        col_or_null("notes"),
        col_or_null("extra_data"),
        col_or_null("created_at"),
        col_or_null("updated_at"),
    ]
    
    # valuation_method قد يكون باسمين (cost_method قديم)
    if "valuation_method" in existing:
        select_cols.append("COALESCE(i.valuation_method, 'avg') AS valuation_method")
    elif "cost_method" in existing:
        select_cols.append("COALESCE(i.cost_method, 'avg') AS valuation_method")
    else:
        select_cols.append("'avg'::text AS valuation_method")
    
    # JOINs (defensive — نتأكّد من جداول الربط)
    join_cats = "LEFT JOIN inv_categories c ON c.id = i.category_id" if "category_id" in existing else ""
    join_brands = "LEFT JOIN inv_brands b ON b.id = i.brand_id" if "brand_id" in existing else ""
    join_uom = "LEFT JOIN inv_uom u ON u.id = i.uom_id" if "uom_id" in existing else ""
    
    select_cols.append("c.category_name AS category_name" if join_cats else "NULL AS category_name")
    select_cols.append("b.brand_name AS brand_name" if join_brands else "NULL AS brand_name")
    select_cols.append("u.uom_code AS uom_code" if join_uom else "NULL AS uom_code")
    select_cols.append("u.uom_name AS uom_name" if join_uom else "NULL AS uom_name")
    
    # Stock totals (defensive — نتحقّق من inv_balances)
    bal_check = await db.execute(text("""
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema='public' AND table_name='inv_balances'
    """))
    if bal_check.fetchone():
        select_cols.append(
            "(SELECT COALESCE(SUM(qty_on_hand),0) FROM inv_balances bal "
            "WHERE bal.item_id=i.id AND bal.tenant_id=:tid) AS total_qty"
        )
        select_cols.append(
            "(SELECT COALESCE(SUM(total_value),0) FROM inv_balances bal "
            "WHERE bal.item_id=i.id AND bal.tenant_id=:tid) AS total_value"
        )
    else:
        select_cols.append("0 AS total_qty")
        select_cols.append("0 AS total_value")

    select_clause = ",\n            ".join(select_cols)

    sql = f"""
        SELECT
            {select_clause}
        FROM inv_items i
        {join_cats}
        {join_brands}
        {join_uom}
        WHERE {where}
        ORDER BY {"i.item_name" if "item_name" in existing else "i.id"}
        LIMIT :limit OFFSET :offset
    """
    
    try:
        r = await db.execute(text(sql), params)
        items = [dict(row._mapping) for row in r.fetchall()]
        return ok(data={"total": total, "items": items, "limit": limit, "offset": offset})
    except Exception as e:
        await db.rollback()
        # نطبع الخطأ للتشخيص
        import traceback
        return ok(data={
            "total": 0,
            "items": [],
            "limit": limit,
            "offset": offset,
            "_debug_error": str(e),
            "_debug_columns_found": sorted(existing),
        })


# ═══════════════════════════════════════════════════════════════════════════
# GET ITEM V2 — مع variants + variant_attrs
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/items-v2/{item_id}")
async def get_item_v2(
    item_id: uuid.UUID,
    include_variants: bool = Query(default=True),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
            i.*, c.category_name, b.brand_name,
            u.uom_code, u.uom_name,
            pu.uom_code AS purchase_uom_code, pu.uom_name AS purchase_uom_name,
            su.uom_code AS sales_uom_code, su.uom_name AS sales_uom_name,
            parent.item_code AS parent_item_code, parent.item_name AS parent_item_name
        FROM inv_items i
        LEFT JOIN inv_categories c ON c.id = i.category_id
        LEFT JOIN inv_brands b ON b.id = i.brand_id
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        LEFT JOIN inv_uom pu ON pu.id = i.purchase_uom_id
        LEFT JOIN inv_uom su ON su.id = i.sales_uom_id
        LEFT JOIN inv_items parent ON parent.id = i.parent_item_id
        WHERE i.id=:id AND i.tenant_id=:tid
    """), {"id": str(item_id), "tid": tid})
    row = r.fetchone()
    if not row:
        raise HTTPException(404, "الصنف غير موجود")
    item = dict(row._mapping)

    # variant attributes (if it's a variant)
    if item.get("is_variant"):
        rv = await db.execute(text("""
            SELECT
                va.attribute_id, va.value_id,
                a.attribute_code, a.attribute_name,
                v.value_code, v.value_name, v.color_hex
            FROM inv_item_variant_attrs va
            LEFT JOIN inv_item_attributes a ON a.id = va.attribute_id
            LEFT JOIN inv_item_attribute_values v ON v.id = va.value_id
            WHERE va.item_id=:iid AND va.tenant_id=:tid
            ORDER BY a.sort_order, a.attribute_name
        """), {"iid": str(item_id), "tid": tid})
        item["variant_attrs"] = [dict(r._mapping) for r in rv.fetchall()]
    else:
        item["variant_attrs"] = []

    # Children variants (if it's a parent template)
    if include_variants and item.get("has_variants"):
        rc = await db.execute(text("""
            SELECT
                child.id, child.item_code, child.item_name, child.barcode,
                child.purchase_price, child.sale_price, child.is_active,
                (SELECT COALESCE(SUM(qty_on_hand),0) FROM inv_balances bal
                 WHERE bal.item_id=child.id AND bal.tenant_id=:tid) AS total_qty
            FROM inv_items child
            WHERE child.parent_item_id=:pid AND child.tenant_id=:tid
            ORDER BY child.item_code
        """), {"pid": str(item_id), "tid": tid})
        children = [dict(r._mapping) for r in rc.fetchall()]
        # Get attrs for each child
        for c in children:
            ra = await db.execute(text("""
                SELECT a.attribute_name, v.value_name, v.color_hex
                FROM inv_item_variant_attrs va
                LEFT JOIN inv_item_attributes a ON a.id = va.attribute_id
                LEFT JOIN inv_item_attribute_values v ON v.id = va.value_id
                WHERE va.item_id=:cid AND va.tenant_id=:tid
                ORDER BY a.sort_order
            """), {"cid": str(c["id"]), "tid": tid})
            c["attrs"] = [dict(r._mapping) for r in ra.fetchall()]
        item["variants"] = children
    else:
        item["variants"] = []

    # Balances by warehouse
    rb = await db.execute(text("""
        SELECT b.warehouse_id, b.qty_on_hand, b.qty_reserved,
               b.avg_cost, b.total_value, b.last_movement,
               w.warehouse_name, w.warehouse_code
        FROM inv_balances b
        LEFT JOIN inv_warehouses w ON w.id = b.warehouse_id
        WHERE b.item_id=:iid AND b.tenant_id=:tid
        ORDER BY w.warehouse_name
    """), {"iid": str(item_id), "tid": tid})
    item["balances"] = [dict(r._mapping) for r in rb.fetchall()]

    return ok(data=item)


# ═══════════════════════════════════════════════════════════════════════════
# CREATE ITEM V2
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/items-v2", status_code=201)
async def create_item_v2(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    إنشاء صنف جديد — defensive schema-aware.
    
    يبني INSERT ديناميكياً بناء على الأعمدة الموجودة فعلاً.
    لا يفشل بسبب أعمدة مفقودة (cost_method, valuation_method, إلخ).
    """
    tid = str(user.tenant_id)
    iid = str(uuid.uuid4())
    
    if not data.get("item_code") or not data.get("item_name"):
        raise HTTPException(400, "item_code و item_name مطلوبان")

    # ─── اكتشف الأعمدة الموجودة فعلياً ──────────────
    cols_q = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='inv_items'
    """))
    existing = {row[0] for row in cols_q.fetchall()}
    
    # ─── تحقّق من الكود فريد ────────────────────────
    chk = await db.execute(
        text("SELECT 1 FROM inv_items WHERE tenant_id=:tid AND item_code=:c LIMIT 1"),
        {"tid": tid, "c": data["item_code"]},
    )
    if chk.fetchone():
        raise HTTPException(400, f"الكود {data['item_code']} موجود مسبقاً")

    # ─── ابنِ INSERT ديناميكياً ──────────────────────
    fields = ["id", "tenant_id", "item_code", "item_name"]
    values = [":id", ":tid", ":code", ":name"]
    params: dict = {
        "id": iid, "tid": tid,
        "code": data["item_code"], "name": data["item_name"],
    }
    
    # خريطة: column_name → (param_key, type_class, default_value)
    # مهم: الحقول الرقمية الافتراضية = 0 (وليس None) لتجنّب NotNullViolationError
    optional_cols = [
        # text fields
        ("item_name_en",        "en",       "text",     None),
        ("description",         "desc",     "text",     None),
        ("barcode",             "bar",      "text",     None),
        # ⭐ نوع الصنف (item_type)
        ("item_type",           "itype",    "text",     "stockable"),
        ("product_type",        "ptype",    "text",     None),
        ("nature",              "nat",      "text",     None),
        ("kind",                "kind",     "text",     None),
        # FK fields (UUID) - None مسموح في FK
        ("category_id",         "cat",      "uuid",     None),
        ("brand_id",            "brand",    "uuid",     None),
        ("uom_id",              "uom",      "uuid",     None),
        ("purchase_uom_id",     "puom",     "uuid",     None),
        ("sales_uom_id",        "suom",     "uuid",     None),
        ("parent_item_id",      "pid",      "uuid",     None),
        # numeric fields - افتراضي 0 لتجنّب NOT NULL violations
        ("purchase_price",      "pp",       "numeric",  0),
        ("sale_price",          "sp",       "numeric",  0),
        ("standard_cost",       "sc",       "numeric",  0),
        ("weight_kg",           "wt",       "numeric",  0),
        ("volume_m3",           "vol",      "numeric",  0),
        ("reorder_point",       "rop",      "numeric",  0),
        ("reorder_qty",         "roq",      "numeric",  0),
        ("min_stock",           "mins",     "numeric",  0),
        ("max_stock",           "maxs",     "numeric",  0),
        ("safety_stock",        "ss",       "numeric",  0),
        ("lead_time_days",      "lt",       "numeric",  0),
        # accounts (text)
        ("gl_account_code",     "gl",       "text",     None),
        ("cogs_account_code",   "cogs",     "text",     None),
        ("income_account_code", "inc",      "text",     None),
        # misc text
        ("unspsc_code",         "unspsc",   "text",     None),
        ("classification_code", "cls",      "text",     None),
        ("image_url",           "img",      "text",     None),
        ("notes",               "notes",    "text",     None),
        # booleans (default True)
        ("is_active",           "act",      "bool_t",   True),
        ("is_purchasable",      "purch",    "bool_t",   True),
        ("is_sellable",         "sell",     "bool_t",   True),
        # booleans (default False)
        ("is_serialized",       "ser",      "bool_f",   False),
        ("is_lot_tracked",      "lot",      "bool_f",   False),
        ("is_expiry_tracked",   "exp",      "bool_f",   False),
        ("has_variants",        "hv",       "bool_f",   False),
        ("is_variant",          "iv",       "bool_f",   False),
        ("allow_negative_stock", "ans",     "bool_f",   False),
    ]
    
    for col_name, param_key, type_class, default in optional_cols:
        if col_name in existing:
            fields.append(col_name)
            values.append(f":{param_key}")
            raw_value = data.get(col_name, default)
            
            # تحويل ذكي حسب النوع
            if type_class == "numeric":
                cleaned = _clean_numeric(raw_value)
                # ✅ الحقول الرقمية: استخدم default (عادة 0) بدلاً من None
                params[param_key] = cleaned if cleaned is not None else default
            elif type_class == "uuid":
                params[param_key] = _clean_uuid(raw_value)
            elif type_class == "text":
                params[param_key] = _clean_text(raw_value)
            elif type_class in ("bool_t", "bool_f"):
                params[param_key] = _clean_bool(raw_value, default)
            else:
                params[param_key] = raw_value
    
    # ─── valuation_method / cost_method (الـ bug الأصلي) ──
    # كلاهما أو أحدهما أو لا شيء
    vm_value = data.get("valuation_method", "avg")
    if "valuation_method" in existing:
        fields.append("valuation_method")
        values.append(":vm")
        params["vm"] = vm_value
    if "cost_method" in existing:
        fields.append("cost_method")
        values.append(":cm")
        params["cm"] = vm_value
    
    # ─── extra_data (JSONB) ─────────────────────────
    if "extra_data" in existing:
        import json as _json
        extra = data.get("extra_data") or {}
        if isinstance(extra, dict):
            extra_json = _json.dumps(extra, ensure_ascii=False)
        else:
            extra_json = str(extra)
        fields.append("extra_data")
        values.append("CAST(:extra AS JSONB)")
        params["extra"] = extra_json

    # ─── نفّذ INSERT ────────────────────────────────
    sql = f"""
        INSERT INTO inv_items ({', '.join(fields)})
        VALUES ({', '.join(values)})
    """
    
    try:
        await db.execute(text(sql), params)
    except Exception as e:
        await db.rollback()
        # نُرجع رسالة واضحة بدلاً من 500
        err_msg = str(e)[:300]
        raise HTTPException(400, f"فشل إنشاء الصنف: {err_msg}")

    # ─── Variant attrs (defensive) ──────────────────
    var_attrs = data.get("variant_attrs", [])
    if var_attrs:
        attrs_check = await db.execute(text("""
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema='public' AND table_name='inv_item_variant_attrs'
        """))
        if attrs_check.fetchone():
            try:
                for va in var_attrs:
                    await db.execute(text("""
                        INSERT INTO inv_item_variant_attrs (
                            id, tenant_id, item_id, attribute_id, value_id
                        ) VALUES (
                            gen_random_uuid(), :tid, :iid, :aid, :vid
                        )
                    """), {
                        "tid": tid, "iid": iid,
                        "aid": va["attribute_id"], "vid": va["value_id"],
                    })
            except Exception:
                # نتجاهل أخطاء variants — الصنف الرئيسي محفوظ
                pass

    await db.commit()
    return created(
        data={"id": iid, "item_code": data["item_code"]},
        message="تم إنشاء الصنف ✅"
    )


# ═══════════════════════════════════════════════════════════════════════════
# UPDATE ITEM V2
# ═══════════════════════════════════════════════════════════════════════════
@router.put("/items-v2/{item_id}")
async def update_item_v2(
    item_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    تحديث صنف — defensive schema-aware.
    
    يستخدم نفس type-safe helpers المستخدمة في POST.
    لا يفشل بسبب أعمدة مفقودة أو قيم فارغة.
    """
    tid = str(user.tenant_id)
    
    # ─── اكتشف الأعمدة الموجودة ──────────────────────
    cols_q = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='inv_items'
    """))
    existing = {row[0] for row in cols_q.fetchall()}
    
    # ─── تحقّق من وجود الصنف ─────────────────────────
    chk = await db.execute(
        text("SELECT id FROM inv_items WHERE id=:id AND tenant_id=:tid LIMIT 1"),
        {"id": str(item_id), "tid": tid},
    )
    if not chk.fetchone():
        raise HTTPException(404, "الصنف غير موجود")
    
    # ─── خريطة الأعمدة مع أنواعها (مُطابقة للـ POST) ──
    update_cols = [
        # text fields
        ("item_code",           "text"),
        ("item_name",           "text"),
        ("item_name_en",        "text"),
        ("description",         "text"),
        ("barcode",             "text"),
        # ⭐ نوع الصنف (item_type) - مهم جداً!
        # المستخدم يُغيّره من dropdown (بضاعة مخزنية، مواد خام، خدمة، ...)
        ("item_type",           "text"),
        ("product_type",        "text"),  # احتياطي لو الاسم مختلف
        ("nature",              "text"),  # احتياطي إضافي
        ("kind",                "text"),  # احتياطي إضافي
        # FK fields (UUID)
        ("category_id",         "uuid"),
        ("brand_id",            "uuid"),
        ("uom_id",              "uuid"),
        ("purchase_uom_id",     "uuid"),
        ("sales_uom_id",        "uuid"),
        ("parent_item_id",      "uuid"),
        # numeric fields
        ("purchase_price",      "numeric"),
        ("sale_price",          "numeric"),
        ("standard_cost",       "numeric"),
        ("weight_kg",           "numeric"),
        ("volume_m3",           "numeric"),
        ("reorder_point",       "numeric"),
        ("reorder_qty",         "numeric"),
        ("min_stock",           "numeric"),
        ("max_stock",           "numeric"),
        ("safety_stock",        "numeric"),
        ("lead_time_days",      "numeric"),
        # accounts (text)
        ("gl_account_code",     "text"),
        ("cogs_account_code",   "text"),
        ("income_account_code", "text"),
        # misc text
        ("unspsc_code",         "text"),
        ("classification_code", "text"),
        ("image_url",           "text"),
        ("notes",               "text"),
        # booleans
        ("is_active",           "bool"),
        ("is_purchasable",      "bool"),
        ("is_sellable",         "bool"),
        ("is_serialized",       "bool"),
        ("is_lot_tracked",      "bool"),
        ("is_expiry_tracked",   "bool"),
        ("has_variants",        "bool"),
        ("is_variant",          "bool"),
        ("allow_negative_stock", "bool"),
    ]
    
    # ─── ابنِ UPDATE ديناميكياً ──────────────────────
    fields = []
    params: dict = {"id": str(item_id), "tid": tid}
    
    for col_name, type_class in update_cols:
        # حدّث فقط الأعمدة الموجودة في DB والمُرسلة من المستخدم
        if col_name in data and col_name in existing:
            raw_value = data[col_name]
            
            # تحويل ذكي
            if type_class == "numeric":
                cleaned = _clean_numeric(raw_value)
                # للحقول الرقمية: استخدم 0 لو None
                value = cleaned if cleaned is not None else 0
            elif type_class == "uuid":
                value = _clean_uuid(raw_value)
            elif type_class == "text":
                value = _clean_text(raw_value)
            elif type_class == "bool":
                value = _clean_bool(raw_value)
            else:
                value = raw_value
            
            fields.append(f"{col_name} = :{col_name}")
            params[col_name] = value
    
    # ─── valuation_method / cost_method (مهم جداً) ──
    if "valuation_method" in data:
        vm_value = data["valuation_method"] or "avg"
        if "valuation_method" in existing:
            fields.append("valuation_method = :vm")
            params["vm"] = vm_value
        if "cost_method" in existing:
            fields.append("cost_method = :cm")
            params["cm"] = vm_value
    
    # ─── extra_data (JSONB) ─────────────────────────
    if "extra_data" in data and "extra_data" in existing:
        import json as _json
        extra = data["extra_data"]
        if isinstance(extra, dict):
            extra_json = _json.dumps(extra, ensure_ascii=False)
        else:
            extra_json = str(extra)
        fields.append("extra_data = CAST(:extra AS JSONB)")
        params["extra"] = extra_json

    if not fields:
        return ok(data={"id": str(item_id)}, message="لا تغييرات")
    
    # updated_at لو موجود
    if "updated_at" in existing:
        fields.append("updated_at = NOW()")

    sql = f"""
        UPDATE inv_items SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """
    
    try:
        await db.execute(text(sql), params)
        await db.commit()
        return ok(data={"id": str(item_id)}, message="تم تحديث الصنف ✅")
    except Exception as e:
        await db.rollback()
        # ✅ القاعدة الذهبية #30 — رسالة عربية واضحة
        raise HTTPException(400, f"فشل تحديث الصنف: {str(e)[:300]}")


# ═══════════════════════════════════════════════════════════════════════════
# DELETE ITEM V2 — مع فحوصات
# ═══════════════════════════════════════════════════════════════════════════
@router.delete("/items-v2/{item_id}", status_code=204)
async def delete_item_v2(
    item_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    # Check no transactions exist
    cnt = await db.execute(text("""
        SELECT COUNT(*) FROM inv_transaction_lines
        WHERE item_id=:iid AND tenant_id=:tid
    """), {"iid": str(item_id), "tid": tid})
    if cnt.scalar() > 0:
        raise HTTPException(400, "لا يمكن حذف صنف عليه حركات. استخدم is_active=false")

    # Check no balance > 0
    bcnt = await db.execute(text("""
        SELECT COUNT(*) FROM inv_balances
        WHERE item_id=:iid AND tenant_id=:tid AND qty_on_hand != 0
    """), {"iid": str(item_id), "tid": tid})
    if bcnt.scalar() > 0:
        raise HTTPException(400, "لا يمكن حذف صنف برصيد غير صفري")

    # Check no variants reference this as parent
    vcnt = await db.execute(text("""
        SELECT COUNT(*) FROM inv_items WHERE parent_item_id=:iid AND tenant_id=:tid
    """), {"iid": str(item_id), "tid": tid})
    if vcnt.scalar() > 0:
        raise HTTPException(400, "هذا الصنف له variants — احذفها أولاً")

    # Cascade: delete attrs, balances=0
    await db.execute(text("DELETE FROM inv_item_variant_attrs WHERE item_id=:id"), {"id": str(item_id)})
    await db.execute(text("DELETE FROM inv_balances WHERE item_id=:id"), {"id": str(item_id)})
    await db.execute(
        text("DELETE FROM inv_items WHERE id=:id AND tenant_id=:tid"),
        {"id": str(item_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# GENERATE VARIANTS — توليد كل التركيبات الممكنة من attributes
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/items-v2/{parent_id}/generate-variants", status_code=201)
async def generate_variants(
    parent_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    يُولّد variants تلقائياً من Cartesian product للـ attributes المختارة.
    Body:
    {
      "attribute_value_ids": {
         "attribute_id_1": ["value_id_a", "value_id_b"],
         "attribute_id_2": ["value_id_c", "value_id_d", "value_id_e"]
      },
      "code_suffix_format": "-{COLOR}-{SIZE}",  // optional
      "skip_existing": true
    }
    """
    tid = str(user.tenant_id)

    # Get parent
    rp = await db.execute(text("""
        SELECT * FROM inv_items WHERE id=:id AND tenant_id=:tid
    """), {"id": str(parent_id), "tid": tid})
    parent = rp.fetchone()
    if not parent:
        raise HTTPException(404, "الصنف الأب غير موجود")
    parent_dict = dict(parent._mapping)

    attr_value_map = data.get("attribute_value_ids", {})
    if not attr_value_map:
        raise HTTPException(400, "attribute_value_ids مطلوب")

    # Mark parent as has_variants if not already
    if not parent_dict.get("has_variants"):
        await db.execute(text("""
            UPDATE inv_items SET has_variants = true, updated_at = NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"id": str(parent_id), "tid": tid})

    # Get attribute names + value names for code generation
    attr_ids = list(attr_value_map.keys())
    value_ids_flat = [v for vals in attr_value_map.values() for v in vals]

    if not attr_ids or not value_ids_flat:
        raise HTTPException(400, "attribute_value_ids فارغ")

    ra = await db.execute(text(f"""
        SELECT id, attribute_code, attribute_name FROM inv_item_attributes
        WHERE id IN ({','.join([f"'{a}'" for a in attr_ids])}) AND tenant_id=:tid
    """), {"tid": tid})
    attrs_by_id = {str(r._mapping["id"]): dict(r._mapping) for r in ra.fetchall()}

    rv = await db.execute(text(f"""
        SELECT id, attribute_id, value_code, value_name FROM inv_item_attribute_values
        WHERE id IN ({','.join([f"'{v}'" for v in value_ids_flat])}) AND tenant_id=:tid
    """), {"tid": tid})
    values_by_id = {str(r._mapping["id"]): dict(r._mapping) for r in rv.fetchall()}

    # Generate Cartesian product
    attr_id_list = list(attr_value_map.keys())
    value_lists = [attr_value_map[aid] for aid in attr_id_list]
    combinations = list(product(*value_lists))

    skip_existing = data.get("skip_existing", True)
    code_format = data.get("code_suffix_format", "-{vals}")
    created_count = 0
    skipped_count = 0
    created_ids = []

    for combo in combinations:
        # Build code suffix from value codes
        val_codes = []
        val_names = []
        for vid in combo:
            v = values_by_id.get(vid, {})
            val_codes.append(v.get("value_code") or v.get("value_name", "X"))
            val_names.append(v.get("value_name", "X"))
        suffix = "-" + "-".join(val_codes)
        new_code = f"{parent_dict['item_code']}{suffix}"
        new_name = f"{parent_dict['item_name']} - " + " / ".join(val_names)

        # Check existing
        if skip_existing:
            chk = await db.execute(
                text("SELECT id FROM inv_items WHERE tenant_id=:tid AND item_code=:c"),
                {"tid": tid, "c": new_code},
            )
            if chk.fetchone():
                skipped_count += 1
                continue

        new_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO inv_items (
                id, tenant_id, item_code, item_name, item_name_en,
                description, category_id, brand_id, uom_id,
                purchase_price, sale_price, standard_cost,
                valuation_method, cost_method,
                gl_account_code, cogs_account_code,
                is_active, is_purchasable, is_sellable,
                is_serialized, is_lot_tracked, is_expiry_tracked,
                has_variants, is_variant, parent_item_id
            ) VALUES (
                :id, :tid, :code, :name, :en,
                :desc, :cat, :brand, :uom,
                :pp, :sp, :sc,
                :vm, :vm,
                :gl, :cogs,
                true, :purch, :sell,
                :ser, :lot, :exp,
                false, true, :pid
            )
        """), {
            "id": new_id, "tid": tid,
            "code": new_code, "name": new_name,
            "en": parent_dict.get("item_name_en"),
            "desc": parent_dict.get("description"),
            "cat": parent_dict.get("category_id"),
            "brand": parent_dict.get("brand_id"),
            "uom": parent_dict.get("uom_id"),
            "pp": parent_dict.get("purchase_price", 0),
            "sp": parent_dict.get("sale_price", 0),
            "sc": parent_dict.get("standard_cost", 0),
            "vm": parent_dict.get("valuation_method") or parent_dict.get("cost_method") or "avg",
            "gl": parent_dict.get("gl_account_code"),
            "cogs": parent_dict.get("cogs_account_code"),
            "purch": parent_dict.get("is_purchasable", True),
            "sell": parent_dict.get("is_sellable", True),
            "ser": parent_dict.get("is_serialized", False),
            "lot": parent_dict.get("is_lot_tracked", False),
            "exp": parent_dict.get("is_expiry_tracked", False),
            "pid": str(parent_id),
        })

        # Insert variant_attrs
        for i, vid in enumerate(combo):
            aid = attr_id_list[i]
            await db.execute(text("""
                INSERT INTO inv_item_variant_attrs (
                    id, tenant_id, item_id, attribute_id, value_id
                ) VALUES (
                    gen_random_uuid(), :tid, :iid, :aid, :vid
                )
            """), {"tid": tid, "iid": new_id, "aid": aid, "vid": vid})

        created_count += 1
        created_ids.append({"id": new_id, "item_code": new_code, "item_name": new_name})

    await db.commit()
    return created(
        data={
            "created_count": created_count,
            "skipped_count": skipped_count,
            "total_combinations": len(combinations),
            "created_items": created_ids,
        },
        message=f"تم توليد {created_count} variant — تم تخطي {skipped_count} موجود مسبقاً ✅",
    )
