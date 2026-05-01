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
    tid = str(user.tenant_id)
    conds = ["i.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}

    if search:
        conds.append("(i.item_code ILIKE :s OR i.item_name ILIKE :s OR i.barcode ILIKE :s)")
        params["s"] = f"%{search}%"
    if category_id is not None:
        conds.append("i.category_id=:cid")
        params["cid"] = str(category_id)
    if brand_id is not None:
        conds.append("i.brand_id=:bid")
        params["bid"] = str(brand_id)
    if is_active is not None:
        conds.append("i.is_active=:act")
        params["act"] = is_active
    if has_variants is not None:
        conds.append("i.has_variants=:hv")
        params["hv"] = has_variants
    if is_variant is not None:
        conds.append("i.is_variant=:iv")
        params["iv"] = is_variant
    if parent_item_id is not None:
        conds.append("i.parent_item_id=:pid")
        params["pid"] = str(parent_item_id)
    if is_serialized is not None:
        conds.append("i.is_serialized=:ser")
        params["ser"] = is_serialized
    if is_lot_tracked is not None:
        conds.append("i.is_lot_tracked=:lt")
        params["lt"] = is_lot_tracked
    if valuation_method:
        conds.append("(i.valuation_method=:vm OR i.cost_method=:vm)")
        params["vm"] = valuation_method

    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM inv_items i WHERE {where}"), params)
    total = cnt.scalar() or 0

    r = await db.execute(text(f"""
        SELECT
            i.id, i.item_code, i.item_name, i.item_name_en,
            i.description, i.barcode,
            i.category_id, i.brand_id, i.uom_id,
            i.purchase_uom_id, i.sales_uom_id,
            i.purchase_price, i.sale_price, i.standard_cost,
            COALESCE(i.valuation_method, i.cost_method, 'avg') AS valuation_method,
            i.gl_account_code, i.cogs_account_code, i.income_account_code,
            i.unspsc_code, i.classification_code,
            i.is_active, i.is_purchasable, i.is_sellable,
            i.is_serialized, i.is_lot_tracked, i.is_expiry_tracked,
            i.has_variants, i.is_variant, i.parent_item_id,
            i.weight_kg, i.volume_m3,
            i.reorder_point, i.reorder_qty,
            i.image_url, i.notes, i.extra_data,
            c.category_name, b.brand_name, u.uom_code, u.uom_name,
            (SELECT COALESCE(SUM(qty_on_hand),0) FROM inv_balances bal
             WHERE bal.item_id=i.id AND bal.tenant_id=:tid) AS total_qty,
            (SELECT COALESCE(SUM(total_value),0) FROM inv_balances bal
             WHERE bal.item_id=i.id AND bal.tenant_id=:tid) AS total_value,
            i.created_at, i.updated_at
        FROM inv_items i
        LEFT JOIN inv_categories c ON c.id = i.category_id
        LEFT JOIN inv_brands b ON b.id = i.brand_id
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        WHERE {where}
        ORDER BY i.item_name
        LIMIT :limit OFFSET :offset
    """), params)
    items = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"total": total, "items": items, "limit": limit, "offset": offset})


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
    tid = str(user.tenant_id)
    iid = str(uuid.uuid4())
    if not data.get("item_code") or not data.get("item_name"):
        raise HTTPException(400, "item_code و item_name مطلوبان")

    # Check unique code
    chk = await db.execute(
        text("SELECT 1 FROM inv_items WHERE tenant_id=:tid AND item_code=:c LIMIT 1"),
        {"tid": tid, "c": data["item_code"]},
    )
    if chk.fetchone():
        raise HTTPException(400, f"الكود {data['item_code']} موجود مسبقاً")

    import json as _json
    extra = data.get("extra_data") or {}
    if isinstance(extra, dict):
        extra_json = _json.dumps(extra, ensure_ascii=False)
    else:
        extra_json = str(extra)

    await db.execute(text("""
        INSERT INTO inv_items (
            id, tenant_id, item_code, item_name, item_name_en,
            description, barcode,
            category_id, brand_id, uom_id, purchase_uom_id, sales_uom_id,
            purchase_price, sale_price, standard_cost,
            valuation_method, cost_method,
            gl_account_code, cogs_account_code, income_account_code,
            unspsc_code, classification_code,
            is_active, is_purchasable, is_sellable,
            is_serialized, is_lot_tracked, is_expiry_tracked,
            has_variants, is_variant, parent_item_id,
            weight_kg, volume_m3, reorder_point, reorder_qty,
            image_url, notes, extra_data
        ) VALUES (
            :id, :tid, :code, :name, :en,
            :desc, :bar,
            :cat, :brand, :uom, :puom, :suom,
            :pp, :sp, :sc,
            :vm, :vm,
            :gl, :cogs, :inc,
            :unspsc, :cls,
            :act, :purch, :sell,
            :ser, :lot, :exp,
            :hv, :iv, :pid,
            :wt, :vol, :rop, :roq,
            :img, :notes, CAST(:extra AS JSONB)
        )
    """), {
        "id": iid, "tid": tid,
        "code": data["item_code"], "name": data["item_name"],
        "en": data.get("item_name_en"),
        "desc": data.get("description"),
        "bar": data.get("barcode"),
        "cat": data.get("category_id"),
        "brand": data.get("brand_id"),
        "uom": data.get("uom_id"),
        "puom": data.get("purchase_uom_id"),
        "suom": data.get("sales_uom_id"),
        "pp": data.get("purchase_price", 0),
        "sp": data.get("sale_price", 0),
        "sc": data.get("standard_cost", 0),
        "vm": data.get("valuation_method", "avg"),
        "gl": data.get("gl_account_code"),
        "cogs": data.get("cogs_account_code"),
        "inc": data.get("income_account_code"),
        "unspsc": data.get("unspsc_code"),
        "cls": data.get("classification_code"),
        "act": data.get("is_active", True),
        "purch": data.get("is_purchasable", True),
        "sell": data.get("is_sellable", True),
        "ser": data.get("is_serialized", False),
        "lot": data.get("is_lot_tracked", False),
        "exp": data.get("is_expiry_tracked", False),
        "hv": data.get("has_variants", False),
        "iv": data.get("is_variant", False),
        "pid": data.get("parent_item_id"),
        "wt": data.get("weight_kg"),
        "vol": data.get("volume_m3"),
        "rop": data.get("reorder_point"),
        "roq": data.get("reorder_qty"),
        "img": data.get("image_url"),
        "notes": data.get("notes"),
        "extra": extra_json,
    })

    # Variant attrs (if creating a variant directly)
    var_attrs = data.get("variant_attrs", [])
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

    await db.commit()
    return created(data={"id": iid}, message="تم إنشاء الصنف ✅")


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
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(item_id), "tid": tid}
    for col in [
        "item_code", "item_name", "item_name_en", "description", "barcode",
        "category_id", "brand_id", "uom_id",
        "purchase_uom_id", "sales_uom_id",
        "purchase_price", "sale_price", "standard_cost",
        "valuation_method",
        "gl_account_code", "cogs_account_code", "income_account_code",
        "unspsc_code", "classification_code",
        "is_active", "is_purchasable", "is_sellable",
        "is_serialized", "is_lot_tracked", "is_expiry_tracked",
        "has_variants",
        "weight_kg", "volume_m3", "reorder_point", "reorder_qty",
        "image_url", "notes",
    ]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]

    if "extra_data" in data:
        import json as _json
        extra = data["extra_data"]
        params["extra"] = _json.dumps(extra, ensure_ascii=False) if isinstance(extra, dict) else str(extra)
        fields.append("extra_data = CAST(:extra AS JSONB)")

    if not fields:
        return ok(data={}, message="لا تغييرات")
    fields.append("updated_at = NOW()")

    await db.execute(text(f"""
        UPDATE inv_items SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(item_id)}, message="تم التحديث ✅")


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
