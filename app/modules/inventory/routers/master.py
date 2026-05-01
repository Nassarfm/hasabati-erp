"""
app/modules/inventory/routers/master.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Master Data Router
═══════════════════════════════════════════════════════════════════════════
Endpoints:
  /inventory/uom-conversions    — تحويلات الوحدات (1 كرتون = 24 علبة)
  /inventory/brands             — العلامات التجارية
  /inventory/reason-codes       — أكواد الأسباب (تالف، سرقة، إلخ)
  /inventory/attributes         — خصائص Variants (Color, Size, ...)
  /inventory/attribute-values   — قيم الخصائص (Red, Blue, S, M, L, ...)
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory", tags=["inventory-master"])


# ═══════════════════════════════════════════════════════════════════════════
# UOM CONVERSIONS — تحويلات وحدات القياس
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/uom-conversions")
async def list_uom_conversions(
    item_id: Optional[uuid.UUID] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة تحويلات الوحدات — عام (item_id=NULL) أو خاص بصنف"""
    tid = str(user.tenant_id)
    conds = ["c.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if item_id is not None:
        conds.append("c.item_id=:iid")
        params["iid"] = str(item_id)
    if is_active is not None:
        conds.append("c.is_active=:act")
        params["act"] = is_active

    r = await db.execute(text(f"""
        SELECT
            c.id, c.from_uom_id, c.to_uom_id, c.factor, c.item_id,
            c.is_active, c.notes,
            fu.uom_code AS from_uom_code, fu.uom_name AS from_uom_name,
            tu.uom_code AS to_uom_code,   tu.uom_name AS to_uom_name,
            i.item_code AS item_code,     i.item_name AS item_name,
            c.created_at, c.updated_at
        FROM inv_uom_conversions c
        LEFT JOIN inv_uom fu  ON fu.id = c.from_uom_id
        LEFT JOIN inv_uom tu  ON tu.id = c.to_uom_id
        LEFT JOIN inv_items i ON i.id  = c.item_id
        WHERE {' AND '.join(conds)}
        ORDER BY c.is_active DESC, fu.uom_name, tu.uom_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/uom-conversions", status_code=201)
async def create_uom_conversion(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    cid = str(uuid.uuid4())
    factor = data.get("factor")
    if factor is None or float(factor) <= 0:
        raise HTTPException(400, "factor يجب أن يكون رقماً موجباً")
    from_uom = data.get("from_uom_id")
    to_uom = data.get("to_uom_id")
    if not from_uom or not to_uom:
        raise HTTPException(400, "from_uom_id و to_uom_id مطلوبان")
    if from_uom == to_uom:
        raise HTTPException(400, "from_uom و to_uom يجب أن يكونا مختلفين")

    await db.execute(text("""
        INSERT INTO inv_uom_conversions (
            id, tenant_id, from_uom_id, to_uom_id, factor,
            item_id, is_active, notes
        ) VALUES (
            :id, :tid, :fu, :tu, :f,
            :iid, :act, :notes
        )
    """), {
        "id": cid, "tid": tid,
        "fu": from_uom, "tu": to_uom, "f": factor,
        "iid": data.get("item_id"),
        "act": data.get("is_active", True),
        "notes": data.get("notes"),
    })
    await db.commit()
    return created(data={"id": cid}, message="تم إنشاء التحويل ✅")


@router.put("/uom-conversions/{conv_id}")
async def update_uom_conversion(
    conv_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(conv_id), "tid": tid}
    for col, key in [
        ("factor", "factor"), ("is_active", "is_active"),
        ("notes", "notes"), ("from_uom_id", "from_uom_id"),
        ("to_uom_id", "to_uom_id"), ("item_id", "item_id"),
    ]:
        if key in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[key]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    fields.append("updated_at = NOW()")
    await db.execute(text(f"""
        UPDATE inv_uom_conversions SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(conv_id)}, message="تم التحديث")


@router.delete("/uom-conversions/{conv_id}", status_code=204)
async def delete_uom_conversion(
    conv_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(
        text("DELETE FROM inv_uom_conversions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(conv_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# BRANDS — العلامات التجارية
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/brands")
async def list_brands(
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["tenant_id=:tid"]
    params: dict = {"tid": tid}
    if is_active is not None:
        conds.append("is_active=:act")
        params["act"] = is_active
    r = await db.execute(text(f"""
        SELECT id, brand_code, brand_name, brand_name_en,
               manufacturer, country_of_origin, website,
               logo_url, is_active, notes, created_at, updated_at
        FROM inv_brands
        WHERE {' AND '.join(conds)}
        ORDER BY brand_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/brands", status_code=201)
async def create_brand(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    bid = str(uuid.uuid4())
    if not data.get("brand_name"):
        raise HTTPException(400, "brand_name مطلوب")
    await db.execute(text("""
        INSERT INTO inv_brands (
            id, tenant_id, brand_code, brand_name, brand_name_en,
            manufacturer, country_of_origin, website, logo_url,
            is_active, notes
        ) VALUES (
            :id, :tid, :code, :name, :en,
            :mfg, :country, :web, :logo,
            :act, :notes
        )
    """), {
        "id": bid, "tid": tid,
        "code": data.get("brand_code"),
        "name": data["brand_name"],
        "en": data.get("brand_name_en"),
        "mfg": data.get("manufacturer"),
        "country": data.get("country_of_origin"),
        "web": data.get("website"),
        "logo": data.get("logo_url"),
        "act": data.get("is_active", True),
        "notes": data.get("notes"),
    })
    await db.commit()
    return created(data={"id": bid}, message="تم إنشاء العلامة التجارية ✅")


@router.put("/brands/{brand_id}")
async def update_brand(
    brand_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(brand_id), "tid": tid}
    for col in ["brand_code", "brand_name", "brand_name_en", "manufacturer",
                "country_of_origin", "website", "logo_url", "is_active", "notes"]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    fields.append("updated_at = NOW()")
    await db.execute(text(f"""
        UPDATE inv_brands SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(brand_id)}, message="تم التحديث")


@router.delete("/brands/{brand_id}", status_code=204)
async def delete_brand(
    brand_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    # Check no items use this brand
    cnt = await db.execute(
        text("SELECT COUNT(*) FROM inv_items WHERE brand_id=:bid AND tenant_id=:tid"),
        {"bid": str(brand_id), "tid": tid},
    )
    if cnt.scalar() > 0:
        raise HTTPException(400, "لا يمكن حذف علامة تجارية مرتبطة بأصناف")
    await db.execute(
        text("DELETE FROM inv_brands WHERE id=:id AND tenant_id=:tid"),
        {"id": str(brand_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# REASON CODES — أكواد الأسباب (تالف، سرقة، تقادم، إلخ)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/reason-codes")
async def list_reason_codes(
    applicable_for: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    قائمة أكواد الأسباب — اختياري filter حسب النوع.
    
    NOTE: DB يستخدم أسماء أعمدة مختلفة:
      • applies_to_tx_types (DB) → applicable_for (frontend alias)
      • is_system           (DB) → is_system_protected (frontend alias)
    """
    tid = str(user.tenant_id)
    conds = ["tenant_id=:tid"]
    params: dict = {"tid": tid}
    if applicable_for:
        conds.append("(applies_to_tx_types IS NULL OR applies_to_tx_types=:af OR applies_to_tx_types='all')")
        params["af"] = applicable_for
    if is_active is not None:
        conds.append("is_active=:act")
        params["act"] = is_active
    r = await db.execute(text(f"""
        SELECT 
          id, reason_code, reason_name, reason_name_en,
          expense_account_code,
          applies_to_tx_types AS applicable_for,
          is_increase,
          is_system AS is_system_protected,
          is_active, notes, sort_order,
          requires_expense_acc, affects_cogs,
          created_at,
          NULL::timestamptz AS updated_at
        FROM inv_reason_codes
        WHERE {' AND '.join(conds)}
        ORDER BY sort_order NULLS LAST, reason_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/reason-codes", status_code=201)
async def create_reason_code(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    إنشاء سبب جديد. الأسباب الجديدة من المستخدم تكون is_system=false دائماً.
    
    NOTE: frontend يرسل applicable_for/is_system_protected لكن DB يستخدم
    applies_to_tx_types/is_system. نُحوِّل هنا.
    """
    tid = str(user.tenant_id)
    rid = str(uuid.uuid4())
    if not data.get("reason_code") or not data.get("reason_name"):
        raise HTTPException(400, "reason_code و reason_name مطلوبان")
    await db.execute(text("""
        INSERT INTO inv_reason_codes (
            id, tenant_id, reason_code, reason_name, reason_name_en,
            expense_account_code, applies_to_tx_types, is_increase,
            is_system, is_active, notes
        ) VALUES (
            :id, :tid, :code, :name, :en,
            :acc, :af, :inc,
            false, :act, :notes
        )
    """), {
        "id": rid, "tid": tid,
        "code": data["reason_code"], "name": data["reason_name"],
        "en": data.get("reason_name_en"),
        "acc": data.get("expense_account_code"),
        "af": data.get("applicable_for"),
        "inc": data.get("is_increase", False),
        "act": data.get("is_active", True),
        "notes": data.get("notes"),
    })
    await db.commit()
    return created(data={"id": rid}, message="تم إنشاء كود السبب ✅")


@router.put("/reason-codes/{reason_id}")
async def update_reason_code(
    reason_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    تحديث سبب. الأسباب النظامية (is_system=true) لا يمكن تغيير reason_code لها.
    
    NOTE: نُحوِّل أسماء الحقول من frontend (applicable_for, is_system_protected)
    إلى أعمدة DB (applies_to_tx_types, is_system).
    """
    tid = str(user.tenant_id)
    # Check is_system + reason_code
    r = await db.execute(
        text("SELECT is_system, reason_code FROM inv_reason_codes WHERE id=:id AND tenant_id=:tid"),
        {"id": str(reason_id), "tid": tid},
    )
    row = r.fetchone()
    if not row:
        raise HTTPException(404, "كود السبب غير موجود")
    if row[0] and ("reason_code" in data and data["reason_code"] != row[1]):
        raise HTTPException(400, "لا يمكن تغيير reason_code للأكواد المحمية")

    # خريطة تحويل أسماء الحقول من frontend إلى DB
    COL_MAP = {
        "applicable_for": "applies_to_tx_types",
        "is_system_protected": "is_system",
    }

    fields = []
    params: dict = {"id": str(reason_id), "tid": tid}
    for col in ["reason_code", "reason_name", "reason_name_en", "expense_account_code",
                "applicable_for", "is_increase", "is_active", "notes"]:
        if col in data:
            db_col = COL_MAP.get(col, col)
            fields.append(f"{db_col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    # ملاحظة: لا نُحدِّث updated_at لأنه غير موجود في الجدول
    await db.execute(text(f"""
        UPDATE inv_reason_codes SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(reason_id)}, message="تم التحديث")


@router.delete("/reason-codes/{reason_id}", status_code=204)
async def delete_reason_code(
    reason_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حذف سبب. الأسباب النظامية (is_system=true) محميّة لا يمكن حذفها."""
    tid = str(user.tenant_id)
    r = await db.execute(
        text("SELECT is_system FROM inv_reason_codes WHERE id=:id AND tenant_id=:tid"),
        {"id": str(reason_id), "tid": tid},
    )
    row = r.fetchone()
    if row and row[0]:
        raise HTTPException(400, "لا يمكن حذف الأكواد المحمية — استخدم is_active=false بدلاً من ذلك")
    await db.execute(
        text("DELETE FROM inv_reason_codes WHERE id=:id AND tenant_id=:tid"),
        {"id": str(reason_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# ITEM ATTRIBUTES — الخصائص (Color, Size, Material, ...)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/attributes")
async def list_attributes(
    is_active: Optional[bool] = None,
    include_values: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة خصائص المتغيرات (Color/Size/Material). include_values=true يضيف القيم."""
    tid = str(user.tenant_id)
    conds = ["tenant_id=:tid"]
    params: dict = {"tid": tid}
    if is_active is not None:
        conds.append("is_active=:act")
        params["act"] = is_active
    r = await db.execute(text(f"""
        SELECT id, attribute_code, attribute_name, attribute_name_en,
               display_type, is_active, sort_order,
               NULL::text AS notes,
               created_at,
               NULL::timestamptz AS updated_at
        FROM inv_item_attributes
        WHERE {' AND '.join(conds)}
        ORDER BY sort_order, attribute_name
    """), params)
    attrs = [dict(row._mapping) for row in r.fetchall()]

    if include_values and attrs:
        attr_ids = [str(a["id"]) for a in attrs]
        rv = await db.execute(text(f"""
            SELECT id, attribute_id, value_code, value_name, value_name_en,
                   color_hex, sort_order, is_active
            FROM inv_item_attribute_values
            WHERE tenant_id=:tid AND attribute_id IN ({','.join([f"'{i}'" for i in attr_ids])})
            ORDER BY sort_order, value_name
        """), {"tid": tid})
        all_vals = [dict(row._mapping) for row in rv.fetchall()]
        # Group by attribute_id
        by_attr: dict = {}
        for v in all_vals:
            by_attr.setdefault(str(v["attribute_id"]), []).append(v)
        for a in attrs:
            a["values"] = by_attr.get(str(a["id"]), [])

    return ok(data=attrs)


@router.post("/attributes", status_code=201)
async def create_attribute(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    aid = str(uuid.uuid4())
    if not data.get("attribute_code") or not data.get("attribute_name"):
        raise HTTPException(400, "attribute_code و attribute_name مطلوبان")
    # NOTE: notes column غير موجود في DB - نتجاهله
    await db.execute(text("""
        INSERT INTO inv_item_attributes (
            id, tenant_id, attribute_code, attribute_name, attribute_name_en,
            display_type, is_active, sort_order
        ) VALUES (
            :id, :tid, :code, :name, :en,
            :dt, :act, :so
        )
    """), {
        "id": aid, "tid": tid,
        "code": data["attribute_code"], "name": data["attribute_name"],
        "en": data.get("attribute_name_en"),
        "dt": data.get("display_type", "select"),
        "act": data.get("is_active", True),
        "so": data.get("sort_order", 0),
    })
    await db.commit()
    return created(data={"id": aid}, message="تم إنشاء الخاصية ✅")


@router.put("/attributes/{attr_id}")
async def update_attribute(
    attr_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(attr_id), "tid": tid}
    # NOTE: notes و updated_at أعمدة غير موجودة في DB - مستثناة
    for col in ["attribute_code", "attribute_name", "attribute_name_en",
                "display_type", "is_active", "sort_order"]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    await db.execute(text(f"""
        UPDATE inv_item_attributes SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(attr_id)}, message="تم التحديث")


@router.delete("/attributes/{attr_id}", status_code=204)
async def delete_attribute(
    attr_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    cnt = await db.execute(
        text("SELECT COUNT(*) FROM inv_item_variant_attrs WHERE attribute_id=:aid AND tenant_id=:tid"),
        {"aid": str(attr_id), "tid": tid},
    )
    if cnt.scalar() > 0:
        raise HTTPException(400, "لا يمكن حذف خاصية مستخدمة في variants")
    # Cascade delete values
    await db.execute(
        text("DELETE FROM inv_item_attribute_values WHERE attribute_id=:aid AND tenant_id=:tid"),
        {"aid": str(attr_id), "tid": tid},
    )
    await db.execute(
        text("DELETE FROM inv_item_attributes WHERE id=:id AND tenant_id=:tid"),
        {"id": str(attr_id), "tid": tid},
    )
    await db.commit()


# ─── Attribute Values ─────────────────────────────────────────────────────
@router.get("/attributes/{attr_id}/values")
async def list_attribute_values(
    attr_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT id, attribute_id, value_code, value_name, value_name_en,
               color_hex, sort_order, is_active,
               NULL::timestamptz AS created_at,
               NULL::timestamptz AS updated_at
        FROM inv_item_attribute_values
        WHERE tenant_id=:tid AND attribute_id=:aid
        ORDER BY sort_order, value_name
    """), {"tid": tid, "aid": str(attr_id)})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/attributes/{attr_id}/values", status_code=201)
async def create_attribute_value(
    attr_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    vid = str(uuid.uuid4())
    if not data.get("value_name"):
        raise HTTPException(400, "value_name مطلوب")
    await db.execute(text("""
        INSERT INTO inv_item_attribute_values (
            id, tenant_id, attribute_id, value_code, value_name, value_name_en,
            color_hex, sort_order, is_active
        ) VALUES (
            :id, :tid, :aid, :code, :name, :en,
            :hex, :so, :act
        )
    """), {
        "id": vid, "tid": tid, "aid": str(attr_id),
        "code": data.get("value_code"),
        "name": data["value_name"],
        "en": data.get("value_name_en"),
        "hex": data.get("color_hex"),
        "so": data.get("sort_order", 0),
        "act": data.get("is_active", True),
    })
    await db.commit()
    return created(data={"id": vid}, message="تم إنشاء القيمة ✅")


@router.put("/attribute-values/{value_id}")
async def update_attribute_value(
    value_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(value_id), "tid": tid}
    # NOTE: updated_at غير موجود في DB - مستثنى
    for col in ["value_code", "value_name", "value_name_en",
                "color_hex", "sort_order", "is_active"]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    await db.execute(text(f"""
        UPDATE inv_item_attribute_values SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(value_id)}, message="تم التحديث")


@router.delete("/attribute-values/{value_id}", status_code=204)
async def delete_attribute_value(
    value_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    cnt = await db.execute(
        text("SELECT COUNT(*) FROM inv_item_variant_attrs WHERE value_id=:vid AND tenant_id=:tid"),
        {"vid": str(value_id), "tid": tid},
    )
    if cnt.scalar() > 0:
        raise HTTPException(400, "لا يمكن حذف قيمة مستخدمة في variants")
    await db.execute(
        text("DELETE FROM inv_item_attribute_values WHERE id=:id AND tenant_id=:tid"),
        {"id": str(value_id), "tid": tid},
    )
    await db.commit()
