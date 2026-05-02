"""
app/modules/inventory/routers/warehouse.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Warehouse Hierarchy Router
═══════════════════════════════════════════════════════════════════════════
3-Level SAP-style hierarchy:
    Warehouse (المستودع) → Zone (المنطقة) → Location (الموقع)

Endpoints:
  /inventory/warehouses-v2       — مستودعات v5 enhanced
  /inventory/zones               — المناطق داخل المستودع
  /inventory/locations           — المواقع داخل المنطقة
  /inventory/warehouse-tree      — شجرة هرمية كاملة لجميع المستودعات
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


router = APIRouter(prefix="/inventory", tags=["inventory-warehouse"])


# ═══════════════════════════════════════════════════════════════════════════
# WAREHOUSES V2 — مع كل الحقول الجديدة (Defensive Schema-Aware)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/warehouses-v2")
async def list_warehouses_v2(
    is_active: Optional[bool] = None,
    warehouse_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة المستودعات — defensive schema-aware (لا يفشل بسبب أعمدة مفقودة)."""
    tid = str(user.tenant_id)
    
    # ─── اكتشف الأعمدة الموجودة ──────────────────────
    cols_q = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='inv_warehouses'
    """))
    existing = {row[0] for row in cols_q.fetchall()}
    
    def col(name, alias=None):
        a = alias or name
        if name in existing:
            return f"w.{name}" + (f" AS {a}" if alias else "")
        return f"NULL AS {a}"
    
    # ─── WHERE conditions (آمنة) ────────────────────
    conds = ["w.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if is_active is not None and "is_active" in existing:
        conds.append("w.is_active=:act")
        params["act"] = is_active
    if warehouse_type and "warehouse_type" in existing:
        conds.append("w.warehouse_type=:wt")
        params["wt"] = warehouse_type
    
    where = " AND ".join(conds)
    
    # ─── SELECT ديناميكي ────────────────────────────
    select_cols = [
        "w.id", "w.tenant_id",
        col("warehouse_code"),
        col("warehouse_name"),
        col("warehouse_name_en"),
        col("warehouse_type"),
        col("parent_warehouse_id"),
        col("address"),
        col("city"),
        col("country"),
        col("contact_person"),
        col("contact_phone"),
        col("contact_email"),
        col("inventory_account_code"),
        col("branch_code"),
        col("cost_center_code"),
        col("is_active"),
        col("is_default"),
        col("allow_negative_stock"),
        col("notes"),
        col("created_at"),
        col("updated_at"),
    ]
    
    # parent join (لو parent_warehouse_id موجود)
    if "parent_warehouse_id" in existing:
        select_cols.append("pw.warehouse_name AS parent_name")
        parent_join = "LEFT JOIN inv_warehouses pw ON pw.id = w.parent_warehouse_id"
    else:
        select_cols.append("NULL AS parent_name")
        parent_join = ""
    
    # zones/locations counts (defensive)
    zones_check = await db.execute(text("""
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema='public' AND table_name='inv_zones'
    """))
    if zones_check.fetchone():
        select_cols.append(
            "(SELECT COUNT(*) FROM inv_zones z WHERE z.warehouse_id=w.id AND z.is_active=true) AS zones_count"
        )
    else:
        select_cols.append("0 AS zones_count")
    
    locations_check = await db.execute(text("""
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema='public' AND table_name='inv_locations'
    """))
    if locations_check.fetchone():
        select_cols.append(
            "(SELECT COUNT(*) FROM inv_locations l WHERE l.warehouse_id=w.id AND l.is_active=true) AS locations_count"
        )
    else:
        select_cols.append("0 AS locations_count")
    
    select_clause = ",\n            ".join(select_cols)
    order_col = "w.warehouse_name" if "warehouse_name" in existing else "w.id"
    order_default = "w.is_default DESC," if "is_default" in existing else ""
    
    sql = f"""
        SELECT
            {select_clause}
        FROM inv_warehouses w
        {parent_join}
        WHERE {where}
        ORDER BY {order_default} {order_col}
    """
    
    try:
        r = await db.execute(text(sql), params)
        return ok(data=[dict(row._mapping) for row in r.fetchall()])
    except Exception as e:
        await db.rollback()
        return ok(data=[], message=f"Debug: {str(e)[:200]}")


@router.post("/warehouses-v2", status_code=201)
async def create_warehouse_v2(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إنشاء مستودع جديد — defensive."""
    tid = str(user.tenant_id)
    wid = str(uuid.uuid4())
    
    if not data.get("warehouse_code") or not data.get("warehouse_name"):
        raise HTTPException(400, "warehouse_code و warehouse_name مطلوبان")
    
    # تحقّق من الأعمدة الموجودة
    cols_q = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='inv_warehouses'
    """))
    existing = {row[0] for row in cols_q.fetchall()}
    
    # تحقّق من unique
    chk = await db.execute(
        text("SELECT 1 FROM inv_warehouses WHERE tenant_id=:tid AND warehouse_code=:c LIMIT 1"),
        {"tid": tid, "c": data["warehouse_code"]},
    )
    if chk.fetchone():
        raise HTTPException(400, f"الكود {data['warehouse_code']} موجود مسبقاً")
    
    # ابنِ INSERT ديناميكياً
    fields = ["id", "tenant_id", "warehouse_code", "warehouse_name"]
    values = [":id", ":tid", ":code", ":name"]
    params = {
        "id": wid, "tid": tid,
        "code": data["warehouse_code"],
        "name": data["warehouse_name"],
    }
    
    optional_cols = {
        "warehouse_name_en": "en",
        "warehouse_type": "wtype",
        "parent_warehouse_id": "pid",
        "address": "addr",
        "city": "city",
        "country": "country",
        "contact_person": "person",
        "contact_phone": "phone",
        "contact_email": "email",
        "inventory_account_code": "gl",
        "branch_code": "branch",
        "cost_center_code": "cc",
        "is_active": "act",
        "is_default": "default_",
        "allow_negative_stock": "neg",
        "notes": "notes",
    }
    
    for col_name, param_key in optional_cols.items():
        if col_name in existing:
            fields.append(col_name)
            values.append(f":{param_key}")
            # default values
            if col_name == "is_active":
                params[param_key] = data.get(col_name, True)
            elif col_name in ("is_default", "allow_negative_stock"):
                params[param_key] = data.get(col_name, False)
            elif col_name == "warehouse_type":
                params[param_key] = data.get(col_name, "main")
            else:
                params[param_key] = data.get(col_name)
    
    sql = f"INSERT INTO inv_warehouses ({', '.join(fields)}) VALUES ({', '.join(values)})"
    
    try:
        await db.execute(text(sql), params)
        await db.commit()
        return created(data={"id": wid}, message="تم إنشاء المستودع ✅")
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل الإنشاء: {str(e)[:200]}")


@router.delete("/warehouses-v2/{warehouse_id}", status_code=204)
async def delete_warehouse_v2(
    warehouse_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حذف مستودع. ممنوع لو فيه أرصدة."""
    tid = str(user.tenant_id)
    # تحقّق من الأرصدة
    bal_check = await db.execute(text("""
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema='public' AND table_name='inv_balances'
    """))
    if bal_check.fetchone():
        cnt = await db.execute(text("""
            SELECT COUNT(*) FROM inv_balances 
            WHERE warehouse_id=:wid AND tenant_id=:tid AND COALESCE(qty_on_hand,0) > 0
        """), {"wid": str(warehouse_id), "tid": tid})
        if cnt.scalar() > 0:
            raise HTTPException(400, "لا يمكن حذف مستودع به أرصدة")
    
    await db.execute(
        text("DELETE FROM inv_warehouses WHERE id=:id AND tenant_id=:tid"),
        {"id": str(warehouse_id), "tid": tid},
    )
    await db.commit()


@router.put("/warehouses-v2/{warehouse_id}")
async def update_warehouse_v2(
    warehouse_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(warehouse_id), "tid": tid}
    for col in [
        "warehouse_code", "warehouse_name", "warehouse_name_en",
        "warehouse_type", "parent_warehouse_id",
        "address", "city", "country",
        "contact_person", "contact_phone", "contact_email",
        "inventory_account_code", "branch_code", "cost_center_code",
        "is_active", "is_default", "allow_negative_stock", "notes",
    ]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")

    # If setting is_default=true, unset others first
    if data.get("is_default") is True:
        await db.execute(text("""
            UPDATE inv_warehouses SET is_default = false
            WHERE tenant_id=:tid AND id != :id
        """), {"tid": tid, "id": str(warehouse_id)})

    fields.append("updated_at = NOW()")
    await db.execute(text(f"""
        UPDATE inv_warehouses SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(warehouse_id)}, message="تم تحديث المستودع ✅")


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA HELPER — Defensive Column Detection
# ═══════════════════════════════════════════════════════════════════════════
async def _get_columns(db: AsyncSession, table_name: str) -> set:
    """يُرجع set بأسماء الأعمدة الموجودة فعلاً في الجدول.
    يمنع كسر الـ endpoints لو حصل تعديل في schema (إضافة/حذف عمود)."""
    r = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:tbl
    """), {"tbl": table_name})
    return {row[0] for row in r.fetchall()}


# ═══════════════════════════════════════════════════════════════════════════
# ZONES — المناطق داخل المستودع (Schema-Aware)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/zones")
async def list_zones(
    warehouse_id: Optional[uuid.UUID] = None,
    is_active: Optional[bool] = None,
    parent_zone_id: Optional[uuid.UUID] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة المناطق — defensive schema-aware (لا يفشل بسبب أعمدة مفقودة)."""
    tid = str(user.tenant_id)
    existing = await _get_columns(db, "inv_zones")

    def col(name, alias=None):
        a = alias or name
        if name in existing:
            return f"z.{name}" + (f" AS {a}" if alias else "")
        return f"NULL AS {a}"

    conds = ["z.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if warehouse_id is not None:
        conds.append("z.warehouse_id=:wid")
        params["wid"] = str(warehouse_id)
    if is_active is not None and "is_active" in existing:
        conds.append("z.is_active=:act")
        params["act"] = is_active
    if parent_zone_id is not None and "parent_zone_id" in existing:
        conds.append("z.parent_zone_id=:pzid")
        params["pzid"] = str(parent_zone_id)
    where = " AND ".join(conds)

    select_cols = [
        "z.id", "z.tenant_id", "z.warehouse_id",
        col("zone_code"),
        col("zone_name"),
        col("zone_name_en"),
        col("zone_type"),
        col("parent_zone_id"),
        col("is_active"),
        col("notes"),
        col("created_at"),
        col("updated_at"),  # NULL لو ما موجود
        "w.warehouse_name",
        "w.warehouse_code",
        "(SELECT COUNT(*) FROM inv_locations l "
        "WHERE l.zone_id=z.id AND l.tenant_id=:tid) AS locations_count",
    ]

    try:
        r = await db.execute(text(f"""
            SELECT {', '.join(select_cols)}
            FROM inv_zones z
            LEFT JOIN inv_warehouses w ON w.id = z.warehouse_id
            WHERE {where}
            ORDER BY w.warehouse_name, z.zone_name
        """), params)
        return ok(data=[dict(row._mapping) for row in r.fetchall()])
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل تحميل المناطق: {str(e)[:300]}")


@router.post("/zones", status_code=201)
async def create_zone(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إنشاء منطقة — defensive schema-aware INSERT."""
    tid = str(user.tenant_id)
    if not data.get("warehouse_id") or not data.get("zone_name"):
        raise HTTPException(400, "warehouse_id و zone_name مطلوبان")

    existing = await _get_columns(db, "inv_zones")
    zid = str(uuid.uuid4())

    # ─── ابن INSERT ديناميكي ───────────────────────────
    fields = ["id", "tenant_id", "warehouse_id"]
    values = [":id", ":tid", ":wid"]
    params = {
        "id": zid,
        "tid": tid,
        "wid": data["warehouse_id"],
    }

    candidates = {
        "zone_code": data.get("zone_code"),
        "zone_name": data["zone_name"],
        "zone_name_en": data.get("zone_name_en"),
        "zone_type": data.get("zone_type", "storage"),
        "parent_zone_id": data.get("parent_zone_id"),
        "is_active": data.get("is_active", True),
        "notes": data.get("notes"),
    }

    for col_name, val in candidates.items():
        if col_name in existing:
            fields.append(col_name)
            values.append(f":{col_name}")
            params[col_name] = val

    try:
        await db.execute(text(f"""
            INSERT INTO inv_zones ({', '.join(fields)})
            VALUES ({', '.join(values)})
        """), params)
        await db.commit()
        return created(data={"id": zid}, message="تم إنشاء المنطقة ✅")
    except Exception as e:
        await db.rollback()
        err = str(e)[:300]
        if "duplicate key" in err.lower() or "unique" in err.lower():
            raise HTTPException(400, f"رمز المنطقة موجود مسبقاً: {data.get('zone_code')}")
        raise HTTPException(400, f"فشل إنشاء المنطقة: {err}")


@router.put("/zones/{zone_id}")
async def update_zone(
    zone_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تحديث منطقة — defensive schema-aware UPDATE."""
    tid = str(user.tenant_id)
    existing = await _get_columns(db, "inv_zones")

    fields = []
    params: dict = {"id": str(zone_id), "tid": tid}

    for col in ["zone_code", "zone_name", "zone_name_en", "zone_type",
                "parent_zone_id", "is_active", "notes"]:
        if col in data and col in existing:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]

    if not fields:
        return ok(data={}, message="لا تغييرات")

    # تحديث updated_at لو الـ عمود موجود
    if "updated_at" in existing:
        fields.append("updated_at = NOW()")

    try:
        await db.execute(text(f"""
            UPDATE inv_zones SET {', '.join(fields)}
            WHERE id=:id AND tenant_id=:tid
        """), params)
        await db.commit()
        return ok(data={"id": str(zone_id)}, message="تم تحديث المنطقة ✅")
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل تحديث المنطقة: {str(e)[:300]}")


@router.delete("/zones/{zone_id}", status_code=204)
async def delete_zone(
    zone_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حذف منطقة — يمنع الحذف لو فيه مواقع مرتبطة."""
    tid = str(user.tenant_id)

    # تحقّق من عدم وجود locations مرتبطة
    cnt_q = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_locations
        WHERE zone_id=:zid AND tenant_id=:tid
    """), {"zid": str(zone_id), "tid": tid})
    cnt = cnt_q.fetchone()._mapping["c"]
    if cnt > 0:
        raise HTTPException(
            400,
            f"لا يمكن حذف المنطقة: يوجد {cnt} موقع مرتبط بها. احذفها أولاً."
        )

    try:
        await db.execute(text("""
            DELETE FROM inv_zones
            WHERE id=:id AND tenant_id=:tid
        """), {"id": str(zone_id), "tid": tid})
        await db.commit()
        return ok(data={"id": str(zone_id)}, message="تم حذف المنطقة ✅")
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل حذف المنطقة: {str(e)[:300]}")


# ═══════════════════════════════════════════════════════════════════════════
# LOCATIONS — المواقع (Schema-Aware — يدعم capacity tracking + WMS)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/locations")
async def list_locations(
    warehouse_id: Optional[uuid.UUID] = None,
    zone_id: Optional[uuid.UUID] = None,
    location_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    is_pickable: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة المواقع — defensive schema-aware."""
    tid = str(user.tenant_id)
    existing = await _get_columns(db, "inv_locations")

    def col(name, alias=None):
        a = alias or name
        if name in existing:
            return f"l.{name}" + (f" AS {a}" if alias else "")
        return f"NULL AS {a}"

    conds = ["l.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if warehouse_id is not None:
        conds.append("l.warehouse_id=:wid")
        params["wid"] = str(warehouse_id)
    if zone_id is not None:
        conds.append("l.zone_id=:zid")
        params["zid"] = str(zone_id)
    if location_type and "location_type" in existing:
        conds.append("l.location_type=:lt")
        params["lt"] = location_type
    if is_active is not None and "is_active" in existing:
        conds.append("l.is_active=:act")
        params["act"] = is_active
    if is_pickable is not None and "is_pickable" in existing:
        conds.append("l.is_pickable=:pick")
        params["pick"] = is_pickable
    where = " AND ".join(conds)

    # SELECT يشمل كل الأعمدة المعروفة (موجودة أو NULL)
    select_cols = [
        "l.id", "l.tenant_id", "l.warehouse_id", "l.zone_id",
        col("location_code"),
        col("location_name"),
        col("location_type"),
        col("barcode"),
        col("max_capacity_qty"),
        col("max_capacity_volume"),
        col("max_capacity_weight"),
        col("is_pickable"),
        col("is_active"),
        col("notes"),
        col("created_at"),
        col("updated_at"),
        # WMS classic fields (لو موجودة)
        col("aisle"),
        col("rack"),
        col("shelf"),
        col("bin_position"),
        col("zone", "zone_legacy"),  # legacy text column
        # Joined info
        "w.warehouse_name",
        "w.warehouse_code",
        "z.zone_name",
        "z.zone_code",
    ]

    try:
        r = await db.execute(text(f"""
            SELECT {', '.join(select_cols)}
            FROM inv_locations l
            LEFT JOIN inv_warehouses w ON w.id = l.warehouse_id
            LEFT JOIN inv_zones z ON z.id = l.zone_id
            WHERE {where}
            ORDER BY w.warehouse_name, z.zone_name, l.location_code
        """), params)
        return ok(data=[dict(row._mapping) for row in r.fetchall()])
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل تحميل المواقع: {str(e)[:300]}")


@router.post("/locations", status_code=201)
async def create_location(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إنشاء موقع — defensive schema-aware INSERT."""
    tid = str(user.tenant_id)
    if not data.get("warehouse_id") or not data.get("location_code"):
        raise HTTPException(400, "warehouse_id و location_code مطلوبان")

    existing = await _get_columns(db, "inv_locations")
    lid = str(uuid.uuid4())

    fields = ["id", "tenant_id", "warehouse_id"]
    values = [":id", ":tid", ":wid"]
    params = {
        "id": lid,
        "tid": tid,
        "wid": data["warehouse_id"],
    }

    candidates = {
        "zone_id": data.get("zone_id"),
        "location_code": data["location_code"],
        "location_name": data.get("location_name") or data["location_code"],
        "location_type": data.get("location_type", "storage"),
        "barcode": data.get("barcode"),
        "max_capacity_qty": data.get("max_capacity_qty"),
        "max_capacity_volume": data.get("max_capacity_volume"),
        "max_capacity_weight": data.get("max_capacity_weight"),
        "is_pickable": data.get("is_pickable", True),
        "is_active": data.get("is_active", True),
        "notes": data.get("notes"),
        # WMS classic (لو الـ schema مدّعمها)
        "aisle": data.get("aisle"),
        "rack": data.get("rack"),
        "shelf": data.get("shelf"),
        "bin_position": data.get("bin_position"),
    }

    for col_name, val in candidates.items():
        if col_name in existing:
            fields.append(col_name)
            values.append(f":{col_name}")
            params[col_name] = val

    try:
        await db.execute(text(f"""
            INSERT INTO inv_locations ({', '.join(fields)})
            VALUES ({', '.join(values)})
        """), params)
        await db.commit()
        return created(data={"id": lid}, message="تم إنشاء الموقع ✅")
    except Exception as e:
        await db.rollback()
        err = str(e)[:300]
        if "duplicate key" in err.lower() or "unique" in err.lower():
            raise HTTPException(400, f"رمز الموقع موجود مسبقاً: {data.get('location_code')}")
        raise HTTPException(400, f"فشل إنشاء الموقع: {err}")


@router.put("/locations/{location_id}")
async def update_location(
    location_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تحديث موقع — defensive schema-aware UPDATE."""
    tid = str(user.tenant_id)
    existing = await _get_columns(db, "inv_locations")

    fields = []
    params: dict = {"id": str(location_id), "tid": tid}

    updatable = [
        "zone_id", "location_code", "location_name", "location_type",
        "barcode", "max_capacity_qty", "max_capacity_volume", "max_capacity_weight",
        "is_pickable", "is_active", "notes",
        "aisle", "rack", "shelf", "bin_position",  # WMS classic
    ]
    for col in updatable:
        if col in data and col in existing:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]

    if not fields:
        return ok(data={}, message="لا تغييرات")

    if "updated_at" in existing:
        fields.append("updated_at = NOW()")

    try:
        await db.execute(text(f"""
            UPDATE inv_locations SET {', '.join(fields)}
            WHERE id=:id AND tenant_id=:tid
        """), params)
        await db.commit()
        return ok(data={"id": str(location_id)}, message="تم تحديث الموقع ✅")
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل تحديث الموقع: {str(e)[:300]}")


@router.delete("/locations/{location_id}", status_code=204)
async def delete_location(
    location_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حذف موقع — يمنع الحذف لو فيه أرصدة أو حركات."""
    tid = str(user.tenant_id)

    # تحقّق من inv_balances (لو الجدول موجود ويحوي location_id)
    bal_check_q = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='inv_balances'
          AND column_name='location_id'
    """))
    has_loc_balance = bal_check_q.fetchone() is not None

    if has_loc_balance:
        cnt_q = await db.execute(text("""
            SELECT COUNT(*) AS c FROM inv_balances
            WHERE location_id=:lid AND tenant_id=:tid
              AND COALESCE(qty_on_hand, 0) <> 0
        """), {"lid": str(location_id), "tid": tid})
        cnt = cnt_q.fetchone()._mapping["c"]
        if cnt > 0:
            raise HTTPException(
                400,
                f"لا يمكن حذف الموقع: يوجد {cnt} رصيد غير صفري. حرّكها أولاً."
            )

    try:
        await db.execute(text("""
            DELETE FROM inv_locations
            WHERE id=:id AND tenant_id=:tid
        """), {"id": str(location_id), "tid": tid})
        await db.commit()
        return ok(data={"id": str(location_id)}, message="تم حذف الموقع ✅")
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل حذف الموقع: {str(e)[:300]}")


# ═══════════════════════════════════════════════════════════════════════════
# WAREHOUSE TREE — شجرة هرمية كاملة (للعرض)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/warehouse-tree")
async def warehouse_tree(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """شجرة هرمية: مستودعات → مناطق → مواقع — مع عدد الأصناف في كل مستوى"""
    tid = str(user.tenant_id)

    rw = await db.execute(text("""
        SELECT id, warehouse_code, warehouse_name, warehouse_type,
               is_active, is_default,
               (SELECT COALESCE(SUM(qty_on_hand),0) FROM inv_balances b WHERE b.warehouse_id=w.id) AS total_qty,
               (SELECT COALESCE(SUM(total_value),0) FROM inv_balances b WHERE b.warehouse_id=w.id) AS total_value
        FROM inv_warehouses w
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY is_default DESC, warehouse_name
    """), {"tid": tid})
    warehouses = [dict(row._mapping) for row in rw.fetchall()]

    rz = await db.execute(text("""
        SELECT id, warehouse_id, zone_code, zone_name, zone_type, is_active
        FROM inv_zones
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY zone_name
    """), {"tid": tid})
    zones = [dict(row._mapping) for row in rz.fetchall()]

    rl = await db.execute(text("""
        SELECT id, warehouse_id, zone_id, location_code, location_name,
               location_type, is_active, is_pickable
        FROM inv_locations
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY location_name
    """), {"tid": tid})
    locations = [dict(row._mapping) for row in rl.fetchall()]

    # Build hierarchy
    for w in warehouses:
        w_zones = [z for z in zones if str(z["warehouse_id"]) == str(w["id"])]
        unzoned_locs = [
            l for l in locations
            if str(l["warehouse_id"]) == str(w["id"]) and l["zone_id"] is None
        ]
        for z in w_zones:
            z["locations"] = [
                l for l in locations
                if str(l["zone_id"]) == str(z["id"])
            ]
        w["zones"] = w_zones
        w["unzoned_locations"] = unzoned_locs

    return ok(data=warehouses)
