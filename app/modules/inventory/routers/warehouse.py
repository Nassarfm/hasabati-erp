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
# WAREHOUSES V2 — مع كل الحقول الجديدة
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/warehouses-v2")
async def list_warehouses_v2(
    is_active: Optional[bool] = None,
    warehouse_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["w.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if is_active is not None:
        conds.append("w.is_active=:act")
        params["act"] = is_active
    if warehouse_type:
        conds.append("w.warehouse_type=:wt")
        params["wt"] = warehouse_type

    r = await db.execute(text(f"""
        SELECT
            w.id, w.warehouse_code, w.warehouse_name, w.warehouse_name_en,
            w.warehouse_type, w.parent_warehouse_id,
            w.address, w.city, w.country,
            w.contact_person, w.contact_phone, w.contact_email,
            w.inventory_account_code, w.branch_code, w.cost_center_code,
            w.is_active, w.is_default, w.allow_negative_stock,
            w.notes, w.created_at, w.updated_at,
            pw.warehouse_name AS parent_name,
            (SELECT COUNT(*) FROM inv_zones z WHERE z.warehouse_id=w.id AND z.is_active=true) AS zones_count,
            (SELECT COUNT(*) FROM inv_locations l WHERE l.warehouse_id=w.id AND l.is_active=true) AS locations_count
        FROM inv_warehouses w
        LEFT JOIN inv_warehouses pw ON pw.id = w.parent_warehouse_id
        WHERE {' AND '.join(conds)}
        ORDER BY w.is_default DESC, w.warehouse_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


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
# ZONES — المناطق داخل المستودع (طابق، قسم، إلخ)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/zones")
async def list_zones(
    warehouse_id: Optional[uuid.UUID] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["z.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if warehouse_id is not None:
        conds.append("z.warehouse_id=:wid")
        params["wid"] = str(warehouse_id)
    if is_active is not None:
        conds.append("z.is_active=:act")
        params["act"] = is_active

    r = await db.execute(text(f"""
        SELECT
            z.id, z.warehouse_id, z.zone_code, z.zone_name, z.zone_name_en,
            z.zone_type, z.is_active, z.notes,
            w.warehouse_name, w.warehouse_code,
            (SELECT COUNT(*) FROM inv_locations l WHERE l.zone_id=z.id AND l.is_active=true) AS locations_count,
            z.created_at, z.updated_at
        FROM inv_zones z
        LEFT JOIN inv_warehouses w ON w.id = z.warehouse_id
        WHERE {' AND '.join(conds)}
        ORDER BY w.warehouse_name, z.zone_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/zones", status_code=201)
async def create_zone(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    zid = str(uuid.uuid4())
    if not data.get("warehouse_id") or not data.get("zone_name"):
        raise HTTPException(400, "warehouse_id و zone_name مطلوبان")
    await db.execute(text("""
        INSERT INTO inv_zones (
            id, tenant_id, warehouse_id, zone_code, zone_name, zone_name_en,
            zone_type, is_active, notes
        ) VALUES (
            :id, :tid, :wid, :code, :name, :en,
            :type, :act, :notes
        )
    """), {
        "id": zid, "tid": tid,
        "wid": data["warehouse_id"],
        "code": data.get("zone_code"),
        "name": data["zone_name"],
        "en": data.get("zone_name_en"),
        "type": data.get("zone_type", "storage"),
        "act": data.get("is_active", True),
        "notes": data.get("notes"),
    })
    await db.commit()
    return created(data={"id": zid}, message="تم إنشاء المنطقة ✅")


@router.put("/zones/{zone_id}")
async def update_zone(
    zone_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(zone_id), "tid": tid}
    for col in ["zone_code", "zone_name", "zone_name_en",
                "zone_type", "is_active", "notes"]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    fields.append("updated_at = NOW()")
    await db.execute(text(f"""
        UPDATE inv_zones SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(zone_id)}, message="تم التحديث")


@router.delete("/zones/{zone_id}", status_code=204)
async def delete_zone(
    zone_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    cnt = await db.execute(
        text("SELECT COUNT(*) FROM inv_locations WHERE zone_id=:zid AND tenant_id=:tid"),
        {"zid": str(zone_id), "tid": tid},
    )
    if cnt.scalar() > 0:
        raise HTTPException(400, "لا يمكن حذف منطقة تحتوي مواقع")
    await db.execute(
        text("DELETE FROM inv_zones WHERE id=:id AND tenant_id=:tid"),
        {"id": str(zone_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# LOCATIONS — المواقع (kept under inv_locations after rename from inv_bins)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/locations")
async def list_locations(
    warehouse_id: Optional[uuid.UUID] = None,
    zone_id: Optional[uuid.UUID] = None,
    location_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if warehouse_id is not None:
        conds.append("l.warehouse_id=:wid")
        params["wid"] = str(warehouse_id)
    if zone_id is not None:
        conds.append("l.zone_id=:zid")
        params["zid"] = str(zone_id)
    if location_type:
        conds.append("l.location_type=:lt")
        params["lt"] = location_type
    if is_active is not None:
        conds.append("l.is_active=:act")
        params["act"] = is_active

    r = await db.execute(text(f"""
        SELECT
            l.id, l.warehouse_id, l.zone_id,
            l.location_code, l.location_name,
            l.location_type, l.aisle, l.rack, l.shelf, l.bin_position,
            l.is_active, l.is_pickable, l.notes,
            w.warehouse_name, w.warehouse_code,
            z.zone_name, z.zone_code,
            l.created_at, l.updated_at
        FROM inv_locations l
        LEFT JOIN inv_warehouses w ON w.id = l.warehouse_id
        LEFT JOIN inv_zones z ON z.id = l.zone_id
        WHERE {' AND '.join(conds)}
        ORDER BY w.warehouse_name, z.zone_name, l.location_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/locations", status_code=201)
async def create_location(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    lid = str(uuid.uuid4())
    if not data.get("warehouse_id") or not data.get("location_code"):
        raise HTTPException(400, "warehouse_id و location_code مطلوبان")
    await db.execute(text("""
        INSERT INTO inv_locations (
            id, tenant_id, warehouse_id, zone_id,
            location_code, location_name,
            location_type, aisle, rack, shelf, bin_position,
            is_active, is_pickable, notes
        ) VALUES (
            :id, :tid, :wid, :zid,
            :code, :name,
            :type, :aisle, :rack, :shelf, :bin,
            :act, :pick, :notes
        )
    """), {
        "id": lid, "tid": tid,
        "wid": data["warehouse_id"],
        "zid": data.get("zone_id"),
        "code": data["location_code"],
        "name": data.get("location_name") or data["location_code"],
        "type": data.get("location_type", "storage"),
        "aisle": data.get("aisle"),
        "rack": data.get("rack"),
        "shelf": data.get("shelf"),
        "bin": data.get("bin_position"),
        "act": data.get("is_active", True),
        "pick": data.get("is_pickable", True),
        "notes": data.get("notes"),
    })
    await db.commit()
    return created(data={"id": lid}, message="تم إنشاء الموقع ✅")


@router.put("/locations/{location_id}")
async def update_location(
    location_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fields = []
    params: dict = {"id": str(location_id), "tid": tid}
    for col in [
        "zone_id", "location_code", "location_name",
        "location_type", "aisle", "rack", "shelf", "bin_position",
        "is_active", "is_pickable", "notes",
    ]:
        if col in data:
            fields.append(f"{col} = :{col}")
            params[col] = data[col]
    if not fields:
        return ok(data={}, message="لا تغييرات")
    fields.append("updated_at = NOW()")
    await db.execute(text(f"""
        UPDATE inv_locations SET {', '.join(fields)}
        WHERE id=:id AND tenant_id=:tid
    """), params)
    await db.commit()
    return ok(data={"id": str(location_id)}, message="تم التحديث")


@router.delete("/locations/{location_id}", status_code=204)
async def delete_location(
    location_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(
        text("DELETE FROM inv_locations WHERE id=:id AND tenant_id=:tid"),
        {"id": str(location_id), "tid": tid},
    )
    await db.commit()


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
