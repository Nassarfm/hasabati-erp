"""
app/modules/inventory/routers/counts.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Physical Count (Stocktaking) Router
═══════════════════════════════════════════════════════════════════════════
جلسات الجرد الفعلي (Physical Inventory Count) — جرد كامل أو دوري أو نقطي.

Posting flow:
  1. Get session + variance lines (where actual_qty <> system_qty)
  2. Split into overage (positive variance) + shortage (negative variance)
  3. For each variance line:
       a. Adjust balance (+/-)
       b. Add or consume FIFO layer (with avg fallback)
       c. Write inv_ledger entry with reason_code + dimensions
  4. Post 2 atomic JEs (one per direction) — share AsyncSession
  5. Update session status, je references, totals

Reason-Driven Accounting:
  • Overage  → IJ + reason='count_overage'  → Cr 4901 (Other Income — Inventory Adjustment Gain)
  • Shortage → IJ - reason='count_variance' → Dr 5304 (Inventory Adjustment Loss)
  • Both inherit dimensions (branch, cost_center, project) from session header
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

from app.modules.inventory.helpers import (
    next_inv_serial,
    resolve_reason_code,
    get_tx_accounts,
    get_balance,
    adjust_balance,
    fifo_add_layer,
    fifo_consume,
    avg_consume_cost,
    add_ledger_v5,
    post_je_v5,
    get_item_metadata,
)


router = APIRouter(prefix="/inventory", tags=["inventory-counts"])


# ═══════════════════════════════════════════════════════════════════════════
# LIST COUNT SESSIONS V2
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/count-sessions-v2")
async def list_count_sessions_v2(
    status: Optional[str] = None,
    warehouse_id: Optional[uuid.UUID] = None,
    branch_code: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    search: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["s.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}

    if status:
        conds.append("s.status=:st"); params["st"] = status
    if warehouse_id:
        conds.append("s.warehouse_id=:wid"); params["wid"] = str(warehouse_id)
    if branch_code:
        conds.append("s.branch_code=:br"); params["br"] = branch_code
    if date_from:
        conds.append("s.count_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("s.count_date<=:dt"); params["dt"] = date_to
    if search:
        conds.append("(s.serial ILIKE :s OR s.notes ILIKE :s)")
        params["s"] = f"%{search}%"

    where = " AND ".join(conds)

    # Total count
    rc = await db.execute(
        text(f"SELECT COUNT(*) FROM inv_count_sessions s WHERE {where}"), params
    )
    total = rc.scalar() or 0

    # Page
    r = await db.execute(text(f"""
        SELECT s.id, s.serial, s.count_date, s.warehouse_id, s.count_type,
               s.status, s.notes, s.created_by, s.created_at,
               s.posted_by, s.posted_at,
               s.branch_code, s.cost_center_code, s.project_code,
               s.zone_id, s.location_id, s.category_id,
               s.items_counted, s.items_with_variance,
               s.total_overage_value, s.total_shortage_value,
               s.overage_je_id, s.shortage_je_id,
               s.overage_je_serial, s.shortage_je_serial,
               w.warehouse_name,
               z.name AS zone_name,
               bin.code AS location_code,
               c.category_name
        FROM inv_count_sessions s
        LEFT JOIN inv_warehouses w   ON w.id = s.warehouse_id
        LEFT JOIN inv_zones      z   ON z.id = s.zone_id
        LEFT JOIN inv_locations  bin ON bin.id = s.location_id
        LEFT JOIN inv_categories c   ON c.id = s.category_id
        WHERE {where}
        ORDER BY s.count_date DESC, s.created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    rows = [dict(r._mapping) for r in r.fetchall()]
    return ok(data={"items": rows, "total": total, "limit": limit, "offset": offset})


# ═══════════════════════════════════════════════════════════════════════════
# GET SESSION (with optional lines)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/count-sessions-v2/{sess_id}")
async def get_count_session_v2(
    sess_id: uuid.UUID,
    include_lines: bool = Query(True),
    only_variance: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT s.*, w.warehouse_name, z.name AS zone_name,
               bin.code AS location_code, c.category_name
        FROM inv_count_sessions s
        LEFT JOIN inv_warehouses w   ON w.id = s.warehouse_id
        LEFT JOIN inv_zones      z   ON z.id = s.zone_id
        LEFT JOIN inv_locations  bin ON bin.id = s.location_id
        LEFT JOIN inv_categories c   ON c.id = s.category_id
        WHERE s.id=:id AND s.tenant_id=:tid
    """), {"id": str(sess_id), "tid": tid})
    sess_row = r.fetchone()
    if not sess_row:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    sess = dict(sess_row._mapping)

    if include_lines:
        line_filter = ""
        if only_variance:
            line_filter = " AND COALESCE(l.actual_qty, l.system_qty) <> l.system_qty"
        rl = await db.execute(text(f"""
            SELECT l.*, i.item_code, i.item_name, u.uom_name,
                   bin.code AS location_code,
                   lot.lot_number
            FROM inv_count_lines l
            JOIN inv_items i ON i.id = l.item_id
            LEFT JOIN inv_uom u ON u.id = i.uom_id
            LEFT JOIN inv_locations bin ON bin.id = l.location_id
            LEFT JOIN inv_lots lot ON lot.id = l.lot_id
            WHERE l.session_id=:sid AND l.tenant_id=:tid{line_filter}
            ORDER BY COALESCE(l.line_order, 0), i.item_name
        """), {"sid": str(sess_id), "tid": tid})
        sess["lines"] = [dict(r._mapping) for r in rl.fetchall()]

    return ok(data=sess)


# ═══════════════════════════════════════════════════════════════════════════
# CREATE COUNT SESSION
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/count-sessions-v2", status_code=201)
async def create_count_session_v2(
    data: dict,
    auto_populate: bool = Query(True, description="إنشاء أسطر تلقائياً من الأرصدة"),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    إنشاء جلسة جرد. إذا auto_populate=True (افتراضي)، يتم إنشاء أسطر تلقائياً
    من الأرصدة الحالية في المستودع المحدد (مع فلترة بالـ category/zone/location
    إن وُجدت في الـ payload).
    """
    tid = str(user.tenant_id)
    count_date = date.fromisoformat(str(data.get("count_date", date.today())))
    serial = await next_inv_serial(db, "PIC", count_date, tid)
    sess_id = str(uuid.uuid4())
    wh_id = str(data["warehouse_id"])

    await db.execute(text("""
        INSERT INTO inv_count_sessions (
            id, tenant_id, serial, count_date, warehouse_id, count_type,
            status, notes, created_by,
            zone_id, location_id, category_id,
            branch_code, cost_center_code, project_code,
            reason_code_overage, reason_code_shortage
        ) VALUES (
            :id, :tid, :serial, :dt, :wid, :type,
            'open', :notes, :by,
            :zone, :loc, :cat,
            :br, :cc, :prj,
            :r_over, :r_short
        )
    """), {
        "id": sess_id, "tid": tid, "serial": serial, "dt": count_date, "wid": wh_id,
        "type": data.get("count_type", "full"),
        "notes": data.get("notes"), "by": user.email,
        "zone": str(data["zone_id"]) if data.get("zone_id") else None,
        "loc": str(data["location_id"]) if data.get("location_id") else None,
        "cat": str(data["category_id"]) if data.get("category_id") else None,
        "br": data.get("branch_code"),
        "cc": data.get("cost_center_code"),
        "prj": data.get("project_code"),
        "r_over": data.get("reason_code_overage", "count_overage"),
        "r_short": data.get("reason_code_shortage", "count_variance"),
    })

    item_count = 0
    if auto_populate:
        # Build dynamic filter
        item_conds = ["b.warehouse_id=:wid", "b.tenant_id=:tid", "i.is_active=true"]
        item_params: dict = {"wid": wh_id, "tid": tid}
        if data.get("category_id"):
            item_conds.append("i.category_id=:cat")
            item_params["cat"] = str(data["category_id"])
        if data.get("zone_id"):
            item_conds.append("EXISTS (SELECT 1 FROM inv_locations bin WHERE bin.zone_id=:zone AND bin.warehouse_id=b.warehouse_id)")
            item_params["zone"] = str(data["zone_id"])
        item_where = " AND ".join(item_conds)

        ri = await db.execute(text(f"""
            SELECT b.item_id, b.qty_on_hand, b.avg_cost
            FROM inv_balances b
            JOIN inv_items i ON i.id = b.item_id
            WHERE {item_where}
            ORDER BY i.item_name
        """), item_params)
        items = ri.fetchall()

        for idx, item in enumerate(items, start=1):
            await db.execute(text("""
                INSERT INTO inv_count_lines (
                    id, tenant_id, session_id, item_id,
                    system_qty, actual_qty, unit_cost, line_order
                ) VALUES (
                    gen_random_uuid(), :tid, :sid, :iid,
                    :sys, NULL, :uc, :ord
                )
            """), {
                "tid": tid, "sid": sess_id, "iid": str(item[0]),
                "sys": item[1], "uc": item[2] or 0, "ord": idx,
            })
        item_count = len(items)

        # Update items_counted
        await db.execute(
            text("UPDATE inv_count_sessions SET items_counted=:n WHERE id=:id"),
            {"n": item_count, "id": sess_id},
        )

    await db.commit()
    return created(
        data={"id": sess_id, "serial": serial, "item_count": item_count},
        message=f"✅ تم إنشاء جلسة الجرد {serial} — {item_count} صنف",
    )


# ═══════════════════════════════════════════════════════════════════════════
# ADD LINE (manual)
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/count-sessions-v2/{sess_id}/lines", status_code=201)
async def add_count_line(
    sess_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    # Validate session
    rs = await db.execute(
        text("SELECT status, warehouse_id FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(sess_id), "tid": tid},
    )
    srow = rs.fetchone()
    if not srow:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    if srow[0] not in ("open", "in_progress"):
        raise HTTPException(400, f"لا يمكن إضافة أصناف لجلسة بحالة '{srow[0]}'")
    wid = str(srow[1])

    item_id = str(data["item_id"])

    # Get expected qty + cost from balances
    rb = await db.execute(text("""
        SELECT qty_on_hand, avg_cost FROM inv_balances
        WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
    """), {"tid": tid, "iid": item_id, "wid": wid})
    brow = rb.fetchone()
    sys_qty = Decimal(str(brow[0])) if brow and brow[0] is not None else Decimal(0)
    avg_cost = Decimal(str(brow[1])) if brow and brow[1] is not None else Decimal(0)

    counted_qty = data.get("actual_qty") or data.get("counted_qty")
    if counted_qty is not None:
        counted_qty = Decimal(str(counted_qty))
    variance_value = ((counted_qty - sys_qty) * avg_cost) if counted_qty is not None else None

    line_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO inv_count_lines (
            id, tenant_id, session_id, item_id,
            system_qty, actual_qty, unit_cost, variance_value,
            location_id, lot_id, notes,
            counted_at, counted_by, line_order
        ) VALUES (
            :id, :tid, :sid, :iid,
            :sys, :act, :uc, :vv,
            :loc, :lot, :notes,
            CASE WHEN :act IS NOT NULL THEN NOW() ELSE NULL END,
            CASE WHEN :act IS NOT NULL THEN :by   ELSE NULL END,
            COALESCE((SELECT MAX(line_order)+1 FROM inv_count_lines WHERE session_id=:sid), 1)
        )
    """), {
        "id": line_id, "tid": tid, "sid": str(sess_id), "iid": item_id,
        "sys": sys_qty, "act": counted_qty, "uc": avg_cost, "vv": variance_value,
        "loc": str(data["location_id"]) if data.get("location_id") else None,
        "lot": str(data["lot_id"]) if data.get("lot_id") else None,
        "notes": data.get("notes"), "by": user.email,
    })
    await db.commit()
    return created(
        data={"id": line_id, "system_qty": float(sys_qty), "actual_qty": float(counted_qty) if counted_qty is not None else None},
        message="✅ تمت إضافة السطر",
    )


# ═══════════════════════════════════════════════════════════════════════════
# UPDATE COUNT LINE (the most-used endpoint)
# ═══════════════════════════════════════════════════════════════════════════
@router.put("/count-sessions-v2/{sess_id}/lines/{line_id}")
async def update_count_line(
    sess_id: uuid.UUID,
    line_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    # Validate session status
    rs = await db.execute(
        text("SELECT status FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(sess_id), "tid": tid},
    )
    srow = rs.fetchone()
    if not srow:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    if srow[0] not in ("open", "in_progress"):
        raise HTTPException(400, f"لا يمكن تعديل أسطر لجلسة بحالة '{srow[0]}'")

    rl = await db.execute(
        text("SELECT system_qty, unit_cost FROM inv_count_lines WHERE id=:id AND tenant_id=:tid"),
        {"id": str(line_id), "tid": tid},
    )
    line = rl.fetchone()
    if not line:
        raise HTTPException(404, "السطر غير موجود")

    sys_qty = Decimal(str(line[0] or 0))
    unit_cost = Decimal(str(line[1] or 0))
    actual = data.get("actual_qty", data.get("counted_qty"))
    if actual is None:
        raise HTTPException(400, "actual_qty مطلوب")
    actual_qty = Decimal(str(actual))
    variance_value = (actual_qty - sys_qty) * unit_cost

    await db.execute(text("""
        UPDATE inv_count_lines
        SET actual_qty   = :aq,
            variance_value = :vv,
            location_id  = COALESCE(:loc, location_id),
            lot_id       = COALESCE(:lot, lot_id),
            notes        = COALESCE(:notes, notes),
            counted_at   = NOW(),
            counted_by   = :by
        WHERE id=:id AND tenant_id=:tid
    """), {
        "aq": actual_qty, "vv": variance_value,
        "loc": str(data["location_id"]) if data.get("location_id") else None,
        "lot": str(data["lot_id"]) if data.get("lot_id") else None,
        "notes": data.get("notes"), "by": user.email,
        "id": str(line_id), "tid": tid,
    })

    # Mark session as in_progress
    await db.execute(text("""
        UPDATE inv_count_sessions SET status='in_progress'
        WHERE id=:id AND tenant_id=:tid AND status='open'
    """), {"id": str(sess_id), "tid": tid})

    await db.commit()
    return ok(data={
        "variance": float(actual_qty - sys_qty),
        "variance_value": float(variance_value),
    })


# ═══════════════════════════════════════════════════════════════════════════
# SCAN — Barcode workflow (auto-add or auto-increment)
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/count-sessions-v2/{sess_id}/scan")
async def scan_count_line(
    sess_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    استلام مسح باركود — يبحث عن الصنف في الجلسة:
      • إن وُجد سطر: increment actual_qty +1
      • إن لم يوجد: ينشئ سطر جديد actual_qty=1
    Payload: { "barcode": "..." } أو { "item_code": "..." } أو { "item_id": "..." }
    """
    tid = str(user.tenant_id)

    # Validate session
    rs = await db.execute(
        text("SELECT status, warehouse_id FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(sess_id), "tid": tid},
    )
    srow = rs.fetchone()
    if not srow:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    if srow[0] not in ("open", "in_progress"):
        raise HTTPException(400, f"لا يمكن المسح لجلسة بحالة '{srow[0]}'")
    wid = str(srow[1])

    # Resolve item by barcode/code/id
    item_id = None
    if data.get("item_id"):
        item_id = str(data["item_id"])
    else:
        lookup = data.get("barcode") or data.get("item_code")
        if not lookup:
            raise HTTPException(400, "يجب تمرير barcode أو item_code أو item_id")
        ri = await db.execute(text("""
            SELECT id FROM inv_items
            WHERE tenant_id=:tid AND (barcode=:b OR item_code=:b)
            LIMIT 1
        """), {"tid": tid, "b": lookup})
        irow = ri.fetchone()
        if not irow:
            raise HTTPException(404, f"الصنف غير موجود: {lookup}")
        item_id = str(irow[0])

    # Find existing line
    rl = await db.execute(text("""
        SELECT id, system_qty, actual_qty, unit_cost
        FROM inv_count_lines
        WHERE session_id=:sid AND item_id=:iid AND tenant_id=:tid
        LIMIT 1
    """), {"sid": str(sess_id), "iid": item_id, "tid": tid})
    line = rl.fetchone()

    increment = Decimal(str(data.get("qty", 1)))
    action = "incremented"
    line_id: str

    if line:
        sys_qty = Decimal(str(line[1] or 0))
        cur_act = Decimal(str(line[2] or 0))
        unit_cost = Decimal(str(line[3] or 0))
        new_act = cur_act + increment
        variance_value = (new_act - sys_qty) * unit_cost
        line_id = str(line[0])
        await db.execute(text("""
            UPDATE inv_count_lines
            SET actual_qty=:aq, variance_value=:vv, counted_at=NOW(), counted_by=:by
            WHERE id=:id AND tenant_id=:tid
        """), {
            "aq": new_act, "vv": variance_value, "by": user.email,
            "id": line_id, "tid": tid,
        })
        result = {"line_id": line_id, "actual_qty": float(new_act), "action": action}
    else:
        # Create new line — fetch sys_qty + cost from balance
        rb = await db.execute(text("""
            SELECT qty_on_hand, avg_cost FROM inv_balances
            WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
        """), {"tid": tid, "iid": item_id, "wid": wid})
        brow = rb.fetchone()
        sys_qty = Decimal(str(brow[0])) if brow and brow[0] is not None else Decimal(0)
        unit_cost = Decimal(str(brow[1])) if brow and brow[1] is not None else Decimal(0)
        variance_value = (increment - sys_qty) * unit_cost
        line_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO inv_count_lines (
                id, tenant_id, session_id, item_id,
                system_qty, actual_qty, unit_cost, variance_value,
                counted_at, counted_by, line_order
            ) VALUES (
                :id, :tid, :sid, :iid,
                :sys, :aq, :uc, :vv,
                NOW(), :by,
                COALESCE((SELECT MAX(line_order)+1 FROM inv_count_lines WHERE session_id=:sid), 1)
            )
        """), {
            "id": line_id, "tid": tid, "sid": str(sess_id), "iid": item_id,
            "sys": sys_qty, "aq": increment, "uc": unit_cost, "vv": variance_value,
            "by": user.email,
        })
        action = "created"
        result = {"line_id": line_id, "actual_qty": float(increment), "action": action}

    # Mark in_progress
    await db.execute(text("""
        UPDATE inv_count_sessions SET status='in_progress'
        WHERE id=:id AND tenant_id=:tid AND status='open'
    """), {"id": str(sess_id), "tid": tid})

    await db.commit()
    return ok(data=result, message=f"✅ {action}")


# ═══════════════════════════════════════════════════════════════════════════
# DELETE LINE
# ═══════════════════════════════════════════════════════════════════════════
@router.delete("/count-sessions-v2/{sess_id}/lines/{line_id}", status_code=204)
async def delete_count_line(
    sess_id: uuid.UUID,
    line_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    rs = await db.execute(
        text("SELECT status FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(sess_id), "tid": tid},
    )
    srow = rs.fetchone()
    if not srow:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    if srow[0] not in ("open", "in_progress"):
        raise HTTPException(400, f"لا يمكن حذف أسطر لجلسة بحالة '{srow[0]}'")

    await db.execute(
        text("DELETE FROM inv_count_lines WHERE id=:id AND tenant_id=:tid"),
        {"id": str(line_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# CANCEL SESSION
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/count-sessions-v2/{sess_id}/cancel")
async def cancel_count_session(
    sess_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(
        text("SELECT status FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(sess_id), "tid": tid},
    )
    row = r.fetchone()
    if not row:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    if row[0] == "posted":
        raise HTTPException(400, "لا يمكن إلغاء جلسة مرحَّلة — استخدم Reverse JE المرتبط")
    await db.execute(
        text("UPDATE inv_count_sessions SET status='cancelled', updated_at=NOW() WHERE id=:id AND tenant_id=:tid"),
        {"id": str(sess_id), "tid": tid},
    )
    await db.commit()
    return ok(data={"id": str(sess_id), "status": "cancelled"}, message="✅ تم الإلغاء")


# ═══════════════════════════════════════════════════════════════════════════
# POST COUNT SESSION — THE CORE
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/count-sessions-v2/{sess_id}/post")
async def post_count_session_v2(
    sess_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    ترحيل جرد فعلي:
    • لكل سطر بفارق: تحديث الرصيد + FIFO/AVG + Ledger مع reason_code
    • قيد JE واحد لصافي الزيادات (count_overage → 4901)
    • قيد JE واحد لصافي النواقص (count_variance → 5304)
    • تحديث الجلسة بالإحصائيات وأرقام القيود
    """
    tid = str(user.tenant_id)

    # Get session
    rs = await db.execute(text("""
        SELECT * FROM inv_count_sessions WHERE id=:id AND tenant_id=:tid
    """), {"id": str(sess_id), "tid": tid})
    srow = rs.fetchone()
    if not srow:
        raise HTTPException(404, "جلسة الجرد غير موجودة")
    sess = dict(srow._mapping)
    if sess["status"] == "posted":
        raise HTTPException(400, "الجلسة مرحَّلة مسبقاً")
    if sess["status"] == "cancelled":
        raise HTTPException(400, "الجلسة ملغاة")

    count_date = sess["count_date"]
    wh_id = sess["warehouse_id"]
    reason_overage = sess.get("reason_code_overage") or "count_overage"
    reason_shortage = sess.get("reason_code_shortage") or "count_variance"

    # Get variance lines
    rl = await db.execute(text("""
        SELECT l.id, l.item_id, l.system_qty, l.actual_qty, l.unit_cost,
               l.location_id, l.lot_id,
               COALESCE(i.valuation_method, i.cost_method, 'avg') AS valuation_method,
               i.is_lot_tracked, i.is_serialized
        FROM inv_count_lines l
        JOIN inv_items i ON i.id = l.item_id
        WHERE l.session_id=:sid AND l.tenant_id=:tid
          AND l.actual_qty IS NOT NULL
          AND l.actual_qty <> l.system_qty
        ORDER BY COALESCE(l.line_order, 0)
    """), {"sid": str(sess_id), "tid": tid})
    var_lines = [dict(r._mapping) for r in rl.fetchall()]

    if not var_lines:
        # No variance — just close it
        await db.execute(text("""
            UPDATE inv_count_sessions
            SET status='posted', posted_by=:by, posted_at=NOW(),
                items_with_variance=0, total_overage_value=0, total_shortage_value=0
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "id": str(sess_id), "tid": tid})
        await db.commit()
        return ok(
            data={"adjusted_lines": 0, "total_overage": 0, "total_shortage": 0},
            message="✅ تم ترحيل الجرد — لا توجد فروقات",
        )

    # Resolve reason code accounts
    overage_acc, _, _ = await resolve_reason_code(db, reason_overage, tid)
    shortage_acc, _, _ = await resolve_reason_code(db, reason_shortage, tid)

    # Default IJ accounts
    ij_dr_default, ij_cr_default, _ = await get_tx_accounts(db, "IJ", tid)

    # Inventory account = the IJ default credit (5304 etc) is for the loss/gain account
    # IJ debit  = 1140 inventory
    # IJ credit = adjustment account (5304 for loss, 4901 for gain)
    inv_acc = ij_dr_default  # 1140
    if not overage_acc:
        overage_acc = "4901"
    if not shortage_acc:
        shortage_acc = "5304"

    total_overage = Decimal(0)
    total_shortage = Decimal(0)
    items_with_var = 0

    for line in var_lines:
        items_with_var += 1
        sys_qty = Decimal(str(line["system_qty"] or 0))
        actual_qty = Decimal(str(line["actual_qty"] or 0))
        diff = actual_qty - sys_qty
        unit_cost = Decimal(str(line["unit_cost"] or 0))
        item_id = line["item_id"]
        valuation = line.get("valuation_method") or "avg"

        if diff > 0:
            # OVERAGE — increase
            qty_in = diff
            qty_out = Decimal(0)
            tc = qty_in * unit_cost
            total_overage += tc

            # Adjust balance
            await adjust_balance(
                db, item_id=item_id, warehouse_id=wh_id,
                qty_delta=qty_in, cost_delta=tc,
                tx_date=count_date, tenant_id=tid,
            )

            # FIFO add layer (or AVG just affects total_value via balance)
            if valuation == "fifo":
                await fifo_add_layer(
                    db, item_id=item_id, warehouse_id=wh_id,
                    qty=qty_in, unit_cost=unit_cost,
                    receipt_date=count_date,
                    reference_id=sess_id, tenant_id=tid,
                )

            # Ledger
            await add_ledger_v5(
                db,
                item_id=item_id, warehouse_id=wh_id,
                tx_type="IJ", tx_date=count_date,
                qty_in=qty_in, qty_out=Decimal(0),
                unit_cost=unit_cost, total_cost=tc,
                reference_id=sess_id, reference_type="COUNT",
                location_id=line.get("location_id"),
                lot_id=line.get("lot_id"),
                branch_code=sess.get("branch_code"),
                cost_center_code=sess.get("cost_center_code"),
                project_code=sess.get("project_code"),
                reason_code=reason_overage,
                tenant_id=tid,
            )
        else:
            # SHORTAGE — decrease
            qty_out = -diff
            tc = qty_out * unit_cost
            total_shortage += tc

            await adjust_balance(
                db, item_id=item_id, warehouse_id=wh_id,
                qty_delta=-qty_out, cost_delta=-tc,
                tx_date=count_date, tenant_id=tid,
            )

            # Consume FIFO (or just reduce avg balance)
            if valuation == "fifo":
                _consumed_cost, _layers = await fifo_consume(
                    db,
                    item_id=item_id, warehouse_id=wh_id,
                    qty_needed=qty_out, tenant_id=tid,
                )

            await add_ledger_v5(
                db,
                item_id=item_id, warehouse_id=wh_id,
                tx_type="IJ", tx_date=count_date,
                qty_in=Decimal(0), qty_out=qty_out,
                unit_cost=unit_cost, total_cost=tc,
                reference_id=sess_id, reference_type="COUNT",
                location_id=line.get("location_id"),
                lot_id=line.get("lot_id"),
                branch_code=sess.get("branch_code"),
                cost_center_code=sess.get("cost_center_code"),
                project_code=sess.get("project_code"),
                reason_code=reason_shortage,
                tenant_id=tid,
            )

    # Post JEs (one per direction with non-zero net)
    overage_je = None
    shortage_je = None
    if total_overage > 0:
        # Dr Inventory / Cr Other Income (Overage Gain)
        overage_je = await post_je_v5(
            db,
            user_email=user.email,
            tx_type="IJ",
            tx_date=count_date,
            description=f"جرد فعلي — زيادة {sess['serial']}",
            debit_account=inv_acc,
            credit_account=overage_acc,
            amount=total_overage,
            reference=sess["serial"],
            branch_code=sess.get("branch_code"),
            cost_center_code=sess.get("cost_center_code"),
            project_code=sess.get("project_code"),
            reason_code=reason_overage,
            source_id=sess_id,
            tenant_id=tid,
        )
    if total_shortage > 0:
        # Dr Inventory Adjustment Loss / Cr Inventory
        shortage_je = await post_je_v5(
            db,
            user_email=user.email,
            tx_type="IJ",
            tx_date=count_date,
            description=f"جرد فعلي — نقص {sess['serial']}",
            debit_account=shortage_acc,
            credit_account=inv_acc,
            amount=total_shortage,
            reference=sess["serial"],
            branch_code=sess.get("branch_code"),
            cost_center_code=sess.get("cost_center_code"),
            project_code=sess.get("project_code"),
            reason_code=reason_shortage,
            source_id=sess_id,
            tenant_id=tid,
        )

    # Update session
    await db.execute(text("""
        UPDATE inv_count_sessions
        SET status='posted',
            posted_by=:by,
            posted_at=NOW(),
            items_with_variance=:n,
            total_overage_value=:tov,
            total_shortage_value=:tsh,
            overage_je_id=:oje, overage_je_serial=:oser,
            shortage_je_id=:sje, shortage_je_serial=:sser
        WHERE id=:id AND tenant_id=:tid
    """), {
        "by": user.email,
        "n": items_with_var,
        "tov": total_overage, "tsh": total_shortage,
        "oje": overage_je["je_id"] if overage_je else None,
        "oser": overage_je["je_serial"] if overage_je else None,
        "sje": shortage_je["je_id"] if shortage_je else None,
        "sser": shortage_je["je_serial"] if shortage_je else None,
        "id": str(sess_id), "tid": tid,
    })

    await db.commit()
    return ok(
        data={
            "adjusted_lines": items_with_var,
            "total_overage": float(total_overage),
            "total_shortage": float(total_shortage),
            "overage_je": overage_je,
            "shortage_je": shortage_je,
        },
        message=f"✅ تم ترحيل الجرد — {items_with_var} صنف بفارق",
    )
