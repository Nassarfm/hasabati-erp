"""
app/modules/inventory/routers/transactions.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Transactions Router (THE CORE)
═══════════════════════════════════════════════════════════════════════════
كل عمليات حركات المخزون: GRN/GIN/GIT/IJ/SCRAP/RETURN_IN/RETURN_OUT

Layer 1 (header): party_id + party_role + responsible_user + dimensions + reason
Layer 2 (lines):  party_id + party_role + lot/serial overrides
Layer 3 (ledger): full snapshot of party + dimensions + reason

Posting flow:
  1. Resolve party & accounts (Smart Hybrid)
  2. Update inv_balances
  3. Add/consume FIFO layers (or use AVG)
  4. Update inv_lots / inv_serials (if tracking enabled)
  5. Add inv_ledger entries (Universal Subsidiary Ledger)
  6. Post JE via PostingEngine (same AsyncSession ⇒ atomicity)
  7. Update tx status to 'posted'
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

from app.modules.inventory.helpers import (
    resolve_party, resolve_reason_code, get_tx_accounts,
    get_balance, adjust_balance,
    fifo_add_layer, fifo_consume, avg_consume_cost,
    add_ledger_v5, post_je_v5,
    next_inv_serial, get_or_create_lot, adjust_lot_qty,
    get_or_create_serial, update_serial_status,
    get_item_metadata,
)


router = APIRouter(prefix="/inventory", tags=["inventory-transactions"])


# ═══════════════════════════════════════════════════════════════════════════
# LIST TRANSACTIONS V2
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/transactions-v2")
async def list_transactions_v2(
    tx_type: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    warehouse_id: Optional[uuid.UUID] = None,
    party_id: Optional[uuid.UUID] = None,
    branch_code: Optional[str] = None,
    cost_center_code: Optional[str] = None,
    project_code: Optional[str] = None,
    reason_code: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["t.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}

    if tx_type:
        conds.append("t.tx_type=:tt"); params["tt"] = tx_type
    if status:
        conds.append("t.status=:st"); params["st"] = status
    if date_from:
        conds.append("t.tx_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("t.tx_date<=:dt"); params["dt"] = date_to
    if warehouse_id:
        conds.append("(t.from_warehouse_id=:wid OR t.to_warehouse_id=:wid)")
        params["wid"] = str(warehouse_id)
    if party_id:
        conds.append("t.party_id=:pid"); params["pid"] = str(party_id)
    if branch_code:
        conds.append("t.branch_code=:br"); params["br"] = branch_code
    if cost_center_code:
        conds.append("t.cost_center_code=:cc"); params["cc"] = cost_center_code
    if project_code:
        conds.append("t.project_code=:prj"); params["prj"] = project_code
    if reason_code:
        conds.append("t.reason_code=:rc"); params["rc"] = reason_code
    if search:
        conds.append("(t.serial ILIKE :s OR t.reference ILIKE :s OR t.description ILIKE :s)")
        params["s"] = f"%{search}%"

    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM inv_transactions t WHERE {where}"), params)
    total = cnt.scalar() or 0

    r = await db.execute(text(f"""
        SELECT
            t.id, t.serial, t.tx_type, t.tx_date, t.status,
            t.from_warehouse_id, t.to_warehouse_id,
            t.party_id, t.party_role, t.party_name_snapshot,
            t.responsible_user_id, t.approved_by_user_id,
            t.branch_code, t.cost_center_code, t.project_code, t.reason_code,
            t.reference, t.description, t.notes,
            t.total_qty, t.total_cost,
            t.je_id, t.je_serial, t.posted_at, t.posted_by,
            t.reverses_id,
            t.created_by, t.created_at, t.updated_at,
            fw.warehouse_name AS from_warehouse_name,
            tw.warehouse_name AS to_warehouse_name,
            p.name AS party_name,
            rc.reason_name AS reason_name
        FROM inv_transactions t
        LEFT JOIN inv_warehouses fw ON fw.id = t.from_warehouse_id
        LEFT JOIN inv_warehouses tw ON tw.id = t.to_warehouse_id
        LEFT JOIN parties p ON p.id = t.party_id
        LEFT JOIN inv_reason_codes rc ON rc.reason_code = t.reason_code AND rc.tenant_id=:tid
        WHERE {where}
        ORDER BY t.tx_date DESC, t.created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    items = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"total": total, "items": items, "limit": limit, "offset": offset})


# ═══════════════════════════════════════════════════════════════════════════
# GET TRANSACTION V2 — مع كل الـ lines + meta
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/transactions-v2/{tx_id}")
async def get_transaction_v2(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
            t.*,
            fw.warehouse_name AS from_warehouse_name,
            tw.warehouse_name AS to_warehouse_name,
            p.name AS party_name,
            rc.reason_name AS reason_name
        FROM inv_transactions t
        LEFT JOIN inv_warehouses fw ON fw.id = t.from_warehouse_id
        LEFT JOIN inv_warehouses tw ON tw.id = t.to_warehouse_id
        LEFT JOIN parties p ON p.id = t.party_id
        LEFT JOIN inv_reason_codes rc ON rc.reason_code = t.reason_code AND rc.tenant_id=:tid
        WHERE t.id=:id AND t.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    row = r.fetchone()
    if not row:
        raise HTTPException(404, "المستند غير موجود")
    tx = dict(row._mapping)

    rl = await db.execute(text("""
        SELECT
            l.*, i.item_code, i.item_name, i.is_serialized, i.is_lot_tracked,
            u.uom_code, u.uom_name,
            p.name AS line_party_name,
            lot.lot_number AS lot_number_resolved,
            ser.serial_number AS serial_number_resolved
        FROM inv_transaction_lines l
        LEFT JOIN inv_items i ON i.id = l.item_id
        LEFT JOIN inv_uom u ON u.id = l.uom_id
        LEFT JOIN parties p ON p.id = l.party_id
        LEFT JOIN inv_lots lot ON lot.id = l.lot_id
        LEFT JOIN inv_serials ser ON ser.id = l.serial_id
        WHERE l.tx_id=:tid_id
        ORDER BY l.created_at
    """), {"tid_id": str(tx_id)})
    tx["lines"] = [dict(row._mapping) for row in rl.fetchall()]
    return ok(data=tx)


# ═══════════════════════════════════════════════════════════════════════════
# CREATE TRANSACTION V2 — draft فقط (لا ترحيل)
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/transactions-v2", status_code=201)
async def create_transaction_v2(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    if not data.get("tx_type") or not data.get("tx_date"):
        raise HTTPException(400, "tx_type و tx_date مطلوبان")

    tx_type = data["tx_type"]
    tx_date = date.fromisoformat(str(data["tx_date"]))
    serial = await next_inv_serial(db, tx_type, tx_date, tid)
    tx_id = str(uuid.uuid4())

    lines = data.get("lines", [])
    if not lines:
        raise HTTPException(400, "يجب إضافة سطر واحد على الأقل")

    # Resolve party_name_snapshot
    party_id = data.get("party_id")
    party_name = data.get("party_name_snapshot")
    if party_id and not party_name:
        rp = await db.execute(
            text("SELECT name FROM parties WHERE id=:pid AND tenant_id=:tid"),
            {"pid": party_id, "tid": tid},
        )
        rr = rp.fetchone()
        if rr: party_name = rr[0]

    total_qty = sum(Decimal(str(l.get("qty", 0))) for l in lines)
    total_cost = sum(
        Decimal(str(l.get("qty", 0))) * Decimal(str(l.get("unit_cost", 0)))
        for l in lines
    )

    import json as _json
    stake = data.get("stakeholders") or {}
    stake_json = _json.dumps(stake, ensure_ascii=False) if stake else None

    await db.execute(text("""
        INSERT INTO inv_transactions (
            id, tenant_id, serial, tx_type, tx_date, status,
            from_warehouse_id, to_warehouse_id,
            party_id, party_role, party_name_snapshot,
            responsible_user_id, approved_by_user_id,
            branch_code, cost_center_code, project_code, reason_code,
            reference, description, notes,
            total_qty, total_cost,
            stakeholders, extra_data,
            created_by
        ) VALUES (
            :id, :tid, :serial, :tt, :dt, 'draft',
            :fw, :tw,
            :pid, :prole, :pname,
            :resp, :appr,
            :br, :cc, :prj, :rc,
            :ref, :desc, :notes,
            :tq, :tc,
            CAST(:stake AS JSONB), '{}',
            :by
        )
    """), {
        "id": tx_id, "tid": tid, "serial": serial,
        "tt": tx_type, "dt": tx_date,
        "fw": data.get("from_warehouse_id"),
        "tw": data.get("to_warehouse_id"),
        "pid": party_id,
        "prole": data.get("party_role"),
        "pname": party_name,
        "resp": data.get("responsible_user_id"),
        "appr": data.get("approved_by_user_id"),
        "br": data.get("branch_code"),
        "cc": data.get("cost_center_code"),
        "prj": data.get("project_code"),
        "rc": data.get("reason_code"),
        "ref": data.get("reference"),
        "desc": data.get("description", ""),
        "notes": data.get("notes"),
        "tq": total_qty, "tc": total_cost,
        "stake": stake_json,
        "by": user.email,
    })

    # Insert lines
    # ⚠️ Schema fix (2026-05-03): inv_transaction_lines uses tx_id (not transaction_id),
    # has no line_order column, and uses from_location_id/to_location_id (not location_id).
    for i, line in enumerate(lines):
        line_id = str(uuid.uuid4())
        # Determine location based on tx_type direction
        from_loc = line.get("from_location_id") or (
            line.get("location_id") if tx_type in ("GIN", "GDN", "IT", "IJ", "SCRAP", "RETURN_IN") else None
        )
        to_loc = line.get("to_location_id") or (
            line.get("location_id") if tx_type in ("GRN", "RETURN_OUT") else None
        )
        try:
            await db.execute(text("""
                INSERT INTO inv_transaction_lines (
                    id, tenant_id, tx_id,
                    item_id, item_code, item_name,
                    uom_id, uom_name,
                    qty, unit_cost, total_cost,
                    lot_id, lot_number, expiry_date,
                    serial_id, serial_number,
                    from_location_id, to_location_id,
                    party_id, party_role,
                    notes
                ) VALUES (
                    :id, :tid, :tx_id,
                    :iid, :icode, :iname,
                    :uom, :uname,
                    :qty, :uc, :tc,
                    :lot_id, :lot, :exp,
                    :ser_id, :ser,
                    :from_loc, :to_loc,
                    :pid, :prole,
                    :notes
                )
            """), {
                "id": line_id, "tid": tid, "tx_id": tx_id,
                "iid": line["item_id"],
                "icode": line.get("item_code", ""),
                "iname": line.get("item_name", ""),
                "uom": line.get("uom_id"),
                "uname": line.get("uom_name"),
                "qty": Decimal(str(line["qty"])),
                "uc": Decimal(str(line.get("unit_cost", 0))),
                "tc": Decimal(str(line["qty"])) * Decimal(str(line.get("unit_cost", 0))),
                "lot_id": line.get("lot_id"),
                "lot": line.get("lot_number"),
                "exp": line.get("expiry_date"),
                "ser_id": line.get("serial_id"),
                "ser": line.get("serial_number"),
                "from_loc": from_loc,
                "to_loc": to_loc,
                "pid": line.get("party_id"),
                "prole": line.get("party_role"),
                "notes": line.get("notes"),
            })
        except Exception as e:
            await db.rollback()
            raise HTTPException(
                status_code=400,
                detail=f"فشل إنشاء سطر {i+1}: {str(e)[:300]}"
            )

    # If item_code/item_name missing, fill them from inv_items
    await db.execute(text("""
        UPDATE inv_transaction_lines l
        SET item_code = COALESCE(NULLIF(l.item_code, ''), i.item_code),
            item_name = COALESCE(NULLIF(l.item_name, ''), i.item_name)
        FROM inv_items i
        WHERE l.tx_id = :tx_id AND l.item_id = i.id
    """), {"tx_id": tx_id})

    await db.commit()
    return created(
        data={"id": tx_id, "serial": serial},
        message=f"تم إنشاء {serial} ✅ — يحتاج ترحيل",
    )


# ═══════════════════════════════════════════════════════════════════════════
# UPDATE TRANSACTION V2 — تعديل المسودات فقط (added 2026-05-03)
# ═══════════════════════════════════════════════════════════════════════════
@router.put("/transactions-v2/{tx_id}")
async def update_transaction_v2(
    tx_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تعديل حركة مسودة فقط (لا يمكن تعديل المُرحَّل)"""
    tid = str(user.tenant_id)

    # Check status
    r = await db.execute(text("""
        SELECT status FROM inv_transactions
        WHERE id=:id AND tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    row = r.fetchone()
    if not row:
        raise HTTPException(404, f"الحركة غير موجودة")
    if row[0] != "draft":
        raise HTTPException(400, f"لا يمكن تعديل حركة بحالة '{row[0]}' — فقط المسودات قابلة للتعديل")

    try:
        # Update header
        update_fields = []
        params = {"id": str(tx_id), "tid": tid}
        allowed = [
            "tx_date", "from_warehouse_id", "to_warehouse_id",
            "party_id", "party_role", "branch_code", "cost_center_code",
            "project_code", "reason_code", "reference", "description", "notes",
        ]
        for f in allowed:
            if f in data:
                update_fields.append(f"{f} = :{f}")
                params[f] = data.get(f)

        if update_fields:
            sql = f"UPDATE inv_transactions SET {', '.join(update_fields)}, updated_at = NOW() WHERE id = :id AND tenant_id = :tid"
            await db.execute(text(sql), params)

        # Replace lines if provided
        lines = data.get("lines")
        if lines is not None:
            # Delete old lines
            await db.execute(text("""
                DELETE FROM inv_transaction_lines WHERE tx_id=:id
            """), {"id": str(tx_id)})

            tx_type = data.get("tx_type", "GRN")
            for i, line in enumerate(lines):
                from_loc = line.get("from_location_id") or (
                    line.get("location_id") if tx_type in ("GIN", "GDN", "IT", "IJ", "SCRAP", "RETURN_IN") else None
                )
                to_loc = line.get("to_location_id") or (
                    line.get("location_id") if tx_type in ("GRN", "RETURN_OUT") else None
                )
                await db.execute(text("""
                    INSERT INTO inv_transaction_lines (
                        id, tenant_id, tx_id,
                        item_id, item_code, item_name,
                        uom_id, uom_name,
                        qty, unit_cost, total_cost,
                        lot_id, lot_number, expiry_date,
                        serial_id, serial_number,
                        from_location_id, to_location_id,
                        party_id, party_role,
                        notes
                    ) VALUES (
                        gen_random_uuid(), :tid, :tx_id,
                        :iid, :icode, :iname,
                        :uom, :uname,
                        :qty, :uc, :tc,
                        :lot_id, :lot, :exp,
                        :ser_id, :ser,
                        :from_loc, :to_loc,
                        :pid, :prole,
                        :notes
                    )
                """), {
                    "tid": tid, "tx_id": str(tx_id),
                    "iid": line["item_id"],
                    "icode": line.get("item_code", ""),
                    "iname": line.get("item_name", ""),
                    "uom": line.get("uom_id"),
                    "uname": line.get("uom_name"),
                    "qty": Decimal(str(line["qty"])),
                    "uc": Decimal(str(line.get("unit_cost", 0))),
                    "tc": Decimal(str(line["qty"])) * Decimal(str(line.get("unit_cost", 0))),
                    "lot_id": line.get("lot_id"),
                    "lot": line.get("lot_number"),
                    "exp": line.get("expiry_date"),
                    "ser_id": line.get("serial_id"),
                    "ser": line.get("serial_number"),
                    "from_loc": from_loc,
                    "to_loc": to_loc,
                    "pid": line.get("party_id"),
                    "prole": line.get("party_role"),
                    "notes": line.get("notes"),
                })

            # Recalculate totals
            await db.execute(text("""
                UPDATE inv_transactions
                SET total_qty = COALESCE((SELECT SUM(qty) FROM inv_transaction_lines WHERE tx_id=:id), 0),
                    total_cost = COALESCE((SELECT SUM(total_cost) FROM inv_transaction_lines WHERE tx_id=:id), 0)
                WHERE id=:id
            """), {"id": str(tx_id)})

            # Fill item_code/item_name from inv_items
            await db.execute(text("""
                UPDATE inv_transaction_lines l
                SET item_code = COALESCE(NULLIF(l.item_code, ''), i.item_code),
                    item_name = COALESCE(NULLIF(l.item_name, ''), i.item_name)
                FROM inv_items i
                WHERE l.tx_id = :id AND l.item_id = i.id
            """), {"id": str(tx_id)})

        # If auto_post requested
        if data.get("auto_post"):
            await db.commit()
            return await post_transaction_v2(tx_id, db, user)

        await db.commit()

        # Get serial for response
        r2 = await db.execute(text("""
            SELECT serial FROM inv_transactions WHERE id=:id
        """), {"id": str(tx_id)})
        serial_row = r2.fetchone()
        serial = serial_row[0] if serial_row else ""

        return ok(
            data={"id": str(tx_id), "serial": serial},
            message=f"تم تحديث {serial} ✅",
        )
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"فشل التحديث: {str(e)[:300]}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# DELETE/CANCEL TRANSACTION
# ═══════════════════════════════════════════════════════════════════════════
@router.delete("/transactions-v2/{tx_id}", status_code=204)
async def cancel_transaction_v2(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(
        text("SELECT status FROM inv_transactions WHERE id=:id AND tenant_id=:tid"),
        {"id": str(tx_id), "tid": tid},
    )
    row = r.fetchone()
    if not row:
        raise HTTPException(404, "المستند غير موجود")
    if row[0] not in ("draft", "submitted"):
        raise HTTPException(400, f"لا يمكن إلغاء المستند بحالة '{row[0]}' — استخدم Reverse")

    await db.execute(
        text("UPDATE inv_transactions SET status='cancelled', updated_at=NOW() WHERE id=:id AND tenant_id=:tid"),
        {"id": str(tx_id), "tid": tid},
    )
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# POST TRANSACTION V2 — العمل الحقيقي
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/transactions-v2/{tx_id}/post")
async def post_transaction_v2(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    ترحيل حركة مخزون مع كل تأثيراتها:
    - Balances
    - FIFO Layers / AVG
    - Lots (إذا الصنف is_lot_tracked)
    - Serials (إذا is_serialized)
    - Ledger (Layer 3 — Universal Subsidiary)
    - JE (مع party + dimensions + reason)
    """
    tid = str(user.tenant_id)

    # Get tx header
    rt = await db.execute(text("""
        SELECT * FROM inv_transactions WHERE id=:id AND tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    row = rt.fetchone()
    if not row:
        raise HTTPException(404, "المستند غير موجود")
    tx = dict(row._mapping)

    if tx["status"] != "draft":
        raise HTTPException(400, f"المستند بحالة '{tx['status']}' — لا يمكن ترحيله")

    tx_type = tx["tx_type"]
    tx_date = tx["tx_date"]

    # Get lines
    rl = await db.execute(text("""
        SELECT l.*, i.is_lot_tracked, i.is_serialized, i.is_expiry_tracked,
               COALESCE(i.valuation_method, i.cost_method, 'avg') AS valuation_method
        FROM inv_transaction_lines l
        LEFT JOIN inv_items i ON i.id = l.item_id
        WHERE l.tx_id=:tx_id
        ORDER BY l.created_at
    """), {"tx_id": str(tx_id)})
    lines = [dict(r._mapping) for r in rl.fetchall()]

    if not lines:
        raise HTTPException(400, "لا توجد أسطر للترحيل")

    # Resolve accounts
    debit_acc, credit_acc, _desc = await get_tx_accounts(db, tx_type, tid)

    # Resolve reason — may override accounts
    reason_acc, reason_name, reason_is_increase = await resolve_reason_code(
        db, tx["reason_code"], tid
    )

    # For IJ specifically, reason_code may flip direction & change account
    if tx_type == "IJ" and reason_acc:
        if reason_is_increase:
            # IJ+ — increase: Dr Inventory / Cr Other Income (or reason expense)
            # Use defaults for IJ+
            debit_acc, credit_acc, _ = await get_tx_accounts(db, "IJ+", tid)
            credit_acc = reason_acc  # override credit with reason account
        else:
            # IJ- — decrease: Dr Reason Expense / Cr Inventory
            debit_acc = reason_acc

    # SCRAP always uses reason expense
    if tx_type == "SCRAP" and reason_acc:
        debit_acc = reason_acc

    grand_total_cost = Decimal(0)

    # Process each line
    for line in lines:
        qty = Decimal(str(line["qty"]))
        unit_cost = Decimal(str(line["unit_cost"] or 0))
        item_id = line["item_id"]
        valuation = line["valuation_method"] or "avg"

        # Resolve party at line level (Smart Hybrid)
        line_party_id, line_party_role, line_party_name = await resolve_party(
            db,
            line_party_id=line.get("party_id"),
            line_party_role=line.get("party_role"),
            tx_party_id=tx.get("party_id"),
            tx_party_role=tx.get("party_role"),
            item_id=item_id,
            tenant_id=tid,
        )

        # ─── GRN / RETURN_IN — Inbound (increase stock) ────────────────────
        if tx_type in ("GRN", "RETURN_IN", "OPENING"):
            wh_id = tx["to_warehouse_id"]
            if not wh_id:
                raise HTTPException(400, f"to_warehouse_id مطلوب لحركة {tx_type}")

            line_cost = qty * unit_cost
            grand_total_cost += line_cost

            # Lot creation (if tracked)
            lot_id = line.get("lot_id")
            if line["is_lot_tracked"] and not lot_id and line.get("lot_number"):
                lot_id = await get_or_create_lot(
                    db,
                    item_id=item_id,
                    lot_number=line["lot_number"],
                    expiry_date=line.get("expiry_date"),
                    supplier_party_id=line_party_id,
                    tenant_id=tid,
                )
                # Link back
                await db.execute(
                    text("UPDATE inv_transaction_lines SET lot_id=:lid WHERE id=:id"),
                    {"lid": str(lot_id), "id": str(line["id"])},
                )
            if lot_id:
                await adjust_lot_qty(db, lot_id, qty, tid)

            # FIFO layer
            await fifo_add_layer(
                db,
                item_id=item_id, warehouse_id=wh_id,
                receipt_date=tx_date, qty=qty, unit_cost=unit_cost,
                reference_id=tx_id, lot_id=lot_id, tenant_id=tid,
            )

            # Balance
            await adjust_balance(db, item_id, wh_id, qty, line_cost, tx_date, tid)

            # Serial (if tracked)
            ser_id = line.get("serial_id")
            if line["is_serialized"] and line.get("serial_number"):
                ser_id = await get_or_create_serial(
                    db, item_id=item_id, serial_number=line["serial_number"],
                    warehouse_id=wh_id, tenant_id=tid,
                )
                await update_serial_status(db, ser_id, "available", wh_id, tid)
                await db.execute(
                    text("UPDATE inv_transaction_lines SET serial_id=:sid WHERE id=:id"),
                    {"sid": str(ser_id), "id": str(line["id"])},
                )

            # Ledger
            await add_ledger_v5(
                db,
                item_id=item_id, warehouse_id=wh_id,
                tx_type=tx_type, tx_date=tx_date,
                qty_in=qty, qty_out=Decimal(0),
                unit_cost=unit_cost, total_cost=line_cost,
                reference_id=tx_id,
                party_id=line_party_id, party_role=line_party_role,
                party_name_snapshot=line_party_name,
                branch_code=tx.get("branch_code"),
                cost_center_code=tx.get("cost_center_code"),
                project_code=tx.get("project_code"),
                reason_code=tx.get("reason_code"),
                lot_id=lot_id, lot_number=line.get("lot_number"),
                serial_id=ser_id, serial_number=line.get("serial_number"),
                location_id=line.get("location_id"),
                tenant_id=tid,
            )

        # ─── GIN / SCRAP / RETURN_OUT — Outbound (decrease stock) ──────────
        elif tx_type in ("GIN", "SCRAP", "RETURN_OUT"):
            wh_id = tx["from_warehouse_id"]
            if not wh_id:
                raise HTTPException(400, f"from_warehouse_id مطلوب لحركة {tx_type}")

            # Cost via FIFO or AVG
            if valuation == "fifo":
                line_cost, _consumed = await fifo_consume(db, item_id, wh_id, qty, tid)
            else:
                line_cost = await avg_consume_cost(db, item_id, wh_id, qty, tid)

            unit_c = (line_cost / qty) if qty > 0 else Decimal(0)
            grand_total_cost += line_cost

            # Lot decrement
            lot_id = line.get("lot_id")
            if lot_id:
                await adjust_lot_qty(db, lot_id, -qty, tid)

            # Serial mark sold/consumed
            ser_id = line.get("serial_id")
            if ser_id:
                ser_status = "sold" if tx_type == "GIN" else (
                    "scrapped" if tx_type == "SCRAP" else "returned"
                )
                await update_serial_status(db, ser_id, ser_status, None, tid)

            # Balance
            await adjust_balance(db, item_id, wh_id, -qty, -line_cost, tx_date, tid)

            # Ledger
            await add_ledger_v5(
                db,
                item_id=item_id, warehouse_id=wh_id,
                tx_type=tx_type, tx_date=tx_date,
                qty_in=Decimal(0), qty_out=qty,
                unit_cost=unit_c, total_cost=line_cost,
                reference_id=tx_id,
                party_id=line_party_id, party_role=line_party_role,
                party_name_snapshot=line_party_name,
                branch_code=tx.get("branch_code"),
                cost_center_code=tx.get("cost_center_code"),
                project_code=tx.get("project_code"),
                reason_code=tx.get("reason_code"),
                lot_id=lot_id, lot_number=line.get("lot_number"),
                serial_id=ser_id, serial_number=line.get("serial_number"),
                location_id=line.get("location_id"),
                tenant_id=tid,
            )

        # ─── GIT — Internal Transfer ───────────────────────────────────────
        elif tx_type == "GIT":
            wh_from = tx["from_warehouse_id"]
            wh_to = tx["to_warehouse_id"]
            if not wh_from or not wh_to:
                raise HTTPException(400, "from_warehouse_id و to_warehouse_id مطلوبان للتحويل")
            if wh_from == wh_to:
                raise HTTPException(400, "لا يمكن التحويل داخل نفس المستودع")

            # Cost
            if valuation == "fifo":
                line_cost, _ = await fifo_consume(db, item_id, wh_from, qty, tid)
            else:
                line_cost = await avg_consume_cost(db, item_id, wh_from, qty, tid)
            unit_c = (line_cost / qty) if qty > 0 else Decimal(0)
            grand_total_cost += line_cost

            # FROM: decrease
            await adjust_balance(db, item_id, wh_from, -qty, -line_cost, tx_date, tid)
            # TO: increase + new FIFO layer
            await adjust_balance(db, item_id, wh_to, qty, line_cost, tx_date, tid)
            await fifo_add_layer(
                db, item_id=item_id, warehouse_id=wh_to,
                receipt_date=tx_date, qty=qty, unit_cost=unit_c,
                reference_id=tx_id, tenant_id=tid,
            )

            # Lot transfer
            lot_id = line.get("lot_id")
            # serial transfer
            ser_id = line.get("serial_id")
            if ser_id:
                await update_serial_status(db, ser_id, "available", wh_to, tid)

            # Ledger — TWO entries (out + in)
            await add_ledger_v5(
                db, item_id=item_id, warehouse_id=wh_from,
                tx_type=tx_type, tx_date=tx_date,
                qty_in=Decimal(0), qty_out=qty,
                unit_cost=unit_c, total_cost=line_cost,
                reference_id=tx_id,
                party_id=line_party_id, party_role=line_party_role,
                party_name_snapshot=line_party_name,
                branch_code=tx.get("branch_code"),
                cost_center_code=tx.get("cost_center_code"),
                project_code=tx.get("project_code"),
                reason_code=tx.get("reason_code"),
                lot_id=lot_id, lot_number=line.get("lot_number"),
                serial_id=ser_id, serial_number=line.get("serial_number"),
                location_id=line.get("location_id"), tenant_id=tid,
            )
            await add_ledger_v5(
                db, item_id=item_id, warehouse_id=wh_to,
                tx_type=tx_type, tx_date=tx_date,
                qty_in=qty, qty_out=Decimal(0),
                unit_cost=unit_c, total_cost=line_cost,
                reference_id=tx_id,
                party_id=line_party_id, party_role=line_party_role,
                party_name_snapshot=line_party_name,
                branch_code=tx.get("branch_code"),
                cost_center_code=tx.get("cost_center_code"),
                project_code=tx.get("project_code"),
                reason_code=tx.get("reason_code"),
                lot_id=lot_id, lot_number=line.get("lot_number"),
                serial_id=ser_id, serial_number=line.get("serial_number"),
                location_id=line.get("location_id"), tenant_id=tid,
            )

        # ─── IJ — Inventory Adjustment ─────────────────────────────────────
        elif tx_type == "IJ":
            wh_id = tx["to_warehouse_id"] or tx["from_warehouse_id"]
            if not wh_id:
                raise HTTPException(400, "warehouse_id مطلوب للتسوية")

            # qty المرسلة هنا تمثل: المقدار الجديد للزيادة/النقص
            # is_increase من reason_code يحدد الاتجاه
            is_inc = bool(reason_is_increase) if reason_acc else (qty > 0)

            bal = await get_balance(db, item_id, wh_id, tid)
            avg_cost = bal["avg_cost"]
            if is_inc:
                line_cost = qty * (unit_cost if unit_cost > 0 else avg_cost)
                qty_in, qty_out = qty, Decimal(0)
                delta = qty
                cost_delta = line_cost
            else:
                line_cost = qty * avg_cost
                qty_in, qty_out = Decimal(0), qty
                delta = -qty
                cost_delta = -line_cost

            grand_total_cost += line_cost
            await adjust_balance(db, item_id, wh_id, delta, cost_delta, tx_date, tid)

            await add_ledger_v5(
                db, item_id=item_id, warehouse_id=wh_id,
                tx_type=tx_type, tx_date=tx_date,
                qty_in=qty_in, qty_out=qty_out,
                unit_cost=unit_cost if unit_cost > 0 else avg_cost,
                total_cost=line_cost,
                reference_id=tx_id,
                party_id=line_party_id, party_role=line_party_role,
                party_name_snapshot=line_party_name,
                branch_code=tx.get("branch_code"),
                cost_center_code=tx.get("cost_center_code"),
                project_code=tx.get("project_code"),
                reason_code=tx.get("reason_code"),
                location_id=line.get("location_id"), tenant_id=tid,
            )
        else:
            raise HTTPException(400, f"نوع الحركة غير مدعوم: {tx_type}")

    # ─── Post JE ───────────────────────────────────────────────────────────
    je_result = await post_je_v5(
        db,
        user_email=user.email,
        tx_type=tx_type,
        tx_date=tx_date,
        description=tx.get("description") or tx["serial"],
        debit_account=debit_acc,
        credit_account=credit_acc,
        amount=grand_total_cost,
        reference=tx["serial"],
        party_id=tx.get("party_id"),
        party_role=tx.get("party_role"),
        branch_code=tx.get("branch_code"),
        cost_center_code=tx.get("cost_center_code"),
        project_code=tx.get("project_code"),
        reason_code=tx.get("reason_code"),
        source_id=tx_id,
        tenant_id=tid,
    )

    # Update tx
    await db.execute(text("""
        UPDATE inv_transactions
        SET status='posted', je_id=:je_id, je_serial=:je_serial,
            posted_by=:by, posted_at=NOW(),
            total_cost=:tc, updated_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {
        "je_id": je_result["je_id"], "je_serial": je_result["je_serial"],
        "by": user.email, "tc": grand_total_cost,
        "id": str(tx_id), "tid": tid,
    })

    await db.commit()
    return ok(
        data={"je_id": je_result["je_id"], "je_serial": je_result["je_serial"],
              "total_cost": float(grand_total_cost)},
        message=f"✅ تم الترحيل — {je_result['je_serial']}",
    )


# ═══════════════════════════════════════════════════════════════════════════
# REVERSE TRANSACTION — يعكس الحركة (بعكس الكميات والمبالغ)
# ═══════════════════════════════════════════════════════════════════════════
@router.post("/transactions-v2/{tx_id}/reverse")
async def reverse_transaction_v2(
    tx_id: uuid.UUID,
    data: dict = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    ينشئ حركة عكسية ويُرحّلها تلقائياً.
    """
    tid = str(user.tenant_id)
    data = data or {}

    rt = await db.execute(text("""
        SELECT * FROM inv_transactions WHERE id=:id AND tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    row = rt.fetchone()
    if not row:
        raise HTTPException(404, "المستند غير موجود")
    tx = dict(row._mapping)

    if tx["status"] != "posted":
        raise HTTPException(400, "يمكن عكس المستندات المرحّلة فقط")

    rl = await db.execute(text("""
        SELECT * FROM inv_transaction_lines WHERE tx_id=:tid_id ORDER BY created_at
    """), {"tid_id": str(tx_id)})
    lines = [dict(r._mapping) for r in rl.fetchall()]

    # Reversal type mapping
    reverse_map = {
        "GRN": "RETURN_OUT",   # got too much → return to vendor
        "GIN": "RETURN_IN",    # we issued back → received
        "GIT": "GIT",           # opposite direction
        "IJ":  "IJ",
        "SCRAP": "RETURN_IN",
        "RETURN_IN": "RETURN_OUT",
        "RETURN_OUT": "RETURN_IN",
        "OPENING": "IJ",
    }
    rev_type = reverse_map.get(tx["tx_type"], "IJ")
    rev_date = date.fromisoformat(str(data.get("reverse_date", date.today())))
    rev_serial = await next_inv_serial(db, rev_type, rev_date, tid)
    rev_id = str(uuid.uuid4())

    # Swap warehouses for GIT
    from_wh = tx["from_warehouse_id"]
    to_wh = tx["to_warehouse_id"]
    if tx["tx_type"] == "GIT":
        from_wh, to_wh = to_wh, from_wh
    elif tx["tx_type"] in ("GRN", "RETURN_IN", "OPENING"):
        from_wh, to_wh = tx["to_warehouse_id"], None
    elif tx["tx_type"] in ("GIN", "RETURN_OUT", "SCRAP"):
        from_wh, to_wh = None, tx["from_warehouse_id"]

    await db.execute(text("""
        INSERT INTO inv_transactions (
            id, tenant_id, serial, tx_type, tx_date, status,
            from_warehouse_id, to_warehouse_id,
            party_id, party_role, party_name_snapshot,
            branch_code, cost_center_code, project_code, reason_code,
            reference, description, notes,
            reverses_id,
            created_by
        ) VALUES (
            :id, :tid, :sr, :tt, :dt, 'draft',
            :fw, :tw,
            :pid, :prole, :pname,
            :br, :cc, :prj, :rc,
            :ref, :desc, :notes,
            :rev_id,
            :by
        )
    """), {
        "id": rev_id, "tid": tid, "sr": rev_serial,
        "tt": rev_type, "dt": rev_date,
        "fw": str(from_wh) if from_wh else None,
        "tw": str(to_wh) if to_wh else None,
        "pid": tx.get("party_id"), "prole": tx.get("party_role"),
        "pname": tx.get("party_name_snapshot"),
        "br": tx.get("branch_code"),
        "cc": tx.get("cost_center_code"),
        "prj": tx.get("project_code"),
        "rc": data.get("reason_code") or "reversal",
        "ref": tx["serial"],
        "desc": f"عكس {tx['serial']}: " + (data.get("reason") or "تصحيح"),
        "notes": data.get("notes"),
        "rev_id": str(tx_id),
        "by": user.email,
    })

    for i, line in enumerate(lines):
        # Schema-aware: convert old line_order/transaction_id/location_id to new column names
        line_dict = dict(line) if hasattr(line, 'keys') else line
        line_no = line_dict.get('line_order', i + 1) if isinstance(line_dict, dict) else (i + 1)
        await db.execute(text("""
            INSERT INTO inv_transaction_lines (
                id, tenant_id, tx_id,
                item_id, item_code, item_name,
                uom_id, uom_name,
                qty, unit_cost, total_cost,
                lot_id, lot_number, expiry_date,
                serial_id, serial_number,
                from_location_id, to_location_id,
                party_id, party_role,
                notes
            ) VALUES (
                gen_random_uuid(), :tid, :tx_id,
                :iid, :icode, :iname,
                :uom, :uname,
                :qty, :uc, :tc,
                :lot_id, :lot, :exp,
                :ser_id, :ser,
                :from_loc, :to_loc,
                :pid, :prole,
                :notes
            )
        """), {
            "tid": tid, "tx_id": rev_id,
            "iid": line["item_id"],
            "icode": line.get("item_code", "") if isinstance(line, dict) else "",
            "iname": line.get("item_name", "") if isinstance(line, dict) else "",
            "uom": line.get("uom_id"),
            "uname": line.get("uom_name") if isinstance(line, dict) else None,
            "qty": line["qty"],
            "uc": line.get("unit_cost", 0),
            "tc": Decimal(str(line["qty"])) * Decimal(str(line.get("unit_cost", 0))),
            "lot_id": line.get("lot_id"),
            "lot": line.get("lot_number"),
            "exp": line.get("expiry_date"),
            "ser_id": line.get("serial_id"),
            "ser": line.get("serial_number"),
            "from_loc": line.get("to_location_id"),  # Reversed!
            "to_loc": line.get("from_location_id"),  # Reversed!
            "pid": line.get("party_id"),
            "prole": line.get("party_role"),
            "notes": f"عكس سطر {line_no}",
        })

    await db.commit()

    # Auto-post reversal
    return await post_transaction_v2(uuid.UUID(rev_id), db, user)
