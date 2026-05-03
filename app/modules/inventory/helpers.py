"""
app/modules/inventory/helpers.py
═══════════════════════════════════════════════════════════════════════════
Inventory Module v5 — Helpers & Resolvers
═══════════════════════════════════════════════════════════════════════════
الوظائف الأساسية:
  1. resolve_party()        → Smart Hybrid Party Resolver
                              (line → tx → item → category → null)
  2. resolve_reason_code()  → reason_code → expense_account_code
  3. get_tx_accounts()      → جلب حسابات الترحيل لكل tx_type
  4. post_je_v5()           → ترحيل JE مع party + dimensions + reason
                              يُمرّر AsyncSession نفسها للـ PostingEngine
                              ⇒ atomicity كاملة
  5. add_ledger_v5()        → Universal Subsidiary Ledger
                              يكتب في inv_ledger مع snapshot كامل
                              لـ party + dimensions + reason
  6. get_balance() / adjust_balance()
  7. fifo_consume() / fifo_add_layer()
  8. get_or_create_lot() / consume_from_lot() / validate_serial()
  9. next_inv_serial()      → ترقيم تسلسلي date-aware
                              يستخدم jl_numbering_series
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


TENANT_ID = "00000000-0000-0000-0000-000000000001"


# ═══════════════════════════════════════════════════════════════════════════
# 1. PARTY RESOLVER — Smart Hybrid (line → tx → item → category → null)
# ═══════════════════════════════════════════════════════════════════════════
async def resolve_party(
    db: AsyncSession,
    *,
    line_party_id: Optional[uuid.UUID] = None,
    line_party_role: Optional[str] = None,
    tx_party_id: Optional[uuid.UUID] = None,
    tx_party_role: Optional[str] = None,
    item_id: Optional[uuid.UUID] = None,
    category_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
) -> Tuple[Optional[uuid.UUID], Optional[str], Optional[str]]:
    """
    يحدد الطرف المالي للحركة بترتيب الأولويات:
      1. Line-level (الأعلى — للـ consignment / multi-vendor receipts)
      2. Header-level
      3. Item.extra_data.default_party_role
      4. Category.extra_data.default_party_role
      5. NULL

    Returns: (party_id, party_role, party_name_snapshot)
    """
    # Priority 1 — Line override
    if line_party_id is not None:
        r = await db.execute(
            text("SELECT name FROM parties WHERE id=:pid AND tenant_id=:tid"),
            {"pid": str(line_party_id), "tid": tenant_id},
        )
        row = r.fetchone()
        return line_party_id, line_party_role, (row[0] if row else None)

    # Priority 2 — Header
    if tx_party_id is not None:
        r = await db.execute(
            text("SELECT name FROM parties WHERE id=:pid AND tenant_id=:tid"),
            {"pid": str(tx_party_id), "tid": tenant_id},
        )
        row = r.fetchone()
        return tx_party_id, tx_party_role, (row[0] if row else None)

    # Priority 3 — Item default party role (smart hybrid)
    if item_id is not None:
        r = await db.execute(
            text(
                """
                SELECT extra_data->>'default_party_role' AS role,
                       extra_data->>'default_party_id'   AS pid
                FROM inv_items
                WHERE id=:iid AND tenant_id=:tid
                """
            ),
            {"iid": str(item_id), "tid": tenant_id},
        )
        row = r.fetchone()
        if row and row[0]:
            role = row[0]
            pid = uuid.UUID(row[1]) if row[1] else None
            name = None
            if pid:
                r2 = await db.execute(
                    text("SELECT name FROM parties WHERE id=:pid AND tenant_id=:tid"),
                    {"pid": str(pid), "tid": tenant_id},
                )
                rr = r2.fetchone()
                name = rr[0] if rr else None
            return pid, role, name

    # Priority 4 — Category default
    if category_id is not None:
        r = await db.execute(
            text(
                """
                SELECT extra_data->>'default_party_role' AS role,
                       extra_data->>'default_party_id'   AS pid
                FROM inv_categories
                WHERE id=:cid AND tenant_id=:tid
                """
            ),
            {"cid": str(category_id), "tid": tenant_id},
        )
        row = r.fetchone()
        if row and row[0]:
            role = row[0]
            pid = uuid.UUID(row[1]) if row[1] else None
            return pid, role, None

    return None, None, None


# ═══════════════════════════════════════════════════════════════════════════
# 2. REASON CODE RESOLVER — reason_code → expense_account_code
# ═══════════════════════════════════════════════════════════════════════════
async def resolve_reason_code(
    db: AsyncSession,
    reason_code: Optional[str],
    tenant_id: str = TENANT_ID,
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    يحدد حساب المصروفات حسب reason_code.

    Returns: (expense_account_code, reason_name, is_increase)
        is_increase=True يعني الحركة تزيد المخزون (overage)
        is_increase=False يعني الحركة تنقص المخزون (variance/damage/theft)
    """
    if not reason_code:
        return None, None, False
    r = await db.execute(
        text(
            """
            SELECT expense_account_code, reason_name, is_increase
            FROM inv_reason_codes
            WHERE reason_code=:rc AND tenant_id=:tid AND is_active=true
            """
        ),
        {"rc": reason_code, "tid": tenant_id},
    )
    row = r.fetchone()
    if not row:
        return None, None, False
    return row[0], row[1], bool(row[2])


# ═══════════════════════════════════════════════════════════════════════════
# 3. TRANSACTION ACCOUNTS — جلب حسابات الترحيل لكل tx_type
# ═══════════════════════════════════════════════════════════════════════════
DEFAULT_TX_ACCOUNTS = {
    "GRN":         ("1140", "2102", "استلام بضاعة من مورد"),
    "GIN":         ("5101", "1140", "صرف بضاعة (تكلفة بضاعة مباعة)"),
    "GIT":         ("1140", "1140", "تحويل بين مستودعات"),
    "IJ":          ("5301", "1140", "تسوية مخزون - نقص"),
    "IJ+":         ("1140", "4901", "تسوية مخزون - زيادة"),
    "SCRAP":       ("5301", "1140", "تالف"),
    "RETURN_IN":   ("1140", "4101", "مرتجع موردين"),
    "RETURN_OUT":  ("5101", "1140", "مرتجع عملاء"),
    "OPENING":     ("1140", "3101", "رصيد افتتاحي"),
}


async def get_tx_accounts(
    db: AsyncSession,
    tx_type: str,
    tenant_id: str = TENANT_ID,
) -> Tuple[str, str, str]:
    """
    Returns (debit_account, credit_account, description).
    Reads from inv_account_settings if configured, else uses defaults.
    """
    r = await db.execute(
        text(
            """
            SELECT debit_account, credit_account, description
            FROM inv_account_settings
            WHERE tenant_id=:tid AND tx_type=:tt
            """
        ),
        {"tid": tenant_id, "tt": tx_type},
    )
    row = r.fetchone()
    if row:
        d, c, desc = row[0], row[1], row[2]
        # Fallback to defaults if any field is empty
        defaults = DEFAULT_TX_ACCOUNTS.get(tx_type, ("1140", "1140", tx_type))
        return (d or defaults[0], c or defaults[1], desc or defaults[2])
    return DEFAULT_TX_ACCOUNTS.get(tx_type, ("1140", "1140", tx_type))


# ═══════════════════════════════════════════════════════════════════════════
# 4. INV_LEDGER — Universal Subsidiary Ledger
# ═══════════════════════════════════════════════════════════════════════════
async def add_ledger_v5(
    db: AsyncSession,
    *,
    item_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    tx_type: str,
    tx_date: date,
    qty_in: Decimal,
    qty_out: Decimal,
    unit_cost: Decimal,
    total_cost: Decimal,
    reference_id: uuid.UUID,
    reference_type: str = "INV_TX",
    # Layer 3 — Universal Subsidiary
    party_id: Optional[uuid.UUID] = None,
    party_role: Optional[str] = None,
    party_name_snapshot: Optional[str] = None,
    branch_code: Optional[str] = None,
    cost_center_code: Optional[str] = None,
    project_code: Optional[str] = None,
    reason_code: Optional[str] = None,
    lot_id: Optional[uuid.UUID] = None,
    lot_number: Optional[str] = None,
    serial_id: Optional[uuid.UUID] = None,
    serial_number: Optional[str] = None,
    location_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
) -> uuid.UUID:
    """
    يكتب سطر في inv_ledger مع snapshot كامل لكل الأبعاد.
    هذا الجدول هو "السجل الفرعي العالمي" (Universal Subsidiary Ledger).
    """
    # Compute running balance
    r = await db.execute(
        text(
            """
            SELECT COALESCE(SUM(qty_in - qty_out), 0),
                   COALESCE(SUM(CASE WHEN qty_in > 0 THEN total_cost
                                     ELSE -total_cost END), 0)
            FROM inv_ledger
            WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
            """
        ),
        {"tid": tenant_id, "iid": str(item_id), "wid": str(warehouse_id)},
    )
    row = r.fetchone()
    prev_qty = Decimal(str(row[0] or 0))
    prev_cost = Decimal(str(row[1] or 0))
    sign = 1 if qty_in > 0 else -1
    bal_qty = prev_qty + qty_in - qty_out
    bal_cost = prev_cost + (total_cost * sign)

    led_id = uuid.uuid4()
    await db.execute(
        text(
            """
            INSERT INTO inv_ledger (
                id, tenant_id, item_id, warehouse_id, location_id,
                tx_id,
                tx_type, tx_date,
                qty_in, qty_out, unit_cost, total_cost,
                qty_balance, wac_after,
                balance_qty, balance_cost,
                lot_id, lot_number, serial_id,
                party_id, party_role, party_name_snapshot,
                branch_code, cost_center_code, project_code, reason_code
            ) VALUES (
                :id, :tid, :iid, :wid, :loc,
                :ref_id,
                :tx_type, :tx_date,
                :qi, :qo, :uc, :tc,
                :bq, :wac,
                :bq, :bc,
                :lot_id, :lot, :ser_id,
                :pid, :prole, :pname,
                :br, :cc, :prj, :rc
            )
            """
        ),
        {
            "id": str(led_id), "tid": tenant_id,
            "iid": str(item_id), "wid": str(warehouse_id),
            "loc": str(location_id) if location_id else None,
            "ref_id": str(reference_id),
            "tx_type": tx_type, "tx_date": tx_date,
            "qi": qty_in, "qo": qty_out, "uc": unit_cost, "tc": total_cost,
            "bq": bal_qty, "wac": (bal_cost / bal_qty) if bal_qty > 0 else Decimal(0),
            "bc": bal_cost,
            "lot_id": str(lot_id) if lot_id else None,
            "lot": lot_number,
            "ser_id": str(serial_id) if serial_id else None,
            "pid": str(party_id) if party_id else None,
            "prole": party_role, "pname": party_name_snapshot,
            "br": branch_code, "cc": cost_center_code,
            "prj": project_code, "rc": reason_code,
        },
    )
    return led_id


# ═══════════════════════════════════════════════════════════════════════════
# 5. BALANCES (qty + value)
# ═══════════════════════════════════════════════════════════════════════════
async def get_balance(
    db: AsyncSession,
    item_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    tenant_id: str = TENANT_ID,
) -> Dict[str, Any]:
    r = await db.execute(
        text(
            """
            SELECT qty_on_hand, qty_reserved, avg_cost, total_value
            FROM inv_balances
            WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
            """
        ),
        {"tid": tenant_id, "iid": str(item_id), "wid": str(warehouse_id)},
    )
    row = r.fetchone()
    if row:
        return {
            "qty_on_hand": Decimal(str(row[0])),
            "qty_reserved": Decimal(str(row[1])),
            "avg_cost": Decimal(str(row[2])),
            "total_value": Decimal(str(row[3])),
        }
    return {
        "qty_on_hand": Decimal(0),
        "qty_reserved": Decimal(0),
        "avg_cost": Decimal(0),
        "total_value": Decimal(0),
    }


async def adjust_balance(
    db: AsyncSession,
    item_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    qty_delta: Decimal,
    cost_delta: Decimal,
    tx_date: date,
    tenant_id: str = TENANT_ID,
) -> Dict[str, Any]:
    """
    يحدّث inv_balances بـ delta. يحسب avg_cost الجديد تلقائياً.
    
    ⚠️ Schema-aware (2026-05-03):
    inv_balances يطلب أعمدة NOT NULL:
      - item_code, item_name (من inv_items)
      - warehouse_code, warehouse_name (من inv_warehouses)
      - qty_available, qty_incoming, min_qty, reorder_point (default 0)
    """
    bal = await get_balance(db, item_id, warehouse_id, tenant_id)
    new_qty = bal["qty_on_hand"] + qty_delta
    new_val = bal["total_value"] + cost_delta
    new_cost = (new_val / new_qty) if new_qty > 0 else Decimal(0)

    # Fetch item info (needed for NOT NULL constraints)
    item_info = await db.execute(
        text("SELECT item_code, item_name FROM inv_items WHERE id=:iid LIMIT 1"),
        {"iid": str(item_id)},
    )
    item_row = item_info.fetchone()
    item_code = item_row[0] if item_row else "UNKNOWN"
    item_name = item_row[1] if item_row else "Unknown Item"

    # Fetch warehouse info (needed for NOT NULL constraints)
    # ⚠️ inv_warehouses uses warehouse_code/warehouse_name (no 'code'/'name' columns)
    wh_info = await db.execute(
        text("""
            SELECT warehouse_code, warehouse_name
            FROM inv_warehouses WHERE id=:wid LIMIT 1
        """),
        {"wid": str(warehouse_id)},
    )
    wh_row = wh_info.fetchone()
    warehouse_code = wh_row[0] if wh_row else "UNKNOWN"
    warehouse_name = wh_row[1] if wh_row else "Unknown Warehouse"

    qty_available = new_qty  # available = on_hand - reserved (we use 0 reserved)

    await db.execute(
        text(
            """
            INSERT INTO inv_balances (
                id, tenant_id,
                item_id, item_code, item_name,
                warehouse_id, warehouse_code, warehouse_name,
                qty_on_hand, qty_reserved, qty_available, qty_incoming,
                avg_cost, total_value,
                min_qty, reorder_point,
                last_movement
            ) VALUES (
                gen_random_uuid(), :tid,
                :iid, :icode, :iname,
                :wid, :wcode, :wname,
                :qty, 0, :qavail, 0,
                :cost, :val,
                0, 0,
                :dt
            )
            ON CONFLICT (tenant_id, item_id, warehouse_id) DO UPDATE SET
                qty_on_hand   = :qty,
                qty_available = :qavail,
                avg_cost      = :cost,
                total_value   = :val,
                last_movement = :dt,
                updated_at    = NOW()
            """
        ),
        {
            "tid": tenant_id, "iid": str(item_id),
            "icode": item_code, "iname": item_name,
            "wid": str(warehouse_id),
            "wcode": warehouse_code, "wname": warehouse_name,
            "qty": new_qty, "qavail": qty_available,
            "cost": new_cost, "val": new_val, "dt": tx_date,
        },
    )
    return {"qty_on_hand": new_qty, "avg_cost": new_cost, "total_value": new_val}


# ═══════════════════════════════════════════════════════════════════════════
# 6. FIFO COSTING
# ═══════════════════════════════════════════════════════════════════════════
async def fifo_add_layer(
    db: AsyncSession,
    *,
    item_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    receipt_date: date,
    qty: Decimal,
    unit_cost: Decimal,
    reference_id: uuid.UUID,
    lot_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
) -> uuid.UUID:
    """
    إضافة طبقة FIFO جديدة عند استلام بضاعة.
    
    ⚠️ Schema-aware (2026-05-03):
    DB الفعلي يستخدم: tx_id, qty_received, qty_remaining
    (وليس: reference_id, original_qty, remaining_qty)
    """
    layer_id = uuid.uuid4()
    await db.execute(
        text(
            """
            INSERT INTO inv_fifo_layers (
                id, tenant_id, item_id, warehouse_id,
                tx_id, receipt_date,
                qty_received, qty_remaining,
                unit_cost
            ) VALUES (
                :id, :tid, :iid, :wid,
                :ref, :dt,
                :qty, :qty,
                :uc
            )
            """
        ),
        {
            "id": str(layer_id), "tid": tenant_id,
            "iid": str(item_id), "wid": str(warehouse_id),
            "ref": str(reference_id),
            "dt": receipt_date, "qty": qty, "uc": unit_cost,
        },
    )
    return layer_id


async def fifo_consume(
    db: AsyncSession,
    item_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    qty_needed: Decimal,
    tenant_id: str = TENANT_ID,
) -> Tuple[Decimal, List[Dict[str, Any]]]:
    """
    يستهلك FIFO layers ويعيد (total_cost, consumed_layers_info).
    """
    r = await db.execute(
        text(
            """
            SELECT id, qty_remaining, unit_cost
            FROM inv_fifo_layers
            WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
              AND qty_remaining > 0
            ORDER BY receipt_date, created_at
            """
        ),
        {"tid": tenant_id, "iid": str(item_id), "wid": str(warehouse_id)},
    )
    layers = r.fetchall()
    total_cost = Decimal(0)
    remaining = qty_needed
    consumed_info = []

    for layer in layers:
        if remaining <= 0:
            break
        avail = Decimal(str(layer[1]))
        consume = min(avail, remaining)
        cost = consume * Decimal(str(layer[2]))
        total_cost += cost
        remaining -= consume
        consumed_info.append({
            "layer_id": str(layer[0]),
            "qty_consumed": float(consume),
            "unit_cost": float(layer[2]),
            "cost": float(cost),
        })
        await db.execute(
            text(
                "UPDATE inv_fifo_layers SET qty_remaining = qty_remaining - :c WHERE id=:lid"
            ),
            {"c": consume, "lid": str(layer[0])},
        )

    if remaining > 0:
        # Insufficient FIFO layers — fallback to AVG cost for remainder
        bal = await get_balance(db, item_id, warehouse_id, tenant_id)
        total_cost += remaining * bal["avg_cost"]

    return total_cost, consumed_info


# ═══════════════════════════════════════════════════════════════════════════
# 7. AVG COSTING
# ═══════════════════════════════════════════════════════════════════════════
async def avg_consume_cost(
    db: AsyncSession,
    item_id: uuid.UUID,
    warehouse_id: uuid.UUID,
    qty: Decimal,
    tenant_id: str = TENANT_ID,
) -> Decimal:
    bal = await get_balance(db, item_id, warehouse_id, tenant_id)
    return qty * bal["avg_cost"]


# ═══════════════════════════════════════════════════════════════════════════
# 8. LOTS MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════
async def get_or_create_lot(
    db: AsyncSession,
    *,
    item_id: uuid.UUID,
    lot_number: str,
    expiry_date: Optional[date] = None,
    manufactured_date: Optional[date] = None,
    supplier_party_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
) -> uuid.UUID:
    """
    يبحث عن لوط موجود أو ينشئ جديد.
    """
    r = await db.execute(
        text(
            """
            SELECT id FROM inv_lots
            WHERE tenant_id=:tid AND item_id=:iid AND lot_number=:ln
            """
        ),
        {"tid": tenant_id, "iid": str(item_id), "ln": lot_number},
    )
    row = r.fetchone()
    if row:
        return uuid.UUID(str(row[0]))

    lot_id = uuid.uuid4()
    await db.execute(
        text(
            """
            INSERT INTO inv_lots (
                id, tenant_id, item_id, lot_number,
                expiry_date, manufactured_date, supplier_party_id,
                qty_on_hand, status
            ) VALUES (
                :id, :tid, :iid, :ln,
                :exp, :mfg, :sup,
                0, 'active'
            )
            """
        ),
        {
            "id": str(lot_id), "tid": tenant_id,
            "iid": str(item_id), "ln": lot_number,
            "exp": expiry_date, "mfg": manufactured_date,
            "sup": str(supplier_party_id) if supplier_party_id else None,
        },
    )
    return lot_id


async def adjust_lot_qty(
    db: AsyncSession,
    lot_id: uuid.UUID,
    qty_delta: Decimal,
    tenant_id: str = TENANT_ID,
):
    await db.execute(
        text(
            """
            UPDATE inv_lots
            SET qty_on_hand = qty_on_hand + :d, updated_at = NOW()
            WHERE id=:id AND tenant_id=:tid
            """
        ),
        {"d": qty_delta, "id": str(lot_id), "tid": tenant_id},
    )


# ═══════════════════════════════════════════════════════════════════════════
# 9. SERIAL NUMBERS
# ═══════════════════════════════════════════════════════════════════════════
async def get_or_create_serial(
    db: AsyncSession,
    *,
    item_id: uuid.UUID,
    serial_number: str,
    warehouse_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
) -> uuid.UUID:
    r = await db.execute(
        text(
            "SELECT id FROM inv_serials WHERE tenant_id=:tid AND item_id=:iid AND serial_number=:sn"
        ),
        {"tid": tenant_id, "iid": str(item_id), "sn": serial_number},
    )
    row = r.fetchone()
    if row:
        return uuid.UUID(str(row[0]))

    sid = uuid.uuid4()
    await db.execute(
        text(
            """
            INSERT INTO inv_serials (
                id, tenant_id, item_id, serial_number, current_warehouse_id, status
            ) VALUES (
                :id, :tid, :iid, :sn, :wid, 'available'
            )
            """
        ),
        {
            "id": str(sid), "tid": tenant_id,
            "iid": str(item_id), "sn": serial_number,
            "wid": str(warehouse_id) if warehouse_id else None,
        },
    )
    return sid


async def update_serial_status(
    db: AsyncSession,
    serial_id: uuid.UUID,
    status: str,
    warehouse_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
):
    await db.execute(
        text(
            """
            UPDATE inv_serials
            SET status=:st, current_warehouse_id=:wid, updated_at=NOW()
            WHERE id=:id AND tenant_id=:tid
            """
        ),
        {
            "st": status, "id": str(serial_id), "tid": tenant_id,
            "wid": str(warehouse_id) if warehouse_id else None,
        },
    )


# ═══════════════════════════════════════════════════════════════════════════
# 10. NUMBERING — date-aware serial generator
# ═══════════════════════════════════════════════════════════════════════════
async def next_inv_serial(
    db: AsyncSession,
    tx_type: str,
    tx_date: date,
    tenant_id: str = TENANT_ID,
) -> str:
    """
    يولّد serial متسلسل للسنة المالية الخاصة بـ tx_date.
    يستخدم jl_numbering_series (مشتركة بين كل الموديولات).
    """
    year = tx_date.year

    # Atomic increment
    r = await db.execute(
        text(
            """
            UPDATE jl_numbering_series
            SET next_serial = next_serial + 1, updated_at = NOW()
            WHERE tenant_id=:tid AND series_type=:tt AND year=:yr
            RETURNING next_serial
            """
        ),
        {"tid": tenant_id, "tt": tx_type, "yr": year},
    )
    row = r.fetchone()
    if not row:
        # First entry for this year/type
        await db.execute(
            text(
                """
                INSERT INTO jl_numbering_series (
                    id, tenant_id, series_type, year, next_serial, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :tid, :tt, :yr, 2, NOW(), NOW()
                )
                ON CONFLICT (tenant_id, series_type, year) DO UPDATE SET
                    next_serial = jl_numbering_series.next_serial + 1,
                    updated_at = NOW()
                """
            ),
            {"tid": tenant_id, "tt": tx_type, "yr": year},
        )
        seq = 1
    else:
        seq = int(row[0]) - 1  # We incremented before reading

    # Read prefix format
    sr = await db.execute(
        text(
            """
            SELECT prefix_format
            FROM jl_numbering_series
            WHERE tenant_id=:tid AND series_type=:tt AND year=:yr
            """
        ),
        {"tid": tenant_id, "tt": tx_type, "yr": year},
    )
    srow = sr.fetchone()
    fmt = srow[0] if srow and srow[0] else f"{tx_type}-{{year}}-{{serial:07d}}"

    try:
        return fmt.format(year=year, serial=seq)
    except Exception:
        return f"{tx_type}-{year}-{seq:07d}"


# ═══════════════════════════════════════════════════════════════════════════
# 11. POST_JE_V5 — Posting with party + dimensions + reason
# ═══════════════════════════════════════════════════════════════════════════
async def post_je_v5(
    db: AsyncSession,
    *,
    user_email: str,
    tx_type: str,
    tx_date: date,
    description: str,
    debit_account: str,
    credit_account: str,
    amount: Decimal,
    reference: Optional[str] = None,
    # v5 enhancements
    party_id: Optional[uuid.UUID] = None,
    party_role: Optional[str] = None,
    branch_code: Optional[str] = None,
    cost_center_code: Optional[str] = None,
    project_code: Optional[str] = None,
    reason_code: Optional[str] = None,
    source_id: Optional[uuid.UUID] = None,
    tenant_id: str = TENANT_ID,
) -> Dict[str, str]:
    """
    يرحّل قيد محاسبي مع كل أبعاد المخزون.
    يستخدم نفس AsyncSession ⇒ atomicity كاملة مع باقي العمليات.
    """
    try:
        from app.services.posting.engine import (
            PostingEngine, PostingRequest, PostingLine,
        )
        t_id = uuid.UUID(tenant_id)
        engine = PostingEngine(db, t_id)

        # Build dimension dict — common dims (apply to all lines)
        common_dims: Dict[str, Any] = {}
        if branch_code:        common_dims["branch_code"] = branch_code
        if cost_center_code:   common_dims["cost_center_code"] = cost_center_code
        if project_code:       common_dims["project_code"] = project_code

        # ⭐ Smart party assignment (2026-05-04) — accounting best practice
        # Party should appear ONLY on the payable/receivable side, NOT both
        # Otherwise Subsidiary Ledger shows dr+cr canceling to zero balance
        #
        # Logic by tx_type:
        #   GRN, RETURN_OUT  → party on CREDIT line  (vendor payable)
        #   GDN, RETURN_IN   → party on DEBIT line   (customer receivable)
        #   GIN, SCRAP       → party on DEBIT line   (cost charged to party)
        #   IT, IJ, IJ+      → no party (internal movement)
        party_dims: Dict[str, Any] = {}
        if party_id:
            party_dims["party_id"] = str(party_id)
        if party_role:
            party_dims["party_role"] = party_role

        party_on_debit = False
        party_on_credit = False
        if party_dims:
            if tx_type in ("GRN", "RETURN_OUT"):
                party_on_credit = True
            elif tx_type in ("GDN", "RETURN_IN"):
                party_on_debit = True
            elif tx_type in ("GIN", "SCRAP"):
                party_on_debit = True
            # IT, IJ, IJ+ → no party

        line_dr = PostingLine(
            account_code=debit_account,
            description=description,
            debit=amount,
            credit=Decimal(0),
        )
        line_cr = PostingLine(
            account_code=credit_account,
            description=description,
            debit=Decimal(0),
            credit=amount,
        )

        # Attach common dimensions (branch, cost_center, project) to BOTH lines
        for line in (line_dr, line_cr):
            for k, v in common_dims.items():
                if hasattr(line, k):
                    setattr(line, k, v)

        # Attach party ONLY to the correct side
        if party_on_debit:
            for k, v in party_dims.items():
                if hasattr(line_dr, k):
                    setattr(line_dr, k, v)
        if party_on_credit:
            for k, v in party_dims.items():
                if hasattr(line_cr, k):
                    setattr(line_cr, k, v)

        # Build PostingRequest kwargs — only pass what PostingRequest accepts
        # ⚠️ source_id removed (not accepted by PostingRequest)
        # If your PostingEngine version supports it, uncomment below
        pr_kwargs = {
            "tenant_id": t_id,
            "je_type": tx_type,
            "description": description,
            "entry_date": tx_date,
            "lines": [line_dr, line_cr],
            "created_by_email": user_email,
            "reference": reference,
            "source_module": "inventory",
        }
        # Try to attach source_id if PostingRequest supports it
        try:
            import inspect
            sig = inspect.signature(PostingRequest)
            if "source_id" in sig.parameters and source_id:
                pr_kwargs["source_id"] = str(source_id)
        except Exception:
            pass

        result = await engine.post(PostingRequest(**pr_kwargs))
        return {
            "je_id": str(result.je_id),
            "je_serial": result.je_serial,
        }
    except Exception as e:
        # Posting failure is a critical error — bubble it up
        raise HTTPException(
            status_code=500,
            detail=f"فشل ترحيل القيد المحاسبي: {str(e)}",
        )


# ═══════════════════════════════════════════════════════════════════════════
# 12. Stakeholders Snapshot — للحالات الاستثنائية (Landed Cost المستقبلي)
# ═══════════════════════════════════════════════════════════════════════════
def build_stakeholders_jsonb(
    shipper: Optional[Dict] = None,
    insurer: Optional[Dict] = None,
    broker: Optional[Dict] = None,
    extras: Optional[Dict] = None,
) -> Optional[str]:
    data = {}
    if shipper: data["shipper"] = shipper
    if insurer: data["insurer"] = insurer
    if broker:  data["broker"] = broker
    if extras:  data.update(extras)
    return json.dumps(data, ensure_ascii=False) if data else None


# ═══════════════════════════════════════════════════════════════════════════
# 13. Item Lookup helpers (لـ resolvers)
# ═══════════════════════════════════════════════════════════════════════════
async def get_item_metadata(
    db: AsyncSession,
    item_id: uuid.UUID,
    tenant_id: str = TENANT_ID,
) -> Optional[Dict[str, Any]]:
    """
    يجلب metadata صنف مفيدة للـ posting (cost_method, accounts, tracking flags).
    """
    r = await db.execute(
        text(
            """
            SELECT
                i.id, i.item_code, i.item_name,
                i.category_id, i.uom_id,
                COALESCE(i.valuation_method, i.cost_method, 'avg') AS valuation_method,
                i.gl_account_code, i.cogs_account_code,
                i.is_serialized, i.is_lot_tracked, i.is_expiry_tracked,
                i.has_variants, i.is_variant, i.parent_item_id,
                i.extra_data
            FROM inv_items i
            WHERE i.id=:iid AND i.tenant_id=:tid
            """
        ),
        {"iid": str(item_id), "tid": tenant_id},
    )
    row = r.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "item_code": row[1], "item_name": row[2],
        "category_id": row[3], "uom_id": row[4],
        "valuation_method": row[5] or "avg",
        "gl_account_code": row[6], "cogs_account_code": row[7],
        "is_serialized": bool(row[8]),
        "is_lot_tracked": bool(row[9]),
        "is_expiry_tracked": bool(row[10]),
        "has_variants": bool(row[11]),
        "is_variant": bool(row[12]),
        "parent_item_id": row[13],
        "extra_data": row[14] or {},
    }


async def get_warehouse_accounts(
    db: AsyncSession,
    warehouse_id: uuid.UUID,
    tenant_id: str = TENANT_ID,
) -> Optional[Dict[str, str]]:
    """
    يجلب حسابات المستودع (إن كانت مختلفة عن الافتراضي).
    """
    r = await db.execute(
        text(
            """
            SELECT inventory_account_code, branch_code, cost_center_code
            FROM inv_warehouses
            WHERE id=:wid AND tenant_id=:tid
            """
        ),
        {"wid": str(warehouse_id), "tid": tenant_id},
    )
    row = r.fetchone()
    if not row:
        return None
    return {
        "inventory_account_code": row[0],
        "branch_code": row[1],
        "cost_center_code": row[2],
    }
