"""
app/modules/inventory/routers/reports.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Reports Router (14 Enterprise Reports)
═══════════════════════════════════════════════════════════════════════════
1.  stock-balance        — رصيد المخزون الحالي
2.  stock-card           — كرت الصنف (movements timeline)
3.  valuation            — التقييم (group by category/warehouse/branch/brand)
4.  aging                — التقادم (0-30, 31-90, 91-180, >180)
5.  expiry               — قرب انتهاء الصلاحية
6.  negative-stock       — أرصدة سالبة (تنبيه!)
7.  below-reorder        — أصناف تحت نقطة إعادة الطلب
8.  slow-moving          — حركة بطيئة
9.  dead-stock           — مخزون راكد (لا حركة منذ N يوم)
10. turnover             — معدل الدوران
11. abc-analysis         — تحليل ABC (Pareto)
12. party-statement      ⭐ Universal Subsidiary Ledger (UNIQUE TO HASABATI)
13. by-dimension         — حركات حسب فرع/مركز تكلفة/مشروع
14. cogs                 — تكلفة البضاعة المباعة
15. variance             — فروقات الجرد
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory/reports-v2", tags=["inventory-reports"])


def _to_float(rows, *fields):
    """Convert specific Decimal fields to float for JSON safety."""
    out = []
    for r in rows:
        d = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
        for f in fields:
            if f in d and d[f] is not None:
                try:
                    d[f] = float(d[f])
                except (TypeError, ValueError):
                    pass
        out.append(d)
    return out


# ═══════════════════════════════════════════════════════════════════════════
# 1. STOCK BALANCE
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/stock-balance")
async def stock_balance(
    warehouse_id: Optional[uuid.UUID] = None,
    category_id: Optional[uuid.UUID] = None,
    brand_id: Optional[uuid.UUID] = None,
    only_with_qty: bool = Query(True),
    search: Optional[str] = None,
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["b.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}

    if warehouse_id:
        conds.append("b.warehouse_id=:wid"); params["wid"] = str(warehouse_id)
    if category_id:
        conds.append("i.category_id=:cat"); params["cat"] = str(category_id)
    if brand_id:
        conds.append("i.brand_id=:bid"); params["bid"] = str(brand_id)
    if only_with_qty:
        conds.append("b.qty_on_hand <> 0")
    if search:
        conds.append("(i.item_code ILIKE :s OR i.item_name ILIKE :s OR i.barcode ILIKE :s)")
        params["s"] = f"%{search}%"

    where = " AND ".join(conds)

    rc = await db.execute(text(f"""
        SELECT COUNT(*) FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        WHERE {where}
    """), params)
    total = rc.scalar() or 0

    r = await db.execute(text(f"""
        SELECT i.id AS item_id, i.item_code, i.item_name, i.barcode,
               c.category_name, br.name AS brand_name, u.uom_name,
               w.warehouse_name,
               b.qty_on_hand, b.avg_cost, b.total_value,
               b.last_movement, b.warehouse_id
        FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        LEFT JOIN inv_categories c ON c.id = i.category_id
        LEFT JOIN inv_brands     br ON br.id = i.brand_id
        LEFT JOIN inv_uom        u  ON u.id = i.uom_id
        LEFT JOIN inv_warehouses w  ON w.id = b.warehouse_id
        WHERE {where}
        ORDER BY i.item_name, w.warehouse_name
        LIMIT :limit OFFSET :offset
    """), params)
    rows = _to_float(r.fetchall(), "qty_on_hand", "avg_cost", "total_value")
    grand_total = sum(row.get("total_value") or 0 for row in rows)
    return ok(data={
        "items": rows,
        "total": total,
        "grand_total_value": float(grand_total),
        "as_of": date.today().isoformat(),
    })


# ═══════════════════════════════════════════════════════════════════════════
# 2. STOCK CARD
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/stock-card")
async def stock_card(
    item_id: uuid.UUID,
    warehouse_id: Optional[uuid.UUID] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = Query(500, ge=1, le=5000),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid", "l.item_id=:iid"]
    params: dict = {"tid": tid, "iid": str(item_id), "limit": limit}

    if warehouse_id:
        conds.append("l.warehouse_id=:wid"); params["wid"] = str(warehouse_id)
    if date_from:
        conds.append("l.tx_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("l.tx_date<=:dt"); params["dt"] = date_to

    where = " AND ".join(conds)

    r = await db.execute(text(f"""
        SELECT l.tx_date, l.tx_type, l.qty_in, l.qty_out,
               l.unit_cost, l.total_cost,
               l.balance_qty, l.balance_cost,
               l.reference_id, l.reference_type,
               l.party_id, l.party_role, l.party_name_snapshot,
               l.branch_code, l.cost_center_code, l.project_code,
               l.reason_code, l.lot_number, l.serial_number,
               w.warehouse_name,
               t.serial AS tx_serial
        FROM inv_ledger l
        LEFT JOIN inv_warehouses w ON w.id = l.warehouse_id
        LEFT JOIN inv_transactions t ON t.id = l.reference_id
        WHERE {where}
        ORDER BY l.tx_date, l.created_at
        LIMIT :limit
    """), params)
    rows = _to_float(
        r.fetchall(),
        "qty_in", "qty_out", "unit_cost", "total_cost",
        "balance_qty", "balance_cost",
    )

    # Item header
    ri = await db.execute(text("""
        SELECT i.item_code, i.item_name, u.uom_name,
               c.category_name, br.name AS brand_name
        FROM inv_items i
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        LEFT JOIN inv_categories c ON c.id = i.category_id
        LEFT JOIN inv_brands br ON br.id = i.brand_id
        WHERE i.id=:iid AND i.tenant_id=:tid
    """), {"iid": str(item_id), "tid": tid})
    item = ri.fetchone()
    item_dict = dict(item._mapping) if item else None

    return ok(data={
        "item": item_dict,
        "movements": rows,
        "total_movements": len(rows),
    })


# ═══════════════════════════════════════════════════════════════════════════
# 3. VALUATION
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/valuation")
async def valuation(
    group_by: str = Query("category", pattern="^(category|warehouse|brand|branch)$"),
    warehouse_id: Optional[uuid.UUID] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    params: dict = {"tid": tid}

    group_sql = {
        "category":  ("c.id, c.category_name", "c.category_name", "LEFT JOIN inv_categories c ON c.id=i.category_id"),
        "warehouse": ("w.id, w.warehouse_name", "w.warehouse_name", "JOIN inv_warehouses w ON w.id=b.warehouse_id"),
        "brand":     ("br.id, br.name",         "br.name AS brand_name", "LEFT JOIN inv_brands br ON br.id=i.brand_id"),
        "branch":    ("b.warehouse_id, w.warehouse_name", "w.warehouse_name", "JOIN inv_warehouses w ON w.id=b.warehouse_id"),
    }
    group_cols, label_col, extra_join = group_sql[group_by]

    wh_filter = ""
    if warehouse_id:
        wh_filter = " AND b.warehouse_id=:wid"
        params["wid"] = str(warehouse_id)

    r = await db.execute(text(f"""
        SELECT {group_cols},
               COUNT(DISTINCT i.id)               AS item_count,
               COALESCE(SUM(b.qty_on_hand), 0)    AS total_qty,
               COALESCE(SUM(b.total_value), 0)    AS total_value,
               COALESCE(AVG(NULLIF(b.avg_cost,0)), 0) AS avg_cost
        FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        {extra_join}
        WHERE b.tenant_id=:tid{wh_filter}
        GROUP BY {group_cols}
        ORDER BY total_value DESC
    """), params)
    rows = _to_float(r.fetchall(), "total_qty", "total_value", "avg_cost")
    grand_total = sum(r.get("total_value") or 0 for r in rows)
    return ok(data={
        "groups": rows,
        "grand_total": grand_total,
        "as_of": date.today().isoformat(),
        "group_by": group_by,
    })


# ═══════════════════════════════════════════════════════════════════════════
# 4. AGING
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/aging")
async def aging(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT i.item_code, i.item_name, c.category_name, u.uom_name,
               w.warehouse_name,
               b.qty_on_hand, b.avg_cost, b.total_value, b.last_movement,
               (CURRENT_DATE - b.last_movement) AS days_since_movement,
               CASE
                 WHEN b.last_movement IS NULL THEN 'غير محدد'
                 WHEN CURRENT_DATE - b.last_movement <= 30  THEN '0-30 يوم'
                 WHEN CURRENT_DATE - b.last_movement <= 90  THEN '31-90 يوم'
                 WHEN CURRENT_DATE - b.last_movement <= 180 THEN '91-180 يوم'
                 ELSE 'أكثر من 180 يوم (راكد)'
               END AS aging_bucket
        FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        LEFT JOIN inv_categories c ON c.id = i.category_id
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        LEFT JOIN inv_warehouses w ON w.id = b.warehouse_id
        WHERE b.tenant_id=:tid AND b.qty_on_hand > 0
        ORDER BY days_since_movement DESC NULLS LAST
    """), {"tid": tid})
    rows = _to_float(r.fetchall(), "qty_on_hand", "avg_cost", "total_value")
    # Build summary
    buckets: dict = {}
    for row in rows:
        bk = row.get("aging_bucket") or "غير محدد"
        buckets.setdefault(bk, {"bucket": bk, "item_count": 0, "total_value": 0.0})
        buckets[bk]["item_count"] += 1
        buckets[bk]["total_value"] += row.get("total_value") or 0
    return ok(data={
        "items": rows,
        "summary": list(buckets.values()),
    })


# ═══════════════════════════════════════════════════════════════════════════
# 5. EXPIRY
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/expiry")
async def expiry(
    days_ahead: int = Query(90, ge=1, le=365),
    warehouse_id: Optional[uuid.UUID] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    params: dict = {"tid": tid, "days": days_ahead}
    wh_filter = ""
    if warehouse_id:
        wh_filter = " AND lot.warehouse_id=:wid"
        params["wid"] = str(warehouse_id)

    r = await db.execute(text(f"""
        SELECT lot.id AS lot_id, lot.lot_number, lot.expiry_date,
               lot.qty_on_hand, lot.unit_cost,
               (lot.qty_on_hand * lot.unit_cost) AS total_value,
               (lot.expiry_date - CURRENT_DATE)  AS days_to_expiry,
               CASE
                 WHEN lot.expiry_date < CURRENT_DATE THEN 'منتهية'
                 WHEN lot.expiry_date - CURRENT_DATE <= 30 THEN 'خلال 30 يوم'
                 WHEN lot.expiry_date - CURRENT_DATE <= 90 THEN 'خلال 90 يوم'
                 ELSE 'سليم'
               END AS expiry_status,
               i.item_code, i.item_name, u.uom_name,
               w.warehouse_name
        FROM inv_lots lot
        JOIN inv_items i ON i.id = lot.item_id
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        LEFT JOIN inv_warehouses w ON w.id = lot.warehouse_id
        WHERE lot.tenant_id=:tid
          AND lot.expiry_date IS NOT NULL
          AND lot.qty_on_hand > 0
          AND lot.expiry_date <= CURRENT_DATE + (:days * INTERVAL '1 day')
          {wh_filter}
        ORDER BY lot.expiry_date ASC
    """), params)
    rows = _to_float(r.fetchall(), "qty_on_hand", "unit_cost", "total_value")
    return ok(data=rows)


# ═══════════════════════════════════════════════════════════════════════════
# 6. NEGATIVE STOCK
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/negative-stock")
async def negative_stock(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT i.item_code, i.item_name, u.uom_name,
               w.warehouse_name,
               b.qty_on_hand, b.avg_cost, b.total_value, b.last_movement
        FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        JOIN inv_warehouses w ON w.id = b.warehouse_id
        WHERE b.tenant_id=:tid AND b.qty_on_hand < 0
        ORDER BY b.qty_on_hand ASC
    """), {"tid": tid})
    rows = _to_float(r.fetchall(), "qty_on_hand", "avg_cost", "total_value")
    return ok(data=rows)


# ═══════════════════════════════════════════════════════════════════════════
# 7. BELOW REORDER
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/below-reorder")
async def below_reorder(
    warehouse_id: Optional[uuid.UUID] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    params: dict = {"tid": tid}
    wh_filter = ""
    if warehouse_id:
        wh_filter = " AND b.warehouse_id=:wid"
        params["wid"] = str(warehouse_id)

    r = await db.execute(text(f"""
        SELECT i.id AS item_id, i.item_code, i.item_name, u.uom_name,
               i.min_qty, i.reorder_point, i.reorder_qty, i.max_qty,
               COALESCE(SUM(b.qty_on_hand), 0) AS qty_on_hand,
               (i.reorder_point - COALESCE(SUM(b.qty_on_hand), 0)) AS shortage_qty,
               s.party_name AS preferred_supplier
        FROM inv_items i
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid{wh_filter}
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        LEFT JOIN parties s ON s.id = i.preferred_supplier_id
        WHERE i.tenant_id=:tid AND i.is_active=true
          AND i.reorder_point > 0
        GROUP BY i.id, i.item_code, i.item_name, u.uom_name,
                 i.min_qty, i.reorder_point, i.reorder_qty, i.max_qty, s.party_name
        HAVING COALESCE(SUM(b.qty_on_hand), 0) <= i.reorder_point
        ORDER BY (i.reorder_point - COALESCE(SUM(b.qty_on_hand), 0)) DESC
    """), params)
    rows = _to_float(
        r.fetchall(),
        "min_qty", "reorder_point", "reorder_qty", "max_qty",
        "qty_on_hand", "shortage_qty",
    )
    return ok(data=rows)


# ═══════════════════════════════════════════════════════════════════════════
# 8. SLOW MOVING (low turnover)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/slow-moving")
async def slow_moving(
    days_window: int = Query(90, ge=7, le=730),
    max_movements: int = Query(3, ge=0, le=100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    cutoff = date.today() - timedelta(days=days_window)
    r = await db.execute(text("""
        SELECT i.item_code, i.item_name, u.uom_name,
               COALESCE(SUM(b.qty_on_hand), 0)  AS qty_on_hand,
               COALESCE(SUM(b.total_value), 0)  AS total_value,
               (SELECT COUNT(*) FROM inv_ledger l
                WHERE l.item_id=i.id AND l.tenant_id=:tid
                  AND l.tx_date >= :cutoff AND l.qty_out > 0) AS movements_in_window,
               (SELECT MAX(l.tx_date) FROM inv_ledger l
                WHERE l.item_id=i.id AND l.tenant_id=:tid AND l.qty_out > 0) AS last_sale
        FROM inv_items i
        LEFT JOIN inv_balances b ON b.item_id = i.id AND b.tenant_id=:tid
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        WHERE i.tenant_id=:tid AND i.is_active=true
        GROUP BY i.id, i.item_code, i.item_name, u.uom_name
        HAVING COALESCE(SUM(b.qty_on_hand),0) > 0
           AND (SELECT COUNT(*) FROM inv_ledger l
                WHERE l.item_id=i.id AND l.tenant_id=:tid
                  AND l.tx_date >= :cutoff AND l.qty_out > 0) <= :mm
        ORDER BY total_value DESC
    """), {"tid": tid, "cutoff": cutoff, "mm": max_movements})
    rows = _to_float(r.fetchall(), "qty_on_hand", "total_value", "movements_in_window")
    return ok(data={
        "items": rows,
        "criteria": {
            "days_window": days_window,
            "max_movements": max_movements,
            "cutoff_date": cutoff.isoformat(),
        },
    })


# ═══════════════════════════════════════════════════════════════════════════
# 9. DEAD STOCK
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/dead-stock")
async def dead_stock(
    days_threshold: int = Query(180, ge=30, le=1095),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    cutoff = date.today() - timedelta(days=days_threshold)
    r = await db.execute(text("""
        SELECT i.item_code, i.item_name, u.uom_name,
               w.warehouse_name,
               b.qty_on_hand, b.avg_cost, b.total_value,
               b.last_movement,
               (CURRENT_DATE - b.last_movement) AS days_idle
        FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        LEFT JOIN inv_uom u ON u.id = i.uom_id
        JOIN inv_warehouses w ON w.id = b.warehouse_id
        WHERE b.tenant_id=:tid
          AND b.qty_on_hand > 0
          AND (b.last_movement IS NULL OR b.last_movement <= :cutoff)
        ORDER BY b.total_value DESC
    """), {"tid": tid, "cutoff": cutoff})
    rows = _to_float(r.fetchall(), "qty_on_hand", "avg_cost", "total_value")
    total_locked = sum(r.get("total_value") or 0 for r in rows)
    return ok(data={
        "items": rows,
        "criteria": {"days_threshold": days_threshold, "cutoff_date": cutoff.isoformat()},
        "total_locked_value": total_locked,
    })


# ═══════════════════════════════════════════════════════════════════════════
# 10. TURNOVER
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/turnover")
async def turnover(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid", "l.qty_out > 0"]
    params: dict = {"tid": tid}
    if date_from:
        conds.append("l.tx_date >= :df"); params["df"] = date_from
    if date_to:
        conds.append("l.tx_date <= :dt"); params["dt"] = date_to
    where = " AND ".join(conds)

    r = await db.execute(text(f"""
        SELECT i.item_code, i.item_name,
               SUM(l.total_cost)                AS cogs,
               AVG(COALESCE(b.total_value, 0))  AS avg_inventory,
               CASE
                 WHEN AVG(COALESCE(b.total_value,0)) > 0
                 THEN SUM(l.total_cost) / AVG(COALESCE(b.total_value, 0))
                 ELSE 0
               END AS turnover_rate
        FROM inv_ledger l
        JOIN inv_items i ON i.id = l.item_id
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
        WHERE {where}
        GROUP BY i.id, i.item_code, i.item_name
        ORDER BY turnover_rate DESC
    """), params)
    items = _to_float(r.fetchall(), "cogs", "avg_inventory", "turnover_rate")
    total_cogs = sum(i.get("cogs") or 0 for i in items)
    total_inv  = sum(i.get("avg_inventory") or 0 for i in items)
    return ok(data={
        "items": items,
        "summary": {
            "overall_turnover": round(total_cogs / total_inv, 2) if total_inv > 0 else 0,
            "total_cogs": round(total_cogs, 2),
            "total_avg_inventory": round(total_inv, 2),
        },
    })


# ═══════════════════════════════════════════════════════════════════════════
# 11. ABC ANALYSIS (Pareto)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/abc-analysis")
async def abc_analysis(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    تصنيف ABC للأصناف حسب قيمة المبيعات (COGS) خلال الفترة.
    A = أعلى 80% من القيمة (أهم الأصناف)
    B = 80–95%
    C = 95–100%
    """
    tid = str(user.tenant_id)
    if not date_from:
        date_from = date.today() - timedelta(days=365)
    if not date_to:
        date_to = date.today()

    r = await db.execute(text("""
        SELECT i.id AS item_id, i.item_code, i.item_name,
               COALESCE(SUM(l.total_cost), 0) AS total_cogs,
               COALESCE(SUM(l.qty_out), 0)    AS total_qty_sold
        FROM inv_items i
        LEFT JOIN inv_ledger l
          ON l.item_id = i.id AND l.tenant_id=:tid
         AND l.qty_out > 0
         AND l.tx_date BETWEEN :df AND :dt
        WHERE i.tenant_id=:tid AND i.is_active=true
        GROUP BY i.id, i.item_code, i.item_name
        ORDER BY total_cogs DESC
    """), {"tid": tid, "df": date_from, "dt": date_to})

    items = []
    grand_total = Decimal(0)
    for row in r.fetchall():
        d = dict(row._mapping)
        d["total_cogs"] = float(d["total_cogs"] or 0)
        d["total_qty_sold"] = float(d["total_qty_sold"] or 0)
        grand_total += Decimal(str(d["total_cogs"]))
        items.append(d)

    # Compute cumulative %
    grand_total_f = float(grand_total) or 1
    cumulative = 0.0
    for i in items:
        cumulative += i["total_cogs"]
        cum_pct = (cumulative / grand_total_f) * 100
        i["cumulative_pct"] = round(cum_pct, 2)
        if cum_pct <= 80:
            i["abc_class"] = "A"
        elif cum_pct <= 95:
            i["abc_class"] = "B"
        else:
            i["abc_class"] = "C"

    return ok(data={
        "items": items,
        "summary": {
            "A_count": sum(1 for x in items if x["abc_class"] == "A"),
            "B_count": sum(1 for x in items if x["abc_class"] == "B"),
            "C_count": sum(1 for x in items if x["abc_class"] == "C"),
            "total_cogs": round(grand_total_f, 2),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        },
    })


# ═══════════════════════════════════════════════════════════════════════════
# 12. PARTY STATEMENT — UNIVERSAL SUBSIDIARY LEDGER ⭐
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/party-statement")
async def party_statement(
    party_id: uuid.UUID,
    item_id: Optional[uuid.UUID] = None,
    warehouse_id: Optional[uuid.UUID] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    party_role: Optional[str] = None,
    branch_code: Optional[str] = None,
    cost_center_code: Optional[str] = None,
    project_code: Optional[str] = None,
    reason_code: Optional[str] = None,
    limit: int = Query(1000, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    ⭐ Universal Subsidiary Ledger — لا يوجد في Odoo/SAP/NetSuite
    
    يعرض كل حركات المخزون المرتبطة بطرف محدد (مورد/عميل/موظف/فرع/مشروع/إلخ).
    يدعم تصفية متعددة الأبعاد:
      • طرف + صنف   → كرت الصنف للمورد (كم اشترينا منه؟)
      • طرف + مشروع → استهلاك المخزون لمشروع
      • طرف + سبب  → عوادم/فروقات لمورد معين
    """
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid", "l.party_id=:pid"]
    params: dict = {"tid": tid, "pid": str(party_id), "limit": limit}

    if item_id:
        conds.append("l.item_id=:iid"); params["iid"] = str(item_id)
    if warehouse_id:
        conds.append("l.warehouse_id=:wid"); params["wid"] = str(warehouse_id)
    if date_from:
        conds.append("l.tx_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("l.tx_date<=:dt"); params["dt"] = date_to
    if party_role:
        conds.append("l.party_role=:pr"); params["pr"] = party_role
    if branch_code:
        conds.append("l.branch_code=:br"); params["br"] = branch_code
    if cost_center_code:
        conds.append("l.cost_center_code=:cc"); params["cc"] = cost_center_code
    if project_code:
        conds.append("l.project_code=:prj"); params["prj"] = project_code
    if reason_code:
        conds.append("l.reason_code=:rc"); params["rc"] = reason_code

    where = " AND ".join(conds)

    r = await db.execute(text(f"""
        SELECT l.tx_date, l.tx_type,
               l.qty_in, l.qty_out, l.unit_cost, l.total_cost,
               l.balance_qty, l.balance_cost,
               l.party_id, l.party_role, l.party_name_snapshot,
               l.branch_code, l.cost_center_code, l.project_code,
               l.reason_code, l.lot_number, l.serial_number,
               l.reference_id, l.reference_type,
               i.item_code, i.item_name,
               w.warehouse_name,
               t.serial AS tx_serial,
               rc.name_ar AS reason_name
        FROM inv_ledger l
        JOIN inv_items      i  ON i.id = l.item_id
        LEFT JOIN inv_warehouses w  ON w.id = l.warehouse_id
        LEFT JOIN inv_transactions t ON t.id = l.reference_id
        LEFT JOIN inv_reason_codes rc
               ON rc.tenant_id = l.tenant_id AND rc.code = l.reason_code
        WHERE {where}
        ORDER BY l.tx_date, l.created_at
        LIMIT :limit
    """), params)
    rows = _to_float(
        r.fetchall(),
        "qty_in", "qty_out", "unit_cost", "total_cost",
        "balance_qty", "balance_cost",
    )

    # Header info
    rp = await db.execute(text("""
        SELECT id, name, party_type, tax_number, code
        FROM parties WHERE id=:pid AND tenant_id=:tid
    """), {"pid": str(party_id), "tid": tid})
    pt = rp.fetchone()
    party_info = dict(pt._mapping) if pt else None

    # Aggregates
    total_qty_in = sum(r.get("qty_in") or 0 for r in rows)
    total_qty_out = sum(r.get("qty_out") or 0 for r in rows)
    total_value_in = sum((r.get("total_cost") or 0) for r in rows if (r.get("qty_in") or 0) > 0)
    total_value_out = sum((r.get("total_cost") or 0) for r in rows if (r.get("qty_out") or 0) > 0)

    return ok(data={
        "party": party_info,
        "movements": rows,
        "summary": {
            "total_movements": len(rows),
            "total_qty_in": total_qty_in,
            "total_qty_out": total_qty_out,
            "total_value_in": round(total_value_in, 2),
            "total_value_out": round(total_value_out, 2),
            "net_value": round(total_value_in - total_value_out, 2),
        },
        "filters": {
            "item_id": str(item_id) if item_id else None,
            "warehouse_id": str(warehouse_id) if warehouse_id else None,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "party_role": party_role,
            "branch_code": branch_code,
            "cost_center_code": cost_center_code,
            "project_code": project_code,
            "reason_code": reason_code,
        },
    })


# ═══════════════════════════════════════════════════════════════════════════
# 13. BY DIMENSION (branch/cost_center/project)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/by-dimension")
async def by_dimension(
    dimension: str = Query(..., pattern="^(branch|cost_center|project|reason)$"),
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    تجميع حركات المخزون حسب البُعد:
      • branch       → حركات الفرع
      • cost_center  → حركات مركز التكلفة
      • project      → حركات المشروع
      • reason       → حركات حسب السبب (تالف/مسروق/إلخ)
    """
    tid = str(user.tenant_id)
    col_map = {
        "branch":      "l.branch_code",
        "cost_center": "l.cost_center_code",
        "project":     "l.project_code",
        "reason":      "l.reason_code",
    }
    col = col_map[dimension]

    conds = ["l.tenant_id=:tid", f"{col} IS NOT NULL"]
    params: dict = {"tid": tid}
    if date_from:
        conds.append("l.tx_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("l.tx_date<=:dt"); params["dt"] = date_to
    where = " AND ".join(conds)

    r = await db.execute(text(f"""
        SELECT {col} AS dim_value,
               COUNT(*)               AS movement_count,
               SUM(l.qty_in)          AS total_qty_in,
               SUM(l.qty_out)         AS total_qty_out,
               SUM(CASE WHEN l.qty_in > 0 THEN l.total_cost ELSE 0 END) AS total_value_in,
               SUM(CASE WHEN l.qty_out > 0 THEN l.total_cost ELSE 0 END) AS total_value_out
        FROM inv_ledger l
        WHERE {where}
        GROUP BY {col}
        ORDER BY total_value_out DESC NULLS LAST
    """), params)
    rows = _to_float(
        r.fetchall(),
        "movement_count", "total_qty_in", "total_qty_out",
        "total_value_in", "total_value_out",
    )
    return ok(data={"dimension": dimension, "groups": rows})


# ═══════════════════════════════════════════════════════════════════════════
# 14. COGS
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/cogs")
async def cogs(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid", "l.qty_out>0"]
    params: dict = {"tid": tid}
    if date_from:
        conds.append("l.tx_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("l.tx_date<=:dt"); params["dt"] = date_to
    where = " AND ".join(conds)

    r = await db.execute(text(f"""
        SELECT i.item_code, i.item_name,
               SUM(l.qty_out)                       AS qty_sold,
               AVG(NULLIF(l.unit_cost, 0))          AS avg_cost,
               SUM(l.total_cost)                    AS total_cogs
        FROM inv_ledger l
        JOIN inv_items i ON i.id = l.item_id
        WHERE {where}
        GROUP BY i.id, i.item_code, i.item_name
        ORDER BY total_cogs DESC
    """), params)
    rows = _to_float(r.fetchall(), "qty_sold", "avg_cost", "total_cogs")
    total = sum(r.get("total_cogs") or 0 for r in rows)
    return ok(data={"items": rows, "total_cogs": round(total, 2)})


# ═══════════════════════════════════════════════════════════════════════════
# 15. VARIANCE (count results)
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/variance")
async def variance(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["s.tenant_id=:tid", "s.status='posted'"]
    params: dict = {"tid": tid}
    if date_from:
        conds.append("s.count_date>=:df"); params["df"] = date_from
    if date_to:
        conds.append("s.count_date<=:dt"); params["dt"] = date_to
    where = " AND ".join(conds)

    r = await db.execute(text(f"""
        SELECT s.id, s.serial, s.count_date, s.warehouse_id,
               s.items_with_variance,
               s.total_overage_value, s.total_shortage_value,
               s.overage_je_serial, s.shortage_je_serial,
               s.branch_code, s.cost_center_code, s.project_code,
               w.warehouse_name
        FROM inv_count_sessions s
        LEFT JOIN inv_warehouses w ON w.id = s.warehouse_id
        WHERE {where}
        ORDER BY s.count_date DESC
    """), params)
    sessions = _to_float(
        r.fetchall(),
        "items_with_variance", "total_overage_value", "total_shortage_value",
    )

    for sess in sessions:
        r2 = await db.execute(text("""
            SELECT i.item_code, i.item_name, l.system_qty, l.actual_qty,
                   COALESCE(l.actual_qty, 0) - COALESCE(l.system_qty, 0) AS variance,
                   l.unit_cost, l.variance_value
            FROM inv_count_lines l
            JOIN inv_items i ON i.id = l.item_id
            WHERE l.session_id=:sid AND l.tenant_id=:tid
              AND l.actual_qty IS NOT NULL
              AND COALESCE(l.actual_qty, 0) <> COALESCE(l.system_qty, 0)
        """), {"sid": str(sess["id"]), "tid": tid})
        sess["lines"] = _to_float(
            r2.fetchall(),
            "system_qty", "actual_qty", "variance", "unit_cost", "variance_value",
        )

    return ok(data=sessions)
