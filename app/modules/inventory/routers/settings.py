"""
app/modules/inventory/routers/settings.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Settings Router
═══════════════════════════════════════════════════════════════════════════
الإعدادات والصحة العامة للموديول:
  1. Account Settings        — حسابات الترحيل لكل tx_type
  2. Numbering Series         — تسلسل ترقيم المستندات (jl_numbering_series)
  3. Dashboard                — KPIs ومؤشرات سريعة
  4. Health Check             — التحقق من جاهزية الموديول
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

from app.modules.inventory.helpers import DEFAULT_TX_ACCOUNTS


router = APIRouter(prefix="/inventory/settings", tags=["inventory-settings"])


# ═══════════════════════════════════════════════════════════════════════════
# 1. ACCOUNT SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
TX_TYPES_ALL = list(DEFAULT_TX_ACCOUNTS.keys())


@router.get("/accounts")
async def list_account_settings(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    يُرجع كل إعدادات الحسابات لكل tx_type. إن لم يوجد إعداد، يُرجع الافتراضيات.
    """
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT tx_type, debit_account, credit_account, description
        FROM inv_account_settings
        WHERE tenant_id=:tid
    """), {"tid": tid})
    saved = {row[0]: dict(row._mapping) for row in r.fetchall()}

    # Merge with defaults so UI always has all tx_types
    out = []
    for tx_type in TX_TYPES_ALL:
        if tx_type in saved:
            row = saved[tx_type]
            out.append({
                "tx_type": tx_type,
                "debit_account": row.get("debit_account") or DEFAULT_TX_ACCOUNTS[tx_type][0],
                "credit_account": row.get("credit_account") or DEFAULT_TX_ACCOUNTS[tx_type][1],
                "description": row.get("description") or DEFAULT_TX_ACCOUNTS[tx_type][2],
                "is_default": False,
            })
        else:
            d, c, desc = DEFAULT_TX_ACCOUNTS[tx_type]
            out.append({
                "tx_type": tx_type,
                "debit_account": d,
                "credit_account": c,
                "description": desc,
                "is_default": True,
            })
    return ok(data=out)


@router.put("/accounts/{tx_type}")
async def update_account_setting(
    tx_type: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    if tx_type not in TX_TYPES_ALL:
        raise HTTPException(400, f"نوع الحركة '{tx_type}' غير معروف")

    debit = data.get("debit_account")
    credit = data.get("credit_account")

    # Validate accounts exist in CoA (skip if both empty)
    for label, code in [("Debit", debit), ("Credit", credit)]:
        if code:
            rc = await db.execute(text("""
                SELECT account_code, account_name, is_active
                FROM coa_accounts
                WHERE tenant_id=:tid AND account_code=:c
            """), {"tid": tid, "c": code})
            row = rc.fetchone()
            if not row:
                raise HTTPException(400, f"{label} account '{code}' غير موجود في دليل الحسابات")
            if not row[2]:
                raise HTTPException(400, f"{label} account '{code}' غير مفعّل")

    await db.execute(text("""
        INSERT INTO inv_account_settings (
            id, tenant_id, tx_type, debit_account, credit_account, description
        ) VALUES (
            gen_random_uuid(), :tid, :tt, :dr, :cr, :desc
        )
        ON CONFLICT (tenant_id, tx_type) DO UPDATE SET
            debit_account  = EXCLUDED.debit_account,
            credit_account = EXCLUDED.credit_account,
            description    = EXCLUDED.description
    """), {
        "tid": tid, "tt": tx_type,
        "dr": debit, "cr": credit,
        "desc": data.get("description"),
    })
    await db.commit()
    return ok(
        data={"tx_type": tx_type, "debit_account": debit, "credit_account": credit},
        message="✅ تم حفظ الإعدادات",
    )


@router.delete("/accounts/{tx_type}", status_code=204)
async def reset_account_setting(
    tx_type: str,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """يُعيد إعدادات tx_type للقيم الافتراضية بحذف السجل المخصص."""
    tid = str(user.tenant_id)
    await db.execute(text("""
        DELETE FROM inv_account_settings
        WHERE tenant_id=:tid AND tx_type=:tt
    """), {"tid": tid, "tt": tx_type})
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# 2. NUMBERING SERIES — jl_numbering_series
# ═══════════════════════════════════════════════════════════════════════════
INV_SERIES_TYPES = [
    "GRN", "GIN", "GIT", "IJ", "SCRAP",
    "RETURN_IN", "RETURN_OUT", "OPENING", "PIC",
]


@router.get("/numbering-series")
async def list_numbering_series(
    year: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    قائمة تسلسلات الترقيم لسنة معيّنة.
    
    يُرجع:
      • الـ 9 أنواع الموصى بها (recommended) — حتى لو لم تُحفظ بعد (defaults)
      • + أيّ أنواع مخصّصة (custom) محفوظة في DB
    
    كل نوع له is_default + is_recommended:
      • is_recommended=True  → نوع موصى به للمخزون
      • is_recommended=False → نوع مخصّص أضافه المستخدم
      • is_default=True       → القيم الافتراضية (لم يُحفظ في DB بعد)
      • is_default=False      → مُعدَّل ومحفوظ في DB
    """
    tid = str(user.tenant_id)
    cur_year = year or date.today().year
    
    # احصل على كل التسلسلات الموجودة في DB لهذا التينانت + السنة
    r = await db.execute(text("""
        SELECT series_type, year, prefix_format, next_serial, padding_width,
               created_at, updated_at
        FROM jl_numbering_series
        WHERE tenant_id=:tid AND year=:yr
        ORDER BY series_type
    """), {"tid": tid, "yr": cur_year})
    saved = {row[0]: dict(row._mapping) for row in r.fetchall()}

    out = []
    seen = set()
    
    # 1) الأنواع الموصى بها (حتى لو لم تُحفظ بعد)
    for st in INV_SERIES_TYPES:
        seen.add(st)
        if st in saved:
            d = saved[st]
            d["is_default"] = False
            d["is_recommended"] = True
            out.append(d)
        else:
            out.append({
                "series_type": st,
                "year": cur_year,
                "prefix_format": f"{st}-{{year}}-{{serial:07d}}",
                "next_serial": 1,
                "padding_width": 7,
                "is_default": True,
                "is_recommended": True,
            })
    
    # 2) الأنواع المخصّصة (موجودة في DB لكن ليست في الموصى بها)
    for st, d in saved.items():
        if st not in seen:
            d["is_default"] = False
            d["is_recommended"] = False
            out.append(d)
    
    return ok(data={"year": cur_year, "series": out})


@router.put("/numbering-series/{series_type}")
async def update_numbering_series(
    series_type: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    إنشاء أو تعديل تسلسل ترقيم.
    
    يقبل أيّ series_type جديد (مرونة كاملة) مع validation:
      • حروف لاتينية كبيرة فقط (A-Z) + underscore
      • 2-15 حرف
      • لا يبدأ برقم
    
    ملاحظة: لم تعد هناك whitelist ثابتة. أي نوع جديد مسموح.
    """
    import re
    
    tid = str(user.tenant_id)
    
    # Validation: حروف لاتينية كبيرة فقط + underscore
    if not re.match(r'^[A-Z][A-Z0-9_]{1,14}$', series_type):
        raise HTTPException(
            400,
            "series_type يجب أن يكون 2-15 حرف، حروف لاتينية كبيرة فقط (A-Z, 0-9, _)، ويبدأ بحرف"
        )

    yr = int(data.get("year") or date.today().year)
    prefix = data.get("prefix_format") or f"{series_type}-{{year}}-{{serial:07d}}"
    next_seq = int(data.get("next_serial", 1))
    padding = int(data.get("padding_width", 7))

    # Validate prefix has serial placeholder
    if "{serial" not in prefix:
        raise HTTPException(400, "prefix_format يجب أن يحتوي على {serial:0Nd}")
    
    # Validate next_serial >= 1
    if next_seq < 1:
        raise HTTPException(400, "next_serial يجب أن يكون 1 أو أكثر")

    await db.execute(text("""
        INSERT INTO jl_numbering_series (
            id, tenant_id, series_type, year, prefix_format,
            next_serial, padding_width, created_at, updated_at
        ) VALUES (
            gen_random_uuid(), :tid, :st, :yr, :pf, :ns, :pw, NOW(), NOW()
        )
        ON CONFLICT (tenant_id, series_type, year) DO UPDATE SET
            prefix_format = EXCLUDED.prefix_format,
            next_serial   = EXCLUDED.next_serial,
            padding_width = EXCLUDED.padding_width,
            updated_at    = NOW()
    """), {
        "tid": tid, "st": series_type, "yr": yr,
        "pf": prefix, "ns": next_seq, "pw": padding,
    })
    await db.commit()
    return ok(
        data={"series_type": series_type, "year": yr, "next_serial": next_seq},
        message="✅ تم حفظ تسلسل الترقيم",
    )


@router.delete("/numbering-series/{series_type}", status_code=204)
async def reset_numbering_series(
    series_type: str,
    year: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """يُعيد تسلسل الترقيم للقيم الافتراضية بحذف السجل المُخصَّص."""
    tid = str(user.tenant_id)
    yr = year or date.today().year
    await db.execute(text("""
        DELETE FROM jl_numbering_series
        WHERE tenant_id=:tid AND series_type=:st AND year=:yr
    """), {"tid": tid, "st": series_type, "yr": yr})
    await db.commit()


# ═══════════════════════════════════════════════════════════════════════════
# 3. DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/dashboard")
async def dashboard(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    KPIs شاملة للموديول مع معالجة دفاعية للأخطاء.
    لو فشل query واحد، الباقي يستمر، ويُرجع قائمة الأخطاء في 'errors'.
    هذا يساعد على التشخيص بدلاً من 500 صامت.
    """
    tid = str(user.tenant_id)
    today = date.today()
    thirty_ago = today - timedelta(days=30)
    seven_ago  = today - timedelta(days=7)

    errors: list = []

    # ─── helper: safe execute ─────────────────────────────────────
    async def _safe(name: str, sql: str, params: dict, default):
        try:
            r = await db.execute(text(sql), params)
            return r
        except Exception as e:
            err_msg = name + ": " + type(e).__name__ + ": " + str(e)
            errors.append(err_msg)
            return None

    # ─── 1. Master data counts ────────────────────────────────────
    master: dict = {
        "items_active": 0, "categories": 0, "brands": 0,
        "warehouses": 0, "zones": 0, "locations": 0,
        "uoms": 0, "uom_conversions": 0, "reason_codes": 0,
        "attributes": 0, "active_lots": 0, "active_serials": 0,
    }
    r = await _safe("master_counts", """
        SELECT
          (SELECT COUNT(*) FROM inv_items      WHERE tenant_id=:tid AND is_active=true) AS items_active,
          (SELECT COUNT(*) FROM inv_categories WHERE tenant_id=:tid) AS categories,
          (SELECT COUNT(*) FROM inv_brands     WHERE tenant_id=:tid AND is_active=true) AS brands,
          (SELECT COUNT(*) FROM inv_warehouses WHERE tenant_id=:tid AND is_active=true) AS warehouses,
          (SELECT COUNT(*) FROM inv_zones      WHERE tenant_id=:tid AND is_active=true) AS zones,
          (SELECT COUNT(*) FROM inv_locations  WHERE tenant_id=:tid AND is_active=true) AS locations,
          (SELECT COUNT(*) FROM inv_uom        WHERE tenant_id=:tid AND is_active=true) AS uoms,
          (SELECT COUNT(*) FROM inv_uom_conversions WHERE tenant_id=:tid) AS uom_conversions,
          (SELECT COUNT(*) FROM inv_reason_codes    WHERE tenant_id=:tid AND is_active=true) AS reason_codes,
          (SELECT COUNT(*) FROM inv_item_attributes WHERE tenant_id=:tid AND is_active=true) AS attributes,
          (SELECT COUNT(*) FROM inv_lots        WHERE tenant_id=:tid AND qty_on_hand>0) AS active_lots,
          (SELECT COUNT(*) FROM inv_serials     WHERE tenant_id=:tid AND status='in_stock') AS active_serials
    """, {"tid": tid}, master)
    if r is not None:
        try:
            master = dict(r.fetchone()._mapping)
        except Exception as e:
            errors.append("master_fetch: " + str(e))

    # ─── 2. Stock totals ──────────────────────────────────────────
    stock: dict = {"total_qty": 0, "total_value": 0, "avg_cost": 0, "negative_count": 0, "zero_count": 0}
    rs = await _safe("stock_totals", """
        SELECT
          COALESCE(SUM(qty_on_hand), 0)                              AS total_qty,
          COALESCE(SUM(total_value), 0)                              AS total_value,
          COALESCE(AVG(NULLIF(avg_cost, 0)), 0)                      AS avg_cost,
          COUNT(*) FILTER (WHERE qty_on_hand < 0)                    AS negative_count,
          COUNT(*) FILTER (WHERE qty_on_hand = 0)                    AS zero_count
        FROM inv_balances WHERE tenant_id=:tid
    """, {"tid": tid}, stock)
    if rs is not None:
        try:
            stock = dict(rs.fetchone()._mapping)
        except Exception as e:
            errors.append("stock_fetch: " + str(e))

    # ─── 3. Below reorder ─────────────────────────────────────────
    below_reorder_count = 0
    rb = await _safe("below_reorder", """
        SELECT COUNT(DISTINCT i.id) AS cnt
        FROM inv_items i
        LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
        WHERE i.tenant_id=:tid AND i.is_active=true AND i.reorder_point > 0
        GROUP BY i.id, i.reorder_point
        HAVING COALESCE(SUM(b.qty_on_hand), 0) <= i.reorder_point
    """, {"tid": tid}, 0)
    if rb is not None:
        try:
            below_reorder_count = len(rb.fetchall())
        except Exception as e:
            errors.append("below_reorder_fetch: " + str(e))

    # ─── 4. Expiring soon (90 days) ───────────────────────────────
    expiring_count = 0
    re_ = await _safe("expiring", """
        SELECT COUNT(*) FROM inv_lots
        WHERE tenant_id=:tid AND qty_on_hand>0
          AND expiry_date IS NOT NULL
          AND expiry_date <= CURRENT_DATE + INTERVAL '90 days'
    """, {"tid": tid}, 0)
    if re_ is not None:
        try:
            expiring_count = re_.scalar() or 0
        except Exception as e:
            errors.append("expiring_fetch: " + str(e))

    # ─── 5. COGS last 30 days for turnover ────────────────────────
    cogs_30d = 0.0
    rc = await _safe("cogs_30d", """
        SELECT COALESCE(SUM(total_cost), 0) AS cogs
        FROM inv_ledger
        WHERE tenant_id=:tid AND qty_out>0 AND tx_date>=:since
    """, {"tid": tid, "since": thirty_ago}, 0)
    if rc is not None:
        try:
            cogs_30d = float(rc.scalar() or 0)
        except Exception as e:
            errors.append("cogs_fetch: " + str(e))

    total_val = float(stock.get("total_value") or 0)
    turnover_rate = round((cogs_30d / total_val * 12) if total_val > 0 else 0, 2)

    # ─── 6. Transactions activity ─────────────────────────────────
    tx_stats: dict = {"pending": 0, "posted_total": 0, "posted_last_7d": 0}
    rt = await _safe("tx_stats", """
        SELECT
          COUNT(*) FILTER (WHERE status='draft')                    AS pending,
          COUNT(*) FILTER (WHERE status='posted')                   AS posted_total,
          COUNT(*) FILTER (WHERE status='posted' AND tx_date>=:s7)  AS posted_last_7d
        FROM inv_transactions WHERE tenant_id=:tid
    """, {"tid": tid, "s7": seven_ago}, tx_stats)
    if rt is not None:
        try:
            tx_stats = dict(rt.fetchone()._mapping)
        except Exception as e:
            errors.append("tx_stats_fetch: " + str(e))

    # ─── 7. Top 5 by value ────────────────────────────────────────
    top_items: list = []
    rtop = await _safe("top_items", """
        SELECT i.item_code, i.item_name, b.qty_on_hand, b.total_value
        FROM inv_balances b
        JOIN inv_items i ON i.id = b.item_id
        WHERE b.tenant_id=:tid
        ORDER BY b.total_value DESC NULLS LAST
        LIMIT 5
    """, {"tid": tid}, [])
    if rtop is not None:
        try:
            for row in rtop.fetchall():
                d = dict(row._mapping)
                for k in ("qty_on_hand", "total_value"):
                    if d.get(k) is not None:
                        d[k] = float(d[k])
                top_items.append(d)
        except Exception as e:
            errors.append("top_items_fetch: " + str(e))

    return ok(data={
        "kpis": {
            "total_value": round(total_val, 2),
            "total_qty": float(stock.get("total_qty") or 0),
            "items_active": master.get("items_active", 0),
            "warehouses": master.get("warehouses", 0),
            "below_reorder_count": below_reorder_count,
            "negative_stock_count": stock.get("negative_count", 0),
            "expiring_soon_count": expiring_count,
            "turnover_rate": turnover_rate,
            "pending_transactions": tx_stats.get("pending", 0),
            "posted_last_7d": tx_stats.get("posted_last_7d", 0),
        },
        "master_data": master,
        "stock": {
            "total_qty": float(stock.get("total_qty") or 0),
            "total_value": round(total_val, 2),
            "avg_cost": float(stock.get("avg_cost") or 0),
            "negative_count": stock.get("negative_count", 0),
            "zero_count": stock.get("zero_count", 0),
        },
        "alerts": {
            "below_reorder_count": below_reorder_count,
            "negative_stock_count": stock.get("negative_count", 0),
            "expiring_soon_count": expiring_count,
        },
        "top_items": top_items,
        "errors": errors,  # ⚠️ يُرجع قائمة الأخطاء للتشخيص
    })


# ═══════════════════════════════════════════════════════════════════════════
# 4. HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/health")
async def health_check(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    🩺 HealthCheck v2.0 — فحص صحّة شامل بـ 3 طبقات.
    
    Layer 1: Schema Compatibility — هل الجداول والأعمدة موجودة؟
    Layer 2: Data Integrity — هل البيانات منطقيّة؟
    Layer 3: Business Logic — هل المنطق متطابق؟
    
    كل فحص يأتي مع:
      - status: ok | warn | fail
      - message: رسالة عربية واضحة
      - layer: رقم الطبقة
      - suggestion: SQL مُقترَح للإصلاح (لو متوفّر)
      - tip: شرح مختصر للمستخدم
    """
    tid = str(user.tenant_id)
    checks = []
    overall_ok = True

    # ─── helper آمن ────────────────────────────────────────────
    def _add_check(name, status, message, layer=1, suggestion=None, tip=None):
        nonlocal overall_ok
        if status == "fail":
            overall_ok = False
        item = {
            "name": name,
            "status": status,
            "message": message,
            "layer": layer,
        }
        if suggestion:
            item["suggestion"] = suggestion
        if tip:
            item["tip"] = tip
        checks.append(item)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 1: Schema Compatibility — وجود الجداول والأعمدة
    # ═══════════════════════════════════════════════════════════════

    # ─── 1.1 Critical tables ───────────────────────────────────
    try:
        critical_tables = [
            "inv_items", "inv_warehouses", "inv_balances", "inv_transactions",
            "inv_transaction_lines", "inv_ledger", "inv_fifo_layers",
            "inv_zones", "inv_locations", "inv_brands", "inv_uom_conversions",
            "inv_reason_codes", "inv_item_attributes", "inv_item_attribute_values",
            "inv_lots", "inv_serials",
            "inv_count_sessions", "inv_count_lines",
            "inv_account_settings", "jl_numbering_series",
            "inv_categories",
        ]
        rt = await db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_name = ANY(:names)
        """), {"names": critical_tables})
        existing_tables = {r[0] for r in rt.fetchall()}
        missing_tables = [t for t in critical_tables if t not in existing_tables]
        if missing_tables:
            _add_check("Critical Tables", "fail",
                       f"جداول ناقصة: {', '.join(missing_tables)}",
                       layer=1,
                       tip="بعض الجداول الأساسية مفقودة — قد يحتاج النظام لـ migration")
        else:
            _add_check("Critical Tables", "ok",
                       f"جميع الجداول الـ {len(critical_tables)} موجودة",
                       layer=1)
    except Exception as e:
        await db.rollback()
        _add_check("Critical Tables", "fail",
                   f"فحص الجداول فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=1)

    # ─── 1.2 Schema Compatibility (Backend ↔ DB columns) ───────
    try:
        EXPECTED_COLUMNS = {
            "inv_items": ["item_code", "item_name", "item_type", "category_id",
                          "uom_id", "is_active", "purchase_price", "sale_price"],
            "inv_warehouses": ["warehouse_code", "warehouse_name", "warehouse_type",
                               "is_active", "is_default"],
            "inv_categories": ["category_code", "category_name", "item_type",
                               "parent_id", "gl_account_code"],
            "inv_balances": ["item_id", "warehouse_id", "qty_on_hand", "total_value"],
            "inv_brands": ["brand_code", "brand_name", "brand_name_en", "is_active"],
            "inv_reason_codes": ["reason_code", "reason_name", "expense_account_code",
                                 "applies_to_tx_types", "is_increase", "is_system",
                                 "is_active"],
            "inv_item_attributes": ["attribute_code", "attribute_name", "display_type",
                                    "is_active", "sort_order"],
            "inv_item_attribute_values": ["attribute_id", "value_code", "value_name",
                                          "color_hex", "sort_order", "is_active"],
            "inv_uom": ["uom_code", "uom_name", "uom_name_en", "is_base",
                        "is_active", "description"],
            "inv_uom_conversions": ["from_uom_id", "to_uom_id", "factor",
                                    "item_id", "is_active", "notes"],
            "inv_zones": ["zone_code", "zone_name", "warehouse_id", "is_active"],
            "inv_locations": ["location_code", "location_name", "warehouse_id",
                              "zone_id", "is_active"],
            "jl_numbering_series": ["series_type", "year", "prefix_format",
                                    "next_serial", "padding_width"],
        }

        rs = await db.execute(text("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name = ANY(:tables)
        """), {"tables": list(EXPECTED_COLUMNS.keys())})
        actual_cols: dict = {}
        for row in rs.fetchall():
            actual_cols.setdefault(row[0], set()).add(row[1])

        mismatches: list = []
        for tbl, expected in EXPECTED_COLUMNS.items():
            if tbl not in actual_cols:
                continue
            missing = [c for c in expected if c not in actual_cols[tbl]]
            if missing:
                mismatches.append(f"{tbl}: {', '.join(missing)}")

        if mismatches:
            _add_check("Schema Compatibility", "warn",
                       f"أعمدة متوقّعة غير موجودة ({len(mismatches)} جدول): "
                       + " | ".join(mismatches[:3]),
                       layer=1,
                       tip="Backend يتوقّع أعمدة معيّنة لكنّها مفقودة — تحتاج migration")
        else:
            _add_check("Schema Compatibility", "ok",
                       f"كل الأعمدة المتوقّعة موجودة ({len(EXPECTED_COLUMNS)} جدول)",
                       layer=1)
    except Exception as e:
        await db.rollback()
        _add_check("Schema Compatibility", "warn",
                   f"فحص توافق Schema فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=1)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 2: Data Integrity — هل البيانات منطقيّة؟
    # ═══════════════════════════════════════════════════════════════

    # ─── 2.1 Categories item_type Diversity ⭐ ─────────────────
    # المشكلة التي اكتشفها فادي: كل categories لها نفس item_type
    try:
        rd = await db.execute(text("""
            SELECT COUNT(DISTINCT item_type) AS distinct_types,
                   COUNT(*) AS total_count
            FROM inv_categories
            WHERE tenant_id=:tid
        """), {"tid": tid})
        row = rd.fetchone()
        if row:
            distinct_types = row[0] or 0
            total = row[1] or 0
            if total > 1 and distinct_types == 1:
                _add_check("Categories item_type Diversity", "warn",
                           f"كل التصنيفات الـ {total} لها نفس item_type — قد تحتاج تنوّع",
                           layer=2,
                           suggestion=(
                               "-- إصلاح مقترح: عيّن item_type مناسب لكل تصنيف\n"
                               "UPDATE inv_categories SET item_type='service' WHERE category_code='SRV';\n"
                               "UPDATE inv_categories SET item_type='consumable' WHERE category_code='CONS';\n"
                               "UPDATE inv_categories SET item_type='raw_material' WHERE category_code='RAW';\n"
                               "UPDATE inv_categories SET item_type='finished' WHERE category_code='FG';"
                           ),
                           tip="عادةً كل تصنيف له نوع مختلف (stock, service, raw_material, ...)")
            elif total == 0:
                _add_check("Categories item_type Diversity", "warn",
                           "لا يوجد تصنيفات بعد — أضف بعض التصنيفات",
                           layer=2)
            else:
                _add_check("Categories item_type Diversity", "ok",
                           f"تنوّع جيّد: {distinct_types} أنواع في {total} تصنيف",
                           layer=2)
    except Exception as e:
        await db.rollback()
        _add_check("Categories item_type Diversity", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=2)

    # ─── 2.2 Orphaned Items (FK مكسورة) ────────────────────────
    try:
        ro = await db.execute(text("""
            SELECT COUNT(*) FROM inv_items i
            WHERE i.tenant_id=:tid
              AND i.category_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM inv_categories c WHERE c.id = i.category_id
              )
        """), {"tid": tid})
        orphans = ro.scalar() or 0
        if orphans > 0:
            _add_check("Orphaned Items (FK)", "fail",
                       f"❌ {orphans} صنف يُشير لتصنيفات محذوفة",
                       layer=2,
                       suggestion=(
                           "-- اختر أحد:\n"
                           "-- (أ) عيّن category_id إلى NULL:\n"
                           "UPDATE inv_items SET category_id=NULL\n"
                           "WHERE category_id NOT IN (SELECT id FROM inv_categories);\n\n"
                           "-- (ب) أو احذف الأصناف اليتيمة (احذر!):\n"
                           "-- DELETE FROM inv_items WHERE category_id NOT IN ..."
                       ),
                       tip="FK مكسورة - الأصناف تُشير لتصنيف غير موجود")
        else:
            _add_check("Orphaned Items (FK)", "ok",
                       "كل الأصناف تُشير لتصنيفات موجودة",
                       layer=2)
    except Exception as e:
        await db.rollback()
        _add_check("Orphaned Items (FK)", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=2)

    # ─── 2.3 Duplicate Item Codes ──────────────────────────────
    try:
        rd = await db.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT item_code FROM inv_items
                WHERE tenant_id=:tid
                GROUP BY item_code
                HAVING COUNT(*) > 1
            ) duplicates
        """), {"tid": tid})
        dups = rd.scalar() or 0
        if dups > 0:
            _add_check("Duplicate Item Codes", "fail",
                       f"❌ {dups} كود صنف مكرّر — يجب تكون فريدة",
                       layer=2,
                       suggestion=(
                           "-- ابحث عن المكرّرات:\n"
                           "SELECT item_code, COUNT(*) FROM inv_items\n"
                           "WHERE tenant_id=:tid GROUP BY item_code HAVING COUNT(*) > 1;"
                       ),
                       tip="الأكواد يجب تكون فريدة — قد يسبّب مشاكل في الترحيل")
        else:
            _add_check("Duplicate Item Codes", "ok",
                       "لا يوجد أكواد مكرّرة",
                       layer=2)
    except Exception as e:
        await db.rollback()
        _add_check("Duplicate Item Codes", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=2)

    # ─── 2.4 Items without UoM ─────────────────────────────────
    try:
        ru = await db.execute(text("""
            SELECT COUNT(*) FROM inv_items
            WHERE tenant_id=:tid AND uom_id IS NULL AND is_active=true
        """), {"tid": tid})
        no_uom = ru.scalar() or 0
        if no_uom > 0:
            _add_check("Items Without UoM", "warn",
                       f"⚠️ {no_uom} صنف نشط بدون وحدة قياس",
                       layer=2,
                       suggestion=(
                           "-- اعرض الأصناف بدون UoM:\n"
                           "SELECT item_code, item_name FROM inv_items\n"
                           "WHERE uom_id IS NULL AND is_active=true;"
                       ),
                       tip="وحدة القياس مطلوبة للحركات (GRN, GIN, ...)")
        else:
            _add_check("Items Without UoM", "ok",
                       "كل الأصناف النشطة لديها وحدة قياس",
                       layer=2)
    except Exception as e:
        await db.rollback()
        _add_check("Items Without UoM", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=2)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 3: Business Logic — هل المنطق متطابق؟
    # ═══════════════════════════════════════════════════════════════

    # ─── 3.1 item_type Sync بين item و category ⭐⭐ ────────────
    # المنطق الذي اكتشفناه: items.item_type يجب يطابق categories.item_type
    try:
        rm = await db.execute(text("""
            SELECT COUNT(*) FROM inv_items i
            JOIN inv_categories c ON c.id = i.category_id
            WHERE i.tenant_id=:tid
              AND i.item_type IS NOT NULL
              AND c.item_type IS NOT NULL
              AND i.item_type != c.item_type
        """), {"tid": tid})
        mismatched = rm.scalar() or 0
        if mismatched > 0:
            _add_check("item_type ↔ Category Sync", "warn",
                       f"⚠️ {mismatched} صنف نوعه (item_type) لا يطابق تصنيفه",
                       layer=3,
                       suggestion=(
                           "-- مزامنة الأصناف مع تصنيفاتها:\n"
                           "UPDATE inv_items i\n"
                           "SET item_type = c.item_type\n"
                           "FROM inv_categories c\n"
                           "WHERE i.category_id = c.id\n"
                           "  AND i.tenant_id = c.tenant_id\n"
                           "  AND (i.item_type != c.item_type OR i.item_type IS NULL);"
                       ),
                       tip="عند تعديل التصنيف، يجب تحديث item_type تلقائياً (تمّ تطبيقه في POST/PUT)")
        else:
            _add_check("item_type ↔ Category Sync", "ok",
                       "كل الأصناف نوعها يطابق تصنيفاتها",
                       layer=3)
    except Exception as e:
        await db.rollback()
        _add_check("item_type ↔ Category Sync", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=3)

    # ─── 3.2 Negative Stock Without Permission ─────────────────
    try:
        # نتحقّق من وجود الأعمدة أوّلاً
        check_q = await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' 
              AND table_name='inv_items' 
              AND column_name='allow_negative_stock'
        """))
        if check_q.fetchone():
            rn = await db.execute(text("""
                SELECT COUNT(*) FROM inv_balances b
                JOIN inv_items i ON i.id = b.item_id
                WHERE b.tenant_id=:tid
                  AND b.qty_on_hand < 0
                  AND COALESCE(i.allow_negative_stock, false) = false
            """), {"tid": tid})
            neg = rn.scalar() or 0
            if neg > 0:
                _add_check("Negative Stock Without Permission", "fail",
                           f"❌ {neg} صنف برصيد سالب رغم منع المخزون السالب",
                           layer=3,
                           suggestion=(
                               "-- اعرض الأصناف:\n"
                               "SELECT i.item_code, i.item_name, w.warehouse_name, b.qty_on_hand\n"
                               "FROM inv_balances b\n"
                               "JOIN inv_items i ON i.id = b.item_id\n"
                               "JOIN inv_warehouses w ON w.id = b.warehouse_id\n"
                               "WHERE b.qty_on_hand < 0 AND COALESCE(i.allow_negative_stock, false) = false;"
                           ),
                           tip="رصيد سالب رغم المنع - مشكلة في تكامل البيانات")
            else:
                _add_check("Negative Stock Permission", "ok",
                           "لا توجد مخالفات مخزون سالب",
                           layer=3)
        else:
            _add_check("Negative Stock Permission", "warn",
                       "عمود allow_negative_stock غير موجود — تخطّي الفحص",
                       layer=3)
    except Exception as e:
        await db.rollback()
        _add_check("Negative Stock Permission", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=3)

    # ─── 3.3 Categories Without GL Accounts ────────────────────
    try:
        rc = await db.execute(text("""
            SELECT COUNT(*) FROM inv_categories
            WHERE tenant_id=:tid 
              AND (gl_account_code IS NULL OR gl_account_code = '')
        """), {"tid": tid})
        no_gl = rc.scalar() or 0
        if no_gl > 0:
            _add_check("Categories Without GL Account", "warn",
                       f"⚠️ {no_gl} تصنيف بدون حساب محاسبي (GL)",
                       layer=3,
                       suggestion=(
                           "-- اعرض التصنيفات:\n"
                           "SELECT category_code, category_name FROM inv_categories\n"
                           "WHERE gl_account_code IS NULL OR gl_account_code='';"
                       ),
                       tip="GL account ضروري للترحيل المحاسبي عند حركات الأصناف")
        else:
            _add_check("Categories Without GL Account", "ok",
                       "كل التصنيفات لديها حسابات محاسبية",
                       layer=3)
    except Exception as e:
        await db.rollback()
        _add_check("Categories Without GL Account", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=3)

    # ═══════════════════════════════════════════════════════════════
    # LEGACY CHECKS (من v1) — تبقى للتوافق
    # ═══════════════════════════════════════════════════════════════

    # ─── L.1 Critical reason codes ─────────────────────────────
    try:
        critical_reasons = [
            "count_overage", "count_variance", "damaged",
            "theft", "obsolete", "quality_reject",
        ]
        rr = await db.execute(text("""
            SELECT reason_code FROM inv_reason_codes
            WHERE tenant_id=:tid AND reason_code = ANY(:codes) AND is_active=true
        """), {"tid": tid, "codes": critical_reasons})
        existing_reasons = {r[0] for r in rr.fetchall()}
        missing_reasons = [c for c in critical_reasons if c not in existing_reasons]
        if missing_reasons:
            _add_check("Critical Reason Codes", "fail",
                       f"أسباب ناقصة: {', '.join(missing_reasons)}",
                       layer=2,
                       tip="الأسباب الحرجة مطلوبة لتسويات الجرد")
        else:
            _add_check("Critical Reason Codes", "ok",
                       "كل الأسباب الحرجة الـ 6 موجودة",
                       layer=2)
    except Exception as e:
        await db.rollback()
        _add_check("Critical Reason Codes", "fail",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=2)

    # ─── L.2 Account settings → CoA ────────────────────────────
    try:
        ra = await db.execute(text("""
            SELECT s.tx_type, s.debit_account, s.credit_account
            FROM inv_account_settings s
            WHERE s.tenant_id=:tid
        """), {"tid": tid})
        saved_accs = ra.fetchall()
        invalid_accs: list = []
        for row in saved_accs:
            for acc in (row[1], row[2]):
                if acc:
                    rv = await db.execute(text("""
                        SELECT 1 FROM coa_accounts
                        WHERE tenant_id=:tid AND account_code=:c AND is_active=true
                    """), {"tid": tid, "c": acc})
                    if not rv.fetchone():
                        invalid_accs.append(f"{row[0]}: {acc}")

        if invalid_accs:
            _add_check("Account Settings → CoA", "fail",
                       f"حسابات غير صالحة: {', '.join(invalid_accs[:5])}",
                       layer=3,
                       tip="الحسابات في إعدادات الترحيل غير موجودة في دليل الحسابات")
        else:
            _add_check("Account Settings → CoA", "ok",
                       f"كل الحسابات المُعدَّة صالحة ({len(saved_accs)} إعداد)",
                       layer=3)
    except Exception as e:
        await db.rollback()
        _add_check("Account Settings → CoA", "fail",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=3)

    # ─── L.3 Default CoA accounts ──────────────────────────────
    try:
        default_codes = set()
        for d, c, _ in DEFAULT_TX_ACCOUNTS.values():
            default_codes.add(d)
            default_codes.add(c)
        rd = await db.execute(text("""
            SELECT account_code FROM coa_accounts
            WHERE tenant_id=:tid AND is_active=true AND account_code = ANY(:codes)
        """), {"tid": tid, "codes": list(default_codes)})
        found_defaults = {r[0] for r in rd.fetchall()}
        missing_defaults = [c for c in default_codes if c not in found_defaults]

        if missing_defaults:
            _add_check("Default CoA Accounts", "warn",
                       f"حسابات افتراضية ناقصة: {', '.join(missing_defaults[:5])}",
                       layer=3,
                       tip="ستُستخدم القيم الافتراضية كـ fallback")
        else:
            _add_check("Default CoA Accounts", "ok",
                       f"كل الحسابات الافتراضية الـ {len(default_codes)} موجودة",
                       layer=3)
    except Exception as e:
        await db.rollback()
        _add_check("Default CoA Accounts", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=3)

    # ─── L.4 Numbering series ──────────────────────────────────
    try:
        cur_year = date.today().year
        rn = await db.execute(text("""
            SELECT series_type FROM jl_numbering_series
            WHERE tenant_id=:tid AND year=:yr AND series_type = ANY(:types)
        """), {"tid": tid, "yr": cur_year, "types": INV_SERIES_TYPES})
        existing_series = {r[0] for r in rn.fetchall()}
        missing_series = [s for s in INV_SERIES_TYPES if s not in existing_series]
        if missing_series:
            _add_check("Numbering Series", "warn",
                       f"تسلسلات سنة {cur_year} ستُنشأ تلقائياً عند أول استخدام",
                       layer=1)
        else:
            _add_check("Numbering Series", "ok",
                       f"كل تسلسلات سنة {cur_year} موجودة ({len(INV_SERIES_TYPES)} نوع)",
                       layer=1)
    except Exception as e:
        await db.rollback()
        _add_check("Numbering Series", "warn",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=1)

    # ─── L.5 Active warehouses ─────────────────────────────────
    try:
        rw = await db.execute(text("""
            SELECT COUNT(*) FROM inv_warehouses
            WHERE tenant_id=:tid AND is_active=true
        """), {"tid": tid})
        wh_count = rw.scalar() or 0
        if wh_count == 0:
            _add_check("Active Warehouses", "fail",
                       "لا يوجد أي مستودع نشط",
                       layer=2,
                       suggestion="-- أضف مستودع جديد من صفحة المستودعات",
                       tip="مستودع واحد على الأقل مطلوب لبدء أيّ حركة")
        else:
            _add_check("Active Warehouses", "ok",
                       f"{wh_count} مستودع نشط",
                       layer=2)
    except Exception as e:
        await db.rollback()
        _add_check("Active Warehouses", "fail",
                   f"الفحص فشل: {type(e).__name__}: {str(e)[:100]}",
                   layer=2)

    # ═══════════════════════════════════════════════════════════════
    # SUMMARY & RETURN
    # ═══════════════════════════════════════════════════════════════
    
    # إحصائيات حسب الطبقة
    by_layer = {}
    for c in checks:
        layer = c.get("layer", 1)
        by_layer.setdefault(layer, {"ok": 0, "warn": 0, "fail": 0})
        by_layer[layer][c["status"]] += 1

    return ok(data={
        "status": "healthy" if overall_ok else "issues",
        "checks": checks,
        "summary": {
            "total": len(checks),
            "ok": sum(1 for c in checks if c["status"] == "ok"),
            "warn": sum(1 for c in checks if c["status"] == "warn"),
            "fail": sum(1 for c in checks if c["status"] == "fail"),
        },
        "by_layer": by_layer,
        "layers_info": {
            "1": {"name": "Schema Compatibility", "icon": "🏗️"},
            "2": {"name": "Data Integrity", "icon": "🔍"},
            "3": {"name": "Business Logic", "icon": "⚙️"},
        },
        "version": "v2.0",
        "checked_at": date.today().isoformat(),
    })

