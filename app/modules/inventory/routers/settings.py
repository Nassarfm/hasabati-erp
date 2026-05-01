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
    tid = str(user.tenant_id)
    cur_year = year or date.today().year
    r = await db.execute(text("""
        SELECT series_type, year, prefix_format, next_serial, padding_width,
               created_at, updated_at
        FROM jl_numbering_series
        WHERE tenant_id=:tid AND year=:yr AND series_type = ANY(:types)
        ORDER BY series_type
    """), {"tid": tid, "yr": cur_year, "types": INV_SERIES_TYPES})
    saved = {row[0]: dict(row._mapping) for row in r.fetchall()}

    out = []
    for st in INV_SERIES_TYPES:
        if st in saved:
            d = saved[st]
            d["is_default"] = False
            out.append(d)
        else:
            out.append({
                "series_type": st,
                "year": cur_year,
                "prefix_format": f"{st}-{{year}}-{{serial:07d}}",
                "next_serial": 1,
                "padding_width": 7,
                "is_default": True,
            })
    return ok(data={"year": cur_year, "series": out})


@router.put("/numbering-series/{series_type}")
async def update_numbering_series(
    series_type: str,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    if series_type not in INV_SERIES_TYPES:
        raise HTTPException(400, f"نوع التسلسل '{series_type}' غير صالح")

    yr = int(data.get("year") or date.today().year)
    prefix = data.get("prefix_format") or f"{series_type}-{{year}}-{{serial:07d}}"
    next_seq = int(data.get("next_serial", 1))
    padding = int(data.get("padding_width", 7))

    # Validate prefix has serial placeholder
    if "{serial" not in prefix:
        raise HTTPException(400, "prefix_format يجب أن يحتوي على {serial:0Nd}")

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
    تحقق شامل من جاهزية الموديول مع معالجة دفاعية لكل فحص:
      • وجود الجداول الأساسية
      • وجود الـ reason_codes الحرجة
      • ربط حسابات الترحيل في CoA
      • صلاحية إعدادات الترقيم
      • وجود مستودعات نشطة
    
    لو فشل فحص واحد، الباقي يستمر، ويظهر الفحص الفاشل كـ 'fail' مع رسالة الخطأ.
    هذا يساعد على التشخيص بدلاً من 500 صامت.
    """
    tid = str(user.tenant_id)
    checks = []
    overall_ok = True

    # ─── helper آمن ────────────────────────────────────────────
    def _add_check(name, status, message):
        nonlocal overall_ok
        if status == "fail":
            overall_ok = False
        checks.append({"name": name, "status": status, "message": message})

    # ─── 1. Critical tables ────────────────────────────────────
    try:
        critical_tables = [
            "inv_items", "inv_warehouses", "inv_balances", "inv_transactions",
            "inv_transaction_lines", "inv_ledger", "inv_fifo_layers",
            "inv_zones", "inv_locations", "inv_brands", "inv_uom_conversions",
            "inv_reason_codes", "inv_item_attributes", "inv_item_attribute_values",
            "inv_item_variant_attrs", "inv_lots", "inv_serials",
            "inv_count_sessions", "inv_count_lines",
            "inv_account_settings", "jl_numbering_series",
        ]
        rt = await db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_name = ANY(:names)
        """), {"names": critical_tables})
        existing_tables = {r[0] for r in rt.fetchall()}
        missing_tables = [t for t in critical_tables if t not in existing_tables]
        if missing_tables:
            _add_check("Critical Tables", "fail",
                       f"جداول ناقصة: {', '.join(missing_tables)}")
        else:
            _add_check("Critical Tables", "ok",
                       f"جميع الجداول الـ {len(critical_tables)} موجودة")
    except Exception as e:
        await db.rollback()
        _add_check("Critical Tables", "fail",
                   f"فحص الجداول فشل: {type(e).__name__}: {str(e)[:100]}")

    # ─── 2. Critical reason codes ──────────────────────────────
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
                       f"أسباب ناقصة: {', '.join(missing_reasons)}")
        else:
            _add_check("Critical Reason Codes", "ok",
                       "كل الأسباب الحرجة الـ 6 موجودة")
    except Exception as e:
        await db.rollback()
        _add_check("Critical Reason Codes", "fail",
                   f"فحص الأسباب فشل: {type(e).__name__}: {str(e)[:100]}")

    # ─── 3. Account settings → CoA ─────────────────────────────
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
                       f"حسابات غير صالحة: {', '.join(invalid_accs[:5])}")
        else:
            _add_check("Account Settings → CoA", "ok",
                       f"كل الحسابات المُعدَّة صالحة ({len(saved_accs)} إعداد)")
    except Exception as e:
        await db.rollback()
        _add_check("Account Settings → CoA", "fail",
                   f"فحص الحسابات فشل: {type(e).__name__}: {str(e)[:100]}")

    # ─── 4. Default CoA accounts ───────────────────────────────
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
                       f"حسابات افتراضية ناقصة في CoA: {', '.join(missing_defaults[:5])} (سيستخدم الفولباك)")
        else:
            _add_check("Default CoA Accounts", "ok",
                       f"كل الحسابات الافتراضية الـ {len(default_codes)} موجودة")
    except Exception as e:
        await db.rollback()
        _add_check("Default CoA Accounts", "warn",
                   f"فحص الحسابات الافتراضية فشل: {type(e).__name__}: {str(e)[:100]}")

    # ─── 5. Numbering series ───────────────────────────────────
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
                       f"تسلسلات لسنة {cur_year} ستُنشأ تلقائياً عند أول استخدام: {', '.join(missing_series[:5])}")
        else:
            _add_check("Numbering Series", "ok",
                       f"كل تسلسلات سنة {cur_year} موجودة ({len(INV_SERIES_TYPES)} نوع)")
    except Exception as e:
        await db.rollback()
        _add_check("Numbering Series", "warn",
                   f"فحص التسلسلات فشل: {type(e).__name__}: {str(e)[:100]}")

    # ─── 6. Active warehouses ──────────────────────────────────
    try:
        rw = await db.execute(text("""
            SELECT COUNT(*) FROM inv_warehouses
            WHERE tenant_id=:tid AND is_active=true
        """), {"tid": tid})
        wh_count = rw.scalar() or 0
        if wh_count == 0:
            _add_check("Active Warehouses", "fail",
                       "لا يوجد أي مستودع نشط — يجب إضافة مستودع قبل بدء أي حركة")
        else:
            _add_check("Active Warehouses", "ok",
                       f"{wh_count} مستودع نشط")
    except Exception as e:
        await db.rollback()
        _add_check("Active Warehouses", "fail",
                   f"فحص المستودعات فشل: {type(e).__name__}: {str(e)[:100]}")

    # ─── 7. Schema Compatibility (Backend ↔ DB columns) ────────
    # 💡 فكرة فادي العبقريّة: نفحص أن الأعمدة المطلوبة من Backend موجودة فعلاً في DB.
    # هذا يكشف انحراف schema بسرعة بدلاً من اكتشافه من خطأ 500 صامت.
    try:
        EXPECTED_COLUMNS = {
            "inv_brands": ["brand_code", "brand_name", "brand_name_en", "is_active",
                           "manufacturer", "country_of_origin"],
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
        }

        # احصل على كل الأعمدة الفعلية لكل الجداول دفعة واحدة
        rs = await db.execute(text("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name = ANY(:tables)
        """), {"tables": list(EXPECTED_COLUMNS.keys())})
        actual_cols: dict = {}
        for row in rs.fetchall():
            actual_cols.setdefault(row[0], set()).add(row[1])

        # قارن
        mismatches: list = []
        for tbl, expected in EXPECTED_COLUMNS.items():
            if tbl not in actual_cols:
                continue  # الجدول مفقود (يُكشف في فحص آخر)
            missing = [c for c in expected if c not in actual_cols[tbl]]
            if missing:
                mismatches.append(f"{tbl}: {', '.join(missing)}")

        if mismatches:
            _add_check("Schema Compatibility", "warn",
                       f"أعمدة متوقّعة من Backend غير موجودة في DB ({len(mismatches)} جدول): "
                       + " | ".join(mismatches[:3]))
        else:
            _add_check("Schema Compatibility", "ok",
                       f"كل الأعمدة المتوقّعة موجودة في الـ {len(EXPECTED_COLUMNS)} جداول الحسّاسة")
    except Exception as e:
        await db.rollback()
        _add_check("Schema Compatibility", "warn",
                   f"فحص توافق Schema فشل: {type(e).__name__}: {str(e)[:100]}")

    return ok(data={
        "status": "healthy" if overall_ok else "issues",
        "checks": checks,
        "summary": {
            "total": len(checks),
            "ok": sum(1 for c in checks if c["status"] == "ok"),
            "warn": sum(1 for c in checks if c["status"] == "warn"),
            "fail": sum(1 for c in checks if c["status"] == "fail"),
        },
        "version": "v5.0",
        "checked_at": date.today().isoformat(),
    })
