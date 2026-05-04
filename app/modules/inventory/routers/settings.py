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
    """
    إنشاء/تعديل إعدادات حسابات tx_type معيّن.
    يتحقّق من وجود الحسابات في دليل الحسابات coa_accounts،
    وأنّها مفعّلة وقابلة للترحيل قبل الحفظ.
    """
    try:
        tid = str(user.tenant_id)
        if tx_type not in TX_TYPES_ALL:
            raise HTTPException(400, f"نوع الحركة '{tx_type}' غير معروف")

        debit = (data.get("debit_account") or "").strip() or None
        credit = (data.get("credit_account") or "").strip() or None
        desc = data.get("description")

        # ── Validate accounts in CoA (skip if both empty) ──
        # ⚠️ coa_accounts uses 'code' and 'name_ar' (NOT account_code/account_name)
        for label_ar, label_en, code in [
            ("الحساب المدين", "Debit", debit),
            ("الحساب الدائن", "Credit", credit),
        ]:
            if not code:
                continue
            rc = await db.execute(text("""
                SELECT code, name_ar, is_active, postable
                FROM coa_accounts
                WHERE tenant_id = :tid AND code = :c
                LIMIT 1
            """), {"tid": tid, "c": code})
            row = rc.fetchone()
            if not row:
                raise HTTPException(
                    400,
                    f"{label_ar} '{code}' غير موجود في دليل الحسابات"
                )
            if not row.is_active:
                raise HTTPException(
                    400,
                    f"{label_ar} '{code} - {row.name_ar}' غير مفعّل"
                )
            if not row.postable:
                raise HTTPException(
                    400,
                    f"{label_ar} '{code} - {row.name_ar}' غير قابل للترحيل (مجموعة وليس حساباً تفصيلياً)"
                )

        # ── Upsert ──
        await db.execute(text("""
            INSERT INTO inv_account_settings (
                id, tenant_id, tx_type, debit_account, credit_account, description, updated_at
            ) VALUES (
                gen_random_uuid(), :tid, :tt, :dr, :cr, :desc, NOW()
            )
            ON CONFLICT (tenant_id, tx_type) DO UPDATE SET
                debit_account  = EXCLUDED.debit_account,
                credit_account = EXCLUDED.credit_account,
                description    = EXCLUDED.description,
                updated_at     = NOW()
        """), {
            "tid": tid, "tt": tx_type,
            "dr": debit, "cr": credit, "desc": desc,
        })
        await db.commit()

        return ok(
            data={
                "tx_type": tx_type,
                "debit_account": debit,
                "credit_account": credit,
                "description": desc,
            },
            message="✅ تم حفظ الإعدادات",
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            400,
            f"فشل حفظ إعدادات حسابات '{tx_type}': {str(e)[:300]}"
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
# 4. HEALTH CHECK — moved to settings_health.py + settings_fix.py (v5 unified)
# ═══════════════════════════════════════════════════════════════════════════
# الفحوصات الـ 15 الموحَّدة الآن في:
#   - settings_health.py  (GET  /inventory/settings/health)
#   - settings_fix.py     (GET/POST /inventory/settings/health/fix/...)
# Frontend: HealthCheckPanel(moduleId="inventory-settings")
