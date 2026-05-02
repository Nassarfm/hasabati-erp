"""
app/modules/inventory/routers/settings_health.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Settings HealthCheck (Unified Contract)
═══════════════════════════════════════════════════════════════════════════
نسخة موحَّدة من /health تتبع نفس الـ contract المُستَخدَم في warehouse_health.py
ليعمل مع HealthCheckPanel الموحَّد.

15 فحص × 3 طبقات:
  Layer 1 (Schema):     Critical Tables, Schema Compatibility
  Layer 2 (Integrity):  8 فحوصات — orphans, duplicates, missing UoM, ...
  Layer 3 (Business):   5 فحوصات — type sync, negative stock, GL accounts, ...

Endpoint:
  GET /inventory/settings/health   — يُرجع كل النتائج + summary
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory/settings", tags=["inventory-settings-health"])


# ═══════════════════════════════════════════════════════════════════════════
# Helper: build a check result (unified contract)
# ═══════════════════════════════════════════════════════════════════════════
def _check(check_id, category, title, status, message, details=None, suggestion=None):
    """يبني سجل فحص موحّد. status: 'ok'|'warning'|'critical'|'error'."""
    return {
        "id": check_id,
        "category": category,
        "title": title,
        "status": status,
        "message": message,
        "details": details or [],
        "suggestion": suggestion,
    }


async def _get_columns(db, table_name):
    r = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:tbl
    """), {"tbl": table_name})
    return {row[0] for row in r.fetchall()}


# ═══════════════════════════════════════════════════════════════════════════
# Layer 1: SCHEMA CHECKS (2)
# ═══════════════════════════════════════════════════════════════════════════
CRITICAL_TABLES = [
    "inv_items", "inv_warehouses", "inv_balances", "inv_transactions",
    "inv_transaction_lines", "inv_ledger", "inv_fifo_layers",
    "inv_zones", "inv_locations", "inv_brands", "inv_uom_conversions",
    "inv_reason_codes", "inv_item_attributes", "inv_item_attribute_values",
    "inv_lots", "inv_serials",
    "inv_count_sessions", "inv_count_lines",
    "inv_account_settings", "jl_numbering_series",
    "inv_categories",
]


async def _check_critical_tables(db):
    try:
        rt = await db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_name = ANY(:names)
        """), {"names": CRITICAL_TABLES})
        existing = {r[0] for r in rt.fetchall()}
        missing = [t for t in CRITICAL_TABLES if t not in existing]
        if missing:
            return _check("critical_tables", "schema",
                          "الجداول الأساسية", "critical",
                          f"جداول ناقصة: {len(missing)}",
                          details=missing,
                          suggestion="بعض الجداول الأساسية مفقودة — قد يحتاج النظام لـ migration كامل")
        return _check("critical_tables", "schema",
                      "الجداول الأساسية", "ok",
                      f"جميع الجداول الـ {len(CRITICAL_TABLES)} موجودة")
    except Exception as e:
        await db.rollback()
        return _check("critical_tables", "schema",
                      "الجداول الأساسية", "error",
                      f"الفحص فشل: {type(e).__name__}: {str(e)[:200]}")


async def _check_schema_compatibility(db):
    """يفحص توافق Backend مع DB schema (الأعمدة المتوقّعة)."""
    expected = {
        "inv_items": ["uom_id", "purchase_uom_id", "sales_uom_id", "tracking_type"],
        "inv_balances": ["item_id", "warehouse_id", "qty_on_hand", "wac"],
        "inv_transactions": ["tx_type", "tx_date", "warehouse_id", "status"],
        "inv_categories": ["item_type", "default_gl_account"],
    }
    issues = []
    try:
        for tbl, cols in expected.items():
            existing = await _get_columns(db, tbl)
            if not existing:
                continue  # table missing handled in critical_tables check
            missing = [c for c in cols if c not in existing]
            if missing:
                issues.append(f"{tbl}: {', '.join(missing)}")
        if not issues:
            return _check("schema_compat", "schema",
                          "توافق Schema (Backend ↔ DB)", "ok",
                          "كل الأعمدة المتوقّعة موجودة")
        return _check("schema_compat", "schema",
                      "توافق Schema (Backend ↔ DB)", "warning",
                      f"اكتُشف {len(issues)} نقطة عدم تطابق",
                      details=issues,
                      suggestion="ALTER TABLE <table> ADD COLUMN <missing>;")
    except Exception as e:
        await db.rollback()
        return _check("schema_compat", "schema",
                      "توافق Schema (Backend ↔ DB)", "error",
                      f"الفحص فشل: {str(e)[:200]}")


# ═══════════════════════════════════════════════════════════════════════════
# Layer 2: DATA INTEGRITY CHECKS (8)
# ═══════════════════════════════════════════════════════════════════════════
async def _check_categories_diversity(db, tid):
    try:
        r = await db.execute(text("""
            SELECT item_type, COUNT(*) AS c FROM inv_categories
            WHERE tenant_id=:tid GROUP BY item_type
        """), {"tid": tid})
        rows = {row._mapping["item_type"]: row._mapping["c"] for row in r.fetchall()}
        if not rows:
            return _check("categories_diversity", "integrity",
                          "تنوّع أنواع التصنيفات", "warning",
                          "لا توجد تصنيفات",
                          suggestion="أضف تصنيفات أساسية على الأقل (سلعة، خدمة، نصف مصنّع)")
        types = list(rows.keys())
        if len(types) < 2:
            return _check("categories_diversity", "integrity",
                          "تنوّع أنواع التصنيفات", "warning",
                          f"نوع واحد فقط: {types[0]}",
                          details=[f"{k}: {v}" for k, v in rows.items()],
                          suggestion="أنواع متعدّدة (سلعة، خدمة، نصف مصنّع) تحسّن دقّة المحاسبة")
        return _check("categories_diversity", "integrity",
                      "تنوّع أنواع التصنيفات", "ok",
                      f"{len(types)} أنواع مختلفة",
                      details=[f"{k}: {v}" for k, v in rows.items()])
    except Exception as e:
        await db.rollback()
        return _check("categories_diversity", "integrity",
                      "تنوّع أنواع التصنيفات", "error",
                      f"فشل: {str(e)[:200]}")


async def _check_orphan_items(db, tid):
    try:
        r = await db.execute(text("""
            SELECT i.item_code, i.item_name, i.category_id FROM inv_items i
            LEFT JOIN inv_categories c ON c.id=i.category_id AND c.tenant_id=i.tenant_id
            WHERE i.tenant_id=:tid AND i.category_id IS NOT NULL AND c.id IS NULL
            LIMIT 50
        """), {"tid": tid})
        rows = r.fetchall()
        if not rows:
            return _check("orphan_items", "integrity",
                          "أصناف يتيمة (تصنيف غير موجود)", "ok",
                          "كل الأصناف مرتبطة بتصنيف صحيح")
        return _check("orphan_items", "integrity",
                      "أصناف يتيمة (تصنيف غير موجود)", "critical",
                      f"وُجد {len(rows)} صنف بتصنيف غير موجود",
                      details=[f"{r._mapping['item_code']} - {r._mapping['item_name']}" for r in rows[:10]],
                      suggestion="UPDATE inv_items SET category_id=NULL WHERE category_id NOT IN (SELECT id FROM inv_categories);")
    except Exception as e:
        await db.rollback()
        return _check("orphan_items", "integrity",
                      "أصناف يتيمة", "error",
                      f"فشل: {str(e)[:200]}")


async def _check_duplicate_item_codes(db, tid):
    try:
        r = await db.execute(text("""
            SELECT item_code, COUNT(*) AS c FROM inv_items
            WHERE tenant_id=:tid AND item_code IS NOT NULL
            GROUP BY item_code HAVING COUNT(*) > 1
        """), {"tid": tid})
        rows = r.fetchall()
        if not rows:
            return _check("dup_item_codes", "integrity",
                          "أكواد أصناف مكرّرة", "ok",
                          "كل أكواد الأصناف فريدة")
        return _check("dup_item_codes", "integrity",
                      "أكواد أصناف مكرّرة", "critical",
                      f"{len(rows)} كود متكرّر",
                      details=[f"{r._mapping['item_code']}: {r._mapping['c']} مرّات" for r in rows[:10]],
                      suggestion="غيّر الأكواد المكرّرة لتكون unique")
    except Exception as e:
        await db.rollback()
        return _check("dup_item_codes", "integrity",
                      "أكواد مكرّرة", "error", f"فشل: {str(e)[:200]}")


async def _check_items_without_uom(db, tid):
    try:
        r = await db.execute(text("""
            SELECT COUNT(*) AS c FROM inv_items
            WHERE tenant_id=:tid AND uom_id IS NULL
        """), {"tid": tid})
        cnt = r.fetchone()._mapping["c"]
        if cnt == 0:
            return _check("items_no_uom", "integrity",
                          "أصناف بدون وحدة قياس", "ok",
                          "كل الأصناف لها وحدة قياس")
        return _check("items_no_uom", "integrity",
                      "أصناف بدون وحدة قياس", "warning",
                      f"{cnt} صنف بدون UoM",
                      suggestion="UPDATE inv_items SET uom_id='<default-uom-id>' WHERE uom_id IS NULL;")
    except Exception as e:
        await db.rollback()
        return _check("items_no_uom", "integrity",
                      "أصناف بدون UoM", "error", f"فشل: {str(e)[:200]}")


async def _check_critical_reason_codes(db, tid):
    try:
        critical_codes = ["count_overage", "count_shortage", "scrap_damaged"]
        r = await db.execute(text("""
            SELECT reason_code FROM inv_reason_codes
            WHERE tenant_id=:tid AND reason_code = ANY(:codes)
        """), {"tid": tid, "codes": critical_codes})
        existing = {row._mapping["reason_code"] for row in r.fetchall()}
        missing = [c for c in critical_codes if c not in existing]
        if missing:
            return _check("critical_reasons", "integrity",
                          "رموز الأسباب الحرجة", "warning",
                          f"رموز ناقصة: {len(missing)}",
                          details=missing,
                          suggestion="أضف هذه الرموز عبر صفحة 'أسباب الحركات'")
        return _check("critical_reasons", "integrity",
                      "رموز الأسباب الحرجة", "ok",
                      f"كل الرموز الـ {len(critical_codes)} موجودة")
    except Exception as e:
        await db.rollback()
        return _check("critical_reasons", "integrity",
                      "رموز الأسباب", "error", f"فشل: {str(e)[:200]}")


async def _check_default_coa(db, tid):
    """Default CoA accounts للترحيل."""
    try:
        required = ["1140", "1141", "5110", "4110"]
        r = await db.execute(text("""
            SELECT account_code FROM coa_accounts
            WHERE tenant_id=:tid AND account_code = ANY(:codes)
        """), {"tid": tid, "codes": required})
        existing = {row._mapping["account_code"] for row in r.fetchall()}
        missing = [c for c in required if c not in existing]
        if missing:
            return _check("default_coa", "integrity",
                          "حسابات CoA الأساسية", "critical",
                          f"حسابات ناقصة: {', '.join(missing)}",
                          suggestion="أضف الحسابات في دليل الحسابات قبل أيّ ترحيل")
        return _check("default_coa", "integrity",
                      "حسابات CoA الأساسية", "ok",
                      "كل الحسابات الأساسية موجودة")
    except Exception as e:
        await db.rollback()
        return _check("default_coa", "integrity",
                      "حسابات CoA", "error", f"فشل: {str(e)[:200]}")


async def _check_numbering_series(db, tid):
    try:
        required = ["GRN", "GIN", "IT", "IJ", "PCR", "PV", "RV", "BP", "BR"]
        r = await db.execute(text("""
            SELECT series_code FROM jl_numbering_series
            WHERE tenant_id=:tid AND series_code = ANY(:codes)
        """), {"tid": tid, "codes": required})
        existing = {row._mapping["series_code"] for row in r.fetchall()}
        missing = [c for c in required if c not in existing]
        if missing:
            return _check("numbering_series", "integrity",
                          "تسلسلات ترقيم المستندات", "warning",
                          f"تسلسلات ناقصة: {len(missing)}",
                          details=missing,
                          suggestion="أضف التسلسلات الناقصة من صفحة 'التسلسلات'")
        return _check("numbering_series", "integrity",
                      "تسلسلات ترقيم المستندات", "ok",
                      f"كل التسلسلات الـ {len(required)} موجودة")
    except Exception as e:
        await db.rollback()
        return _check("numbering_series", "integrity",
                      "التسلسلات", "error", f"فشل: {str(e)[:200]}")


async def _check_active_warehouses(db, tid):
    try:
        r = await db.execute(text("""
            SELECT COUNT(*) AS c FROM inv_warehouses
            WHERE tenant_id=:tid AND is_active=true
        """), {"tid": tid})
        cnt = r.fetchone()._mapping["c"]
        if cnt == 0:
            return _check("active_warehouses", "integrity",
                          "مستودعات نشطة", "critical",
                          "لا يوجد مستودع نشط!",
                          suggestion="أضف مستودعاً واحداً على الأقل لبدء أيّ حركة")
        return _check("active_warehouses", "integrity",
                      "مستودعات نشطة", "ok",
                      f"{cnt} مستودع نشط")
    except Exception as e:
        await db.rollback()
        return _check("active_warehouses", "integrity",
                      "مستودعات نشطة", "error", f"فشل: {str(e)[:200]}")


# ═══════════════════════════════════════════════════════════════════════════
# Layer 3: BUSINESS LOGIC CHECKS (5)
# ═══════════════════════════════════════════════════════════════════════════
async def _check_item_type_sync(db, tid):
    """يفحص تطابق item.item_type مع category.item_type."""
    try:
        # فحص فقط لو الأعمدة موجودة
        item_cols = await _get_columns(db, "inv_items")
        cat_cols = await _get_columns(db, "inv_categories")
        if "item_type" not in item_cols or "item_type" not in cat_cols:
            return _check("item_type_sync", "business",
                          "تطابق نوع الصنف ↔ التصنيف", "ok",
                          "عمود item_type غير موجود — تخطّي")

        r = await db.execute(text("""
            SELECT i.item_code, i.item_type AS item_t, c.item_type AS cat_t
            FROM inv_items i
            JOIN inv_categories c ON c.id=i.category_id AND c.tenant_id=i.tenant_id
            WHERE i.tenant_id=:tid AND i.item_type IS NOT NULL
              AND c.item_type IS NOT NULL AND i.item_type <> c.item_type
            LIMIT 50
        """), {"tid": tid})
        rows = r.fetchall()
        if not rows:
            return _check("item_type_sync", "business",
                          "تطابق نوع الصنف ↔ التصنيف", "ok",
                          "كل الأصناف متطابقة مع تصنيفاتها")
        return _check("item_type_sync", "business",
                      "تطابق نوع الصنف ↔ التصنيف", "warning",
                      f"{len(rows)} صنف غير متطابق",
                      details=[f"{r._mapping['item_code']}: item={r._mapping['item_t']}, cat={r._mapping['cat_t']}"
                               for r in rows[:10]])
    except Exception as e:
        await db.rollback()
        return _check("item_type_sync", "business",
                      "تطابق item_type", "error", f"فشل: {str(e)[:200]}")


async def _check_negative_stock(db, tid):
    """فحص الأرصدة السالبة."""
    try:
        bal_cols = await _get_columns(db, "inv_balances")
        if "qty_on_hand" not in bal_cols:
            return _check("negative_stock", "business",
                          "أرصدة سالبة", "ok", "عمود qty_on_hand غير موجود — تخطّي")

        # هل أيّ مستودع لا يسمح بالسالب؟
        r = await db.execute(text("""
            SELECT COUNT(*) AS c FROM inv_balances b
            JOIN inv_warehouses w ON w.id=b.warehouse_id AND w.tenant_id=b.tenant_id
            WHERE b.tenant_id=:tid AND b.qty_on_hand < 0
              AND COALESCE(w.allow_negative_stock, false) = false
        """), {"tid": tid})
        cnt = r.fetchone()._mapping["c"]
        if cnt == 0:
            return _check("negative_stock", "business",
                          "أرصدة سالبة دون إذن", "ok",
                          "لا توجد أرصدة سالبة في مستودعات لا تسمح بذلك")
        return _check("negative_stock", "business",
                      "أرصدة سالبة دون إذن", "critical",
                      f"{cnt} رصيد سالب في مستودعات لا تسمح",
                      suggestion="إمّا تفعيل allow_negative_stock، أو تعديل الأرصدة")
    except Exception as e:
        await db.rollback()
        return _check("negative_stock", "business",
                      "أرصدة سالبة", "error", f"فشل: {str(e)[:200]}")


async def _check_categories_no_gl(db, tid):
    """تصنيفات بدون حساب GL مرتبط."""
    try:
        cat_cols = await _get_columns(db, "inv_categories")
        if "default_gl_account" not in cat_cols:
            return _check("categories_no_gl", "business",
                          "تصنيفات بدون GL", "ok",
                          "العمود default_gl_account غير موجود — تخطّي")

        r = await db.execute(text("""
            SELECT category_code, category_name FROM inv_categories
            WHERE tenant_id=:tid
              AND (default_gl_account IS NULL OR default_gl_account = '')
            LIMIT 50
        """), {"tid": tid})
        rows = r.fetchall()
        if not rows:
            return _check("categories_no_gl", "business",
                          "تصنيفات بدون حساب GL", "ok",
                          "كل التصنيفات لها حساب GL افتراضي")
        return _check("categories_no_gl", "business",
                      "تصنيفات بدون حساب GL", "warning",
                      f"{len(rows)} تصنيف بدون GL",
                      details=[f"{r._mapping['category_code']} - {r._mapping['category_name']}" for r in rows[:10]],
                      suggestion="ربط الـ GL يُسرّع الترحيل ويقلّل الأخطاء")
    except Exception as e:
        await db.rollback()
        return _check("categories_no_gl", "business",
                      "تصنيفات بدون GL", "error", f"فشل: {str(e)[:200]}")


async def _check_account_settings_coa(db, tid):
    """يفحص أن كل tx_type في inv_account_settings له حساب موجود في CoA."""
    try:
        r = await db.execute(text("""
            SELECT s.tx_type, s.dr_account_code, s.cr_account_code
            FROM inv_account_settings s
            WHERE s.tenant_id=:tid
              AND (
                (s.dr_account_code IS NOT NULL AND s.dr_account_code NOT IN
                  (SELECT account_code FROM coa_accounts WHERE tenant_id=:tid))
                OR
                (s.cr_account_code IS NOT NULL AND s.cr_account_code NOT IN
                  (SELECT account_code FROM coa_accounts WHERE tenant_id=:tid))
              )
        """), {"tid": tid})
        rows = r.fetchall()
        if not rows:
            return _check("account_settings_coa", "business",
                          "ربط Account Settings بـ CoA", "ok",
                          "كل الحسابات في إعدادات الحركات صالحة في CoA")
        return _check("account_settings_coa", "business",
                      "ربط Account Settings بـ CoA", "critical",
                      f"{len(rows)} tx_type يحوي حساب غير موجود",
                      details=[f"{r._mapping['tx_type']}: DR={r._mapping['dr_account_code']}, CR={r._mapping['cr_account_code']}" for r in rows[:10]],
                      suggestion="حدِّث الحسابات في صفحة 'الحسابات' لتشير لحسابات موجودة")
    except Exception as e:
        await db.rollback()
        return _check("account_settings_coa", "business",
                      "Account Settings ↔ CoA", "error", f"فشل: {str(e)[:200]}")


async def _check_default_warehouse(db, tid):
    try:
        wh_cols = await _get_columns(db, "inv_warehouses")
        if "is_default" not in wh_cols:
            return _check("settings_default_warehouse", "business",
                          "مستودع افتراضي", "warning",
                          "عمود is_default غير موجود في inv_warehouses",
                          suggestion="ALTER TABLE inv_warehouses ADD COLUMN is_default BOOLEAN DEFAULT FALSE")
        r = await db.execute(text("""
            SELECT COUNT(*) AS c FROM inv_warehouses
            WHERE tenant_id=:tid AND is_default=true AND is_active=true
        """), {"tid": tid})
        cnt = r.fetchone()._mapping["c"]
        if cnt == 0:
            return _check("settings_default_warehouse", "business",
                          "مستودع افتراضي", "warning",
                          "لا يوجد مستودع افتراضي نشط",
                          suggestion="عيّن مستودعاً افتراضياً واحداً")
        if cnt > 1:
            return _check("settings_default_warehouse", "business",
                          "مستودع افتراضي", "warning",
                          f"{cnt} مستودعات افتراضيّة (المتوقّع 1)",
                          suggestion="ضع is_default=false على الإضافيّة")
        return _check("settings_default_warehouse", "business",
                      "مستودع افتراضي", "ok",
                      "يوجد مستودع افتراضي واحد")
    except Exception as e:
        await db.rollback()
        return _check("settings_default_warehouse", "business",
                      "مستودع افتراضي", "error", f"فشل: {str(e)[:200]}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/health")
async def settings_health_check(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    🩺 Settings HealthCheck v2 — Unified Contract.
    15 فحص × 3 طبقات (متوافق مع HealthCheckPanel الموحَّد).
    """
    tid = str(user.tenant_id)
    checks = []

    # Layer 1: Schema (2)
    try:
        checks.append(await _check_critical_tables(db))
        checks.append(await _check_schema_compatibility(db))
    except Exception as e:
        checks.append(_check("schema_error", "schema",
                             "أخطاء فحوصات Schema", "error",
                             f"خطأ: {str(e)[:200]}"))

    # Layer 2: Data Integrity (8)
    try:
        checks.append(await _check_categories_diversity(db, tid))
        checks.append(await _check_orphan_items(db, tid))
        checks.append(await _check_duplicate_item_codes(db, tid))
        checks.append(await _check_items_without_uom(db, tid))
        checks.append(await _check_critical_reason_codes(db, tid))
        checks.append(await _check_default_coa(db, tid))
        checks.append(await _check_numbering_series(db, tid))
        checks.append(await _check_active_warehouses(db, tid))
    except Exception as e:
        checks.append(_check("integrity_error", "integrity",
                             "أخطاء فحوصات Integrity", "error",
                             f"خطأ: {str(e)[:200]}"))

    # Layer 3: Business Logic (5)
    try:
        checks.append(await _check_item_type_sync(db, tid))
        checks.append(await _check_negative_stock(db, tid))
        checks.append(await _check_categories_no_gl(db, tid))
        checks.append(await _check_account_settings_coa(db, tid))
        checks.append(await _check_default_warehouse(db, tid))
    except Exception as e:
        checks.append(_check("business_error", "business",
                             "أخطاء فحوصات Business", "error",
                             f"خطأ: {str(e)[:200]}"))

    # Summary (unified format)
    summary = {
        "total": len(checks),
        "ok": sum(1 for c in checks if c["status"] == "ok"),
        "warning": sum(1 for c in checks if c["status"] == "warning"),
        "critical": sum(1 for c in checks if c["status"] == "critical"),
        "error": sum(1 for c in checks if c["status"] == "error"),
    }

    if summary["critical"] > 0 or summary["error"] > 0:
        overall = "critical"
    elif summary["warning"] > 0:
        overall = "warning"
    else:
        overall = "ok"

    return ok(data={
        "overall_status": overall,
        "summary": summary,
        "checks": checks,
        "categories": {
            "schema": "تطابق الـ Schema",
            "integrity": "سلامة البيانات",
            "business": "منطق الأعمال",
        },
        "version": "v2.0-unified",
        "checked_at": date.today().isoformat(),
    })
