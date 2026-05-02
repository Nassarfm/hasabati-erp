"""
app/modules/inventory/routers/warehouse_health.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Warehouse Health Check
═══════════════════════════════════════════════════════════════════════════
فحص صحّي شامل للهيكل الهرمي (مستودعات → مناطق → مواقع)

3 طبقات × 14 فحص:
  1. Schema Layer       (5 فحوصات) — تطابق الأعمدة + tech debt
  2. Data Integrity     (5 فحوصات) — orphans + duplicates
  3. Business Logic     (4 فحوصات) — defaults + consistency

Endpoint:
  GET /inventory/warehouse-health     — يُرجع كل النتائج + summary
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory", tags=["inventory-warehouse-health"])


# ═══════════════════════════════════════════════════════════════════════════
# Helper: build a check result
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
# Layer 1: SCHEMA CHECKS
# ═══════════════════════════════════════════════════════════════════════════
WAREHOUSES_REQUIRED = {
    "id", "tenant_id", "warehouse_code", "warehouse_name",
    "warehouse_type", "is_active", "is_default",
}
ZONES_REQUIRED = {
    "id", "tenant_id", "warehouse_id", "zone_code", "zone_name",
    "zone_type", "is_active",
}
LOCATIONS_REQUIRED = {
    "id", "tenant_id", "warehouse_id", "location_code",
    "is_active",
}


async def _check_schema_warehouses(db):
    cols = await _get_columns(db, "inv_warehouses")
    missing = WAREHOUSES_REQUIRED - cols
    if not cols:
        return _check("schema_warehouses", "schema",
                      "أعمدة inv_warehouses", "critical",
                      "الجدول inv_warehouses غير موجود!",
                      suggestion="هذا خطأ جوهري — تحقّق من Migrations الأساسية")
    if missing:
        return _check("schema_warehouses", "schema",
                      "أعمدة inv_warehouses", "critical",
                      f"أعمدة مطلوبة مفقودة: {len(missing)}",
                      details=sorted(missing),
                      suggestion="ALTER TABLE inv_warehouses ADD COLUMN ...")
    return _check("schema_warehouses", "schema",
                  "أعمدة inv_warehouses", "ok",
                  f"كل الأعمدة المطلوبة موجودة ({len(cols)} عمود إجمالي)")


async def _check_schema_zones(db):
    cols = await _get_columns(db, "inv_zones")
    missing = ZONES_REQUIRED - cols
    if not cols:
        return _check("schema_zones", "schema",
                      "أعمدة inv_zones", "critical",
                      "الجدول inv_zones غير موجود!")
    if missing:
        return _check("schema_zones", "schema",
                      "أعمدة inv_zones", "critical",
                      f"أعمدة مطلوبة مفقودة: {len(missing)}",
                      details=sorted(missing))
    return _check("schema_zones", "schema",
                  "أعمدة inv_zones", "ok",
                  f"كل الأعمدة المطلوبة موجودة ({len(cols)} عمود)")


async def _check_schema_locations(db):
    cols = await _get_columns(db, "inv_locations")
    missing = LOCATIONS_REQUIRED - cols
    if not cols:
        return _check("schema_locations", "schema",
                      "أعمدة inv_locations", "critical",
                      "الجدول inv_locations غير موجود!")
    if missing:
        return _check("schema_locations", "schema",
                      "أعمدة inv_locations", "critical",
                      f"أعمدة مطلوبة مفقودة: {len(missing)}",
                      details=sorted(missing))
    return _check("schema_locations", "schema",
                  "أعمدة inv_locations", "ok",
                  f"كل الأعمدة المطلوبة موجودة ({len(cols)} عمود)")


async def _check_tech_debt(db):
    """يكشف tech debt: عمودان لنفس الغرض."""
    issues = []

    loc_cols = await _get_columns(db, "inv_locations")
    if "zone" in loc_cols and "zone_id" in loc_cols:
        issues.append("inv_locations: عمودان للمنطقة (zone نصّي legacy + zone_id uuid حديث)")

    wh_cols = await _get_columns(db, "inv_warehouses")
    if "warehouse_type" in wh_cols and "warehouse_subtype" in wh_cols:
        issues.append("inv_warehouses: warehouse_type + warehouse_subtype — ربّما مكرّران")

    if not issues:
        return _check("tech_debt", "schema",
                      "ديون تقنيّة في الـ schema", "ok",
                      "لا توجد ديون تقنيّة ظاهرة")
    return _check("tech_debt", "schema",
                  "ديون تقنيّة في الـ schema", "warning",
                  f"اكتُشف {len(issues)} نقطة ديون تقنيّة",
                  details=issues,
                  suggestion="الـ Backend يدعم كلا النموذجَين — لكن ينبغي توحيدهما لاحقاً")


async def _check_updated_at(db):
    """تأكّد من توفّر updated_at على الجداول الرئيسيّة (للتدقيق التتبّعي)."""
    missing_in = []
    for tbl in ["inv_warehouses", "inv_zones", "inv_locations"]:
        cols = await _get_columns(db, tbl)
        if cols and "updated_at" not in cols:
            missing_in.append(tbl)
    if not missing_in:
        return _check("updated_at_coverage", "schema",
                      "تتبّع التحديث (updated_at)", "ok",
                      "كل الجداول تتتبّع تاريخ آخر تعديل")
    return _check("updated_at_coverage", "schema",
                  "تتبّع التحديث (updated_at)", "warning",
                  f"عمود updated_at غير موجود في {len(missing_in)} جدول",
                  details=missing_in,
                  suggestion=("ALTER TABLE <table> "
                              "ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE"))


# ═══════════════════════════════════════════════════════════════════════════
# Layer 2: DATA INTEGRITY CHECKS
# ═══════════════════════════════════════════════════════════════════════════
async def _check_orphan_zones(db, tid):
    r = await db.execute(text("""
        SELECT z.id, z.zone_code, z.zone_name, z.warehouse_id
        FROM inv_zones z
        LEFT JOIN inv_warehouses w
          ON w.id = z.warehouse_id AND w.tenant_id = z.tenant_id
        WHERE z.tenant_id=:tid AND w.id IS NULL
        LIMIT 50
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("orphan_zones", "integrity",
                      "مناطق يتيمة (FK violation)", "ok",
                      "كل المناطق مرتبطة بمستودع موجود")
    return _check("orphan_zones", "integrity",
                  "مناطق يتيمة (FK violation)", "critical",
                  f"وُجد {len(rows)} منطقة بمستودع غير موجود",
                  details=[f"{r._mapping['zone_code']} - {r._mapping['zone_name']}" for r in rows[:10]],
                  suggestion=("DELETE FROM inv_zones WHERE warehouse_id NOT IN "
                              "(SELECT id FROM inv_warehouses);"))


async def _check_orphan_locations(db, tid):
    r1 = await db.execute(text("""
        SELECT l.id, l.location_code, l.warehouse_id
        FROM inv_locations l
        LEFT JOIN inv_warehouses w
          ON w.id = l.warehouse_id AND w.tenant_id = l.tenant_id
        WHERE l.tenant_id=:tid AND w.id IS NULL
        LIMIT 50
    """), {"tid": tid})
    bad_wh = r1.fetchall()

    r2 = await db.execute(text("""
        SELECT l.id, l.location_code, l.zone_id
        FROM inv_locations l
        LEFT JOIN inv_zones z
          ON z.id = l.zone_id AND z.tenant_id = l.tenant_id
        WHERE l.tenant_id=:tid
          AND l.zone_id IS NOT NULL
          AND z.id IS NULL
        LIMIT 50
    """), {"tid": tid})
    bad_zn = r2.fetchall()

    total = len(bad_wh) + len(bad_zn)
    if total == 0:
        return _check("orphan_locations", "integrity",
                      "مواقع يتيمة (FK violations)", "ok",
                      "كل المواقع مرتبطة بمستودع/منطقة صالحة")
    details = []
    if bad_wh:
        details.append(f"❌ {len(bad_wh)} موقع بمستودع غير موجود")
    if bad_zn:
        details.append(f"❌ {len(bad_zn)} موقع بمنطقة غير موجودة")
    return _check("orphan_locations", "integrity",
                  "مواقع يتيمة (FK violations)", "critical",
                  f"وُجد {total} موقع يتيم",
                  details=details,
                  suggestion="مسح المواقع اليتيمة أو إعادة ربطها يدويّاً")


async def _check_duplicate_warehouses(db, tid):
    r = await db.execute(text("""
        SELECT warehouse_code, COUNT(*) AS c
        FROM inv_warehouses
        WHERE tenant_id=:tid
        GROUP BY warehouse_code
        HAVING COUNT(*) > 1
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("dup_warehouses", "integrity",
                      "مستودعات بأكواد مكرّرة", "ok",
                      "كل أكواد المستودعات فريدة")
    return _check("dup_warehouses", "integrity",
                  "مستودعات بأكواد مكرّرة", "critical",
                  f"{len(rows)} كود متكرّر",
                  details=[f"{r._mapping['warehouse_code']}: {r._mapping['c']} مرّات" for r in rows],
                  suggestion="غيّر الأكواد المكرّرة لتكون unique")


async def _check_duplicate_zones(db, tid):
    r = await db.execute(text("""
        SELECT warehouse_id, zone_code, COUNT(*) AS c
        FROM inv_zones
        WHERE tenant_id=:tid AND zone_code IS NOT NULL
        GROUP BY warehouse_id, zone_code
        HAVING COUNT(*) > 1
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("dup_zones", "integrity",
                      "مناطق بأكواد مكرّرة", "ok",
                      "كل أكواد المناطق فريدة داخل كل مستودع")
    return _check("dup_zones", "integrity",
                  "مناطق بأكواد مكرّرة", "warning",
                  f"{len(rows)} كود منطقة متكرّر",
                  details=[f"{r._mapping['zone_code']}: {r._mapping['c']} مرّات" for r in rows[:10]],
                  suggestion="غيّر الأكواد المكرّرة لتكون unique لكل مستودع")


async def _check_duplicate_locations(db, tid):
    r = await db.execute(text("""
        SELECT warehouse_id, zone_id, location_code, COUNT(*) AS c
        FROM inv_locations
        WHERE tenant_id=:tid
        GROUP BY warehouse_id, zone_id, location_code
        HAVING COUNT(*) > 1
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("dup_locations", "integrity",
                      "مواقع بأكواد مكرّرة", "ok",
                      "كل أكواد المواقع فريدة داخل المنطقة")
    return _check("dup_locations", "integrity",
                  "مواقع بأكواد مكرّرة", "warning",
                  f"{len(rows)} كود موقع متكرّر",
                  details=[f"{r._mapping['location_code']}: {r._mapping['c']} مرّات" for r in rows[:10]])


# ═══════════════════════════════════════════════════════════════════════════
# Layer 3: BUSINESS LOGIC CHECKS
# ═══════════════════════════════════════════════════════════════════════════
async def _check_default_warehouse(db, tid):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" not in cols:
        return _check("default_warehouse", "business",
                      "مستودع افتراضي", "warning",
                      "عمود is_default غير موجود في inv_warehouses",
                      suggestion="ALTER TABLE inv_warehouses ADD COLUMN is_default BOOLEAN DEFAULT FALSE")

    r = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_warehouses
        WHERE tenant_id=:tid AND is_default=true AND is_active=true
    """), {"tid": tid})
    cnt = r.fetchone()._mapping["c"]
    if cnt == 0:
        return _check("default_warehouse", "business",
                      "مستودع افتراضي", "warning",
                      "لا يوجد مستودع افتراضي نشط",
                      suggestion="عيّن مستودعاً افتراضيّاً واحداً (يُستخدم تلقائياً عند الترحيل)")
    if cnt > 1:
        return _check("default_warehouse", "business",
                      "مستودع افتراضي", "warning",
                      f"يوجد {cnt} مستودع افتراضي (الـ ERP يحتاج 1 فقط)",
                      suggestion="ضع is_default=false على الإضافيّة")
    return _check("default_warehouse", "business",
                  "مستودع افتراضي", "ok",
                  "يوجد مستودع افتراضي واحد كما هو متوقّع")


async def _check_inactive_warehouses_with_active_zones(db, tid):
    r = await db.execute(text("""
        SELECT w.warehouse_code, w.warehouse_name, COUNT(z.id) AS active_zones
        FROM inv_warehouses w
        JOIN inv_zones z ON z.warehouse_id=w.id AND z.tenant_id=w.tenant_id
        WHERE w.tenant_id=:tid AND w.is_active=false AND z.is_active=true
        GROUP BY w.warehouse_code, w.warehouse_name
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("inactive_with_active_zones", "business",
                      "تناسق المستودعات الموقوفة", "ok",
                      "كل مستودع موقوف لا يحوي مناطق نشطة")
    return _check("inactive_with_active_zones", "business",
                  "تناسق المستودعات الموقوفة", "warning",
                  f"{len(rows)} مستودع موقوف به مناطق نشطة",
                  details=[
                      f"{r._mapping['warehouse_code']} - {r._mapping['warehouse_name']}: "
                      f"{r._mapping['active_zones']} منطقة نشطة"
                      for r in rows
                  ],
                  suggestion="إمّا تفعيل المستودع، أو تعطيل مناطقه")


async def _check_negative_capacity(db, tid):
    cols = await _get_columns(db, "inv_locations")
    capacity_cols = [
        c for c in ["max_capacity_qty", "max_capacity_volume", "max_capacity_weight"]
        if c in cols
    ]
    if not capacity_cols:
        return _check("negative_capacity", "business",
                      "سعة المواقع", "ok",
                      "أعمدة السعة غير موجودة (لا يوجد ما يُتحقَّق منه)")

    conditions = " OR ".join([f"{c} < 0" for c in capacity_cols])
    r = await db.execute(text(f"""
        SELECT location_code, location_name, max_capacity_qty,
               max_capacity_volume, max_capacity_weight
        FROM inv_locations
        WHERE tenant_id=:tid AND ({conditions})
        LIMIT 50
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("negative_capacity", "business",
                      "سعة المواقع", "ok",
                      "لا توجد قيم سعة سالبة")
    return _check("negative_capacity", "business",
                  "سعة المواقع", "warning",
                  f"{len(rows)} موقع بسعة سالبة",
                  details=[r._mapping["location_code"] for r in rows],
                  suggestion="UPDATE inv_locations SET max_capacity_X=0 WHERE max_capacity_X<0")


async def _check_empty_zones(db, tid):
    r = await db.execute(text("""
        SELECT z.zone_code, z.zone_name, w.warehouse_code
        FROM inv_zones z
        LEFT JOIN inv_locations l
          ON l.zone_id=z.id AND l.tenant_id=z.tenant_id AND l.is_active=true
        LEFT JOIN inv_warehouses w
          ON w.id=z.warehouse_id
        WHERE z.tenant_id=:tid AND z.is_active=true
        GROUP BY z.id, z.zone_code, z.zone_name, w.warehouse_code
        HAVING COUNT(l.id) = 0
        LIMIT 50
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return _check("empty_zones", "business",
                      "مناطق فارغة (بلا مواقع)", "ok",
                      "كل المناطق النشطة تحوي مواقع")
    return _check("empty_zones", "business",
                  "مناطق فارغة (بلا مواقع)", "warning",
                  f"{len(rows)} منطقة نشطة بلا مواقع",
                  details=[
                      f"{r._mapping['warehouse_code']} ← {r._mapping['zone_code']} ({r._mapping['zone_name']})"
                      for r in rows
                  ],
                  suggestion="أضف مواقع للمناطق الفارغة، أو عطّلها")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/warehouse-health")
async def warehouse_health_check(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    فحص صحّة شامل للمستودعات + المناطق + المواقع.
    14 فحص × 3 طبقات.
    """
    tid = str(user.tenant_id)
    checks = []

    # Layer 1: Schema
    try:
        checks.append(await _check_schema_warehouses(db))
        checks.append(await _check_schema_zones(db))
        checks.append(await _check_schema_locations(db))
        checks.append(await _check_tech_debt(db))
        checks.append(await _check_updated_at(db))
    except Exception as e:
        checks.append(_check("schema_error", "schema",
                             "أخطاء في فحوصات Schema", "error",
                             f"خطأ: {str(e)[:200]}"))

    # Layer 2: Data Integrity
    try:
        checks.append(await _check_orphan_zones(db, tid))
        checks.append(await _check_orphan_locations(db, tid))
        checks.append(await _check_duplicate_warehouses(db, tid))
        checks.append(await _check_duplicate_zones(db, tid))
        checks.append(await _check_duplicate_locations(db, tid))
    except Exception as e:
        checks.append(_check("integrity_error", "integrity",
                             "أخطاء في فحوصات Data Integrity", "error",
                             f"خطأ: {str(e)[:200]}"))

    # Layer 3: Business Logic
    try:
        checks.append(await _check_default_warehouse(db, tid))
        checks.append(await _check_inactive_warehouses_with_active_zones(db, tid))
        checks.append(await _check_negative_capacity(db, tid))
        checks.append(await _check_empty_zones(db, tid))
    except Exception as e:
        checks.append(_check("business_error", "business",
                             "أخطاء في فحوصات Business Logic", "error",
                             f"خطأ: {str(e)[:200]}"))

    # Summary
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
    })
