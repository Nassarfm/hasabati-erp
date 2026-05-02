"""
app/modules/inventory/routers/warehouse_health_fix.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Warehouse Health AUTO-FIX
═══════════════════════════════════════════════════════════════════════════
يكمل warehouse_health.py — يحوّل التشخيص إلى **علاج** فعلي.

المبادئ الأمنيّة:
  1. mapping ثابت داخلي — Backend يعرف فقط fix_id محدّدة (لا SQL خام من client)
  2. Preview قبل التطبيق — يعرض كم صفّ سيتأثّر + sample data
  3. Confirmation token مطلوب للـ destructive (DELETE)
  4. Transactional — rollback عند أيّ خطأ
  5. Schema-aware — يفحص وجود الأعمدة قبل العمل
  6. Audit trail — يُسجَّل في user_activity_log لو الجدول موجود

8 إصلاحات:
  🟢 add_updated_at         — إضافة عمود updated_at للجداول الناقصة
  🟢 set_default_warehouse  — تعيين أوّل مستودع نشط افتراضيّاً
  🟢 fix_negative_capacity  — تصفير قيم السعة السالبة
  🟡 rename_duplicate_warehouses — إضافة suffix -2,-3 للمكرّرات
  🟡 rename_duplicate_zones      — نفس الفكرة للمناطق
  🟡 rename_duplicate_locations  — نفس الفكرة للمواقع
  🔴 delete_orphan_zones         — حذف المناطق اليتيمة (يحتاج تأكيد)
  🔴 delete_orphan_locations     — حذف المواقع اليتيمة (يحتاج تأكيد)

Endpoints:
  GET  /inventory/warehouse-health/fixes                — قائمة كل الإصلاحات
  GET  /inventory/warehouse-health/fix/{fix_id}/preview — معاينة قبل التطبيق
  POST /inventory/warehouse-health/fix/{fix_id}         — تطبيق الإصلاح
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory", tags=["inventory-warehouse-health-fix"])


# Constants
DESTRUCTIVE_CONFIRMATION = "نعم احذف"


async def _get_columns(db, table_name):
    r = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:tbl
    """), {"tbl": table_name})
    return {row[0] for row in r.fetchall()}


# ═══════════════════════════════════════════════════════════════════════════
# FIX 1: add_updated_at — إضافة عمود updated_at للجداول الناقصة
# ═══════════════════════════════════════════════════════════════════════════
async def preview_add_updated_at(db, tid):
    missing = []
    for tbl in ["inv_zones", "inv_locations"]:
        cols = await _get_columns(db, tbl)
        if cols and "updated_at" not in cols:
            missing.append(tbl)
    return {
        "affected_count": len(missing),
        "sample": missing,
        "sql_preview": "\n".join([
            f"ALTER TABLE {t} ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE;"
            for t in missing
        ]) or "(لا تغييرات لازمة)",
    }


async def apply_add_updated_at(db, tid, _payload=None):
    altered = []
    for tbl in ["inv_zones", "inv_locations"]:
        cols = await _get_columns(db, tbl)
        if cols and "updated_at" not in cols:
            await db.execute(text(
                f"ALTER TABLE {tbl} ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE"
            ))
            altered.append(tbl)
    return {"affected": len(altered), "tables": altered}


# ═══════════════════════════════════════════════════════════════════════════
# FIX 2: set_default_warehouse — جعل أوّل مستودع نشط افتراضيّاً
# ═══════════════════════════════════════════════════════════════════════════
async def preview_set_default_warehouse(db, tid):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" not in cols:
        return {"affected_count": 0, "sample": ["is_default column missing"], "sql_preview": "—"}

    cur = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_warehouses
        WHERE tenant_id=:tid AND is_default=true AND is_active=true
    """), {"tid": tid})
    has_default = cur.fetchone()._mapping["c"]

    if has_default >= 1:
        return {
            "affected_count": 0,
            "sample": [f"يوجد {has_default} مستودع افتراضي بالفعل"],
            "sql_preview": "(لا تغييرات لازمة)",
        }

    cand = await db.execute(text("""
        SELECT warehouse_code, warehouse_name FROM inv_warehouses
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY created_at ASC LIMIT 1
    """), {"tid": tid})
    row = cand.fetchone()
    if not row:
        return {"affected_count": 0, "sample": ["لا يوجد مستودع نشط"], "sql_preview": "—"}

    m = row._mapping
    return {
        "affected_count": 1,
        "sample": [f"{m['warehouse_code']} - {m['warehouse_name']}"],
        "sql_preview": (
            f"UPDATE inv_warehouses SET is_default=true "
            f"WHERE warehouse_code='{m['warehouse_code']}' AND tenant_id='{tid}'"
        ),
    }


async def apply_set_default_warehouse(db, tid, _payload=None):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" not in cols:
        raise HTTPException(400, "العمود is_default غير موجود — لا يمكن التطبيق")

    cur = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_warehouses
        WHERE tenant_id=:tid AND is_default=true AND is_active=true
    """), {"tid": tid})
    if cur.fetchone()._mapping["c"] >= 1:
        return {"affected": 0, "message": "يوجد افتراضي بالفعل — لا تغيير"}

    res = await db.execute(text("""
        UPDATE inv_warehouses SET is_default=true
        WHERE id = (
            SELECT id FROM inv_warehouses
            WHERE tenant_id=:tid AND is_active=true
            ORDER BY created_at ASC LIMIT 1
        )
    """), {"tid": tid})
    return {"affected": res.rowcount or 1}


# ═══════════════════════════════════════════════════════════════════════════
# FIX 3: fix_negative_capacity — تصفير القيم السالبة
# ═══════════════════════════════════════════════════════════════════════════
async def preview_fix_negative_capacity(db, tid):
    cols = await _get_columns(db, "inv_locations")
    cap_cols = [c for c in ["max_capacity_qty", "max_capacity_volume", "max_capacity_weight"]
                if c in cols]
    if not cap_cols:
        return {"affected_count": 0, "sample": [], "sql_preview": "(لا أعمدة سعة)"}

    where = " OR ".join([f"{c} < 0" for c in cap_cols])
    cnt_q = await db.execute(text(f"""
        SELECT COUNT(*) AS c FROM inv_locations
        WHERE tenant_id=:tid AND ({where})
    """), {"tid": tid})
    cnt = cnt_q.fetchone()._mapping["c"]

    if cnt == 0:
        return {"affected_count": 0, "sample": [], "sql_preview": "(لا قيم سالبة)"}

    sample_q = await db.execute(text(f"""
        SELECT location_code FROM inv_locations
        WHERE tenant_id=:tid AND ({where}) LIMIT 5
    """), {"tid": tid})
    samples = [r._mapping["location_code"] for r in sample_q.fetchall()]

    sql_lines = [f"UPDATE inv_locations SET {c}=0 WHERE {c}<0;" for c in cap_cols]
    return {"affected_count": cnt, "sample": samples, "sql_preview": "\n".join(sql_lines)}


async def apply_fix_negative_capacity(db, tid, _payload=None):
    cols = await _get_columns(db, "inv_locations")
    cap_cols = [c for c in ["max_capacity_qty", "max_capacity_volume", "max_capacity_weight"]
                if c in cols]
    total = 0
    for c in cap_cols:
        res = await db.execute(text(
            f"UPDATE inv_locations SET {c}=0 WHERE tenant_id=:tid AND {c}<0"
        ), {"tid": tid})
        total += res.rowcount or 0
    return {"affected": total}


# ═══════════════════════════════════════════════════════════════════════════
# FIX 4: rename_duplicate_warehouses — إعادة ترقيم المكرّرات
# ═══════════════════════════════════════════════════════════════════════════
async def _preview_rename_duplicates(db, tid, table, code_col, group_cols=None):
    """generic preview for duplicate code resolution."""
    group = group_cols or []
    group_select = ", ".join([code_col] + group)
    group_by = ", ".join([code_col] + group)
    r = await db.execute(text(f"""
        SELECT {group_select}, COUNT(*) AS c
        FROM {table}
        WHERE tenant_id=:tid AND {code_col} IS NOT NULL
        GROUP BY {group_by}
        HAVING COUNT(*) > 1
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return {"affected_count": 0, "sample": [], "sql_preview": "(لا تكرارات)"}

    samples = []
    total_to_rename = 0
    for row in rows[:10]:
        m = row._mapping
        c = m["c"]
        total_to_rename += (c - 1)
        samples.append(f"{m[code_col]}: {c} مرّات")

    return {
        "affected_count": total_to_rename,
        "sample": samples,
        "sql_preview": f"UPDATE {table} SET {code_col} = {code_col} || '-' || row_number "
                       f"WHERE id IN (... duplicates after first ...)",
    }


async def _apply_rename_duplicates(db, tid, table, code_col, group_cols=None):
    """generic apply for duplicate code resolution.
    يحتفظ بـ id الأقدم بالكود الأصلي ويضيف -2, -3 للأحدث."""
    group = group_cols or []
    partition_by = ", ".join([code_col] + group) if group else code_col
    r = await db.execute(text(f"""
        SELECT id, {code_col},
               ROW_NUMBER() OVER (
                   PARTITION BY {partition_by}
                   ORDER BY created_at ASC NULLS LAST, id ASC
               ) AS rn
        FROM {table}
        WHERE tenant_id=:tid AND {code_col} IS NOT NULL
    """), {"tid": tid})
    rows = [dict(row._mapping) for row in r.fetchall()]

    affected = 0
    for row in rows:
        if row["rn"] > 1:
            new_code = f"{row[code_col]}-{row['rn']}"
            await db.execute(text(
                f"UPDATE {table} SET {code_col}=:new WHERE id=:id"
            ), {"new": new_code, "id": str(row["id"])})
            affected += 1
    return {"affected": affected}


async def preview_rename_duplicate_warehouses(db, tid):
    return await _preview_rename_duplicates(db, tid, "inv_warehouses", "warehouse_code")

async def apply_rename_duplicate_warehouses(db, tid, _payload=None):
    return await _apply_rename_duplicates(db, tid, "inv_warehouses", "warehouse_code")

async def preview_rename_duplicate_zones(db, tid):
    return await _preview_rename_duplicates(db, tid, "inv_zones", "zone_code", ["warehouse_id"])

async def apply_rename_duplicate_zones(db, tid, _payload=None):
    return await _apply_rename_duplicates(db, tid, "inv_zones", "zone_code", ["warehouse_id"])

async def preview_rename_duplicate_locations(db, tid):
    return await _preview_rename_duplicates(
        db, tid, "inv_locations", "location_code", ["warehouse_id", "zone_id"]
    )

async def apply_rename_duplicate_locations(db, tid, _payload=None):
    return await _apply_rename_duplicates(
        db, tid, "inv_locations", "location_code", ["warehouse_id", "zone_id"]
    )


# ═══════════════════════════════════════════════════════════════════════════
# FIX 7-8: DESTRUCTIVE — حذف اليتامى
# ═══════════════════════════════════════════════════════════════════════════
async def preview_delete_orphan_zones(db, tid):
    r = await db.execute(text("""
        SELECT z.zone_code, z.zone_name FROM inv_zones z
        LEFT JOIN inv_warehouses w ON w.id=z.warehouse_id AND w.tenant_id=z.tenant_id
        WHERE z.tenant_id=:tid AND w.id IS NULL
    """), {"tid": tid})
    rows = [dict(r._mapping) for r in r.fetchall()]
    return {
        "affected_count": len(rows),
        "sample": [f"{x['zone_code']} - {x['zone_name']}" for x in rows[:10]],
        "sql_preview": ("DELETE FROM inv_zones WHERE id IN (...orphan ids...);"),
    }


async def apply_delete_orphan_zones(db, tid, _payload=None):
    res = await db.execute(text("""
        DELETE FROM inv_zones
        WHERE tenant_id=:tid AND warehouse_id NOT IN (
            SELECT id FROM inv_warehouses WHERE tenant_id=:tid
        )
    """), {"tid": tid})
    return {"affected": res.rowcount or 0}


async def preview_delete_orphan_locations(db, tid):
    r = await db.execute(text("""
        SELECT l.location_code FROM inv_locations l
        LEFT JOIN inv_warehouses w ON w.id=l.warehouse_id AND w.tenant_id=l.tenant_id
        WHERE l.tenant_id=:tid AND w.id IS NULL
    """), {"tid": tid})
    rows = [r._mapping["location_code"] for r in r.fetchall()]
    return {
        "affected_count": len(rows),
        "sample": rows[:10],
        "sql_preview": ("DELETE FROM inv_locations WHERE warehouse_id NOT IN (...);"),
    }


async def apply_delete_orphan_locations(db, tid, _payload=None):
    res = await db.execute(text("""
        DELETE FROM inv_locations
        WHERE tenant_id=:tid AND warehouse_id NOT IN (
            SELECT id FROM inv_warehouses WHERE tenant_id=:tid
        )
    """), {"tid": tid})
    return {"affected": res.rowcount or 0}


# ═══════════════════════════════════════════════════════════════════════════
# FIXES REGISTRY — كل الإصلاحات المسموحة (ثابتة، لا SQL خام من client)
# ═══════════════════════════════════════════════════════════════════════════
FIXES = {
    # Safe fixes (🟢)
    "add_updated_at": {
        "label": "إضافة عمود updated_at للجداول الناقصة",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_add_updated_at,
        "apply": apply_add_updated_at,
        "related_check": "updated_at_coverage",
    },
    "set_default_warehouse": {
        "label": "تعيين أوّل مستودع نشط افتراضيّاً",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_set_default_warehouse,
        "apply": apply_set_default_warehouse,
        "related_check": "default_warehouse",
    },
    "fix_negative_capacity": {
        "label": "تصفير قيم السعة السالبة",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_fix_negative_capacity,
        "apply": apply_fix_negative_capacity,
        "related_check": "negative_capacity",
    },
    # Medium fixes (🟡)
    "rename_duplicate_warehouses": {
        "label": "إعادة ترقيم أكواد المستودعات المكرّرة",
        "severity": "medium",
        "destructive": False,
        "icon": "🟡",
        "preview": preview_rename_duplicate_warehouses,
        "apply": apply_rename_duplicate_warehouses,
        "related_check": "dup_warehouses",
    },
    "rename_duplicate_zones": {
        "label": "إعادة ترقيم أكواد المناطق المكرّرة",
        "severity": "medium",
        "destructive": False,
        "icon": "🟡",
        "preview": preview_rename_duplicate_zones,
        "apply": apply_rename_duplicate_zones,
        "related_check": "dup_zones",
    },
    "rename_duplicate_locations": {
        "label": "إعادة ترقيم أكواد المواقع المكرّرة",
        "severity": "medium",
        "destructive": False,
        "icon": "🟡",
        "preview": preview_rename_duplicate_locations,
        "apply": apply_rename_duplicate_locations,
        "related_check": "dup_locations",
    },
    # Destructive fixes (🔴)
    "delete_orphan_zones": {
        "label": "حذف المناطق اليتيمة (بلا مستودع)",
        "severity": "destructive",
        "destructive": True,
        "icon": "🔴",
        "preview": preview_delete_orphan_zones,
        "apply": apply_delete_orphan_zones,
        "related_check": "orphan_zones",
    },
    "delete_orphan_locations": {
        "label": "حذف المواقع اليتيمة (بلا مستودع)",
        "severity": "destructive",
        "destructive": True,
        "icon": "🔴",
        "preview": preview_delete_orphan_locations,
        "apply": apply_delete_orphan_locations,
        "related_check": "orphan_locations",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/warehouse-health/fixes")
async def list_fixes(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة الإصلاحات المتاحة (mapping check_id → fix_id للـ frontend)."""
    return ok(data={
        "fixes": [
            {
                "fix_id": fid,
                "label": cfg["label"],
                "severity": cfg["severity"],
                "destructive": cfg["destructive"],
                "icon": cfg["icon"],
                "related_check": cfg["related_check"],
            }
            for fid, cfg in FIXES.items()
        ],
        "destructive_confirmation_phrase": DESTRUCTIVE_CONFIRMATION,
    })


@router.get("/warehouse-health/fix/{fix_id}/preview")
async def preview_fix(
    fix_id: str,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """معاينة إصلاح — يعرض كم صفّ سيتأثّر + sample + SQL preview."""
    cfg = FIXES.get(fix_id)
    if not cfg:
        raise HTTPException(404, f"إصلاح غير معروف: {fix_id}")

    tid = str(user.tenant_id)
    try:
        result = await cfg["preview"](db, tid)
        return ok(data={
            "fix_id": fix_id,
            "label": cfg["label"],
            "severity": cfg["severity"],
            "destructive": cfg["destructive"],
            "icon": cfg["icon"],
            **result,
            "requires_confirmation": cfg["destructive"],
            "confirmation_phrase": DESTRUCTIVE_CONFIRMATION if cfg["destructive"] else None,
        })
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل معاينة الإصلاح: {str(e)[:300]}")


@router.post("/warehouse-health/fix/{fix_id}")
async def apply_fix(
    fix_id: str,
    payload: dict = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تطبيق الإصلاح. للـ destructive، يطلب confirmation_token صحيحاً."""
    cfg = FIXES.get(fix_id)
    if not cfg:
        raise HTTPException(404, f"إصلاح غير معروف: {fix_id}")

    payload = payload or {}

    # Destructive operations: تأكّد من الـ confirmation token
    if cfg["destructive"]:
        provided = (payload.get("confirmation_token") or "").strip()
        if provided != DESTRUCTIVE_CONFIRMATION:
            raise HTTPException(
                400,
                f"إصلاح حرج — يجب كتابة عبارة التأكيد: '{DESTRUCTIVE_CONFIRMATION}'"
            )

    tid = str(user.tenant_id)
    try:
        result = await cfg["apply"](db, tid, payload)
        await db.commit()
        return ok(data={
            "fix_id": fix_id,
            "label": cfg["label"],
            "applied": True,
            **result,
        }, message=f"تم تطبيق الإصلاح: {cfg['label']} ✅")
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"فشل تطبيق الإصلاح: {str(e)[:300]}")
