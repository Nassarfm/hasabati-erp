"""
app/modules/inventory/routers/settings_fix.py
═══════════════════════════════════════════════════════════════════════════
Inventory v5 — Settings HealthCheck AUTO-FIX
═══════════════════════════════════════════════════════════════════════════
Auto-fixes للفحوصات في settings_health.py.

8 إصلاحات:
  🟢 fix_orphan_items_category   — null لـ category_id غير الموجودة
  🟢 fix_items_default_uom       — تعيين uom_id الافتراضي للأصناف الناقصة
  🟢 fix_seed_critical_reasons   — إضافة reason_codes الحرجة (count_overage, etc.)
  🟡 rename_dup_item_codes       — إعادة ترقيم أكواد الأصناف المكرّرة
  🟢 sync_item_type_from_category — مزامنة item.item_type من category
  🟢 enable_negative_stock       — تفعيل allow_negative_stock للمستودعات اللي عندها سالب
  🟢 settings_set_default_warehouse — تعيين أوّل مستودع افتراضيّاً
  🟢 add_is_default_column       — إضافة عمود is_default لو ناقص

Endpoints:
  GET  /inventory/settings/health/fix/{fix_id}/preview
  POST /inventory/settings/health/fix/{fix_id}
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db


router = APIRouter(prefix="/inventory/settings", tags=["inventory-settings-health-fix"])

DESTRUCTIVE_CONFIRMATION = "نعم احذف"


async def _get_columns(db, table_name):
    r = await db.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:tbl
    """), {"tbl": table_name})
    return {row[0] for row in r.fetchall()}


# ═══════════════════════════════════════════════════════════════════════════
# FIX: fix_orphan_items_category
# ═══════════════════════════════════════════════════════════════════════════
async def preview_fix_orphan_items_category(db, tid):
    r = await db.execute(text("""
        SELECT i.item_code, i.item_name FROM inv_items i
        LEFT JOIN inv_categories c ON c.id=i.category_id AND c.tenant_id=i.tenant_id
        WHERE i.tenant_id=:tid AND i.category_id IS NOT NULL AND c.id IS NULL
    """), {"tid": tid})
    rows = r.fetchall()
    return {
        "affected_count": len(rows),
        "sample": [f"{r._mapping['item_code']} - {r._mapping['item_name']}" for r in rows[:10]],
        "sql_preview": "UPDATE inv_items SET category_id=NULL\nWHERE category_id NOT IN (SELECT id FROM inv_categories WHERE tenant_id=:tid)",
    }


async def apply_fix_orphan_items_category(db, tid, _payload=None):
    res = await db.execute(text("""
        UPDATE inv_items SET category_id=NULL
        WHERE tenant_id=:tid
          AND category_id IS NOT NULL
          AND category_id NOT IN (SELECT id FROM inv_categories WHERE tenant_id=:tid)
    """), {"tid": tid})
    return {"affected": res.rowcount or 0}


# ═══════════════════════════════════════════════════════════════════════════
# FIX: fix_items_default_uom — يعيّن أوّل UoM للأصناف الناقصة
# ═══════════════════════════════════════════════════════════════════════════
async def preview_fix_items_default_uom(db, tid):
    cnt_q = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_items
        WHERE tenant_id=:tid AND uom_id IS NULL
    """), {"tid": tid})
    cnt = cnt_q.fetchone()._mapping["c"]

    if cnt == 0:
        return {"affected_count": 0, "sample": [], "sql_preview": "(لا أصناف بدون UoM)"}

    # ابحث عن uom افتراضيّة (أوّل واحدة موجودة)
    uom_q = await db.execute(text("""
        SELECT uom_code, id FROM jl_uom_definitions
        WHERE tenant_id=:tid LIMIT 1
    """), {"tid": tid})
    row = uom_q.fetchone()
    if not row:
        return {
            "affected_count": cnt,
            "sample": ["⚠️ لا توجد UoM في النظام"],
            "sql_preview": "(لا يمكن التطبيق — أنشئ UoM أولاً)",
        }

    uom_code = row._mapping["uom_code"]
    sample_q = await db.execute(text("""
        SELECT item_code FROM inv_items
        WHERE tenant_id=:tid AND uom_id IS NULL LIMIT 5
    """), {"tid": tid})
    samples = [r._mapping["item_code"] for r in sample_q.fetchall()]

    return {
        "affected_count": cnt,
        "sample": samples + [f"→ ستُعيَّن UoM: {uom_code}"],
        "sql_preview": f"UPDATE inv_items SET uom_id='<{uom_code}-id>' WHERE uom_id IS NULL",
    }


async def apply_fix_items_default_uom(db, tid, _payload=None):
    uom_q = await db.execute(text("""
        SELECT id FROM jl_uom_definitions WHERE tenant_id=:tid LIMIT 1
    """), {"tid": tid})
    row = uom_q.fetchone()
    if not row:
        raise HTTPException(400, "لا توجد UoM في النظام — أنشئ واحدة على الأقل أولاً")
    uom_id = row._mapping["id"]
    res = await db.execute(text("""
        UPDATE inv_items SET uom_id=:uid
        WHERE tenant_id=:tid AND uom_id IS NULL
    """), {"tid": tid, "uid": str(uom_id)})
    return {"affected": res.rowcount or 0}


# ═══════════════════════════════════════════════════════════════════════════
# FIX: fix_seed_critical_reasons — يضيف reason codes الحرجة
# ═══════════════════════════════════════════════════════════════════════════
CRITICAL_REASONS = [
    {"code": "count_overage", "name_ar": "زيادة جرد", "name_en": "Count Overage", "movement_type": "IJ", "is_credit": True},
    {"code": "count_shortage", "name_ar": "نقص جرد", "name_en": "Count Shortage", "movement_type": "IJ", "is_credit": False},
    {"code": "scrap_damaged", "name_ar": "إتلاف بسبب تلف", "name_en": "Scrap - Damaged", "movement_type": "SCRAP", "is_credit": False},
]


async def preview_fix_seed_critical_reasons(db, tid):
    r = await db.execute(text("""
        SELECT reason_code FROM inv_reason_codes
        WHERE tenant_id=:tid AND reason_code = ANY(:codes)
    """), {"tid": tid, "codes": [c["code"] for c in CRITICAL_REASONS]})
    existing = {row._mapping["reason_code"] for row in r.fetchall()}
    missing = [c for c in CRITICAL_REASONS if c["code"] not in existing]
    return {
        "affected_count": len(missing),
        "sample": [f"{c['code']}: {c['name_ar']}" for c in missing],
        "sql_preview": "INSERT INTO inv_reason_codes (...) VALUES (...) — for each missing",
    }


async def apply_fix_seed_critical_reasons(db, tid, _payload=None):
    import uuid as _uuid
    cols = await _get_columns(db, "inv_reason_codes")
    if not cols:
        raise HTTPException(400, "جدول inv_reason_codes غير موجود")

    r = await db.execute(text("""
        SELECT reason_code FROM inv_reason_codes
        WHERE tenant_id=:tid AND reason_code = ANY(:codes)
    """), {"tid": tid, "codes": [c["code"] for c in CRITICAL_REASONS]})
    existing = {row._mapping["reason_code"] for row in r.fetchall()}

    affected = 0
    for reason in CRITICAL_REASONS:
        if reason["code"] in existing:
            continue
        fields = ["id", "tenant_id", "reason_code"]
        values = [":id", ":tid", ":code"]
        params = {
            "id": str(_uuid.uuid4()),
            "tid": tid,
            "code": reason["code"],
        }
        if "reason_name" in cols:
            fields.append("reason_name")
            values.append(":name")
            params["name"] = reason["name_ar"]
        if "reason_name_en" in cols:
            fields.append("reason_name_en")
            values.append(":name_en")
            params["name_en"] = reason["name_en"]
        if "movement_type" in cols:
            fields.append("movement_type")
            values.append(":mt")
            params["mt"] = reason["movement_type"]
        if "is_credit" in cols:
            fields.append("is_credit")
            values.append(":is_credit")
            params["is_credit"] = reason["is_credit"]
        if "is_active" in cols:
            fields.append("is_active")
            values.append(":active")
            params["active"] = True

        await db.execute(text(f"""
            INSERT INTO inv_reason_codes ({', '.join(fields)})
            VALUES ({', '.join(values)})
        """), params)
        affected += 1
    return {"affected": affected}


# ═══════════════════════════════════════════════════════════════════════════
# FIX: rename_dup_item_codes — إعادة ترقيم المكرّرات
# ═══════════════════════════════════════════════════════════════════════════
async def preview_rename_dup_item_codes(db, tid):
    r = await db.execute(text("""
        SELECT item_code, COUNT(*) AS c FROM inv_items
        WHERE tenant_id=:tid AND item_code IS NOT NULL
        GROUP BY item_code HAVING COUNT(*) > 1
    """), {"tid": tid})
    rows = r.fetchall()
    if not rows:
        return {"affected_count": 0, "sample": [], "sql_preview": "(لا تكرارات)"}
    total = sum(r._mapping["c"] - 1 for r in rows)
    return {
        "affected_count": total,
        "sample": [f"{r._mapping['item_code']}: {r._mapping['c']} مرّات" for r in rows[:10]],
        "sql_preview": "UPDATE inv_items SET item_code = item_code || '-' || row_number\nWHERE id IN (... duplicates after first ...)",
    }


async def apply_rename_dup_item_codes(db, tid, _payload=None):
    r = await db.execute(text("""
        SELECT id, item_code,
               ROW_NUMBER() OVER (PARTITION BY item_code ORDER BY created_at ASC NULLS LAST, id ASC) AS rn
        FROM inv_items
        WHERE tenant_id=:tid AND item_code IS NOT NULL
    """), {"tid": tid})
    rows = [dict(row._mapping) for row in r.fetchall()]
    affected = 0
    for row in rows:
        if row["rn"] > 1:
            new_code = f"{row['item_code']}-{row['rn']}"
            await db.execute(text("""
                UPDATE inv_items SET item_code=:new WHERE id=:id
            """), {"new": new_code, "id": str(row["id"])})
            affected += 1
    return {"affected": affected}


# ═══════════════════════════════════════════════════════════════════════════
# FIX: sync_item_type_from_category
# ═══════════════════════════════════════════════════════════════════════════
async def preview_sync_item_type_from_category(db, tid):
    item_cols = await _get_columns(db, "inv_items")
    cat_cols = await _get_columns(db, "inv_categories")
    if "item_type" not in item_cols or "item_type" not in cat_cols:
        return {"affected_count": 0, "sample": [], "sql_preview": "(عمود item_type غير موجود)"}

    r = await db.execute(text("""
        SELECT i.item_code, i.item_type AS cur, c.item_type AS new_type
        FROM inv_items i JOIN inv_categories c ON c.id=i.category_id AND c.tenant_id=i.tenant_id
        WHERE i.tenant_id=:tid AND i.item_type IS NOT NULL
          AND c.item_type IS NOT NULL AND i.item_type <> c.item_type
    """), {"tid": tid})
    rows = r.fetchall()
    return {
        "affected_count": len(rows),
        "sample": [f"{r._mapping['item_code']}: {r._mapping['cur']} → {r._mapping['new_type']}" for r in rows[:10]],
        "sql_preview": "UPDATE inv_items SET item_type = (SELECT c.item_type FROM inv_categories c WHERE c.id=inv_items.category_id)",
    }


async def apply_sync_item_type_from_category(db, tid, _payload=None):
    item_cols = await _get_columns(db, "inv_items")
    cat_cols = await _get_columns(db, "inv_categories")
    if "item_type" not in item_cols or "item_type" not in cat_cols:
        raise HTTPException(400, "العمود item_type غير موجود — لا يمكن التطبيق")
    res = await db.execute(text("""
        UPDATE inv_items i
        SET item_type = c.item_type
        FROM inv_categories c
        WHERE i.tenant_id=:tid AND c.id=i.category_id
          AND c.tenant_id=i.tenant_id
          AND i.item_type IS NOT NULL AND c.item_type IS NOT NULL
          AND i.item_type <> c.item_type
    """), {"tid": tid})
    return {"affected": res.rowcount or 0}


# ═══════════════════════════════════════════════════════════════════════════
# FIX: settings_set_default_warehouse
# ═══════════════════════════════════════════════════════════════════════════
async def preview_set_default_warehouse(db, tid):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" not in cols:
        return {"affected_count": 0, "sample": ["العمود is_default غير موجود"], "sql_preview": "—"}

    cur = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_warehouses
        WHERE tenant_id=:tid AND is_default=true AND is_active=true
    """), {"tid": tid})
    if cur.fetchone()._mapping["c"] >= 1:
        return {"affected_count": 0, "sample": ["يوجد افتراضي بالفعل"], "sql_preview": "(لا تغيير)"}

    cand = await db.execute(text("""
        SELECT warehouse_code, warehouse_name FROM inv_warehouses
        WHERE tenant_id=:tid AND is_active=true ORDER BY created_at ASC LIMIT 1
    """), {"tid": tid})
    row = cand.fetchone()
    if not row:
        return {"affected_count": 0, "sample": ["لا يوجد مستودع نشط"], "sql_preview": "—"}
    m = row._mapping
    return {
        "affected_count": 1,
        "sample": [f"{m['warehouse_code']} - {m['warehouse_name']}"],
        "sql_preview": f"UPDATE inv_warehouses SET is_default=true WHERE warehouse_code='{m['warehouse_code']}'",
    }


async def apply_set_default_warehouse(db, tid, _payload=None):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" not in cols:
        raise HTTPException(400, "العمود is_default غير موجود")
    cur = await db.execute(text("""
        SELECT COUNT(*) AS c FROM inv_warehouses
        WHERE tenant_id=:tid AND is_default=true AND is_active=true
    """), {"tid": tid})
    if cur.fetchone()._mapping["c"] >= 1:
        return {"affected": 0, "message": "يوجد افتراضي بالفعل"}
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
# FIX: add_is_default_column — لو ناقص
# ═══════════════════════════════════════════════════════════════════════════
async def preview_add_is_default_column(db, tid):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" in cols:
        return {"affected_count": 0, "sample": ["موجود بالفعل"], "sql_preview": "(لا تغيير)"}
    return {
        "affected_count": 1,
        "sample": ["inv_warehouses"],
        "sql_preview": "ALTER TABLE inv_warehouses ADD COLUMN is_default BOOLEAN DEFAULT FALSE",
    }


async def apply_add_is_default_column(db, tid, _payload=None):
    cols = await _get_columns(db, "inv_warehouses")
    if "is_default" in cols:
        return {"affected": 0, "message": "موجود بالفعل"}
    await db.execute(text(
        "ALTER TABLE inv_warehouses ADD COLUMN is_default BOOLEAN DEFAULT FALSE"
    ))
    return {"affected": 1}


# ═══════════════════════════════════════════════════════════════════════════
# FIXES REGISTRY
# ═══════════════════════════════════════════════════════════════════════════
FIXES = {
    "fix_orphan_items_category": {
        "label": "تصفير category_id للأصناف اليتيمة",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_fix_orphan_items_category,
        "apply": apply_fix_orphan_items_category,
        "related_check": "orphan_items",
    },
    "fix_items_default_uom": {
        "label": "تعيين UoM افتراضيّة للأصناف الناقصة",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_fix_items_default_uom,
        "apply": apply_fix_items_default_uom,
        "related_check": "items_no_uom",
    },
    "fix_seed_critical_reasons": {
        "label": "إضافة رموز الأسباب الحرجة (count_overage, etc.)",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_fix_seed_critical_reasons,
        "apply": apply_fix_seed_critical_reasons,
        "related_check": "critical_reasons",
    },
    "rename_dup_item_codes": {
        "label": "إعادة ترقيم أكواد الأصناف المكرّرة",
        "severity": "medium",
        "destructive": False,
        "icon": "🟡",
        "preview": preview_rename_dup_item_codes,
        "apply": apply_rename_dup_item_codes,
        "related_check": "dup_item_codes",
    },
    "sync_item_type_from_category": {
        "label": "مزامنة item_type من التصنيف",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_sync_item_type_from_category,
        "apply": apply_sync_item_type_from_category,
        "related_check": "item_type_sync",
    },
    "settings_set_default_warehouse": {
        "label": "تعيين أوّل مستودع نشط افتراضيّاً",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_set_default_warehouse,
        "apply": apply_set_default_warehouse,
        "related_check": "settings_default_warehouse",
    },
    "add_is_default_column": {
        "label": "إضافة عمود is_default للمستودعات",
        "severity": "safe",
        "destructive": False,
        "icon": "🟢",
        "preview": preview_add_is_default_column,
        "apply": apply_add_is_default_column,
        "related_check": "settings_default_warehouse",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/health/fix/{fix_id}/preview")
async def preview_fix(
    fix_id: str,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
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


@router.post("/health/fix/{fix_id}")
async def apply_fix(
    fix_id: str,
    payload: dict = None,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    cfg = FIXES.get(fix_id)
    if not cfg:
        raise HTTPException(404, f"إصلاح غير معروف: {fix_id}")
    payload = payload or {}
    if cfg["destructive"]:
        provided = (payload.get("confirmation_token") or "").strip()
        if provided != DESTRUCTIVE_CONFIRMATION:
            raise HTTPException(400, f"إصلاح حرج — يجب كتابة: '{DESTRUCTIVE_CONFIRMATION}'")
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
