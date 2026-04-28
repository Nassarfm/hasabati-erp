"""
admin_router.py — Admin Tools: Backup & Reset
Path: app/modules/admin/router.py
"""
import uuid, json
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import io

from app.db.session import get_db
from app.core.tenant import CurrentUser, get_current_user

router = APIRouter(prefix="/admin", tags=["Admin Tools"])

TABLES_TO_BACKUP = [
    "journal_entries",
    "je_lines",
    "je_attachments",
    "account_balances",
    "tr_cash_transactions",
    "tr_bank_transactions",
    "tr_internal_transfers",
    "tr_petty_cash_expenses",
    "tr_petty_cash_expense_lines",
    "tr_replenishments",
    "tr_bank_fees",
    "tr_checks",
    "tr_recurring_transactions",
    "parties",
    "party_role_definitions",
    "accounts",
    "dimensions",
    "dimension_values",
    "branches",
    "cost_centers",
    "projects",
    "fiscal_periods",
    "fiscal_years",
    "number_series",
    "tax_types",
    "company_settings",
    "user_activity_log",
]

TABLES_TO_RESET = [
    # أولاً: الجداول الفرعية (foreign keys)
    "je_attachments",
    "je_lines",
    "account_balances",
    # القيود المحاسبية
    "journal_entries",
    # حركات الخزينة
    "tr_petty_cash_expense_lines",
    "tr_petty_cash_expenses",
    "tr_replenishments",
    "tr_bank_fees",
    "tr_checks",
    "tr_recurring_transactions",
    "tr_cash_transactions",
    "tr_bank_transactions",
    "tr_internal_transfers",
    # سجل النشاط (اختياري)
    "user_activity_log",
]

RESET_BALANCES = [
    "tr_bank_accounts",
    "tr_petty_cash_funds",
]

def _require_admin(user: CurrentUser):
    """التحقق من صلاحية المدير"""
    if hasattr(user, 'is_admin') and user.is_admin:
        return True
    if hasattr(user, 'role') and user.role in ('admin', 'system_admin', 'superadmin'):
        return True
    if hasattr(user, 'is_owner') and user.is_owner:
        return True
    raise HTTPException(403, "هذه العملية تتطلب صلاحية مدير النظام")


@router.get("/is-admin")
async def check_is_admin(
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """التحقق إذا كان المستخدم مديراً — يُستدعى من الـ Frontend"""
    tid = str(user.tenant_id)
    email = str(user.email)

    # 1. تحقق من خصائص الـ CurrentUser object
    if hasattr(user, 'is_admin') and user.is_admin:
        return {"data": {"is_admin": True, "method": "user_object"}}
    if hasattr(user, 'role') and user.role in ('admin', 'system_admin', 'superadmin'):
        return {"data": {"is_admin": True, "method": "user_role"}}

    # 2. تحقق من جدول user_roles في قاعدة البيانات
    try:
        r = await db.execute(text("""
            SELECT ur.role_name
            FROM user_roles ur
            WHERE ur.tenant_id = :tid
              AND ur.user_email = :email
              AND ur.role_name IN ('admin','system_admin','superadmin','owner')
            LIMIT 1
        """), {"tid": tid, "email": email})
        row = r.fetchone()
        if row:
            return {"data": {"is_admin": True, "method": "user_roles_table"}}
    except Exception:
        pass

    # 3. تحقق إذا كان أول مستخدم في الـ tenant (المالك)
    try:
        r = await db.execute(text("""
            SELECT COUNT(*) FROM user_roles WHERE tenant_id = :tid
        """), {"tid": tid})
        count = r.scalar() or 0
        # إذا لا يوجد أي أدوار محددة = المستخدم الوحيد هو المالك
        if count == 0:
            return {"data": {"is_admin": True, "method": "sole_user"}}
    except Exception:
        pass

    # 4. تحقق من جدول users إن وُجد
    try:
        r = await db.execute(text("""
            SELECT role FROM users
            WHERE tenant_id = :tid AND email = :email
            LIMIT 1
        """), {"tid": tid, "email": email})
        row = r.mappings().fetchone()
        if row and row.get("role") in ('admin', 'system_admin', 'owner', 'superadmin'):
            return {"data": {"is_admin": True, "method": "users_table"}}
    except Exception:
        pass

    return {"data": {"is_admin": False}}


# ══════════════════════════════════════════════════════════
# BACKUP — نسخة احتياطية شاملة
# ══════════════════════════════════════════════════════════
@router.get("/backup/download")
async def download_backup(
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """تنزيل نسخة احتياطية كاملة بصيغة JSON"""
    _require_admin(user)
    tid = str(user.tenant_id)

    backup = {
        "metadata": {
            "created_at":    datetime.utcnow().isoformat(),
            "created_by":    user.email,
            "tenant_id":     tid,
            "version":       "hasabati-v2.0",
            "tables_count":  0,
            "rows_count":    0,
        },
        "tables": {}
    }

    total_rows = 0
    for tbl in TABLES_TO_BACKUP:
        try:
            # تحقق أن الجدول موجود
            exists = await db.execute(text("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name=:t
            """), {"t": tbl})
            if not exists.scalar():
                continue

            # تحقق أن الجدول يحتوي tenant_id
            has_tenant = await db.execute(text("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:t AND column_name='tenant_id'
            """), {"t": tbl})

            if has_tenant.scalar():
                r = await db.execute(text(f"SELECT * FROM {tbl} WHERE tenant_id=:tid"), {"tid": tid})
            else:
                r = await db.execute(text(f"SELECT * FROM {tbl}"))

            rows = [dict(row) for row in r.mappings().fetchall()]
            # تحويل UUID و datetime لـ string
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, 'isoformat'):
                        row[k] = v.isoformat()
                    elif isinstance(v, uuid.UUID):
                        row[k] = str(v)
                    elif v is not None and not isinstance(v, (str, int, float, bool)):
                        row[k] = str(v)

            backup["tables"][tbl] = rows
            total_rows += len(rows)
        except Exception as e:
            backup["tables"][tbl] = {"error": str(e)}

    backup["metadata"]["tables_count"] = len(backup["tables"])
    backup["metadata"]["rows_count"]   = total_rows

    json_bytes = json.dumps(backup, ensure_ascii=False, indent=2).encode("utf-8")
    filename   = f"hasabati_backup_{tid[:8]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    return StreamingResponse(
        io.BytesIO(json_bytes),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get("/backup/summary")
async def backup_summary(
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """ملخص بعدد السجلات في كل جدول قبل الحذف"""
    _require_admin(user)
    tid = str(user.tenant_id)
    summary = {}

    for tbl in TABLES_TO_BACKUP + TABLES_TO_RESET:
        try:
            has_tenant = await db.execute(text("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:t AND column_name='tenant_id'
            """), {"t": tbl})

            if has_tenant.scalar():
                r = await db.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE tenant_id=:tid"), {"tid": tid})
            else:
                tbl_exists = await db.execute(text("""
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=:t
                """), {"t": tbl})
                if not tbl_exists.scalar():
                    continue
                r = await db.execute(text(f"SELECT COUNT(*) FROM {tbl}"))

            count = r.scalar() or 0
            if count > 0:
                summary[tbl] = count
        except Exception:
            pass

    return {"data": summary, "total_rows": sum(summary.values())}


# ══════════════════════════════════════════════════════════
# RESET — إعادة تهيئة جميع الحركات
# ══════════════════════════════════════════════════════════
@router.post("/reset/transactions")
async def reset_transactions(
    body: dict,
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """
    مسح جميع الحركات المحاسبية والخزينة.
    يتطلب: { "confirm": "RESET", "keep_coa": true }
    """
    _require_admin(user)

    if body.get("confirm") != "RESET":
        raise HTTPException(400, "يجب إرسال confirm='RESET' للتأكيد")

    tid = str(user.tenant_id)
    deleted = {}
    errors  = []

    try:
        for tbl in TABLES_TO_RESET:
            try:
                has_tenant = await db.execute(text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t AND column_name='tenant_id'
                """), {"t": tbl})

                if has_tenant.scalar():
                    r = await db.execute(text(f"DELETE FROM {tbl} WHERE tenant_id=:tid"), {"tid": tid})
                else:
                    tbl_exists = await db.execute(text("""
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema='public' AND table_name=:t
                    """), {"t": tbl})
                    if tbl_exists.scalar():
                        r = await db.execute(text(f"DELETE FROM {tbl}"))
                    else:
                        continue

                deleted[tbl] = r.rowcount
            except Exception as e:
                errors.append(f"{tbl}: {str(e)}")

        # إعادة تصفير الأرصدة
        for tbl in RESET_BALANCES:
            try:
                tbl_exists = await db.execute(text("""
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=:t
                """), {"t": tbl})
                if tbl_exists.scalar():
                    await db.execute(text(
                        f"UPDATE {tbl} SET current_balance=0 WHERE tenant_id=:tid"
                    ), {"tid": tid})
            except Exception as e:
                errors.append(f"reset balance {tbl}: {str(e)}")

        # إعادة تصفير تسلسل الترقيم
        try:
            await db.execute(text("""
                UPDATE number_series
                SET last_number = 0
                WHERE tenant_id = :tid
            """), {"tid": tid})
        except Exception:
            pass

        await db.commit()

        return {
            "data": {
                "deleted":       deleted,
                "errors":        errors,
                "total_deleted": sum(deleted.values()),
                "reset_by":      user.email,
                "reset_at":      datetime.utcnow().isoformat(),
            },
            "message": f"تم مسح {sum(deleted.values())} سجل بنجاح"
        }

    except Exception as e:
        await db.rollback()
        raise HTTPException(500, f"فشل إعادة التهيئة: {str(e)}")
