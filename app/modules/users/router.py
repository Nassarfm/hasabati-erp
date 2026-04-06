"""
app/modules/users/router.py
══════════════════════════════════════════════════════════
User Management API — Users, Roles, Permissions
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from typing import Optional, List
from datetime import date

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.core.exceptions import NotFoundError, DuplicateError
from app.db.session import get_db

router = APIRouter(prefix="/users", tags=["إدارة المستخدمين"])


def _ctx(db=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    return db, user


# ══════════════════════════════════════════════════════════
# Schemas
# ══════════════════════════════════════════════════════════
class UserCreate(BaseModel):
    full_name:      str
    name_ar:        Optional[str]  = None
    email:          str
    phone:          Optional[str]  = None
    role_ids:       List[str]      = []
    branch_codes:   List[str]      = []
    account_expiry: Optional[date] = None
    status:         str            = 'active'


class UserUpdate(BaseModel):
    full_name:      Optional[str]  = None
    name_ar:        Optional[str]  = None
    phone:          Optional[str]  = None
    avatar_url:     Optional[str]  = None
    status:         Optional[str]  = None
    account_expiry: Optional[date] = None
    role_ids:       Optional[List[str]] = None
    branch_codes:   Optional[List[str]] = None


class RoleCreate(BaseModel):
    name:        str
    name_ar:     str
    description: Optional[str] = None
    color:       str            = '#3b82f6'
    icon:        str            = '👤'
    parent_role_id: Optional[str] = None


class RolePermissionUpdate(BaseModel):
    permission_ids: List[str]
    granted:        bool = True
    financial_limit: Optional[float] = None


# ══════════════════════════════════════════════════════════
# Dashboard Stats
# ══════════════════════════════════════════════════════════
@router.get("/dashboard", summary="إحصائيات المستخدمين")
async def get_dashboard(ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)

    try:
        stats_result = await db.execute(text("""
            SELECT
                COUNT(*)                                           AS total,
                COUNT(*) FILTER (WHERE status='active')           AS active,
                COUNT(*) FILTER (WHERE status='inactive')         AS inactive,
                COUNT(*) FILTER (WHERE status='locked')           AS locked,
                COUNT(*) FILTER (WHERE account_expiry < NOW()
                                 AND account_expiry IS NOT NULL)   AS expired
            FROM user_profiles WHERE tenant_id = :tid
        """), {"tid": tid})
        s = stats_result.fetchone()
    except Exception:
        s = type('obj', (object,), {'total':0,'active':0,'inactive':0,'locked':0,'expired':0})()


    try:
        roles_count = await db.execute(text(
            "SELECT COUNT(*) FROM roles WHERE tenant_id = :tid AND is_active = true"
        ), {"tid": tid})
        rc = roles_count.scalar() or 0
    except Exception:
        rc = 0

    # آخر 5 عمليات
    try:
        logs = await db.execute(text("""
            SELECT user_email, action, module, ip_address, created_at
            FROM audit_log
            WHERE tenant_id = :tid
            ORDER BY created_at DESC LIMIT 5
        """), {"tid": tid})
        recent = [dict(r._mapping) for r in logs.fetchall()]
    except Exception:
        recent = []

    return ok(data={
        "total":    s.total,
        "active":   s.active,
        "inactive": s.inactive,
        "locked":   s.locked,
        "expired":  s.expired,
        "roles":    rc,
        "recent_activity": recent,
    })


# ══════════════════════════════════════════════════════════
# Users CRUD
# ══════════════════════════════════════════════════════════
@router.get("", summary="قائمة المستخدمين")
async def list_users(
    search:  Optional[str] = Query(None),
    role_id: Optional[str] = Query(None),
    status:  Optional[str] = Query(None),
    ctx=Depends(_ctx),
):
    db, user = ctx
    tid = str(user.tenant_id)

    where = ["up.tenant_id = :tid"]
    params = {"tid": tid}

    if search:
        where.append("(up.full_name ILIKE :s OR up.email ILIKE :s)")
        params["s"] = f"%{search}%"
    if status:
        where.append("up.status = :status")
        params["status"] = status

    q = f"""
        SELECT
            up.id, up.full_name, up.name_ar, up.email, up.phone,
            up.avatar_url, up.status, up.account_expiry,
            up.last_login_at, up.last_login_ip, up.failed_attempts,
            up.created_at,
            COALESCE(
                json_agg(DISTINCT jsonb_build_object(
                    'id', r.id, 'name', r.name, 'name_ar', r.name_ar,
                    'color', r.color, 'icon', r.icon
                )) FILTER (WHERE r.id IS NOT NULL),
                '[]'
            ) AS roles,
            COALESCE(
                json_agg(DISTINCT ub.branch_code)
                FILTER (WHERE ub.branch_code IS NOT NULL),
                '[]'
            ) AS branches
        FROM user_profiles up
        LEFT JOIN user_roles ur ON ur.user_id = up.id AND ur.tenant_id = :tid
        LEFT JOIN roles r ON (r.id = ur.role_id OR r.id::text = ur.role)
        LEFT JOIN user_branches ub ON ub.user_id = up.id AND ub.tenant_id = :tid
        WHERE {' AND '.join(where)}
        GROUP BY up.id
        ORDER BY up.created_at DESC
    """
    result = await db.execute(text(q), params)
    rows = [dict(r._mapping) for r in result.fetchall()]

    # filter by role after grouping
    if role_id:
        rows = [r for r in rows if any(
            str(role.get('id')) == role_id
            for role in (r.get('roles') or [])
            if isinstance(role, dict)
        )]

    return ok(data=rows, message=f"{len(rows)} مستخدم")


@router.post("", status_code=201, summary="إنشاء مستخدم")
async def create_user(data: UserCreate, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)

    # تحقق من التكرار
    exists = await db.execute(text(
        "SELECT id FROM user_profiles WHERE tenant_id=:tid AND email=:email"
    ), {"tid": tid, "email": data.email})
    if exists.fetchone():
        raise DuplicateError("مستخدم", "email", data.email)

    uid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO user_profiles
            (id, tenant_id, auth_user_id, full_name, name_ar, email,
             phone, status, account_expiry, created_at)
        VALUES
            (:id, :tid, :auth_id, :full_name, :name_ar, :email,
             :phone, :status, :expiry, NOW())
    """), {
        "id": uid, "tid": tid,
        "auth_id": uid,  # placeholder — will link to Supabase auth later
        "full_name": data.full_name, "name_ar": data.name_ar,
        "email": data.email, "phone": data.phone,
        "status": data.status, "expiry": data.account_expiry,
    })

    # أدوار
    for role_id in data.role_ids:
        await db.execute(text("""
            INSERT INTO user_roles (id, tenant_id, user_id, role_id)
            VALUES (gen_random_uuid(), :tid, :uid, :rid::uuid)
            ON CONFLICT DO NOTHING
        """), {"tid": tid, "uid": uid, "rid": role_id})

    # فروع
    for branch_code in data.branch_codes:
        await db.execute(text("""
            INSERT INTO user_branches (id, tenant_id, user_id, branch_code)
            VALUES (gen_random_uuid(), :tid, :uid, :bc)
            ON CONFLICT DO NOTHING
        """), {"tid": tid, "uid": uid, "bc": branch_code})

    # Audit
    await _audit(db, tid, user.email, "create_user", "users", uid,
                 None, {"email": data.email, "roles": data.role_ids})

    await db.commit()
    return created(data={"id": uid}, message=f"تم إنشاء المستخدم {data.full_name}")


@router.put("/{user_id}", summary="تعديل مستخدم")
async def update_user(user_id: str, data: UserUpdate, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)

    fields = data.model_dump(exclude_unset=True,
                             exclude={'role_ids', 'branch_codes'})
    if fields:
        sets = ", ".join([f"{k}=:{k}" for k in fields])
        fields.update({"tid": tid, "uid": user_id})
        await db.execute(text(
            f"UPDATE user_profiles SET {sets}, updated_at=NOW() "
            f"WHERE tenant_id=:tid AND id=:uid"
        ), fields)

    # تحديث الأدوار
    if data.role_ids is not None:
        await db.execute(text(
            "DELETE FROM user_roles WHERE tenant_id=:tid AND user_id=:uid"
        ), {"tid": tid, "uid": user_id})
        for role_id in data.role_ids:
            await db.execute(text("""
                INSERT INTO user_roles (id, tenant_id, user_id, role_id)
                VALUES (gen_random_uuid(), :tid, :uid, :rid)
                ON CONFLICT DO NOTHING
            """), {"tid": tid, "uid": user_id, "rid": role_id})

    # تحديث الفروع
    if data.branch_codes is not None:
        await db.execute(text(
            "DELETE FROM user_branches WHERE tenant_id=:tid AND user_id=:uid"
        ), {"tid": tid, "uid": user_id})
        for bc in data.branch_codes:
            await db.execute(text("""
                INSERT INTO user_branches (id, tenant_id, user_id, branch_code)
                VALUES (gen_random_uuid(), :tid, :uid, :bc)
                ON CONFLICT DO NOTHING
            """), {"tid": tid, "uid": user_id, "bc": bc})

    await _audit(db, tid, user.email, "update_user", "users", user_id, None, fields)
    await db.commit()
    return ok(data={"id": user_id}, message="تم تحديث المستخدم")


@router.patch("/{user_id}/status", summary="تغيير حالة المستخدم")
async def toggle_status(user_id: str, status: str, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE user_profiles SET status=:status, updated_at=NOW()
        WHERE tenant_id=:tid AND id=:uid
    """), {"status": status, "tid": tid, "uid": user_id})
    await _audit(db, tid, user.email, f"set_status_{status}", "users", user_id, None, {"status": status})
    await db.commit()
    return ok(data={"status": status}, message=f"تم تغيير الحالة إلى {status}")


@router.post("/bulk-action", summary="إجراء جماعي")
async def bulk_action(
    user_ids: List[str],
    action: str,   # activate | deactivate | assign_role
    role_id: Optional[str] = None,
    ctx=Depends(_ctx),
):
    db, user = ctx
    tid = str(user.tenant_id)

    for uid in user_ids:
        if action in ('activate', 'deactivate'):
            status = 'active' if action == 'activate' else 'inactive'
            await db.execute(text(
                "UPDATE user_profiles SET status=:s WHERE tenant_id=:tid AND id=:uid"
            ), {"s": status, "tid": tid, "uid": uid})
        elif action == 'assign_role' and role_id:
            await db.execute(text("""
                INSERT INTO user_roles (id, tenant_id, user_id, role_id)
                VALUES (gen_random_uuid(), :tid, :uid, :rid)
                ON CONFLICT DO NOTHING
            """), {"tid": tid, "uid": uid, "rid": role_id})

    await _audit(db, tid, user.email, f"bulk_{action}", "users", None,
                 None, {"count": len(user_ids), "action": action})
    await db.commit()
    return ok(data={"affected": len(user_ids)}, message=f"تم تطبيق الإجراء على {len(user_ids)} مستخدم")


# ══════════════════════════════════════════════════════════
# Roles
# ══════════════════════════════════════════════════════════
@router.get("/roles", summary="قائمة الأدوار")
async def list_roles(ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    result = await db.execute(text("""
        SELECT r.*,
            COUNT(DISTINCT ur.user_id) AS users_count,
            COUNT(DISTINCT rp.permission_id) AS permissions_count
        FROM roles r
        LEFT JOIN user_roles ur ON ur.tenant_id = r.tenant_id
            AND (ur.role_id = r.id OR ur.role = r.id::text)
        LEFT JOIN role_permissions rp ON rp.role_id = r.id
            AND rp.tenant_id = r.tenant_id
        WHERE r.tenant_id = :tid
        GROUP BY r.id
        ORDER BY r.sort_order, r.name
    """), {"tid": tid})
    rows = [dict(r._mapping) for r in result.fetchall()]
    for r in rows:
        if r.get('id'): r['id'] = str(r['id'])
    return ok(data=rows, message=f"{len(rows)} دور")


@router.post("/roles", status_code=201, summary="إنشاء دور")
async def create_role(data: RoleCreate, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    rid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO roles (id, tenant_id, name, name_ar, description,
            color, icon, parent_role_id, is_system)
        VALUES (:id, :tid, :name, :name_ar, :desc,
            :color, :icon, :parent_id, false)
    """), {
        "id": rid, "tid": tid, "name": data.name, "name_ar": data.name_ar,
        "desc": data.description, "color": data.color, "icon": data.icon,
        "parent_id": data.parent_role_id,
    })
    await _audit(db, tid, user.email, "create_role", "roles", rid, None, {"name": data.name})
    await db.commit()
    return created(data={"id": rid}, message=f"تم إنشاء الدور {data.name_ar}")


@router.get("/roles/{role_id}/permissions", summary="صلاحيات دور")
async def get_role_permissions(role_id: str, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)

    # كل الصلاحيات مع حالة الدور
    result = await db.execute(text("""
        SELECT
            p.id, p.module, p.screen, p.action, p.name_ar, p.is_sensitive,
            rp.granted, rp.financial_limit
        FROM permissions p
        LEFT JOIN role_permissions rp
            ON rp.permission_id = p.id
            AND rp.role_id = :rid
            AND rp.tenant_id = :tid
        ORDER BY p.module, p.screen, p.action
    """), {"rid": role_id, "tid": tid})

    rows = [dict(r._mapping) for r in result.fetchall()]

    # تجميع حسب module > screen
    grouped = {}
    for r in rows:
        m = r['module']
        s = r['screen']
        if m not in grouped:
            grouped[m] = {}
        if s not in grouped[m]:
            grouped[m][s] = []
        grouped[m][s].append(r)

    return ok(data={"grouped": grouped, "flat": rows})


@router.put("/roles/{role_id}/permissions", summary="تحديث صلاحيات دور")
async def update_role_permissions(
    role_id: str, data: RolePermissionUpdate, ctx=Depends(_ctx)
):
    db, user = ctx
    tid = str(user.tenant_id)

    for perm_id in data.permission_ids:
        await db.execute(text("""
            INSERT INTO role_permissions
                (id, tenant_id, role_id, permission_id, granted, financial_limit)
            VALUES
                (gen_random_uuid(), :tid, :rid, :pid, :granted, :limit)
            ON CONFLICT (tenant_id, role_id, permission_id)
            DO UPDATE SET granted=:granted, financial_limit=:limit
        """), {
            "tid": tid, "rid": role_id, "pid": perm_id,
            "granted": data.granted, "limit": data.financial_limit,
        })

    await _audit(db, tid, user.email, "update_role_permissions", "roles",
                 role_id, None, {"count": len(data.permission_ids)})
    await db.commit()
    return ok(data={"updated": len(data.permission_ids)}, message="تم تحديث الصلاحيات")


# ══════════════════════════════════════════════════════════
# Permissions
# ══════════════════════════════════════════════════════════
@router.get("/permissions", summary="كل الصلاحيات")
async def list_permissions(ctx=Depends(_ctx)):
    db, user = ctx
    result = await db.execute(text(
        "SELECT * FROM permissions ORDER BY module, screen, action"
    ))
    rows = [dict(r._mapping) for r in result.fetchall()]
    grouped = {}
    for r in rows:
        m = r['module']
        s = r['screen']
        if m not in grouped: grouped[m] = {}
        if s not in grouped[m]: grouped[m][s] = []
        grouped[m][s].append(r)
    return ok(data={"grouped": grouped, "flat": rows})


# ══════════════════════════════════════════════════════════
# Audit Log
# ══════════════════════════════════════════════════════════
@router.get("/audit-log", summary="سجل النشاط")
async def get_audit_log(
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    user_id: Optional[str] = Query(None),
    ctx=Depends(_ctx),
):
    db, user = ctx
    tid = str(user.tenant_id)
    where = ["tenant_id=:tid"]
    params = {"tid": tid, "limit": limit, "offset": offset}
    if user_id:
        where.append("user_id=:uid")
        params["uid"] = user_id

    result = await db.execute(text(f"""
        SELECT * FROM audit_log
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    rows = [dict(r._mapping) for r in result.fetchall()]
    return ok(data=rows, message=f"{len(rows)} عملية")


# ══════════════════════════════════════════════════════════
# Audit Helper
# ══════════════════════════════════════════════════════════
async def _audit(db, tid, email, action, module, record_id, old, new):
    try:
        import json
        await db.execute(text("""
            INSERT INTO audit_log
                (id, tenant_id, user_email, action, module,
                 record_id, old_values, new_values, created_at)
            VALUES
                (gen_random_uuid(), :tid, :email, :action, :module,
                 :rid, :old, :new, NOW())
        """), {
            "tid": tid, "email": email, "action": action, "module": module,
            "rid": str(record_id) if record_id else None,
            "old": json.dumps(old) if old else None,
            "new": json.dumps(new, default=str) if new else None,
        })
    except Exception:
        pass
