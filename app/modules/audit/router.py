"""
app/modules/audit/router.py
══════════════════════════════════════════════════════════
Audit Trail Router — سجل نشاط المستخدمين
GET /audit/activities     — كل الأحداث مع فلاتر
GET /audit/users-summary  — ملخص يومي لكل مستخدم
GET /audit/sessions       — جلسات الدخول والخروج
GET /audit/user/{email}   — تفاصيل نشاط مستخدم بعينه
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/audit", tags=["سجل النشاط"])


@router.get("/activities")
async def list_activities(
    date_from:   Optional[date] = Query(None),
    date_to:     Optional[date] = Query(None),
    user_email:  Optional[str]  = Query(None),
    action_type: Optional[str]  = Query(None),
    module:      Optional[str]  = Query(None),
    limit:       int            = Query(default=100, ge=1, le=500),
    offset:      int            = Query(default=0, ge=0),
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """جميع أحداث النشاط مع فلاتر"""
    conditions = ["tenant_id = :tid"]
    params: dict = {"tid": str(user.tenant_id), "limit": limit, "offset": offset}

    if date_from:
        conditions.append("created_at::date >= :date_from")
        params["date_from"] = str(date_from)
    if date_to:
        conditions.append("created_at::date <= :date_to")
        params["date_to"] = str(date_to)
    if user_email:
        conditions.append("user_email = :email")
        params["email"] = user_email
    if action_type:
        conditions.append("action_type = :atype")
        params["atype"] = action_type
    if module:
        conditions.append("module = :module")
        params["module"] = module

    where = " AND ".join(conditions)

    # الإجمالي
    count_r = await db.execute(
        text(f"SELECT COUNT(*) FROM user_activity_log WHERE {where}"), params
    )
    total = count_r.scalar() or 0

    # البيانات
    result = await db.execute(text(f"""
        SELECT
            id, user_email, display_name,
            action_type, action_ar, module, module_ar,
            resource_type, resource_id, resource_label,
            ip_address, status, extra_data, created_at
        FROM user_activity_log
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)

    rows = result.mappings().all()
    return ok(data={
        "total": total,
        "items": [{
            "id":             str(r["id"]),
            "user_email":     r["user_email"],
            "display_name":   r["display_name"] or (r["user_email"] or "").split("@")[0],
            "action_type":    r["action_type"],
            "action_ar":      r["action_ar"],
            "module":         r["module"],
            "module_ar":      r["module_ar"],
            "resource_type":  r["resource_type"],
            "resource_id":    r["resource_id"],
            "resource_label": r["resource_label"],
            "ip_address":     r["ip_address"],
            "status":         r["status"],
            "extra_data":     r["extra_data"],
            "created_at":     str(r["created_at"]),
        } for r in rows]
    })


@router.get("/users-summary")
async def users_daily_summary(
    target_date: date = Query(default=None),
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """
    ملخص يومي لكل مستخدم — من دخل؟ من لم يدخل؟ كم حدث؟
    """
    if not target_date:
        from datetime import date as date_type
        target_date = date_type.today()

    # نشاط المستخدمين في اليوم المحدد
    result = await db.execute(text("""
        SELECT
            user_email,
            display_name,
            COUNT(*)                                    AS total_actions,
            MIN(created_at)                             AS first_activity,
            MAX(created_at)                             AS last_activity,
            COUNT(CASE WHEN action_type='login' THEN 1 END) AS login_count,
            COUNT(CASE WHEN action_type='create' THEN 1 END) AS creates,
            COUNT(CASE WHEN action_type='update' THEN 1 END) AS updates,
            COUNT(CASE WHEN action_type='post'   THEN 1 END) AS posts,
            COUNT(CASE WHEN action_type='delete' THEN 1 END) AS deletes,
            COUNT(CASE WHEN action_type='view'   THEN 1 END) AS views,
            STRING_AGG(DISTINCT module_ar, ' • '
                       ORDER BY module_ar)              AS modules_used
        FROM user_activity_log
        WHERE tenant_id = :tid
          AND created_at::date = :target_date
        GROUP BY user_email, display_name
        ORDER BY total_actions DESC
    """), {"tid": str(user.tenant_id), "target_date": str(target_date)})

    active_users = result.mappings().all()
    active_emails = {r["user_email"] for r in active_users}

    # جلب كل مستخدمي النظام
    all_users_r = await db.execute(text("""
        SELECT DISTINCT user_email, display_name
        FROM user_activity_log
        WHERE tenant_id = :tid
        ORDER BY user_email
    """), {"tid": str(user.tenant_id)})
    all_users = all_users_r.mappings().all()

    inactive = [
        {"user_email": u["user_email"], "display_name": u["display_name"]}
        for u in all_users if u["user_email"] not in active_emails
    ]

    return ok(data={
        "date":     str(target_date),
        "active":   [{
            "user_email":    r["user_email"],
            "display_name":  r["display_name"] or r["user_email"].split("@")[0],
            "total_actions": r["total_actions"],
            "first_activity":str(r["first_activity"]),
            "last_activity": str(r["last_activity"]),
            "login_count":   r["login_count"],
            "creates":       r["creates"],
            "updates":       r["updates"],
            "posts":         r["posts"],
            "deletes":       r["deletes"],
            "views":         r["views"],
            "modules_used":  r["modules_used"] or "",
        } for r in active_users],
        "inactive": inactive,
        "active_count":   len(active_users),
        "inactive_count": len(inactive),
    })


@router.get("/user/{email:path}")
async def user_activity_detail(
    email:     str,
    date_from: Optional[date] = Query(None),
    date_to:   Optional[date] = Query(None),
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """تفاصيل نشاط مستخدم محدد مع مخطط زمني"""
    params: dict = {"tid": str(user.tenant_id), "email": email}
    date_filter = ""
    if date_from:
        date_filter += " AND created_at::date >= :date_from"
        params["date_from"] = str(date_from)
    if date_to:
        date_filter += " AND created_at::date <= :date_to"
        params["date_to"] = str(date_to)

    result = await db.execute(text(f"""
        SELECT
            action_type, action_ar, module_ar,
            resource_type, resource_id, resource_label,
            ip_address, status, extra_data, created_at
        FROM user_activity_log
        WHERE tenant_id = :tid AND user_email = :email {date_filter}
        ORDER BY created_at DESC
        LIMIT 200
    """), params)

    rows = result.mappings().all()

    # ملخص يومي
    daily_r = await db.execute(text(f"""
        SELECT
            created_at::date AS day,
            COUNT(*) AS actions,
            MIN(created_at) AS first_in,
            MAX(created_at) AS last_in
        FROM user_activity_log
        WHERE tenant_id = :tid AND user_email = :email {date_filter}
        GROUP BY day
        ORDER BY day DESC
        LIMIT 30
    """), params)

    daily = daily_r.mappings().all()

    return ok(data={
        "user_email": email,
        "timeline": [{
            "action_type":  r["action_type"],
            "action_ar":    r["action_ar"],
            "module_ar":    r["module_ar"],
            "resource_id":  r["resource_id"],
            "resource_label": r["resource_label"],
            "ip_address":   r["ip_address"],
            "status":       r["status"],
            "created_at":   str(r["created_at"]),
        } for r in rows],
        "daily_summary": [{
            "day":      str(d["day"]),
            "actions":  d["actions"],
            "first_in": str(d["first_in"]),
            "last_in":  str(d["last_in"]),
        } for d in daily],
    })


@router.get("/stats")
async def audit_stats(
    days: int = Query(default=7, ge=1, le=90),
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """إحصائيات عامة للفترة الأخيرة"""
    result = await db.execute(text("""
        SELECT
            action_type,
            COUNT(*) AS count
        FROM user_activity_log
        WHERE tenant_id = :tid
          AND created_at >= NOW() - INTERVAL ':days days'
        GROUP BY action_type
        ORDER BY count DESC
    """.replace(":days days", f"{days} days")), {"tid": str(user.tenant_id)})

    by_action = {r[0]: r[1] for r in result.all()}

    result2 = await db.execute(text(f"""
        SELECT
            created_at::date AS day,
            COUNT(*) AS actions,
            COUNT(DISTINCT user_email) AS active_users
        FROM user_activity_log
        WHERE tenant_id = :tid
          AND created_at >= NOW() - INTERVAL '{days} days'
        GROUP BY day
        ORDER BY day DESC
    """), {"tid": str(user.tenant_id)})

    daily = [{
        "day":          str(r[0]),
        "actions":      r[1],
        "active_users": r[2],
    } for r in result2.all()]

    return ok(data={"by_action": by_action, "daily": daily})
