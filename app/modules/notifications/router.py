"""
app/modules/notifications/router.py
نظام الإشعارات الداخلية
"""
from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/notifications", tags=["الإشعارات"])

def _deps(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    return db, user


async def create_notification(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    title: str,
    message: str,
    notif_type: str,
    je_id: uuid.UUID = None,
    je_serial: str = None,
    created_by: str = None,
):
    """Helper — يُستدعى من service.py عند كل حدث"""
    await db.execute(
        text("""
            INSERT INTO notifications
                (id, tenant_id, user_id, type, title, message,
                 je_id, je_serial, is_read, created_at, created_by)
            VALUES
                (gen_random_uuid(), :tid, NULL, :type, :title, :msg,
                 :je_id, :serial, false, now(), :by)
        """),
        {
            "tid":    str(tenant_id),
            "type":   notif_type,
            "title":  title,
            "msg":    message,
            "je_id":  str(je_id) if je_id else None,
            "serial": je_serial,
            "by":     created_by,
        }
    )


@router.get("")
async def list_notifications(deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("""
            SELECT id, type, title, message, je_id, je_serial,
                   is_read, created_at, created_by
            FROM notifications
            WHERE tenant_id = :tid
            ORDER BY created_at DESC
            LIMIT 50
        """),
        {"tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id":         str(r[0]),
        "type":       r[1],
        "title":      r[2],
        "message":    r[3],
        "je_id":      str(r[4]) if r[4] else None,
        "je_serial":  r[5],
        "is_read":    r[6],
        "created_at": str(r[7]),
        "created_by": r[8],
    } for r in rows])


@router.get("/unread-count")
async def unread_count(deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("SELECT COUNT(*) FROM notifications WHERE tenant_id = :tid AND is_read = false"),
        {"tid": str(user.tenant_id)}
    )
    count = result.scalar() or 0
    return ok(data={"count": count})


@router.post("/mark-read/{notif_id}")
async def mark_read(notif_id: uuid.UUID, deps=Depends(_deps)):
    db, user = deps
    await db.execute(
        text("UPDATE notifications SET is_read = true WHERE id = :id AND tenant_id = :tid"),
        {"id": str(notif_id), "tid": str(user.tenant_id)}
    )
    await db.commit()
    return ok(data={"marked": True})


@router.post("/mark-all-read")
async def mark_all_read(deps=Depends(_deps)):
    db, user = deps
    await db.execute(
        text("UPDATE notifications SET is_read = true WHERE tenant_id = :tid AND is_read = false"),
        {"tid": str(user.tenant_id)}
    )
    await db.commit()
    return ok(data={"marked": True})
