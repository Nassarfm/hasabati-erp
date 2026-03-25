"""
app/modules/accounting/je_activity_router.py
سجل أحداث القيود — Activity Log
"""
from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/accounting/je", tags=["سجل الأحداث"])


async def log_activity(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    je_id: uuid.UUID,
    je_serial: str,
    action: str,
    action_ar: str,
    performed_by: str,
    display_name: str = None,
    notes: str = None,
    metadata: dict = None,
):
    """Helper — يُستدعى من service.py عند كل حدث"""
    import json
    await db.execute(
        text("""
            INSERT INTO je_activity_log
                (id, tenant_id, je_id, je_serial, action, action_ar,
                 performed_by, display_name, notes, metadata)
            VALUES
                (gen_random_uuid(), :tid, :je_id, :serial, :action, :action_ar,
                 :by, :dname, :notes, :meta)
        """),
        {
            "tid":       str(tenant_id),
            "je_id":     str(je_id),
            "serial":    je_serial,
            "action":    action,
            "action_ar": action_ar,
            "by":        performed_by,
            "dname":     display_name or performed_by.split('@')[0] if performed_by else None,
            "notes":     notes,
            "meta":      json.dumps(metadata) if metadata else None,
        }
    )


@router.get("/{je_id}/activity")
async def get_activity(
    je_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    result = await db.execute(
        text("""
            SELECT id, action, action_ar, performed_by, display_name,
                   notes, metadata, created_at
            FROM je_activity_log
            WHERE je_id = :je_id AND tenant_id = :tid
            ORDER BY created_at ASC
        """),
        {"je_id": str(je_id), "tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id":         str(r[0]),
        "action":     r[1],
        "action_ar":  r[2],
        "performed_by": r[3],
        "display_name": r[4],
        "notes":      r[5],
        "metadata":   r[6],
        "created_at": str(r[7]),
    } for r in rows])


@router.get("/activity/recent")
async def recent_activity(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """آخر 50 حدث في كل القيود"""
    result = await db.execute(
        text("""
            SELECT a.id, a.action, a.action_ar, a.performed_by, a.display_name,
                   a.je_serial, a.notes, a.created_at
            FROM je_activity_log a
            WHERE a.tenant_id = :tid
            ORDER BY a.created_at DESC
            LIMIT 50
        """),
        {"tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id":           str(r[0]),
        "action":       r[1],
        "action_ar":    r[2],
        "performed_by": r[3],
        "display_name": r[4],
        "je_serial":    r[5],
        "notes":        r[6],
        "created_at":   str(r[7]),
    } for r in rows])
