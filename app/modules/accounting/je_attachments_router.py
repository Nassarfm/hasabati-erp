"""
app/modules/accounting/je_attachments_router.py
رفع واسترجاع مرفقات القيود
"""
from __future__ import annotations
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
import httpx, os, base64
from datetime import datetime

router = APIRouter(prefix="/accounting/je", tags=["مرفقات القيود"])

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY", ""))
BUCKET = "je-attachments"


def _svc(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    return db, user


# ── رفع مرفق ──────────────────────────────────────────
@router.post("/{je_id}/attachments", status_code=201)
async def upload_attachment(
    je_id: uuid.UUID,
    file: UploadFile = File(...),
    notes: Optional[str] = Form(None),
    deps=Depends(_svc),
):
    db, user = deps
    content = await file.read()
    file_size = len(content)

    # رفع لـ Supabase Storage
    storage_path = f"{str(user.tenant_id)}/{str(je_id)}/{file.filename}"
    upload_url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{storage_path}"

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            upload_url,
            content=content,
            headers={
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": file.content_type or "application/octet-stream",
            },
        )
        if resp.status_code not in (200, 201):
            raise HTTPException(status_code=502, detail=f"فشل رفع الملف: {resp.text}")

    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{storage_path}"

    att_id = uuid.uuid4()
    await db.execute(
        text("""
            INSERT INTO je_attachments
                (id, tenant_id, journal_entry_id, file_name, file_size, file_type,
                 storage_path, storage_url, uploaded_by, uploaded_at, notes)
            VALUES
                (:id, :tid, :je_id, :fname, :fsize, :ftype,
                 :spath, :surl, :by, :at, :notes)
            ON CONFLICT (tenant_id, journal_entry_id, file_name) DO UPDATE
                SET storage_url = EXCLUDED.storage_url,
                    uploaded_at = EXCLUDED.uploaded_at,
                    notes = EXCLUDED.notes
        """),
        {
            "id": str(att_id), "tid": str(user.tenant_id), "je_id": str(je_id),
            "fname": file.filename, "fsize": file_size, "ftype": file.content_type,
            "spath": storage_path, "surl": public_url,
            "by": user.email, "at": datetime.utcnow(), "notes": notes,
        }
    )
    await db.commit()

    return created(data={
        "id": str(att_id),
        "file_name": file.filename,
        "file_size": file_size,
        "storage_url": public_url,
        "uploaded_by": user.email,
    }, message=f"تم رفع {file.filename}")


# ── قائمة المرفقات ────────────────────────────────────
@router.get("/{je_id}/attachments")
async def list_attachments(je_id: uuid.UUID, deps=Depends(_svc)):
    db, user = deps
    result = await db.execute(
        text("""
            SELECT id, file_name, file_size, file_type, storage_url,
                   uploaded_by, uploaded_at, notes
            FROM je_attachments
            WHERE journal_entry_id = :je_id AND tenant_id = :tid
            ORDER BY uploaded_at DESC
        """),
        {"je_id": str(je_id), "tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id": str(r[0]), "file_name": r[1], "file_size": r[2],
        "file_type": r[3], "storage_url": r[4],
        "uploaded_by": r[5], "uploaded_at": str(r[6]), "notes": r[7],
    } for r in rows])


# ── حذف مرفق ─────────────────────────────────────────
@router.delete("/{je_id}/attachments/{att_id}")
async def delete_attachment(
    je_id: uuid.UUID, att_id: uuid.UUID, deps=Depends(_svc)
):
    db, user = deps
    result = await db.execute(
        text("SELECT storage_path FROM je_attachments WHERE id = :id AND tenant_id = :tid"),
        {"id": str(att_id), "tid": str(user.tenant_id)}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="المرفق غير موجود")

    # حذف من Storage
    async with httpx.AsyncClient() as client:
        await client.delete(
            f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{row[0]}",
            headers={"Authorization": f"Bearer {SUPABASE_KEY}"},
        )

    await db.execute(
        text("DELETE FROM je_attachments WHERE id = :id AND tenant_id = :tid"),
        {"id": str(att_id), "tid": str(user.tenant_id)}
    )
    await db.commit()
    return ok(data={"deleted": True}, message="تم حذف المرفق")
