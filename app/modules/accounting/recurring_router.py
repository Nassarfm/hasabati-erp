"""
app/modules/accounting/recurring_router.py
API endpoints للقيود المتكررة
──────────────────────────────────────────
GET    /recurring                    — قائمة القيود المتكررة
POST   /recurring/preview            — معاينة جدول الإطفاء
POST   /recurring                    — إنشاء قيد متكرر جديد
GET    /recurring/{id}               — تفاصيل قيد متكرر
PATCH  /recurring/{id}/status        — تغيير الحالة (pause/resume/cancel)
POST   /recurring/{id}/post-pending  — ترحيل الأقساط المستحقة الآن
POST   /recurring/{instance_id}/skip — تخطي قسط معين
DELETE /recurring/{id}               — حذف (فقط إذا لم يُرحَّل أي قسط)
"""
from __future__ import annotations

import calendar
import math
import uuid
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.accounting.recurring_models import (
    RecurringEntry, RecurringEntryInstance,
)
from app.modules.accounting.recurring_schemas import (
    RecurringEntryCreate, RecurringEntryListItem, RecurringEntryOut,
    RecurringInstanceOut, RecurringPreviewRequest, RecurringPreviewResponse,
    RecurringPreviewItem, RecurringSkipInstance, RecurringStatusUpdate,
)
from app.modules.accounting.schemas import JournalEntryCreate
from app.modules.accounting.service import AccountingService

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/recurring", tags=["Recurring Entries"])


# ══════════════════════════════════════════════════════════
# HELPERS — حساب جدول الإطفاء
# ══════════════════════════════════════════════════════════

def compute_schedule(
    total_amount: Decimal,
    frequency: str,
    post_day: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """
    يحسب التواريخ والمبالغ لكل قسط.
    يعيد قائمة من dicts: {installment_number, scheduled_date, amount}
    """
    dates = []
    current = start_date

    while current <= end_date:
        if post_day == "start":
            posting_date = current.replace(day=1)
        else:
            # آخر يوم في الشهر
            last_day = calendar.monthrange(current.year, current.month)[1]
            posting_date = current.replace(day=last_day)

        # لا تتجاوز end_date
        if posting_date > end_date:
            posting_date = end_date

        dates.append(posting_date)

        # الانتقال للفترة التالية
        if frequency == "weekly":
            from datetime import timedelta
            current = current + timedelta(weeks=1)
        elif frequency == "monthly":
            # انتقل شهر
            month = current.month + 1
            year  = current.year
            if month > 12:
                month = 1
                year += 1
            last_day = calendar.monthrange(year, month)[1]
            current = current.replace(year=year, month=month, day=min(current.day, last_day))
        elif frequency == "quarterly":
            month = current.month + 3
            year  = current.year
            while month > 12:
                month -= 12
                year  += 1
            last_day = calendar.monthrange(year, month)[1]
            current = current.replace(year=year, month=month, day=min(current.day, last_day))
        elif frequency == "semiannual":
            month = current.month + 6
            year  = current.year
            while month > 12:
                month -= 12
                year  += 1
            last_day = calendar.monthrange(year, month)[1]
            current = current.replace(year=year, month=month, day=min(current.day, last_day))
        elif frequency == "annual":
            current = current.replace(year=current.year + 1)
        else:
            break

    if not dates:
        raise ValueError("لم يتم توليد أي تاريخ — تحقق من الفترة والتكرار")

    n = len(dates)
    # مبلغ كل قسط (بدون آخر قسط)
    installment = (total_amount / n).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    # آخر قسط يأخذ الباقي
    last = total_amount - installment * (n - 1)

    schedule = []
    cumulative = Decimal("0")
    for i, d in enumerate(dates, 1):
        amt = last if i == n else installment
        cumulative += amt
        schedule.append({
            "installment_number": i,
            "scheduled_date":     d,
            "amount":             amt,
            "cumulative":         cumulative,
        })

    return schedule, installment, last


# ══════════════════════════════════════════════════════════
# 1. معاينة جدول الإطفاء — بدون حفظ
# ══════════════════════════════════════════════════════════
@router.post("/preview", response_model=RecurringPreviewResponse)
async def preview_schedule(
    payload: RecurringPreviewRequest,
    user: CurrentUser = Depends(get_current_user),
):
    """
    يُرجع جدول الإطفاء للمراجعة قبل الإنشاء — لا يحفظ شيئاً
    """
    try:
        schedule, installment, last = compute_schedule(
            payload.total_amount,
            payload.frequency,
            payload.post_day,
            payload.start_date,
            payload.end_date,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return RecurringPreviewResponse(
        total_installments=len(schedule),
        installment_amount=installment,
        last_installment_amount=last,
        total_amount=payload.total_amount,
        schedule=[RecurringPreviewItem(**s) for s in schedule],
    )


# ══════════════════════════════════════════════════════════
# 2. إنشاء قيد متكرر
# ══════════════════════════════════════════════════════════
@router.post("", response_model=RecurringEntryOut, status_code=201)
async def create_recurring_entry(
    payload: RecurringEntryCreate,
    db:      AsyncSession      = Depends(get_db),
    user:    CurrentUser       = Depends(get_current_user),
):
    """
    ينشئ القالب + جميع الأقساط دفعة واحدة
    """
    try:
        schedule, installment, last = compute_schedule(
            payload.total_amount,
            payload.frequency,
            payload.post_day,
            payload.start_date,
            payload.end_date,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    n = len(schedule)

    # توليد كود فريد
    code = f"REC-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    entry = RecurringEntry(
        tenant_id=user.tenant_id,
        code=code,
        name=payload.name,
        description=payload.description,
        total_amount=payload.total_amount,
        installment_amount=installment,
        last_installment_amount=last,
        total_installments=n,
        frequency=payload.frequency,
        post_day=payload.post_day,
        start_date=payload.start_date,
        end_date=payload.end_date,
        je_type=payload.je_type,
        lines_template=[l.model_dump(mode="json") for l in payload.lines],
        status="active",
        posted_count=0,
        pending_count=n,
        skipped_count=0,
        notes=payload.notes,
        created_by=user.email,
    )
    db.add(entry)
    await db.flush()  # نحتاج الـ id

    # إنشاء الأقساط
    for s in schedule:
        inst = RecurringEntryInstance(
            tenant_id=user.tenant_id,
            recurring_entry_id=entry.id,
            installment_number=s["installment_number"],
            scheduled_date=s["scheduled_date"],
            amount=s["amount"],
            status="pending",
            created_by=user.email,
        )
        db.add(inst)

    await db.commit()
    await db.refresh(entry)

    # جلب مع الأقساط
    result = await db.execute(
        select(RecurringEntry)
        .where(RecurringEntry.id == entry.id)
        .options(selectinload(RecurringEntry.instances))
    )
    entry = result.scalar_one()
    return entry


# ══════════════════════════════════════════════════════════
# 3. قائمة القيود المتكررة
# ══════════════════════════════════════════════════════════
@router.get("", response_model=List[RecurringEntryListItem])
async def list_recurring_entries(
    status:   Optional[str] = None,
    db:       AsyncSession  = Depends(get_db),
    user:     CurrentUser   = Depends(get_current_user),
):
    q = select(RecurringEntry).where(RecurringEntry.tenant_id == user.tenant_id)
    if status:
        q = q.where(RecurringEntry.status == status)
    q = q.order_by(RecurringEntry.created_at.desc())
    result = await db.execute(q)
    return result.scalars().all()


# ══════════════════════════════════════════════════════════
# 4. تفاصيل قيد متكرر مع كل الأقساط
# ══════════════════════════════════════════════════════════
@router.get("/{entry_id}", response_model=RecurringEntryOut)
async def get_recurring_entry(
    entry_id: uuid.UUID,
    db:       AsyncSession = Depends(get_db),
    user:     CurrentUser  = Depends(get_current_user),
):
    result = await db.execute(
        select(RecurringEntry)
        .where(
            RecurringEntry.id == entry_id,
            RecurringEntry.tenant_id == user.tenant_id,
        )
        .options(selectinload(RecurringEntry.instances))
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(404, "القيد المتكرر غير موجود")
    return entry


# ══════════════════════════════════════════════════════════
# 5. ترحيل الأقساط المستحقة اليوم أو قبله
# ══════════════════════════════════════════════════════════
@router.post("/{entry_id}/post-pending")
async def post_pending_instances(
    entry_id: uuid.UUID,
    db:       AsyncSession = Depends(get_db),
    user:     CurrentUser  = Depends(get_current_user),
):
    """
    يُرحّل جميع الأقساط المعلّقة التي حان موعدها (scheduled_date <= today)
    """
    result = await db.execute(
        select(RecurringEntry)
        .where(
            RecurringEntry.id == entry_id,
            RecurringEntry.tenant_id == user.tenant_id,
        )
        .options(selectinload(RecurringEntry.instances))
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(404, "القيد المتكرر غير موجود")
    if entry.status not in ("active",):
        raise HTTPException(400, f"لا يمكن الترحيل — الحالة: {entry.status}")

    today = date.today()
    due_instances = [
        i for i in entry.instances
        if i.status == "pending" and i.scheduled_date <= today
    ]

    if not due_instances:
        return ok({"posted": 0, "message": "لا توجد أقساط مستحقة اليوم"})

    svc = AccountingService(db, user)
    posted = []
    failed = []

    for inst in due_instances:
        try:
            # بناء القيد من القالب
            lines_data = []
            for lt in entry.lines_template:
                debit_pct  = Decimal(str(lt.get("debit_pct",  0)))
                credit_pct = Decimal(str(lt.get("credit_pct", 0)))
                debit  = (inst.amount * debit_pct  / 100).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
                credit = (inst.amount * credit_pct / 100).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
                lines_data.append({
                    "account_code":               lt["account_code"],
                    "description":                lt.get("description") or entry.description,
                    "debit":                      float(debit),
                    "credit":                     float(credit),
                    "branch_code":                lt.get("branch_code"),
                    "branch_name":                lt.get("branch_name"),
                    "cost_center":                lt.get("cost_center"),
                    "cost_center_name":           lt.get("cost_center_name"),
                    "project_code":               lt.get("project_code"),
                    "project_name":               lt.get("project_name"),
                    "expense_classification_code":lt.get("expense_classification_code"),
                    "expense_classification_name":lt.get("expense_classification_name"),
                })

            from app.modules.accounting.schemas import JournalEntryCreate, JournalEntryLineCreate

            je_payload = JournalEntryCreate(
                je_type=entry.je_type,
                entry_date=inst.scheduled_date,
                description=f"{entry.name} — قسط {inst.installment_number}/{entry.total_installments}",
                reference=entry.code,
                lines=[JournalEntryLineCreate(**l) for l in lines_data],
            )

            # إنشاء وترحيل مباشر
            je = await svc.create_journal_entry(je_payload)
            await svc.post_journal_entry(je.id)

            # تحديث القسط
            inst.status               = "posted"
            inst.journal_entry_id     = je.id
            inst.journal_entry_serial = je.serial
            inst.posted_at            = datetime.utcnow()
            inst.posted_by            = user.email
            posted.append(str(inst.id))

        except Exception as e:
            inst.status = "failed"
            inst.note   = str(e)[:400]
            failed.append({"id": str(inst.id), "error": str(e)[:200]})
            logger.error("recurring_post_failed", instance_id=str(inst.id), error=str(e))

    # تحديث عدادات القالب
    entry.posted_count  = sum(1 for i in entry.instances if i.status == "posted")
    entry.pending_count = sum(1 for i in entry.instances if i.status == "pending")
    entry.skipped_count = sum(1 for i in entry.instances if i.status == "skipped")

    # إذا انتهت جميع الأقساط
    if entry.pending_count == 0:
        entry.status = "completed"

    await db.commit()

    return ok({
        "posted":  len(posted),
        "failed":  len(failed),
        "details": failed,
        "message": f"تم ترحيل {len(posted)} قسط" + (f" — {len(failed)} فشل" if failed else ""),
    })


# ══════════════════════════════════════════════════════════
# 6. تخطي قسط معين
# ══════════════════════════════════════════════════════════
@router.post("/instances/{instance_id}/skip")
async def skip_instance(
    instance_id: uuid.UUID,
    payload:     RecurringSkipInstance,
    db:          AsyncSession = Depends(get_db),
    user:        CurrentUser  = Depends(get_current_user),
):
    result = await db.execute(
        select(RecurringEntryInstance)
        .where(RecurringEntryInstance.id == instance_id)
        .options(selectinload(RecurringEntryInstance.recurring_entry))
    )
    inst = result.scalar_one_or_none()
    if not inst:
        raise HTTPException(404, "القسط غير موجود")
    if inst.recurring_entry.tenant_id != user.tenant_id:
        raise HTTPException(403, "غير مصرح")
    if inst.status != "pending":
        raise HTTPException(400, f"لا يمكن تخطي قسط بحالة: {inst.status}")

    inst.status = "skipped"
    inst.note   = payload.note or "تم التخطي يدوياً"

    entry = inst.recurring_entry
    entry.pending_count = sum(1 for i in entry.instances if i.status == "pending")
    entry.skipped_count = sum(1 for i in entry.instances if i.status == "skipped")
    if entry.pending_count == 0:
        entry.status = "completed"

    await db.commit()
    return ok({"message": "تم تخطي القسط"})


# ══════════════════════════════════════════════════════════
# 7. تغيير حالة القيد المتكرر (إيقاف / استئناف / إلغاء)
# ══════════════════════════════════════════════════════════
@router.patch("/{entry_id}/status")
async def update_recurring_status(
    entry_id: uuid.UUID,
    payload:  RecurringStatusUpdate,
    db:       AsyncSession = Depends(get_db),
    user:     CurrentUser  = Depends(get_current_user),
):
    allowed_transitions = {
        "active":    {"paused", "cancelled"},
        "paused":    {"active", "cancelled"},
        "completed": set(),
        "cancelled": set(),
    }
    result = await db.execute(
        select(RecurringEntry)
        .where(
            RecurringEntry.id == entry_id,
            RecurringEntry.tenant_id == user.tenant_id,
        )
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(404, "القيد المتكرر غير موجود")

    if payload.status not in allowed_transitions.get(entry.status, set()):
        raise HTTPException(400, f"لا يمكن الانتقال من '{entry.status}' إلى '{payload.status}'")

    entry.status = payload.status
    await db.commit()
    return ok({"status": entry.status, "message": f"تم تغيير الحالة إلى {payload.status}"})


# ══════════════════════════════════════════════════════════
# 8. حذف (فقط إذا لم يُرحَّل أي قسط)
# ══════════════════════════════════════════════════════════
@router.delete("/{entry_id}", status_code=204)
async def delete_recurring_entry(
    entry_id: uuid.UUID,
    db:       AsyncSession = Depends(get_db),
    user:     CurrentUser  = Depends(get_current_user),
):
    result = await db.execute(
        select(RecurringEntry)
        .where(
            RecurringEntry.id == entry_id,
            RecurringEntry.tenant_id == user.tenant_id,
        )
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(404, "القيد المتكرر غير موجود")
    if entry.posted_count > 0:
        raise HTTPException(400, "لا يمكن حذف قيد تم ترحيل بعض أقساطه")

    await db.delete(entry)
    await db.commit()
