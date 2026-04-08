"""
app/modules/accounting/fiscal_router.py
إدارة السنوات والفترات المالية
"""
from __future__ import annotations
import uuid
from datetime import date, datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/accounting/fiscal", tags=["السنوات والفترات المالية"])

MONTH_NAMES_AR = {
    1:'يناير',2:'فبراير',3:'مارس',4:'أبريل',
    5:'مايو',6:'يونيو',7:'يوليو',8:'أغسطس',
    9:'سبتمبر',10:'أكتوبر',11:'نوفمبر',12:'ديسمبر'
}

def _deps(db=Depends(get_db), user=Depends(get_current_user)):
    return db, user


# ══════════════════════════════════════════════
# Schemas
# ══════════════════════════════════════════════
class FiscalYearCreate(BaseModel):
    start_year: int
    start_month: int = 1
    has_adjustment_period: bool = False

class PeriodAction(BaseModel):
    notes: Optional[str] = None

class ReopenPeriod(BaseModel):
    reason: str


# ══════════════════════════════════════════════
# Helper: توليد الفترات تلقائياً
# ══════════════════════════════════════════════
def generate_periods(start_year: int, start_month: int, has_adj: bool):
    from calendar import monthrange
    periods = []
    year = start_year
    month = start_month

    for i in range(12):
        last_day = monthrange(year, month)[1]
        period_date_start = date(year, month, 1)
        period_date_end   = date(year, month, last_day)
        periods.append({
            "period_number":       i + 1,
            "period_name":         f"{MONTH_NAMES_AR[month]} {year}",
            "period_name_ar":      f"{MONTH_NAMES_AR[month]} {year}",
            "start_date":          period_date_start,
            "end_date":            period_date_end,
            "is_adjustment_period": False,
        })
        month += 1
        if month > 12:
            month = 1
            year += 1

    if has_adj:
        last = periods[-1]
        periods.append({
            "period_number":       13,
            "period_name":         f"فترة التسوية {start_year}",
            "period_name_ar":      f"فترة التسوية {start_year}",
            "start_date":          last["end_date"],
            "end_date":            last["end_date"],
            "is_adjustment_period": True,
        })
    return periods


# ══════════════════════════════════════════════
# السنوات المالية
# ══════════════════════════════════════════════
@router.get("/years")
async def list_fiscal_years(deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("""
            SELECT fy.id, fy.year_name, fy.start_month, fy.start_year, fy.end_year,
                   fy.start_date, fy.end_date, fy.has_adjustment_period,
                   fy.status, fy.is_current, fy.closed_at, fy.closed_by,
                   COUNT(ap.id) as period_count,
                   SUM(CASE WHEN ap.status = 'open' THEN 1 ELSE 0 END) as open_count,
                   SUM(CASE WHEN ap.status = 'closed' THEN 1 ELSE 0 END) as closed_count
            FROM fiscal_years fy
            LEFT JOIN accounting_periods ap ON ap.fiscal_year_id = fy.id
            WHERE fy.tenant_id = :tid
            GROUP BY fy.id
            ORDER BY fy.start_year DESC, fy.start_month DESC
        """),
        {"tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id": str(r[0]), "year_name": r[1], "start_month": r[2],
        "start_year": r[3], "end_year": r[4],
        "start_date": str(r[5]), "end_date": str(r[6]),
        "has_adjustment_period": r[7], "status": r[8], "is_current": r[9],
        "closed_at": str(r[10]) if r[10] else None, "closed_by": r[11],
        "period_count": r[12], "open_count": r[13], "closed_count": r[14],
    } for r in rows])


@router.post("/years", status_code=201)
async def create_fiscal_year(data: FiscalYearCreate, deps=Depends(_deps)):
    db, user = deps

    exists = await db.execute(
        text("SELECT id FROM fiscal_years WHERE tenant_id=:tid AND start_year=:yr AND start_month=:mo"),
        {"tid": str(user.tenant_id), "yr": data.start_year, "mo": data.start_month}
    )
    if exists.fetchone():
        raise HTTPException(400, "السنة المالية موجودة مسبقاً")

    from calendar import monthrange
    start_date = date(data.start_year, data.start_month, 1)
    end_month  = data.start_month - 1 or 12
    end_year   = data.start_year + (1 if data.start_month > 1 else 0)
    end_day    = monthrange(end_year, end_month)[1]
    end_date   = date(end_year, end_month, end_day)

    year_name = f"السنة المالية {data.start_year}"
    if data.start_month != 1:
        year_name = f"السنة المالية {data.start_year}/{data.start_year+1}"

    fy_id = uuid.uuid4()
    today = date.today()
    is_current = start_date <= today <= end_date

    await db.execute(
        text("""
            INSERT INTO fiscal_years
                (id, tenant_id, year_name, start_month, start_year, end_year,
                 start_date, end_date, has_adjustment_period, status, is_current, created_by)
            VALUES
                (:id, :tid, :name, :smo, :syr, :eyr,
                 :sd, :ed, :hadj, 'open', :curr, :by)
        """),
        {
            "id": str(fy_id), "tid": str(user.tenant_id),
            "name": year_name, "smo": data.start_month,
            "syr": data.start_year, "eyr": end_year,
            "sd": start_date, "ed": end_date,
            "hadj": data.has_adjustment_period,
            "curr": is_current, "by": user.email,
        }
    )

    periods = generate_periods(data.start_year, data.start_month, data.has_adjustment_period)
    for p in periods:
        await db.execute(
            text("""
                INSERT INTO accounting_periods
                    (id, tenant_id, fiscal_year_id, period_number, period_name,
                     period_name_ar, start_date, end_date, is_adjustment_period, status)
                VALUES
                    (gen_random_uuid(), :tid, :fy_id, :num, :name,
                     :name_ar, :sd, :ed, :is_adj, 'open')
            """),
            {
                "tid": str(user.tenant_id), "fy_id": str(fy_id),
                "num": p["period_number"], "name": p["period_name"],
                "name_ar": p["period_name_ar"],
                "sd": p["start_date"], "ed": p["end_date"],
                "is_adj": p["is_adjustment_period"],
            }
        )

    await db.commit()
    return created(data={"id": str(fy_id), "year_name": year_name},
                   message=f"تم إنشاء {year_name} مع {len(periods)} فترة")


# ══════════════════════════════════════════════
# الفترات
# ══════════════════════════════════════════════
@router.get("/years/{fy_id}/periods")
async def list_periods(fy_id: uuid.UUID, deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("""
            SELECT id, period_number, period_name, period_name_ar,
                   start_date, end_date, is_adjustment_period, status,
                   locked_at, locked_by, reopened_at, reopened_by, reopen_reason
            FROM accounting_periods
            WHERE fiscal_year_id = :fy_id AND tenant_id = :tid
            ORDER BY period_number
        """),
        {"fy_id": str(fy_id), "tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id": str(r[0]), "period_number": r[1],
        "period_name": r[2], "period_name_ar": r[3],
        "start_date": str(r[4]), "end_date": str(r[5]),
        "is_adjustment_period": r[6], "status": r[7],
        "locked_at": str(r[8]) if r[8] else None,
        "locked_by": r[9],
        "reopened_at": str(r[10]) if r[10] else None,
        "reopened_by": r[11], "reopen_reason": r[12],
    } for r in rows])


@router.get("/current-period")
async def get_current_period(entry_date: date, deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("""
            SELECT ap.id, ap.period_name, ap.status, ap.period_number,
                   fy.year_name
            FROM accounting_periods ap
            JOIN fiscal_years fy ON fy.id = ap.fiscal_year_id
            WHERE ap.tenant_id = :tid
              AND :edate BETWEEN ap.start_date AND ap.end_date
            ORDER BY ap.start_date DESC
            LIMIT 1
        """),
        {"tid": str(user.tenant_id), "edate": entry_date}
    )
    row = result.fetchone()
    if not row:
        return ok(data=None, message="لا توجد فترة مالية لهذا التاريخ")
    return ok(data={
        "id": str(row[0]), "period_name": row[1],
        "status": row[2], "period_number": row[3],
        "year_name": row[4],
    })


# ══════════════════════════════════════════════
# ✅ جديد: Pre-Close Check — فحص قبل الإغلاق
# ══════════════════════════════════════════════
@router.get("/periods/{period_id}/pre-close-check")
async def pre_close_check(period_id: uuid.UUID, deps=Depends(_deps)):
    """
    يفحص الفترة قبل إغلاقها ويُعيد:
    - عدد القيود المعلقة (draft/pending)
    - عدد القيود المرحّلة
    - مجموع المدين والدائن للقيود المرحّلة
    - هل يمكن الإغلاق أم لا
    """
    db, user = deps
    tid = str(user.tenant_id)
    pid = str(period_id)

    # جلب بيانات الفترة
    period_res = await db.execute(
        text("""
            SELECT ap.period_name_ar, ap.start_date, ap.end_date, ap.status
            FROM accounting_periods ap
            WHERE ap.id = :pid AND ap.tenant_id = :tid
        """),
        {"pid": pid, "tid": tid}
    )
    period = period_res.fetchone()
    if not period:
        raise HTTPException(404, "الفترة غير موجودة")
    if period[3] == 'closed':
        raise HTTPException(400, "الفترة مغلقة مسبقاً")

    # عدد القيود حسب الحالة في نطاق الفترة
    je_res = await db.execute(
        text("""
            SELECT
                status,
                COUNT(*)            AS cnt,
                COALESCE(SUM(total_debit),  0) AS total_dr,
                COALESCE(SUM(total_credit), 0) AS total_cr
            FROM journal_entries
            WHERE tenant_id  = :tid
              AND entry_date BETWEEN :sd AND :ed
            GROUP BY status
        """),
        {"tid": tid, "sd": period[1], "ed": period[2]}
    )
    je_rows = je_res.fetchall()

    posted_count  = 0
    draft_count   = 0
    pending_count = 0
    total_debit   = 0.0
    total_credit  = 0.0

    for row in je_rows:
        status = row[0]
        cnt    = int(row[1])
        dr     = float(row[2])
        cr     = float(row[3])
        if status == 'posted':
            posted_count += cnt
            total_debit  += dr
            total_credit += cr
        elif status in ('draft',):
            draft_count += cnt
        elif status in ('pending_review', 'approved'):
            pending_count += cnt

    unposted_count = draft_count + pending_count
    is_balanced    = abs(total_debit - total_credit) < 0.01
    can_close      = unposted_count == 0

    warnings = []
    if draft_count > 0:
        warnings.append(f"يوجد {draft_count} قيد في حالة مسودة — يجب ترحيلها أو حذفها")
    if pending_count > 0:
        warnings.append(f"يوجد {pending_count} قيد في انتظار الموافقة — يجب إتمام الموافقة أولاً")
    if not is_balanced and posted_count > 0:
        warnings.append(f"الفترة غير متوازنة — مدين: {total_debit:.3f} | دائن: {total_credit:.3f}")

    return ok(data={
        "period_name":     period[0],
        "start_date":      str(period[1]),
        "end_date":        str(period[2]),
        "posted_count":    posted_count,
        "draft_count":     draft_count,
        "pending_count":   pending_count,
        "unposted_count":  unposted_count,
        "total_debit":     round(total_debit,  3),
        "total_credit":    round(total_credit, 3),
        "is_balanced":     is_balanced,
        "can_close":       can_close,
        "warnings":        warnings,
    })


# ══════════════════════════════════════════════
# إغلاق فترة
# ══════════════════════════════════════════════
@router.post("/periods/{period_id}/close")
async def close_period(period_id: uuid.UUID, body: PeriodAction, deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("SELECT id, status, period_name FROM accounting_periods WHERE id=:id AND tenant_id=:tid"),
        {"id": str(period_id), "tid": str(user.tenant_id)}
    )
    period = result.fetchone()
    if not period:
        raise HTTPException(404, "الفترة غير موجودة")
    if period[1] == 'closed':
        raise HTTPException(400, "الفترة مغلقة مسبقاً")

    await db.execute(
        text("""UPDATE accounting_periods
                SET status='closed', locked_at=now(), locked_by=:by
                WHERE id=:id AND tenant_id=:tid"""),
        {"id": str(period_id), "tid": str(user.tenant_id), "by": user.email}
    )
    await db.execute(
        text("""INSERT INTO period_audit_log
                (id,tenant_id,period_id,action,action_ar,performed_by,notes)
                VALUES(gen_random_uuid(),:tid,:pid,'closed','إغلاق الفترة',:by,:notes)"""),
        {"tid":str(user.tenant_id),"pid":str(period_id),"by":user.email,"notes":body.notes}
    )
    await db.commit()
    return ok(data={"status":"closed"}, message=f"تم إغلاق {period[2]}")


# ══════════════════════════════════════════════
# ✅ جديد: إغلاق السنة كاملة
# ══════════════════════════════════════════════
@router.post("/years/{fy_id}/close-all")
async def close_all_periods(fy_id: uuid.UUID, body: PeriodAction, deps=Depends(_deps)):
    """
    يغلق كل الفترات المفتوحة في السنة المالية دفعة واحدة.
    يتحقق أولاً من عدم وجود قيود معلقة في أي فترة.
    """
    db, user = deps
    tid = str(user.tenant_id)
    fid = str(fy_id)
    now = datetime.now(timezone.utc)

    # جلب الفترات المفتوحة
    open_res = await db.execute(
        text("""
            SELECT id, period_name_ar, start_date, end_date
            FROM accounting_periods
            WHERE fiscal_year_id = :fid AND tenant_id = :tid AND status = 'open'
            ORDER BY period_number
        """),
        {"fid": fid, "tid": tid}
    )
    open_periods = open_res.fetchall()

    if not open_periods:
        raise HTTPException(400, "لا توجد فترات مفتوحة في هذه السنة")

    # التحقق من عدم وجود قيود معلقة في أي فترة
    total_unposted = 0
    for p in open_periods:
        je_res = await db.execute(
            text("""
                SELECT COUNT(*) FROM journal_entries
                WHERE tenant_id  = :tid
                  AND entry_date BETWEEN :sd AND :ed
                  AND status IN ('draft','pending_review','approved')
            """),
            {"tid": tid, "sd": p[2], "ed": p[3]}
        )
        count = je_res.scalar() or 0
        total_unposted += count

    if total_unposted > 0:
        raise HTTPException(400,
            f"يوجد {total_unposted} قيد غير مرحَّل في فترات هذه السنة — يجب ترحيل أو حذف كل القيود المعلقة أولاً"
        )

    # إغلاق كل الفترات
    closed = 0
    for p in open_periods:
        await db.execute(
            text("""
                UPDATE accounting_periods
                SET status='closed', locked_at=:now, locked_by=:by
                WHERE id=:id AND tenant_id=:tid
            """),
            {"id": str(p[0]), "tid": tid, "by": user.email, "now": now}
        )
        await db.execute(
            text("""
                INSERT INTO period_audit_log
                    (id,tenant_id,period_id,action,action_ar,performed_by,notes)
                VALUES
                    (gen_random_uuid(),:tid,:pid,'closed','إغلاق الفترة (إغلاق السنة)',:by,:notes)
            """),
            {"tid": tid, "pid": str(p[0]), "by": user.email, "notes": body.notes or "إغلاق السنة المالية"}
        )
        closed += 1

    # تحديث حالة السنة
    await db.execute(
        text("""
            UPDATE fiscal_years
            SET status='closed', closed_at=:now, closed_by=:by
            WHERE id=:fid AND tenant_id=:tid
        """),
        {"fid": fid, "tid": tid, "by": user.email, "now": now}
    )

    await db.commit()
    return ok(
        data={"closed_periods": closed},
        message=f"تم إغلاق {closed} فترة وإغلاق السنة المالية بنجاح"
    )


# ══════════════════════════════════════════════
# إعادة فتح فترة
# ══════════════════════════════════════════════
@router.post("/periods/{period_id}/reopen")
async def reopen_period(period_id: uuid.UUID, body: ReopenPeriod, deps=Depends(_deps)):
    db, user = deps
    if user.role not in ('owner', 'admin'):
        raise HTTPException(403, "فقط مدير النظام يستطيع إعادة فتح الفترة")

    result = await db.execute(
        text("SELECT id, status, period_name FROM accounting_periods WHERE id=:id AND tenant_id=:tid"),
        {"id": str(period_id), "tid": str(user.tenant_id)}
    )
    period = result.fetchone()
    if not period:
        raise HTTPException(404, "الفترة غير موجودة")
    if period[1] == 'open':
        raise HTTPException(400, "الفترة مفتوحة مسبقاً")

    await db.execute(
        text("""UPDATE accounting_periods
                SET status='open', reopened_at=now(), reopened_by=:by, reopen_reason=:reason
                WHERE id=:id AND tenant_id=:tid"""),
        {"id":str(period_id),"tid":str(user.tenant_id),"by":user.email,"reason":body.reason}
    )
    await db.execute(
        text("""INSERT INTO period_audit_log
                (id,tenant_id,period_id,action,action_ar,performed_by,notes)
                VALUES(gen_random_uuid(),:tid,:pid,'reopened','إعادة فتح الفترة',:by,:notes)"""),
        {"tid":str(user.tenant_id),"pid":str(period_id),"by":user.email,"notes":body.reason}
    )
    await db.commit()
    return ok(data={"status":"open"}, message=f"تم إعادة فتح {period[2]}")


# ══════════════════════════════════════════════
# سجل أحداث الفترة
# ══════════════════════════════════════════════
@router.get("/periods/{period_id}/audit")
async def period_audit(period_id: uuid.UUID, deps=Depends(_deps)):
    db, user = deps
    result = await db.execute(
        text("""SELECT action, action_ar, performed_by, notes, created_at
                FROM period_audit_log
                WHERE period_id=:pid AND tenant_id=:tid
                ORDER BY created_at DESC"""),
        {"pid": str(period_id), "tid": str(user.tenant_id)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "action": r[0], "action_ar": r[1],
        "performed_by": r[2], "notes": r[3],
        "created_at": str(r[4])
    } for r in rows])
