"""
app/modules/settings/series_router.py
إعدادات الترقيم التلقائي
"""
from __future__ import annotations
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/settings/series", tags=["الترقيم التلقائي"])


class SeriesUpdate(BaseModel):
    prefix:               Optional[str]  = None
    padding:              Optional[int]  = None
    separator:            Optional[str]  = None
    use_entry_date_year:  Optional[bool] = None
    notes:                Optional[str]  = None


@router.get("")
async def list_series(
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """جلب إعدادات الترقيم مع آخر تسلسل لكل نوع"""
    result = await db.execute(text("""
        SELECT
            s.je_type_code,
            s.name_ar,
            s.prefix,
            s.padding,
            s.separator,
            s.use_entry_date_year,
            s.is_active,
            s.notes,
            -- آخر تسلسل للسنة الحالية
            COALESCE(
                (SELECT last_sequence FROM je_sequences
                 WHERE tenant_id = s.tenant_id
                   AND je_type_code = s.je_type_code
                   AND fiscal_year = EXTRACT(YEAR FROM NOW())::INT),
                0
            ) AS current_seq,
            -- آخر تسلسل لكل السنوات
            (SELECT json_agg(json_build_object(
                'year', fiscal_year,
                'last_seq', last_sequence,
                'last_serial',
                    s.prefix || s.separator ||
                    fiscal_year::text || s.separator ||
                    LPAD(last_sequence::text, s.padding, '0')
             ) ORDER BY fiscal_year DESC)
             FROM je_sequences
             WHERE tenant_id = s.tenant_id
               AND je_type_code = s.je_type_code
             LIMIT 5
            ) AS history
        FROM series_settings s
        WHERE s.tenant_id = :tid
        ORDER BY s.is_active DESC, s.je_type_code
    """), {"tid": str(user.tenant_id)})

    rows = result.mappings().all()
    year = date.today().year

    return ok(data=[{
        "je_type_code":        r["je_type_code"],
        "name_ar":             r["name_ar"],
        "prefix":              r["prefix"],
        "padding":             r["padding"],
        "separator":           r["separator"],
        "use_entry_date_year": r["use_entry_date_year"],
        "is_active":           r["is_active"],
        "notes":               r["notes"] or "",
        "current_seq":         r["current_seq"],
        "next_serial":         f"{r['prefix']}{r['separator']}{year}{r['separator']}{(r['current_seq']+1):0{r['padding']}d}",
        "history":             r["history"] or [],
    } for r in rows])


@router.put("/{je_type_code}")
async def update_series(
    je_type_code: str,
    body: SeriesUpdate,
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """تعديل إعدادات ترقيم نوع قيد"""
    fields = body.model_dump(exclude_unset=True)
    if not fields:
        return ok(data={}, message="لا توجد تغييرات")

    set_clause = ", ".join([f"{k} = :{k}" for k in fields])
    fields.update({"tid": str(user.tenant_id), "type": je_type_code})

    await db.execute(text(f"""
        UPDATE series_settings
        SET {set_clause}, updated_at = NOW()
        WHERE tenant_id = :tid AND je_type_code = :type
    """), fields)
    await db.commit()
    return ok(data={"je_type_code": je_type_code}, message="✅ تم تعديل إعدادات الترقيم")


@router.post("/{je_type_code}/reset")
async def reset_series(
    je_type_code: str,
    year:         int  = Query(..., description="السنة المالية"),
    start_from:   int  = Query(default=0, ge=0),
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """
    إعادة تعيين التسلسل لسنة محددة.
    تحذير: لا تستخدمه إلا إذا كنت متأكداً — سيؤثر على الأرقام القادمة.
    """
    # تحقق: لا يمكن إعادة التعيين لسنة بها قيود مرحّلة
    check = await db.execute(text("""
        SELECT COUNT(*) FROM journal_entries
        WHERE tenant_id = :tid AND fiscal_year = :year AND status = 'posted'
    """), {"tid": str(user.tenant_id), "year": year})
    posted = check.scalar() or 0
    if posted > 0:
        from app.core.exceptions import ValidationError
        raise ValidationError(
            f"لا يمكن إعادة التعيين — يوجد {posted} قيد مرحّل في {year}"
        )

    await db.execute(text("""
        INSERT INTO je_sequences (id, tenant_id, je_type_code, fiscal_year, last_sequence)
        VALUES (gen_random_uuid(), :tid, :type, :year, :start)
        ON CONFLICT (tenant_id, je_type_code, fiscal_year)
        DO UPDATE SET last_sequence = :start
    """), {"tid": str(user.tenant_id), "type": je_type_code, "year": year, "start": start_from})
    await db.commit()

    return ok(data={"year": year, "reset_to": start_from},
              message=f"✅ تم إعادة تعيين تسلسل {je_type_code} لسنة {year} إلى {start_from}")


@router.get("/{je_type_code}/preview")
async def preview_next_serial(
    je_type_code: str,
    entry_date:   Optional[date] = Query(default=None),
    db:   AsyncSession = Depends(get_db),
    user: CurrentUser  = Depends(get_current_user),
):
    """معاينة الرقم التالي بدون حفظ"""
    from app.services.numbering.series_service import NumberSeriesService
    svc = NumberSeriesService(db, user.tenant_id)
    data = await svc.preview_next(je_type_code, entry_date)
    return ok(data=data)
