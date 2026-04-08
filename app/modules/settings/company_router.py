"""
app/modules/settings/company_router.py
══════════════════════════════════════════════════════════
Company Settings API — GET + PUT
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from typing import Optional
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/settings/company", tags=["إعدادات الشركة"])


# ── Schema ──
class CompanySettingsUpdate(BaseModel):
    # المعلومات الأساسية
    name_ar:                Optional[str]     = None
    name_en:                Optional[str]     = None
    logo_url:               Optional[str]     = None
    national_unified_number:Optional[str]     = None
    entity_type:            Optional[str]     = None
    cr_number:              Optional[str]     = None
    cr_issue_date:          Optional[str]     = None
    cr_status:              Optional[str]     = None
    founded_date:           Optional[str]     = None
    country:                Optional[str]     = None
    isic_code:              Optional[str]     = None
    industry:               Optional[str]     = None
    is_group:               Optional[bool]    = None
    parent_company:         Optional[str]     = None
    # معلومات الضريبة
    vat_number:             Optional[str]     = None
    distinguishing_number:  Optional[str]     = None
    tax_account_number:     Optional[str]     = None
    national_id:            Optional[str]     = None
    tax_name_ar:            Optional[str]     = None
    vat_rate:               Optional[Decimal] = None
    vat_period:             Optional[str]     = None
    zatca_csid:             Optional[str]     = None
    zatca_status:           Optional[str]     = None
    # العنوان
    national_address:       Optional[str]     = None
    city:                   Optional[str]     = None
    region:                 Optional[str]     = None
    postal_code:            Optional[str]     = None
    po_box:                 Optional[str]     = None
    phone:                  Optional[str]     = None
    fax:                    Optional[str]     = None
    email:                  Optional[str]     = None
    website:                Optional[str]     = None
    # المسؤول الأساسي
    admin_name:             Optional[str]     = None
    admin_email:            Optional[str]     = None
    # مواقع التواصل
    linkedin_url:           Optional[str]     = None
    twitter_url:            Optional[str]     = None
    instagram_url:          Optional[str]     = None
    # الإعدادات المالية
    currency:               Optional[str]     = None
    fiscal_year_start:      Optional[int]     = None
    decimal_places:         Optional[int]     = None
    # ✅ الإقليمية والتوطين
    default_language:       Optional[str]     = None  # ar | en
    date_format:            Optional[str]     = None  # DD/MM/YYYY | MM/DD/YYYY | YYYY/MM/DD | YYYY-MM-DD
    time_format:            Optional[str]     = None  # 12h | 24h
    timezone:               Optional[str]     = None  # Asia/Riyadh | UTC | ...
    calendar_type:          Optional[str]     = None  # gregorian | hijri | both
    number_format:          Optional[str]     = None  # western | arabic
    currency_display:       Optional[str]     = None  # code | symbol | name
    currency_position:      Optional[str]     = None  # before | after
    first_day_of_week:      Optional[int]     = None  # 0=الأحد | 1=الاثنين | 6=السبت


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    return dict(row._mapping)


@router.get("", summary="جلب إعدادات الشركة")
async def get_company_settings(
    db:   AsyncSession  = Depends(get_db),
    user: CurrentUser   = Depends(get_current_user),
):
    result = await db.execute(
        text("SELECT * FROM company_settings WHERE tenant_id = :tid LIMIT 1"),
        {"tid": str(user.tenant_id)}
    )
    row = result.fetchone()
    data = _row_to_dict(row) if row else {}

    for k, v in data.items():
        if isinstance(v, uuid.UUID):
            data[k] = str(v)
        elif isinstance(v, Decimal):
            data[k] = float(v)

    return ok(data=data, message="إعدادات الشركة")


@router.put("", summary="حفظ إعدادات الشركة")
async def update_company_settings(
    body: CompanySettingsUpdate,
    db:   AsyncSession  = Depends(get_db),
    user: CurrentUser   = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    exists = await db.execute(
        text("SELECT id FROM company_settings WHERE tenant_id = :tid"),
        {"tid": tid}
    )
    existing = exists.fetchone()

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        return ok(data={}, message="لا توجد تغييرات")

    if existing:
        set_clauses = ", ".join([f"{k} = :{k}" for k in fields])
        set_clauses += ", updated_at = NOW()"
        fields["tid"] = tid
        await db.execute(
            text(f"UPDATE company_settings SET {set_clauses} WHERE tenant_id = :tid"),
            fields
        )
    else:
        fields["tenant_id"] = tid
        cols = ", ".join(fields.keys())
        vals = ", ".join([f":{k}" for k in fields.keys()])
        await db.execute(
            text(f"INSERT INTO company_settings ({cols}) VALUES ({vals})"),
            fields
        )

    await db.commit()

    result = await db.execute(
        text("SELECT * FROM company_settings WHERE tenant_id = :tid LIMIT 1"),
        {"tid": tid}
    )
    row = result.fetchone()
    data = _row_to_dict(row) if row else {}
    for k, v in data.items():
        if isinstance(v, uuid.UUID): data[k] = str(v)
        elif isinstance(v, Decimal): data[k] = float(v)

    return ok(data=data, message="✅ تم حفظ إعدادات الشركة")
