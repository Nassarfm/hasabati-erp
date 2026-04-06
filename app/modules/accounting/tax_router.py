"""
app/modules/accounting/tax_router.py
══════════════════════════════════════
VAT / Tax Types API
"""
from __future__ import annotations
import uuid
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.core.exceptions import NotFoundError, DuplicateError
from app.db.session import get_db
from app.modules.accounting.tax_models import TaxType

router = APIRouter(prefix="/accounting/tax-types", tags=["إعدادات ضريبة القيمة المضافة"])


# ── Schemas ──
class TaxTypeCreate(BaseModel):
    code:         str     = Field(..., min_length=1, max_length=20)
    name_ar:      str     = Field(..., min_length=1, max_length=255)
    name_en:      Optional[str] = None
    rate:         Decimal = Field(..., ge=0, le=100)
    tax_category: str     = Field('standard')
    is_input:     bool    = True
    is_output:    bool    = True
    output_account_code: Optional[str] = None
    input_account_code:  Optional[str] = None
    is_active:    bool    = True
    is_default:   bool    = False
    sort_order:   int     = 0


class TaxTypeUpdate(BaseModel):
    name_ar:      Optional[str]     = None
    name_en:      Optional[str]     = None
    rate:         Optional[Decimal] = None
    tax_category: Optional[str]     = None
    is_input:     Optional[bool]    = None
    is_output:    Optional[bool]    = None
    output_account_code: Optional[str] = None
    input_account_code:  Optional[str] = None
    is_active:    Optional[bool]    = None
    is_default:   Optional[bool]    = None
    sort_order:   Optional[int]     = None


def _ctx(db=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    return db, user


def _to_dict(t: TaxType) -> dict:
    return {
        "id":           str(t.id),
        "code":         t.code,
        "name_ar":      t.name_ar,
        "name_en":      t.name_en,
        "rate":         float(t.rate),
        "tax_category": t.tax_category,
        "is_input":     t.is_input,
        "is_output":    t.is_output,
        "output_account_code": t.output_account_code,
        "input_account_code":  t.input_account_code,
        "is_active":    t.is_active,
        "is_default":   t.is_default,
        "sort_order":   t.sort_order,
        # label للـ Frontend
        "label": f"{t.name_ar} ({float(t.rate)}%)",
        "category_label": {
            "standard":    "خاضع للضريبة الأساسية",
            "zero_rated":  "خاضع للنسبة الصفرية",
            "exempt":      "معفى من الضريبة",
            "out_of_scope":"خارج نطاق الضريبة",
        }.get(t.tax_category, t.tax_category),
    }


@router.get("", summary="قائمة أنواع الضريبة")
async def list_tax_types(
    active_only: bool = Query(default=True),
    ctx=Depends(_ctx),
):
    db, user = ctx
    q = select(TaxType).where(TaxType.tenant_id == user.tenant_id)
    if active_only:
        q = q.where(TaxType.is_active == True)
    q = q.order_by(TaxType.sort_order, TaxType.code)
    result = await db.execute(q)
    items = result.scalars().all()
    return ok(data=[_to_dict(t) for t in items], message=f"{len(items)} نوع ضريبة")


@router.post("", status_code=201, summary="إنشاء نوع ضريبة")
async def create_tax_type(data: TaxTypeCreate, ctx=Depends(_ctx)):
    db, user = ctx
    # تحقق من عدم التكرار
    exists = await db.execute(
        select(TaxType).where(TaxType.tenant_id==user.tenant_id, TaxType.code==data.code)
    )
    if exists.scalar_one_or_none():
        raise DuplicateError("نوع ضريبة", "code", data.code)

    # إذا is_default → إلغاء الآخرين
    if data.is_default:
        await db.execute(
            select(TaxType).where(TaxType.tenant_id==user.tenant_id, TaxType.is_default==True)
        )
        existing_defaults = (await db.execute(
            select(TaxType).where(TaxType.tenant_id==user.tenant_id, TaxType.is_default==True)
        )).scalars().all()
        for t in existing_defaults:
            t.is_default = False

    t = TaxType(
        tenant_id=user.tenant_id,
        code=data.code, name_ar=data.name_ar, name_en=data.name_en,
        rate=data.rate, tax_category=data.tax_category,
        is_input=data.is_input, is_output=data.is_output,
        output_account_code=data.output_account_code,
        input_account_code=data.input_account_code,
        is_active=data.is_active, is_default=data.is_default,
        sort_order=data.sort_order, created_by=user.email,
    )
    db.add(t)
    await db.flush()
    return created(data=_to_dict(t), message=f"تم إنشاء نوع الضريبة {t.name_ar}")


@router.put("/{tax_id}", summary="تعديل نوع ضريبة")
async def update_tax_type(tax_id: uuid.UUID, data: TaxTypeUpdate, ctx=Depends(_ctx)):
    db, user = ctx
    result = await db.execute(
        select(TaxType).where(TaxType.tenant_id==user.tenant_id, TaxType.id==tax_id)
    )
    t = result.scalar_one_or_none()
    if not t:
        raise NotFoundError("نوع الضريبة", tax_id)

    for field, val in data.model_dump(exclude_unset=True).items():
        setattr(t, field, val)

    # إذا is_default → إلغاء الآخرين
    if data.is_default:
        others = (await db.execute(
            select(TaxType).where(TaxType.tenant_id==user.tenant_id, TaxType.is_default==True, TaxType.id!=tax_id)
        )).scalars().all()
        for o in others:
            o.is_default = False

    try: t.updated_by = user.email
    except: pass
    await db.flush()
    return ok(data=_to_dict(t), message=f"تم تعديل {t.name_ar}")


@router.delete("/{tax_id}", summary="حذف نوع ضريبة")
async def delete_tax_type(tax_id: uuid.UUID, ctx=Depends(_ctx)):
    db, user = ctx
    result = await db.execute(
        select(TaxType).where(TaxType.tenant_id==user.tenant_id, TaxType.id==tax_id)
    )
    t = result.scalar_one_or_none()
    if not t:
        raise NotFoundError("نوع الضريبة", tax_id)
    if t.is_default:
        from app.core.exceptions import ValidationError
        raise ValidationError("لا يمكن حذف نوع الضريبة الافتراضي")
    await db.execute(delete(TaxType).where(TaxType.id==tax_id))
    await db.flush()
    return ok(data={"id": str(tax_id)}, message=f"تم حذف {t.name_ar}")
