"""
app/modules/accounting/tax_router.py
Raw SQL version — no ORM model dependency
"""
from __future__ import annotations
import uuid
from typing import Optional
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/accounting/tax-types", tags=["إعدادات ضريبة القيمة المضافة"])

class TaxTypeCreate(BaseModel):
    code:         str           = Field(..., min_length=1, max_length=20)
    name_ar:      str           = Field(..., min_length=1, max_length=255)
    name_en:      Optional[str] = None
    rate:         Decimal       = Field(..., ge=0, le=100)
    tax_category: str           = Field('standard')
    is_input:     bool          = True
    is_output:    bool          = True
    output_account_code: Optional[str] = None
    input_account_code:  Optional[str] = None
    is_active:    bool          = True
    is_default:   bool          = False
    sort_order:   int           = 0

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

def _row(r) -> dict:
    if r is None: return {}
    d = dict(r._mapping)
    for k, v in d.items():
        if isinstance(v, uuid.UUID): d[k] = str(v)
        elif isinstance(v, Decimal): d[k] = float(v)
    rate = float(d.get('rate', 0))
    d['label'] = f"{d.get('name_ar','')} ({rate}%)"
    d['category_label'] = {
        'standard':    'خاضع للضريبة الأساسية',
        'zero_rated':  'خاضع للنسبة الصفرية',
        'exempt':      'معفى من الضريبة',
        'out_of_scope':'خارج نطاق الضريبة',
    }.get(d.get('tax_category',''), d.get('tax_category',''))
    return d

@router.get("", summary="قائمة أنواع الضريبة")
async def list_tax_types(active_only: bool = Query(default=True), ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    where = "WHERE tenant_id = :tid"
    if active_only:
        where += " AND is_active = true"
    result = await db.execute(
        text(f"SELECT * FROM tax_types {where} ORDER BY sort_order, code"),
        {"tid": tid}
    )
    rows = [_row(r) for r in result.fetchall()]
    return ok(data=rows, message=f"{len(rows)} نوع ضريبة")

@router.post("", status_code=201, summary="إنشاء نوع ضريبة")
async def create_tax_type(data: TaxTypeCreate, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    exists = await db.execute(
        text("SELECT id FROM tax_types WHERE tenant_id=:tid AND code=:code"),
        {"tid": tid, "code": data.code}
    )
    if exists.fetchone():
        from app.core.exceptions import DuplicateError
        raise DuplicateError("نوع ضريبة", "code", data.code)
    if data.is_default:
        await db.execute(text("UPDATE tax_types SET is_default=false WHERE tenant_id=:tid"), {"tid": tid})
    new_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO tax_types (id,tenant_id,code,name_ar,name_en,rate,tax_category,
            is_input,is_output,output_account_code,input_account_code,
            is_active,is_default,sort_order,created_at)
        VALUES (:id,:tid,:code,:name_ar,:name_en,:rate,:tax_category,
            :is_input,:is_output,:out_acc,:in_acc,:is_active,:is_default,:sort_order,NOW())
    """), {
        "id":new_id,"tid":tid,"code":data.code,"name_ar":data.name_ar,"name_en":data.name_en,
        "rate":float(data.rate),"tax_category":data.tax_category,
        "is_input":data.is_input,"is_output":data.is_output,
        "out_acc":data.output_account_code,"in_acc":data.input_account_code,
        "is_active":data.is_active,"is_default":data.is_default,"sort_order":data.sort_order,
    })
    await db.commit()
    return created(data={"id":new_id,"code":data.code}, message=f"تم إنشاء {data.name_ar}")

@router.put("/{tax_id}", summary="تعديل نوع ضريبة")
async def update_tax_type(tax_id: str, data: TaxTypeUpdate, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    fields = data.model_dump(exclude_unset=True)
    if not fields: return ok(data={}, message="لا توجد تغييرات")
    if fields.get('is_default'):
        await db.execute(text("UPDATE tax_types SET is_default=false WHERE tenant_id=:tid AND id!=:id"), {"tid":tid,"id":tax_id})
    if 'rate' in fields and isinstance(fields['rate'], Decimal):
        fields['rate'] = float(fields['rate'])
    set_clause = ", ".join([f"{k}=:{k}" for k in fields])
    fields.update({"tid":tid,"tax_id":tax_id})
    await db.execute(text(f"UPDATE tax_types SET {set_clause},updated_at=NOW() WHERE tenant_id=:tid AND id=:tax_id"), fields)
    await db.commit()
    return ok(data={"id":tax_id}, message="تم التعديل")

@router.delete("/{tax_id}", summary="حذف نوع ضريبة")
async def delete_tax_type(tax_id: str, ctx=Depends(_ctx)):
    db, user = ctx
    tid = str(user.tenant_id)
    row = await db.execute(text("SELECT name_ar,is_default FROM tax_types WHERE tenant_id=:tid AND id=:id"), {"tid":tid,"id":tax_id})
    t = row.fetchone()
    if not t:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("نوع الضريبة", tax_id)
    if t.is_default:
        from app.core.exceptions import ValidationError
        raise ValidationError("لا يمكن حذف نوع الضريبة الافتراضي")
    await db.execute(text("DELETE FROM tax_types WHERE tenant_id=:tid AND id=:id"), {"tid":tid,"id":tax_id})
    await db.commit()
    return ok(data={"id":tax_id}, message=f"تم حذف {t.name_ar}")
