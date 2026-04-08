"""
app/modules/settings/currency_router.py
══════════════════════════════════════════════════════════
Multi Currency — إدارة العملات وأسعار الصرف
Raw SQL — بدون ORM

Endpoints:
  GET    /settings/currencies                        → قائمة العملات
  POST   /settings/currencies                        → إضافة عملة
  PUT    /settings/currencies/{code}                 → تعديل عملة
  PATCH  /settings/currencies/{code}/set-base        → تعيين العملة الأساسية
  PATCH  /settings/currencies/{code}/toggle-active   → تفعيل/تعطيل

  GET    /settings/currencies/exchange-rates         → أسعار الصرف
  POST   /settings/currencies/exchange-rates         → إضافة سعر صرف
  PUT    /settings/currencies/exchange-rates/{id}    → تعديل سعر صرف
  DELETE /settings/currencies/exchange-rates/{id}    → حذف سعر صرف
  GET    /settings/currencies/exchange-rates/latest  → آخر سعر لكل زوج
  POST   /settings/currencies/convert                → تحويل مبلغ
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/settings/currencies", tags=["العملات"])


# ── Helpers ───────────────────────────────────────────────
def _deps(db=Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    return db, user

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Schemas ───────────────────────────────────────────────
class CurrencyCreate(BaseModel):
    code:           str           = Field(..., min_length=2, max_length=10)
    name_ar:        str           = Field(..., min_length=1)
    name_en:        str           = Field(..., min_length=1)
    symbol:         str           = Field(..., min_length=1, max_length=10)
    decimal_places: int           = Field(default=3, ge=0, le=6)
    is_active:      bool          = True
    notes:          Optional[str] = None


class CurrencyUpdate(BaseModel):
    name_ar:        Optional[str] = None
    name_en:        Optional[str] = None
    symbol:         Optional[str] = None
    decimal_places: Optional[int] = None
    is_active:      Optional[bool] = None
    notes:          Optional[str]  = None


class ExchangeRateCreate(BaseModel):
    from_currency: str            = Field(..., min_length=2, max_length=10)
    to_currency:   str            = Field(..., min_length=2, max_length=10)
    rate:          Decimal        = Field(..., gt=0)
    rate_date:     date
    source:        str            = "manual"
    notes:         Optional[str]  = None


class ExchangeRateUpdate(BaseModel):
    rate:    Optional[Decimal] = None
    source:  Optional[str]     = None
    notes:   Optional[str]     = None


class ConvertRequest(BaseModel):
    amount:        Decimal
    from_currency: str
    to_currency:   str
    rate_date:     Optional[date] = None


# ══════════════════════════════════════════════════════════
# العملات — Currencies
# ══════════════════════════════════════════════════════════

@router.get("")
async def list_currencies(
    active_only: bool = Query(default=False),
    deps=Depends(_deps),
):
    db, user = deps
    tid = str(user.tenant_id)
    where = "WHERE tenant_id = :tid"
    if active_only:
        where += " AND is_active = true"

    result = await db.execute(text(f"""
        SELECT id, code, name_ar, name_en, symbol, decimal_places,
               is_base, is_active, notes, created_at, updated_at
        FROM currencies
        {where}
        ORDER BY is_base DESC, is_active DESC, code
    """), {"tid": tid})

    rows = result.mappings().all()
    return ok(data=[{
        "id":             str(r["id"]),
        "code":           r["code"],
        "name_ar":        r["name_ar"],
        "name_en":        r["name_en"],
        "symbol":         r["symbol"],
        "decimal_places": r["decimal_places"],
        "is_base":        r["is_base"],
        "is_active":      r["is_active"],
        "notes":          r["notes"] or "",
    } for r in rows], message=f"{len(rows)} عملة")


@router.post("", status_code=201)
async def create_currency(body: CurrencyCreate, deps=Depends(_deps)):
    db, user = deps
    tid = str(user.tenant_id)

    # التحقق من عدم التكرار
    exists = await db.execute(text("""
        SELECT id FROM currencies WHERE tenant_id = :tid AND code = :code
    """), {"tid": tid, "code": body.code.upper()})
    if exists.fetchone():
        raise HTTPException(400, f"العملة {body.code} موجودة مسبقاً")

    new_id = str(uuid.uuid4())
    now    = _now()
    await db.execute(text("""
        INSERT INTO currencies
            (id, tenant_id, code, name_ar, name_en, symbol, decimal_places,
             is_base, is_active, notes, created_at, updated_at)
        VALUES
            (:id, :tid, :code, :name_ar, :name_en, :symbol, :dec,
             false, :active, :notes, :now, :now)
    """), {
        "id":     new_id,
        "tid":    tid,
        "code":   body.code.upper(),
        "name_ar": body.name_ar,
        "name_en": body.name_en,
        "symbol":  body.symbol,
        "dec":     body.decimal_places,
        "active":  body.is_active,
        "notes":   body.notes or "",
        "now":     now,
    })
    await db.commit()
    return created(data={"id": new_id, "code": body.code.upper()},
                   message=f"تمت إضافة عملة {body.name_ar}")


@router.put("/{code}")
async def update_currency(code: str, body: CurrencyUpdate, deps=Depends(_deps)):
    db, user = deps
    tid = str(user.tenant_id)

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        return ok(data={}, message="لا توجد تغييرات")

    set_clause = ", ".join([f"{k} = :{k}" for k in fields])
    fields.update({"tid": tid, "code": code.upper(), "now": _now()})

    await db.execute(text(f"""
        UPDATE currencies
        SET {set_clause}, updated_at = :now
        WHERE tenant_id = :tid AND code = :code
    """), fields)
    await db.commit()
    return ok(data={"code": code.upper()}, message="تم تعديل العملة")


@router.patch("/{code}/set-base")
async def set_base_currency(code: str, deps=Depends(_deps)):
    """تعيين عملة كعملة أساسية — تُلغي تعيين الأخريات"""
    db, user = deps
    tid = str(user.tenant_id)
    now = _now()

    # تحقق من وجود العملة
    exists = await db.execute(text("""
        SELECT id FROM currencies WHERE tenant_id = :tid AND code = :code
    """), {"tid": tid, "code": code.upper()})
    if not exists.fetchone():
        raise HTTPException(404, f"العملة {code} غير موجودة")

    # إلغاء الأساسية من الكل ثم تعيين الجديدة
    await db.execute(text("""
        UPDATE currencies SET is_base = false, updated_at = :now WHERE tenant_id = :tid
    """), {"tid": tid, "now": now})

    await db.execute(text("""
        UPDATE currencies SET is_base = true, is_active = true, updated_at = :now
        WHERE tenant_id = :tid AND code = :code
    """), {"tid": tid, "code": code.upper(), "now": now})

    await db.commit()
    return ok(data={"code": code.upper(), "is_base": True},
              message=f"تم تعيين {code} كعملة أساسية")


@router.patch("/{code}/toggle-active")
async def toggle_currency_active(code: str, deps=Depends(_deps)):
    db, user = deps
    tid = str(user.tenant_id)

    # لا يمكن تعطيل العملة الأساسية
    check = await db.execute(text("""
        SELECT is_base, is_active FROM currencies
        WHERE tenant_id = :tid AND code = :code
    """), {"tid": tid, "code": code.upper()})
    row = check.fetchone()
    if not row:
        raise HTTPException(404, "العملة غير موجودة")
    if row[0]:  # is_base
        raise HTTPException(400, "لا يمكن تعطيل العملة الأساسية")

    new_status = not row[1]
    await db.execute(text("""
        UPDATE currencies SET is_active = :status, updated_at = :now
        WHERE tenant_id = :tid AND code = :code
    """), {"tid": tid, "code": code.upper(), "status": new_status, "now": _now()})
    await db.commit()
    return ok(data={"code": code.upper(), "is_active": new_status},
              message=f"تم {'تفعيل' if new_status else 'تعطيل'} العملة {code}")


# ══════════════════════════════════════════════════════════
# أسعار الصرف — Exchange Rates
# ══════════════════════════════════════════════════════════

@router.get("/exchange-rates")
async def list_exchange_rates(
    from_currency: Optional[str] = None,
    to_currency:   Optional[str] = None,
    limit:         int = Query(default=50, ge=1, le=500),
    deps=Depends(_deps),
):
    db, user = deps
    tid = str(user.tenant_id)

    conditions = ["tenant_id = :tid"]
    params: dict = {"tid": tid, "limit": limit}

    if from_currency:
        conditions.append("from_currency = :from_c")
        params["from_c"] = from_currency.upper()
    if to_currency:
        conditions.append("to_currency = :to_c")
        params["to_c"] = to_currency.upper()

    where = " AND ".join(conditions)
    result = await db.execute(text(f"""
        SELECT id, from_currency, to_currency, rate, rate_date, source, notes, created_at, created_by
        FROM currency_exchange_rates
        WHERE {where}
        ORDER BY rate_date DESC, from_currency
        LIMIT :limit
    """), params)

    rows = result.mappings().all()
    return ok(data=[{
        "id":            str(r["id"]),
        "from_currency": r["from_currency"],
        "to_currency":   r["to_currency"],
        "rate":          float(r["rate"]),
        "rate_date":     str(r["rate_date"]),
        "source":        r["source"] or "manual",
        "notes":         r["notes"] or "",
        "created_by":    r["created_by"] or "",
    } for r in rows])


@router.get("/exchange-rates/latest")
async def get_latest_rates(deps=Depends(_deps)):
    """آخر سعر صرف لكل زوج عملات"""
    db, user = deps
    tid = str(user.tenant_id)

    result = await db.execute(text("""
        SELECT DISTINCT ON (from_currency, to_currency)
            id, from_currency, to_currency, rate, rate_date, source
        FROM currency_exchange_rates
        WHERE tenant_id = :tid
        ORDER BY from_currency, to_currency, rate_date DESC
    """), {"tid": tid})

    rows = result.mappings().all()
    return ok(data=[{
        "id":            str(r["id"]),
        "from_currency": r["from_currency"],
        "to_currency":   r["to_currency"],
        "rate":          float(r["rate"]),
        "rate_date":     str(r["rate_date"]),
        "source":        r["source"] or "manual",
    } for r in rows])


@router.post("/exchange-rates", status_code=201)
async def create_exchange_rate(body: ExchangeRateCreate, deps=Depends(_deps)):
    db, user = deps
    tid = str(user.tenant_id)
    now = _now()

    from_c = body.from_currency.upper()
    to_c   = body.to_currency.upper()

    if from_c == to_c:
        raise HTTPException(400, "لا يمكن إضافة سعر صرف بين نفس العملة")

    new_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO currency_exchange_rates
            (id, tenant_id, from_currency, to_currency, rate, rate_date, source, notes, created_at, updated_at, created_by)
        VALUES
            (:id, :tid, :from_c, :to_c, :rate, :rdate, :source, :notes, :now, :now, :by)
        ON CONFLICT (tenant_id, from_currency, to_currency, rate_date)
        DO UPDATE SET rate = EXCLUDED.rate, source = EXCLUDED.source,
                      notes = EXCLUDED.notes, updated_at = EXCLUDED.updated_at
    """), {
        "id":     new_id,
        "tid":    tid,
        "from_c": from_c,
        "to_c":   to_c,
        "rate":   float(body.rate),
        "rdate":  body.rate_date,
        "source": body.source,
        "notes":  body.notes or "",
        "now":    now,
        "by":     user.email,
    })
    await db.commit()
    return created(data={"id": new_id},
                   message=f"تم إضافة سعر صرف {from_c}/{to_c} = {body.rate}")


@router.put("/exchange-rates/{rate_id}")
async def update_exchange_rate(
    rate_id: uuid.UUID,
    body: ExchangeRateUpdate,
    deps=Depends(_deps),
):
    db, user = deps
    tid = str(user.tenant_id)

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        return ok(data={}, message="لا توجد تغييرات")

    if "rate" in fields:
        fields["rate"] = float(fields["rate"])

    set_clause = ", ".join([f"{k} = :{k}" for k in fields])
    fields.update({"tid": tid, "rid": str(rate_id), "now": _now()})

    await db.execute(text(f"""
        UPDATE currency_exchange_rates
        SET {set_clause}, updated_at = :now
        WHERE id = :rid AND tenant_id = :tid
    """), fields)
    await db.commit()
    return ok(data={"id": str(rate_id)}, message="تم تعديل سعر الصرف")


@router.delete("/exchange-rates/{rate_id}", status_code=204)
async def delete_exchange_rate(rate_id: uuid.UUID, deps=Depends(_deps)):
    db, user = deps
    tid = str(user.tenant_id)
    await db.execute(text("""
        DELETE FROM currency_exchange_rates WHERE id = :rid AND tenant_id = :tid
    """), {"rid": str(rate_id), "tid": tid})
    await db.commit()


@router.post("/convert")
async def convert_amount(body: ConvertRequest, deps=Depends(_deps)):
    """تحويل مبلغ من عملة إلى أخرى باستخدام آخر سعر صرف متاح"""
    db, user = deps
    tid = str(user.tenant_id)

    from_c = body.from_currency.upper()
    to_c   = body.to_currency.upper()

    if from_c == to_c:
        return ok(data={"amount": float(body.amount), "rate": 1.0, "rate_date": None})

    # البحث عن سعر الصرف المباشر
    rate_q = await db.execute(text("""
        SELECT rate, rate_date FROM currency_exchange_rates
        WHERE tenant_id = :tid AND from_currency = :from_c AND to_currency = :to_c
        ORDER BY rate_date DESC LIMIT 1
    """), {"tid": tid, "from_c": from_c, "to_c": to_c})
    row = rate_q.fetchone()

    if row:
        rate      = Decimal(str(row[0]))
        rate_date = str(row[1])
    else:
        # البحث عن سعر عكسي
        rev_q = await db.execute(text("""
            SELECT rate, rate_date FROM currency_exchange_rates
            WHERE tenant_id = :tid AND from_currency = :to_c AND to_currency = :from_c
            ORDER BY rate_date DESC LIMIT 1
        """), {"tid": tid, "from_c": from_c, "to_c": to_c})
        rev = rev_q.fetchone()
        if rev:
            rate      = Decimal("1") / Decimal(str(rev[0]))
            rate_date = str(rev[1])
        else:
            raise HTTPException(404, f"لا يوجد سعر صرف بين {from_c} و {to_c}")

    converted = (body.amount * rate).quantize(Decimal("0.001"))
    return ok(data={
        "from_currency": from_c,
        "to_currency":   to_c,
        "original":      float(body.amount),
        "converted":     float(converted),
        "rate":          float(rate),
        "rate_date":     rate_date,
    })
