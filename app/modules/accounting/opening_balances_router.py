"""
app/modules/accounting/opening_balances_router.py
══════════════════════════════════════════════════════════
الأرصدة الافتتاحية — Opening Balances
Raw SQL — بدون ORM

Endpoints:
  GET    /api/v1/opening-balances          → قائمة الأرصدة مدمجة مع COA
  POST   /api/v1/opening-balances/batch    → حفظ دفعة (upsert)
  POST   /api/v1/opening-balances/post     → ترحيل إلى account_balances
  POST   /api/v1/opening-balances/unpost   → إلغاء الترحيل
  GET    /api/v1/opening-balances/summary  → ملخص الإجماليات
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
# from app.core.auth import get_current_tenant_id

router = APIRouter(prefix="/opening-balances", tags=["Opening Balances"])

TENANT_ID = "00000000-0000-0000-0000-000000000001"


# ── Schemas ──────────────────────────────────────────────
class OBLine(BaseModel):
    account_code: str
    account_name: str
    debit:        float = 0.0
    credit:       float = 0.0
    notes:        Optional[str] = None


class OBBatchRequest(BaseModel):
    fiscal_year: int
    lines:       List[OBLine]


class OBPostRequest(BaseModel):
    fiscal_year: int


# ── Helpers ───────────────────────────────────────────────
def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _get_status(db: AsyncSession, tenant_id: str, fiscal_year: int) -> str:
    """يُعيد 'posted' إذا كان الترحيل مكتملاً، وإلا 'draft'."""
    result = await db.execute(text("""
        SELECT COUNT(*) FROM opening_balances
        WHERE tenant_id = :tid AND fiscal_year = :yr AND status = 'posted'
    """), {"tid": tenant_id, "yr": fiscal_year})
    count = result.scalar()
    return "posted" if count and count > 0 else "draft"


# ══════════════════════════════════════════════════════════
# GET — قائمة الأرصدة مدمجة مع COA
# ══════════════════════════════════════════════════════════
@router.get("")
async def list_opening_balances(
    fiscal_year: int,
    db: AsyncSession = Depends(get_db),
):
    """
    يُعيد كل الحسابات القابلة للترحيل مع أرصدتها الافتتاحية.
    الحسابات التي لا يوجد لها رصيد ترجع بـ debit=0, credit=0.
    """
    tenant_id = TENANT_ID

    result = await db.execute(text("""
        SELECT
            c.code                  AS account_code,
            c.name_ar               AS account_name,
            c.account_type,
            c.account_nature,
            COALESCE(ob.debit,  0)  AS debit,
            COALESCE(ob.credit, 0)  AS credit,
            COALESCE(ob.notes,  '')  AS notes,
            COALESCE(ob.status, 'draft') AS status,
            ob.posted_at,
            ob.posted_by
        FROM coa_accounts c
        LEFT JOIN opening_balances ob
            ON ob.account_code = c.code
            AND ob.tenant_id   = c.tenant_id
            AND ob.fiscal_year = :yr
        WHERE c.tenant_id = :tid
          AND c.postable   = true
          AND c.is_active  = true
        ORDER BY c.code
    """), {"tid": tenant_id, "yr": fiscal_year})

    rows = result.mappings().all()

    overall_status = await _get_status(db, tenant_id, fiscal_year)

    lines = []
    for r in rows:
        lines.append({
            "account_code":   r["account_code"],
            "account_name":   r["account_name"],
            "account_type":   r["account_type"],
            "account_nature": r["account_nature"],
            "debit":          float(r["debit"]),
            "credit":         float(r["credit"]),
            "notes":          r["notes"] or "",
            "status":         r["status"],
        })

    total_debit  = sum(l["debit"]  for l in lines)
    total_credit = sum(l["credit"] for l in lines)

    return {
        "fiscal_year":    fiscal_year,
        "status":         overall_status,
        "total_debit":    round(total_debit,  3),
        "total_credit":   round(total_credit, 3),
        "difference":     round(total_debit - total_credit, 3),
        "balanced":       abs(total_debit - total_credit) < 0.01,
        "lines":          lines,
    }


# ══════════════════════════════════════════════════════════
# GET — ملخص الإجماليات
# ══════════════════════════════════════════════════════════
@router.get("/summary")
async def get_summary(
    fiscal_year: int,
    db: AsyncSession = Depends(get_db),
):
    tenant_id = TENANT_ID

    result = await db.execute(text("""
        SELECT
            COALESCE(SUM(debit),  0) AS total_debit,
            COALESCE(SUM(credit), 0) AS total_credit,
            COUNT(*)                 AS account_count,
            COUNT(CASE WHEN debit > 0 OR credit > 0 THEN 1 END) AS non_zero_count
        FROM opening_balances
        WHERE tenant_id  = :tid
          AND fiscal_year = :yr
    """), {"tid": tenant_id, "yr": fiscal_year})

    row    = result.mappings().first()
    status = await _get_status(db, tenant_id, fiscal_year)

    td = float(row["total_debit"])
    tc = float(row["total_credit"])

    return {
        "fiscal_year":    fiscal_year,
        "status":         status,
        "total_debit":    round(td, 3),
        "total_credit":   round(tc, 3),
        "difference":     round(td - tc, 3),
        "balanced":       abs(td - tc) < 0.01,
        "account_count":  int(row["account_count"]),
        "non_zero_count": int(row["non_zero_count"]),
    }


# ══════════════════════════════════════════════════════════
# POST — حفظ دفعة (upsert)
# ══════════════════════════════════════════════════════════
@router.post("/batch")
async def save_batch(
    body: OBBatchRequest,
    db:   AsyncSession = Depends(get_db),
):
    """
    يحفظ الأرصدة الافتتاحية (INSERT or UPDATE).
    يرفض التعديل إذا كان الترحيل مكتملاً.
    """
    tenant_id = TENANT_ID

    # تحقق من أن الفترة لم تُرحَّل بعد
    status = await _get_status(db, tenant_id, body.fiscal_year)
    if status == "posted":
        raise HTTPException(400, "الأرصدة الافتتاحية مرحَّلة — يجب إلغاء الترحيل أولاً")

    now = _now()
    saved = 0

    for line in body.lines:
        # تخطّى الحسابات بقيمة صفر ما لم يكن موجوداً مسبقاً
        await db.execute(text("""
            INSERT INTO opening_balances
                (id, tenant_id, fiscal_year, account_code, account_name,
                 debit, credit, notes, status, created_at, updated_at)
            VALUES
                (gen_random_uuid(), :tid, :yr, :code, :name,
                 :dr, :cr, :notes, 'draft', :now, :now)
            ON CONFLICT (tenant_id, fiscal_year, account_code)
            DO UPDATE SET
                account_name = EXCLUDED.account_name,
                debit        = EXCLUDED.debit,
                credit       = EXCLUDED.credit,
                notes        = EXCLUDED.notes,
                updated_at   = EXCLUDED.updated_at
        """), {
            "tid":   tenant_id,
            "yr":    body.fiscal_year,
            "code":  line.account_code,
            "name":  line.account_name,
            "dr":    round(line.debit,  3),
            "cr":    round(line.credit, 3),
            "notes": line.notes or "",
            "now":   now,
        })
        saved += 1

    await db.commit()
    return {"message": f"تم حفظ {saved} حساب بنجاح", "saved": saved}


# ══════════════════════════════════════════════════════════
# POST — ترحيل إلى account_balances (fiscal_month = 0)
# ══════════════════════════════════════════════════════════
@router.post("/post")
async def post_opening_balances(
    body: OBPostRequest,
    db:   AsyncSession = Depends(get_db),
):
    """
    يُرحِّل الأرصدة الافتتاحية إلى account_balances بـ fiscal_month=0.
    يتحقق من التوازن (مدين = دائن) قبل الترحيل.
    """
    tenant_id = TENANT_ID
    now = _now()

    # تحقق من عدم الترحيل المسبق
    status = await _get_status(db, tenant_id, body.fiscal_year)
    if status == "posted":
        raise HTTPException(400, "الأرصدة الافتتاحية مرحَّلة مسبقاً")

    # جلب الأرصدة الافتتاحية غير الصفرية
    result = await db.execute(text("""
        SELECT account_code, account_name, debit, credit
        FROM opening_balances
        WHERE tenant_id   = :tid
          AND fiscal_year = :yr
          AND (debit > 0 OR credit > 0)
    """), {"tid": tenant_id, "yr": body.fiscal_year})

    lines = result.mappings().all()

    if not lines:
        raise HTTPException(400, "لا توجد أرصدة افتتاحية لترحيلها")

    total_debit  = sum(float(l["debit"])  for l in lines)
    total_credit = sum(float(l["credit"]) for l in lines)

    if abs(total_debit - total_credit) >= 0.01:
        raise HTTPException(400, (
            f"الأرصدة الافتتاحية غير متوازنة — "
            f"مدين: {total_debit:.3f} | دائن: {total_credit:.3f} | "
            f"الفرق: {abs(total_debit - total_credit):.3f}"
        ))

    # كتابة في account_balances بـ fiscal_month=0
    for line in lines:
        balance = float(line["debit"]) - float(line["credit"])
        await db.execute(text("""
            INSERT INTO account_balances
                (id, tenant_id, account_code, fiscal_year, fiscal_month,
                 debit_total, credit_total, balance,
                 opening_balance, closing_balance, last_posted_at)
            VALUES
                (gen_random_uuid(), :tid, :code, :yr, 0,
                 :dr, :cr, :bal,
                 :bal, :bal, :now)
            ON CONFLICT (tenant_id, account_code, fiscal_year, fiscal_month)
            DO UPDATE SET
                debit_total     = EXCLUDED.debit_total,
                credit_total    = EXCLUDED.credit_total,
                balance         = EXCLUDED.balance,
                opening_balance = EXCLUDED.opening_balance,
                closing_balance = EXCLUDED.closing_balance,
                last_posted_at  = EXCLUDED.last_posted_at
        """), {
            "tid":  tenant_id,
            "code": line["account_code"],
            "yr":   body.fiscal_year,
            "dr":   float(line["debit"]),
            "cr":   float(line["credit"]),
            "bal":  round(balance, 3),
            "now":  now,
        })

    # تحديث حالة الأرصدة الافتتاحية إلى posted
    await db.execute(text("""
        UPDATE opening_balances
        SET status     = 'posted',
            posted_at  = :now,
            posted_by  = 'system',
            updated_at = :now
        WHERE tenant_id   = :tid
          AND fiscal_year = :yr
    """), {"tid": tenant_id, "yr": body.fiscal_year, "now": now})

    await db.commit()

    return {
        "message":      f"تم ترحيل الأرصدة الافتتاحية لسنة {body.fiscal_year} بنجاح",
        "fiscal_year":  body.fiscal_year,
        "lines_posted": len(lines),
        "total_debit":  round(total_debit,  3),
        "total_credit": round(total_credit, 3),
    }


# ══════════════════════════════════════════════════════════
# POST — إلغاء الترحيل
# ══════════════════════════════════════════════════════════
@router.post("/unpost")
async def unpost_opening_balances(
    body: OBPostRequest,
    db:   AsyncSession = Depends(get_db),
):
    """
    يُلغي ترحيل الأرصدة الافتتاحية:
    - يحذف سجلات account_balances بـ fiscal_month=0
    - يُعيد status إلى draft
    """
    tenant_id = TENANT_ID
    now = _now()

    status = await _get_status(db, tenant_id, body.fiscal_year)
    if status != "posted":
        raise HTTPException(400, "الأرصدة الافتتاحية غير مرحَّلة")

    # حذف من account_balances
    await db.execute(text("""
        DELETE FROM account_balances
        WHERE tenant_id   = :tid
          AND fiscal_year = :yr
          AND fiscal_month = 0
    """), {"tid": tenant_id, "yr": body.fiscal_year})

    # إعادة الحالة إلى draft
    await db.execute(text("""
        UPDATE opening_balances
        SET status     = 'draft',
            posted_at  = NULL,
            posted_by  = NULL,
            updated_at = :now
        WHERE tenant_id   = :tid
          AND fiscal_year = :yr
    """), {"tid": tenant_id, "yr": body.fiscal_year, "now": now})

    await db.commit()

    return {
        "message":     f"تم إلغاء ترحيل الأرصدة الافتتاحية لسنة {body.fiscal_year}",
        "fiscal_year": body.fiscal_year,
    }
