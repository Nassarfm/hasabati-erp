"""
app/modules/parties/router.py
══════════════════════════════════════════════════════════
المتعاملون — Financial Parties
نموذج موحد يجمع: الموظفين، أمناء الصناديق، العملاء، الموردين
كل كيان يتعامل معه المال = متعامل واحد بـ ID موحد

Endpoints:
  /parties          — CRUD المتعاملين
  /parties/{id}/statement — كشف حساب المتعامل الموحد
  /parties/{id}/balance   — رصيد المتعامل الحالي
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/parties", tags=["المتعاملون"])


# ══════════════════════════════════════════════════════════
# PARTY ROLES — أدوار المتعامل
# ══════════════════════════════════════════════════════════
PARTY_ROLES = {
    "employee_loan":      "سلفة موظف",
    "petty_cash_keeper":  "أمين عهدة نثرية",
    "fund_keeper":        "أمين صندوق",
    "customer":           "عميل",
    "vendor":             "مورد",
    "shareholder":        "مساهم",
    "other":              "أخرى",
}

PARTY_TYPES = {
    "employee":  "موظف",
    "customer":  "عميل",
    "vendor":    "مورد",
    "other":     "أخرى",
}


# ══════════════════════════════════════════════════════════
# LIST & CREATE
# ══════════════════════════════════════════════════════════

@router.get("")
async def list_parties(
    party_type:  Optional[str] = Query(default=None),
    search:      Optional[str] = Query(default=None),
    is_active:   Optional[bool] = Query(default=None),
    limit:       int            = Query(default=100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة المتعاملين مع فلترة"""
    tid = str(user.tenant_id)
    where = ["tenant_id = :tid"]
    params: dict = {"tid": tid, "limit": limit}

    if party_type:
        where.append("party_type = :ptype")
        params["ptype"] = party_type
    if search:
        where.append("(party_name_ar ILIKE :q OR party_code ILIKE :q OR national_id ILIKE :q)")
        params["q"] = f"%{search}%"
    if is_active is not None:
        where.append("is_active = :active")
        params["active"] = is_active

    r = await db.execute(text(f"""
        SELECT
            id, party_code, party_name_ar, party_name_en,
            party_type, is_employee, is_customer, is_vendor, is_fund_keeper,
            national_id, phone, email, notes,
            net_balance, is_active,
            employee_id, customer_id, vendor_id,
            created_at
        FROM parties
        WHERE {' AND '.join(where)}
        ORDER BY party_name_ar
        LIMIT :limit
    """), params)

    rows = []
    for row in r.mappings().fetchall():
        d = dict(row)
        d["net_balance"] = float(d.get("net_balance") or 0)
        d["party_type_label"] = PARTY_TYPES.get(d.get("party_type", ""), d.get("party_type", ""))
        rows.append(d)

    return ok(data=rows)


@router.post("", status_code=201)
async def create_party(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إنشاء متعامل جديد"""
    tid = str(user.tenant_id)

    if not data.get("party_name_ar"):
        raise HTTPException(400, "اسم المتعامل بالعربية مطلوب")

    # توليد كود تلقائي إذا لم يُحدَّد
    party_code = data.get("party_code")
    if not party_code:
        prefix = {
            "employee": "EMP",
            "customer": "CUST",
            "vendor":   "VEND",
        }.get(data.get("party_type", "other"), "PARTY")
        r_seq = await db.execute(text("""
            SELECT COUNT(*) + 1 AS next_num
            FROM parties WHERE tenant_id = :tid AND party_code LIKE :prefix
        """), {"tid": tid, "prefix": f"{prefix}%"})
        seq = r_seq.scalar() or 1
        party_code = f"{prefix}-{seq:04d}"

    party_id = str(uuid.uuid4())
    try:
        await db.execute(text("""
            INSERT INTO parties (
                id, tenant_id, party_code, party_name_ar, party_name_en,
                party_type, is_employee, is_customer, is_vendor, is_fund_keeper,
                national_id, phone, email, notes,
                employee_id, customer_id, vendor_id,
                is_active, created_by
            ) VALUES (
                :id, :tid, :code, :name_ar, :name_en,
                :ptype, :is_emp, :is_cust, :is_vend, :is_fund,
                :national_id, :phone, :email, :notes,
                :emp_id, :cust_id, :vend_id,
                true, :created_by
            )
        """), {
            "id":          party_id,
            "tid":         tid,
            "code":        party_code,
            "name_ar":     data["party_name_ar"].strip(),
            "name_en":     data.get("party_name_en") or None,
            "ptype":       data.get("party_type") or "other",
            "is_emp":      bool(data.get("is_employee", False)),
            "is_cust":     bool(data.get("is_customer", False)),
            "is_vend":     bool(data.get("is_vendor", False)),
            "is_fund":     bool(data.get("is_fund_keeper", False)),
            "national_id": data.get("national_id") or None,
            "phone":       data.get("phone") or None,
            "email":       data.get("email") or None,
            "notes":       data.get("notes") or None,
            "emp_id":      data.get("employee_id") or None,
            "cust_id":     data.get("customer_id") or None,
            "vend_id":     data.get("vendor_id") or None,
            "created_by":  user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في إنشاء المتعامل: {str(e)}")

    return created(data={"id": party_id, "party_code": party_code},
                   message=f"تم إنشاء المتعامل {party_code} ✅")


# ══════════════════════════════════════════════════════════
# ROLE DEFINITIONS — يجب أن تكون قبل /{party_id} لتجنب التعارض
# ══════════════════════════════════════════════════════════

@router.get("/role-definitions")
async def list_role_definitions(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة أدوار المتعاملين — النظامية والمخصصة"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT id, role_code, role_name_ar, role_name_en,
                   is_system, is_active, sort_order
            FROM party_role_definitions
            WHERE tenant_id = :tid AND is_active = true
            ORDER BY sort_order, role_name_ar
        """), {"tid": tid})
        rows = [dict(row) for row in r.mappings().fetchall()]
        if not rows:
            raise Exception("no rows")
        return ok(data=rows)
    except Exception:
        return ok(data=[
            {"role_code":"employee_loan",    "role_name_ar":"سلفة موظف",       "role_name_en":"Employee Loan",     "is_system":True,  "sort_order":1},
            {"role_code":"petty_cash_keeper","role_name_ar":"أمين عهدة نثرية", "role_name_en":"Petty Cash Keeper", "is_system":True,  "sort_order":2},
            {"role_code":"fund_keeper",      "role_name_ar":"أمين صندوق",       "role_name_en":"Fund Keeper",       "is_system":True,  "sort_order":3},
            {"role_code":"customer",         "role_name_ar":"عميل",             "role_name_en":"Customer",          "is_system":True,  "sort_order":4},
            {"role_code":"vendor",           "role_name_ar":"مورد",             "role_name_en":"Vendor",            "is_system":True,  "sort_order":5},
            {"role_code":"shareholder",      "role_name_ar":"مساهم",            "role_name_en":"Shareholder",       "is_system":False, "sort_order":6},
            {"role_code":"contractor",       "role_name_ar":"مقاول / متعاقد",   "role_name_en":"Contractor",        "is_system":False, "sort_order":7},
            {"role_code":"government",       "role_name_ar":"جهة حكومية",       "role_name_en":"Government Entity", "is_system":False, "sort_order":8},
            {"role_code":"other",            "role_name_ar":"أخرى",             "role_name_en":"Other",             "is_system":True,  "sort_order":99},
        ])


@router.post("/role-definitions", status_code=201)
async def create_role_definition_v2(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إضافة دور مخصص جديد"""
    tid = str(user.tenant_id)
    if not data.get("role_name_ar"):
        raise HTTPException(400, "اسم الدور بالعربية مطلوب")
    role_code = (data.get("role_code") or
                 data["role_name_ar"].strip().replace(" ","_").lower())
    rid = str(uuid.uuid4())
    try:
        await db.execute(text("""
            INSERT INTO party_role_definitions
              (id, tenant_id, role_code, role_name_ar, role_name_en,
               is_system, is_active, sort_order)
            VALUES (:id,:tid,:code,:name_ar,:name_en,FALSE,TRUE,:sort)
            ON CONFLICT (tenant_id, role_code) DO UPDATE
              SET role_name_ar=:name_ar, role_name_en=:name_en, sort_order=:sort
        """), {"id":rid,"tid":tid,"code":role_code,
               "name_ar":data["role_name_ar"].strip(),
               "name_en":data.get("role_name_en") or None,
               "sort":data.get("sort_order",50)})
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في إضافة الدور: {str(e)}")
    return created(data={"id":rid,"role_code":role_code}, message="تم إضافة الدور ✅")


@router.delete("/role-definitions/{role_id}")
async def delete_role_definition_v2(
    role_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حذف دور مخصص"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text(
            "DELETE FROM party_role_definitions "
            "WHERE id=:id AND tenant_id=:tid AND is_system=FALSE"
        ), {"id":str(role_id),"tid":tid})
        if r.rowcount == 0:
            raise HTTPException(404, "الدور غير موجود أو هو دور نظامي محمي")
        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(message="تم الحذف ✅")


# ══════════════════════════════════════════════════════════
# PARTY CRUD — /{party_id} routes يجب أن تكون بعد الـ static routes
# ══════════════════════════════════════════════════════════

@router.get("/{party_id}")
async def get_party(
    party_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تفاصيل متعامل"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT * FROM parties
        WHERE id = :id AND tenant_id = :tid
    """), {"id": str(party_id), "tid": tid})
    row = r.mappings().fetchone()
    if not row:
        raise HTTPException(404, "المتعامل غير موجود")
    d = dict(row)
    d["net_balance"] = float(d.get("net_balance") or 0)
    return ok(data=d)
async def update_party(
    party_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تعديل بيانات متعامل"""
    tid = str(user.tenant_id)
    ALLOWED = {
        "party_name_ar", "party_name_en", "party_type",
        "is_employee", "is_customer", "is_vendor", "is_fund_keeper",
        "national_id", "phone", "email", "notes", "is_active",
        "employee_id", "customer_id", "vendor_id",
    }
    safe = {k: v for k, v in data.items() if k in ALLOWED}
    if not safe:
        raise HTTPException(400, "لا توجد بيانات للتعديل")

    safe["updated_at"] = "NOW()"
    set_clause = ", ".join([
        f"{k} = NOW()" if v == "NOW()" else f"{k} = :{k}"
        for k, v in safe.items()
    ])
    safe_params = {k: v for k, v in safe.items() if v != "NOW()"}
    safe_params.update({"id": str(party_id), "tid": tid})

    try:
        await db.execute(text(
            f"UPDATE parties SET {set_clause}, updated_at = NOW() "
            f"WHERE id = :id AND tenant_id = :tid"
        ), safe_params)
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في التعديل: {str(e)}")

    return ok(data={"id": str(party_id)}, message="تم التعديل ✅")


# ══════════════════════════════════════════════════════════
# PARTY STATEMENT — كشف حساب المتعامل الموحد
# ══════════════════════════════════════════════════════════

@router.get("/{party_id}/statement")
async def party_statement(
    party_id:  uuid.UUID,
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    كشف حساب المتعامل الموحد — يجمع كل القيود المرتبطة بهذا المتعامل
    من جميع الموديولات: الخزينة، الرواتب، الذمم، المشتريات
    """
    tid = str(user.tenant_id)

    # بيانات المتعامل
    r_party = await db.execute(text("""
        SELECT party_code, party_name_ar, party_type, net_balance
        FROM parties WHERE id = :id AND tenant_id = :tid
    """), {"id": str(party_id), "tid": tid})
    party = r_party.mappings().fetchone()
    if not party:
        raise HTTPException(404, "المتعامل غير موجود")

    # فلاتر التاريخ
    date_filter = ""
    params: dict = {"tid": tid, "pid": str(party_id)}
    if date_from:
        date_filter += " AND je.entry_date >= :df"
        params["df"] = date_from
    if date_to:
        date_filter += " AND je.entry_date <= :dt"
        params["dt"] = date_to

    # جلب سطور القيود المرتبطة بالمتعامل
    try:
        r = await db.execute(text(f"""
            SELECT
                je.entry_date::text      AS entry_date,
                je.serial                AS je_serial,
                je.je_type               AS je_type,
                je.description           AS je_description,
                je.source_module         AS source_module,
                jl.account_code,
                jl.description           AS line_description,
                jl.party_role,
                COALESCE(jl.debit,  0)   AS debit,
                COALESCE(jl.credit, 0)   AS credit
            FROM je_lines jl
            JOIN journal_entries je ON je.id = jl.je_id
            WHERE jl.party_id::text = :pid
              AND je.tenant_id = :tid
              AND je.status    = 'posted'
              {date_filter}
            ORDER BY je.entry_date, je.serial
        """), params)

        rows = []
        running_balance = 0.0
        for row in r.mappings().fetchall():
            d = dict(row)
            d["debit"]  = float(d.get("debit")  or 0)
            d["credit"] = float(d.get("credit") or 0)
            running_balance += d["debit"] - d["credit"]
            d["running_balance"] = round(running_balance, 3)
            d["party_role_label"] = PARTY_ROLES.get(d.get("party_role", ""), d.get("party_role", ""))
            rows.append(d)

        total_debit  = sum(r["debit"]  for r in rows)
        total_credit = sum(r["credit"] for r in rows)

        return ok(data={
            "party": {
                "id":          str(party_id),
                "party_code":  party["party_code"],
                "party_name":  party["party_name_ar"],
                "party_type":  party["party_type"],
                "net_balance": float(party.get("net_balance") or 0),
            },
            "rows":          rows,
            "total_debit":   round(total_debit, 3),
            "total_credit":  round(total_credit, 3),
            "closing_balance": round(running_balance, 3),
            "period": {
                "from": date_from or "—",
                "to":   date_to   or "—",
            }
        })
    except Exception as e:
        import traceback; print(f"[party-statement] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في كشف الحساب: {str(e)}")


# ══════════════════════════════════════════════════════════
# PARTY BALANCE — رصيد المتعامل الحالي
# ══════════════════════════════════════════════════════════

@router.get("/{party_id}/balance")
async def party_balance(
    party_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """رصيد المتعامل الحالي محسوباً من القيود"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT
                jl.party_role,
                COALESCE(SUM(jl.debit),  0) AS total_debit,
                COALESCE(SUM(jl.credit), 0) AS total_credit,
                COALESCE(SUM(jl.debit - jl.credit), 0) AS net
            FROM je_lines jl
            JOIN journal_entries je ON je.id = jl.je_id
            WHERE jl.party_id::text = :pid
              AND je.tenant_id = :tid
              AND je.status = 'posted'
            GROUP BY jl.party_role
        """), {"pid": str(party_id), "tid": tid})

        breakdown = []
        total_net = 0.0
        for row in r.mappings().fetchall():
            net = float(row.get("net") or 0)
            total_net += net
            breakdown.append({
                "party_role":       row["party_role"],
                "party_role_label": PARTY_ROLES.get(row["party_role"] or "", row["party_role"] or ""),
                "total_debit":      float(row.get("total_debit")  or 0),
                "total_credit":     float(row.get("total_credit") or 0),
                "net":              round(net, 3),
            })

        # تحديث net_balance في جدول parties
        await db.execute(text("""
            UPDATE parties SET net_balance = :bal, updated_at = NOW()
            WHERE id = :id AND tenant_id = :tid
        """), {"bal": round(total_net, 3), "id": str(party_id), "tid": tid})
        await db.commit()

        return ok(data={
            "party_id":  str(party_id),
            "net_balance": round(total_net, 3),
            "breakdown": breakdown,
        })
    except Exception as e:
        raise HTTPException(500, f"خطأ في حساب الرصيد: {str(e)}")


# ══════════════════════════════════════════════════════════
# OPEN BALANCES — الأرصدة المفتوحة لجميع المتعاملين
# ══════════════════════════════════════════════════════════

@router.get("/reports/open-balances")
async def open_balances(
    party_type:   Optional[str] = Query(default=None),
    min_balance:  Optional[float] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تقرير الأرصدة المفتوحة لجميع المتعاملين"""
    tid = str(user.tenant_id)
    type_filter = "AND p.party_type = :ptype" if party_type else ""
    bal_filter  = "HAVING ABS(COALESCE(SUM(jl.debit - jl.credit), 0)) >= :minbal" if min_balance is not None else ""
    params: dict = {"tid": tid}
    if party_type:    params["ptype"]  = party_type
    if min_balance is not None: params["minbal"] = min_balance

    try:
        r = await db.execute(text(f"""
            SELECT
                p.id,
                p.party_code,
                p.party_name_ar,
                p.party_type,
                COALESCE(SUM(jl.debit),  0) AS total_debit,
                COALESCE(SUM(jl.credit), 0) AS total_credit,
                COALESCE(SUM(jl.debit - jl.credit), 0) AS net_balance
            FROM parties p
            LEFT JOIN je_lines jl ON jl.party_id::text = p.id::text
            LEFT JOIN journal_entries je ON je.id = jl.je_id
                AND je.status = 'posted' AND je.tenant_id = :tid
            WHERE p.tenant_id = :tid AND p.is_active = true
            {type_filter}
            GROUP BY p.id, p.party_code, p.party_name_ar, p.party_type
            {bal_filter}
            ORDER BY ABS(COALESCE(SUM(jl.debit - jl.credit), 0)) DESC
        """), params)

        rows = []
        for row in r.mappings().fetchall():
            d = dict(row)
            d["total_debit"]  = float(d.get("total_debit")  or 0)
            d["total_credit"] = float(d.get("total_credit") or 0)
            d["net_balance"]  = float(d.get("net_balance")  or 0)
            d["party_type_label"] = PARTY_TYPES.get(d.get("party_type", ""), "")
            rows.append(d)

        return ok(data={
            "rows":  rows,
            "total": len(rows),
            "total_net": round(sum(r["net_balance"] for r in rows), 3),
        })
    except Exception as e:
        import traceback; print(f"[open-balances] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في تقرير الأرصدة: {str(e)}")


# نهاية ملف المتعاملين — الأدوار معرّفة في أعلى الملف قبل /{party_id}

