"""
app/modules/treasury/router.py  # v3-fixed
══════════════════════════════════════════════════════════
Treasury & Banking Module — Complete API
الخزينة والبنوك — واجهة برمجية كاملة

Endpoints:
  Dashboard, Bank Accounts, Cash Transactions (PV/RV),
  Bank Transactions (BP/BR/BT), Internal Transfers (IT),
  Checks, Bank Reconciliation, Petty Cash
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Body, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/treasury", tags=["الخزينة والبنوك"])

TID = "00000000-0000-0000-0000-000000000001"


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
async def _next_serial(db: AsyncSession, tid: str, je_type: str, tx_date: date) -> str:
    year = tx_date.year
    r = await db.execute(text("""
        UPDATE je_sequences SET last_sequence = last_sequence + 1
        WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:year
        RETURNING last_sequence
    """), {"tid": tid, "code": je_type, "year": year})
    row = r.fetchone()
    if not row:
        await db.execute(text("""
            INSERT INTO je_sequences (id,tenant_id,je_type_code,fiscal_year,last_sequence)
            VALUES (gen_random_uuid(),:tid,:code,:year,1)
            ON CONFLICT (tenant_id,je_type_code,fiscal_year)
            DO UPDATE SET last_sequence=je_sequences.last_sequence+1
        """), {"tid": tid, "code": je_type, "year": year})
        r2 = await db.execute(text("""
            SELECT last_sequence FROM je_sequences
            WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:year
        """), {"tid": tid, "code": je_type, "year": year})
        seq = r2.fetchone()[0]
    else:
        seq = row[0]

    # جلب الإعدادات
    sr = await db.execute(text("""
        SELECT prefix, padding, separator FROM series_settings
        WHERE tenant_id=:tid AND je_type_code=:code LIMIT 1
    """), {"tid": tid, "code": je_type})
    srow = sr.fetchone()
    prefix = srow[0] if srow else je_type
    padding = srow[1] if srow else 7
    sep = srow[2] if srow else "-"
    return f"{prefix}{sep}{year}{sep}{seq:0{padding}d}"


async def _get_account_default_roles(db, tid: str, account_codes: list) -> dict:
    """جلب default_party_role لكل حساب من coa_accounts.extra_data
    يرجع dict: {account_code: default_role_or_None}"""
    if not account_codes:
        return {}
    try:
        codes_unique = list({c for c in account_codes if c})
        if not codes_unique:
            return {}
        r = await db.execute(text("""
            SELECT code, extra_data->>'default_party_role' AS dr
            FROM coa_accounts
            WHERE tenant_id = :tid
              AND code = ANY(:codes)
        """), {"tid": tid, "codes": codes_unique})
        return {row[0]: row[1] for row in r.fetchall() if row[1]}
    except Exception:
        return {}


async def _post_je(db, tid: str, user_email: str, je_type: str, tx_date: date,
                   description: str, lines: list, reference: str = None) -> dict:
    """إنشاء قيد محاسبي مرتبط بعملية الخزينة — يرفع exception عند الفشل
    Smart Hybrid: لو السطر فيه party_id لكن لا party_role → يُستنتج من default_party_role للحساب"""
    valid = [l for l in lines if l.get("account_code") and
             (Decimal(str(l.get("debit",0))) > 0 or Decimal(str(l.get("credit",0))) > 0)]
    if not valid:
        raise HTTPException(400, "لا توجد سطور قيد صحيحة للترحيل")

    total_dr = sum(Decimal(str(l.get("debit",0)))  for l in valid)
    total_cr = sum(Decimal(str(l.get("credit",0))) for l in valid)
    if abs(total_dr - total_cr) > Decimal("0.01"):
        raise HTTPException(400, f"القيد غير متوازن: مدين={total_dr} دائن={total_cr}")

    # Smart Hybrid: استنتاج party_role من الحساب لو السطر فيه party_id بدون role
    needs_role = [l.get("account_code") for l in valid
                  if l.get("party_id") and not l.get("party_role")]
    if needs_role:
        role_map = await _get_account_default_roles(db, tid, needs_role)
        for l in valid:
            if l.get("party_id") and not l.get("party_role"):
                inferred = role_map.get(l.get("account_code"))
                if inferred:
                    l["party_role"] = inferred

    try:
        from app.services.posting.engine import PostingEngine, PostingRequest, PostingLine
        t_id = uuid.UUID(tid)
        engine = PostingEngine(db, t_id)
        posting_lines = [PostingLine(
            account_code=l["account_code"],
            description=l.get("description", description),
            debit=Decimal(str(l.get("debit", 0))),
            credit=Decimal(str(l.get("credit", 0))),
            branch_code=l.get("branch_code"),
            cost_center=l.get("cost_center"),
            project_code=l.get("project_code"),
            expense_classification_code=l.get("expense_classification_code"),
            currency_code=l.get("currency_code", "SAR"),
            # ── المتعامل / Party ──
            party_id=l.get("party_id"),
            party_role=l.get("party_role"),
        ) for l in valid]
        result = await engine.post(PostingRequest(
            tenant_id=t_id,
            je_type=je_type,
            description=description,
            entry_date=tx_date,
            lines=posting_lines,
            created_by_email=user_email,
            reference=reference,
            source_module="treasury",
        ))
        if not result or not result.je_id:
            raise HTTPException(500, "فشل محرك القيود — لم يُنشأ القيد")
        return {"je_id": str(result.je_id), "je_serial": result.je_serial}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"خطأ في محرك القيود: {str(e)}")


async def _update_balance(db, bank_account_id: str, delta: Decimal):
    await db.execute(text("""
        UPDATE tr_bank_accounts
        SET current_balance = current_balance + :delta, updated_at = NOW()
        WHERE id = :id
    """), {"delta": delta, "id": bank_account_id})


# ══════════════════════════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════════════════════════
@router.get("/dashboard")
async def dashboard(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        # 1. أرصدة البنوك والصناديق
        r = await db.execute(text("""
            SELECT id, account_code, account_name, account_type, currency_code,
                   COALESCE(current_balance,0)   AS current_balance,
                   COALESCE(low_balance_alert,0) AS low_balance_alert
            FROM tr_bank_accounts
            WHERE tenant_id=:tid AND is_active=true
            ORDER BY account_type, account_name
        """), {"tid": tid})
        accounts = []
        for row in r.fetchall():
            a = dict(row._mapping)
            a["current_balance"]   = float(a.get("current_balance")   or 0)
            a["low_balance_alert"] = float(a.get("low_balance_alert") or 0)
            accounts.append(a)

        total_bank = sum(a["current_balance"] for a in accounts if a["account_type"]=="bank")
        total_cash = sum(a["current_balance"] for a in accounts if a["account_type"] in ("cash_fund","petty_cash"))
        alerts = [a for a in accounts if a["low_balance_alert"]>0 and a["current_balance"]<=a["low_balance_alert"]]

        # 2. حركات اليوم
        today = date.today()   # asyncpg يحتاج date object وليس string
        r2 = await db.execute(text("""
            SELECT COALESCE(SUM(CASE WHEN tx_type='RV' THEN amount ELSE 0 END),0) AS receipts,
                   COALESCE(SUM(CASE WHEN tx_type='PV' THEN amount ELSE 0 END),0) AS payments
            FROM tr_cash_transactions
            WHERE tenant_id=:tid AND tx_date=:today AND status='posted'
        """), {"tid": tid, "today": today})
        today_row = r2.fetchone()
        today_receipts = float(today_row[0] or 0)
        today_payments = float(today_row[1] or 0)

        # 3. الشيكات المستحقة (7 أيام)
        r3 = await db.execute(text("""
            SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS total
            FROM tr_checks
            WHERE tenant_id=:tid AND status='issued'
              AND due_date IS NOT NULL
              AND due_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7
        """), {"tid": tid})
        ck = r3.fetchone()
        due_checks = {"count": int(ck[0] or 0), "total": float(ck[1] or 0)}

        # 4. تدفقات آخر 30 يوم
        r4 = await db.execute(text("""
            SELECT tx_date::text AS dt,
                   SUM(CASE WHEN tx_type='RV' THEN amount ELSE 0 END) AS receipts,
                   SUM(CASE WHEN tx_type='PV' THEN amount ELSE 0 END) AS payments
            FROM tr_cash_transactions
            WHERE tenant_id=:tid AND status='posted'
              AND tx_date >= CURRENT_DATE - 29
            GROUP BY tx_date ORDER BY tx_date
        """), {"tid": tid})
        cash_flow_chart = [{"date": row[0], "receipts": float(row[1] or 0), "payments": float(row[2] or 0)}
                           for row in r4.fetchall()]

        # 5. KPI counts — كل استعلام منفصل لتجنب فشل الكل بسبب جدول مفقود
        kpi = {"banks":0,"funds":0,"petty_funds":0,"pending_expenses":0,"pending_amount":0,"pending_vouchers":0,"pending_bank_tx":0}
        try:
            r5a = await db.execute(text("""
                SELECT
                  (SELECT COUNT(*) FROM tr_bank_accounts WHERE tenant_id=:tid AND account_type='bank'      AND is_active=true) AS banks,
                  (SELECT COUNT(*) FROM tr_bank_accounts WHERE tenant_id=:tid AND account_type='cash_fund' AND is_active=true) AS funds,
                  (SELECT COUNT(*) FROM tr_cash_transactions WHERE tenant_id=:tid AND status='draft')  AS pending_vouchers,
                  (SELECT COUNT(*) FROM tr_bank_transactions WHERE tenant_id=:tid AND status='draft')  AS pending_bank_tx
            """), {"tid": tid})
            row5a = r5a.mappings().fetchone() or {}
            kpi.update({k: int(row5a.get(k) or 0) for k in row5a.keys()})
        except Exception as e5a:
            print(f"[Dashboard KPI-1] {e5a}")
        try:
            r5b = await db.execute(text("""
                SELECT
                  COUNT(*) AS petty_funds
                FROM tr_petty_cash_funds WHERE tenant_id=:tid AND is_active=true
            """), {"tid": tid})
            row5b = r5b.mappings().fetchone()
            if row5b: kpi["petty_funds"] = int(row5b.get("petty_funds") or 0)
        except Exception as e5b:
            print(f"[Dashboard KPI-2 petty_funds] {e5b}")
        try:
            r5c = await db.execute(text("""
                SELECT COUNT(*) AS pending_expenses,
                       COALESCE(SUM(COALESCE(total_amount,0)),0) AS pending_amount
                FROM tr_petty_cash_expenses WHERE tenant_id=:tid AND status='draft'
            """), {"tid": tid})
            row5c = r5c.mappings().fetchone()
            if row5c:
                kpi["pending_expenses"] = int(row5c.get("pending_expenses") or 0)
                kpi["pending_amount"]   = float(row5c.get("pending_amount") or 0)
        except Exception as e5c:
            print(f"[Dashboard KPI-3 petty_expenses] {e5c}")

        return ok(data={
            "kpis": {
                "total_balance":     total_bank + total_cash,
                "bank_balance":      total_bank,
                "cash_balance":      total_cash,
                "bank_count":        int(kpi.get("banks")         or 0),
                "fund_count":        int(kpi.get("funds")         or 0),
                "petty_fund_count":  int(kpi.get("petty_funds")   or 0),
                "today_receipts":    today_receipts,
                "today_payments":    today_payments,
                "pending_vouchers":  int(kpi.get("pending_vouchers") or 0),
                "pending_bank_tx":   int(kpi.get("pending_bank_tx")  or 0),
                "pending_expenses":  int(kpi.get("pending_expenses") or 0),
                "pending_expense_amount": float(kpi.get("pending_amount") or 0),
                "need_replenish":    0,
            },
            "accounts":        accounts,
            "alerts":          alerts,
            "due_checks":      due_checks,
            "cash_flow_chart": cash_flow_chart,
            "reconciliation": {
                "total_posted":0,"reconciled":0,"unreconciled":0,
                "reconciled_amount":0.0,"unreconciled_amount":0.0,
            },
        })

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[Dashboard ERROR] {tb}")
        raise HTTPException(status_code=500, detail=f"خطأ في لوحة التحكم: {str(e)} | {tb[-300:]}")


@router.get("/reports/cash-forecast")
async def cash_forecast(
    days: int = Query(default=30),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """توقع التدفق النقدي للأيام القادمة بناءً على المعاملات المتكررة والشيكات"""
    tid = str(user.tenant_id)
    # الرصيد الحالي
    r = await db.execute(text("""
        SELECT COALESCE(SUM(current_balance),0) AS total
        FROM tr_bank_accounts WHERE tenant_id=:tid AND is_active=true
    """), {"tid": tid})
    current_balance = float(r.scalar() or 0)

    # الشيكات المستحقة
    r2 = await db.execute(text("""
        SELECT due_date::text, SUM(amount) AS total,
               SUM(CASE WHEN check_type='incoming' THEN amount ELSE 0 END) AS inflow,
               SUM(CASE WHEN check_type='outgoing' THEN amount ELSE 0 END) AS outflow
        FROM tr_checks
        WHERE tenant_id=:tid AND status='issued'
          AND due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + :days
        GROUP BY due_date ORDER BY due_date
    """), {"tid": tid, "days": days})
    check_rows = [dict(r._mapping) for r in r2.fetchall()]

    # حركات معلقة (draft)
    r3 = await db.execute(text("""
        SELECT COALESCE(SUM(CASE WHEN tx_type='PV' THEN amount ELSE 0 END),0) AS pending_out,
               COALESCE(SUM(CASE WHEN tx_type='RV' THEN amount ELSE 0 END),0) AS pending_in
        FROM tr_cash_transactions WHERE tenant_id=:tid AND status='draft'
    """), {"tid": tid})
    p = r3.mappings().fetchone()
    r4 = await db.execute(text("""
        SELECT COALESCE(SUM(CASE WHEN tx_type IN ('BP','BT') THEN amount ELSE 0 END),0) AS pending_out,
               COALESCE(SUM(CASE WHEN tx_type='BR' THEN amount ELSE 0 END),0) AS pending_in
        FROM tr_bank_transactions WHERE tenant_id=:tid AND status='draft'
    """), {"tid": tid})
    p2 = r4.mappings().fetchone()

    return ok(data={
        "current_balance": current_balance,
        "pending_inflow":  float(p["pending_in"]) + float(p2["pending_in"]),
        "pending_outflow": float(p["pending_out"]) + float(p2["pending_out"]),
        "expected_checks": check_rows,
        "forecast_balance": current_balance + float(p["pending_in"]) + float(p2["pending_in"])
                            - float(p["pending_out"]) - float(p2["pending_out"]),
    })


# ══════════════════════════════════════════════════════════
# BANK ACCOUNTS
# ══════════════════════════════════════════════════════════
@router.get("/bank-accounts")
async def list_bank_accounts(
    account_type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    where = "WHERE tenant_id=:tid"
    params = {"tid": tid}
    if account_type:
        where += " AND account_type=:atype"
        params["atype"] = account_type
    r = await db.execute(text(f"""
        SELECT * FROM tr_bank_accounts {where}
        ORDER BY account_type, account_name
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/bank-accounts", status_code=201)
async def create_bank_account(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    # التحقق من الحقول المطلوبة
    if not data.get("account_code"):
        raise HTTPException(400, "كود الحساب مطلوب")
    if not data.get("account_name"):
        raise HTTPException(400, "اسم الحساب مطلوب")
    if not data.get("gl_account_code"):
        raise HTTPException(400, "حساب الأستاذ العام مطلوب")

    ba_id = str(uuid.uuid4())
    account_type = data.get("account_type") or "bank"

    # ── Subledger Logic ──────────────────────────────────────────────────
    # البنوك: كل بنك يجب أن يكون له GL account مستقل (1:1)
    # الصناديق: يجوز لعدة صناديق مشاركة نفس GL account (Subledger)
    # لأن الصناديق تُعتبر دفتر أستاذ مساعد لـ Control Account واحد
    if account_type == "bank":
        dup = await db.execute(text(
            "SELECT account_name FROM tr_bank_accounts"
            " WHERE tenant_id=:tid AND gl_account_code=:gl AND account_type='bank'"
        ), {"tid": tid, "gl": str(data["gl_account_code"]).strip()})
        dup_row = dup.fetchone()
        if dup_row:
            raise HTTPException(400,
                f"حساب الأستاذ العام مستخدم مسبقاً في البنك '{dup_row[0]}' — "
                f"كل حساب بنكي يجب أن يرتبط بحساب GL مستقل")

    # تحويل آمن للأرقام
    def to_decimal(v, default=0):
        try: return Decimal(str(v)) if v not in (None, "", "null") else Decimal(default)
        except: return Decimal(default)

    # تحويل آمن للتاريخ
    def to_date_safe(v):
        if v in (None, "", "null", "undefined"): return None
        try: return date.fromisoformat(str(v))
        except: return None

    try:
        await db.execute(text("""
            INSERT INTO tr_bank_accounts (
                id, tenant_id, account_code, account_name,
                account_type, account_sub_type,
                bank_name, bank_branch, account_number,
                iban, swift_code, currency_code, gl_account_code,
                opening_balance, current_balance, low_balance_alert,
                opening_date, contact_person, contact_phone, notes,
                is_active, created_by
            ) VALUES (
                :id, :tid, :account_code, :account_name,
                :account_type, :account_sub_type,
                :bank_name, :bank_branch, :account_number,
                :iban, :swift_code, :currency_code, :gl_account_code,
                :opening_balance, :current_balance, :low_balance_alert,
                :opening_date, :contact_person, :contact_phone, :notes,
                true, :created_by
            )
        """), {
            "id":               ba_id,
            "tid":              tid,
            "account_code":     str(data["account_code"]).strip(),
            "account_name":     str(data["account_name"]).strip(),
            "account_type":     data.get("account_type") or "bank",
            "account_sub_type": data.get("account_sub_type") or None,
            "bank_name":        data.get("bank_name") or None,
            "bank_branch":      data.get("bank_branch") or None,
            "account_number":   data.get("account_number") or None,
            "iban":             data.get("iban") or None,
            "swift_code":       data.get("swift_code") or None,
            "currency_code":    data.get("currency_code") or "SAR",
            "gl_account_code":  str(data["gl_account_code"]).strip(),
            "opening_balance":  to_decimal(data.get("opening_balance"), 0),
            "current_balance":  to_decimal(data.get("opening_balance"), 0),
            "low_balance_alert":to_decimal(data.get("low_balance_alert"), 0),
            "opening_date":     to_date_safe(data.get("opening_date")),
            "contact_person":   data.get("contact_person") or None,
            "contact_phone":    data.get("contact_phone") or None,
            "notes":            data.get("notes") or None,
            "created_by":       user.email,
        })
        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"خطأ في قاعدة البيانات: {str(e)}")

    return created(
        data={"id": ba_id, "account_code": data["account_code"]},
        message=f"تم إنشاء الحساب {data['account_code']} ✅"
    )


@router.put("/bank-accounts/{ba_id}")
async def update_bank_account(
    ba_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    ALLOWED = {
        "account_code","account_name","account_name_en","account_type","account_sub_type",
        "bank_name","bank_branch","account_number","iban","swift_code",
        "currency_code","gl_account_code","opening_balance",
        "low_balance_alert","credit_limit","notes","is_active",
        "opening_date","contact_person","contact_phone",
    }
    safe = {k:v for k,v in data.items() if k in ALLOWED}
    if not safe:
        raise HTTPException(status_code=400, detail="لا توجد بيانات للتعديل")

    # ── Subledger Logic في التعديل ──
    # فقط للبنوك: تحقق من تفرد GL account
    # الصناديق: يجوز المشاركة (Subledger)
    if "gl_account_code" in safe:
        new_gl = str(safe["gl_account_code"]).strip()
        # اجلب نوع الحساب الحالي
        cur_type_r = await db.execute(text(
            "SELECT account_type FROM tr_bank_accounts WHERE id=:id AND tenant_id=:tid"
        ), {"id": str(ba_id), "tid": tid})
        cur_type_row = cur_type_r.fetchone()
        cur_account_type = cur_type_row[0] if cur_type_row else (safe.get("account_type") or "bank")

        if cur_account_type == "bank":
            dup = await db.execute(text(
                "SELECT account_name FROM tr_bank_accounts"
                " WHERE tenant_id=:tid AND gl_account_code=:gl"
                " AND account_type='bank' AND id != :skip"
            ), {"tid": tid, "gl": new_gl, "skip": str(ba_id)})
            dup_row = dup.fetchone()
            if dup_row:
                raise HTTPException(400,
                    f"حساب الأستاذ العام مستخدم مسبقاً في البنك '{dup_row[0]}' — "
                    f"كل حساب بنكي يجب أن يرتبط بحساب GL مستقل")

    # ── تحويل الحقول الحساسة ────────────────────────────────
    # حقول التاريخ: string فارغ → None
    for date_field in ("opening_date",):
        if date_field in safe:
            v = safe[date_field]
            if v in (None, "", "null", "undefined"):
                safe[date_field] = None
            else:
                try:
                    safe[date_field] = date.fromisoformat(str(v))
                except Exception:
                    safe[date_field] = None

    # حقول Decimal: string فارغ → 0
    for dec_field in ("opening_balance", "low_balance_alert", "credit_limit"):
        if dec_field in safe:
            try:
                safe[dec_field] = Decimal(str(safe[dec_field])) if safe[dec_field] not in (None,"") else Decimal(0)
            except Exception:
                safe[dec_field] = Decimal(0)

    # حقول نصية: string فارغ → None
    for str_field in ("account_name_en","bank_name","bank_branch","account_number",
                      "iban","swift_code","contact_person","contact_phone","notes",
                      "account_sub_type"):
        if str_field in safe and safe[str_field] == "":
            safe[str_field] = None

    safe["updated_at"] = datetime.utcnow()
    set_clause = ", ".join([f"{k}=:{k}" for k in safe.keys()])
    safe.update({"id": str(ba_id), "tid": tid})

    try:
        await db.execute(text(f"UPDATE tr_bank_accounts SET {set_clause} WHERE id=:id AND tenant_id=:tid"), safe)
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"خطأ في التعديل: {str(e)}")

    return ok(data={"id": str(ba_id)}, message="تم التعديل ✅")


@router.delete("/bank-accounts/{ba_id}")
async def delete_bank_account(
    ba_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_bank_accounts
        SET is_active=false, updated_at=NOW(),
            deactivated_at=NOW(), deactivated_by=:by
        WHERE id=:id AND tenant_id=:tid
    """), {"id": str(ba_id), "tid": tid, "by": user.email})
    await db.commit()
    return ok(data={}, message="تم إيقاف الحساب")


@router.patch("/bank-accounts/{ba_id}/toggle-active")
async def toggle_bank_account_active(
    ba_id: uuid.UUID,
    data: dict = Body(default={}),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تفعيل / إيقاف حساب بنكي مع تسجيل السبب والتاريخ"""
    tid = str(user.tenant_id)
    cur = await db.execute(text(
        "SELECT is_active, account_name FROM tr_bank_accounts"
        " WHERE id=:id AND tenant_id=:tid"
    ), {"id": str(ba_id), "tid": tid})
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "الحساب غير موجود")

    new_active = not row[0]
    reason     = data.get("reason") or None

    try:
        if new_active:
            # إعادة التفعيل
            await db.execute(text("""
                UPDATE tr_bank_accounts
                SET is_active=true, updated_at=NOW(),
                    deactivated_at=NULL,
                    deactivation_reason=NULL,
                    deactivated_by=NULL
                WHERE id=:id AND tenant_id=:tid
            """), {"id": str(ba_id), "tid": tid})
        else:
            # إيقاف — تسجيل التاريخ والسبب والمستخدم
            await db.execute(text("""
                UPDATE tr_bank_accounts
                SET is_active=false, updated_at=NOW(),
                    deactivated_at=NOW(),
                    deactivation_reason=:reason,
                    deactivated_by=:by
                WHERE id=:id AND tenant_id=:tid
            """), {"id": str(ba_id), "tid": tid, "reason": reason, "by": user.email})
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في تغيير الحالة: {str(e)}")

    status_ar = "مفعّل ✅" if new_active else "موقوف 🔴"
    return ok(
        data={"is_active": new_active, "account_name": row[1]},
        message=f"تم تغيير حالة '{row[1]}' إلى {status_ar}"
    )


# ══════════════════════════════════════════════════════════
# CASH TRANSACTIONS (PV / RV)
# ══════════════════════════════════════════════════════════
@router.get("/cash-transactions")
async def list_cash_transactions(
    tx_type:        Optional[str]  = Query(None),
    status:         Optional[str]  = Query(None),
    bank_account_id: Optional[uuid.UUID] = Query(None),
    date_from:      Optional[date] = Query(None),
    date_to:        Optional[date] = Query(None),
    limit:          int            = Query(50),
    offset:         int            = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["ct.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}
    if tx_type:        conds.append("ct.tx_type=:tx_type");       params["tx_type"] = tx_type
    if status:         conds.append("ct.status=:status");         params["status"] = status
    if bank_account_id: conds.append("ct.bank_account_id=:ba");   params["ba"] = str(bank_account_id)
    if date_from:      conds.append("ct.tx_date>=:df");           params["df"] = str(date_from)
    if date_to:        conds.append("ct.tx_date<=:dt");           params["dt"] = str(date_to)
    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM tr_cash_transactions ct WHERE {where}"), params)
    total = cnt.scalar()

    r = await db.execute(text(f"""
        SELECT ct.*, ba.account_name AS bank_account_name, ba.account_type AS bank_type
        FROM tr_cash_transactions ct
        LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
        WHERE {where}
        ORDER BY ct.tx_date DESC, ct.created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total": total, "items": [dict(row._mapping) for row in r.fetchall()]})


@router.post("/cash-transactions", status_code=201)
async def create_cash_transaction(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    tx_date = date.fromisoformat(str(data["tx_date"]))
    tx_type = data["tx_type"]  # PV | RV
    serial = await _next_serial(db, tid, tx_type, tx_date)

    tx_id = str(uuid.uuid4())
    amt = Decimal(str(data["amount"]))
    amt_sar = amt * Decimal(str(data.get("exchange_rate", 1)))

    try:
        await db.execute(text("""
            INSERT INTO tr_cash_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,
               currency_code,exchange_rate,amount_sar,counterpart_account,
               description,party_name,reference,payment_method,check_number,
               branch_code,cost_center,project_code,notes,
               vat_amount,vat_account_code,expense_classification_code,
               party_id,party_role,
               status,created_by)
            VALUES
              (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,
               :cur,:rate,:amt_sar,:cp_acc,
               :desc,:party,:ref,:method,:check_no,
               :branch,:cc,:proj,:notes,
               :vat_amount,:vat_acc,:exp_cls,
               :party_id,:party_role,
               'draft',:by)
        """), {
            "id": tx_id, "tid": tid, "serial": serial,
            "tx_type": tx_type, "tx_date": tx_date,
            "ba_id": str(data["bank_account_id"]) if data.get("bank_account_id") else None,
            "amount": amt, "cur": data.get("currency_code","SAR"),
            "rate": data.get("exchange_rate",1), "amt_sar": amt_sar,
            "cp_acc": data["counterpart_account"],
            "desc": data["description"], "party": data.get("party_name"),
            "ref": data.get("reference"), "method": data.get("payment_method","cash"),
            "check_no": data.get("check_number"), "branch": data.get("branch_code"),
            "cc": data.get("cost_center"), "proj": data.get("project_code"),
            "notes": data.get("notes"),
            "vat_amount": Decimal(str(data.get("vat_amount") or 0)),
            "vat_acc":    data.get("vat_account_code") or None,
            "exp_cls":    data.get("expense_classification_code") or None,
            "party_id":   data.get("party_id") or None,
            "party_role": data.get("party_role") or None,
            "by": user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"خطأ في الحفظ: {str(e)}")
    return created(data={"id": tx_id, "serial": serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/cash-transactions/{tx_id}/post")
async def post_cash_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ct.*, ba.gl_account_code
        FROM tr_cash_transactions ct
        LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
        WHERE ct.id=:id AND ct.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] != "draft":
        raise HTTPException(400, f"السند في حالة '{tx['status']}' ولا يمكن ترحيله")

    gl  = tx["gl_account_code"]
    cp  = tx["counterpart_account"]
    if not gl: raise HTTPException(400, "حساب الأستاذ العام غير محدد — تحقق من بطاقة الحساب البنكي")
    if not cp: raise HTTPException(400, "الحساب المقابل غير محدد في السند")

    amt      = Decimal(str(tx["amount_sar"] or tx["amount"]))
    vat_amt  = Decimal(str(tx.get("vat_amount") or 0))
    vat_acc  = tx.get("vat_account_code")
    base_amt = amt - vat_amt
    total    = amt
    tx_date  = tx["tx_date"]
    desc     = tx["description"] or ""
    # الأبعاد
    dims     = {
        "branch_code": tx.get("branch_code"),
        "cost_center": tx.get("cost_center"),
        "project_code": tx.get("project_code"),
        "expense_classification_code": tx.get("expense_classification_code"),
    }
    # المتعامل — يُضاف للسطر المقابل (الحساب المقابل)
    party_dims = {
        **dims,
        "party_id":   str(tx["party_id"]) if tx.get("party_id") else None,
        "party_role": tx.get("party_role") or None,
    }

    if tx["tx_type"] == "RV":
        lines = [
            {"account_code": gl, "debit": float(total),    "credit": 0,               "description": desc},
            {"account_code": cp, "debit": 0,               "credit": float(base_amt), "description": desc, **party_dims},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": 0, "credit": float(vat_amt), "description": "ضريبة القيمة المضافة"})
        delta = total
    else:  # PV
        lines = [
            {"account_code": cp, "debit": float(base_amt), "credit": 0,            "description": desc, **party_dims},
            {"account_code": gl, "debit": 0,               "credit": float(total), "description": desc},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": float(vat_amt), "credit": 0, "description": "ضريبة القيمة المضافة"})
        delta = -total

    try:
        je = await _post_je(db, tid, user.email, tx["tx_type"], tx_date, desc, lines, tx.get("reference"))
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        await db.execute(text("""
            UPDATE tr_cash_transactions
            SET status='posted', je_id=:je_id, je_serial=:je_serial,
                posted_by=:by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"je_id": je["je_id"], "je_serial": je["je_serial"],
               "by": user.email, "id": str(tx_id), "tid": tid})
        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الترحيل: {str(e)}")
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


@router.delete("/cash-transactions/{tx_id}")
async def cancel_cash_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_cash_transactions SET status='cancelled'
        WHERE id=:id AND tenant_id=:tid AND status='draft'
    """), {"id": str(tx_id), "tid": tid})
    await db.commit()
    return ok(data={}, message="تم إلغاء السند")


# ══════════════════════════════════════════════════════════
# BANK TRANSACTIONS (BP / BR / BT)
# ══════════════════════════════════════════════════════════
@router.get("/bank-transactions")
async def list_bank_transactions(
    tx_type:         Optional[str]       = Query(None),
    status:          Optional[str]       = Query(None),
    bank_account_id: Optional[uuid.UUID] = Query(None),
    date_from:       Optional[date]      = Query(None),
    date_to:         Optional[date]      = Query(None),
    limit:           int                 = Query(50),
    offset:          int                 = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["bt.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}
    if tx_type:         conds.append("bt.tx_type=:tx_type");   params["tx_type"] = tx_type
    if status:          conds.append("bt.status=:status");     params["status"] = status
    if bank_account_id: conds.append("bt.bank_account_id=:ba"); params["ba"] = str(bank_account_id)
    if date_from:       conds.append("bt.tx_date>=:df");        params["df"] = str(date_from)
    if date_to:         conds.append("bt.tx_date<=:dt");        params["dt"] = str(date_to)
    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM tr_bank_transactions bt WHERE {where}"), params)
    total = cnt.scalar()

    r = await db.execute(text(f"""
        SELECT bt.*, ba.account_name AS bank_account_name
        FROM tr_bank_transactions bt
        LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
        WHERE {where}
        ORDER BY bt.tx_date DESC, bt.created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total": total, "items": [dict(row._mapping) for row in r.fetchall()]})


@router.post("/bank-transactions", status_code=201)
async def create_bank_transaction(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    tx_date = date.fromisoformat(str(data["tx_date"]))
    tx_type = data["tx_type"]
    serial = await _next_serial(db, tid, tx_type, tx_date)

    tx_id = str(uuid.uuid4())
    amt = Decimal(str(data["amount"]))
    amt_sar = amt * Decimal(str(data.get("exchange_rate", 1)))

    await db.execute(text("""
        INSERT INTO tr_bank_transactions
          (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,
           currency_code,exchange_rate,amount_sar,counterpart_account,
           beneficiary_name,beneficiary_iban,beneficiary_bank,
           description,reference,payment_method,check_number,
           branch_code,cost_center,project_code,notes,
           vat_amount,vat_account_code,expense_classification_code,
           party_id,party_role,
           status,created_by)
        VALUES
          (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,
           :cur,:rate,:amt_sar,:cp_acc,
           :ben_name,:ben_iban,:ben_bank,
           :desc,:ref,:method,:check_no,
           :branch,:cc,:proj,:notes,
           :vat_amount,:vat_acc,:exp_cls,
           :party_id,:party_role,
           'draft',:by)
    """), {
        "id": tx_id, "tid": tid, "serial": serial,
        "tx_type": tx_type, "tx_date": tx_date,
        "ba_id": str(data["bank_account_id"]) if data.get("bank_account_id") else None,
        "amount": amt, "cur": data.get("currency_code","SAR"),
        "rate": data.get("exchange_rate",1), "amt_sar": amt_sar,
        "cp_acc": data.get("counterpart_account"),
        "ben_name": data.get("beneficiary_name"), "ben_iban": data.get("beneficiary_iban"),
        "ben_bank": data.get("beneficiary_bank"),
        "desc": data["description"], "ref": data.get("reference"),
        "method": data.get("payment_method","wire"),
        "check_no": data.get("check_number"), "branch": data.get("branch_code"),
        "cc": data.get("cost_center"), "proj": data.get("project_code"),
        "notes": data.get("notes"), "by": user.email,
        "vat_amount": Decimal(str(data.get("vat_amount") or 0)),
        "vat_acc":    data.get("vat_account_code") or None,
        "exp_cls":    data.get("expense_classification_code") or None,
        "party_id":   data.get("party_id") or None,
        "party_role": data.get("party_role") or None,
    })
    await db.commit()
    return created(data={"id": tx_id, "serial": serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/bank-transactions/{tx_id}/post")
async def post_bank_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT bt.*, ba.gl_account_code
        FROM tr_bank_transactions bt
        LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
        WHERE bt.id=:id AND bt.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] != "draft":
        raise HTTPException(400, f"السند في حالة '{tx['status']}' ولا يمكن ترحيله")

    gl  = tx["gl_account_code"]
    cp  = tx["counterpart_account"]
    if not gl: raise HTTPException(400, "حساب الأستاذ العام غير محدد")
    if not cp: raise HTTPException(400, "الحساب المقابل غير محدد")

    amt      = Decimal(str(tx["amount_sar"] or tx["amount"]))
    vat_amt  = Decimal(str(tx.get("vat_amount") or 0))
    vat_acc  = tx.get("vat_account_code")
    base_amt = amt - vat_amt
    total    = amt
    tx_date  = tx["tx_date"]
    desc     = tx["description"] or ""
    dims     = {
        "branch_code": tx.get("branch_code"),
        "cost_center": tx.get("cost_center"),
        "project_code": tx.get("project_code"),
        "expense_classification_code": tx.get("expense_classification_code"),
    }
    party_dims = {
        **dims,
        "party_id":   str(tx["party_id"]) if tx.get("party_id") else None,
        "party_role": tx.get("party_role") or None,
    }

    if tx["tx_type"] == "BR":
        lines = [
            {"account_code": gl, "debit": float(total),    "credit": 0,               "description": desc},
            {"account_code": cp, "debit": 0,               "credit": float(base_amt), "description": desc, **party_dims},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": 0, "credit": float(vat_amt), "description": "ضريبة"})
        delta = total
    else:  # BP or BT
        lines = [
            {"account_code": cp, "debit": float(base_amt), "credit": 0,            "description": desc, **party_dims},
            {"account_code": gl, "debit": 0,               "credit": float(total), "description": desc},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": float(vat_amt), "credit": 0, "description": "ضريبة"})
        delta = -total

    try:
        je = await _post_je(db, tid, user.email, tx["tx_type"], tx_date, desc, lines, tx.get("reference"))
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        await db.execute(text("""
            UPDATE tr_bank_transactions
            SET status='posted', je_id=:je_id, je_serial=:je_serial,
                posted_by=:by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"je_id": je["je_id"], "je_serial": je["je_serial"],
               "by": user.email, "id": str(tx_id), "tid": tid})
        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الترحيل: {str(e)}")
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")

@router.get("/internal-transfers")
async def list_internal_transfers(
    status:    Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    limit:     int           = Query(default=50),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        where, params = ["it.tenant_id=:tid"], {"tid": tid}
        if status:    where.append("it.status=:status");    params["status"] = status
        if date_from: where.append("it.tx_date>=:df");      params["df"]     = date_from
        if date_to:   where.append("it.tx_date<=:dt");      params["dt"]     = date_to
        r = await db.execute(text("""
            SELECT
                it.id, it.tenant_id, it.serial, it.tx_date::text AS tx_date,
                it.from_account_id, it.to_account_id,
                COALESCE(it.amount, 0)              AS amount,
                COALESCE(it.amount_to, 0)           AS amount_to,
                COALESCE(it.currency_code, 'SAR')   AS currency_code,
                COALESCE(it.description, '')         AS description,
                COALESCE(it.reference, '')           AS reference,
                COALESCE(it.status, 'draft')         AS status,
                it.je_serial,
                it.created_by, it.created_at,
                it.posted_by,  it.posted_at,
                COALESCE(it.is_reconciled, false) AS is_reconciled,
                it.reconciled_at,
                fa.account_name AS from_account_name,
                ta.account_name AS to_account_name
            FROM tr_internal_transfers it
            LEFT JOIN tr_bank_accounts fa ON fa.id = it.from_account_id
            LEFT JOIN tr_bank_accounts ta ON ta.id = it.to_account_id
            WHERE """ + " AND ".join(where) + """
            ORDER BY it.tx_date DESC, it.serial DESC
            LIMIT :limit
        """), {**params, "limit": limit})
        rows = []
        for row in r.mappings().fetchall():
            d = dict(row)
            d["amount"]    = float(d.get("amount") or 0)
            d["amount_to"] = float(d.get("amount_to") or 0)
            rows.append(d)

        r2 = await db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE status='draft')  AS drafts,
                COUNT(*) FILTER (WHERE status='posted') AS posted,
                COALESCE(SUM(amount), 0)                AS total_amount
            FROM tr_internal_transfers WHERE tenant_id=:tid
        """), {"tid": tid})
        stats = r2.mappings().fetchone() or {}
        return ok(data={
            "items": rows, "total": len(rows),
            "stats": {
                "drafts":       int(stats.get("drafts") or 0),
                "posted":       int(stats.get("posted") or 0),
                "total_amount": float(stats.get("total_amount") or 0),
            }
        })
    except Exception as e:
        import traceback
        print(f"[internal-transfers] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في جلب التحويلات الداخلية: {str(e)}")


@router.post("/internal-transfers", status_code=201)
async def create_internal_transfer(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    tx_date = date.fromisoformat(str(data["tx_date"]))
    serial = await _next_serial(db, tid, "IT", tx_date)
    tx_id = str(uuid.uuid4())
    amt = Decimal(str(data["amount"]))

    try:
        await db.execute(text("""
            INSERT INTO tr_internal_transfers
              (id,tenant_id,serial,tx_date,from_account_id,to_account_id,
               amount,currency_code,exchange_rate,amount_to,
               description,reference,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:tx_date,:from_id,:to_id,
               :amount,:cur,:rate,:amount_to,
               :desc,:ref,:notes,'draft',:by)
        """), {
            "id": tx_id, "tid": tid, "serial": serial, "tx_date": tx_date,
            "from_id": str(data["from_account_id"]),
            "to_id":   str(data["to_account_id"]),
            "amount":  amt, "cur": data.get("currency_code","SAR"),
            "rate":    data.get("exchange_rate",1),
            "amount_to": data.get("amount_to", amt),
            "desc":    data["description"], "ref": data.get("reference"),
            "notes":   data.get("notes"), "by": user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"خطأ في حفظ التحويل: {str(e)}")
    return created(data={"id": tx_id, "serial": serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/internal-transfers/{tx_id}/post")
async def post_internal_transfer(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT it.*,
               fa.gl_account_code AS from_gl,
               ta.gl_account_code AS to_gl
        FROM tr_internal_transfers it
        JOIN tr_bank_accounts fa ON fa.id=it.from_account_id
        JOIN tr_bank_accounts ta ON ta.id=it.to_account_id
        WHERE it.id=:id AND it.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise Exception("التحويل غير موجود")
    if tx["status"] != "draft": raise Exception("مُرحَّل مسبقاً")

    amt = Decimal(str(tx["amount"]))
    desc = tx["description"]
    lines = [
        {"account_code": tx["to_gl"],   "debit": amt, "credit": 0,   "description": desc},
        {"account_code": tx["from_gl"], "debit": 0,   "credit": amt, "description": desc},
    ]
    je = await _post_je(db, tid, user.email, "IT", tx["tx_date"], desc, lines, tx["reference"])
    if not je.get("je_id"):
        raise HTTPException(500, "فشل إنشاء قيد التحويل — تحقق من حسابات الأستاذ العام")
    await _update_balance(db, str(tx["from_account_id"]), -amt)
    await _update_balance(db, str(tx["to_account_id"]),   amt)

    await db.execute(text("""
        UPDATE tr_internal_transfers
        SET status='posted', je_id=:je_id, je_serial=:je_serial,
            posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id": je["je_id"], "je_serial": je["je_serial"],
           "by": user.email, "id": str(tx_id), "tid": tid})
    await db.commit()
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# CHECKS
# ══════════════════════════════════════════════════════════
@router.get("/checks")
async def list_checks(
    check_type: Optional[str]  = Query(None),
    status:     Optional[str]  = Query(None),
    date_from:  Optional[date] = Query(None),
    date_to:    Optional[date] = Query(None),
    book_id:    Optional[uuid.UUID] = Query(None),
    limit: int = Query(50), offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["ck.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}
    if check_type: conds.append("ck.check_type=:ct");     params["ct"]    = check_type
    if status:     conds.append("ck.status=:status");     params["status"]= status
    if date_from:  conds.append("ck.check_date>=:df");    params["df"]    = str(date_from)
    if date_to:    conds.append("ck.check_date<=:dt");    params["dt"]    = str(date_to)
    if book_id:    conds.append("ck.cheque_book_id=:bid");params["bid"]   = str(book_id)
    where = " AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM tr_checks ck WHERE {where}"), params)
    try:
        r = await db.execute(text(f"""
            SELECT ck.*,
                   ba.account_name   AS bank_account_name,
                   ba.account_code   AS bank_account_code,
                   ba.gl_account     AS bank_gl_account,
                   cb.book_code      AS cheque_book_code,
                   coa.name_ar       AS gl_account_name
            FROM tr_checks ck
            LEFT JOIN tr_bank_accounts ba ON ba.id = ck.bank_account_id
            LEFT JOIN tr_cheque_books  cb ON cb.id = ck.cheque_book_id
            LEFT JOIN coa_accounts     coa ON coa.code = ck.gl_account_code
                                          AND coa.tenant_id = ck.tenant_id
                                          AND coa.is_deleted = FALSE
            WHERE {where}
            ORDER BY ck.check_date DESC
            LIMIT :limit OFFSET :offset
        """), params)
        return ok(data={"total": cnt.scalar(), "items": [dict(row._mapping) for row in r.fetchall()]})
    except Exception as e:
        # Fallback: بدون JOIN على coa_accounts (في حال فشل الـ JOIN لأي سبب)
        import logging
        logging.getLogger(__name__).warning("[list_checks] coa JOIN failed, fallback: " + str(e))
        r2 = await db.execute(text(f"""
            SELECT ck.*,
                   ba.account_name   AS bank_account_name,
                   ba.account_code   AS bank_account_code,
                   ba.gl_account     AS bank_gl_account,
                   cb.book_code      AS cheque_book_code,
                   NULL              AS gl_account_name
            FROM tr_checks ck
            LEFT JOIN tr_bank_accounts ba ON ba.id = ck.bank_account_id
            LEFT JOIN tr_cheque_books  cb ON cb.id = ck.cheque_book_id
            WHERE {where}
            ORDER BY ck.check_date DESC
            LIMIT :limit OFFSET :offset
        """), params)
        return ok(data={"total": cnt.scalar(), "items": [dict(row._mapping) for row in r2.fetchall()]})


@router.post("/checks", status_code=201)
async def create_check(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    check_date = date.fromisoformat(str(data["check_date"]))
    serial = await _next_serial(db, tid, "CHK", check_date)
    ck_id  = str(uuid.uuid4())

    # إذا تم تحديد دفتر شيكات — نأخذ الرقم التالي منه دائماً (نتجاهل ما يرسله الـ frontend)
    # هذا يمنع تكرار رقم الشيك حتى لو الـ frontend أرسل next_number قديم
    check_number = data.get("check_number")
    book_id = data.get("cheque_book_id")
    if book_id:
        r = await db.execute(text("""
            SELECT next_number, series_to FROM tr_cheque_books
            WHERE id=:id AND tenant_id=:tid AND status='active'
        """), {"id": str(book_id), "tid": tid})
        book = r.mappings().fetchone()
        if not book:
            raise HTTPException(400, "دفتر الشيكات غير موجود أو غير نشط")
        if book["next_number"] > book["series_to"]:
            raise HTTPException(400, "دفتر الشيكات منتهي — اختر دفتراً آخر")
        # نأخذ الرقم من الدفتر دائماً (نتجاوز ما يرسله الـ frontend)
        check_number = str(book["next_number"])
        # نحدّث الدفتر مباشرة لمنع التكرار
        await db.execute(text("""
            UPDATE tr_cheque_books
            SET next_number = next_number + 1,
                used_leaves  = used_leaves  + 1,
                status = CASE WHEN next_number + 1 > series_to THEN 'exhausted' ELSE status END
            WHERE id=:id AND tenant_id=:tid
        """), {"id": str(book_id), "tid": tid})
    elif not check_number:
        raise HTTPException(400, "يجب تحديد دفتر شيكات أو رقم شيك يدوي")

    try:
        await db.execute(text("""
            INSERT INTO tr_checks
              (id,tenant_id,serial,check_number,check_type,check_date,due_date,
               bank_account_id,cheque_book_id,amount,payee_name,party_id,party_name,party_role,
               description,gl_account_code,status,notes,created_by)
            VALUES
              (:id,:tid,:serial,:ck_no,:ck_type,:ck_date,:due_date,
               :ba_id,:book_id,:amount,:payee,:party_id,:party_name,:party_role,
               :desc,:gl_code,'draft',:notes,:by)
        """), {
            "id": ck_id, "tid": tid, "serial": serial,
            "ck_no":     check_number,
            "ck_type":   data.get("check_type","outgoing"),
            "ck_date":   check_date,
            "due_date":  date.fromisoformat(str(data["due_date"])) if data.get("due_date") else None,
            "ba_id":     str(data["bank_account_id"]) if data.get("bank_account_id") else None,
            "book_id":   str(book_id) if book_id else None,
            "amount":    Decimal(str(data["amount"])),
            "payee":     data.get("payee_name"),
            "party_id":  data.get("party_id") or None,
            "party_name":data.get("party_name"),
            "party_role":data.get("party_role") or None,
            "desc":      data.get("description"),
            "gl_code":   data.get("gl_account_code"),
            "notes":     data.get("notes"),
            "by":        user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, str(e))

    return created(data={"id": ck_id, "serial": serial, "check_number": check_number},
                   message="تم انشاء الشيك " + serial)


@router.get("/checks/books")
async def list_cheque_books(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT cb.*,
               ba.account_name AS bank_account_name,
               ba.account_code AS bank_account_code,
               (cb.series_to - cb.next_number + 1) AS remaining_leaves
        FROM tr_cheque_books cb
        LEFT JOIN tr_bank_accounts ba ON ba.id = cb.bank_account_id
        WHERE cb.tenant_id = :tid
        ORDER BY cb.created_at DESC
    """), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/checks/books", status_code=201)
async def create_cheque_book(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    book_id = str(uuid.uuid4())
    series_from = int(data["series_from"])
    series_to   = int(data["series_to"])
    if series_from >= series_to:
        raise HTTPException(400, "رقم البداية يجب أن يكون أصغر من رقم النهاية")
    await db.execute(text("""
        INSERT INTO tr_cheque_books
          (id, tenant_id, book_code, bank_account_id, bank_name,
           series_from, series_to, next_number, currency_code, notes, created_by)
        VALUES
          (:id, :tid, :code, :ba_id, :bank_name,
           :s_from, :s_to, :s_from, :currency, :notes, :by)
    """), {
        "id": book_id, "tid": tid,
        "code":      data.get("book_code") or data.get("bank_name","BK")[:3]+"-"+str(series_from),
        "ba_id":     str(data["bank_account_id"]) if data.get("bank_account_id") else None,
        "bank_name": data.get("bank_name"),
        "s_from":    series_from,
        "s_to":      series_to,
        "currency":  data.get("currency_code","SAR"),
        "notes":     data.get("notes"),
        "by":        user.email,
    })
    await db.commit()
    return created(data={"id": book_id}, message="تم انشاء دفتر الشيكات")


@router.put("/checks/books/{book_id}")
async def update_cheque_book(
    book_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_cheque_books
        SET bank_name=:bank_name, notes=:notes, status=:status
        WHERE id=:id AND tenant_id=:tid
    """), {"bank_name": data.get("bank_name"), "notes": data.get("notes"),
           "status": data.get("status","active"), "id": str(book_id), "tid": tid})
    await db.commit()
    return ok(message="تم التحديث")




@router.get("/checks/{ck_id}")
async def get_check(
    ck_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT ck.*,
                   ba.account_name   AS bank_account_name,
                   ba.account_code   AS bank_account_code,
                   ba.gl_account     AS bank_gl_account,
                   cb.book_code      AS cheque_book_code,
                   coa.name_ar       AS gl_account_name
            FROM tr_checks ck
            LEFT JOIN tr_bank_accounts ba ON ba.id = ck.bank_account_id
            LEFT JOIN tr_cheque_books  cb ON cb.id = ck.cheque_book_id
            LEFT JOIN coa_accounts     coa ON coa.code = ck.gl_account_code
                                          AND coa.tenant_id = ck.tenant_id
                                          AND coa.is_deleted = FALSE
            WHERE ck.id=:id AND ck.tenant_id=:tid
        """), {"id": str(ck_id), "tid": tid})
        row = r.mappings().fetchone()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("[get_check] coa JOIN failed, fallback: " + str(e))
        r = await db.execute(text("""
            SELECT ck.*,
                   ba.account_name   AS bank_account_name,
                   ba.account_code   AS bank_account_code,
                   ba.gl_account     AS bank_gl_account,
                   cb.book_code      AS cheque_book_code,
                   NULL              AS gl_account_name
            FROM tr_checks ck
            LEFT JOIN tr_bank_accounts ba ON ba.id = ck.bank_account_id
            LEFT JOIN tr_cheque_books  cb ON cb.id = ck.cheque_book_id
            WHERE ck.id=:id AND ck.tenant_id=:tid
        """), {"id": str(ck_id), "tid": tid})
        row = r.mappings().fetchone()
    if not row: raise HTTPException(404, "الشيك غير موجود")
    return ok(data=dict(row))


@router.put("/checks/{ck_id}")
async def update_check(
    ck_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text(
        "SELECT status FROM tr_checks WHERE id=:id AND tenant_id=:tid"
    ), {"id": str(ck_id), "tid": tid})
    row = r.fetchone()
    if not row: raise HTTPException(404, "الشيك غير موجود")
    if row[0] not in ("draft","submitted"): raise HTTPException(400, "لا يمكن تعديل شيك مُرحَّل")
    await db.execute(text("""
        UPDATE tr_checks SET
            check_date=:ck_date, due_date=:due_date,
            bank_account_id=:ba_id, amount=:amount,
            payee_name=:payee, party_id=:party_id, party_name=:party_name,
            description=:desc, gl_account_code=:gl_code,
            notes=:notes, updated_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {
        "ck_date":    data.get("check_date"),
        "due_date":   data.get("due_date"),
        "ba_id":      str(data["bank_account_id"]) if data.get("bank_account_id") else None,
        "amount":     Decimal(str(data["amount"])),
        "payee":      data.get("payee_name"),
        "party_id":   data.get("party_id") or None,
        "party_name": data.get("party_name"),
        "desc":       data.get("description"),
        "gl_code":    data.get("gl_account_code"),
        "notes":      data.get("notes"),
        "id": str(ck_id), "tid": tid,
    })
    await db.commit()
    return ok(message="تم التحديث")


@router.post("/checks/{ck_id}/submit")
async def submit_check(ck_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT status FROM tr_checks WHERE id=:id AND tenant_id=:tid"), {"id":str(ck_id),"tid":tid})
    row = r.fetchone()
    if not row: raise HTTPException(404, "غير موجود")
    if row[0] != "draft": raise HTTPException(400, "يجب أن يكون الشيك في حالة مسودة")
    await db.execute(text("UPDATE tr_checks SET status='submitted', submitted_by=:by, submitted_at=NOW() WHERE id=:id AND tenant_id=:tid"),
                     {"by": user.email, "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(message="تم الإرسال للمراجعة")


@router.post("/checks/{ck_id}/approve")
async def approve_check(ck_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT status FROM tr_checks WHERE id=:id AND tenant_id=:tid"), {"id":str(ck_id),"tid":tid})
    row = r.fetchone()
    if not row: raise HTTPException(404, "غير موجود")
    if row[0] not in ("submitted","draft"): raise HTTPException(400, "لا يمكن اعتماد هذا الشيك")
    await db.execute(text("UPDATE tr_checks SET status='approved', approved_by=:by, approved_at=NOW() WHERE id=:id AND tenant_id=:tid"),
                     {"by": user.email, "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(message="تم الاعتماد")


@router.post("/checks/{ck_id}/reject")
async def reject_check(ck_id: uuid.UUID, body: dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_checks SET status='rejected', rejected_by=:by, rejected_at=NOW(), rejection_reason=:reason
        WHERE id=:id AND tenant_id=:tid
    """), {"by": user.email, "reason": body.get("reason",""), "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(message="تم الرفض")


@router.post("/checks/{ck_id}/post")
async def post_check(ck_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ck.*, ba.gl_account AS bank_gl
        FROM tr_checks ck
        LEFT JOIN tr_bank_accounts ba ON ba.id=ck.bank_account_id
        WHERE ck.id=:id AND ck.tenant_id=:tid
    """), {"id": str(ck_id), "tid": tid})
    ck = r.mappings().fetchone()
    if not ck: raise HTTPException(404, "غير موجود")
    if ck["status"] not in ("draft","submitted","approved"):
        raise HTTPException(400, "لا يمكن ترحيل هذا الشيك")

    ck_date = ck["check_date"]
    amount  = Decimal(str(ck["amount"]))
    bank_gl = ck["bank_gl"] or "11010201"
    payee_gl = ck["gl_account_code"]
    if not payee_gl:
        raise HTTPException(400, "يجب تحديد الحساب المقابل قبل الترحيل")

    # القيد المحاسبي: مدين الحساب المقابل ← دائن البنك
    # نُمرّر party_id + party_role للسطر المرتبط بالمتعامل (السطر المدين)
    je = await _post_je(db, tid, user.email, "CHK", ck_date,
        description="شيك " + ("صادر" if ck.get("check_type")=="outgoing" else "وارد") + " " + str(ck["check_number"]) + " — " + (ck["payee_name"] or ""),
        lines=[
            {
                "account_code": payee_gl,
                "debit": float(amount),
                "credit": 0,
                "description": ck["description"] or "",
                "party_id":   str(ck["party_id"]) if ck.get("party_id") else None,
                "party_role": ck.get("party_role"),
            },
            {
                "account_code": bank_gl,
                "debit": 0,
                "credit": float(amount),
                "description": ck["description"] or "",
            },
        ]
    )
    await db.execute(text("""
        UPDATE tr_checks
        SET status='posted', posted_by=:by, posted_at=NOW(),
            je_id=:je_id, je_serial=:je_serial
        WHERE id=:id AND tenant_id=:tid
    """), {"by": user.email, "je_id": je["je_id"], "je_serial": je["je_serial"],
           "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(data={"je_serial": je["je_serial"]}, message="تم الترحيل — " + je["je_serial"])


@router.post("/checks/{ck_id}/clear")
async def clear_check(ck_id: uuid.UUID, body: dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    """تسوية الشيك — يُظهر في التسوية البنكية"""
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT status FROM tr_checks WHERE id=:id AND tenant_id=:tid"), {"id":str(ck_id),"tid":tid})
    row = r.fetchone()
    if not row: raise HTTPException(404, "غير موجود")
    if row[0] != "posted": raise HTTPException(400, "يجب ترحيل الشيك قبل التسوية")
    await db.execute(text("""
        UPDATE tr_checks
        SET status='cleared', cleared_at=NOW(), cleared_by=:by, cleared_reference=:ref
        WHERE id=:id AND tenant_id=:tid
    """), {"by": user.email, "ref": body.get("reference",""), "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(message="تم التسوية")


@router.post("/checks/{ck_id}/return")
async def return_check(ck_id: uuid.UUID, body: dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_checks
        SET status='returned', returned_at=NOW(), return_reason=:reason
        WHERE id=:id AND tenant_id=:tid
    """), {"reason": body.get("reason",""), "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(message="تم تسجيل إعادة الشيك")


# ══════════════════════════════════════════════════════════
# CHEQUE BOOKS — جداول الشيكات
# ══════════════════════════════════════════════════════════
@router.put("/checks/{ck_id}/status")
async def update_check_status(
    ck_id: uuid.UUID,
    status: str = Query(...),
    notes: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """Legacy endpoint للتوافق مع الكود القديم"""
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_checks SET status=:status, notes=:notes, updated_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"status": status, "notes": notes, "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(message="تم التحديث")


# ══════════════════════════════════════════════════════════
# BANK RECONCILIATION
# ══════════════════════════════════════════════════════════
@router.get("/reconciliation/sessions")
async def list_reconciliation_sessions(
    bank_account_id: Optional[uuid.UUID] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["rs.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if bank_account_id: conds.append("rs.bank_account_id=:ba"); params["ba"] = str(bank_account_id)
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT rs.*, ba.account_name AS bank_account_name
        FROM tr_reconciliation_sessions rs
        LEFT JOIN tr_bank_accounts ba ON ba.id=rs.bank_account_id
        WHERE {where}
        ORDER BY rs.statement_date DESC
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/reconciliation/sessions", status_code=201)
async def create_reconciliation_session(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    stmt_date = date.fromisoformat(str(data["statement_date"]))
    serial = await _next_serial(db, tid, "REC", stmt_date)
    sess_id = str(uuid.uuid4())

    # رصيد الدفتر الحالي
    r = await db.execute(text("""
        SELECT current_balance FROM tr_bank_accounts
        WHERE id=:ba_id AND tenant_id=:tid
    """), {"ba_id": str(data["bank_account_id"]), "tid": tid})
    row = r.fetchone()
    book_balance = Decimal(str(row[0])) if row else Decimal(0)
    stmt_balance = Decimal(str(data["statement_balance"]))
    diff = stmt_balance - book_balance

    await db.execute(text("""
        INSERT INTO tr_reconciliation_sessions
          (id,tenant_id,serial,bank_account_id,statement_date,
           statement_balance,book_balance,difference,status,notes,created_by)
        VALUES
          (:id,:tid,:serial,:ba_id,:stmt_date,
           :stmt_bal,:book_bal,:diff,'open',:notes,:by)
    """), {
        "id": sess_id, "tid": tid, "serial": serial,
        "ba_id": str(data["bank_account_id"]),
        "stmt_date": stmt_date, "stmt_bal": stmt_balance,
        "book_bal": book_balance, "diff": diff,
        "notes": data.get("notes"), "by": user.email,
    })
    await db.commit()
    return created(data={"id": sess_id, "serial": serial, "difference": float(diff)},
                   message=f"تم إنشاء جلسة التسوية {serial}")


@router.post("/reconciliation/sessions/{sess_id}/import-lines")
async def import_statement_lines(
    sess_id: uuid.UUID,
    lines: list,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """استيراد أسطر كشف حساب البنك"""
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM tr_reconciliation_sessions WHERE id=:id AND tenant_id=:tid"),
                          {"id": str(sess_id), "tid": tid})
    sess = r.mappings().fetchone()
    if not sess: raise Exception("الجلسة غير موجودة")

    inserted = 0
    for line in lines:
        await db.execute(text("""
            INSERT INTO tr_bank_statement_lines
              (id,tenant_id,session_id,bank_account_id,line_date,
               description,reference,debit,credit,running_balance)
            VALUES
              (gen_random_uuid(),:tid,:sess_id,:ba_id,:line_date,
               :desc,:ref,:debit,:credit,:balance)
        """), {
            "tid": tid, "sess_id": str(sess_id),
            "ba_id": str(sess["bank_account_id"]),
            "line_date": date.fromisoformat(str(line["date"])),
            "desc": line.get("description",""),
            "ref": line.get("reference"),
            "debit": Decimal(str(line.get("debit",0))),
            "credit": Decimal(str(line.get("credit",0))),
            "balance": line.get("running_balance"),
        })
        inserted += 1

    await db.commit()
    return ok(data={"inserted": inserted}, message=f"تم استيراد {inserted} سطر ✅")


@router.post("/reconciliation/sessions/{sess_id}/match")
async def match_transaction(
    sess_id: uuid.UUID,
    statement_line_id: uuid.UUID = Query(...),
    tx_id: uuid.UUID = Query(...),
    tx_type: str = Query(...),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """مطابقة سطر كشف البنك مع حركة في النظام"""
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_bank_statement_lines
        SET match_status='matched', matched_tx_id=:tx_id, matched_tx_type=:tx_type
        WHERE id=:line_id AND session_id=:sess_id AND tenant_id=:tid
    """), {"tx_id": str(tx_id), "tx_type": tx_type,
           "line_id": str(statement_line_id), "sess_id": str(sess_id), "tid": tid})

    # تحديث حالة الحركة
    tbl = "tr_bank_transactions" if tx_type in ("BP","BR","BT") else "tr_cash_transactions"
    await db.execute(text(f"""
        UPDATE {tbl} SET is_reconciled=true, reconciled_at=NOW()
        WHERE id=:tx_id AND tenant_id=:tid
    """), {"tx_id": str(tx_id), "tid": tid})

    await db.commit()
    return ok(data={}, message="✅ تمت المطابقة")


@router.post("/transactions/{tx_id}/reconcile")
async def toggle_reconcile_tx(
    tx_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تبديل حالة التسوية لأي حركة (بنكية، نقدية، تحويل داخلي)"""
    tid = str(user.tenant_id)
    is_rec = bool(data.get("is_reconciled", True))
    tx_id_str = str(tx_id)

    # نبحث في الجداول الثلاثة
    tables = [
        "tr_bank_transactions",
        "tr_cash_transactions",
        "tr_internal_transfers",
    ]
    updated = False
    for tbl in tables:
        try:
            r = await db.execute(text(f"""
                UPDATE {tbl}
                SET is_reconciled = :rec,
                    reconciled_at = CASE WHEN :rec THEN NOW() ELSE NULL END
                WHERE id = :id AND tenant_id = :tid
            """), {"rec": is_rec, "id": tx_id_str, "tid": tid})
            if r.rowcount > 0:
                updated = True
                break
        except Exception:
            continue

    if not updated:
        await db.rollback()
        raise HTTPException(400, "لم يتم العثور على الحركة أو عمود is_reconciled غير موجود — شغّل migration التسوية")

    await db.commit()
    status = "مُسوَّى" if is_rec else "غير مُسوَّى"
    return ok(data={"updated": True, "is_reconciled": is_rec}, message=status)


@router.get("/reconciliation/sessions/{sess_id}/lines")
async def get_session_lines(
    sess_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT * FROM tr_bank_statement_lines
        WHERE session_id=:sess_id AND tenant_id=:tid
        ORDER BY line_date, id
    """), {"sess_id": str(sess_id), "tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


# ══════════════════════════════════════════════════════════
# EXPENSE CLASSIFICATIONS (تصنيفات المصاريف)
# ══════════════════════════════════════════════════════════
@router.get("/settings/expense-classifications")
async def list_expense_classifications(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT id, code, name_ar, name_en, description, is_active, created_at
            FROM settings_expense_classifications
            WHERE tenant_id = :tid
            ORDER BY code
        """), {"tid": tid})
        rows = [dict(row) for row in r.mappings().fetchall()]
        return ok(data=rows)
    except Exception:
        # جدول غير موجود بعد — أرجع قائمة افتراضية
        defaults = [
            {"id":"1","code":"ADM","name_ar":"مصاريف إدارية وعمومية","name_en":"Admin & General","is_active":True},
            {"id":"2","code":"MKT","name_ar":"مصاريف تسويقية","name_en":"Marketing","is_active":True},
            {"id":"3","code":"OPS","name_ar":"مصاريف تشغيلية","name_en":"Operational","is_active":True},
            {"id":"4","code":"TRV","name_ar":"مصاريف سفر وانتقالات","name_en":"Travel","is_active":True},
            {"id":"5","code":"ENT","name_ar":"مصاريف ضيافة واستضافة","name_en":"Entertainment","is_active":True},
            {"id":"6","code":"MNT","name_ar":"مصاريف صيانة","name_en":"Maintenance","is_active":True},
            {"id":"7","code":"TRN","name_ar":"مصاريف تدريب وتطوير","name_en":"Training","is_active":True},
            {"id":"8","code":"OTH","name_ar":"مصاريف أخرى","name_en":"Other","is_active":True},
        ]
        return ok(data=defaults)


@router.post("/settings/expense-classifications", status_code=201)
async def create_expense_classification(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        new_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO settings_expense_classifications
              (id, tenant_id, code, name_ar, name_en, description, is_active)
            VALUES (:id, :tid, :code, :name_ar, :name_en, :desc, true)
        """), {
            "id": new_id, "tid": tid,
            "code": data.get("code","").upper(),
            "name_ar": data["name_ar"],
            "name_en": data.get("name_en",""),
            "desc": data.get("description",""),
        })
        await db.commit()
        return created(data={"id": new_id}, message="تم الإنشاء ✅")
    except Exception as e:
        raise HTTPException(400, str(e))


# ══════════════════════════════════════════════════════════
# PETTY CASH FUNDS
# ══════════════════════════════════════════════════════════
@router.get("/petty-cash/funds")
async def list_petty_cash_funds(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT f.*,
               ba.account_name AS bank_account_name,
               CASE WHEN f.current_balance <= (f.limit_amount * f.replenish_threshold / 100)
                    THEN true ELSE false END AS needs_replenishment,
               ROUND(f.current_balance / NULLIF(f.limit_amount,0) * 100, 1) AS balance_pct
        FROM tr_petty_cash_funds f
        LEFT JOIN tr_bank_accounts ba ON ba.id=f.bank_account_id
        WHERE f.tenant_id=:tid
        ORDER BY f.fund_name
    """), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/petty-cash/funds", status_code=201)
async def create_petty_cash_fund(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid     = str(user.tenant_id)
    fund_id = str(uuid.uuid4())
    try:
        await db.execute(text("""
            INSERT INTO tr_petty_cash_funds
              (id,tenant_id,fund_code,fund_name,fund_type,
               custodian_name,custodian_email,custodian_phone,custodian_party_id,
               currency_code,limit_amount,current_balance,gl_account_code,
               bank_account_id,branch_code,replenish_threshold,
               require_daily_close,notes,is_active,created_by)
            VALUES
              (:id,:tid,:code,:name,:fund_type,
               :custodian,:custodian_email,:custodian_phone,:custodian_party_id,
               :cur,:limit,:balance,:gl,
               :ba_id,:branch,:threshold,
               :daily_close,:notes,true,:by)
        """), {
            "id":          fund_id,
            "tid":         tid,
            "code":        data["fund_code"],
            "name":        data["fund_name"],
            "fund_type":   data.get("fund_type") or "main",
            "custodian":   data.get("custodian_name"),
            "custodian_email": data.get("custodian_email"),
            "custodian_phone": data.get("custodian_phone"),
            "custodian_party_id": data.get("custodian_party_id") or None,
            "cur":         data.get("currency_code","SAR"),
            "limit":       Decimal(str(data["limit_amount"])),
            "balance":     Decimal(str(data.get("opening_balance",0))),
            "gl":          data["gl_account_code"],
            "ba_id":       str(data["bank_account_id"]) if data.get("bank_account_id") else None,
            "branch":      data.get("branch_code"),
            "threshold":   data.get("replenish_threshold", 20),
            "daily_close": bool(data.get("require_daily_close", False)),
            "notes":       data.get("notes"),
            "by":          user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الحفظ: {str(e)}")
    return created(data={"id": fund_id}, message="تم إنشاء صندوق العهدة ✅")


@router.put("/petty-cash/funds/{fund_id}")
async def update_petty_cash_fund(
    fund_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ALLOWED_PF = {
        "fund_code","fund_name","fund_type","custodian_name","custodian_email",
        "custodian_phone","custodian_party_id","currency_code","limit_amount","gl_account_code",
        "bank_account_id","branch_code","replenish_threshold","notes",
        "is_active","require_daily_close",
        "deactivated_at","deactivation_reason","deactivated_by",
    }
    safe = {k:v for k,v in data.items() if k in ALLOWED_PF}
    if not safe:
        raise HTTPException(400, "لا توجد بيانات للتعديل")
    set_clause = ", ".join([f"{k}=:{k}" for k in safe.keys()])
    safe.update({"id": str(fund_id), "tid": tid})
    data = safe
    await db.execute(text(f"UPDATE tr_petty_cash_funds SET {set_clause} WHERE id=:id AND tenant_id=:tid"), data)
    await db.commit()
    return ok(data={}, message="تم التعديل ✅")


# ══════════════════════════════════════════════════════════
# PETTY CASH EXPENSES (PET)
# ══════════════════════════════════════════════════════════
@router.get("/petty-cash/expenses")
async def list_petty_cash_expenses(
    fund_id:   Optional[uuid.UUID] = Query(None),
    status:    Optional[str]       = Query(None),
    date_from: Optional[date]      = Query(None),
    date_to:   Optional[date]      = Query(None),
    limit: int = Query(50), offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["pe.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}
    if fund_id:  conds.append("pe.fund_id=:fund");   params["fund"] = str(fund_id)
    if status:   conds.append("pe.status=:status");  params["status"] = status
    if date_from:conds.append("pe.expense_date>=:df");params["df"] = str(date_from)
    if date_to:  conds.append("pe.expense_date<=:dt");params["dt"] = str(date_to)
    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM tr_petty_cash_expenses pe WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT pe.*, f.fund_name
        FROM tr_petty_cash_expenses pe
        LEFT JOIN tr_petty_cash_funds f ON f.id=pe.fund_id
        WHERE {where}
        ORDER BY pe.expense_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    items = [dict(row._mapping) for row in r.fetchall()]

    # جلب الأسطر لكل مصروف
    for item in items:
        lr = await db.execute(text("""
            SELECT * FROM tr_petty_cash_expense_lines
            WHERE expense_id=:eid ORDER BY line_order
        """), {"eid": item["id"]})
        item["lines"] = [dict(row._mapping) for row in lr.fetchall()]

    return ok(data={"total": cnt.scalar(), "items": items})


@router.post("/petty-cash/expenses", status_code=201)
async def create_petty_cash_expense(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    exp_date = date.fromisoformat(str(data["expense_date"]))
    serial = await _next_serial(db, tid, "PCR", exp_date)
    exp_id = str(uuid.uuid4())
    lines = data.get("lines", [])
    total = sum(Decimal(str(l["amount"])) for l in lines if float(l.get("amount",0)) > 0)
    vat_total = sum(Decimal(str(l.get("vat_amount",0))) for l in lines)

    # أولاً: حاول مع party_id و party_name
    try:
        await db.execute(text("""
            INSERT INTO tr_petty_cash_expenses
              (id,tenant_id,serial,fund_id,expense_date,total_amount,vat_total,
               description,reference,notes,party_id,party_name,status,created_by)
            VALUES
              (:id,:tid,:serial,:fund_id,:exp_date,:total,:vat,
               :desc,:ref,:notes,:party_id,:party_name,'draft',:by)
        """), {
            "id": exp_id, "tid": tid, "serial": serial,
            "fund_id": str(data["fund_id"]),
            "exp_date": exp_date, "total": total, "vat": vat_total,
            "desc": data["description"], "ref": data.get("reference"),
            "notes": data.get("notes"), "by": user.email,
            "party_id": data.get("party_id") or None,
            "party_name": data.get("party_name") or None,
        })
    except Exception:
        # fallback بدون party إذا لم تكن الأعمدة موجودة بعد
        await db.execute(text("""
            INSERT INTO tr_petty_cash_expenses
              (id,tenant_id,serial,fund_id,expense_date,total_amount,vat_total,
               description,reference,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:fund_id,:exp_date,:total,:vat,
               :desc,:ref,:notes,'draft',:by)
        """), {
            "id": exp_id, "tid": tid, "serial": serial,
            "fund_id": str(data["fund_id"]),
            "exp_date": exp_date, "total": total, "vat": vat_total,
            "desc": data["description"], "ref": data.get("reference"),
            "notes": data.get("notes"), "by": user.email,
        })

    for i, line in enumerate(lines):
        if not line.get("expense_account") or float(line.get("amount",0)) <= 0:
            continue
        try:
            await db.execute(text("""
                INSERT INTO tr_petty_cash_expense_lines
                  (id,tenant_id,expense_id,line_order,expense_account,expense_account_name,
                   description,amount,vat_amount,net_amount,vendor_name,
                   branch_code,cost_center,project_code,expense_classification_code)
                VALUES
                  (gen_random_uuid(),:tid,:exp_id,:order,:acc,:acc_name,
                   :desc,:amount,:vat,:net,:vendor,
                   :branch,:cc,:proj,:exp_cls)
            """), {
                "tid": tid, "exp_id": exp_id, "order": i+1,
                "acc":      line["expense_account"],
                "acc_name": line.get("expense_account_name",""),
                "desc":     line.get("description"),
                "amount":   Decimal(str(line["amount"])),
                "vat":      Decimal(str(line.get("vat_amount",0))),
                "net":      Decimal(str(line.get("net_amount", line["amount"]))),
                "vendor":   line.get("vendor_name"),
                "branch":   line.get("branch_code") or None,
                "cc":       line.get("cost_center") or None,
                "proj":     line.get("project_code") or None,
                "exp_cls":  line.get("expense_classification_code") or None,
            })
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"line insert failed: {e}")

    await db.commit()
    return created(data={"id": exp_id, "serial": serial, "total": float(total)},
                   message="تم انشاء " + serial + " بنجاح")


@router.put("/petty-cash/expenses/{exp_id}", summary="تعديل مصروف نثري مسودة")
async def update_petty_cash_expense(
    exp_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    # التحقق من أن المصروف مسودة وينتمي لنفس الـ tenant
    r = await db.execute(text("""
        SELECT id, status FROM tr_petty_cash_expenses
        WHERE id=:id AND tenant_id=:tid
    """), {"id": str(exp_id), "tid": tid})
    exp = r.mappings().fetchone()
    if not exp:
        raise HTTPException(404, "المصروف غير موجود")
    if exp["status"] != "draft":
        raise HTTPException(400, "لا يمكن تعديل مصروف تم ترحيله")

    lines = data.get("lines", [])
    total = sum(Decimal(str(l["amount"])) for l in lines if float(l.get("amount",0)) > 0)
    vat_total = sum(Decimal(str(l.get("vat_amount",0))) for l in lines)

    # تحديث بيانات المصروف — نحاول مع party ثم بدونه إذا لم تكن الأعمدة موجودة
    try:
        await db.execute(text("""
            UPDATE tr_petty_cash_expenses
            SET fund_id=:fund_id, expense_date=:exp_date,
                total_amount=:total, vat_total=:vat,
                description=:desc, reference=:ref, notes=:notes,
                party_id=:party_id, party_name=:party_name
            WHERE id=:id AND tenant_id=:tid
        """), {
            "id": str(exp_id), "tid": tid,
            "fund_id": str(data["fund_id"]),
            "exp_date": date.fromisoformat(str(data["expense_date"])),
            "total": total, "vat": vat_total,
            "desc": data["description"],
            "ref": data.get("reference"),
            "notes": data.get("notes"),
            "party_id": data.get("party_id") or None,
            "party_name": data.get("party_name") or None,
        })
    except Exception:
        # fallback بدون party إذا لم تكن الأعمدة موجودة
        await db.execute(text("""
            UPDATE tr_petty_cash_expenses
            SET fund_id=:fund_id, expense_date=:exp_date,
                total_amount=:total, vat_total=:vat,
                description=:desc, reference=:ref, notes=:notes
            WHERE id=:id AND tenant_id=:tid
        """), {
            "id": str(exp_id), "tid": tid,
            "fund_id": str(data["fund_id"]),
            "exp_date": date.fromisoformat(str(data["expense_date"])),
            "total": total, "vat": vat_total,
            "desc": data["description"],
            "ref": data.get("reference"),
            "notes": data.get("notes"),
        })

    # حذف السطور القديمة وإعادة إدراجها
    await db.execute(text(
        "DELETE FROM tr_petty_cash_expense_lines WHERE expense_id=:id AND tenant_id=:tid"
    ), {"id": str(exp_id), "tid": tid})

    for i, line in enumerate(lines):
        if not line.get("expense_account") or float(line.get("amount",0)) <= 0:
            continue
        await db.execute(text("""
            INSERT INTO tr_petty_cash_expense_lines
              (id,tenant_id,expense_id,line_order,expense_account,expense_account_name,
               description,amount,vat_amount,net_amount,vendor_name,
               branch_code,cost_center,project_code,expense_classification_code)
            VALUES
              (gen_random_uuid(),:tid,:exp_id,:order,:acc,:acc_name,
               :desc,:amount,:vat,:net,:vendor,
               :branch,:cc,:proj,:exp_cls)
        """), {
            "tid": tid, "exp_id": str(exp_id), "order": i+1,
            "acc": line["expense_account"],
            "acc_name": line.get("expense_account_name",""),
            "desc": line.get("description"),
            "amount": Decimal(str(line["amount"])),
            "vat": Decimal(str(line.get("vat_amount",0))),
            "net": Decimal(str(line.get("net_amount", line["amount"]))),
            "vendor": line.get("vendor_name"),
            "branch": line.get("branch_code"),
            "cc": line.get("cost_center"),
            "proj": line.get("project_code"),
            "exp_cls": line.get("expense_classification_code"),
        })

    await db.commit()
    return ok(data={"id": str(exp_id), "total": float(total)},
              message="تم تحديث المصروف ✅")


@router.post("/petty-cash/expenses/{exp_id}/submit", summary="إرسال للمراجعة")
async def submit_petty_cash_expense(
    exp_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text(
        "SELECT status FROM tr_petty_cash_expenses WHERE id=:id AND tenant_id=:tid"
    ), {"id": str(exp_id), "tid": tid})
    row = r.fetchone()
    if not row: raise HTTPException(404, "غير موجود")
    if row[0] != "draft": raise HTTPException(400, "يجب أن يكون المصروف في حالة مسودة")
    await db.execute(text("""
        UPDATE tr_petty_cash_expenses
        SET status='review', submitted_by=:by, submitted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"by": user.email, "id": str(exp_id), "tid": tid})
    await db.commit()
    return ok(message="تم الإرسال للمراجعة ✅")


@router.post("/petty-cash/expenses/{exp_id}/approve", summary="اعتماد المصروف")
async def approve_petty_cash_expense(
    exp_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text(
        "SELECT status FROM tr_petty_cash_expenses WHERE id=:id AND tenant_id=:tid"
    ), {"id": str(exp_id), "tid": tid})
    row = r.fetchone()
    if not row: raise HTTPException(404, "غير موجود")
    if row[0] not in ("review", "draft"): raise HTTPException(400, "لا يمكن اعتماد هذا المصروف")
    await db.execute(text("""
        UPDATE tr_petty_cash_expenses
        SET status='approved', approved_by=:by, approved_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"by": user.email, "id": str(exp_id), "tid": tid})
    await db.commit()
    return ok(message="تم الاعتماد ✅")


@router.post("/petty-cash/expenses/{exp_id}/reject", summary="رفض المصروف")
async def reject_petty_cash_expense(
    exp_id: uuid.UUID,
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_petty_cash_expenses
        SET status='rejected', rejected_by=:by, rejected_at=NOW(), rejection_reason=:reason
        WHERE id=:id AND tenant_id=:tid
    """), {"by": user.email, "id": str(exp_id), "tid": tid,
           "reason": body.get("reason", "")})
    await db.commit()
    return ok(message="تم الرفض")


@router.post("/petty-cash/expenses/{exp_id}/post")
async def post_petty_cash_expense(
    exp_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT pe.*, f.gl_account_code AS fund_gl
        FROM tr_petty_cash_expenses pe
        JOIN tr_petty_cash_funds f ON f.id=pe.fund_id
        WHERE pe.id=:id AND pe.tenant_id=:tid
    """), {"id": str(exp_id), "tid": tid})
    exp = r.mappings().fetchone()
    if not exp: raise Exception("المصروف غير موجود")
    if exp["status"] not in ("draft", "review", "approved"): raise Exception("لا يمكن ترحيل هذا المصروف — الحالة: " + str(exp["status"]))

    lr = await db.execute(text("""
        SELECT * FROM tr_petty_cash_expense_lines
        WHERE expense_id=:id ORDER BY line_order
    """), {"id": str(exp_id)})
    exp_lines = lr.mappings().all()

    # بناء القيد: DR مصاريف متعددة, CR حساب العهدة
    # party_id من أمين صندوق العهدة إن وُجد
    fund_party_id   = str(exp.get("party_id"))   if exp.get("party_id")   else (
                      str(exp.get("fund_custodian_party_id")) if exp.get("fund_custodian_party_id") else None)
    fund_party_role = exp.get("party_role") or "petty_cash_keeper"

    lines = [{"account_code": l["expense_account"],
               "debit": Decimal(str(l["amount"])),
               "credit": 0,
               "description": l.get("description") or exp["description"],
               "branch_code": l.get("branch_code"),
               "cost_center": l.get("cost_center"),
               "project_code": l.get("project_code"),
               "party_id":   fund_party_id,
               "party_role": fund_party_role,
               }
             for l in exp_lines]
    total_amt = Decimal(str(exp["total_amount"]))
    lines.append({"account_code": exp["fund_gl"], "debit": 0, "credit": total_amt,
                   "description": exp["description"]})

    je = await _post_je(db, tid, user.email, "PET", exp["expense_date"],
                        exp["description"], lines, exp["reference"])
    if not je.get("je_id"):
        raise HTTPException(500, "فشل إنشاء قيد العهدة")

    # خصم من رصيد العهدة
    await db.execute(text("""
        UPDATE tr_petty_cash_funds
        SET current_balance = current_balance - :amt
        WHERE id=:fund_id AND tenant_id=:tid
    """), {"amt": total_amt, "fund_id": str(exp["fund_id"]), "tid": tid})

    await db.execute(text("""
        UPDATE tr_petty_cash_expenses
        SET status='posted', je_id=:je_id, je_serial=:je_serial,
            posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id": je["je_id"], "je_serial": je["je_serial"],
           "by": user.email, "id": str(exp_id), "tid": tid})
    await db.commit()
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# PETTY CASH REPLENISHMENTS
# ══════════════════════════════════════════════════════════
@router.post("/petty-cash/replenishments", status_code=201)
async def create_replenishment(
    fund_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT f.*,
               COALESCE(SUM(pe.total_amount),0) AS unreplenished
        FROM tr_petty_cash_funds f
        LEFT JOIN tr_petty_cash_expenses pe ON pe.fund_id=f.id
          AND pe.status='posted'
          AND pe.id NOT IN (SELECT DISTINCT UNNEST(ARRAY[linked_payment_id]::UUID[])
                            FROM tr_petty_cash_replenishments
                            WHERE fund_id=f.id AND status='paid'
                            AND linked_payment_id IS NOT NULL)
        WHERE f.id=:fund_id AND f.tenant_id=:tid
        GROUP BY f.id
    """), {"fund_id": str(fund_id), "tid": tid})
    fund = r.mappings().fetchone()
    if not fund: raise Exception("الصندوق غير موجود")

    spent = Decimal(str(fund["limit_amount"])) - Decimal(str(fund["current_balance"]))
    if spent <= 0: raise Exception("لا توجد مصاريف تحتاج تعبئة")

    serial = await _next_serial(db, tid, "PCR", date.today())
    rep_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO tr_petty_cash_replenishments
          (id,tenant_id,serial,fund_id,replenishment_date,amount,status,created_by)
        VALUES
          (:id,:tid,:serial,:fund_id,:rep_date,:amount,'pending',:by)
    """), {"id": rep_id, "tid": tid, "serial": serial, "fund_id": str(fund_id),
           "rep_date": date.today(), "amount": spent, "by": user.email})
    await db.commit()
    return created(data={"id": rep_id, "serial": serial, "amount": float(spent)},
                   message=f"تم إنشاء طلب التعبئة {serial} — {float(spent):,.3f}")


@router.get("/petty-cash/replenishments")
async def list_replenishments(
    fund_id: Optional[uuid.UUID] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["r.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if fund_id: conds.append("r.fund_id=:fund"); params["fund"] = str(fund_id)
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT r.*, f.fund_name
        FROM tr_petty_cash_replenishments r
        LEFT JOIN tr_petty_cash_funds f ON f.id=r.fund_id
        WHERE {where}
        ORDER BY r.replenishment_date DESC
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


# ══════════════════════════════════════════════════════════
# PETTY CASH COUNT
# ══════════════════════════════════════════════════════════
@router.post("/petty-cash/counts", status_code=201)
async def create_petty_cash_count(
    fund_id:        uuid.UUID = Query(...),
    actual_balance: float     = Query(...),
    notes:          Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT current_balance FROM tr_petty_cash_funds WHERE id=:id AND tenant_id=:tid"),
                          {"id": str(fund_id), "tid": tid})
    fund = r.fetchone()
    if not fund: raise Exception("الصندوق غير موجود")

    count_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO tr_petty_cash_counts
          (id,tenant_id,fund_id,count_date,system_balance,actual_balance,notes,counted_by)
        VALUES
          (:id,:tid,:fund_id,:count_date,:sys_bal,:actual,:notes,:by)
    """), {
        "id": count_id, "tid": tid, "fund_id": str(fund_id),
        "count_date": date.today(),
        "sys_bal": Decimal(str(fund[0])),
        "actual": Decimal(str(actual_balance)),
        "notes": notes, "by": user.email,
    })
    await db.commit()
    variance = float(actual_balance) - float(fund[0])
    return created(data={"variance": variance}, message=f"تم تسجيل الجرد — الفرق: {variance:+.3f}")


# ══════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════
@router.get("/reports/cash-position")
async def cash_position_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT account_code, account_name, account_type,
               currency_code, current_balance, low_balance_alert,
               CASE WHEN low_balance_alert IS NOT NULL AND current_balance <= low_balance_alert THEN true ELSE false END AS is_low
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY account_type, account_name
    """), {"tid": tid})
    accounts = []
    for row in r.fetchall():
        a = dict(row._mapping)
        a["current_balance"] = float(a.get("current_balance") or 0)
        a["low_balance_alert"] = float(a.get("low_balance_alert") or 0)
        accounts.append(a)
    total = sum(a["current_balance"] for a in accounts)
    return ok(data={"total": total, "accounts": accounts, "as_of": date.today().isoformat()})


@router.get("/reports/outstanding-checks")
async def outstanding_checks_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ck.*, ba.account_name AS bank_name,
               (CURRENT_DATE - ck.due_date) AS overdue_days
        FROM tr_checks ck
        LEFT JOIN tr_bank_accounts ba ON ba.id=ck.bank_account_id
        WHERE ck.tenant_id=:tid AND ck.status='issued'
        ORDER BY ck.due_date
    """), {"tid": tid})
    checks = [dict(row._mapping) for row in r.fetchall()]
    total = sum(float(c["amount"]) for c in checks)
    overdue = [c for c in checks if c["overdue_days"] and c["overdue_days"] > 0]
    return ok(data={"total": total, "count": len(checks), "overdue": len(overdue), "checks": checks})


@router.get("/reports/check-aging")
async def check_aging(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """أعمار الشيكات المستحقة"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
            ch.*,
            ba.account_name AS bank_account_name,
            (CURRENT_DATE - ch.due_date) AS days_overdue,
            CASE
                WHEN ch.due_date IS NULL THEN 'unknown'
                WHEN ch.due_date >= CURRENT_DATE THEN 'upcoming'
                WHEN (CURRENT_DATE - ch.due_date) <= 30 THEN '0-30'
                WHEN (CURRENT_DATE - ch.due_date) <= 60 THEN '31-60'
                WHEN (CURRENT_DATE - ch.due_date) <= 90 THEN '61-90'
                ELSE 'over-90'
            END AS aging_bucket
        FROM tr_checks ch
        LEFT JOIN tr_bank_accounts ba ON ba.id=ch.bank_account_id
        WHERE ch.tenant_id=:tid AND ch.status IN ('issued','deposited')
        ORDER BY ch.due_date
    """), {"tid": tid})
    rows = [dict(r._mapping) for r in r.fetchall()]
    for row in rows:
        if row.get("amount"):    row["amount"]       = float(row["amount"])
        if row.get("due_date"):  row["due_date"]     = str(row["due_date"])
        if row.get("days_overdue"): row["days_overdue"] = int(row["days_overdue"])
    return ok(data=rows)



@router.get("/reports/petty-cash-statement")
async def petty_cash_statement(
    fund_id:   Optional[uuid.UUID] = Query(None),
    date_from: Optional[date]      = Query(None),
    date_to:   Optional[date]      = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["pe.tenant_id=:tid AND pe.status='posted'"]
    params: dict = {"tid": tid}
    if fund_id:  conds.append("pe.fund_id=:fund");    params["fund"] = str(fund_id)
    if date_from:conds.append("pe.expense_date>=:df"); params["df"] = str(date_from)
    if date_to:  conds.append("pe.expense_date<=:dt"); params["dt"] = str(date_to)
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT pe.*, f.fund_name, f.custodian_name
        FROM tr_petty_cash_expenses pe
        JOIN tr_petty_cash_funds f ON f.id=pe.fund_id
        WHERE {where}
        ORDER BY pe.expense_date DESC
    """), params)
    items = [dict(row._mapping) for row in r.fetchall()]
    total = sum(float(i["total_amount"]) for i in items)
    return ok(data={"total": total, "count": len(items), "items": items})

@router.get("/reports/gl-balance-check")
async def gl_balance_check(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    مقارنة رصيد الخزينة مع رصيد الأستاذ العام لكل حساب
    يكشف الفروق بين tr_bank_accounts.current_balance و account_balances في GL
    """
    tid = str(user.tenant_id)

    # نجلب أرصدة الخزينة مع حسابات GL المقابلة
    r = await db.execute(text("""
        SELECT
            ba.id,
            ba.account_name,
            ba.account_code,
            ba.account_type,
            ba.gl_account_code,
            ba.current_balance                              AS treasury_balance,
            COALESCE(
                (SELECT SUM(jl.debit) - SUM(jl.credit)
                 FROM je_lines jl
                 JOIN journal_entries je ON je.id = jl.journal_entry_id
                 WHERE jl.account_code  = ba.gl_account_code
                   AND je.tenant_id     = ba.tenant_id
                   AND je.status        = 'posted'),
                0
            )                                               AS gl_balance,
            ABS(ba.current_balance - COALESCE(
                (SELECT SUM(jl.debit) - SUM(jl.credit)
                 FROM je_lines jl
                 JOIN journal_entries je ON je.id = jl.journal_entry_id
                 WHERE jl.account_code  = ba.gl_account_code
                   AND je.tenant_id     = ba.tenant_id
                   AND je.status        = 'posted'),
                0
            ))                                              AS diff,
            CASE
                WHEN ABS(ba.current_balance - COALESCE(
                    (SELECT SUM(jl.debit) - SUM(jl.credit)
                     FROM je_lines jl
                     JOIN journal_entries je ON je.id = jl.journal_entry_id
                     WHERE jl.account_code  = ba.gl_account_code
                       AND je.tenant_id     = ba.tenant_id
                       AND je.status        = 'posted'),
                    0
                )) < 0.01
                THEN 'matched'
                ELSE 'mismatch'
            END                                             AS status
        FROM tr_bank_accounts ba
        WHERE ba.tenant_id = :tid AND ba.is_active = true
        ORDER BY ba.account_type, ba.account_name
    """), {"tid": tid})

    rows = [dict(row._mapping) for row in r.fetchall()]
    # تحويل Decimal إلى float
    for row in rows:
        for k in ("treasury_balance","gl_balance","diff"):
            if row.get(k) is not None:
                row[k] = float(row[k])

    mismatches  = [r for r in rows if r["status"] == "mismatch"]
    total_diff  = sum(r["diff"] for r in mismatches)

    return ok(data={
        "accounts":       rows,
        "total_accounts": len(rows),
        "mismatches":     len(mismatches),
        "total_diff":     round(total_diff, 3),
        "all_matched":    len(mismatches) == 0,
    })

# ══════════════════════════════════════════════════════════
# CASH WORKFLOW — submit / approve / reject / reverse / bulk
# ══════════════════════════════════════════════════════════

async def _create_notification(db, tid: str, title: str, body: str, ref_type: str = None, ref_id: str = None):
    """ينشئ إشعاراً في جدول notifications — يُستدعى بعد commit"""
    try:
        await db.execute(text("""
            INSERT INTO notifications
                (id, tenant_id, title, body, is_read, ref_type, ref_id, created_at)
            VALUES
                (gen_random_uuid(), :tid, :title, :body, false, :ref_type, :ref_id, NOW())
        """), {"tid": tid, "title": title, "body": body,
               "ref_type": ref_type or "treasury", "ref_id": ref_id})
        await db.commit()
    except Exception as e:
        print(f"[Notification] فشل إنشاء الإشعار: {e}")


@router.post("/cash-transactions/{tx_id}/submit")
async def submit_cash_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        UPDATE tr_cash_transactions
        SET status='pending_approval', submitted_by=:by, submitted_at=NOW()
        WHERE id=:id AND tenant_id=:tid AND status='draft'
        RETURNING id, serial, tx_type, amount
    """), {"id": str(tx_id), "tid": tid, "by": user.email})
    row = r.fetchone()
    if not row: raise HTTPException(400, "السند غير موجود أو ليس مسودة")
    await db.commit()
    # إنشاء إشعار
    tx_label = "سند قبض" if row[2] == "RV" else "سند صرف"
    await _create_notification(
        db, tid,
        title=f"🔔 {tx_label} بانتظار الاعتماد",
        body=f"السند {row[1]} بمبلغ {float(row[3] or 0):,.3f} ر.س — أرسله {user.email} للاعتماد",
        ref_type="cash_transaction",
        ref_id=str(tx_id),
    )
    return ok(data={}, message="تم إرسال السند للاعتماد ✅")


@router.post("/cash-transactions/{tx_id}/approve")
async def approve_cash_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ct.*, ba.gl_account_code FROM tr_cash_transactions ct
        LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
        WHERE ct.id=:id AND ct.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] not in ("pending_approval", "draft"):
        raise HTTPException(400, "لا يمكن اعتماد هذا السند")

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"]
    if not gl: raise HTTPException(400, "حساب الأستاذ العام غير محدد")
    if not cp: raise HTTPException(400, "الحساب المقابل غير محدد")

    base_amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    vat_amt  = Decimal(str(tx.get("vat_amount") or 0))
    vat_acc  = tx.get("vat_account_code") or None
    total    = base_amt + vat_amt
    dims     = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
                "project_code": tx.get("project_code"),
                "expense_classification_code": tx.get("expense_classification_code")}

    if tx["tx_type"] == "RV":
        lines = [{"account_code": gl, "debit": float(total), "credit": 0, "description": tx["description"]},
                 {"account_code": cp, "debit": 0, "credit": float(base_amt), "description": tx["description"], **dims}]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": 0, "credit": float(vat_amt), "description": "ضريبة"})
        delta = total
    else:
        lines = [{"account_code": cp, "debit": float(base_amt), "credit": 0, "description": tx["description"], **dims},
                 {"account_code": gl, "debit": 0, "credit": float(total), "description": tx["description"]}]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": float(vat_amt), "credit": 0, "description": "ضريبة"})
        delta = -total

    try:
        je = await _post_je(db, tid, user.email, tx["tx_type"], tx["tx_date"],
                            tx["description"], lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        await db.execute(text("""
            UPDATE tr_cash_transactions
            SET status='posted', approved_by=:approved_by, approved_at=NOW(),
                je_id=:je_id, je_serial=:je_serial, posted_by=:posted_by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"approved_by": user.email, "posted_by": user.email,
               "je_id": je["je_id"], "je_serial": je["je_serial"],
               "id": str(tx_id), "tid": tid})
        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الاعتماد: {str(e)}")
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الاعتماد والترحيل — {je['je_serial']}")


@router.post("/cash-transactions/{tx_id}/reject")
async def reject_cash_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        UPDATE tr_cash_transactions
        SET status='draft', rejected_by=:by, rejected_at=NOW(), rejection_note=:note
        WHERE id=:id AND tenant_id=:tid AND status='pending_approval'
        RETURNING id
    """), {"id": str(tx_id), "tid": tid, "by": user.email, "note": data.get("note","مرفوض")})
    if not r.fetchone(): raise HTTPException(400, "السند غير موجود أو ليس في انتظار الاعتماد")
    await db.commit()
    return ok(data={}, message="تم رفض السند")


@router.post("/cash-transactions/{tx_id}/reverse")
async def reverse_cash_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ct.*, ba.gl_account_code FROM tr_cash_transactions ct
        LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
        WHERE ct.id=:id AND ct.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] != "posted": raise HTTPException(400, "يمكن عكس المُرحَّل فقط")

    gl   = tx["gl_account_code"]
    cp   = tx["counterpart_account"]
    amt  = Decimal(str(tx["amount_sar"] or tx["amount"]))
    note = data.get("note", f"عكس {tx['serial']}")
    dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
            "project_code": tx.get("project_code"),
            "expense_classification_code": tx.get("expense_classification_code")}
    rev_type = "PV" if tx["tx_type"] == "RV" else "RV"

    if tx["tx_type"] == "RV":
        lines = [{"account_code": cp, "debit": float(amt), "credit": 0, "description": note, **dims},
                 {"account_code": gl, "debit": 0, "credit": float(amt), "description": note}]
        delta = -amt
    else:
        lines = [{"account_code": gl, "debit": float(amt), "credit": 0, "description": note},
                 {"account_code": cp, "debit": 0, "credit": float(amt), "description": note, **dims}]
        delta = amt

    try:
        je = await _post_je(db, tid, user.email, rev_type, tx["tx_date"], note, lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        rev_serial = await _next_serial(db, tid, rev_type, tx["tx_date"])
        rev_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO tr_cash_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,currency_code,
               exchange_rate,amount_sar,counterpart_account,description,reference,
               payment_method,status,je_id,je_serial,posted_by,posted_at,created_by)
            VALUES
              (:id,:tid,:serial,:tt,:dt,:ba,:amt,:cur,:rate,:amt_sar,
               :cp,:desc,:ref,:method,'posted',:je_id,:je_serial,:by,NOW(),:by)
        """), {"id": rev_id, "tid": tid, "serial": rev_serial, "tt": rev_type,
               "dt": tx["tx_date"], "ba": str(tx["bank_account_id"]) if tx["bank_account_id"] else None,
               "amt": amt, "cur": tx["currency_code"] or "SAR", "rate": tx["exchange_rate"] or 1,
               "amt_sar": amt, "cp": cp, "desc": note, "ref": tx["reference"],
               "method": tx["payment_method"] or "cash",
               "je_id": je["je_id"], "je_serial": je["je_serial"], "by": user.email})
        await db.execute(text("""
            UPDATE tr_cash_transactions SET status='reversed', reversed_by=:by, reversed_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "id": str(tx_id), "tid": tid})
        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في العكس: {str(e)}")
    return ok(data={"reversal_serial": rev_serial}, message=f"✅ تم القيد العكسي — {rev_serial}")


@router.post("/cash-transactions/bulk-post")
async def bulk_post_cash(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    ids = data.get("ids", [])
    if not ids: raise HTTPException(400, "لم يتم تحديد أي سندات")
    tid = str(user.tenant_id)
    posted, errors = [], []
    for raw_id in ids:
        try:
            r = await db.execute(text("""
                SELECT ct.*, ba.gl_account_code FROM tr_cash_transactions ct
                LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
                WHERE ct.id=:id AND ct.tenant_id=:tid
            """), {"id": str(raw_id), "tid": tid})
            tx = r.mappings().fetchone()
            if not tx or tx["status"] != "draft": errors.append(str(raw_id)); continue
            gl = tx["gl_account_code"]; cp = tx["counterpart_account"]
            if not gl or not cp: errors.append(str(raw_id)); continue
            amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
            dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
                    "project_code": tx.get("project_code"),
                    "expense_classification_code": tx.get("expense_classification_code")}
            if tx["tx_type"] == "RV":
                lines = [{"account_code": gl, "debit": float(amt), "credit": 0, "description": tx["description"]},
                         {"account_code": cp, "debit": 0, "credit": float(amt), "description": tx["description"], **dims}]
                delta = amt
            else:
                lines = [{"account_code": cp, "debit": float(amt), "credit": 0, "description": tx["description"], **dims},
                         {"account_code": gl, "debit": 0, "credit": float(amt), "description": tx["description"]}]
                delta = -amt
            je = await _post_je(db, tid, user.email, tx["tx_type"], tx["tx_date"], tx["description"], lines, tx["reference"])
            if tx["bank_account_id"]: await _update_balance(db, str(tx["bank_account_id"]), delta)
            await db.execute(text("""
                UPDATE tr_cash_transactions
                SET status='posted', je_id=:je_id, je_serial=:je_serial, posted_by=:by, posted_at=NOW()
                WHERE id=:id AND tenant_id=:tid
            """), {"je_id": je["je_id"], "je_serial": je["je_serial"],
                   "by": user.email, "id": str(raw_id), "tid": tid})
            await db.commit()
            posted.append(je["je_serial"])
        except Exception:
            await db.rollback(); errors.append(str(raw_id))
    return ok(data={"posted": posted, "errors": errors},
              message=f"✅ تم ترحيل {len(posted)} سند" + (f" | ⚠️ {len(errors)} فشل" if errors else ""))

# ══════════════════════════════════════════════════════════
# BANK WORKFLOW — submit / approve / reject / reverse / bulk
# ══════════════════════════════════════════════════════════

@router.post("/bank-transactions/{tx_id}/submit")
async def submit_bank_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        UPDATE tr_bank_transactions
        SET status='pending_approval', submitted_by=:by, submitted_at=NOW()
        WHERE id=:id AND tenant_id=:tid AND status='draft'
        RETURNING id, serial, tx_type, amount
    """), {"id": str(tx_id), "tid": tid, "by": user.email})
    row = r.fetchone()
    if not row: raise HTTPException(400, "السند غير موجود أو ليس مسودة")
    await db.commit()
    # إنشاء إشعار
    tx_labels = {"BR": "قبض بنكي", "BP": "دفعة بنكية", "BT": "تحويل بنكي"}
    tx_label = tx_labels.get(row[2], "معاملة بنكية")
    await _create_notification(
        db, tid,
        title=f"🔔 {tx_label} بانتظار الاعتماد",
        body=f"السند {row[1]} بمبلغ {float(row[3] or 0):,.3f} ر.س — أرسله {user.email} للاعتماد",
        ref_type="bank_transaction",
        ref_id=str(tx_id),
    )
    return ok(data={}, message="تم إرسال السند للاعتماد ✅")


@router.post("/bank-transactions/{tx_id}/approve")
async def approve_bank_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT bt.*, ba.gl_account_code FROM tr_bank_transactions bt
        LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
        WHERE bt.id=:id AND bt.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] not in ("pending_approval","draft"):
        raise HTTPException(400, "لا يمكن اعتماد هذا السند")
    gl = tx["gl_account_code"]; cp = tx["counterpart_account"]
    if not gl: raise HTTPException(400, "حساب الأستاذ العام غير محدد")
    if not cp: raise HTTPException(400, "الحساب المقابل غير محدد")
    base_amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    vat_amt  = Decimal(str(tx.get("vat_amount") or 0))
    vat_acc  = tx.get("vat_account_code") or None
    total    = base_amt + vat_amt
    dims     = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
                "project_code": tx.get("project_code"),
                "expense_classification_code": tx.get("expense_classification_code")}
    if tx["tx_type"] == "BR":
        lines = [{"account_code": gl, "debit": float(total), "credit": 0, "description": tx["description"]},
                 {"account_code": cp, "debit": 0, "credit": float(base_amt), "description": tx["description"], **dims}]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": 0, "credit": float(vat_amt), "description": "ضريبة"})
        delta = total
    else:
        lines = [{"account_code": cp, "debit": float(base_amt), "credit": 0, "description": tx["description"], **dims},
                 {"account_code": gl, "debit": 0, "credit": float(total), "description": tx["description"]}]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": float(vat_amt), "credit": 0, "description": "ضريبة"})
        delta = -total
    try:
        je = await _post_je(db, tid, user.email, tx["tx_type"], tx["tx_date"],
                            tx["description"], lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        await db.execute(text("""
            UPDATE tr_bank_transactions
            SET status='posted', approved_by=:approved_by, approved_at=NOW(),
                je_id=:je_id, je_serial=:je_serial, posted_by=:posted_by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"approved_by": user.email, "posted_by": user.email,
               "je_id": je["je_id"], "je_serial": je["je_serial"],
               "id": str(tx_id), "tid": tid})
        await db.commit()
    except HTTPException: raise
    except Exception as e:
        await db.rollback(); raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الاعتماد — {je['je_serial']}")


@router.post("/bank-transactions/{tx_id}/reject")
async def reject_bank_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        UPDATE tr_bank_transactions
        SET status='draft', rejected_by=:by, rejected_at=NOW(), rejection_note=:note
        WHERE id=:id AND tenant_id=:tid AND status='pending_approval'
        RETURNING id
    """), {"id": str(tx_id), "tid": tid, "by": user.email, "note": data.get("note","مرفوض")})
    if not r.fetchone(): raise HTTPException(400, "السند غير موجود أو ليس في انتظار الاعتماد")
    await db.commit()
    return ok(data={}, message="تم رفض السند")


@router.post("/bank-transactions/{tx_id}/reverse")
async def reverse_bank_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT bt.*, ba.gl_account_code FROM tr_bank_transactions bt
        LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
        WHERE bt.id=:id AND bt.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] != "posted": raise HTTPException(400, "يمكن عكس المُرحَّل فقط")
    gl   = tx["gl_account_code"]; cp = tx["counterpart_account"]
    amt  = Decimal(str(tx["amount_sar"] or tx["amount"]))
    note = data.get("note", f"عكس {tx['serial']}")
    dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
            "project_code": tx.get("project_code"),
            "expense_classification_code": tx.get("expense_classification_code")}
    if tx["tx_type"] == "BR":
        rev_type = "BP"
        lines = [{"account_code": cp, "debit": float(amt), "credit": 0, "description": note, **dims},
                 {"account_code": gl, "debit": 0, "credit": float(amt), "description": note}]
        delta = -amt
    else:
        rev_type = "BR"
        lines = [{"account_code": gl, "debit": float(amt), "credit": 0, "description": note},
                 {"account_code": cp, "debit": 0, "credit": float(amt), "description": note, **dims}]
        delta = amt
    try:
        je = await _post_je(db, tid, user.email, rev_type, tx["tx_date"], note, lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        rev_serial = await _next_serial(db, tid, rev_type, tx["tx_date"])
        rev_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO tr_bank_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,currency_code,
               exchange_rate,amount_sar,counterpart_account,description,reference,
               payment_method,status,je_id,je_serial,posted_by,posted_at,created_by)
            VALUES
              (:id,:tid,:serial,:tt,:dt,:ba,:amt,:cur,:rate,:amt_sar,
               :cp,:desc,:ref,:method,'posted',:je_id,:je_serial,:by,NOW(),:by)
        """), {"id": rev_id, "tid": tid, "serial": rev_serial, "tt": rev_type,
               "dt": tx["tx_date"], "ba": str(tx["bank_account_id"]) if tx["bank_account_id"] else None,
               "amt": amt, "cur": tx["currency_code"] or "SAR", "rate": tx["exchange_rate"] or 1,
               "amt_sar": amt, "cp": cp, "desc": note, "ref": tx["reference"],
               "method": tx["payment_method"] or "wire",
               "je_id": je["je_id"], "je_serial": je["je_serial"], "by": user.email})
        await db.execute(text("""
            UPDATE tr_bank_transactions SET status='reversed', reversed_by=:by, reversed_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "id": str(tx_id), "tid": tid})
        await db.commit()
    except HTTPException: raise
    except Exception as e:
        await db.rollback(); raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={"reversal_serial": rev_serial}, message=f"✅ تم العكس — {rev_serial}")


@router.post("/bank-transactions/bulk-post")
async def bulk_post_bank(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    ids = data.get("ids", [])
    if not ids: raise HTTPException(400, "لم يتم تحديد أي سندات")
    tid = str(user.tenant_id)
    posted, errors = [], []
    for raw_id in ids:
        try:
            r = await db.execute(text("""
                SELECT bt.*, ba.gl_account_code FROM tr_bank_transactions bt
                LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
                WHERE bt.id=:id AND bt.tenant_id=:tid
            """), {"id": str(raw_id), "tid": tid})
            tx = r.mappings().fetchone()
            if not tx or tx["status"] != "draft": errors.append(str(raw_id)); continue
            gl = tx["gl_account_code"]; cp = tx["counterpart_account"]
            if not gl or not cp: errors.append(str(raw_id)); continue
            amt  = Decimal(str(tx["amount_sar"] or tx["amount"]))
            dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
                    "project_code": tx.get("project_code"),
                    "expense_classification_code": tx.get("expense_classification_code")}
            if tx["tx_type"] == "BR":
                lines = [{"account_code": gl, "debit": float(amt), "credit": 0, "description": tx["description"]},
                         {"account_code": cp, "debit": 0, "credit": float(amt), "description": tx["description"], **dims}]
                delta = amt
            else:
                lines = [{"account_code": cp, "debit": float(amt), "credit": 0, "description": tx["description"], **dims},
                         {"account_code": gl, "debit": 0, "credit": float(amt), "description": tx["description"]}]
                delta = -amt
            je = await _post_je(db, tid, user.email, tx["tx_type"], tx["tx_date"],
                                tx["description"], lines, tx["reference"])
            if tx["bank_account_id"]:
                await _update_balance(db, str(tx["bank_account_id"]), delta)
            await db.execute(text("""
                UPDATE tr_bank_transactions
                SET status='posted', je_id=:je_id, je_serial=:je_serial,
                    posted_by=:by, posted_at=NOW()
                WHERE id=:id AND tenant_id=:tid
            """), {"je_id": je["je_id"], "je_serial": je["je_serial"],
                   "by": user.email, "id": str(raw_id), "tid": tid})
            await db.commit()
            posted.append(je["je_serial"])
        except Exception:
            await db.rollback(); errors.append(str(raw_id))
    return ok(data={"posted": posted, "errors": errors},
              message=f"✅ تم ترحيل {len(posted)} سند" + (f" | ⚠️ {len(errors)} فشل" if errors else ""))


@router.get("/reports/low-balance-alerts")
async def low_balance_alerts(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT id, account_name, account_type, current_balance, low_balance_alert, currency_code
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true
          AND low_balance_alert > 0 AND current_balance <= low_balance_alert
        ORDER BY (current_balance - low_balance_alert)
    """), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])

# ══════════════════════════════════════════════════════════
# GL IMPORT — استيراد القيود اليومية إلى موديول الخزينة
# تحل مشكلة: قيود JV/REV/REC على حسابات البنك لا تظهر في الخزينة
# ══════════════════════════════════════════════════════════

@router.get("/gl-import/unlinked-entries")
async def get_unlinked_gl_entries(
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """جلب قيود الأستاذ العام التي تؤثر على حسابات البنك/الصندوق
       ولم تُستورَد بعد إلى موديول الخزينة."""
    tid = str(user.tenant_id)

    # نجد أكواد GL لحسابات البنك والصناديق
    r = await db.execute(text("""
        SELECT DISTINCT gl_account_code, account_name, account_type, id
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true AND gl_account_code IS NOT NULL
    """), {"tid": tid})
    bank_accounts = {row.gl_account_code: row._mapping for row in r.fetchall()}

    if not bank_accounts:
        return ok(data=[], message="لا توجد حسابات بنكية مرتبطة بالأستاذ العام")

    gl_codes = list(bank_accounts.keys())
    codes_placeholder = ",".join([f":code_{i}" for i in range(len(gl_codes))])
    codes_params = {f"code_{i}": code for i, code in enumerate(gl_codes)}

    where_date = ""
    if date_from:
        where_date += " AND je.entry_date >= :date_from"
    if date_to:
        where_date += " AND je.entry_date <= :date_to"

    r2 = await db.execute(text(f"""
        SELECT
            je.id          AS je_id,
            je.serial      AS je_serial,
            je.entry_date  AS tx_date,
            je.je_type,
            je.description,
            je.reference,
            jl.account_code,
            jl.debit,
            jl.credit,
            jl.description AS line_desc,
            jl.branch_code,
            jl.cost_center,
            jl.project_code
        FROM journal_entries je
        JOIN je_lines jl ON jl.je_id = je.id
        WHERE je.tenant_id = :tid
          AND je.status = 'posted'
          AND jl.account_code IN ({codes_placeholder})
          AND COALESCE(je.source_module, '') != 'treasury'
          AND je.id NOT IN (
              SELECT je_id FROM tr_bank_transactions
              WHERE tenant_id = :tid AND je_id IS NOT NULL
          )
          AND je.id NOT IN (
              SELECT je_id FROM tr_cash_transactions
              WHERE tenant_id = :tid AND je_id IS NOT NULL
          )
          {where_date}
        ORDER BY je.entry_date DESC, je.serial DESC
        LIMIT 200
    """), {"tid": tid, **codes_params,
           **({} if not date_from else {"date_from": date_from}),
           **({} if not date_to   else {"date_to":   date_to})})

    rows = r2.mappings().fetchall()
    result = []
    for row in rows:
        acc_info = bank_accounts.get(row["account_code"], {})
        result.append({
            "je_id":         str(row["je_id"]),
            "je_serial":     row["je_serial"],
            "je_type":       row["je_type"],
            "tx_date":       str(row["tx_date"]),
            "description":   row["description"] or row["line_desc"] or "",
            "reference":     row["reference"],
            "account_code":  row["account_code"],
            "bank_account_id": str(acc_info.get("id","")) if acc_info else None,
            "bank_account_name": acc_info.get("account_name","") if acc_info else row["account_code"],
            "account_type":  acc_info.get("account_type","bank") if acc_info else "bank",
            "debit":         float(row["debit"] or 0),
            "credit":        float(row["credit"] or 0),
            "amount":        float(row["debit"] or row["credit"] or 0),
            "direction":     "debit" if float(row["debit"] or 0) > 0 else "credit",
            "tx_type":       "BR" if float(row["debit"] or 0) > 0 else "BP",
            "branch_code":   row["branch_code"],
            "cost_center":   row["cost_center"],
            "project_code":  row["project_code"],
        })

    return ok(data=result, message=f"يوجد {len(result)} قيد غير مُستورَد")


@router.post("/gl-import/import-entries")
async def import_gl_entries(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """استيراد قيود محددة من الأستاذ العام إلى موديول الخزينة كسندات مُرحَّلة."""
    tid = str(user.tenant_id)
    entry_ids = data.get("je_ids", [])
    if not entry_ids:
        raise HTTPException(400, "لم يتم تحديد أي قيود للاستيراد")

    imported, errors = [], []

    for je_id in entry_ids:
        try:
            # جلب بيانات القيد
            r = await db.execute(text("""
                SELECT
                    je.id, je.serial, je.entry_date, je.je_type,
                    je.description, je.reference,
                    jl.account_code, jl.debit, jl.credit,
                    jl.branch_code, jl.cost_center, jl.project_code,
                    ba.id AS bank_account_id, ba.account_name AS bank_name,
                    ba.account_type
                FROM journal_entries je
                JOIN je_lines jl ON jl.je_id = je.id
                JOIN tr_bank_accounts ba
                     ON ba.gl_account_code = jl.account_code
                     AND ba.tenant_id = :tid
                WHERE je.id = :je_id AND je.tenant_id = :tid
                  AND je.status = 'posted'
                LIMIT 1
            """), {"je_id": je_id, "tid": tid})

            je = r.mappings().fetchone()
            if not je:
                errors.append({"je_id": je_id, "reason": "القيد غير موجود أو غير مُرحَّل"})
                continue

            amt       = Decimal(str(je["debit"] or je["credit"] or 0))
            is_debit  = float(je["debit"] or 0) > 0
            tx_type   = "BR" if is_debit else "BP"
            tx_date   = je["entry_date"]
            ba_type   = je["account_type"] or "bank"

            # تحديد الجدول: نقدي أم بنكي
            is_cash = ba_type in ("cash_fund", "petty_cash")
            serial  = await _next_serial(db, tid, tx_type, tx_date)
            tx_id   = str(uuid.uuid4())

            if is_cash:
                await db.execute(text("""
                    INSERT INTO tr_cash_transactions
                      (id,tenant_id,serial,tx_type,tx_date,bank_account_id,
                       amount,currency_code,amount_sar,
                       description,reference,payment_method,
                       branch_code,cost_center,project_code,
                       status,je_id,je_serial,posted_by,posted_at,created_by)
                    VALUES
                      (:id,:tid,:serial,:tt,:dt,:ba,
                       :amt,'SAR',:amt,
                       :desc,:ref,'cash',
                       :branch,:cc,:proj,
                       'posted',:je_id,:je_serial,:by,NOW(),:by)
                """), {
                    "id":tx_id,"tid":tid,"serial":serial,"tt":tx_type,"dt":tx_date,
                    "ba":str(je["bank_account_id"]),"amt":amt,
                    "desc":je["description"],"ref":je["reference"],
                    "branch":je["branch_code"],"cc":je["cost_center"],"proj":je["project_code"],
                    "je_id":je_id,"je_serial":je["serial"],"by":user.email,
                })
            else:
                await db.execute(text("""
                    INSERT INTO tr_bank_transactions
                      (id,tenant_id,serial,tx_type,tx_date,bank_account_id,
                       amount,currency_code,amount_sar,
                       description,reference,payment_method,
                       branch_code,cost_center,project_code,
                       status,je_id,je_serial,posted_by,posted_at,created_by)
                    VALUES
                      (:id,:tid,:serial,:tt,:dt,:ba,
                       :amt,'SAR',:amt,
                       :desc,:ref,'wire',
                       :branch,:cc,:proj,
                       'posted',:je_id,:je_serial,:by,NOW(),:by)
                """), {
                    "id":tx_id,"tid":tid,"serial":serial,"tt":tx_type,"dt":tx_date,
                    "ba":str(je["bank_account_id"]),"amt":amt,
                    "desc":je["description"],"ref":je["reference"],
                    "branch":je["branch_code"],"cc":je["cost_center"],"proj":je["project_code"],
                    "je_id":je_id,"je_serial":je["serial"],"by":user.email,
                })

            await db.commit()
            imported.append({"je_id": je_id, "serial": serial, "tx_type": tx_type})

        except Exception as e:
            await db.rollback()
            errors.append({"je_id": je_id, "reason": str(e)})

    return ok(
        data={"imported": imported, "errors": errors},
        message=f"✅ تم استيراد {len(imported)} قيد" + (f" | ⚠️ {len(errors)} فشل" if errors else "")
    )

# ══════════════════════════════════════════════════════════
# REPORTS — التقارير المفقودة
# ══════════════════════════════════════════════════════════

@router.get("/reports/account-statement")
async def account_statement(
    bank_account_id: Optional[str] = Query(default=None),
    date_from:       Optional[str] = Query(default=None),
    date_to:         Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """كشف حساب بنكي — يجمع كل الحركات النقدية والبنكية"""
    tid = str(user.tenant_id)
    if not bank_account_id:
        raise HTTPException(400, "يرجى تحديد الحساب البنكي")

    # معلومات الحساب
    r = await db.execute(text("""
        SELECT * FROM tr_bank_accounts
        WHERE tenant_id=:tid AND id=:id
    """), {"tid": tid, "id": bank_account_id})
    account = r.mappings().fetchone()
    if not account:
        raise HTTPException(404, "الحساب غير موجود")

    where = ["tenant_id=:tid", "bank_account_id=:ba_id", "status='posted'"]
    params: dict = {"tid": tid, "ba_id": bank_account_id}
    if date_from:
        try:
            from datetime import date as _date
            params["df"] = _date.fromisoformat(str(date_from)[:10])
        except Exception:
            params["df"] = date_from
        where.append("tx_date>=:df")
    if date_to:
        try:
            from datetime import date as _date
            params["dt"] = _date.fromisoformat(str(date_to)[:10])
        except Exception:
            params["dt"] = date_to
        where.append("tx_date<=:dt")
    w = " AND ".join(where)

    try:
        # سندات نقدية — COALESCE لكل عمود قد يكون مفقوداً
        r2 = await db.execute(text(f"""
            SELECT serial, tx_type, tx_date::text,
                   COALESCE(description,'') AS description,
                   COALESCE(reference,'')   AS reference,
                   COALESCE(party_name,'')  AS party,
                   COALESCE(amount,0)       AS amount,
                   CASE WHEN tx_type='RV' THEN COALESCE(amount,0) ELSE 0 END AS debit,
                   CASE WHEN tx_type='PV' THEN COALESCE(amount,0) ELSE 0 END AS credit,
                   'cash' AS source,
                   COALESCE(is_reconciled, false) AS is_reconciled,
                   je_serial
            FROM tr_cash_transactions WHERE {w} ORDER BY tx_date, serial
        """), params)
        cash_rows = [dict(r._mapping) for r in r2.fetchall()]

        # معاملات بنكية
        r3 = await db.execute(text(f"""
            SELECT serial, tx_type, tx_date::text,
                   COALESCE(description,'') AS description,
                   COALESCE(reference,'')   AS reference,
                   COALESCE(beneficiary_name, '') AS party,
                   COALESCE(amount,0) AS amount,
                   CASE WHEN tx_type='BR' THEN COALESCE(amount,0) ELSE 0 END AS debit,
                   CASE WHEN tx_type IN ('BP','BT') THEN COALESCE(amount,0) ELSE 0 END AS credit,
                   'bank' AS source,
                   COALESCE(is_reconciled, false) AS is_reconciled,
                   je_serial
            FROM tr_bank_transactions WHERE {w} ORDER BY tx_date, serial
        """), params)
        bank_rows = [dict(r._mapping) for r in r3.fetchall()]

        all_rows = sorted(cash_rows + bank_rows,
                         key=lambda x: (str(x.get("tx_date","") or ""), str(x.get("serial","") or "")))
        opening = float(account.get("opening_balance") or 0)
        balance = opening
        for row in all_rows:
            row["debit"]  = float(row.get("debit")  or 0)
            row["credit"] = float(row.get("credit") or 0)
            row["amount"] = float(row.get("amount") or 0)
            balance += row["debit"] - row["credit"]
            row["running_balance"] = round(balance, 3)

        return ok(data={
            "account":      dict(account),
            "rows":         all_rows,
            "opening":      opening,
            "closing":      round(balance, 3),
            "total_debit":  round(sum(r["debit"]  for r in all_rows), 3),
            "total_credit": round(sum(r["credit"] for r in all_rows), 3),
        })
    except Exception as e:
        import traceback; print(f"[account-statement] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في كشف الحساب: {str(e)}")


@router.get("/reports/monthly-cash-flow")
async def monthly_cash_flow(
    bank_account_id: Optional[str] = Query(default=None),
    year:            int            = Query(default=2026),
    month:           Optional[int]  = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """التدفق النقدي الشهري"""
    tid = str(user.tenant_id)
    try:
        ba_filter    = "AND bank_account_id=:ba_id" if bank_account_id else ""
        month_filter = "AND EXTRACT(MONTH FROM tx_date)=:month" if month else ""
        params: dict = {"tid": tid, "year": year}
        if bank_account_id: params["ba_id"] = bank_account_id
        if month:           params["month"] = month

        r = await db.execute(text(f"""
            SELECT
                EXTRACT(MONTH FROM tx_date)::int AS month,
                TO_CHAR(tx_date,'YYYY-MM')       AS period,
                COALESCE(SUM(CASE WHEN tx_type IN ('RV','BR') THEN amount ELSE 0 END),0) AS inflow,
                COALESCE(SUM(CASE WHEN tx_type IN ('PV','BP','BT') THEN amount ELSE 0 END),0) AS outflow
            FROM (
                SELECT tx_type, tx_date, COALESCE(amount,0) AS amount, bank_account_id
                FROM tr_cash_transactions
                WHERE tenant_id=:tid AND status='posted'
                  AND EXTRACT(YEAR FROM tx_date)=:year
                  {ba_filter} {month_filter}
                UNION ALL
                SELECT tx_type, tx_date, COALESCE(amount,0) AS amount, bank_account_id
                FROM tr_bank_transactions
                WHERE tenant_id=:tid AND status='posted'
                  AND EXTRACT(YEAR FROM tx_date)=:year
                  {ba_filter} {month_filter}
            ) t
            GROUP BY EXTRACT(MONTH FROM tx_date), TO_CHAR(tx_date,'YYYY-MM')
            ORDER BY period
        """), params)

        rows = []
        for row in r.mappings().fetchall():
            inflow  = float(row.get("inflow")  or 0)
            outflow = float(row.get("outflow") or 0)
            rows.append({
                "month":   int(row.get("month") or 0),
                "period":  row.get("period",""),
                "inflow":  round(inflow, 3),
                "outflow": round(outflow, 3),
                "net":     round(inflow - outflow, 3),
            })

        return ok(data={
            "rows":          rows,
            "total_inflow":  round(sum(r["inflow"]  for r in rows), 3),
            "total_outflow": round(sum(r["outflow"] for r in rows), 3),
            "net":           round(sum(r["net"]     for r in rows), 3),
            "year":          year,
        })
    except Exception as e:
        import traceback; print(f"[monthly-cash-flow] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في التدفق الشهري: {str(e)}")

@router.get("/reports/inactive-accounts")
async def inactive_accounts(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """الحسابات غير النشطة — لم يكن عليها أي حركة في آخر 90 يوماً"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
            ba.id, ba.account_name, ba.account_type, ba.currency_code,
            ba.current_balance, ba.is_active,
            MAX(GREATEST(
                COALESCE(ct.tx_date, '2000-01-01'),
                COALESCE(bt.tx_date, '2000-01-01')
            )) AS last_activity
        FROM tr_bank_accounts ba
        LEFT JOIN tr_cash_transactions ct
              ON ct.bank_account_id=ba.id AND ct.tenant_id=ba.tenant_id AND ct.status='posted'
        LEFT JOIN tr_bank_transactions bt
              ON bt.bank_account_id=ba.id AND bt.tenant_id=ba.tenant_id AND bt.status='posted'
        WHERE ba.tenant_id=:tid
        GROUP BY ba.id, ba.account_name, ba.account_type,
                 ba.currency_code, ba.current_balance, ba.is_active
        HAVING MAX(GREATEST(
                COALESCE(ct.tx_date, '2000-01-01'),
                COALESCE(bt.tx_date, '2000-01-01')
            )) < NOW() - INTERVAL '90 days'
            OR MAX(GREATEST(
                COALESCE(ct.tx_date, '2000-01-01'),
                COALESCE(bt.tx_date, '2000-01-01')
            )) = '2000-01-01'
        ORDER BY last_activity
    """), {"tid": tid})
    rows = []
    for row in r.mappings().fetchall():
        rows.append({
            "id":              str(row["id"]),
            "account_name":    row["account_name"],
            "account_type":    row["account_type"],
            "currency_code":   row["currency_code"],
            "current_balance": float(row["current_balance"] if row["current_balance"] is not None else 0),
            "is_active":       row["is_active"],
            "last_activity":   str(row["last_activity"]) if row["last_activity"] and str(row["last_activity"]) != "2000-01-01" else None,
            "days_inactive":   None,
        })
    return ok(data=rows, message=f"{len(rows)} حساب غير نشط")


@router.get("/reports/balance-history")
async def balance_history(
    months: int = Query(default=6),
    bank_account_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تاريخ الأرصدة الشهرية للحسابات"""
    tid = str(user.tenant_id)
    try:
        ba_filter = "AND (ct.bank_account_id=:ba_id OR bt.bank_account_id=:ba_id)" if bank_account_id else ""
        params: dict = {"tid": tid, "months": months}
        if bank_account_id: params["ba_id"] = bank_account_id

        r = await db.execute(text(f"""
            WITH monthly AS (
                SELECT
                    ba.id,
                    ba.account_name,
                    ba.account_type,
                    ba.current_balance,
                    TO_CHAR(DATE_TRUNC('month', gs.m), 'YYYY-MM') AS period,
                    COALESCE((
                        SELECT SUM(CASE WHEN ct2.tx_type='RV' THEN ct2.amount ELSE -ct2.amount END)
                        FROM tr_cash_transactions ct2
                        WHERE ct2.tenant_id=:tid AND ct2.bank_account_id=ba.id
                          AND ct2.status='posted'
                          AND TO_CHAR(DATE_TRUNC('month', ct2.tx_date),'YYYY-MM') = TO_CHAR(DATE_TRUNC('month', gs.m),'YYYY-MM')
                    ),0) +
                    COALESCE((
                        SELECT SUM(CASE WHEN bt2.tx_type='BR' THEN bt2.amount ELSE -bt2.amount END)
                        FROM tr_bank_transactions bt2
                        WHERE bt2.tenant_id=:tid AND bt2.bank_account_id=ba.id
                          AND bt2.status='posted'
                          AND TO_CHAR(DATE_TRUNC('month', bt2.tx_date),'YYYY-MM') = TO_CHAR(DATE_TRUNC('month', gs.m),'YYYY-MM')
                    ),0) AS net_change
                FROM tr_bank_accounts ba
                CROSS JOIN (
                    SELECT generate_series(
                        DATE_TRUNC('month', NOW()) - ((:months - 1) * INTERVAL '1 month'),
                        DATE_TRUNC('month', NOW()),
                        INTERVAL '1 month'
                    ) AS m
                ) gs
                WHERE ba.tenant_id=:tid AND ba.is_active=true
            )
            SELECT id, account_name, account_type,
                   period, COALESCE(net_change,0) AS net_change,
                   current_balance AS balance
            FROM monthly
            ORDER BY id, period
        """), params)

        rows_raw = r.mappings().fetchall()
        # تجميع حسب account_id
        accounts_map: dict = {}
        for row in rows_raw:
            aid = str(row["id"])
            if aid not in accounts_map:
                accounts_map[aid] = {
                    "id": aid,
                    "account_name": row["account_name"],
                    "account_type": row["account_type"],
                    "current_balance": float(row["balance"] or 0),
                    "history": [],
                }
            accounts_map[aid]["history"].append({
                "period":     row["period"],
                "net_change": float(row["net_change"] or 0),
                "balance":    float(row["balance"] or 0),
            })

        return ok(data=list(accounts_map.values()))
    except Exception as e:
        import traceback; print(f"[balance-history] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في تاريخ الأرصدة: {str(e)}")


# ── المصاريف البنكية ──────────────────────────────────────
@router.get("/bank-fees")
async def list_bank_fees(
    bank_account_id: Optional[str] = Query(default=None),
    date_from:       Optional[str] = Query(default=None),
    date_to:         Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    where = ["tenant_id=:tid"]
    params: dict = {"tid": tid}
    if bank_account_id: where.append("bank_account_id=:ba"); params["ba"] = bank_account_id
    if date_from: where.append("fee_date>=:df"); params["df"] = date_from
    if date_to:   where.append("fee_date<=:dt"); params["dt"] = date_to
    r = await db.execute(text("""
        SELECT bf.*, ba.account_name AS bank_account_name
        FROM tr_bank_fees bf
        LEFT JOIN tr_bank_accounts ba ON ba.id=bf.bank_account_id
        WHERE """ + " AND ".join(where) + """
        ORDER BY fee_date DESC
        LIMIT 200
    """), params)
    rows = [dict(r._mapping) for r in r.fetchall()]
    for row in rows:
        if row.get("amount"): row["amount"] = float(row["amount"])
    return ok(data={"items": rows, "total": len(rows), "total_amount": sum(float(r.get("amount") or 0) for r in rows)})


@router.post("/bank-fees")
async def create_bank_fee(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    fee_id = str(uuid.uuid4())
    try:
        await db.execute(text("""
            INSERT INTO tr_bank_fees
              (id,tenant_id,bank_account_id,fee_type,fee_date,amount,description,created_by)
            VALUES (:id,:tid,:ba,:type,:dt,:amt,:desc,:by)
        """), {
            "id": fee_id, "tid": tid,
            "ba":   data.get("bank_account_id"),
            "type": data.get("fee_type","other"),
            "dt":   data.get("fee_date"),
            "amt":  Decimal(str(data.get("amount",0))),
            "desc": data.get("description",""),
            "by":   user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الحفظ: {str(e)}")
    return created(data={"id": fee_id})


@router.delete("/bank-fees/{fee_id}")
async def delete_bank_fee(
    fee_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("DELETE FROM tr_bank_fees WHERE id=:id AND tenant_id=:tid"),
                     {"id": str(fee_id), "tid": tid})
    await db.commit()
    return ok(data={})


# ── المعاملات المتكررة ────────────────────────────────────
@router.get("/recurring-transactions")
async def list_recurring(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT
              rt.*,
              ba.account_name AS bank_account_name,
              COALESCE(inst.total,0)   AS total_instances,
              COALESCE(inst.posted,0)  AS posted_count,
              COALESCE(inst.draft,0)   AS draft_count,
              COALESCE(inst.pending,0) AS pending_count,
              COALESCE(inst.skipped,0) AS skipped_count
            FROM tr_recurring_transactions rt
            LEFT JOIN tr_bank_accounts ba ON ba.id = rt.bank_account_id
            LEFT JOIN LATERAL (
              SELECT
                COUNT(*)                                        AS total,
                COUNT(*) FILTER (WHERE status='posted')        AS posted,
                COUNT(*) FILTER (WHERE status='draft')         AS draft,
                COUNT(*) FILTER (WHERE status='pending')       AS pending,
                COUNT(*) FILTER (WHERE status='skipped')       AS skipped
              FROM tr_recurring_instances
              WHERE recurring_id = rt.id
            ) inst ON true
            WHERE rt.tenant_id=:tid
            ORDER BY rt.created_at DESC
        """), {"tid": tid})
        rows = []
        for row in r.mappings().fetchall():
            d = dict(row)
            if d.get("amount"):
                d["amount"] = float(d["amount"])
            rows.append(d)
        return ok(data={"items": rows, "total": len(rows)})
    except Exception as e:
        import traceback; print(f"[recurring-transactions] {traceback.format_exc()}")
        return ok(data={"items": [], "total": 0})


@router.get("/recurring-transactions/{rec_id}")
async def get_recurring(
    rec_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT rt.*, ba.account_name AS bank_account_name
        FROM tr_recurring_transactions rt
        LEFT JOIN tr_bank_accounts ba ON ba.id = rt.bank_account_id
        WHERE rt.id=:id AND rt.tenant_id=:tid
    """), {"id": str(rec_id), "tid": tid})
    row = r.mappings().fetchone()
    if not row: raise HTTPException(404, "القالب غير موجود")
    rec = dict(row)
    if rec.get("amount"): rec["amount"] = float(rec["amount"])

    # الأقساط
    r2 = await db.execute(text("""
        SELECT * FROM tr_recurring_instances
        WHERE recurring_id=:rid
        ORDER BY instance_number
    """), {"rid": str(rec_id)})
    instances = []
    for row in r2.mappings().fetchall():
        d = dict(row)
        if d.get("amount"): d["amount"] = float(d["amount"])
        instances.append(d)

    rec["instances"] = instances
    return ok(data=rec)


@router.get("/recurring-transactions/{rec_id}/instances")
async def list_instances(
    rec_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ri.* FROM tr_recurring_instances ri
        JOIN tr_recurring_transactions rt ON rt.id = ri.recurring_id
        WHERE ri.recurring_id=:rid AND rt.tenant_id=:tid
        ORDER BY ri.instance_number
    """), {"rid": str(rec_id), "tid": tid})
    rows = []
    for row in r.mappings().fetchall():
        d = dict(row)
        if d.get("amount"): d["amount"] = float(d["amount"])
        rows.append(d)
    return ok(data={"items": rows, "total": len(rows)})




@router.post("/recurring-transactions")
async def create_recurring(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إنشاء قالب متكرر مع توليد جدول الأقساط تلقائياً"""
    tid = str(user.tenant_id)
    rec_id = str(uuid.uuid4())

    # الترقيم FBT
    serial = await _next_serial(db, tid, "FBT", date.today())

    # حساب تاريخ البداية وعدد الأقساط
    total_installments = int(data.get("total_installments") or 1)
    start_d = date.fromisoformat(str(data["start_date"])) if data.get("start_date") else date.today()
    freq     = data.get("frequency", "monthly")
    amt      = Decimal(str(data.get("amount", 0)))

    # حساب تواريخ الأقساط
    from calendar import monthrange
    def add_months(d, n):
        month = d.month - 1 + n
        year  = d.year + month // 12
        month = month % 12 + 1
        day   = min(d.day, monthrange(year, month)[1])
        return d.replace(year=year, month=month, day=day)

    def next_date(d, f):
        from datetime import timedelta
        if   f == "daily":      return d + timedelta(days=1)
        elif f == "weekly":     return d + timedelta(weeks=1)
        elif f == "biweekly":   return d + timedelta(weeks=2)
        elif f == "monthly":    return add_months(d, 1)
        elif f == "quarterly":  return add_months(d, 3)
        elif f == "semiannual": return add_months(d, 6)
        elif f == "annual":     return add_months(d, 12)
        return add_months(d, 1)

    instance_dates = []
    cur = start_d
    for i in range(total_installments):
        instance_dates.append(cur)
        cur = next_date(cur, freq)
    end_date_val = instance_dates[-1] if instance_dates else start_d

    try:
        await db.execute(text("""
            INSERT INTO tr_recurring_transactions
              (id,tenant_id,serial,name,source,tx_type,bank_account_id,counterpart_account,
               amount,currency_code,description,frequency,start_date,end_date,
               total_installments,next_run_date,
               branch_code,cost_center,project_code,expense_classification_code,
               status,is_active,created_by)
            VALUES
              (:id,:tid,:serial,:name,:source,:tt,:ba,:cp,
               :amt,:curr,:desc,:freq,:start,:end_date,
               :total,:next,
               :branch,:cc,:proj,:exp_cls,
               'active',true,:by)
        """), {
            "id": rec_id, "tid": tid, "serial": serial,
            "name":     data.get("name",""),
            "source":   data.get("source","bank"),
            "tt":       data.get("tx_type","BP"),
            "ba":       data.get("bank_account_id") or None,
            "cp":       data.get("counterpart_account") or None,
            "amt":      amt,
            "curr":     data.get("currency_code","SAR"),
            "desc":     data.get("description",""),
            "freq":     freq,
            "start":    start_d,
            "end_date": end_date_val,
            "total":    total_installments,
            "next":     instance_dates[0] if instance_dates else start_d,
            "branch":   data.get("branch_code") or None,
            "cc":       data.get("cost_center") or None,
            "proj":     data.get("project_code") or None,
            "exp_cls":  data.get("expense_classification_code") or None,
            "by":       user.email,
        })

        # توليد جدول الأقساط تلقائياً
        for i, inst_date in enumerate(instance_dates, 1):
            await db.execute(text("""
                INSERT INTO tr_recurring_instances
                  (id,tenant_id,recurring_id,instance_number,due_date,amount,currency_code,status)
                VALUES
                  (:id,:tid,:rid,:num,:due,:amt,:curr,'pending')
            """), {
                "id":   str(uuid.uuid4()),
                "tid":  tid,
                "rid":  rec_id,
                "num":  i,
                "due":  inst_date,
                "amt":  amt,
                "curr": data.get("currency_code","SAR"),
            })

        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ: {str(e)}")

    return created(data={"id": rec_id, "serial": serial,
                         "total_installments": total_installments},
                   message=f"تم إنشاء {serial} مع {total_installments} قسط")


@router.put("/recurring-transactions/{rec_id}")
async def update_recurring(
    rec_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ALLOWED = {"name","source","tx_type","bank_account_id","counterpart_account",
               "amount","currency_code","description","frequency","is_active","notes",
               "branch_code","cost_center","project_code","expense_classification_code"}
    safe = {k:v for k,v in data.items() if k in ALLOWED}
    if not safe: raise HTTPException(400, "لا بيانات للتعديل")
    if "amount" in safe:
        try: safe["amount"] = Decimal(str(safe["amount"]))
        except: raise HTTPException(400, "المبلغ غير صحيح")
    safe["updated_at"] = datetime.utcnow()
    set_clause = ", ".join([f"{k}=:{k}" for k in safe.keys()])
    safe.update({"id": str(rec_id), "tid": tid})
    try:
        await db.execute(text(
            f"UPDATE tr_recurring_transactions SET {set_clause} WHERE id=:id AND tenant_id=:tid"
        ), safe)
        await db.commit()
    except Exception as e:
        await db.rollback(); raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={"id": str(rec_id)}, message="تم التعديل ✅")


@router.delete("/recurring-transactions/{rec_id}")
async def delete_recurring(
    rec_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        await db.execute(text(
            "DELETE FROM tr_recurring_transactions WHERE id=:id AND tenant_id=:tid"
        ), {"id": str(rec_id), "tid": tid})
        await db.commit()
    except Exception as e:
        await db.rollback(); raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={}, message="تم الحذف ✅")


@router.post("/recurring-instances/{inst_id}/execute")
async def execute_instance(
    inst_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تنفيذ قسط — ينشئ سند مسودة"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ri.*, rt.tx_type, rt.source, rt.bank_account_id,
               rt.counterpart_account, rt.description, rt.currency_code,
               rt.branch_code, rt.cost_center, rt.project_code,
               rt.expense_classification_code, rt.name AS rec_name,
               rt.id AS recurring_id
        FROM tr_recurring_instances ri
        JOIN tr_recurring_transactions rt ON rt.id = ri.recurring_id
        WHERE ri.id=:id AND rt.tenant_id=:tid
    """), {"id": str(inst_id), "tid": tid})
    inst = r.mappings().fetchone()
    if not inst: raise HTTPException(404, "القسط غير موجود")
    if inst["status"] != "pending":
        raise HTTPException(400, f"حالة القسط ({inst['status']}) لا تسمح بالتنفيذ")

    tx_date = inst["due_date"]
    serial  = await _next_serial(db, tid, inst["tx_type"], tx_date)
    tx_id   = str(uuid.uuid4())
    amt     = Decimal(str(inst["amount"]))
    source  = inst.get("source","bank")
    tx_tbl  = "tr_cash_transactions" if source == "cash" else "tr_bank_transactions"

    try:
        await db.execute(text(f"""
            INSERT INTO {tx_tbl}
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,
               amount,currency_code,amount_sar,counterpart_account,
               description,payment_method,
               branch_code,cost_center,project_code,expense_classification_code,
               status,created_by)
            VALUES
              (:id,:tid,:serial,:tt,:dt,:ba,
               :amt,:curr,:amt,:cp,:desc,:pm,
               :branch,:cc,:proj,:exp_cls,
               'draft',:by)
        """), {
            "id": tx_id, "tid": tid, "serial": serial,
            "tt":       inst["tx_type"],
            "dt":       tx_date,
            "ba":       str(inst["bank_account_id"]) if inst.get("bank_account_id") else None,
            "amt":      amt,
            "curr":     inst.get("currency_code","SAR"),
            "cp":       inst.get("counterpart_account"),
            "desc":     f"[{inst['rec_name']}] قسط {inst['instance_number']} — {inst.get('description','')}",
            "pm":       "cash" if source == "cash" else "wire",
            "branch":   inst.get("branch_code"),
            "cc":       inst.get("cost_center"),
            "proj":     inst.get("project_code"),
            "exp_cls":  inst.get("expense_classification_code"),
            "by":       user.email,
        })
        await db.execute(text("""
            UPDATE tr_recurring_instances
            SET status='draft', tx_id=:tx_id, tx_serial=:serial,
                tx_table=:tbl, executed_by=:by, executed_at=NOW()
            WHERE id=:id
        """), {"tx_id": tx_id, "serial": serial, "tbl": tx_tbl,
               "by": user.email, "id": str(inst_id)})
        # next_run_date
        r2 = await db.execute(text("""
            SELECT due_date FROM tr_recurring_instances
            WHERE recurring_id=:rid AND status='pending'
            ORDER BY instance_number LIMIT 1
        """), {"rid": str(inst["recurring_id"])})
        nxt = r2.mappings().fetchone()
        await db.execute(text("""
            UPDATE tr_recurring_transactions
            SET last_run_date=:last, next_run_date=:next
            WHERE id=:rid AND tenant_id=:tid
        """), {"last": tx_date, "next": nxt["due_date"] if nxt else None,
               "rid": str(inst["recurring_id"]), "tid": tid})
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={"serial": serial, "tx_id": tx_id},
              message=f"✅ سند مسودة {serial}")


@router.post("/recurring-instances/{inst_id}/skip")
async def skip_instance(
    inst_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ri.* FROM tr_recurring_instances ri
        JOIN tr_recurring_transactions rt ON rt.id = ri.recurring_id
        WHERE ri.id=:id AND rt.tenant_id=:tid
    """), {"id": str(inst_id), "tid": tid})
    inst = r.mappings().fetchone()
    if not inst: raise HTTPException(404, "القسط غير موجود")
    if inst["status"] != "pending":
        raise HTTPException(400, "لا يمكن تخطي هذا القسط")
    await db.execute(text("""
        UPDATE tr_recurring_instances
        SET status='skipped', skipped_by=:by, skipped_at=NOW(), skip_reason=:reason
        WHERE id=:id
    """), {"by": user.email, "reason": data.get("reason",""), "id": str(inst_id)})
    await db.commit()
    return ok(data={}, message="تم تخطي القسط")


@router.post("/recurring-transactions/{rec_id}/execute")
async def execute_recurring_legacy(
    rec_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تنفيذ القسط التالي المعلق — للتوافق مع الكود القديم"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ri.id FROM tr_recurring_instances ri
        JOIN tr_recurring_transactions rt ON rt.id = ri.recurring_id
        WHERE ri.recurring_id=:rid AND rt.tenant_id=:tid AND ri.status='pending'
        ORDER BY ri.instance_number LIMIT 1
    """), {"rid": str(rec_id), "tid": tid})
    inst = r.mappings().fetchone()
    if not inst: raise HTTPException(400, "لا يوجد قسط معلق للتنفيذ")
    return await execute_instance(inst["id"], db, user)


# ══════════════════════════════════════════════════════════
# AUTHORITY MATRIX — مصفوفة التفويض / موقعو الشيكات
# ══════════════════════════════════════════════════════════

@router.get("/signatories")
async def list_signatories(
    bank_account_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        where = ["s.tenant_id=:tid"]
        params = {"tid": tid}
        if bank_account_id:
            where.append("s.bank_account_id=:ba_id")
            params["ba_id"] = bank_account_id
        r = await db.execute(text("""
            SELECT s.*, ba.account_name AS bank_name, ba.account_code AS bank_label
            FROM cheque_signatories s
            LEFT JOIN tr_bank_accounts ba ON ba.id = s.bank_account_id
            WHERE """ + " AND ".join(where) + """
            ORDER BY ba.account_name, s.signatory_name
        """), params)
        rows = [dict(row._mapping) for row in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        import traceback; print(f"[signatories] {traceback.format_exc()}")
        return ok(data=[], message="جدول التفويضات غير موجود — شغّل migration الشيكات")


@router.post("/signatories")
async def create_signatory(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    sig_id = str(uuid.uuid4())
    try:
        await db.execute(text("""
            INSERT INTO cheque_signatories
              (id, tenant_id, bank_account_id, signatory_name, signatory_title,
               signatory_email, authorization_type, min_amount, max_amount,
               valid_from, valid_to, is_active, notes, created_by)
            VALUES
              (:id, :tid, :ba_id, :name, :title,
               :email, :auth_type, :min_amt, :max_amt,
               :valid_from, :valid_to, :is_active, :notes, :by)
        """), {
            "id":        sig_id,
            "tid":       tid,
            "ba_id":     data.get("bank_account_id") or None,
            "name":      data.get("signatory_name",""),
            "title":     data.get("signatory_title",""),
            "email":     data.get("signatory_email",""),
            "auth_type": data.get("authorization_type","single"),
            "min_amt":   Decimal(str(data.get("min_amount",0))),
            "max_amt":   Decimal(str(data["max_amount"])) if data.get("max_amount") not in (None,"") else None,
            "valid_from":date.fromisoformat(str(data["valid_from"])) if data.get("valid_from") else date.today(),
            "valid_to":  date.fromisoformat(str(data["valid_to"])) if data.get("valid_to") else None,
            "is_active": data.get("is_active", True),
            "notes":     data.get("notes",""),
            "by":        user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ: {str(e)}")
    return created(data={"id": sig_id}, message="تمت إضافة الموقع ✅")


@router.put("/signatories/{sig_id}")
async def update_signatory(
    sig_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ALLOWED = {"bank_account_id","signatory_name","signatory_title","signatory_email",
               "authorization_type","min_amount","max_amount","valid_from","valid_to",
               "is_active","notes"}
    safe = {k:v for k,v in data.items() if k in ALLOWED}
    if "min_amount" in safe: safe["min_amount"] = Decimal(str(safe["min_amount"]))
    if "max_amount" in safe: safe["max_amount"] = Decimal(str(safe["max_amount"])) if safe["max_amount"] not in (None,"") else None
    if "valid_from" in safe and safe["valid_from"]: safe["valid_from"] = date.fromisoformat(str(safe["valid_from"]))
    if "valid_to"   in safe and safe["valid_to"]:   safe["valid_to"]   = date.fromisoformat(str(safe["valid_to"]))
    if not safe: raise HTTPException(400, "لا بيانات للتعديل")
    safe["updated_at"] = datetime.utcnow()
    set_clause = ", ".join([f"{k}=:{k}" for k in safe.keys()])
    safe.update({"id": str(sig_id), "tid": tid})
    try:
        await db.execute(text(
            f"UPDATE cheque_signatories SET {set_clause} WHERE id=:id AND tenant_id=:tid"
        ), safe)
        await db.commit()
    except Exception as e:
        await db.rollback(); raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={"id": str(sig_id)}, message="تم التعديل ✅")


@router.delete("/signatories/{sig_id}")
async def delete_signatory(
    sig_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        await db.execute(text(
            "DELETE FROM cheque_signatories WHERE id=:id AND tenant_id=:tid"
        ), {"id": str(sig_id), "tid": tid})
        await db.commit()
    except Exception as e:
        await db.rollback(); raise HTTPException(400, f"خطأ: {str(e)}")
    return ok(data={}, message="تم الحذف ✅")



# ══════════════════════════════════════════════════════════
# INVENTORY REPORTS — Additional
# ══════════════════════════════════════════════════════════

@router.get("/reports/cogs")
async def cogs_report(
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تحليل تكلفة البضاعة المباعة من دفتر الأستاذ"""
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid","l.qty_out>0"]
    params: dict = {"tid": tid}
    if date_from: conds.append("l.tx_date>=:df"); params["df"] = str(date_from)
    if date_to:   conds.append("l.tx_date<=:dt"); params["dt"] = str(date_to)
    where = " AND ".join(conds)
    try:
        r = await db.execute(text(f"""
            SELECT i.item_code, i.item_name,
                   SUM(l.qty_out)           AS qty_sold,
                   AVG(l.unit_cost)         AS avg_cost,
                   SUM(l.total_cost)        AS total_cogs
            FROM inv_ledger l
            JOIN inv_items i ON i.id=l.item_id
            WHERE {where}
            GROUP BY i.id, i.item_code, i.item_name
            ORDER BY total_cogs DESC
        """), params)
        rows = [{**dict(row._mapping),
                 "qty_sold":   float(row._mapping["qty_sold"]  or 0),
                 "avg_cost":   float(row._mapping["avg_cost"]  or 0),
                 "total_cogs": float(row._mapping["total_cogs"]or 0)}
                for row in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/turnover")
async def turnover_report(
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """معدل دوران المخزون لكل صنف"""
    tid = str(user.tenant_id)
    conds = ["l.tenant_id=:tid","l.qty_out>0"]
    params: dict = {"tid": tid}
    if date_from: conds.append("l.tx_date>=:df"); params["df"] = str(date_from)
    if date_to:   conds.append("l.tx_date<=:dt"); params["dt"] = str(date_to)
    where = " AND ".join(conds)
    try:
        r = await db.execute(text(f"""
            SELECT i.item_code, i.item_name,
                   SUM(l.total_cost)               AS cogs,
                   AVG(b.total_value)               AS avg_inventory,
                   CASE WHEN AVG(b.total_value)>0
                     THEN SUM(l.total_cost)/AVG(b.total_value)
                     ELSE 0 END                     AS turnover_rate,
                   CASE WHEN SUM(l.total_cost)>0
                     THEN ROUND(AVG(b.total_value)/SUM(l.total_cost)*365)
                     ELSE NULL END                  AS days_in_stock
            FROM inv_ledger l
            JOIN inv_items i ON i.id=l.item_id
            LEFT JOIN inv_balances b ON b.item_id=i.id AND b.tenant_id=:tid
            WHERE {where}
            GROUP BY i.id, i.item_code, i.item_name
            ORDER BY turnover_rate DESC
        """), params)
        items = [{**dict(row._mapping),
                  "cogs":          float(row._mapping["cogs"]         or 0),
                  "avg_inventory": float(row._mapping["avg_inventory"] or 0),
                  "turnover_rate": float(row._mapping["turnover_rate"] or 0)}
                 for row in r.fetchall()]
        total_cogs = sum(i["cogs"] for i in items)
        total_inv  = sum(i["avg_inventory"] for i in items)
        return ok(data={
            "items": items,
            "summary": {
                "overall_turnover": round(total_cogs/total_inv, 2) if total_inv>0 else 0,
                "total_cogs":       round(total_cogs, 2),
                "avg_inventory_value": round(total_inv, 2),
            }
        })
    except Exception as e:
        return ok(data={"items":[], "summary":{}}, message=str(e))


@router.get("/reports/variance")
async def variance_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """فروقات الجرد من جلسات العد المرحّلة"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT s.serial, s.count_date, s.warehouse_name,
                   s.lines_with_variance, s.total_variance_value
            FROM inv_count_sessions s
            WHERE s.tenant_id=:tid AND s.status='posted'
            ORDER BY s.count_date DESC
        """), {"tid": tid})
        sessions = [dict(row._mapping) for row in r.fetchall()]
        # جلب أسطر الفروقات لكل جلسة
        for sess in sessions:
            r2 = await db.execute(text("""
                SELECT l.item_code, l.item_name, l.system_qty, l.actual_qty,
                       l.variance, l.variance_value
                FROM inv_count_lines l
                JOIN inv_count_sessions s ON s.id=l.session_id
                WHERE s.serial=:serial AND s.tenant_id=:tid AND l.variance!=0
            """), {"serial": sess["serial"], "tid": tid})
            sess["lines"] = [dict(row._mapping) for row in r2.fetchall()]
        return ok(data=sessions)
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/negative-stock")
async def negative_stock_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """الأصناف ذات المخزون السالب"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT i.item_code, i.item_name, u.uom_name,
                   w.warehouse_name, b.qty_on_hand, b.avg_cost, b.total_value
            FROM inv_balances b
            JOIN inv_items i ON i.id=b.item_id
            LEFT JOIN inv_uom u ON u.id=i.uom_id
            JOIN inv_warehouses w ON w.id=b.warehouse_id
            WHERE b.tenant_id=:tid AND b.qty_on_hand < 0
            ORDER BY b.qty_on_hand ASC
        """), {"tid": tid})
        rows = [{**dict(row._mapping),
                 "qty_on_hand": float(row._mapping["qty_on_hand"] or 0),
                 "total_value": float(row._mapping["total_value"] or 0)}
                for row in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/expiry")
async def expiry_report(
    days_ahead: int = Query(default=90),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """الأصناف القريبة من انتهاء الصلاحية (من أسطر الحركات)"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT DISTINCT i.item_code, i.item_name, u.uom_name,
                   w.warehouse_name, tl.lot_number, tl.expiry_date,
                   tl.qty AS qty_on_hand,
                   (tl.expiry_date - CURRENT_DATE) AS days_to_expiry
            FROM inv_transaction_lines tl
            JOIN inv_transactions t  ON t.id=tl.tx_id AND t.tenant_id=:tid AND t.status='posted'
            JOIN inv_items i         ON i.id=tl.item_id
            LEFT JOIN inv_uom u      ON u.id=i.uom_id
            JOIN inv_warehouses w    ON w.id=t.to_warehouse_id
            WHERE tl.expiry_date IS NOT NULL
              AND tl.expiry_date <= CURRENT_DATE + (:days * INTERVAL '1 day')
            ORDER BY tl.expiry_date ASC
        """), {"tid": tid, "days": days_ahead})
        rows = [{**dict(row._mapping),
                 "days_to_expiry": int(row._mapping["days_to_expiry"] or 0)}
                for row in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))

# ══════════════════════════════════════════════════════════
# SMART BANK IMPORT — استيراد كشف البنك الذكي
# يقرأ Excel ← ينشئ سندات مسودة PAY/REC بحسابات وسيطة
# ══════════════════════════════════════════════════════════

@router.post("/smart-import/preview")
async def smart_import_preview(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """يستقبل JSON بصفوف كشف البنك ويُنشئ معاينة القيود قبل الحفظ"""
    tid  = str(user.tenant_id)
    body = await request.json()

    rows            = body.get("rows", [])
    bank_account_id = body.get("bank_account_id")
    col_date        = body.get("col_date",   "date")
    col_desc        = body.get("col_desc",   "description")
    col_debit       = body.get("col_debit",  "debit")
    col_credit      = body.get("col_credit", "credit")
    transit_pay     = body.get("transit_pay_account",  "")   # حساب دفعات تحت التسوية
    transit_rec     = body.get("transit_rec_account",  "")   # حساب مقبوضات تحت التسوية

    if not rows:
        raise HTTPException(400, "لا توجد صفوف بيانات")
    if not bank_account_id:
        raise HTTPException(400, "يرجى تحديد الحساب البنكي")

    # نجلب بيانات الحساب البنكي
    r = await db.execute(text("""
        SELECT id, account_name, gl_account_code
        FROM tr_bank_accounts
        WHERE id=:id AND tenant_id=:tid
    """), {"id": bank_account_id, "tid": tid})
    bank = r.mappings().fetchone()
    if not bank:
        raise HTTPException(404, "الحساب البنكي غير موجود")

    bank_gl   = bank["gl_account_code"]
    bank_name = bank["account_name"]

    # نُنشئ معاينة لكل صف
    preview = []
    pay_seq = 1
    rec_seq = 1
    today   = date.today()
    year    = today.year

    for idx, row in enumerate(rows):
        raw_date   = str(row.get(col_date,   "")).strip()
        desc       = str(row.get(col_desc,   "")).strip() or f"حركة بنكية {idx+1}"
        debit_raw  = str(row.get(col_debit,  "") or "").replace(",","").strip()
        credit_raw = str(row.get(col_credit, "") or "").replace(",","").strip()

        # تحويل الأرقام
        try: debit  = float(debit_raw)  if debit_raw  else 0.0
        except: debit = 0.0
        try: credit = float(credit_raw) if credit_raw else 0.0
        except: credit = 0.0

        if debit <= 0 and credit <= 0:
            continue  # تجاهل الصفوف الفارغة

        # تحويل التاريخ
        tx_date = today
        for fmt_str in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%m/%d/%Y","%Y/%m/%d"):
            try:
                from datetime import datetime as dt
                tx_date = dt.strptime(raw_date, fmt_str).date()
                year = tx_date.year
                break
            except: pass

        if debit > 0:
            # خروج من البنك → سند دفع PAY
            serial   = f"PAY-{year}-{pay_seq}"
            pay_seq += 1
            entry = {
                "row_idx":    idx,
                "serial":     serial,
                "direction":  "out",
                "tx_type":    "BSI",
                "tx_date":    str(tx_date),
                "description": desc,
                "amount":     round(debit, 3),
                "bank_account_id": bank_account_id,
                "bank_name":  bank_name,
                "bank_gl":    bank_gl,
                "debit_account":  transit_pay or "",   # ح/ دفعات تحت التسوية ← المحاسب يعدّله
                "credit_account": bank_gl or "",       # ح/ البنك
                "debit_name":  "دفعات تحت التسوية" if transit_pay else "⚠️ حدد الحساب",
                "credit_name": bank_name,
                "tx_subtype":  "expense",              # المحاسب يختار: expense / vendor / other
                "vendor_id":   None,
                "status":      "pending",
            }
        else:
            # دخول للبنك → سند قبض REC
            serial   = f"COL-{year}-{rec_seq}"
            rec_seq += 1
            entry = {
                "row_idx":    idx,
                "serial":     serial,
                "direction":  "in",
                "tx_type":    "BSI",
                "tx_date":    str(tx_date),
                "description": desc,
                "amount":     round(credit, 3),
                "bank_account_id": bank_account_id,
                "bank_name":  bank_name,
                "bank_gl":    bank_gl,
                "debit_account":  bank_gl or "",         # ح/ البنك
                "credit_account": transit_rec or "",     # ح/ مقبوضات تحت التسوية ← المحاسب يعدّله
                "debit_name":  bank_name,
                "credit_name": "مقبوضات تحت التسوية" if transit_rec else "⚠️ حدد الحساب",
                "tx_subtype":  "receipt",
                "customer_id": None,
                "status":      "pending",
            }
        preview.append(entry)

    return ok(data={
        "preview": preview,
        "summary": {
            "total_rows":   len(rows),
            "valid_rows":   len(preview),
            "payments":     sum(1 for p in preview if p["direction"]=="out"),
            "receipts":     sum(1 for p in preview if p["direction"]=="in"),
            "total_out":    round(sum(p["amount"] for p in preview if p["direction"]=="out"),3),
            "total_in":     round(sum(p["amount"] for p in preview if p["direction"]=="in"),3),
        }
    })


@router.post("/smart-import/create-drafts")
async def smart_import_create_drafts(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """ينشئ سندات BP/BR مسودة من الصفوف المراجَعة"""
    tid  = str(user.tenant_id)
    body = await request.json()
    rows = body.get("rows", [])

    if not rows:
        raise HTTPException(400, "لا توجد صفوف")

    created, errors = [], []

    for row in rows:
        try:
            tx_date  = date.fromisoformat(str(row["tx_date"]))
            amt      = Decimal(str(row["amount"]))
            tx_type  = row.get("tx_type", "BP")

            # نستخدم serial من الـ preview دائماً (PAY/COL format)
            serial = row.get("serial")
            if not serial:
                direction = row.get("direction","out")
                pfx = "PAY" if direction=="out" else "COL"
                serial = await _next_serial(db, tid, pfx, tx_date)
            tx_id    = str(uuid.uuid4())

            await db.execute(text("""
                INSERT INTO tr_bank_transactions
                  (id,tenant_id,serial,tx_type,tx_date,bank_account_id,
                   amount,currency_code,amount_sar,
                   counterpart_account,
                   description,reference,payment_method,
                   branch_code,cost_center,project_code,
                   status,created_by)
                VALUES
                  (:id,:tid,:serial,:tx_type,:tx_date,:ba,
                   :amt,'SAR',:amt,
                   :cp_acc,
                   :desc,:ref,'bank_import',
                   :branch,:cc,:proj,
                   'draft',:by)
            """), {
                "id":      tx_id,
                "tid":     tid,
                "serial":  serial,
                "tx_type": tx_type,
                "tx_date": tx_date,
                "ba":      row.get("bank_account_id"),
                "amt":     amt,
                "cp_acc":  row.get("credit_account") if tx_type=="BR" else row.get("debit_account"),
                "desc":    row.get("description","") or ("دفعة مستوردة" if row.get("direction","out")=="out" else "تحصيل مستورد"),
                "ref":     row.get("reference",""),
                "branch":  row.get("branch_code"),
                "cc":      row.get("cost_center"),
                "proj":    row.get("project_code"),
                "by":      user.email,
            })
            created.append({"tx_id": tx_id, "serial": serial, "tx_type": tx_type})

        except Exception as e:
            await db.rollback()
            errors.append({"row": row.get("serial","?"), "error": str(e)})
            continue

    if created:
        await db.commit()

    return ok(
        data={"created": created, "errors": errors},
        message=f"✅ تم إنشاء {len(created)} سند مسودة" +
                (f" | ⚠️ {len(errors)} فشل" if errors else "")
    )




@router.get("/smart-import/imported")
async def list_imported_transactions(
    bank_account_id: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """قائمة السندات المستوردة من كشف البنك (BSI)"""
    tid = str(user.tenant_id)
    conds = ["bt.tenant_id=:tid", "bt.payment_method='bank_import'"]
    params: dict = {"tid": tid}
    if bank_account_id:
        conds.append("bt.bank_account_id=:ba")
        params["ba"] = bank_account_id
    try:
        r = await db.execute(text(f"""
            SELECT bt.id, bt.serial, bt.tx_type, bt.tx_date::text,
                   bt.amount, bt.description, bt.status,
                   bt.counterpart_account,
                   ba.account_name AS bank_account_name,
                   bt.created_at::text, bt.je_serial,
                   CASE WHEN bt.serial LIKE 'PAY%%' THEN 'out' ELSE 'in' END AS direction
            FROM tr_bank_transactions bt
            LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
            WHERE {" AND ".join(conds)}
            ORDER BY bt.tx_date DESC, bt.serial DESC
            LIMIT 500
        """), params)
        rows = [dict(r._mapping) for r in r.fetchall()]
        summary = {
            "total":    len(rows),
            "payments": sum(1 for r in rows if r["direction"]=="out"),
            "receipts": sum(1 for r in rows if r["direction"]=="in"),
            "total_out":round(sum(float(r["amount"]) for r in rows if r["direction"]=="out"),2),
            "total_in": round(sum(float(r["amount"]) for r in rows if r["direction"]=="in"),2),
            "posted":   sum(1 for r in rows if r["status"]=="posted"),
            "draft":    sum(1 for r in rows if r["status"]=="draft"),
        }
        return ok(data={"items": rows, "summary": summary})
    except Exception as e:
        import traceback; print(f"[smart-import/imported] {traceback.format_exc()}")
        return ok(data={"items":[], "summary":{}})

@router.get("/smart-import/settings")
async def get_smart_import_settings(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إعدادات الحسابات الوسيطة للاستيراد الذكي"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT
                transit_pay_account  AS smart_import_transit_pay,
                transit_rec_account  AS smart_import_transit_rec,
                transit_pay_name     AS smart_import_transit_pay_name,
                transit_rec_name     AS smart_import_transit_rec_name
            FROM smart_import_settings
            WHERE tenant_id=:tid
            LIMIT 1
        """), {"tid": tid})
        row = r.mappings().fetchone()
        if not row:
            return ok(data={
                "smart_import_transit_pay": None,
                "smart_import_transit_rec": None,
                "smart_import_transit_pay_name": None,
                "smart_import_transit_rec_name": None,
            })
        return ok(data=dict(row))
    except Exception as e:
        import traceback; print(f"[smart-import/settings GET] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في جلب الإعدادات: {str(e)}")


@router.post("/smart-import/settings")
async def save_smart_import_settings(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حفظ إعدادات الحسابات الوسيطة"""
    tid = str(user.tenant_id)
    try:
        await db.execute(text("""
            INSERT INTO smart_import_settings
                (id, tenant_id, transit_pay_account, transit_rec_account,
                 transit_pay_name, transit_rec_name, updated_at)
            VALUES
                (gen_random_uuid(), :tid, :pay, :rec, :pay_name, :rec_name, NOW())
            ON CONFLICT (tenant_id) DO UPDATE SET
                transit_pay_account = EXCLUDED.transit_pay_account,
                transit_rec_account = EXCLUDED.transit_rec_account,
                transit_pay_name    = EXCLUDED.transit_pay_name,
                transit_rec_name    = EXCLUDED.transit_rec_name,
                updated_at          = NOW()
        """), {
            "tid":      tid,
            "pay":      data.get("smart_import_transit_pay") or None,
            "rec":      data.get("smart_import_transit_rec") or None,
            "pay_name": data.get("smart_import_transit_pay_name") or None,
            "rec_name": data.get("smart_import_transit_rec_name") or None,
        })
        await db.commit()
        return ok(data={}, message="✅ تم حفظ الإعدادات")
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[smart-import/settings POST] {traceback.format_exc()}")
        raise HTTPException(500, f"خطأ في حفظ الإعدادات: {str(e)}")

# ══════════════════════════════════════════════════════════
# ACTIVITY LOG — سجل الأحداث والـ Audit Trail للخزينة
# ══════════════════════════════════════════════════════════

@router.get("/activity-log")
async def treasury_activity_log(
    date_from:  Optional[str] = Query(default=None),
    date_to:    Optional[str] = Query(default=None),
    tx_type:    Optional[str] = Query(default=None),
    action:     Optional[str] = Query(default=None),
    user_email: Optional[str] = Query(default=None),
    limit:      int           = Query(default=100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """سجل جميع أحداث الخزينة: إنشاء، ترحيل، عكس، اعتماد، رفض"""
    tid = str(user.tenant_id)

    # نجمع من جميع جداول الخزينة
    where_cash  = ["ct.tenant_id=:tid"]
    where_bank  = ["bt.tenant_id=:tid"]
    where_it    = ["it.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit}

    if date_from:
        where_cash.append("ct.tx_date>=:df"); where_bank.append("bt.tx_date>=:df")
        where_it.append("it.tx_date>=:df");   params["df"] = date_from
    if date_to:
        where_cash.append("ct.tx_date<=:dt"); where_bank.append("bt.tx_date<=:dt")
        where_it.append("it.tx_date<=:dt");   params["dt"] = date_to
    if tx_type:
        where_cash.append("ct.tx_type=:tt"); where_bank.append("bt.tx_type=:tt")
        params["tt"] = tx_type
    if user_email:
        where_cash.append("(ct.created_by=:ue OR ct.posted_by=:ue)")
        where_bank.append("(bt.created_by=:ue OR bt.posted_by=:ue)")
        params["ue"] = user_email

    wc = " AND ".join(where_cash)
    wb = " AND ".join(where_bank)
    wi = " AND ".join(where_it)

    r = await db.execute(text(f"""
        SELECT * FROM (
            -- سندات نقدية
            SELECT
                ct.id, ct.serial, ct.tx_type, ct.tx_date::text,
                ct.amount, ct.status, ct.description,
                ct.created_by, ct.created_at,
                ct.posted_by, ct.posted_at,
                ct.approved_by, ct.approved_at,
                ct.rejected_by, ct.rejected_at,
                ct.reversed_by, ct.reversed_at,
                ct.je_serial,
                ba.account_name AS bank_account_name,
                'cash' AS source_table
            FROM tr_cash_transactions ct
            LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
            WHERE {wc}

            UNION ALL

            -- معاملات بنكية
            SELECT
                bt.id, bt.serial, bt.tx_type, bt.tx_date::text,
                bt.amount, bt.status, bt.description,
                bt.created_by, bt.created_at,
                bt.posted_by, bt.posted_at,
                bt.approved_by, bt.approved_at,
                bt.rejected_by, bt.rejected_at,
                NULL AS reversed_by, NULL AS reversed_at,
                bt.je_serial,
                ba2.account_name AS bank_account_name,
                'bank' AS source_table
            FROM tr_bank_transactions bt
            LEFT JOIN tr_bank_accounts ba2 ON ba2.id=bt.bank_account_id
            WHERE {wb}

            UNION ALL

            -- تحويلات داخلية
            SELECT
                it.id, it.serial, 'IT'::text AS tx_type, it.tx_date::text,
                it.amount, it.status, it.description,
                it.created_by, it.created_at,
                it.posted_by, it.posted_at,
                NULL AS approved_by, NULL AS approved_at,
                NULL AS rejected_by, NULL AS rejected_at,
                NULL AS reversed_by, NULL AS reversed_at,
                it.je_serial,
                fa.account_name AS bank_account_name,
                'transfer' AS source_table
            FROM tr_internal_transfers it
            LEFT JOIN tr_bank_accounts fa ON fa.id=it.from_account_id
            WHERE {wi}
        ) t
        ORDER BY created_at DESC NULLS LAST
        LIMIT :limit
    """), params)

    rows = []
    for row in r.mappings().fetchall():
        r_dict = dict(row)
        # نبني timeline الأحداث لكل سند
        events = []
        if r_dict.get("created_at"):
            events.append({"action": "created",  "by": r_dict.get("created_by"),  "at": str(r_dict.get("created_at","")),  "label": "تم الإنشاء", "color": "blue"})
        if r_dict.get("approved_at"):
            events.append({"action": "approved", "by": r_dict.get("approved_by"), "at": str(r_dict.get("approved_at","")), "label": "تم الاعتماد", "color": "emerald"})
        if r_dict.get("posted_at"):
            events.append({"action": "posted",   "by": r_dict.get("posted_by"),   "at": str(r_dict.get("posted_at","")),   "label": "تم الترحيل", "color": "green"})
        if r_dict.get("rejected_at"):
            events.append({"action": "rejected", "by": r_dict.get("rejected_by"), "at": str(r_dict.get("rejected_at","")), "label": "تم الرفض",   "color": "red"})
        if r_dict.get("reversed_at"):
            events.append({"action": "reversed", "by": r_dict.get("reversed_by"), "at": str(r_dict.get("reversed_at","")), "label": "تم العكس",   "color": "orange"})

        if r_dict.get("amount"): r_dict["amount"] = float(r_dict["amount"])
        r_dict["events"] = sorted(events, key=lambda x: x["at"])
        rows.append(r_dict)

    # إحصائيات
    stats = {
        "total":    len(rows),
        "posted":   sum(1 for r in rows if r["status"]=="posted"),
        "draft":    sum(1 for r in rows if r["status"]=="draft"),
        "reversed": sum(1 for r in rows if r["status"]=="reversed"),
        "pending":  sum(1 for r in rows if r["status"]=="pending_approval"),
    }
    return ok(data={"rows": rows, "stats": stats})


# ══════════════════════════════════════════════════════════
# CASH FLOW REPORT — تقرير التدفقات النقدية الرسمي
# ══════════════════════════════════════════════════════════

@router.get("/reports/cash-flow-statement")
async def cash_flow_statement(
    year:       int           = Query(default=2026),
    date_from:  Optional[str] = Query(default=None),
    date_to:    Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تقرير التدفقات النقدية الرسمي — Cash In / Cash Out / Net + Forecast"""
    tid = str(user.tenant_id)

    df = date_from or f"{year}-01-01"
    dt = date_to   or f"{year}-12-31"
    params = {"tid": tid, "df": df, "dt": dt}

    # ── 1. التدفقات التشغيلية (Operating) ─────────────────
    r_op = await db.execute(text("""
        SELECT
            EXTRACT(MONTH FROM tx_date) AS month,
            TO_CHAR(tx_date,'YYYY-MM')  AS period,
            -- نقدي
            COALESCE(SUM(CASE WHEN tx_type='RV' AND source='cash' THEN amount ELSE 0 END),0) AS cash_in_receipts,
            COALESCE(SUM(CASE WHEN tx_type='PV' AND source='cash' THEN amount ELSE 0 END),0) AS cash_out_payments,
            -- بنكي
            COALESCE(SUM(CASE WHEN tx_type='BR' AND source='bank' THEN amount ELSE 0 END),0) AS bank_in,
            COALESCE(SUM(CASE WHEN tx_type IN ('BP','BT') AND source='bank' THEN amount ELSE 0 END),0) AS bank_out
        FROM (
            SELECT tx_type, tx_date, amount, 'cash' AS source
            FROM tr_cash_transactions
            WHERE tenant_id=:tid AND status='posted' AND tx_date BETWEEN :df AND :dt
            UNION ALL
            SELECT tx_type, tx_date, amount, 'bank' AS source
            FROM tr_bank_transactions
            WHERE tenant_id=:tid AND status='posted' AND tx_date BETWEEN :df AND :dt
        ) t
        GROUP BY EXTRACT(MONTH FROM tx_date), TO_CHAR(tx_date,'YYYY-MM')
        ORDER BY period
    """), params)

    monthly = []
    for row in r_op.mappings().fetchall():
        cash_in  = float(row["cash_in_receipts"]) + float(row["bank_in"])
        cash_out = float(row["cash_out_payments"]) + float(row["bank_out"])
        monthly.append({
            "month":    int(row["month"]),
            "period":   row["period"],
            "cash_in":  round(cash_in, 3),
            "cash_out": round(cash_out, 3),
            "net":      round(cash_in - cash_out, 3),
            # تفصيل
            "receipts":  round(float(row["cash_in_receipts"]), 3),
            "payments":  round(float(row["cash_out_payments"]), 3),
            "bank_in":   round(float(row["bank_in"]), 3),
            "bank_out":  round(float(row["bank_out"]), 3),
        })

    # ── 2. الرصيد الافتتاحي والختامي ─────────────────────
    r_bal = await db.execute(text("""
        SELECT COALESCE(SUM(current_balance),0) AS current_total
        FROM tr_bank_accounts WHERE tenant_id=:tid AND is_active=true
    """), {"tid": tid})
    current_balance = float(r_bal.scalar() or 0)

    total_in  = sum(m["cash_in"]  for m in monthly)
    total_out = sum(m["cash_out"] for m in monthly)
    net_flow  = total_in - total_out

    # ── 3. التوقعات (Forecast) — المسودات القائمة ─────────
    r_fc = await db.execute(text("""
        SELECT
            COALESCE(SUM(CASE WHEN tx_type IN ('RV','BR') THEN amount ELSE 0 END),0) AS expected_in,
            COALESCE(SUM(CASE WHEN tx_type IN ('PV','BP','BT') THEN amount ELSE 0 END),0) AS expected_out
        FROM (
            SELECT tx_type, amount FROM tr_cash_transactions
            WHERE tenant_id=:tid AND status='draft'
            UNION ALL
            SELECT tx_type, amount FROM tr_bank_transactions
            WHERE tenant_id=:tid AND status='draft'
        ) t
    """), {"tid": tid})
    fc = r_fc.mappings().fetchone()
    forecast = {
        "expected_in":       round(float(fc["expected_in"]), 3),
        "expected_out":      round(float(fc["expected_out"]), 3),
        "expected_net":      round(float(fc["expected_in"]) - float(fc["expected_out"]), 3),
        "forecast_balance":  round(current_balance + float(fc["expected_in"]) - float(fc["expected_out"]), 3),
    }

    # ── 4. تحليل ربعي ─────────────────────────────────────
    quarters = []
    for q in range(1, 5):
        months = [m for m in monthly if (m["month"]-1)//3 + 1 == q]
        if months:
            quarters.append({
                "quarter": f"Q{q}",
                "cash_in":  round(sum(m["cash_in"]  for m in months), 3),
                "cash_out": round(sum(m["cash_out"] for m in months), 3),
                "net":      round(sum(m["net"]       for m in months), 3),
            })

    return ok(data={
        "period":           {"from": df, "to": dt, "year": year},
        "summary": {
            "total_cash_in":  round(total_in, 3),
            "total_cash_out": round(total_out, 3),
            "net_flow":       round(net_flow, 3),
            "current_balance":current_balance,
        },
        "monthly":   monthly,
        "quarterly": quarters,
        "forecast":  forecast,
    })
