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
from fastapi import APIRouter, Depends, Query, HTTPException, Body
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


async def _post_je(db, tid: str, user_email: str, je_type: str, tx_date: date,
                   description: str, lines: list, reference: str = None) -> dict:
    """إنشاء قيد محاسبي مرتبط بعملية الخزينة"""
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
    ) for l in lines]
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
        raise HTTPException(400, "فشل إنشاء القيد المحاسبي — تحقق من أكواد الحسابات")
    return {"je_id": str(result.je_id), "je_serial": result.je_serial}


async def _update_balance(db, bank_account_id: str, delta: Decimal):
    await db.execute(text("""
        UPDATE tr_bank_accounts
        SET current_balance = current_balance + :delta, updated_at = NOW()
        WHERE id = :id
    """), {"delta": delta, "id": bank_account_id})


async def _check_period(db: AsyncSession, tid: str, tx_date: date):
    """
    حارس الفترة المحاسبية — يرفع 422 إذا كانت الفترة مغلقة أو غير موجودة
    استدعِه في بداية كل endpoint يُنشئ قيوداً مالية
    """
    r = await db.execute(text("""
        SELECT status, period_name FROM fiscal_periods
        WHERE tenant_id = :tid
          AND start_date <= :dt AND end_date >= :dt
        LIMIT 1
    """), {"tid": tid, "dt": tx_date})
    row = r.fetchone()
    if not row:
        raise HTTPException(
            status_code=422,
            detail=f"لا توجد فترة مالية للتاريخ {tx_date} — يرجى إنشاء فترة أو مراجعة مدير النظام"
        )
    if row[0] != "open":
        raise HTTPException(
            status_code=422,
            detail=f"الفترة المالية '{row[1]}' مغلقة — لا يمكن تسجيل قيود في فترة مغلقة"
        )


# ══════════════════════════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════════════════════════
@router.get("/dashboard")
async def dashboard(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    # أرصدة البنوك والصناديق
    r = await db.execute(text("""
        SELECT id, account_code, account_name, account_type,
               currency_code, current_balance, low_balance_alert, is_active
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY account_type, account_name
    """), {"tid": tid})
    accounts = [dict(row._mapping) for row in r.fetchall()]

    # إجماليات
    total_bank = sum(float(a["current_balance"]) for a in accounts if a["account_type"]=="bank")
    total_cash = sum(float(a["current_balance"]) for a in accounts if a["account_type"]=="cash_fund")
    alerts = [a for a in accounts if float(a["current_balance"]) <= float(a["low_balance_alert"] or 0)]

    # حركات اليوم
    today = date.today().isoformat()
    r2 = await db.execute(text("""
        SELECT COALESCE(SUM(amount),0) AS total
        FROM tr_cash_transactions
        WHERE tenant_id=:tid AND tx_date=:today AND tx_type='RV' AND status='posted'
    """), {"tid": tid, "today": today})
    today_receipts = float(r2.scalar() or 0)

    r3 = await db.execute(text("""
        SELECT COALESCE(SUM(amount),0) AS total
        FROM tr_cash_transactions
        WHERE tenant_id=:tid AND tx_date=:today AND tx_type='PV' AND status='posted'
    """), {"tid": tid, "today": today})
    today_payments = float(r3.scalar() or 0)

    # الشيكات المستحقة (خلال 7 أيام)
    r4 = await db.execute(text("""
        SELECT COUNT(*), COALESCE(SUM(amount),0)
        FROM tr_checks
        WHERE tenant_id=:tid AND status='issued'
          AND due_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7
    """), {"tid": tid})
    ck = r4.fetchone()
    due_checks = {"count": ck[0], "total": float(ck[1])}

    # عهد تحتاج تعبئة
    r5 = await db.execute(text("""
        SELECT COUNT(*) FROM tr_petty_cash_funds
        WHERE tenant_id=:tid AND is_active=true
          AND current_balance <= (limit_amount * replenish_threshold / 100)
    """), {"tid": tid})
    need_replenish = int(r5.scalar() or 0)

    # تدفقات آخر 30 يوم
    r6 = await db.execute(text("""
        SELECT tx_date::text, SUM(CASE WHEN tx_type='RV' THEN amount ELSE 0 END) AS receipts,
               SUM(CASE WHEN tx_type='PV' THEN amount ELSE 0 END) AS payments
        FROM tr_cash_transactions
        WHERE tenant_id=:tid AND status='posted'
          AND tx_date >= CURRENT_DATE - 29
        GROUP BY tx_date ORDER BY tx_date
    """), {"tid": tid})
    cash_flow_chart = [{"date": row[0], "receipts": float(row[1]), "payments": float(row[2])}
                       for row in r6.fetchall()]

    # أعداد للـ KPI
    r7 = await db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM tr_bank_accounts WHERE tenant_id=:tid AND account_type='bank' AND is_active=true) AS banks,
          (SELECT COUNT(*) FROM tr_bank_accounts WHERE tenant_id=:tid AND account_type='cash_fund' AND is_active=true) AS funds,
          (SELECT COUNT(*) FROM tr_petty_cash_funds WHERE tenant_id=:tid AND is_active=true) AS petty_funds,
          (SELECT COUNT(*) FROM tr_petty_cash_expenses WHERE tenant_id=:tid AND status='draft') AS pending_expenses,
          (SELECT COALESCE(SUM(total_amount),0) FROM tr_petty_cash_expenses WHERE tenant_id=:tid AND status='draft') AS pending_amount,
          (SELECT COUNT(*) FROM tr_cash_transactions WHERE tenant_id=:tid AND status='draft') AS pending_vouchers,
          (SELECT COUNT(*) FROM tr_bank_transactions WHERE tenant_id=:tid AND status='draft') AS pending_bank_tx
    """), {"tid": tid})
    kpi_row = r7.mappings().fetchone()

    return ok(data={
        "kpis": {
            "total_balance":    total_bank + total_cash,
            "bank_balance":     total_bank,
            "cash_balance":     total_cash,
            "bank_count":       kpi_row["banks"],
            "fund_count":       kpi_row["funds"],
            "petty_fund_count": kpi_row["petty_funds"],
            "today_receipts":   today_receipts,
            "today_payments":   today_payments,
            "pending_vouchers": kpi_row["pending_vouchers"],
            "pending_bank_tx":  kpi_row["pending_bank_tx"],
            "pending_expenses": kpi_row["pending_expenses"],
            "pending_expense_amount": float(kpi_row["pending_amount"]),
            "need_replenish":   need_replenish,
        },
        "accounts":        accounts,
        "alerts":          alerts,
        "due_checks":      due_checks,
        "cash_flow_chart": cash_flow_chart,
    })


@router.get("/reports/gl-balance-check")
async def gl_balance_check(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    مقارنة رصيد الخزينة مع رصيد الأستاذ العام لكل حساب
    يكشف الفروق بين رصيد tr_bank_accounts.current_balance ورصيد account_balances في GL
    """
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
            ba.id,
            ba.account_name,
            ba.account_code,
            ba.account_type,
            ba.gl_account_code,
            ba.current_balance   AS treasury_balance,
            COALESCE(ab.balance, 0) AS gl_balance,
            ABS(ba.current_balance - COALESCE(ab.balance, 0)) AS diff,
            CASE WHEN ABS(ba.current_balance - COALESCE(ab.balance, 0)) < 0.01
                 THEN 'matched' ELSE 'mismatch' END AS status
        FROM tr_bank_accounts ba
        LEFT JOIN account_balances ab
               ON ab.account_code = ba.gl_account_code
              AND ab.tenant_id    = ba.tenant_id
        WHERE ba.tenant_id = :tid AND ba.is_active = true
        ORDER BY ba.account_type, ba.account_name
    """), {"tid": tid})
    rows = [dict(row._mapping) for row in r.fetchall()]

    mismatches = [r for r in rows if r["status"] == "mismatch"]
    total_diff  = sum(float(r["diff"]) for r in mismatches)

    return ok(data={
        "accounts": rows,
        "total_accounts": len(rows),
        "mismatches": len(mismatches),
        "total_diff": round(total_diff, 3),
        "all_matched": len(mismatches) == 0,
    })


# ══════════════════════════════════════════════════════════
# BANK ACCOUNTS
# ══════════════════════════════════════════════════════════
@router.get("/bank-accounts")
async def list_bank_accounts(
    account_type: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    where = "WHERE tenant_id=:tid"
    params = {"tid": tid}
    if account_type:
        where += " AND account_type=:atype"
        params["atype"] = account_type
    if is_active is not None:
        where += " AND is_active=:ia"
        params["ia"] = is_active
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

    # تحويل آمن للأرقام
    def to_decimal(v, default=0):
        try: return Decimal(str(v)) if v not in (None, "", "null") else Decimal(default)
        except: return Decimal(default)

    try:
        await db.execute(text("""
            INSERT INTO tr_bank_accounts (
                id, tenant_id, account_code, account_name,
                account_type, account_sub_type, bank_name, bank_branch, account_number,
                iban, swift_code, currency_code, gl_account_code,
                opening_balance, current_balance, low_balance_alert,
                opening_date, contact_person, contact_phone, notes,
                is_active, created_by
            ) VALUES (
                :id, :tid, :account_code, :account_name,
                :account_type, :account_sub_type, :bank_name, :bank_branch, :account_number,
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
            "opening_date":     data.get("opening_date") or None,
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

    for date_field in ("opening_date",):
        if date_field in safe:
            v = safe[date_field]
            if v == "" or v is None:
                safe[date_field] = None
            else:
                try:
                    safe[date_field] = date.fromisoformat(str(v))
                except Exception:
                    safe[date_field] = None

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
        UPDATE tr_bank_accounts SET is_active=false, updated_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"id": str(ba_id), "tid": tid})
    await db.commit()
    return ok(data={}, message="تم إلغاء تفعيل الحساب")


@router.patch("/bank-accounts/{ba_id}/toggle-active")
async def toggle_bank_account_active(
    ba_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تفعيل / إلغاء تفعيل حساب بنكي أو صندوق"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        UPDATE tr_bank_accounts
        SET is_active = NOT is_active, updated_at = NOW()
        WHERE id = :id AND tenant_id = :tid
        RETURNING is_active, account_name
    """), {"id": str(ba_id), "tid": tid})
    row = r.fetchone()
    if not row:
        raise HTTPException(404, "الحساب غير موجود")
    await db.commit()
    status_ar = "مفعّل" if row[0] else "موقوف"
    return ok(data={"is_active": row[0]}, message=f"تم تغيير حالة {row[1]} إلى {status_ar}")


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
    await _check_period(db, tid, tx_date)
    tx_type = data["tx_type"]  # PV | RV
    serial = await _next_serial(db, tid, tx_type, tx_date)

    tx_id = str(uuid.uuid4())
    amt = Decimal(str(data["amount"]))
    amt_sar = amt * Decimal(str(data.get("exchange_rate", 1)))

    vat_rate   = Decimal(str(data.get("vat_rate", 0) or 0))
    vat_amount = (amt * vat_rate / 100).quantize(Decimal("0.001"))
    vat_acc    = data.get("vat_account_code") or None

    try:
        await db.execute(text("""
            INSERT INTO tr_cash_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,
               currency_code,exchange_rate,amount_sar,counterpart_account,
               description,party_name,reference,payment_method,check_number,
               branch_code,cost_center,project_code,expense_classification_code,
               vat_rate,vat_amount,vat_account_code,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,
               :cur,:rate,:amt_sar,:cp_acc,
               :desc,:party,:ref,:method,:check_no,
               :branch,:cc,:proj,:exp_cls,
               :vat_rate,:vat_amount,:vat_acc,:notes,'draft',:by)
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
            "exp_cls": data.get("expense_classification_code"),
            "vat_rate": vat_rate, "vat_amount": vat_amount,
            "vat_acc": vat_acc, "notes": data.get("notes"), "by": user.email,
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
    if not tx: raise Exception("السند غير موجود")
    if tx["status"] != "draft": raise Exception("السند مُرحَّل مسبقاً")

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"]
    base_amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    vat_amt  = Decimal(str(tx.get("vat_amount") or 0))
    vat_acc  = tx.get("vat_account_code") or None
    total_amt = base_amt + vat_amt
    tx_date = tx["tx_date"]
    desc = tx["description"]

    # PV = صرف → CR صندوق/بنك بالإجمالي, DR الطرف المقابل + DR ضريبة
    # RV = قبض → DR صندوق/بنك بالإجمالي, CR الطرف المقابل + CR ضريبة
    dims = {
        "branch_code": tx.get("branch_code"),
        "cost_center": tx.get("cost_center"),
        "project_code": tx.get("project_code"),
        "expense_classification_code": tx.get("expense_classification_code"),
    }
    if tx["tx_type"] == "RV":
        lines = [
            {"account_code": gl, "debit": total_amt, "credit": 0,        "description": desc},
            {"account_code": cp, "debit": 0,         "credit": base_amt, "description": desc, **dims},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": 0, "credit": vat_amt, "description": f"ضريبة — {desc}"})
    else:
        lines = [
            {"account_code": cp, "debit": base_amt, "credit": 0,        "description": desc, **dims},
            {"account_code": gl, "debit": 0,        "credit": total_amt, "description": desc},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": vat_amt, "credit": 0, "description": f"ضريبة — {desc}"})
    amt = total_amt  # لتحديث الرصيد بالمبلغ الإجمالي

    je = await _post_je(db, tid, user.email, tx["tx_type"], tx_date, desc, lines, tx["reference"])
    delta = amt if tx["tx_type"] == "RV" else -amt
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
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


@router.put("/cash-transactions/{tx_id}")
async def update_cash_transaction(
    tx_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ALLOWED = {"tx_date","bank_account_id","amount","currency_code","counterpart_account",
               "description","party_name","reference","payment_method","check_number",
               "branch_code","cost_center","project_code","expense_classification_code","notes"}
    safe = {k: v for k, v in data.items() if k in ALLOWED}
    if not safe:
        raise HTTPException(400, "لا توجد حقول صالحة للتعديل")
    if "amount" in safe:
        safe["amount"] = Decimal(str(safe["amount"]))
        safe["amount_sar"] = safe["amount"] * Decimal(str(data.get("exchange_rate", 1)))
    safe["updated_at"] = "NOW()"
    safe["updated_by"] = user.email
    set_clause = ", ".join([f"{k}={'NOW()' if v=='NOW()' else f':{k}'}" for k in safe])
    params = {k: v for k, v in safe.items() if v != "NOW()"}
    params.update({"id": str(tx_id), "tid": tid})
    try:
        await db.execute(text(f"""
            UPDATE tr_cash_transactions SET {set_clause}
            WHERE id=:id AND tenant_id=:tid AND status='draft'
        """), params)
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في التعديل: {str(e)}")
    return ok(data={"id": str(tx_id)}, message="تم التعديل ✅")


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
    await _check_period(db, tid, tx_date)
    tx_type = data["tx_type"]
    serial = await _next_serial(db, tid, tx_type, tx_date)

    tx_id = str(uuid.uuid4())
    amt = Decimal(str(data["amount"]))
    amt_sar = amt * Decimal(str(data.get("exchange_rate", 1)))

    vat_rate   = Decimal(str(data.get("vat_rate", 0) or 0))
    vat_amount = (amt * vat_rate / 100).quantize(Decimal("0.001"))
    vat_acc    = data.get("vat_account_code") or None

    await db.execute(text("""
        INSERT INTO tr_bank_transactions
          (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,
           currency_code,exchange_rate,amount_sar,counterpart_account,
           beneficiary_name,beneficiary_iban,beneficiary_bank,
           description,reference,payment_method,check_number,
           branch_code,cost_center,project_code,expense_classification_code,
           vat_rate,vat_amount,vat_account_code,notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,
           :cur,:rate,:amt_sar,:cp_acc,
           :ben_name,:ben_iban,:ben_bank,
           :desc,:ref,:method,:check_no,
           :branch,:cc,:proj,:exp_cls,
           :vat_rate,:vat_amount,:vat_acc,:notes,'draft',:by)
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
        "exp_cls": data.get("expense_classification_code"),
        "vat_rate": vat_rate, "vat_amount": vat_amount,
        "vat_acc": vat_acc, "notes": data.get("notes"), "by": user.email,
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
    if not tx: raise Exception("السند غير موجود")
    if tx["status"] != "draft": raise Exception("السند مُرحَّل مسبقاً")

    gl        = tx["gl_account_code"]
    cp        = tx["counterpart_account"] or "9999"
    base_amt  = Decimal(str(tx["amount_sar"] or tx["amount"]))
    vat_amt   = Decimal(str(tx.get("vat_amount") or 0))
    vat_acc   = tx.get("vat_account_code") or None
    total_amt = base_amt + vat_amt
    desc      = tx["description"]
    tx_date   = tx["tx_date"]

    # BP = دفعة → DR مورد/مصروف + DR ضريبة, CR بنك بالإجمالي
    # BR = قبض  → DR بنك بالإجمالي, CR عميل/إيراد + CR ضريبة
    # BT = تحويل → DR مستفيد, CR بنك
    dims = {
        "branch_code": tx.get("branch_code"),
        "cost_center": tx.get("cost_center"),
        "project_code": tx.get("project_code"),
        "expense_classification_code": tx.get("expense_classification_code"),
    }
    if tx["tx_type"] == "BR":
        lines = [
            {"account_code": gl, "debit": total_amt, "credit": 0,        "description": desc},
            {"account_code": cp, "debit": 0,         "credit": base_amt, "description": desc, **dims},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": 0, "credit": vat_amt, "description": f"ضريبة — {desc}"})
        delta = total_amt
    else:  # BP, BT
        lines = [
            {"account_code": cp, "debit": base_amt, "credit": 0,        "description": desc, **dims},
            {"account_code": gl, "debit": 0,        "credit": total_amt, "description": desc},
        ]
        if vat_amt > 0 and vat_acc:
            lines.append({"account_code": vat_acc, "debit": vat_amt, "credit": 0, "description": f"ضريبة — {desc}"})
        delta = -total_amt

    je = await _post_je(db, tid, user.email, tx["tx_type"], tx_date, desc, lines, tx["reference"])
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
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# REVERSE POSTED TRANSACTION
# ══════════════════════════════════════════════════════════

@router.post("/cash-transactions/{tx_id}/reverse")
async def reverse_cash_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
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
    if tx["status"] != "posted": raise HTTPException(400, "يمكن عكس السندات المُرحَّلة فقط")

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"]
    amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    tx_date = tx["tx_date"]
    note = data.get("note", f"عكس السند {tx['serial']}")
    dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
            "project_code": tx.get("project_code"), "expense_classification_code": tx.get("expense_classification_code")}

    rev_type = "PV" if tx["tx_type"] == "RV" else "RV"
    if tx["tx_type"] == "RV":
        lines = [{"account_code": cp, "debit": amt, "credit": 0, "description": note, **dims},
                 {"account_code": gl, "debit": 0, "credit": amt, "description": note}]
        delta = -amt
    else:
        lines = [{"account_code": gl, "debit": amt, "credit": 0, "description": note},
                 {"account_code": cp, "debit": 0, "credit": amt, "description": note, **dims}]
        delta = amt

    try:
        je = await _post_je(db, tid, user.email, rev_type, tx_date, note, lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        rev_serial = await _next_serial(db, tid, rev_type, tx_date)
        rev_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO tr_cash_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,currency_code,
               exchange_rate,amount_sar,counterpart_account,description,reference,
               payment_method,branch_code,cost_center,project_code,expense_classification_code,
               status,je_id,je_serial,posted_by,posted_at,created_by)
            VALUES
              (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,:cur,
               :rate,:amt_sar,:cp,:desc,:ref,
               :method,:branch,:cc,:proj,:exp_cls,
               'posted',:je_id,:je_serial,:by,NOW(),:by)
        """), {
            "id": rev_id, "tid": tid, "serial": rev_serial,
            "tx_type": rev_type, "tx_date": tx_date,
            "ba_id": str(tx["bank_account_id"]) if tx["bank_account_id"] else None,
            "amount": amt, "cur": tx["currency_code"] or "SAR",
            "rate": tx["exchange_rate"] or 1, "amt_sar": amt,
            "cp": cp, "desc": note, "ref": tx["reference"],
            "method": tx["payment_method"] or "cash",
            "branch": tx.get("branch_code"), "cc": tx.get("cost_center"),
            "proj": tx.get("project_code"), "exp_cls": tx.get("expense_classification_code"),
            "je_id": je["je_id"], "je_serial": je["je_serial"], "by": user.email,
        })
        await db.execute(text("""
            UPDATE tr_cash_transactions SET status='reversed', reversed_by=:by, reversed_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "id": str(tx_id), "tid": tid})
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في العكس: {str(e)}")
    return ok(data={"reversal_serial": rev_serial, "je_serial": je["je_serial"]},
              message=f"✅ تم إنشاء قيد عكسي — {rev_serial}")


@router.post("/bank-transactions/{tx_id}/reverse")
async def reverse_bank_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
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
    if tx["status"] != "posted": raise HTTPException(400, "يمكن عكس السندات المُرحَّلة فقط")

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"] or "9999"
    amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    tx_date = tx["tx_date"]
    note = data.get("note", f"عكس السند {tx['serial']}")
    dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
            "project_code": tx.get("project_code"), "expense_classification_code": tx.get("expense_classification_code")}

    if tx["tx_type"] == "BR":
        rev_type = "BP"
        lines = [{"account_code": cp, "debit": amt, "credit": 0, "description": note, **dims},
                 {"account_code": gl, "debit": 0, "credit": amt, "description": note}]
        delta = -amt
    else:
        rev_type = "BR"
        lines = [{"account_code": gl, "debit": amt, "credit": 0, "description": note},
                 {"account_code": cp, "debit": 0, "credit": amt, "description": note, **dims}]
        delta = amt

    try:
        je = await _post_je(db, tid, user.email, rev_type, tx_date, note, lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        rev_serial = await _next_serial(db, tid, rev_type, tx_date)
        rev_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO tr_bank_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,currency_code,
               exchange_rate,amount_sar,counterpart_account,description,reference,
               payment_method,branch_code,cost_center,project_code,expense_classification_code,
               status,je_id,je_serial,posted_by,posted_at,created_by)
            VALUES
              (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,:cur,
               :rate,:amt_sar,:cp,:desc,:ref,
               :method,:branch,:cc,:proj,:exp_cls,
               'posted',:je_id,:je_serial,:by,NOW(),:by)
        """), {
            "id": rev_id, "tid": tid, "serial": rev_serial,
            "tx_type": rev_type, "tx_date": tx_date,
            "ba_id": str(tx["bank_account_id"]) if tx["bank_account_id"] else None,
            "amount": amt, "cur": tx["currency_code"] or "SAR",
            "rate": tx["exchange_rate"] or 1, "amt_sar": amt,
            "cp": cp, "desc": note, "ref": tx["reference"],
            "method": tx["payment_method"] or "wire",
            "branch": tx.get("branch_code"), "cc": tx.get("cost_center"),
            "proj": tx.get("project_code"), "exp_cls": tx.get("expense_classification_code"),
            "je_id": je["je_id"], "je_serial": je["je_serial"], "by": user.email,
        })
        await db.execute(text("""
            UPDATE tr_bank_transactions SET status='reversed', reversed_by=:by, reversed_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "id": str(tx_id), "tid": tid})
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في العكس: {str(e)}")
    return ok(data={"reversal_serial": rev_serial, "je_serial": je["je_serial"]},
              message=f"✅ تم إنشاء قيد عكسي — {rev_serial}")


# ══════════════════════════════════════════════════════════
# WORKFLOW — submit / approve / reject
# ══════════════════════════════════════════════════════════

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
        RETURNING id
    """), {"id": str(tx_id), "tid": tid, "by": user.email})
    if not r.fetchone(): raise HTTPException(400, "السند غير موجود أو ليس مسودة")
    await db.commit()
    return ok(data={}, message="تم إرسال السند للاعتماد ✅")


@router.post("/cash-transactions/{tx_id}/approve")
async def approve_cash_transaction(
    tx_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """اعتماد السند ثم ترحيله مباشرة"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT ct.*, ba.gl_account_code
        FROM tr_cash_transactions ct
        LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
        WHERE ct.id=:id AND ct.tenant_id=:tid
    """), {"id": str(tx_id), "tid": tid})
    tx = r.mappings().fetchone()
    if not tx: raise HTTPException(404, "السند غير موجود")
    if tx["status"] != "pending_approval": raise HTTPException(400, "السند ليس في انتظار الاعتماد")

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"]
    amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
            "project_code": tx.get("project_code"), "expense_classification_code": tx.get("expense_classification_code")}
    if tx["tx_type"] == "RV":
        lines = [{"account_code": gl, "debit": amt, "credit": 0, "description": tx["description"]},
                 {"account_code": cp, "debit": 0, "credit": amt, "description": tx["description"], **dims}]
        delta = amt
    else:
        lines = [{"account_code": cp, "debit": amt, "credit": 0, "description": tx["description"], **dims},
                 {"account_code": gl, "debit": 0, "credit": amt, "description": tx["description"]}]
        delta = -amt
    try:
        je = await _post_je(db, tid, user.email, tx["tx_type"], tx["tx_date"], tx["description"], lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        await db.execute(text("""
            UPDATE tr_cash_transactions
            SET status='posted', approved_by=:by::text, approved_at=NOW(),
                je_id=:je_id, je_serial=:je_serial, posted_by=:by::text, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "je_id": je["je_id"], "je_serial": je["je_serial"],
               "id": str(tx_id), "tid": tid})
        await db.commit()
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
    note = data.get("note", "مرفوض")
    r = await db.execute(text("""
        UPDATE tr_cash_transactions
        SET status='draft', rejected_by=:by, rejected_at=NOW(), rejection_note=:note
        WHERE id=:id AND tenant_id=:tid AND status='pending_approval'
        RETURNING id
    """), {"id": str(tx_id), "tid": tid, "by": user.email, "note": note})
    if not r.fetchone(): raise HTTPException(400, "السند غير موجود أو ليس في انتظار الاعتماد")
    await db.commit()
    return ok(data={}, message="تم رفض السند وإعادته للمسودة")


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
        RETURNING id
    """), {"id": str(tx_id), "tid": tid, "by": user.email})
    if not r.fetchone(): raise HTTPException(400, "السند غير موجود أو ليس مسودة")
    await db.commit()
    return ok(data={}, message="تم إرسال السند للاعتماد ✅")


@router.post("/bank-transactions/{tx_id}/approve")
async def approve_bank_transaction(
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
    if tx["status"] != "pending_approval": raise HTTPException(400, "السند ليس في انتظار الاعتماد")

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"] or "9999"
    amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
            "project_code": tx.get("project_code"), "expense_classification_code": tx.get("expense_classification_code")}
    if tx["tx_type"] == "BR":
        lines = [{"account_code": gl, "debit": amt, "credit": 0, "description": tx["description"]},
                 {"account_code": cp, "debit": 0, "credit": amt, "description": tx["description"], **dims}]
        delta = amt
    else:
        lines = [{"account_code": cp, "debit": amt, "credit": 0, "description": tx["description"], **dims},
                 {"account_code": gl, "debit": 0, "credit": amt, "description": tx["description"]}]
        delta = -amt
    try:
        je = await _post_je(db, tid, user.email, tx["tx_type"], tx["tx_date"], tx["description"], lines, tx["reference"])
        if tx["bank_account_id"]:
            await _update_balance(db, str(tx["bank_account_id"]), delta)
        await db.execute(text("""
            UPDATE tr_bank_transactions
            SET status='posted', approved_by=:by::text, approved_at=NOW(),
                je_id=:je_id, je_serial=:je_serial, posted_by=:by::text, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"by": user.email, "je_id": je["je_id"], "je_serial": je["je_serial"],
               "id": str(tx_id), "tid": tid})
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الاعتماد: {str(e)}")
    return ok(data={"je_serial": je["je_serial"]}, message=f"✅ تم الاعتماد والترحيل — {je['je_serial']}")


@router.post("/bank-transactions/{tx_id}/reject")
async def reject_bank_transaction(
    tx_id: uuid.UUID,
    data: dict = Body(default={}),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    note = data.get("note", "مرفوض")
    r = await db.execute(text("""
        UPDATE tr_bank_transactions
        SET status='draft', rejected_by=:by, rejected_at=NOW(), rejection_note=:note
        WHERE id=:id AND tenant_id=:tid AND status='pending_approval'
        RETURNING id
    """), {"id": str(tx_id), "tid": tid, "by": user.email, "note": note})
    if not r.fetchone(): raise HTTPException(400, "السند غير موجود أو ليس في انتظار الاعتماد")
    await db.commit()
    return ok(data={}, message="تم رفض السند وإعادته للمسودة")


# ══════════════════════════════════════════════════════════
# BULK POST
# ══════════════════════════════════════════════════════════

@router.post("/cash-transactions/bulk-post")
async def bulk_post_cash(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    ids = data.get("ids", [])
    if not ids:
        raise HTTPException(400, "لم يتم تحديد أي سندات")
    tid = str(user.tenant_id)
    posted = []
    errors = []
    for raw_id in ids:
        try:
            r = await db.execute(text("""
                SELECT ct.*, ba.gl_account_code
                FROM tr_cash_transactions ct
                LEFT JOIN tr_bank_accounts ba ON ba.id=ct.bank_account_id
                WHERE ct.id=:id AND ct.tenant_id=:tid
            """), {"id": str(raw_id), "tid": tid})
            tx = r.mappings().fetchone()
            if not tx or tx["status"] != "draft":
                errors.append(str(raw_id))
                continue
            gl = tx["gl_account_code"]
            cp = tx["counterpart_account"]
            amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
            desc = tx["description"]
            tx_date = tx["tx_date"]
            dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
                    "project_code": tx.get("project_code"), "expense_classification_code": tx.get("expense_classification_code")}
            if tx["tx_type"] == "RV":
                lines = [{"account_code": gl, "debit": amt, "credit": 0, "description": desc},
                         {"account_code": cp, "debit": 0, "credit": amt, "description": desc, **dims}]
            else:
                lines = [{"account_code": cp, "debit": amt, "credit": 0, "description": desc, **dims},
                         {"account_code": gl, "debit": 0, "credit": amt, "description": desc}]
            je = await _post_je(db, tid, user.email, tx["tx_type"], tx_date, desc, lines, tx["reference"])
            delta = amt if tx["tx_type"] == "RV" else -amt
            if tx["bank_account_id"]:
                await _update_balance(db, str(tx["bank_account_id"]), delta)
            await db.execute(text("""
                UPDATE tr_cash_transactions
                SET status='posted', je_id=:je_id, je_serial=:je_serial, posted_by=:by, posted_at=NOW()
                WHERE id=:id AND tenant_id=:tid
            """), {"je_id": je["je_id"], "je_serial": je["je_serial"], "by": user.email,
                   "id": str(raw_id), "tid": tid})
            await db.commit()
            posted.append(je["je_serial"])
        except Exception as e:
            await db.rollback()
            errors.append(str(raw_id))
    return ok(data={"posted": posted, "errors": errors},
              message=f"✅ تم ترحيل {len(posted)} سند" + (f" | ⚠️ {len(errors)} فشل" if errors else ""))


@router.post("/bank-transactions/bulk-post")
async def bulk_post_bank(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    ids = data.get("ids", [])
    if not ids:
        raise HTTPException(400, "لم يتم تحديد أي سندات")
    tid = str(user.tenant_id)
    posted = []
    errors = []
    for raw_id in ids:
        try:
            r = await db.execute(text("""
                SELECT bt.*, ba.gl_account_code
                FROM tr_bank_transactions bt
                LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
                WHERE bt.id=:id AND bt.tenant_id=:tid
            """), {"id": str(raw_id), "tid": tid})
            tx = r.mappings().fetchone()
            if not tx or tx["status"] != "draft":
                errors.append(str(raw_id))
                continue
            gl = tx["gl_account_code"]
            cp = tx["counterpart_account"] or "9999"
            amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
            desc = tx["description"]
            tx_date = tx["tx_date"]
            dims = {"branch_code": tx.get("branch_code"), "cost_center": tx.get("cost_center"),
                    "project_code": tx.get("project_code"), "expense_classification_code": tx.get("expense_classification_code")}
            if tx["tx_type"] == "BR":
                lines = [{"account_code": gl, "debit": amt, "credit": 0, "description": desc},
                         {"account_code": cp, "debit": 0, "credit": amt, "description": desc, **dims}]
                delta = amt
            else:
                lines = [{"account_code": cp, "debit": amt, "credit": 0, "description": desc, **dims},
                         {"account_code": gl, "debit": 0, "credit": amt, "description": desc}]
                delta = -amt
            je = await _post_je(db, tid, user.email, tx["tx_type"], tx_date, desc, lines, tx["reference"])
            if tx["bank_account_id"]:
                await _update_balance(db, str(tx["bank_account_id"]), delta)
            await db.execute(text("""
                UPDATE tr_bank_transactions
                SET status='posted', je_id=:je_id, je_serial=:je_serial, posted_by=:by, posted_at=NOW()
                WHERE id=:id AND tenant_id=:tid
            """), {"je_id": je["je_id"], "je_serial": je["je_serial"], "by": user.email,
                   "id": str(raw_id), "tid": tid})
            await db.commit()
            posted.append(je["je_serial"])
        except Exception as e:
            await db.rollback()
            errors.append(str(raw_id))
    return ok(data={"posted": posted, "errors": errors},
              message=f"✅ تم ترحيل {len(posted)} سند" + (f" | ⚠️ {len(errors)} فشل" if errors else ""))


# ══════════════════════════════════════════════════════════
# INTERNAL TRANSFERS (IT)
# ══════════════════════════════════════════════════════════
@router.get("/internal-transfers")
async def list_internal_transfers(
    status:    Optional[str]  = Query(None),
    date_from: Optional[date] = Query(None),
    date_to:   Optional[date] = Query(None),
    limit: int = Query(50), offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["it.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}
    if status:    conds.append("it.status=:status"); params["status"] = status
    if date_from: conds.append("it.tx_date>=:df");   params["df"] = str(date_from)
    if date_to:   conds.append("it.tx_date<=:dt");   params["dt"] = str(date_to)
    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM tr_internal_transfers it WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT it.*,
               fa.account_name AS from_account_name,
               ta.account_name AS to_account_name
        FROM tr_internal_transfers it
        LEFT JOIN tr_bank_accounts fa ON fa.id=it.from_account_id
        LEFT JOIN tr_bank_accounts ta ON ta.id=it.to_account_id
        WHERE {where}
        ORDER BY it.tx_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total": cnt.scalar(), "items": [dict(row._mapping) for row in r.fetchall()]})


@router.post("/internal-transfers", status_code=201)
async def create_internal_transfer(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    tx_date = date.fromisoformat(str(data["tx_date"]))
    await _check_period(db, tid, tx_date)
    serial = await _next_serial(db, tid, "IT", tx_date)
    tx_id = str(uuid.uuid4())
    amt = Decimal(str(data["amount"]))

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
        "to_id": str(data["to_account_id"]),
        "amount": amt, "cur": data.get("currency_code","SAR"),
        "rate": data.get("exchange_rate",1),
        "amount_to": data.get("amount_to", amt),
        "desc": data["description"], "ref": data.get("reference"),
        "notes": data.get("notes"), "by": user.email,
    })
    await db.commit()
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
    limit: int = Query(50), offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["ck.tenant_id=:tid"]
    params: dict = {"tid": tid, "limit": limit, "offset": offset}
    if check_type: conds.append("ck.check_type=:ct");   params["ct"] = check_type
    if status:     conds.append("ck.status=:status");   params["status"] = status
    if date_from:  conds.append("ck.check_date>=:df");  params["df"] = str(date_from)
    if date_to:    conds.append("ck.check_date<=:dt");  params["dt"] = str(date_to)
    where = " AND ".join(conds)

    cnt = await db.execute(text(f"SELECT COUNT(*) FROM tr_checks ck WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT ck.*, ba.account_name AS bank_account_name
        FROM tr_checks ck
        LEFT JOIN tr_bank_accounts ba ON ba.id=ck.bank_account_id
        WHERE {where}
        ORDER BY ck.check_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total": cnt.scalar(), "items": [dict(row._mapping) for row in r.fetchall()]})


@router.post("/checks", status_code=201)
async def create_check(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    check_date = date.fromisoformat(str(data["check_date"]))
    await _check_period(db, tid, check_date)
    serial = await _next_serial(db, tid, "CHK", check_date)
    ck_id = str(uuid.uuid4())

    await db.execute(text("""
        INSERT INTO tr_checks
          (id,tenant_id,serial,check_number,check_type,check_date,due_date,
           bank_account_id,amount,payee_name,description,status,notes,created_by)
        VALUES
          (:id,:tid,:serial,:ck_no,:ck_type,:ck_date,:due_date,
           :ba_id,:amount,:payee,:desc,'issued',:notes,:by)
    """), {
        "id": ck_id, "tid": tid, "serial": serial,
        "ck_no": data["check_number"], "ck_type": data["check_type"],
        "ck_date": check_date, "due_date": data.get("due_date"),
        "ba_id": str(data["bank_account_id"]) if data.get("bank_account_id") else None,
        "amount": Decimal(str(data["amount"])),
        "payee": data.get("payee_name"), "desc": data.get("description"),
        "notes": data.get("notes"), "by": user.email,
    })
    await db.commit()
    return created(data={"id": ck_id, "serial": serial}, message=f"تم إنشاء الشيك {serial} ✅")


@router.put("/checks/{ck_id}/status")
async def update_check_status(
    ck_id: uuid.UUID,
    status: str = Query(..., regex="^(issued|deposited|cleared|bounced|cancelled)$"),
    notes: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE tr_checks SET status=:status, notes=:notes, updated_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"status": status, "notes": notes, "id": str(ck_id), "tid": tid})
    await db.commit()
    return ok(data={}, message=f"تم تحديث حالة الشيك إلى: {status}")


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
    """مطابقة سطر كشف البنك مع حركة في النظام (bank tx / AR receipt / AP payment)"""
    tid = str(user.tenant_id)

    # ── تحديث سطر الكشف ───────────────────────────────────
    await db.execute(text("""
        UPDATE tr_bank_statement_lines
        SET match_status='matched', matched_tx_id=:tx_id, matched_tx_type=:tx_type
        WHERE id=:line_id AND session_id=:sess_id AND tenant_id=:tid
    """), {"tx_id": str(tx_id), "tx_type": tx_type,
           "line_id": str(statement_line_id), "sess_id": str(sess_id), "tid": tid})

    # ── تحديث حالة المستند المطابق ───────────────────────
    if tx_type == "AR_RECEIPT":
        await db.execute(text("""
            UPDATE ar_receipts SET is_reconciled=true, reconciled_at=NOW()
            WHERE id=:tx_id AND tenant_id=:tid
        """), {"tx_id": str(tx_id), "tid": tid})
    elif tx_type == "AP_PAYMENT":
        await db.execute(text("""
            UPDATE ap_payments SET is_reconciled=true, reconciled_at=NOW()
            WHERE id=:tx_id AND tenant_id=:tid
        """), {"tx_id": str(tx_id), "tid": tid})
    else:
        tbl = "tr_bank_transactions" if tx_type in ("BP","BR","BT") else "tr_cash_transactions"
        await db.execute(text(f"""
            UPDATE {tbl} SET is_reconciled=true, reconciled_at=NOW()
            WHERE id=:tx_id AND tenant_id=:tid
        """), {"tx_id": str(tx_id), "tid": tid})

    await db.commit()
    return ok(data={}, message="✅ تمت المطابقة")


@router.post("/reconciliation/sessions/{sess_id}/auto-match")
async def auto_match_session(
    sess_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """مطابقة تلقائية — تُقارن كل سطر غير مطابق مع AR receipts + AP payments + bank transactions
       وتُطبّق المطابقات التي تحصل على ثقة 100٪ (مبلغ مطابق تماماً + نفس الحساب + فارق ≤ 7 أيام)
    """
    tid = str(user.tenant_id)
    from datetime import timedelta as td

    # جلب الجلسة
    sr = await db.execute(text("""
        SELECT * FROM tr_reconciliation_sessions
        WHERE id=:id AND tenant_id=:tid
    """), {"id": str(sess_id), "tid": tid})
    sess = sr.mappings().fetchone()
    if not sess: raise HTTPException(404, "الجلسة غير موجودة")

    bank_account_id = str(sess["bank_account_id"])

    # ── الأسطر غير المطابقة ─────────────────────────────
    lr = await db.execute(text("""
        SELECT id, line_date, debit, credit, reference, description
        FROM tr_bank_statement_lines
        WHERE session_id=:sess_id AND tenant_id=:tid
          AND (match_status IS NULL OR match_status != 'matched')
        ORDER BY line_date
    """), {"sess_id": str(sess_id), "tid": tid})
    lines = lr.mappings().fetchall()

    # ── مصادر المطابقة: AR receipts (credit) ───────────
    ar_r = await db.execute(text("""
        SELECT r.id, r.serial, r.receipt_date AS tx_date,
               r.amount_sar AS amount, r.reference,
               c.customer_name AS party_name,
               r.is_reconciled
        FROM ar_receipts r
        LEFT JOIN ar_customers c ON c.id = r.customer_id
        WHERE r.tenant_id=:tid AND r.status='posted'
          AND (r.bank_account_id=:ba OR r.payment_method='bank')
          AND (r.is_reconciled IS NULL OR r.is_reconciled=false)
    """), {"tid": tid, "ba": bank_account_id})
    ar_receipts = ar_r.mappings().fetchall()

    # ── مصادر المطابقة: AP payments (debit) ─────────────
    ap_r = await db.execute(text("""
        SELECT p.id, p.serial, p.payment_date AS tx_date,
               p.amount_sar AS amount, p.reference,
               v.vendor_name AS party_name,
               p.is_reconciled
        FROM ap_payments p
        LEFT JOIN ap_vendors v ON v.id = p.vendor_id
        WHERE p.tenant_id=:tid AND p.status='posted'
          AND (p.bank_account_id=:ba OR p.payment_method='bank')
          AND (p.is_reconciled IS NULL OR p.is_reconciled=false)
    """), {"tid": tid, "ba": bank_account_id})
    ap_payments = ap_r.mappings().fetchall()

    # ── مصادر المطابقة: Bank transactions ───────────────
    br_r = await db.execute(text("""
        SELECT id, serial, tx_date, amount, reference, description,
               tx_type, is_reconciled
        FROM tr_bank_transactions
        WHERE tenant_id=:tid AND bank_account_id=:ba AND status='posted'
          AND (is_reconciled IS NULL OR is_reconciled=false)
    """), {"tid": tid, "ba": bank_account_id})
    bank_txns = br_r.mappings().fetchall()

    def score(line_date, line_ref, cand_date, cand_ref, cand_amt, line_amt):
        """نقاط الثقة من 0 إلى 100"""
        if abs(float(line_amt) - float(cand_amt)) > 0.01:
            return 0   # المبلغ لا بد أن يتطابق تماماً
        days_diff = abs((line_date - cand_date).days)
        if days_diff > 7: return 0
        pts = 60                         # مبلغ مطابق
        pts += max(0, 30 - days_diff*4)  # كلما قلّ الفارق زادت النقاط
        if line_ref and cand_ref and (str(line_ref).strip() in str(cand_ref) or str(cand_ref).strip() in str(line_ref)):
            pts += 10                    # المرجع متطابق
        return pts

    matched_count  = 0
    skipped_count  = 0
    suggestions    = []   # مطابقات بثقة < 100 — ترجع للمستخدم
    used_cand_ids  = set()

    for line in lines:
        line_date = line["line_date"]
        credit    = float(line["credit"] or 0)
        debit     = float(line["debit"]  or 0)
        best_score = 0
        best_cand  = None
        best_type  = None

        if credit > 0:
            # نبحث في AR receipts أولاً
            for rec in ar_receipts:
                if str(rec["id"]) in used_cand_ids: continue
                s = score(line_date, line["reference"], rec["tx_date"], rec["reference"], rec["amount"], credit)
                if s > best_score:
                    best_score = s; best_cand = rec; best_type = "AR_RECEIPT"
            # ثم Bank BR
            for bt in bank_txns:
                if bt["tx_type"] != "BR": continue
                if str(bt["id"]) in used_cand_ids: continue
                s = score(line_date, line["reference"], bt["tx_date"], bt["reference"], bt["amount"], credit)
                if s > best_score:
                    best_score = s; best_cand = bt; best_type = bt["tx_type"]

        elif debit > 0:
            # نبحث في AP payments أولاً
            for pay in ap_payments:
                if str(pay["id"]) in used_cand_ids: continue
                s = score(line_date, line["reference"], pay["tx_date"], pay["reference"], pay["amount"], debit)
                if s > best_score:
                    best_score = s; best_cand = pay; best_type = "AP_PAYMENT"
            # ثم Bank BP
            for bt in bank_txns:
                if bt["tx_type"] != "BP": continue
                if str(bt["id"]) in used_cand_ids: continue
                s = score(line_date, line["reference"], bt["tx_date"], bt["reference"], bt["amount"], debit)
                if s > best_score:
                    best_score = s; best_cand = bt; best_type = bt["tx_type"]

        if best_cand is None or best_score == 0:
            skipped_count += 1
            continue

        cand_id = str(best_cand["id"])

        if best_score >= 90:
            # تطبيق المطابقة تلقائياً
            await db.execute(text("""
                UPDATE tr_bank_statement_lines
                SET match_status='matched', matched_tx_id=:tx_id, matched_tx_type=:tx_type
                WHERE id=:line_id AND tenant_id=:tid
            """), {"tx_id": cand_id, "tx_type": best_type,
                   "line_id": str(line["id"]), "tid": tid})

            if best_type == "AR_RECEIPT":
                await db.execute(text("""
                    UPDATE ar_receipts SET is_reconciled=true, reconciled_at=NOW()
                    WHERE id=:id AND tenant_id=:tid
                """), {"id": cand_id, "tid": tid})
            elif best_type == "AP_PAYMENT":
                await db.execute(text("""
                    UPDATE ap_payments SET is_reconciled=true, reconciled_at=NOW()
                    WHERE id=:id AND tenant_id=:tid
                """), {"id": cand_id, "tid": tid})
            else:
                tbl = "tr_bank_transactions" if best_type in ("BP","BR","BT") else "tr_cash_transactions"
                await db.execute(text(f"""
                    UPDATE {tbl} SET is_reconciled=true, reconciled_at=NOW()
                    WHERE id=:id AND tenant_id=:tid
                """), {"id": cand_id, "tid": tid})

            used_cand_ids.add(cand_id)
            matched_count += 1
        else:
            # اقتراح فقط
            suggestions.append({
                "line_id":   str(line["id"]),
                "line_date": str(line_date),
                "line_ref":  line["reference"],
                "amount":    credit if credit > 0 else debit,
                "direction": "credit" if credit > 0 else "debit",
                "candidate_id":     cand_id,
                "candidate_type":   best_type,
                "candidate_serial": best_cand.get("serial",""),
                "candidate_date":   str(best_cand.get("tx_date","")),
                "candidate_party":  best_cand.get("party_name","") or best_cand.get("description",""),
                "score": best_score,
            })

    await db.commit()

    msg = f"✅ تم تطبيق {matched_count} مطابقة تلقائية"
    if suggestions: msg += f" · {len(suggestions)} اقتراح يحتاج مراجعة"
    if skipped_count: msg += f" · {skipped_count} سطر بلا مرشح"

    return ok(data={
        "matched":     matched_count,
        "suggestions": suggestions,
        "skipped":     skipped_count,
    }, message=msg)


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
    tid = str(user.tenant_id)
    fund_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO tr_petty_cash_funds
          (id,tenant_id,fund_code,fund_name,custodian_name,custodian_email,
           currency_code,limit_amount,current_balance,gl_account_code,
           bank_account_id,branch_code,replenish_threshold,notes,created_by)
        VALUES
          (:id,:tid,:code,:name,:custodian,:custodian_email,
           :cur,:limit,:balance,:gl,
           :ba_id,:branch,:threshold,:notes,:by)
    """), {
        "id": fund_id, "tid": tid,
        "code": data["fund_code"], "name": data["fund_name"],
        "custodian": data.get("custodian_name"),
        "custodian_email": data.get("custodian_email"),
        "cur": data.get("currency_code","SAR"),
        "limit": Decimal(str(data["limit_amount"])),
        "balance": Decimal(str(data.get("opening_balance",0))),
        "gl": data["gl_account_code"],
        "ba_id": str(data["bank_account_id"]) if data.get("bank_account_id") else None,
        "branch": data.get("branch_code"),
        "threshold": data.get("replenish_threshold", 20),
        "notes": data.get("notes"), "by": user.email,
    })
    await db.commit()
    return created(data={"id": fund_id}, message="تم إنشاء صندوق العهدة ✅")


@router.put("/petty-cash/funds/{fund_id}")
async def update_petty_cash_fund(
    fund_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    data.pop("id",None); data.pop("tenant_id",None); data.pop("current_balance",None)
    set_clause = ", ".join([f"{k}=:{k}" for k in data.keys()])
    data.update({"id": str(fund_id), "tid": tid})
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
    await _check_period(db, tid, exp_date)
    serial = await _next_serial(db, tid, "PET", exp_date)
    exp_id = str(uuid.uuid4())
    lines = data.get("lines", [])
    total = sum(Decimal(str(l["amount"])) for l in lines)
    vat_total = sum(Decimal(str(l.get("vat_amount",0))) for l in lines)

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
        await db.execute(text("""
            INSERT INTO tr_petty_cash_expense_lines
              (id,tenant_id,expense_id,line_order,expense_account,expense_account_name,
               description,amount,vat_amount,net_amount,vendor_name,
               branch_code,cost_center,project_code,attachment_url)
            VALUES
              (gen_random_uuid(),:tid,:exp_id,:order,:acc,:acc_name,
               :desc,:amount,:vat,:net,:vendor,
               :branch,:cc,:proj,:attach)
        """), {
            "tid": tid, "exp_id": exp_id, "order": i+1,
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
            "attach": line.get("attachment_url"),
        })

    await db.commit()
    return created(data={"id": exp_id, "serial": serial, "total": float(total)},
                   message=f"تم إنشاء {serial} ✅")


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
    if exp["status"] != "draft": raise Exception("مُرحَّل مسبقاً")

    lr = await db.execute(text("""
        SELECT * FROM tr_petty_cash_expense_lines
        WHERE expense_id=:id ORDER BY line_order
    """), {"id": str(exp_id)})
    exp_lines = lr.mappings().all()

    # بناء القيد: DR مصاريف متعددة, CR حساب العهدة
    lines = [{"account_code": l["expense_account"],
               "debit": Decimal(str(l["amount"])),
               "credit": 0,
               "description": l.get("description") or exp["description"],
               "branch_code": l.get("branch_code"),
               "cost_center": l.get("cost_center"),
               "project_code": l.get("project_code")}
             for l in exp_lines]
    total_amt = Decimal(str(exp["total_amount"]))
    lines.append({"account_code": exp["fund_gl"], "debit": 0, "credit": total_amt,
                   "description": exp["description"]})

    je = await _post_je(db, tid, user.email, "PET", exp["expense_date"],
                        exp["description"], lines, exp["reference"])

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
# RECURRING TRANSACTIONS
# ══════════════════════════════════════════════════════════
@router.get("/recurring-transactions")
async def list_recurring(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT rt.*, ba.account_name AS bank_account_name
        FROM tr_recurring_transactions rt
        LEFT JOIN tr_bank_accounts ba ON ba.id=rt.bank_account_id
        WHERE rt.tenant_id=:tid
        ORDER BY rt.next_due_date, rt.name
    """), {"tid": tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/recurring-transactions", status_code=201)
async def create_recurring(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    rid = str(uuid.uuid4())
    # تحويل التاريخ والمبلغ بأمان
    raw_ndd = data.get("next_due_date")
    ndd = date.fromisoformat(str(raw_ndd)) if raw_ndd else None
    try:
        await db.execute(text("""
            INSERT INTO tr_recurring_transactions
              (id, tenant_id, name, source, tx_type, bank_account_id,
               counterpart_account, amount, currency_code, description,
               frequency, next_due_date, is_active,
               branch_code, cost_center, project_code, expense_classification_code,
               created_by)
            VALUES
              (:id, :tid, :name, :src, :tt, :ba,
               :cp, :amt, :cur, :desc,
               :freq, :ndd, true,
               :br, :cc, :pr, :ec,
               :by)
        """), {
            "id": rid, "tid": tid,
            "name": str(data["name"]).strip(),
            "src":  data.get("source") or "bank",
            "tt":   data.get("tx_type") or "BP",
            "ba":   str(data["bank_account_id"]) if data.get("bank_account_id") else None,
            "cp":   data.get("counterpart_account") or None,
            "amt":  Decimal(str(data.get("amount") or 0)),
            "cur":  data.get("currency_code") or "SAR",
            "desc": data.get("description") or None,
            "freq": data.get("frequency") or "monthly",
            "ndd":  ndd,
            "br":   data.get("branch_code") or None,
            "cc":   data.get("cost_center") or None,
            "pr":   data.get("project_code") or None,
            "ec":   data.get("expense_classification_code") or None,
            "by":   user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في الحفظ: {str(e)}")
    return created(data={"id": rid}, message="تم إنشاء المعاملة المتكررة ✅")


@router.put("/recurring-transactions/{rid}")
async def update_recurring(
    rid: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ALLOWED = {"name","source","tx_type","bank_account_id","counterpart_account","amount",
               "currency_code","description","frequency","next_due_date","is_active",
               "branch_code","cost_center","project_code","expense_classification_code"}
    safe = {k:v for k,v in data.items() if k in ALLOWED}
    if not safe: raise HTTPException(400, "لا توجد بيانات")
    # تحويل الأنواع بأمان
    if "amount" in safe and safe["amount"] is not None:
        safe["amount"] = Decimal(str(safe["amount"]))
    if "next_due_date" in safe and safe["next_due_date"]:
        safe["next_due_date"] = date.fromisoformat(str(safe["next_due_date"]))
    elif "next_due_date" in safe:
        safe["next_due_date"] = None
    if "bank_account_id" in safe and safe["bank_account_id"]:
        safe["bank_account_id"] = str(safe["bank_account_id"])
    safe["updated_at"] = datetime.utcnow()
    set_clause = ", ".join([f"{k}=:{k}" for k in safe.keys()])
    safe.update({"id": str(rid), "tid": tid})
    try:
        await db.execute(text(f"UPDATE tr_recurring_transactions SET {set_clause} WHERE id=:id AND tenant_id=:tid"), safe)
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ في التعديل: {str(e)}")
    return ok(message="تم التعديل ✅")


@router.delete("/recurring-transactions/{rid}")
async def delete_recurring(
    rid: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    await db.execute(text("DELETE FROM tr_recurring_transactions WHERE id=:id AND tenant_id=:tid"),
                     {"id": str(rid), "tid": tid})
    await db.commit()
    return ok(message="تم الحذف")


@router.post("/recurring-transactions/{rid}/execute")
async def execute_recurring(
    rid: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """إنشاء مسودة معاملة من القالب المتكرر"""
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM tr_recurring_transactions WHERE id=:id AND tenant_id=:tid"),
                         {"id": str(rid), "tid": tid})
    rec = r.mappings().fetchone()
    if not rec: raise HTTPException(404, "القالب غير موجود")

    tx_date = date.today()
    if rec["source"] == "cash":
        serial = await _next_serial(db, tid, rec["tx_type"], tx_date)
        tx_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO tr_cash_transactions
              (id,tenant_id,serial,tx_type,tx_date,amount,currency_code,counterpart_account,
               description,branch_code,cost_center,project_code,expense_classification_code,
               status,created_by)
            VALUES
              (:id,:tid,:serial,:tt,:dt,:amt,:cur,:cp,
               :desc,:br,:cc,:pr,:ec,
               'draft',:by)
        """), {
            "id": tx_id, "tid": tid, "serial": serial,
            "tt": rec["tx_type"], "dt": tx_date,
            "amt": rec["amount"], "cur": rec["currency_code"],
            "cp": rec["counterpart_account"], "desc": rec["description"],
            "br": rec["branch_code"], "cc": rec["cost_center"],
            "pr": rec["project_code"], "ec": rec["expense_classification_code"],
            "by": user.email,
        })
    else:
        serial = await _next_serial(db, tid, rec["tx_type"], tx_date)
        tx_id = str(uuid.uuid4())
        await db.execute(text("""
            INSERT INTO tr_bank_transactions
              (id,tenant_id,serial,tx_type,tx_date,bank_account_id,amount,currency_code,
               counterpart_account,description,branch_code,cost_center,project_code,
               expense_classification_code,status,created_by)
            VALUES
              (:id,:tid,:serial,:tt,:dt,:ba,:amt,:cur,
               :cp,:desc,:br,:cc,:pr,:ec,'draft',:by)
        """), {
            "id": tx_id, "tid": tid, "serial": serial,
            "tt": rec["tx_type"], "dt": tx_date,
            "ba": str(rec["bank_account_id"]) if rec["bank_account_id"] else None,
            "amt": rec["amount"], "cur": rec["currency_code"],
            "cp": rec["counterpart_account"], "desc": rec["description"],
            "br": rec["branch_code"], "cc": rec["cost_center"],
            "pr": rec["project_code"], "ec": rec["expense_classification_code"],
            "by": user.email,
        })

    # تحديث تاريخ التنفيذ الأخير والتالي
    from dateutil.relativedelta import relativedelta
    freq = rec["frequency"] or "monthly"
    current_due = rec["next_due_date"] or tx_date
    if freq == "weekly":     next_due = current_due + relativedelta(weeks=1)
    elif freq == "quarterly": next_due = current_due + relativedelta(months=3)
    elif freq == "yearly":   next_due = current_due + relativedelta(years=1)
    else:                    next_due = current_due + relativedelta(months=1)

    await db.execute(text("""
        UPDATE tr_recurring_transactions
        SET last_executed_date=:last, next_due_date=:next
        WHERE id=:id AND tenant_id=:tid
    """), {"last": tx_date, "next": next_due, "id": str(rid), "tid": tid})

    await db.commit()
    return created(data={"tx_id": tx_id, "serial": serial},
                   message=f"تم إنشاء مسودة {serial} ✅")


# ══════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════
# BANK FEES / COMMISSIONS
# ══════════════════════════════════════════════════════════
@router.get("/bank-fees")
async def list_bank_fees(
    bank_account_id: Optional[uuid.UUID] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to:   Optional[date] = Query(None),
    limit: int = Query(200),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds = ["f.tenant_id=:tid"]
    params: dict = {"tid": tid}
    if bank_account_id: conds.append("f.bank_account_id=:ba"); params["ba"] = str(bank_account_id)
    if date_from: conds.append("f.fee_date>=:df"); params["df"] = str(date_from)
    if date_to:   conds.append("f.fee_date<=:dt"); params["dt"] = str(date_to)
    where = " AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT f.*, ba.account_name AS bank_account_name
        FROM tr_bank_fees f
        LEFT JOIN tr_bank_accounts ba ON ba.id=f.bank_account_id
        WHERE {where}
        ORDER BY f.fee_date DESC, f.created_at DESC
        LIMIT :lim
    """), {**params, "lim": limit})
    items = [dict(row._mapping) for row in r.fetchall()]
    total = sum(float(i["amount"]) for i in items)
    return ok(data={"items": items, "total": total, "count": len(items)})


@router.post("/bank-fees")
async def create_bank_fee(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    if not data.get("bank_account_id"): raise HTTPException(400, "الحساب البنكي مطلوب")
    if not data.get("fee_date"):        raise HTTPException(400, "تاريخ الرسوم مطلوب")
    if not data.get("amount"):          raise HTTPException(400, "المبلغ مطلوب")
    fee_date_dt = date.fromisoformat(str(data["fee_date"]))
    await _check_period(db, tid, fee_date_dt)

    fee_id = str(uuid.uuid4())
    try:
        await db.execute(text("""
            INSERT INTO tr_bank_fees
              (id, tenant_id, bank_account_id, fee_date, fee_type, amount, currency_code, description, created_by)
            VALUES
              (:id, :tid, :ba, :dt, :ft, :amt, :cur, :desc, :by)
        """), {
            "id":  fee_id,
            "tid": tid,
            "ba":  str(data["bank_account_id"]),
            "dt":  date.fromisoformat(str(data["fee_date"])),
            "ft":  data.get("fee_type") or "other",
            "amt": Decimal(str(data["amount"])),
            "cur": data.get("currency_code") or "SAR",
            "desc":data.get("description") or None,
            "by":  user.email,
        })
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, f"خطأ: {str(e)}")
    return created(data={"id": fee_id}, message="تم تسجيل الرسوم ✅")


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
    return ok(message="تم الحذف")


# ══════════════════════════════════════════════════════════
# ACTIVITY LOG
# ══════════════════════════════════════════════════════════
@router.get("/activity-log")
async def activity_log(
    limit: int = Query(150),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT 'cash' AS source, serial, tx_type, tx_date::text AS tx_date,
               amount, currency_code, status, description,
               created_by, created_at,
               posted_by, posted_at,
               NULL::text AS bank_account_name
        FROM tr_cash_transactions WHERE tenant_id=:tid
        UNION ALL
        SELECT 'bank', bt.serial, bt.tx_type, bt.tx_date::text,
               bt.amount, bt.currency_code, bt.status, bt.description,
               bt.created_by, bt.created_at,
               bt.posted_by, bt.posted_at,
               ba.account_name
        FROM tr_bank_transactions bt
        LEFT JOIN tr_bank_accounts ba ON ba.id=bt.bank_account_id
        WHERE bt.tenant_id=:tid
        UNION ALL
        SELECT 'transfer', it.serial, 'IT', it.tx_date::text,
               it.amount, it.currency_code, it.status, it.notes,
               it.created_by, it.created_at,
               it.posted_by, it.posted_at,
               fa.account_name
        FROM tr_internal_transfers it
        LEFT JOIN tr_bank_accounts fa ON fa.id=it.from_account_id
        WHERE it.tenant_id=:tid
        ORDER BY created_at DESC
        LIMIT :lim
    """), {"tid": tid, "lim": limit})
    items = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"items": items, "count": len(items)})


@router.get("/reports/cash-position")
async def cash_position_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT account_code, account_name, account_type,
               currency_code, current_balance, low_balance_alert,
               CASE WHEN current_balance <= low_balance_alert THEN true ELSE false END AS is_low
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true
        ORDER BY account_type, account_name
    """), {"tid": tid})
    accounts = [dict(row._mapping) for row in r.fetchall()]
    total = sum(float(a["current_balance"]) for a in accounts)
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


@router.get("/reports/account-statement")
async def account_statement(
    account_id: uuid.UUID = Query(...),
    date_from:  Optional[date] = Query(None),
    date_to:    Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """كشف حساب بنكي مع رصيد متراكم"""
    tid = str(user.tenant_id)
    acc_r = await db.execute(text("""
        SELECT * FROM tr_bank_accounts WHERE id=:id AND tenant_id=:tid
    """), {"id": str(account_id), "tid": tid})
    acc = acc_r.mappings().fetchone()
    if not acc: raise HTTPException(404, "الحساب غير موجود")

    conds_base = ["tenant_id=:tid", "bank_account_id=:acc_id", "status='posted'"]
    params: dict = {"tid": tid, "acc_id": str(account_id)}
    if date_from: conds_base.append("tx_date>=:df"); params["df"] = str(date_from)
    if date_to:   conds_base.append("tx_date<=:dt"); params["dt"] = str(date_to)
    where = " AND ".join(conds_base)

    # opening balance = balance before date_from
    opening = float(acc["current_balance"] or 0)
    if date_from:
        ob_r = await db.execute(text(f"""
            SELECT COALESCE(SUM(
                CASE WHEN tx_type IN ('RV','BR') THEN amount_sar ELSE -amount_sar END
            ), 0)
            FROM (
                SELECT tx_type, amount_sar FROM tr_cash_transactions
                WHERE tenant_id=:tid AND bank_account_id=:acc_id AND status='posted' AND tx_date < :df
                UNION ALL
                SELECT tx_type, amount_sar FROM tr_bank_transactions
                WHERE tenant_id=:tid AND bank_account_id=:acc_id AND status='posted' AND tx_date < :df
            ) t
        """), {"tid": tid, "acc_id": str(account_id), "df": str(date_from)})
        recent_total = float(ob_r.scalar() or 0)
        opening = float(acc["current_balance"] or 0) - recent_total

    tx_r = await db.execute(text(f"""
        SELECT tx_date, serial, tx_type, description, party_name AS party,
               reference, amount_sar AS amount, 'cash' AS src, id
        FROM tr_cash_transactions WHERE {where}
        UNION ALL
        SELECT tx_date, serial, tx_type, description, beneficiary_name AS party,
               reference, amount_sar AS amount, 'bank' AS src, id
        FROM tr_bank_transactions WHERE {where}
        ORDER BY tx_date, serial
    """), params)

    rows = []
    running = opening
    for row in tx_r.fetchall():
        r = dict(row._mapping)
        amt = float(r["amount"] or 0)
        is_debit = r["tx_type"] in ("RV", "BR")
        debit = amt if is_debit else 0
        credit = 0 if is_debit else amt
        running += debit - credit
        rows.append({**r, "debit": debit, "credit": credit, "balance": round(running, 2)})

    return ok(data={
        "account": dict(acc._mapping),
        "opening_balance": round(opening, 2),
        "closing_balance": round(running, 2),
        "rows": rows,
        "total_debit":  sum(r["debit"] for r in rows),
        "total_credit": sum(r["credit"] for r in rows),
    })


@router.get("/reports/check-aging")
async def check_aging_report(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تقرير أعمار الديون — الشيكات والمدفوعات المتأخرة"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT c.*,
               ba.account_name AS bank_account_name,
               CURRENT_DATE - due_date AS days_overdue
        FROM tr_checks c
        LEFT JOIN tr_bank_accounts ba ON ba.id=c.bank_account_id
        WHERE c.tenant_id=:tid
          AND c.status IN ('pending','deposited')
          AND c.due_date IS NOT NULL
        ORDER BY c.due_date
    """), {"tid": tid})
    checks = [dict(row._mapping) for row in r.fetchall()]

    def bucket(days):
        if days < 0: return "future"
        if days == 0: return "today"
        if days <= 30: return "1-30"
        if days <= 60: return "31-60"
        if days <= 90: return "61-90"
        return "90+"

    buckets: dict = {"future": [], "today": [], "1-30": [], "31-60": [], "61-90": [], "90+": []}
    for c in checks:
        days = int(c["days_overdue"] or 0)
        c["days_overdue"] = days
        b = bucket(days)
        c["bucket"] = b
        buckets[b].append(c)

    summary = {k: {"count": len(v), "total": sum(float(x.get("amount",0)) for x in v)} for k, v in buckets.items()}
    return ok(data={"checks": checks, "buckets": summary})


@router.get("/reports/low-balance-alerts")
async def low_balance_alerts(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """حسابات رصيدها أقل من أو يساوي حد التنبيه"""
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT id, account_name, account_type, current_balance, low_balance_alert, currency_code
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true
          AND low_balance_alert > 0
          AND current_balance <= low_balance_alert
        ORDER BY (current_balance - low_balance_alert)
    """), {"tid": tid})
    alerts = [dict(row._mapping) for row in r.fetchall()]
    return ok(data=alerts)


@router.get("/reports/balance-history")
async def balance_history_report(
    months: int = Query(6),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """تاريخ الرصيد الشهري لكل حساب — آخر N أشهر"""
    tid = str(user.tenant_id)
    # fetch all accounts
    acc_r = await db.execute(text("""
        SELECT id, account_name, current_balance
        FROM tr_bank_accounts WHERE tenant_id=:tid AND is_active=true
    """), {"tid": tid})
    accounts = [dict(r._mapping) for r in acc_r.fetchall()]

    # fetch all posted transactions grouped by account + month
    tx_r = await db.execute(text("""
        SELECT bank_account_id,
               TO_CHAR(DATE_TRUNC('month', tx_date), 'YYYY-MM') AS month,
               SUM(CASE WHEN tx_type IN ('RV','BR') THEN amount_sar ELSE -amount_sar END) AS net
        FROM (
            SELECT bank_account_id, tx_date, tx_type, amount_sar FROM tr_cash_transactions
            WHERE tenant_id=:tid AND status='posted' AND bank_account_id IS NOT NULL
              AND tx_date >= DATE_TRUNC('month', CURRENT_DATE) - (:months - 1) * INTERVAL '1 month'
            UNION ALL
            SELECT bank_account_id, tx_date, tx_type, amount_sar FROM tr_bank_transactions
            WHERE tenant_id=:tid AND status='posted'
              AND tx_date >= DATE_TRUNC('month', CURRENT_DATE) - (:months - 1) * INTERVAL '1 month'
        ) t
        GROUP BY bank_account_id, DATE_TRUNC('month', tx_date)
        ORDER BY bank_account_id, month
    """), {"tid": tid, "months": months})

    # build month buckets
    from datetime import date as dt_date
    today = dt_date.today()
    month_list = []
    for i in range(months - 1, -1, -1):
        y = today.year
        m = today.month - i
        while m <= 0: m += 12; y -= 1
        month_list.append(f"{y:04d}-{m:02d}")

    # group net by account + month
    nets: dict = {}
    for row in tx_r.fetchall():
        acc_id = str(row[0])
        nets.setdefault(acc_id, {})[row[1]] = float(row[2] or 0)

    result = []
    for acc in accounts:
        acc_id = str(acc["id"])
        acc_nets = nets.get(acc_id, {})
        current_bal = float(acc["current_balance"] or 0)

        # work backwards from current balance to reconstruct monthly snapshots
        months_net = [acc_nets.get(m, 0) for m in month_list]
        running = current_bal
        snapshots = []
        for i in range(len(month_list) - 1, -1, -1):
            snapshots.insert(0, {"month": month_list[i], "balance": round(running, 2)})
            if i > 0:
                running -= months_net[i]  # go back

        result.append({
            "id": acc_id,
            "account_name": acc["account_name"],
            "history": snapshots,
        })
    return ok(data=result)


@router.get("/reports/cash-forecast")
async def cash_forecast(
    days: int = Query(30),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """توقع المركز النقدي للأيام القادمة:
       رصيد حالي + متكررات + مستحقات AP (مدفوعات) + مستحقات AR (تحصيلات)
    """
    tid = str(user.tenant_id)
    from datetime import date as dt_date, timedelta

    today  = dt_date.today()
    cutoff = today + timedelta(days=days)
    p = {"tid": tid, "today": str(today), "cutoff": str(cutoff)}

    # ── رصيد البنوك والصناديق الحالي ──────────────────────────
    bal_r = await db.execute(text("""
        SELECT COALESCE(SUM(current_balance), 0)
        FROM tr_bank_accounts
        WHERE tenant_id=:tid AND is_active=true
    """), {"tid": tid})
    total_balance = float(bal_r.scalar() or 0)

    # ── المعاملات المتكررة ─────────────────────────────────────
    rec_r = await db.execute(text("""
        SELECT next_due_date::text, tx_type, estimated_amount, description
        FROM tr_recurring_transactions
        WHERE tenant_id=:tid AND is_active=true
          AND next_due_date BETWEEN :today AND :cutoff
        ORDER BY next_due_date
    """), p)
    recs = rec_r.mappings().fetchall()

    # ── فواتير AP مستحقة الدفع (outflow) ─────────────────────
    try:
        ap_r = await db.execute(text("""
            SELECT
                GREATEST(due_date, :today::date)::text AS day,
                COALESCE(SUM(balance_due), 0)          AS total,
                COUNT(*)                               AS cnt,
                STRING_AGG(DISTINCT v.vendor_name, '، ' ORDER BY v.vendor_name) AS vendors
            FROM ap_invoices i
            LEFT JOIN ap_vendors v ON v.id = i.vendor_id
            WHERE i.tenant_id=:tid
              AND i.status='posted'
              AND i.balance_due > 0
              AND i.due_date <= :cutoff
            GROUP BY GREATEST(due_date, :today::date)
            ORDER BY 1
        """), p)
        ap_rows = ap_r.mappings().fetchall()
    except Exception:
        ap_rows = []

    # ── فواتير AR مستحقة التحصيل (inflow) ────────────────────
    try:
        ar_r = await db.execute(text("""
            SELECT
                GREATEST(due_date, :today::date)::text AS day,
                COALESCE(SUM(balance_due), 0)          AS total,
                COUNT(*)                               AS cnt,
                STRING_AGG(DISTINCT c.customer_name, '، ' ORDER BY c.customer_name) AS customers
            FROM ar_invoices i
            LEFT JOIN ar_customers c ON c.id = i.customer_id
            WHERE i.tenant_id=:tid
              AND i.status='posted'
              AND i.balance_due > 0
              AND i.due_date <= :cutoff
            GROUP BY GREATEST(due_date, :today::date)
            ORDER BY 1
        """), p)
        ar_rows = ar_r.mappings().fetchall()
    except Exception:
        ar_rows = []

    # ── بناء خريطة الأحداث اليومية ────────────────────────────
    # الهيكل: { "YYYY-MM-DD": { inflow, outflow, items: [...] } }
    events: dict = {}

    def add_event(d: str, direction: str, amt: float, source: str, label: str):
        events.setdefault(d, {"inflow": 0.0, "outflow": 0.0, "items": []})
        events[d][direction] += amt
        events[d]["items"].append({"source": source, "label": label,
                                   "amount": round(amt, 2), "direction": direction})

    for rec in recs:
        d   = rec["next_due_date"]
        amt = float(rec["estimated_amount"] or 0)
        direction = "inflow" if rec["tx_type"] in ("RV", "BR") else "outflow"
        add_event(d, direction, amt, "recurring", rec["description"] or "متكرر")

    for row in ap_rows:
        add_event(row["day"], "outflow", float(row["total"]),
                  "ap", f"مستحقات موردين ({row['cnt']} فاتورة) — {row['vendors'] or ''}")

    for row in ar_rows:
        add_event(row["day"], "inflow", float(row["total"]),
                  "ar", f"تحصيلات عملاء ({row['cnt']} فاتورة) — {row['customers'] or ''}")

    # ── بناء التوقع اليومي ─────────────────────────────────────
    running  = total_balance
    forecast = []
    for i in range(days + 1):
        d  = str(today + timedelta(days=i))
        ev = events.get(d, {})
        inflow  = ev.get("inflow",  0.0)
        outflow = ev.get("outflow", 0.0)
        running += inflow - outflow
        forecast.append({
            "date":    d,
            "balance": round(running, 2),
            "inflow":  round(inflow,  2),
            "outflow": round(outflow, 2),
            "items":   ev.get("items", []),
        })

    # ── ملخص المصادر ───────────────────────────────────────────
    summary = {
        "ap_due":  round(sum(float(r["total"]) for r in ap_rows), 2),
        "ar_due":  round(sum(float(r["total"]) for r in ar_rows), 2),
        "rec_out": round(sum(float(r["estimated_amount"] or 0)
                             for r in recs if r["tx_type"] not in ("RV","BR")), 2),
        "rec_in":  round(sum(float(r["estimated_amount"] or 0)
                             for r in recs if r["tx_type"] in ("RV","BR")), 2),
    }

    return ok(data={"start_balance": total_balance, "days": forecast, "summary": summary})


@router.get("/reports/monthly-cash-flow")
async def monthly_cash_flow_report(
    months: int = Query(12),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
            TO_CHAR(DATE_TRUNC('month', tx_date), 'YYYY-MM') AS month,
            SUM(CASE WHEN tx_type='RV' AND src='cash' THEN amount ELSE 0 END) AS cash_receipts,
            SUM(CASE WHEN tx_type='PV' AND src='cash' THEN amount ELSE 0 END) AS cash_payments,
            SUM(CASE WHEN tx_type='BR' AND src='bank' THEN amount ELSE 0 END) AS bank_receipts,
            SUM(CASE WHEN tx_type='BP' AND src='bank' THEN amount ELSE 0 END) AS bank_payments
        FROM (
            SELECT tx_date, tx_type, amount, 'cash' AS src
            FROM tr_cash_transactions
            WHERE tenant_id=:tid AND status='posted'
              AND tx_date >= DATE_TRUNC('month', CURRENT_DATE) - (:months - 1) * INTERVAL '1 month'
            UNION ALL
            SELECT tx_date, tx_type, amount, 'bank' AS src
            FROM tr_bank_transactions
            WHERE tenant_id=:tid AND status='posted'
              AND tx_date >= DATE_TRUNC('month', CURRENT_DATE) - (:months - 1) * INTERVAL '1 month'
        ) combined
        GROUP BY DATE_TRUNC('month', tx_date)
        ORDER BY month
    """), {"tid": tid, "months": months})
    rows = []
    for row in r.fetchall():
        cr = float(row[1] or 0)
        cp = float(row[2] or 0)
        br = float(row[3] or 0)
        bp = float(row[4] or 0)
        rows.append({
            "month": row[0],
            "cash_receipts":  cr,
            "cash_payments":  cp,
            "bank_receipts":  br,
            "bank_payments":  bp,
            "total_receipts": cr + br,
            "total_payments": cp + bp,
            "net": (cr + br) - (cp + bp),
        })
    return ok(data={"rows": rows, "count": len(rows)})
