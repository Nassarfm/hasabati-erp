"""
app/modules/treasury/router.py
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
from fastapi import APIRouter, Depends, Query
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
    from app.services.numbering.series_service import NumberSeriesService
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
    return {"je_id": str(result.je_id), "je_serial": result.je_serial}


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

    # الحقول المسموح بها فقط — تجنب SQL error من حقول غير موجودة
    ALLOWED = {
        "account_code","account_name","account_name_en","account_type",
        "bank_name","bank_branch","account_number","iban","swift_code",
        "currency_code","gl_account_code","opening_balance",
        "low_balance_alert","credit_limit","notes",
    }
    safe = {k:v for k,v in data.items() if k in ALLOWED and v is not None and v != ""}

    ba_id = str(uuid.uuid4())
    safe["id"]              = ba_id
    safe["tenant_id"]       = tid
    safe["current_balance"] = safe.get("opening_balance", 0)
    safe["created_by"]      = user.email

    cols = ", ".join(safe.keys())
    vals = ", ".join([f":{k}" for k in safe.keys()])

    try:
        await db.execute(text(f"INSERT INTO tr_bank_accounts ({cols}) VALUES ({vals})"), safe)
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"خطأ في الحفظ: {str(e)}")

    return created(data={"id": ba_id, "account_code": safe.get("account_code")},
                   message="تم تعريف الحساب ✅")


@router.put("/bank-accounts/{ba_id}")
async def update_bank_account(
    ba_id: uuid.UUID,
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)

    ALLOWED = {
        "account_code","account_name","account_name_en","account_type",
        "bank_name","bank_branch","account_number","iban","swift_code",
        "currency_code","gl_account_code","opening_balance",
        "low_balance_alert","credit_limit","notes","is_active",
    }
    safe = {k:v for k,v in data.items() if k in ALLOWED}
    if not safe:
        raise HTTPException(status_code=400, detail="لا توجد بيانات للتعديل")

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
               branch_code,cost_center,project_code,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,
               :cur,:rate,:amt_sar,:cp_acc,
               :desc,:party,:ref,:method,:check_no,
               :branch,:cc,:proj,:notes,'draft',:by)
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
            "notes": data.get("notes"), "by": user.email,
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
    amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    tx_date = tx["tx_date"]
    desc = tx["description"]

    # PV = صرف → CR صندوق/بنك, DR الطرف المقابل
    # RV = قبض → DR صندوق/بنك, CR الطرف المقابل
    if tx["tx_type"] == "RV":
        lines = [
            {"account_code": gl, "debit": amt, "credit": 0, "description": desc},
            {"account_code": cp, "debit": 0, "credit": amt, "description": desc},
        ]
    else:
        lines = [
            {"account_code": cp, "debit": amt, "credit": 0, "description": desc},
            {"account_code": gl, "debit": 0, "credit": amt, "description": desc},
        ]

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
           branch_code,cost_center,project_code,notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:tx_type,:tx_date,:ba_id,:amount,
           :cur,:rate,:amt_sar,:cp_acc,
           :ben_name,:ben_iban,:ben_bank,
           :desc,:ref,:method,:check_no,
           :branch,:cc,:proj,:notes,'draft',:by)
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

    gl = tx["gl_account_code"]
    cp = tx["counterpart_account"] or "9999"
    amt = Decimal(str(tx["amount_sar"] or tx["amount"]))
    desc = tx["description"]
    tx_date = tx["tx_date"]

    # BP = دفعة → DR مورد/مصروف, CR بنك
    # BR = قبض  → DR بنك, CR عميل/إيراد
    # BT = تحويل → DR مستفيد, CR بنك
    if tx["tx_type"] == "BR":
        lines = [
            {"account_code": gl, "debit": amt, "credit": 0, "description": desc},
            {"account_code": cp, "debit": 0, "credit": amt, "description": desc},
        ]
        delta = amt
    else:  # BP, BT
        lines = [
            {"account_code": cp, "debit": amt, "credit": 0, "description": desc},
            {"account_code": gl, "debit": 0, "credit": amt, "description": desc},
        ]
        delta = -amt

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
