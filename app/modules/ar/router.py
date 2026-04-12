"""
app/modules/ar/router.py
══════════════════════════════════════════════════════════
AR & Sales Module — Complete API
الذمم المدينة والمبيعات | ZATCA Compliant

Endpoints: Customers, Invoices, Receipts, Credit Notes,
           ZATCA QR/XML, Reports (Aging, Statement)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/ar", tags=["المبيعات والذمم المدينة"])
TID = "00000000-0000-0000-0000-000000000001"


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
async def _next_serial(db, tid: str, je_type: str, tx_date: date) -> str:
    year = tx_date.year
    r = await db.execute(text("""
        UPDATE je_sequences SET last_sequence=last_sequence+1
        WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:year
        RETURNING last_sequence
    """), {"tid":tid,"code":je_type,"year":year})
    row = r.fetchone()
    if not row:
        await db.execute(text("""
            INSERT INTO je_sequences (id,tenant_id,je_type_code,fiscal_year,last_sequence)
            VALUES (gen_random_uuid(),:tid,:code,:year,1)
            ON CONFLICT(tenant_id,je_type_code,fiscal_year)
            DO UPDATE SET last_sequence=je_sequences.last_sequence+1
        """), {"tid":tid,"code":je_type,"year":year})
        r2 = await db.execute(text("""
            SELECT last_sequence FROM je_sequences
            WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:year
        """), {"tid":tid,"code":je_type,"year":year})
        seq = r2.fetchone()[0]
    else:
        seq = row[0]
    sr = await db.execute(text("""
        SELECT prefix,padding,separator FROM series_settings
        WHERE tenant_id=:tid AND je_type_code=:code LIMIT 1
    """), {"tid":tid,"code":je_type})
    srow = sr.fetchone()
    prefix  = srow[0] if srow else je_type
    padding = srow[1] if srow else 7
    sep     = srow[2] if srow else "-"
    return f"{prefix}{sep}{year}{sep}{seq:0{padding}d}"


async def _post_je(db, tid, user_email, je_type, tx_date, description, lines, reference=None):
    try:
        from app.services.posting.engine import PostingEngine, PostingRequest, PostingLine
        t_id = uuid.UUID(tid)
        engine = PostingEngine(db, t_id)
        posting_lines = [PostingLine(
            account_code=l["account_code"],description=l.get("description",description),
            debit=Decimal(str(l.get("debit",0))),credit=Decimal(str(l.get("credit",0))),
        ) for l in lines]
        result = await engine.post(PostingRequest(
            tenant_id=t_id,je_type=je_type,description=description,
            entry_date=tx_date,lines=posting_lines,
            created_by_email=user_email,reference=reference,source_module="ar",
        ))
        return {"je_id":str(result.je_id),"je_serial":result.je_serial}
    except Exception:
        return {"je_id":None,"je_serial":None}


async def _get_zatca_settings(db, tid: str) -> dict:
    r = await db.execute(text("SELECT * FROM ar_zatca_settings WHERE tenant_id=:tid"), {"tid":tid})
    row = r.mappings().fetchone()
    if not row:
        # جلب من company_settings كبديل
        r2 = await db.execute(text("""
            SELECT company_name, vat_number, cr_number,
                   city, district, street, building_no, postal_code
            FROM company_settings WHERE tenant_id=:tid
        """), {"tid":tid})
        cs = r2.mappings().fetchone()
        if cs:
            return {"seller_name":cs["company_name"],"vat_number":cs["vat_number"],
                    "cr_number":cs["cr_number"],"city":cs["city"],
                    "district":cs["district"],"street":cs["street"],
                    "building_number":cs.get("building_no"),"postal_code":cs["postal_code"]}
    return dict(row._mapping) if row else {}


def _calc_lines(lines: list) -> dict:
    """حساب إجماليات الفاتورة من الأسطر"""
    subtotal = Decimal(0)
    vat_total = Decimal(0)
    discount_total = Decimal(0)
    for line in lines:
        qty   = Decimal(str(line.get("quantity", 1)))
        price = Decimal(str(line.get("unit_price", 0)))
        disc_pct = Decimal(str(line.get("discount_pct", 0)))
        vat_rate = Decimal(str(line.get("vat_rate", 15)))
        gross  = qty * price
        disc   = gross * disc_pct / 100
        net    = gross - disc
        vat    = net * vat_rate / 100
        line["discount_amount"] = float(disc)
        line["net_amount"]      = float(net)
        line["vat_amount"]      = float(round(vat, 2))
        line["total_amount"]    = float(round(net + vat, 2))
        subtotal      += net
        vat_total     += vat
        discount_total+= disc
    return {
        "subtotal":        float(round(subtotal, 2)),
        "discount_amount": float(round(discount_total, 2)),
        "taxable_amount":  float(round(subtotal, 2)),
        "vat_amount":      float(round(vat_total, 2)),
        "total_amount":    float(round(subtotal + vat_total, 2)),
    }


# ══════════════════════════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════════════════════════
@router.get("/dashboard")
async def dashboard(db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM ar_customers WHERE tenant_id=:tid AND is_active=true) AS customers,
          (SELECT COUNT(*) FROM ar_invoices  WHERE tenant_id=:tid AND status='draft') AS draft_invoices,
          (SELECT COUNT(*) FROM ar_invoices  WHERE tenant_id=:tid AND status='posted' AND balance_due>0) AS open_invoices,
          (SELECT COALESCE(SUM(balance_due),0) FROM ar_invoices WHERE tenant_id=:tid AND status='posted') AS total_receivables,
          (SELECT COALESCE(SUM(total_amount),0) FROM ar_invoices WHERE tenant_id=:tid AND status='posted' AND invoice_date>=DATE_TRUNC('month',CURRENT_DATE)) AS month_sales,
          (SELECT COALESCE(SUM(amount),0) FROM ar_receipts WHERE tenant_id=:tid AND status='posted' AND receipt_date>=DATE_TRUNC('month',CURRENT_DATE)) AS month_collections,
          (SELECT COUNT(*) FROM ar_invoices WHERE tenant_id=:tid AND status='posted' AND due_date<CURRENT_DATE AND balance_due>0) AS overdue_count,
          (SELECT COALESCE(SUM(balance_due),0) FROM ar_invoices WHERE tenant_id=:tid AND status='posted' AND due_date<CURRENT_DATE AND balance_due>0) AS overdue_amount,
          (SELECT COUNT(*) FROM ar_invoices WHERE tenant_id=:tid AND zatca_status='pending' AND status='posted') AS zatca_pending
    """), {"tid":tid})
    krow = r.mappings().fetchone()

    # أكبر 5 عملاء بالمديونية
    r2 = await db.execute(text("""
        SELECT c.customer_name, COALESCE(SUM(i.balance_due),0) AS balance
        FROM ar_customers c
        JOIN ar_invoices i ON i.customer_id=c.id AND i.tenant_id=:tid AND i.status='posted'
        WHERE c.tenant_id=:tid AND i.balance_due>0
        GROUP BY c.id,c.customer_name
        ORDER BY balance DESC LIMIT 5
    """), {"tid":tid})
    top_debtors = [dict(row._mapping) for row in r2.fetchall()]

    # مبيعات آخر 6 أشهر
    r3 = await db.execute(text("""
        SELECT TO_CHAR(invoice_date,'YYYY-MM') AS month,
               COALESCE(SUM(total_amount),0) AS sales,
               COALESCE(SUM(vat_amount),0) AS vat
        FROM ar_invoices
        WHERE tenant_id=:tid AND status='posted'
          AND invoice_date >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY month ORDER BY month
    """), {"tid":tid})
    monthly_sales = [dict(row._mapping) for row in r3.fetchall()]

    # Aging buckets
    r4 = await db.execute(text("""
        SELECT
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date<=30  THEN balance_due END),0) AS bucket_30,
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date BETWEEN 31 AND 60 THEN balance_due END),0) AS bucket_60,
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date BETWEEN 61 AND 90 THEN balance_due END),0) AS bucket_90,
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date>90  THEN balance_due END),0) AS bucket_90plus
        FROM ar_invoices
        WHERE tenant_id=:tid AND status='posted' AND balance_due>0
    """), {"tid":tid})
    aging = dict(r4.mappings().fetchone())

    return ok(data={
        "kpis": dict(krow._mapping),
        "top_debtors":   top_debtors,
        "monthly_sales": monthly_sales,
        "aging_summary": aging,
    })


# ══════════════════════════════════════════════════════════
# CUSTOMERS
# ══════════════════════════════════════════════════════════
@router.get("/customers")
async def list_customers(
    search:      Optional[str]=Query(None),
    customer_type:Optional[str]=Query(None),
    limit: int=Query(50), offset: int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["c.tenant_id=:tid AND c.is_active=true"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if search: conds.append("(c.customer_name ILIKE :s OR c.customer_code ILIKE :s OR c.vat_number ILIKE :s OR c.phone ILIKE :s)"); params["s"]=f"%{search}%"
    if customer_type: conds.append("c.customer_type=:ct"); params["ct"]=customer_type
    where=" AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM ar_customers c WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT c.*,
               COALESCE(SUM(i.balance_due),0) AS current_balance,
               COUNT(DISTINCT i.id) AS invoice_count
        FROM ar_customers c
        LEFT JOIN ar_invoices i ON i.customer_id=c.id AND i.status='posted'
        WHERE {where}
        GROUP BY c.id
        ORDER BY c.customer_name
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/customers", status_code=201)
async def create_customer(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    cid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO ar_customers
          (id,tenant_id,customer_code,customer_name,customer_name_en,customer_type,
           vat_number,cr_number,country,city,district,street,building_number,postal_code,
           email,phone,contact_person,currency_code,payment_terms_days,credit_limit,
           gl_account_code,sales_rep,tax_treatment,notes,created_by)
        VALUES
          (:id,:tid,:code,:name,:name_en,:ctype,
           :vat,:cr,:country,:city,:district,:street,:bldg,:postal,
           :email,:phone,:contact,:cur,:terms,:limit,
           :gl,:rep,:tax,:notes,:by)
    """), {
        "id":cid,"tid":tid,"code":data["customer_code"],"name":data["customer_name"],
        "name_en":data.get("customer_name_en"),"ctype":data.get("customer_type","B2C"),
        "vat":data.get("vat_number"),"cr":data.get("cr_number"),
        "country":data.get("country","SA"),"city":data.get("city"),
        "district":data.get("district"),"street":data.get("street"),
        "bldg":data.get("building_number"),"postal":data.get("postal_code"),
        "email":data.get("email"),"phone":data.get("phone"),
        "contact":data.get("contact_person"),
        "cur":data.get("currency_code","SAR"),"terms":data.get("payment_terms_days",30),
        "limit":data.get("credit_limit",0),"gl":data.get("gl_account_code"),
        "rep":data.get("sales_rep"),"tax":data.get("tax_treatment","standard"),
        "notes":data.get("notes"),"by":user.email,
    })
    await db.commit()
    return created(data={"id":cid}, message="تم إنشاء العميل ✅")


@router.put("/customers/{cid}")
async def update_customer(cid: uuid.UUID, data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    data.pop("id",None); data.pop("tenant_id",None); data["updated_at"]=datetime.utcnow()
    set_clause=", ".join([f"{k}=:{k}" for k in data.keys()])
    data.update({"id":str(cid),"tid":tid})
    await db.execute(text(f"UPDATE ar_customers SET {set_clause} WHERE id=:id AND tenant_id=:tid"), data)
    await db.commit()
    return ok(data={}, message="تم التعديل ✅")


@router.get("/customers/{cid}")
async def get_customer(cid: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_customers WHERE id=:id AND tenant_id=:tid"), {"id":str(cid),"tid":tid})
    row = r.mappings().fetchone()
    if not row: raise HTTPException(404,"العميل غير موجود")
    # فواتير مفتوحة
    r2 = await db.execute(text("""
        SELECT serial,invoice_date,total_amount,balance_due,status,invoice_type
        FROM ar_invoices WHERE customer_id=:cid AND tenant_id=:tid AND status='posted'
        ORDER BY invoice_date DESC LIMIT 10
    """), {"cid":str(cid),"tid":tid})
    invoices = [dict(row._mapping) for row in r2.fetchall()]
    return ok(data={"customer":dict(row._mapping),"open_invoices":invoices})


# ══════════════════════════════════════════════════════════
# INVOICES
# ══════════════════════════════════════════════════════════
@router.get("/invoices")
async def list_invoices(
    customer_id: Optional[uuid.UUID]=Query(None),
    status:      Optional[str]=Query(None),
    invoice_type:Optional[str]=Query(None),
    date_from:   Optional[date]=Query(None),
    date_to:     Optional[date]=Query(None),
    zatca_status:Optional[str]=Query(None),
    limit: int=Query(50), offset: int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["i.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if customer_id: conds.append("i.customer_id=:cid"); params["cid"]=str(customer_id)
    if status:      conds.append("i.status=:status"); params["status"]=status
    if invoice_type:conds.append("i.invoice_type=:itype"); params["itype"]=invoice_type
    if date_from:   conds.append("i.invoice_date>=:df"); params["df"]=str(date_from)
    if date_to:     conds.append("i.invoice_date<=:dt"); params["dt"]=str(date_to)
    if zatca_status:conds.append("i.zatca_status=:zs"); params["zs"]=zatca_status
    where=" AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM ar_invoices i WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT i.*, c.customer_name AS cust_name, c.customer_code AS cust_code
        FROM ar_invoices i
        LEFT JOIN ar_customers c ON c.id=i.customer_id
        WHERE {where}
        ORDER BY i.invoice_date DESC, i.created_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/invoices", status_code=201)
async def create_invoice(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    inv_date = date.fromisoformat(str(data["invoice_date"]))
    inv_type = data.get("invoice_type","tax")

    # تحديد نوع الترقيم
    type_to_series = {
        "tax":"INV","simplified":"SINV","credit_note":"CRN","debit_note":"DBN"
    }
    series_type = type_to_series.get(inv_type,"INV")
    serial = await _next_serial(db, tid, series_type, inv_date)
    inv_id = str(uuid.uuid4())
    inv_uuid = str(uuid.uuid4())

    lines = data.get("lines",[])
    totals = _calc_lines(lines)

    # جلب بيانات العميل
    cust_name = data.get("customer_name","")
    cust_vat  = data.get("customer_vat","")
    if data.get("customer_id"):
        r = await db.execute(text("SELECT * FROM ar_customers WHERE id=:cid AND tenant_id=:tid"),
                              {"cid":data["customer_id"],"tid":tid})
        cust = r.mappings().fetchone()
        if cust:
            cust_name = cust_name or cust["customer_name"]
            cust_vat  = cust_vat  or cust.get("vat_number","")

    # توليد QR Code (Phase 1)
    seller = await _get_zatca_settings(db, tid)
    from app.services.zatca.engine import generate_qr_code_phase1, calculate_invoice_hash
    inv_datetime = f"{inv_date}T{datetime.now().strftime('%H:%M:%S')}Z"
    qr = generate_qr_code_phase1(
        seller_name      = seller.get("seller_name",""),
        vat_number       = seller.get("vat_number",""),
        invoice_datetime = inv_datetime,
        total_amount     = f"{totals['total_amount']:.2f}",
        vat_amount       = f"{totals['vat_amount']:.2f}",
    )

    # Hash الفاتورة
    inv_data_for_hash = {"serial":serial,"uuid_zatca":inv_uuid,"invoice_date":str(inv_date),
                          "customer_vat":cust_vat,**totals}
    inv_hash = calculate_invoice_hash(inv_data_for_hash)

    # جلب Hash السابق للسلسلة
    r_prev = await db.execute(text("""
        SELECT invoice_hash FROM ar_invoices
        WHERE tenant_id=:tid AND status='posted' AND invoice_hash IS NOT NULL
        ORDER BY posted_at DESC LIMIT 1
    """), {"tid":tid})
    prev_row = r_prev.fetchone()
    prev_hash = prev_row[0] if prev_row else "NWZlY2Y5YTMxZTYyOTk2MDI2MDhmNmFjNjAxMWVlMzE="

    due_date = data.get("due_date")
    if not due_date and data.get("customer_id"):
        r_terms = await db.execute(text("SELECT payment_terms_days FROM ar_customers WHERE id=:cid"),
                                    {"cid":data["customer_id"]})
        terms_row = r_terms.fetchone()
        if terms_row:
            from datetime import timedelta
            due_date = str(inv_date + timedelta(days=terms_row[0] or 30))

    await db.execute(text("""
        INSERT INTO ar_invoices
          (id,tenant_id,serial,invoice_type,customer_id,customer_name,customer_vat,
           invoice_date,invoice_time,due_date,currency_code,exchange_rate,
           sales_rep_id,po_reference,notes,
           subtotal,discount_amount,taxable_amount,vat_amount,total_amount,balance_due,
           status,uuid_zatca,qr_code,invoice_hash,previous_hash,zatca_status,created_by)
        VALUES
          (:id,:tid,:serial,:inv_type,:cust_id,:cust_name,:cust_vat,
           :inv_date,:inv_time,:due_date,:cur,:rate,
           :rep,:po,:notes,
           :sub,:disc,:taxable,:vat,:total,:total,
           'draft',:uuid,:qr,:hash,:prev_hash,'pending',:by)
    """), {
        "id":inv_id,"tid":tid,"serial":serial,"inv_type":inv_type,
        "cust_id":data.get("customer_id"),"cust_name":cust_name,"cust_vat":cust_vat,
        "inv_date":inv_date,"inv_time":datetime.now().strftime("%H:%M:%S"),
        "due_date":due_date,"cur":data.get("currency_code","SAR"),
        "rate":data.get("exchange_rate",1),"rep":data.get("sales_rep_id"),
        "po":data.get("po_reference"),"notes":data.get("notes"),
        **totals, "uuid":inv_uuid,"qr":qr,"hash":inv_hash,"prev_hash":prev_hash,"by":user.email,
    })

    for i, line in enumerate(lines):
        await db.execute(text("""
            INSERT INTO ar_invoice_lines
              (id,tenant_id,invoice_id,line_order,item_code,item_name,item_name_en,
               description,quantity,unit_price,discount_pct,discount_amount,
               net_amount,vat_rate,vat_amount,total_amount,vat_category,cost_center,project_code)
            VALUES
              (gen_random_uuid(),:tid,:inv_id,:order,:code,:name,:name_en,
               :desc,:qty,:price,:disc_pct,:disc_amt,
               :net,:vat_rate,:vat,:total,:cat,:cc,:proj)
        """), {
            "tid":tid,"inv_id":inv_id,"order":i+1,
            "code":line.get("item_code"),"name":line["item_name"],
            "name_en":line.get("item_name_en"),"desc":line.get("description"),
            "qty":Decimal(str(line["quantity"])),"price":Decimal(str(line["unit_price"])),
            "disc_pct":line.get("discount_pct",0),"disc_amt":line.get("discount_amount",0),
            "net":line["net_amount"],"vat_rate":line.get("vat_rate",15),
            "vat":line["vat_amount"],"total":line["total_amount"],
            "cat":line.get("vat_category","S"),"cc":line.get("cost_center"),
            "proj":line.get("project_code"),
        })

    await db.commit()
    return created(data={"id":inv_id,"serial":serial,"qr_code":qr,"totals":totals},
                   message=f"تم إنشاء الفاتورة {serial} ✅")


@router.get("/invoices/{inv_id}")
async def get_invoice(inv_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_invoices WHERE id=:id AND tenant_id=:tid"), {"id":str(inv_id),"tid":tid})
    inv = r.mappings().fetchone()
    if not inv: raise HTTPException(404,"الفاتورة غير موجودة")
    r2 = await db.execute(text("SELECT * FROM ar_invoice_lines WHERE invoice_id=:id ORDER BY line_order"), {"id":str(inv_id)})
    lines = [dict(row._mapping) for row in r2.fetchall()]
    # validation
    seller = await _get_zatca_settings(db, tid)
    from app.services.zatca.engine import validate_invoice_zatca
    validation = validate_invoice_zatca(dict(inv._mapping), lines, seller)
    return ok(data={"invoice":dict(inv._mapping),"lines":lines,"validation":validation})


@router.post("/invoices/{inv_id}/post")
async def post_invoice(inv_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_invoices WHERE id=:id AND tenant_id=:tid"), {"id":str(inv_id),"tid":tid})
    inv = r.mappings().fetchone()
    if not inv: raise HTTPException(404,"الفاتورة غير موجودة")
    if inv["status"] != "draft": raise HTTPException(400,"الفاتورة مُرحَّلة مسبقاً")

    # جلب الأسطر
    r2 = await db.execute(text("SELECT * FROM ar_invoice_lines WHERE invoice_id=:id"), {"id":str(inv_id)})
    lines = [dict(row._mapping) for row in r2.fetchall()]

    # ZATCA Validation
    seller = await _get_zatca_settings(db, tid)
    from app.services.zatca.engine import validate_invoice_zatca, generate_invoice_xml
    validation = validate_invoice_zatca(dict(inv._mapping), lines, seller)
    if not validation["valid"]:
        return ok(data={"validation":validation}, message="❌ الفاتورة لا تستوفي متطلبات ZATCA")

    # توليد XML
    xml_content = generate_invoice_xml(
        invoice=dict(inv._mapping), lines=lines, seller=seller,
        previous_hash=inv["previous_hash"] or "",
        invoice_hash=inv["invoice_hash"] or "",
    )

    # ── جلب الحسابات من gl_account_mappings ──────────────
    async def _get_gl(db, tid, key, default):
        r = await db.execute(text("""
            SELECT account_code FROM gl_account_mappings
            WHERE tenant_id=:tid AND mapping_key=:key LIMIT 1
        """), {"tid":tid,"key":key})
        row = r.fetchone()
        return row[0] if row else default

    ar_default  = await _get_gl(db, tid, "ar_default",    "120101")
    revenue_acc = await _get_gl(db, tid, "ar_revenue",    "410101")
    vat_acc     = await _get_gl(db, tid, "ar_vat_output", "240101")

    # حساب العميل من بطاقة العميل (أو الافتراضي)
    r3 = await db.execute(text("SELECT gl_account_code FROM ar_customers WHERE id=:cid AND tenant_id=:tid"),
                           {"cid":str(inv["customer_id"]),"tid":tid})
    cust_row = r3.fetchone()
    ar_account = (cust_row[0] if cust_row and cust_row[0] else None) or ar_default

    total = Decimal(str(inv["total_amount"]))
    vat   = Decimal(str(inv["vat_amount"]))
    net   = Decimal(str(inv["subtotal"]))

    je_lines = [
        {"account_code": ar_account,   "debit": total, "credit": 0,   "description": f"فاتورة {inv['serial']}"},
        {"account_code": revenue_acc,  "debit": 0,     "credit": net,  "description": f"إيراد — {inv['serial']}"},
        {"account_code": vat_acc,      "debit": 0,     "credit": vat,  "description": f"ضريبة مخرجات — {inv['serial']}"},
    ]
    if inv["invoice_type"] in ("credit_note","debit_note"):
        je_lines = [{"account_code":l["account_code"],"debit":l["credit"],"credit":l["debit"],"description":l["description"]} for l in je_lines]

    je = await _post_je(db, tid, user.email, "INV", inv["invoice_date"],
                        f"فاتورة مبيعات {inv['serial']}", je_lines, inv["serial"])

    await db.execute(text("""
        UPDATE ar_invoices SET
          status='posted', is_locked=true, xml_content=:xml,
          je_id=:je_id, je_serial=:je_serial,
          posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"xml":xml_content,"je_id":je["je_id"],"je_serial":je["je_serial"],
           "by":user.email,"id":str(inv_id),"tid":tid})
    await db.commit()
    return ok(data={"je_serial":je["je_serial"],"xml_ready":True,"validation":validation},
              message=f"✅ تم ترحيل الفاتورة {inv['serial']}")


@router.post("/invoices/{inv_id}/cancel")
async def cancel_invoice(inv_id: uuid.UUID, reason:str=Query(...), db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT status,is_locked FROM ar_invoices WHERE id=:id AND tenant_id=:tid"), {"id":str(inv_id),"tid":tid})
    inv = r.fetchone()
    if not inv: raise HTTPException(404)
    if inv[1]: raise HTTPException(400,"الفاتورة مقفلة — أنشئ إشعاراً دائناً بدلاً من الإلغاء")
    await db.execute(text("""
        UPDATE ar_invoices SET status='cancelled', cancelled_by=:by, cancelled_at=NOW()
        WHERE id=:id AND tenant_id=:tid AND status='draft'
    """), {"by":user.email,"id":str(inv_id),"tid":tid})
    await db.commit()
    return ok(data={}, message="تم الإلغاء")


@router.get("/invoices/{inv_id}/xml")
async def get_invoice_xml(inv_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT serial,xml_content,qr_code,invoice_hash FROM ar_invoices WHERE id=:id AND tenant_id=:tid"),
                          {"id":str(inv_id),"tid":tid})
    row = r.fetchone()
    if not row: raise HTTPException(404)
    return ok(data={"serial":row[0],"xml":row[1],"qr_code":row[2],"hash":row[3]})


# ══════════════════════════════════════════════════════════
# CREDIT NOTES
# ══════════════════════════════════════════════════════════
@router.post("/invoices/{inv_id}/credit-note", status_code=201)
async def create_credit_note(
    inv_id: uuid.UUID,
    data: dict,
    db: AsyncSession=Depends(get_db),
    user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_invoices WHERE id=:id AND tenant_id=:tid"), {"id":str(inv_id),"tid":tid})
    orig = r.mappings().fetchone()
    if not orig: raise HTTPException(404)
    if orig["status"] != "posted": raise HTTPException(400,"الفاتورة الأصلية غير مُرحَّلة")

    cn_date = date.fromisoformat(str(data.get("note_date", date.today())))
    serial  = await _next_serial(db, tid, "CRN", cn_date)
    cn_id   = str(uuid.uuid4())
    lines   = data.get("lines", [])
    totals  = _calc_lines(lines) if lines else {
        "subtotal":float(orig["subtotal"]),"discount_amount":0,
        "taxable_amount":float(orig["taxable_amount"]),
        "vat_amount":float(orig["vat_amount"]),"total_amount":float(orig["total_amount"]),
    }

    # QR للإشعار الدائن
    seller = await _get_zatca_settings(db, tid)
    from app.services.zatca.engine import generate_qr_code_phase1, calculate_invoice_hash
    qr = generate_qr_code_phase1(
        seller.get("seller_name",""), seller.get("vat_number",""),
        f"{cn_date}T{datetime.now().strftime('%H:%M:%S')}Z",
        f"{totals['total_amount']:.2f}", f"{totals['vat_amount']:.2f}",
    )
    cn_hash = calculate_invoice_hash({**totals,"serial":serial,"uuid_zatca":str(uuid.uuid4()),"invoice_date":str(cn_date),"customer_vat":orig["customer_vat"] or ""})

    await db.execute(text("""
        INSERT INTO ar_invoices
          (id,tenant_id,serial,invoice_type,customer_id,customer_name,customer_vat,
           invoice_date,due_date,currency_code,notes,
           subtotal,discount_amount,taxable_amount,vat_amount,total_amount,balance_due,
           status,uuid_zatca,qr_code,invoice_hash,previous_hash,
           original_invoice_id,zatca_status,created_by)
        VALUES
          (:id,:tid,:serial,'credit_note',:cust_id,:cust_name,:cust_vat,
           :inv_date,:inv_date,:cur,:notes,
           :sub,:disc,:taxable,:vat,:total,:total,
           'draft',:uuid,:qr,:hash,:prev_hash,
           :orig_id,'pending',:by)
    """), {
        "id":cn_id,"tid":tid,"serial":serial,
        "cust_id":str(orig["customer_id"]),"cust_name":orig["customer_name"],"cust_vat":orig["customer_vat"],
        "inv_date":cn_date,"cur":orig["currency_code"],
        "notes":data.get("reason","إشعار دائن"),
        **{k:totals[k] for k in["subtotal","discount_amount","taxable_amount","vat_amount","total_amount"]},
        "uuid":str(uuid.uuid4()),"qr":qr,"hash":cn_hash,"prev_hash":orig["invoice_hash"],
        "orig_id":str(inv_id),"by":user.email,
    })

    if lines:
        for i,line in enumerate(lines):
            await db.execute(text("""
                INSERT INTO ar_invoice_lines
                  (id,tenant_id,invoice_id,line_order,item_name,quantity,unit_price,
                   discount_pct,discount_amount,net_amount,vat_rate,vat_amount,total_amount)
                VALUES
                  (gen_random_uuid(),:tid,:inv_id,:order,:name,:qty,:price,
                   :disc_pct,:disc,:net,:vat_rate,:vat,:total)
            """), {"tid":tid,"inv_id":cn_id,"order":i+1,"name":line["item_name"],
                   "qty":line.get("quantity",1),"price":line["unit_price"],
                   "disc_pct":line.get("discount_pct",0),"disc":line.get("discount_amount",0),
                   "net":line["net_amount"],"vat_rate":line.get("vat_rate",15),
                   "vat":line["vat_amount"],"total":line["total_amount"]})

    await db.commit()
    return created(data={"id":cn_id,"serial":serial}, message=f"تم إنشاء الإشعار الدائن {serial} ✅")


# ══════════════════════════════════════════════════════════
# RECEIPTS
# ══════════════════════════════════════════════════════════
@router.get("/receipts")
async def list_receipts(
    customer_id: Optional[uuid.UUID]=Query(None),
    status:      Optional[str]=Query(None),
    date_from:   Optional[date]=Query(None),
    date_to:     Optional[date]=Query(None),
    limit: int=Query(50), offset: int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["r.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if customer_id: conds.append("r.customer_id=:cid"); params["cid"]=str(customer_id)
    if status:      conds.append("r.status=:status");   params["status"]=status
    if date_from:   conds.append("r.receipt_date>=:df"); params["df"]=str(date_from)
    if date_to:     conds.append("r.receipt_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM ar_receipts r WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT r.*, c.customer_name AS cust_name
        FROM ar_receipts r
        LEFT JOIN ar_customers c ON c.id=r.customer_id
        WHERE {where}
        ORDER BY r.receipt_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/receipts", status_code=201)
async def create_receipt(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    rec_date = date.fromisoformat(str(data["receipt_date"]))
    serial   = await _next_serial(db, tid, "RV", rec_date)
    rec_id   = str(uuid.uuid4())
    amt      = Decimal(str(data["amount"]))
    rate     = Decimal(str(data.get("exchange_rate",1)))
    amt_sar  = amt * rate

    await db.execute(text("""
        INSERT INTO ar_receipts
          (id,tenant_id,serial,customer_id,receipt_date,amount,currency_code,
           exchange_rate,amount_sar,payment_method,bank_account_id,check_number,
           reference,description,notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:cust_id,:dt,:amt,:cur,
           :rate,:sar,:method,:bank,:check,
           :ref,:desc,:notes,'draft',:by)
    """), {
        "id":rec_id,"tid":tid,"serial":serial,
        "cust_id":data.get("customer_id"),"dt":rec_date,
        "amt":amt,"cur":data.get("currency_code","SAR"),
        "rate":rate,"sar":amt_sar,"method":data.get("payment_method","cash"),
        "bank":data.get("bank_account_id"),"check":data.get("check_number"),
        "ref":data.get("reference"),"desc":data.get("description"),
        "notes":data.get("notes"),"by":user.email,
    })
    await db.commit()
    return created(data={"id":rec_id,"serial":serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/receipts/{rec_id}/post")
async def post_receipt(rec_id: uuid.UUID, invoice_id: Optional[uuid.UUID]=Query(None), db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_receipts WHERE id=:id AND tenant_id=:tid"), {"id":str(rec_id),"tid":tid})
    rec = r.mappings().fetchone()
    if not rec: raise HTTPException(404)
    if rec["status"] != "draft": raise HTTPException(400,"مُرحَّل مسبقاً")

    # جلب حساب العميل
    ar_account = "120101"
    if rec["customer_id"]:
        r2 = await db.execute(text("SELECT gl_account_code FROM ar_customers WHERE id=:cid"),{"cid":str(rec["customer_id"])})
        cust_row = r2.fetchone()
        if cust_row and cust_row[0]: ar_account = cust_row[0]

    cash_account = "110101"  # الصندوق
    if rec["payment_method"] in ("bank","transfer"): cash_account = "110201"
    if rec["payment_method"] == "check":             cash_account = "110301"

    amt = Decimal(str(rec["amount_sar"] or rec["amount"]))
    je_lines = [
        {"account_code": cash_account, "debit": amt, "credit": 0,   "description": f"قبض {rec['serial']}"},
        {"account_code": ar_account,   "debit": 0,   "credit": amt, "description": f"قبض {rec['serial']}"},
    ]
    je = await _post_je(db, tid, user.email, "RV", rec["receipt_date"],
                        f"قبض من عميل — {rec['serial']}", je_lines, rec["serial"])

    await db.execute(text("""
        UPDATE ar_receipts SET status='posted', je_id=:je_id, je_serial=:je_serial,
               posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id":je["je_id"],"je_serial":je["je_serial"],"by":user.email,"id":str(rec_id),"tid":tid})

    # تطبيق على فاتورة إن وُجدت
    if invoice_id:
        r3 = await db.execute(text("SELECT balance_due FROM ar_invoices WHERE id=:id AND tenant_id=:tid"),{"id":str(invoice_id),"tid":tid})
        inv_row = r3.fetchone()
        if inv_row:
            applied = min(amt, Decimal(str(inv_row[0])))
            await db.execute(text("""
                INSERT INTO ar_receipt_applications (id,tenant_id,receipt_id,invoice_id,amount_applied)
                VALUES (gen_random_uuid(),:tid,:rec_id,:inv_id,:applied)
            """), {"tid":tid,"rec_id":str(rec_id),"inv_id":str(invoice_id),"applied":applied})
            new_balance = Decimal(str(inv_row[0])) - applied
            new_status = "paid" if new_balance <= 0 else "partial"
            await db.execute(text("""
                UPDATE ar_invoices SET balance_due=:bal, status=:st, paid_amount=paid_amount+:paid
                WHERE id=:id AND tenant_id=:tid
            """), {"bal":new_balance,"st":new_status,"paid":applied,"id":str(invoice_id),"tid":tid})

    await db.commit()
    return ok(data={"je_serial":je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# ZATCA SETTINGS
# ══════════════════════════════════════════════════════════
@router.get("/zatca/settings")
async def get_zatca_settings(db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    settings = await _get_zatca_settings(db, tid)
    return ok(data=settings)


@router.put("/zatca/settings")
async def update_zatca_settings(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    data.pop("id",None); data.pop("tenant_id",None)
    data["tenant_id"] = tid; data["updated_at"] = datetime.utcnow()
    cols = ", ".join([f"{k}=:{k}" for k in data.keys() if k != "tenant_id"])
    data["tid"] = tid
    await db.execute(text(f"""
        INSERT INTO ar_zatca_settings (id,tenant_id,{','.join([k for k in data.keys() if k not in ('tid',)])})
        VALUES (gen_random_uuid(),:tid,{','.join([f':{k}' for k in data.keys() if k not in ('tid',)])})
        ON CONFLICT (tenant_id) DO UPDATE SET {cols}
    """), data)
    await db.commit()
    return ok(data={}, message="تم حفظ إعدادات ZATCA ✅")


@router.post("/zatca/validate/{inv_id}")
async def validate_invoice(inv_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_invoices WHERE id=:id AND tenant_id=:tid"), {"id":str(inv_id),"tid":tid})
    inv = r.mappings().fetchone()
    r2 = await db.execute(text("SELECT * FROM ar_invoice_lines WHERE invoice_id=:id"), {"id":str(inv_id)})
    lines = [dict(row._mapping) for row in r2.fetchall()]
    seller = await _get_zatca_settings(db, tid)
    from app.services.zatca.engine import validate_invoice_zatca
    result = validate_invoice_zatca(dict(inv._mapping), lines, seller)
    return ok(data=result)


# ══════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════
@router.get("/reports/aging")
async def aging_report(
    as_of: Optional[date]=Query(None),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ref_date = str(as_of or date.today())
    r = await db.execute(text("""
        SELECT c.customer_code, c.customer_name, c.customer_type,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date BETWEEN 0 AND 30  THEN i.balance_due END),0) AS bucket_current,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date BETWEEN 31 AND 60 THEN i.balance_due END),0) AS bucket_30,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date BETWEEN 61 AND 90 THEN i.balance_due END),0) AS bucket_60,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date >90              THEN i.balance_due END),0) AS bucket_90plus,
               COALESCE(SUM(i.balance_due),0) AS total_balance,
               COUNT(i.id) AS invoice_count
        FROM ar_customers c
        JOIN ar_invoices i ON i.customer_id=c.id AND i.tenant_id=:tid
                           AND i.status='posted' AND i.balance_due>0
        WHERE c.tenant_id=:tid
        GROUP BY c.id,c.customer_code,c.customer_name,c.customer_type
        HAVING COALESCE(SUM(i.balance_due),0)>0
        ORDER BY total_balance DESC
    """), {"tid":tid,"dt":ref_date})
    rows = [dict(row._mapping) for row in r.fetchall()]
    totals = {
        "current":  sum(float(r["bucket_current"] or 0) for r in rows),
        "30":       sum(float(r["bucket_30"] or 0) for r in rows),
        "60":       sum(float(r["bucket_60"] or 0) for r in rows),
        "90plus":   sum(float(r["bucket_90plus"] or 0) for r in rows),
        "total":    sum(float(r["total_balance"] or 0) for r in rows),
    }
    return ok(data={"customers":rows,"totals":totals,"as_of":ref_date})


@router.get("/reports/customer-statement/{cid}")
async def customer_statement(
    cid: uuid.UUID,
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_customers WHERE id=:id AND tenant_id=:tid"),{"id":str(cid),"tid":tid})
    cust = r.mappings().fetchone()
    if not cust: raise HTTPException(404)
    # الفواتير
    conds=["i.customer_id=:cid AND i.tenant_id=:tid AND i.status='posted'"]
    params:dict={"cid":str(cid),"tid":tid}
    if date_from: conds.append("i.invoice_date>=:df"); params["df"]=str(date_from)
    if date_to:   conds.append("i.invoice_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)
    r2 = await db.execute(text(f"""
        SELECT 'invoice' AS tx_type, serial AS ref, invoice_date AS tx_date,
               invoice_type AS sub_type, total_amount AS debit, 0 AS credit,
               balance_due, status
        FROM ar_invoices i WHERE {where}
        UNION ALL
        SELECT 'receipt' AS tx_type, r.serial AS ref, r.receipt_date AS tx_date,
               r.payment_method AS sub_type, 0 AS debit, r.amount AS credit,
               NULL, r.status
        FROM ar_receipts r
        JOIN ar_receipt_applications ra ON ra.receipt_id=r.id
        JOIN ar_invoices inv ON inv.id=ra.invoice_id AND inv.customer_id=:cid
        WHERE r.tenant_id=:tid AND r.status='posted'
        ORDER BY tx_date
    """), params)
    txs = [dict(row._mapping) for row in r2.fetchall()]
    total_invoiced = sum(float(t["debit"] or 0) for t in txs if t["tx_type"]=="invoice")
    total_collected= sum(float(t["credit"] or 0) for t in txs if t["tx_type"]=="receipt")
    return ok(data={
        "customer": dict(cust._mapping),
        "transactions": txs,
        "summary": {
            "total_invoiced":  total_invoiced,
            "total_collected": total_collected,
            "balance":         total_invoiced - total_collected,
        },
        "period": {"from":str(date_from or ""),"to":str(date_to or "")}
    })


@router.get("/reports/sales-summary")
async def sales_summary(
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    group_by:  str=Query("month"), # month|customer|rep
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["i.tenant_id=:tid AND i.status='posted' AND i.invoice_type IN ('tax','simplified')"]
    params:dict={"tid":tid}
    if date_from: conds.append("i.invoice_date>=:df"); params["df"]=str(date_from)
    if date_to:   conds.append("i.invoice_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)

    if group_by == "customer":
        sql = f"""
            SELECT c.customer_name AS group_key, c.customer_type,
                   COUNT(i.id) AS invoice_count,
                   SUM(i.subtotal) AS net_sales,
                   SUM(i.vat_amount) AS vat_total,
                   SUM(i.total_amount) AS gross_sales,
                   SUM(i.balance_due) AS outstanding
            FROM ar_invoices i JOIN ar_customers c ON c.id=i.customer_id
            WHERE {where} GROUP BY c.id,c.customer_name,c.customer_type
            ORDER BY gross_sales DESC
        """
    elif group_by == "rep":
        sql = f"""
            SELECT COALESCE(sr.rep_name,'غير محدد') AS group_key,
                   COUNT(i.id) AS invoice_count,
                   SUM(i.subtotal) AS net_sales,
                   SUM(i.vat_amount) AS vat_total,
                   SUM(i.total_amount) AS gross_sales
            FROM ar_invoices i LEFT JOIN ar_sales_reps sr ON sr.id=i.sales_rep_id
            WHERE {where} GROUP BY sr.id,sr.rep_name ORDER BY gross_sales DESC
        """
    else:
        sql = f"""
            SELECT TO_CHAR(i.invoice_date,'YYYY-MM') AS group_key,
                   COUNT(i.id) AS invoice_count,
                   SUM(i.subtotal) AS net_sales,
                   SUM(i.vat_amount) AS vat_total,
                   SUM(i.total_amount) AS gross_sales
            FROM ar_invoices i WHERE {where}
            GROUP BY group_key ORDER BY group_key
        """
    r = await db.execute(text(sql), params)
    rows = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"rows":rows,"group_by":group_by})


@router.get("/reports/vat-report")
async def vat_report(
    date_from: date=Query(...),
    date_to:   date=Query(...),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
          COUNT(id) AS invoice_count,
          SUM(subtotal) AS taxable_sales,
          SUM(vat_amount) AS output_vat,
          SUM(total_amount) AS total_with_vat,
          SUM(CASE WHEN invoice_type='tax' THEN total_amount END) AS b2b_sales,
          SUM(CASE WHEN invoice_type='simplified' THEN total_amount END) AS b2c_sales,
          SUM(CASE WHEN invoice_type='credit_note' THEN total_amount END) AS credit_notes
        FROM ar_invoices
        WHERE tenant_id=:tid AND status='posted'
          AND invoice_date BETWEEN :df AND :dt
    """), {"tid":tid,"df":str(date_from),"dt":str(date_to)})
    row = r.mappings().fetchone()
    return ok(data={**dict(row._mapping),"period_from":str(date_from),"period_to":str(date_to)})


# ══════════════════════════════════════════════════════════
# QUOTATIONS
# ══════════════════════════════════════════════════════════
@router.get("/quotations")
async def list_quotations(
    status: Optional[str]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["q.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if status: conds.append("q.status=:status"); params["status"]=status
    where=" AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT q.*, c.customer_name
        FROM ar_quotations q
        LEFT JOIN ar_customers c ON c.id=q.customer_id
        WHERE {where}
        ORDER BY q.quote_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/quotations", status_code=201)
async def create_quotation(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    q_date = date.fromisoformat(str(data["quote_date"]))
    serial = await _next_serial(db, tid, "QUO", q_date)
    q_id   = str(uuid.uuid4())
    lines  = data.get("lines",[])
    totals = _calc_lines(lines)
    await db.execute(text("""
        INSERT INTO ar_quotations (id,tenant_id,serial,customer_id,quote_date,expiry_date,
               subtotal,vat_amount,total_amount,status,notes,created_by)
        VALUES (:id,:tid,:serial,:cust_id,:q_date,:exp,:sub,:vat,:total,'draft',:notes,:by)
    """), {"id":q_id,"tid":tid,"serial":serial,"cust_id":data.get("customer_id"),
           "q_date":q_date,"exp":data.get("expiry_date"),
           "sub":totals["subtotal"],"vat":totals["vat_amount"],"total":totals["total_amount"],
           "notes":data.get("notes"),"by":user.email})
    for i,line in enumerate(lines):
        await db.execute(text("""
            INSERT INTO ar_quotation_lines (id,tenant_id,quotation_id,line_order,item_code,item_name,
                   quantity,unit_price,discount_pct,net_amount,vat_rate,vat_amount,total_amount)
            VALUES (gen_random_uuid(),:tid,:qid,:order,:code,:name,:qty,:price,:disc,:net,:vat_rate,:vat,:total)
        """), {"tid":tid,"qid":q_id,"order":i+1,"code":line.get("item_code"),
               "name":line["item_name"],"qty":line.get("quantity",1),
               "price":line["unit_price"],"disc":line.get("discount_pct",0),
               "net":line["net_amount"],"vat_rate":line.get("vat_rate",15),
               "vat":line["vat_amount"],"total":line["total_amount"]})
    await db.commit()
    return created(data={"id":q_id,"serial":serial,"totals":totals}, message=f"تم إنشاء عرض السعر {serial} ✅")


@router.post("/quotations/{q_id}/convert")
async def convert_quotation(q_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    """تحويل عرض السعر إلى فاتورة"""
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_quotations WHERE id=:id AND tenant_id=:tid"),{"id":str(q_id),"tid":tid})
    q = r.mappings().fetchone()
    if not q: raise HTTPException(404)
    r2 = await db.execute(text("SELECT * FROM ar_quotation_lines WHERE quotation_id=:id ORDER BY line_order"),{"id":str(q_id)})
    lines = [dict(row._mapping) for row in r2.fetchall()]
    # إنشاء فاتورة
    inv_data = {
        "invoice_date": str(date.today()), "invoice_type": "tax",
        "customer_id": str(q["customer_id"]),
        "notes": f"محول من عرض السعر {q['serial']}",
        "lines": [{"item_name":l["item_name"],"item_code":l.get("item_code"),
                   "quantity":float(l["quantity"]),"unit_price":float(l["unit_price"]),
                   "discount_pct":float(l.get("discount_pct",0)),"vat_rate":float(l.get("vat_rate",15))}
                  for l in lines]
    }
    from fastapi import Request
    class FakeUser:
        tenant_id=uuid.UUID(tid); email=user.email
    inv_result = await create_invoice(inv_data, db, FakeUser())
    inv_id = inv_result.body  # placeholder
    await db.execute(text("UPDATE ar_quotations SET status='converted' WHERE id=:id AND tenant_id=:tid"),{"id":str(q_id),"tid":tid})
    await db.commit()
    return ok(data={"quotation_serial":q["serial"]}, message="تم تحويل عرض السعر إلى فاتورة ✅")


# ══════════════════════════════════════════════════════════
# SALES REPS
# ══════════════════════════════════════════════════════════
@router.get("/sales-reps")
async def list_sales_reps(db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ar_sales_reps WHERE tenant_id=:tid AND is_active=true ORDER BY rep_name"),{"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/sales-reps", status_code=201)
async def create_sales_rep(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    rid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO ar_sales_reps (id,tenant_id,rep_code,rep_name,email,phone,commission_pct)
        VALUES (:id,:tid,:code,:name,:email,:phone,:comm)
    """), {"id":rid,"tid":tid,"code":data["rep_code"],"name":data["rep_name"],
           "email":data.get("email"),"phone":data.get("phone"),
           "comm":data.get("commission_pct",0)})
    await db.commit()
    return created(data={"id":rid}, message="تم إنشاء المندوب ✅")
