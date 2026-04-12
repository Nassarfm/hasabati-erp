"""
app/modules/ap/router.py
══════════════════════════════════════════════════════════
Procurement & AP Module — Complete API
المشتريات والذمم الدائنة

Flow: PR → RFQ → PO → GRN → AP Invoice → Payment
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/ap", tags=["المشتريات والذمم الدائنة"])


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
            account_code=l["account_code"], description=l.get("description", description),
            debit=Decimal(str(l.get("debit",0))), credit=Decimal(str(l.get("credit",0))),
        ) for l in lines]
        result = await engine.post(PostingRequest(
            tenant_id=t_id, je_type=je_type, description=description,
            entry_date=tx_date, lines=posting_lines,
            created_by_email=user_email, reference=reference, source_module="ap",
        ))
        return {"je_id":str(result.je_id),"je_serial":result.je_serial}
    except Exception:
        return {"je_id":None,"je_serial":None}


def _calc_lines(lines: list) -> dict:
    subtotal = vat_total = discount_total = Decimal(0)
    for line in lines:
        qty      = Decimal(str(line.get("quantity",1)))
        price    = Decimal(str(line.get("unit_price",0)))
        disc_pct = Decimal(str(line.get("discount_pct",0)))
        vat_rate = Decimal(str(line.get("vat_rate",15)))
        gross = qty * price
        disc  = gross * disc_pct / 100
        net   = gross - disc
        vat   = net * vat_rate / 100
        line["discount_amount"] = float(disc)
        line["net_amount"]      = float(net)
        line["vat_amount"]      = float(round(vat,2))
        line["total_amount"]    = float(round(net + vat,2))
        subtotal      += net
        vat_total     += vat
        discount_total+= disc
    return {
        "subtotal":        float(round(subtotal,2)),
        "discount_amount": float(round(discount_total,2)),
        "vat_amount":      float(round(vat_total,2)),
        "total_amount":    float(round(subtotal + vat_total,2)),
    }


# ══════════════════════════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════════════════════════
@router.get("/dashboard")
async def dashboard(db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM ap_vendors WHERE tenant_id=:tid AND is_active=true) AS vendors,
          (SELECT COUNT(*) FROM ap_purchase_orders WHERE tenant_id=:tid AND status NOT IN ('closed','cancelled')) AS open_pos,
          (SELECT COALESCE(SUM(total_amount),0) FROM ap_purchase_orders WHERE tenant_id=:tid AND status NOT IN ('closed','cancelled')) AS open_po_value,
          (SELECT COUNT(*) FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND balance_due>0) AS open_invoices,
          (SELECT COALESCE(SUM(balance_due),0) FROM ap_invoices WHERE tenant_id=:tid AND status='posted') AS total_payables,
          (SELECT COUNT(*) FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND due_date<CURRENT_DATE AND balance_due>0) AS overdue_count,
          (SELECT COALESCE(SUM(balance_due),0) FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND due_date<CURRENT_DATE AND balance_due>0) AS overdue_amount,
          (SELECT COUNT(*) FROM ap_purchase_requests WHERE tenant_id=:tid AND status='pending') AS pending_prs,
          (SELECT COALESCE(SUM(total_amount),0) FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND invoice_date>=DATE_TRUNC('month',CURRENT_DATE)) AS month_purchases
    """), {"tid":tid})
    krow = r.mappings().fetchone()

    # أكبر 5 موردين بالمديونية
    r2 = await db.execute(text("""
        SELECT v.vendor_name, COALESCE(SUM(i.balance_due),0) AS balance
        FROM ap_vendors v
        JOIN ap_invoices i ON i.vendor_id=v.id AND i.tenant_id=:tid AND i.status='posted'
        WHERE v.tenant_id=:tid AND i.balance_due>0
        GROUP BY v.id,v.vendor_name ORDER BY balance DESC LIMIT 5
    """), {"tid":tid})
    top_vendors = [dict(row._mapping) for row in r2.fetchall()]

    # مشتريات آخر 6 أشهر
    r3 = await db.execute(text("""
        SELECT TO_CHAR(invoice_date,'YYYY-MM') AS month,
               COALESCE(SUM(total_amount),0) AS purchases,
               COALESCE(SUM(vat_amount),0) AS vat
        FROM ap_invoices
        WHERE tenant_id=:tid AND status='posted'
          AND invoice_date >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY month ORDER BY month
    """), {"tid":tid})
    monthly = [dict(row._mapping) for row in r3.fetchall()]

    # Aging الذمم الدائنة
    r4 = await db.execute(text("""
        SELECT
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date<=30  THEN balance_due END),0) AS bucket_30,
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date BETWEEN 31 AND 60 THEN balance_due END),0) AS bucket_60,
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date BETWEEN 61 AND 90 THEN balance_due END),0) AS bucket_90,
          COALESCE(SUM(CASE WHEN CURRENT_DATE-due_date>90 THEN balance_due END),0) AS bucket_90plus
        FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND balance_due>0
    """), {"tid":tid})
    aging = dict(r4.mappings().fetchone())

    # POs قريبة من الاستحقاق
    r5 = await db.execute(text("""
        SELECT po.serial, v.vendor_name, po.total_amount, po.expected_date, po.status
        FROM ap_purchase_orders po
        LEFT JOIN ap_vendors v ON v.id=po.vendor_id
        WHERE po.tenant_id=:tid AND po.status NOT IN ('closed','cancelled')
          AND po.expected_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7
        ORDER BY po.expected_date LIMIT 5
    """), {"tid":tid})
    upcoming_pos = [dict(row._mapping) for row in r5.fetchall()]

    return ok(data={
        "kpis":        dict(krow._mapping),
        "top_vendors": top_vendors,
        "monthly":     monthly,
        "aging":       aging,
        "upcoming_pos":upcoming_pos,
    })


# ══════════════════════════════════════════════════════════
# VENDORS
# ══════════════════════════════════════════════════════════
@router.get("/vendors")
async def list_vendors(
    search: Optional[str]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["v.tenant_id=:tid AND v.is_active=true"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if search:
        conds.append("(v.vendor_name ILIKE :s OR v.vendor_code ILIKE :s OR v.vat_number ILIKE :s)")
        params["s"]=f"%{search}%"
    where=" AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM ap_vendors v WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT v.*,
               COALESCE(SUM(i.balance_due),0) AS current_balance,
               COUNT(DISTINCT po.id) AS po_count
        FROM ap_vendors v
        LEFT JOIN ap_invoices i ON i.vendor_id=v.id AND i.status='posted'
        LEFT JOIN ap_purchase_orders po ON po.vendor_id=v.id AND po.status NOT IN ('closed','cancelled')
        WHERE {where}
        GROUP BY v.id ORDER BY v.vendor_name
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/vendors", status_code=201)
async def create_vendor(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    vid = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO ap_vendors
          (id,tenant_id,vendor_code,vendor_name,vendor_name_en,vendor_type,
           vat_number,cr_number,country,city,district,street,building_number,postal_code,
           email,phone,contact_person,currency_code,payment_terms_days,credit_limit,
           gl_account_code,tax_treatment,notes,created_by)
        VALUES
          (:id,:tid,:code,:name,:name_en,:vtype,
           :vat,:cr,:country,:city,:district,:street,:bldg,:postal,
           :email,:phone,:contact,:cur,:terms,:limit,
           :gl,:tax,:notes,:by)
    """), {
        "id":vid,"tid":tid,"code":data["vendor_code"],"name":data["vendor_name"],
        "name_en":data.get("vendor_name_en"),"vtype":data.get("vendor_type","supplier"),
        "vat":data.get("vat_number"),"cr":data.get("cr_number"),
        "country":data.get("country","SA"),"city":data.get("city"),
        "district":data.get("district"),"street":data.get("street"),
        "bldg":data.get("building_number"),"postal":data.get("postal_code"),
        "email":data.get("email"),"phone":data.get("phone"),
        "contact":data.get("contact_person"),
        "cur":data.get("currency_code","SAR"),"terms":data.get("payment_terms_days",30),
        "limit":data.get("credit_limit",0),"gl":data.get("gl_account_code"),
        "tax":data.get("tax_treatment","standard"),
        "notes":data.get("notes"),"by":user.email,
    })
    await db.commit()
    return created(data={"id":vid}, message="تم إنشاء المورد ✅")


@router.put("/vendors/{vid}")
async def update_vendor(vid: uuid.UUID, data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    data.pop("id",None); data.pop("tenant_id",None); data["updated_at"]=datetime.utcnow()
    set_clause=", ".join([f"{k}=:{k}" for k in data.keys()])
    data.update({"id":str(vid),"tid":tid})
    await db.execute(text(f"UPDATE ap_vendors SET {set_clause} WHERE id=:id AND tenant_id=:tid"), data)
    await db.commit()
    return ok(data={}, message="تم التعديل ✅")


@router.get("/vendors/{vid}")
async def get_vendor(vid: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ap_vendors WHERE id=:id AND tenant_id=:tid"), {"id":str(vid),"tid":tid})
    vendor = r.mappings().fetchone()
    if not vendor: raise HTTPException(404,"المورد غير موجود")
    r2 = await db.execute(text("""
        SELECT serial,invoice_date,total_amount,balance_due,status
        FROM ap_invoices WHERE vendor_id=:vid AND tenant_id=:tid AND status='posted'
        ORDER BY invoice_date DESC LIMIT 10
    """), {"vid":str(vid),"tid":tid})
    invoices = [dict(row._mapping) for row in r2.fetchall()]
    return ok(data={"vendor":dict(vendor._mapping),"invoices":invoices})


# ══════════════════════════════════════════════════════════
# PURCHASE REQUESTS (PR)
# ══════════════════════════════════════════════════════════
@router.get("/purchase-requests")
async def list_purchase_requests(
    status: Optional[str]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["pr.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if status: conds.append("pr.status=:status"); params["status"]=status
    where=" AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT pr.*, COUNT(l.id) AS line_count
        FROM ap_purchase_requests pr
        LEFT JOIN ap_purchase_request_lines l ON l.request_id=pr.id
        WHERE {where}
        GROUP BY pr.id ORDER BY pr.request_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/purchase-requests", status_code=201)
async def create_purchase_request(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    req_date = date.fromisoformat(str(data.get("request_date", date.today())))
    serial   = await _next_serial(db, tid, "PR", req_date)
    pr_id    = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO ap_purchase_requests
          (id,tenant_id,serial,request_date,required_date,requested_by,department,priority,description,notes,status,created_by)
        VALUES (:id,:tid,:serial,:dt,:req_dt,:req_by,:dept,:priority,:desc,:notes,'draft',:by)
    """), {"id":pr_id,"tid":tid,"serial":serial,"dt":req_date,
           "req_dt":data.get("required_date"),"req_by":data.get("requested_by",user.email),
           "dept":data.get("department"),"priority":data.get("priority","normal"),
           "desc":data.get("description"),"notes":data.get("notes"),"by":user.email})
    for i,line in enumerate(data.get("lines",[])):
        await db.execute(text("""
            INSERT INTO ap_purchase_request_lines
              (id,tenant_id,request_id,line_order,item_code,item_name,quantity,uom,estimated_price,notes)
            VALUES (gen_random_uuid(),:tid,:pr_id,:order,:code,:name,:qty,:uom,:price,:notes)
        """), {"tid":tid,"pr_id":pr_id,"order":i+1,
               "code":line.get("item_code"),"name":line["item_name"],
               "qty":line["quantity"],"uom":line.get("uom"),
               "price":line.get("estimated_price",0),"notes":line.get("notes")})
    await db.commit()
    return created(data={"id":pr_id,"serial":serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/purchase-requests/{pr_id}/approve")
async def approve_pr(pr_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE ap_purchase_requests SET status='approved', approved_by=:by, approved_at=NOW()
        WHERE id=:id AND tenant_id=:tid AND status='pending'
    """), {"by":user.email,"id":str(pr_id),"tid":tid})
    await db.commit()
    return ok(data={}, message="تم اعتماد طلب الشراء ✅")


# ══════════════════════════════════════════════════════════
# PURCHASE ORDERS (PO)
# ══════════════════════════════════════════════════════════
@router.get("/purchase-orders")
async def list_purchase_orders(
    vendor_id: Optional[uuid.UUID]=Query(None),
    status:    Optional[str]=Query(None),
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["po.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if vendor_id: conds.append("po.vendor_id=:vid"); params["vid"]=str(vendor_id)
    if status:    conds.append("po.status=:status"); params["status"]=status
    if date_from: conds.append("po.po_date>=:df");   params["df"]=str(date_from)
    if date_to:   conds.append("po.po_date<=:dt");   params["dt"]=str(date_to)
    where=" AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM ap_purchase_orders po WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT po.*, v.vendor_name AS vendor_name_disp, v.vendor_code
        FROM ap_purchase_orders po
        LEFT JOIN ap_vendors v ON v.id=po.vendor_id
        WHERE {where}
        ORDER BY po.po_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/purchase-orders", status_code=201)
async def create_purchase_order(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    po_date = date.fromisoformat(str(data["po_date"]))
    serial  = await _next_serial(db, tid, "PO", po_date)
    po_id   = str(uuid.uuid4())
    lines   = data.get("lines",[])
    totals  = _calc_lines(lines)

    # جلب بيانات المورد
    vendor_name = data.get("vendor_name","")
    vendor_vat  = data.get("vendor_vat","")
    if data.get("vendor_id"):
        r = await db.execute(text("SELECT * FROM ap_vendors WHERE id=:vid AND tenant_id=:tid"),
                              {"vid":data["vendor_id"],"tid":tid})
        vendor = r.mappings().fetchone()
        if vendor:
            vendor_name = vendor_name or vendor["vendor_name"]
            vendor_vat  = vendor_vat  or vendor.get("vat_number","")
            if not data.get("payment_terms"):
                data["payment_terms"] = f"صافي {vendor.get('payment_terms_days',30)} يوم"

    await db.execute(text("""
        INSERT INTO ap_purchase_orders
          (id,tenant_id,serial,vendor_id,vendor_name,vendor_vat,po_date,expected_date,
           currency_code,exchange_rate,pr_id,rfq_id,
           subtotal,discount_amount,vat_amount,total_amount,
           warehouse_id,payment_terms,delivery_terms,notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:vid,:vname,:vvat,:po_date,:exp_date,
           :cur,:rate,:pr_id,:rfq_id,
           :sub,:disc,:vat,:total,
           :wh,:pay_terms,:del_terms,:notes,'draft',:by)
    """), {
        "id":po_id,"tid":tid,"serial":serial,"vid":data.get("vendor_id"),
        "vname":vendor_name,"vvat":vendor_vat,"po_date":po_date,
        "exp_date":data.get("expected_date"),"cur":data.get("currency_code","SAR"),
        "rate":data.get("exchange_rate",1),"pr_id":data.get("pr_id"),
        "rfq_id":data.get("rfq_id"),**totals,
        "wh":data.get("warehouse_id"),"pay_terms":data.get("payment_terms"),
        "del_terms":data.get("delivery_terms"),"notes":data.get("notes"),"by":user.email,
    })

    for i,line in enumerate(lines):
        await db.execute(text("""
            INSERT INTO ap_purchase_order_lines
              (id,tenant_id,po_id,line_order,item_code,item_name,quantity,uom,
               unit_price,discount_pct,discount_amount,net_amount,vat_rate,vat_amount,total_amount,
               warehouse_id,cost_center,project_code,notes)
            VALUES
              (gen_random_uuid(),:tid,:po_id,:order,:code,:name,:qty,:uom,
               :price,:disc_pct,:disc,:net,:vat_rate,:vat,:total,
               :wh,:cc,:proj,:notes)
        """), {
            "tid":tid,"po_id":po_id,"order":i+1,
            "code":line.get("item_code"),"name":line["item_name"],
            "qty":Decimal(str(line["quantity"])),"uom":line.get("uom"),
            "price":Decimal(str(line["unit_price"])),
            "disc_pct":line.get("discount_pct",0),"disc":line.get("discount_amount",0),
            "net":line["net_amount"],"vat_rate":line.get("vat_rate",15),
            "vat":line["vat_amount"],"total":line["total_amount"],
            "wh":data.get("warehouse_id"),"cc":line.get("cost_center"),
            "proj":line.get("project_code"),"notes":line.get("notes"),
        })

    await db.commit()
    return created(data={"id":po_id,"serial":serial,"totals":totals},
                   message=f"تم إنشاء أمر الشراء {serial} ✅")


@router.get("/purchase-orders/{po_id}")
async def get_purchase_order(po_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT po.*, v.vendor_name AS vendor_name_disp, v.email AS vendor_email, v.phone AS vendor_phone
        FROM ap_purchase_orders po LEFT JOIN ap_vendors v ON v.id=po.vendor_id
        WHERE po.id=:id AND po.tenant_id=:tid
    """), {"id":str(po_id),"tid":tid})
    po = r.mappings().fetchone()
    if not po: raise HTTPException(404,"أمر الشراء غير موجود")
    r2 = await db.execute(text("SELECT * FROM ap_purchase_order_lines WHERE po_id=:id ORDER BY line_order"), {"id":str(po_id)})
    lines = [dict(row._mapping) for row in r2.fetchall()]
    return ok(data={"po":dict(po._mapping),"lines":lines})


@router.post("/purchase-orders/{po_id}/approve")
async def approve_po(po_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    await db.execute(text("""
        UPDATE ap_purchase_orders SET status='approved', approved_by=:by, approved_at=NOW()
        WHERE id=:id AND tenant_id=:tid AND status='draft'
    """), {"by":user.email,"id":str(po_id),"tid":tid})
    await db.commit()
    return ok(data={}, message="تم اعتماد أمر الشراء ✅")


# ══════════════════════════════════════════════════════════
# GOODS RECEIPT (GRN)
# ══════════════════════════════════════════════════════════
@router.get("/receipts")
async def list_receipts(
    po_id:     Optional[uuid.UUID]=Query(None),
    status:    Optional[str]=Query(None),
    date_from: Optional[date]=Query(None),
    date_to:   Optional[date]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["r.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if po_id:  conds.append("r.po_id=:po"); params["po"]=str(po_id)
    if status: conds.append("r.status=:status"); params["status"]=status
    if date_from: conds.append("r.receipt_date>=:df"); params["df"]=str(date_from)
    if date_to:   conds.append("r.receipt_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT r.*, v.vendor_name, po.serial AS po_serial
        FROM ap_receipts r
        LEFT JOIN ap_vendors v ON v.id=r.vendor_id
        LEFT JOIN ap_purchase_orders po ON po.id=r.po_id
        WHERE {where}
        ORDER BY r.receipt_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/receipts", status_code=201)
async def create_receipt(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    rec_date = date.fromisoformat(str(data["receipt_date"]))
    serial   = await _next_serial(db, tid, "GRN", rec_date)
    rec_id   = str(uuid.uuid4())
    lines    = data.get("lines",[])
    total_qty  = sum(float(l.get("qty_received",0)) for l in lines)
    total_cost = sum(float(l.get("qty_received",0))*float(l.get("unit_cost",0)) for l in lines)

    await db.execute(text("""
        INSERT INTO ap_receipts
          (id,tenant_id,serial,po_id,vendor_id,receipt_date,warehouse_id,
           total_qty,total_cost,notes,status,created_by)
        VALUES (:id,:tid,:serial,:po_id,:vid,:dt,:wh,:tqty,:tcost,:notes,'draft',:by)
    """), {"id":rec_id,"tid":tid,"serial":serial,
           "po_id":data.get("po_id"),"vid":data.get("vendor_id"),
           "dt":rec_date,"wh":data.get("warehouse_id"),
           "tqty":total_qty,"tcost":total_cost,
           "notes":data.get("notes"),"by":user.email})

    for i,line in enumerate(lines):
        qty  = Decimal(str(line["qty_received"]))
        cost = Decimal(str(line.get("unit_cost",0)))
        await db.execute(text("""
            INSERT INTO ap_receipt_lines
              (id,tenant_id,receipt_id,po_line_id,line_order,item_code,item_name,
               qty_ordered,qty_received,unit_cost,total_cost,lot_number,expiry_date,notes)
            VALUES
              (gen_random_uuid(),:tid,:rec_id,:pol_id,:order,:code,:name,
               :qty_ord,:qty_rec,:uc,:tc,:lot,:exp,:notes)
        """), {"tid":tid,"rec_id":rec_id,"pol_id":line.get("po_line_id"),
               "order":i+1,"code":line.get("item_code"),"name":line["item_name"],
               "qty_ord":line.get("qty_ordered",qty),"qty_rec":qty,"uc":cost,"tc":qty*cost,
               "lot":line.get("lot_number"),"exp":line.get("expiry_date"),"notes":line.get("notes")})

        # تحديث كمية المستلمة في أمر الشراء
        if line.get("po_line_id"):
            await db.execute(text("""
                UPDATE ap_purchase_order_lines SET qty_received=qty_received+:qty
                WHERE id=:id AND tenant_id=:tid
            """), {"qty":qty,"id":line["po_line_id"],"tid":tid})

    await db.commit()
    return created(data={"id":rec_id,"serial":serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/receipts/{rec_id}/post")
async def post_receipt(rec_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ap_receipts WHERE id=:id AND tenant_id=:tid"), {"id":str(rec_id),"tid":tid})
    rec = r.mappings().fetchone()
    if not rec: raise HTTPException(404)
    if rec["status"] != "draft": raise HTTPException(400,"مُرحَّل مسبقاً")

    total = Decimal(str(rec["total_cost"] or 0))

    # جلب حسابات من إعدادات GL
    grni_account = "210201"  # بضاعة بالطريق (GRNI)
    r_grni = await db.execute(text("""
        SELECT account_code FROM gl_account_mappings
        WHERE tenant_id=:tid AND mapping_key='ap_grni' LIMIT 1
    """), {"tid":tid})
    grni_row = r_grni.fetchone()
    if grni_row: grni_account = grni_row[0]

    # جمع حسابات الأصناف من أسطر الاستلام
    r_lines = await db.execute(text("""
        SELECT rl.total_cost, COALESCE(i.gl_account_code,'130101') AS gl_acc
        FROM ap_receipt_lines rl
        LEFT JOIN inv_items i ON i.item_code=rl.item_code AND i.tenant_id=:tid
        WHERE rl.receipt_id=:rec_id
    """), {"tid":tid,"rec_id":str(rec_id)})
    item_lines = r_lines.fetchall()

    # بناء أسطر القيد — DR لكل حساب مخزون
    je_lines = []
    from collections import defaultdict
    grouped = defaultdict(Decimal)
    for il in item_lines:
        grouped[il[1]] += Decimal(str(il[0] or 0))
    for acc, amt in grouped.items():
        je_lines.append({"account_code":acc,"debit":amt,"credit":0,"description":f"استلام {rec['serial']}"})
    if not je_lines:  # fallback
        je_lines.append({"account_code":"130101","debit":total,"credit":0,"description":f"استلام {rec['serial']}"})
    # CR بضاعة بالطريق (GRNI)
    je_lines.append({"account_code":grni_account,"debit":0,"credit":total,"description":f"استلام {rec['serial']}"})
    je = await _post_je(db, tid, user.email, "GRN", rec["receipt_date"],
                        f"استلام بضاعة {rec['serial']}", je_lines, rec["serial"])

    # تحديث حالة PO
    if rec["po_id"]:
        r2 = await db.execute(text("""
            SELECT SUM(quantity) AS tot, SUM(qty_received) AS rec
            FROM ap_purchase_order_lines WHERE po_id=:po_id AND tenant_id=:tid
        """), {"po_id":str(rec["po_id"]),"tid":tid})
        sums = r2.fetchone()
        po_status = "received" if sums and sums[0] and sums[1] and float(sums[1]) >= float(sums[0]) else "partial"
        await db.execute(text("UPDATE ap_purchase_orders SET status=:st WHERE id=:id AND tenant_id=:tid"),
                          {"st":po_status,"id":str(rec["po_id"]),"tid":tid})

    await db.execute(text("""
        UPDATE ap_receipts SET status='posted', je_id=:je_id, je_serial=:je_serial,
               posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id":je["je_id"],"je_serial":je["je_serial"],"by":user.email,"id":str(rec_id),"tid":tid})
    await db.commit()
    return ok(data={"je_serial":je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# AP INVOICES
# ══════════════════════════════════════════════════════════
@router.get("/invoices")
async def list_invoices(
    vendor_id:    Optional[uuid.UUID]=Query(None),
    status:       Optional[str]=Query(None),
    invoice_type: Optional[str]=Query(None),
    date_from:    Optional[date]=Query(None),
    date_to:      Optional[date]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["i.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if vendor_id:    conds.append("i.vendor_id=:vid"); params["vid"]=str(vendor_id)
    if status:       conds.append("i.status=:status"); params["status"]=status
    if invoice_type: conds.append("i.invoice_type=:itype"); params["itype"]=invoice_type
    if date_from:    conds.append("i.invoice_date>=:df"); params["df"]=str(date_from)
    if date_to:      conds.append("i.invoice_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)
    cnt = await db.execute(text(f"SELECT COUNT(*) FROM ap_invoices i WHERE {where}"), params)
    r = await db.execute(text(f"""
        SELECT i.*, v.vendor_name AS vendor_name_disp, v.vendor_code
        FROM ap_invoices i LEFT JOIN ap_vendors v ON v.id=i.vendor_id
        WHERE {where}
        ORDER BY i.invoice_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data={"total":cnt.scalar(),"items":[dict(row._mapping) for row in r.fetchall()]})


@router.post("/invoices", status_code=201)
async def create_invoice(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    inv_date = date.fromisoformat(str(data["invoice_date"]))
    serial   = await _next_serial(db, tid, "APINV", inv_date)
    inv_id   = str(uuid.uuid4())
    lines    = data.get("lines",[])
    totals   = _calc_lines(lines)

    # تاريخ الاستحقاق
    due_date = data.get("due_date")
    if not due_date and data.get("vendor_id"):
        r = await db.execute(text("SELECT payment_terms_days FROM ap_vendors WHERE id=:vid"), {"vid":data["vendor_id"]})
        vrow = r.fetchone()
        due_date = str(inv_date + timedelta(days=vrow[0] or 30)) if vrow else None

    await db.execute(text("""
        INSERT INTO ap_invoices
          (id,tenant_id,serial,invoice_type,vendor_id,vendor_name,vendor_vat,vendor_invoice_no,
           invoice_date,due_date,po_id,receipt_id,currency_code,exchange_rate,
           subtotal,discount_amount,vat_amount,total_amount,balance_due,
           notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:inv_type,:vid,:vname,:vvat,:vinv_no,
           :inv_date,:due_date,:po_id,:rec_id,:cur,:rate,
           :sub,:disc,:vat,:total,:total,
           :notes,'draft',:by)
    """), {
        "id":inv_id,"tid":tid,"serial":serial,
        "inv_type":data.get("invoice_type","purchase"),
        "vid":data.get("vendor_id"),"vname":data.get("vendor_name"),
        "vvat":data.get("vendor_vat"),
        "vinv_no":data.get("vendor_invoice_no"),
        "inv_date":inv_date,"due_date":due_date,
        "po_id":data.get("po_id"),"rec_id":data.get("receipt_id"),
        "cur":data.get("currency_code","SAR"),"rate":data.get("exchange_rate",1),
        **totals,"notes":data.get("notes"),"by":user.email,
    })

    for i,line in enumerate(lines):
        await db.execute(text("""
            INSERT INTO ap_invoice_lines
              (id,tenant_id,invoice_id,po_line_id,line_order,item_code,item_name,
               quantity,unit_price,discount_pct,discount_amount,net_amount,
               vat_rate,vat_amount,total_amount,cost_center,project_code)
            VALUES
              (gen_random_uuid(),:tid,:inv_id,:pol_id,:order,:code,:name,
               :qty,:price,:disc_pct,:disc,:net,:vat_rate,:vat,:total,:cc,:proj)
        """), {
            "tid":tid,"inv_id":inv_id,"pol_id":line.get("po_line_id"),
            "order":i+1,"code":line.get("item_code"),"name":line["item_name"],
            "qty":Decimal(str(line["quantity"])),"price":Decimal(str(line["unit_price"])),
            "disc_pct":line.get("discount_pct",0),"disc":line.get("discount_amount",0),
            "net":line["net_amount"],"vat_rate":line.get("vat_rate",15),
            "vat":line["vat_amount"],"total":line["total_amount"],
            "cc":line.get("cost_center"),"proj":line.get("project_code"),
        })

    await db.commit()
    return created(data={"id":inv_id,"serial":serial,"totals":totals},
                   message=f"تم إنشاء الفاتورة {serial} ✅")


@router.post("/invoices/{inv_id}/post")
async def post_invoice(inv_id: uuid.UUID, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ap_invoices WHERE id=:id AND tenant_id=:tid"), {"id":str(inv_id),"tid":tid})
    inv = r.mappings().fetchone()
    if not inv: raise HTTPException(404)
    if inv["status"] != "draft": raise HTTPException(400,"مُرحَّلة مسبقاً")

    # ── جلب الحسابات من gl_account_mappings ──────────────
    async def _get_mapping(db, tid, key, default):
        r = await db.execute(text("""
            SELECT account_code FROM gl_account_mappings
            WHERE tenant_id=:tid AND mapping_key=:key LIMIT 1
        """), {"tid":tid,"key":key})
        row = r.fetchone()
        return row[0] if row else default

    ap_account   = "210101"
    grni_account = await _get_mapping(db, tid, "ap_grni", "210201")
    vat_account  = await _get_mapping(db, tid, "ap_vat_input", "240201")
    expense_acc  = await _get_mapping(db, tid, "ap_purchase_expense", "510101")

    if inv["vendor_id"]:
        r2 = await db.execute(text("SELECT gl_account_code FROM ap_vendors WHERE id=:vid"),{"vid":str(inv["vendor_id"])})
        vrow = r2.fetchone()
        if vrow and vrow[0]: ap_account = vrow[0]

    total = Decimal(str(inv["total_amount"]))
    vat   = Decimal(str(inv["vat_amount"]))
    net   = Decimal(str(inv["subtotal"]))

    # ── 3-Way Match: مرتبطة بـ GRN → تصفير GRNI ─────────
    if inv.get("receipt_id"):
        je_lines = [
            {"account_code": grni_account, "debit": net,  "credit": 0,    "description": f"إغلاق GRNI {inv['serial']}"},
            {"account_code": vat_account,  "debit": vat,  "credit": 0,    "description": f"ضريبة مدخلات {inv['serial']}"},
            {"account_code": ap_account,   "debit": 0,    "credit": total, "description": f"فاتورة مورد {inv['serial']}"},
        ]
    # ── 2-Way Match: مشتريات مباشرة (خدمات / مصاريف) ────
    else:
        je_lines = [
            {"account_code": expense_acc,  "debit": net,  "credit": 0,    "description": f"مشتريات {inv['serial']}"},
            {"account_code": vat_account,  "debit": vat,  "credit": 0,    "description": f"ضريبة مدخلات {inv['serial']}"},
            {"account_code": ap_account,   "debit": 0,    "credit": total, "description": f"فاتورة مورد {inv['serial']}"},
        ]

    je = await _post_je(db, tid, user.email, "APINV", inv["invoice_date"],
                        f"فاتورة مورد {inv['serial']}", je_lines, inv["serial"])

    await db.execute(text("""
        UPDATE ap_invoices SET status='posted', je_id=:je_id, je_serial=:je_serial,
               posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id":je["je_id"],"je_serial":je["je_serial"],"by":user.email,"id":str(inv_id),"tid":tid})
    await db.commit()
    return ok(data={"je_serial":je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# PAYMENTS
# ══════════════════════════════════════════════════════════
@router.get("/payments")
async def list_payments(
    vendor_id: Optional[uuid.UUID]=Query(None),
    status:    Optional[str]=Query(None),
    limit:int=Query(50), offset:int=Query(0),
    db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user),
):
    tid = str(user.tenant_id)
    conds=["p.tenant_id=:tid"]
    params:dict={"tid":tid,"limit":limit,"offset":offset}
    if vendor_id: conds.append("p.vendor_id=:vid"); params["vid"]=str(vendor_id)
    if status:    conds.append("p.status=:status"); params["status"]=status
    where=" AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT p.*, v.vendor_name
        FROM ap_payments p LEFT JOIN ap_vendors v ON v.id=p.vendor_id
        WHERE {where}
        ORDER BY p.payment_date DESC
        LIMIT :limit OFFSET :offset
    """), params)
    return ok(data=[dict(row._mapping) for row in r.fetchall()])


@router.post("/payments", status_code=201)
async def create_payment(data:dict, db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    pay_date = date.fromisoformat(str(data["payment_date"]))
    serial   = await _next_serial(db, tid, "APPAY", pay_date)
    pay_id   = str(uuid.uuid4())
    amt      = Decimal(str(data["amount"]))
    rate     = Decimal(str(data.get("exchange_rate",1)))

    await db.execute(text("""
        INSERT INTO ap_payments
          (id,tenant_id,serial,vendor_id,payment_date,amount,currency_code,
           exchange_rate,amount_sar,payment_method,bank_account_id,check_number,
           reference,description,notes,status,created_by)
        VALUES
          (:id,:tid,:serial,:vid,:dt,:amt,:cur,:rate,:sar,:method,:bank,:check,:ref,:desc,:notes,'draft',:by)
    """), {
        "id":pay_id,"tid":tid,"serial":serial,"vid":data.get("vendor_id"),
        "dt":pay_date,"amt":amt,"cur":data.get("currency_code","SAR"),
        "rate":rate,"sar":amt*rate,"method":data.get("payment_method","bank"),
        "bank":data.get("bank_account_id"),"check":data.get("check_number"),
        "ref":data.get("reference"),"desc":data.get("description"),
        "notes":data.get("notes"),"by":user.email,
    })
    await db.commit()
    return created(data={"id":pay_id,"serial":serial}, message=f"تم إنشاء {serial} ✅")


@router.post("/payments/{pay_id}/post")
async def post_payment(pay_id: uuid.UUID, invoice_id: Optional[uuid.UUID]=Query(None), db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ap_payments WHERE id=:id AND tenant_id=:tid"), {"id":str(pay_id),"tid":tid})
    pay = r.mappings().fetchone()
    if not pay: raise HTTPException(404)
    if pay["status"] != "draft": raise HTTPException(400,"مُرحَّل مسبقاً")

    ap_account = "210101"
    if pay["vendor_id"]:
        r2 = await db.execute(text("SELECT gl_account_code FROM ap_vendors WHERE id=:vid"),{"vid":str(pay["vendor_id"])})
        vrow = r2.fetchone()
        if vrow and vrow[0]: ap_account = vrow[0]

    bank_account = "110201"
    amt = Decimal(str(pay["amount_sar"] or pay["amount"]))
    je_lines = [
        {"account_code":ap_account, "debit":amt, "credit":0,  "description":f"دفعة {pay['serial']}"},
        {"account_code":bank_account,"debit":0, "credit":amt, "description":f"دفعة {pay['serial']}"},
    ]
    je = await _post_je(db, tid, user.email, "APPAY", pay["payment_date"],
                        f"دفعة لمورد {pay['serial']}", je_lines, pay["serial"])

    await db.execute(text("""
        UPDATE ap_payments SET status='posted', je_id=:je_id, je_serial=:je_serial,
               posted_by=:by, posted_at=NOW()
        WHERE id=:id AND tenant_id=:tid
    """), {"je_id":je["je_id"],"je_serial":je["je_serial"],"by":user.email,"id":str(pay_id),"tid":tid})

    if invoice_id:
        r3 = await db.execute(text("SELECT balance_due FROM ap_invoices WHERE id=:id AND tenant_id=:tid"),{"id":str(invoice_id),"tid":tid})
        inv_row = r3.fetchone()
        if inv_row:
            applied = min(amt, Decimal(str(inv_row[0])))
            await db.execute(text("""
                INSERT INTO ap_payment_applications (id,tenant_id,payment_id,invoice_id,amount_applied)
                VALUES (gen_random_uuid(),:tid,:pay_id,:inv_id,:applied)
            """), {"tid":tid,"pay_id":str(pay_id),"inv_id":str(invoice_id),"applied":applied})
            new_bal = Decimal(str(inv_row[0])) - applied
            await db.execute(text("""
                UPDATE ap_invoices SET balance_due=:bal, paid_amount=paid_amount+:paid,
                       status=CASE WHEN :bal<=0 THEN 'paid' ELSE 'partial' END
                WHERE id=:id AND tenant_id=:tid
            """), {"bal":new_bal,"paid":applied,"id":str(invoice_id),"tid":tid})

    await db.commit()
    return ok(data={"je_serial":je["je_serial"]}, message=f"✅ تم الترحيل — {je['je_serial']}")


# ══════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════
@router.get("/reports/aging")
async def aging_report(as_of: Optional[date]=Query(None), db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    ref = str(as_of or date.today())
    r = await db.execute(text("""
        SELECT v.vendor_code, v.vendor_name,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date<=30  THEN i.balance_due END),0) AS bucket_30,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date BETWEEN 31 AND 60 THEN i.balance_due END),0) AS bucket_60,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date BETWEEN 61 AND 90 THEN i.balance_due END),0) AS bucket_90,
               COALESCE(SUM(CASE WHEN :dt::date-i.due_date>90 THEN i.balance_due END),0) AS bucket_90plus,
               COALESCE(SUM(i.balance_due),0) AS total_balance,
               COUNT(i.id) AS invoice_count
        FROM ap_vendors v
        JOIN ap_invoices i ON i.vendor_id=v.id AND i.tenant_id=:tid
                           AND i.status='posted' AND i.balance_due>0
        WHERE v.tenant_id=:tid
        GROUP BY v.id,v.vendor_code,v.vendor_name
        HAVING COALESCE(SUM(i.balance_due),0)>0
        ORDER BY total_balance DESC
    """), {"tid":tid,"dt":ref})
    rows = [dict(row._mapping) for row in r.fetchall()]
    totals = {k:sum(float(r.get(k,0)) for r in rows)
              for k in ["bucket_30","bucket_60","bucket_90","bucket_90plus","total_balance"]}
    return ok(data={"vendors":rows,"totals":totals,"as_of":ref})


@router.get("/reports/vendor-statement/{vid}")
async def vendor_statement(vid: uuid.UUID, date_from: Optional[date]=Query(None), date_to: Optional[date]=Query(None), db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("SELECT * FROM ap_vendors WHERE id=:id AND tenant_id=:tid"),{"id":str(vid),"tid":tid})
    vendor = r.mappings().fetchone()
    if not vendor: raise HTTPException(404)
    conds=["i.vendor_id=:vid AND i.tenant_id=:tid AND i.status='posted'"]
    params:dict={"vid":str(vid),"tid":tid}
    if date_from: conds.append("i.invoice_date>=:df"); params["df"]=str(date_from)
    if date_to:   conds.append("i.invoice_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)
    r2 = await db.execute(text(f"""
        SELECT 'invoice' AS tx_type, serial AS ref, invoice_date AS tx_date,
               invoice_type AS sub_type, total_amount AS credit, 0 AS debit, balance_due
        FROM ap_invoices i WHERE {where}
        UNION ALL
        SELECT 'payment', p.serial, p.payment_date, p.payment_method, 0, p.amount, NULL
        FROM ap_payments p
        JOIN ap_payment_applications pa ON pa.payment_id=p.id
        JOIN ap_invoices inv ON inv.id=pa.invoice_id AND inv.vendor_id=:vid
        WHERE p.tenant_id=:tid AND p.status='posted'
        ORDER BY tx_date
    """), params)
    txs = [dict(row._mapping) for row in r2.fetchall()]
    total_invoiced  = sum(float(t["credit"] or 0) for t in txs if t["tx_type"]=="invoice")
    total_paid      = sum(float(t["debit"] or 0) for t in txs if t["tx_type"]=="payment")
    return ok(data={"vendor":dict(vendor._mapping),"transactions":txs,
                    "summary":{"total_invoiced":total_invoiced,"total_paid":total_paid,"balance":total_invoiced-total_paid}})


@router.get("/reports/purchase-summary")
async def purchase_summary(date_from: Optional[date]=Query(None), date_to: Optional[date]=Query(None), db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    conds=["i.tenant_id=:tid AND i.status='posted' AND i.invoice_type='purchase'"]
    params:dict={"tid":tid}
    if date_from: conds.append("i.invoice_date>=:df"); params["df"]=str(date_from)
    if date_to:   conds.append("i.invoice_date<=:dt"); params["dt"]=str(date_to)
    where=" AND ".join(conds)
    r = await db.execute(text(f"""
        SELECT v.vendor_name, COUNT(i.id) AS invoice_count,
               SUM(i.subtotal) AS net_purchases, SUM(i.vat_amount) AS vat_total,
               SUM(i.total_amount) AS gross_purchases, SUM(i.balance_due) AS outstanding
        FROM ap_invoices i JOIN ap_vendors v ON v.id=i.vendor_id
        WHERE {where} GROUP BY v.id,v.vendor_name ORDER BY gross_purchases DESC
    """), params)
    rows = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"rows":rows,"total":sum(float(r["gross_purchases"] or 0) for r in rows)})


@router.get("/reports/pending-delivery")
async def pending_delivery(db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT po.serial, v.vendor_name, po.po_date, po.expected_date,
               l.item_name, l.quantity, l.qty_received,
               l.quantity-l.qty_received AS qty_pending,
               l.unit_price, (l.quantity-l.qty_received)*l.unit_price AS pending_value
        FROM ap_purchase_order_lines l
        JOIN ap_purchase_orders po ON po.id=l.po_id
        LEFT JOIN ap_vendors v ON v.id=po.vendor_id
        WHERE po.tenant_id=:tid AND po.status NOT IN ('closed','cancelled')
          AND l.qty_received < l.quantity
        ORDER BY po.expected_date, po.serial
    """), {"tid":tid})
    rows = [dict(row._mapping) for row in r.fetchall()]
    return ok(data={"rows":rows,"total_pending":sum(float(r["pending_value"] or 0) for r in rows)})


@router.get("/reports/vendor-performance")
async def vendor_performance(db: AsyncSession=Depends(get_db), user: CurrentUser=Depends(get_current_user)):
    tid = str(user.tenant_id)
    r = await db.execute(text("""
        SELECT v.vendor_name, v.vendor_code, v.rating,
               COUNT(DISTINCT po.id) AS total_orders,
               COALESCE(SUM(po.total_amount),0) AS total_purchases,
               ROUND(AVG(CASE WHEN po.status='received'
                 THEN (SELECT receipt_date FROM ap_receipts WHERE po_id=po.id AND status='posted' LIMIT 1)
                      - po.po_date END),1) AS avg_lead_days
        FROM ap_vendors v
        LEFT JOIN ap_purchase_orders po ON po.vendor_id=v.id AND po.tenant_id=:tid
        WHERE v.tenant_id=:tid AND v.is_active=true
        GROUP BY v.id,v.vendor_name,v.vendor_code,v.rating
        ORDER BY total_purchases DESC
    """), {"tid":tid})
    return ok(data=[dict(row._mapping) for row in r.fetchall()])
