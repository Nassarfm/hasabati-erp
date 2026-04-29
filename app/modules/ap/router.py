"""
app/modules/ap/router.py
══════════════════════════════════════════════════════════
Accounts Payable & Procurement Module
حساباتي ERP — موديول المشتريات والذمم الدائنة
Procure-to-Pay Cycle (P2P)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import ok, created
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

router = APIRouter(prefix="/ap", tags=["المشتريات والذمم الدائنة"])


# ══════════════════════════════════════════════════════════
# Serial Generator
# ══════════════════════════════════════════════════════════
async def _next_serial(db: AsyncSession, tid: str, code: str, tx_date: date) -> str:
    year = tx_date.year
    try:
        r = await db.execute(text("""
            SELECT last_sequence FROM je_sequences
            WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:yr
        """), {"tid":tid,"code":code,"yr":year})
        row = r.fetchone()
        seq = (row[0] if row else 0) + 1
        if row:
            await db.execute(text("""
                UPDATE je_sequences SET last_sequence=:seq
                WHERE tenant_id=:tid AND je_type_code=:code AND fiscal_year=:yr
            """), {"tid":tid,"code":code,"yr":year,"seq":seq})
        else:
            await db.execute(text("""
                INSERT INTO je_sequences (id,tenant_id,je_type_code,fiscal_year,last_sequence)
                VALUES (gen_random_uuid(),:tid,:code,:yr,:seq)
            """), {"tid":tid,"code":code,"yr":year,"seq":seq})
        r2 = await db.execute(text("""
            SELECT prefix, padding, separator FROM series_settings
            WHERE tenant_id=:tid AND je_type_code=:code LIMIT 1
        """), {"tid":tid,"code":code})
        sr = r2.fetchone()
        prefix  = sr[0] if sr else code
        padding = sr[1] if sr else 7
        sep     = sr[2] if sr else "-"
        return f"{prefix}{sep}{year}{sep}{seq:0{padding}d}"
    except Exception:
        import traceback; print(f"[serial] {traceback.format_exc()}")
        return f"{code}-{year}-{uuid.uuid4().hex[:6].upper()}"


# ══════════════════════════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════════════════════════
@router.get("/dashboard")
async def ap_dashboard(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT
              (SELECT COUNT(*) FROM ap_purchase_orders WHERE tenant_id=:tid AND status NOT IN ('cancelled')) AS total_pos,
              (SELECT COUNT(*) FROM ap_purchase_orders WHERE tenant_id=:tid AND status='draft') AS pending_pos,
              (SELECT COALESCE(SUM(total_amount),0) FROM ap_purchase_orders WHERE tenant_id=:tid AND status='approved' AND EXTRACT(YEAR FROM po_date)=EXTRACT(YEAR FROM CURRENT_DATE)) AS year_spend,
              (SELECT COALESCE(SUM(remaining_amount),0) FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND remaining_amount>0) AS total_payable,
              (SELECT COUNT(*) FROM ap_invoices WHERE tenant_id=:tid AND status='posted' AND remaining_amount>0 AND due_date < CURRENT_DATE) AS overdue_count,
              (SELECT COUNT(*) FROM ap_purchase_requests WHERE tenant_id=:tid AND status='pending') AS pending_prs,
              (SELECT COUNT(*) FROM ap_receipts WHERE tenant_id=:tid AND status='draft') AS pending_grn,
              (SELECT COUNT(DISTINCT vendor_id) FROM ap_purchase_orders WHERE tenant_id=:tid AND status NOT IN ('cancelled','draft')) AS active_vendors
        """), {"tid": tid})
        krow = dict(r.mappings().fetchone() or {})

        # Top vendors by spend
        r2 = await db.execute(text("""
            SELECT p.vendor_name, COALESCE(SUM(po.total_amount),0) AS total_spend,
                   COUNT(po.id) AS po_count
            FROM ap_purchase_orders po
            JOIN parties p ON p.id=po.vendor_id
            WHERE po.tenant_id=:tid AND po.status NOT IN ('cancelled','draft')
              AND EXTRACT(YEAR FROM po.po_date)=EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY p.id, p.vendor_name
            ORDER BY total_spend DESC LIMIT 5
        """), {"tid": tid})
        top_vendors = [dict(r._mapping) for r in r2.fetchall()]

        # Monthly spend (last 6 months)
        r3 = await db.execute(text("""
            SELECT TO_CHAR(po_date,'YYYY-MM') AS month,
                   COALESCE(SUM(total_amount),0) AS spend
            FROM ap_purchase_orders
            WHERE tenant_id=:tid AND status NOT IN ('cancelled','draft')
              AND po_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY TO_CHAR(po_date,'YYYY-MM')
            ORDER BY month
        """), {"tid": tid})
        monthly_spend = [dict(r._mapping) for r in r3.fetchall()]

        # Recent POs
        r4 = await db.execute(text("""
            SELECT po.serial, po.po_date::text, po.total_amount, po.status,
                   p.vendor_name
            FROM ap_purchase_orders po
            LEFT JOIN parties p ON p.id=po.vendor_id
            WHERE po.tenant_id=:tid
            ORDER BY po.created_at DESC LIMIT 8
        """), {"tid": tid})
        recent_pos = [dict(r._mapping) for r in r4.fetchall()]

        # Overdue invoices
        r5 = await db.execute(text("""
            SELECT inv.serial, inv.due_date::text, inv.remaining_amount,
                   p.vendor_name,
                   (CURRENT_DATE - inv.due_date) AS days_overdue
            FROM ap_invoices inv
            LEFT JOIN parties p ON p.id=inv.vendor_id
            WHERE inv.tenant_id=:tid AND inv.status='posted'
              AND inv.remaining_amount>0 AND inv.due_date < CURRENT_DATE
            ORDER BY inv.due_date ASC LIMIT 5
        """), {"tid": tid})
        overdue = [dict(r._mapping) for r in r5.fetchall()]

        return ok(data={
            "kpis": {
                "total_pos":      int(krow.get("total_pos") or 0),
                "pending_pos":    int(krow.get("pending_pos") or 0),
                "year_spend":     float(krow.get("year_spend") or 0),
                "total_payable":  float(krow.get("total_payable") or 0),
                "overdue_count":  int(krow.get("overdue_count") or 0),
                "pending_prs":    int(krow.get("pending_prs") or 0),
                "pending_grn":    int(krow.get("pending_grn") or 0),
                "active_vendors": int(krow.get("active_vendors") or 0),
            },
            "top_vendors":   top_vendors,
            "monthly_spend": monthly_spend,
            "recent_pos":    recent_pos,
            "overdue":       overdue,
        })
    except Exception as e:
        import traceback; print(f"[ap/dashboard] {traceback.format_exc()}")
        return ok(data={"kpis":{},"top_vendors":[],"monthly_spend":[],"recent_pos":[],"overdue":[]}, message=str(e))


# ══════════════════════════════════════════════════════════
# VENDORS — يستخدم parties
# ══════════════════════════════════════════════════════════
@router.get("/vendors")
async def list_vendors(
    search: Optional[str] = Query(None),
    limit:  int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["p.tenant_id=:tid", "pr.role_code='VENDOR'"]
        params: dict = {"tid":tid,"limit":limit}
        if search:
            conds.append("(p.name ILIKE :s OR p.code ILIKE :s OR p.tax_number ILIKE :s)")
            params["s"] = f"%{search}%"
        r = await db.execute(text(f"""
            SELECT p.*, pr.role_code,
                   COALESCE(SUM(CASE WHEN inv.status='posted' THEN inv.remaining_amount ELSE 0 END),0) AS open_balance,
                   COUNT(DISTINCT po.id) AS po_count
            FROM parties p
            JOIN party_roles pr ON pr.party_id=p.id AND pr.role_code='VENDOR'
            LEFT JOIN ap_invoices inv ON inv.vendor_id=p.id AND inv.tenant_id=:tid
            LEFT JOIN ap_purchase_orders po ON po.vendor_id=p.id AND po.tenant_id=:tid
            WHERE {" AND ".join(conds)}
            GROUP BY p.id, pr.role_code
            ORDER BY p.name LIMIT :limit
        """), params)
        rows = [dict(r._mapping) for r in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        import traceback; print(f"[ap/vendors] {traceback.format_exc()}")
        return ok(data=[], message=str(e))


@router.get("/vendors/{vendor_id}")
async def get_vendor(
    vendor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT p.*,
                   COALESCE(SUM(CASE WHEN inv.status='posted' THEN inv.remaining_amount ELSE 0 END),0) AS open_balance,
                   COUNT(DISTINCT po.id) AS po_count,
                   COALESCE(SUM(po.total_amount),0) AS total_spend
            FROM parties p
            LEFT JOIN ap_invoices inv ON inv.vendor_id=p.id AND inv.tenant_id=:tid
            LEFT JOIN ap_purchase_orders po ON po.vendor_id=p.id AND po.tenant_id=:tid AND po.status NOT IN ('cancelled')
            WHERE p.id=:id AND p.tenant_id=:tid
            GROUP BY p.id
        """), {"id":str(vendor_id),"tid":tid})
        row = r.mappings().fetchone()
        if not row: raise HTTPException(404, "المورد غير موجود")
        return ok(data=dict(row))
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/vendors/{vendor_id}/open-invoices")
async def vendor_open_invoices(
    vendor_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT inv.*, p.vendor_name
            FROM ap_invoices inv
            LEFT JOIN parties p ON p.id=inv.vendor_id
            WHERE inv.tenant_id=:tid AND inv.vendor_id=:vid
              AND inv.status='posted' AND inv.remaining_amount>0
            ORDER BY inv.due_date ASC
        """), {"tid":tid,"vid":str(vendor_id)})
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


# ══════════════════════════════════════════════════════════
# PURCHASE REQUESTS (PR)
# ══════════════════════════════════════════════════════════
@router.get("/purchase-requests")
async def list_prs(
    status: Optional[str] = Query(None),
    limit:  int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["pr.tenant_id=:tid"]
        params: dict = {"tid":tid,"limit":limit}
        if status: conds.append("pr.status=:st"); params["st"]=status
        r = await db.execute(text(f"""
            SELECT pr.*, p.name AS requester_name
            FROM ap_purchase_requests pr
            LEFT JOIN parties p ON p.id=pr.requester_id
            WHERE {" AND ".join(conds)}
            ORDER BY pr.created_at DESC LIMIT :limit
        """), params)
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.post("/purchase-requests", status_code=201)
async def create_pr(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    pr_id = str(uuid.uuid4())
    req_date = date.fromisoformat(str(data.get("request_date", date.today())))
    serial = await _next_serial(db, tid, "PR", req_date)
    try:
        await db.execute(text("""
            INSERT INTO ap_purchase_requests
              (id,tenant_id,serial,request_date,requester_id,department,
               priority,estimated_amount,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:rd,:req_id,:dept,
               :priority,:amount,:notes,'pending',:by)
        """), {
            "id":pr_id,"tid":tid,"serial":serial,
            "rd":req_date,
            "req_id":data.get("requester_id"),
            "dept":data.get("department",""),
            "priority":data.get("priority","normal"),
            "amount":Decimal(str(data.get("estimated_amount",0))),
            "notes":data.get("notes",""),
            "by":user.email,
        })
        # Insert lines
        for i,line in enumerate(data.get("lines",[])):
            await db.execute(text("""
                INSERT INTO ap_pr_lines
                  (id,tenant_id,pr_id,line_no,item_id,description,qty,uom,estimated_unit_price,notes)
                VALUES
                  (gen_random_uuid(),:tid,:prid,:ln,:item,:desc,:qty,:uom,:price,:notes)
            """), {
                "tid":tid,"prid":pr_id,"ln":i+1,
                "item":line.get("item_id"),
                "desc":line.get("description",""),
                "qty":Decimal(str(line.get("qty",1))),
                "uom":line.get("uom",""),
                "price":Decimal(str(line.get("estimated_unit_price",0))),
                "notes":line.get("notes",""),
            })
        await db.commit()
        return created(data={"id":pr_id,"serial":serial}, message=f"تم إنشاء طلب الشراء {serial} ✅")
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/pr] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


@router.post("/purchase-requests/{pr_id}/approve")
async def approve_pr(
    pr_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        await db.execute(text("""
            UPDATE ap_purchase_requests SET status='approved',
              approved_by=:by, approved_at=NOW()
            WHERE id=:id AND tenant_id=:tid AND status='pending'
        """), {"id":str(pr_id),"tid":tid,"by":user.email})
        await db.commit()
        return ok(data={}, message="تم اعتماد طلب الشراء ✅")
    except Exception as e:
        await db.rollback(); raise HTTPException(400, str(e))


@router.get("/purchase-requests/{pr_id}")
async def get_pr(
    pr_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT pr.* FROM ap_purchase_requests pr
            WHERE pr.id=:id AND pr.tenant_id=:tid
        """), {"id":str(pr_id),"tid":tid})
        row = r.mappings().fetchone()
        if not row: raise HTTPException(404,"طلب الشراء غير موجود")
        result = dict(row)
        r2 = await db.execute(text("""
            SELECT l.*, i.item_name, i.item_code
            FROM ap_pr_lines l
            LEFT JOIN inv_items i ON i.id=l.item_id
            WHERE l.pr_id=:prid ORDER BY l.line_no
        """), {"prid":str(pr_id)})
        result["lines"] = [dict(r._mapping) for r in r2.fetchall()]
        return ok(data=result)
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ══════════════════════════════════════════════════════════
# RFQ — طلب عروض الأسعار
# ══════════════════════════════════════════════════════════
@router.get("/rfq")
async def list_rfq(
    status: Optional[str] = Query(None),
    limit:  int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["r.tenant_id=:tid"]
        params: dict = {"tid":tid,"limit":limit}
        if status: conds.append("r.status=:st"); params["st"]=status
        r = await db.execute(text(f"""
            SELECT r.*,
                   COUNT(DISTINCT rv.id) AS response_count
            FROM ap_rfq r
            LEFT JOIN ap_rfq_responses rv ON rv.rfq_id=r.id
            WHERE {" AND ".join(conds)}
            GROUP BY r.id
            ORDER BY r.created_at DESC LIMIT :limit
        """), params)
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.post("/rfq", status_code=201)
async def create_rfq(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    rfq_id = str(uuid.uuid4())
    rfq_date = date.fromisoformat(str(data.get("rfq_date", date.today())))
    serial = await _next_serial(db, tid, "RFQ", rfq_date)
    try:
        await db.execute(text("""
            INSERT INTO ap_rfq
              (id,tenant_id,serial,rfq_date,deadline_date,pr_id,
               description,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:rd,:deadline,:pr_id,
               :desc,:notes,'open',:by)
        """), {
            "id":rfq_id,"tid":tid,"serial":serial,
            "rd":rfq_date,
            "deadline":date.fromisoformat(str(data["deadline_date"])) if data.get("deadline_date") else None,
            "pr_id":data.get("pr_id"),
            "desc":data.get("description",""),
            "notes":data.get("notes",""),
            "by":user.email,
        })
        # Invite vendors
        for vendor_id in data.get("vendor_ids",[]):
            await db.execute(text("""
                INSERT INTO ap_rfq_vendors (id,rfq_id,vendor_id,tenant_id)
                VALUES (gen_random_uuid(),:rfq,:vid,:tid)
                ON CONFLICT DO NOTHING
            """), {"rfq":rfq_id,"vid":str(vendor_id),"tid":tid})
        # Lines
        for i,line in enumerate(data.get("lines",[])):
            await db.execute(text("""
                INSERT INTO ap_rfq_lines
                  (id,tenant_id,rfq_id,line_no,item_id,description,qty,uom)
                VALUES (gen_random_uuid(),:tid,:rfq,:ln,:item,:desc,:qty,:uom)
            """), {
                "tid":tid,"rfq":rfq_id,"ln":i+1,
                "item":line.get("item_id"),
                "desc":line.get("description",""),
                "qty":Decimal(str(line.get("qty",1))),
                "uom":line.get("uom",""),
            })
        await db.commit()
        return created(data={"id":rfq_id,"serial":serial}, message=f"تم إنشاء طلب العروض {serial} ✅")
    except Exception as e:
        await db.rollback()
        raise HTTPException(400, str(e))


@router.get("/rfq/{rfq_id}")
async def get_rfq(
    rfq_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("SELECT * FROM ap_rfq WHERE id=:id AND tenant_id=:tid"),
                             {"id":str(rfq_id),"tid":tid})
        row = r.mappings().fetchone()
        if not row: raise HTTPException(404,"RFQ غير موجود")
        result = dict(row)
        # Lines
        r2 = await db.execute(text("""
            SELECT l.*, i.item_name FROM ap_rfq_lines l
            LEFT JOIN inv_items i ON i.id=l.item_id
            WHERE l.rfq_id=:id ORDER BY l.line_no
        """), {"id":str(rfq_id)})
        result["lines"] = [dict(r._mapping) for r in r2.fetchall()]
        # Responses
        r3 = await db.execute(text("""
            SELECT rv.*, p.name AS vendor_name
            FROM ap_rfq_responses rv
            JOIN parties p ON p.id=rv.vendor_id
            WHERE rv.rfq_id=:id
            ORDER BY rv.total_amount ASC
        """), {"id":str(rfq_id)})
        result["responses"] = [dict(r._mapping) for r in r3.fetchall()]
        return ok(data=result)
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ══════════════════════════════════════════════════════════
# PURCHASE ORDERS (PO)
# ══════════════════════════════════════════════════════════
@router.get("/purchase-orders")
async def list_pos(
    status:    Optional[str] = Query(None),
    vendor_id: Optional[uuid.UUID] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to:   Optional[date] = Query(None),
    search:    Optional[str] = Query(None),
    limit:     int = Query(100),
    offset:    int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["po.tenant_id=:tid"]
        params: dict = {"tid":tid,"limit":limit,"offset":offset}
        if status:    conds.append("po.status=:st"); params["st"]=status
        if vendor_id: conds.append("po.vendor_id=:vid"); params["vid"]=str(vendor_id)
        if date_from: conds.append("po.po_date>=:df"); params["df"]=str(date_from)
        if date_to:   conds.append("po.po_date<=:dt"); params["dt"]=str(date_to)
        if search:    conds.append("(po.serial ILIKE :s OR p.name ILIKE :s)"); params["s"]=f"%{search}%"
        cnt = await db.execute(text(f"SELECT COUNT(*) FROM ap_purchase_orders po LEFT JOIN parties p ON p.id=po.vendor_id WHERE {' AND '.join(conds)}"), params)
        r = await db.execute(text(f"""
            SELECT po.id, po.serial, po.po_date::text, po.delivery_date::text,
                   po.total_amount, po.vat_amount, po.grand_total,
                   po.status, po.currency_code, po.pr_id, po.rfq_id,
                   po.payment_terms, po.notes,
                   p.name AS vendor_name, p.id AS vendor_id,
                   -- استلام %
                   COALESCE((SELECT SUM(rl.qty_received)/NULLIF(SUM(pl.qty),0)*100
                     FROM ap_po_lines pl JOIN ap_receipt_lines rl ON rl.po_line_id=pl.id
                     WHERE pl.po_id=po.id),0) AS received_pct,
                   -- فوترة %
                   COALESCE((SELECT SUM(il.qty)/NULLIF(SUM(pl.qty),0)*100
                     FROM ap_po_lines pl JOIN ap_invoice_lines il ON il.po_line_id=pl.id
                     WHERE pl.po_id=po.id),0) AS invoiced_pct
            FROM ap_purchase_orders po
            LEFT JOIN parties p ON p.id=po.vendor_id
            WHERE {" AND ".join(conds)}
            ORDER BY po.created_at DESC
            LIMIT :limit OFFSET :offset
        """), params)
        rows = [dict(r._mapping) for r in r.fetchall()]
        return ok(data={"total":cnt.scalar(),"items":rows})
    except Exception as e:
        import traceback; print(f"[ap/pos] {traceback.format_exc()}")
        return ok(data={"total":0,"items":[]}, message=str(e))


@router.post("/purchase-orders", status_code=201)
async def create_po(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    po_id = str(uuid.uuid4())
    po_date = date.fromisoformat(str(data.get("po_date", date.today())))
    serial  = await _next_serial(db, tid, "PO", po_date)
    try:
        # Calculate totals
        lines = data.get("lines",[])
        subtotal = sum(Decimal(str(l.get("qty",0)))*Decimal(str(l.get("unit_price",0))) for l in lines)
        vat_rate = Decimal(str(data.get("vat_rate",15)))/100
        vat_amt  = (subtotal * vat_rate).quantize(Decimal("0.01"))
        grand    = subtotal + vat_amt

        await db.execute(text("""
            INSERT INTO ap_purchase_orders
              (id,tenant_id,serial,po_date,delivery_date,vendor_id,
               pr_id,rfq_id,currency_code,total_amount,vat_amount,grand_total,
               payment_terms,delivery_address,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:pd,:dd,:vid,
               :pr_id,:rfq_id,:curr,:sub,:vat,:grand,
               :terms,:addr,:notes,'draft',:by)
        """), {
            "id":po_id,"tid":tid,"serial":serial,
            "pd":po_date,
            "dd":date.fromisoformat(str(data["delivery_date"])) if data.get("delivery_date") else None,
            "vid":data.get("vendor_id"),
            "pr_id":data.get("pr_id"),
            "rfq_id":data.get("rfq_id"),
            "curr":data.get("currency_code","SAR"),
            "sub":subtotal,"vat":vat_amt,"grand":grand,
            "terms":data.get("payment_terms",""),
            "addr":data.get("delivery_address",""),
            "notes":data.get("notes",""),
            "by":user.email,
        })
        for i,line in enumerate(lines):
            qty   = Decimal(str(line.get("qty",1)))
            price = Decimal(str(line.get("unit_price",0)))
            await db.execute(text("""
                INSERT INTO ap_po_lines
                  (id,tenant_id,po_id,line_no,item_id,description,
                   qty,uom,unit_price,discount_pct,line_total,
                   qty_received,qty_invoiced,notes)
                VALUES
                  (gen_random_uuid(),:tid,:po,:ln,:item,:desc,
                   :qty,:uom,:price,:disc,:total,
                   0,0,:notes)
            """), {
                "tid":tid,"po":po_id,"ln":i+1,
                "item":line.get("item_id"),
                "desc":line.get("description",""),
                "qty":qty,"uom":line.get("uom",""),
                "price":price,
                "disc":Decimal(str(line.get("discount_pct",0))),
                "total":qty*price,
                "notes":line.get("notes",""),
            })
        await db.commit()
        return created(data={"id":po_id,"serial":serial}, message=f"تم إنشاء أمر الشراء {serial} ✅")
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/po create] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


@router.get("/purchase-orders/{po_id}")
async def get_po(
    po_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT po.*, p.name AS vendor_name, p.tax_number AS vendor_tax
            FROM ap_purchase_orders po
            LEFT JOIN parties p ON p.id=po.vendor_id
            WHERE po.id=:id AND po.tenant_id=:tid
        """), {"id":str(po_id),"tid":tid})
        row = r.mappings().fetchone()
        if not row: raise HTTPException(404,"أمر الشراء غير موجود")
        result = dict(row)
        r2 = await db.execute(text("""
            SELECT l.*, i.item_name, i.item_code
            FROM ap_po_lines l
            LEFT JOIN inv_items i ON i.id=l.item_id
            WHERE l.po_id=:id ORDER BY l.line_no
        """), {"id":str(po_id)})
        result["lines"] = [dict(r._mapping) for r in r2.fetchall()]
        return ok(data=result)
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/purchase-orders/{po_id}/approve")
async def approve_po(
    po_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        await db.execute(text("""
            UPDATE ap_purchase_orders SET status='approved',
              approved_by=:by, approved_at=NOW()
            WHERE id=:id AND tenant_id=:tid AND status='draft'
        """), {"id":str(po_id),"tid":tid,"by":user.email})
        await db.commit()
        return ok(data={}, message="تم اعتماد أمر الشراء ✅")
    except Exception as e:
        await db.rollback(); raise HTTPException(400, str(e))


@router.post("/purchase-orders/{po_id}/cancel")
async def cancel_po(
    po_id: uuid.UUID,
    data: dict = {},
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        await db.execute(text("""
            UPDATE ap_purchase_orders SET status='cancelled',
              cancelled_by=:by, cancelled_at=NOW(), cancel_reason=:reason
            WHERE id=:id AND tenant_id=:tid AND status IN ('draft','approved')
        """), {"id":str(po_id),"tid":tid,"by":user.email,"reason":data.get("reason","")})
        await db.commit()
        return ok(data={}, message="تم إلغاء أمر الشراء")
    except Exception as e:
        await db.rollback(); raise HTTPException(400, str(e))


# ══════════════════════════════════════════════════════════
# GOODS RECEIPTS (GRN)
# ══════════════════════════════════════════════════════════
@router.get("/receipts")
async def list_receipts(
    status:    Optional[str] = Query(None),
    po_id:     Optional[uuid.UUID] = Query(None),
    limit:     int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["r.tenant_id=:tid"]
        params: dict = {"tid":tid,"limit":limit}
        if status: conds.append("r.status=:st"); params["st"]=status
        if po_id:  conds.append("r.po_id=:po"); params["po"]=str(po_id)
        r = await db.execute(text(f"""
            SELECT r.*, p.name AS vendor_name, po.serial AS po_serial
            FROM ap_receipts r
            LEFT JOIN ap_purchase_orders po ON po.id=r.po_id
            LEFT JOIN parties p ON p.id=po.vendor_id
            WHERE {" AND ".join(conds)}
            ORDER BY r.created_at DESC LIMIT :limit
        """), params)
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.post("/receipts", status_code=201)
async def create_receipt(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    rec_id  = str(uuid.uuid4())
    rec_date = date.fromisoformat(str(data.get("receipt_date", date.today())))
    serial  = await _next_serial(db, tid, "GRN", rec_date)
    try:
        await db.execute(text("""
            INSERT INTO ap_receipts
              (id,tenant_id,serial,receipt_date,po_id,warehouse_id,
               notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:rd,:po,:wh,
               :notes,'draft',:by)
        """), {
            "id":rec_id,"tid":tid,"serial":serial,
            "rd":rec_date,
            "po":data.get("po_id"),
            "wh":data.get("warehouse_id"),
            "notes":data.get("notes",""),
            "by":user.email,
        })
        for i,line in enumerate(data.get("lines",[])):
            qty_recv = Decimal(str(line.get("qty_received",0)))
            unit_cost = Decimal(str(line.get("unit_cost",0)))
            await db.execute(text("""
                INSERT INTO ap_receipt_lines
                  (id,tenant_id,receipt_id,po_line_id,line_no,
                   item_id,qty_received,unit_cost,total_cost,
                   lot_number,serial_number,notes)
                VALUES
                  (gen_random_uuid(),:tid,:rec,:pol,:ln,
                   :item,:qty,:cost,:total,
                   :lot,:ser,:notes)
            """), {
                "tid":tid,"rec":rec_id,
                "pol":line.get("po_line_id"),
                "ln":i+1,
                "item":line.get("item_id"),
                "qty":qty_recv,
                "cost":unit_cost,
                "total":qty_recv*unit_cost,
                "lot":line.get("lot_number",""),
                "ser":line.get("serial_number",""),
                "notes":line.get("notes",""),
            })
        await db.commit()
        return created(data={"id":rec_id,"serial":serial}, message=f"تم إنشاء إشعار الاستلام {serial} ✅")
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/grn create] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


@router.post("/receipts/{rec_id}/post")
async def post_receipt(
    rec_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """ترحيل GRN → تحديث المخزون + PO qty_received"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT r.*, po.vendor_id FROM ap_receipts r
            LEFT JOIN ap_purchase_orders po ON po.id=r.po_id
            WHERE r.id=:id AND r.tenant_id=:tid AND r.status='draft'
        """), {"id":str(rec_id),"tid":tid})
        rec = r.mappings().fetchone()
        if not rec: raise HTTPException(404,"إشعار الاستلام غير موجود أو مرحّل")

        # Get lines
        r2 = await db.execute(text("""
            SELECT * FROM ap_receipt_lines WHERE receipt_id=:id AND tenant_id=:tid
        """), {"id":str(rec_id),"tid":tid})
        lines = [dict(r._mapping) for r in r2.fetchall()]

        for line in lines:
            # Update PO line qty_received
            if line.get("po_line_id"):
                await db.execute(text("""
                    UPDATE ap_po_lines
                    SET qty_received = qty_received + :qty
                    WHERE id=:id AND tenant_id=:tid
                """), {"qty":Decimal(str(line["qty_received"])),"id":str(line["po_line_id"]),"tid":tid})

            # Update inventory balance
            if line.get("item_id") and rec.get("warehouse_id"):
                item_id = str(line["item_id"])
                wh_id   = str(rec["warehouse_id"])
                qty     = Decimal(str(line["qty_received"]))
                cost    = Decimal(str(line.get("unit_cost",0)))

                # Upsert balance
                r3 = await db.execute(text("""
                    SELECT qty_on_hand, avg_cost FROM inv_balances
                    WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
                """), {"tid":tid,"iid":item_id,"wid":wh_id})
                bal = r3.fetchone()
                if bal:
                    old_qty  = Decimal(str(bal[0] or 0))
                    old_cost = Decimal(str(bal[1] or 0))
                    new_qty  = old_qty + qty
                    new_wac  = ((old_qty*old_cost + qty*cost)/new_qty).quantize(Decimal("0.0001")) if new_qty>0 else cost
                    await db.execute(text("""
                        UPDATE inv_balances SET
                          qty_on_hand=:nq, qty_available=:nq,
                          avg_cost=:wac, total_value=:val,
                          last_movement_date=:dt, updated_at=NOW()
                        WHERE tenant_id=:tid AND item_id=:iid AND warehouse_id=:wid
                    """), {"nq":new_qty,"wac":new_wac,"val":new_qty*new_wac,"dt":rec["receipt_date"],"tid":tid,"iid":item_id,"wid":wh_id})
                else:
                    # Get item/warehouse names
                    ri = await db.execute(text("SELECT item_name,item_code FROM inv_items WHERE id=:id"),{"id":item_id})
                    ri_row = ri.fetchone()
                    rw = await db.execute(text("SELECT warehouse_name,warehouse_code FROM inv_warehouses WHERE id=:id"),{"id":wh_id})
                    rw_row = rw.fetchone()
                    await db.execute(text("""
                        INSERT INTO inv_balances
                          (id,tenant_id,item_id,warehouse_id,item_code,item_name,
                           warehouse_code,warehouse_name,
                           qty_on_hand,qty_available,qty_reserved,qty_incoming,
                           avg_cost,total_value,last_movement_date)
                        VALUES
                          (gen_random_uuid(),:tid,:iid,:wid,:icode,:iname,
                           :wcode,:wname,
                           :qty,:qty,0,0,:cost,:val,:dt)
                    """), {
                        "tid":tid,"iid":item_id,"wid":wh_id,
                        "icode":ri_row[1] if ri_row else "","iname":ri_row[0] if ri_row else "",
                        "wcode":rw_row[1] if rw_row else "","wname":rw_row[0] if rw_row else "",
                        "qty":qty,"cost":cost,"val":qty*cost,"dt":rec["receipt_date"],
                    })

        # Mark receipt as posted
        await db.execute(text("""
            UPDATE ap_receipts SET status='posted', posted_by=:by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"id":str(rec_id),"tid":tid,"by":user.email})

        # Check if PO fully received
        await db.execute(text("""
            UPDATE ap_purchase_orders SET status='received'
            WHERE id=:po AND tenant_id=:tid
              AND NOT EXISTS (
                SELECT 1 FROM ap_po_lines
                WHERE po_id=:po AND tenant_id=:tid AND qty_received < qty
              )
        """), {"po":str(rec["po_id"]),"tid":tid})

        await db.commit()
        return ok(data={"serial":rec["serial"]}, message=f"تم ترحيل GRN {rec['serial']} وتحديث المخزون ✅")
    except HTTPException: raise
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/grn post] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


# ══════════════════════════════════════════════════════════
# AP INVOICES — فواتير الموردين
# ══════════════════════════════════════════════════════════
@router.get("/invoices")
async def list_invoices(
    status:    Optional[str] = Query(None),
    vendor_id: Optional[uuid.UUID] = Query(None),
    limit:     int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["inv.tenant_id=:tid"]
        params: dict = {"tid":tid,"limit":limit}
        if status:    conds.append("inv.status=:st"); params["st"]=status
        if vendor_id: conds.append("inv.vendor_id=:vid"); params["vid"]=str(vendor_id)
        r = await db.execute(text(f"""
            SELECT inv.*, p.name AS vendor_name,
                   po.serial AS po_serial,
                   CASE
                     WHEN inv.status='posted' AND inv.remaining_amount>0 AND inv.due_date < CURRENT_DATE THEN 'overdue'
                     WHEN inv.status='posted' AND inv.remaining_amount>0 THEN 'pending'
                     WHEN inv.status='posted' AND inv.remaining_amount<=0 THEN 'paid'
                     ELSE inv.status
                   END AS payment_status,
                   (CURRENT_DATE - inv.due_date) AS days_overdue
            FROM ap_invoices inv
            LEFT JOIN parties p ON p.id=inv.vendor_id
            LEFT JOIN ap_purchase_orders po ON po.id=inv.po_id
            WHERE {" AND ".join(conds)}
            ORDER BY inv.invoice_date DESC LIMIT :limit
        """), params)
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.post("/invoices", status_code=201)
async def create_invoice(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    inv_id = str(uuid.uuid4())
    inv_date = date.fromisoformat(str(data.get("invoice_date", date.today())))
    serial   = await _next_serial(db, tid, "APINV", inv_date)
    try:
        lines    = data.get("lines",[])
        subtotal = sum(Decimal(str(l.get("qty",0)))*Decimal(str(l.get("unit_price",0))) for l in lines)
        vat_amt  = Decimal(str(data.get("vat_amount",0)))
        grand    = subtotal + vat_amt

        # Payment terms → due date
        pt = int(data.get("payment_terms_days",30))
        due = date.fromisoformat(str(data.get("due_date","") or "")) if data.get("due_date") else \
              date.fromisoformat(str(inv_date.isoformat()))
        if not data.get("due_date"):
            from datetime import timedelta
            due = inv_date + timedelta(days=pt)

        await db.execute(text("""
            INSERT INTO ap_invoices
              (id,tenant_id,serial,vendor_ref,invoice_date,due_date,
               vendor_id,po_id,
               total_amount,vat_amount,grand_total,remaining_amount,
               currency_code,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:vref,:id_,:due,
               :vid,:po,
               :sub,:vat,:grand,:grand,
               :curr,:notes,'draft',:by)
        """), {
            "id":inv_id,"tid":tid,"serial":serial,
            "vref":data.get("vendor_ref",""),
            "id_":inv_date,"due":due,
            "vid":data.get("vendor_id"),
            "po":data.get("po_id"),
            "sub":subtotal,"vat":vat_amt,"grand":grand,
            "curr":data.get("currency_code","SAR"),
            "notes":data.get("notes",""),
            "by":user.email,
        })
        for i,line in enumerate(lines):
            qty   = Decimal(str(line.get("qty",1)))
            price = Decimal(str(line.get("unit_price",0)))
            await db.execute(text("""
                INSERT INTO ap_invoice_lines
                  (id,tenant_id,invoice_id,po_line_id,line_no,
                   item_id,description,qty,unit_price,line_total,vat_amount)
                VALUES
                  (gen_random_uuid(),:tid,:inv,:pol,:ln,
                   :item,:desc,:qty,:price,:total,:vat)
            """), {
                "tid":tid,"inv":inv_id,
                "pol":line.get("po_line_id"),
                "ln":i+1,
                "item":line.get("item_id"),
                "desc":line.get("description",""),
                "qty":qty,"price":price,
                "total":qty*price,
                "vat":Decimal(str(line.get("vat_amount",0))),
            })
        await db.commit()
        return created(data={"id":inv_id,"serial":serial}, message=f"تم إنشاء الفاتورة {serial} ✅")
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/invoice create] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


@router.post("/invoices/{inv_id}/post")
async def post_invoice(
    inv_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """ترحيل الفاتورة → قيد محاسبي + تحديث الذمم"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT inv.*, p.name AS vendor_name
            FROM ap_invoices inv
            LEFT JOIN parties p ON p.id=inv.vendor_id
            WHERE inv.id=:id AND inv.tenant_id=:tid AND inv.status='draft'
        """), {"id":str(inv_id),"tid":tid})
        inv = r.mappings().fetchone()
        if not inv: raise HTTPException(404,"الفاتورة غير موجودة أو مرحّلة")

        # Create JE
        je_id = str(uuid.uuid4())
        tx_date = date.fromisoformat(str(inv["invoice_date"])[:10]) if inv["invoice_date"] else date.today()
        je_serial = await _next_serial(db, tid, "APINV", tx_date)

        await db.execute(text("""
            INSERT INTO journal_entries
              (id,tenant_id,serial,entry_date,je_type,description,
               total_debit,total_credit,status,created_by,posted_at)
            VALUES
              (:id,:tid,:serial,:dt,'APINV',:desc,
               :amt,:amt,'posted',:by,NOW())
        """), {
            "id":je_id,"tid":tid,"serial":je_serial,
            "dt":tx_date,
            "desc":f"فاتورة مورد {inv['serial']} — {inv['vendor_name']}",
            "amt":Decimal(str(inv["grand_total"])),
            "by":user.email,
        })

        # DR: مخزون/مصروف, CR: ذمم دائنة
        await db.execute(text("""
            INSERT INTO je_lines (id,tenant_id,je_id,line_no,account_code,description,debit,credit)
            VALUES
              (gen_random_uuid(),:tid,:je,1,'1300',:desc1,:amt,0),
              (gen_random_uuid(),:tid,:je,2,'2100',:desc2,0,:amt)
        """), {
            "tid":tid,"je":je_id,
            "desc1":f"مشتريات — {inv['vendor_name']}",
            "desc2":f"ذمة دائنة — {inv['vendor_name']}",
            "amt":Decimal(str(inv["grand_total"])),
        })

        # Update invoice status
        await db.execute(text("""
            UPDATE ap_invoices SET status='posted', je_serial=:je_s, posted_by=:by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"id":str(inv_id),"tid":tid,"je_s":je_serial,"by":user.email})

        await db.commit()
        return ok(data={"je_serial":je_serial}, message=f"تم ترحيل الفاتورة — القيد: {je_serial} ✅")
    except HTTPException: raise
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/invoice post] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


@router.get("/invoices/{inv_id}")
async def get_invoice(
    inv_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT inv.*, p.name AS vendor_name, po.serial AS po_serial
            FROM ap_invoices inv
            LEFT JOIN parties p ON p.id=inv.vendor_id
            LEFT JOIN ap_purchase_orders po ON po.id=inv.po_id
            WHERE inv.id=:id AND inv.tenant_id=:tid
        """), {"id":str(inv_id),"tid":tid})
        row = r.mappings().fetchone()
        if not row: raise HTTPException(404,"الفاتورة غير موجودة")
        result = dict(row)
        r2 = await db.execute(text("""
            SELECT l.*, i.item_name FROM ap_invoice_lines l
            LEFT JOIN inv_items i ON i.id=l.item_id
            WHERE l.invoice_id=:id ORDER BY l.line_no
        """), {"id":str(inv_id)})
        result["lines"] = [dict(r._mapping) for r in r2.fetchall()]
        return ok(data=result)
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, str(e))


# ══════════════════════════════════════════════════════════
# VENDOR PAYMENTS — دفعات الموردين
# ══════════════════════════════════════════════════════════
@router.get("/payments")
async def list_payments(
    vendor_id: Optional[uuid.UUID] = Query(None),
    status:    Optional[str] = Query(None),
    limit:     int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["pay.tenant_id=:tid"]
        params: dict = {"tid":tid,"limit":limit}
        if vendor_id: conds.append("pay.vendor_id=:vid"); params["vid"]=str(vendor_id)
        if status:    conds.append("pay.status=:st"); params["st"]=status
        r = await db.execute(text(f"""
            SELECT pay.*, p.name AS vendor_name, inv.serial AS invoice_serial
            FROM ap_payments pay
            LEFT JOIN parties p ON p.id=pay.vendor_id
            LEFT JOIN ap_invoices inv ON inv.id=pay.invoice_id
            WHERE {" AND ".join(conds)}
            ORDER BY pay.payment_date DESC LIMIT :limit
        """), params)
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.post("/payments", status_code=201)
async def create_payment(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    pay_id   = str(uuid.uuid4())
    pay_date = date.fromisoformat(str(data.get("payment_date", date.today())))
    serial   = await _next_serial(db, tid, "APPAY", pay_date)
    try:
        amount = Decimal(str(data.get("amount",0)))
        await db.execute(text("""
            INSERT INTO ap_payments
              (id,tenant_id,serial,payment_date,vendor_id,invoice_id,
               amount,payment_method,bank_account_id,reference,notes,
               status,created_by)
            VALUES
              (:id,:tid,:serial,:pd,:vid,:inv_id,
               :amt,:method,:bank,:ref,:notes,
               'draft',:by)
        """), {
            "id":pay_id,"tid":tid,"serial":serial,
            "pd":pay_date,
            "vid":data.get("vendor_id"),
            "inv_id":data.get("invoice_id"),
            "amt":amount,
            "method":data.get("payment_method","bank"),
            "bank":data.get("bank_account_id"),
            "ref":data.get("reference",""),
            "notes":data.get("notes",""),
            "by":user.email,
        })
        await db.commit()
        return created(data={"id":pay_id,"serial":serial}, message=f"تم إنشاء الدفعة {serial} ✅")
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/payment create] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


@router.post("/payments/{pay_id}/post")
async def post_payment(
    pay_id: uuid.UUID,
    invoice_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """ترحيل الدفعة → قيد + تسوية الفاتورة"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT pay.*, p.name AS vendor_name
            FROM ap_payments pay
            LEFT JOIN parties p ON p.id=pay.vendor_id
            WHERE pay.id=:id AND pay.tenant_id=:tid AND pay.status='draft'
        """), {"id":str(pay_id),"tid":tid})
        pay = r.mappings().fetchone()
        if not pay: raise HTTPException(404,"الدفعة غير موجودة")

        amount = Decimal(str(pay["amount"]))
        je_id  = str(uuid.uuid4())
        je_serial = await _next_serial(db, tid, "APPAY", date.fromisoformat(str(pay["payment_date"])[:10]))

        # Create JE: DR ذمم دائنة / CR البنك
        await db.execute(text("""
            INSERT INTO journal_entries
              (id,tenant_id,serial,entry_date,je_type,description,
               total_debit,total_credit,status,created_by,posted_at)
            VALUES (:id,:tid,:serial,:dt,'APPAY',:desc,:amt,:amt,'posted',:by,NOW())
        """), {
            "id":je_id,"tid":tid,"serial":je_serial,
            "dt":date.fromisoformat(str(pay["payment_date"])[:10]),
            "desc":f"دفعة مورد {pay['serial']} — {pay['vendor_name']}",
            "amt":amount,"by":user.email,
        })
        await db.execute(text("""
            INSERT INTO je_lines (id,tenant_id,je_id,line_no,account_code,description,debit,credit)
            VALUES
              (gen_random_uuid(),:tid,:je,1,'2100',:d1,:amt,0),
              (gen_random_uuid(),:tid,:je,2,'1100',:d2,0,:amt)
        """), {"tid":tid,"je":je_id,
               "d1":f"سداد ذمة — {pay['vendor_name']}",
               "d2":f"دفعة مورد — {pay['vendor_name']}","amt":amount})

        # Update invoice remaining
        target_inv = pay.get("invoice_id") or invoice_id
        if target_inv:
            await db.execute(text("""
                UPDATE ap_invoices
                SET remaining_amount = GREATEST(0, remaining_amount - :amt),
                    paid_amount = COALESCE(paid_amount,0) + :amt
                WHERE id=:id AND tenant_id=:tid
            """), {"amt":amount,"id":str(target_inv),"tid":tid})

        # Post payment
        await db.execute(text("""
            UPDATE ap_payments SET status='posted', je_serial=:je_s, posted_by=:by, posted_at=NOW()
            WHERE id=:id AND tenant_id=:tid
        """), {"id":str(pay_id),"tid":tid,"je_s":je_serial,"by":user.email})

        await db.commit()
        return ok(data={"je_serial":je_serial}, message=f"تم ترحيل الدفعة — {je_serial} ✅")
    except HTTPException: raise
    except Exception as e:
        await db.rollback()
        import traceback; print(f"[ap/payment post] {traceback.format_exc()}")
        raise HTTPException(400, str(e))


# ══════════════════════════════════════════════════════════
# PURCHASE RETURNS — مردودات المشتريات
# ══════════════════════════════════════════════════════════
@router.get("/returns")
async def list_returns(
    limit: int = Query(100),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT ret.*, p.name AS vendor_name, po.serial AS po_serial
            FROM ap_purchase_returns ret
            LEFT JOIN ap_purchase_orders po ON po.id=ret.po_id
            LEFT JOIN parties p ON p.id=ret.vendor_id
            WHERE ret.tenant_id=:tid
            ORDER BY ret.created_at DESC LIMIT :limit
        """), {"tid":tid,"limit":limit})
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.post("/returns", status_code=201)
async def create_return(
    data: dict,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    ret_id   = str(uuid.uuid4())
    ret_date = date.fromisoformat(str(data.get("return_date", date.today())))
    serial   = await _next_serial(db, tid, "PURRET", ret_date)
    try:
        total = sum(Decimal(str(l.get("qty",0)))*Decimal(str(l.get("unit_cost",0))) for l in data.get("lines",[]))
        await db.execute(text("""
            INSERT INTO ap_purchase_returns
              (id,tenant_id,serial,return_date,vendor_id,po_id,
               total_amount,reason,notes,status,created_by)
            VALUES
              (:id,:tid,:serial,:rd,:vid,:po,
               :amt,:reason,:notes,'draft',:by)
        """), {
            "id":ret_id,"tid":tid,"serial":serial,"rd":ret_date,
            "vid":data.get("vendor_id"),"po":data.get("po_id"),
            "amt":total,"reason":data.get("reason",""),
            "notes":data.get("notes",""),"by":user.email,
        })
        for i,line in enumerate(data.get("lines",[])):
            await db.execute(text("""
                INSERT INTO ap_return_lines
                  (id,tenant_id,return_id,line_no,item_id,description,qty,unit_cost,total_cost)
                VALUES (gen_random_uuid(),:tid,:ret,:ln,:item,:desc,:qty,:cost,:total)
            """), {
                "tid":tid,"ret":ret_id,"ln":i+1,
                "item":line.get("item_id"),"desc":line.get("description",""),
                "qty":Decimal(str(line.get("qty",1))),
                "cost":Decimal(str(line.get("unit_cost",0))),
                "total":Decimal(str(line.get("qty",1)))*Decimal(str(line.get("unit_cost",0))),
            })
        await db.commit()
        return created(data={"id":ret_id,"serial":serial}, message=f"تم إنشاء مرتجع الشراء {serial} ✅")
    except Exception as e:
        await db.rollback(); raise HTTPException(400, str(e))


# ══════════════════════════════════════════════════════════
# 3-WAY MATCHING
# ══════════════════════════════════════════════════════════
@router.get("/invoices/{inv_id}/three-way-match")
async def three_way_match(
    inv_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """مقارنة PO vs GRN vs Invoice"""
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT inv.*, po.serial AS po_serial, po.total_amount AS po_amount,
                   p.name AS vendor_name
            FROM ap_invoices inv
            LEFT JOIN ap_purchase_orders po ON po.id=inv.po_id
            LEFT JOIN parties p ON p.id=inv.vendor_id
            WHERE inv.id=:id AND inv.tenant_id=:tid
        """), {"id":str(inv_id),"tid":tid})
        inv = r.mappings().fetchone()
        if not inv: raise HTTPException(404,"الفاتورة غير موجودة")

        # Get PO lines
        r2 = await db.execute(text("""
            SELECT pl.*, i.item_name
            FROM ap_po_lines pl
            LEFT JOIN inv_items i ON i.id=pl.item_id
            WHERE pl.po_id=:po ORDER BY pl.line_no
        """), {"po":str(inv["po_id"]) if inv["po_id"] else "00000000-0000-0000-0000-000000000000"})
        po_lines = [dict(r._mapping) for r in r2.fetchall()]

        # Get GRN totals
        r3 = await db.execute(text("""
            SELECT pl.id AS po_line_id,
                   COALESCE(SUM(rl.qty_received),0) AS total_received
            FROM ap_po_lines pl
            LEFT JOIN ap_receipt_lines rl ON rl.po_line_id=pl.id
            WHERE pl.po_id=:po
            GROUP BY pl.id
        """), {"po":str(inv["po_id"]) if inv["po_id"] else ""})
        received_map = {str(r._mapping["po_line_id"]): float(r._mapping["total_received"]) for r in r3.fetchall()}

        # Get Invoice lines
        r4 = await db.execute(text("""
            SELECT * FROM ap_invoice_lines WHERE invoice_id=:id ORDER BY line_no
        """), {"id":str(inv_id)})
        inv_lines = [dict(r._mapping) for r in r4.fetchall()]
        invoiced_map = {str(l.get("po_line_id","")): float(l.get("qty",0)) for l in inv_lines}

        # Build comparison
        comparison = []
        for pl in po_lines:
            pid = str(pl["id"])
            po_qty  = float(pl.get("qty",0))
            rec_qty = received_map.get(pid, 0)
            inv_qty = invoiced_map.get(pid, 0)
            comparison.append({
                "item_name":   pl.get("item_name",""),
                "po_qty":      po_qty,
                "po_price":    float(pl.get("unit_price",0)),
                "received_qty":rec_qty,
                "invoiced_qty":inv_qty,
                "match_status": "✅ مطابق" if abs(rec_qty-inv_qty)<0.001 and abs(po_qty-rec_qty)<0.001
                                else "⚠️ فارق في الكمية" if abs(rec_qty-inv_qty)>0.001
                                else "❌ لم يُستلم",
            })

        # Overall match
        po_total  = float(inv.get("po_amount") or 0)
        inv_total = float(inv.get("grand_total") or 0)
        overall   = "✅ مطابق 100%" if abs(po_total-inv_total)<0.01 else f"⚠️ فارق: {abs(po_total-inv_total):.2f}"

        return ok(data={
            "invoice":    dict(inv),
            "comparison": comparison,
            "overall_match": overall,
            "po_total":   po_total,
            "inv_total":  inv_total,
            "variance":   abs(po_total-inv_total),
        })
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ══════════════════════════════════════════════════════════
# REPORTS
# ══════════════════════════════════════════════════════════
@router.get("/reports/aging")
async def ap_aging(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT p.name AS vendor_name,
                   COALESCE(SUM(CASE WHEN CURRENT_DATE-inv.due_date<=0 THEN inv.remaining_amount ELSE 0 END),0) AS current_amt,
                   COALESCE(SUM(CASE WHEN CURRENT_DATE-inv.due_date BETWEEN 1 AND 30 THEN inv.remaining_amount ELSE 0 END),0) AS days_1_30,
                   COALESCE(SUM(CASE WHEN CURRENT_DATE-inv.due_date BETWEEN 31 AND 60 THEN inv.remaining_amount ELSE 0 END),0) AS days_31_60,
                   COALESCE(SUM(CASE WHEN CURRENT_DATE-inv.due_date BETWEEN 61 AND 90 THEN inv.remaining_amount ELSE 0 END),0) AS days_61_90,
                   COALESCE(SUM(CASE WHEN CURRENT_DATE-inv.due_date > 90 THEN inv.remaining_amount ELSE 0 END),0) AS days_over_90,
                   COALESCE(SUM(inv.remaining_amount),0) AS total_balance
            FROM ap_invoices inv
            JOIN parties p ON p.id=inv.vendor_id
            WHERE inv.tenant_id=:tid AND inv.status='posted' AND inv.remaining_amount>0
            GROUP BY p.id, p.name
            ORDER BY total_balance DESC
        """), {"tid": tid})
        rows = [dict(r._mapping) for r in r.fetchall()]
        grand = {
            "current": sum(float(r.get("current_amt",0)) for r in rows),
            "1_30":    sum(float(r.get("days_1_30",0)) for r in rows),
            "31_60":   sum(float(r.get("days_31_60",0)) for r in rows),
            "61_90":   sum(float(r.get("days_61_90",0)) for r in rows),
            "over_90": sum(float(r.get("days_over_90",0)) for r in rows),
            "total":   sum(float(r.get("total_balance",0)) for r in rows),
        }
        return ok(data={"vendors": rows, "summary": grand})
    except Exception as e:
        return ok(data={"vendors":[],"summary":{}}, message=str(e))


@router.get("/reports/purchase-summary")
async def purchase_summary(
    date_from: Optional[date] = Query(None),
    date_to:   Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        conds = ["po.tenant_id=:tid","po.status NOT IN ('cancelled','draft')"]
        params: dict = {"tid":tid}
        if date_from: conds.append("po.po_date>=:df"); params["df"]=str(date_from)
        if date_to:   conds.append("po.po_date<=:dt"); params["dt"]=str(date_to)
        # By vendor
        r = await db.execute(text(f"""
            SELECT p.name AS vendor_name, COUNT(po.id) AS po_count,
                   SUM(po.grand_total) AS total_spend,
                   AVG(po.grand_total) AS avg_po_value
            FROM ap_purchase_orders po
            JOIN parties p ON p.id=po.vendor_id
            WHERE {" AND ".join(conds)}
            GROUP BY p.id, p.name
            ORDER BY total_spend DESC LIMIT 20
        """), params)
        by_vendor = [dict(r._mapping) for r in r.fetchall()]
        # By month
        r2 = await db.execute(text(f"""
            SELECT TO_CHAR(po.po_date,'YYYY-MM') AS month,
                   COUNT(po.id) AS po_count,
                   SUM(po.grand_total) AS total_spend
            FROM ap_purchase_orders po
            WHERE {" AND ".join(conds)}
            GROUP BY TO_CHAR(po.po_date,'YYYY-MM')
            ORDER BY month
        """), params)
        by_month = [dict(r._mapping) for r in r2.fetchall()]
        return ok(data={"by_vendor": by_vendor, "by_month": by_month})
    except Exception as e:
        return ok(data={"by_vendor":[],"by_month":[]}, message=str(e))


@router.get("/reports/vendor-performance")
async def vendor_performance(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT p.name AS vendor_name,
                   COUNT(DISTINCT po.id) AS total_pos,
                   COUNT(DISTINCT CASE WHEN po.status='received' THEN po.id END) AS completed_pos,
                   COALESCE(SUM(po.grand_total),0) AS total_spend,
                   COALESCE(AVG(CASE WHEN po.delivery_date IS NOT NULL
                     THEN EXTRACT(DAY FROM (po.delivery_date - po.po_date)) END),0) AS avg_lead_days,
                   COALESCE(COUNT(CASE WHEN inv.remaining_amount>0 AND inv.due_date<CURRENT_DATE THEN 1 END),0) AS overdue_invoices
            FROM ap_purchase_orders po
            JOIN parties p ON p.id=po.vendor_id
            LEFT JOIN ap_invoices inv ON inv.vendor_id=p.id AND inv.tenant_id=:tid
            WHERE po.tenant_id=:tid
            GROUP BY p.id, p.name
            ORDER BY total_spend DESC
        """), {"tid": tid})
        rows = [{
            **dict(r._mapping),
            "on_time_rate": round(float(r._mapping.get("completed_pos",0))/max(float(r._mapping.get("total_pos",1)),1)*100,1),
        } for r in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/pending-delivery")
async def pending_delivery(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        r = await db.execute(text("""
            SELECT po.serial, po.po_date::text, po.delivery_date::text,
                   po.grand_total, po.status,
                   p.name AS vendor_name,
                   (po.delivery_date - CURRENT_DATE) AS days_remaining
            FROM ap_purchase_orders po
            LEFT JOIN parties p ON p.id=po.vendor_id
            WHERE po.tenant_id=:tid
              AND po.status IN ('approved','partially_received')
            ORDER BY po.delivery_date ASC NULLS LAST
        """), {"tid":tid})
        return ok(data=[dict(r._mapping) for r in r.fetchall()])
    except Exception as e:
        return ok(data=[], message=str(e))


@router.get("/reports/vendor-statement/{vendor_id}")
async def vendor_statement(
    vendor_id: uuid.UUID,
    date_from: Optional[date] = Query(None),
    date_to:   Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    tid = str(user.tenant_id)
    try:
        # Invoices
        r = await db.execute(text("""
            SELECT 'فاتورة' AS type, serial AS ref, invoice_date::text AS tx_date,
                   grand_total AS debit, 0 AS credit, remaining_amount AS balance, status
            FROM ap_invoices
            WHERE tenant_id=:tid AND vendor_id=:vid AND status='posted'
            UNION ALL
            SELECT 'دفعة', serial, payment_date::text, 0, amount, 0, status
            FROM ap_payments
            WHERE tenant_id=:tid AND vendor_id=:vid AND status='posted'
            ORDER BY tx_date
        """), {"tid":tid,"vid":str(vendor_id)})
        rows = [dict(r._mapping) for r in r.fetchall()]
        return ok(data=rows)
    except Exception as e:
        return ok(data=[], message=str(e))

