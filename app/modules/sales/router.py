"""
app/modules/sales/router.py
══════════════════════════════════════════════════════════
Sales Module API — 20 endpoints

Customers:
  POST   /sales/customers              إنشاء عميل
  GET    /sales/customers              قائمة العملاء
  GET    /sales/customers/search       بحث عن عميل
  GET    /sales/customers/{id}         تفاصيل عميل
  PUT    /sales/customers/{id}         تعديل عميل

Invoices:
  POST   /sales/invoices               إنشاء فاتورة (draft)
  GET    /sales/invoices               قائمة الفواتير
  GET    /sales/invoices/{id}          تفاصيل فاتورة
  POST   /sales/invoices/{id}/post     ترحيل فاتورة
  POST   /sales/invoices/{id}/cancel   إلغاء فاتورة (draft فقط)

Returns:
  POST   /sales/returns                إنشاء مرتجع (draft)
  GET    /sales/returns                قائمة المرتجعات
  GET    /sales/returns/{id}           تفاصيل مرتجع
  POST   /sales/returns/{id}/post      ترحيل مرتجع
  GET    /sales/invoices/{id}/returns  مرتجعات فاتورة

Reports:
  GET    /sales/dashboard              KPIs المبيعات
  GET    /sales/ar-aging               تقادم الذمم المدينة
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import created, ok, paginated
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.sales.schemas import (
    CustomerCreate, CustomerUpdate,
    SalesInvoiceCreate, SalesReturnCreate,
)
from app.modules.sales.service import SalesService

router = APIRouter(prefix="/sales", tags=["المبيعات"])


def _svc(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
) -> SalesService:
    return SalesService(db, user)


# ══════════════════════════════════════════════════════════
# Customers
# ══════════════════════════════════════════════════════════
@router.post("/customers", status_code=201, summary="إنشاء عميل جديد")
async def create_customer(
    data: CustomerCreate,
    svc: SalesService = Depends(_svc),
):
    c = await svc.create_customer(data)
    return created(data=c.to_dict(), message=f"تم إنشاء العميل {data.code}")


@router.get("/customers", summary="قائمة العملاء")
async def list_customers(
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    svc: SalesService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc.list_customers(offset=offset, limit=page_size)
    return paginated(
        items=[c.to_dict() for c in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/customers/search", summary="بحث عن عميل")
async def search_customers(
    q: str = Query(..., min_length=1),
    svc: SalesService = Depends(_svc),
):
    results = await svc.search_customers(q)
    return ok(data=[c.to_dict() for c in results])


@router.get("/customers/{customer_id}", summary="تفاصيل عميل")
async def get_customer(
    customer_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    c = await svc.get_customer(customer_id)
    return ok(data=c.to_dict())


@router.put("/customers/{customer_id}", summary="تعديل عميل")
async def update_customer(
    customer_id: uuid.UUID,
    data: CustomerUpdate,
    svc: SalesService = Depends(_svc),
):
    c = await svc.update_customer(customer_id, data)
    return ok(data=c.to_dict(), message="تم التعديل")


# ══════════════════════════════════════════════════════════
# Invoices
# ══════════════════════════════════════════════════════════
@router.post("/invoices", status_code=201, summary="إنشاء فاتورة مبيعات")
async def create_invoice(
    data: SalesInvoiceCreate,
    svc: SalesService = Depends(_svc),
):
    """
    ينشئ فاتورة في حالة **draft**.
    لا يتم أي قيد محاسبي أو حركة مخزون حتى يتم الترحيل.
    """
    inv = await svc.create_invoice(data)
    return created(
        data=inv.to_dict(),
        message=f"تم إنشاء الفاتورة {inv.invoice_number} — الإجمالي: {float(inv.total_amount):,.3f} ر.س",
    )


@router.get("/invoices", summary="قائمة فواتير المبيعات")
async def list_invoices(
    customer_id: Optional[uuid.UUID] = Query(None),
    status:      Optional[str]       = Query(None),
    date_from:   Optional[date]      = Query(None),
    date_to:     Optional[date]      = Query(None),
    page:        int = Query(1, ge=1),
    page_size:   int = Query(20, ge=1, le=100),
    svc: SalesService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc.list_invoices(
        customer_id=customer_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        offset=offset,
        limit=page_size,
    )
    return paginated(
        items=[i.to_dict() for i in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/invoices/{invoice_id}", summary="تفاصيل فاتورة مبيعات")
async def get_invoice(
    invoice_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    inv = await svc.get_invoice(invoice_id)
    return ok(data=inv.to_dict())


@router.post("/invoices/{invoice_id}/post", summary="ترحيل فاتورة المبيعات")
async def post_invoice(
    invoice_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    """
    يرحّل الفاتورة:
    - يُخرج المخزون (SALES_ISSUE) لكل صنف قابل للتتبع
    - يحسب تكلفة البضاعة المباعة (WAC)
    - ينشئ قيد محاسبي:
      - DR ذمم عملاء
      - CR إيرادات مبيعات
      - CR ضريبة القيمة المضافة
    - DR تكلفة المبيعات / CR مخزون (قيد منفصل من InventoryService)
    """
    inv = await svc.post_invoice(invoice_id)
    return ok(
        data=inv.to_dict(),
        message=(
            f"✅ تم ترحيل الفاتورة {inv.invoice_number} | "
            f"قيد: {inv.je_serial} | "
            f"هامش الربح: {float(inv.gross_profit):,.3f} ر.س"
        ),
    )


@router.post("/invoices/{invoice_id}/cancel", summary="إلغاء فاتورة (draft فقط)")
async def cancel_invoice(
    invoice_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    """يلغي الفاتورة إذا كانت لا تزال في حالة draft."""
    from app.modules.sales.models import InvoiceStatus
    inv = await svc.get_invoice(invoice_id)
    if inv.status != InvoiceStatus.DRAFT:
        from app.core.exceptions import InvalidStateError
        raise InvalidStateError("الفاتورة", inv.status, [InvoiceStatus.DRAFT])
    inv.status = InvoiceStatus.CANCELLED
    inv.updated_by = svc.user.email
    await svc.db.flush()
    return ok(data=inv.to_dict(), message=f"تم إلغاء الفاتورة {inv.invoice_number}")


# ══════════════════════════════════════════════════════════
# Sales Returns
# ══════════════════════════════════════════════════════════
@router.post("/returns", status_code=201, summary="إنشاء مرتجع مبيعات")
async def create_return(
    data: SalesReturnCreate,
    svc: SalesService = Depends(_svc),
):
    """ينشئ مرتجع في حالة draft مرتبط بفاتورة مرحّلة."""
    ret = await svc.create_return(data)
    return created(
        data=ret.to_dict(),
        message=f"تم إنشاء المرتجع {ret.return_number} للفاتورة {ret.invoice_number}",
    )


@router.post("/returns/{return_id}/post", summary="ترحيل مرتجع المبيعات")
async def post_return(
    return_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    """
    يرحّل المرتجع:
    - يُعيد المخزون (SALES_RETURN)
    - ينشئ قيد عكسي:
      - DR إيرادات مبيعات
      - DR ضريبة القيمة المضافة
      - CR ذمم عملاء
      - DR مخزون / CR تكلفة المبيعات (لاسترداد التكلفة)
    """
    ret = await svc.post_return(return_id)
    return ok(
        data=ret.to_dict(),
        message=f"✅ تم ترحيل المرتجع {ret.return_number} | قيد: {ret.je_serial}",
    )


@router.get("/returns", summary="قائمة مرتجعات المبيعات")
async def list_returns(
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    svc: SalesService = Depends(_svc),
):
    items, total = await svc._return_repo.list(
        order_by=None, offset=(page-1)*page_size, limit=page_size
    )
    return paginated(
        items=[r.to_dict() for r in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/returns/{return_id}", summary="تفاصيل مرتجع")
async def get_return(
    return_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    ret = await svc._return_repo.get_with_lines(return_id)
    if not ret:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("مرتجع", return_id)
    return ok(data=ret.to_dict())


@router.get("/invoices/{invoice_id}/returns", summary="مرتجعات فاتورة")
async def invoice_returns(
    invoice_id: uuid.UUID,
    svc: SalesService = Depends(_svc),
):
    returns = await svc._return_repo.list_by_invoice(invoice_id)
    return ok(data=[r.to_dict() for r in returns])


# ══════════════════════════════════════════════════════════
# Dashboard & Reports
# ══════════════════════════════════════════════════════════
@router.get("/dashboard", summary="لوحة تحكم المبيعات")
async def sales_dashboard(
    date_from: date = Query(...),
    date_to:   date = Query(...),
    svc: SalesService = Depends(_svc),
):
    """
    KPIs المبيعات للفترة المحددة:
    - إجمالي الفواتير
    - الإيرادات + الضريبة + التكلفة
    - هامش الربح الإجمالي
    - الذمم المدينة المستحقة
    """
    data = await svc.get_dashboard(date_from, date_to)
    return ok(data=data)


@router.get("/ar-aging", summary="تقادم الذمم المدينة")
async def ar_aging(svc: SalesService = Depends(_svc)):
    """
    تقرير تقادم الذمم المدينة — يصنّف الفواتير المستحقة حسب:
    0-30 يوم | 31-60 | 61-90 | +90 يوم
    """
    from sqlalchemy import select, func
    from app.modules.sales.models import SalesInvoice, InvoiceStatus
    from datetime import date as date_type

    today = date_type.today()
    db = svc.db
    tid = svc.user.tenant_id

    result = await db.execute(
        select(SalesInvoice)
        .where(SalesInvoice.tenant_id == tid)
        .where(SalesInvoice.status.in_(["posted", "partially_returned"]))
        .where(SalesInvoice.balance_due > 0)
        .order_by(SalesInvoice.due_date)
    )
    invoices = result.scalars().all()

    buckets = {"0_30": 0.0, "31_60": 0.0, "61_90": 0.0, "over_90": 0.0}
    lines = []
    for inv in invoices:
        due = inv.due_date
        if due:
            days_overdue = (today - due).days if hasattr(due, 'days') else 0
        else:
            days_overdue = 0

        balance = float(inv.balance_due)
        if days_overdue <= 30:
            buckets["0_30"] += balance
        elif days_overdue <= 60:
            buckets["31_60"] += balance
        elif days_overdue <= 90:
            buckets["61_90"] += balance
        else:
            buckets["over_90"] += balance

        lines.append({
            "invoice_number": inv.invoice_number,
            "customer_name":  inv.customer_name,
            "invoice_date":   str(inv.invoice_date),
            "due_date":       str(inv.due_date) if inv.due_date else None,
            "days_overdue":   days_overdue,
            "balance_due":    balance,
        })

    return ok(data={
        "summary":       buckets,
        "total_ar":      sum(buckets.values()),
        "invoice_count": len(invoices),
        "lines":         lines,
    })
