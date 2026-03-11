"""
app/modules/purchases/router.py
══════════════════════════════════════════════════════════
Purchases Module API — 20 endpoints

Suppliers:
  POST   /purchases/suppliers              إنشاء مورد
  GET    /purchases/suppliers              قائمة الموردين
  GET    /purchases/suppliers/search       بحث عن مورد
  GET    /purchases/suppliers/{id}         تفاصيل مورد
  PUT    /purchases/suppliers/{id}         تعديل مورد

Purchase Orders:
  POST   /purchases/orders                 إنشاء PO (draft)
  GET    /purchases/orders                 قائمة POs
  GET    /purchases/orders/{id}            تفاصيل PO
  POST   /purchases/orders/{id}/approve    اعتماد PO

GRN:
  POST   /purchases/grn                    إنشاء GRN (draft)
  GET    /purchases/grn                    قائمة GRNs
  GET    /purchases/grn/{id}               تفاصيل GRN
  POST   /purchases/grn/{id}/post          ترحيل GRN → مخزون + قيد
  GET    /purchases/orders/{id}/grns       GRNs لأمر شراء

Vendor Invoices:
  POST   /purchases/vendor-invoices        إنشاء فاتورة مورد
  GET    /purchases/vendor-invoices        قائمة فواتير الموردين
  GET    /purchases/vendor-invoices/{id}   تفاصيل فاتورة
  POST   /purchases/vendor-invoices/{id}/match   تشغيل 3-way match
  POST   /purchases/vendor-invoices/{id}/post    ترحيل فاتورة المورد

Reports:
  GET    /purchases/dashboard              KPIs المشتريات
  GET    /purchases/ap-aging              تقادم الذمم الدائنة
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
from app.modules.purchases.schemas import (
    GRNCreate, POCreate,
    SupplierCreate, SupplierUpdate,
    VendorInvoiceCreate,
)
from app.modules.purchases.service import PurchasesService

router = APIRouter(prefix="/purchases", tags=["المشتريات"])


def _svc(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
) -> PurchasesService:
    return PurchasesService(db, user)


# ══════════════════════════════════════════════════════════
# Suppliers
# ══════════════════════════════════════════════════════════
@router.post("/suppliers", status_code=201, summary="إنشاء مورد جديد")
async def create_supplier(
    data: SupplierCreate,
    svc: PurchasesService = Depends(_svc),
):
    s = await svc.create_supplier(data)
    return created(data=s.to_dict(), message=f"تم إنشاء المورد {data.code}")


@router.get("/suppliers", summary="قائمة الموردين")
async def list_suppliers(
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    svc: PurchasesService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc.list_suppliers(offset=offset, limit=page_size)
    return paginated(
        items=[s.to_dict() for s in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/suppliers/search", summary="بحث عن مورد")
async def search_suppliers(
    q: str = Query(..., min_length=1),
    svc: PurchasesService = Depends(_svc),
):
    results = await svc.search_suppliers(q)
    return ok(data=[s.to_dict() for s in results])


@router.get("/suppliers/{supplier_id}", summary="تفاصيل مورد")
async def get_supplier(
    supplier_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    s = await svc.get_supplier(supplier_id)
    return ok(data=s.to_dict())


@router.put("/suppliers/{supplier_id}", summary="تعديل مورد")
async def update_supplier(
    supplier_id: uuid.UUID,
    data: SupplierUpdate,
    svc: PurchasesService = Depends(_svc),
):
    s = await svc.update_supplier(supplier_id, data)
    return ok(data=s.to_dict(), message="تم التعديل")


# ══════════════════════════════════════════════════════════
# Purchase Orders
# ══════════════════════════════════════════════════════════
@router.post("/orders", status_code=201, summary="إنشاء أمر شراء")
async def create_po(
    data: POCreate,
    svc: PurchasesService = Depends(_svc),
):
    """
    ينشئ أمر الشراء في حالة **draft**.
    يتطلب اعتماداً قبل إنشاء GRN.
    """
    po = await svc.create_po(data)
    return created(
        data=po.to_dict(),
        message=f"تم إنشاء أمر الشراء {po.po_number} — الإجمالي: {float(po.total_amount):,.3f} ر.س",
    )


@router.get("/orders", summary="قائمة أوامر الشراء")
async def list_pos(
    supplier_id: Optional[uuid.UUID] = Query(None),
    status:      Optional[str]       = Query(None),
    date_from:   Optional[date]      = Query(None),
    date_to:     Optional[date]      = Query(None),
    page:        int = Query(1, ge=1),
    page_size:   int = Query(20, ge=1, le=100),
    svc: PurchasesService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc._po_repo.list_pos(
        supplier_id=supplier_id,
        status=status,
        date_from=date_from,
        date_to=date_to,
        offset=offset,
        limit=page_size,
    )
    return paginated(
        items=[p.to_dict() for p in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/orders/{po_id}", summary="تفاصيل أمر الشراء")
async def get_po(
    po_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    po = await svc._po_repo.get_with_lines(po_id)
    if not po:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("أمر شراء", po_id)
    return ok(data=po.to_dict())


@router.post("/orders/{po_id}/approve", summary="اعتماد أمر الشراء")
async def approve_po(
    po_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    """يغير حالة PO من draft إلى approved — يسمح بإنشاء GRN."""
    po = await svc.approve_po(po_id)
    return ok(
        data=po.to_dict(),
        message=f"✅ تم اعتماد أمر الشراء {po.po_number}",
    )


# ══════════════════════════════════════════════════════════
# GRN — Goods Receipt Note
# ══════════════════════════════════════════════════════════
@router.post("/grn", status_code=201, summary="إنشاء إشعار استلام بضاعة")
async def create_grn(
    data: GRNCreate,
    svc: PurchasesService = Depends(_svc),
):
    """
    ينشئ GRN في حالة draft مرتبط بـ PO معتمد.
    لا يتم أي قيد أو حركة مخزون حتى الترحيل.
    """
    grn = await svc.create_grn(data)
    return created(
        data=grn.to_dict(),
        message=f"تم إنشاء GRN {grn.grn_number} للأمر {grn.po_number}",
    )


@router.get("/grn", summary="قائمة إشعارات الاستلام")
async def list_grns(
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    svc: PurchasesService = Depends(_svc),
):
    items, total = await svc._grn_repo.list(
        order_by=None, offset=(page - 1) * page_size, limit=page_size
    )
    return paginated(
        items=[g.to_dict() for g in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/grn/{grn_id}", summary="تفاصيل GRN")
async def get_grn(
    grn_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    grn = await svc._grn_repo.get_with_lines(grn_id)
    if not grn:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("GRN", grn_id)
    return ok(data=grn.to_dict())


@router.post("/grn/{grn_id}/post", summary="ترحيل GRN")
async def post_grn(
    grn_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    """
    يرحّل GRN:
    - يضيف الكميات إلى المخزون (PURCHASE_RECEIPT) ويحدّث WAC
    - ينشئ قيد محاسبي:
      - DR مخزون
      - CR ذمم الموردين (مؤقت حتى ترحيل فاتورة المورد)
    - يحدّث نسبة الاستلام في PO
    """
    grn = await svc.post_grn(grn_id)
    return ok(
        data=grn.to_dict(),
        message=f"✅ تم ترحيل GRN {grn.grn_number} | قيد: {grn.je_serial} | تكلفة: {float(grn.total_cost):,.3f} ر.س",
    )


@router.get("/orders/{po_id}/grns", summary="GRNs لأمر شراء")
async def po_grns(
    po_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    grns = await svc._grn_repo.list_by_po(po_id)
    return ok(data=[g.to_dict() for g in grns])


# ══════════════════════════════════════════════════════════
# Vendor Invoices
# ══════════════════════════════════════════════════════════
@router.post("/vendor-invoices", status_code=201, summary="إنشاء فاتورة مورد")
async def create_vendor_invoice(
    data: VendorInvoiceCreate,
    svc: PurchasesService = Depends(_svc),
):
    """
    ينشئ فاتورة المورد مرتبطة بـ PO (وGRN اختياري).
    يجب تشغيل المطابقة الثلاثية قبل الترحيل.
    """
    vi = await svc.create_vendor_invoice(data)
    return created(
        data=vi.to_dict(),
        message=f"تم إنشاء فاتورة المورد {vi.vi_number}",
    )


@router.get("/vendor-invoices", summary="قائمة فواتير الموردين")
async def list_vendor_invoices(
    supplier_id: Optional[uuid.UUID] = Query(None),
    status:      Optional[str]       = Query(None),
    page:        int = Query(1, ge=1),
    page_size:   int = Query(20, ge=1, le=100),
    svc: PurchasesService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc._vi_repo.list_vendor_invoices(
        supplier_id=supplier_id,
        status=status,
        offset=offset,
        limit=page_size,
    )
    return paginated(
        items=[v.to_dict() for v in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/vendor-invoices/{vi_id}", summary="تفاصيل فاتورة المورد")
async def get_vendor_invoice(
    vi_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    vi = await svc._vi_repo.get_with_lines(vi_id)
    if not vi:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("فاتورة مورد", vi_id)
    return ok(data=vi.to_dict())


@router.post("/vendor-invoices/{vi_id}/match", summary="تشغيل المطابقة الثلاثية")
async def run_3way_match(
    vi_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    """
    **3-Way Match**: يتحقق من تطابق:
    - الكمية المفوترة ≤ الكمية المستلمة (GRN)
    - السعر في حدود ±5% من سعر PO

    إذا نجحت → تتغير حالة الفاتورة إلى `matched`.
    """
    result = await svc.run_3way_match(vi_id)
    icon = "✅" if result["passed"] else "❌"
    return ok(
        data=result,
        message=f"{icon} المطابقة الثلاثية: {'ناجحة' if result['passed'] else 'فاشلة'}",
    )


@router.post("/vendor-invoices/{vi_id}/post", summary="ترحيل فاتورة المورد")
async def post_vendor_invoice(
    vi_id: uuid.UUID,
    svc: PurchasesService = Depends(_svc),
):
    """
    يرحّل فاتورة المورد بعد نجاح المطابقة الثلاثية:
    - DR مخزون (تكلفة البضاعة)
    - DR ضريبة مدخلات (VAT قابل للاسترداد)
    - CR ذمم الموردين (إجمالي الفاتورة)
    - يحدّث نسبة الفوترة في PO
    """
    vi = await svc.post_vendor_invoice(vi_id)
    return ok(
        data=vi.to_dict(),
        message=f"✅ تم ترحيل فاتورة المورد {vi.vi_number} | قيد: {vi.je_serial}",
    )


# ══════════════════════════════════════════════════════════
# Dashboard & Reports
# ══════════════════════════════════════════════════════════
@router.get("/dashboard", summary="لوحة تحكم المشتريات")
async def purchases_dashboard(svc: PurchasesService = Depends(_svc)):
    """KPIs المشتريات: ذمم دائنة + أوامر شراء مفتوحة."""
    data = await svc.get_dashboard()
    return ok(data=data)


@router.get("/ap-aging", summary="تقادم الذمم الدائنة")
async def ap_aging(svc: PurchasesService = Depends(_svc)):
    """
    تقرير تقادم الذمم الدائنة — يصنّف فواتير الموردين حسب:
    0-30 يوم | 31-60 | 61-90 | +90 يوم
    """
    from datetime import date as date_type
    from app.modules.purchases.models import VendorInvoice, VendorInvoiceStatus

    today = date_type.today()
    from sqlalchemy import select
    result = await svc.db.execute(
        select(VendorInvoice)
        .where(VendorInvoice.tenant_id == svc.user.tenant_id)
        .where(VendorInvoice.status.in_(["posted", "matched"]))
        .where(VendorInvoice.balance_due > 0)
        .order_by(VendorInvoice.due_date)
    )
    invoices = result.scalars().all()

    buckets = {"0_30": 0.0, "31_60": 0.0, "61_90": 0.0, "over_90": 0.0}
    lines = []
    for vi in invoices:
        days_overdue = (today - vi.due_date).days if vi.due_date else 0
        balance = float(vi.balance_due)
        if days_overdue <= 30:
            buckets["0_30"]   += balance
        elif days_overdue <= 60:
            buckets["31_60"]  += balance
        elif days_overdue <= 90:
            buckets["61_90"]  += balance
        else:
            buckets["over_90"] += balance
        lines.append({
            "vi_number":     vi.vi_number,
            "supplier_name": vi.supplier_name,
            "invoice_date":  str(vi.invoice_date),
            "due_date":      str(vi.due_date) if vi.due_date else None,
            "days_overdue":  days_overdue,
            "balance_due":   balance,
        })

    return ok(data={
        "summary":       buckets,
        "total_ap":      sum(buckets.values()),
        "invoice_count": len(invoices),
        "lines":         lines,
    })
