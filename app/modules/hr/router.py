"""app/modules/hr/router.py — HR API (17 endpoints) — بدون Contract"""
from __future__ import annotations
import uuid
from typing import List, Optional
from fastapi import APIRouter, Body, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import created, ok, paginated
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.hr.schemas import (
    EmployeeCreate, EmployeeUpdate,
    LeaveRequestCreate, PayrollLineOverride, PayrollRunCreate,
)
from app.modules.hr.service import HRService

router = APIRouter(prefix="/hr", tags=["الموارد البشرية"])


def _svc(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    return HRService(db, user)


# ── Employees ─────────────────────────────────────────────
@router.post("/employees", status_code=201, summary="إنشاء موظف جديد")
async def create_employee(data: EmployeeCreate, svc: HRService = Depends(_svc)):
    emp = await svc.create_employee(data)
    return created(data=emp.to_dict(), message=f"تم إنشاء الموظف {data.employee_number}")


@router.get("/employees", summary="قائمة الموظفين")
async def list_employees(
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100),
    svc: HRService = Depends(_svc),
):
    offset = (page - 1) * page_size
    items, total = await svc.list_employees(offset=offset, limit=page_size)
    return paginated(items=[e.to_dict() for e in items], total=total, page=page, page_size=page_size)


@router.get("/employees/search", summary="بحث عن موظف")
async def search_employees(q: str = Query(..., min_length=1), svc: HRService = Depends(_svc)):
    results = await svc.search_employees(q)
    return ok(data=[e.to_dict() for e in results])


@router.get("/employees/{employee_id}", summary="تفاصيل موظف")
async def get_employee(employee_id: uuid.UUID, svc: HRService = Depends(_svc)):
    emp = await svc.get_employee(employee_id)
    return ok(data=emp.to_dict())


@router.put("/employees/{employee_id}", summary="تعديل بيانات موظف")
async def update_employee(
    employee_id: uuid.UUID, data: EmployeeUpdate, svc: HRService = Depends(_svc),
):
    emp = await svc.update_employee(employee_id, data)
    return ok(data=emp.to_dict(), message="تم التعديل")


@router.get("/employees/{employee_id}/eosb", summary="حساب مكافأة نهاية الخدمة")
async def calculate_eosb(employee_id: uuid.UUID, svc: HRService = Depends(_svc)):
    """
    يحسب EOSB وفق نظام العمل السعودي:
    - أقل من سنتين: لا يستحق
    - 2–5 سنوات: نصف الراتب × سنوات الخدمة
    - أكثر من 5 سنوات: راتب كامل × سنوات الخدمة
    """
    result = await svc.calculate_eosb(employee_id)
    return ok(data=result)


@router.get("/employees/{employee_id}/leaves", summary="إجازات موظف")
async def employee_leaves(employee_id: uuid.UUID, svc: HRService = Depends(_svc)):
    items = await svc._leave_repo.list_by_employee(employee_id)
    return ok(data=[r.to_dict() for r in items])


# ── Payroll ───────────────────────────────────────────────
@router.post("/payroll", status_code=201, summary="إنشاء دورة رواتب")
async def create_payroll_run(
    data: PayrollRunCreate,
    overrides: Optional[List[PayrollLineOverride]] = Body(default=None),
    svc: HRService = Depends(_svc),
):
    """
    ينشئ دورة رواتب لجميع الموظفين النشطين.
    يحسب تلقائياً: الراتب الإجمالي + GOSI + EOSB + الصافي.
    أرفق `overrides` لإضافة: ساعات إضافية، مكافآت، خصومات.
    """
    run = await svc.create_payroll_run(data, overrides)
    period = f"{run.period_year}-{run.period_month:02d}"
    return created(
        data=run.to_dict(),
        message=f"✅ دورة رواتب {period} — {run.employee_count} موظف | صافي: {float(run.total_net):,.3f} ر.س",
    )


@router.get("/payroll", summary="قائمة دورات الرواتب")
async def list_payroll_runs(
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100),
    svc: HRService = Depends(_svc),
):
    items, total = await svc._payroll_repo.list(
        order_by=None, offset=(page-1)*page_size, limit=page_size
    )
    return paginated(items=[r.to_dict() for r in items], total=total, page=page, page_size=page_size)


@router.get("/payroll/{run_id}", summary="تفاصيل دورة الرواتب")
async def get_payroll_run(run_id: uuid.UUID, svc: HRService = Depends(_svc)):
    run = await svc._payroll_repo.get_with_lines(run_id)
    if not run:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("دورة رواتب", run_id)
    return ok(data=run.to_dict())


@router.post("/payroll/{run_id}/post", summary="ترحيل دورة الرواتب")
async def post_payroll(run_id: uuid.UUID, svc: HRService = Depends(_svc)):
    """
    ينشئ قيد محاسبي كامل:
    DR رواتب + DR GOSI عمل + DR EOSB
    CR رواتب مستحقة + CR GOSI + CR مخصص EOSB
    """
    run = await svc.post_payroll(run_id)
    period = f"{run.period_year}-{run.period_month:02d}"
    return ok(
        data=run.to_dict(),
        message=f"✅ ترحيل رواتب {period} | قيد: {run.je_serial} | صافي: {float(run.total_net):,.3f} ر.س",
    )


@router.get("/payroll/{run_id}/slip/{employee_id}", summary="قسيمة راتب موظف")
async def payroll_slip(run_id: uuid.UUID, employee_id: uuid.UUID, svc: HRService = Depends(_svc)):
    from app.modules.hr.models import PayrollLine
    result = await svc.db.execute(
        select(PayrollLine)
        .where(PayrollLine.tenant_id == svc.user.tenant_id)
        .where(PayrollLine.run_id == run_id)
        .where(PayrollLine.employee_id == employee_id)
    )
    line = result.scalar_one_or_none()
    if not line:
        from app.core.exceptions import NotFoundError
        raise NotFoundError("قسيمة الراتب", employee_id)
    return ok(data=line.to_dict())


# ── Leave ─────────────────────────────────────────────────
@router.post("/leaves", status_code=201, summary="طلب إجازة")
async def create_leave(data: LeaveRequestCreate, svc: HRService = Depends(_svc)):
    req = await svc.create_leave_request(data)
    return created(data=req.to_dict(), message=f"تم تقديم طلب الإجازة ({req.days_requested} يوم)")


@router.get("/leaves", summary="قائمة طلبات الإجازات")
async def list_leaves(
    employee_id: Optional[uuid.UUID] = Query(None),
    svc: HRService = Depends(_svc),
):
    items = await svc.list_leaves(employee_id)
    return ok(data=[r.to_dict() for r in items])


@router.post("/leaves/{leave_id}/approve", summary="اعتماد طلب إجازة")
async def approve_leave(leave_id: uuid.UUID, svc: HRService = Depends(_svc)):
    req = await svc.approve_leave(leave_id)
    return ok(data=req.to_dict(), message="✅ تم اعتماد طلب الإجازة")


@router.post("/leaves/{leave_id}/reject", summary="رفض طلب إجازة")
async def reject_leave(
    leave_id: uuid.UUID,
    reason: str = Body(..., embed=True),
    svc: HRService = Depends(_svc),
):
    req = await svc.reject_leave(leave_id, reason)
    return ok(data=req.to_dict(), message="تم رفض طلب الإجازة")


# ── Dashboard ─────────────────────────────────────────────
@router.get("/dashboard", summary="لوحة تحكم الموارد البشرية")
async def hr_dashboard(svc: HRService = Depends(_svc)):
    data = await svc.get_dashboard()
    return ok(data=data)
