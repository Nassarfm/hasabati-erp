"""
app/modules/hr/service.py
══════════════════════════════════════════════════════════
HR Service — متوافق مع models.py (بدون Contract)
بيانات الراتب مدمجة مباشرة في Employee.

Payroll Posting JE:
  DR  مصروف رواتب (6001)          ← total_gross
  DR  GOSI صاحب عمل (6002)        ← total_gosi_employer
  DR  مكافأة نهاية الخدمة (6003)
    CR  رواتب مستحقة (2301)        ← total_net
    CR  التزام GOSI (2302)          ← emp + er
    CR  مخصص EOSB (2303)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.exceptions import (
    DuplicateError, InvalidStateError,
    NotFoundError, ValidationError,
)
from app.core.tenant import CurrentUser
from app.db.transactions import atomic_transaction
from app.modules.hr.gosi import calc_payroll_line
from app.modules.hr.models import (
    Employee, EmployeeStatus, LeaveRequest, LeaveStatus,
    PayrollLine, PayrollRun, PayrollStatus,
)
from app.modules.hr.repository import (
    EmployeeRepository, LeaveRepository, PayrollRunRepository,
)
from app.modules.hr.schemas import (
    EmployeeCreate, EmployeeUpdate,
    LeaveRequestCreate, PayrollLineOverride, PayrollRunCreate,
)
from app.services.numbering.series_service import NumberSeriesService
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import ACC

logger = structlog.get_logger(__name__)
PRICE_PREC = Decimal("0.001")


class HRService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db   = db
        self.user = user
        tid = user.tenant_id

        self._emp_repo     = EmployeeRepository(db, tid)
        self._payroll_repo = PayrollRunRepository(db, tid)
        self._leave_repo   = LeaveRepository(db, tid)
        self._num_svc      = NumberSeriesService(db, tid)
        self._posting      = PostingEngine(db, tid)

    # ══════════════════════════════════════════════════════
    # Employee CRUD
    # ══════════════════════════════════════════════════════
    async def create_employee(self, data: EmployeeCreate) -> Employee:
        self.user.require("can_manage_hr")
        if await self._emp_repo.exists(employee_number=data.employee_number):
            raise DuplicateError("موظف", "employee_number", data.employee_number)
        emp = self._emp_repo.create(**data.model_dump())
        emp.created_by = self.user.email
        return await self._emp_repo.save(emp)

    async def update_employee(self, eid: uuid.UUID, data: EmployeeUpdate) -> Employee:
        self.user.require("can_manage_hr")
        emp = await self._emp_repo.get_or_raise(eid)
        for k, v in data.model_dump(exclude_none=True).items():
            setattr(emp, k, v)
        emp.updated_by = self.user.email
        await self.db.flush()
        return emp

    async def get_employee(self, eid: uuid.UUID) -> Employee:
        return await self._emp_repo.get_or_raise(eid)

    async def list_employees(
        self, offset: int = 0, limit: int = 50
    ) -> Tuple[List[Employee], int]:
        return await self._emp_repo.list_active(offset=offset, limit=limit)

    async def search_employees(self, query: str) -> List[Employee]:
        return await self._emp_repo.search(query)

    # ══════════════════════════════════════════════════════
    # Payroll Run
    # ══════════════════════════════════════════════════════
    async def create_payroll_run(
        self,
        data: PayrollRunCreate,
        overrides: Optional[List[PayrollLineOverride]] = None,
    ) -> PayrollRun:
        """
        ينشئ دورة رواتب لجميع الموظفين النشطين.
        يحسب تلقائياً: GOSI + EOSB + الصافي لكل موظف.
        """
        self.user.require("can_manage_payroll")

        existing = await self._payroll_repo.get_by_period(data.period_year, data.period_month)
        if existing:
            raise DuplicateError("دورة رواتب", "period",
                                 f"{data.period_year}-{data.period_month:02d}")

        run_number  = await self._num_svc.next("PAY", include_month=True)
        period_date = date(data.period_year, data.period_month, 1)

        # Override map: employee_id → override
        override_map: Dict[uuid.UUID, PayrollLineOverride] = {}
        if overrides:
            for ov in overrides:
                override_map[ov.employee_id] = ov

        employees = await self._emp_repo.get_all_active()

        async with atomic_transaction(self.db, label=f"payroll_{data.period_year}_{data.period_month}"):
            run = PayrollRun(
                tenant_id=self.user.tenant_id,
                run_number=run_number,
                period_year=data.period_year,
                period_month=data.period_month,
                pay_date=data.pay_date,
                status=PayrollStatus.DRAFT,
                notes=data.notes,
                created_by=self.user.email,
            )
            self.db.add(run)
            await self.db.flush()

            total_basic = total_allow = total_gross = Decimal("0")
            total_gosi_emp = total_gosi_er = Decimal("0")
            total_ded = total_net = total_eosb = Decimal("0")
            emp_count = 0

            for emp in employees:
                ov = override_map.get(emp.id)

                calc = calc_payroll_line(
                    basic_salary=emp.basic_salary,
                    housing_allowance=emp.housing_allow,
                    transport_allowance=emp.transport_allow,
                    food_allowance=Decimal("0"),
                    phone_allowance=Decimal("0"),
                    other_allowances=emp.other_allow,
                    overtime_amount=ov.overtime_amount    if ov else Decimal("0"),
                    bonus_amount=ov.bonus_amount          if ov else Decimal("0"),
                    nationality=emp.nationality,
                    hire_date=emp.hire_date,
                    period_date=period_date,
                    advance_deduction=ov.advance_deduction if ov else Decimal("0"),
                    absence_deduction=ov.absence_deduction if ov else Decimal("0"),
                    other_deductions=ov.other_deductions   if ov else Decimal("0"),
                )

                line = PayrollLine(
                    tenant_id=self.user.tenant_id,
                    run_id=run.id,
                    employee_id=emp.id,
                    employee_number=emp.employee_number,
                    employee_name=emp.full_name_ar,
                    nationality=emp.nationality,
                    department=emp.department,
                    basic_salary=emp.basic_salary,
                    housing_allow=emp.housing_allow,
                    transport_allow=emp.transport_allow,
                    other_allow=emp.other_allow,
                    overtime_amount=ov.overtime_amount    if ov else Decimal("0"),
                    bonus_amount=ov.bonus_amount          if ov else Decimal("0"),
                    gross_salary=calc["gross_salary"],
                    gosi_base=calc.get("gosi_base", emp.basic_salary),
                    gosi_employee=calc["gosi_employee"],
                    gosi_employer=calc["gosi_employer"],
                    deductions_total=calc["total_deductions"],
                    absence_deduction=ov.absence_deduction if ov else Decimal("0"),
                    advance_deduction=ov.advance_deduction if ov else Decimal("0"),
                    other_deductions=ov.other_deductions   if ov else Decimal("0"),
                    net_salary=calc["net_salary"],
                    eosb_accrual=calc["eosb_accrual"],
                    working_days=30,
                    absent_days=ov.absent_days if ov else 0,
                    created_by=self.user.email,
                )
                self.db.add(line)

                total_basic    += emp.basic_salary
                total_allow    += emp.housing_allow + emp.transport_allow + emp.other_allow
                total_gross    += calc["gross_salary"]
                total_gosi_emp += calc["gosi_employee"]
                total_gosi_er  += calc["gosi_employer"]
                total_ded      += calc["total_deductions"]
                total_net      += calc["net_salary"]
                total_eosb     += calc["eosb_accrual"]
                emp_count      += 1

            run.total_basic         = total_basic.quantize(PRICE_PREC)
            run.total_allowances    = total_allow.quantize(PRICE_PREC)
            run.total_gross         = total_gross.quantize(PRICE_PREC)
            run.total_gosi_employee = total_gosi_emp.quantize(PRICE_PREC)
            run.total_gosi_employer = total_gosi_er.quantize(PRICE_PREC)
            run.total_deductions    = total_ded.quantize(PRICE_PREC)
            run.total_net           = total_net.quantize(PRICE_PREC)
            run.total_eosb_accrual  = total_eosb.quantize(PRICE_PREC)
            run.employee_count      = emp_count
            await self.db.flush()

        logger.info("payroll_created", period=f"{data.period_year}-{data.period_month:02d}",
                    employees=emp_count, net=float(total_net))
        return await self._payroll_repo.get_with_lines(run.id)

    async def post_payroll(self, run_id: uuid.UUID) -> PayrollRun:
        """
        يرحّل دورة الرواتب — قيد محاسبي:
          DR  رواتب (6001)        ← gross
          DR  GOSI عمل (6002)    ← er_gosi
          DR  EOSB (6003)        ← eosb_accrual
            CR  رواتب مستحقة (2301) ← net
            CR  GOSI (2302)          ← emp+er
            CR  مخصص EOSB (2303)
        """
        self.user.require("can_manage_payroll")
        run = await self._payroll_repo.get_with_lines(run_id)
        if not run:
            raise NotFoundError("دورة رواتب", run_id)
        if run.status != PayrollStatus.DRAFT:
            raise InvalidStateError("دورة الرواتب", run.status, [PayrollStatus.DRAFT])

        period_label = f"{run.period_year}-{run.period_month:02d}"

        async with atomic_transaction(self.db, label=f"post_payroll_{period_label}"):
            total_gosi = (run.total_gosi_employee + run.total_gosi_employer).quantize(PRICE_PREC)

            je_lines = [
                PostingLine(account_code=ACC.SALARIES,
                            description=f"رواتب شهر {period_label}",
                            debit=run.total_gross),
                PostingLine(account_code=ACC.GOSI_EXP,
                            description=f"GOSI صاحب عمل — {period_label}",
                            debit=run.total_gosi_employer),
                PostingLine(account_code=ACC.PAY_LIA,
                            description=f"رواتب مستحقة الدفع — {period_label}",
                            credit=run.total_net),
                PostingLine(account_code=ACC.GOSI_LIA,
                            description=f"التزام GOSI — {period_label}",
                            credit=total_gosi),
            ]

            if run.total_eosb_accrual > 0:
                je_lines.append(PostingLine(
                    account_code="6003",
                    description=f"مصروف مكافأة نهاية الخدمة — {period_label}",
                    debit=run.total_eosb_accrual,
                ))
                je_lines.append(PostingLine(
                    account_code="2303",
                    description=f"مخصص نهاية الخدمة — {period_label}",
                    credit=run.total_eosb_accrual,
                ))

            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="PAY",
                description=f"رواتب {period_label} — {run.employee_count} موظف",
                entry_date=date(run.period_year, run.period_month, 1),
                lines=je_lines,
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="hr",
                source_doc_type="payroll_run",
                source_doc_number=run.run_number,
                idempotency_key=f"HR:PAY:{run.id}:{self.user.tenant_id}",
                user_role=self.user.role,
            )
            je_result = await self._posting.post(je_req)

            run.je_id     = je_result.je_id
            run.je_serial = je_result.je_serial
            run.status    = PayrollStatus.POSTED
            run.posted_at = datetime.now(timezone.utc)
            run.posted_by = self.user.email
            await self.db.flush()

        logger.info("payroll_posted", period=period_label,
                    employees=run.employee_count, net=float(run.total_net), je=run.je_serial)
        return run

    # ══════════════════════════════════════════════════════
    # Leave Management
    # ══════════════════════════════════════════════════════
    async def create_leave_request(self, data: LeaveRequestCreate) -> LeaveRequest:
        self.user.require("can_manage_hr")
        emp = await self._emp_repo.get_or_raise(data.employee_id)
        days = (data.end_date - data.start_date).days + 1
        leave_number = await self._num_svc.next("LVE", include_month=True)

        req = LeaveRequest(
            tenant_id=self.user.tenant_id,
            leave_number=leave_number,
            employee_id=emp.id,
            employee_number=emp.employee_number,
            employee_name=emp.full_name_ar,
            leave_type=data.leave_type,
            status=LeaveStatus.PENDING,
            start_date=data.start_date,
            end_date=data.end_date,
            days_requested=days,
            reason=data.reason,
            notes=data.notes,
            created_by=self.user.email,
        )
        self.db.add(req)
        await self.db.flush()
        return req

    async def approve_leave(self, leave_id: uuid.UUID) -> LeaveRequest:
        self.user.require("can_manage_hr")
        req = await self._leave_repo.get_or_raise(leave_id)
        if req.status != LeaveStatus.PENDING:
            raise InvalidStateError("طلب الإجازة", req.status, [LeaveStatus.PENDING])
        req.status      = LeaveStatus.APPROVED
        req.days_approved = req.days_requested
        req.approved_by = self.user.email
        req.approved_at = datetime.now(timezone.utc)
        await self.db.flush()
        return req

    async def reject_leave(self, leave_id: uuid.UUID, reason: str) -> LeaveRequest:
        self.user.require("can_manage_hr")
        req = await self._leave_repo.get_or_raise(leave_id)
        if req.status != LeaveStatus.PENDING:
            raise InvalidStateError("طلب الإجازة", req.status, [LeaveStatus.PENDING])
        req.status = LeaveStatus.REJECTED
        req.notes  = reason
        await self.db.flush()
        return req

    async def list_leaves(
        self, employee_id: Optional[uuid.UUID] = None
    ) -> List[LeaveRequest]:
        if employee_id:
            return await self._leave_repo.list_by_employee(employee_id)
        items, _ = await self._leave_repo.list(order_by=None)
        return items

    # ══════════════════════════════════════════════════════
    # EOSB Calculator
    # ══════════════════════════════════════════════════════
    async def calculate_eosb(self, employee_id: uuid.UUID) -> dict:
        from app.modules.hr.gosi import calc_eosb
        emp = await self._emp_repo.get_or_raise(employee_id)
        eosb = calc_eosb(emp.basic_salary, emp.hire_date, date.today())
        return {
            "employee_number":   emp.employee_number,
            "employee_name":     emp.full_name_ar,
            "hire_date":         str(emp.hire_date),
            "years_of_service":  float(eosb.years_of_service),
            "basic_salary":      float(emp.basic_salary),
            "monthly_accrual":   float(eosb.accrual_amount),
            "total_entitlement": float(eosb.total_entitlement),
        }

    # ══════════════════════════════════════════════════════
    # Dashboard
    # ══════════════════════════════════════════════════════
    async def get_dashboard(self) -> dict:
        _, total = await self._emp_repo.list_active(limit=1)
        return {"active_employees": total}
