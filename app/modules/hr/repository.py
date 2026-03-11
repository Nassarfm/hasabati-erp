"""app/modules/hr/repository.py — بدون Contract، يعتمد على Employee مباشرة"""
from __future__ import annotations
import uuid
from typing import List, Optional, Tuple

from sqlalchemy import desc, select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.hr.models import (
    Employee, EmployeeStatus,
    LeaveRequest, PayrollRun,
)
from app.repositories.base_repo import BaseRepository


class EmployeeRepository(BaseRepository[Employee]):
    model = Employee

    async def get_by_number(self, number: str) -> Optional[Employee]:
        result = await self.db.execute(
            self._base_query().where(Employee.employee_number == number)
        )
        return result.scalar_one_or_none()

    async def list_active(self, offset: int = 0, limit: int = 50) -> Tuple[List[Employee], int]:
        return await self.list(
            filters=[Employee.status == EmployeeStatus.ACTIVE],
            order_by=Employee.employee_number,
            offset=offset, limit=limit,
        )

    async def get_all_active(self) -> List[Employee]:
        result = await self.db.execute(
            self._base_query()
            .where(Employee.status == EmployeeStatus.ACTIVE)
            .where(Employee.is_active == True)
        )
        return list(result.scalars().all())

    async def search(self, query: str, limit: int = 20) -> List[Employee]:
        result = await self.db.execute(
            self._base_query()
            .where(
                Employee.first_name_ar.ilike(f"%{query}%") |
                Employee.last_name_ar.ilike(f"%{query}%")  |
                Employee.employee_number.ilike(f"%{query}%") |
                Employee.department.ilike(f"%{query}%")
            )
            .limit(limit)
        )
        return list(result.scalars().all())


class PayrollRunRepository(BaseRepository[PayrollRun]):
    model = PayrollRun

    async def get_with_lines(self, run_id: uuid.UUID) -> Optional[PayrollRun]:
        result = await self.db.execute(
            self._base_query()
            .where(PayrollRun.id == run_id)
            .options(selectinload(PayrollRun.lines))
        )
        return result.scalar_one_or_none()

    async def get_by_period(self, year: int, month: int) -> Optional[PayrollRun]:
        result = await self.db.execute(
            self._base_query()
            .where(PayrollRun.period_year == year)
            .where(PayrollRun.period_month == month)
        )
        return result.scalar_one_or_none()


class LeaveRepository(BaseRepository[LeaveRequest]):
    model = LeaveRequest

    async def list_by_employee(self, employee_id: uuid.UUID) -> List[LeaveRequest]:
        result = await self.db.execute(
            self._base_query()
            .where(LeaveRequest.employee_id == employee_id)
            .order_by(desc(LeaveRequest.start_date))
        )
        return list(result.scalars().all())
