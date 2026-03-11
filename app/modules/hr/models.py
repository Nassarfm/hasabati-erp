"""
app/modules/hr/models.py — HR Module DB models
Tables: hr_employees, hr_payroll_runs, hr_payroll_lines,
        hr_leave_requests
GOSI: Employee 9% / Employer 11.75% (Saudi)
EOSB: < 5yr → basic/3 per yr, ≥ 5yr → basic/2 per yr
"""
from __future__ import annotations
import enum, uuid
from decimal import Decimal
from typing import List, Optional
from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime, Index, Integer,
    Numeric, String, Text, UniqueConstraint, ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.base import Base
from app.db.mixins import ERPModel, SoftDeleteMixin


class EmployeeStatus(str, enum.Enum):
    ACTIVE = "active"; ON_LEAVE = "on_leave"; TERMINATED = "terminated"

class NationalityType(str, enum.Enum):
    SAUDI = "saudi"; EXPAT = "expat"

class PayrollStatus(str, enum.Enum):
    DRAFT = "draft"; APPROVED = "approved"; POSTED = "posted"; CANCELLED = "cancelled"

class LeaveType(str, enum.Enum):
    ANNUAL = "annual"; SICK = "sick"; MATERNITY = "maternity"
    PATERNITY = "paternity"; EMERGENCY = "emergency"; UNPAID = "unpaid"; OTHER = "other"

class LeaveStatus(str, enum.Enum):
    PENDING = "pending"; APPROVED = "approved"; REJECTED = "rejected"; CANCELLED = "cancelled"

class Gender(str, enum.Enum):
    MALE = "male"; FEMALE = "female"


class Employee(ERPModel, SoftDeleteMixin, Base):
    __tablename__ = "hr_employees"

    employee_number: Mapped[str]             = mapped_column(String(30),  nullable=False, index=True)
    first_name_ar:   Mapped[str]             = mapped_column(String(100), nullable=False)
    last_name_ar:    Mapped[str]             = mapped_column(String(100), nullable=False)
    first_name_en:   Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    last_name_en:    Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    gender:          Mapped[Gender]          = mapped_column(String(10),  default=Gender.MALE)
    date_of_birth:   Mapped[Optional[str]]   = mapped_column(Date,        nullable=True)
    nationality:     Mapped[NationalityType] = mapped_column(String(10),  default=NationalityType.SAUDI)
    national_id:     Mapped[Optional[str]]   = mapped_column(String(20),  nullable=True)
    iqama_number:    Mapped[Optional[str]]   = mapped_column(String(20),  nullable=True)
    iqama_expiry:    Mapped[Optional[str]]   = mapped_column(Date,        nullable=True)
    passport_number: Mapped[Optional[str]]   = mapped_column(String(20),  nullable=True)
    status:          Mapped[EmployeeStatus]  = mapped_column(String(20),  default=EmployeeStatus.ACTIVE, index=True)
    hire_date:       Mapped[str]             = mapped_column(Date,        nullable=False)
    termination_date: Mapped[Optional[str]]  = mapped_column(Date,        nullable=True)
    department:      Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    job_title:       Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    cost_center:     Mapped[Optional[str]]   = mapped_column(String(50),  nullable=True)
    basic_salary:    Mapped[Decimal]         = mapped_column(Numeric(18, 3), nullable=False)
    housing_allow:   Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    transport_allow: Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    other_allow:     Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    gosi_enrolled:   Mapped[bool]            = mapped_column(Boolean, default=True)
    gosi_number:     Mapped[Optional[str]]   = mapped_column(String(30),  nullable=True)
    bank_name:       Mapped[Optional[str]]   = mapped_column(String(100), nullable=True)
    bank_iban:       Mapped[Optional[str]]   = mapped_column(String(34),  nullable=True)
    annual_leave_balance: Mapped[Decimal]    = mapped_column(Numeric(8, 2), default=0)
    phone:           Mapped[Optional[str]]   = mapped_column(String(30),  nullable=True)
    email:           Mapped[Optional[str]]   = mapped_column(String(255), nullable=True)
    notes:           Mapped[Optional[str]]   = mapped_column(Text,        nullable=True)

    payroll_lines:  Mapped[List["PayrollLine"]]  = relationship("PayrollLine",  back_populates="employee")
    leave_requests: Mapped[List["LeaveRequest"]] = relationship("LeaveRequest", back_populates="employee")

    __table_args__ = (
        UniqueConstraint("tenant_id", "employee_number", name="uq_emp_tenant_number"),
        Index("ix_emp_tenant_status", "tenant_id", "status"),
    )

    @property
    def full_name_ar(self) -> str:
        return f"{self.first_name_ar} {self.last_name_ar}"

    @property
    def gross_salary(self) -> Decimal:
        return self.basic_salary + self.housing_allow + self.transport_allow + self.other_allow


class PayrollRun(ERPModel, Base):
    __tablename__ = "hr_payroll_runs"

    run_number:           Mapped[str]             = mapped_column(String(50), nullable=False, index=True)
    period_year:          Mapped[int]             = mapped_column(Integer, nullable=False)
    period_month:         Mapped[int]             = mapped_column(Integer, nullable=False)
    pay_date:             Mapped[str]             = mapped_column(Date, nullable=False)
    status:               Mapped[PayrollStatus]   = mapped_column(String(20), default=PayrollStatus.DRAFT, index=True)
    total_basic:          Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_allowances:     Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_gross:          Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_gosi_employee:  Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_gosi_employer:  Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_deductions:     Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_net:            Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    total_eosb_accrual:   Mapped[Decimal]         = mapped_column(Numeric(18, 3), default=0)
    employee_count:       Mapped[int]             = mapped_column(Integer, default=0)
    je_id:                Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    je_serial:            Mapped[Optional[str]]   = mapped_column(String(50), nullable=True)
    posted_at:            Mapped[Optional[str]]   = mapped_column(DateTime(timezone=True), nullable=True)
    posted_by:            Mapped[Optional[str]]   = mapped_column(String(255), nullable=True)
    notes:                Mapped[Optional[str]]   = mapped_column(Text, nullable=True)

    lines: Mapped[List["PayrollLine"]] = relationship(
        "PayrollLine", back_populates="run", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "run_number",  name="uq_payroll_run_number"),
        UniqueConstraint("tenant_id", "period_year", "period_month", name="uq_payroll_period"),
        CheckConstraint("period_month BETWEEN 1 AND 12", name="ck_payroll_month"),
    )


class PayrollLine(ERPModel, Base):
    __tablename__ = "hr_payroll_lines"

    run_id:            Mapped[uuid.UUID]        = mapped_column(UUID(as_uuid=True), ForeignKey("hr_payroll_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    employee_id:       Mapped[uuid.UUID]        = mapped_column(UUID(as_uuid=True), ForeignKey("hr_employees.id"), nullable=False, index=True)
    employee_number:   Mapped[str]              = mapped_column(String(30),  nullable=False)
    employee_name:     Mapped[str]              = mapped_column(String(255), nullable=False)
    department:        Mapped[Optional[str]]    = mapped_column(String(100), nullable=True)
    nationality:       Mapped[str]              = mapped_column(String(10),  nullable=False)
    basic_salary:      Mapped[Decimal]          = mapped_column(Numeric(18, 3), nullable=False)
    housing_allow:     Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    transport_allow:   Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    other_allow:       Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    overtime_amount:   Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    bonus_amount:      Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    gross_salary:      Mapped[Decimal]          = mapped_column(Numeric(18, 3), nullable=False)
    gosi_base:         Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    gosi_employee:     Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    gosi_employer:     Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    deductions_total:  Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    absence_deduction: Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    advance_deduction: Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    other_deductions:  Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    net_salary:        Mapped[Decimal]          = mapped_column(Numeric(18, 3), nullable=False)
    eosb_accrual:      Mapped[Decimal]          = mapped_column(Numeric(18, 3), default=0)
    working_days:      Mapped[int]              = mapped_column(Integer, default=30)
    absent_days:       Mapped[int]              = mapped_column(Integer, default=0)

    run:      Mapped["PayrollRun"] = relationship("PayrollRun", back_populates="lines")
    employee: Mapped["Employee"]  = relationship("Employee",   back_populates="payroll_lines")

    __table_args__ = (
        UniqueConstraint("run_id", "employee_id", name="uq_payroll_line_run_emp"),
        CheckConstraint("gross_salary >= 0", name="ck_pl_gross_positive"),
        CheckConstraint("net_salary >= 0",   name="ck_pl_net_positive"),
    )


class LeaveRequest(ERPModel, Base):
    __tablename__ = "hr_leave_requests"

    leave_number:    Mapped[str]         = mapped_column(String(50),  nullable=False, index=True)
    employee_id:     Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), ForeignKey("hr_employees.id"), nullable=False, index=True)
    employee_number: Mapped[str]         = mapped_column(String(30),  nullable=False)
    employee_name:   Mapped[str]         = mapped_column(String(255), nullable=False)
    leave_type:      Mapped[LeaveType]   = mapped_column(String(20),  nullable=False)
    status:          Mapped[LeaveStatus] = mapped_column(String(20),  default=LeaveStatus.PENDING)
    start_date:      Mapped[str]         = mapped_column(Date,        nullable=False)
    end_date:        Mapped[str]         = mapped_column(Date,        nullable=False)
    days_requested:  Mapped[int]         = mapped_column(Integer,     nullable=False)
    days_approved:   Mapped[int]         = mapped_column(Integer,     default=0)
    approved_by:     Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    approved_at:     Mapped[Optional[str]] = mapped_column(DateTime(timezone=True), nullable=True)
    reason:          Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    notes:           Mapped[Optional[str]] = mapped_column(Text,        nullable=True)

    employee: Mapped["Employee"] = relationship("Employee", back_populates="leave_requests")

    __table_args__ = (
        UniqueConstraint("tenant_id", "leave_number", name="uq_leave_tenant_number"),
        CheckConstraint("days_requested > 0", name="ck_leave_days_positive"),
        CheckConstraint("end_date >= start_date", name="ck_leave_dates"),
    )
