"""
app/modules/settings/models.py
الإعدادات المالية — الفروع، مراكز التكلفة، المشاريع
"""
from __future__ import annotations
import uuid
from decimal import Decimal
from typing import List, Optional
from sqlalchemy import Boolean, Date, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.base import Base
from app.db.mixins import ERPModel


class Region(ERPModel, Base):
    __tablename__ = "regions"
    code: Mapped[str] = mapped_column(String(10), nullable=False)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    cities: Mapped[List["City"]] = relationship("City", back_populates="region")
    __table_args__ = (UniqueConstraint("tenant_id", "code", name="uq_region_tenant_code"),)


class City(ERPModel, Base):
    __tablename__ = "cities"
    region_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("regions.id"), nullable=False)
    code: Mapped[str] = mapped_column(String(10), nullable=False)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    region: Mapped["Region"] = relationship("Region", back_populates="cities")
    branches: Mapped[List["Branch"]] = relationship("Branch", back_populates="city")
    __table_args__ = (UniqueConstraint("tenant_id", "code", name="uq_city_tenant_code"),)


class Branch(ERPModel, Base):
    __tablename__ = "branches"
    code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    name_ar: Mapped[str] = mapped_column(String(255), nullable=False)
    name_en: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    branch_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    country: Mapped[str] = mapped_column(String(100), default="KSA")
    currency: Mapped[str] = mapped_column(String(10), default="SAR")
    parent_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("branches.id"), nullable=True)
    region_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("regions.id"), nullable=True)
    city_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("cities.id"), nullable=True)
    city_sequence: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    children: Mapped[List["Branch"]] = relationship("Branch", back_populates="parent")
    parent: Mapped[Optional["Branch"]] = relationship("Branch", back_populates="children", remote_side="Branch.id")
    region: Mapped[Optional["Region"]] = relationship("Region")
    city: Mapped[Optional["City"]] = relationship("City", back_populates="branches")
    __table_args__ = (UniqueConstraint("tenant_id", "code", name="uq_branch_tenant_code"),)


class CostCenter(ERPModel, Base):
    __tablename__ = "cost_centers"
    code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    name_en: Mapped[str] = mapped_column(String(255), nullable=False)
    name_ar: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    level: Mapped[int] = mapped_column(Integer, default=1)
    department_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    department_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    parent_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("cost_centers.id"), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    children: Mapped[List["CostCenter"]] = relationship("CostCenter", back_populates="parent")
    parent: Mapped[Optional["CostCenter"]] = relationship("CostCenter", back_populates="children", remote_side="CostCenter.id")
    __table_args__ = (UniqueConstraint("tenant_id", "code", name="uq_cc_tenant_code"),)


class Project(ERPModel, Base):
    __tablename__ = "projects"
    code: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    project_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    customer_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    customer_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    customer_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    contract_value: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    budget_value: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    project_duration: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    start_date: Mapped[Optional[str]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[str]] = mapped_column(Date, nullable=True)
    revenue_recognition: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    bank_facilities_limit: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    bank_facilities_utilized: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=0)
    bank_facilities_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="active")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    __table_args__ = (UniqueConstraint("tenant_id", "code", name="uq_project_tenant_code"),)
