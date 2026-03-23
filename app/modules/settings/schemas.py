"""
app/modules/settings/schemas.py
"""
from __future__ import annotations
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


# ── Branch ────────────────────────────────────
class BranchCreate(BaseModel):
    code: str = Field(..., min_length=1, max_length=10)
    name_ar: str = Field(..., min_length=1, max_length=255)
    name_en: Optional[str] = None
    branch_type: Optional[str] = None
    address: Optional[str] = None
    country: str = "KSA"
    currency: str = "SAR"
    parent_id: Optional[uuid.UUID] = None
    region_id: Optional[uuid.UUID] = None
    city_id: Optional[uuid.UUID] = None
    city_sequence: int = 1

class BranchUpdate(BaseModel):
    name_ar: Optional[str] = None
    name_en: Optional[str] = None
    branch_type: Optional[str] = None
    address: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    parent_id: Optional[uuid.UUID] = None
    region_id: Optional[uuid.UUID] = None
    city_id: Optional[uuid.UUID] = None
    city_sequence: Optional[int] = None
    is_active: Optional[bool] = None


# ── Cost Center ───────────────────────────────
class CostCenterCreate(BaseModel):
    code: str = Field(..., min_length=1, max_length=10)
    name_en: str = Field(..., min_length=1, max_length=255)
    name_ar: Optional[str] = None
    level: int = 1
    department_code: Optional[str] = None
    department_name: Optional[str] = None
    parent_id: Optional[uuid.UUID] = None

class CostCenterUpdate(BaseModel):
    name_en: Optional[str] = None
    name_ar: Optional[str] = None
    level: Optional[int] = None
    department_code: Optional[str] = None
    department_name: Optional[str] = None
    parent_id: Optional[uuid.UUID] = None
    is_active: Optional[bool] = None


# ── Project ───────────────────────────────────
class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=500)
    project_type: Optional[str] = None
    customer_id: Optional[uuid.UUID] = None
    customer_name: Optional[str] = None
    customer_type: Optional[str] = None
    contract_value: Decimal = Decimal("0")
    budget_value: Decimal = Decimal("0")
    project_duration: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    revenue_recognition: Optional[str] = None
    bank_facilities_limit: Decimal = Decimal("0")
    bank_facilities_utilized: Decimal = Decimal("0")
    bank_facilities_name: Optional[str] = None
    status: str = "active"

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    project_type: Optional[str] = None
    customer_id: Optional[uuid.UUID] = None
    customer_name: Optional[str] = None
    customer_type: Optional[str] = None
    contract_value: Optional[Decimal] = None
    budget_value: Optional[Decimal] = None
    project_duration: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    revenue_recognition: Optional[str] = None
    bank_facilities_limit: Optional[Decimal] = None
    bank_facilities_utilized: Optional[Decimal] = None
    bank_facilities_name: Optional[str] = None
    status: Optional[str] = None
    is_active: Optional[bool] = None
