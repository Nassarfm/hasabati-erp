"""
app/modules/settings/router.py
الإعدادات المالية — الفروع، مراكز التكلفة، المشاريع
"""
from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import created, ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.settings.schemas import BranchCreate, BranchUpdate, CostCenterCreate, CostCenterUpdate, ProjectCreate, ProjectUpdate
from app.modules.settings.service import SettingsService

router = APIRouter(prefix="/settings", tags=["الإعدادات المالية"])

def _svc(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)) -> SettingsService:
    return SettingsService(db, user)

def _branch_dict(b):
    return {
        "id": str(b.id), "code": b.code, "name_ar": b.name_ar, "name_en": b.name_en,
        "branch_type": b.branch_type, "address": b.address, "country": b.country,
        "currency": b.currency, "parent_id": str(b.parent_id) if b.parent_id else None,
        "region_id": str(b.region_id) if b.region_id else None,
        "region_name": b.region.name_ar if b.region else None,
        "city_id": str(b.city_id) if b.city_id else None,
        "city_name": b.city.name_ar if b.city else None,
        "city_sequence": b.city_sequence, "is_active": b.is_active,
    }

def _cc_dict(c):
    return {
        "id": str(c.id), "code": c.code, "name_en": c.name_en, "name_ar": c.name_ar,
        "level": c.level, "department_code": c.department_code, "department_name": c.department_name,
        "parent_id": str(c.parent_id) if c.parent_id else None, "is_active": c.is_active,
    }

def _project_dict(p):
    return {
        "id": str(p.id), "code": p.code, "name": p.name, "project_type": p.project_type,
        "customer_id": str(p.customer_id) if p.customer_id else None,
        "customer_name": p.customer_name, "customer_type": p.customer_type,
        "contract_value": float(p.contract_value), "budget_value": float(p.budget_value),
        "project_duration": p.project_duration,
        "start_date": str(p.start_date) if p.start_date else None,
        "end_date": str(p.end_date) if p.end_date else None,
        "revenue_recognition": p.revenue_recognition,
        "bank_facilities_limit": float(p.bank_facilities_limit),
        "bank_facilities_utilized": float(p.bank_facilities_utilized),
        "bank_facilities_name": p.bank_facilities_name,
        "status": p.status, "is_active": p.is_active,
    }

# ── Regions ───────────────────────────────────
@router.get("/regions", summary="المناطق والمدن")
async def list_regions(svc: SettingsService = Depends(_svc)):
    regions = await svc.list_regions()
    return ok(data=[{
        "id": str(r.id), "code": r.code, "name_ar": r.name_ar, "name_en": r.name_en,
        "cities": [{"id": str(c.id), "code": c.code, "name_ar": c.name_ar, "name_en": c.name_en}
                   for c in r.cities if c.is_active],
    } for r in regions])

# ── Branches ──────────────────────────────────
@router.get("/branches", summary="قائمة الفروع")
async def list_branches(svc: SettingsService = Depends(_svc)):
    branches = await svc.list_branches()
    return ok(data=[_branch_dict(b) for b in branches], message=f"{len(branches)} فرع")

@router.post("/branches", status_code=201, summary="إضافة فرع")
async def create_branch(data: BranchCreate, svc: SettingsService = Depends(_svc)):
    branch = await svc.create_branch(data)
    return created(data={"id": str(branch.id), "code": branch.code}, message=f"تم إضافة الفرع {branch.name_ar}")

@router.put("/branches/{branch_id}", summary="تعديل فرع")
async def update_branch(branch_id: uuid.UUID, data: BranchUpdate, svc: SettingsService = Depends(_svc)):
    branch = await svc.update_branch(branch_id, data)
    return ok(data={"id": str(branch.id), "code": branch.code}, message=f"تم تعديل الفرع {branch.name_ar}")

@router.delete("/branches/{branch_id}", summary="تعطيل فرع")
async def delete_branch(branch_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    result = await svc.delete_branch(branch_id)
    return ok(data=result)

# ── Cost Centers ──────────────────────────────
@router.get("/cost-centers", summary="مراكز التكلفة")
async def list_cost_centers(svc: SettingsService = Depends(_svc)):
    ccs = await svc.list_cost_centers()
    return ok(data=[_cc_dict(c) for c in ccs], message=f"{len(ccs)} مركز")

@router.post("/cost-centers", status_code=201, summary="إضافة مركز تكلفة")
async def create_cost_center(data: CostCenterCreate, svc: SettingsService = Depends(_svc)):
    cc = await svc.create_cost_center(data)
    return created(data={"id": str(cc.id), "code": cc.code}, message=f"تم إضافة مركز التكلفة {cc.name_en}")

@router.put("/cost-centers/{cc_id}", summary="تعديل مركز تكلفة")
async def update_cost_center(cc_id: uuid.UUID, data: CostCenterUpdate, svc: SettingsService = Depends(_svc)):
    cc = await svc.update_cost_center(cc_id, data)
    return ok(data={"id": str(cc.id), "code": cc.code}, message=f"تم تعديل مركز التكلفة {cc.name_en}")

# ── Projects ──────────────────────────────────
@router.get("/projects", summary="المشاريع")
async def list_projects(svc: SettingsService = Depends(_svc)):
    projects = await svc.list_projects()
    return ok(data=[_project_dict(p) for p in projects], message=f"{len(projects)} مشروع")

@router.post("/projects", status_code=201, summary="إضافة مشروع")
async def create_project(data: ProjectCreate, svc: SettingsService = Depends(_svc)):
    project = await svc.create_project(data)
    return created(data={"id": str(project.id), "code": project.code}, message=f"تم إضافة المشروع {project.name}")

@router.put("/projects/{project_id}", summary="تعديل مشروع")
async def update_project(project_id: uuid.UUID, data: ProjectUpdate, svc: SettingsService = Depends(_svc)):
    project = await svc.update_project(project_id, data)
    return ok(data={"id": str(project.id), "code": project.code}, message=f"تم تعديل المشروع {project.name}")
