"""
app/modules/settings/router.py
"""
from __future__ import annotations
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import created, ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.settings.service import SettingsService

router = APIRouter(prefix="/settings", tags=["الإعدادات المالية"])

def _svc(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)) -> SettingsService:
    return SettingsService(db, user)

# ── Schemas ───────────────────────────────────
class NameBody(BaseModel):
    code: Optional[str] = None
    name_ar: str
    name_en: Optional[str] = None
    is_active: bool = True

class BranchBody(BaseModel):
    code: str
    name_ar: str
    name_en: Optional[str] = None
    branch_type: Optional[str] = None
    branch_type_id: Optional[uuid.UUID] = None
    address: Optional[str] = None
    country: str = "KSA"
    currency: str = "SAR"
    parent_id: Optional[uuid.UUID] = None
    region_id: Optional[uuid.UUID] = None
    city_id: Optional[uuid.UUID] = None
    city_sequence: int = 1
    is_active: bool = True

class CCBody(BaseModel):
    code: str
    name_en: str
    name_ar: Optional[str] = None
    level: int = 1
    department_code: Optional[str] = None
    department_name: Optional[str] = None
    parent_id: Optional[uuid.UUID] = None
    is_active: bool = True

class ProjectBody(BaseModel):
    name: str
    project_type: Optional[str] = None
    customer_id: Optional[uuid.UUID] = None
    customer_name: Optional[str] = None
    customer_type: Optional[str] = None
    contract_value: float = 0
    budget_value: float = 0
    project_duration: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    revenue_recognition: Optional[str] = None
    bank_facilities_limit: float = 0
    bank_facilities_utilized: float = 0
    bank_facilities_name: Optional[str] = None
    status: str = "active"
    is_active: bool = True

# ── Regions ───────────────────────────────────
@router.get("/regions")
async def list_regions(svc: SettingsService = Depends(_svc)):
    regions = await svc.list_regions()
    return ok(data=[{
        "id": str(r.id), "code": r.code, "name_ar": r.name_ar, "name_en": r.name_en, "is_active": r.is_active,
        "cities": [{"id": str(c.id), "code": c.code, "name_ar": c.name_ar, "name_en": c.name_en, "is_active": c.is_active}
                   for c in sorted(r.cities, key=lambda x: x.code)],
    } for r in regions])

@router.get("/regions/suggest-code")
async def suggest_region_code(svc: SettingsService = Depends(_svc)):
    from sqlalchemy import text as _text
    result = await svc.db.execute(
        _text("SELECT COALESCE(MAX(CAST(code AS INTEGER)), 0) + 1 FROM regions WHERE tenant_id = :tid"),
        {"tid": str(svc.tid)}
    )
    code = str(result.scalar() or 1)
    return ok(data={"suggested_code": code})

@router.post("/regions", status_code=201)
async def create_region(body: NameBody, svc: SettingsService = Depends(_svc)):
    r = await svc.create_region(body.code, body.name_ar, body.name_en)
    return created(data={"id": str(r.id), "code": r.code}, message=f"تم إضافة المنطقة {r.name_ar}")

@router.put("/regions/{region_id}")
async def update_region(region_id: uuid.UUID, body: NameBody, svc: SettingsService = Depends(_svc)):
    r = await svc.update_region(region_id, body.name_ar, body.name_en, body.is_active)
    return ok(data={"id": str(r.id)}, message=f"تم تعديل المنطقة {r.name_ar}")

@router.delete("/regions/{region_id}")
async def delete_region(region_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.delete_region(region_id))

# ── Cities ────────────────────────────────────
@router.get("/cities")
async def list_cities(region_id: Optional[uuid.UUID] = Query(None), svc: SettingsService = Depends(_svc)):
    cities = await svc.list_cities(region_id)
    return ok(data=[{
        "id": str(c.id), "code": c.code, "name_ar": c.name_ar, "name_en": c.name_en,
        "region_id": str(c.region_id), "region_name": c.region.name_ar if c.region else None, "is_active": c.is_active,
    } for c in cities])

@router.get("/cities/suggest-code")
async def suggest_city_code(region_id: uuid.UUID = Query(...), svc: SettingsService = Depends(_svc)):
    code = await svc.suggest_city_code(region_id)
    return ok(data={"suggested_code": code})

@router.post("/cities", status_code=201)
async def create_city(body: NameBody, region_id: uuid.UUID = Query(...), svc: SettingsService = Depends(_svc)):
    c = await svc.create_city(region_id, body.code or None, body.name_ar, body.name_en)
    return created(data={"id": str(c.id), "code": c.code}, message=f"تم إضافة المدينة {c.name_ar}")

@router.put("/cities/{city_id}")
async def update_city(city_id: uuid.UUID, body: NameBody, svc: SettingsService = Depends(_svc)):
    c = await svc.update_city(city_id, body.name_ar, body.name_en, body.is_active)
    return ok(data={"id": str(c.id)}, message=f"تم تعديل المدينة {c.name_ar}")

@router.delete("/cities/{city_id}")
async def delete_city(city_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.delete_city(city_id))

# ── Branch Types ──────────────────────────────
@router.get("/branch-types")
async def list_branch_types(svc: SettingsService = Depends(_svc)):
    bts = await svc.list_branch_types()
    return ok(data=[{"id": str(b.id), "code": b.code, "name_ar": b.name_ar, "name_en": b.name_en, "is_active": b.is_active} for b in bts])

@router.post("/branch-types", status_code=201)
async def create_branch_type(body: NameBody, svc: SettingsService = Depends(_svc)):
    bt = await svc.create_branch_type(body.code, body.name_ar, body.name_en)
    return created(data={"id": str(bt.id)}, message=f"تم إضافة {bt.name_ar}")

@router.put("/branch-types/{bt_id}")
async def update_branch_type(bt_id: uuid.UUID, body: NameBody, svc: SettingsService = Depends(_svc)):
    bt = await svc.update_branch_type(bt_id, body.name_ar, body.name_en, body.is_active)
    return ok(data={"id": str(bt.id)}, message=f"تم تعديل {bt.name_ar}")

@router.delete("/branch-types/{bt_id}")
async def delete_branch_type(bt_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.delete_branch_type(bt_id))

# ── Branch Code Suggestion ────────────────────
@router.get("/branches/suggest-code")
async def suggest_branch_code(
    region_code: str = Query(...),
    city_code: str = Query(...),
    svc: SettingsService = Depends(_svc),
):
    code = await svc.suggest_branch_code(region_code, city_code)
    return ok(data={"suggested_code": code})

# ── Branches ──────────────────────────────────
@router.get("/branches")
async def list_branches(svc: SettingsService = Depends(_svc)):
    branches = await svc.list_branches()
    return ok(data=[{
        "id": str(b.id), "code": b.code, "name_ar": b.name_ar, "name_en": b.name_en,
        "branch_type": b.branch_type, "branch_type_id": str(b.branch_type_id) if b.branch_type_id else None,
        "branch_type_name": b.branch_type_rel.name_ar if b.branch_type_rel else b.branch_type,
        "address": b.address, "country": b.country, "currency": b.currency,
        "parent_id": str(b.parent_id) if b.parent_id else None,
        "region_id": str(b.region_id) if b.region_id else None,
        "region_name": b.region.name_ar if b.region else None,
        "city_id": str(b.city_id) if b.city_id else None,
        "city_name": b.city.name_ar if b.city else None,
        "city_sequence": b.city_sequence, "is_active": b.is_active,
        "deactivated_at": str(b.deactivated_at) if b.deactivated_at else None,
        "deactivated_by": b.deactivated_by,
        "deactivation_reason": b.deactivation_reason,
    } for b in branches], message=f"{len(branches)} فرع")

@router.post("/branches", status_code=201)
async def create_branch(body: BranchBody, svc: SettingsService = Depends(_svc)):
    data = body.model_dump()
    b = await svc.create_branch(data)
    return created(data={"id": str(b.id), "code": b.code}, message=f"تم إضافة الفرع {b.name_ar}")

@router.put("/branches/{branch_id}")
async def update_branch(branch_id: uuid.UUID, body: BranchBody, svc: SettingsService = Depends(_svc)):
    data = body.model_dump(exclude={'code'})
    b = await svc.update_branch(branch_id, data)
    return ok(data={"id": str(b.id)}, message=f"تم تعديل الفرع {b.name_ar}")

class DeactivateBody(BaseModel):
    reason: Optional[str] = None

@router.post("/branches/{branch_id}/deactivate")
async def deactivate_branch(branch_id: uuid.UUID, body: DeactivateBody, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.deactivate_branch(branch_id, body.reason))

@router.post("/branches/{branch_id}/activate")
async def activate_branch(branch_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.activate_branch(branch_id))

@router.delete("/branches/{branch_id}")
async def delete_branch(branch_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.delete_branch(branch_id))

# ── Cost Center Types ────────────────────────
@router.get("/cost-center-types")
async def list_cc_types(svc: SettingsService = Depends(_svc)):
    types = await svc.list_cc_types()
    return ok(data=[{
        "id": str(t.id), "code": t.code, "name_en": t.name_en, "name_ar": t.name_ar,
        "is_system": t.is_system, "is_active": t.is_active, "sort_order": t.sort_order,
    } for t in types])

@router.post("/cost-center-types", status_code=201)
async def create_cc_type(body: NameBody, svc: SettingsService = Depends(_svc)):
    ct = await svc.create_cc_type({"code": body.code, "name_en": body.name_en or body.name_ar, "name_ar": body.name_ar})
    return created(data={"id": str(ct.id)}, message=f"تم إضافة {ct.name_en}")

@router.put("/cost-center-types/{ct_id}")
async def update_cc_type(ct_id: uuid.UUID, body: NameBody, svc: SettingsService = Depends(_svc)):
    ct = await svc.update_cc_type(ct_id, {"name_en": body.name_en or body.name_ar, "name_ar": body.name_ar, "is_active": body.is_active})
    return ok(data={"id": str(ct.id)}, message=f"تم تعديل {ct.name_en}")

@router.delete("/cost-center-types/{ct_id}")
async def delete_cc_type(ct_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.delete_cc_type(ct_id))

# ── Cost Centers ──────────────────────────────
@router.get("/cost-centers/suggest-code")
async def suggest_cc_code(parent_code: str = Query(...), svc: SettingsService = Depends(_svc)):
    code = await svc.suggest_cc_code(parent_code)
    return ok(data={"suggested_code": code})

@router.get("/cost-centers")
async def list_cost_centers(svc: SettingsService = Depends(_svc)):
    ccs = await svc.list_cost_centers()
    return ok(data=[{
        "id": str(c.id), "code": c.code, "name_en": c.name_en, "name_ar": c.name_ar,
        "level": c.level,
        "cost_center_type": c.cost_center_type,
        "cost_center_type_id": str(c.cost_center_type_id) if c.cost_center_type_id else None,
        "cost_center_type_name": c.cc_type_rel.name_en if c.cc_type_rel else c.cost_center_type,
        "department_code": c.department_code, "department_name": c.department_name,
        "parent_id": str(c.parent_id) if c.parent_id else None, "is_active": c.is_active,
        "deactivated_at": str(c.deactivated_at) if c.deactivated_at else None,
        "deactivated_by": c.deactivated_by,
        "deactivation_reason": c.deactivation_reason,
    } for c in ccs])

@router.post("/cost-centers", status_code=201)
async def create_cost_center(body: CCBody, svc: SettingsService = Depends(_svc)):
    cc = await svc.create_cost_center(body.model_dump())
    return created(data={"id": str(cc.id), "code": cc.code}, message=f"تم إضافة مركز التكلفة {cc.name_en}")

@router.put("/cost-centers/{cc_id}")
async def update_cost_center(cc_id: uuid.UUID, body: CCBody, svc: SettingsService = Depends(_svc)):
    cc = await svc.update_cost_center(cc_id, body.model_dump(exclude={'code'}))
    return ok(data={"id": str(cc.id)}, message=f"تم تعديل مركز التكلفة {cc.name_en}")

@router.post("/cost-centers/{cc_id}/deactivate")
async def deactivate_cc(cc_id: uuid.UUID, body: DeactivateBody, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.deactivate_cost_center(cc_id, body.reason))

@router.post("/cost-centers/{cc_id}/activate")
async def activate_cc(cc_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.activate_cost_center(cc_id))

@router.delete("/cost-centers/{cc_id}")
async def delete_cost_center(cc_id: uuid.UUID, svc: SettingsService = Depends(_svc)):
    return ok(data=await svc.delete_cost_center(cc_id))

# ── Projects ──────────────────────────────────
# ── JE Types ─────────────────────────────────────────
@router.get("/je-types")
async def list_je_types(svc: SettingsService = Depends(_svc)):
    from sqlalchemy import text as _text
    result = await svc.db.execute(
        _text("SELECT id, code, name_en, name_ar, is_system, is_active, sort_order "
              "FROM je_types WHERE tenant_id = :tid AND is_active = true ORDER BY sort_order"),
        {"tid": str(svc.tid)}
    )
    rows = result.fetchall()
    return ok(data=[{
        "id": str(r[0]), "code": r[1], "name_en": r[2], "name_ar": r[3],
        "is_system": r[4], "is_active": r[5], "sort_order": r[6],
    } for r in rows])

@router.post("/je-types", status_code=201)
async def create_je_type(body: NameBody, svc: SettingsService = Depends(_svc)):
    import uuid as _uuid
    from sqlalchemy import text as _text
    svc.user.require("can_manage_coa")
    await svc.db.execute(
        _text("INSERT INTO je_types (id, tenant_id, code, name_en, name_ar, is_system, is_active, sort_order, created_by) "
              "VALUES (:id, :tid, :code, :name_en, :name_ar, false, true, 99, :by) "
              "ON CONFLICT (tenant_id, code) DO NOTHING"),
        {"id": str(_uuid.uuid4()), "tid": str(svc.tid), "code": body.code,
         "name_en": body.name_en or body.name_ar, "name_ar": body.name_ar, "by": svc.user.email}
    )
    return created(data={"code": body.code}, message=f"تم إضافة النوع {body.code}")

@router.put("/je-types/{je_type_id}")
async def update_je_type(je_type_id: uuid.UUID, body: NameBody, svc: SettingsService = Depends(_svc)):
    from sqlalchemy import text as _text
    svc.user.require("can_manage_coa")
    await svc.db.execute(
        _text("UPDATE je_types SET name_ar = :name_ar, name_en = :name_en, "
              "is_active = :active, updated_at = now() "
              "WHERE id = :id AND tenant_id = :tid AND is_system = false"),
        {"id": str(je_type_id), "tid": str(svc.tid),
         "name_ar": body.name_ar, "name_en": body.name_en or body.name_ar, "active": body.is_active}
    )
    return ok(data={"id": str(je_type_id)}, message="تم التعديل")


@router.get("/projects")
async def list_projects(svc: SettingsService = Depends(_svc)):
    projects = await svc.list_projects()
    return ok(data=[{
        "id": str(p.id), "code": p.code, "name": p.name, "project_type": p.project_type,
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
    } for p in projects])

@router.post("/projects", status_code=201)
async def create_project(body: ProjectBody, svc: SettingsService = Depends(_svc)):
    p = await svc.create_project(body.model_dump())
    return created(data={"id": str(p.id), "code": p.code}, message=f"تم إضافة المشروع {p.name}")

@router.put("/projects/{project_id}")
async def update_project(project_id: uuid.UUID, body: ProjectBody, svc: SettingsService = Depends(_svc)):
    p = await svc.update_project(project_id, body.model_dump())
    return ok(data={"id": str(p.id)}, message=f"تم تعديل المشروع {p.name}")
