"""
app/modules/settings/service.py
"""
from __future__ import annotations
import uuid
from typing import List
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.exceptions import NotFoundError, DuplicateError, ValidationError
from app.core.tenant import CurrentUser
from app.modules.settings.models import Branch, BranchType, CostCenter, Project, Region, City


class SettingsService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db = db
        self.user = user
        self.tid = user.tenant_id

    # ══════════════════════════════════════════════
    # Regions
    # ══════════════════════════════════════════════
    async def list_regions(self) -> List[Region]:
        result = await self.db.execute(
            select(Region).options(selectinload(Region.cities))
            .where(Region.tenant_id == self.tid, Region.is_active == True)
            .order_by(Region.code)
        )
        return result.scalars().all()

    async def create_region(self, code: str, name_ar: str, name_en: str = None) -> Region:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(Region).where(Region.tenant_id == self.tid, Region.code == code)
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("منطقة", "code", code)
        region = Region(tenant_id=self.tid, code=code, name_ar=name_ar, name_en=name_en, created_by=self.user.email)
        self.db.add(region)
        await self.db.flush()
        return region

    async def update_region(self, region_id: uuid.UUID, name_ar: str, name_en: str = None, is_active: bool = True) -> Region:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(Region).where(Region.tenant_id == self.tid, Region.id == region_id))
        region = result.scalar_one_or_none()
        if not region: raise NotFoundError("المنطقة", region_id)
        region.name_ar = name_ar
        if name_en is not None: region.name_en = name_en
        region.is_active = is_active
        await self.db.flush()
        return region

    async def delete_region(self, region_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(Region).where(Region.tenant_id == self.tid, Region.id == region_id))
        region = result.scalar_one_or_none()
        if not region: raise NotFoundError("المنطقة", region_id)
        region.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل المنطقة {region.name_ar}"}

    # ══════════════════════════════════════════════
    # Cities
    # ══════════════════════════════════════════════
    async def list_cities(self, region_id: uuid.UUID = None) -> List[City]:
        q = select(City).options(selectinload(City.region)).where(City.tenant_id == self.tid, City.is_active == True)
        if region_id:
            q = q.where(City.region_id == region_id)
        q = q.order_by(City.code)
        result = await self.db.execute(q)
        return result.scalars().all()

    async def create_city(self, region_id: uuid.UUID, code: str, name_ar: str, name_en: str = None) -> City:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(City).where(City.tenant_id == self.tid, City.code == code)
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("مدينة", "code", code)
        city = City(tenant_id=self.tid, region_id=region_id, code=code, name_ar=name_ar, name_en=name_en, created_by=self.user.email)
        self.db.add(city)
        await self.db.flush()
        return city

    async def update_city(self, city_id: uuid.UUID, name_ar: str, name_en: str = None, is_active: bool = True) -> City:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(City).where(City.tenant_id == self.tid, City.id == city_id))
        city = result.scalar_one_or_none()
        if not city: raise NotFoundError("المدينة", city_id)
        city.name_ar = name_ar
        if name_en is not None: city.name_en = name_en
        city.is_active = is_active
        await self.db.flush()
        return city

    async def delete_city(self, city_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(City).where(City.tenant_id == self.tid, City.id == city_id))
        city = result.scalar_one_or_none()
        if not city: raise NotFoundError("المدينة", city_id)
        city.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل المدينة {city.name_ar}"}

    # ══════════════════════════════════════════════
    # Branch Types
    # ══════════════════════════════════════════════
    async def list_branch_types(self) -> List[BranchType]:
        result = await self.db.execute(
            select(BranchType).where(BranchType.tenant_id == self.tid, BranchType.is_active == True).order_by(BranchType.code)
        )
        return result.scalars().all()

    async def create_branch_type(self, code: str, name_ar: str, name_en: str = None) -> BranchType:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(BranchType).where(BranchType.tenant_id == self.tid, BranchType.code == code)
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("نوع فرع", "code", code)
        bt = BranchType(tenant_id=self.tid, code=code, name_ar=name_ar, name_en=name_en, created_by=self.user.email)
        self.db.add(bt)
        await self.db.flush()
        return bt

    async def update_branch_type(self, bt_id: uuid.UUID, name_ar: str, name_en: str = None, is_active: bool = True) -> BranchType:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(BranchType).where(BranchType.tenant_id == self.tid, BranchType.id == bt_id))
        bt = result.scalar_one_or_none()
        if not bt: raise NotFoundError("نوع الفرع", bt_id)
        bt.name_ar = name_ar
        if name_en is not None: bt.name_en = name_en
        bt.is_active = is_active
        await self.db.flush()
        return bt

    async def delete_branch_type(self, bt_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(BranchType).where(BranchType.tenant_id == self.tid, BranchType.id == bt_id))
        bt = result.scalar_one_or_none()
        if not bt: raise NotFoundError("نوع الفرع", bt_id)
        bt.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل {bt.name_ar}"}

    # ══════════════════════════════════════════════
    # Branches
    # ══════════════════════════════════════════════
    async def list_branches(self) -> List[Branch]:
        result = await self.db.execute(
            select(Branch).options(
                selectinload(Branch.region),
                selectinload(Branch.city),
                selectinload(Branch.branch_type_rel),
            )
            .where(Branch.tenant_id == self.tid)
            .order_by(Branch.code)
        )
        return result.scalars().all()

    async def suggest_branch_code(self, region_code: str, city_code: str) -> str:
        """توليد كود الفرع تلقائياً: منطقة(1) + مدينة(2) + تسلسل(1)"""
        prefix = f"{region_code}{city_code}"
        result = await self.db.execute(
            select(Branch.code)
            .where(Branch.tenant_id == self.tid, Branch.code.like(f"{prefix}%"))
            .order_by(Branch.code.desc())
        )
        codes = result.scalars().all()
        if not codes:
            return f"{prefix}1"
        last = codes[0]
        try:
            seq = int(last[len(prefix):]) + 1
        except (ValueError, IndexError):
            seq = len(codes) + 1
        return f"{prefix}{seq}"

    async def create_branch(self, data: dict) -> Branch:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(Branch).where(Branch.tenant_id == self.tid, Branch.code == data['code'])
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("فرع", "code", data['code'])
        branch = Branch(tenant_id=self.tid, created_by=self.user.email, **data)
        self.db.add(branch)
        await self.db.flush()
        return branch

    async def update_branch(self, branch_id: uuid.UUID, data: dict) -> Branch:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(Branch).where(Branch.tenant_id == self.tid, Branch.id == branch_id))
        branch = result.scalar_one_or_none()
        if not branch: raise NotFoundError("الفرع", branch_id)
        for k, v in data.items():
            setattr(branch, k, v)
        await self.db.flush()
        return branch

    async def delete_branch(self, branch_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(Branch).where(Branch.tenant_id == self.tid, Branch.id == branch_id))
        branch = result.scalar_one_or_none()
        if not branch: raise NotFoundError("الفرع", branch_id)
        branch.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل الفرع {branch.name_ar}"}

    # ══════════════════════════════════════════════
    # Cost Center Types
    # ══════════════════════════════════════════════
    async def list_cc_types(self) -> list:
        from app.modules.settings.models import CostCenterType
        result = await self.db.execute(
            select(CostCenterType)
            .where(CostCenterType.tenant_id == self.tid, CostCenterType.is_active == True)
            .order_by(CostCenterType.sort_order, CostCenterType.code)
        )
        return result.scalars().all()

    async def create_cc_type(self, data: dict) -> object:
        from app.modules.settings.models import CostCenterType
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(CostCenterType).where(CostCenterType.tenant_id == self.tid, CostCenterType.code == data['code'])
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("نوع مركز تكلفة", "code", data['code'])
        ct = CostCenterType(tenant_id=self.tid, is_system=False, created_by=self.user.email, **data)
        self.db.add(ct)
        await self.db.flush()
        return ct

    async def update_cc_type(self, ct_id: uuid.UUID, data: dict) -> object:
        from app.modules.settings.models import CostCenterType
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(CostCenterType).where(CostCenterType.tenant_id == self.tid, CostCenterType.id == ct_id))
        ct = result.scalar_one_or_none()
        if not ct: raise NotFoundError("نوع مركز التكلفة", ct_id)
        if ct.is_system and data.get('code'):
            raise ValidationError("لا يمكن تعديل كود النوع الأساسي")
        for k, v in data.items():
            setattr(ct, k, v)
        await self.db.flush()
        return ct

    async def delete_cc_type(self, ct_id: uuid.UUID) -> dict:
        from app.modules.settings.models import CostCenterType
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(CostCenterType).where(CostCenterType.tenant_id == self.tid, CostCenterType.id == ct_id))
        ct = result.scalar_one_or_none()
        if not ct: raise NotFoundError("نوع مركز التكلفة", ct_id)
        if ct.is_system:
            raise ValidationError("لا يمكن حذف النوع الأساسي — يمكن تعطيله فقط")
        ct.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل {ct.name_en}"}

    # ══════════════════════════════════════════════
    # Cost Centers
    # ══════════════════════════════════════════════
    async def list_cost_centers(self) -> List[CostCenter]:
        result = await self.db.execute(
            select(CostCenter)
            .options(selectinload(CostCenter.cc_type_rel))
            .where(CostCenter.tenant_id == self.tid)
            .order_by(CostCenter.code)
        )
        return result.scalars().all()

    async def suggest_cc_code(self, parent_code: str) -> str:
        """توليد كود مركز التكلفة تلقائياً تحت القسم الأب"""
        result = await self.db.execute(
            select(CostCenter.code)
            .where(CostCenter.tenant_id == self.tid, CostCenter.level == 2,
                   CostCenter.parent_id.in_(
                       select(CostCenter.id).where(CostCenter.tenant_id == self.tid, CostCenter.code == parent_code)
                   ))
            .order_by(CostCenter.code.desc())
        )
        codes = result.scalars().all()
        parent_num = int(parent_code)
        if not codes:
            return str(parent_num + 1)
        last_num = max(int(c) for c in codes if c.isdigit())
        return str(last_num + 1)

    async def create_cost_center(self, data: dict) -> CostCenter:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(CostCenter).where(CostCenter.tenant_id == self.tid, CostCenter.code == data['code'])
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("مركز تكلفة", "code", data['code'])
        cc = CostCenter(tenant_id=self.tid, created_by=self.user.email, **data)
        self.db.add(cc)
        await self.db.flush()
        return cc

    async def update_cost_center(self, cc_id: uuid.UUID, data: dict) -> CostCenter:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(CostCenter).where(CostCenter.tenant_id == self.tid, CostCenter.id == cc_id))
        cc = result.scalar_one_or_none()
        if not cc: raise NotFoundError("مركز التكلفة", cc_id)
        for k, v in data.items():
            setattr(cc, k, v)
        await self.db.flush()
        return cc

    async def delete_cost_center(self, cc_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(CostCenter).where(CostCenter.tenant_id == self.tid, CostCenter.id == cc_id))
        cc = result.scalar_one_or_none()
        if not cc: raise NotFoundError("مركز التكلفة", cc_id)
        cc.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل مركز التكلفة {cc.name_en}"}

    # ══════════════════════════════════════════════
    # Projects
    # ══════════════════════════════════════════════
    async def list_projects(self) -> List[Project]:
        result = await self.db.execute(
            select(Project).where(Project.tenant_id == self.tid).order_by(Project.code)
        )
        return result.scalars().all()

    async def create_project(self, data: dict) -> Project:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(func.coalesce(func.max(Project.code), 0)).where(Project.tenant_id == self.tid)
        )
        next_code = (result.scalar() or 0) + 1
        project = Project(tenant_id=self.tid, code=next_code, created_by=self.user.email, **data)
        self.db.add(project)
        await self.db.flush()
        return project

    async def update_project(self, project_id: uuid.UUID, data: dict) -> Project:
        self.user.require("can_manage_coa")
        result = await self.db.execute(select(Project).where(Project.tenant_id == self.tid, Project.id == project_id))
        project = result.scalar_one_or_none()
        if not project: raise NotFoundError("المشروع", project_id)
        for k, v in data.items():
            setattr(project, k, v)
        await self.db.flush()
        return project
