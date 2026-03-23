"""
app/modules/settings/service.py
"""
from __future__ import annotations
import uuid
from typing import List
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.exceptions import NotFoundError, DuplicateError
from app.core.tenant import CurrentUser
from app.modules.settings.models import Branch, CostCenter, Project, Region, City
from app.modules.settings.schemas import BranchCreate, BranchUpdate, CostCenterCreate, CostCenterUpdate, ProjectCreate, ProjectUpdate


class SettingsService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db = db
        self.user = user
        self.tid = user.tenant_id

    # ── Regions & Cities ──────────────────────
    async def list_regions(self) -> List[Region]:
        result = await self.db.execute(
            select(Region).options(selectinload(Region.cities))
            .where(Region.tenant_id == self.tid, Region.is_active == True)
            .order_by(Region.code)
        )
        return result.scalars().all()

    # ── Branches ──────────────────────────────
    async def list_branches(self) -> List[Branch]:
        result = await self.db.execute(
            select(Branch)
            .options(selectinload(Branch.region), selectinload(Branch.city))
            .where(Branch.tenant_id == self.tid)
            .order_by(Branch.code)
        )
        return result.scalars().all()

    async def create_branch(self, data: BranchCreate) -> Branch:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(Branch).where(Branch.tenant_id == self.tid, Branch.code == data.code)
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("فرع", "code", data.code)
        branch = Branch(tenant_id=self.tid, created_by=self.user.email, **data.model_dump())
        self.db.add(branch)
        await self.db.flush()
        return branch

    async def update_branch(self, branch_id: uuid.UUID, data: BranchUpdate) -> Branch:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(Branch).where(Branch.tenant_id == self.tid, Branch.id == branch_id)
        )
        branch = result.scalar_one_or_none()
        if not branch:
            raise NotFoundError("الفرع", branch_id)
        for k, v in data.model_dump(exclude_unset=True).items():
            setattr(branch, k, v)
        await self.db.flush()
        return branch

    async def delete_branch(self, branch_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(Branch).where(Branch.tenant_id == self.tid, Branch.id == branch_id)
        )
        branch = result.scalar_one_or_none()
        if not branch:
            raise NotFoundError("الفرع", branch_id)
        branch.is_active = False
        await self.db.flush()
        return {"message": f"تم تعطيل الفرع {branch.name_ar}"}

    # ── Cost Centers ──────────────────────────
    async def list_cost_centers(self) -> List[CostCenter]:
        result = await self.db.execute(
            select(CostCenter)
            .where(CostCenter.tenant_id == self.tid)
            .order_by(CostCenter.code)
        )
        return result.scalars().all()

    async def create_cost_center(self, data: CostCenterCreate) -> CostCenter:
        self.user.require("can_manage_coa")
        exists = await self.db.execute(
            select(CostCenter).where(CostCenter.tenant_id == self.tid, CostCenter.code == data.code)
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("مركز تكلفة", "code", data.code)
        cc = CostCenter(tenant_id=self.tid, created_by=self.user.email, **data.model_dump())
        self.db.add(cc)
        await self.db.flush()
        return cc

    async def update_cost_center(self, cc_id: uuid.UUID, data: CostCenterUpdate) -> CostCenter:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(CostCenter).where(CostCenter.tenant_id == self.tid, CostCenter.id == cc_id)
        )
        cc = result.scalar_one_or_none()
        if not cc:
            raise NotFoundError("مركز التكلفة", cc_id)
        for k, v in data.model_dump(exclude_unset=True).items():
            setattr(cc, k, v)
        await self.db.flush()
        return cc

    # ── Projects ──────────────────────────────
    async def list_projects(self) -> List[Project]:
        result = await self.db.execute(
            select(Project).where(Project.tenant_id == self.tid).order_by(Project.code)
        )
        return result.scalars().all()

    async def create_project(self, data: ProjectCreate) -> Project:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(func.coalesce(func.max(Project.code), 0))
            .where(Project.tenant_id == self.tid)
        )
        next_code = (result.scalar() or 0) + 1
        project = Project(
            tenant_id=self.tid,
            code=next_code,
            created_by=self.user.email,
            **data.model_dump()
        )
        self.db.add(project)
        await self.db.flush()
        return project

    async def update_project(self, project_id: uuid.UUID, data: ProjectUpdate) -> Project:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(Project).where(Project.tenant_id == self.tid, Project.id == project_id)
        )
        project = result.scalar_one_or_none()
        if not project:
            raise NotFoundError("المشروع", project_id)
        for k, v in data.model_dump(exclude_unset=True).items():
            setattr(project, k, v)
        await self.db.flush()
        return project
