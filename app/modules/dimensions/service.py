"""
app/modules/dimensions/service.py
"""
from __future__ import annotations

import uuid
from typing import List

import structlog
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError, ValidationError, DuplicateError
from app.core.tenant import CurrentUser
from app.modules.dimensions.models import Dimension, DimensionValue
from app.modules.dimensions.schemas import (
    DimensionCreate, DimensionUpdate,
    DimensionValueCreate, DimensionValueUpdate,
)

logger = structlog.get_logger(__name__)


class DimensionService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db = db
        self.user = user
        self.tid = user.tenant_id

    # ══════════════════════════════════════════════
    # Dimensions CRUD
    # ══════════════════════════════════════════════
    async def list_dimensions(self, active_only: bool = True) -> List[Dimension]:
        from sqlalchemy.orm import selectinload
        q = select(Dimension).options(
            selectinload(Dimension.values)
        ).where(Dimension.tenant_id == self.tid)
        if active_only:
            q = q.where(Dimension.is_active == True)
        q = q.order_by(Dimension.sort_order, Dimension.code)
        result = await self.db.execute(q)
        return result.scalars().all()

    async def get_dimension(self, dim_id: uuid.UUID) -> Dimension:
        from sqlalchemy.orm import selectinload
        result = await self.db.execute(
            select(Dimension).options(
                selectinload(Dimension.values)
            ).where(
                Dimension.tenant_id == self.tid,
                Dimension.id == dim_id,
            )
        )
        dim = result.scalar_one_or_none()
        if not dim:
            raise NotFoundError("البُعد", dim_id)
        return dim

    async def create_dimension(self, data: DimensionCreate) -> Dimension:
        self.user.require("can_manage_coa")

        # تحقق من عدم التكرار
        exists = await self.db.execute(
            select(Dimension).where(
                Dimension.tenant_id == self.tid,
                Dimension.code == data.code,
            )
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("بُعد", "code", data.code)

        dim = Dimension(
            tenant_id=self.tid,
            code=data.code,
            name_ar=data.name_ar,
            name_en=data.name_en,
            classification=data.classification,
            is_required=data.is_required,
            is_system=False,
            is_active=True,
            sort_order=data.sort_order,
            created_by=self.user.email,
        )
        self.db.add(dim)
        await self.db.flush()
        return dim

    async def update_dimension(self, dim_id: uuid.UUID, data: DimensionUpdate) -> Dimension:
        self.user.require("can_manage_coa")
        dim = await self.get_dimension(dim_id)

        update_data = data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(dim, field, value)

        try:
            dim.updated_by = self.user.email
        except Exception:
            pass
        await self.db.flush()
        return dim

    async def delete_dimension(self, dim_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        dim = await self.get_dimension(dim_id)

        if dim.is_system:
            raise ValidationError("لا يمكن حذف البُعد الأساسي — يمكن تعطيله فقط")

        await self.db.execute(
            delete(Dimension).where(Dimension.id == dim_id)
        )
        await self.db.flush()
        return {"message": f"تم حذف البُعد {dim.name_ar}"}

    # ══════════════════════════════════════════════
    # Dimension Values CRUD
    # ══════════════════════════════════════════════
    async def list_values(self, dim_id: uuid.UUID) -> List[DimensionValue]:
        await self.get_dimension(dim_id)  # verify exists
        result = await self.db.execute(
            select(DimensionValue).where(
                DimensionValue.tenant_id == self.tid,
                DimensionValue.dimension_id == dim_id,
                DimensionValue.is_active == True,
            ).order_by(DimensionValue.code)
        )
        return result.scalars().all()

    async def create_value(
        self, dim_id: uuid.UUID, data: DimensionValueCreate
    ) -> DimensionValue:
        self.user.require("can_manage_coa")
        await self.get_dimension(dim_id)

        # تحقق من عدم التكرار
        exists = await self.db.execute(
            select(DimensionValue).where(
                DimensionValue.tenant_id == self.tid,
                DimensionValue.dimension_id == dim_id,
                DimensionValue.code == data.code,
            )
        )
        if exists.scalar_one_or_none():
            raise DuplicateError("قيمة البُعد", "code", data.code)

        val = DimensionValue(
            tenant_id=self.tid,
            dimension_id=dim_id,
            code=data.code,
            name_ar=data.name_ar,
            name_en=data.name_en,
            is_active=True,
            created_by=self.user.email,
        )
        self.db.add(val)
        await self.db.flush()
        return val

    async def update_value(
        self, dim_id: uuid.UUID, value_id: uuid.UUID, data: DimensionValueUpdate
    ) -> DimensionValue:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(DimensionValue).where(
                DimensionValue.tenant_id == self.tid,
                DimensionValue.id == value_id,
                DimensionValue.dimension_id == dim_id,
            )
        )
        val = result.scalar_one_or_none()
        if not val:
            raise NotFoundError("قيمة البُعد", value_id)

        update_data = data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(val, field, value)

        try:
            val.updated_by = self.user.email
        except Exception:
            pass
        await self.db.flush()
        return val

    async def delete_value(self, dim_id: uuid.UUID, value_id: uuid.UUID) -> dict:
        self.user.require("can_manage_coa")
        result = await self.db.execute(
            select(DimensionValue).where(
                DimensionValue.tenant_id == self.tid,
                DimensionValue.id == value_id,
                DimensionValue.dimension_id == dim_id,
            )
        )
        val = result.scalar_one_or_none()
        if not val:
            raise NotFoundError("قيمة البُعد", value_id)

        await self.db.execute(
            delete(DimensionValue).where(DimensionValue.id == value_id)
        )
        await self.db.flush()
        return {"message": f"تم حذف القيمة {val.name_ar}"}
