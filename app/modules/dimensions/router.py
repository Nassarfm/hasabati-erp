"""
app/modules/dimensions/router.py
══════════════════════════════════════════════════════════
Dimensions API — 9 endpoints

GET    /dimensions              قائمة الأبعاد
POST   /dimensions              إنشاء بُعد جديد
GET    /dimensions/{id}         تفاصيل بُعد
PUT    /dimensions/{id}         تعديل بُعد
DELETE /dimensions/{id}         حذف بُعد

GET    /dimensions/{id}/values         قائمة القيم
POST   /dimensions/{id}/values         إضافة قيمة
PUT    /dimensions/{id}/values/{vid}   تعديل قيمة
DELETE /dimensions/{id}/values/{vid}   حذف قيمة
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import created, ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.dimensions.schemas import (
    DimensionCreate, DimensionUpdate,
    DimensionValueCreate, DimensionValueUpdate,
)
from app.modules.dimensions.service import DimensionService

router = APIRouter(prefix="/dimensions", tags=["الأبعاد المحاسبية"])


def _svc(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
) -> DimensionService:
    return DimensionService(db, user)


# ══════════════════════════════════════════════
# Dimensions
# ══════════════════════════════════════════════
@router.get("", summary="قائمة الأبعاد")
async def list_dimensions(
    active_only: bool = Query(default=True),
    svc: DimensionService = Depends(_svc),
):
    dims = await svc.list_dimensions(active_only=active_only)
    return ok(
        data=[{
            "id":             str(d.id),
            "code":           d.code,
            "name_ar":        d.name_ar,
            "name_en":        d.name_en,
            "classification": d.classification,
            "is_required":    d.is_required,
            "is_system":      d.is_system,
            "is_active":      d.is_active,
            "sort_order":     d.sort_order,
            "values_count":   len(d.values),
            "values": [{
                "id":       str(v.id),
                "code":     v.code,
                "name_ar":  v.name_ar,
                "name_en":  v.name_en,
                "is_active": v.is_active,
            } for v in d.values],
        } for d in dims],
        message=f"{len(dims)} بُعد",
    )


@router.post("", status_code=201, summary="إنشاء بُعد جديد")
async def create_dimension(
    data: DimensionCreate,
    svc: DimensionService = Depends(_svc),
):
    dim = await svc.create_dimension(data)
    return created(
        data={"id": str(dim.id), "code": dim.code, "name_ar": dim.name_ar},
        message=f"تم إنشاء البُعد {dim.name_ar}",
    )


@router.get("/{dim_id}", summary="تفاصيل بُعد")
async def get_dimension(
    dim_id: uuid.UUID,
    svc: DimensionService = Depends(_svc),
):
    dim = await svc.get_dimension(dim_id)
    return ok(data={
        "id":             str(dim.id),
        "code":           dim.code,
        "name_ar":        dim.name_ar,
        "name_en":        dim.name_en,
        "classification": dim.classification,
        "is_required":    dim.is_required,
        "is_system":      dim.is_system,
        "is_active":      dim.is_active,
        "sort_order":     dim.sort_order,
        "values": [{
            "id":       str(v.id),
            "code":     v.code,
            "name_ar":  v.name_ar,
            "name_en":  v.name_en,
            "is_active": v.is_active,
        } for v in dim.values],
    })


@router.put("/{dim_id}", summary="تعديل بُعد")
async def update_dimension(
    dim_id: uuid.UUID,
    data: DimensionUpdate,
    svc: DimensionService = Depends(_svc),
):
    dim = await svc.update_dimension(dim_id, data)
    return ok(
        data={"id": str(dim.id), "code": dim.code, "name_ar": dim.name_ar},
        message=f"تم تعديل البُعد {dim.name_ar}",
    )


@router.delete("/{dim_id}", summary="حذف بُعد")
async def delete_dimension(
    dim_id: uuid.UUID,
    svc: DimensionService = Depends(_svc),
):
    result = await svc.delete_dimension(dim_id)
    return ok(data=result)


# ══════════════════════════════════════════════
# Dimension Values
# ══════════════════════════════════════════════
@router.get("/{dim_id}/values", summary="قائمة قيم البُعد")
async def list_values(
    dim_id: uuid.UUID,
    svc: DimensionService = Depends(_svc),
):
    values = await svc.list_values(dim_id)
    return ok(
        data=[{
            "id":       str(v.id),
            "code":     v.code,
            "name_ar":  v.name_ar,
            "name_en":  v.name_en,
            "is_active": v.is_active,
        } for v in values],
        message=f"{len(values)} قيمة",
    )


@router.post("/{dim_id}/values", status_code=201, summary="إضافة قيمة")
async def create_value(
    dim_id: uuid.UUID,
    data: DimensionValueCreate,
    svc: DimensionService = Depends(_svc),
):
    val = await svc.create_value(dim_id, data)
    return created(
        data={"id": str(val.id), "code": val.code, "name_ar": val.name_ar},
        message=f"تم إضافة القيمة {val.name_ar}",
    )


@router.put("/{dim_id}/values/{value_id}", summary="تعديل قيمة")
async def update_value(
    dim_id: uuid.UUID,
    value_id: uuid.UUID,
    data: DimensionValueUpdate,
    svc: DimensionService = Depends(_svc),
):
    val = await svc.update_value(dim_id, value_id, data)
    return ok(
        data={"id": str(val.id), "code": val.code, "name_ar": val.name_ar},
        message=f"تم تعديل القيمة {val.name_ar}",
    )


@router.delete("/{dim_id}/values/{value_id}", summary="حذف قيمة")
async def delete_value(
    dim_id: uuid.UUID,
    value_id: uuid.UUID,
    svc: DimensionService = Depends(_svc),
):
    result = await svc.delete_value(dim_id, value_id)
    return ok(data=result)
