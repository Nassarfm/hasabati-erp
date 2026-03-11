"""app/modules/assets/router.py — Fixed Assets API (14 endpoints)"""
from __future__ import annotations
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.response import created, ok, paginated
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db
from app.modules.assets.service import AssetsService

router = APIRouter(prefix="/assets", tags=["الأصول الثابتة"])


def _svc(db: AsyncSession = Depends(get_db), user: CurrentUser = Depends(get_current_user)):
    return AssetsService(db, user)


@router.post("", status_code=201, summary="تسجيل أصل ثابت جديد")
async def register_asset(data: dict, svc: AssetsService = Depends(_svc)):
    """
    يسجّل أصلاً ثابتاً وينشئ قيد الشراء:
    DR أصل ثابت / CR ذمم موردين
    """
    asset = await svc.register_asset(data)
    return created(data=asset.to_dict(),
                   message=f"تم تسجيل الأصل {asset.asset_number} — {asset.name_ar}")


@router.get("", summary="قائمة الأصول الثابتة")
async def list_assets(
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100),
    svc: AssetsService = Depends(_svc),
):
    items, total = await svc.list_assets(offset=(page-1)*page_size, limit=page_size)
    return paginated(items=[a.to_dict() for a in items], total=total, page=page, page_size=page_size)


@router.get("/summary", summary="ملخص الأصول الثابتة")
async def asset_summary(svc: AssetsService = Depends(_svc)):
    """إجمالي التكلفة + مجمع الإهلاك + صافي القيمة الدفترية"""
    return ok(data=await svc.get_asset_summary())


@router.get("/{asset_id}", summary="تفاصيل أصل ثابت")
async def get_asset(asset_id: uuid.UUID, svc: AssetsService = Depends(_svc)):
    a = await svc.get_asset(asset_id)
    return ok(data=a.to_dict())


@router.post("/depreciation/run", summary="تشغيل الإهلاك الشهري")
async def run_depreciation(
    year:     int  = Query(..., ge=2020, le=2099),
    month:    int  = Query(..., ge=1, le=12),
    dep_date: date = Query(...),
    svc: AssetsService = Depends(_svc),
):
    """
    يحسب ويرحّل الإهلاك الشهري لجميع الأصول النشطة.
    ينشئ قيداً لكل أصل:
    DR مصروف إهلاك / CR مجمع إهلاك
    """
    result = await svc.run_depreciation(year, month, dep_date)
    return ok(
        data=result,
        message=f"✅ إهلاك {year}/{month:02d} — {result['processed']} أصل — إجمالي: {result['total_depreciation']:,.3f} ر.س",
    )


@router.post("/{asset_id}/dispose", summary="التخلص من أصل ثابت")
async def dispose_asset(
    asset_id:      uuid.UUID,
    disposal_date: date     = Query(...),
    disposal_type: str      = Query(default="sale", pattern="^(sale|scrap)$"),
    sale_price:    float    = Query(default=0.0, ge=0),
    notes:         Optional[str] = Query(default=None),
    svc: AssetsService = Depends(_svc),
):
    """
    يتخلص من أصل ثابت (بيع أو إتلاف):
    DR مجمع الإهلاك + DR/CR ربح/خسارة بيع
    CR الأصل الثابت
    """
    disposal = await svc.dispose_asset(
        asset_id=asset_id,
        disposal_date=disposal_date,
        disposal_type=disposal_type,
        sale_price=Decimal(str(sale_price)),
        notes=notes,
    )
    gain_label = "ربح" if disposal.gain_loss >= 0 else "خسارة"
    return ok(
        data=disposal.to_dict(),
        message=f"✅ تم التخلص من الأصل | {gain_label}: {abs(float(disposal.gain_loss)):,.3f} ر.س | قيد: {disposal.je_serial}",
    )
