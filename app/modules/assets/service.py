"""
app/modules/assets/service.py
══════════════════════════════════════════════════════════
Fixed Assets Service.

Features:
  register_asset()      ← تسجيل أصل جديد + قيد الشراء
  run_depreciation()    ← إهلاك شهري لجميع الأصول
  dispose_asset()       ← التخلص + قيد الربح/الخسارة

Depreciation JE (Monthly):
  DR مصروف الإهلاك   (dep_amount)
  CR مجمع الإهلاك    (dep_amount)

Disposal JE:
  DR مجمع الإهلاك    (accumulated_dep)
  DR خسارة بيع أصول  (إذا خسارة)
  CR الأصل الثابت    (purchase_cost)
  CR ربح بيع أصول    (إذا ربح)
  DR نقدية / بنك     (sale_price) ← إذا بيع
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

import structlog
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.exceptions import (
    DuplicateError, InvalidStateError, NotFoundError, ValidationError,
)
from app.core.tenant import CurrentUser
from app.db.transactions import atomic_transaction
from app.modules.assets.models import (
    AssetDisposal, AssetStatus, DepreciationSchedule, FixedAsset,
)
from app.repositories.base_repo import BaseRepository
from app.services.numbering.series_service import NumberSeriesService
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import ACC

logger = structlog.get_logger(__name__)

PREC = Decimal("0.001")

# Default account map per category
CATEGORY_ACCOUNTS = {
    "buildings":  ("1502", "1551", "6501"),
    "machinery":  ("1503", "1552", "6502"),
    "vehicles":   ("1504", "1553", "6503"),
    "furniture":  ("1505", "1554", "6504"),
    "computers":  ("1506", "1555", "6505"),
    "land":       ("1501", "1501", "6501"),   # land not depreciated
    "intangible": ("1601", "1601", "6506"),
    "other":      ("1503", "1552", "6502"),
}


class FixedAssetRepository(BaseRepository[FixedAsset]):
    model = FixedAsset

    async def get_active(self) -> List[FixedAsset]:
        result = await self.db.execute(
            self._base_query().where(FixedAsset.status == AssetStatus.ACTIVE)
        )
        return list(result.scalars().all())

    async def get_by_number(self, number: str) -> Optional[FixedAsset]:
        result = await self.db.execute(
            self._base_query().where(FixedAsset.asset_number == number)
        )
        return result.scalar_one_or_none()

    async def get_with_schedules(self, asset_id: uuid.UUID) -> Optional[FixedAsset]:
        result = await self.db.execute(
            self._base_query()
            .where(FixedAsset.id == asset_id)
            .options(selectinload(FixedAsset.schedules))
        )
        return result.scalar_one_or_none()


class AssetsService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db      = db
        self.user    = user
        self._repo   = FixedAssetRepository(db, user.tenant_id)
        self._num    = NumberSeriesService(db, user.tenant_id)
        self._posting = PostingEngine(db, user.tenant_id)

    # ══════════════════════════════════════════════════════
    # Register Asset
    # ══════════════════════════════════════════════════════
    async def register_asset(self, data: dict) -> FixedAsset:
        self.user.require("can_manage_assets")

        cat = data.get("category", "other")
        asset_acc, accum_acc, dep_acc = CATEGORY_ACCOUNTS.get(cat, ("1503", "1552", "6502"))

        asset_number = data.get("asset_number") or await self._num.next("AST", include_month=False)

        if await self._repo.exists(asset_number=asset_number):
            raise DuplicateError("أصل ثابت", "asset_number", asset_number)

        purchase_cost = Decimal(str(data["purchase_cost"]))
        salvage       = Decimal(str(data.get("salvage_value", 0)))

        async with atomic_transaction(self.db, label=f"register_asset_{asset_number}"):
            asset = FixedAsset(
                tenant_id=self.user.tenant_id,
                asset_number=asset_number,
                name_ar=data["name_ar"],
                name_en=data.get("name_en"),
                category=cat,
                status=AssetStatus.ACTIVE,
                depreciation_method="straight_line",
                purchase_date=data["purchase_date"],
                in_service_date=data.get("in_service_date") or data["purchase_date"],
                purchase_cost=purchase_cost,
                salvage_value=salvage,
                useful_life_months=int(data.get("useful_life_months", 60)),
                net_book_value=purchase_cost,
                location=data.get("location"),
                serial_number=data.get("serial_number"),
                supplier_name=data.get("supplier_name"),
                asset_account=data.get("asset_account") or asset_acc,
                accum_dep_account=data.get("accum_dep_account") or accum_acc,
                dep_expense_account=data.get("dep_expense_account") or dep_acc,
                notes=data.get("notes"),
                created_by=self.user.email,
            )
            self.db.add(asset)
            await self.db.flush()

            # Purchase JE: DR Asset / CR Cash or AP
            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="AJE",
                description=f"شراء أصل ثابت — {asset.name_ar} — {asset_number}",
                entry_date=asset.purchase_date,
                lines=[
                    PostingLine(account_code=asset.asset_account,
                                description=f"أصل ثابت — {asset.name_ar}",
                                debit=purchase_cost),
                    PostingLine(account_code=ACC.AP,
                                description=f"التزام شراء أصل — {asset_number}",
                                credit=purchase_cost),
                ],
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="assets",
                source_doc_type="asset_purchase",
                source_doc_number=asset_number,
                idempotency_key=f"AST:PURCH:{asset.id}",
                user_role=self.user.role,
            )
            je = await self._posting.post(je_req)
            await self.db.flush()

        logger.info("asset_registered", number=asset_number, cost=float(purchase_cost))
        return asset

    # ══════════════════════════════════════════════════════
    # Monthly Depreciation Run
    # ══════════════════════════════════════════════════════
    async def run_depreciation(self, year: int, month: int, dep_date: date) -> dict:
        self.user.require("can_manage_assets")

        assets = await self._repo.get_active()
        processed = []
        skipped   = []

        for asset in assets:
            if asset.category == "land":   # land not deprecated
                skipped.append(asset.asset_number)
                continue

            # Check already run for this period
            existing = await self.db.execute(
                select(DepreciationSchedule)
                .where(DepreciationSchedule.asset_id == asset.id)
                .where(DepreciationSchedule.period_year == year)
                .where(DepreciationSchedule.period_month == month)
            )
            if existing.scalar_one_or_none():
                skipped.append(asset.asset_number)
                continue

            dep_amount = asset.monthly_depreciation
            if dep_amount <= 0:
                skipped.append(asset.asset_number)
                continue

            # Don't exceed NBV minus salvage
            max_dep = max(asset.net_book_value - asset.salvage_value, Decimal("0"))
            dep_amount = min(dep_amount, max_dep).quantize(PREC)
            if dep_amount <= 0:
                skipped.append(asset.asset_number)
                continue

            async with atomic_transaction(self.db, label=f"dep_{asset.asset_number}_{year}_{month}"):
                accum_after = (asset.accumulated_depreciation + dep_amount).quantize(PREC)
                nbv_after   = (asset.net_book_value - dep_amount).quantize(PREC)

                sched = DepreciationSchedule(
                    tenant_id=self.user.tenant_id,
                    asset_id=asset.id,
                    asset_number=asset.asset_number,
                    period_year=year,
                    period_month=month,
                    dep_date=dep_date,
                    dep_amount=dep_amount,
                    accum_dep_before=asset.accumulated_depreciation,
                    accum_dep_after=accum_after,
                    nbv_after=nbv_after,
                    created_by=self.user.email,
                )
                self.db.add(sched)

                je_req = PostingRequest(
                    tenant_id=self.user.tenant_id,
                    je_type="AJE",
                    description=f"إهلاك شهري — {asset.name_ar} — {year}/{month:02d}",
                    entry_date=dep_date,
                    lines=[
                        PostingLine(account_code=asset.dep_expense_account,
                                    description=f"مصروف إهلاك — {asset.asset_number}",
                                    debit=dep_amount),
                        PostingLine(account_code=asset.accum_dep_account,
                                    description=f"مجمع إهلاك — {asset.asset_number}",
                                    credit=dep_amount),
                    ],
                    created_by_id=self.user.user_id,
                    created_by_email=self.user.email,
                    source_module="assets",
                    source_doc_type="depreciation",
                    source_doc_number=f"{asset.asset_number}-{year}-{month:02d}",
                    idempotency_key=f"AST:DEP:{asset.id}:{year}:{month}",
                    user_role=self.user.role,
                )
                je = await self._posting.post(je_req)
                sched.je_id     = je.je_id
                sched.je_serial = je.je_serial

                asset.accumulated_depreciation = accum_after
                asset.net_book_value = nbv_after
                asset.last_dep_date  = dep_date
                if nbv_after <= asset.salvage_value:
                    asset.status = AssetStatus.FULLY_DEPRECIATED
                await self.db.flush()

            processed.append({"asset": asset.asset_number, "dep": float(dep_amount)})

        total_dep = sum(p["dep"] for p in processed)
        logger.info("depreciation_run", year=year, month=month,
                    processed=len(processed), total=total_dep)
        return {
            "period": f"{year}/{month:02d}",
            "processed": len(processed),
            "skipped":   len(skipped),
            "total_depreciation": total_dep,
            "details": processed,
        }

    # ══════════════════════════════════════════════════════
    # Dispose Asset
    # ══════════════════════════════════════════════════════
    async def dispose_asset(
        self, asset_id: uuid.UUID, disposal_date: date,
        disposal_type: str = "sale", sale_price: Decimal = Decimal("0"),
        notes: Optional[str] = None,
    ) -> AssetDisposal:
        self.user.require("can_manage_assets")
        asset = await self._repo.get_or_raise(asset_id)
        if asset.status == AssetStatus.DISPOSED:
            raise InvalidStateError("الأصل", asset.status, [AssetStatus.ACTIVE])

        nbv   = asset.net_book_value
        gain_loss = (sale_price - nbv).quantize(PREC)

        async with atomic_transaction(self.db, label=f"dispose_{asset.asset_number}"):
            je_lines = [
                PostingLine(account_code=asset.accum_dep_account,
                            description=f"إزالة مجمع إهلاك — {asset.asset_number}",
                            debit=asset.accumulated_depreciation),
                PostingLine(account_code=asset.asset_account,
                            description=f"إزالة أصل ثابت — {asset.asset_number}",
                            credit=asset.purchase_cost),
            ]
            if sale_price > 0:
                je_lines.append(PostingLine(
                    account_code=ACC.CASH,
                    description=f"عائد بيع أصل — {asset.asset_number}",
                    debit=sale_price,
                ))
            if gain_loss > 0:
                je_lines.append(PostingLine(
                    account_code=ACC.OTHER_REV,
                    description=f"ربح بيع أصل — {asset.asset_number}",
                    credit=gain_loss,
                ))
            elif gain_loss < 0:
                je_lines.append(PostingLine(
                    account_code=ACC.MISC_EXP,
                    description=f"خسارة بيع أصل — {asset.asset_number}",
                    debit=abs(gain_loss),
                ))

            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="AJE",
                description=f"التخلص من أصل — {asset.name_ar} — {asset.asset_number}",
                entry_date=disposal_date,
                lines=je_lines,
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="assets",
                source_doc_type="asset_disposal",
                source_doc_number=asset.asset_number,
                idempotency_key=f"AST:DISP:{asset.id}",
                user_role=self.user.role,
            )
            je = await self._posting.post(je_req)

            disposal = AssetDisposal(
                tenant_id=self.user.tenant_id,
                asset_id=asset.id,
                asset_number=asset.asset_number,
                disposal_date=disposal_date,
                disposal_type=disposal_type,
                sale_price=sale_price,
                nbv_at_disposal=nbv,
                gain_loss=gain_loss,
                je_id=je.je_id,
                je_serial=je.je_serial,
                notes=notes,
                created_by=self.user.email,
            )
            self.db.add(disposal)
            asset.status = AssetStatus.DISPOSED
            await self.db.flush()

        logger.info("asset_disposed", number=asset.asset_number, gain_loss=float(gain_loss))
        return disposal

    async def list_assets(self, offset: int = 0, limit: int = 50) -> Tuple[List[FixedAsset], int]:
        return await self._repo.list(order_by=FixedAsset.asset_number, offset=offset, limit=limit)

    async def get_asset(self, asset_id: uuid.UUID) -> FixedAsset:
        a = await self._repo.get_with_schedules(asset_id)
        if not a:
            raise NotFoundError("أصل ثابت", asset_id)
        return a

    async def get_asset_summary(self) -> dict:
        assets = await self._repo.list(order_by=None, offset=0, limit=9999)
        all_assets = assets[0]
        total_cost   = sum(a.purchase_cost for a in all_assets)
        total_accum  = sum(a.accumulated_depreciation for a in all_assets)
        total_nbv    = sum(a.net_book_value for a in all_assets)
        active_count = sum(1 for a in all_assets if a.status == AssetStatus.ACTIVE)
        return {
            "total_assets":    len(all_assets),
            "active_assets":   active_count,
            "total_cost":      float(total_cost),
            "total_accum_dep": float(total_accum),
            "total_nbv":       float(total_nbv),
        }
