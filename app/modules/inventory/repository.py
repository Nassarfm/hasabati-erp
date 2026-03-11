"""
app/modules/inventory/repository.py
══════════════════════════════════════════════════════════
Inventory data access layer.
All inventory DB queries here — nothing in service layer.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import List, Optional, Tuple

import structlog
from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.modules.inventory.models import (
    MovementStatus, MovementType,
    Product, StockAdjustment, StockBalance, StockMovement, Warehouse,
)
from app.repositories.base_repo import BaseRepository

logger = structlog.get_logger(__name__)


# ══════════════════════════════════════════════════════════
# Product Repository
# ══════════════════════════════════════════════════════════
class ProductRepository(BaseRepository[Product]):
    model = Product

    async def get_by_code(self, code: str) -> Optional[Product]:
        result = await self.db.execute(
            self._base_query()
            .where(Product.code == code)
            .where(Product.is_active == True)
        )
        return result.scalar_one_or_none()

    async def get_by_code_or_raise(self, code: str) -> Product:
        p = await self.get_by_code(code)
        if not p:
            from app.core.exceptions import NotFoundError
            raise NotFoundError("منتج", code)
        return p

    async def bulk_get_by_codes(self, codes: List[str]) -> dict[str, Product]:
        result = await self.db.execute(
            self._base_query()
            .where(Product.code.in_(codes))
            .where(Product.is_active == True)
        )
        return {p.code: p for p in result.scalars().all()}

    async def list_active(
        self,
        category: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[Product], int]:
        filters = [Product.is_active == True]
        if category:
            filters.append(Product.category_code == category)
        return await self.list(
            filters=filters,
            order_by=Product.code,
            offset=offset,
            limit=limit,
        )

    async def search(self, query: str, limit: int = 20) -> List[Product]:
        result = await self.db.execute(
            self._base_query()
            .where(Product.is_active == True)
            .where(
                Product.name_ar.ilike(f"%{query}%") |
                Product.code.ilike(f"%{query}%") |
                Product.barcode.ilike(f"%{query}%")
            )
            .limit(limit)
        )
        return list(result.scalars().all())


# ══════════════════════════════════════════════════════════
# Warehouse Repository
# ══════════════════════════════════════════════════════════
class WarehouseRepository(BaseRepository[Warehouse]):
    model = Warehouse

    async def get_by_code(self, code: str) -> Optional[Warehouse]:
        result = await self.db.execute(
            self._base_query()
            .where(Warehouse.code == code)
            .where(Warehouse.is_active == True)
        )
        return result.scalar_one_or_none()

    async def get_default(self) -> Optional[Warehouse]:
        result = await self.db.execute(
            self._base_query()
            .where(Warehouse.is_default == True)
            .where(Warehouse.is_active == True)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_active(self) -> List[Warehouse]:
        result = await self.db.execute(
            self._base_query()
            .where(Warehouse.is_active == True)
            .order_by(Warehouse.code)
        )
        return list(result.scalars().all())


# ══════════════════════════════════════════════════════════
# Stock Balance Repository
# ══════════════════════════════════════════════════════════
class StockBalanceRepository(BaseRepository[StockBalance]):
    model = StockBalance

    async def get_balance(
        self,
        product_id: uuid.UUID,
        warehouse_id: uuid.UUID,
    ) -> Optional[StockBalance]:
        result = await self.db.execute(
            self._base_query()
            .where(StockBalance.product_id == product_id)
            .where(StockBalance.warehouse_id == warehouse_id)
        )
        return result.scalar_one_or_none()

    async def get_or_create_balance(
        self,
        product: Product,
        warehouse: Warehouse,
    ) -> StockBalance:
        """Get balance row, create if not exists."""
        bal = await self.get_balance(product.id, warehouse.id)
        if bal is None:
            bal = StockBalance(
                tenant_id=self.tenant_id,
                product_id=product.id,
                product_code=product.code,
                warehouse_id=warehouse.id,
                warehouse_code=warehouse.code,
                qty_on_hand=Decimal("0"),
                qty_reserved=Decimal("0"),
                qty_available=Decimal("0"),
                qty_incoming=Decimal("0"),
                average_cost=Decimal("0"),
                total_value=Decimal("0"),
            )
            self.db.add(bal)
            await self.db.flush()
        return bal

    async def list_by_product(
        self, product_id: uuid.UUID
    ) -> List[StockBalance]:
        result = await self.db.execute(
            self._base_query()
            .where(StockBalance.product_id == product_id)
        )
        return list(result.scalars().all())

    async def list_by_warehouse(
        self, warehouse_id: uuid.UUID
    ) -> List[StockBalance]:
        result = await self.db.execute(
            self._base_query()
            .where(StockBalance.warehouse_id == warehouse_id)
            .order_by(StockBalance.product_code)
        )
        return list(result.scalars().all())

    async def list_low_stock(self) -> List[StockBalance]:
        """Products below reorder point."""
        result = await self.db.execute(
            self._base_query()
            .join(Product, StockBalance.product_id == Product.id)
            .where(Product.reorder_point > 0)
            .where(StockBalance.qty_available <= Product.reorder_point)
        )
        return list(result.scalars().all())

    async def get_total_value(self) -> Decimal:
        """Total inventory value across all products/warehouses."""
        result = await self.db.execute(
            select(func.sum(StockBalance.total_value))
            .where(StockBalance.tenant_id == self.tenant_id)
        )
        return result.scalar_one() or Decimal("0")


# ══════════════════════════════════════════════════════════
# Stock Movement Repository
# ══════════════════════════════════════════════════════════
class StockMovementRepository(BaseRepository[StockMovement]):
    model = StockMovement

    async def get_by_source(
        self,
        source_doc_id: uuid.UUID,
        movement_type: Optional[str] = None,
    ) -> List[StockMovement]:
        """Find all movements generated by a source document."""
        q = (
            self._base_query()
            .where(StockMovement.source_doc_id == source_doc_id)
            .where(StockMovement.status == MovementStatus.POSTED)
        )
        if movement_type:
            q = q.where(StockMovement.movement_type == movement_type)
        result = await self.db.execute(q)
        return list(result.scalars().all())

    async def list_for_product(
        self,
        product_id: uuid.UUID,
        *,
        warehouse_id: Optional[uuid.UUID] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[StockMovement], int]:
        filters = [
            StockMovement.product_id == product_id,
            StockMovement.status == MovementStatus.POSTED,
        ]
        if warehouse_id:
            filters.append(StockMovement.warehouse_id == warehouse_id)
        if date_from:
            filters.append(StockMovement.movement_date >= date_from)
        if date_to:
            filters.append(StockMovement.movement_date <= date_to)

        return await self.list(
            filters=filters,
            order_by=desc(StockMovement.movement_date),
            offset=offset,
            limit=limit,
        )
