"""app/modules/purchases/repository.py"""
from __future__ import annotations
import uuid
from datetime import date
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.modules.purchases.models import (
    GRN, POLine, Supplier,
    PurchaseOrder, VendorInvoice,
)
from app.repositories.base_repo import BaseRepository


class SupplierRepository(BaseRepository[Supplier]):
    model = Supplier

    async def get_by_code(self, code: str) -> Optional[Supplier]:
        result = await self.db.execute(
            self._base_query()
            .where(Supplier.code == code)
            .where(Supplier.is_active == True)
        )
        return result.scalar_one_or_none()

    async def get_by_code_or_raise(self, code: str) -> Supplier:
        s = await self.get_by_code(code)
        if not s:
            from app.core.exceptions import NotFoundError
            raise NotFoundError("مورد", code)
        return s

    async def list_active(self, offset: int = 0, limit: int = 50) -> Tuple[List[Supplier], int]:
        return await self.list(
            filters=[Supplier.is_active == True],
            order_by=Supplier.code,
            offset=offset, limit=limit,
        )

    async def search(self, query: str, limit: int = 20) -> List[Supplier]:
        result = await self.db.execute(
            self._base_query()
            .where(Supplier.is_active == True)
            .where(
                Supplier.name_ar.ilike(f"%{query}%") |
                Supplier.code.ilike(f"%{query}%")
            )
            .limit(limit)
        )
        return list(result.scalars().all())


class PurchaseOrderRepository(BaseRepository[PurchaseOrder]):
    model = PurchaseOrder

    async def get_with_lines(self, po_id: uuid.UUID) -> Optional[PurchaseOrder]:
        result = await self.db.execute(
            self._base_query()
            .where(PurchaseOrder.id == po_id)
            .options(selectinload(PurchaseOrder.lines))
        )
        return result.scalar_one_or_none()

    async def list_pos(
        self,
        supplier_id: Optional[uuid.UUID] = None,
        status: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to:   Optional[date] = None,
        offset: int = 0, limit: int = 20,
    ) -> Tuple[List[PurchaseOrder], int]:
        filters = []
        if supplier_id:
            filters.append(PurchaseOrder.supplier_id == supplier_id)
        if status:
            filters.append(PurchaseOrder.status == status)
        if date_from:
            filters.append(PurchaseOrder.po_date >= date_from)
        if date_to:
            filters.append(PurchaseOrder.po_date <= date_to)
        return await self.list(
            filters=filters,
            order_by=desc(PurchaseOrder.po_date),
            offset=offset, limit=limit,
        )


class GRNRepository(BaseRepository[GRN]):
    model = GRN

    async def get_with_lines(self, grn_id: uuid.UUID) -> Optional[GRN]:
        result = await self.db.execute(
            self._base_query()
            .where(GRN.id == grn_id)
            .options(selectinload(GRN.lines))
        )
        return result.scalar_one_or_none()

    async def list_by_po(self, po_id: uuid.UUID) -> List[GRN]:
        result = await self.db.execute(
            self._base_query()
            .where(GRN.po_id == po_id)
            .order_by(desc(GRN.grn_date))
        )
        return list(result.scalars().all())

    async def get_total_received(self, po_id: uuid.UUID) -> Decimal:
        result = await self.db.execute(
            select(func.sum(GRN.total_cost))
            .where(GRN.tenant_id == self.tenant_id)
            .where(GRN.po_id == po_id)
            .where(GRN.status == "posted")
        )
        return result.scalar_one() or Decimal("0")


class VendorInvoiceRepository(BaseRepository[VendorInvoice]):
    model = VendorInvoice

    async def get_with_lines(self, vi_id: uuid.UUID) -> Optional[VendorInvoice]:
        result = await self.db.execute(
            self._base_query()
            .where(VendorInvoice.id == vi_id)
            .options(selectinload(VendorInvoice.lines))
        )
        return result.scalar_one_or_none()

    async def list_vendor_invoices(
        self,
        supplier_id: Optional[uuid.UUID] = None,
        status: Optional[str] = None,
        offset: int = 0, limit: int = 20,
    ) -> Tuple[List[VendorInvoice], int]:
        filters = []
        if supplier_id:
            filters.append(VendorInvoice.supplier_id == supplier_id)
        if status:
            filters.append(VendorInvoice.status == status)
        return await self.list(
            filters=filters,
            order_by=desc(VendorInvoice.invoice_date),
            offset=offset, limit=limit,
        )

    async def get_outstanding_ap(self) -> Decimal:
        result = await self.db.execute(
            select(func.sum(VendorInvoice.balance_due))
            .where(VendorInvoice.tenant_id == self.tenant_id)
            .where(VendorInvoice.status.in_(["posted", "matched"]))
        )
        return result.scalar_one() or Decimal("0")
