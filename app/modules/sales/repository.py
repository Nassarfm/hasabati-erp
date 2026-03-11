"""
app/modules/sales/repository.py
══════════════════════════════════════════════════════════
Sales repositories — all DB queries here.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.modules.sales.models import (
    Customer, InvoiceStatus,
    SalesInvoice, SalesInvoiceLine,
    SalesReturn, SalesReturnLine,
)
from app.repositories.base_repo import BaseRepository


class CustomerRepository(BaseRepository[Customer]):
    model = Customer

    async def get_by_code(self, code: str) -> Optional[Customer]:
        result = await self.db.execute(
            self._base_query()
            .where(Customer.code == code)
            .where(Customer.is_active == True)
        )
        return result.scalar_one_or_none()

    async def get_by_code_or_raise(self, code: str) -> Customer:
        c = await self.get_by_code(code)
        if not c:
            from app.core.exceptions import NotFoundError
            raise NotFoundError("عميل", code)
        return c

    async def list_active(
        self,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[Customer], int]:
        return await self.list(
            filters=[Customer.is_active == True],
            order_by=Customer.code,
            offset=offset,
            limit=limit,
        )

    async def search(self, query: str, limit: int = 20) -> List[Customer]:
        result = await self.db.execute(
            self._base_query()
            .where(Customer.is_active == True)
            .where(
                Customer.name_ar.ilike(f"%{query}%") |
                Customer.code.ilike(f"%{query}%") |
                Customer.phone.ilike(f"%{query}%")
            )
            .limit(limit)
        )
        return list(result.scalars().all())


class SalesInvoiceRepository(BaseRepository[SalesInvoice]):
    model = SalesInvoice

    async def get_with_lines(self, invoice_id: uuid.UUID) -> Optional[SalesInvoice]:
        result = await self.db.execute(
            self._base_query()
            .where(SalesInvoice.id == invoice_id)
            .options(selectinload(SalesInvoice.lines))
        )
        return result.scalar_one_or_none()

    async def list_invoices(
        self,
        customer_id: Optional[uuid.UUID] = None,
        status: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        offset: int = 0,
        limit: int = 20,
    ) -> Tuple[List[SalesInvoice], int]:
        filters = []
        if customer_id:
            filters.append(SalesInvoice.customer_id == customer_id)
        if status:
            filters.append(SalesInvoice.status == status)
        if date_from:
            filters.append(SalesInvoice.invoice_date >= date_from)
        if date_to:
            filters.append(SalesInvoice.invoice_date <= date_to)
        return await self.list(
            filters=filters,
            order_by=desc(SalesInvoice.invoice_date),
            offset=offset,
            limit=limit,
        )

    async def get_outstanding_ar(self) -> Decimal:
        result = await self.db.execute(
            select(func.sum(SalesInvoice.balance_due))
            .where(SalesInvoice.tenant_id == self.tenant_id)
            .where(SalesInvoice.status.in_(["posted", "partially_returned"]))
        )
        return result.scalar_one() or Decimal("0")

    async def get_revenue_summary(
        self,
        date_from: date,
        date_to: date,
    ) -> dict:
        result = await self.db.execute(
            select(
                func.count(SalesInvoice.id).label("count"),
                func.sum(SalesInvoice.taxable_amount).label("revenue"),
                func.sum(SalesInvoice.vat_amount).label("vat"),
                func.sum(SalesInvoice.total_cost).label("cogs"),
                func.sum(SalesInvoice.gross_profit).label("gp"),
            )
            .where(SalesInvoice.tenant_id == self.tenant_id)
            .where(SalesInvoice.status == InvoiceStatus.POSTED)
            .where(SalesInvoice.invoice_date >= date_from)
            .where(SalesInvoice.invoice_date <= date_to)
        )
        row = result.one()
        return {
            "count":   row.count or 0,
            "revenue": row.revenue or Decimal("0"),
            "vat":     row.vat or Decimal("0"),
            "cogs":    row.cogs or Decimal("0"),
            "gp":      row.gp or Decimal("0"),
        }


class SalesReturnRepository(BaseRepository[SalesReturn]):
    model = SalesReturn

    async def get_with_lines(self, return_id: uuid.UUID) -> Optional[SalesReturn]:
        result = await self.db.execute(
            self._base_query()
            .where(SalesReturn.id == return_id)
            .options(selectinload(SalesReturn.lines))
        )
        return result.scalar_one_or_none()

    async def list_by_invoice(self, invoice_id: uuid.UUID) -> List[SalesReturn]:
        result = await self.db.execute(
            self._base_query()
            .where(SalesReturn.invoice_id == invoice_id)
            .order_by(desc(SalesReturn.return_date))
        )
        return list(result.scalars().all())
