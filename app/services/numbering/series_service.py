"""
app/services/numbering/series_service.py
══════════════════════════════════════════════════════════
Document number series generator.
Generates sequential, tenant-isolated document numbers.

Format examples:
  JE-2024-00001   (Journal Entry)
  GRN-202412-001  (Goods Receipt)
  VINV-202412-001 (Vendor Invoice)
  PO-202412-001   (Purchase Order)

Uses DB sequence via SELECT FOR UPDATE to guarantee
uniqueness under concurrent requests.
MVP: uses in-memory counter with DB fallback stub.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

# In-memory counters for development (replace with DB in production)
_counters: Dict[str, int] = {}


class NumberSeriesService:
    """
    Generates unique sequential document numbers per tenant.
    Thread-safe via database locking in production.
    """

    def __init__(self, db: AsyncSession, tenant_id: uuid.UUID) -> None:
        self.db = db
        self.tenant_id = tenant_id

    async def next(
        self,
        prefix: str,
        include_month: bool = False,
    ) -> str:
        """
        Generate next number in series.

        Args:
            prefix:        "JE", "GRN", "VINV", "PO", etc.
            include_month: True → JE-202412-00001
                          False → JE-2024-00001

        Returns unique serial string.
        """
        now = datetime.utcnow()
        year = now.year
        month = now.month

        if include_month:
            period = f"{year}{month:02d}"
        else:
            period = str(year)

        key = f"{self.tenant_id}::{prefix}::{period}"

        # MVP: in-memory (replace with DB SELECT FOR UPDATE)
        _counters[key] = _counters.get(key, 0) + 1
        seq = _counters[key]

        serial = f"{prefix}-{period}-{seq:05d}"
        logger.debug("number_series_next", prefix=prefix, serial=serial)
        return serial

    async def next_je(self, je_type: str) -> str:
        """Shortcut for journal entry serials."""
        return await self.next(je_type, include_month=False)

    async def next_po(self) -> str:
        return await self.next("PO", include_month=True)

    async def next_grn(self) -> str:
        return await self.next("GRN", include_month=True)

    async def next_vendor_invoice(self) -> str:
        return await self.next("VINV", include_month=True)
