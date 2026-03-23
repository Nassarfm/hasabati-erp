"""
app/services/numbering/series_service.py
══════════════════════════════════════════════════════════
خدمة التسلسل الرقمي

تنسيق القيود:  TYPE-YEAR-0000001  (7 أرقام، يبدأ من جديد كل سنة)
تنسيق الوثائق: PREFIX-YEARMONTH-00001 (5 أرقام)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import datetime

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


class NumberSeriesService:
    def __init__(self, db: AsyncSession, tenant_id: uuid.UUID) -> None:
        self.db = db
        self.tenant_id = tenant_id

    # ══════════════════════════════════════════════
    # القيود المحاسبية — je_sequences
    # TYPE-YEAR-0000001
    # ══════════════════════════════════════════════
    async def next_je(self, je_type: str) -> str:
        """
        توليد رقم القيد: TYPE-YEAR-0000001
        يبدأ التسلسل من 1 في كل سنة جديدة لكل نوع.
        """
        year = datetime.utcnow().year

        # محاولة تحديث التسلسل الموجود
        result = await self.db.execute(
            text("""
                UPDATE je_sequences
                SET last_sequence = last_sequence + 1
                WHERE tenant_id = :tid
                  AND je_type_code = :code
                  AND fiscal_year  = :year
                RETURNING last_sequence
            """),
            {"tid": str(self.tenant_id), "code": je_type, "year": year},
        )
        row = result.fetchone()

        if not row:
            # إنشاء سجل جديد لهذا النوع + السنة
            await self.db.execute(
                text("""
                    INSERT INTO je_sequences
                        (id, tenant_id, je_type_code, fiscal_year, last_sequence)
                    VALUES
                        (gen_random_uuid(), :tid, :code, :year, 1)
                    ON CONFLICT (tenant_id, je_type_code, fiscal_year)
                    DO UPDATE SET last_sequence = je_sequences.last_sequence + 1
                    RETURNING last_sequence
                """),
                {"tid": str(self.tenant_id), "code": je_type, "year": year},
            )
            result2 = await self.db.execute(
                text("""
                    SELECT last_sequence FROM je_sequences
                    WHERE tenant_id = :tid
                      AND je_type_code = :code
                      AND fiscal_year  = :year
                """),
                {"tid": str(self.tenant_id), "code": je_type, "year": year},
            )
            row2 = result2.fetchone()
            seq = row2[0] if row2 else 1
        else:
            seq = row[0]

        serial = f"{je_type}-{year}-{seq:07d}"
        logger.debug("je_serial_generated", je_type=je_type, serial=serial)
        return serial

    # ══════════════════════════════════════════════
    # وثائق أخرى — num_series
    # PREFIX-YEARMONTH-00001
    # ══════════════════════════════════════════════
    async def next(self, prefix: str, include_month: bool = False) -> str:
        now = datetime.utcnow()
        year = now.year
        month = now.month
        period = f"{year}{month:02d}" if include_month else str(year)

        result = await self.db.execute(
            text("""
                UPDATE num_series
                SET next_value = next_value + 1, updated_at = now()
                WHERE tenant_id  = :tid
                  AND prefix     = :prefix
                  AND period_key = :period
                RETURNING next_value - 1 AS seq
            """),
            {"tid": str(self.tenant_id), "prefix": prefix, "period": period},
        )
        row = result.fetchone()

        if not row:
            await self.db.execute(
                text("""
                    INSERT INTO num_series
                        (id, tenant_id, prefix, period_key, next_value, padding, created_at, updated_at)
                    VALUES
                        (gen_random_uuid(), :tid, :prefix, :period, 2, 5, now(), now())
                    ON CONFLICT DO NOTHING
                """),
                {"tid": str(self.tenant_id), "prefix": prefix, "period": period},
            )
            seq = 1
        else:
            seq = row[0]

        serial = f"{prefix}-{period}-{seq:05d}"
        logger.debug("num_series_next", prefix=prefix, serial=serial)
        return serial

    async def next_po(self) -> str:
        return await self.next("PO", include_month=True)

    async def next_grn(self) -> str:
        return await self.next("GRN", include_month=True)

    async def next_vendor_invoice(self) -> str:
        return await self.next("VINV", include_month=True)
