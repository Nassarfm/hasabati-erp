"""
app/services/numbering/series_service.py
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

    async def next(self, prefix: str, include_month: bool = False) -> str:
        now = datetime.utcnow()
        year = now.year
        month = now.month
        period = f"{year}{month:02d}" if include_month else str(year)

        # محاولة تحديث السلسلة الموجودة
        result = await self.db.execute(
            text("""
                UPDATE num_series
                SET next_value = next_value + 1, updated_at = now()
                WHERE tenant_id = :tid AND prefix = :prefix AND period_key = :period
                RETURNING next_value - 1 AS seq
            """),
            {"tid": str(self.tenant_id), "prefix": prefix, "period": period}
        )
        row = result.fetchone()

        if not row:
            # إنشاء سلسلة جديدة تبدأ من 1 وترجع 1
            await self.db.execute(
                text("""
                    INSERT INTO num_series (id, tenant_id, prefix, period_key, next_value, padding, created_at, updated_at)
                    VALUES (gen_random_uuid(), :tid, :prefix, :period, 2, 5, now(), now())
                    ON CONFLICT DO NOTHING
                """),
                {"tid": str(self.tenant_id), "prefix": prefix, "period": period}
            )
            seq = 1
        else:
            seq = row[0]

        # تحقق من أن الرقم غير مستخدم
        check = await self.db.execute(
            text("SELECT 1 FROM journal_entries WHERE tenant_id = :tid AND serial = :serial"),
            {"tid": str(self.tenant_id), "serial": f"{prefix}-{period}-{seq:05d}"}
        )
        if check.fetchone():
            max_result = await self.db.execute(
                text("""
                    SELECT COALESCE(MAX(CAST(REGEXP_REPLACE(serial, '[^0-9]', '', 'g') AS INTEGER)), 0) + 1
                    FROM journal_entries
                    WHERE tenant_id = :tid AND serial LIKE :pattern
                """),
                {"tid": str(self.tenant_id), "pattern": f"{prefix}-{period}-%"}
            )
            seq = max_result.scalar() or seq
            await self.db.execute(
                text("UPDATE num_series SET next_value = :val WHERE tenant_id = :tid AND prefix = :prefix AND period_key = :period"),
                {"val": seq + 1, "tid": str(self.tenant_id), "prefix": prefix, "period": period}
            )

        serial = f"{prefix}-{period}-{seq:05d}"
        logger.debug("number_series_next", prefix=prefix, serial=serial)
        return serial

    async def next_je(self, je_type: str) -> str:
        return await self.next(je_type, include_month=False)

    async def next_po(self) -> str:
        return await self.next("PO", include_month=True)

    async def next_grn(self) -> str:
        return await self.next("GRN", include_month=True)

    async def next_vendor_invoice(self) -> str:
        return await self.next("VINV", include_month=True)
