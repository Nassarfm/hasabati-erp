"""
app/services/numbering/series_service.py
══════════════════════════════════════════════════════════
خدمة التسلسل الرقمي — محدَّثة

الإصلاح الرئيسي:
  use_entry_date_year = True  → يستخدم سنة تاريخ القيد (الافتراضي)
  use_entry_date_year = False → يستخدم سنة إنشاء القيد

مثال:
  قيد بتاريخ 28/12/2024 أُنشئ في 3/1/2025
  use_entry_date_year=True  → JV-2024-0000148  ✅ صحيح
  use_entry_date_year=False → JV-2025-0000001  ❌ مضلل
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


class NumberSeriesService:
    def __init__(self, db: AsyncSession, tenant_id: uuid.UUID) -> None:
        self.db = db
        self.tenant_id = tenant_id
        self._settings_cache: dict = {}

    async def _get_settings(self, je_type: str) -> dict:
        """جلب إعدادات الترقيم من series_settings"""
        if je_type in self._settings_cache:
            return self._settings_cache[je_type]

        result = await self.db.execute(text("""
            SELECT prefix, padding, separator, use_entry_date_year
            FROM series_settings
            WHERE tenant_id = :tid AND je_type_code = :type AND is_active = true
            LIMIT 1
        """), {"tid": str(self.tenant_id), "type": je_type})

        row = result.fetchone()
        settings = {
            "prefix":    row[0] if row else je_type,
            "padding":   row[1] if row else 7,
            "separator": row[2] if row else "-",
            "use_entry_date_year": row[3] if row else True,
        }
        self._settings_cache[je_type] = settings
        return settings

    # ══════════════════════════════════════════════
    # القيود المحاسبية
    # ══════════════════════════════════════════════
    async def next_je(
        self,
        je_type: str,
        entry_date: Optional[date] = None,
    ) -> str:
        """
        توليد رقم القيد: PREFIX-YEAR-NNNNNNN

        entry_date: تاريخ القيد — يُستخدم لتحديد السنة إذا use_entry_date_year=True
                    إذا لم يُمرَّر → يستخدم التاريخ الحالي
        """
        settings = await self._get_settings(je_type)

        # ── تحديد السنة ───────────────────────────────────
        if settings["use_entry_date_year"] and entry_date:
            # ✅ يستخدم سنة تاريخ القيد الفعلي
            if isinstance(entry_date, str):
                year = int(entry_date[:4])
            else:
                year = entry_date.year
        else:
            # يستخدم سنة اليوم (السلوك القديم)
            year = datetime.utcnow().year

        prefix    = settings["prefix"]
        padding   = settings["padding"]
        separator = settings["separator"]

        # ── تحديث/إنشاء التسلسل ──────────────────────────
        result = await self.db.execute(text("""
            UPDATE je_sequences
            SET last_sequence = last_sequence + 1
            WHERE tenant_id    = :tid
              AND je_type_code = :code
              AND fiscal_year  = :year
            RETURNING last_sequence
        """), {"tid": str(self.tenant_id), "code": je_type, "year": year})

        row = result.fetchone()

        if not row:
            await self.db.execute(text("""
                INSERT INTO je_sequences
                    (id, tenant_id, je_type_code, fiscal_year, last_sequence)
                VALUES
                    (gen_random_uuid(), :tid, :code, :year, 1)
                ON CONFLICT (tenant_id, je_type_code, fiscal_year)
                DO UPDATE SET last_sequence = je_sequences.last_sequence + 1
            """), {"tid": str(self.tenant_id), "code": je_type, "year": year})

            result2 = await self.db.execute(text("""
                SELECT last_sequence FROM je_sequences
                WHERE tenant_id = :tid AND je_type_code = :code AND fiscal_year = :year
            """), {"tid": str(self.tenant_id), "code": je_type, "year": year})
            row2 = result2.fetchone()
            seq = row2[0] if row2 else 1
        else:
            seq = row[0]

        serial = f"{prefix}{separator}{year}{separator}{seq:0{padding}d}"
        logger.debug("je_serial_generated", je_type=je_type, serial=serial, year=year)
        return serial

    # ══════════════════════════════════════════════
    # وثائق أخرى — num_series
    # ══════════════════════════════════════════════
    async def next(self, prefix: str, include_month: bool = False) -> str:
        now    = datetime.utcnow()
        period = f"{now.year}{now.month:02d}" if include_month else str(now.year)

        result = await self.db.execute(text("""
            UPDATE num_series
            SET next_value = next_value + 1, updated_at = now()
            WHERE tenant_id  = :tid
              AND prefix     = :prefix
              AND period_key = :period
            RETURNING next_value - 1 AS seq
        """), {"tid": str(self.tenant_id), "prefix": prefix, "period": period})

        row = result.fetchone()
        if not row:
            await self.db.execute(text("""
                INSERT INTO num_series
                    (id, tenant_id, prefix, period_key, next_value, padding, created_at, updated_at)
                VALUES
                    (gen_random_uuid(), :tid, :prefix, :period, 2, 5, now(), now())
                ON CONFLICT DO NOTHING
            """), {"tid": str(self.tenant_id), "prefix": prefix, "period": period})
            seq = 1
        else:
            seq = row[0]

        return f"{prefix}-{period}-{seq:05d}"

    async def next_po(self)             -> str: return await self.next("PO",   True)
    async def next_grn(self)            -> str: return await self.next("GRN",  True)
    async def next_vendor_invoice(self) -> str: return await self.next("VINV", True)


    # ══════════════════════════════════════════════
    # معاينة الرقم التالي (بدون حفظ)
    # ══════════════════════════════════════════════
    async def preview_next(
        self,
        je_type: str,
        entry_date: Optional[date] = None,
    ) -> dict:
        """معاينة الرقم التالي بدون حفظ — للعرض في الشاشة"""
        settings = await self._get_settings(je_type)

        if settings["use_entry_date_year"] and entry_date:
            year = entry_date.year if hasattr(entry_date, 'year') else int(str(entry_date)[:4])
        else:
            year = datetime.utcnow().year

        result = await self.db.execute(text("""
            SELECT last_sequence FROM je_sequences
            WHERE tenant_id = :tid AND je_type_code = :code AND fiscal_year = :year
        """), {"tid": str(self.tenant_id), "code": je_type, "year": year})

        row = result.fetchone()
        current_seq = row[0] if row else 0
        next_seq    = current_seq + 1

        prefix    = settings["prefix"]
        padding   = settings["padding"]
        separator = settings["separator"]

        return {
            "je_type":      je_type,
            "year":         year,
            "current_seq":  current_seq,
            "next_seq":     next_seq,
            "next_serial":  f"{prefix}{separator}{year}{separator}{next_seq:0{padding}d}",
            "settings":     settings,
        }
