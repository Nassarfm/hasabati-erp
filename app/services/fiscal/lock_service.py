"""
app/services/fiscal/lock_service.py
══════════════════════════════════════════════════════════
Fiscal Period Lock Service.

Enforces two-tier locking:
  SOFT lock → warning, passable with admin permission
  HARD lock → absolute block, no one can bypass

Called by PostingEngine before every journal entry.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Optional

import structlog

from app.core.exceptions import FiscalPeriodLockedError, PermissionDeniedError
from app.modules.accounting.models import FiscalLock, FiscalLockType
from app.modules.accounting.repository import FiscalLockRepository

logger = structlog.get_logger(__name__)


class FiscalLockService:
    """
    Usage (inside PostingEngine):
        await fiscal_lock_svc.guard(
            entry_date=date(2024, 12, 1),
            user_role="accountant",
            force=False,
        )
        # raises FiscalPeriodLockedError if blocked
        # returns silently if allowed
    """

    def __init__(self, repo: FiscalLockRepository) -> None:
        self.repo = repo

    async def guard(
        self,
        entry_date: date,
        user_role: str,
        *,
        force: bool = False,
    ) -> None:
        """
        Check if entry_date falls in a locked period.
        Raises FiscalPeriodLockedError if blocked.
        """
        lock = await self.repo.get_active_lock(
            fiscal_year=entry_date.year,
            fiscal_month=entry_date.month,
        )

        if lock is None:
            return  # No lock — proceed

        lock_type = lock.lock_type

        # HARD lock — absolute block
        if lock_type == FiscalLockType.HARD:
            logger.warning(
                "fiscal_hard_lock_blocked",
                year=entry_date.year,
                month=entry_date.month,
                user_role=user_role,
            )
            raise FiscalPeriodLockedError(
                entry_date.year, entry_date.month, "hard"
            )

        # SOFT lock — admin/owner can bypass with force=True
        if lock_type == FiscalLockType.SOFT:
            if force and user_role in ("owner", "admin"):
                logger.info(
                    "fiscal_soft_lock_bypassed",
                    year=entry_date.year,
                    month=entry_date.month,
                    user_role=user_role,
                )
                return

            if not force:
                raise FiscalPeriodLockedError(
                    entry_date.year, entry_date.month, "soft"
                )

            # force=True but not admin
            raise PermissionDeniedError(
                "تجاوز قفل الفترة المالية — يحتاج صلاحية admin أو owner"
            )

    async def lock_period(
        self,
        fiscal_year: int,
        fiscal_month: Optional[int],
        lock_type: str,
        locked_by_email: str,
        reason: Optional[str],
        period_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> FiscalLock:
        """Create a new fiscal lock."""
        lock = FiscalLock(
            tenant_id=tenant_id,
            period_id=period_id,
            fiscal_year=fiscal_year,
            fiscal_month=fiscal_month,
            lock_type=FiscalLockType(lock_type),
            reason=reason,
            locked_by=locked_by_email,
            locked_at=datetime.now(timezone.utc),
            is_active=True,
        )
        self.repo.db.add(lock)
        await self.repo.db.flush()

        logger.info(
            "fiscal_period_locked",
            year=fiscal_year,
            month=fiscal_month,
            lock_type=lock_type,
            by=locked_by_email,
        )
        return lock

    async def unlock_period(
        self,
        lock_id: uuid.UUID,
        unlocked_by_email: str,
        user_role: str,
    ) -> FiscalLock:
        """
        Unlock a period.
        Hard locks can only be unlocked by owner.
        Soft locks can be unlocked by admin or owner.
        """
        lock = await self.repo.get_or_raise(lock_id)

        if lock.lock_type == FiscalLockType.HARD and user_role != "owner":
            raise PermissionDeniedError("فك قفل الفترة الصارم — يحتاج صلاحية owner فقط")

        lock.is_active = False
        lock.unlocked_by = unlocked_by_email
        lock.unlocked_at = datetime.now(timezone.utc)
        await self.repo.db.flush()

        logger.info(
            "fiscal_period_unlocked",
            lock_id=str(lock_id),
            lock_type=lock.lock_type,
            by=unlocked_by_email,
        )
        return lock
