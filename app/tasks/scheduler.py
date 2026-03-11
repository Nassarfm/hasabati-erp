"""
app/tasks/scheduler.py
══════════════════════════════════════════════════════════
APScheduler setup for background jobs.
Disabled by default (SCHEDULER_ENABLED=false).
Activated when HR payroll and fixed asset modules are ready.

Planned jobs:
  - Monthly depreciation run     (1st of month, 02:00)
  - Payroll period close         (end of month)
  - Overdue invoice alerts        (daily, 08:00)
  - Fiscal period auto-reminder   (5th of month)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings

logger = structlog.get_logger(__name__)

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone=settings.SCHEDULER_TIMEZONE)
    return _scheduler


async def start_scheduler() -> None:
    if not settings.SCHEDULER_ENABLED:
        logger.info("scheduler_disabled", reason="SCHEDULER_ENABLED=false")
        return

    scheduler = get_scheduler()

    # ── Register jobs here when modules are ready ──────
    # scheduler.add_job(
    #     run_monthly_depreciation,
    #     CronTrigger(day=1, hour=2, minute=0),
    #     id="monthly_depreciation",
    #     replace_existing=True,
    # )
    # scheduler.add_job(
    #     check_overdue_invoices,
    #     CronTrigger(hour=8, minute=0),
    #     id="overdue_invoice_check",
    #     replace_existing=True,
    # )

    scheduler.start()
    logger.info("scheduler_started", timezone=settings.SCHEDULER_TIMEZONE)


async def stop_scheduler() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
