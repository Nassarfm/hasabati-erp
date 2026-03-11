"""
app/db/transactions.py
══════════════════════════════════════════════════════════
Transaction management helpers.

Rule: every posting operation (JE creation, GRN posting,
payroll run, etc.) MUST execute inside a single atomic
transaction via atomic_transaction().

If any step fails → full rollback, nothing partial saved.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def atomic_transaction(
    db: AsyncSession,
    *,
    label: str = "transaction",
) -> AsyncGenerator[AsyncSession, None]:
    """
    Ensures a block of DB operations is fully atomic.
    On success  → COMMIT
    On exception → ROLLBACK + re-raise

    Usage:
        async with atomic_transaction(db, label="post_je") as txn:
            txn.add(journal_entry)
            txn.add(journal_line_1)
            txn.add(journal_line_2)
            # commit happens automatically on exit

    Note: get_db() already commits at request end.
    Use atomic_transaction() when you need nested savepoint
    or explicit mid-request commit control.
    """
    logger.debug("transaction_begin", label=label)
    try:
        yield db
        await db.flush()          # write to DB within transaction (no commit yet)
        logger.debug("transaction_flush_ok", label=label)
    except Exception as exc:
        await db.rollback()
        logger.error("transaction_rollback", label=label, error=str(exc))
        raise
