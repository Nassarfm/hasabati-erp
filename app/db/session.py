"""
app/db/session.py
══════════════════════════════════════════════════════════
Async SQLAlchemy engine and session factory.
get_db() is the FastAPI dependency injected everywhere.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations
from typing import AsyncGenerator
import structlog
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from app.core.config import settings

logger = structlog.get_logger(__name__)

# ── Supabase connection pooler (port 6543 Transaction mode) ──
# pool_size=5, max_overflow=10 → max 15 اتصال متزامن آمن
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=5,          # رُفع من 2 إلى 5
    max_overflow=10,      # رُفع من 2 إلى 10
    pool_timeout=30,
    pool_recycle=600,
    pool_pre_ping=True,
    echo=settings.DEBUG,
    connect_args={
        "prepared_statement_cache_size": 0,
        "statement_cache_size": 0,
    },
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency — yields a DB session per request.
    Commits on success, rolls back on any exception.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
