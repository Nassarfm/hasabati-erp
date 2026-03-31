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

# ── Supabase free tier: max 5 direct connections ──────────
# pool_size=2 + max_overflow=2 + 2 workers = 8 max → آمن
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=2,                 # خُفِّض من 5 إلى 2
    max_overflow=2,              # خُفِّض من 10 إلى 2
    pool_timeout=settings.DATABASE_POOL_TIMEOUT,
    pool_recycle=300,            # خُفِّض من 1800 إلى 300
    pool_pre_ping=True,
    echo=settings.DEBUG,
    connect_args={
        "prepared_statement_cache_size": 0,   # مطلوب مع Supabase pooler
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
