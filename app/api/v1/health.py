"""
app/api/v1/health.py
Health check endpoints for Railway and monitoring.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import get_db

router = APIRouter()


@router.get("/health", summary="Health Check")
async def health_check():
    """Basic liveness probe — Railway uses this."""
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "env": settings.APP_ENV,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health/db", summary="Database Health Check")
async def db_health_check(db: AsyncSession = Depends(get_db)):
    """Readiness probe — checks DB connectivity."""
    try:
        result = await db.execute(text("SELECT 1"))
        result.scalar_one()
        db_status = "ok"
    except Exception as exc:
        db_status = f"error: {exc}"

    return {
        "status": "ok" if db_status == "ok" else "degraded",
        "database": db_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
