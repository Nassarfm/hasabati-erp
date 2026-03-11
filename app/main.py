"""
app/main.py
══════════════════════════════════════════════════════════
FastAPI application factory.
Follows the create_app pattern for testability.
All configuration is pulled from settings — nothing hardcoded.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.config import settings
from app.core.errors import (
    erp_exception_handler,
    http_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from app.core.exceptions import ERPException
from app.core.logging import configure_logging
from app.middleware.audit import AuditMiddleware
from app.middleware.idempotency import IdempotencyMiddleware
from app.middleware.request_id import RequestIDMiddleware
from app.tasks.scheduler import start_scheduler, stop_scheduler
from app.api.v1.router import v1_router

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info(
        "startup",
        app=settings.APP_NAME,
        version=settings.APP_VERSION,
        env=settings.APP_ENV,
    )
    await start_scheduler()
    yield
    await stop_scheduler()
    logger.info("shutdown")


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="حساباتي ERP — نظام محاسبة وإدارة متكامل",
        docs_url="/api/docs" if not settings.is_production else None,
        redoc_url="/api/redoc" if not settings.is_production else None,
        openapi_url="/openapi.json" if not settings.is_production else None,
        lifespan=lifespan,
    )

    # ── Middleware (order matters — outermost = first to run) ──
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.BACKEND_CORS_ORIGINS or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestIDMiddleware)   # inject X-Request-ID
    app.add_middleware(AuditMiddleware)       # log all mutations
    app.add_middleware(IdempotencyMiddleware) # prevent double-submit

    # ── Exception Handlers ─────────────────────────────────────
    app.add_exception_handler(ERPException, erp_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    # ── Routers ────────────────────────────────────────────────
    app.include_router(v1_router, prefix=settings.API_V1_PREFIX)

    return app


app = create_app()
