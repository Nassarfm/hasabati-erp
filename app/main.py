"""
app/main.py
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
from app.tasks.scheduler import start_scheduler, stop_scheduler
from app.middleware.audit_middleware import AuditMiddleware
from app.api.v1.router import v1_router
from app.modules.parties.router import router as parties_router
from app.modules.admin.router import router as admin_router

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("startup", app=settings.APP_NAME, version=settings.APP_VERSION, env=settings.APP_ENV)
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://nassarfm.github.io",
            "http://localhost:5173",
            "http://localhost:3000",
            "http://127.0.0.1:5173",
            "*",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
        max_age=600,
    )
    # ── Audit Middleware — يسجّل نشاط المستخدمين تلقائياً ──
    app.add_middleware(AuditMiddleware)
    app.add_exception_handler(ERPException, erp_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
    app.include_router(v1_router,      prefix=settings.API_V1_PREFIX)
    app.include_router(parties_router, prefix=settings.API_V1_PREFIX)  # المتعاملون الماليون
    app.include_router(admin_router,   prefix=settings.API_V1_PREFIX)  # أدوات المدير
    return app


app = create_app()
