"""
app/core/errors.py
══════════════════════════════════════════════════════════
FastAPI exception handlers.
Every exception → consistent Arabic JSON response:
{
    "success": false,
    "error": {
        "code":    "POSTING_ERROR",
        "message": "القيد غير متوازن ...",
        "detail":  {...}           // optional structured data
    },
    "request_id": "uuid"
}
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import traceback
from typing import Any

import structlog
from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import ERPException
from app.core.config import settings

logger = structlog.get_logger(__name__)


def _response(
    status_code: int,
    code: str,
    message: str,
    detail: Any = None,
    request_id: str = "",
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "error": {
                "code": code,
                "message": message,
                "detail": detail,
            },
            "request_id": request_id,
        },
    )


def _get_request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "")


async def erp_exception_handler(request: Request, exc: ERPException) -> JSONResponse:
    """Handle all typed ERP business exceptions."""
    logger.warning(
        "erp_exception",
        code=exc.code,
        message=exc.message,
        status_code=exc.status_code,
        path=request.url.path,
    )
    return _response(
        exc.status_code,
        exc.code,
        exc.message,
        exc.detail,
        _get_request_id(request),
    )


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """Handle standard HTTP errors (404, 405, etc.)."""
    code_map = {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        405: "METHOD_NOT_ALLOWED",
        422: "UNPROCESSABLE_ENTITY",
        500: "INTERNAL_SERVER_ERROR",
    }
    return _response(
        exc.status_code,
        code_map.get(exc.status_code, "HTTP_ERROR"),
        str(exc.detail),
        None,
        _get_request_id(request),
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle Pydantic validation errors — translate field paths to Arabic."""
    errors = []
    for err in exc.errors():
        loc = " → ".join(str(l) for l in err["loc"] if l != "body")
        errors.append({"field": loc, "message": err["msg"], "type": err["type"]})

    return _response(
        status.HTTP_422_UNPROCESSABLE_ENTITY,
        "VALIDATION_ERROR",
        "بيانات الطلب غير صحيحة — راجع التفاصيل",
        errors,
        _get_request_id(request),
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unexpected errors."""
    logger.error(
        "unhandled_exception",
        exc_type=type(exc).__name__,
        exc_str=str(exc),
        path=request.url.path,
        traceback=traceback.format_exc(),
    )
    detail = str(exc) if settings.DEBUG else None
    return _response(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        "INTERNAL_SERVER_ERROR",
        "خطأ داخلي غير متوقع — يرجى المحاولة لاحقاً",
        detail,
        _get_request_id(request),
    )
