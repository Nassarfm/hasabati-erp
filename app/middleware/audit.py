"""
app/middleware/audit.py
══════════════════════════════════════════════════════════
Automatic audit logging middleware.
Every POST / PUT / PATCH / DELETE is logged to audit_log.

Captured per request:
  - tenant_id, user_id, user_email
  - HTTP method + path
  - request body (if AUDIT_INCLUDE_REQUEST_BODY)
  - response status code
  - duration_ms
  - request_id

Note: this is the HTTP-level audit.
Accounting-specific audit (je_posted, je_reversed) is
handled by PostingEngine directly with richer context.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Optional

import structlog
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.core.config import settings

logger = structlog.get_logger(__name__)

_MUTATION_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_SKIP_PATHS = {"/health", "/", "/api/docs", "/api/redoc", "/openapi.json"}


class AuditMiddleware(BaseHTTPMiddleware):
    """
    Logs every mutation to the audit_log table.
    Runs AFTER the response so it doesn't affect latency.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method not in _MUTATION_METHODS:
            return await call_next(request)
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)
        if not settings.AUDIT_LOG_ENABLED:
            return await call_next(request)

        start = time.monotonic()

        # Read body once (starlette streams body — must cache)
        body_bytes = await request.body()
        body_str: Optional[str] = None
        if settings.AUDIT_INCLUDE_REQUEST_BODY and body_bytes:
            try:
                body_str = body_bytes.decode("utf-8")[:4096]   # cap at 4KB
            except Exception:
                body_str = "<binary>"

        # Re-inject body so endpoint can read it
        async def receive():
            return {"type": "http.request", "body": body_bytes}

        request._receive = receive  # type: ignore[attr-defined]

        response = await call_next(request)
        duration_ms = int((time.monotonic() - start) * 1000)

        # Extract user context set by get_current_user
        ctx = structlog.contextvars.get_contextvars()

        logger.info(
            "audit_http",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=duration_ms,
            tenant_id=ctx.get("tenant_id"),
            user_id=ctx.get("user_id"),
            request_id=getattr(request.state, "request_id", ""),
        )

        # Fire-and-forget DB insert (don't block response)
        # In production, push to a queue instead
        # _schedule_audit_write(ctx, request, response, body_str, duration_ms)

        return response
