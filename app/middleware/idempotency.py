"""
app/middleware/idempotency.py
══════════════════════════════════════════════════════════
Idempotency enforcement for mutation endpoints.

How it works:
  1. Client sends header: Idempotency-Key: <uuid>
  2. On first request: execute normally, cache result
  3. On retry with same key: return cached result (no re-execution)
  4. Same key + different body: 409 Conflict

Prevents double-posting scenarios:
  - Network timeout → client retries → double JE
  - Frontend bug → duplicate invoice submission

Backend: uses DB table `idempotency_keys` (no Redis required for MVP).
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Optional

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.core.config import settings
from app.core.exceptions import IdempotencyConflictError

logger = structlog.get_logger(__name__)

IDEMPOTENCY_HEADER = "Idempotency-Key"
_APPLY_TO_METHODS = {"POST", "PUT", "PATCH"}

# In-memory store for development (replace with DB in production)
_memory_store: dict[str, dict] = {}


def _hash_body(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Processes Idempotency-Key header on mutation requests.
    MVP uses in-memory store; production should use DB/Redis.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method not in _APPLY_TO_METHODS:
            return await call_next(request)

        key = request.headers.get(IDEMPOTENCY_HEADER)
        if not key:
            # Idempotency-Key is optional for non-financial endpoints
            return await call_next(request)

        body_bytes = await request.body()
        body_hash = _hash_body(body_bytes)

        # Namespace key by path + tenant (from request state or header)
        full_key = f"{request.url.path}::{key}"

        cached = _memory_store.get(full_key)
        if cached:
            if cached["body_hash"] != body_hash:
                # Same key, different payload — reject
                logger.warning(
                    "idempotency_conflict",
                    key=key,
                    path=request.url.path,
                )
                err = IdempotencyConflictError(key)
                return JSONResponse(
                    status_code=err.status_code,
                    content={"success": False, "error": err.to_dict()},
                )

            # Same key, same payload — return cached response
            logger.info("idempotency_cache_hit", key=key)
            return JSONResponse(
                status_code=cached["status_code"],
                content=cached["response_body"],
                headers={"X-Idempotent-Replayed": "true"},
            )

        # Re-inject body
        async def receive():
            return {"type": "http.request", "body": body_bytes}

        request._receive = receive  # type: ignore[attr-defined]

        response = await call_next(request)

        # Cache the response (only success responses)
        if 200 <= response.status_code < 300:
            body_chunks = []
            async for chunk in response.body_iterator:
                body_chunks.append(chunk)
            response_body = b"".join(body_chunks)

            _memory_store[full_key] = {
                "body_hash":    body_hash,
                "status_code":  response.status_code,
                "response_body": json.loads(response_body),
                "cached_at":    time.time(),
                "ttl":          settings.IDEMPOTENCY_TTL_SECONDS,
            }

            return Response(
                content=response_body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type="application/json",
            )

        return response
