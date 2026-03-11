"""
app/core/logging.py
══════════════════════════════════════════════════════════
Structured logging via structlog.
JSON in production, colored console in development.

Fix: use stdlib LoggerFactory (not PrintLoggerFactory).
     add_logger_name requires logging.Logger.name attribute
     which PrintLogger does NOT have.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from app.core.config import settings


def configure_logging() -> None:
    """Call once at application startup."""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    # ── Processors shared by all environments ─────────────
    # NOTE: add_logger_name requires a stdlib logger (.name attribute).
    # This works correctly with stdlib LoggerFactory below.
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.is_production:
        # JSON for Railway / Datadog log aggregation
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Readable colored output for development
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    # ── Configure structlog ────────────────────────────────
    # stdlib LoggerFactory produces real logging.Logger objects,
    # which have the .name attribute that add_logger_name needs.
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),   # ← الإصلاح
        cache_logger_on_first_use=True,
    )

    # ── Configure stdlib root logger ──────────────────────
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,   # override any existing handlers
    )

    # Suppress noisy libraries
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.DEBUG if settings.DEBUG else logging.WARNING
    )
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)


def get_logger(name: str = __name__) -> Any:
    """
    Get a structlog logger bound to a module name.
    Usage: logger = get_logger(__name__)
    """
    return structlog.get_logger(name)
