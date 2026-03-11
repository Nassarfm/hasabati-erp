"""
app/core/config.py
══════════════════════════════════════════════════════════
Typed application settings via Pydantic BaseSettings.
All values come from environment variables / .env file.
No hardcoded secrets anywhere in the codebase.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
from functools import lru_cache
from typing import List, Literal, Union

from pydantic import AnyHttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ── Application ────────────────────────────────────
    APP_NAME: str = "حساباتي ERP"
    APP_VERSION: str = "1.0.0"
    APP_ENV: Literal["development", "staging", "production"] = "development"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"

    # ── Database ───────────────────────────────────────
    DATABASE_URL: str
    DATABASE_POOL_SIZE: int = 5
    DATABASE_MAX_OVERFLOW: int = 10
    DATABASE_POOL_TIMEOUT: int = 30
    DATABASE_POOL_RECYCLE: int = 1800

    # ── Supabase ───────────────────────────────────────
    SUPABASE_URL: str
    SUPABASE_ANON_KEY: str
    SUPABASE_SERVICE_KEY: str = ""
    SUPABASE_JWT_SECRET: str

    # ── API ────────────────────────────────────────────
    API_V1_PREFIX: str = "/api/v1"
    BACKEND_CORS_ORIGINS: Union[List[str], str] = []

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    @classmethod
    def _parse_cors(cls, v: Union[str, list]) -> List[str]:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return [o.strip() for o in v.split(",") if o.strip()]
        return v

    # ── Security ───────────────────────────────────────
    SECRET_KEY: str = "dev-only-change-in-production"

    # ── Idempotency ────────────────────────────────────
    IDEMPOTENCY_TTL_SECONDS: int = 86400
    IDEMPOTENCY_BACKEND: Literal["db", "redis"] = "db"
    REDIS_URL: str = ""

    # ── Background Jobs ────────────────────────────────
    SCHEDULER_ENABLED: bool = False
    SCHEDULER_TIMEZONE: str = "Asia/Riyadh"

    # ── Audit ──────────────────────────────────────────
    AUDIT_LOG_ENABLED: bool = True
    AUDIT_INCLUDE_REQUEST_BODY: bool = True
    AUDIT_INCLUDE_RESPONSE_BODY: bool = False

    # ── Server ─────────────────────────────────────────
    PORT: int = 8000

    # ── Derived properties ─────────────────────────────
    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def is_development(self) -> bool:
        return self.APP_ENV == "development"

    @model_validator(mode="after")
    def _validate_production_settings(self) -> "Settings":
        if self.is_production:
            if self.SECRET_KEY == "dev-only-change-in-production":
                raise ValueError("SECRET_KEY must be changed in production")
            if not self.SUPABASE_JWT_SECRET:
                raise ValueError("SUPABASE_JWT_SECRET is required")
        return self


@lru_cache
def get_settings() -> Settings:
    """
    Cached singleton — call get_settings() anywhere.
    Use as FastAPI dependency: settings: Settings = Depends(get_settings)
    """
    return Settings()


# Module-level singleton for non-DI usage
settings = get_settings()
