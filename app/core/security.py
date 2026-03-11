"""
app/core/security.py
══════════════════════════════════════════════════════════
Supabase JWT verification.
Backend NEVER issues tokens — Supabase owns auth.
We only verify the JWT signature and extract claims.

Flow:
  1. Client logs in via Supabase (frontend unchanged)
  2. Client sends Bearer <supabase_access_token> on every request
  3. Backend verifies signature with SUPABASE_JWT_SECRET
  4. Backend extracts sub (user UUID) + email + role
  5. TenantMiddleware then enriches with tenant_id + permissions
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Optional
from uuid import UUID

import structlog
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, EmailStr

from app.core.config import settings
from app.core.exceptions import AuthenticationError

logger = structlog.get_logger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)

# Supabase issues tokens with aud="authenticated" for logged-in users
_SUPABASE_AUDIENCE = "authenticated"
_ALGORITHM = "HS256"


class RawTokenClaims(BaseModel):
    """Raw claims extracted from Supabase JWT."""
    sub: str                        # Supabase user UUID
    email: Optional[str] = None
    role: Optional[str] = None      # Supabase role (authenticated | service_role)
    aud: Optional[str] = None
    exp: Optional[int] = None

    @property
    def user_id(self) -> UUID:
        return UUID(self.sub)


def decode_supabase_token(token: str) -> RawTokenClaims:
    """
    Verify and decode a Supabase-issued JWT.
    Raises AuthenticationError on any failure.
    """
    try:
        payload = jwt.decode(
            token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=[_ALGORITHM],
            audience=_SUPABASE_AUDIENCE,
        )
        return RawTokenClaims(**payload)
    except ExpiredSignatureError:
        raise AuthenticationError("رمز المصادقة منتهي الصلاحية — يرجى تسجيل الدخول مجدداً")
    except JWTError as exc:
        logger.warning("jwt_decode_failed", error=str(exc))
        raise AuthenticationError()


async def get_raw_claims(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> RawTokenClaims:
    """
    FastAPI dependency — extracts raw JWT claims.
    Use get_current_user (in tenant.py) for full user context.
    """
    if not credentials:
        raise AuthenticationError("لم يتم تقديم رمز المصادقة")
    return decode_supabase_token(credentials.credentials)
