"""
app/core/security.py
══════════════════════════════════════════════════════════
Supabase JWT verification using PyJWT.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import base64
from typing import Optional
from uuid import UUID

import jwt
import structlog
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.core.config import settings
from app.core.exceptions import AuthenticationError

logger = structlog.get_logger(__name__)
bearer_scheme = HTTPBearer(auto_error=False)

_SUPABASE_AUDIENCE = "authenticated"
_ALGORITHM = "HS256"


class RawTokenClaims(BaseModel):
    sub: str
    email: Optional[str] = None
    role: Optional[str] = None
    aud: Optional[str] = None
    exp: Optional[int] = None

    @property
    def user_id(self) -> UUID:
        return UUID(self.sub)


def decode_supabase_token(token: str) -> RawTokenClaims:
    try:
        # Decode base64 secret
        secret = base64.b64decode(settings.SUPABASE_JWT_SECRET)
        payload = jwt.decode(
            token,
            secret,
            algorithms=[_ALGORITHM],
            audience=_SUPABASE_AUDIENCE,
        )
        return RawTokenClaims(**payload)
    except jwt.ExpiredSignatureError:
        raise AuthenticationError("رمز المصادقة منتهي الصلاحية — يرجى تسجيل الدخول مجدداً")
    except jwt.PyJWTError as exc:
        logger.warning("jwt_decode_failed", error=str(exc))
        raise AuthenticationError()


async def get_raw_claims(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> RawTokenClaims:
    if not credentials:
        raise AuthenticationError("لم يتم تقديم رمز المصادقة")
    return decode_supabase_token(credentials.credentials)
