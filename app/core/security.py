"""
app/core/security.py
Supabase JWT verification — supports ES256 via JWKS.
"""
from __future__ import annotations
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
        jwks_client = jwt.PyJWKClient(
            f"{settings.SUPABASE_URL}/auth/v1/.well-known/jwks.json"
        )
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "ES256", "HS256"],
            audience=_SUPABASE_AUDIENCE,
        )
        logger.info("jwt_decode_success", sub=payload.get("sub"), email=payload.get("email"))
        return RawTokenClaims(**payload)
    except jwt.ExpiredSignatureError:
        raise AuthenticationError("رمز المصادقة منتهي الصلاحية — يرجى تسجيل الدخول مجدداً")
    except jwt.PyJWTError as exc:
        logger.warning("jwt_decode_failed", error=str(exc))
        raise AuthenticationError()
    except Exception as exc:
        logger.warning("jwt_decode_unexpected", error=str(exc))
        raise AuthenticationError()


async def get_raw_claims(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> RawTokenClaims:
    if not credentials:
        raise AuthenticationError("لم يتم تقديم رمز المصادقة")
    return decode_supabase_token(credentials.credentials)
