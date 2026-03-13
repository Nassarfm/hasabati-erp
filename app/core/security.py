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
        unverified_header = jwt.get_unverified_header(token)
        alg = unverified_header.get("alg")
        logger.warning("jwt_header_debug", alg=alg)

        if alg == "HS256":
            payload = jwt.decode(
                token,
                settings.SUPABASE_JWT_SECRET,
                algorithms=["HS256"],
                audience=_SUPABASE_AUDIENCE,
            )
            return RawTokenClaims(**payload)

        raise AuthenticationError(f"خوارزمية التوكن غير مدعومة حالياً: {alg}")

    except jwt.ExpiredSignatureError:
        raise AuthenticationError("رمز المصادقة منتهي الصلاحية — يرجى تسجيل الدخول مجدداً")
    except jwt.PyJWTError as exc:
        logger.warning("jwt_decode_failed", error=str(exc))
        raise AuthenticationError("تعذر التحقق من رمز المصادقة")


async def get_raw_claims(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> RawTokenClaims:
    if not credentials:
        raise AuthenticationError("لم يتم تقديم رمز المصادقة")
    return decode_supabase_token(credentials.credentials)