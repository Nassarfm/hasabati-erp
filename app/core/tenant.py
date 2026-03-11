"""
app/core/tenant.py
══════════════════════════════════════════════════════════
Tenant context extraction.

After JWT verification, we need to know:
  - which tenant this user belongs to
  - what role they have in that tenant
  - what permissions they hold

This is loaded from the `user_roles` table in Supabase
(same table populated by saas_migration.sql).

CurrentUser is the single source of truth passed to
every service and repository.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

import structlog
from fastapi import Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import TenantNotFoundError
from app.core.security import RawTokenClaims, get_raw_claims
from app.db.session import get_db

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CurrentUser:
    """
    Immutable user context — injected into every endpoint.
    Built once per request after JWT + DB lookup.
    """
    user_id:    UUID
    email:      str
    tenant_id:  UUID
    role:       str          # owner | admin | accountant | sales | warehouse | viewer
    permissions: dict = field(default_factory=dict, hash=False, compare=False)

    # ── Permission helpers ─────────────────────────────
    def can(self, permission: str) -> bool:
        """True if user has permission OR is owner/admin (bypass)."""
        if self.role in ("owner", "admin"):
            return True
        return bool(self.permissions.get(permission, False))

    def require(self, permission: str) -> None:
        """Raise PermissionDeniedError if user lacks permission."""
        if not self.can(permission):
            from app.core.exceptions import PermissionDeniedError
            raise PermissionDeniedError(permission)

    def is_owner_or_admin(self) -> bool:
        return self.role in ("owner", "admin")


# ── DEMO users (bypass DB for local dev / GitHub Pages demo) ─
_DEMO_USERS: dict[str, dict] = {
    "admin@hasabati.com": {
        "role": "owner",
        "permissions": {},
        "tenant_id": "00000000-0000-0000-0000-000000000001",
    },
    "accountant@hasabati.com": {
        "role": "accountant",
        "permissions": {
            "can_post_je": True,
            "can_view_reports": True,
        },
        "tenant_id": "00000000-0000-0000-0000-000000000001",
    },
    "sales@hasabati.com": {
        "role": "sales",
        "permissions": {
            "can_create_invoice": True,
        },
        "tenant_id": "00000000-0000-0000-0000-000000000001",
    },
    "warehouse@hasabati.com": {
        "role": "warehouse",
        "permissions": {
            "can_post_grn": True,
        },
        "tenant_id": "00000000-0000-0000-0000-000000000001",
    },
}

_DEMO_TENANT_UUID = UUID("00000000-0000-0000-0000-000000000001")


async def _load_user_tenant(
    user_id: UUID,
    email: str,
    db: AsyncSession,
) -> tuple[UUID, str, dict]:
    """
    Load tenant_id, role, permissions from user_roles table.
    Falls back to demo profile for demo emails.
    Returns (tenant_id, role, permissions).
    """
    # ── Demo bypass ───────────────────────────────────
    demo = _DEMO_USERS.get(email)
    if demo:
        logger.debug("demo_user_bypass", email=email, role=demo["role"])
        return UUID(demo["tenant_id"]), demo["role"], demo["permissions"]

    # ── Real DB lookup ────────────────────────────────
    result = await db.execute(
        text("""
            SELECT ur.tenant_id, ur.role, ur.permissions
            FROM user_roles ur
            WHERE ur.user_id = :uid
              AND ur.is_active = true
            LIMIT 1
        """),
        {"uid": str(user_id)},
    )
    row = result.mappings().first()
    if not row:
        raise TenantNotFoundError()

    return UUID(str(row["tenant_id"])), row["role"], row["permissions"] or {}


async def get_current_user(
    claims: RawTokenClaims = Depends(get_raw_claims),
    db: AsyncSession = Depends(get_db),
) -> CurrentUser:
    """
    Primary dependency for all protected endpoints.
    Verifies JWT → loads tenant → returns CurrentUser.

    Usage:
        @router.get("/...")
        async def my_endpoint(user: CurrentUser = Depends(get_current_user)):
    """
    tenant_id, role, permissions = await _load_user_tenant(
        claims.user_id, claims.email or "", db
    )

    user = CurrentUser(
        user_id=claims.user_id,
        email=claims.email or "",
        tenant_id=tenant_id,
        role=role,
        permissions=permissions,
    )

    # Bind to structlog context for this request's log entries
    structlog.contextvars.bind_contextvars(
        user_id=str(user.user_id),
        tenant_id=str(user.tenant_id),
        role=user.role,
    )

    return user
