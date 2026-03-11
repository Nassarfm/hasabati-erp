"""
app/repositories/base_repo.py
══════════════════════════════════════════════════════════
Generic async repository.
Provides standard CRUD with automatic tenant isolation.

Every concrete repository extends BaseRepository:
    class JournalEntryRepository(BaseRepository[JournalEntry]):
        model = JournalEntry

Rules:
  1. All queries filter by tenant_id — no exceptions.
  2. Services call repositories — never raw SQL.
  3. Repositories never contain business logic.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from typing import Any, Generic, List, Optional, Sequence, Tuple, Type, TypeVar

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundError, TenantIsolationError
from app.db.mixins import ERPModel

ModelT = TypeVar("ModelT", bound=ERPModel)


class BaseRepository(Generic[ModelT]):
    """
    Tenant-aware async CRUD repository.
    Subclasses must set `model` class attribute.
    """
    model: Type[ModelT]

    def __init__(self, db: AsyncSession, tenant_id: uuid.UUID) -> None:
        self.db = db
        self.tenant_id = tenant_id

    # ── Internal helpers ───────────────────────────────
    def _base_query(self):
        """Every query starts here — tenant_id always applied."""
        return select(self.model).where(
            self.model.tenant_id == self.tenant_id,  # type: ignore[attr-defined]
        )

    def _enforce_tenant(self, obj: ModelT) -> None:
        """Guard: raises if obj belongs to different tenant."""
        if obj.tenant_id != self.tenant_id:  # type: ignore[attr-defined]
            raise TenantIsolationError()

    # ── Read ───────────────────────────────────────────
    async def get(self, obj_id: uuid.UUID) -> Optional[ModelT]:
        """Return object by ID if it belongs to current tenant."""
        result = await self.db.execute(
            self._base_query().where(self.model.id == obj_id)  # type: ignore[attr-defined]
        )
        return result.scalar_one_or_none()

    async def get_or_raise(self, obj_id: uuid.UUID) -> ModelT:
        """Like get() but raises NotFoundError instead of returning None."""
        obj = await self.get(obj_id)
        if obj is None:
            raise NotFoundError(self.model.__name__, obj_id)
        return obj

    async def list(
        self,
        *,
        filters: Optional[List[Any]] = None,
        order_by: Optional[Any] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[ModelT], int]:
        """
        Paginated list with optional filters.
        Returns (items, total_count).
        """
        q = self._base_query()
        if filters:
            for f in filters:
                q = q.where(f)

        # Total count (separate query)
        count_q = select(func.count()).select_from(q.subquery())
        total = (await self.db.execute(count_q)).scalar_one()

        if order_by is not None:
            q = q.order_by(order_by)
        q = q.offset(offset).limit(limit)

        result = await self.db.execute(q)
        return list(result.scalars().all()), total

    async def exists(self, **kwargs: Any) -> bool:
        """Check existence by field values (always tenant-scoped)."""
        q = self._base_query()
        for attr, val in kwargs.items():
            q = q.where(getattr(self.model, attr) == val)
        result = await self.db.execute(select(func.count()).select_from(q.subquery()))
        return result.scalar_one() > 0

    # ── Write ──────────────────────────────────────────
    def create(self, **data: Any) -> ModelT:
        """
        Instantiate model with tenant_id injected.
        Caller must call db.add(obj) — explicit is better.
        """
        obj = self.model(tenant_id=self.tenant_id, **data)  # type: ignore[call-arg]
        return obj

    async def save(self, obj: ModelT) -> ModelT:
        """Add to session and flush (write to DB, not yet committed)."""
        self._enforce_tenant(obj)
        self.db.add(obj)
        await self.db.flush()
        await self.db.refresh(obj)
        return obj

    async def delete_soft(
        self,
        obj_id: uuid.UUID,
        *,
        deleted_by: str,
    ) -> None:
        """Soft delete — never physically remove accounting records."""
        from datetime import datetime, timezone
        obj = await self.get_or_raise(obj_id)
        if hasattr(obj, "is_deleted"):
            obj.is_deleted = True  # type: ignore[attr-defined]
            obj.deleted_at = datetime.now(timezone.utc)  # type: ignore[attr-defined]
            obj.deleted_by = deleted_by  # type: ignore[attr-defined]
            await self.db.flush()
        else:
            raise ValueError(f"{self.model.__name__} does not support soft delete")
