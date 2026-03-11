"""
app/core/response.py
══════════════════════════════════════════════════════════
Unified API response helpers.
Every endpoint uses these — never builds raw dicts.

Success shape:
{
    "success": true,
    "data": <payload>,
    "message": "تم الحفظ",
    "meta": {"page": 1, "total": 100, ...},
    "request_id": "uuid"
}
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import math
from typing import Any, Generic, List, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class Meta(BaseModel):
    page: int
    page_size: int
    total: int
    total_pages: int


class APIResponse(BaseModel, Generic[T]):
    success: bool = True
    data: Optional[T] = None
    message: Optional[str] = None
    meta: Optional[Meta] = None
    request_id: Optional[str] = None


# ── Builder helpers (used in routers) ─────────────────────
def ok(
    data: Any = None,
    *,
    message: Optional[str] = None,
    meta: Optional[dict] = None,
    request_id: str = "",
) -> dict:
    return {
        "success": True,
        "data": data,
        "message": message,
        "meta": meta,
        "request_id": request_id,
    }


def created(data: Any = None, *, message: str = "تم الإنشاء بنجاح") -> dict:
    return ok(data=data, message=message)


def paginated(
    items: List[Any],
    *,
    total: int,
    page: int,
    page_size: int,
    message: Optional[str] = None,
) -> dict:
    return ok(
        data=items,
        message=message,
        meta={
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": math.ceil(total / page_size) if page_size else 0,
        },
    )


def no_content(*, message: str = "تم التنفيذ") -> dict:
    return ok(message=message)
