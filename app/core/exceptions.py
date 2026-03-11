"""
app/core/exceptions.py
══════════════════════════════════════════════════════════
ERP exception hierarchy.
Every business error is a typed exception with:
  - Arabic message for the user
  - machine-readable code for the frontend
  - HTTP status code
  - optional structured detail payload

Rule: raise typed exceptions from services,
      never return error dicts.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Any, Optional


# ── Base ───────────────────────────────────────────────────
class ERPException(Exception):
    """Root of all ERP exceptions."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "ERP_ERROR",
        status_code: int = 400,
        detail: Optional[Any] = None,
    ) -> None:
        self.message = message
        self.code = code
        self.status_code = status_code
        self.detail = detail
        super().__init__(message)

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "detail": self.detail,
        }


# ── Auth / Tenant ──────────────────────────────────────────
class AuthenticationError(ERPException):
    def __init__(self, msg: str = "رمز المصادقة غير صالح أو منتهي الصلاحية") -> None:
        super().__init__(msg, code="AUTHENTICATION_FAILED", status_code=401)


class PermissionDeniedError(ERPException):
    def __init__(self, action: str) -> None:
        super().__init__(
            f"ليس لديك صلاحية تنفيذ: {action}",
            code="PERMISSION_DENIED",
            status_code=403,
        )


class TenantNotFoundError(ERPException):
    def __init__(self) -> None:
        super().__init__(
            "المستخدم غير مرتبط بأي مؤسسة — تواصل مع المدير",
            code="TENANT_NOT_FOUND",
            status_code=403,
        )


class TenantIsolationError(ERPException):
    """Attempt to access another tenant's data — always 403, never 404."""
    def __init__(self) -> None:
        super().__init__(
            "لا يمكن الوصول لبيانات هذا المستأجر",
            code="TENANT_ISOLATION_VIOLATION",
            status_code=403,
        )


# ── Resource ───────────────────────────────────────────────
class NotFoundError(ERPException):
    def __init__(self, resource: str, identifier: Any) -> None:
        super().__init__(
            f"'{resource}' غير موجود — معرّف: {identifier}",
            code="NOT_FOUND",
            status_code=404,
        )


class DuplicateError(ERPException):
    def __init__(self, resource: str, field: str, value: Any) -> None:
        super().__init__(
            f"'{resource}' مكرر — {field}: {value} موجود مسبقاً",
            code="DUPLICATE_RECORD",
            status_code=409,
        )


# ── Validation ─────────────────────────────────────────────
class ValidationError(ERPException):
    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(
            message,
            code="VALIDATION_ERROR",
            status_code=422,
            detail=detail,
        )


class InvalidStateError(ERPException):
    """Operation not allowed in current document state."""
    def __init__(self, resource: str, current: str, allowed: list[str]) -> None:
        super().__init__(
            f"'{resource}' في حالة '{current}' — العمليات المسموحة: {', '.join(allowed)}",
            code="INVALID_STATE",
            status_code=409,
        )


# ── Accounting / Posting ───────────────────────────────────
class DoubleEntryImbalanceError(ERPException):
    def __init__(self, total_dr: float, total_cr: float) -> None:
        diff = abs(total_dr - total_cr)
        super().__init__(
            f"القيد غير متوازن — مدين: {total_dr:,.3f} | دائن: {total_cr:,.3f} | فرق: {diff:,.3f}",
            code="DOUBLE_ENTRY_IMBALANCE",
            status_code=422,
            detail={"total_dr": total_dr, "total_cr": total_cr, "diff": diff},
        )


class FiscalPeriodLockedError(ERPException):
    def __init__(self, year: int, month: Optional[int], lock_type: str) -> None:
        period = f"{year}" + (f"/{month:02d}" if month else "")
        super().__init__(
            f"الفترة المالية {period} مقفلة ({lock_type}) — لا يمكن الترحيل",
            code="FISCAL_PERIOD_LOCKED",
            status_code=409,
            detail={"year": year, "month": month, "lock_type": lock_type},
        )


class PostingError(ERPException):
    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(
            message,
            code="POSTING_ERROR",
            status_code=400,
            detail=detail,
        )


class AlreadyPostedError(ERPException):
    def __init__(self, doc_type: str, doc_number: str) -> None:
        super().__init__(
            f"'{doc_type}' رقم {doc_number} مرحَّل مسبقاً — لا يمكن إعادة الترحيل",
            code="ALREADY_POSTED",
            status_code=409,
        )


class ReversalError(ERPException):
    def __init__(self, message: str) -> None:
        super().__init__(message, code="REVERSAL_ERROR", status_code=409)


# ── Idempotency ────────────────────────────────────────────
class IdempotencyConflictError(ERPException):
    """Same idempotency key was used with different payload."""
    def __init__(self, key: str) -> None:
        super().__init__(
            f"مفتاح التكرار '{key}' مستخدم مسبقاً مع بيانات مختلفة",
            code="IDEMPOTENCY_CONFLICT",
            status_code=409,
        )


# ── Business Domain ────────────────────────────────────────
class InsufficientStockError(ERPException):
    def __init__(self, product: str, required: float, available: float) -> None:
        super().__init__(
            f"مخزون غير كافٍ — '{product}': مطلوب {required:,.3f} | متوفر {available:,.3f}",
            code="INSUFFICIENT_STOCK",
            status_code=409,
            detail={"product": product, "required": required, "available": available},
        )


class ThreeWayMatchError(ERPException):
    def __init__(self, message: str, match_detail: Any = None) -> None:
        super().__init__(
            message,
            code="THREE_WAY_MATCH_FAILED",
            status_code=409,
            detail=match_detail,
        )


class AccountNotPostableError(ERPException):
    def __init__(self, account_code: str) -> None:
        super().__init__(
            f"الحساب '{account_code}' غير قابل للترحيل — يجب استخدام حساب تحليلي",
            code="ACCOUNT_NOT_POSTABLE",
            status_code=422,
        )
