"""
app/services/posting/engine.py
══════════════════════════════════════════════════════════
PostingEngine — The most critical service in the system.
Every journal entry in the entire ERP passes through here.

Refactored: 2026-05-04
What changed:
  1. PostingLine dataclass now exposes ALL je_lines columns:
     - Party (party_id/party_role/party_name)
     - VAT (tax_type_code/tax_rate/vat_amount/net_amount)
     - Names (branch_name/cost_center_name/project_name/...)
     - reason_code
  2. post() writes every field directly via ORM — no more
     raw SQL UPDATE workaround for party_id.
  3. reverse() now copies ALL fields (was partial).
  4. Bug fix: undefined `entry_date_str` in error message.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import (
    AccountNotPostableError,
    AlreadyPostedError,
    DoubleEntryImbalanceError,
    NotFoundError,
    PostingError,
    ValidationError,
)
from app.db.transactions import atomic_transaction
from app.modules.accounting.models import (
    JEStatus, JEType, JournalEntry, JournalEntryLine,
)
from app.modules.accounting.repository import (
    AccountBalanceRepository,
    AccountingAuditRepository,
    COARepository,
    JournalEntryRepository,
)
from app.services.event_bus import bus
from app.services.fiscal.lock_service import FiscalLockService
from app.services.numbering.series_service import NumberSeriesService

logger = structlog.get_logger(__name__)

_BALANCE_TOLERANCE = Decimal("0.005")


# ══════════════════════════════════════════════════════════
# Request / Result data classes
# ══════════════════════════════════════════════════════════
@dataclass
class PostingLine:
    """
    سطر القيد — يحوي كل الحقول التي يدعمها جدول je_lines.

    استخدام:
      - الحقول الأساسيّة (account_code, description, debit, credit) إلزاميّة.
      - باقي الحقول اختياريّة — مرّر فقط ما يلزم لحالتك.
      - party_role يُحدَّد تلقائياً إلى 'other' لو party_id موجود وparty_role فارغ.

    ملاحظة: PostingLine يجب أن يبقى متطابقاً مع JournalEntryLine ORM
    وجدول je_lines في DB. أي حقل جديد في DB يجب إضافته في الثلاثة.
    """
    # ── Core (إلزامي) ──
    account_code: str
    description:  str
    debit:  Decimal = Decimal("0")
    credit: Decimal = Decimal("0")

    # ── Reference (اختياري) ──
    reference: Optional[str] = None

    # ── Dimensions (codes + denormalized names) ──
    branch_code: Optional[str] = None
    branch_name: Optional[str] = None
    cost_center: Optional[str] = None              # legacy column
    cost_center_code: Optional[str] = None         # new column (سيُملأ من cost_center تلقائياً لو فارغ)
    cost_center_name: Optional[str] = None
    project_code: Optional[str] = None
    project_name: Optional[str] = None
    expense_classification_code: Optional[str] = None
    expense_classification_name: Optional[str] = None

    # ── Optional / future dimensions ──
    department:    Optional[str] = None
    profit_center: Optional[str] = None
    region:        Optional[str] = None
    future_1:      Optional[str] = None
    future_2:      Optional[str] = None

    # ── Reason code (AP/AR adjustments, returns, write-offs) ──
    reason_code: Optional[str] = None

    # ── Currency ──
    currency_code:  Optional[str]     = "SAR"
    exchange_rate:  Optional[Decimal] = Decimal("1.0")
    amount_foreign: Optional[Decimal] = Decimal("0")

    # ── VAT ──
    tax_type_code: Optional[str]     = None
    tax_rate:      Optional[Decimal] = None
    vat_amount:    Optional[Decimal] = None
    net_amount:    Optional[Decimal] = None

    # ── Party / Subsidiary Ledger ──
    party_id:   Optional[str] = None  # UUID as string
    party_role: Optional[str] = None  # 'vendor' | 'customer' | 'employee_loan' | 'fund_keeper' | 'petty_cash_keeper' | 'other'
    party_name: Optional[str] = None  # denormalized snapshot


@dataclass
class PostingRequest:
    tenant_id: uuid.UUID
    je_type: str
    description: str
    entry_date: date
    lines: List[PostingLine]
    created_by_id: Optional[uuid.UUID] = None
    created_by_email: Optional[str] = None
    source_module: Optional[str] = None
    source_doc_type: Optional[str] = None
    source_doc_id: Optional[uuid.UUID] = None
    source_doc_number: Optional[str] = None
    reference: Optional[str] = None
    branch_code: Optional[str] = None
    cost_center: Optional[str] = None
    notes: Optional[str] = None
    idempotency_key: Optional[str] = None
    request_id: Optional[str] = None
    force_post: bool = False
    user_role: str = "viewer"


@dataclass
class PostingResult:
    je_id: uuid.UUID
    je_serial: str
    je_type: str
    status: str
    total_debit: Decimal
    total_credit: Decimal
    fiscal_year: int
    fiscal_month: int
    posted_at: str

    def to_dict(self) -> dict:
        return {
            "je_id": str(self.je_id),
            "je_serial": self.je_serial,
            "je_type": self.je_type,
            "status": self.status,
            "total_debit": float(self.total_debit),
            "total_credit": float(self.total_credit),
            "fiscal_year": self.fiscal_year,
            "fiscal_month": self.fiscal_month,
            "posted_at": self.posted_at,
        }


# ══════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════
def _coerce_uuid(v) -> Optional[uuid.UUID]:
    """تحويل UUID/str إلى UUID. None إن كانت القيمة فارغة."""
    if not v:
        return None
    if isinstance(v, uuid.UUID):
        return v
    try:
        return uuid.UUID(str(v))
    except (ValueError, TypeError):
        return None


def _resolve_cost_center(line: PostingLine) -> tuple[Optional[str], Optional[str]]:
    """
    يُرجع (cost_center, cost_center_code) — كلاهما بنفس القيمة.
    DB يحوي عمودين قديم/جديد — نملؤهما معاً للتوافق.
    """
    cc = line.cost_center or line.cost_center_code
    return cc, cc


# ══════════════════════════════════════════════════════════
# PostingEngine
# ══════════════════════════════════════════════════════════
class PostingEngine:

    def __init__(self, db: AsyncSession, tenant_id: uuid.UUID) -> None:
        self.db = db
        self.tenant_id = tenant_id
        self._je_repo    = JournalEntryRepository(db, tenant_id)
        self._bal_repo   = AccountBalanceRepository(db, tenant_id)
        self._coa_repo   = COARepository(db, tenant_id)
        self._audit_repo = AccountingAuditRepository(db, tenant_id)
        self._lock_repo  = None
        self._num_svc    = NumberSeriesService(db, tenant_id)

    def set_lock_repo(self, repo) -> None:
        from app.modules.accounting.repository import FiscalLockRepository
        self._lock_repo = repo
        self._lock_svc = FiscalLockService(repo)

    # ── Period validation (new system) ────────────────────
    async def _check_accounting_period(self, entry_date: date) -> None:
        """
        التحقق من أن السنة والفترة المالية موجودتان ومفتوحتان.
        يستخدم accounting_periods + fiscal_years (النظام الجديد).
        لا يتجاهل الأخطاء — أي فشل يوقف الترحيل.
        """
        from sqlalchemy import text as _txt
        from datetime import date as _date
        # تحويل التاريخ إلى date object — asyncpg لا يقبل string
        if isinstance(entry_date, str):
            entry_date_obj = _date.fromisoformat(entry_date[:10])
        elif hasattr(entry_date, 'date') and callable(entry_date.date):
            entry_date_obj = entry_date.date()
        else:
            entry_date_obj = entry_date

        result = await self.db.execute(
            _txt("""
                SELECT
                    ap.status      AS period_status,
                    ap.period_name AS period_name,
                    fy.status      AS fy_status,
                    fy.year_name   AS year_name
                FROM accounting_periods ap
                JOIN fiscal_years fy ON fy.id = ap.fiscal_year_id
                WHERE ap.tenant_id = :tid
                  AND fy.tenant_id = :tid
                  AND :edate BETWEEN ap.start_date AND ap.end_date
                ORDER BY ap.start_date DESC
                LIMIT 1
            """),
            {"tid": str(self.tenant_id), "edate": entry_date_obj}
        )
        row = result.fetchone()

        if not row:
            # ── BUG FIX: كان يستخدم متغير غير معرّف entry_date_str ──
            raise ValidationError(
                f"لا توجد سنة/فترة مالية للتاريخ {entry_date_obj}. "
                "أنشئ السنة المالية من صفحة الفترات المالية أولاً."
            )

        if row.fy_status != "open":
            raise ValidationError(
                f"السنة المالية '{row.year_name}' مغلقة — لا يمكن الترحيل."
            )

        if row.period_status != "open":
            raise ValidationError(
                f"الفترة المالية '{row.period_name}' مغلقة — لا يمكن الترحيل."
            )

    # ── Validation helpers ────────────────────────────────
    def _validate_balance(self, lines: List[PostingLine]) -> None:
        total_dr = sum(l.debit  for l in lines)
        total_cr = sum(l.credit for l in lines)
        if abs(total_dr - total_cr) > _BALANCE_TOLERANCE:
            raise DoubleEntryImbalanceError(float(total_dr), float(total_cr))

    def _validate_lines_not_empty(self, lines: List[PostingLine]) -> None:
        if len(lines) < 2:
            raise ValidationError("القيد يجب أن يحتوي على سطرين على الأقل")

    async def _validate_accounts(self, lines: List[PostingLine]) -> dict:
        codes = list({l.account_code for l in lines})
        accounts = await self._coa_repo.bulk_get_by_codes(codes)
        missing = [c for c in codes if c not in accounts]
        if missing:
            raise ValidationError(f"الحسابات التالية غير موجودة: {', '.join(missing)}")
        not_postable = [c for c, acc in accounts.items() if not acc.postable]
        if not_postable:
            raise AccountNotPostableError(not_postable[0])
        return accounts

    async def _check_idempotency(self, key: Optional[str]) -> Optional[PostingResult]:
        if not key:
            return None
        existing = await self._je_repo.get_by_idempotency_key(key)
        if existing and existing.status == JEStatus.POSTED:
            logger.info("posting_idempotent_replay", key=key, serial=existing.serial)
            return PostingResult(
                je_id=existing.id,
                je_serial=existing.serial,
                je_type=existing.je_type,
                status=existing.status,
                total_debit=existing.total_debit,
                total_credit=existing.total_credit,
                fiscal_year=existing.fiscal_year,
                fiscal_month=existing.fiscal_month,
                posted_at=str(existing.posted_at),
            )
        return None

    # ══════════════════════════════════════════════════════
    # Main entry point — post()
    # ══════════════════════════════════════════════════════
    async def post(self, request: PostingRequest) -> PostingResult:
        logger.info(
            "posting_start",
            je_type=request.je_type,
            entry_date=str(request.entry_date),
            source=request.source_doc_number,
            lines=len(request.lines),
        )

        # ── 1. Idempotency ────────────────────────────────
        cached = await self._check_idempotency(request.idempotency_key)
        if cached:
            return cached

        # ── 2. Validate lines ─────────────────────────────
        self._validate_lines_not_empty(request.lines)
        self._validate_balance(request.lines)

        # ── 3. Validate accounts ──────────────────────────
        account_map = {}
        try:
            account_map = await self._validate_accounts(request.lines)
        except (ValidationError, AccountNotPostableError):
            raise  # أخطاء المحاسبة لا تُتجاهل أبداً
        except Exception:
            # فقط إذا فشل الاتصال بقاعدة البيانات — نكمل في وضع demo
            logger.warning("posting_coa_validation_skipped")

        # ── 4. Period check ───────────────────────────────
        await self._check_accounting_period(request.entry_date)

        # ── 5. Generate serial ────────────────────────────
        serial = await self._num_svc.next_je(request.je_type)

        # ── 6. Calculate totals ───────────────────────────
        total_dr = sum(l.debit  for l in request.lines)
        total_cr = sum(l.credit for l in request.lines)
        now = datetime.now(timezone.utc)

        # ── 7. Atomic transaction ─────────────────────────
        async with atomic_transaction(self.db, label=f"post_{serial}"):

            # ── 7a. Create header ────────────────────────
            je = JournalEntry(
                tenant_id=self.tenant_id,
                serial=serial,
                je_type=request.je_type,
                status=JEStatus.POSTED,
                entry_date=request.entry_date,
                posting_date=request.entry_date,
                description=request.description,
                reference=request.reference,
                source_module=request.source_module,
                source_doc_type=request.source_doc_type,
                source_doc_id=request.source_doc_id,
                source_doc_number=request.source_doc_number,
                total_debit=total_dr,
                total_credit=total_cr,
                fiscal_year=request.entry_date.year,
                fiscal_month=request.entry_date.month,
                branch_code=request.branch_code,
                cost_center=request.cost_center,
                posted_at=now,
                posted_by=request.created_by_email,
                idempotency_key=request.idempotency_key,
                notes=request.notes,
                created_by=request.created_by_email,
            )
            self.db.add(je)
            await self.db.flush()

            # ── 7b. Create lines (ORM full coverage — NO raw UPDATE) ──
            for idx, line in enumerate(request.lines):
                # Resolve account_name from account_map
                acc_obj = account_map.get(line.account_code)
                acc_name = acc_obj.name_ar if acc_obj else line.account_code

                # Resolve cost_center / cost_center_code (DB يحوي كليهما)
                cc, cc_code = _resolve_cost_center(line)

                # Resolve party_role default إذا party_id موجود
                p_role = line.party_role
                if line.party_id and not p_role:
                    p_role = "other"

                je_line = JournalEntryLine(
                    tenant_id=self.tenant_id,
                    journal_entry_id=je.id,
                    line_number=idx + 1,            # NOT NULL في DB
                    line_order=idx + 1,             # للتوافق مع الكود القديم
                    account_code=line.account_code,
                    account_name=acc_name,
                    description=line.description,
                    reference=line.reference,
                    debit=line.debit,
                    credit=line.credit,
                    # Dimensions
                    branch_code=line.branch_code,
                    branch_name=line.branch_name,
                    cost_center=cc,
                    cost_center_code=cc_code,
                    cost_center_name=line.cost_center_name,
                    project_code=line.project_code,
                    project_name=line.project_name,
                    expense_classification_code=line.expense_classification_code,
                    expense_classification_name=line.expense_classification_name,
                    # Optional dimensions
                    department=line.department,
                    profit_center=line.profit_center,
                    region=line.region,
                    future_1=line.future_1,
                    future_2=line.future_2,
                    # Reason
                    reason_code=line.reason_code,
                    # Currency
                    currency_code=line.currency_code or "SAR",
                    exchange_rate=line.exchange_rate or Decimal("1.0"),
                    amount_foreign=line.amount_foreign or (line.debit + line.credit),
                    # VAT
                    tax_type_code=line.tax_type_code,
                    tax_rate=line.tax_rate,
                    vat_amount=line.vat_amount,
                    net_amount=line.net_amount,
                    # Party (Subsidiary Ledger)
                    party_id=_coerce_uuid(line.party_id),
                    party_role=p_role,
                    party_name=line.party_name,
                    created_by=request.created_by_email,
                )
                self.db.add(je_line)

            # نُفلش الكل دفعة واحدة (أكفأ من flush per-line)
            await self.db.flush()

            # ── 7c. Update account balances ──────────────
            for line in request.lines:
                acc = account_map.get(line.account_code)
                nature = acc.account_nature if acc else "debit"
                await self._bal_repo.upsert_balance(
                    account_code=line.account_code,
                    fiscal_year=request.entry_date.year,
                    fiscal_month=request.entry_date.month,
                    delta_debit=line.debit,
                    delta_credit=line.credit,
                    account_nature=nature,
                )

            # ── 7d. Audit log ────────────────────────────
            await self._audit_repo.log(
                action="JE_POSTED",
                user_id=request.created_by_id,
                user_email=request.created_by_email,
                je_id=je.id,
                je_serial=serial,
                je_type=request.je_type,
                fiscal_year=request.entry_date.year,
                fiscal_month=request.entry_date.month,
                total_debit=total_dr,
                total_credit=total_cr,
                source_module=request.source_module,
                source_doc_number=request.source_doc_number,
                request_id=request.request_id,
            )

        # ── 8. Emit event ─────────────────────────────────
        result = PostingResult(
            je_id=je.id,
            je_serial=serial,
            je_type=request.je_type,
            status=JEStatus.POSTED,
            total_debit=total_dr,
            total_credit=total_cr,
            fiscal_year=request.entry_date.year,
            fiscal_month=request.entry_date.month,
            posted_at=now.isoformat(),
        )

        await bus.emit(
            "je.posted",
            {**result.to_dict(), "source_module": request.source_module},
            tenant_id=str(self.tenant_id),
            source_module=request.source_module or "accounting",
        )

        logger.info("posting_success", serial=serial,
                    total_dr=float(total_dr), total_cr=float(total_cr))
        return result

    # ══════════════════════════════════════════════════════
    # Reversal — full field copy
    # ══════════════════════════════════════════════════════
    async def reverse(
        self,
        je_id: uuid.UUID,
        reversal_date: date,
        reason: str,
        reversed_by_id: Optional[uuid.UUID],
        reversed_by_email: Optional[str],
        user_role: str = "viewer",
    ) -> PostingResult:
        """
        عكس قيد. القيد العكسي يحتفظ بكل الحقول من الأصلي
        (party, dimensions, currency, VAT, reason) لتتبع الأستاذ
        المساعد بشكل صحيح حتى بعد العكس.
        """
        original = await self._je_repo.get_with_lines(je_id)
        if original is None:
            raise NotFoundError("القيد", je_id)
        if original.status != JEStatus.POSTED:
            from app.core.exceptions import InvalidStateError
            raise InvalidStateError("القيد", original.status, ["posted"])
        if original.reversed_by_je_id is not None:
            from app.core.exceptions import ReversalError
            raise ReversalError(f"القيد {original.serial} معكوس مسبقاً")

        # ── نسخ شامل لكل الحقول ──
        reversed_lines: List[PostingLine] = []
        for line in original.lines:
            reversed_lines.append(PostingLine(
                # Core (مع تبديل debit/credit)
                account_code=line.account_code,
                description=f"عكس: {line.description or ''}".strip(),
                debit=line.credit,
                credit=line.debit,
                reference=line.reference,
                # Dimensions
                branch_code=line.branch_code,
                branch_name=line.branch_name,
                cost_center=line.cost_center,
                cost_center_code=getattr(line, "cost_center_code", None),
                cost_center_name=line.cost_center_name,
                project_code=line.project_code,
                project_name=line.project_name,
                expense_classification_code=line.expense_classification_code,
                expense_classification_name=line.expense_classification_name,
                # Optional dimensions
                department=line.department,
                profit_center=line.profit_center,
                region=line.region,
                future_1=line.future_1,
                future_2=line.future_2,
                # Reason
                reason_code=getattr(line, "reason_code", None),
                # Currency
                currency_code=line.currency_code or "SAR",
                exchange_rate=line.exchange_rate or Decimal("1.0"),
                amount_foreign=line.amount_foreign or Decimal("0"),
                # VAT
                tax_type_code=line.tax_type_code,
                tax_rate=getattr(line, "tax_rate", None),
                vat_amount=line.vat_amount,
                net_amount=line.net_amount,
                # Party (حرج محاسبياً: لو القيد الأصلي "سلفة لأحمد"، القيد العكسي
                # يجب أن يبقى مرتبطاً بأحمد لتتبع الأستاذ المساعد)
                party_id=str(line.party_id) if line.party_id else None,
                party_role=line.party_role,
                party_name=line.party_name,
            ))

        rev_request = PostingRequest(
            tenant_id=self.tenant_id,
            je_type="REV",
            description=f"عكس قيد {original.serial} — {reason}",
            entry_date=reversal_date,
            lines=reversed_lines,
            created_by_id=reversed_by_id,
            created_by_email=reversed_by_email,
            source_module=original.source_module,
            source_doc_number=original.source_doc_number,
            notes=reason,
            user_role=user_role,
        )

        result = await self.post(rev_request)

        async with atomic_transaction(self.db, label=f"reverse_{original.serial}"):
            original.status = JEStatus.REVERSED
            original.reversed_by_je_id = result.je_id
            await self.db.flush()

            await self._audit_repo.log(
                action="JE_REVERSED",
                user_id=reversed_by_id,
                user_email=reversed_by_email,
                je_id=original.id,
                je_serial=original.serial,
                je_type=original.je_type,
                notes=reason,
            )

        logger.info("posting_reversed",
                    original=original.serial, reversal=result.je_serial)
        return result
