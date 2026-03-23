"""
app/modules/accounting/service.py
══════════════════════════════════════════════════════════
Accounting module service layer.
Orchestrates repositories + PostingEngine.
Routers call this — never repositories directly.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import List, Optional, Tuple

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import (
    InvalidStateError, NotFoundError, ValidationError,
)
from app.core.tenant import CurrentUser
from app.modules.accounting.models import (
    AccountBalance, ChartOfAccount, FiscalPeriod, JEStatus, JournalEntry,
)
from app.modules.accounting.repository import (
    AccountBalanceRepository, AccountingAuditRepository,
    COARepository, FiscalLockRepository, JournalEntryRepository,
)
from app.modules.accounting.schemas import (
    COAAccountCreate, COAAccountUpdate, JournalEntryCreate,
    LockPeriodRequest, ReverseJERequest,
)
from app.services.fiscal.lock_service import FiscalLockService
from app.services.posting.engine import PostingEngine

logger = structlog.get_logger(__name__)


class AccountingService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db = db
        self.user = user
        tid = user.tenant_id

        self._je_repo    = JournalEntryRepository(db, tid)
        self._coa_repo   = COARepository(db, tid)
        self._bal_repo   = AccountBalanceRepository(db, tid)
        self._lock_repo  = FiscalLockRepository(db, tid)
        self._audit_repo = AccountingAuditRepository(db, tid)

        self._engine = PostingEngine(db, tid)
        self._engine.set_lock_repo(self._lock_repo)
        self._lock_svc = FiscalLockService(self._lock_repo)

    # ══════════════════════════════════════════════════════
    # Chart of Accounts
    # ══════════════════════════════════════════════════════
    async def create_account(self, data: COAAccountCreate) -> ChartOfAccount:
        self.user.require("can_manage_coa")

        from app.core.exceptions import DuplicateError
        if await self._coa_repo.exists(code=data.code):
            raise DuplicateError("حساب", "code", data.code)

        level = 1
        if data.parent_id:
            parent = await self._coa_repo.get_or_raise(data.parent_id)
            level = parent.level + 1

        acc = self._coa_repo.create(
            code=data.code,
            name_ar=data.name_ar,
            name_en=data.name_en,
            account_type=data.account_type,
            account_nature=data.account_nature,
            parent_id=data.parent_id,
            level=level,
            postable=data.postable,
            is_active=data.is_active,
            opening_balance=data.opening_balance,
            function_type=data.function_type,
            grp=data.grp,
            sub_group=data.sub_group,
            cash_flow_type=data.cash_flow_type,
            dimension_required=data.dimension_required,
            created_by=self.user.email,
        )
        return await self._coa_repo.save(acc)

    async def update_account(self, account_id: uuid.UUID, data: COAAccountUpdate) -> ChartOfAccount:
        self.user.require("can_manage_coa")

        from app.core.exceptions import DuplicateError

        acc = await self._coa_repo.get_or_raise(account_id)
        update_data = data.model_dump(exclude_unset=True)

        # 1) الحقول الأساسية
        if "name_ar" in update_data:
            acc.name_ar = update_data["name_ar"]
        if "name_en" in update_data:
            acc.name_en = update_data["name_en"]
        if "account_type" in update_data:
            acc.account_type = update_data["account_type"]
        if "account_nature" in update_data:
            acc.account_nature = update_data["account_nature"]
        if "postable" in update_data:
            acc.postable = update_data["postable"]
        if "is_active" in update_data:
            acc.is_active = update_data["is_active"]
        if "opening_balance" in update_data:
            acc.opening_balance = update_data["opening_balance"]
        if "function_type" in update_data:
            acc.function_type = update_data["function_type"]
        if "grp" in update_data:
            acc.grp = update_data["grp"]
        if "sub_group" in update_data:
            acc.sub_group = update_data["sub_group"]
        if "cash_flow_type" in update_data:
            acc.cash_flow_type = update_data["cash_flow_type"]
        if "dimension_required" in update_data:
            acc.dimension_required = update_data["dimension_required"]

        # 2) الحساب الأب — بشكل آمن
        if "parent_id" in update_data:
            new_parent_id = update_data["parent_id"]
            if new_parent_id and str(new_parent_id) == str(acc.id):
                raise ValidationError("لا يمكن جعل الحساب أبًا لنفسه")
            if new_parent_id:
                parent = await self._coa_repo.get_or_raise(new_parent_id)
                acc.parent_id = parent.id
                acc.level = parent.level + 1
            else:
                acc.parent_id = None
                acc.level = 1

        try:
            acc.updated_by = self.user.email
        except Exception:
            pass

        await self.db.flush()
        await self.db.refresh(acc)
        return acc

    async def list_accounts(self) -> List[ChartOfAccount]:
        return await self._coa_repo.list_active()

    async def reset_coa(self) -> dict:
        """
        إعادة تهيئة دليل الحسابات — حذف كامل.
        يُسمح فقط إذا لا توجد قيود مرحّلة.
        صلاحية: owner فقط.
        """
        self.user.require("can_manage_coa")

        from sqlalchemy import select, delete, func as sql_func
        from app.modules.accounting.models import (
            JournalEntry, AccountBalance, ChartOfAccount
        )

        # 1) التحقق: لا توجد قيود مرحّلة
        result = await self.db.execute(
            select(sql_func.count()).select_from(JournalEntry).where(
                JournalEntry.tenant_id == self.user.tenant_id,
                JournalEntry.status == "posted",
            )
        )
        posted_count = result.scalar() or 0
        if posted_count > 0:
            raise ValidationError(
                f"لا يمكن حذف دليل الحسابات — يوجد {posted_count} قيد مرحّل. "
                "يجب حذف جميع القيود أولاً."
            )

        # 2) حذف الأرصدة
        await self.db.execute(
            delete(AccountBalance).where(
                AccountBalance.tenant_id == self.user.tenant_id
            )
        )

        # 3) حذف دليل الحسابات
        result2 = await self.db.execute(
            select(sql_func.count()).select_from(ChartOfAccount).where(
                ChartOfAccount.tenant_id == self.user.tenant_id
            )
        )
        coa_count = result2.scalar() or 0

        await self.db.execute(
            delete(ChartOfAccount).where(
                ChartOfAccount.tenant_id == self.user.tenant_id
            )
        )

        # 4) تسجيل في Audit Log
        await self._audit_repo.log(
            action="COA_RESET",
            user_id=self.user.user_id,
            user_email=self.user.email,
            notes=f"تم حذف {coa_count} حساب من دليل الحسابات",
        )

        await self.db.flush()

        return {
            "deleted_accounts": coa_count,
            "message": f"تم حذف {coa_count} حساب بنجاح — دليل الحسابات فارغ الآن",
        }

    # ══════════════════════════════════════════════════════
    # Journal Entries
    # ══════════════════════════════════════════════════════
    async def create_draft_je(self, data: JournalEntryCreate) -> JournalEntry:
        self.user.require("can_create_je")

        from app.services.numbering.series_service import NumberSeriesService
        num_svc = NumberSeriesService(self.db, self.user.tenant_id)
        serial = await num_svc.next_je(data.je_type)

        je = JournalEntry(
            tenant_id=self.user.tenant_id,
            serial=serial,
            je_type=data.je_type,
            status=JEStatus.DRAFT,
            entry_date=data.entry_date,
            description=data.description,
            reference=data.reference,
            source_module=data.source_module,
            source_doc_type=data.source_doc_type,
            source_doc_id=data.source_doc_id,
            source_doc_number=data.source_doc_number,
            branch_code=data.branch_code,
            cost_center=data.cost_center,
            notes=data.notes,
            fiscal_year=data.entry_date.year,
            fiscal_month=data.entry_date.month,
            total_debit=sum(l.debit for l in data.lines),
            total_credit=sum(l.credit for l in data.lines),
            created_by=self.user.email,
        )
        self.db.add(je)
        await self.db.flush()

        for idx, line in enumerate(data.lines):
            from app.modules.accounting.models import JournalEntryLine
            je_line = JournalEntryLine(
                tenant_id=self.user.tenant_id,
                journal_entry_id=je.id,
                line_order=idx + 1,
                account_code=line.account_code,
                account_name=line.account_code,
                description=line.description,
                debit=line.debit,
                credit=line.credit,
                branch_code=line.branch_code,
                cost_center=line.cost_center,
                created_by=self.user.email,
            )
            self.db.add(je_line)

        await self.db.flush()
        logger.info("je_draft_created", serial=serial)
        return je

    async def submit_je(self, je_id: uuid.UUID) -> JournalEntry:
        """إرسال القيد للمراجعة: draft → pending_review"""
        from datetime import datetime, timezone
        je = await self._je_repo.get_with_lines(je_id)
        if not je:
            raise NotFoundError("القيد", je_id)
        if je.status != "draft":
            raise ValidationError(f"لا يمكن إرسال القيد — الحالة الحالية: {je.status}")
        je.status = "pending_review"
        je.submitted_at = datetime.now(timezone.utc)
        je.submitted_by = self.user.email
        await self.db.flush()
        return je

    async def approve_je(self, je_id: uuid.UUID) -> JournalEntry:
        """الموافقة على القيد: pending_review → posted"""
        from datetime import datetime, timezone
        je = await self._je_repo.get_with_lines(je_id)
        if not je:
            raise NotFoundError("القيد", je_id)
        if je.status != "pending_review":
            raise ValidationError(f"لا يمكن الموافقة — الحالة الحالية: {je.status}")
        je.approved_at = datetime.now(timezone.utc)
        je.approved_by = self.user.email
        await self.db.flush()
        # ترحيل تلقائي بعد الموافقة
        return await self.post_je(je_id, force=False)

    async def reject_je(self, je_id: uuid.UUID, note: str = "") -> JournalEntry:
        """رفض القيد: pending_review → draft مع ملاحظة"""
        from datetime import datetime, timezone
        je = await self._je_repo.get_with_lines(je_id)
        if not je:
            raise NotFoundError("القيد", je_id)
        if je.status != "pending_review":
            raise ValidationError(f"لا يمكن الرفض — الحالة الحالية: {je.status}")
        je.status = "draft"
        je.rejected_at = datetime.now(timezone.utc)
        je.rejected_by = self.user.email
        je.rejection_note = note
        await self.db.flush()
        return je

    async def post_je(self, je_id: uuid.UUID, *, force: bool = False) -> JournalEntry:
        self.user.require("can_post_je")

        from datetime import datetime, timezone
        from sqlalchemy import select
        from app.core.exceptions import AccountNotPostableError

        je = await self._je_repo.get_with_lines(je_id)
        if not je:
            raise NotFoundError("القيد", je_id)

        if je.status != JEStatus.DRAFT:
            raise InvalidStateError("القيد", je.status, ["draft"])

        if not je.lines or len(je.lines) < 2:
            raise ValidationError("القيد يجب أن يحتوي على سطرين على الأقل")

        total_dr = sum((l.debit or Decimal("0")) for l in je.lines)
        total_cr = sum((l.credit or Decimal("0")) for l in je.lines)

        if abs(total_dr - total_cr) > Decimal("0.001"):
            raise ValidationError(f"القيد غير متوازن — مدين: {total_dr} | دائن: {total_cr}")

        await self._lock_svc.guard(
            entry_date=je.entry_date,
            user_role=self.user.role,
            force=force,
        )

        # ── التحقق من الأبعاد ──────────────────────────────────────
        from sqlalchemy import select as _sel
        try:
            from app.modules.settings.models import Branch, CostCenter, Project
            from app.modules.accounting.models import ChartOfAccount as _COA

            for line in je.lines:
                # جلب بيانات الحساب للتحقق من dimension_required
                acct_r = (await self.db.execute(
                    _sel(_COA).where(
                        _COA.tenant_id == self.user.tenant_id,
                        _COA.code == line.account_code,
                    )
                )).scalar_one_or_none()

                dim_required = acct_r.dimension_required if acct_r else False
                is_expense   = acct_r.account_type == "expense" if acct_r else False

                if dim_required:
                    # فرع إجباري
                    if not line.branch_code:
                        raise ValidationError(
                            f"الحساب '{line.account_code}' يتطلب تحديد الفرع"
                        )
                    # مركز التكلفة إجباري
                    if not line.cost_center:
                        raise ValidationError(
                            f"الحساب '{line.account_code}' يتطلب تحديد مركز التكلفة"
                        )
                    # المشروع إجباري
                    if not line.project_code:
                        raise ValidationError(
                            f"الحساب '{line.account_code}' يتطلب تحديد المشروع"
                        )
                    # تصنيف المصروف إجباري للمصاريف
                    if is_expense and not line.expense_classification_code:
                        raise ValidationError(
                            f"الحساب '{line.account_code}' يتطلب تحديد تصنيف المصروف"
                        )

                # التحقق من حالة الفرع
                if line.branch_code:
                    br = (await self.db.execute(
                        _sel(Branch).where(
                            Branch.tenant_id == self.user.tenant_id,
                            Branch.code == line.branch_code,
                        )
                    )).scalar_one_or_none()
                    if br and not br.is_active:
                        raise ValidationError(
                            f"الفرع '{line.branch_code}' موقف — السبب: {br.deactivation_reason or 'غير محدد'}"
                        )

                # التحقق من حالة مركز التكلفة
                if line.cost_center:
                    cc = (await self.db.execute(
                        _sel(CostCenter).where(
                            CostCenter.tenant_id == self.user.tenant_id,
                            CostCenter.code == line.cost_center,
                        )
                    )).scalar_one_or_none()
                    if cc and not cc.is_active:
                        raise ValidationError(
                            f"مركز التكلفة '{line.cost_center}' موقف — السبب: {cc.deactivation_reason or 'غير محدد'}"
                        )

                # التحقق من حالة المشروع
                if line.project_code:
                    proj = (await self.db.execute(
                        _sel(Project).where(
                            Project.tenant_id == self.user.tenant_id,
                            Project.code == line.project_code,
                        )
                    )).scalar_one_or_none()
                    if proj and not proj.is_active:
                        raise ValidationError(
                            f"المشروع '{line.project_code}' غير نشط — لا يمكن الترحيل عليه."
                        )
                    if proj and proj.status in ('completed', 'cancelled'):
                        raise ValidationError(
                            f"المشروع '{line.project_code}' في حالة '{proj.status}' — لا يمكن الترحيل عليه."
                        )
        except ValidationError:
            raise
        except Exception:
            pass

        codes = list({line.account_code for line in je.lines})
        result = await self.db.execute(
            select(ChartOfAccount).where(
                ChartOfAccount.tenant_id == self.user.tenant_id,
                ChartOfAccount.code.in_(codes),
            )
        )
        accounts = result.scalars().all()
        account_map = {acc.code: acc for acc in accounts}

        missing_codes = [code for code in codes if code not in account_map]
        if missing_codes:
            raise ValidationError(f"الحسابات التالية غير موجودة: {', '.join(missing_codes)}")

        not_postable = [acc.code for acc in accounts if not acc.postable]
        if not_postable:
            raise AccountNotPostableError(not_postable[0])

        now = datetime.now(timezone.utc)

        for line in je.lines:
            acc = account_map[line.account_code]
            line.account_name = acc.name_ar
            await self._bal_repo.upsert_balance(
                account_code=line.account_code,
                fiscal_year=je.entry_date.year,
                fiscal_month=je.entry_date.month,
                delta_debit=line.debit,
                delta_credit=line.credit,
                account_nature=acc.account_nature,
            )

        je.status = JEStatus.POSTED
        je.posting_date = je.entry_date
        je.posted_at = now
        je.posted_by = self.user.email
        je.fiscal_year = je.entry_date.year
        je.fiscal_month = je.entry_date.month

        await self.db.flush()

        await self._audit_repo.log(
            action="JE_POSTED",
            user_id=self.user.user_id,
            user_email=self.user.email,
            je_id=je.id,
            je_serial=je.serial,
            je_type=je.je_type,
            fiscal_year=je.fiscal_year,
            fiscal_month=je.fiscal_month,
            total_debit=total_dr,
            total_credit=total_cr,
            source_module=je.source_module,
            source_doc_number=je.source_doc_number,
        )

        return await self._je_repo.get_with_lines(je_id)

    async def reverse_je(self, je_id: uuid.UUID, data: ReverseJERequest) -> dict:
        self.user.require("can_reverse_je")
        result = await self._engine.reverse(
            je_id=je_id,
            reversal_date=data.reversal_date,
            reason=data.reason,
            reversed_by_id=self.user.user_id,
            reversed_by_email=self.user.email,
            user_role=self.user.role,
        )
        return result.to_dict()

    async def get_je(self, je_id: uuid.UUID) -> JournalEntry:
        je = await self._je_repo.get_with_lines(je_id)
        if not je:
            raise NotFoundError("القيد", je_id)
        return je

    async def list_je(
        self,
        *,
        status: Optional[str] = None,
        je_type: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        fiscal_year: Optional[int] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[JournalEntry], int]:
        return await self._je_repo.list_paginated(
            status=status,
            je_type=je_type,
            date_from=date_from,
            date_to=date_to,
            fiscal_year=fiscal_year,
            offset=offset,
            limit=limit,
        )

    # ══════════════════════════════════════════════════════
    # Fiscal Locks
    # ══════════════════════════════════════════════════════
    async def lock_period(self, data: LockPeriodRequest) -> dict:
        self.user.require("can_lock_period")

        from sqlalchemy import select
        from app.modules.accounting.models import FiscalPeriod
        result = await self.db.execute(
            select(FiscalPeriod)
            .where(FiscalPeriod.tenant_id == self.user.tenant_id)
            .where(FiscalPeriod.fiscal_year == data.fiscal_year)
            .where(FiscalPeriod.fiscal_month == (data.fiscal_month or 0))
        )
        period = result.scalar_one_or_none()

        if not period:
            from datetime import date as date_type
            period = FiscalPeriod(
                tenant_id=self.user.tenant_id,
                fiscal_year=data.fiscal_year,
                fiscal_month=data.fiscal_month or 0,
                name_ar=f"فترة {data.fiscal_year}" + (
                    f"/{data.fiscal_month:02d}" if data.fiscal_month else ""
                ),
                start_date=date_type(data.fiscal_year, data.fiscal_month or 1, 1),
                end_date=date_type(data.fiscal_year, data.fiscal_month or 12, 28),
                created_by=self.user.email,
            )
            self.db.add(period)
            await self.db.flush()

        lock = await self._lock_svc.lock_period(
            fiscal_year=data.fiscal_year,
            fiscal_month=data.fiscal_month,
            lock_type=data.lock_type,
            locked_by_email=self.user.email,
            reason=data.reason,
            period_id=period.id,
            tenant_id=self.user.tenant_id,
        )
        return {"id": str(lock.id), "lock_type": lock.lock_type, "locked_by": lock.locked_by}

    async def unlock_period(self, lock_id: uuid.UUID) -> dict:
        self.user.require("can_lock_period")
        lock = await self._lock_svc.unlock_period(
            lock_id=lock_id,
            unlocked_by_email=self.user.email,
            user_role=self.user.role,
        )
        return {"id": str(lock.id), "is_active": lock.is_active}

    async def rebuild_balances(self, fiscal_year: int) -> dict:
        self.user.require("can_post_je")

        from sqlalchemy import select, delete
        from app.modules.accounting.models import JournalEntry, JournalEntryLine, AccountBalance, ChartOfAccount

        await self.db.execute(
            delete(AccountBalance).where(
                AccountBalance.tenant_id == self.user.tenant_id,
                AccountBalance.fiscal_year == fiscal_year,
            )
        )
        await self.db.flush()

        je_result = await self.db.execute(
            select(JournalEntry).where(
                JournalEntry.tenant_id == self.user.tenant_id,
                JournalEntry.fiscal_year == fiscal_year,
                JournalEntry.status == "posted",
            )
        )
        journal_entries = je_result.scalars().all()

        codes_result = await self.db.execute(
            select(ChartOfAccount.code, ChartOfAccount.account_nature).where(
                ChartOfAccount.tenant_id == self.user.tenant_id,
            )
        )
        nature_map = {row.code: row.account_nature for row in codes_result.fetchall()}

        je_count = 0
        for je in journal_entries:
            lines_result = await self.db.execute(
                select(JournalEntryLine).where(
                    JournalEntryLine.journal_entry_id == je.id
                )
            )
            lines = lines_result.scalars().all()
            for line in lines:
                nature = nature_map.get(line.account_code, "debit")
                await self._bal_repo.upsert_balance(
                    account_code=line.account_code,
                    fiscal_year=je.entry_date.year,
                    fiscal_month=je.entry_date.month,
                    delta_debit=line.debit or Decimal("0"),
                    delta_credit=line.credit or Decimal("0"),
                    account_nature=nature,
                )
            je_count += 1

        await self.db.flush()

        return {
            "fiscal_year": fiscal_year,
            "journal_entries_processed": je_count,
            "message": f"تم إعادة بناء الأرصدة بنجاح — تمت معالجة {je_count} قيد",
        }

    async def list_locks(self) -> list:
        locks = await self._lock_repo.list_active_locks()
        return [
            {
                "id": str(l.id),
                "fiscal_year": l.fiscal_year,
                "fiscal_month": l.fiscal_month,
                "lock_type": l.lock_type,
                "locked_by": l.locked_by,
                "reason": l.reason,
            }
            for l in locks
        ]

    # ══════════════════════════════════════════════════════
    # Reports
    # ══════════════════════════════════════════════════════
    async def get_trial_balance(
        self,
        fiscal_year: int,
        fiscal_month: Optional[int] = None,
    ) -> dict:
        from sqlalchemy import select as sa_select

        q = sa_select(AccountBalance).where(
            AccountBalance.tenant_id == self.user.tenant_id,
            AccountBalance.fiscal_year == fiscal_year,
        )
        if fiscal_month is not None:
            q = q.where(AccountBalance.fiscal_month <= fiscal_month)

        result = await self.db.execute(q.order_by(AccountBalance.account_code, AccountBalance.fiscal_month))
        balances = result.scalars().all()

        codes = sorted({b.account_code for b in balances})
        account_map = {}
        if codes:
            acc_result = await self.db.execute(
                sa_select(ChartOfAccount).where(
                    ChartOfAccount.tenant_id == self.user.tenant_id,
                    ChartOfAccount.code.in_(codes),
                )
            )
            account_map = {acc.code: acc for acc in acc_result.scalars().all()}

        account_data: dict = {}
        for bal in balances:
            code = bal.account_code
            acc = account_map.get(code)
            nature = (acc.account_nature if acc else "debit").lower()
            if code not in account_data:
                account_data[code] = {
                    "account_code": code,
                    "account_name": acc.name_ar if acc else code,
                    "account_nature": nature,
                    "opening_debit": Decimal("0"),
                    "opening_credit": Decimal("0"),
                    "period_debit": Decimal("0"),
                    "period_credit": Decimal("0"),
                }
            row = account_data[code]
            debit_total  = Decimal(str(bal.debit_total  or 0))
            credit_total = Decimal(str(bal.credit_total or 0))
            if fiscal_month is None:
                row["period_debit"]   += debit_total
                row["period_credit"]  += credit_total
            else:
                if bal.fiscal_month < fiscal_month:
                    row["opening_debit"]  += debit_total
                    row["opening_credit"] += credit_total
                elif bal.fiscal_month == fiscal_month:
                    row["period_debit"]   += debit_total
                    row["period_credit"]  += credit_total

        lines = []
        total_opening_dr = total_opening_cr = Decimal("0")
        total_period_dr  = total_period_cr  = Decimal("0")
        total_closing_dr = total_closing_cr = Decimal("0")
        total_closing_net = Decimal("0")

        for code in sorted(account_data.keys()):
            row    = account_data[code]
            nature = row["account_nature"]
            od = row["opening_debit"];  oc = row["opening_credit"]
            pd = row["period_debit"];   pc = row["period_credit"]

            if nature == "debit":
                opening_net = od - oc
                period_net  = pd - pc
            else:
                opening_net = oc - od
                period_net  = pc - pd

            closing_natural = opening_net + period_net

            if nature == "debit":
                cd = closing_natural if closing_natural > 0 else Decimal("0")
                cc = (-closing_natural) if closing_natural < 0 else Decimal("0")
            else:
                cc = closing_natural if closing_natural > 0 else Decimal("0")
                cd = (-closing_natural) if closing_natural < 0 else Decimal("0")

            closing_net = cd if cd > 0 else -cc

            lines.append({
                "account_code":   code,
                "account_name":   row["account_name"],
                "account_nature": nature,
                "opening_debit":  float(od),
                "opening_credit": float(oc),
                "period_debit":   float(pd),
                "period_credit":  float(pc),
                "closing_debit":  float(cd),
                "closing_credit": float(cc),
                "closing_net":    float(closing_net),
            })

            total_opening_dr += od;   total_opening_cr += oc
            total_period_dr  += pd;   total_period_cr  += pc
            total_closing_dr += cd;   total_closing_cr += cc
            total_closing_net += closing_net

        return {
            "fiscal_year":           fiscal_year,
            "fiscal_month":          fiscal_month,
            "lines":                 lines,
            "opening_debit_total":   float(total_opening_dr),
            "opening_credit_total":  float(total_opening_cr),
            "period_debit_total":    float(total_period_dr),
            "period_credit_total":   float(total_period_cr),
            "closing_debit_total":   float(total_closing_dr),
            "closing_credit_total":  float(total_closing_cr),
            "closing_net_total":     float(total_closing_net),
            "is_balanced":           abs(total_closing_dr - total_closing_cr) < Decimal("0.01"),
        }
