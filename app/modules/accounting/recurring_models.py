"""
app/modules/accounting/recurring_models.py
نماذج القيود المتكررة — جدولان:
  1. recurring_entries        — القالب
  2. recurring_entry_instances — الأقساط المجدولة
"""
from __future__ import annotations
import uuid
from decimal import Decimal
from typing import List, Optional
from sqlalchemy import (
    Boolean, Date, DateTime, Integer,
    Numeric, String, Text, ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.base import Base
from app.db.mixins import ERPModel


# ══════════════════════════════════════════════════════════
# 1. القالب — RecurringEntry Template
# ══════════════════════════════════════════════════════════
class RecurringEntry(ERPModel, Base):
    """
    قالب القيد المتكرر — يحتوي كل المعلومات الثابتة
    """
    __tablename__ = "recurring_entries"

    # المعرّف والوصف
    code:        Mapped[str] = mapped_column(String(50),  nullable=False, index=True)
    name:        Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)

    # المبلغ والتوزيع
    total_amount:            Mapped[Decimal] = mapped_column(Numeric(18,3), nullable=False)
    installment_amount:      Mapped[Decimal] = mapped_column(Numeric(18,3), nullable=False)
    last_installment_amount: Mapped[Decimal] = mapped_column(Numeric(18,3), nullable=False)
    total_installments:      Mapped[int]     = mapped_column(Integer, nullable=False)

    # التكرار: monthly | quarterly | semiannual | annual | weekly
    frequency: Mapped[str] = mapped_column(String(20), nullable=False, default="monthly")
    # موعد الترحيل: start | end
    post_day:  Mapped[str] = mapped_column(String(10), nullable=False, default="start")

    # الفترة الزمنية
    start_date: Mapped[str] = mapped_column(Date, nullable=False)
    end_date:   Mapped[str] = mapped_column(Date, nullable=False)

    # نوع القيد المُنشأ
    je_type: Mapped[str] = mapped_column(String(10), nullable=False, default="JV")

    # أسطر القيد مخزّنة كـ JSON
    # format: [{account_code, account_name, debit_pct, credit_pct, description,
    #           branch_code, cost_center, project_code, expense_classification_code}]
    lines_template: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    # الحالة: active | paused | completed | cancelled
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active", index=True)

    # إحصاءات
    posted_count:  Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    pending_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    instances: Mapped[List["RecurringEntryInstance"]] = relationship(
        "RecurringEntryInstance",
        back_populates="recurring_entry",
        cascade="all, delete-orphan",
        order_by="RecurringEntryInstance.installment_number",
    )


# ══════════════════════════════════════════════════════════
# 2. الأقساط — RecurringEntryInstance
# ══════════════════════════════════════════════════════════
class RecurringEntryInstance(ERPModel, Base):
    """
    كل سطر في جدول الإطفاء — قسط واحد
    """
    __tablename__ = "recurring_entry_instances"

    recurring_entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("recurring_entries.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    installment_number: Mapped[int]     = mapped_column(Integer, nullable=False)
    scheduled_date:     Mapped[str]     = mapped_column(Date, nullable=False, index=True)
    amount:             Mapped[Decimal] = mapped_column(Numeric(18,3), nullable=False)

    # الحالة: pending | posted | skipped | failed
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)

    # القيد المُنشأ بعد الترحيل
    journal_entry_id:     Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    journal_entry_serial: Mapped[Optional[str]]       = mapped_column(String(50), nullable=True)

    posted_at: Mapped[Optional[str]] = mapped_column(DateTime(timezone=True), nullable=True)
    posted_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    note:      Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    recurring_entry: Mapped["RecurringEntry"] = relationship(
        "RecurringEntry", back_populates="instances"
    )
