"""
app/modules/accounting/tax_models.py
══════════════════════════════════════
نماذج ضريبة القيمة المضافة
"""
from __future__ import annotations
import uuid
from decimal import Decimal
from typing import Optional
from sqlalchemy import Boolean, Index, Numeric, String, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base
from app.db.mixins import ERPModel


class TaxType(ERPModel, Base):
    __tablename__ = "tax_types"

    code:        Mapped[str]            = mapped_column(String(20),   nullable=False)
    name_ar:     Mapped[str]            = mapped_column(String(255),  nullable=False)
    name_en:     Mapped[Optional[str]]  = mapped_column(String(255),  nullable=True)
    rate:        Mapped[Decimal]        = mapped_column(Numeric(5,2), nullable=False, default=0)
    tax_category:Mapped[str]            = mapped_column(String(20),   nullable=False, default='standard')
    # standard | zero_rated | exempt | out_of_scope
    is_input:    Mapped[bool]           = mapped_column(Boolean, default=True,  nullable=False)
    is_output:   Mapped[bool]           = mapped_column(Boolean, default=True,  nullable=False)
    output_account_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    input_account_code:  Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    is_active:   Mapped[bool]           = mapped_column(Boolean, default=True,  nullable=False)
    is_default:  Mapped[bool]           = mapped_column(Boolean, default=False, nullable=False)
    sort_order:  Mapped[int]            = mapped_column(Integer, default=0,     nullable=False)

    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_tax_tenant_code"),
        Index("ix_tax_tenant_active", "tenant_id", "is_active"),
    )
