"""
app/services/posting/templates.py
══════════════════════════════════════════════════════════
Pre-built PostingRequest templates for every transaction type.

Each module calls these helpers instead of building
PostingRequest from scratch. Ensures consistency of:
  - account codes used
  - JE types
  - description formatting
  - VAT treatment

Account map mirrors the existing frontend LEDGER accounts
for backward compatibility with existing data.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from app.services.posting.engine import PostingLine, PostingRequest


# ══════════════════════════════════════════════════════════
# Account Code Constants (same as frontend ACC_MAP)
# ══════════════════════════════════════════════════════════
class ACC:
    CASH         = "1001"
    BANK         = "1100"
    AR           = "1200"   # ذمم مدينة
    INVENTORY    = "1300"   # بضاعة
    VAT_REC      = "1400"   # VAT مدخلات
    PREPAID      = "1450"   # مدفوعات مقدمة
    FIXED_ASSETS = "1500"   # أصول ثابتة
    ACCUM_DEPR   = "1501"   # مجمع إهلاك

    AP           = "2100"   # ذمم دائنة — موردون
    VAT_PAY      = "2200"   # VAT مخرجات
    GOSI_LIA     = "2300"   # التزامات GOSI
    PAY_LIA      = "2301"   # التزامات رواتب
    LT_DEBT      = "2500"   # ديون طويلة الأجل

    CAPITAL      = "3001"
    RETAINED     = "3100"

    SALES_REV    = "4001"   # إيرادات مبيعات
    SVC_REV      = "4002"   # إيرادات خدمات
    OTHER_REV    = "4003"   # إيرادات أخرى

    COGS         = "5001"   # تكلفة البضاعة

    SALARIES     = "6001"
    GOSI_EXP     = "6002"
    RENT         = "6003"
    DEPRECIATION = "6004"
    MISC_EXP     = "6005"
    FINANCE_COST = "6006"


# ══════════════════════════════════════════════════════════
# Template builders — one per transaction type
# ══════════════════════════════════════════════════════════

def sales_invoice_posting(
    *,
    tenant_id: uuid.UUID,
    invoice_number: str,
    customer_name: str,
    entry_date: date,
    subtotal: Decimal,
    vat_amount: Decimal,
    total: Decimal,
    cogs_amount: Decimal,
    inventory_amount: Decimal,
    created_by_id: Optional[uuid.UUID] = None,
    created_by_email: Optional[str] = None,
    source_doc_id: Optional[uuid.UUID] = None,
    idempotency_key: Optional[str] = None,
    user_role: str = "viewer",
) -> PostingRequest:
    """
    Sales Invoice:
      DR: الذمم المدينة (AR)          = total (subtotal + VAT)
      CR: إيرادات المبيعات            = subtotal
      CR: VAT مخرجات                 = vat_amount
      DR: تكلفة البضاعة المباعة (COGS) = cogs_amount
      CR: بضاعة (Inventory)           = inventory_amount
    """
    lines = [
        PostingLine(
            account_code=ACC.AR,
            description=f"مبيعات — {customer_name} — {invoice_number}",
            debit=total,
        ),
        PostingLine(
            account_code=ACC.SALES_REV,
            description=f"إيراد مبيعات — {invoice_number}",
            credit=subtotal,
        ),
        PostingLine(
            account_code=ACC.VAT_PAY,
            description=f"VAT مخرجات 15% — {invoice_number}",
            credit=vat_amount,
        ),
        PostingLine(
            account_code=ACC.COGS,
            description=f"تكلفة مبيعات — {invoice_number}",
            debit=cogs_amount,
        ),
        PostingLine(
            account_code=ACC.INVENTORY,
            description=f"إخراج بضاعة — {invoice_number}",
            credit=inventory_amount,
        ),
    ]

    return PostingRequest(
        tenant_id=tenant_id,
        je_type="SJE",
        description=f"فاتورة مبيعات — {invoice_number} — {customer_name}",
        entry_date=entry_date,
        lines=lines,
        created_by_id=created_by_id,
        created_by_email=created_by_email,
        source_module="sales",
        source_doc_type="sales_invoice",
        source_doc_id=source_doc_id,
        source_doc_number=invoice_number,
        idempotency_key=idempotency_key or f"SJE:{invoice_number}:{tenant_id}",
        user_role=user_role,
    )


def grn_posting(
    *,
    tenant_id: uuid.UUID,
    grn_number: str,
    supplier_name: str,
    entry_date: date,
    inventory_cost: Decimal,
    vat_amount: Decimal,
    total_ap: Decimal,
    created_by_id: Optional[uuid.UUID] = None,
    created_by_email: Optional[str] = None,
    source_doc_id: Optional[uuid.UUID] = None,
    idempotency_key: Optional[str] = None,
    user_role: str = "viewer",
) -> PostingRequest:
    """
    Goods Receipt Note (GRN):
      DR: بضاعة (Inventory)    = inventory_cost
      DR: VAT مدخلات           = vat_amount
      CR: ذمم دائنة (AP)       = total_ap (inventory_cost + vat)
    """
    lines = [
        PostingLine(
            account_code=ACC.INVENTORY,
            description=f"بضاعة واردة — {grn_number} — {supplier_name}",
            debit=inventory_cost,
        ),
        PostingLine(
            account_code=ACC.VAT_REC,
            description=f"VAT مدخلات 15% — {grn_number}",
            debit=vat_amount,
        ),
        PostingLine(
            account_code=ACC.AP,
            description=f"ذمة مورد — {supplier_name} — {grn_number}",
            credit=total_ap,
        ),
    ]

    return PostingRequest(
        tenant_id=tenant_id,
        je_type="PJE",
        description=f"استلام بضاعة — {grn_number} — {supplier_name}",
        entry_date=entry_date,
        lines=lines,
        created_by_id=created_by_id,
        created_by_email=created_by_email,
        source_module="purchases",
        source_doc_type="grn",
        source_doc_id=source_doc_id,
        source_doc_number=grn_number,
        idempotency_key=idempotency_key or f"PJE:{grn_number}:{tenant_id}",
        user_role=user_role,
    )


def vendor_invoice_posting(
    *,
    tenant_id: uuid.UUID,
    invoice_number: str,
    supplier_name: str,
    entry_date: date,
    subtotal: Decimal,
    vat_amount: Decimal,
    total: Decimal,
    created_by_id: Optional[uuid.UUID] = None,
    created_by_email: Optional[str] = None,
    source_doc_id: Optional[uuid.UUID] = None,
    idempotency_key: Optional[str] = None,
    user_role: str = "viewer",
) -> PostingRequest:
    """
    Vendor Invoice (after 3-way match):
      DR: بضاعة / مصروف       = subtotal
      DR: VAT مدخلات          = vat_amount
      CR: ذمم دائنة (AP)      = total
    """
    lines = [
        PostingLine(
            account_code=ACC.INVENTORY,
            description=f"مشتريات — {invoice_number}",
            debit=subtotal,
        ),
        PostingLine(
            account_code=ACC.VAT_REC,
            description=f"VAT مدخلات — {invoice_number}",
            debit=vat_amount,
        ),
        PostingLine(
            account_code=ACC.AP,
            description=f"فاتورة مورد — {supplier_name} — {invoice_number}",
            credit=total,
        ),
    ]

    return PostingRequest(
        tenant_id=tenant_id,
        je_type="PIE",
        description=f"فاتورة مورد — {invoice_number} — {supplier_name}",
        entry_date=entry_date,
        lines=lines,
        created_by_id=created_by_id,
        created_by_email=created_by_email,
        source_module="purchases",
        source_doc_type="vendor_invoice",
        source_doc_id=source_doc_id,
        source_doc_number=invoice_number,
        idempotency_key=idempotency_key or f"PIE:{invoice_number}:{tenant_id}",
        user_role=user_role,
    )


def vendor_payment_posting(
    *,
    tenant_id: uuid.UUID,
    payment_ref: str,
    supplier_name: str,
    entry_date: date,
    amount: Decimal,
    bank_account: str = ACC.BANK,
    created_by_id: Optional[uuid.UUID] = None,
    created_by_email: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    user_role: str = "viewer",
) -> PostingRequest:
    """
    Vendor Payment:
      DR: ذمم دائنة (AP)  = amount
      CR: البنك           = amount
    """
    lines = [
        PostingLine(
            account_code=ACC.AP,
            description=f"تسوية ذمة مورد — {supplier_name} — {payment_ref}",
            debit=amount,
        ),
        PostingLine(
            account_code=bank_account,
            description=f"دفعة للمورد — {payment_ref}",
            credit=amount,
        ),
    ]

    return PostingRequest(
        tenant_id=tenant_id,
        je_type="PAY",
        description=f"دفعة مورد — {supplier_name} — {payment_ref}",
        entry_date=entry_date,
        lines=lines,
        created_by_id=created_by_id,
        created_by_email=created_by_email,
        source_module="treasury",
        source_doc_type="payment",
        source_doc_number=payment_ref,
        idempotency_key=idempotency_key or f"PAY:{payment_ref}:{tenant_id}",
        user_role=user_role,
    )


def payroll_posting(
    *,
    tenant_id: uuid.UUID,
    period_label: str,
    entry_date: date,
    gross_salaries: Decimal,
    gosi_employee: Decimal,
    gosi_employer: Decimal,
    net_payable: Decimal,
    created_by_id: Optional[uuid.UUID] = None,
    created_by_email: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    user_role: str = "viewer",
) -> PostingRequest:
    """
    Monthly Payroll Run:
      DR: مصروف رواتب        = gross_salaries
      DR: مصروف GOSI         = gosi_employer
      CR: التزامات GOSI      = gosi_employee + gosi_employer
      CR: التزامات رواتب     = net_payable
    """
    total_gosi = gosi_employee + gosi_employer

    lines = [
        PostingLine(
            account_code=ACC.SALARIES,
            description=f"رواتب — {period_label}",
            debit=gross_salaries,
        ),
        PostingLine(
            account_code=ACC.GOSI_EXP,
            description=f"GOSI صاحب عمل — {period_label}",
            debit=gosi_employer,
        ),
        PostingLine(
            account_code=ACC.GOSI_LIA,
            description=f"التزام GOSI — {period_label}",
            credit=total_gosi,
        ),
        PostingLine(
            account_code=ACC.PAY_LIA,
            description=f"رواتب مستحقة — {period_label}",
            credit=net_payable,
        ),
    ]

    return PostingRequest(
        tenant_id=tenant_id,
        je_type="PRV",
        description=f"قيد رواتب — {period_label}",
        entry_date=entry_date,
        lines=lines,
        created_by_id=created_by_id,
        created_by_email=created_by_email,
        source_module="hr",
        source_doc_type="payroll_run",
        source_doc_number=period_label,
        idempotency_key=idempotency_key or f"PRV:{period_label}:{tenant_id}",
        user_role=user_role,
    )


def depreciation_posting(
    *,
    tenant_id: uuid.UUID,
    period_label: str,
    entry_date: date,
    depreciation_amount: Decimal,
    asset_name: str,
    created_by_id: Optional[uuid.UUID] = None,
    created_by_email: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    user_role: str = "viewer",
) -> PostingRequest:
    """
    Monthly Depreciation:
      DR: مصروف إهلاك        = depreciation_amount
      CR: مجمع إهلاك         = depreciation_amount
    """
    lines = [
        PostingLine(
            account_code=ACC.DEPRECIATION,
            description=f"إهلاك — {asset_name} — {period_label}",
            debit=depreciation_amount,
        ),
        PostingLine(
            account_code=ACC.ACCUM_DEPR,
            description=f"مجمع إهلاك — {asset_name} — {period_label}",
            credit=depreciation_amount,
        ),
    ]

    return PostingRequest(
        tenant_id=tenant_id,
        je_type="DEP",
        description=f"إهلاك أصول — {asset_name} — {period_label}",
        entry_date=entry_date,
        lines=lines,
        created_by_id=created_by_id,
        created_by_email=created_by_email,
        source_module="assets",
        source_doc_type="depreciation",
        source_doc_number=f"DEP-{period_label}-{asset_name}",
        idempotency_key=idempotency_key or f"DEP:{period_label}:{asset_name}:{tenant_id}",
        user_role=user_role,
    )
