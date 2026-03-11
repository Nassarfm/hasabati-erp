"""
tests/test_accounting/test_posting_engine.py
Unit tests for PostingEngine — the most critical service.
These tests run WITHOUT a real DB (pure logic validation).
"""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal

import pytest

from app.core.exceptions import (
    DoubleEntryImbalanceError, ValidationError,
)
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import (
    ACC,
    grn_posting,
    payroll_posting,
    sales_invoice_posting,
    vendor_payment_posting,
)

TENANT = uuid.UUID("00000000-0000-0000-0000-000000000001")
TODAY = date(2024, 6, 15)


# ══════════════════════════════════════════════════════════
# Balance Validation Tests
# ══════════════════════════════════════════════════════════
def test_balanced_je_passes():
    engine = PostingEngine.__new__(PostingEngine)
    engine._validate_balance([
        PostingLine(ACC.AR, "test", debit=Decimal("1000")),
        PostingLine(ACC.SALES_REV, "test", credit=Decimal("1000")),
    ])  # should not raise


def test_imbalanced_je_raises():
    engine = PostingEngine.__new__(PostingEngine)
    with pytest.raises(DoubleEntryImbalanceError) as exc_info:
        engine._validate_balance([
            PostingLine(ACC.AR, "test", debit=Decimal("1000")),
            PostingLine(ACC.SALES_REV, "test", credit=Decimal("900")),
        ])
    assert "100" in str(exc_info.value.message)  # shows the difference


def test_single_line_je_raises():
    engine = PostingEngine.__new__(PostingEngine)
    with pytest.raises(ValidationError):
        engine._validate_lines_not_empty([
            PostingLine(ACC.AR, "test", debit=Decimal("1000")),
        ])


def test_rounding_tolerance_passes():
    """Decimal rounding within 0.005 should pass."""
    engine = PostingEngine.__new__(PostingEngine)
    engine._validate_balance([
        PostingLine(ACC.AR, "test", debit=Decimal("1000.003")),
        PostingLine(ACC.SALES_REV, "test", credit=Decimal("1000.000")),
    ])  # diff = 0.003 < 0.005, should pass


# ══════════════════════════════════════════════════════════
# Template Tests — verify correct account mapping
# ══════════════════════════════════════════════════════════
def test_sales_invoice_template_is_balanced():
    req = sales_invoice_posting(
        tenant_id=TENANT,
        invoice_number="INV-001",
        customer_name="عميل اختبار",
        entry_date=TODAY,
        subtotal=Decimal("1000"),
        vat_amount=Decimal("150"),
        total=Decimal("1150"),
        cogs_amount=Decimal("700"),
        inventory_amount=Decimal("700"),
    )
    total_dr = sum(l.debit  for l in req.lines)
    total_cr = sum(l.credit for l in req.lines)
    assert abs(total_dr - total_cr) < Decimal("0.005"), \
        f"Sales template imbalanced: DR={total_dr} CR={total_cr}"
    assert req.je_type == "SJE"
    assert req.source_module == "sales"


def test_grn_template_is_balanced():
    req = grn_posting(
        tenant_id=TENANT,
        grn_number="GRN-001",
        supplier_name="مورد اختبار",
        entry_date=TODAY,
        inventory_cost=Decimal("5000"),
        vat_amount=Decimal("750"),
        total_ap=Decimal("5750"),
    )
    total_dr = sum(l.debit  for l in req.lines)
    total_cr = sum(l.credit for l in req.lines)
    assert abs(total_dr - total_cr) < Decimal("0.005")
    assert req.je_type == "PJE"

    # Verify DR side has Inventory + VAT
    dr_accounts = {l.account_code for l in req.lines if l.debit > 0}
    assert ACC.INVENTORY in dr_accounts
    assert ACC.VAT_REC in dr_accounts

    # Verify CR side is AP
    cr_accounts = {l.account_code for l in req.lines if l.credit > 0}
    assert ACC.AP in cr_accounts


def test_payroll_template_is_balanced():
    req = payroll_posting(
        tenant_id=TENANT,
        period_label="2024/06",
        entry_date=TODAY,
        gross_salaries=Decimal("50000"),
        gosi_employee=Decimal("2250"),   # 4.5% employee
        gosi_employer=Decimal("4750"),   # 9.5% employer
        net_payable=Decimal("47750"),    # gross - gosi_employee
    )
    total_dr = sum(l.debit  for l in req.lines)
    total_cr = sum(l.credit for l in req.lines)
    assert abs(total_dr - total_cr) < Decimal("0.005"), \
        f"Payroll DR={total_dr} CR={total_cr}"


def test_vendor_payment_template_is_balanced():
    req = vendor_payment_posting(
        tenant_id=TENANT,
        payment_ref="PAY-001",
        supplier_name="مورد",
        entry_date=TODAY,
        amount=Decimal("5750"),
    )
    total_dr = sum(l.debit  for l in req.lines)
    total_cr = sum(l.credit for l in req.lines)
    assert abs(total_dr - total_cr) < Decimal("0.005")
    # DR = AP cleared, CR = Bank
    dr_accounts = {l.account_code for l in req.lines if l.debit > 0}
    assert ACC.AP in dr_accounts


# ══════════════════════════════════════════════════════════
# Exception hierarchy
# ══════════════════════════════════════════════════════════
def test_double_entry_error_message():
    err = DoubleEntryImbalanceError(1000.0, 850.0)
    assert err.code == "DOUBLE_ENTRY_IMBALANCE"
    assert err.status_code == 422
    assert "150" in err.message or "1000" in err.message
    assert err.detail["diff"] == 150.0
