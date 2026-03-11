"""Initial schema — all modules

Revision ID: 0001
Revises:
Create Date: 2025-01-01 00:00:00.000000

Creates all tables for:
  M0  — core (erp_tenants, erp_audit_log)
  M1  — accounting (acc_chart_of_accounts, acc_journal_entries, acc_je_lines,
                    acc_account_balances, acc_fiscal_periods, acc_fiscal_locks)
  M2A — inventory (inv_products, inv_warehouses, inv_stock_balances,
                   inv_movements, inv_adjustments, inv_adjustment_lines)
  M2B — sales (sal_customers, sal_invoices, sal_invoice_lines,
               sal_returns, sal_return_lines)
  M3  — purchases (pur_suppliers, pur_purchase_orders, pur_po_lines,
                   pur_grn, pur_grn_lines, pur_vendor_invoices, pur_vendor_inv_lines)
  M4  — hr (hr_employees, hr_payroll_runs, hr_payroll_lines, hr_leave_requests)
  M5  — assets (ast_assets, ast_depreciation_schedules, ast_disposals)
  M6  — treasury (tr_bank_accounts, tr_cash_transactions)
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


# ══════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════
def _erp_cols() -> list:
    """Standard ERPModel columns shared by every table."""
    return [
        sa.Column("id",         postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id",  postgresql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=True),
        sa.Column("created_by", sa.String(255), nullable=True),
        sa.Column("updated_by", sa.String(255), nullable=True),
    ]


def _soft_delete_cols() -> list:
    return [
        sa.Column("is_deleted",  sa.Boolean(), server_default="false", nullable=False),
        sa.Column("deleted_at",  sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_by",  sa.String(255), nullable=True),
    ]


def upgrade() -> None:
    # ── Enable uuid-ossp / pgcrypto if needed ─────────────
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # ══════════════════════════════════════════════════════
    # M1 — ACCOUNTING
    # ══════════════════════════════════════════════════════

    # Chart of Accounts
    op.create_table(
        "coa_accounts",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("code",            sa.String(20),   nullable=False),
        sa.Column("name_ar",         sa.String(255),  nullable=False),
        sa.Column("name_en",         sa.String(255),  nullable=True),
        sa.Column("account_type",    sa.String(20),   nullable=False),
        sa.Column("account_nature",  sa.String(10),   nullable=False),
        sa.Column("level",           sa.Integer(),    nullable=False, server_default="1"),
        sa.Column("parent_id",       postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("is_postable",     sa.Boolean(),    server_default="true", nullable=False),
        sa.Column("is_active",       sa.Boolean(),    server_default="true", nullable=False),
        sa.Column("opening_balance", sa.Numeric(18,3), server_default="0"),
        sa.Column("current_balance", sa.Numeric(18,3), server_default="0"),
        sa.Column("notes",           sa.Text(),       nullable=True),
        sa.Column("extra_data",      sa.JSON(),       nullable=True),
        sa.UniqueConstraint("tenant_id", "code", name="uq_coa_tenant_code"),
    )
    op.create_index("ix_coa_tenant_type", "coa_accounts", ["tenant_id", "account_type"])

    # Fiscal Periods
    op.create_table(
        "fiscal_periods",
        *_erp_cols(),
        sa.Column("name",       sa.String(50),  nullable=False),
        sa.Column("start_date", sa.Date(),      nullable=False),
        sa.Column("end_date",   sa.Date(),      nullable=False),
        sa.Column("status",     sa.String(20),  server_default="open"),
        sa.Column("period_type",sa.String(20),  server_default="month"),
        sa.UniqueConstraint("tenant_id", "start_date", "end_date", name="uq_fiscal_period"),
    )

    # Fiscal Locks
    op.create_table(
        "fiscal_locks",
        *_erp_cols(),
        sa.Column("lock_date",  sa.Date(),     nullable=False),
        sa.Column("locked_by",  sa.String(255), nullable=False),
        sa.Column("notes",      sa.Text(),     nullable=True),
        sa.UniqueConstraint("tenant_id", "lock_date", name="uq_fiscal_lock"),
    )

    # Journal Entries
    op.create_table(
        "journal_entries",
        *_erp_cols(),
        sa.Column("serial_number",   sa.String(50),  nullable=False),
        sa.Column("je_type",         sa.String(10),  server_default="JE"),
        sa.Column("description",     sa.String(500), nullable=False),
        sa.Column("entry_date",      sa.Date(),      nullable=False),
        sa.Column("status",          sa.String(20),  server_default="posted"),
        sa.Column("total_debit",     sa.Numeric(18,3), server_default="0"),
        sa.Column("total_credit",    sa.Numeric(18,3), server_default="0"),
        sa.Column("fiscal_period_id",postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_module",   sa.String(50),  nullable=True),
        sa.Column("source_doc_type", sa.String(50),  nullable=True),
        sa.Column("source_doc_number",sa.String(100),nullable=True),
        sa.Column("created_by_id",   postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_by_email",sa.String(255), nullable=True),
        sa.Column("idempotency_key", sa.String(255), nullable=True),
        sa.Column("reversal_of_id",  postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("reversed_by_id",  postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id", "serial_number", name="uq_je_tenant_serial"),
        sa.UniqueConstraint("idempotency_key", name="uq_je_idempotency"),
    )
    op.create_index("ix_je_tenant_date",   "journal_entries", ["tenant_id","entry_date"])
    op.create_index("ix_je_tenant_source", "journal_entries", ["tenant_id","source_module","source_doc_type"])

    # Journal Entry Lines
    op.create_table(
        "je_lines",
        *_erp_cols(),
        sa.Column("journal_entry_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("journal_entries.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("line_number",   sa.Integer(),   nullable=False),
        sa.Column("account_code",  sa.String(20),  nullable=False),
        sa.Column("account_name",  sa.String(255), nullable=True),
        sa.Column("description",   sa.String(500), nullable=True),
        sa.Column("debit",         sa.Numeric(18,3), server_default="0"),
        sa.Column("credit",        sa.Numeric(18,3), server_default="0"),
        sa.Column("cost_center",   sa.String(50),  nullable=True),
        sa.Column("reference",     sa.String(100), nullable=True),
        sa.CheckConstraint("debit >= 0",  name="ck_jeline_debit_positive"),
        sa.CheckConstraint("credit >= 0", name="ck_jeline_credit_positive"),
    )
    op.create_index("ix_jeline_je",      "je_lines", ["journal_entry_id"])
    op.create_index("ix_jeline_account", "je_lines", ["tenant_id","account_code"])

    # Account Balances
    op.create_table(
        "account_balances",
        *_erp_cols(),
        sa.Column("account_code",   sa.String(20),  nullable=False),
        sa.Column("fiscal_year",    sa.Integer(),   nullable=False),
        sa.Column("period_month",   sa.Integer(),   nullable=True),
        sa.Column("opening_dr",     sa.Numeric(18,3), server_default="0"),
        sa.Column("opening_cr",     sa.Numeric(18,3), server_default="0"),
        sa.Column("period_dr",      sa.Numeric(18,3), server_default="0"),
        sa.Column("period_cr",      sa.Numeric(18,3), server_default="0"),
        sa.Column("closing_dr",     sa.Numeric(18,3), server_default="0"),
        sa.Column("closing_cr",     sa.Numeric(18,3), server_default="0"),
        sa.UniqueConstraint("tenant_id","account_code","fiscal_year","period_month",
                            name="uq_bal_tenant_acc_period"),
    )

    # ══════════════════════════════════════════════════════
    # M2A — INVENTORY
    # ══════════════════════════════════════════════════════

    # Warehouses
    op.create_table(
        "inv_warehouses",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("code",        sa.String(20),  nullable=False),
        sa.Column("name_ar",     sa.String(255), nullable=False),
        sa.Column("name_en",     sa.String(255), nullable=True),
        sa.Column("is_default",  sa.Boolean(),   server_default="false"),
        sa.Column("is_active",   sa.Boolean(),   server_default="true"),
        sa.Column("location",    sa.String(255), nullable=True),
        sa.Column("notes",       sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","code", name="uq_warehouse_tenant_code"),
    )

    # Products
    op.create_table(
        "inv_products",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("code",             sa.String(50),  nullable=False),
        sa.Column("name_ar",          sa.String(255), nullable=False),
        sa.Column("name_en",          sa.String(255), nullable=True),
        sa.Column("product_type",     sa.String(20),  server_default="stockable"),
        sa.Column("unit_of_measure",  sa.String(20),  server_default="قطعة"),
        sa.Column("category",         sa.String(100), nullable=True),
        sa.Column("barcode",          sa.String(100), nullable=True),
        sa.Column("is_active",        sa.Boolean(),   server_default="true"),
        sa.Column("track_stock",      sa.Boolean(),   server_default="true"),
        sa.Column("reorder_point",    sa.Numeric(18,3), server_default="0"),
        sa.Column("standard_cost",    sa.Numeric(18,4), server_default="0"),
        sa.Column("sale_price",       sa.Numeric(18,4), server_default="0"),
        sa.Column("inventory_account",sa.String(20),  server_default="1301"),
        sa.Column("cogs_account",     sa.String(20),  server_default="5001"),
        sa.Column("revenue_account",  sa.String(20),  server_default="4001"),
        sa.Column("notes",            sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","code", name="uq_product_tenant_code"),
    )

    # Stock Balances
    op.create_table(
        "inv_stock_balances",
        *_erp_cols(),
        sa.Column("product_id",   postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("inv_products.id"),  nullable=False),
        sa.Column("warehouse_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("inv_warehouses.id"), nullable=False),
        sa.Column("qty_on_hand",  sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_reserved", sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_available",sa.Numeric(18,3), server_default="0"),
        sa.Column("wac",          sa.Numeric(18,4), server_default="0"),
        sa.Column("total_cost",   sa.Numeric(18,3), server_default="0"),
        sa.Column("last_movement_date", sa.Date(), nullable=True),
        sa.UniqueConstraint("product_id","warehouse_id", name="uq_stock_product_warehouse"),
    )

    # Inventory Movements
    op.create_table(
        "inv_movements",
        *_erp_cols(),
        sa.Column("product_id",    postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("warehouse_id",  postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("movement_type", sa.String(30),   nullable=False),
        sa.Column("movement_date", sa.Date(),       nullable=False),
        sa.Column("qty",           sa.Numeric(18,3), nullable=False),
        sa.Column("unit_cost",     sa.Numeric(18,4), server_default="0"),
        sa.Column("total_cost",    sa.Numeric(18,3), server_default="0"),
        sa.Column("wac_before",    sa.Numeric(18,4), server_default="0"),
        sa.Column("wac_after",     sa.Numeric(18,4), server_default="0"),
        sa.Column("qty_before",    sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_after",     sa.Numeric(18,3), server_default="0"),
        sa.Column("source_module", sa.String(50),   nullable=True),
        sa.Column("source_doc_type",sa.String(50),  nullable=True),
        sa.Column("source_doc_number",sa.String(100),nullable=True),
        sa.Column("je_id",         postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",     sa.String(50),   nullable=True),
        sa.Column("notes",         sa.Text(),       nullable=True),
        sa.CheckConstraint("qty > 0", name="ck_movement_qty_positive"),
    )
    op.create_index("ix_movement_product_date","inv_movements",["tenant_id","product_id","movement_date"])

    # Inventory Adjustments
    op.create_table(
        "inv_adjustments",
        *_erp_cols(),
        sa.Column("adjustment_number",sa.String(50), nullable=False),
        sa.Column("adjustment_date", sa.Date(),      nullable=False),
        sa.Column("adjustment_type", sa.String(20),  server_default="count"),
        sa.Column("status",          sa.String(20),  server_default="draft"),
        sa.Column("warehouse_id",    postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("warehouse_code",  sa.String(20),  nullable=False),
        sa.Column("total_variance_cost",sa.Numeric(18,3), server_default="0"),
        sa.Column("je_id",           postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",       sa.String(50),  nullable=True),
        sa.Column("posted_at",       sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","adjustment_number", name="uq_adj_tenant_number"),
    )

    # Adjustment Lines
    op.create_table(
        "inv_adjustment_lines",
        *_erp_cols(),
        sa.Column("adjustment_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("inv_adjustments.id", ondelete="CASCADE"), nullable=False),
        sa.Column("line_number",   sa.Integer(),    nullable=False),
        sa.Column("product_id",    postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("product_code",  sa.String(50),   nullable=False),
        sa.Column("product_name",  sa.String(255),  nullable=False),
        sa.Column("qty_system",    sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_actual",    sa.Numeric(18,3), nullable=False),
        sa.Column("qty_variance",  sa.Numeric(18,3), server_default="0"),
        sa.Column("unit_cost",     sa.Numeric(18,4), server_default="0"),
        sa.Column("variance_cost", sa.Numeric(18,3), server_default="0"),
        sa.Column("notes",         sa.String(500),  nullable=True),
    )

    # ══════════════════════════════════════════════════════
    # M2B — SALES
    # ══════════════════════════════════════════════════════

    # Customers
    op.create_table(
        "sal_customers",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("code",           sa.String(30),  nullable=False),
        sa.Column("name_ar",        sa.String(255), nullable=False),
        sa.Column("name_en",        sa.String(255), nullable=True),
        sa.Column("customer_type",  sa.String(20),  server_default="company"),
        sa.Column("phone",          sa.String(30),  nullable=True),
        sa.Column("email",          sa.String(255), nullable=True),
        sa.Column("address",        sa.String(500), nullable=True),
        sa.Column("city",           sa.String(100), nullable=True),
        sa.Column("country",        sa.String(10),  server_default="SA"),
        sa.Column("vat_number",     sa.String(20),  nullable=True),
        sa.Column("cr_number",      sa.String(20),  nullable=True),
        sa.Column("payment_term",   sa.String(20),  server_default="net_30"),
        sa.Column("credit_limit",   sa.Numeric(18,3), server_default="0"),
        sa.Column("discount_pct",   sa.Numeric(5,2),  server_default="0"),
        sa.Column("ar_account",     sa.String(20),  server_default="1201"),
        sa.Column("is_active",      sa.Boolean(),   server_default="true"),
        sa.Column("notes",          sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","code", name="uq_customer_tenant_code"),
        sa.CheckConstraint("credit_limit >= 0", name="ck_customer_credit_positive"),
    )

    # Sales Invoices
    op.create_table(
        "sal_invoices",
        *_erp_cols(),
        sa.Column("invoice_number",  sa.String(50),  nullable=False),
        sa.Column("invoice_date",    sa.Date(),      nullable=False),
        sa.Column("due_date",        sa.Date(),      nullable=True),
        sa.Column("status",          sa.String(30),  server_default="draft"),
        sa.Column("customer_id",     postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("sal_customers.id"), nullable=False),
        sa.Column("customer_code",   sa.String(30),  nullable=False),
        sa.Column("customer_name",   sa.String(255), nullable=False),
        sa.Column("customer_vat",    sa.String(20),  nullable=True),
        sa.Column("warehouse_id",    postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("warehouse_code",  sa.String(20),  server_default="MAIN"),
        sa.Column("subtotal",        sa.Numeric(18,3), server_default="0"),
        sa.Column("discount_amount", sa.Numeric(18,3), server_default="0"),
        sa.Column("taxable_amount",  sa.Numeric(18,3), server_default="0"),
        sa.Column("vat_amount",      sa.Numeric(18,3), server_default="0"),
        sa.Column("total_amount",    sa.Numeric(18,3), server_default="0"),
        sa.Column("paid_amount",     sa.Numeric(18,3), server_default="0"),
        sa.Column("balance_due",     sa.Numeric(18,3), server_default="0"),
        sa.Column("total_cost",      sa.Numeric(18,3), server_default="0"),
        sa.Column("gross_profit",    sa.Numeric(18,3), server_default="0"),
        sa.Column("payment_term",    sa.String(20),  server_default="net_30"),
        sa.Column("discount_pct",    sa.Numeric(5,2),  server_default="0"),
        sa.Column("je_id",           postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",       sa.String(50),  nullable=True),
        sa.Column("ar_account",      sa.String(20),  server_default="1201"),
        sa.Column("posted_at",       sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_by",       sa.String(255), nullable=True),
        sa.Column("returned_amount", sa.Numeric(18,3), server_default="0"),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.Column("reference",       sa.String(100), nullable=True),
        sa.UniqueConstraint("tenant_id","invoice_number", name="uq_invoice_tenant_number"),
        sa.CheckConstraint("total_amount >= 0", name="ck_invoice_total_positive"),
    )
    op.create_index("ix_invoice_tenant_date",     "sal_invoices", ["tenant_id","invoice_date"])
    op.create_index("ix_invoice_tenant_customer", "sal_invoices", ["tenant_id","customer_id"])
    op.create_index("ix_invoice_tenant_status",   "sal_invoices", ["tenant_id","status"])

    # Sales Invoice Lines
    op.create_table(
        "sal_invoice_lines",
        *_erp_cols(),
        sa.Column("invoice_id",       postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("sal_invoices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("line_number",      sa.Integer(),   nullable=False),
        sa.Column("product_id",       postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("product_code",     sa.String(50),  nullable=False),
        sa.Column("product_name",     sa.String(255), nullable=False),
        sa.Column("unit_of_measure",  sa.String(20),  server_default="قطعة"),
        sa.Column("qty",              sa.Numeric(18,3), nullable=False),
        sa.Column("unit_price",       sa.Numeric(18,4), nullable=False),
        sa.Column("discount_pct",     sa.Numeric(5,2),  server_default="0"),
        sa.Column("discount_amount",  sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total",       sa.Numeric(18,3), nullable=False),
        sa.Column("vat_rate",         sa.Numeric(5,2),  server_default="15"),
        sa.Column("vat_amount",       sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total_with_vat",sa.Numeric(18,3), nullable=False),
        sa.Column("unit_cost",        sa.Numeric(18,4), server_default="0"),
        sa.Column("total_cost",       sa.Numeric(18,3), server_default="0"),
        sa.Column("revenue_account",  sa.String(20),  server_default="4001"),
        sa.Column("cogs_account",     sa.String(20),  server_default="5001"),
        sa.Column("inventory_account",sa.String(20),  server_default="1301"),
        sa.Column("qty_returned",     sa.Numeric(18,3), server_default="0"),
        sa.Column("notes",            sa.String(500), nullable=True),
        sa.CheckConstraint("qty > 0",           name="ck_inv_line_qty_positive"),
        sa.CheckConstraint("unit_price >= 0",   name="ck_inv_line_price_positive"),
    )

    # Sales Returns
    op.create_table(
        "sal_returns",
        *_erp_cols(),
        sa.Column("return_number",  sa.String(50),  nullable=False),
        sa.Column("return_date",    sa.Date(),      nullable=False),
        sa.Column("status",         sa.String(20),  server_default="draft"),
        sa.Column("invoice_id",     postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("sal_invoices.id"), nullable=False),
        sa.Column("invoice_number", sa.String(50),  nullable=False),
        sa.Column("customer_id",    postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("customer_code",  sa.String(30),  nullable=False),
        sa.Column("customer_name",  sa.String(255), nullable=False),
        sa.Column("warehouse_code", sa.String(20),  server_default="MAIN"),
        sa.Column("subtotal",       sa.Numeric(18,3), server_default="0"),
        sa.Column("vat_amount",     sa.Numeric(18,3), server_default="0"),
        sa.Column("total_amount",   sa.Numeric(18,3), server_default="0"),
        sa.Column("total_cost",     sa.Numeric(18,3), server_default="0"),
        sa.Column("je_id",          postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",      sa.String(50),  nullable=True),
        sa.Column("posted_at",      sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_by",      sa.String(255), nullable=True),
        sa.Column("reason",         sa.String(500), nullable=True),
        sa.Column("notes",          sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","return_number", name="uq_return_tenant_number"),
    )

    # Return Lines
    op.create_table(
        "sal_return_lines",
        *_erp_cols(),
        sa.Column("return_id",      postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("sal_returns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("invoice_line_id",postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("line_number",    sa.Integer(),   nullable=False),
        sa.Column("product_id",     postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("product_code",   sa.String(50),  nullable=False),
        sa.Column("product_name",   sa.String(255), nullable=False),
        sa.Column("qty",            sa.Numeric(18,3), nullable=False),
        sa.Column("unit_price",     sa.Numeric(18,4), nullable=False),
        sa.Column("vat_rate",       sa.Numeric(5,2),  server_default="15"),
        sa.Column("line_total",     sa.Numeric(18,3), nullable=False),
        sa.Column("vat_amount",     sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total_with_vat",sa.Numeric(18,3), nullable=False),
        sa.Column("unit_cost",      sa.Numeric(18,4), server_default="0"),
        sa.Column("total_cost",     sa.Numeric(18,3), server_default="0"),
        sa.CheckConstraint("qty > 0", name="ck_ret_line_qty_positive"),
    )

    # ══════════════════════════════════════════════════════
    # M3 — PURCHASES
    # ══════════════════════════════════════════════════════

    # Suppliers
    op.create_table(
        "pur_suppliers",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("code",          sa.String(30),  nullable=False),
        sa.Column("name_ar",       sa.String(255), nullable=False),
        sa.Column("name_en",       sa.String(255), nullable=True),
        sa.Column("phone",         sa.String(30),  nullable=True),
        sa.Column("email",         sa.String(255), nullable=True),
        sa.Column("address",       sa.String(500), nullable=True),
        sa.Column("city",          sa.String(100), nullable=True),
        sa.Column("country",       sa.String(10),  server_default="SA"),
        sa.Column("vat_number",    sa.String(20),  nullable=True),
        sa.Column("cr_number",     sa.String(20),  nullable=True),
        sa.Column("payment_term",  sa.String(20),  server_default="net_30"),
        sa.Column("credit_limit",  sa.Numeric(18,3), server_default="0"),
        sa.Column("discount_pct",  sa.Numeric(5,2),  server_default="0"),
        sa.Column("ap_account",    sa.String(20),  server_default="2101"),
        sa.Column("is_active",     sa.Boolean(),   server_default="true"),
        sa.Column("notes",         sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","code", name="uq_supplier_tenant_code"),
    )

    # Purchase Orders
    op.create_table(
        "pur_purchase_orders",
        *_erp_cols(),
        sa.Column("po_number",         sa.String(50),  nullable=False),
        sa.Column("po_date",           sa.Date(),      nullable=False),
        sa.Column("required_date",     sa.Date(),      nullable=True),
        sa.Column("status",            sa.String(30),  server_default="draft"),
        sa.Column("supplier_id",       postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_suppliers.id"), nullable=False),
        sa.Column("supplier_code",     sa.String(30),  nullable=False),
        sa.Column("supplier_name",     sa.String(255), nullable=False),
        sa.Column("warehouse_code",    sa.String(20),  server_default="MAIN"),
        sa.Column("subtotal",          sa.Numeric(18,3), server_default="0"),
        sa.Column("discount_amount",   sa.Numeric(18,3), server_default="0"),
        sa.Column("taxable_amount",    sa.Numeric(18,3), server_default="0"),
        sa.Column("vat_amount",        sa.Numeric(18,3), server_default="0"),
        sa.Column("total_amount",      sa.Numeric(18,3), server_default="0"),
        sa.Column("payment_term",      sa.String(20),  server_default="net_30"),
        sa.Column("discount_pct",      sa.Numeric(5,2),  server_default="0"),
        sa.Column("qty_received_pct",  sa.Numeric(5,2),  server_default="0"),
        sa.Column("qty_invoiced_pct",  sa.Numeric(5,2),  server_default="0"),
        sa.Column("approved_by",       sa.String(255), nullable=True),
        sa.Column("approved_at",       sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes",             sa.Text(),      nullable=True),
        sa.Column("reference",         sa.String(100), nullable=True),
        sa.UniqueConstraint("tenant_id","po_number", name="uq_po_tenant_number"),
    )
    op.create_index("ix_po_tenant_date",     "pur_purchase_orders", ["tenant_id","po_date"])
    op.create_index("ix_po_tenant_supplier", "pur_purchase_orders", ["tenant_id","supplier_id"])

    # PO Lines
    op.create_table(
        "pur_po_lines",
        *_erp_cols(),
        sa.Column("po_id",           postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_purchase_orders.id", ondelete="CASCADE"), nullable=False),
        sa.Column("line_number",     sa.Integer(),   nullable=False),
        sa.Column("product_id",      postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("product_code",    sa.String(50),  nullable=False),
        sa.Column("product_name",    sa.String(255), nullable=False),
        sa.Column("unit_of_measure", sa.String(20),  server_default="قطعة"),
        sa.Column("qty_ordered",     sa.Numeric(18,3), nullable=False),
        sa.Column("qty_received",    sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_invoiced",    sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_pending",     sa.Numeric(18,3), server_default="0"),
        sa.Column("unit_price",      sa.Numeric(18,4), nullable=False),
        sa.Column("discount_pct",    sa.Numeric(5,2),  server_default="0"),
        sa.Column("discount_amount", sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total",      sa.Numeric(18,3), nullable=False),
        sa.Column("vat_rate",        sa.Numeric(5,2),  server_default="15"),
        sa.Column("vat_amount",      sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total_with_vat",sa.Numeric(18,3), nullable=False),
        sa.Column("inventory_account",sa.String(20),  server_default="1301"),
        sa.Column("notes",           sa.String(500), nullable=True),
        sa.CheckConstraint("qty_ordered > 0", name="ck_poline_qty_positive"),
        sa.CheckConstraint("unit_price >= 0", name="ck_poline_price_positive"),
    )

    # GRN
    op.create_table(
        "pur_grn",
        *_erp_cols(),
        sa.Column("grn_number",    sa.String(50),  nullable=False),
        sa.Column("grn_date",      sa.Date(),      nullable=False),
        sa.Column("status",        sa.String(20),  server_default="draft"),
        sa.Column("po_id",         postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_purchase_orders.id"), nullable=False),
        sa.Column("po_number",     sa.String(50),  nullable=False),
        sa.Column("supplier_id",   postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("supplier_code", sa.String(30),  nullable=False),
        sa.Column("supplier_name", sa.String(255), nullable=False),
        sa.Column("warehouse_code",sa.String(20),  server_default="MAIN"),
        sa.Column("total_cost",    sa.Numeric(18,3), server_default="0"),
        sa.Column("je_id",         postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",     sa.String(50),  nullable=True),
        sa.Column("posted_at",     sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_by",     sa.String(255), nullable=True),
        sa.Column("delivery_note", sa.String(100), nullable=True),
        sa.Column("notes",         sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","grn_number", name="uq_grn_tenant_number"),
    )

    # GRN Lines
    op.create_table(
        "pur_grn_lines",
        *_erp_cols(),
        sa.Column("grn_id",         postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_grn.id", ondelete="CASCADE"), nullable=False),
        sa.Column("po_line_id",     postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("line_number",    sa.Integer(),   nullable=False),
        sa.Column("product_id",     postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("product_code",   sa.String(50),  nullable=False),
        sa.Column("product_name",   sa.String(255), nullable=False),
        sa.Column("qty_received",   sa.Numeric(18,3), nullable=False),
        sa.Column("unit_cost",      sa.Numeric(18,4), nullable=False),
        sa.Column("total_cost",     sa.Numeric(18,3), nullable=False),
        sa.Column("wac_before",     sa.Numeric(18,4), server_default="0"),
        sa.Column("wac_after",      sa.Numeric(18,4), server_default="0"),
        sa.Column("inventory_account",sa.String(20), server_default="1301"),
        sa.Column("movement_id",    postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("notes",          sa.String(500), nullable=True),
        sa.CheckConstraint("qty_received > 0", name="ck_grnline_qty_positive"),
        sa.CheckConstraint("unit_cost >= 0",   name="ck_grnline_cost_positive"),
    )

    # Vendor Invoices
    op.create_table(
        "pur_vendor_invoices",
        *_erp_cols(),
        sa.Column("vi_number",      sa.String(50),  nullable=False),
        sa.Column("vendor_ref",     sa.String(100), nullable=True),
        sa.Column("invoice_date",   sa.Date(),      nullable=False),
        sa.Column("due_date",       sa.Date(),      nullable=True),
        sa.Column("status",         sa.String(20),  server_default="draft"),
        sa.Column("po_id",          postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_purchase_orders.id"), nullable=False),
        sa.Column("po_number",      sa.String(50),  nullable=False),
        sa.Column("grn_id",         postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_grn.id"), nullable=True),
        sa.Column("grn_number",     sa.String(50),  nullable=True),
        sa.Column("supplier_id",    postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("supplier_code",  sa.String(30),  nullable=False),
        sa.Column("supplier_name",  sa.String(255), nullable=False),
        sa.Column("subtotal",       sa.Numeric(18,3), server_default="0"),
        sa.Column("discount_amount",sa.Numeric(18,3), server_default="0"),
        sa.Column("taxable_amount", sa.Numeric(18,3), server_default="0"),
        sa.Column("vat_amount",     sa.Numeric(18,3), server_default="0"),
        sa.Column("total_amount",   sa.Numeric(18,3), server_default="0"),
        sa.Column("paid_amount",    sa.Numeric(18,3), server_default="0"),
        sa.Column("balance_due",    sa.Numeric(18,3), server_default="0"),
        sa.Column("payment_term",   sa.String(20),  server_default="net_30"),
        sa.Column("ap_account",     sa.String(20),  server_default="2101"),
        sa.Column("match_status",   sa.String(20),  nullable=True),
        sa.Column("match_notes",    sa.Text(),      nullable=True),
        sa.Column("je_id",          postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",      sa.String(50),  nullable=True),
        sa.Column("posted_at",      sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_by",      sa.String(255), nullable=True),
        sa.Column("notes",          sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","vi_number", name="uq_vi_tenant_number"),
    )

    # Vendor Invoice Lines
    op.create_table(
        "pur_vendor_inv_lines",
        *_erp_cols(),
        sa.Column("vi_id",         postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("pur_vendor_invoices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("po_line_id",    postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("grn_line_id",   postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("line_number",   sa.Integer(),   nullable=False),
        sa.Column("product_code",  sa.String(50),  nullable=False),
        sa.Column("product_name",  sa.String(255), nullable=False),
        sa.Column("qty_ordered",   sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_received",  sa.Numeric(18,3), server_default="0"),
        sa.Column("qty_invoiced",  sa.Numeric(18,3), nullable=False),
        sa.Column("unit_price",    sa.Numeric(18,4), nullable=False),
        sa.Column("discount_pct",  sa.Numeric(5,2),  server_default="0"),
        sa.Column("discount_amount",sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total",    sa.Numeric(18,3), nullable=False),
        sa.Column("vat_rate",      sa.Numeric(5,2),  server_default="15"),
        sa.Column("vat_amount",    sa.Numeric(18,3), server_default="0"),
        sa.Column("line_total_with_vat",sa.Numeric(18,3), nullable=False),
        sa.Column("match_ok",      sa.Boolean(),   server_default="true"),
        sa.Column("match_notes",   sa.String(500), nullable=True),
        sa.Column("inventory_account",sa.String(20), server_default="1301"),
        sa.Column("notes",         sa.String(500), nullable=True),
        sa.CheckConstraint("qty_invoiced > 0", name="ck_viline_qty_positive"),
    )

    # ══════════════════════════════════════════════════════
    # M4 — HR
    # ══════════════════════════════════════════════════════

    # Employees
    op.create_table(
        "hr_employees",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("employee_number",  sa.String(30),  nullable=False),
        sa.Column("first_name_ar",    sa.String(100), nullable=False),
        sa.Column("last_name_ar",     sa.String(100), nullable=False),
        sa.Column("first_name_en",    sa.String(100), nullable=True),
        sa.Column("last_name_en",     sa.String(100), nullable=True),
        sa.Column("gender",           sa.String(10),  server_default="male"),
        sa.Column("date_of_birth",    sa.Date(),      nullable=True),
        sa.Column("nationality",      sa.String(10),  server_default="saudi"),
        sa.Column("national_id",      sa.String(20),  nullable=True),
        sa.Column("iqama_number",     sa.String(20),  nullable=True),
        sa.Column("iqama_expiry",     sa.Date(),      nullable=True),
        sa.Column("passport_number",  sa.String(20),  nullable=True),
        sa.Column("status",           sa.String(20),  server_default="active"),
        sa.Column("hire_date",        sa.Date(),      nullable=False),
        sa.Column("termination_date", sa.Date(),      nullable=True),
        sa.Column("department",       sa.String(100), nullable=True),
        sa.Column("job_title",        sa.String(100), nullable=True),
        sa.Column("cost_center",      sa.String(50),  nullable=True),
        sa.Column("basic_salary",     sa.Numeric(18,3), nullable=False),
        sa.Column("housing_allow",    sa.Numeric(18,3), server_default="0"),
        sa.Column("transport_allow",  sa.Numeric(18,3), server_default="0"),
        sa.Column("other_allow",      sa.Numeric(18,3), server_default="0"),
        sa.Column("gosi_enrolled",    sa.Boolean(),   server_default="true"),
        sa.Column("gosi_number",      sa.String(30),  nullable=True),
        sa.Column("bank_name",        sa.String(100), nullable=True),
        sa.Column("bank_iban",        sa.String(34),  nullable=True),
        sa.Column("annual_leave_balance", sa.Numeric(8,2), server_default="0"),
        sa.Column("phone",            sa.String(30),  nullable=True),
        sa.Column("email",            sa.String(255), nullable=True),
        sa.Column("notes",            sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","employee_number", name="uq_emp_tenant_number"),
    )
    op.create_index("ix_emp_tenant_status", "hr_employees", ["tenant_id","status"])

    # Payroll Runs
    op.create_table(
        "hr_payroll_runs",
        *_erp_cols(),
        sa.Column("run_number",          sa.String(50),  nullable=False),
        sa.Column("period_year",         sa.Integer(),   nullable=False),
        sa.Column("period_month",        sa.Integer(),   nullable=False),
        sa.Column("pay_date",            sa.Date(),      nullable=False),
        sa.Column("status",              sa.String(20),  server_default="draft"),
        sa.Column("total_basic",         sa.Numeric(18,3), server_default="0"),
        sa.Column("total_allowances",    sa.Numeric(18,3), server_default="0"),
        sa.Column("total_gross",         sa.Numeric(18,3), server_default="0"),
        sa.Column("total_gosi_employee", sa.Numeric(18,3), server_default="0"),
        sa.Column("total_gosi_employer", sa.Numeric(18,3), server_default="0"),
        sa.Column("total_deductions",    sa.Numeric(18,3), server_default="0"),
        sa.Column("total_net",           sa.Numeric(18,3), server_default="0"),
        sa.Column("total_eosb_accrual",  sa.Numeric(18,3), server_default="0"),
        sa.Column("employee_count",      sa.Integer(),   server_default="0"),
        sa.Column("je_id",               postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",           sa.String(50),  nullable=True),
        sa.Column("posted_at",           sa.DateTime(timezone=True), nullable=True),
        sa.Column("posted_by",           sa.String(255), nullable=True),
        sa.Column("notes",               sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","run_number",  name="uq_payroll_run_number"),
        sa.UniqueConstraint("tenant_id","period_year","period_month", name="uq_payroll_period"),
        sa.CheckConstraint("period_month BETWEEN 1 AND 12", name="ck_payroll_month"),
    )

    # Payroll Lines
    op.create_table(
        "hr_payroll_lines",
        *_erp_cols(),
        sa.Column("run_id",            postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("hr_payroll_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("employee_id",       postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("hr_employees.id"), nullable=False),
        sa.Column("employee_number",   sa.String(30),  nullable=False),
        sa.Column("employee_name",     sa.String(255), nullable=False),
        sa.Column("department",        sa.String(100), nullable=True),
        sa.Column("nationality",       sa.String(10),  nullable=False),
        sa.Column("basic_salary",      sa.Numeric(18,3), nullable=False),
        sa.Column("housing_allow",     sa.Numeric(18,3), server_default="0"),
        sa.Column("transport_allow",   sa.Numeric(18,3), server_default="0"),
        sa.Column("other_allow",       sa.Numeric(18,3), server_default="0"),
        sa.Column("overtime_amount",   sa.Numeric(18,3), server_default="0"),
        sa.Column("bonus_amount",      sa.Numeric(18,3), server_default="0"),
        sa.Column("gross_salary",      sa.Numeric(18,3), nullable=False),
        sa.Column("gosi_base",         sa.Numeric(18,3), server_default="0"),
        sa.Column("gosi_employee",     sa.Numeric(18,3), server_default="0"),
        sa.Column("gosi_employer",     sa.Numeric(18,3), server_default="0"),
        sa.Column("deductions_total",  sa.Numeric(18,3), server_default="0"),
        sa.Column("absence_deduction", sa.Numeric(18,3), server_default="0"),
        sa.Column("advance_deduction", sa.Numeric(18,3), server_default="0"),
        sa.Column("other_deductions",  sa.Numeric(18,3), server_default="0"),
        sa.Column("net_salary",        sa.Numeric(18,3), nullable=False),
        sa.Column("eosb_accrual",      sa.Numeric(18,3), server_default="0"),
        sa.Column("working_days",      sa.Integer(),   server_default="30"),
        sa.Column("absent_days",       sa.Integer(),   server_default="0"),
        sa.UniqueConstraint("run_id","employee_id", name="uq_payroll_line_run_emp"),
        sa.CheckConstraint("gross_salary >= 0", name="ck_pl_gross_positive"),
        sa.CheckConstraint("net_salary >= 0",   name="ck_pl_net_positive"),
    )

    # Leave Requests
    op.create_table(
        "hr_leave_requests",
        *_erp_cols(),
        sa.Column("leave_number",    sa.String(50),  nullable=False),
        sa.Column("employee_id",     postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("hr_employees.id"), nullable=False),
        sa.Column("employee_number", sa.String(30),  nullable=False),
        sa.Column("employee_name",   sa.String(255), nullable=False),
        sa.Column("leave_type",      sa.String(20),  nullable=False),
        sa.Column("status",          sa.String(20),  server_default="pending"),
        sa.Column("start_date",      sa.Date(),      nullable=False),
        sa.Column("end_date",        sa.Date(),      nullable=False),
        sa.Column("days_requested",  sa.Integer(),   nullable=False),
        sa.Column("days_approved",   sa.Integer(),   server_default="0"),
        sa.Column("approved_by",     sa.String(255), nullable=True),
        sa.Column("approved_at",     sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason",          sa.String(500), nullable=True),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","leave_number", name="uq_leave_tenant_number"),
        sa.CheckConstraint("days_requested > 0",   name="ck_leave_days_positive"),
        sa.CheckConstraint("end_date >= start_date",name="ck_leave_dates"),
    )

    # ══════════════════════════════════════════════════════
    # M5 — FIXED ASSETS
    # ══════════════════════════════════════════════════════

    # Assets
    op.create_table(
        "ast_assets",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("asset_number",      sa.String(50),  nullable=False),
        sa.Column("name_ar",           sa.String(255), nullable=False),
        sa.Column("name_en",           sa.String(255), nullable=True),
        sa.Column("category",          sa.String(30),  nullable=False),
        sa.Column("status",            sa.String(30),  server_default="active"),
        sa.Column("depreciation_method",sa.String(20), server_default="straight_line"),
        sa.Column("purchase_date",     sa.Date(),      nullable=False),
        sa.Column("in_service_date",   sa.Date(),      nullable=False),
        sa.Column("purchase_cost",     sa.Numeric(18,3), nullable=False),
        sa.Column("salvage_value",     sa.Numeric(18,3), server_default="0"),
        sa.Column("useful_life_months",sa.Integer(),   nullable=False),
        sa.Column("accumulated_depreciation",sa.Numeric(18,3), server_default="0"),
        sa.Column("net_book_value",    sa.Numeric(18,3), nullable=False),
        sa.Column("last_dep_date",     sa.Date(),      nullable=True),
        sa.Column("location",          sa.String(100), nullable=True),
        sa.Column("serial_number",     sa.String(100), nullable=True),
        sa.Column("supplier_name",     sa.String(255), nullable=True),
        sa.Column("asset_account",     sa.String(20),  server_default="1502"),
        sa.Column("accum_dep_account", sa.String(20),  server_default="1552"),
        sa.Column("dep_expense_account",sa.String(20), server_default="6502"),
        sa.Column("disposal_account",  sa.String(20),  server_default="4202"),
        sa.Column("notes",             sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","asset_number", name="uq_asset_tenant_number"),
        sa.CheckConstraint("purchase_cost > 0",       name="ck_asset_cost_positive"),
        sa.CheckConstraint("useful_life_months > 0",  name="ck_asset_life_positive"),
        sa.CheckConstraint("salvage_value >= 0",      name="ck_asset_salvage_positive"),
    )
    op.create_index("ix_asset_tenant_category","ast_assets",["tenant_id","category"])

    # Depreciation Schedules
    op.create_table(
        "ast_depreciation_schedules",
        *_erp_cols(),
        sa.Column("asset_id",         postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("ast_assets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("asset_number",     sa.String(50),  nullable=False),
        sa.Column("period_year",      sa.Integer(),   nullable=False),
        sa.Column("period_month",     sa.Integer(),   nullable=False),
        sa.Column("dep_date",         sa.Date(),      nullable=False),
        sa.Column("dep_amount",       sa.Numeric(18,3), nullable=False),
        sa.Column("accum_dep_before", sa.Numeric(18,3), server_default="0"),
        sa.Column("accum_dep_after",  sa.Numeric(18,3), nullable=False),
        sa.Column("nbv_after",        sa.Numeric(18,3), nullable=False),
        sa.Column("je_id",            postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",        sa.String(50),  nullable=True),
        sa.UniqueConstraint("asset_id","period_year","period_month", name="uq_dep_asset_period"),
        sa.CheckConstraint("period_month BETWEEN 1 AND 12", name="ck_dep_month"),
        sa.CheckConstraint("dep_amount >= 0",               name="ck_dep_amount_positive"),
    )

    # Disposals
    op.create_table(
        "ast_disposals",
        *_erp_cols(),
        sa.Column("asset_id",        postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("ast_assets.id"), nullable=False),
        sa.Column("asset_number",    sa.String(50),  nullable=False),
        sa.Column("disposal_date",   sa.Date(),      nullable=False),
        sa.Column("disposal_type",   sa.String(20),  server_default="sale"),
        sa.Column("sale_price",      sa.Numeric(18,3), server_default="0"),
        sa.Column("nbv_at_disposal", sa.Numeric(18,3), nullable=False),
        sa.Column("gain_loss",       sa.Numeric(18,3), server_default="0"),
        sa.Column("je_id",           postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",       sa.String(50),  nullable=True),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","asset_id", name="uq_disposal_asset"),
    )

    # ══════════════════════════════════════════════════════
    # M6 — TREASURY
    # ══════════════════════════════════════════════════════

    # Bank Accounts
    op.create_table(
        "tr_bank_accounts",
        *_erp_cols(),
        *_soft_delete_cols(),
        sa.Column("account_code",    sa.String(30),  nullable=False),
        sa.Column("account_name",    sa.String(255), nullable=False),
        sa.Column("account_type",    sa.String(20),  server_default="bank"),
        sa.Column("bank_name",       sa.String(100), nullable=True),
        sa.Column("iban",            sa.String(34),  nullable=True),
        sa.Column("swift",           sa.String(20),  nullable=True),
        sa.Column("currency",        sa.String(3),   server_default="SAR"),
        sa.Column("current_balance", sa.Numeric(18,3), server_default="0"),
        sa.Column("gl_account",      sa.String(20),  server_default="1100"),
        sa.Column("is_active",       sa.Boolean(),   server_default="true"),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","account_code", name="uq_ba_tenant_code"),
    )

    # Cash Transactions
    op.create_table(
        "tr_cash_transactions",
        *_erp_cols(),
        sa.Column("tx_number",       sa.String(50),  nullable=False),
        sa.Column("tx_date",         sa.Date(),      nullable=False),
        sa.Column("tx_type",         sa.String(20),  nullable=False),
        sa.Column("status",          sa.String(20),  server_default="draft"),
        sa.Column("bank_account_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("tr_bank_accounts.id"), nullable=False),
        sa.Column("account_code",    sa.String(30),  nullable=False),
        sa.Column("amount",          sa.Numeric(18,3), nullable=False),
        sa.Column("balance_after",   sa.Numeric(18,3), server_default="0"),
        sa.Column("counterpart_account", sa.String(20), nullable=False),
        sa.Column("description",     sa.String(500), nullable=False),
        sa.Column("reference",       sa.String(100), nullable=True),
        sa.Column("party_name",      sa.String(255), nullable=True),
        sa.Column("je_id",           postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("je_serial",       sa.String(50),  nullable=True),
        sa.Column("posted_at",       sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciled",      sa.Boolean(),   server_default="false"),
        sa.Column("notes",           sa.Text(),      nullable=True),
        sa.UniqueConstraint("tenant_id","tx_number", name="uq_tx_tenant_number"),
        sa.CheckConstraint("amount > 0", name="ck_tx_amount_positive"),
    )
    op.create_index("ix_tx_tenant_date","tr_cash_transactions",["tenant_id","tx_date"])
    op.create_index("ix_tx_bank_account","tr_cash_transactions",["bank_account_id"])

    # ══════════════════════════════════════════════════════
    # Number Series (shared by all modules)
    # ══════════════════════════════════════════════════════
    op.create_table(
        "num_series",
        *_erp_cols(),
        sa.Column("prefix",      sa.String(20),  nullable=False),
        sa.Column("next_value",  sa.Integer(),   server_default="1", nullable=False),
        sa.Column("padding",     sa.Integer(),   server_default="6",  nullable=False),
        sa.Column("period_key",  sa.String(10),  nullable=True),
        sa.UniqueConstraint("tenant_id","prefix","period_key", name="uq_series_tenant_prefix_period"),
    )


def downgrade() -> None:
    # Drop in reverse dependency order
    op.drop_table("tr_cash_transactions")
    op.drop_table("tr_bank_accounts")
    op.drop_table("ast_disposals")
    op.drop_table("ast_depreciation_schedules")
    op.drop_table("ast_assets")
    op.drop_table("hr_leave_requests")
    op.drop_table("hr_payroll_lines")
    op.drop_table("hr_payroll_runs")
    op.drop_table("hr_employees")
    op.drop_table("pur_vendor_inv_lines")
    op.drop_table("pur_vendor_invoices")
    op.drop_table("pur_grn_lines")
    op.drop_table("pur_grn")
    op.drop_table("pur_po_lines")
    op.drop_table("pur_purchase_orders")
    op.drop_table("pur_suppliers")
    op.drop_table("sal_return_lines")
    op.drop_table("sal_returns")
    op.drop_table("sal_invoice_lines")
    op.drop_table("sal_invoices")
    op.drop_table("sal_customers")
    op.drop_table("inv_adjustment_lines")
    op.drop_table("inv_adjustments")
    op.drop_table("inv_movements")
    op.drop_table("inv_stock_balances")
    op.drop_table("inv_products")
    op.drop_table("inv_warehouses")
    op.drop_table("account_balances")
    op.drop_table("je_lines")
    op.drop_table("journal_entries")
    op.drop_table("fiscal_locks")
    op.drop_table("fiscal_periods")
    op.drop_table("coa_accounts")
    op.drop_table("num_series")
