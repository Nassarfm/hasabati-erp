"""Seed default Chart of Accounts + Number Series

Revision ID: 0002
Revises: 0001
Create Date: 2025-01-01 00:01:00.000000

Seeds:
  - دليل الحسابات الافتراضي (135 حساب) للـ tenant التجريبي
  - Number series prefixes
  - Default warehouse (MAIN)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

# Demo tenant — يتطابق مع DEMO_PROFILES في tenant.py
DEMO_TENANT = uuid.UUID("00000000-0000-0000-0000-000000000001")
NOW = datetime.now(timezone.utc)


def _id() -> str:
    return str(uuid.uuid4())


def _coa(code, name_ar, name_en, account_type, nature, level, parent_code=None, is_postable=True):
    return {
        "id": _id(),
        "tenant_id": str(DEMO_TENANT),
        "code": code,
        "name_ar": name_ar,
        "name_en": name_en,
        "account_type": account_type,
        "account_nature": nature,
        "level": level,
        "is_postable": is_postable,
        "is_active": True,
        "opening_balance": 0,
        "current_balance": 0,
        "created_at": NOW,
        "created_by": "system",
        "is_deleted": False,
    }


def upgrade() -> None:
    conn = op.get_bind()

    # ── 1. Number Series ──────────────────────────────────
    series = [
        # (prefix, description)
        ("JE",  "قيود يومية"),
        ("AJE", "قيود تسوية"),
        ("TJE", "قيود خزينة"),
        ("PAY", "دورات رواتب"),
        ("INV", "فواتير مبيعات"),
        ("RCP", "مرتجعات مبيعات"),
        ("PO",  "أوامر شراء"),
        ("GRN", "استلام بضاعة"),
        ("VI",  "فواتير موردين"),
        ("ADJ", "تسويات مخزون"),
        ("EMP", "الموظفون"),
        ("LVE", "طلبات إجازة"),
        ("AST", "الأصول الثابتة"),
        ("PMT", "مدفوعات"),
    ]
    op.bulk_insert(
        sa.table("num_series",
                 sa.column("id"), sa.column("tenant_id"),
                 sa.column("prefix"), sa.column("next_value"),
                 sa.column("padding"), sa.column("period_key"),
                 sa.column("created_at"), sa.column("created_by")),
        [{"id": _id(), "tenant_id": str(DEMO_TENANT),
          "prefix": p, "next_value": 1, "padding": 6,
          "period_key": None, "created_at": NOW, "created_by": "system"}
         for p, _ in series],
    )

    # ── 2. Default Warehouse ──────────────────────────────
    op.bulk_insert(
        sa.table("inv_warehouses",
                 sa.column("id"), sa.column("tenant_id"),
                 sa.column("code"), sa.column("name_ar"), sa.column("name_en"),
                 sa.column("is_default"), sa.column("is_active"),
                 sa.column("created_at"), sa.column("created_by"),
                 sa.column("is_deleted")),
        [{"id": _id(), "tenant_id": str(DEMO_TENANT),
          "code": "MAIN", "name_ar": "المستودع الرئيسي", "name_en": "Main Warehouse",
          "is_default": True, "is_active": True,
          "created_at": NOW, "created_by": "system", "is_deleted": False}],
    )

    # ── 3. Chart of Accounts (135 accounts) ──────────────
    coa_rows = [
        # ════ ASSETS — أصول ════
        _coa("1000", "الأصول",                  "Assets",                   "asset", "debit",  1, is_postable=False),
        _coa("1100", "الأصول المتداولة",         "Current Assets",           "asset", "debit",  2, is_postable=False),
        _coa("1001", "النقدية",                  "Cash",                     "asset", "debit",  3),
        _coa("1100", "البنوك",                   "Banks",                    "asset", "debit",  3),
        _coa("1101", "البنك الأهلي",             "ANB",                      "asset", "debit",  4),
        _coa("1102", "بنك الراجحي",              "Al Rajhi Bank",            "asset", "debit",  4),
        _coa("1103", "بنك الرياض",               "Riyad Bank",               "asset", "debit",  4),
        _coa("1201", "ذمم العملاء",              "Accounts Receivable",      "asset", "debit",  3),
        _coa("1202", "مخصص الديون المشكوك فيها", "Allowance for Doubtful",   "asset", "credit", 4),
        _coa("1203", "أوراق القبض",              "Notes Receivable",         "asset", "debit",  3),
        _coa("1301", "المخزون",                  "Inventory",                "asset", "debit",  3),
        _coa("1302", "مخزون قيد المعالجة",       "WIP Inventory",            "asset", "debit",  4),
        _coa("1401", "ضريبة القيمة المضافة - مدخل","VAT Input",              "asset", "debit",  3),
        _coa("1402", "دفعات مقدمة للموردين",     "Prepaid to Suppliers",     "asset", "debit",  3),
        _coa("1403", "مصاريف مدفوعة مقدماً",    "Prepaid Expenses",         "asset", "debit",  3),
        _coa("1404", "أصول أخرى متداولة",        "Other Current Assets",     "asset", "debit",  3),

        _coa("1500", "الأصول غير المتداولة",     "Non-Current Assets",       "asset", "debit",  2, is_postable=False),
        _coa("1501", "الأراضي",                  "Land",                     "asset", "debit",  3),
        _coa("1502", "المباني",                  "Buildings",                "asset", "debit",  3),
        _coa("1503", "الآلات والمعدات",          "Machinery & Equipment",    "asset", "debit",  3),
        _coa("1504", "السيارات والمركبات",        "Vehicles",                 "asset", "debit",  3),
        _coa("1505", "الأثاث والتجهيزات",        "Furniture & Fixtures",     "asset", "debit",  3),
        _coa("1506", "أجهزة الحاسب الآلي",       "Computer Equipment",       "asset", "debit",  3),
        _coa("1551", "مجمع إهلاك المباني",        "Accum. Dep. Buildings",    "asset", "credit", 3),
        _coa("1552", "مجمع إهلاك الآلات",         "Accum. Dep. Machinery",    "asset", "credit", 3),
        _coa("1553", "مجمع إهلاك السيارات",       "Accum. Dep. Vehicles",     "asset", "credit", 3),
        _coa("1554", "مجمع إهلاك الأثاث",         "Accum. Dep. Furniture",    "asset", "credit", 3),
        _coa("1555", "مجمع إهلاك الحاسب",         "Accum. Dep. Computers",    "asset", "credit", 3),
        _coa("1601", "الأصول غير الملموسة",       "Intangible Assets",        "asset", "debit",  3),
        _coa("1602", "مجمع إطفاء الأصول غير الملموسة","Accum. Amortization", "asset", "credit", 3),
        _coa("1701", "استثمارات طويلة الأجل",    "Long-term Investments",    "asset", "debit",  3),

        # ════ LIABILITIES — التزامات ════
        _coa("2000", "الالتزامات",               "Liabilities",              "liability", "credit", 1, is_postable=False),
        _coa("2100", "الالتزامات المتداولة",      "Current Liabilities",      "liability", "credit", 2, is_postable=False),
        _coa("2101", "ذمم الموردين",              "Accounts Payable",         "liability", "credit", 3),
        _coa("2102", "أوراق الدفع",               "Notes Payable",            "liability", "credit", 3),
        _coa("2103", "دفعات مقدمة من العملاء",   "Customer Advances",        "liability", "credit", 3),
        _coa("2201", "ضريبة القيمة المضافة - مخرجات","VAT Output",           "liability", "credit", 3),
        _coa("2202", "ضريبة القيمة المضافة - صافي","VAT Payable - Net",      "liability", "credit", 3),
        _coa("2301", "رواتب مستحقة الدفع",        "Salaries Payable",         "liability", "credit", 3),
        _coa("2302", "التزام GOSI",               "GOSI Liability",           "liability", "credit", 3),
        _coa("2303", "مخصص مكافأة نهاية الخدمة", "EOSB Provision",           "liability", "credit", 3),
        _coa("2304", "ضرائب مستحقة",              "Taxes Payable",            "liability", "credit", 3),
        _coa("2305", "التزامات أخرى متداولة",     "Other Current Liabilities","liability", "credit", 3),
        _coa("2400", "الالتزامات غير المتداولة",  "Non-Current Liabilities",  "liability", "credit", 2, is_postable=False),
        _coa("2401", "قروض طويلة الأجل",          "Long-term Loans",          "liability", "credit", 3),
        _coa("2402", "سندات قابلة للاسترداد",     "Redeemable Bonds",         "liability", "credit", 3),

        # ════ EQUITY — حقوق الملكية ════
        _coa("3000", "حقوق الملكية",              "Equity",                   "equity", "credit", 1, is_postable=False),
        _coa("3001", "رأس المال المدفوع",          "Paid-in Capital",          "equity", "credit", 2),
        _coa("3101", "احتياطي قانوني",             "Legal Reserve",            "equity", "credit", 2),
        _coa("3102", "احتياطي عام",                "General Reserve",          "equity", "credit", 2),
        _coa("3201", "الأرباح المحتجزة",           "Retained Earnings",        "equity", "credit", 2),
        _coa("3301", "أرباح / خسائر السنة الحالية","Current Year P&L",        "equity", "credit", 2),

        # ════ REVENUE — الإيرادات ════
        _coa("4000", "الإيرادات",                 "Revenue",                  "revenue", "credit", 1, is_postable=False),
        _coa("4001", "إيرادات المبيعات",           "Sales Revenue",            "revenue", "credit", 2),
        _coa("4002", "إيرادات الخدمات",            "Service Revenue",          "revenue", "credit", 2),
        _coa("4101", "مردودات المبيعات",           "Sales Returns",            "revenue", "debit",  2),
        _coa("4102", "خصومات المبيعات",            "Sales Discounts",          "revenue", "debit",  2),
        _coa("4201", "إيرادات أخرى",               "Other Revenue",            "revenue", "credit", 2),
        _coa("4202", "أرباح بيع أصول",             "Gain on Asset Disposal",   "revenue", "credit", 2),
        _coa("4203", "إيرادات استثمارية",           "Investment Income",        "revenue", "credit", 2),

        # ════ COGS — تكلفة المبيعات ════
        _coa("5000", "تكلفة المبيعات",             "Cost of Goods Sold",       "expense", "debit",  1, is_postable=False),
        _coa("5001", "تكلفة البضاعة المباعة",      "COGS",                     "expense", "debit",  2),
        _coa("5002", "تكلفة الخدمات المقدمة",      "Cost of Services",         "expense", "debit",  2),
        _coa("5101", "مشتريات البضاعة",            "Purchases",                "expense", "debit",  2),
        _coa("5102", "مردودات المشتريات",           "Purchase Returns",         "expense", "credit", 2),
        _coa("5103", "خصومات المشتريات",            "Purchase Discounts",       "expense", "credit", 2),
        _coa("5104", "مصاريف الشحن الواردة",        "Freight In",               "expense", "debit",  2),

        # ════ OPERATING EXPENSES — مصاريف التشغيل ════
        _coa("6000", "مصاريف التشغيل",             "Operating Expenses",       "expense", "debit",  1, is_postable=False),
        _coa("6001", "مصاريف الرواتب والأجور",      "Salaries & Wages",         "expense", "debit",  2),
        _coa("6002", "مصاريف GOSI",                "GOSI Expense",             "expense", "debit",  2),
        _coa("6003", "مصاريف مكافأة نهاية الخدمة", "EOSB Expense",             "expense", "debit",  2),
        _coa("6004", "بدل مواصلات الموظفين",        "Transportation Allowance", "expense", "debit",  2),
        _coa("6005", "مصاريف التدريب والتطوير",     "Training & Development",   "expense", "debit",  2),
        _coa("6101", "إيجار المقر",                 "Office Rent",              "expense", "debit",  2),
        _coa("6102", "إيجار المستودع",              "Warehouse Rent",           "expense", "debit",  2),
        _coa("6201", "مصاريف الكهرباء",             "Electricity",              "expense", "debit",  2),
        _coa("6202", "مصاريف المياه",               "Water",                    "expense", "debit",  2),
        _coa("6203", "مصاريف الاتصالات",            "Telecommunications",       "expense", "debit",  2),
        _coa("6204", "مصاريف الإنترنت",             "Internet",                 "expense", "debit",  2),
        _coa("6301", "مصاريف الإيجار",              "Rent Expense",             "expense", "debit",  2),
        _coa("6401", "مصاريف التسويق والإعلان",     "Marketing & Advertising",  "expense", "debit",  2),
        _coa("6402", "مصاريف العلاقات العامة",       "Public Relations",         "expense", "debit",  2),
        _coa("6501", "مصاريف إهلاك المباني",         "Dep. Expense - Buildings", "expense", "debit",  2),
        _coa("6502", "مصاريف إهلاك الآلات",          "Dep. Expense - Machinery", "expense", "debit",  2),
        _coa("6503", "مصاريف إهلاك السيارات",        "Dep. Expense - Vehicles",  "expense", "debit",  2),
        _coa("6504", "مصاريف إهلاك الأثاث",          "Dep. Expense - Furniture", "expense", "debit",  2),
        _coa("6505", "مصاريف إهلاك الحاسب",          "Dep. Expense - Computers", "expense", "debit",  2),
        _coa("6506", "مصاريف الإطفاء",               "Amortization Expense",     "expense", "debit",  2),
        _coa("6601", "مصاريف تمويلية",               "Finance Costs",            "expense", "debit",  2),
        _coa("6602", "فوائد القروض",                  "Loan Interest",            "expense", "debit",  2),
        _coa("6701", "مصاريف قانونية",                "Legal Expenses",           "expense", "debit",  2),
        _coa("6702", "مصاريف استشارية",               "Consulting Expenses",      "expense", "debit",  2),
        _coa("6703", "مصاريف التدقيق والمحاسبة",      "Audit & Accounting",       "expense", "debit",  2),
        _coa("6801", "مصاريف الصيانة والإصلاح",       "Maintenance & Repair",     "expense", "debit",  2),
        _coa("6802", "مصاريف لوازم المكتب",            "Office Supplies",          "expense", "debit",  2),
        _coa("6803", "مصاريف السفر والتنقل",           "Travel & Transport",       "expense", "debit",  2),
        _coa("6804", "مصاريف الضيافة",                 "Entertainment",            "expense", "debit",  2),
        _coa("6805", "مصاريف التأمين",                 "Insurance",                "expense", "debit",  2),
        _coa("6806", "مصاريف الشحن الصادر",            "Freight Out",              "expense", "debit",  2),
        _coa("6807", "مصاريف الرسوم الحكومية",          "Government Fees",          "expense", "debit",  2),
        _coa("6808", "مصاريف التراخيص والاشتراكات",    "Licenses & Subscriptions", "expense", "debit",  2),
        _coa("6901", "مصاريف أخرى",                    "Other Expenses",           "expense", "debit",  2),
        _coa("6902", "خسائر بيع أصول",                 "Loss on Asset Disposal",   "expense", "debit",  2),
        _coa("6903", "ديون معدومة",                     "Bad Debt Expense",         "expense", "debit",  2),
        _coa("6904", "مصاريف متنوعة",                   "Miscellaneous Expenses",   "expense", "debit",  2),
    ]

    # Fix: remove duplicate 1100 (was used for both current assets header AND banks)
    seen_codes = set()
    deduped = []
    for row in coa_rows:
        if row["code"] not in seen_codes:
            deduped.append(row)
            seen_codes.add(row["code"])

    op.bulk_insert(
        sa.table(
            "coa_accounts",
            sa.column("id"), sa.column("tenant_id"),
            sa.column("code"), sa.column("name_ar"), sa.column("name_en"),
            sa.column("account_type"), sa.column("account_nature"),
            sa.column("level"), sa.column("is_postable"), sa.column("is_active"),
            sa.column("opening_balance"), sa.column("current_balance"),
            sa.column("created_at"), sa.column("created_by"), sa.column("is_deleted"),
        ),
        deduped,
    )


def downgrade() -> None:
    op.execute(
        f"DELETE FROM coa_accounts WHERE tenant_id = '{DEMO_TENANT}'"
    )
    op.execute(
        f"DELETE FROM inv_warehouses WHERE tenant_id = '{DEMO_TENANT}'"
    )
    op.execute(
        f"DELETE FROM num_series WHERE tenant_id = '{DEMO_TENANT}'"
    )
