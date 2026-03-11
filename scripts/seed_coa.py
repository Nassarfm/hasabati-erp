"""
scripts/seed_coa.py
══════════════════════════════════════════════════════════
دليل الحسابات الجاهز — موصى به للشركات السعودية
متوافق مع: IFRS / IAS 1 / ZATCA

هيكل هرمي 4 مستويات:
  المستوى 1 — قسم رئيسي    (غير قابل للترحيل)
  المستوى 2 — مجموعة       (غير قابل للترحيل)
  المستوى 3 — حساب رئيسي   (غير قابل للترحيل)
  المستوى 4 — حساب تحليلي  (قابل للترحيل ← هنا تُرحَّل القيود)

تشغيل:
  python scripts/seed_coa.py --tenant-id <UUID> [--env .env]
  python scripts/seed_coa.py --tenant-id 00000000-0000-0000-0000-000000000001

يدعم:
  --dry-run   عرض الحسابات بدون حفظ
  --reset     حذف الحسابات الموجودة ثم إعادة الإدراج
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# ── Add project root to path ──────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))


# ══════════════════════════════════════════════════════════
# Account definition dataclass
# ══════════════════════════════════════════════════════════
@dataclass
class AccountDef:
    code: str
    name_ar: str
    name_en: str
    account_type: str      # asset|liability|equity|revenue|expense
    account_nature: str    # debit|credit
    level: int
    parent_code: Optional[str] = None
    postable: bool = False         # only level-4 accounts are postable
    opening_balance: float = 0.0
    notes: str = ""


# ══════════════════════════════════════════════════════════
# دليل الحسابات الكامل
# ══════════════════════════════════════════════════════════
COA: List[AccountDef] = [

    # ╔══════════════════════════════════════════════════════╗
    # ║  1 — الأصول  Assets                                 ║
    # ╚══════════════════════════════════════════════════════╝
    AccountDef("1", "الأصول", "Assets", "asset", "debit", 1),

    # ── 11 الأصول المتداولة ────────────────────────────────
    AccountDef("11", "الأصول المتداولة", "Current Assets", "asset", "debit", 2, "1"),

    # النقدية وما في حكمها
    AccountDef("1100", "النقدية وما في حكمها", "Cash & Equivalents", "asset", "debit", 3, "11"),
    AccountDef("1001", "الصندوق الرئيسي", "Main Cash", "asset", "debit", 4, "1100", postable=True),
    AccountDef("1002", "صندوق المصروفات النثرية", "Petty Cash", "asset", "debit", 4, "1100", postable=True),
    AccountDef("1100", "البنك الأهلي السعودي", "SNB Bank", "asset", "debit", 4, "1100", postable=True),
    AccountDef("1101", "بنك الراجحي", "Al Rajhi Bank", "asset", "debit", 4, "1100", postable=True),
    AccountDef("1102", "بنك الرياض", "Riyad Bank", "asset", "debit", 4, "1100", postable=True),

    # الذمم المدينة
    AccountDef("1200", "الذمم المدينة", "Accounts Receivable", "asset", "debit", 3, "11"),
    AccountDef("1201", "ذمم العملاء المحليين", "Local Customers AR", "asset", "debit", 4, "1200", postable=True),
    AccountDef("1202", "ذمم العملاء الخارجيين", "Foreign Customers AR", "asset", "debit", 4, "1200", postable=True),
    AccountDef("1203", "أوراق القبض", "Notes Receivable", "asset", "debit", 4, "1200", postable=True),
    AccountDef("1204", "مخصص الديون المشكوك فيها", "Allowance for Doubtful Debts", "asset", "credit", 4, "1200", postable=True),

    # المخزون
    AccountDef("1300", "المخزون", "Inventory", "asset", "debit", 3, "11"),
    AccountDef("1301", "بضاعة للبيع", "Merchandise Inventory", "asset", "debit", 4, "1300", postable=True),
    AccountDef("1302", "مواد خام", "Raw Materials", "asset", "debit", 4, "1300", postable=True),
    AccountDef("1303", "إنتاج تحت التشغيل", "Work in Progress", "asset", "debit", 4, "1300", postable=True),
    AccountDef("1304", "بضاعة في الطريق", "Goods in Transit", "asset", "debit", 4, "1300", postable=True),

    # ضريبة القيمة المضافة
    AccountDef("1400", "ضريبة القيمة المضافة المدخلات", "VAT Input", "asset", "debit", 3, "11"),
    AccountDef("1401", "ضريبة القيمة المضافة القابلة للاسترداد", "VAT Recoverable", "asset", "debit", 4, "1400", postable=True),

    # المصروفات المدفوعة مقدماً
    AccountDef("1450", "المدفوعات المقدمة", "Prepaid Expenses", "asset", "debit", 3, "11"),
    AccountDef("1451", "إيجار مقدم", "Prepaid Rent", "asset", "debit", 4, "1450", postable=True),
    AccountDef("1452", "تأمين مقدم", "Prepaid Insurance", "asset", "debit", 4, "1450", postable=True),
    AccountDef("1453", "دفعات مقدمة للموردين", "Advances to Suppliers", "asset", "debit", 4, "1450", postable=True),

    # ── 12 الأصول غير المتداولة ────────────────────────────
    AccountDef("12", "الأصول غير المتداولة", "Non-Current Assets", "asset", "debit", 2, "1"),

    # الأصول الثابتة
    AccountDef("1500", "الأصول الثابتة", "Fixed Assets", "asset", "debit", 3, "12"),
    AccountDef("1501", "الأراضي", "Land", "asset", "debit", 4, "1500", postable=True),
    AccountDef("1502", "المباني", "Buildings", "asset", "debit", 4, "1500", postable=True),
    AccountDef("1503", "الآلات والمعدات", "Machinery & Equipment", "asset", "debit", 4, "1500", postable=True),
    AccountDef("1504", "السيارات والمركبات", "Vehicles", "asset", "debit", 4, "1500", postable=True),
    AccountDef("1505", "أثاث ومفروشات", "Furniture & Fixtures", "asset", "debit", 4, "1500", postable=True),
    AccountDef("1506", "أجهزة حاسب آلي", "Computer Equipment", "asset", "debit", 4, "1500", postable=True),

    # مجمع الإهلاك
    AccountDef("1550", "مجمع الإهلاك", "Accumulated Depreciation", "asset", "credit", 3, "12"),
    AccountDef("1551", "مجمع إهلاك المباني", "Accum. Dep. Buildings", "asset", "credit", 4, "1550", postable=True),
    AccountDef("1552", "مجمع إهلاك الآلات", "Accum. Dep. Machinery", "asset", "credit", 4, "1550", postable=True),
    AccountDef("1553", "مجمع إهلاك السيارات", "Accum. Dep. Vehicles", "asset", "credit", 4, "1550", postable=True),
    AccountDef("1554", "مجمع إهلاك الأثاث", "Accum. Dep. Furniture", "asset", "credit", 4, "1550", postable=True),
    AccountDef("1555", "مجمع إهلاك أجهزة الحاسب", "Accum. Dep. Computers", "asset", "credit", 4, "1550", postable=True),

    # الأصول غير الملموسة
    AccountDef("1600", "الأصول غير الملموسة", "Intangible Assets", "asset", "debit", 3, "12"),
    AccountDef("1601", "برامج وتراخيص", "Software & Licenses", "asset", "debit", 4, "1600", postable=True),
    AccountDef("1602", "شهرة المحل", "Goodwill", "asset", "debit", 4, "1600", postable=True),

    # ╔══════════════════════════════════════════════════════╗
    # ║  2 — الالتزامات  Liabilities                        ║
    # ╚══════════════════════════════════════════════════════╝
    AccountDef("2", "الالتزامات", "Liabilities", "liability", "credit", 1),

    # ── 21 الالتزامات المتداولة ────────────────────────────
    AccountDef("21", "الالتزامات المتداولة", "Current Liabilities", "liability", "credit", 2, "2"),

    # الذمم الدائنة
    AccountDef("2100", "الذمم الدائنة", "Accounts Payable", "liability", "credit", 3, "21"),
    AccountDef("2101", "ذمم الموردين المحليين", "Local Suppliers AP", "liability", "credit", 4, "2100", postable=True),
    AccountDef("2102", "ذمم الموردين الخارجيين", "Foreign Suppliers AP", "liability", "credit", 4, "2100", postable=True),
    AccountDef("2103", "أوراق الدفع", "Notes Payable", "liability", "credit", 4, "2100", postable=True),

    # ضريبة القيمة المضافة المستحقة
    AccountDef("2200", "ضريبة القيمة المضافة المستحقة", "VAT Payable", "liability", "credit", 3, "21"),
    AccountDef("2201", "ضريبة القيمة المضافة المخرجات", "VAT Output", "liability", "credit", 4, "2200", postable=True),
    AccountDef("2202", "صافي ضريبة القيمة المضافة المستحق", "Net VAT Due", "liability", "credit", 4, "2200", postable=True),

    # الرواتب والمزايا
    AccountDef("2300", "الرواتب والمزايا المستحقة", "Accrued Payroll", "liability", "credit", 3, "21"),
    AccountDef("2301", "رواتب مستحقة الدفع", "Salaries Payable", "liability", "credit", 4, "2300", postable=True),
    AccountDef("2302", "التزامات GOSI", "GOSI Payable", "liability", "credit", 4, "2300", postable=True),
    AccountDef("2303", "مكافأة نهاية الخدمة المستحقة", "End of Service Accrual", "liability", "credit", 4, "2300", postable=True),
    AccountDef("2304", "إجازات مستحقة", "Accrued Leave", "liability", "credit", 4, "2300", postable=True),

    # المصروفات المستحقة
    AccountDef("2400", "المصروفات المستحقة", "Accrued Expenses", "liability", "credit", 3, "21"),
    AccountDef("2401", "إيجار مستحق", "Accrued Rent", "liability", "credit", 4, "2400", postable=True),
    AccountDef("2402", "مصروفات مستحقة أخرى", "Other Accrued Expenses", "liability", "credit", 4, "2400", postable=True),
    AccountDef("2403", "دفعات مقدمة من العملاء", "Customer Advances", "liability", "credit", 4, "2400", postable=True),

    # ── 22 الالتزامات غير المتداولة ───────────────────────
    AccountDef("22", "الالتزامات غير المتداولة", "Non-Current Liabilities", "liability", "credit", 2, "2"),

    AccountDef("2500", "قروض طويلة الأجل", "Long-Term Loans", "liability", "credit", 3, "22"),
    AccountDef("2501", "قرض بنكي طويل الأجل", "Bank Loan LT", "liability", "credit", 4, "2500", postable=True),
    AccountDef("2502", "التزام عقد إيجار تمويلي", "Finance Lease Liability", "liability", "credit", 4, "2500", postable=True),

    AccountDef("2600", "احتياطيات مستحقة طويلة الأجل", "LT Provisions", "liability", "credit", 3, "22"),
    AccountDef("2601", "مخصص مكافأة نهاية الخدمة", "End of Service Provision", "liability", "credit", 4, "2600", postable=True),

    # ╔══════════════════════════════════════════════════════╗
    # ║  3 — حقوق الملكية  Equity                           ║
    # ╚══════════════════════════════════════════════════════╝
    AccountDef("3", "حقوق الملكية", "Equity", "equity", "credit", 1),
    AccountDef("31", "رأس المال والاحتياطيات", "Capital & Reserves", "equity", "credit", 2, "3"),

    AccountDef("3000", "رأس المال", "Share Capital", "equity", "credit", 3, "31"),
    AccountDef("3001", "رأس المال المدفوع", "Paid-in Capital", "equity", "credit", 4, "3000", postable=True),

    AccountDef("3100", "الاحتياطيات", "Reserves", "equity", "credit", 3, "31"),
    AccountDef("3101", "الاحتياطي النظامي", "Statutory Reserve", "equity", "credit", 4, "3100", postable=True),
    AccountDef("3102", "الاحتياطي الاختياري", "Voluntary Reserve", "equity", "credit", 4, "3100", postable=True),

    AccountDef("3200", "الأرباح المبقاة", "Retained Earnings", "equity", "credit", 3, "31"),
    AccountDef("3201", "أرباح مبقاة من سنوات سابقة", "Prior Year Retained Earnings", "equity", "credit", 4, "3200", postable=True),
    AccountDef("3202", "صافي ربح / خسارة السنة الحالية", "Current Year Net Income", "equity", "credit", 4, "3200", postable=True),

    # ╔══════════════════════════════════════════════════════╗
    # ║  4 — الإيرادات  Revenue                             ║
    # ╚══════════════════════════════════════════════════════╝
    AccountDef("4", "الإيرادات", "Revenue", "revenue", "credit", 1),
    AccountDef("41", "إيرادات التشغيل", "Operating Revenue", "revenue", "credit", 2, "4"),

    AccountDef("4000", "إيرادات المبيعات", "Sales Revenue", "revenue", "credit", 3, "41"),
    AccountDef("4001", "مبيعات بضاعة", "Merchandise Sales", "revenue", "credit", 4, "4000", postable=True),
    AccountDef("4002", "مبيعات خدمات", "Service Revenue", "revenue", "credit", 4, "4000", postable=True),
    AccountDef("4003", "مبيعات مواد خام", "Raw Material Sales", "revenue", "credit", 4, "4000", postable=True),

    AccountDef("4100", "مردودات ومسموحات المبيعات", "Sales Returns & Allowances", "revenue", "debit", 3, "41"),
    AccountDef("4101", "مردودات مبيعات", "Sales Returns", "revenue", "debit", 4, "4100", postable=True),
    AccountDef("4102", "خصومات مبيعات", "Sales Discounts", "revenue", "debit", 4, "4100", postable=True),

    AccountDef("42", "إيرادات أخرى", "Other Revenue", "revenue", "credit", 2, "4"),
    AccountDef("4200", "إيرادات أخرى", "Other Income", "revenue", "credit", 3, "42"),
    AccountDef("4201", "إيرادات فوائد", "Interest Income", "revenue", "credit", 4, "4200", postable=True),
    AccountDef("4202", "أرباح بيع أصول", "Gain on Asset Disposal", "revenue", "credit", 4, "4200", postable=True),
    AccountDef("4203", "إيرادات متنوعة", "Miscellaneous Income", "revenue", "credit", 4, "4200", postable=True),
    AccountDef("4204", "فروق جرد موجبة", "Inventory Variance Gain", "revenue", "credit", 4, "4200", postable=True),

    # ╔══════════════════════════════════════════════════════╗
    # ║  5 — تكلفة المبيعات  Cost of Sales                  ║
    # ╚══════════════════════════════════════════════════════╝
    AccountDef("5", "تكلفة المبيعات", "Cost of Sales", "expense", "debit", 1),
    AccountDef("51", "تكلفة البضاعة المباعة", "Cost of Goods Sold", "expense", "debit", 2, "5"),

    AccountDef("5000", "تكلفة المبيعات", "Cost of Sales", "expense", "debit", 3, "51"),
    AccountDef("5001", "تكلفة البضاعة المباعة", "COGS - Merchandise", "expense", "debit", 4, "5000", postable=True),
    AccountDef("5002", "تكلفة الخدمات المقدمة", "Cost of Services", "expense", "debit", 4, "5000", postable=True),
    AccountDef("5003", "مردودات المشتريات", "Purchase Returns", "expense", "credit", 4, "5000", postable=True),
    AccountDef("5004", "خصومات المشتريات", "Purchase Discounts", "expense", "credit", 4, "5000", postable=True),
    AccountDef("5005", "فروق جرد سالبة", "Inventory Variance Loss", "expense", "debit", 4, "5000", postable=True),

    # ╔══════════════════════════════════════════════════════╗
    # ║  6 — المصروفات التشغيلية  Operating Expenses        ║
    # ╚══════════════════════════════════════════════════════╝
    AccountDef("6", "المصروفات", "Expenses", "expense", "debit", 1),

    # مصروفات البيع والتوزيع
    AccountDef("61", "مصروفات البيع والتوزيع", "Selling & Distribution", "expense", "debit", 2, "6"),
    AccountDef("6100", "مصروفات البيع", "Selling Expenses", "expense", "debit", 3, "61"),
    AccountDef("6101", "رواتب فريق المبيعات", "Sales Staff Salaries", "expense", "debit", 4, "6100", postable=True),
    AccountDef("6102", "عمولات مبيعات", "Sales Commissions", "expense", "debit", 4, "6100", postable=True),
    AccountDef("6103", "مصروفات التسويق والإعلان", "Marketing & Advertising", "expense", "debit", 4, "6100", postable=True),
    AccountDef("6104", "مصروفات الشحن والتوصيل", "Freight & Delivery", "expense", "debit", 4, "6100", postable=True),

    # المصروفات العمومية والإدارية
    AccountDef("62", "المصروفات العمومية والإدارية", "General & Administrative", "expense", "debit", 2, "6"),
    AccountDef("6200", "مصروفات الرواتب", "Salaries & Wages", "expense", "debit", 3, "62"),
    AccountDef("6001", "رواتب الموظفين", "Employee Salaries", "expense", "debit", 4, "6200", postable=True),
    AccountDef("6002", "GOSI — حصة صاحب العمل", "GOSI - Employer Share", "expense", "debit", 4, "6200", postable=True),
    AccountDef("6003", "مكافأة نهاية الخدمة", "End of Service Expense", "expense", "debit", 4, "6200", postable=True),
    AccountDef("6004", "بدلات وعلاوات", "Allowances & Benefits", "expense", "debit", 4, "6200", postable=True),

    AccountDef("6300", "مصروفات الإيجار والمرافق", "Rent & Utilities", "expense", "debit", 3, "62"),
    AccountDef("6301", "إيجار المكاتب والمستودعات", "Office & Warehouse Rent", "expense", "debit", 4, "6300", postable=True),
    AccountDef("6302", "الكهرباء والماء", "Utilities", "expense", "debit", 4, "6300", postable=True),
    AccountDef("6303", "الاتصالات والإنترنت", "Telecom & Internet", "expense", "debit", 4, "6300", postable=True),

    AccountDef("6400", "مصروفات إدارية متنوعة", "Misc Admin Expenses", "expense", "debit", 3, "62"),
    AccountDef("6401", "مصروفات التأمين", "Insurance Expenses", "expense", "debit", 4, "6400", postable=True),
    AccountDef("6402", "مصروفات قانونية ومحاسبية", "Legal & Accounting Fees", "expense", "debit", 4, "6400", postable=True),
    AccountDef("6403", "مصروفات القرطاسية والمستلزمات", "Stationery & Supplies", "expense", "debit", 4, "6400", postable=True),
    AccountDef("6404", "مصروفات السفر والتنقلات", "Travel & Transportation", "expense", "debit", 4, "6400", postable=True),
    AccountDef("6005", "مصروفات صيانة وإصلاح", "Maintenance & Repairs", "expense", "debit", 4, "6400", postable=True),
    AccountDef("6006", "مصروفات متنوعة", "Miscellaneous Expenses", "expense", "debit", 4, "6400", postable=True),

    # الإهلاك
    AccountDef("63", "مصروفات الإهلاك والاستهلاك", "Depreciation & Amortization", "expense", "debit", 2, "6"),
    AccountDef("6500", "مصروفات الإهلاك", "Depreciation Expense", "expense", "debit", 3, "63"),
    AccountDef("6501", "إهلاك المباني", "Dep. Buildings", "expense", "debit", 4, "6500", postable=True),
    AccountDef("6502", "إهلاك الآلات والمعدات", "Dep. Machinery", "expense", "debit", 4, "6500", postable=True),
    AccountDef("6503", "إهلاك السيارات", "Dep. Vehicles", "expense", "debit", 4, "6500", postable=True),
    AccountDef("6504", "إهلاك الأثاث والمفروشات", "Dep. Furniture", "expense", "debit", 4, "6500", postable=True),
    AccountDef("6505", "إهلاك أجهزة الحاسب", "Dep. Computers", "expense", "debit", 4, "6500", postable=True),
    AccountDef("6506", "استهلاك الأصول غير الملموسة", "Amortization Intangibles", "expense", "debit", 4, "6500", postable=True),

    # المصروفات المالية
    AccountDef("64", "المصروفات المالية", "Finance Costs", "expense", "debit", 2, "6"),
    AccountDef("6600", "مصروفات التمويل", "Finance Expenses", "expense", "debit", 3, "64"),
    AccountDef("6601", "فوائد قروض بنكية", "Bank Loan Interest", "expense", "debit", 4, "6600", postable=True),
    AccountDef("6602", "عمولات بنكية", "Bank Charges", "expense", "debit", 4, "6600", postable=True),
    AccountDef("6603", "فروق عملة", "Foreign Exchange Loss", "expense", "debit", 4, "6600", postable=True),
]


# ══════════════════════════════════════════════════════════
# Seed logic
# ══════════════════════════════════════════════════════════
async def seed(tenant_id: str, dry_run: bool = False, reset: bool = False) -> None:
    """Insert all accounts for the given tenant."""
    from dotenv import load_dotenv
    load_dotenv()

    from app.core.config import settings
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from sqlalchemy import text, select, delete

    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    tid = uuid.UUID(tenant_id)
    now = datetime.now(timezone.utc)

    # Build code → UUID map
    code_to_id: dict[str, uuid.UUID] = {}

    async with Session() as db:
        if reset and not dry_run:
            await db.execute(
                text("DELETE FROM coa_accounts WHERE tenant_id = :tid"),
                {"tid": str(tid)},
            )
            await db.commit()
            print("🗑  تم حذف الحسابات القديمة")

        inserted = 0
        skipped = 0
        errors = []

        for acc in COA:
            # Fix: some codes appear as both parent and child (like 1100)
            # Deduplicate by assigning consistent UUIDs
            if acc.code not in code_to_id:
                code_to_id[acc.code] = uuid.uuid4()
            acc_id = code_to_id[acc.code]

            parent_id = None
            if acc.parent_code:
                parent_id = code_to_id.get(acc.parent_code)

            if dry_run:
                icon = "📝" if acc.postable else "📁"
                indent = "  " * (acc.level - 1)
                print(f"{indent}{icon} {acc.code} — {acc.name_ar} ({acc.account_type})")
                inserted += 1
                continue

            try:
                await db.execute(
                    text("""
                        INSERT INTO coa_accounts (
                            id, tenant_id, code, name_ar, name_en,
                            account_type, account_nature, level, parent_id,
                            postable, is_active, allow_direct_posting,
                            opening_balance, created_at, updated_at, created_by
                        ) VALUES (
                            :id, :tid, :code, :name_ar, :name_en,
                            :account_type, :account_nature, :level, :parent_id,
                            :postable, true, :postable,
                            :opening_balance, :now, :now, 'seed_script'
                        )
                        ON CONFLICT (tenant_id, code) DO NOTHING
                    """),
                    {
                        "id": str(acc_id),
                        "tid": str(tid),
                        "code": acc.code,
                        "name_ar": acc.name_ar,
                        "name_en": acc.name_en,
                        "account_type": acc.account_type,
                        "account_nature": acc.account_nature,
                        "level": acc.level,
                        "parent_id": str(parent_id) if parent_id else None,
                        "postable": acc.postable,
                        "opening_balance": acc.opening_balance,
                        "now": now,
                    },
                )
                inserted += 1
            except Exception as e:
                errors.append(f"❌ {acc.code}: {e}")
                skipped += 1

        if not dry_run:
            await db.commit()

    await engine.dispose()

    # ── Summary ────────────────────────────────────────────
    print()
    print("=" * 55)
    if dry_run:
        print(f"  🔍 وضع المعاينة — {inserted} حساب سيتم إدراجه")
    else:
        postable_count = sum(1 for a in COA if a.postable)
        print(f"  ✅ تم إدراج: {inserted} حساب")
        print(f"  📊 قابلة للترحيل: {postable_count} حساب")
        print(f"  ⏭  تم تخطي: {skipped} حساب (مكرر أو خطأ)")
        if errors:
            print()
            for err in errors:
                print(f"  {err}")
    print("=" * 55)


def main():
    parser = argparse.ArgumentParser(
        description="بذر دليل الحسابات الجاهز في قاعدة البيانات"
    )
    parser.add_argument("--tenant-id", required=True, help="UUID المستأجر")
    parser.add_argument("--dry-run", action="store_true", help="عرض بدون حفظ")
    parser.add_argument("--reset", action="store_true", help="حذف الحسابات الموجودة أولاً")
    args = parser.parse_args()

    asyncio.run(seed(args.tenant_id, dry_run=args.dry_run, reset=args.reset))


if __name__ == "__main__":
    main()
