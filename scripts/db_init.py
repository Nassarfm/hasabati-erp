#!/usr/bin/env python3
"""
scripts/db_init.py
══════════════════════════════════════════════════════════
إعداد قاعدة البيانات بأمر واحد.

الاستخدام:
    python scripts/db_init.py            ← تطبيق جميع الـ migrations
    python scripts/db_init.py --fresh    ← حذف كل الجداول وإعادة البناء
    python scripts/db_init.py --status   ← عرض حالة الـ migrations

المتطلبات:
    pip install alembic asyncpg sqlalchemy
    يجب ضبط DATABASE_URL في ملف .env
══════════════════════════════════════════════════════════
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

# ── Ensure we run from project root ──────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))


def run(cmd: list[str], check: bool = True) -> int:
    print(f"\n▶ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check)
    return result.returncode


def load_env():
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        example = PROJECT_ROOT / ".env.example"
        if example.exists():
            print("⚠️  لا يوجد ملف .env — انسخ .env.example وعدّل القيم:")
            print("    cp .env.example .env")
        else:
            print("❌ لا يوجد ملف .env — يرجى إنشاؤه قبل المتابعة")
        sys.exit(1)

    from dotenv import load_dotenv
    load_dotenv(env_file)
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        print("❌ DATABASE_URL غير محدد في .env")
        sys.exit(1)
    print(f"✅ DATABASE_URL: {db_url[:40]}...")


def cmd_status():
    """عرض حالة الـ migrations."""
    run(["alembic", "current"])
    run(["alembic", "history", "--verbose"])


def cmd_upgrade():
    """تطبيق جميع الـ migrations حتى الأحدث."""
    print("\n🚀 تطبيق الـ migrations...")
    run(["alembic", "upgrade", "head"])
    print("\n✅ تم إعداد قاعدة البيانات بنجاح!")
    print_summary()


def cmd_fresh():
    """حذف كل الجداول وإعادة البناء (تحذير: يحذف البيانات)."""
    print("\n⚠️  FRESH MODE — سيتم حذف جميع الجداول وإعادة بنائها!")
    confirm = input("هل أنت متأكد؟ اكتب 'نعم' للمتابعة: ").strip()
    if confirm not in ("نعم", "yes", "y"):
        print("❌ تم الإلغاء")
        sys.exit(0)

    print("\n🗑️  تراجع عن جميع الـ migrations...")
    run(["alembic", "downgrade", "base"])

    print("\n🚀 إعادة تطبيق الـ migrations...")
    run(["alembic", "upgrade", "head"])

    print("\n✅ تم إعادة بناء قاعدة البيانات بنجاح!")
    print_summary()


def print_summary():
    print("""
╔══════════════════════════════════════════════════════════╗
║                حساباتي — DB Ready ✅                    ║
╠══════════════════════════════════════════════════════════╣
║  الجداول المُنشأة:                                       ║
║  M1 Accounting  : acc_chart_of_accounts, acc_je_lines…  ║
║  M2A Inventory  : inv_products, inv_movements…           ║
║  M2B Sales      : sal_invoices, sal_customers…           ║
║  M3 Purchases   : pur_purchase_orders, pur_grn…          ║
║  M4 HR          : hr_employees, hr_payroll_runs…         ║
║  M5 Assets      : ast_assets, ast_depreciation…          ║
║  M6 Treasury    : tr_bank_accounts, tr_cash_tx…          ║
║  Shared         : num_series                             ║
╠══════════════════════════════════════════════════════════╣
║  البيانات الأولية (Seed):                                ║
║  ✅ دليل الحسابات (85 حساب) — tenant تجريبي             ║
║  ✅ مستودع رئيسي (MAIN)                                  ║
║  ✅ تسلسلات الترقيم (14 prefix)                          ║
╠══════════════════════════════════════════════════════════╣
║  الخطوة التالية:                                         ║
║  uvicorn app.main:app --reload                           ║
║  http://localhost:8000/api/docs                          ║
╚══════════════════════════════════════════════════════════╝
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="حساباتي — إعداد قاعدة البيانات")
    parser.add_argument("--fresh",  action="store_true", help="حذف وإعادة بناء الجداول")
    parser.add_argument("--status", action="store_true", help="عرض حالة الـ migrations")
    args = parser.parse_args()

    load_env()

    if args.status:
        cmd_status()
    elif args.fresh:
        cmd_fresh()
    else:
        cmd_upgrade()
