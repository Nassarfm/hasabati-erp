# حساباتي — إعداد قاعدة البيانات

## الـ Migrations

| رقم | الاسم | ما تفعله |
|-----|-------|---------|
| 0001 | `initial_schema` | ينشئ **34 جدول** لجميع الـ modules |
| 0002 | `seed_coa` | يُدخل دليل الحسابات + المستودع + تسلسلات الترقيم |

---

## طريقة التشغيل

### 1. إعداد ملف `.env`
```bash
cp .env.example .env
# عدّل القيم:
#   DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/hasabati
#   SUPABASE_JWT_SECRET=your-jwt-secret
```

### 2. تطبيق الـ Migrations (أمر واحد)
```bash
python scripts/db_init.py
```

أو يدوياً:
```bash
alembic upgrade head
```

### 3. التحقق من الحالة
```bash
python scripts/db_init.py --status
# أو
alembic current
alembic history
```

### 4. إعادة البناء من الصفر (تحذير: يحذف البيانات)
```bash
python scripts/db_init.py --fresh
```

---

## على Supabase

اذهب إلى **SQL Editor** في لوحة تحكم Supabase وشغّل:

```sql
-- تأكد من تفعيل pgcrypto
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

ثم شغّل:
```bash
# في ملف .env ضع DATABASE_URL من Supabase:
# DATABASE_URL=postgresql+asyncpg://postgres:[password]@db.[ref].supabase.co:5432/postgres
alembic upgrade head
```

---

## الجداول المُنشأة

### M1 — محاسبة
| الجدول | الوصف |
|--------|-------|
| `acc_chart_of_accounts` | دليل الحسابات |
| `acc_journal_entries` | القيود اليومية |
| `acc_je_lines` | بنود القيود |
| `acc_account_balances` | أرصدة الحسابات |
| `acc_fiscal_periods` | الفترات المالية |
| `acc_fiscal_locks` | قفل الفترات |

### M2A — مخزون
| الجدول | الوصف |
|--------|-------|
| `inv_warehouses` | المستودعات |
| `inv_products` | الأصناف |
| `inv_stock_balances` | أرصدة المخزون |
| `inv_movements` | حركات المخزون |
| `inv_adjustments` | تسويات الجرد |
| `inv_adjustment_lines` | بنود التسويات |

### M2B — مبيعات
| الجدول | الوصف |
|--------|-------|
| `sal_customers` | العملاء |
| `sal_invoices` | فواتير المبيعات |
| `sal_invoice_lines` | بنود الفواتير |
| `sal_returns` | مرتجعات المبيعات |
| `sal_return_lines` | بنود المرتجعات |

### M3 — مشتريات
| الجدول | الوصف |
|--------|-------|
| `pur_suppliers` | الموردون |
| `pur_purchase_orders` | أوامر الشراء |
| `pur_po_lines` | بنود أوامر الشراء |
| `pur_grn` | استلام البضاعة |
| `pur_grn_lines` | بنود الاستلام |
| `pur_vendor_invoices` | فواتير الموردين |
| `pur_vendor_inv_lines` | بنود الفواتير |

### M4 — موارد بشرية
| الجدول | الوصف |
|--------|-------|
| `hr_employees` | الموظفون |
| `hr_payroll_runs` | دورات الرواتب |
| `hr_payroll_lines` | بنود الرواتب |
| `hr_leave_requests` | طلبات الإجازة |

### M5 — أصول ثابتة
| الجدول | الوصف |
|--------|-------|
| `ast_assets` | الأصول الثابتة |
| `ast_depreciation_schedules` | جداول الإهلاك |
| `ast_disposals` | التخلص من الأصول |

### M6 — خزينة
| الجدول | الوصف |
|--------|-------|
| `tr_bank_accounts` | الحسابات البنكية والصناديق |
| `tr_cash_transactions` | الحركات النقدية |

### مشترك
| الجدول | الوصف |
|--------|-------|
| `num_series` | تسلسلات الترقيم التلقائي |

---

## الـ Seed Data (migration 0002)

يُدخل تلقائياً للـ tenant التجريبي (`00000000-0000-0000-0000-000000000001`):

- **85 حساب** في دليل الحسابات (COA) موزعة على 5 مجموعات
- **مستودع رئيسي** (MAIN)
- **14 prefix** لتسلسلات الترقيم (JE, INV, PO, GRN...)

---

## أوامر Alembic المفيدة

```bash
# إنشاء migration جديدة
alembic revision --autogenerate -m "اسم التغيير"

# تطبيق آخر migration فقط
alembic upgrade +1

# التراجع عن آخر migration
alembic downgrade -1

# التراجع عن كل شيء
alembic downgrade base

# عرض الـ SQL بدون تطبيق
alembic upgrade head --sql
```
