-- ══════════════════════════════════════════════════════════
-- Migration 011: إضافة أعمدة ضريبة القيمة المضافة (VAT)
-- على جداول الخزينة
-- تاريخ: 2026-04-16
-- ══════════════════════════════════════════════════════════

-- سندات القبض والصرف النقدي
ALTER TABLE tr_cash_transactions
  ADD COLUMN IF NOT EXISTS vat_rate         NUMERIC(5,2)  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS vat_amount       NUMERIC(20,3) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS vat_account_code VARCHAR(30);

-- الحركات البنكية
ALTER TABLE tr_bank_transactions
  ADD COLUMN IF NOT EXISTS vat_rate         NUMERIC(5,2)  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS vat_amount       NUMERIC(20,3) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS vat_account_code VARCHAR(30);
