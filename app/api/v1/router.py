"""
app/api/v1/router.py
══════════════════════════════════════════════════════════
API v1 router — aggregates all module routers.
حساباتي ERP v2.0
══════════════════════════════════════════════════════════
"""
from fastapi import APIRouter
from app.api.v1 import health

v1_router = APIRouter()

# ── System ─────────────────────────────────────────────
v1_router.include_router(health.router, tags=["النظام"])

# ══════════════════════════════════════════════════════
# MODULE 1 — المحاسبة (Accounting)
# ══════════════════════════════════════════════════════
from app.modules.accounting.router import router as accounting_router
v1_router.include_router(accounting_router)

from app.modules.accounting.je_attachments_router import router as je_att_router
v1_router.include_router(je_att_router)

from app.modules.accounting.je_activity_router import router as je_activity_router
v1_router.include_router(je_activity_router)

from app.modules.accounting.fiscal_router import router as fiscal_router
v1_router.include_router(fiscal_router)

from app.modules.accounting.opening_balances_router import router as ob_router
v1_router.include_router(ob_router)

from app.modules.accounting.tax_router import router as tax_router
v1_router.include_router(tax_router)

# ── AI ────────────────────────────────────────────────
from app.api.v1.ai_narrative import router as ai_router
v1_router.include_router(ai_router)

# ── Notifications ─────────────────────────────────────
from app.modules.notifications.router import router as notifications_router
v1_router.include_router(notifications_router)

# ══════════════════════════════════════════════════════
# SETTINGS — الإعدادات
# ══════════════════════════════════════════════════════
from app.modules.settings.router import router as settings_router
v1_router.include_router(settings_router)

from app.modules.settings.company_router import router as company_router
v1_router.include_router(company_router)

from app.modules.settings.currency_router import router as currency_router
v1_router.include_router(currency_router)

from app.modules.settings.series_router import router as series_router
v1_router.include_router(series_router)

# ── Dimensions ────────────────────────────────────────
from app.modules.dimensions.router import router as dimensions_router
v1_router.include_router(dimensions_router)

# ── User Management ───────────────────────────────────
from app.modules.users.router import router as users_router
v1_router.include_router(users_router)

# ── Audit Trail ───────────────────────────────────────
from app.modules.audit.router import router as audit_router
v1_router.include_router(audit_router)

# ══════════════════════════════════════════════════════
# MODULE 2 — المخزون (Inventory)
# ══════════════════════════════════════════════════════
from app.modules.inventory.router import router as inventory_router
v1_router.include_router(inventory_router)

# ══════════════════════════════════════════════════════
# MODULE 3 — المبيعات والذمم المدينة (AR & Sales)
# ملاحظة: هذا يستبدل app.modules.sales.router القديم
# PREFIX: /ar
# ══════════════════════════════════════════════════════
from app.modules.ar.router import router as ar_router
v1_router.include_router(ar_router)

# ══════════════════════════════════════════════════════
# MODULE 4 — المشتريات والذمم الدائنة (AP & Procurement)
# ملاحظة: هذا يستبدل app.modules.purchases.router القديم
# PREFIX: /ap
# ══════════════════════════════════════════════════════
from app.modules.ap.router import router as ap_router
v1_router.include_router(ap_router)

# ══════════════════════════════════════════════════════
# MODULE 5 — الخزينة والبنوك (Treasury)
# PREFIX: /treasury
# ══════════════════════════════════════════════════════
from app.modules.treasury.router import router as treasury_router
v1_router.include_router(treasury_router)

# ══════════════════════════════════════════════════════
# MODULE 6 — الموارد البشرية (HR) — placeholder
# ══════════════════════════════════════════════════════
from app.modules.hr.router import router as hr_router
v1_router.include_router(hr_router)

# ══════════════════════════════════════════════════════
# MODULE 7 — الأصول الثابتة (Fixed Assets) — placeholder
# ══════════════════════════════════════════════════════
from app.modules.assets.router import router as assets_router
v1_router.include_router(assets_router)

# ══════════════════════════════════════════════════════
# MODULE 8 — التقارير (Reports)
# ══════════════════════════════════════════════════════
from app.modules.reports.router import router as reports_router
v1_router.include_router(reports_router)
