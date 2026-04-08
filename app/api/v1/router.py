"""
app/api/v1/router.py
══════════════════════════════════════════════════════════
API v1 router — aggregates all module routers.
══════════════════════════════════════════════════════════
"""
from fastapi import APIRouter
from app.api.v1 import health

v1_router = APIRouter()

# ── System ─────────────────────────────────────────────
v1_router.include_router(health.router, tags=["النظام"])

# ── Accounting (Module 1) ─────────────────────────────
from app.modules.accounting.router import router as accounting_router
v1_router.include_router(accounting_router)

from app.modules.accounting.je_attachments_router import router as je_att_router
v1_router.include_router(je_att_router)

from app.modules.accounting.je_activity_router import router as je_activity_router
v1_router.include_router(je_activity_router)

from app.modules.accounting.fiscal_router import router as fiscal_router
v1_router.include_router(fiscal_router)

# ── Opening Balances ──────────────────────────────────
from app.modules.accounting.opening_balances_router import router as ob_router
v1_router.include_router(ob_router)

# ── VAT / Tax Types ───────────────────────────────────
from app.modules.accounting.tax_router import router as tax_router
v1_router.include_router(tax_router)

# ── AI ────────────────────────────────────────────────
from app.api.v1.ai_narrative import router as ai_router
v1_router.include_router(ai_router)

# ── Notifications ────────────────────────────────────
from app.modules.notifications.router import router as notifications_router
v1_router.include_router(notifications_router)

# ── Settings (Branches, Cost Centers, Projects) ──────
from app.modules.settings.router import router as settings_router
v1_router.include_router(settings_router)

# ── User Management ──────────────────────────────────
from app.modules.users.router import router as users_router
v1_router.include_router(users_router)

# ── Company Settings ──────────────────────────────────
from app.modules.settings.company_router import router as company_router
v1_router.include_router(company_router)

# ── Multi Currency ────────────────────────────────────
from app.modules.settings.currency_router import router as currency_router
v1_router.include_router(currency_router)

# ── Dimensions ────────────────────────────────────────
from app.modules.dimensions.router import router as dimensions_router
v1_router.include_router(dimensions_router)

# ── Inventory (Module 2A) ──────────────────────────────
from app.modules.inventory.router import router as inventory_router
v1_router.include_router(inventory_router)

# ── Sales (Module 2B) ──────────────────────────────────
from app.modules.sales.router import router as sales_router
v1_router.include_router(sales_router)

# ── Purchases (Module 3) ───────────────────────────────
from app.modules.purchases.router import router as purchases_router
v1_router.include_router(purchases_router)

# ── HR (Module 4) ─────────────────────────────────────
from app.modules.hr.router import router as hr_router
v1_router.include_router(hr_router)

# ── Fixed Assets (Module 5) ───────────────────────────
from app.modules.assets.router import router as assets_router
v1_router.include_router(assets_router)

# ── Treasury (Module 6) ───────────────────────────────
from app.modules.treasury.router import router as treasury_router
v1_router.include_router(treasury_router)

# ── Reports (Module 7) ────────────────────────────────
from app.modules.reports.router import router as reports_router
v1_router.include_router(reports_router)
