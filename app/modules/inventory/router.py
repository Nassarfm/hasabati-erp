"""
app/modules/inventory/router.py
═══════════════════════════════════════════════════════════════════════════
Inventory Module v5 — Main Router (Aggregator Shell)
═══════════════════════════════════════════════════════════════════════════
يجمع كل sub-routers الإصدار الخامس + الـ legacy router للتوافق العكسي.

البنية:
  • router_legacy.py        → الـ raw SQL endpoints القديمة (بدون v5)
                              مثلاً: /inventory/items, /inventory/transactions,
                              /inventory/stock-inquiry, /inventory/dashboard,
                              /inventory/settings/accounts (legacy)
  • routers/master.py       → UOM Conversions, Brands, Reason Codes,
                              Item Attributes, Attribute Values
  • routers/warehouse.py    → Warehouses-v2, Zones, Locations, warehouse-tree
  • routers/items.py        → Items-v2 (with variants, balances, generate-variants)
  • routers/transactions.py → Transactions-v2 (THE CORE — GRN/GIN/GIT/IJ/SCRAP/...)
  • routers/counts.py       → Count-sessions-v2 (with scan + reason-driven posting)
  • routers/reports.py      → Reports-v2 (15 reports + Universal Subsidiary Ledger)
  • routers/settings.py     → Settings (Accounts, Numbering, Dashboard, Health)

كل الـ v5 endpoints لها prefix بـ "-v2" أو "settings/" أو "reports-v2/"
لتفادي الاصطدام مع legacy. الواجهة الأمامية القديمة ستستمر بالعمل بدون تغيير.
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import uuid
from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.response import ok
from app.core.tenant import CurrentUser, get_current_user
from app.db.session import get_db

# Legacy raw-SQL router (preserved for backward compatibility)
from app.modules.inventory.router_legacy import router as legacy_router

# v5 sub-routers
from app.modules.inventory.routers.master       import router as master_router
from app.modules.inventory.routers.warehouse    import router as warehouse_router
from app.modules.inventory.routers.items        import router as items_router
from app.modules.inventory.routers.transactions import router as transactions_router
from app.modules.inventory.routers.counts       import router as counts_router
from app.modules.inventory.routers.reports      import router as reports_router
from app.modules.inventory.routers.settings     import router as settings_router


# ═══════════════════════════════════════════════════════════════════════════
# Top-level aggregator (no prefix — sub-routers carry their own /inventory)
# ═══════════════════════════════════════════════════════════════════════════
router = APIRouter()

# IMPORTANT: include order matters when paths overlap.
# v5 routers are included FIRST so their static routes win over legacy
# {param} routes (e.g. /inventory/items-v2 vs /inventory/items/{id}).
router.include_router(master_router)
router.include_router(warehouse_router)
router.include_router(items_router)
router.include_router(transactions_router)
router.include_router(counts_router)
router.include_router(reports_router)
router.include_router(settings_router)

# Legacy comes last — old endpoints continue to work as-is
router.include_router(legacy_router)


# ═══════════════════════════════════════════════════════════════════════════
# v5 HEALTH PING (lightweight) + version banner
# ═══════════════════════════════════════════════════════════════════════════
@router.get("/inventory/health-v5", tags=["inventory-meta"])
async def health_v5_ping(
    db: AsyncSession = Depends(get_db),
    user: CurrentUser = Depends(get_current_user),
):
    """
    Ping سريع للتحقق من أن موديول v5 محمّل بشكل صحيح.
    للتقرير الشامل: GET /inventory/settings/health
    """
    tid = str(user.tenant_id)
    # Quick smoke test — count one v5-specific table
    r = await db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM inv_reason_codes WHERE tenant_id=:tid) AS reasons,
          (SELECT COUNT(*) FROM inv_zones        WHERE tenant_id=:tid) AS zones,
          (SELECT COUNT(*) FROM inv_brands       WHERE tenant_id=:tid) AS brands
    """), {"tid": tid})
    row = dict(r.fetchone()._mapping)

    return ok(data={
        "module": "inventory",
        "version": "v5.0",
        "status": "loaded",
        "v5_tables": row,
        "endpoints_included": [
            "master (UOM/Brands/Reasons/Attributes)",
            "warehouse (Warehouses-v2/Zones/Locations)",
            "items (Items-v2/Variants)",
            "transactions (THE CORE GRN/GIN/GIT/IJ/SCRAP/...)",
            "counts (Count-sessions-v2 with scan)",
            "reports (15 reports incl. Universal Subsidiary Ledger)",
            "settings (Accounts/Numbering/Dashboard/Health)",
            "legacy (backward-compat)",
        ],
        "checked_at": date.today().isoformat(),
    })


@router.get("/inventory/version", tags=["inventory-meta"])
async def version():
    return ok(data={
        "module": "inventory",
        "version": "v5.0",
        "features": [
            "Multi-Layer Stakeholder Architecture (party at header/line/ledger)",
            "Universal Subsidiary Ledger (party-statement endpoint)",
            "Reason-Driven Accounting (count_overage→4901, etc.)",
            "Smart Hybrid Party Resolver",
            "Dual costing: AVG + FIFO per item",
            "3-level warehouse hierarchy (Warehouse→Zone→Location)",
            "Variant management (Cartesian generation)",
            "Lot & Serial tracking",
            "Atomic JE posting (shared AsyncSession)",
            "Date-aware numbering (jl_numbering_series)",
        ],
    })
