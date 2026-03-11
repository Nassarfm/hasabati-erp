"""
app/modules/inventory/service.py
══════════════════════════════════════════════════════════
Inventory Service — orchestrates repositories + WAC + PostingEngine.

Core rule: every movement that changes inventory VALUE
must create a Journal Entry via PostingEngine.

Movement → Value change?  → JE required?
  PURCHASE_RECEIPT  YES       YES (DR Inventory / CR AP)
  SALES_ISSUE       YES       YES (DR COGS / CR Inventory)
  ADJUSTMENT_IN     YES       YES (DR Inventory / CR Adj Account)
  ADJUSTMENT_OUT    YES       YES (DR Adj Account / CR Inventory)
  TRANSFER_IN/OUT   NO VALUE  NO  (same tenant, just location)
  OPENING_BALANCE   YES       YES (DR Inventory / CR Opening Equity)
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import (
    DuplicateError, InsufficientStockError,
    InvalidStateError, NotFoundError, ValidationError,
)
from app.core.tenant import CurrentUser
from app.db.transactions import atomic_transaction
from app.modules.inventory.costing import WACEngine
from app.modules.inventory.models import (
    MovementStatus, MovementType,
    Product, StockAdjustment, StockAdjustmentLine,
    StockBalance, StockMovement, Warehouse,
)
from app.modules.inventory.repository import (
    ProductRepository, StockBalanceRepository,
    StockMovementRepository, WarehouseRepository,
)
from app.modules.inventory.schemas import (
    ProductCreate, ProductUpdate, StockAdjustmentCreate,
    StockMovementCreate, WarehouseCreate,
)
from app.services.numbering.series_service import NumberSeriesService
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import ACC

logger = structlog.get_logger(__name__)

# Accounts for adjustment variance
ADJ_VARIANCE_ACCOUNT = "4003"  # Other Income/Expense for inventory variances


class InventoryService:
    """
    All inventory operations go through here.
    Instantiated per-request.
    """

    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db = db
        self.user = user
        tid = user.tenant_id

        self._product_repo  = ProductRepository(db, tid)
        self._wh_repo       = WarehouseRepository(db, tid)
        self._balance_repo  = StockBalanceRepository(db, tid)
        self._movement_repo = StockMovementRepository(db, tid)
        self._num_svc       = NumberSeriesService(db, tid)

        self._posting_engine = PostingEngine(db, tid)
        self._wac = WACEngine()

    # ══════════════════════════════════════════════════════
    # Product CRUD
    # ══════════════════════════════════════════════════════
    async def create_product(self, data: ProductCreate) -> Product:
        self.user.require("can_manage_products")

        if await self._product_repo.exists(code=data.code):
            raise DuplicateError("منتج", "code", data.code)

        product = self._product_repo.create(**data.model_dump())
        product.created_by = self.user.email
        return await self._product_repo.save(product)

    async def update_product(
        self, product_id: uuid.UUID, data: ProductUpdate
    ) -> Product:
        self.user.require("can_manage_products")
        product = await self._product_repo.get_or_raise(product_id)

        update_data = data.model_dump(exclude_none=True)
        for key, val in update_data.items():
            setattr(product, key, val)
        product.updated_by = self.user.email
        await self.db.flush()
        return product

    async def get_product(self, product_id: uuid.UUID) -> Product:
        return await self._product_repo.get_or_raise(product_id)

    async def list_products(
        self,
        category: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Tuple[List[Product], int]:
        return await self._product_repo.list_active(
            category=category, offset=offset, limit=limit
        )

    async def search_products(self, query: str) -> List[Product]:
        return await self._product_repo.search(query)

    # ══════════════════════════════════════════════════════
    # Warehouse CRUD
    # ══════════════════════════════════════════════════════
    async def create_warehouse(self, data: WarehouseCreate) -> Warehouse:
        self.user.require("can_manage_warehouses")

        if await self._wh_repo.exists(code=data.code):
            raise DuplicateError("مستودع", "code", data.code)

        wh = self._wh_repo.create(**data.model_dump())
        wh.created_by = self.user.email
        return await self._wh_repo.save(wh)

    async def list_warehouses(self) -> List[Warehouse]:
        return await self._wh_repo.list_active()

    # ══════════════════════════════════════════════════════
    # Stock Queries
    # ══════════════════════════════════════════════════════
    async def get_stock_balance(
        self,
        product_code: str,
        warehouse_code: str,
    ) -> Optional[StockBalance]:
        product = await self._product_repo.get_by_code(product_code)
        if not product:
            return None
        wh = await self._wh_repo.get_by_code(warehouse_code)
        if not wh:
            return None
        return await self._balance_repo.get_balance(product.id, wh.id)

    async def get_available_qty(
        self,
        product_code: str,
        warehouse_code: str,
    ) -> Decimal:
        bal = await self.get_stock_balance(product_code, warehouse_code)
        return bal.qty_available if bal else Decimal("0")

    async def list_stock_by_warehouse(
        self, warehouse_id: uuid.UUID
    ) -> List[StockBalance]:
        return await self._balance_repo.list_by_warehouse(warehouse_id)

    async def get_total_inventory_value(self) -> Decimal:
        return await self._balance_repo.get_total_value()

    async def get_low_stock_items(self) -> List[StockBalance]:
        return await self._balance_repo.list_low_stock()

    # ══════════════════════════════════════════════════════
    # Core Movement Engine
    # ══════════════════════════════════════════════════════
    async def _post_movement(
        self,
        product: Product,
        warehouse: Warehouse,
        movement_type: MovementType,
        qty: Decimal,
        unit_cost: Decimal,
        movement_date: date,
        *,
        source_module: Optional[str] = None,
        source_doc_type: Optional[str] = None,
        source_doc_id: Optional[uuid.UUID] = None,
        source_doc_number: Optional[str] = None,
        dest_warehouse: Optional[Warehouse] = None,
        description: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        create_je: bool = True,
    ) -> StockMovement:
        """
        Core movement posting logic.
        1. Get/create stock balance
        2. Calculate WAC
        3. Update balance atomically
        4. Create movement record
        5. Create JE if value changes
        """
        # Determine direction
        is_in  = movement_type.value.endswith("_IN") or movement_type in (
            MovementType.PURCHASE_RECEIPT,
            MovementType.SALES_RETURN,
            MovementType.PRODUCTION_IN,
            MovementType.OPENING_BALANCE,
        )
        is_out = not is_in

        # Generate movement number
        prefix_map = {
            MovementType.PURCHASE_RECEIPT: "RECV",
            MovementType.SALES_ISSUE:      "ISSU",
            MovementType.ADJUSTMENT_IN:    "ADJP",
            MovementType.ADJUSTMENT_OUT:   "ADJN",
            MovementType.TRANSFER_IN:      "TRFI",
            MovementType.TRANSFER_OUT:     "TRFO",
            MovementType.OPENING_BALANCE:  "OPNB",
            MovementType.SALES_RETURN:     "SRTN",
            MovementType.PURCHASE_RETURN:  "PRTN",
        }
        prefix = prefix_map.get(movement_type, "MOVE")
        mov_number = await self._num_svc.next(prefix, include_month=True)

        async with atomic_transaction(self.db, label=f"inventory_{mov_number}"):

            # Get or create balance
            balance = await self._balance_repo.get_or_create_balance(
                product, warehouse
            )

            wac_before = balance.average_cost
            qty_before = balance.qty_on_hand

            # Calculate WAC
            if is_in:
                wac_result = self._wac.calculate_in(
                    qty_before=qty_before,
                    wac_before=wac_before,
                    qty_in=qty,
                    unit_cost_in=unit_cost,
                )
            else:
                # Check for negative stock
                allow_neg = warehouse.allow_negative_stock
                if not allow_neg and qty > qty_before:
                    raise InsufficientStockError(
                        product=product.name_ar,
                        required=float(qty),
                        available=float(qty_before),
                    )
                wac_result = self._wac.calculate_out(
                    qty_before=qty_before,
                    wac_current=wac_before,
                    qty_out=qty,
                    allow_negative=allow_neg,
                )

            # Update balance
            balance.qty_on_hand  = wac_result.qty_after
            balance.qty_available = max(
                wac_result.qty_after - balance.qty_reserved, Decimal("0")
            )
            balance.average_cost  = wac_result.wac_after
            balance.total_value   = (
                wac_result.qty_after * wac_result.wac_after
            ).quantize(Decimal("0.001"))
            balance.last_movement_date = movement_date
            balance.last_movement_type = movement_type.value
            await self.db.flush()

            # Update product average cost
            product.average_cost = wac_result.wac_after
            if is_in:
                product.last_purchase_price = unit_cost
            await self.db.flush()

            # Create movement record
            movement = StockMovement(
                tenant_id=self.user.tenant_id,
                movement_number=mov_number,
                movement_type=movement_type,
                movement_date=movement_date,
                status=MovementStatus.POSTED,
                product_id=product.id,
                product_code=product.code,
                product_name=product.name_ar,
                warehouse_id=warehouse.id,
                warehouse_code=warehouse.code,
                dest_warehouse_id=dest_warehouse.id if dest_warehouse else None,
                dest_warehouse_code=dest_warehouse.code if dest_warehouse else None,
                qty=qty,
                unit_cost=unit_cost if is_in else wac_before,
                total_cost=wac_result.movement_cost,
                wac_before=wac_before,
                wac_after=wac_result.wac_after,
                qty_before=qty_before,
                qty_after=wac_result.qty_after,
                source_module=source_module,
                source_doc_type=source_doc_type,
                source_doc_id=source_doc_id,
                source_doc_number=source_doc_number,
                posted_at=datetime.now(timezone.utc),
                posted_by=self.user.email,
                description=description,
                created_by=self.user.email,
            )
            self.db.add(movement)
            await self.db.flush()

            # Create Journal Entry for value-changing movements
            if create_je and wac_result.movement_cost > 0:
                je_request = self._build_je_for_movement(
                    movement_type=movement_type,
                    product=product,
                    movement_cost=wac_result.movement_cost,
                    movement_number=mov_number,
                    movement_date=movement_date,
                    source_doc_number=source_doc_number,
                    idempotency_key=idempotency_key or f"INV:{mov_number}:{self.user.tenant_id}",
                )
                if je_request:
                    je_result = await self._posting_engine.post(je_request)
                    movement.je_id = je_result.je_id
                    movement.je_serial = je_result.je_serial
                    await self.db.flush()

        logger.info(
            "inventory_movement_posted",
            movement=mov_number,
            type=movement_type.value,
            product=product.code,
            qty=float(qty),
            wac_before=float(wac_before),
            wac_after=float(wac_result.wac_after),
        )
        return movement

    def _build_je_for_movement(
        self,
        movement_type: MovementType,
        product: Product,
        movement_cost: Decimal,
        movement_number: str,
        movement_date: date,
        source_doc_number: Optional[str],
        idempotency_key: str,
    ) -> Optional[PostingRequest]:
        """Map movement type to the correct JE template."""
        inv_acc = product.inventory_account or ACC.INVENTORY
        cogs_acc = product.cogs_account or ACC.COGS

        je_map = {
            # PURCHASE_RECEIPT: handled by GRN posting (purchases module)
            # OPENING_BALANCE: DR Inventory / CR Retained Earnings
            MovementType.OPENING_BALANCE: (
                [PostingLine(inv_acc, f"رصيد افتتاحي — {product.code}", debit=movement_cost)],
                [PostingLine(ACC.RETAINED, f"رصيد افتتاحي — {product.code}", credit=movement_cost)],
                "ADJ",
            ),
            # SALES_ISSUE: DR COGS / CR Inventory
            MovementType.SALES_ISSUE: (
                [PostingLine(cogs_acc, f"تكلفة مبيعات — {source_doc_number}", debit=movement_cost)],
                [PostingLine(inv_acc, f"إخراج بضاعة — {source_doc_number}", credit=movement_cost)],
                "SJE",
            ),
            # ADJUSTMENT_IN: DR Inventory / CR Variance
            MovementType.ADJUSTMENT_IN: (
                [PostingLine(inv_acc, f"جرد — زيادة — {movement_number}", debit=movement_cost)],
                [PostingLine(ADJ_VARIANCE_ACCOUNT, f"فروق جرد — {movement_number}", credit=movement_cost)],
                "ADJ",
            ),
            # ADJUSTMENT_OUT: DR Variance / CR Inventory
            MovementType.ADJUSTMENT_OUT: (
                [PostingLine(ADJ_VARIANCE_ACCOUNT, f"فروق جرد — {movement_number}", debit=movement_cost)],
                [PostingLine(inv_acc, f"جرد — نقص — {movement_number}", credit=movement_cost)],
                "ADJ",
            ),
        }

        entry = je_map.get(movement_type)
        if not entry:
            return None  # No JE needed (transfers, purchase receipts handled elsewhere)

        dr_lines, cr_lines, je_type = entry
        return PostingRequest(
            tenant_id=self.user.tenant_id,
            je_type=je_type,
            description=f"حركة مخزون — {movement_type.value} — {product.code} — {movement_number}",
            entry_date=movement_date,
            lines=dr_lines + cr_lines,
            created_by_id=self.user.user_id,
            created_by_email=self.user.email,
            source_module="inventory",
            source_doc_type=movement_type.value,
            source_doc_number=movement_number,
            idempotency_key=idempotency_key,
            user_role=self.user.role,
        )

    # ══════════════════════════════════════════════════════
    # Public Movement APIs
    # ══════════════════════════════════════════════════════
    async def post_manual_movement(
        self, data: StockMovementCreate
    ) -> StockMovement:
        """Post a manual movement (opening balance, manual adjustment, transfer)."""
        self.user.require("can_post_inventory_movement")

        product = await self._product_repo.get_by_code_or_raise(data.product_code)
        warehouse = await self._wh_repo.get_by_code(data.warehouse_code)
        if not warehouse:
            raise NotFoundError("مستودع", data.warehouse_code)

        dest_wh = None
        if data.dest_warehouse_code:
            dest_wh = await self._wh_repo.get_by_code(data.dest_warehouse_code)
            if not dest_wh:
                raise NotFoundError("مستودع الوجهة", data.dest_warehouse_code)

        return await self._post_movement(
            product=product,
            warehouse=warehouse,
            movement_type=MovementType(data.movement_type),
            qty=data.qty,
            unit_cost=data.unit_cost,
            movement_date=data.movement_date,
            dest_warehouse=dest_wh,
            description=data.description,
        )

    async def receive_from_grn(
        self,
        *,
        product_code: str,
        warehouse_code: str,
        qty: Decimal,
        unit_cost: Decimal,
        movement_date: date,
        grn_id: uuid.UUID,
        grn_number: str,
    ) -> StockMovement:
        """
        Called by Purchases module when GRN is posted.
        Creates PURCHASE_RECEIPT movement.
        JE is handled by Purchases — no JE here.
        """
        product = await self._product_repo.get_by_code_or_raise(product_code)
        warehouse = await self._wh_repo.get_by_code(warehouse_code)
        if not warehouse:
            raise NotFoundError("مستودع", warehouse_code)

        return await self._post_movement(
            product=product,
            warehouse=warehouse,
            movement_type=MovementType.PURCHASE_RECEIPT,
            qty=qty,
            unit_cost=unit_cost,
            movement_date=movement_date,
            source_module="purchases",
            source_doc_type="grn",
            source_doc_id=grn_id,
            source_doc_number=grn_number,
            create_je=False,   # JE created by Purchases module (GRN posting)
        )

    async def issue_for_sale(
        self,
        *,
        product_code: str,
        warehouse_code: str,
        qty: Decimal,
        movement_date: date,
        invoice_id: uuid.UUID,
        invoice_number: str,
    ) -> StockMovement:
        """
        Called by Sales module when invoice is posted.
        Creates SALES_ISSUE movement + COGS JE.
        """
        product = await self._product_repo.get_by_code_or_raise(product_code)
        warehouse = await self._wh_repo.get_by_code(warehouse_code)
        if not warehouse:
            raise NotFoundError("مستودع", warehouse_code)

        return await self._post_movement(
            product=product,
            warehouse=warehouse,
            movement_type=MovementType.SALES_ISSUE,
            qty=qty,
            unit_cost=product.average_cost,  # WAC out
            movement_date=movement_date,
            source_module="sales",
            source_doc_type="sales_invoice",
            source_doc_id=invoice_id,
            source_doc_number=invoice_number,
            create_je=True,
        )

    # ══════════════════════════════════════════════════════
    # Stock Adjustment
    # ══════════════════════════════════════════════════════
    async def create_adjustment(
        self, data: StockAdjustmentCreate
    ) -> StockAdjustment:
        """Create a draft stock adjustment."""
        self.user.require("can_create_stock_adjustment")

        wh = await self._wh_repo.get_by_code(data.warehouse_code)
        if not wh:
            raise NotFoundError("مستودع", data.warehouse_code)

        adj_number = await self._num_svc.next("ADJ", include_month=True)

        adj = StockAdjustment(
            tenant_id=self.user.tenant_id,
            adj_number=adj_number,
            adj_date=data.adj_date,
            warehouse_id=wh.id,
            warehouse_code=wh.code,
            reason=data.reason,
            notes=data.notes,
            created_by=self.user.email,
        )
        self.db.add(adj)
        await self.db.flush()

        for line_data in data.lines:
            product = await self._product_repo.get_by_code_or_raise(line_data.product_code)
            balance = await self._balance_repo.get_balance(product.id, wh.id)
            qty_system = balance.qty_on_hand if balance else Decimal("0")
            qty_diff = line_data.qty_counted - qty_system

            line = StockAdjustmentLine(
                tenant_id=self.user.tenant_id,
                adjustment_id=adj.id,
                product_id=product.id,
                product_code=product.code,
                product_name=product.name_ar,
                qty_system=qty_system,
                qty_counted=line_data.qty_counted,
                qty_difference=qty_diff,
                unit_cost=product.average_cost,
                variance_value=abs(qty_diff) * product.average_cost,
                notes=line_data.notes,
                created_by=self.user.email,
            )
            self.db.add(line)

        await self.db.flush()
        logger.info("adjustment_created", adj_number=adj_number)
        return adj

    async def post_adjustment(self, adj_id: uuid.UUID) -> StockAdjustment:
        """Approve and post a stock adjustment — creates IN/OUT movements."""
        self.user.require("can_post_stock_adjustment")

        from sqlalchemy.orm import selectinload
        from sqlalchemy import select

        result = await self.db.execute(
            select(StockAdjustment)
            .where(StockAdjustment.tenant_id == self.user.tenant_id)
            .where(StockAdjustment.id == adj_id)
            .options(selectinload(StockAdjustment.lines))
        )
        adj = result.scalar_one_or_none()
        if not adj:
            raise NotFoundError("تسوية مخزون", adj_id)
        if adj.status not in ("draft", "approved"):
            raise InvalidStateError("تسوية المخزون", adj.status, ["draft", "approved"])

        for line in adj.lines:
            if line.qty_difference == 0:
                continue

            mv_type = (
                MovementType.ADJUSTMENT_IN
                if line.qty_difference > 0
                else MovementType.ADJUSTMENT_OUT
            )

            product = await self._product_repo.get_or_raise(line.product_id)
            wh = await self._wh_repo.get_or_raise(adj.warehouse_id)

            await self._post_movement(
                product=product,
                warehouse=wh,
                movement_type=mv_type,
                qty=abs(line.qty_difference),
                unit_cost=line.unit_cost,
                movement_date=adj.adj_date,
                source_module="inventory",
                source_doc_type="adjustment",
                source_doc_number=adj.adj_number,
                create_je=True,
            )

        adj.status = "posted"
        adj.posted_at = datetime.now(timezone.utc)
        adj.approved_by = self.user.email
        await self.db.flush()
        return adj
