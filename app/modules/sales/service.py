"""
app/modules/sales/service.py
══════════════════════════════════════════════════════════
Sales Service — orchestrates:
  Customer CRUD
  Invoice creation + posting
  Return creation + posting

Invoice Posting Flow:
  1. Validate draft invoice
  2. For each stockable line:
       → InventoryService.issue_for_sale() → COGS JE + StockMovement
  3. PostingEngine → Revenue JE:
       DR  ذمم عملاء (AR)     total_with_vat
         CR  إيرادات مبيعات     taxable_amount
         CR  VAT مستحقة        vat_amount

Return Posting Flow:
  1. Validate quantities ≤ original invoice lines
  2. PostingEngine → Reversal JE:
       DR  إيرادات مبيعات      subtotal
       DR  VAT مستحقة          vat_amount
         CR  ذمم عملاء (AR)    total_with_vat
  3. InventoryService → SALES_RETURN movement
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Tuple

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy import select

from app.core.exceptions import (
    DuplicateError, InvalidStateError,
    NotFoundError, ValidationError,
)
from app.core.tenant import CurrentUser
from app.db.transactions import atomic_transaction
from app.modules.sales.models import (
    Customer, InvoiceStatus, ReturnStatus,
    SalesInvoice, SalesInvoiceLine,
    SalesReturn, SalesReturnLine,
)
from app.modules.sales.repository import (
    CustomerRepository, SalesInvoiceRepository, SalesReturnRepository,
)
from app.modules.sales.schemas import (
    CustomerCreate, CustomerUpdate,
    SalesInvoiceCreate, SalesReturnCreate,
)
from app.services.numbering.series_service import NumberSeriesService
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import ACC

logger = structlog.get_logger(__name__)

PRICE_PRECISION = Decimal("0.001")
VAT_RATE_DEFAULT = Decimal("15")


def _calc_due_date(invoice_date: date, payment_term: str) -> Optional[date]:
    from datetime import timedelta
    days_map = {
        "cash": 0, "net_15": 15, "net_30": 30,
        "net_60": 60, "net_90": 90,
    }
    days = days_map.get(payment_term, 30)
    if days == 0:
        return invoice_date
    from datetime import timedelta
    return invoice_date + timedelta(days=days)


class SalesService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db = db
        self.user = user
        tid = user.tenant_id

        self._customer_repo = CustomerRepository(db, tid)
        self._invoice_repo  = SalesInvoiceRepository(db, tid)
        self._return_repo   = SalesReturnRepository(db, tid)
        self._num_svc       = NumberSeriesService(db, tid)
        self._posting       = PostingEngine(db, tid)

    # ══════════════════════════════════════════════════════
    # Customer CRUD
    # ══════════════════════════════════════════════════════
    async def create_customer(self, data: CustomerCreate) -> Customer:
        self.user.require("can_manage_customers")
        if await self._customer_repo.exists(code=data.code):
            raise DuplicateError("عميل", "code", data.code)
        c = self._customer_repo.create(**data.model_dump())
        c.created_by = self.user.email
        return await self._customer_repo.save(c)

    async def update_customer(
        self, cid: uuid.UUID, data: CustomerUpdate
    ) -> Customer:
        self.user.require("can_manage_customers")
        c = await self._customer_repo.get_or_raise(cid)
        for k, v in data.model_dump(exclude_none=True).items():
            setattr(c, k, v)
        c.updated_by = self.user.email
        await self.db.flush()
        return c

    async def list_customers(
        self, offset: int = 0, limit: int = 50
    ) -> Tuple[List[Customer], int]:
        return await self._customer_repo.list_active(offset=offset, limit=limit)

    async def get_customer(self, cid: uuid.UUID) -> Customer:
        return await self._customer_repo.get_or_raise(cid)

    async def search_customers(self, query: str) -> List[Customer]:
        return await self._customer_repo.search(query)

    # ══════════════════════════════════════════════════════
    # Invoice — Create (Draft)
    # ══════════════════════════════════════════════════════
    async def create_invoice(self, data: SalesInvoiceCreate) -> SalesInvoice:
        self.user.require("can_create_invoice")

        customer = await self._customer_repo.get_by_code_or_raise(data.customer_code)

        inv_number = await self._num_svc.next("INV", include_month=False)

        # ── Calculate line amounts ─────────────────────────
        lines_data = []
        subtotal = Decimal("0")

        for i, ld in enumerate(data.lines, 1):
            discount_amt = (ld.qty * ld.unit_price * ld.discount_pct / 100).quantize(PRICE_PRECISION)
            line_total   = (ld.qty * ld.unit_price - discount_amt).quantize(PRICE_PRECISION)
            vat_amt      = (line_total * ld.vat_rate / 100).quantize(PRICE_PRECISION)
            line_total_vat = (line_total + vat_amt).quantize(PRICE_PRECISION)
            subtotal += line_total
            lines_data.append({
                "line_number":       i,
                "product_code":      ld.product_code,
                "product_name":      ld.product_name or ld.product_code,
                "qty":               ld.qty,
                "unit_price":        ld.unit_price,
                "discount_pct":      ld.discount_pct,
                "discount_amount":   discount_amt,
                "line_total":        line_total,
                "vat_rate":          ld.vat_rate,
                "vat_amount":        vat_amt,
                "line_total_with_vat": line_total_vat,
                "revenue_account":   ld.revenue_account,
                "notes":             ld.notes,
            })

        # Header-level discount on top of line discounts
        header_discount = (subtotal * data.discount_pct / 100).quantize(PRICE_PRECISION)
        taxable  = (subtotal - header_discount).quantize(PRICE_PRECISION)
        vat_total = sum(Decimal(str(ld["vat_amount"])) for ld in lines_data)
        # Recalc vat on discounted amount if header discount applied
        if data.discount_pct > 0:
            vat_total = (taxable * VAT_RATE_DEFAULT / 100).quantize(PRICE_PRECISION)
        total = (taxable + vat_total).quantize(PRICE_PRECISION)

        due_date = _calc_due_date(data.invoice_date, data.payment_term)

        async with atomic_transaction(self.db, label=f"create_invoice_{inv_number}"):
            invoice = SalesInvoice(
                tenant_id=self.user.tenant_id,
                invoice_number=inv_number,
                invoice_date=data.invoice_date,
                due_date=due_date,
                status=InvoiceStatus.DRAFT,
                customer_id=customer.id,
                customer_code=customer.code,
                customer_name=customer.name_ar,
                customer_vat=customer.vat_number,
                warehouse_code=data.warehouse_code,
                subtotal=subtotal,
                discount_amount=header_discount,
                taxable_amount=taxable,
                vat_amount=vat_total,
                total_amount=total,
                balance_due=total,
                payment_term=data.payment_term,
                discount_pct=data.discount_pct,
                ar_account=data.ar_account or customer.ar_account,
                notes=data.notes,
                reference=data.reference,
                created_by=self.user.email,
            )
            self.db.add(invoice)
            await self.db.flush()

            for ld in lines_data:
                line = SalesInvoiceLine(
                    tenant_id=self.user.tenant_id,
                    invoice_id=invoice.id,
                    created_by=self.user.email,
                    **{k: v for k, v in ld.items()},
                )
                self.db.add(line)
            await self.db.flush()

        logger.info("invoice_created", number=inv_number, total=float(total))
        return await self._invoice_repo.get_with_lines(invoice.id)

    # ══════════════════════════════════════════════════════
    # Invoice — Post
    # ══════════════════════════════════════════════════════
    async def post_invoice(self, invoice_id: uuid.UUID) -> SalesInvoice:
        self.user.require("can_post_invoice")

        invoice = await self._invoice_repo.get_with_lines(invoice_id)
        if not invoice:
            raise NotFoundError("فاتورة مبيعات", invoice_id)
        if invoice.status != InvoiceStatus.DRAFT:
            raise InvalidStateError(
                "الفاتورة", invoice.status, [InvoiceStatus.DRAFT]
            )

        async with atomic_transaction(self.db, label=f"post_invoice_{invoice.invoice_number}"):

            # ── Step 1: Issue stock + calculate COGS ──────
            total_cost = Decimal("0")

            # Import here to avoid circular imports
            from app.modules.inventory.service import InventoryService
            from app.modules.inventory.repository import ProductRepository, WarehouseRepository

            inv_svc = InventoryService(self.db, self.user)
            prod_repo = ProductRepository(self.db, self.user.tenant_id)
            wh_repo   = WarehouseRepository(self.db, self.user.tenant_id)

            warehouse = await wh_repo.get_by_code(invoice.warehouse_code)
            if not warehouse:
                # fallback to default
                warehouse = await wh_repo.get_default()

            for line in invoice.lines:
                product = await prod_repo.get_by_code(line.product_code)

                if product and product.track_stock and warehouse:
                    # Issue from inventory → creates movement + COGS JE
                    movement = await inv_svc.issue_for_sale(
                        product_code=line.product_code,
                        warehouse_code=warehouse.code,
                        qty=line.qty,
                        movement_date=invoice.invoice_date,
                        invoice_id=invoice.id,
                        invoice_number=invoice.invoice_number,
                    )
                    line.unit_cost  = movement.unit_cost
                    line.total_cost = movement.total_cost
                else:
                    # Service item — no stock movement
                    line.unit_cost  = Decimal("0")
                    line.total_cost = Decimal("0")

                total_cost += line.total_cost
                await self.db.flush()

            invoice.total_cost   = total_cost
            invoice.gross_profit = invoice.taxable_amount - total_cost
            await self.db.flush()

            # ── Step 2: Revenue JE ─────────────────────────
            # DR AR (total with VAT)
            # CR Revenue lines (taxable per line)
            # CR VAT Payable (vat total)

            je_lines: List[PostingLine] = []

            # Debit: AR
            je_lines.append(PostingLine(
                account_code=invoice.ar_account,
                description=f"ذمم عميل — {invoice.customer_name} — {invoice.invoice_number}",
                debit=invoice.total_amount,
            ))

            # Credit: Revenue per line (grouped by revenue account)
            rev_by_account: dict[str, Decimal] = {}
            for line in invoice.lines:
                acc = line.revenue_account or ACC.SALES_REV
                rev_by_account[acc] = rev_by_account.get(acc, Decimal("0")) + line.line_total

            # Apply header discount proportionally
            if invoice.discount_amount > 0:
                for acc in rev_by_account:
                    ratio = rev_by_account[acc] / invoice.subtotal if invoice.subtotal else Decimal("1")
                    rev_by_account[acc] -= (invoice.discount_amount * ratio).quantize(PRICE_PRECISION)

            for acc, amount in rev_by_account.items():
                if amount > 0:
                    je_lines.append(PostingLine(
                        account_code=acc,
                        description=f"إيرادات مبيعات — {invoice.invoice_number}",
                        credit=amount,
                    ))

            # Credit: VAT Payable
            if invoice.vat_amount > 0:
                je_lines.append(PostingLine(
                    account_code=ACC.VAT_PAY,
                    description=f"ضريبة القيمة المضافة — {invoice.invoice_number}",
                    credit=invoice.vat_amount,
                ))

            je_request = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="SJE",
                description=f"فاتورة مبيعات — {invoice.customer_name} — {invoice.invoice_number}",
                entry_date=invoice.invoice_date,
                lines=je_lines,
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="sales",
                source_doc_type="sales_invoice",
                source_doc_number=invoice.invoice_number,
                idempotency_key=f"SALES:INV:{invoice.id}:{self.user.tenant_id}",
                user_role=self.user.role,
            )

            je_result = await self._posting.post(je_request)

            # ── Step 3: Update invoice status ──────────────
            invoice.je_id     = je_result.je_id
            invoice.je_serial = je_result.je_serial
            invoice.status    = InvoiceStatus.POSTED
            invoice.posted_at = datetime.now(timezone.utc)
            invoice.posted_by = self.user.email
            await self.db.flush()

        logger.info(
            "invoice_posted",
            number=invoice.invoice_number,
            total=float(invoice.total_amount),
            cogs=float(invoice.total_cost),
            gp=float(invoice.gross_profit),
            je=invoice.je_serial,
        )
        return invoice

    # ══════════════════════════════════════════════════════
    # Sales Return — Create + Post
    # ══════════════════════════════════════════════════════
    async def create_return(self, data: SalesReturnCreate) -> SalesReturn:
        self.user.require("can_create_sales_return")

        invoice = await self._invoice_repo.get_with_lines(data.invoice_id)
        if not invoice:
            raise NotFoundError("فاتورة", data.invoice_id)
        if invoice.status not in (
            InvoiceStatus.POSTED, InvoiceStatus.PARTIALLY_RETURNED
        ):
            raise InvalidStateError(
                "الفاتورة", invoice.status,
                [InvoiceStatus.POSTED, InvoiceStatus.PARTIALLY_RETURNED]
            )

        # Build product → invoice line map for qty validation
        line_map = {ln.product_code: ln for ln in invoice.lines}

        ret_number = await self._num_svc.next("RTRN", include_month=False)

        async with atomic_transaction(self.db, label=f"create_return_{ret_number}"):
            subtotal = Decimal("0")
            vat_total = Decimal("0")
            lines_data = []

            for i, rl in enumerate(data.lines, 1):
                orig_line = line_map.get(rl.product_code)
                if orig_line:
                    max_ret = orig_line.qty - orig_line.qty_returned
                    if rl.qty > max_ret:
                        raise ValidationError(
                            f"الكمية المرتجعة للصنف {rl.product_code} "
                            f"({rl.qty}) تتجاوز الكمية المتاحة للإرجاع ({max_ret})"
                        )

                line_total = (rl.qty * rl.unit_price).quantize(PRICE_PRECISION)
                vat_amt    = (line_total * rl.vat_rate / 100).quantize(PRICE_PRECISION)
                subtotal  += line_total
                vat_total += vat_amt
                lines_data.append({
                    "line_number":       i,
                    "product_code":      rl.product_code,
                    "product_name":      orig_line.product_name if orig_line else rl.product_code,
                    "invoice_line_id":   rl.invoice_line_id or (orig_line.id if orig_line else None),
                    "qty":               rl.qty,
                    "unit_price":        rl.unit_price,
                    "vat_rate":          rl.vat_rate,
                    "line_total":        line_total,
                    "vat_amount":        vat_amt,
                    "line_total_with_vat": (line_total + vat_amt).quantize(PRICE_PRECISION),
                    "unit_cost":         orig_line.unit_cost if orig_line else Decimal("0"),
                    "total_cost":        (rl.qty * orig_line.unit_cost).quantize(PRICE_PRECISION) if orig_line else Decimal("0"),
                })

            total = (subtotal + vat_total).quantize(PRICE_PRECISION)

            ret = SalesReturn(
                tenant_id=self.user.tenant_id,
                return_number=ret_number,
                return_date=data.return_date,
                status=ReturnStatus.DRAFT,
                invoice_id=invoice.id,
                invoice_number=invoice.invoice_number,
                customer_id=invoice.customer_id,
                customer_code=invoice.customer_code,
                customer_name=invoice.customer_name,
                warehouse_code=data.warehouse_code,
                subtotal=subtotal,
                vat_amount=vat_total,
                total_amount=total,
                total_cost=sum(Decimal(str(l["total_cost"])) for l in lines_data),
                reason=data.reason,
                notes=data.notes,
                created_by=self.user.email,
            )
            self.db.add(ret)
            await self.db.flush()

            for ld in lines_data:
                rl = SalesReturnLine(
                    tenant_id=self.user.tenant_id,
                    return_id=ret.id,
                    created_by=self.user.email,
                    **ld,
                )
                self.db.add(rl)
            await self.db.flush()

        return await self._return_repo.get_with_lines(ret.id)

    async def post_return(self, return_id: uuid.UUID) -> SalesReturn:
        self.user.require("can_post_sales_return")

        ret = await self._return_repo.get_with_lines(return_id)
        if not ret:
            raise NotFoundError("مرتجع مبيعات", return_id)
        if ret.status != ReturnStatus.DRAFT:
            raise InvalidStateError("المرتجع", ret.status, [ReturnStatus.DRAFT])

        invoice = await self._invoice_repo.get_with_lines(ret.invoice_id)
        if not invoice:
            raise NotFoundError("الفاتورة الأصلية", ret.invoice_id)

        line_map = {ln.product_code: ln for ln in invoice.lines}

        async with atomic_transaction(self.db, label=f"post_return_{ret.return_number}"):

            # ── Step 1: Return stock ───────────────────────
            from app.modules.inventory.service import InventoryService
            from app.modules.inventory.models import MovementType
            inv_svc = InventoryService(self.db, self.user)

            for rl in ret.lines:
                orig = line_map.get(rl.product_code)
                if orig and orig.unit_cost > 0:
                    await inv_svc._post_movement(
                        product=await inv_svc._product_repo.get_by_code_or_raise(rl.product_code),
                        warehouse=await inv_svc._wh_repo.get_by_code(ret.warehouse_code)
                                  or await inv_svc._wh_repo.get_default(),
                        movement_type=MovementType.SALES_RETURN,
                        qty=rl.qty,
                        unit_cost=rl.unit_cost,
                        movement_date=ret.return_date,
                        source_module="sales",
                        source_doc_type="sales_return",
                        source_doc_number=ret.return_number,
                        create_je=False,   # JE handled below
                    )
                # Update returned qty on invoice line
                if orig:
                    orig.qty_returned += rl.qty
                    await self.db.flush()

            # ── Step 2: Reversal JE ────────────────────────
            # DR Revenue  (subtotal)
            # DR VAT Payable (vat_amount)
            # CR AR (total_amount)
            je_lines = [
                PostingLine(
                    account_code=invoice.ar_account,
                    description=f"مرتجع مبيعات — {ret.return_number}",
                    credit=ret.total_amount,
                ),
                PostingLine(
                    account_code=ACC.SALES_REV,
                    description=f"إلغاء إيرادات — {ret.return_number}",
                    debit=ret.subtotal,
                ),
            ]
            if ret.vat_amount > 0:
                je_lines.append(PostingLine(
                    account_code=ACC.VAT_PAY,
                    description=f"إلغاء VAT — {ret.return_number}",
                    debit=ret.vat_amount,
                ))
            if ret.total_cost > 0:
                je_lines.extend([
                    PostingLine(
                        account_code=ACC.INVENTORY,
                        description=f"إرجاع بضاعة للمخزون — {ret.return_number}",
                        debit=ret.total_cost,
                    ),
                    PostingLine(
                        account_code=ACC.COGS,
                        description=f"إلغاء تكلفة — {ret.return_number}",
                        credit=ret.total_cost,
                    ),
                ])

            je_request = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="SJE",
                description=f"مرتجع مبيعات — {ret.customer_name} — {ret.return_number}",
                entry_date=ret.return_date,
                lines=je_lines,
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="sales",
                source_doc_type="sales_return",
                source_doc_number=ret.return_number,
                idempotency_key=f"SALES:RTRN:{ret.id}:{self.user.tenant_id}",
                user_role=self.user.role,
            )
            je_result = await self._posting.post(je_request)

            # ── Step 3: Update return + invoice status ─────
            ret.je_id     = je_result.je_id
            ret.je_serial = je_result.je_serial
            ret.status    = ReturnStatus.POSTED
            ret.posted_at = datetime.now(timezone.utc)
            ret.posted_by = self.user.email

            invoice.returned_amount += ret.total_amount
            invoice.balance_due     -= ret.total_amount
            total_returned = sum(
                ln.qty_returned for ln in invoice.lines
            )
            total_invoiced = sum(ln.qty for ln in invoice.lines)
            invoice.status = (
                InvoiceStatus.RETURNED
                if total_returned >= total_invoiced
                else InvoiceStatus.PARTIALLY_RETURNED
            )
            await self.db.flush()

        logger.info(
            "return_posted",
            number=ret.return_number,
            invoice=ret.invoice_number,
            total=float(ret.total_amount),
            je=ret.je_serial,
        )
        return ret

    # ══════════════════════════════════════════════════════
    # Queries
    # ══════════════════════════════════════════════════════
    async def get_invoice(self, inv_id: uuid.UUID) -> SalesInvoice:
        inv = await self._invoice_repo.get_with_lines(inv_id)
        if not inv:
            raise NotFoundError("فاتورة", inv_id)
        return inv

    async def list_invoices(
        self,
        customer_id: Optional[uuid.UUID] = None,
        status: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        offset: int = 0,
        limit: int = 20,
    ) -> Tuple[List[SalesInvoice], int]:
        return await self._invoice_repo.list_invoices(
            customer_id=customer_id,
            status=status,
            date_from=date_from,
            date_to=date_to,
            offset=offset,
            limit=limit,
        )

    async def get_dashboard(
        self, date_from: date, date_to: date
    ) -> dict:
        summary = await self._invoice_repo.get_revenue_summary(date_from, date_to)
        ar = await self._invoice_repo.get_outstanding_ar()
        gp = summary["gp"]
        rev = summary["revenue"]
        margin = (gp / rev * 100).quantize(Decimal("0.01")) if rev > 0 else Decimal("0")
        return {
            "period_label":    f"{date_from} — {date_to}",
            "total_invoices":  summary["count"],
            "total_revenue":   float(rev),
            "total_vat":       float(summary["vat"]),
            "total_cogs":      float(summary["cogs"]),
            "gross_profit":    float(gp),
            "gross_margin_pct": float(margin),
            "outstanding_ar":  float(ar),
        }
