"""
app/modules/purchases/service.py
══════════════════════════════════════════════════════════
Purchases Service — orchestrates:

PO Flow:
  create_po()  → Draft PO
  approve_po() → status = approved

GRN Flow:
  create_grn()  → Draft GRN linked to PO
  post_grn()    → InventoryService.receive_from_grn() per line
                → PostingEngine: DR Inventory / CR GRN Clearing
                → Update PO received quantities

Vendor Invoice Flow:
  create_vendor_invoice() → links to PO + GRN
  run_3way_match()        → qty & price tolerance check
  post_vendor_invoice()   → PostingEngine: DR GRN Clearing / CR AP / CR VAT

3-Way Match Rules:
  qty_invoiced  ≤ qty_received   (tolerance 0%)
  unit_price    within ±5% of PO price (configurable)
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
from app.modules.purchases.models import (
    GRN, GRNLine, GRNStatus, POLine, POStatus,
    PurchaseOrder, Supplier,
    VendorInvoice, VendorInvoiceLine, VendorInvoiceStatus,
)
from app.modules.purchases.repository import (
    GRNRepository, PurchaseOrderRepository,
    SupplierRepository, VendorInvoiceRepository,
)
from app.modules.purchases.schemas import (
    GRNCreate, POCreate, SupplierCreate,
    SupplierUpdate, VendorInvoiceCreate,
)
from app.services.numbering.series_service import NumberSeriesService
from app.services.posting.engine import PostingEngine, PostingLine, PostingRequest
from app.services.posting.templates import ACC

logger = structlog.get_logger(__name__)

PRICE_PREC  = Decimal("0.001")
MATCH_PRICE_TOLERANCE = Decimal("5")   # 5% price tolerance


def _calc_due_date(inv_date: date, term: str) -> Optional[date]:
    from datetime import timedelta
    days = {"cash": 0, "net_15": 15, "net_30": 30, "net_60": 60, "net_90": 90}
    d = days.get(term, 30)
    return inv_date + timedelta(days=d)


class PurchasesService:
    def __init__(self, db: AsyncSession, user: CurrentUser) -> None:
        self.db   = db
        self.user = user
        tid = user.tenant_id

        self._supplier_repo = SupplierRepository(db, tid)
        self._po_repo       = PurchaseOrderRepository(db, tid)
        self._grn_repo      = GRNRepository(db, tid)
        self._vi_repo       = VendorInvoiceRepository(db, tid)
        self._num_svc       = NumberSeriesService(db, tid)
        self._posting       = PostingEngine(db, tid)

    # ══════════════════════════════════════════════════════
    # Supplier CRUD
    # ══════════════════════════════════════════════════════
    async def create_supplier(self, data: SupplierCreate) -> Supplier:
        self.user.require("can_manage_suppliers")
        if await self._supplier_repo.exists(code=data.code):
            raise DuplicateError("مورد", "code", data.code)
        s = self._supplier_repo.create(**data.model_dump())
        s.created_by = self.user.email
        return await self._supplier_repo.save(s)

    async def update_supplier(self, sid: uuid.UUID, data: SupplierUpdate) -> Supplier:
        self.user.require("can_manage_suppliers")
        s = await self._supplier_repo.get_or_raise(sid)
        for k, v in data.model_dump(exclude_none=True).items():
            setattr(s, k, v)
        s.updated_by = self.user.email
        await self.db.flush()
        return s

    async def list_suppliers(self, offset: int = 0, limit: int = 50) -> Tuple[List[Supplier], int]:
        return await self._supplier_repo.list_active(offset=offset, limit=limit)

    async def get_supplier(self, sid: uuid.UUID) -> Supplier:
        return await self._supplier_repo.get_or_raise(sid)

    async def search_suppliers(self, query: str) -> List[Supplier]:
        return await self._supplier_repo.search(query)

    # ══════════════════════════════════════════════════════
    # Purchase Order
    # ══════════════════════════════════════════════════════
    async def create_po(self, data: POCreate) -> PurchaseOrder:
        self.user.require("can_create_po")
        supplier = await self._supplier_repo.get_by_code_or_raise(data.supplier_code)
        po_number = await self._num_svc.next("PO", include_month=False)

        subtotal = Decimal("0")
        lines_data = []
        for i, ld in enumerate(data.lines, 1):
            disc_amt  = (ld.qty_ordered * ld.unit_price * ld.discount_pct / 100).quantize(PRICE_PREC)
            line_tot  = (ld.qty_ordered * ld.unit_price - disc_amt).quantize(PRICE_PREC)
            vat_amt   = (line_tot * ld.vat_rate / 100).quantize(PRICE_PREC)
            subtotal += line_tot
            lines_data.append({
                "line_number":       i,
                "product_code":      ld.product_code,
                "product_name":      ld.product_name or ld.product_code,
                "qty_ordered":       ld.qty_ordered,
                "qty_pending":       ld.qty_ordered,
                "unit_price":        ld.unit_price,
                "discount_pct":      ld.discount_pct,
                "discount_amount":   disc_amt,
                "line_total":        line_tot,
                "vat_rate":          ld.vat_rate,
                "vat_amount":        vat_amt,
                "line_total_with_vat": (line_tot + vat_amt).quantize(PRICE_PREC),
                "inventory_account": ld.inventory_account,
                "notes":             ld.notes,
            })

        hdr_disc = (subtotal * data.discount_pct / 100).quantize(PRICE_PREC)
        taxable  = (subtotal - hdr_disc).quantize(PRICE_PREC)
        vat_tot  = sum(Decimal(str(l["vat_amount"])) for l in lines_data)
        total    = (taxable + vat_tot).quantize(PRICE_PREC)

        async with atomic_transaction(self.db, label=f"create_po_{po_number}"):
            po = PurchaseOrder(
                tenant_id=self.user.tenant_id,
                po_number=po_number,
                po_date=data.po_date,
                required_date=data.required_date,
                status=POStatus.DRAFT,
                supplier_id=supplier.id,
                supplier_code=supplier.code,
                supplier_name=supplier.name_ar,
                warehouse_code=data.warehouse_code,
                subtotal=subtotal,
                discount_amount=hdr_disc,
                taxable_amount=taxable,
                vat_amount=vat_tot,
                total_amount=total,
                payment_term=data.payment_term,
                discount_pct=data.discount_pct,
                notes=data.notes,
                reference=data.reference,
                created_by=self.user.email,
            )
            self.db.add(po)
            await self.db.flush()
            for ld in lines_data:
                self.db.add(POLine(
                    tenant_id=self.user.tenant_id,
                    po_id=po.id,
                    created_by=self.user.email,
                    **ld,
                ))
            await self.db.flush()

        logger.info("po_created", number=po_number, total=float(total))
        return await self._po_repo.get_with_lines(po.id)

    async def approve_po(self, po_id: uuid.UUID) -> PurchaseOrder:
        self.user.require("can_approve_po")
        po = await self._po_repo.get_with_lines(po_id)
        if not po:
            raise NotFoundError("أمر شراء", po_id)
        if po.status != POStatus.DRAFT:
            raise InvalidStateError("أمر الشراء", po.status, [POStatus.DRAFT])
        po.status      = POStatus.APPROVED
        po.approved_by = self.user.email
        po.approved_at = datetime.now(timezone.utc)
        po.updated_by  = self.user.email
        await self.db.flush()
        return po

    # ══════════════════════════════════════════════════════
    # GRN — Goods Receipt
    # ══════════════════════════════════════════════════════
    async def create_grn(self, data: GRNCreate) -> GRN:
        self.user.require("can_post_grn")
        po = await self._po_repo.get_with_lines(data.po_id)
        if not po:
            raise NotFoundError("أمر شراء", data.po_id)
        if po.status not in (POStatus.APPROVED, POStatus.PARTIALLY_RECEIVED):
            raise InvalidStateError(
                "أمر الشراء", po.status,
                [POStatus.APPROVED, POStatus.PARTIALLY_RECEIVED],
            )

        po_line_map = {ln.product_code: ln for ln in po.lines}
        grn_number  = await self._num_svc.next("GRN", include_month=True)

        lines_data = []
        total_cost = Decimal("0")
        for i, ld in enumerate(data.lines, 1):
            po_line = po_line_map.get(ld.product_code)
            if po_line:
                max_recv = po_line.qty_ordered - po_line.qty_received
                if ld.qty_received > max_recv + Decimal("0.001"):
                    raise ValidationError(
                        f"الكمية المستلمة للصنف {ld.product_code} "
                        f"({ld.qty_received}) تتجاوز الكمية المتبقية ({max_recv})"
                    )
            tc = (ld.qty_received * ld.unit_cost).quantize(PRICE_PREC)
            total_cost += tc
            lines_data.append({
                "line_number":  i,
                "product_code": ld.product_code,
                "product_name": po_line.product_name if po_line else ld.product_code,
                "po_line_id":   ld.po_line_id or (po_line.id if po_line else None),
                "qty_received": ld.qty_received,
                "unit_cost":    ld.unit_cost,
                "total_cost":   tc,
                "inventory_account": po_line.inventory_account if po_line else "1301",
                "notes": ld.notes,
            })

        async with atomic_transaction(self.db, label=f"create_grn_{grn_number}"):
            grn = GRN(
                tenant_id=self.user.tenant_id,
                grn_number=grn_number,
                grn_date=data.grn_date,
                status=GRNStatus.DRAFT,
                po_id=po.id,
                po_number=po.po_number,
                supplier_id=po.supplier_id,
                supplier_code=po.supplier_code,
                supplier_name=po.supplier_name,
                warehouse_code=data.warehouse_code,
                total_cost=total_cost,
                delivery_note=data.delivery_note,
                notes=data.notes,
                created_by=self.user.email,
            )
            self.db.add(grn)
            await self.db.flush()
            for ld in lines_data:
                self.db.add(GRNLine(
                    tenant_id=self.user.tenant_id,
                    grn_id=grn.id,
                    created_by=self.user.email,
                    **ld,
                ))
            await self.db.flush()

        return await self._grn_repo.get_with_lines(grn.id)

    async def post_grn(self, grn_id: uuid.UUID) -> GRN:
        self.user.require("can_post_grn")
        grn = await self._grn_repo.get_with_lines(grn_id)
        if not grn:
            raise NotFoundError("GRN", grn_id)
        if grn.status != GRNStatus.DRAFT:
            raise InvalidStateError("GRN", grn.status, [GRNStatus.DRAFT])

        po = await self._po_repo.get_with_lines(grn.po_id)
        po_line_map = {ln.product_code: ln for ln in po.lines}

        async with atomic_transaction(self.db, label=f"post_grn_{grn.grn_number}"):

            # ── Receive into inventory ─────────────────────
            from app.modules.inventory.service import InventoryService
            inv_svc = InventoryService(self.db, self.user)

            for line in grn.lines:
                movement = await inv_svc.receive_from_grn(
                    product_code=line.product_code,
                    warehouse_code=grn.warehouse_code,
                    qty=line.qty_received,
                    unit_cost=line.unit_cost,
                    movement_date=grn.grn_date,
                    grn_id=grn.id,
                    grn_number=grn.grn_number,
                )
                line.wac_before  = movement.wac_before
                line.wac_after   = movement.wac_after
                line.movement_id = movement.id

                # Update PO line quantities
                po_line = po_line_map.get(line.product_code)
                if po_line:
                    po_line.qty_received += line.qty_received
                    po_line.qty_pending   = max(
                        po_line.qty_ordered - po_line.qty_received,
                        Decimal("0"),
                    )

            await self.db.flush()

            # ── GRN Journal Entry ──────────────────────────
            # DR Inventory   (total_cost)
            # CR GRN Clearing / AP Accrual  (total_cost)
            # Note: when vendor invoice is posted, clearing → AP
            inv_acc = grn.lines[0].inventory_account if grn.lines else ACC.INVENTORY

            je_lines = [
                PostingLine(
                    account_code=inv_acc,
                    description=f"استلام بضاعة — {grn.grn_number} — {grn.supplier_name}",
                    debit=grn.total_cost,
                ),
                PostingLine(
                    account_code=ACC.AP,
                    description=f"التزام GRN — {grn.grn_number}",
                    credit=grn.total_cost,
                ),
            ]

            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="GRN",
                description=f"استلام بضاعة — {grn.supplier_name} — {grn.grn_number}",
                entry_date=grn.grn_date,
                lines=je_lines,
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="purchases",
                source_doc_type="grn",
                source_doc_number=grn.grn_number,
                idempotency_key=f"PUR:GRN:{grn.id}:{self.user.tenant_id}",
                user_role=self.user.role,
            )
            je_result = await self._posting.post(je_req)

            grn.je_id     = je_result.je_id
            grn.je_serial = je_result.je_serial
            grn.status    = GRNStatus.POSTED
            grn.posted_at = datetime.now(timezone.utc)
            grn.posted_by = self.user.email

            # Update PO status
            total_ordered  = sum(ln.qty_ordered  for ln in po.lines)
            total_received = sum(ln.qty_received for ln in po.lines)
            rcv_pct = (total_received / total_ordered * 100).quantize(
                Decimal("0.01")
            ) if total_ordered > 0 else Decimal("0")
            po.qty_received_pct = rcv_pct
            po.status = (
                POStatus.RECEIVED
                if total_received >= total_ordered
                else POStatus.PARTIALLY_RECEIVED
            )
            await self.db.flush()

        logger.info(
            "grn_posted",
            grn=grn.grn_number, po=grn.po_number,
            cost=float(grn.total_cost), je=grn.je_serial,
        )
        return grn

    # ══════════════════════════════════════════════════════
    # Vendor Invoice + 3-Way Match
    # ══════════════════════════════════════════════════════
    async def create_vendor_invoice(self, data: VendorInvoiceCreate) -> VendorInvoice:
        self.user.require("can_create_vendor_invoice")
        po = await self._po_repo.get_with_lines(data.po_id)
        if not po:
            raise NotFoundError("أمر شراء", data.po_id)

        grn = None
        if data.grn_id:
            grn = await self._grn_repo.get_with_lines(data.grn_id)
            if not grn:
                raise NotFoundError("GRN", data.grn_id)
            if grn.status != GRNStatus.POSTED:
                raise InvalidStateError("GRN", grn.status, [GRNStatus.POSTED])

        vi_number = await self._num_svc.next("VINV", include_month=False)

        subtotal = Decimal("0")
        lines_data = []
        po_line_map = {ln.product_code: ln for ln in po.lines}

        for i, ld in enumerate(data.lines, 1):
            disc_amt  = (ld.qty_invoiced * ld.unit_price * ld.discount_pct / 100).quantize(PRICE_PREC)
            line_tot  = (ld.qty_invoiced * ld.unit_price - disc_amt).quantize(PRICE_PREC)
            vat_amt   = (line_tot * ld.vat_rate / 100).quantize(PRICE_PREC)
            subtotal += line_tot

            po_line = po_line_map.get(ld.product_code)
            lines_data.append({
                "line_number":     i,
                "product_code":    ld.product_code,
                "product_name":    ld.product_name or (po_line.product_name if po_line else ld.product_code),
                "qty_ordered":     po_line.qty_ordered  if po_line else Decimal("0"),
                "qty_received":    po_line.qty_received if po_line else Decimal("0"),
                "qty_invoiced":    ld.qty_invoiced,
                "unit_price":      ld.unit_price,
                "discount_pct":    ld.discount_pct,
                "discount_amount": disc_amt,
                "line_total":      line_tot,
                "vat_rate":        ld.vat_rate,
                "vat_amount":      vat_amt,
                "line_total_with_vat": (line_tot + vat_amt).quantize(PRICE_PREC),
                "po_line_id":      ld.po_line_id or (po_line.id if po_line else None),
                "inventory_account": po_line.inventory_account if po_line else "1301",
                "notes": ld.notes,
            })

        vat_tot = sum(Decimal(str(l["vat_amount"])) for l in lines_data)
        total   = (subtotal + vat_tot).quantize(PRICE_PREC)
        due_date = _calc_due_date(data.invoice_date, data.payment_term)

        async with atomic_transaction(self.db, label=f"create_vi_{vi_number}"):
            vi = VendorInvoice(
                tenant_id=self.user.tenant_id,
                vi_number=vi_number,
                vendor_ref=data.vendor_ref,
                invoice_date=data.invoice_date,
                due_date=due_date,
                status=VendorInvoiceStatus.DRAFT,
                po_id=po.id,
                po_number=po.po_number,
                grn_id=grn.id if grn else None,
                grn_number=grn.grn_number if grn else None,
                supplier_id=po.supplier_id,
                supplier_code=po.supplier_code,
                supplier_name=po.supplier_name,
                subtotal=subtotal,
                taxable_amount=subtotal,
                vat_amount=vat_tot,
                total_amount=total,
                balance_due=total,
                payment_term=data.payment_term,
                ap_account=data.ap_account,
                notes=data.notes,
                created_by=self.user.email,
            )
            self.db.add(vi)
            await self.db.flush()
            for ld in lines_data:
                self.db.add(VendorInvoiceLine(
                    tenant_id=self.user.tenant_id,
                    vi_id=vi.id,
                    created_by=self.user.email,
                    **ld,
                ))
            await self.db.flush()

        return await self._vi_repo.get_with_lines(vi.id)

    async def run_3way_match(self, vi_id: uuid.UUID) -> dict:
        """
        3-Way Match: PO ↔ GRN ↔ Vendor Invoice.
        Rules:
          1. qty_invoiced  ≤ qty_received
          2. unit_price within ±5% of PO price
        Returns match result dict.
        """
        vi = await self._vi_repo.get_with_lines(vi_id)
        if not vi:
            raise NotFoundError("فاتورة مورد", vi_id)

        po = await self._po_repo.get_with_lines(vi.po_id)
        po_line_map = {ln.product_code: ln for ln in po.lines}

        issues = []
        all_ok = True

        for line in vi.lines:
            po_line = po_line_map.get(line.product_code)
            if not po_line:
                issues.append(f"{line.product_code}: غير موجود في أمر الشراء")
                all_ok = False
                line.match_ok = False
                line.match_notes = "غير موجود في PO"
                continue

            # Rule 1: qty check
            if line.qty_invoiced > po_line.qty_received + Decimal("0.001"):
                msg = (
                    f"{line.product_code}: الكمية المفوترة ({line.qty_invoiced}) "
                    f"تتجاوز الكمية المستلمة ({po_line.qty_received})"
                )
                issues.append(msg)
                all_ok = False
                line.match_ok = False
                line.match_notes = msg
                continue

            # Rule 2: price check ±5%
            if po_line.unit_price > 0:
                price_diff_pct = abs(
                    (line.unit_price - po_line.unit_price) / po_line.unit_price * 100
                )
                if price_diff_pct > MATCH_PRICE_TOLERANCE:
                    msg = (
                        f"{line.product_code}: فرق السعر {float(price_diff_pct):.1f}% "
                        f"يتجاوز الحد المسموح (5%)"
                    )
                    issues.append(msg)
                    all_ok = False
                    line.match_ok = False
                    line.match_notes = msg

        vi.match_status = "matched" if all_ok else "failed"
        vi.match_notes  = "\n".join(issues) if issues else "المطابقة ناجحة"
        if all_ok:
            vi.status = VendorInvoiceStatus.MATCHED
        await self.db.flush()

        return {
            "passed":  all_ok,
            "issues":  issues,
            "vi_number": vi.vi_number,
            "match_status": vi.match_status,
        }

    async def post_vendor_invoice(self, vi_id: uuid.UUID) -> VendorInvoice:
        """
        Posts vendor invoice after 3-way match.
        JE:
          DR Inventory (cost)      ← adjust if price differs from GRN
          DR VAT Input             ← 15% recoverable
          CR Accounts Payable (AP) ← total with VAT
        """
        self.user.require("can_post_vendor_invoice")
        vi = await self._vi_repo.get_with_lines(vi_id)
        if not vi:
            raise NotFoundError("فاتورة مورد", vi_id)
        if vi.status not in (
            VendorInvoiceStatus.DRAFT, VendorInvoiceStatus.MATCHED
        ):
            raise InvalidStateError(
                "فاتورة المورد", vi.status,
                [VendorInvoiceStatus.DRAFT, VendorInvoiceStatus.MATCHED],
            )

        # Auto-run match if not done
        if vi.match_status is None:
            match = await self.run_3way_match(vi_id)
            if not match["passed"]:
                raise ValidationError(
                    f"فشلت المطابقة الثلاثية — {vi.match_notes}"
                )

        async with atomic_transaction(self.db, label=f"post_vi_{vi.vi_number}"):

            # Group by inventory account
            inv_by_acc: dict[str, Decimal] = {}
            for line in vi.lines:
                acc = line.inventory_account or ACC.INVENTORY
                inv_by_acc[acc] = inv_by_acc.get(acc, Decimal("0")) + line.line_total

            je_lines: List[PostingLine] = []

            # DR Inventory per account
            for acc, amount in inv_by_acc.items():
                je_lines.append(PostingLine(
                    account_code=acc,
                    description=f"مشتريات — {vi.vi_number} — {vi.supplier_name}",
                    debit=amount,
                ))

            # DR VAT Input (recoverable)
            if vi.vat_amount > 0:
                je_lines.append(PostingLine(
                    account_code=ACC.VAT_REC,
                    description=f"ضريبة مدخلات — {vi.vi_number}",
                    debit=vi.vat_amount,
                ))

            # CR AP
            je_lines.append(PostingLine(
                account_code=vi.ap_account,
                description=f"ذمم مورد — {vi.supplier_name} — {vi.vi_number}",
                credit=vi.total_amount,
            ))

            je_req = PostingRequest(
                tenant_id=self.user.tenant_id,
                je_type="PJE",
                description=f"فاتورة مورد — {vi.supplier_name} — {vi.vi_number}",
                entry_date=vi.invoice_date,
                lines=je_lines,
                created_by_id=self.user.user_id,
                created_by_email=self.user.email,
                source_module="purchases",
                source_doc_type="vendor_invoice",
                source_doc_number=vi.vi_number,
                idempotency_key=f"PUR:VI:{vi.id}:{self.user.tenant_id}",
                user_role=self.user.role,
            )
            je_result = await self._posting.post(je_req)

            vi.je_id     = je_result.je_id
            vi.je_serial = je_result.je_serial
            vi.status    = VendorInvoiceStatus.POSTED
            vi.posted_at = datetime.now(timezone.utc)
            vi.posted_by = self.user.email

            # Update PO invoiced %
            po = await self._po_repo.get_with_lines(vi.po_id)
            if po:
                po_line_map = {ln.product_code: ln for ln in po.lines}
                for line in vi.lines:
                    po_line = po_line_map.get(line.product_code)
                    if po_line:
                        po_line.qty_invoiced += line.qty_invoiced
                total_ordered  = sum(ln.qty_ordered  for ln in po.lines)
                total_invoiced = sum(ln.qty_invoiced for ln in po.lines)
                inv_pct = (total_invoiced / total_ordered * 100).quantize(
                    Decimal("0.01")
                ) if total_ordered > 0 else Decimal("0")
                po.qty_invoiced_pct = inv_pct
                if total_invoiced >= total_ordered:
                    po.status = POStatus.INVOICED

            await self.db.flush()

        logger.info(
            "vendor_invoice_posted",
            vi=vi.vi_number, supplier=vi.supplier_name,
            total=float(vi.total_amount), je=vi.je_serial,
        )
        return vi

    # ══════════════════════════════════════════════════════
    # Dashboard
    # ══════════════════════════════════════════════════════
    async def get_dashboard(self) -> dict:
        ap = await self._vi_repo.get_outstanding_ap()
        po_items, po_total = await self._po_repo.list_pos(
            status="approved", limit=1
        )
        return {
            "outstanding_ap":   float(ap),
            "open_pos":         po_total,
        }
