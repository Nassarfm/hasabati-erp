"""
app/modules/inventory/costing.py
══════════════════════════════════════════════════════════
Weighted Average Cost (WAC) Engine.

WAC Formula (on every IN movement):
  new_WAC = (qty_before × wac_before + qty_in × unit_cost)
            ÷ (qty_before + qty_in)

OUT movements use current WAC (no new calculation needed).
ADJUSTMENT: recalculate if quantity changes.

This is the standard costing method used in Saudi Arabia
and required by ZATCA for inventory valuation.

All calculations use Decimal for precision.
══════════════════════════════════════════════════════════
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

PRECISION = Decimal("0.0001")   # 4 decimal places for unit cost
QTY_PRECISION = Decimal("0.001")  # 3 decimal places for quantity


@dataclass
class WACResult:
    """Result of a WAC calculation."""
    wac_before: Decimal
    wac_after: Decimal
    qty_before: Decimal
    qty_after: Decimal
    total_cost_before: Decimal
    total_cost_after: Decimal
    movement_cost: Decimal   # total cost of this specific movement


class WACEngine:
    """
    Weighted Average Cost calculator.
    Pure functions — no DB access, fully testable.
    """

    @staticmethod
    def calculate_in(
        qty_before: Decimal,
        wac_before: Decimal,
        qty_in: Decimal,
        unit_cost_in: Decimal,
    ) -> WACResult:
        """
        Calculate new WAC after an IN movement (purchase, return, opening).

        new_WAC = (qty_before × wac_before + qty_in × unit_cost_in)
                  ÷ (qty_before + qty_in)
        """
        qty_before  = Decimal(str(qty_before))
        wac_before  = Decimal(str(wac_before))
        qty_in      = Decimal(str(qty_in))
        unit_cost_in = Decimal(str(unit_cost_in))

        if qty_in <= 0:
            raise ValueError(f"qty_in must be positive, got {qty_in}")
        if unit_cost_in < 0:
            raise ValueError(f"unit_cost_in cannot be negative, got {unit_cost_in}")

        movement_cost     = (qty_in * unit_cost_in).quantize(PRECISION)
        total_cost_before = (qty_before * wac_before).quantize(PRECISION)
        qty_after         = (qty_before + qty_in).quantize(QTY_PRECISION)
        total_cost_after  = (total_cost_before + movement_cost).quantize(PRECISION)

        if qty_after == 0:
            wac_after = unit_cost_in
        else:
            wac_after = (total_cost_after / qty_after).quantize(
                PRECISION, rounding=ROUND_HALF_UP
            )

        return WACResult(
            wac_before=wac_before,
            wac_after=wac_after,
            qty_before=qty_before,
            qty_after=qty_after,
            total_cost_before=total_cost_before,
            total_cost_after=total_cost_after,
            movement_cost=movement_cost,
        )

    @staticmethod
    def calculate_out(
        qty_before: Decimal,
        wac_current: Decimal,
        qty_out: Decimal,
        allow_negative: bool = False,
    ) -> WACResult:
        """
        Calculate cost of an OUT movement (sale, issue, transfer).
        WAC does NOT change on OUT — we just use the current WAC.
        Quantity decreases by qty_out.
        """
        qty_before  = Decimal(str(qty_before))
        wac_current = Decimal(str(wac_current))
        qty_out     = Decimal(str(qty_out))

        if qty_out <= 0:
            raise ValueError(f"qty_out must be positive, got {qty_out}")

        qty_after = (qty_before - qty_out).quantize(QTY_PRECISION)

        if not allow_negative and qty_after < 0:
            from app.core.exceptions import InsufficientStockError
            raise InsufficientStockError(
                product="",   # caller fills product name
                required=float(qty_out),
                available=float(qty_before),
            )

        movement_cost    = (qty_out * wac_current).quantize(PRECISION)
        total_cost_after = (qty_after * wac_current).quantize(PRECISION)

        return WACResult(
            wac_before=wac_current,
            wac_after=wac_current,      # WAC unchanged on OUT
            qty_before=qty_before,
            qty_after=qty_after,
            total_cost_before=(qty_before * wac_current).quantize(PRECISION),
            total_cost_after=total_cost_after,
            movement_cost=movement_cost,
        )

    @staticmethod
    def calculate_adjustment(
        qty_before: Decimal,
        wac_before: Decimal,
        qty_counted: Decimal,
        unit_cost: Optional[Decimal] = None,
    ) -> tuple[WACResult, Decimal]:
        """
        Calculate adjustment (stock count variance).
        Returns (WACResult, qty_difference).
        qty_difference > 0 = overage (IN), < 0 = shortage (OUT).
        """
        from typing import Optional as Opt
        qty_before  = Decimal(str(qty_before))
        wac_before  = Decimal(str(wac_before))
        qty_counted = Decimal(str(qty_counted))
        cost = Decimal(str(unit_cost)) if unit_cost else wac_before

        qty_diff = (qty_counted - qty_before).quantize(QTY_PRECISION)

        if qty_diff > 0:
            # Overage → IN movement
            result = WACEngine.calculate_in(qty_before, wac_before, qty_diff, cost)
        elif qty_diff < 0:
            # Shortage → OUT movement
            result = WACEngine.calculate_out(
                qty_before, wac_before, abs(qty_diff), allow_negative=False
            )
        else:
            # No difference
            result = WACResult(
                wac_before=wac_before,
                wac_after=wac_before,
                qty_before=qty_before,
                qty_after=qty_before,
                total_cost_before=(qty_before * wac_before).quantize(PRECISION),
                total_cost_after=(qty_before * wac_before).quantize(PRECISION),
                movement_cost=Decimal("0"),
            )

        return result, qty_diff


# Module-level singleton
wac_engine = WACEngine()
