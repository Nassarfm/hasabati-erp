"""
tests/test_accounting/test_wac_engine.py
══════════════════════════════════════════════════════════
WAC Engine unit tests — pure math, no DB needed.

Covers:
  - Basic IN calculation
  - Multiple IN movements (running average)
  - OUT uses current WAC (no change)
  - Mixed IN/OUT sequence
  - Zero-cost items
  - Precision / rounding
  - Negative stock guard
  - Adjustment scenarios
══════════════════════════════════════════════════════════
"""
from decimal import Decimal
import pytest
from app.modules.inventory.costing import WACEngine
from app.core.exceptions import InsufficientStockError

wac = WACEngine()


# ══════════════════════════════════════════════════════════
# IN movements
# ══════════════════════════════════════════════════════════
def test_first_in_sets_wac():
    """First purchase — WAC = purchase price."""
    r = wac.calculate_in(
        qty_before=Decimal("0"),
        wac_before=Decimal("0"),
        qty_in=Decimal("100"),
        unit_cost_in=Decimal("10.000"),
    )
    assert r.wac_after == Decimal("10.0000")
    assert r.qty_after == Decimal("100.000")
    assert r.movement_cost == Decimal("1000.0000")


def test_second_in_blends_wac():
    """
    Example:
      100 units @ 10.00 = 1000
      50  units @ 16.00 = 800
      Total: 150 units, value = 1800
      WAC = 1800 / 150 = 12.00
    """
    r = wac.calculate_in(
        qty_before=Decimal("100"),
        wac_before=Decimal("10.000"),
        qty_in=Decimal("50"),
        unit_cost_in=Decimal("16.000"),
    )
    assert r.wac_after == Decimal("12.0000")
    assert r.qty_after == Decimal("150.000")
    assert r.total_cost_after == Decimal("1800.0000")


def test_third_in_cumulative():
    """
    Continue from 150 @ 12.00:
      Add 100 @ 15.00 = 1500
      Total: 250 units, value = 3300
      WAC = 3300 / 250 = 13.20
    """
    r = wac.calculate_in(
        qty_before=Decimal("150"),
        wac_before=Decimal("12.0000"),
        qty_in=Decimal("100"),
        unit_cost_in=Decimal("15.0000"),
    )
    assert r.wac_after == Decimal("13.2000")
    assert r.qty_after == Decimal("250.000")


# ══════════════════════════════════════════════════════════
# OUT movements
# ══════════════════════════════════════════════════════════
def test_out_uses_current_wac():
    """OUT does not change WAC."""
    r = wac.calculate_out(
        qty_before=Decimal("100"),
        wac_current=Decimal("12.5000"),
        qty_out=Decimal("30"),
    )
    assert r.wac_after == Decimal("12.5000")   # unchanged
    assert r.qty_after == Decimal("70.000")
    assert r.movement_cost == Decimal("375.0000")   # 30 × 12.5


def test_full_out_to_zero():
    """Sell entire stock — qty goes to 0, WAC preserved."""
    r = wac.calculate_out(
        qty_before=Decimal("50"),
        wac_current=Decimal("20.0000"),
        qty_out=Decimal("50"),
    )
    assert r.qty_after == Decimal("0.000")
    assert r.wac_after == Decimal("20.0000")
    assert r.movement_cost == Decimal("1000.0000")


def test_out_insufficient_stock_raises():
    """OUT > qty_before with allow_negative=False → InsufficientStockError."""
    with pytest.raises(InsufficientStockError) as exc_info:
        wac.calculate_out(
            qty_before=Decimal("20"),
            wac_current=Decimal("10"),
            qty_out=Decimal("30"),
            allow_negative=False,
        )
    assert exc_info.value.detail["required"] == 30.0
    assert exc_info.value.detail["available"] == 20.0


def test_out_negative_allowed():
    """allow_negative=True skips the stock check."""
    r = wac.calculate_out(
        qty_before=Decimal("10"),
        wac_current=Decimal("15"),
        qty_out=Decimal("25"),
        allow_negative=True,
    )
    assert r.qty_after == Decimal("-15.000")
    assert r.movement_cost == Decimal("375.0000")


# ══════════════════════════════════════════════════════════
# Mixed IN/OUT sequence (realistic scenario)
# ══════════════════════════════════════════════════════════
def test_realistic_wac_sequence():
    """
    Simulates a real business month:
      Buy  200 @ 50.00  → WAC = 50.00
      Buy  100 @ 56.00  → WAC = (200×50 + 100×56) / 300 = 52.00
      Sell 150          → WAC = 52.00 (unchanged)
      Buy   50 @ 60.00  → WAC = (150×52 + 50×60) / 200 = 54.00
      Sell 100          → WAC = 54.00 (unchanged)
    Final: 100 units @ 54.00 = 5400
    """
    # Step 1: Buy 200 @ 50
    r1 = wac.calculate_in(Decimal("0"), Decimal("0"), Decimal("200"), Decimal("50"))
    assert r1.wac_after == Decimal("50.0000")
    assert r1.qty_after == Decimal("200.000")

    # Step 2: Buy 100 @ 56
    r2 = wac.calculate_in(r1.qty_after, r1.wac_after, Decimal("100"), Decimal("56"))
    assert r2.wac_after == Decimal("52.0000")
    assert r2.qty_after == Decimal("300.000")

    # Step 3: Sell 150
    r3 = wac.calculate_out(r2.qty_after, r2.wac_after, Decimal("150"))
    assert r3.wac_after == Decimal("52.0000")
    assert r3.qty_after == Decimal("150.000")
    assert r3.movement_cost == Decimal("7800.0000")   # 150 × 52

    # Step 4: Buy 50 @ 60
    r4 = wac.calculate_in(r3.qty_after, r3.wac_after, Decimal("50"), Decimal("60"))
    assert r4.wac_after == Decimal("54.0000")
    assert r4.qty_after == Decimal("200.000")

    # Step 5: Sell 100
    r5 = wac.calculate_out(r4.qty_after, r4.wac_after, Decimal("100"))
    assert r5.qty_after == Decimal("100.000")
    assert r5.wac_after == Decimal("54.0000")

    # Final check
    final_value = r5.qty_after * r5.wac_after
    assert final_value == Decimal("5400.0000")


# ══════════════════════════════════════════════════════════
# Adjustment scenarios
# ══════════════════════════════════════════════════════════
def test_adjustment_overage():
    """Count finds MORE than system → positive difference → IN."""
    result, diff = wac.calculate_adjustment(
        qty_before=Decimal("100"),
        wac_before=Decimal("10"),
        qty_counted=Decimal("115"),
    )
    assert diff == Decimal("15.000")
    assert result.qty_after == Decimal("115.000")


def test_adjustment_shortage():
    """Count finds LESS than system → negative difference → OUT."""
    result, diff = wac.calculate_adjustment(
        qty_before=Decimal("100"),
        wac_before=Decimal("10"),
        qty_counted=Decimal("92"),
    )
    assert diff == Decimal("-8.000")
    assert result.qty_after == Decimal("92.000")


def test_adjustment_no_difference():
    """System qty matches count — no movement needed."""
    result, diff = wac.calculate_adjustment(
        qty_before=Decimal("50"),
        wac_before=Decimal("25"),
        qty_counted=Decimal("50"),
    )
    assert diff == Decimal("0.000")
    assert result.movement_cost == Decimal("0")


# ══════════════════════════════════════════════════════════
# Edge cases
# ══════════════════════════════════════════════════════════
def test_zero_cost_item():
    """Items with zero cost (samples, promotions)."""
    r = wac.calculate_in(
        qty_before=Decimal("0"),
        wac_before=Decimal("0"),
        qty_in=Decimal("50"),
        unit_cost_in=Decimal("0"),
    )
    assert r.wac_after == Decimal("0")
    assert r.movement_cost == Decimal("0")


def test_wac_precision_4_decimals():
    """WAC is always stored to 4 decimal places."""
    # 100 @ 10 + 1 @ 7 → (1000 + 7) / 101 = 9.9703...
    r = wac.calculate_in(
        qty_before=Decimal("100"),
        wac_before=Decimal("10.0000"),
        qty_in=Decimal("1"),
        unit_cost_in=Decimal("7.0000"),
    )
    # 1007 / 101 = 9.97029...
    expected = (Decimal("1007") / Decimal("101")).quantize(Decimal("0.0001"))
    assert r.wac_after == expected


def test_invalid_qty_in_raises():
    """Negative or zero qty_in should raise ValueError."""
    with pytest.raises(ValueError):
        wac.calculate_in(Decimal("100"), Decimal("10"), Decimal("0"), Decimal("15"))

    with pytest.raises(ValueError):
        wac.calculate_in(Decimal("100"), Decimal("10"), Decimal("-5"), Decimal("15"))
