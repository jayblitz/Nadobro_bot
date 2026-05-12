"""Unit tests for the D-Grid adaptive layer sizing engine."""
from __future__ import annotations

import pytest

from src.nadobro.strategies import _layer_sizing as ls
from src.nadobro.strategies import _regime


def _baseline(**overrides):
    args = dict(
        side="buy",
        level=1,
        base_notional_usd=100.0,
        mid_price=100.0,
        inv_usd=0.0,
        net_units=0.0,
        inv_soft_usd=60.0,
        regime=_regime.REGIME_RANGE_WIDE,
        realized_vol_bp=10.0,
        min_order_notional_usd=10.0,
        max_per_level_usd=500.0,
        state={},
        config={},
    )
    args.update(overrides)
    return args


def test_flat_inventory_no_brake():
    out = ls.size_quote_level(**_baseline())
    assert out["components"]["inventory_brake"] == pytest.approx(1.0)
    assert out["size_usd"] > 0


def test_inventory_brake_reduces_same_side_only():
    # We're long, so buys should be braked, sells should NOT be braked.
    long_args = _baseline(side="buy", net_units=1.0, inv_usd=60.0)
    short_args = _baseline(side="sell", net_units=1.0, inv_usd=60.0)
    buy_out = ls.size_quote_level(**long_args)
    sell_out = ls.size_quote_level(**short_args)
    assert buy_out["components"]["inventory_brake"] < 1.0
    assert sell_out["components"]["inventory_brake"] == pytest.approx(1.0)


def test_trend_regime_reduces_counter_trend_size():
    # In trend_up: buys should size up vs sells (we lean into trend).
    buy_out = ls.size_quote_level(**_baseline(side="buy", regime=_regime.REGIME_TREND_UP))
    sell_out = ls.size_quote_level(**_baseline(side="sell", regime=_regime.REGIME_TREND_UP))
    assert buy_out["components"]["regime_mult"] > sell_out["components"]["regime_mult"]
    assert sell_out["size_usd"] < buy_out["size_usd"]


def test_chop_regime_shrinks_both_sides():
    buy_chop = ls.size_quote_level(**_baseline(side="buy", regime=_regime.REGIME_CHOP_HIGH_VOL))
    buy_range = ls.size_quote_level(**_baseline(side="buy", regime=_regime.REGIME_RANGE_WIDE))
    assert buy_chop["size_usd"] < buy_range["size_usd"]


def test_level_taper_outer_levels_smaller():
    l1 = ls.size_quote_level(**_baseline(level=1))
    l2 = ls.size_quote_level(**_baseline(level=2))
    l3 = ls.size_quote_level(**_baseline(level=3))
    assert l1["size_usd"] > l2["size_usd"] > l3["size_usd"]


def test_vol_explosion_scales_down():
    """Realized vol 5x target should drop size to ~20% of base."""
    args = _baseline(realized_vol_bp=150.0, config={"layer_target_vol_bp": 30.0})
    out = ls.size_quote_level(**args)
    assert out["components"]["vol_scaling"] < 0.25
    assert out["size_usd"] < 25.0  # 100 * 0.2 = 20


def test_clamps_to_min_notional_floor():
    # Tiny realized vol -> tiny scaling -> floor should hold.
    args = _baseline(
        regime=_regime.REGIME_CHOP_HIGH_VOL,
        realized_vol_bp=1000.0,
        min_order_notional_usd=100.0,
        config={"layer_target_vol_bp": 30.0},
    )
    out = ls.size_quote_level(**args)
    assert out["size_usd"] >= 100.0


def test_clamps_to_max_per_level_ceiling():
    args = _baseline(
        base_notional_usd=5000.0,
        regime=_regime.REGIME_RANGE_TIGHT,
        max_per_level_usd=500.0,
    )
    out = ls.size_quote_level(**args)
    assert out["size_usd"] <= 500.0


def test_telemetry_recorded():
    state: dict = {}
    out = ls.size_quote_level(**_baseline(state=state, level=2))
    rows = state.get("mm_layer_sizing_telemetry") or []
    assert rows
    assert rows[-1]["level"] == 2


def test_fill_perf_mult_negative_history_reduces():
    """A history of losing fills should shrink size below neutral."""
    state = {
        "grid_buy_fills": [
            {"price": 100, "size": 1.0, "realized_pnl": -0.5} for _ in range(20)
        ],
    }
    args = _baseline(side="buy", state=state)
    out = ls.size_quote_level(**args)
    assert out["components"]["fill_perf_mult"] < 1.0


def test_fill_perf_mult_positive_history_increases():
    state = {
        "grid_buy_fills": [
            {"price": 100, "size": 1.0, "realized_pnl": 0.5} for _ in range(20)
        ],
    }
    args = _baseline(side="buy", state=state)
    out = ls.size_quote_level(**args)
    assert out["components"]["fill_perf_mult"] > 1.0
