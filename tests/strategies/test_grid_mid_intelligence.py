"""Tests for Grid and Mid Mode intelligence extensions.

Covers:
  - Grid uses the same PM defaults as D-Grid (the conservative profile).
  - Mid Mode uses tighter partial_tp_bp (smaller wins, smaller cuts).
  - Mid Mode's bias_value layers multiplicatively on top of the regime mult.
  - Mid Mode regime table is symmetric on the regime axis (bias does the lean).
"""
from __future__ import annotations

import pytest

from src.nadobro.strategies import _layer_sizing as ls
from src.nadobro.strategies import _position_manager as pm
from src.nadobro.strategies import _regime


# ---------------------------------------------------------------------------
# Per-strategy PM defaults
# ---------------------------------------------------------------------------

def test_grid_pm_defaults_match_dgrid():
    """Grid (classic) is conservative — uses the same defaults as D-Grid."""
    grid_cfg = pm._pm_config({"strategy": "grid"})
    dgrid_cfg = pm._pm_config({"strategy": "dgrid"})
    for key in ("partial_tp_bp", "cut_confidence", "trail_give_back_fraction",
                "stale_hold_seconds", "cooldown_seconds"):
        assert grid_cfg[key] == dgrid_cfg[key], f"key {key} differs"


def test_mid_pm_has_tighter_partial_tp():
    """Mid Mode quotes tighter spreads → lower partial_tp_bp threshold."""
    mid_cfg = pm._pm_config({"strategy": "mid"})
    dgrid_cfg = pm._pm_config({"strategy": "dgrid"})
    assert mid_cfg["partial_tp_bp"] < dgrid_cfg["partial_tp_bp"]


def test_mid_pm_cut_confidence_between_dgrid_and_rgrid():
    """Mid respects user bias signal somewhat — cut_confidence sits between
    dgrid (eager) and rgrid (very patient)."""
    mid_cfg = pm._pm_config({"strategy": "mid"})
    dgrid_cfg = pm._pm_config({"strategy": "dgrid"})
    rgrid_cfg = pm._pm_config({"strategy": "rgrid"})
    assert dgrid_cfg["cut_confidence"] <= mid_cfg["cut_confidence"] <= rgrid_cfg["cut_confidence"]


# ---------------------------------------------------------------------------
# Layer sizing: bias_tilt + Mid table
# ---------------------------------------------------------------------------

def _base(**overrides):
    args = dict(
        side="buy", level=1, base_notional_usd=100.0, mid_price=100.0,
        inv_usd=0.0, net_units=0.0, inv_soft_usd=60.0,
        regime=_regime.REGIME_RANGE_WIDE, realized_vol_bp=10.0,
        min_order_notional_usd=10.0, max_per_level_usd=500.0,
        state={"strategy": "mid"}, config={"strategy": "mid"},
    )
    args.update(overrides)
    return args


def test_mid_bias_tilt_pumps_aligned_side():
    """Positive bias should pump the buy side and shrink the sell side."""
    buy = ls.size_quote_level(**_base(side="buy", bias_value=+0.5))
    sell = ls.size_quote_level(**_base(side="sell", bias_value=+0.5))
    assert buy["components"]["bias_tilt"] > 1.0
    assert sell["components"]["bias_tilt"] < 1.0
    assert buy["size_usd"] > sell["size_usd"]


def test_mid_bias_tilt_zero_when_no_bias():
    """Default bias_value=0 leaves bias_tilt at 1.0."""
    out = ls.size_quote_level(**_base())
    assert out["components"]["bias_tilt"] == pytest.approx(1.0)


def test_mid_bias_layers_on_top_of_regime():
    """At trend_up regime + positive bias on the buy side, the components
    multiply rather than override each other."""
    out = ls.size_quote_level(**_base(regime=_regime.REGIME_TREND_UP, bias_value=+1.0))
    # Mid's TREND_UP table: buy=1.00, sell=0.65. Bias tilt at +1.0 = +0.20.
    # So total side multiplier should reflect both ~1.0 * 1.2 = 1.2.
    assert out["components"]["regime_mult"] == pytest.approx(1.0)
    assert out["components"]["bias_tilt"] == pytest.approx(1.2)
    # raw_size_usd_before_clamp includes both multipliers
    raw = out["components"]["raw_size_usd_before_clamp"]
    assert raw > 100 * 1.0 * 1.0  # at least base * regime_mult


def test_mid_bias_clamped_to_unit_interval():
    """bias_value outside [-1, +1] is clamped."""
    out = ls.size_quote_level(**_base(side="buy", bias_value=+5.0))
    # Even with huge bias, tilt saturates at +0.20
    assert out["components"]["bias_tilt"] == pytest.approx(1.2)


def test_mid_regime_table_symmetric_in_range():
    """Mid's range tables are symmetric on the regime axis — bias does the lean."""
    table = ls.STRATEGY_REGIME_TABLES["mid"]
    for regime in (_regime.REGIME_RANGE_TIGHT, _regime.REGIME_RANGE_WIDE,
                   _regime.REGIME_CHOP_HIGH_VOL):
        assert table[regime]["buy"] == table[regime]["sell"], (
            f"Mid regime table for {regime} should be symmetric"
        )


def test_grid_uses_dgrid_regime_table():
    """Classic Grid shares the conservative D-Grid table by design."""
    assert ls.STRATEGY_REGIME_TABLES["grid"] is ls.DEFAULT_REGIME_SIZE_TABLE
