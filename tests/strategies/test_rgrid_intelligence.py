"""Tests for the R-Grid intelligence path: per-strategy PM defaults,
R-Grid regime size table, and the momentum-classifier veto behavior.
"""
from __future__ import annotations

import sys
import types
import pytest

from src.nadobro.strategies import _layer_sizing as ls
from src.nadobro.strategies import _position_manager as pm
from src.nadobro.strategies import _regime


# ---------------------------------------------------------------------------
# Per-strategy PM defaults
# ---------------------------------------------------------------------------

def test_rgrid_pm_uses_strategy_defaults():
    """When state['strategy'] == 'rgrid' and no explicit pm_* override is set,
    _pm_config should pull from STRATEGY_DEFAULTS['rgrid']."""
    state = {"strategy": "rgrid"}
    cfg = pm._pm_config(state)
    rgrid_table = pm.STRATEGY_DEFAULTS["rgrid"]
    assert cfg["partial_tp_bp"] == rgrid_table["partial_tp_bp"]
    assert cfg["cut_confidence"] == rgrid_table["cut_confidence"]
    assert cfg["trail_give_back_fraction"] == rgrid_table["trail_give_back_fraction"]
    assert cfg["strategy"] == "rgrid"


def test_rgrid_pm_defaults_are_more_momentum_friendly():
    """R-Grid's defaults should be more momentum-friendly than D-Grid's:
    - higher partial_tp_bp (let trend captures run longer)
    - higher cut_confidence (don't cut against the trend casually)
    - higher trail_give_back_fraction (lock in more of HWM)
    """
    dgrid_state = {"strategy": "dgrid"}
    rgrid_state = {"strategy": "rgrid"}
    d_cfg = pm._pm_config(dgrid_state)
    r_cfg = pm._pm_config(rgrid_state)
    assert r_cfg["partial_tp_bp"] > d_cfg["partial_tp_bp"]
    assert r_cfg["cut_confidence"] > d_cfg["cut_confidence"]
    assert r_cfg["trail_give_back_fraction"] > d_cfg["trail_give_back_fraction"]


def test_explicit_state_override_beats_strategy_default():
    """state['pm_partial_tp_bp'] should win even when strategy defaults exist."""
    state = {"strategy": "rgrid", "pm_partial_tp_bp": 3.5}
    cfg = pm._pm_config(state)
    assert cfg["partial_tp_bp"] == 3.5


# ---------------------------------------------------------------------------
# Per-strategy regime size table
# ---------------------------------------------------------------------------

def test_rgrid_layer_sizing_uses_rgrid_table():
    """R-Grid's regime table leans INTO trends more than D-Grid's."""
    base = dict(
        side="buy", level=1, base_notional_usd=100.0, mid_price=100.0,
        inv_usd=0.0, net_units=0.0, inv_soft_usd=60.0,
        regime=_regime.REGIME_TREND_UP, realized_vol_bp=10.0,
        min_order_notional_usd=10.0, max_per_level_usd=500.0,
    )
    dgrid_out = ls.size_quote_level(**base, state={"strategy": "dgrid"}, config={"strategy": "dgrid"})
    rgrid_out = ls.size_quote_level(**base, state={"strategy": "rgrid"}, config={"strategy": "rgrid"})
    # In trend_up, buy side: dgrid = 1.0, rgrid = 1.3
    assert rgrid_out["components"]["regime_mult"] > dgrid_out["components"]["regime_mult"]
    assert rgrid_out["size_usd"] > dgrid_out["size_usd"]


def test_rgrid_counter_trend_side_more_aggressively_braked():
    """In trend_up, R-Grid should be MORE defensive on the sell side than D-Grid."""
    base = dict(
        side="sell", level=1, base_notional_usd=100.0, mid_price=100.0,
        inv_usd=0.0, net_units=0.0, inv_soft_usd=60.0,
        regime=_regime.REGIME_TREND_UP, realized_vol_bp=10.0,
        min_order_notional_usd=10.0, max_per_level_usd=500.0,
    )
    d = ls.size_quote_level(**base, state={"strategy": "dgrid"}, config={"strategy": "dgrid"})
    r = ls.size_quote_level(**base, state={"strategy": "rgrid"}, config={"strategy": "rgrid"})
    assert r["components"]["regime_mult"] < d["components"]["regime_mult"]


def test_explicit_table_overrides_strategy_default():
    """layer_regime_size_table in config beats per-strategy defaults."""
    custom = {
        _regime.REGIME_TREND_UP: {"buy": 0.05, "sell": 0.05},
        _regime.REGIME_RANGE_WIDE: {"buy": 0.05, "sell": 0.05},
    }
    base = dict(
        side="buy", level=1, base_notional_usd=100.0, mid_price=100.0,
        inv_usd=0.0, net_units=0.0, inv_soft_usd=60.0,
        regime=_regime.REGIME_TREND_UP, realized_vol_bp=10.0,
        min_order_notional_usd=1.0, max_per_level_usd=500.0,
    )
    out = ls.size_quote_level(
        **base,
        state={"strategy": "rgrid"},
        config={"strategy": "rgrid", "layer_regime_size_table": custom},
    )
    assert out["components"]["regime_mult"] == pytest.approx(0.05)


# ---------------------------------------------------------------------------
# Veto rule behavior (PM/regime/sizing integration via the helper functions)
# ---------------------------------------------------------------------------

def test_pm_partial_tp_threshold_higher_for_rgrid(monkeypatch):
    """An R-Grid position at 10 bp profit should NOT trigger partial TP
    (R-Grid's threshold is 15 bp), but a D-Grid position at the same profit
    SHOULD trigger it (D-Grid's threshold is 8 bp).
    """
    fake_mod = types.ModuleType("src.nadobro.services.trade_service")
    fake_mod.execute_market_order = lambda *a, **k: {"success": True}  # type: ignore
    sys.modules["src.nadobro.services.trade_service"] = fake_mod

    positions = [{"product_id": 1, "side": "LONG", "amount": 1.0, "unrealized_pnl": 0.10}]

    dgrid_state = {"strategy": "dgrid", "grid_buy_exposure_price": 100.0}
    dgrid_out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=dgrid_state, positions=positions,
        regime_info={"regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.5},
    )
    dgrid_tps = [a for a in dgrid_out["actions"] if a["type"] == pm.ACTION_PARTIAL_TP]
    assert dgrid_tps, "D-Grid should partial TP at 10 bp (>= 8 bp threshold)"

    rgrid_state = {"strategy": "rgrid", "grid_buy_exposure_price": 100.0}
    rgrid_out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=rgrid_state, positions=positions,
        regime_info={"regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.5},
    )
    rgrid_tps = [a for a in rgrid_out["actions"] if a["type"] == pm.ACTION_PARTIAL_TP]
    assert not rgrid_tps, "R-Grid should NOT partial TP at 10 bp (< 15 bp threshold)"


def test_rgrid_cut_requires_higher_confidence(monkeypatch):
    """A losing R-Grid long in trend_down at confidence 0.7 should NOT be cut
    (R-Grid requires 0.75), but D-Grid (requires 0.65) WOULD be cut.
    """
    fake_mod = types.ModuleType("src.nadobro.services.trade_service")
    fake_mod.execute_market_order = lambda *a, **k: {"success": True}  # type: ignore
    sys.modules["src.nadobro.services.trade_service"] = fake_mod

    positions = [{"product_id": 1, "side": "LONG", "amount": 1.0, "unrealized_pnl": -2.0}]
    regime = {"regime": _regime.REGIME_TREND_DOWN, "confidence": 0.70}

    dgrid_out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state={"strategy": "dgrid", "grid_buy_exposure_price": 100.0},
        positions=positions, regime_info=regime,
    )
    rgrid_out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state={"strategy": "rgrid", "grid_buy_exposure_price": 100.0},
        positions=positions, regime_info=regime,
    )
    dgrid_cuts = [a for a in dgrid_out["actions"] if a["type"] == pm.ACTION_ADVERSE_CUT]
    rgrid_cuts = [a for a in rgrid_out["actions"] if a["type"] == pm.ACTION_ADVERSE_CUT]
    assert dgrid_cuts, "D-Grid should cut at confidence 0.70 (>= 0.65)"
    assert not rgrid_cuts, "R-Grid should NOT cut at confidence 0.70 (< 0.75)"
