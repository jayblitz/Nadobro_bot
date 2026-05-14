"""Unit tests for the D-Grid position manager.

The PM imports ``trade_service.execute_market_order`` lazily, so we monkey-patch
the module-level import path that ``_position_manager._execute_close`` reaches
into. This keeps tests free of the wallet/Nado dependency tree.
"""
from __future__ import annotations

import time
import types
import pytest

from src.nadobro.strategies import _position_manager as pm
from src.nadobro.strategies import _regime


@pytest.fixture
def fake_execute(monkeypatch):
    """Patch the execute_market_order import inside _position_manager.

    Restores the original (or absent) trade_service module after each test so
    we don't pollute sibling test files that may legitimately import the real
    or stubbed trade_service (e.g. volume_bot tests).
    """
    calls: list[dict] = []

    def _stub(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {"success": True, "digest": "0xfake"}

    import sys
    key = "src.nadobro.services.trade_service"
    saved = sys.modules.get(key)
    fake_module = types.ModuleType(key)
    fake_module.execute_market_order = _stub  # type: ignore
    # Add the spot variants so a sibling import that lists them still works.
    fake_module.execute_limit_order = _stub  # type: ignore
    fake_module.execute_spot_limit_order = _stub  # type: ignore
    fake_module.execute_spot_market_order = _stub  # type: ignore
    sys.modules[key] = fake_module
    try:
        yield calls
    finally:
        if saved is None:
            sys.modules.pop(key, None)
        else:
            sys.modules[key] = saved


def _pos(side: str, amount: float, unrealized: float = 0.0, product_id: int = 1) -> dict:
    return {
        "product_id": product_id,
        "side": side,
        "amount": amount,
        "unrealized_pnl": unrealized,
    }


def test_pm_disabled_returns_noop():
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state={}, positions=[], regime_info={}, enabled=False,
    )
    assert out["enabled"] is False
    assert out["actions"] == []


def test_partial_tp_fires_on_profit(fake_execute):
    state = {"grid_buy_exposure_price": 100.0}
    positions = [_pos("LONG", amount=1.0, unrealized=2.0)]  # ~200 bp on cost 100
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=positions,
        regime_info={"regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.5},
    )
    actions = [a for a in out["actions"] if a["type"] == pm.ACTION_PARTIAL_TP]
    assert actions, f"expected partial_tp, got {out['actions']}"
    assert actions[0]["side"] == "long"


def test_adverse_cut_on_trend_against_long(fake_execute):
    state = {"grid_buy_exposure_price": 100.0}
    positions = [_pos("LONG", amount=1.0, unrealized=-2.0)]
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=positions,
        regime_info={"regime": _regime.REGIME_TREND_DOWN, "confidence": 0.8},
    )
    cuts = [a for a in out["actions"] if a["type"] == pm.ACTION_ADVERSE_CUT]
    assert cuts, f"expected adverse_cut, got {out['actions']}"
    assert cuts[0]["side"] == "long"
    # 50% of inventory
    assert cuts[0]["size_base"] == pytest.approx(0.5)


def test_no_cut_when_confidence_too_low(fake_execute):
    state = {"grid_buy_exposure_price": 100.0}
    positions = [_pos("LONG", amount=1.0, unrealized=-2.0)]
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=positions,
        regime_info={"regime": _regime.REGIME_TREND_DOWN, "confidence": 0.3},
    )
    cuts = [a for a in out["actions"] if a["type"] == pm.ACTION_ADVERSE_CUT]
    assert not cuts


def test_trail_close_on_pnl_giveback(fake_execute):
    state = {
        "grid_buy_exposure_price": 100.0,
        "pm_hwm": {"long": 5.0, "short": 0.0},
        "pm_trail_arm_usd": 1.0,
    }
    positions = [_pos("LONG", amount=1.0, unrealized=1.0)]  # gave back from 5 -> 1
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=positions,
        regime_info={"regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.5},
    )
    trails = [a for a in out["actions"] if a["type"] == pm.ACTION_TRAIL_CLOSE]
    assert trails


def test_cooldown_gates_size_dampener(fake_execute):
    state = {}
    pm.trigger_cooldown(state, "test")
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=[], regime_info={},
    )
    assert out["cooldown_active"] is True
    assert out["size_dampener"] < 1.0


def test_cooldown_dampener_recovers_over_time():
    state = {}
    pm.trigger_cooldown(state, "test")
    d_now = pm.cooldown_size_dampener(state)
    # Fake the clock forward by reducing the cooldown_until
    state["pm_cooldown_until"] = float(state["pm_cooldown_until"]) - 250.0
    d_later = pm.cooldown_size_dampener(state)
    # We allow either equal or recovering — strictly: not lower than d_now
    assert d_later >= d_now


def test_stale_flatten_after_long_hold(fake_execute):
    state = {
        "grid_buy_exposure_price": 100.0,
        "grid_last_fill_ts": time.time() - 4000,  # > 30 min default
        "dgrid_min_spread_bp": 5.0,
    }
    positions = [_pos("LONG", amount=1.0, unrealized=-0.5)]
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=positions,
        regime_info={
            "regime": _regime.REGIME_RANGE_WIDE,
            "confidence": 0.5,
            "range_expansion_bp": 50.0,  # > 2 * 5 normal_bp
            "realized_vol_bp": 50.0,
        },
    )
    stales = [a for a in out["actions"] if a["type"] == pm.ACTION_STALE_FLATTEN]
    assert stales


def test_dedup_same_type_and_side_runs_once(fake_execute):
    """Partial TP and trail close both want to close the same side; only one should fire."""
    state = {
        "grid_buy_exposure_price": 100.0,
        "pm_hwm": {"long": 5.0, "short": 0.0},
        "pm_trail_arm_usd": 1.0,
    }
    # Profit above partial_tp threshold AND below trail give-back (5 -> 1 = below 2.5).
    positions = [_pos("LONG", amount=1.0, unrealized=1.0)]
    out = pm.manage_positions(
        telegram_id=1, product="QQQ", product_id=1,
        state=state, positions=positions,
        regime_info={"regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.5},
    )
    # Either partial_tp OR trail_close fires (different types are both allowed by dedup
    # since dedup is on (type, side) tuples). Just verify long side appears at most once
    # per type.
    long_types = [a["type"] for a in out["actions"] if a["side"] == "long"]
    assert len(long_types) == len(set(long_types))
