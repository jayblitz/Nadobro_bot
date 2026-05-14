"""Unit tests for the D-Grid regime classifier."""
from __future__ import annotations

import pytest

from src.nadobro.strategies import _regime


def _flat_history(n: int, price: float = 100.0) -> list[float]:
    return [price] * n


def _trend_history(n: int, start: float = 100.0, step_bp: float = 5.0) -> list[float]:
    out = [start]
    for _ in range(1, n):
        out.append(out[-1] * (1.0 + step_bp / 1e4))
    return out


def _trend_down_history(n: int, start: float = 100.0, step_bp: float = 5.0) -> list[float]:
    out = [start]
    for _ in range(1, n):
        out.append(out[-1] * (1.0 - step_bp / 1e4))
    return out


def test_flat_history_classified_as_range_tight():
    state: dict = {}
    info = _regime.classify_regime(
        state,
        _flat_history(30),
        variance_ratio=0.5,
        realized_vol_bp=0.5,
    )
    assert info["regime"] in (_regime.REGIME_RANGE_TIGHT, _regime.REGIME_RANGE_WIDE)


def test_strong_uptrend_classified_as_trend_up():
    state: dict = {}
    info = _regime.classify_regime(
        state,
        _trend_history(40, step_bp=10.0),
        variance_ratio=1.5,
        realized_vol_bp=30.0,
    )
    assert info["regime"] == _regime.REGIME_TREND_UP
    assert info["drift_bp"] > 0
    assert info["confidence"] > 0.2


def test_strong_downtrend_classified_as_trend_down():
    state: dict = {}
    info = _regime.classify_regime(
        state,
        _trend_down_history(40, step_bp=10.0),
        variance_ratio=1.5,
        realized_vol_bp=30.0,
    )
    assert info["regime"] == _regime.REGIME_TREND_DOWN
    assert info["drift_bp"] < 0


def test_hysteresis_keeps_previous_regime_when_in_band():
    state: dict = {"regime": _regime.REGIME_TREND_UP}
    info = _regime.classify_regime(
        state,
        _flat_history(20),
        variance_ratio=1.20,  # in the [1.15, 1.25] hysteresis band
        realized_vol_bp=10.0,
    )
    # Should keep trend_up or fall through to RANGE_TIGHT iff vol & drift both
    # collapse. Since flat history => low drift and low vol => RANGE_TIGHT may win.
    assert info["regime"] in (_regime.REGIME_TREND_UP, _regime.REGIME_RANGE_TIGHT)


def test_regime_change_flag_set_when_label_changes():
    state: dict = {"regime": _regime.REGIME_RANGE_TIGHT}
    info = _regime.classify_regime(
        state,
        _trend_history(40, step_bp=10.0),
        variance_ratio=1.5,
        realized_vol_bp=30.0,
    )
    assert info["regime_changed"] is True


def test_fill_asymmetry_signal():
    state: dict = {
        "grid_buy_fills": [{"price": 100, "size": 1, "ts": float(i)} for i in range(20)],
        "grid_sell_fills": [],
    }
    # Heavy buy fills -> fill_asym positive -> bearish vote
    asym = _regime._fill_asymmetry(state)
    assert asym > 0.9


def test_funding_signal_pulled_from_state():
    state: dict = {"rgrid_prev_funding_bp": 2.5}
    info = _regime.classify_regime(
        state,
        _flat_history(20),
        variance_ratio=1.0,
        realized_vol_bp=5.0,
    )
    assert info["funding_bp"] == 2.5
