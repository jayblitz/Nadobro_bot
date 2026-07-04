"""Signal fusion engine — deterministic multi-timeframe read → Signal."""
from __future__ import annotations

from src.nadobro.services.signal_engine import Signal, build_signal


def _tf(trend, rsi, hist, drift, vr, atr=0.01):
    return {
        "trend": trend, "rsi": rsi, "macd_hist": hist, "macd_cross": 1.0,
        "bb_pct_b": 0.6, "bb_bandwidth": 0.05, "atr_pct": atr,
        "variance_ratio": vr, "drift": drift, "candles": 60,
    }


def _uptrend():
    return {
        "15m": _tf("up", 62, 0.02, 0.004, 1.4),
        "1h": _tf("up", 65, 0.03, 0.006, 1.5),
        "4h": _tf("up", 68, 0.04, 0.008, 1.6),
    }


def test_uptrend_adds_long_with_high_confidence():
    s = build_signal(_uptrend())
    assert s.bias > 0.3
    assert s.regime == "trend_up"
    assert s.scale > 0 and s.entry_ok
    assert s.confidence > 0.6
    # Barriers widen in a trend.
    assert s.tp_pct > 1.0 and s.sl_pct > 0.5


def test_downtrend_biases_short():
    down = {
        "15m": _tf("down", 38, -0.02, -0.004, 1.4),
        "1h": _tf("down", 35, -0.03, -0.006, 1.5),
        "4h": _tf("down", 32, -0.04, -0.008, 1.6),
    }
    s = build_signal(down)
    assert s.bias < -0.3 and s.regime == "trend_down" and s.scale < 0


def test_range_takes_no_directional_add():
    rng = {
        "15m": _tf("flat", 52, 0.001, 0.0002, 1.05),
        "1h": _tf("flat", 49, -0.001, -0.0001, 1.1),
        "4h": _tf("flat", 51, 0.0, 0.0, 1.15),
    }
    s = build_signal(rng)
    assert s.regime == "range"
    assert s.scale == 0.0
    assert s.sl_pct == 0.5 and s.tp_pct == 1.0   # base barriers in range


def test_higher_timeframe_trend_opposes_pauses_entries():
    # 15m spikes up hard but 4h is firmly down -> bias positive, 4h opposes.
    conflict = {
        "15m": _tf("up", 78, 0.06, 0.02, 1.2),
        "1h": _tf("up", 70, 0.04, 0.01, 1.1),
        "4h": _tf("down", 40, -0.01, -0.002, 1.5),
    }
    s = build_signal(conflict)
    if s.bias > 0.3:
        assert s.entry_ok is False
        assert any("opposes" in r for r in s.risks)


def test_rsi_overbought_trims_long_add():
    up = _uptrend()
    up["15m"]["rsi"] = 82.0   # overbought on the fast timeframe
    s = build_signal(up)
    base = build_signal(_uptrend())
    assert s.scale < base.scale       # add is trimmed
    assert any("overbought" in r for r in s.risks)


def test_funding_flagged_as_carry_cost():
    s = build_signal(_uptrend(), funding_rate=0.0012, position_side="long")
    assert any("Funding" in r and "paying" in r for r in s.risks)
    # No funding flag when not holding that side.
    s2 = build_signal(_uptrend(), funding_rate=0.0012, position_side=None)
    assert not any("Funding" in r for r in s2.risks)


def test_cold_history_is_neutral_and_blocks_entry():
    cold = {"15m": {"trend": "flat", "rsi": None, "macd_hist": None,
                    "drift": None, "variance_ratio": None, "atr_pct": None, "candles": 3}}
    s = build_signal(cold)
    assert s.bias == 0.0 and s.confidence == 0.0 and s.entry_ok is False


def test_conflicting_timeframes_lower_confidence():
    strong = build_signal(_uptrend()).confidence
    mixed = build_signal({
        "15m": _tf("up", 60, 0.02, 0.003, 1.3),
        "1h": _tf("down", 45, -0.02, -0.003, 1.3),
        "4h": _tf("up", 55, 0.01, 0.001, 1.3),
    }).confidence
    assert mixed < strong


def test_signal_as_dict_is_bounded():
    s = build_signal(_uptrend())
    d = s.as_dict()
    assert -1.0 <= d["bias"] <= 1.0
    assert -1.0 <= d["scale"] <= 1.0
    assert d["spread_mult"] > 0
    assert 0.0 <= d["confidence"] <= 1.0
    assert isinstance(d["reasons"], list) and isinstance(d["risks"], list)


def test_spread_widens_with_volatility():
    calm = _uptrend()
    for tf in calm.values():
        tf["atr_pct"] = 0.003
    vol = _uptrend()
    for tf in vol.values():
        tf["atr_pct"] = 0.05
    assert build_signal(vol).spread_mult > build_signal(calm).spread_mult
