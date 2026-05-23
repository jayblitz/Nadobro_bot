import asyncio

from src.nadobro.engine.routines import technical_analysis as ta


def _c(closes):
    return [{"close": c, "high": c + 1, "low": c - 1} for c in closes]


def test_uptrend_indicators():
    out = asyncio.run(ta.run("P", _c([float(i) for i in range(1, 40)])))
    assert out["trend"] == "up"
    assert out["ema_fast"] > out["ema_slow"]
    assert out["rsi"] is not None and out["rsi"] > 50
    assert out["atr"] is not None and out["volatility"] is not None


def test_downtrend():
    out = asyncio.run(ta.run("P", _c([float(i) for i in range(40, 1, -1)])))
    assert out["trend"] == "down"
    assert out["rsi"] < 50


def test_flat_and_insufficient_data():
    out = asyncio.run(ta.run("P", _c([100.0, 100.0])))
    assert out["trend"] == "flat"
    assert out["rsi"] is None  # need more than rsi_period closes
