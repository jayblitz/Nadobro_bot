import asyncio

from src.nadobro.engine.routines import support_resistance_ema as sr


def test_uptrend_is_aligned():
    candles = [{"high": c + 1, "low": c - 1, "close": c} for c in [float(i) for i in range(1, 60)]]
    out = asyncio.run(sr.run(candles))
    assert out["aligned"] is True
    assert isinstance(out["support"], list) and isinstance(out["resistance"], list)


def test_pivot_low_detected():
    lows = [5, 4, 3, 4, 5]
    candles = [{"high": v + 10, "low": v, "close": v + 5} for v in lows]
    out = asyncio.run(sr.run(candles, pivot_window=2))
    assert 3 in out["support"]


def test_downtrend_not_aligned_default():
    candles = [{"high": c + 1, "low": c - 1, "close": c} for c in [float(i) for i in range(60, 1, -1)]]
    out = asyncio.run(sr.run(candles))
    assert out["aligned"] is False
    assert asyncio.run(sr.run(candles, ema_alignment="fast<mid<slow"))["aligned"] is True
