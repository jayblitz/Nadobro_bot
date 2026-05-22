import asyncio

from src.nadobro.engine.routines import volatility_regime as vr


def _c(closes):
    return [{"high": c + 1, "low": c - 1, "close": c} for c in closes]


def test_trending_up():
    out = asyncio.run(vr.run("P", _c([float(i) for i in range(1, 60)])))
    assert out["regime"] == vr.TRENDING_UP
    assert out["atr_pct"] >= 0


def test_trending_down():
    out = asyncio.run(vr.run("P", _c([float(i) for i in range(60, 1, -1)])))
    assert out["regime"] == vr.TRENDING_DOWN


def test_ranging():
    out = asyncio.run(vr.run("P", _c([100.0, 100.5, 100.0, 99.5] * 8)))
    assert out["regime"] == vr.RANGING
