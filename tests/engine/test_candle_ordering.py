"""CANDLE-ORDER guardrails (2026-07-31).

The Nado indexer serves candles NEWEST-first while every consumer (regime
gate, EMA/ATR trend stack, variance ratio, backtester) assumes chronological
input. Feeding the raw order made ``candles[-1]`` the 3.3h-old bar (the
breakout test compared a stale close against the value area — $461 off in the
2026-07-31 session-178 repro) and flipped the EMA trend classification.

Two independent defenses are pinned here:
1. ``nado_client._candles_sorted_asc`` — the venue client normalizes every
   ``get_candlesticks`` result (fresh and cache-hit) to oldest-first.
2. ``regime_gate.run`` — sorts timed candles itself, so its verdict is
   order-invariant even if some future provider skips the client.
"""
from __future__ import annotations

import asyncio

from src.nadobro.engine.routines import regime_gate
from src.nadobro.venue.nado_client import _candles_sorted_asc

PAIR = "BTC-PERP"
BASE_TS = 1_785_400_000


def _timed(candles: list[dict], step: int = 60) -> list[dict]:
    """Stamp a chronological ``time`` on a candle series (oldest-first input)."""
    return [
        {**c, "time": BASE_TS + i * step}
        for i, c in enumerate(candles)
    ]


def _uptrend(n: int = 80, base: float = 100.0, step: float = 0.4) -> list[dict]:
    out, price = [], base
    for _ in range(n):
        nxt = price + step
        out.append({
            "open": price, "high": nxt + 0.05, "low": price - 0.05,
            "close": nxt, "volume": 1000.0,
        })
        price = nxt
    return _timed(out)


def _ranging_with_breakout(n: int = 80, base: float = 100.0) -> list[dict]:
    """Tight range whose LAST close escapes the value area."""
    out = []
    for i in range(n - 1):
        wiggle = 0.05 if i % 2 == 0 else -0.05
        close = base + wiggle
        out.append({
            "open": base, "high": close + 0.05, "low": close - 0.05,
            "close": close, "volume": 1000.0,
        })
    out.append({
        "open": base, "high": base + 3.0, "low": base,
        "close": base + 3.0, "volume": 50.0,
    })
    return _timed(out)


# --------------------------------------------------------------------------
# 1. The client-side normalizer
# --------------------------------------------------------------------------
def test_sorted_asc_reverses_indexer_order():
    asc = _timed([{"close": float(i), "volume": 1.0} for i in range(5)])
    desc = list(reversed(asc))
    assert _candles_sorted_asc(desc) == asc


def test_sorted_asc_keeps_chronological_input():
    asc = _timed([{"close": float(i), "volume": 1.0} for i in range(5)])
    assert _candles_sorted_asc(list(asc)) == asc


def test_sorted_asc_coerces_string_timestamps():
    # The indexer serves timestamps as digit STRINGS; a lexicographic sort
    # would order "999" after "1000". The sort must be numeric.
    rows = [
        {"time": "1000", "close": 2.0},
        {"time": "999", "close": 1.0},
        {"time": "10000", "close": 3.0},
    ]
    out = _candles_sorted_asc(rows)
    assert [c["close"] for c in out] == [1.0, 2.0, 3.0]


def test_sorted_asc_fails_open_without_timestamps():
    rows = [{"close": 3.0}, {"close": 1.0}, {"close": 2.0}]
    assert _candles_sorted_asc(list(rows)) == rows


# --------------------------------------------------------------------------
# 2. The gate is order-invariant on timed candles
# --------------------------------------------------------------------------
def test_gate_verdict_is_order_invariant_for_trend():
    candles = _uptrend()
    asc = asyncio.run(regime_gate.run(PAIR, candles))
    desc = asyncio.run(regime_gate.run(PAIR, list(reversed(candles))))
    # Chronologically this is an uptrend; the reversed feed must classify the
    # SAME way (pre-fix it read trending_down — the EMA stack ran backwards).
    assert asc["verdict"] == regime_gate.PAUSE
    assert asc["reason"] == "trending_up"
    assert desc["verdict"] == asc["verdict"]
    assert desc["reason"] == asc["reason"]


def test_gate_verdict_is_order_invariant_for_breakout():
    candles = _ranging_with_breakout()
    asc = asyncio.run(regime_gate.run(PAIR, candles))
    desc = asyncio.run(regime_gate.run(PAIR, list(reversed(candles))))
    # The escape close is chronologically LAST; a newest-first feed put it at
    # index 0, compared the oldest close against the value area, and read the
    # EMA stack backwards. Whatever pause reason the chronological data yields
    # (trend or breakout — both are "price left acceptance"), the reversed
    # feed must yield the SAME one; pre-fix it flipped (e.g. trending_down).
    assert asc["verdict"] == regime_gate.PAUSE
    assert asc["reason"] != ""
    assert desc["verdict"] == asc["verdict"]
    assert desc["reason"] == asc["reason"]


def test_gate_untimed_candles_keep_legacy_order():
    """Fixtures without ``time`` keys (the existing test corpus) must behave
    exactly as before — the defensive sort only engages on timed candles."""
    untimed = []
    for i in range(60):
        wiggle = 0.05 if i % 2 == 0 else -0.05
        close = 100.0 + wiggle
        untimed.append({
            "open": 100.0, "high": close + 0.05, "low": close - 0.05,
            "close": close, "volume": 1000.0,
        })
    res = asyncio.run(regime_gate.run(PAIR, untimed))
    assert res["verdict"] == regime_gate.QUOTE


# --------------------------------------------------------------------------
# 3. variance_regime (dgrid/rgrid brain) is order-invariant on timed candles
# --------------------------------------------------------------------------
def test_variance_regime_direction_is_order_invariant():
    """The drift window reads the LAST long_window closes and its SIGN picks
    the trending side. Under the old newest-first feed dgrid computed drift on
    the negated, ~3h-stale oldest window (long grids bought persistent
    declines — dgrid audit 2026-07-31). Any provider order must now classify
    identically to chronological input."""
    from src.nadobro.engine.routines import variance_regime

    # Persistent slow decline: drift-driven trend, chronologically DOWN.
    candles = _timed([
        {"open": 100.0 - i * 0.05, "high": 100.05 - i * 0.05,
         "low": 99.9 - i * 0.05, "close": 100.0 - i * 0.05, "volume": 1000.0}
        for i in range(200)
    ])
    asc = asyncio.run(variance_regime.run(PAIR, candles))
    desc = asyncio.run(variance_regime.run(PAIR, list(reversed(candles))))
    assert asc["direction"] == "down"
    assert desc["direction"] == asc["direction"]
    assert desc["phase"] == asc["phase"]
    assert abs(float(desc["drift_pct"]) - float(asc["drift_pct"])) < 1e-9
