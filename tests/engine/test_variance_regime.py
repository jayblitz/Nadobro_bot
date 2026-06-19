"""Variance-ratio regime routine — the tunable brain behind dgrid switching.

Verifies:
1. A trending market reads VR > 1 and picks the directional phase
   (down -> rgrid short, up -> grid long).
2. A mean-reverting range reads VR < 1 and picks grid.
3. The trend_on / range_on hysteresis band holds the current phase.
4. Insufficient history holds the current phase (never flips on noise).
5. User windows feed the computation (short/long horizons).
"""
from __future__ import annotations

import asyncio
import math

from src.nadobro.engine.routines import variance_regime as vr

PAIR = "BTC-PERP"


def ranging_candles(n: int = 60, base: float = 100.0, amp: float = 1.0, period: float = 7.0) -> list[dict]:
    """Smooth oscillation — mean-reverting, grid-friendly (bounded long-horizon
    displacement -> VR < 1). Period is coprime-ish to the 4/12 windows so short
    returns never collapse to exact zeros."""
    return [{"close": base + amp * math.sin(2 * math.pi * i / period)} for i in range(n)]


def trending_candles(n: int = 60, base: float = 100.0, step: float = 0.4) -> list[dict]:
    """Monotonic drift — trending, grid-hostile (VR > 1)."""
    out = []
    price = base
    for _ in range(n):
        price += step
        out.append({"close": price})
    return out


def test_downtrend_reads_high_vr_and_picks_rgrid():
    result = asyncio.run(vr.run(PAIR, trending_candles(step=-0.4)))
    assert result["insufficient_history"] is False
    assert float(result["variance_ratio"]) >= 1.25
    assert result["direction"] == vr.DOWN
    assert result["phase"] == vr.RGRID


def test_uptrend_reads_high_vr_but_stays_long_grid():
    result = asyncio.run(vr.run(PAIR, trending_candles(step=0.4)))
    assert float(result["variance_ratio"]) >= 1.25
    assert result["direction"] == vr.UP
    # A reverse grid bleeds in an uptrend: stay long.
    assert result["phase"] == vr.GRID


def test_range_reads_low_vr_and_picks_grid():
    result = asyncio.run(vr.run(PAIR, ranging_candles(), current_phase=vr.RGRID))
    assert float(result["variance_ratio"]) < 1.15
    assert result["phase"] == vr.GRID


def test_hysteresis_band_holds_current_phase():
    # Force a ratio between range_on and trend_on by widening the band so any
    # finite VR lands inside it: the routine must hold whatever phase it had.
    # Drift filter disabled (trend_drift_pct=0) to isolate the VR-band path —
    # otherwise the monotonic decline correctly flips short on the drift signal.
    candles = trending_candles(step=-0.4)
    held_rgrid = asyncio.run(
        vr.run(PAIR, candles, trend_on=99.0, range_on=0.0,
               trend_drift_pct=0.0, current_phase=vr.RGRID)
    )
    held_grid = asyncio.run(
        vr.run(PAIR, candles, trend_on=99.0, range_on=0.0,
               trend_drift_pct=0.0, current_phase=vr.GRID)
    )
    assert held_rgrid["phase"] == vr.RGRID
    assert held_grid["phase"] == vr.GRID


def test_insufficient_history_holds_current_phase():
    result = asyncio.run(
        vr.run(PAIR, trending_candles(n=5, step=-0.4), current_phase=vr.RGRID)
    )
    assert result["insufficient_history"] is True
    assert result["phase"] == vr.RGRID
    assert float(result["variance_ratio"]) == 0.0


def test_user_windows_change_required_history():
    # Long window 30 needs > 32 candles; 20 candles is insufficient.
    short = asyncio.run(
        vr.run(PAIR, trending_candles(n=20, step=-0.4), short_window=4, long_window=30)
    )
    assert short["insufficient_history"] is True
    enough = asyncio.run(
        vr.run(PAIR, trending_candles(n=40, step=-0.4), short_window=4, long_window=30)
    )
    assert enough["insufficient_history"] is False


def _ranging_then_decline(n=200, base=63300.0):
    """Long ranging history then a recent grind-down in the last ~15 bars — the
    real-market case where VR (computed over the WHOLE window) stays < the trend
    cutoff (~0.88 in the logs) yet price is clearly trending down right now."""
    out = []
    for i in range(n):
        c = base + 40.0 * math.sin(2 * math.pi * i / 9)
        if i >= n - 15:
            c -= 30.0 * (i - (n - 15))
        out.append({"close": c})
    return out


def test_drift_filter_flips_short_on_recent_grind_even_when_vr_low():
    # The dgrid-blind bug: a long ranging history keeps VR < trend_on, so the
    # VR-only classifier called it "ranging" and the long grid kept buying into
    # the decline. The directional-drift filter must flip it to rgrid (short).
    candles = _ranging_then_decline()
    r = asyncio.run(vr.run(PAIR, candles, trend_drift_pct=0.30))
    assert float(r["variance_ratio"]) < 1.25      # VR alone says "ranging"
    assert r["direction"] == vr.DOWN
    assert r["trend_by_drift"] is True
    assert r["phase"] == vr.RGRID                 # flips short on the drift


def test_drift_filter_off_reproduces_blind_grid_bug():
    # With the filter disabled, the same recent decline is mis-classified as a
    # range and stays a long GRID — the original blind behavior.
    candles = _ranging_then_decline()
    r = asyncio.run(vr.run(PAIR, candles, trend_drift_pct=0.0))
    assert float(r["variance_ratio"]) < 1.25
    assert r["phase"] == vr.GRID


def test_flat_range_does_not_false_trigger_drift_trend():
    # A pure oscillating range (no net drift) must NOT trip the drift filter.
    candles = [{"close": 63300.0 + 60.0 * math.sin(2 * math.pi * i / 7)} for i in range(120)]
    r = asyncio.run(vr.run(PAIR, candles, trend_drift_pct=0.30))
    assert r["trend_by_drift"] is False
    assert r["phase"] == vr.GRID
