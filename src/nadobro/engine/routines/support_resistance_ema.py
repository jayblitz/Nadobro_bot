"""Support/resistance + EMA-alignment routine.

Detects local pivot lows (support) and highs (resistance) and reports whether
a multi-timeframe EMA stack is aligned (``fast > mid > slow`` for an uptrend,
or the inverse). Deterministic; no state beyond the inputs.

Implemented in Phase 4.
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Sequence

from src.nadobro.engine.routines.technical_analysis import _closes, ema

Candle = Mapping[str, float]


def _pivots(values: Sequence[float], left: int = 2, right: int = 2, kind: str = "low") -> List[float]:
    out: List[float] = []
    for i in range(left, len(values) - right):
        window = values[i - left : i + right + 1]
        v = values[i]
        if kind == "low" and v == min(window) and window.count(v) == 1:
            out.append(v)
        elif kind == "high" and v == max(window) and window.count(v) == 1:
            out.append(v)
    return out


async def run(
    candles: Sequence[Candle],
    *,
    ema_alignment: str = "fast>mid>slow",
    ema_periods: Sequence[int] = (9, 21, 50),
    pivot_window: int = 2,
) -> Dict[str, object]:
    closes = _closes(candles)
    highs = [float(c.get("high", c["close"])) for c in candles]
    lows = [float(c.get("low", c["close"])) for c in candles]
    support = sorted(set(_pivots(lows, pivot_window, pivot_window, "low")))
    resistance = sorted(set(_pivots(highs, pivot_window, pivot_window, "high")))
    fast, mid, slow = (ema(closes, p) for p in ema_periods)
    aligned = False
    if None not in (fast, mid, slow):
        if ema_alignment == "fast>mid>slow":
            aligned = fast > mid > slow  # type: ignore[operator]
        elif ema_alignment == "fast<mid<slow":
            aligned = fast < mid < slow  # type: ignore[operator]
    return {
        "support": support,
        "resistance": resistance,
        "aligned": aligned,
        "ema_fast": fast,
        "ema_mid": mid,
        "ema_slow": slow,
    }
