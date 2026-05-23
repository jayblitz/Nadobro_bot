"""Volatility-regime routine — classifies the market as RANGING /
TRENDING_UP / TRENDING_DOWN from the EMA stack and ATR. Used by the dynamic
grid controller to choose between a Grid and a Reverse Grid. Deterministic.

Implemented in Phase 4.
"""
from __future__ import annotations

from typing import Dict, Mapping, Sequence

from src.nadobro.engine.routines.technical_analysis import _closes, atr, ema

Candle = Mapping[str, float]

RANGING = "RANGING"
TRENDING_UP = "TRENDING_UP"
TRENDING_DOWN = "TRENDING_DOWN"


async def run(
    trading_pair: str,
    candles: Sequence[Candle],
    *,
    atr_window: int = 14,
    ema_fast_period: int = 9,
    ema_slow_period: int = 50,
    trend_threshold: float = 0.002,
) -> Dict[str, object]:
    closes = _closes(candles)
    last = closes[-1] if closes else 0.0
    fast = ema(closes, ema_fast_period)
    slow = ema(closes, ema_slow_period)
    a = atr(candles, atr_window)
    atr_pct = (a / last) if (a is not None and last) else 0.0
    regime = RANGING
    if fast is not None and slow is not None and slow:
        sep = (fast - slow) / slow
        if sep >= trend_threshold:
            regime = TRENDING_UP
        elif sep <= -trend_threshold:
            regime = TRENDING_DOWN
    return {"regime": regime, "atr_pct": atr_pct, "ema_fast": fast, "ema_slow": slow}
