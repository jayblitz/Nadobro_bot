"""Technical-analysis routine — deterministic EMA / RSI / ATR / trend /
volatility over a candle series. No LLM, no state beyond the inputs.

A *candle* is a mapping with at least ``close`` (and ``high`` / ``low`` for
ATR). ``run`` is async to match the routine protocol but the math is pure.

Implemented in Phase 4.
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence

Candle = Mapping[str, float]


def _closes(candles: Sequence[Candle]) -> List[float]:
    return [float(c["close"]) for c in candles]


def ema(values: Sequence[float], period: int) -> Optional[float]:
    """Exponential moving average.

    BUG-TA-1 fix: seed the EMA with the SMA of the first ``period`` values
    (standard convention) and return ``None`` for series shorter than
    ``period``. The previous implementation seeded with ``values[0]`` and
    returned a value for any non-empty series, which produced garbage trend
    classifications on cold-start (fewer than ``period`` candles).
    """
    if period <= 0:
        return None
    n = len(values)
    if n < period:
        return None
    k = 2.0 / (period + 1)
    # SMA of the first `period` values is the canonical seed.
    e = sum(float(v) for v in values[:period]) / period
    for v in values[period:]:
        e = float(v) * k + e * (1 - k)
    return e


def rsi(closes: Sequence[float], period: int = 14) -> Optional[float]:
    if len(closes) <= period:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(d, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-d, 0.0)) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles: Sequence[Candle], period: int = 14) -> Optional[float]:
    """Average True Range using Wilder's smoothing (BUG-TA-2 fix).

    Returns ``None`` if there are fewer than ``period + 1`` candles (need
    ``period`` true-range values to seed Wilder's smoothing).
    """
    if period <= 0 or len(candles) < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(candles)):
        hi = float(candles[i].get("high", candles[i]["close"]))
        lo = float(candles[i].get("low", candles[i]["close"]))
        prev_close = float(candles[i - 1]["close"])
        trs.append(max(hi - lo, abs(hi - prev_close), abs(lo - prev_close)))
    if len(trs) < period:
        return None
    # Wilder's smoothing: seed with SMA of first `period` TRs, then
    # ATR[i] = ((period-1) * ATR[i-1] + TR[i]) / period
    atr_val = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_val = ((period - 1) * atr_val + tr) / period
    return atr_val


async def run(
    trading_pair: str,
    candles: Sequence[Candle],
    *,
    ema_fast_period: int = 9,
    ema_slow_period: int = 21,
    rsi_period: int = 14,
    atr_period: int = 14,
) -> Dict[str, object]:
    closes = _closes(candles)
    ema_fast = ema(closes, ema_fast_period)
    ema_slow = ema(closes, ema_slow_period)
    if ema_fast is None or ema_slow is None:
        trend = "flat"
    elif ema_fast > ema_slow:
        trend = "up"
    elif ema_fast < ema_slow:
        trend = "down"
    else:
        trend = "flat"
    a = atr(candles, atr_period)
    last = closes[-1] if closes else 0.0
    volatility = (a / last) if (a is not None and last) else None
    return {
        "trading_pair": trading_pair,
        "trend": trend,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "atr": a,
        "rsi": rsi(closes, rsi_period),
        "volatility": volatility,
    }
