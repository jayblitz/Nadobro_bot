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


def _ema_series(values: Sequence[float], period: int) -> List[Optional[float]]:
    """Full-length EMA series aligned to ``values`` (SMA-seeded like ``ema``).
    The first ``period - 1`` entries are ``None`` (not enough history yet)."""
    n = len(values)
    if period <= 0 or n < period:
        return []
    out: List[Optional[float]] = [None] * (period - 1)
    k = 2.0 / (period + 1)
    e = sum(float(v) for v in values[:period]) / period
    out.append(e)
    for v in values[period:]:
        e = float(v) * k + e * (1 - k)
        out.append(e)
    return out


def macd(
    closes: Sequence[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Optional[Dict[str, float]]:
    """MACD line (EMA_fast − EMA_slow), its signal line (EMA of the MACD line),
    and the histogram. ``None`` until there is enough history for the signal
    line. The histogram sign is the momentum read; ``macd`` vs ``signal`` is the
    crossover."""
    if fast <= 0 or slow <= fast or signal <= 0:
        return None
    if len(closes) < slow + signal:
        return None
    fast_s = _ema_series(closes, fast)
    slow_s = _ema_series(closes, slow)
    if not fast_s or not slow_s:
        return None
    macd_line = [
        (f - s) if (f is not None and s is not None) else None
        for f, s in zip(fast_s, slow_s)
    ]
    valid = [m for m in macd_line if m is not None]
    if len(valid) < signal:
        return None
    sig = ema(valid, signal)
    if sig is None:
        return None
    macd_val = float(valid[-1])
    return {"macd": macd_val, "signal": float(sig), "histogram": macd_val - float(sig)}


def bollinger(
    closes: Sequence[float],
    period: int = 20,
    num_std: float = 2.0,
) -> Optional[Dict[str, float]]:
    """Bollinger Bands over the last ``period`` closes (population std, the
    standard convention). ``pct_b`` locates price in the band (0 = lower band,
    1 = upper); ``bandwidth`` = band width / middle (a squeeze/expansion read)."""
    if period <= 1 or len(closes) < period:
        return None
    window = [float(c) for c in closes[-period:]]
    mid = sum(window) / period
    var = sum((x - mid) ** 2 for x in window) / period
    sd = var ** 0.5
    upper = mid + num_std * sd
    lower = mid - num_std * sd
    price = float(closes[-1])
    width = upper - lower
    pct_b = ((price - lower) / width) if width > 0 else 0.5
    bandwidth = (width / mid) if mid else 0.0
    return {
        "upper": upper,
        "middle": mid,
        "lower": lower,
        "pct_b": pct_b,
        "bandwidth": bandwidth,
    }


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
        "macd": macd(closes),
        "bollinger": bollinger(closes),
    }
