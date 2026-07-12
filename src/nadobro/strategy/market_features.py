"""Multi-timeframe market features for the financial overlay.

Gathers OHLCV per timeframe (15m / 1h / 4h by default) and reduces each to a
compact, pure feature dict (trend, rsi, macd, bollinger, atr%, variance-ratio
regime + drift). The candle fetch is injectable so the signal engine and tests
never touch the venue: pass ``client.get_candlesticks`` in production, a fixture
callable in tests.

Higher timeframes change slowly, so a small process-local TTL cache keyed by
``(network, product_id, timeframe)`` avoids re-fetching every MM tick on top of
the client's own shared Redis cache. No event-loop I/O lives here — callers run
this off the loop (run_blocking_sdk) like every other SDK read.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Dict, List, Mapping, Optional, Sequence

from src.nadobro.engine.routines import technical_analysis as ta
from src.nadobro.engine.routines.variance_regime import variance_ratio

logger = logging.getLogger(__name__)

Candle = Mapping[str, float]
# fetcher(product_id, timeframe, limit) -> list[candle dict]
CandleFetcher = Callable[[int, str, int], Sequence[Candle]]

DEFAULT_TIMEFRAMES: tuple[str, ...] = ("15m", "1h", "4h")
DEFAULT_LIMIT = 200

# Per-timeframe cache TTL (seconds). A higher bar is refreshed less often.
_TTL_BY_TF: Dict[str, float] = {
    "1m": 20.0, "5m": 60.0, "15m": 120.0, "1h": 300.0, "4h": 600.0, "1d": 1800.0,
}
_DEFAULT_TTL = 120.0

# (network, product_id, timeframe) -> (fetched_at, candles)
_cache: Dict[tuple, tuple[float, List[Candle]]] = {}


def _ttl_for(timeframe: str) -> float:
    return _TTL_BY_TF.get(str(timeframe), _DEFAULT_TTL)


def reset_cache() -> None:
    _cache.clear()


def get_candles_cached(
    fetcher: CandleFetcher,
    network: str,
    product_id: int,
    timeframe: str,
    *,
    limit: int = DEFAULT_LIMIT,
    now: Optional[float] = None,
) -> List[Candle]:
    """Fetch candles with a process-local TTL cache. A fetch failure returns the
    last good cached value (stale-ok) rather than an empty series that would
    blind the signal engine."""
    now = time.time() if now is None else now
    key = (str(network), int(product_id), str(timeframe))
    hit = _cache.get(key)
    if hit is not None and (now - hit[0]) < _ttl_for(timeframe):
        return hit[1]
    try:
        candles = list(fetcher(int(product_id), str(timeframe), int(limit)) or [])
    except Exception:  # noqa: BLE001 - never let a fetch error break a tick
        logger.warning(
            "candle fetch failed pid=%s tf=%s — serving last cached",
            product_id, timeframe, exc_info=True,
        )
        return hit[1] if hit is not None else []
    if candles:
        _cache[key] = (now, candles)
        return candles
    # Empty (throttle / cold): keep the last good value if we have one.
    return hit[1] if hit is not None else []


def compute_tf_features(candles: Sequence[Candle]) -> Dict[str, object]:
    """Reduce one timeframe's candles to a compact, pure feature dict. Fields
    are ``None`` when there is not enough history, so downstream fusion can
    weight only what is actually known."""
    closes = [float(c["close"]) for c in candles] if candles else []
    n = len(closes)
    ema_fast = ta.ema(closes, 9)
    ema_slow = ta.ema(closes, 21)
    if ema_fast is None or ema_slow is None:
        trend = "flat"
    elif ema_fast > ema_slow:
        trend = "up"
    elif ema_fast < ema_slow:
        trend = "down"
    else:
        trend = "flat"
    a = ta.atr(candles, 14)
    last = closes[-1] if closes else 0.0
    atr_pct = (a / last) if (a is not None and last) else None
    m = ta.macd(closes)
    bb = ta.bollinger(closes)
    # Variance-ratio regime read (short/long horizons scaled to the series).
    vr = variance_ratio(closes, 4, 12) if n >= 14 else None
    drift = None
    if n >= 13:
        seg = closes[-13:]
        k = max(2, len(seg) // 3)
        older = sum(seg[:k]) / k
        recent = sum(seg[-k:]) / k
        drift = ((recent - older) / older) if older > 0 else 0.0
    return {
        "candles": n,
        "trend": trend,
        "rsi": ta.rsi(closes, 14),
        # Histogram normalized by price: the raw MACD histogram is in PRICE
        # units, so an unscaled read saturates the fusion vote on a $100k asset
        # and contributes ~nothing on a sub-cent token. As a fraction of the
        # last close it is comparable across products.
        "macd_hist": ((m["histogram"] / last) if (m and last) else None),
        "macd_cross": (1.0 if (m and m["macd"] > m["signal"]) else (-1.0 if m else None)),
        "bb_pct_b": (bb["pct_b"] if bb else None),
        "bb_bandwidth": (bb["bandwidth"] if bb else None),
        "atr_pct": atr_pct,
        "variance_ratio": vr,
        "drift": drift,
    }


def multi_tf_features(
    fetcher: CandleFetcher,
    network: str,
    product_id: int,
    *,
    timeframes: Sequence[str] = DEFAULT_TIMEFRAMES,
    limit: int = DEFAULT_LIMIT,
    now: Optional[float] = None,
) -> Dict[str, Dict[str, object]]:
    """Feature dict per timeframe, e.g. ``{"15m": {...}, "1h": {...}, "4h": {...}}``."""
    out: Dict[str, Dict[str, object]] = {}
    for tf in timeframes:
        candles = get_candles_cached(fetcher, network, product_id, tf, limit=limit, now=now)
        out[str(tf)] = compute_tf_features(candles)
    return out
