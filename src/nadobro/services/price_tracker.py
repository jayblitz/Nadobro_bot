import logging
import time
import math
from collections import defaultdict
from typing import Optional

from src.nadobro.config import PRODUCTS

logger = logging.getLogger(__name__)

_price_history: dict[str, list[dict]] = defaultdict(list)

MAX_HISTORY_POINTS = 480
TICK_INTERVAL_SECONDS = 60

RSI_PERIOD = 14
EMA_FAST = 9
EMA_MID = 21
EMA_SLOW = 50
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
BB_STD_DEV = 2.0


def record_price(product: str, bid: float, ask: float, mid: float):
    if mid <= 0:
        return
    entry = {
        "ts": time.time(),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_bp": ((ask - bid) / mid * 10000) if mid > 0 else 0,
    }
    history = _price_history[product.upper()]
    history.append(entry)
    if len(history) > MAX_HISTORY_POINTS:
        _price_history[product.upper()] = history[-MAX_HISTORY_POINTS:]


def record_prices_from_client(client) -> dict[str, float]:
    recorded = {}
    try:
        all_prices = client.get_all_market_prices()
        if not all_prices:
            return recorded
        for product, price_data in all_prices.items():
            if isinstance(price_data, dict):
                bid = float(price_data.get("bid", 0))
                ask = float(price_data.get("ask", 0))
                mid = float(price_data.get("mid", 0))
            else:
                mid = float(price_data) if price_data else 0
                bid = mid
                ask = mid
            if mid > 0:
                record_price(product, bid, ask, mid)
                recorded[product] = mid
    except Exception as e:
        logger.error("Failed to record prices: %s", e)
    return recorded


def get_history(product: str, limit: int = 0) -> list[dict]:
    history = _price_history.get(product.upper(), [])
    if limit > 0:
        return history[-limit:]
    return list(history)


def get_mids(product: str) -> list[float]:
    return [p["mid"] for p in get_history(product) if p.get("mid", 0) > 0]


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for i in range(1, len(values)):
        result.append(values[i] * k + result[-1] * (1 - k))
    return result


def compute_ema(product: str, period: int) -> Optional[float]:
    mids = get_mids(product)
    if len(mids) < period:
        return None
    ema_vals = _ema(mids, period)
    return ema_vals[-1] if ema_vals else None


def compute_rsi(product: str, period: int = RSI_PERIOD) -> Optional[float]:
    mids = get_mids(product)
    if len(mids) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(mids)):
        delta = mids[i] - mids[i - 1]
        if delta > 0:
            gains.append(delta)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(delta))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_macd(product: str) -> Optional[dict]:
    mids = get_mids(product)
    if len(mids) < MACD_SLOW + MACD_SIGNAL:
        return None

    fast_ema = _ema(mids, MACD_FAST)
    slow_ema = _ema(mids, MACD_SLOW)
    macd_line = [fast_ema[i] - slow_ema[i] for i in range(len(mids))]
    signal_line = _ema(macd_line[MACD_SLOW - 1:], MACD_SIGNAL)

    if not signal_line:
        return None

    current_macd = macd_line[-1]
    current_signal = signal_line[-1]
    histogram = current_macd - current_signal

    prev_macd = macd_line[-2] if len(macd_line) > 1 else current_macd
    prev_signal = signal_line[-2] if len(signal_line) > 1 else current_signal

    crossover = None
    if prev_macd <= prev_signal and current_macd > current_signal:
        crossover = "bullish"
    elif prev_macd >= prev_signal and current_macd < current_signal:
        crossover = "bearish"

    return {
        "macd": round(current_macd, 6),
        "signal": round(current_signal, 6),
        "histogram": round(histogram, 6),
        "crossover": crossover,
    }


def compute_bollinger(product: str, period: int = BB_PERIOD) -> Optional[dict]:
    mids = get_mids(product)
    if len(mids) < period:
        return None

    window = mids[-period:]
    sma = sum(window) / period
    variance = sum((x - sma) ** 2 for x in window) / period
    std_dev = math.sqrt(variance) if variance > 0 else 0

    upper = sma + BB_STD_DEV * std_dev
    lower = sma - BB_STD_DEV * std_dev
    current = mids[-1]

    bandwidth = ((upper - lower) / sma * 100) if sma > 0 else 0
    pct_b = ((current - lower) / (upper - lower)) if (upper - lower) > 0 else 0.5

    return {
        "upper": round(upper, 4),
        "middle": round(sma, 4),
        "lower": round(lower, 4),
        "bandwidth": round(bandwidth, 4),
        "pct_b": round(pct_b, 4),
    }


def compute_price_change(product: str, minutes: int) -> Optional[float]:
    mids = get_mids(product)
    points_needed = int(minutes * 60 / TICK_INTERVAL_SECONDS)
    if len(mids) < points_needed + 1:
        return None
    old_price = mids[-(points_needed + 1)]
    current = mids[-1]
    if old_price <= 0:
        return None
    return ((current - old_price) / old_price) * 100.0


def compute_volatility(product: str, window: int = 20) -> Optional[float]:
    mids = get_mids(product)
    if len(mids) < window + 1:
        return None
    returns = []
    for i in range(len(mids) - window, len(mids)):
        if mids[i - 1] > 0:
            returns.append((mids[i] - mids[i - 1]) / mids[i - 1])
    if len(returns) < 2:
        return None
    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(variance) * 100.0


def get_signal_summary(product: str, timeframe_minutes: int = 60) -> Optional[str]:
    rsi = compute_rsi(product)
    if rsi is None:
        return None

    signals = []

    if rsi < 30:
        signals.append("oversold")
    elif rsi > 70:
        signals.append("overbought")
    elif rsi < 45:
        signals.append("bearish_pressure")
    elif rsi > 55:
        signals.append("bullish_pressure")
    else:
        signals.append("neutral")

    ema_fast = compute_ema(product, EMA_FAST)
    ema_slow = compute_ema(product, EMA_SLOW)
    if ema_fast is not None and ema_slow is not None:
        if ema_fast > ema_slow:
            signals.append("ema_bullish")
        else:
            signals.append("ema_bearish")

    macd = compute_macd(product)
    if macd:
        if macd["crossover"] == "bullish":
            signals.append("macd_bullish_cross")
        elif macd["crossover"] == "bearish":
            signals.append("macd_bearish_cross")
        elif macd["histogram"] > 0:
            signals.append("macd_positive")
        else:
            signals.append("macd_negative")

    bb = compute_bollinger(product)
    if bb:
        if bb["pct_b"] < 0.1:
            signals.append("bb_oversold")
        elif bb["pct_b"] > 0.9:
            signals.append("bb_overbought")

    return ",".join(signals)


def get_full_technicals(product: str) -> dict:
    mids = get_mids(product)
    current_price = mids[-1] if mids else 0

    result = {
        "product": product.upper(),
        "current_price": current_price,
        "data_points": len(mids),
        "rsi_14": compute_rsi(product),
        "ema_9": compute_ema(product, EMA_FAST),
        "ema_21": compute_ema(product, EMA_MID),
        "ema_50": compute_ema(product, EMA_SLOW),
        "macd": compute_macd(product),
        "bollinger": compute_bollinger(product),
        "volatility_20": compute_volatility(product),
        "change_5m": compute_price_change(product, 5),
        "change_15m": compute_price_change(product, 15),
        "change_1h": compute_price_change(product, 60),
        "change_4h": compute_price_change(product, 240),
        "signal_1h": get_signal_summary(product, 60),
    }

    spread_history = [p.get("spread_bp", 0) for p in get_history(product, limit=20)]
    if spread_history:
        result["avg_spread_bp"] = round(sum(spread_history) / len(spread_history), 2)

    return result


def classify_regime(product: str) -> Optional[str]:
    mids = get_mids(product)
    if len(mids) < EMA_SLOW + 5:
        return None

    ema_21_vals = _ema(mids, EMA_MID)
    ema_50_vals = _ema(mids, EMA_SLOW)

    slope_21 = (ema_21_vals[-1] - ema_21_vals[-5]) / max(1e-8, ema_21_vals[-5]) * 100 if len(ema_21_vals) >= 5 else 0
    slope_50 = (ema_50_vals[-1] - ema_50_vals[-5]) / max(1e-8, ema_50_vals[-5]) * 100 if len(ema_50_vals) >= 5 else 0

    bb = compute_bollinger(product)
    vol = compute_volatility(product)

    bb_width = bb["bandwidth"] if bb else 0
    current_vol = vol if vol is not None else 0

    high_vol_threshold = 0.5
    trend_slope_threshold = 0.15

    if current_vol > high_vol_threshold and bb_width > 4.0:
        change_5m = compute_price_change(product, 5)
        if change_5m is not None and abs(change_5m) > 1.5:
            return "news_spike"
        return "high_vol_chop"

    if slope_21 > trend_slope_threshold and slope_50 > 0:
        return "trending_up"
    if slope_21 < -trend_slope_threshold and slope_50 < 0:
        return "trending_down"

    return "range"


def get_all_technicals() -> dict[str, dict]:
    results = {}
    for product, info in PRODUCTS.items():
        if info["type"] != "perp":
            continue
        tech = get_full_technicals(product)
        if tech.get("data_points", 0) > 0:
            results[product] = tech
    return results


def get_tracker_status() -> dict:
    status = {}
    for product in _price_history:
        history = _price_history[product]
        status[product] = {
            "points": len(history),
            "oldest_ts": history[0]["ts"] if history else None,
            "newest_ts": history[-1]["ts"] if history else None,
            "current_mid": history[-1]["mid"] if history else None,
        }
    return status
