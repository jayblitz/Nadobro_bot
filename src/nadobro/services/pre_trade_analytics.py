"""
Pre-Trade Analytics (Tread.fi-style).
Provides Participation Rate, Market Volatility, and Market Volume metrics
to assess market conditions before execution.
"""
import logging
import math
import time
from typing import Optional

from src.nadobro.config import get_product_id, NADO_MAINNET_ARCHIVE, NADO_TESTNET_ARCHIVE

logger = logging.getLogger(__name__)

_ANALYTICS_CACHE: dict = {}
_ANALYTICS_CACHE_TTL = 60


def _x18(val) -> float:
    try:
        v = int(val) if isinstance(val, str) else val
        return float(v) / 1e18
    except Exception:
        return 0.0


def _query_archive(network: str, payload: dict, timeout: float = 8) -> Optional[dict]:
    """Query Nado archive. Network: testnet|mainnet."""
    import requests
    url = NADO_MAINNET_ARCHIVE if str(network).lower() == "mainnet" else NADO_TESTNET_ARCHIVE
    try:
        resp = requests.post(
            url,
            json=payload,
            headers={"Accept-Encoding": "gzip", "Content-Type": "application/json"},
            timeout=timeout,
        )
        return resp.json()
    except Exception as e:
        logger.warning("Pre-trade archive query failed: %s", e)
        return None


def get_pre_trade_analytics(
    product: str,
    order_notional_usd: float,
    order_duration_hours: float,
    network: str = "mainnet",
    client=None,
) -> dict:
    """
    Compute Participation Rate, Market Volatility, and Market Volume.
    Returns dict with participation_rate_pct, market_volatility_1s_pct, market_volume_ratio, market_24h_volume_usd, data_ok.
    """
    product_id = get_product_id(product)
    if product_id is None:
        return {"data_ok": False, "error": "Unknown product"}

    cache_key = f"{network}:{product}:{int(time.time() // _ANALYTICS_CACHE_TTL)}"
    cached = _ANALYTICS_CACHE.get(cache_key)
    if cached:
        return _enrich_with_order(cached, order_notional_usd, order_duration_hours)

    archive_client = client
    if client is None:
        archive_client = _create_archive_client(network)

    payload = {
        "candlesticks": {
            "product_id": product_id,
            "granularity": 3600,
            "limit": 48,
        }
    }
    if archive_client is not None and hasattr(archive_client, "query_archive"):
        resp = archive_client.query_archive(payload)
    else:
        resp = _query_archive(network, payload)

    if not resp or "candlesticks" not in resp:
        return {"data_ok": False, "error": "No candlestick data", "participation_rate_pct": None, "market_volatility_1s_pct": None, "market_volume_ratio": None}

    candles = resp.get("candlesticks") or []
    if len(candles) < 12:
        return {"data_ok": False, "error": "Insufficient price history (need 12h)", "participation_rate_pct": None, "market_volatility_1s_pct": None, "market_volume_ratio": None}

    closes = []
    volumes_usd = []
    for c in candles[:24]:
        close = _x18(c.get("close_x18", 0))
        vol_base = _x18(c.get("volume", 0))
        if close > 0 and vol_base >= 0:
            closes.append(close)
            volumes_usd.append(abs(vol_base) * close)

    if len(closes) < 2:
        return {"data_ok": False, "error": "Insufficient closes", "participation_rate_pct": None, "market_volatility_1s_pct": None, "market_volume_ratio": None}

    market_24h_usd = sum(volumes_usd)
    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            r = math.log(closes[i] / closes[i - 1])
            returns.append(r)
    variance = sum(r * r for r in returns) / max(len(returns), 1)
    vol_hourly_pct = math.sqrt(variance) * 100 if returns else 0.0

    if len(candles) >= 48:
        vol_day2 = sum(_x18(c.get("volume", 0)) * _x18(c.get("close_x18", 0)) for c in candles[24:48] if _x18(c.get("close_x18", 0)) > 0)
        expected_24h = (market_24h_usd + vol_day2) / 2.0
    else:
        expected_24h = market_24h_usd
    market_volume_ratio = market_24h_usd / expected_24h if expected_24h > 0 else 1.0

    base = {
        "data_ok": True,
        "market_24h_volume_usd": market_24h_usd,
        "market_volatility_1s_pct": round(vol_hourly_pct, 4),
        "market_volatility_projected_pct": round(vol_hourly_pct * math.sqrt(max(order_duration_hours, 0.016)), 4),
        "market_volume_ratio": round(market_volume_ratio, 3),
    }
    _ANALYTICS_CACHE[cache_key] = base
    return _enrich_with_order(base, order_notional_usd, order_duration_hours)


def _enrich_with_order(base: dict, order_notional_usd: float, order_duration_hours: float) -> dict:
    market_24h = base.get("market_24h_volume_usd") or 0
    duration = max(order_duration_hours, 0.016)
    if market_24h > 0:
        expected_volume_over_duration = market_24h * (duration / 24.0)
        participation = (order_notional_usd / expected_volume_over_duration) * 100 if expected_volume_over_duration > 0 else 0
    else:
        participation = None
    base["participation_rate_pct"] = round(participation, 4) if participation is not None else None
    return base


def _create_archive_client(network: str):
    """Create a minimal client for archive queries when user has no wallet."""
    try:
        from src.nadobro.services.nado_client import NadoClient
        return NadoClient.from_address("0x0000000000000000000000000000000000000000", network)
    except Exception:
        return None
