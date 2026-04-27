import os
import time
import logging
import requests

logger = logging.getLogger(__name__)

CMC_BASE_URL = "https://pro-api.coinmarketcap.com"
CMC_CACHE_TTL = 120
CMC_SENTIMENT_CACHE_TTL = 300

_cache: dict = {}
_CACHE_MAX_ENTRIES = 128

CMC_SYMBOL_ID_MAP = {
    "BTC": 1, "ETH": 1027, "SOL": 5426, "XRP": 52,
    "BNB": 1839, "LINK": 1975, "DOGE": 74,
    "INK": 34618,
}


def _get_api_key() -> str:
    key = os.environ.get("CMC_API_KEY", "")
    if not key:
        raise RuntimeError("CMC_API_KEY not set")
    return key


def _get_cached(key: str, ttl: int = CMC_CACHE_TTL):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < ttl:
        return entry["data"]
    if entry:
        _cache.pop(key, None)
    return None


def _set_cache(key: str, data):
    now = time.time()
    if len(_cache) >= _CACHE_MAX_ENTRIES:
        stale = [k for k, v in _cache.items() if now - float(v.get("ts") or 0) > max(CMC_SENTIMENT_CACHE_TTL, CMC_CACHE_TTL)]
        for k in stale:
            _cache.pop(k, None)
        while len(_cache) >= _CACHE_MAX_ENTRIES:
            oldest = min(_cache, key=lambda k: float(_cache[k].get("ts") or 0))
            _cache.pop(oldest, None)
    _cache[key] = {"data": data, "ts": now}


def _record_cmc_source(detail: str, ttl: int = CMC_CACHE_TTL, confidence: float = 0.9):
    try:
        from src.nadobro.services.source_registry import record_source

        record_source(
            "coinmarketcap",
            ttl_seconds=ttl,
            confidence=confidence,
            source_url="https://coinmarketcap.com",
            license_tier="api",
            allowed_use="analysis",
            detail=detail,
        )
    except Exception:
        pass


def _cmc_get(path: str, params: dict = None, timeout: int = 8) -> dict:
    headers = {
        "X-CMC_PRO_API_KEY": _get_api_key(),
        "Accept": "application/json",
    }
    url = f"{CMC_BASE_URL}{path}"
    resp = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    error_code = data.get("status", {}).get("error_code", 0)
    if str(error_code) not in ("0", ""):
        err = data["status"].get("error_message", "Unknown CMC error")
        raise RuntimeError(f"CMC API error: {err}")
    return data.get("data", {})


def get_crypto_quotes(symbols: list[str]) -> dict:
    cache_key = f"quotes_{'_'.join(sorted(s.upper() for s in symbols))}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    known_ids = []
    unknown_syms = []
    for sym in symbols:
        cmc_id = CMC_SYMBOL_ID_MAP.get(sym.upper())
        if cmc_id:
            known_ids.append(str(cmc_id))
        else:
            unknown_syms.append(sym.upper())

    raw = {}
    if known_ids:
        raw.update(_cmc_get("/v2/cryptocurrency/quotes/latest", {"id": ",".join(known_ids)}))
    if unknown_syms:
        try:
            sym_data = _cmc_get("/v2/cryptocurrency/quotes/latest", {"symbol": ",".join(unknown_syms)})
            for sym_key, info in sym_data.items():
                if isinstance(info, list):
                    for item in info:
                        raw[str(item.get("id", sym_key))] = item
                else:
                    raw[str(info.get("id", sym_key))] = info
        except Exception as e:
            logger.warning(f"CMC symbol lookup failed for {unknown_syms}: {e}")

    if not raw:
        return {}

    result = {}
    for cmc_id_str, info in raw.items():
        if isinstance(info, list):
            info = info[0]
        sym = info.get("symbol", "")
        quote = info.get("quote", {}).get("USD", {})
        result[sym] = {
            "name": info.get("name", sym),
            "symbol": sym,
            "price": quote.get("price", 0),
            "market_cap": quote.get("market_cap", 0),
            "volume_24h": quote.get("volume_24h", 0),
            "change_1h": quote.get("percent_change_1h", 0),
            "change_24h": quote.get("percent_change_24h", 0),
            "change_7d": quote.get("percent_change_7d", 0),
            "change_30d": quote.get("percent_change_30d", 0),
            "market_cap_dominance": quote.get("market_cap_dominance", 0),
            "fully_diluted_market_cap": quote.get("fully_diluted_market_cap", 0),
        }

    _set_cache(cache_key, result)
    _record_cmc_source(f"CMC quotes: {', '.join(sorted(result.keys()))}")
    return result


def get_fear_greed_index() -> dict:
    cached = _get_cached("fear_greed", CMC_SENTIMENT_CACHE_TTL)
    if cached:
        return cached

    raw = _cmc_get("/v3/fear-and-greed/latest")
    result = {
        "value": raw.get("value", 0),
        "value_classification": raw.get("value_classification", "N/A"),
        "update_time": raw.get("update_time", ""),
    }
    _set_cache("fear_greed", result)
    _record_cmc_source("CMC Fear & Greed", ttl=CMC_SENTIMENT_CACHE_TTL)
    return result


def get_global_metrics() -> dict:
    cached = _get_cached("global_metrics", CMC_CACHE_TTL)
    if cached:
        return cached

    raw = _cmc_get("/v1/global-metrics/quotes/latest")
    quote = raw.get("quote", {}).get("USD", {})
    result = {
        "total_market_cap": quote.get("total_market_cap", 0),
        "total_volume_24h": quote.get("total_volume_24h", 0),
        "btc_dominance": raw.get("btc_dominance", 0),
        "eth_dominance": raw.get("eth_dominance", 0),
        "active_cryptocurrencies": raw.get("active_cryptocurrencies", 0),
        "total_market_cap_change_24h": quote.get("total_market_cap_yesterday_percentage_change", 0),
    }
    _set_cache("global_metrics", result)
    _record_cmc_source("CMC global metrics")
    return result


def get_trending() -> dict:
    cached = _get_cached("trending", CMC_CACHE_TTL)
    if cached:
        return cached

    result = {"trending": [], "gainers": [], "losers": []}

    try:
        trending_raw = _cmc_get("/v1/cryptocurrency/trending/latest", {"limit": 10})
        if isinstance(trending_raw, list):
            for coin in trending_raw[:10]:
                quote = coin.get("quote", {}).get("USD", {})
                result["trending"].append({
                    "name": coin.get("name", ""),
                    "symbol": coin.get("symbol", ""),
                    "price": quote.get("price", 0),
                    "change_24h": quote.get("percent_change_24h", 0),
                })
    except Exception as e:
        logger.warning(f"CMC trending fetch failed: {e}")

    try:
        gl_raw = _cmc_get("/v1/cryptocurrency/trending/gainers-losers", {"limit": 5, "time_period": "24h"})
        if isinstance(gl_raw, list):
            for coin in gl_raw:
                quote = coin.get("quote", {}).get("USD", {})
                entry = {
                    "name": coin.get("name", ""),
                    "symbol": coin.get("symbol", ""),
                    "price": quote.get("price", 0),
                    "change_24h": quote.get("percent_change_24h", 0),
                }
                if entry["change_24h"] >= 0:
                    result["gainers"].append(entry)
                else:
                    result["losers"].append(entry)
            result["gainers"] = sorted(result["gainers"], key=lambda x: x["change_24h"], reverse=True)[:5]
            result["losers"] = sorted(result["losers"], key=lambda x: x["change_24h"])[:5]
    except Exception as e:
        logger.warning(f"CMC gainers/losers fetch failed: {e}")

    _set_cache("trending", result)
    _record_cmc_source("CMC trending/gainers/losers")
    return result


def get_latest_news(limit: int = 5) -> list[dict]:
    cached = _get_cached("news", CMC_SENTIMENT_CACHE_TTL)
    if cached:
        return cached

    try:
        raw = _cmc_get("/v1/content/latest", {"limit": limit})
        articles = []
        if isinstance(raw, list):
            for item in raw[:limit]:
                articles.append({
                    "title": item.get("title", ""),
                    "subtitle": item.get("subtitle", ""),
                    "source": item.get("source_name", ""),
                    "created_at": item.get("created_at", ""),
                    "slug": item.get("slug", ""),
                })
        _set_cache("news", articles)
        _record_cmc_source("CMC latest news", ttl=CMC_SENTIMENT_CACHE_TTL)
        return articles
    except Exception as e:
        logger.warning(f"CMC news fetch failed: {e}")
        return []


def format_crypto_quote(data: dict) -> str:
    sym = data.get("symbol", "?")
    name = data.get("name", sym)
    price = data.get("price") or 0
    mc = data.get("market_cap") or 0
    vol = data.get("volume_24h") or 0
    c1h = data.get("change_1h") or 0
    c24h = data.get("change_24h") or 0
    c7d = data.get("change_7d") or 0
    c30d = data.get("change_30d") or 0
    dom = data.get("market_cap_dominance") or 0

    arrow_24h = "+" if c24h >= 0 else ""
    arrow_7d = "+" if c7d >= 0 else ""
    arrow_1h = "+" if c1h >= 0 else ""
    arrow_30d = "+" if c30d >= 0 else ""

    lines = [
        f"{name} ({sym})",
        f"  Price: ${price:,.2f}",
        f"  1h: {arrow_1h}{c1h:.2f}% | 24h: {arrow_24h}{c24h:.2f}% | 7d: {arrow_7d}{c7d:.2f}% | 30d: {arrow_30d}{c30d:.2f}%",
        f"  Market Cap: ${mc:,.0f}",
        f"  24h Volume: ${vol:,.0f}",
    ]
    if dom > 0.01:
        lines.append(f"  Dominance: {dom:.2f}%")
    return "\n".join(lines)


def format_global_metrics(data: dict) -> str:
    mc = data.get("total_market_cap") or 0
    vol = data.get("total_volume_24h") or 0
    btc_dom = data.get("btc_dominance") or 0
    eth_dom = data.get("eth_dominance") or 0
    active = data.get("active_cryptocurrencies") or 0
    mc_change = data.get("total_market_cap_change_24h") or 0
    arrow = "+" if mc_change >= 0 else ""

    return (
        f"Total Market Cap: ${mc:,.0f} ({arrow}{mc_change:.2f}% 24h)\n"
        f"24h Volume: ${vol:,.0f}\n"
        f"BTC Dominance: {btc_dom:.1f}%\n"
        f"ETH Dominance: {eth_dom:.1f}%\n"
        f"Active Cryptocurrencies: {active:,}"
    )


def format_trending(data: dict) -> str:
    lines = []
    if data.get("trending"):
        lines.append("TRENDING ON COINMARKETCAP:")
        for i, coin in enumerate(data["trending"][:8], 1):
            arrow = "+" if coin["change_24h"] >= 0 else ""
            lines.append(f"  {i}. {coin['symbol']} (${coin['price']:,.2f}) {arrow}{coin['change_24h']:.1f}%")

    if data.get("gainers"):
        lines.append("\nTOP GAINERS (24h):")
        for coin in data["gainers"][:5]:
            lines.append(f"  {coin['symbol']}: +{coin['change_24h']:.1f}% (${coin['price']:,.4f})")

    if data.get("losers"):
        lines.append("\nTOP LOSERS (24h):")
        for coin in data["losers"][:5]:
            lines.append(f"  {coin['symbol']}: {coin['change_24h']:.1f}% (${coin['price']:,.4f})")

    return "\n".join(lines) if lines else "No trending data available."


def format_fear_greed(data: dict) -> str:
    val = data.get("value", 0)
    classification = data.get("value_classification", "N/A")
    return f"Fear & Greed Index: {val}/100 ({classification})"
