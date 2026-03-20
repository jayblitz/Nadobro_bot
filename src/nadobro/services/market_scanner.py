import logging
import time
from datetime import datetime
from typing import Optional

from src.nadobro.services.price_tracker import get_full_technicals, get_all_technicals, classify_regime
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

_sentiment_cache: dict = {}
SENTIMENT_CACHE_TTL = 300


def _get_cmc_data(products: list[str]) -> dict:
    try:
        from src.nadobro.services.cmc_client import (
            get_crypto_quotes,
            get_fear_greed_index,
            get_global_metrics,
        )
        quotes = get_crypto_quotes(products)
        fng = get_fear_greed_index()
        global_metrics = get_global_metrics()
        return {
            "quotes": quotes,
            "fear_greed": fng,
            "global_metrics": global_metrics,
        }
    except Exception as e:
        logger.warning("CMC data fetch failed: %s", e)
        return {"quotes": {}, "fear_greed": {}, "global_metrics": {}}


def _get_funding_rates(client, products: list[str]) -> dict[str, float]:
    rates = {}
    for product in products:
        try:
            from src.nadobro.config import get_product_id

            pid = get_product_id(product, network=getattr(client, "network", "mainnet"), client=client)
            if pid is not None:
                rate = client.get_funding_rate(pid)
                if rate is not None:
                    rates[product] = float(rate)
        except Exception as e:
            logger.debug("Funding rate fetch failed for %s: %s", product, e)
    return rates


def _get_sentiment(products: list[str]) -> Optional[str]:
    cache_key = "bro_sentiment"
    cached = _sentiment_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < SENTIMENT_CACHE_TTL:
        return cached["data"]

    try:
        import os
        from openai import OpenAI

        api_key = os.environ.get("XAI_API_KEY")
        if not api_key:
            return None

        client = OpenAI(api_key=api_key, base_url="https://api.x.ai/v1")
        now = datetime.utcnow()
        symbols_str = ", ".join(products)

        response = client.chat.completions.create(
            model="grok-3",
            messages=[
                {
                    "role": "system",
                    "content": (
                        f"Today is {now.strftime('%Y-%m-%d %H:%M UTC')}. "
                        "Search crypto Twitter for the latest market sentiment on these assets: "
                        f"{symbols_str}. Focus on recent price action commentary, whale movements, "
                        "breaking news, and analyst opinions from major accounts. "
                        "Be concise. Report bullish/bearish signals per asset. Plain text only."
                    ),
                },
                {"role": "user", "content": f"What's the current Twitter sentiment for {symbols_str}?"},
            ],
            max_tokens=600,
            temperature=0.1,
            extra_body={"search_parameters": {"mode": "on", "sources": [{"type": "x"}]}},
        )

        text = response.choices[0].message.content or ""
        _sentiment_cache[cache_key] = {"data": text, "ts": time.time()}
        return text
    except Exception as e:
        logger.warning("Sentiment fetch failed: %s", e)
        return None


def build_market_snapshot(
    client,
    products: list[str],
    use_cmc: bool = True,
    use_sentiment: bool = True,
) -> dict:
    technicals = {}
    for product in products:
        tech = get_full_technicals(product)
        if tech.get("data_points", 0) > 0:
            technicals[product] = tech

    funding_rates = _get_funding_rates(client, products)

    cmc_data = _get_cmc_data(products) if use_cmc else {}

    sentiment = _get_sentiment(products) if use_sentiment else None

    assets = []
    for product in products:
        tech = technicals.get(product, {})
        cmc_quote = cmc_data.get("quotes", {}).get(product, {})
        funding = funding_rates.get(product)

        asset = {
            "product": product,
            "current_price": tech.get("current_price", 0),
            "data_points": tech.get("data_points", 0),
        }

        if tech.get("rsi_14") is not None:
            asset["rsi_14"] = round(tech["rsi_14"], 1)
        if tech.get("ema_9") is not None:
            asset["ema_9"] = round(tech["ema_9"], 2)
        if tech.get("ema_21") is not None:
            asset["ema_21"] = round(tech["ema_21"], 2)
        if tech.get("ema_50") is not None:
            asset["ema_50"] = round(tech["ema_50"], 2)
        if tech.get("macd"):
            asset["macd"] = tech["macd"]
        if tech.get("bollinger"):
            asset["bollinger"] = tech["bollinger"]
        if tech.get("volatility_20") is not None:
            asset["volatility"] = round(tech["volatility_20"], 3)
        if tech.get("signal_1h"):
            asset["signal_1h"] = tech["signal_1h"]

        for tf in ["change_5m", "change_15m", "change_1h", "change_4h"]:
            if tech.get(tf) is not None:
                asset[tf] = round(tech[tf], 3)

        if tech.get("avg_spread_bp") is not None:
            asset["spread_bp"] = tech["avg_spread_bp"]

        regime = classify_regime(product)
        if regime:
            asset["regime"] = regime

        if funding is not None:
            asset["funding_rate"] = funding

        if cmc_quote:
            asset["cmc"] = {
                "change_1h": cmc_quote.get("change_1h", 0),
                "change_24h": cmc_quote.get("change_24h", 0),
                "change_7d": cmc_quote.get("change_7d", 0),
                "volume_24h": cmc_quote.get("volume_24h", 0),
                "market_cap": cmc_quote.get("market_cap", 0),
            }

        assets.append(asset)

    snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "assets": assets,
    }

    fng = cmc_data.get("fear_greed", {})
    if fng:
        snapshot["fear_greed"] = {
            "value": fng.get("value", 0),
            "label": fng.get("value_classification", "N/A"),
        }

    gm = cmc_data.get("global_metrics", {})
    if gm:
        snapshot["global"] = {
            "total_market_cap": gm.get("total_market_cap", 0),
            "btc_dominance": gm.get("btc_dominance", 0),
            "market_cap_change_24h": gm.get("total_market_cap_change_24h", 0),
        }

    if sentiment:
        snapshot["sentiment"] = sentiment

    return snapshot


def format_snapshot_for_llm(snapshot: dict, max_chars: int = 6000) -> str:
    lines = [f"MARKET SNAPSHOT — {snapshot.get('timestamp', 'N/A')}\n"]

    fng = snapshot.get("fear_greed", {})
    if fng:
        lines.append(f"Fear & Greed: {fng.get('value', '?')}/100 ({fng.get('label', '?')})")

    gm = snapshot.get("global", {})
    if gm:
        mc = gm.get("total_market_cap", 0)
        btc_dom = gm.get("btc_dominance", 0)
        mc_chg = gm.get("market_cap_change_24h", 0)
        lines.append(f"Total Crypto Market Cap: ${mc/1e12:.2f}T ({'+' if mc_chg >= 0 else ''}{mc_chg:.1f}% 24h) | BTC Dom: {btc_dom:.1f}%")

    lines.append("")

    for asset in snapshot.get("assets", []):
        product = asset.get("product", "?")
        price = asset.get("current_price", 0)
        parts = [f"--- {product} @ ${price:,.2f} ---"]

        rsi = asset.get("rsi_14")
        if rsi is not None:
            parts.append(f"RSI(14): {rsi:.1f}")

        for ema_key in ["ema_9", "ema_21", "ema_50"]:
            if asset.get(ema_key) is not None:
                parts.append(f"{ema_key.upper()}: ${asset[ema_key]:,.2f}")

        macd = asset.get("macd")
        if macd:
            parts.append(f"MACD: {macd['histogram']:.6f} ({'CROSS ' + macd['crossover'].upper() if macd.get('crossover') else 'no cross'})")

        bb = asset.get("bollinger")
        if bb:
            parts.append(f"BB: [{bb['lower']:.2f} — {bb['middle']:.2f} — {bb['upper']:.2f}] %B={bb['pct_b']:.2f}")

        vol = asset.get("volatility")
        if vol is not None:
            parts.append(f"Volatility: {vol:.3f}%")

        changes = []
        for tf in ["change_5m", "change_15m", "change_1h", "change_4h"]:
            v = asset.get(tf)
            if v is not None:
                label = tf.replace("change_", "")
                changes.append(f"{label}: {'+' if v >= 0 else ''}{v:.2f}%")
        if changes:
            parts.append(f"Price changes: {' | '.join(changes)}")

        signal = asset.get("signal_1h")
        if signal:
            parts.append(f"Signal(1h): {signal}")

        regime = asset.get("regime")
        if regime:
            parts.append(f"Regime: {regime.upper().replace('_', ' ')}")

        fr = asset.get("funding_rate")
        if fr is not None:
            parts.append(f"Funding rate: {fr:.6f}")

        spread = asset.get("spread_bp")
        if spread is not None:
            parts.append(f"Avg spread: {spread:.1f}bp")

        cmc = asset.get("cmc")
        if cmc:
            parts.append(
                f"CMC: 1h {'+' if cmc['change_1h'] >= 0 else ''}{cmc['change_1h']:.1f}% | "
                f"24h {'+' if cmc['change_24h'] >= 0 else ''}{cmc['change_24h']:.1f}% | "
                f"7d {'+' if cmc['change_7d'] >= 0 else ''}{cmc['change_7d']:.1f}% | "
                f"Vol: ${cmc['volume_24h']/1e6:.0f}M"
            )

        lines.append("\n".join(parts))
        lines.append("")

    sentiment = snapshot.get("sentiment")
    if sentiment:
        lines.append(f"TWITTER SENTIMENT:\n{sentiment[:1200]}")

    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n... [truncated]"
    return text
