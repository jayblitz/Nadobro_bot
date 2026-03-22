import re
from typing import Optional

from src.nadobro.config import get_perp_products

TRADE_KEYWORDS = ("buy", "sell", "long", "short", "market", "limit")


def _extract_product(text_lower: str, network: str = "mainnet", client=None) -> Optional[str]:
    for symbol in get_perp_products(network=network, client=client):
        checks = (symbol.lower(), f"{symbol.lower()}-perp")
        if any(token and re.search(rf"\b{re.escape(token)}\b", text_lower) for token in checks):
            return symbol
    return None


def _extract_direction(text_lower: str) -> Optional[str]:
    if re.search(r"\b(buy|long)\b", text_lower):
        return "long"
    if re.search(r"\b(sell|short)\b", text_lower):
        return "short"
    return None


def parse_trade_intent(text: str, network: str = "mainnet", client=None) -> Optional[dict]:
    raw = text.strip()
    if not raw:
        return None
    text_lower = raw.lower()
    if not any(word in text_lower for word in TRADE_KEYWORDS):
        return None

    direction = _extract_direction(text_lower)
    product = _extract_product(text_lower, network=network, client=client)
    order_type = "limit" if re.search(r"\blimit\b", text_lower) else "market"

    leverage = None
    lev_match = re.search(r"\b(\d+(?:\.\d+)?)\s*x\b", text_lower)
    if lev_match:
        try:
            leverage = int(float(lev_match.group(1)))
        except (TypeError, ValueError):
            leverage = None

    tp = None
    sl = None
    limit_price = None
    consumed = []

    tp_match = re.search(r"\btp\s*[:=]?\s*(\d+(?:\.\d+)?)\b", text_lower)
    if tp_match:
        tp = float(tp_match.group(1))
        consumed.append(tp_match.span(1))

    sl_match = re.search(r"\bsl\s*[:=]?\s*(\d+(?:\.\d+)?)\b", text_lower)
    if sl_match:
        sl = float(sl_match.group(1))
        consumed.append(sl_match.span(1))

    limit_match = re.search(r"\blimit(?:\s+at|\s+price|@)?\s*(\d+(?:\.\d+)?)\b", text_lower)
    if limit_match:
        try:
            limit_price = float(limit_match.group(1))
            consumed.append(limit_match.span(1))
        except (TypeError, ValueError):
            limit_price = None

    if lev_match:
        consumed.append(lev_match.span(1))

    numeric_tokens = []
    for m in re.finditer(r"\b\d+(?:\.\d+)?\b", text_lower):
        span = m.span(0)
        if any(span[0] >= c[0] and span[1] <= c[1] for c in consumed):
            continue
        try:
            numeric_tokens.append((float(m.group(0)), span))
        except ValueError:
            pass

    size = numeric_tokens[0][0] if numeric_tokens else None
    if order_type == "limit" and limit_price is None and len(numeric_tokens) >= 2:
        size = numeric_tokens[0][0]
        limit_price = numeric_tokens[1][0]

    looks_like_trade = bool(direction or (product and (size is not None or order_type == "limit")))
    if not looks_like_trade:
        return None

    missing = []
    if not direction:
        missing.append("side")
    if not product:
        missing.append("product")
    if size is None:
        missing.append("size")
    if order_type == "limit" and limit_price is None:
        missing.append("limit_price")

    return {
        "kind": "trade",
        "direction": direction,
        "product": product,
        "order_type": order_type,
        "size": size,
        "leverage": leverage,
        "limit_price": limit_price,
        "tp": tp,
        "sl": sl,
        "missing": missing,
        "raw": raw,
    }


def _looks_like_question(text_lower: str) -> bool:
    if "?" in text_lower:
        return True
    return bool(
        re.match(
            r"^\s*(what|why|how|when|where|who|can|could|would|should|is|are|do|does|did)\b",
            text_lower,
        )
    )


def parse_interaction_intent(text: str, network: str = "mainnet", client=None) -> Optional[dict]:
    raw = (text or "").strip()
    if not raw:
        return None

    text_lower = raw.lower()
    words = re.findall(r"[a-z0-9]+", text_lower)
    if not words:
        return None
    is_short_command = len(words) <= 5

    # PnL/account-profit checks should route to live portfolio data, even when
    # phrased as a question.
    if re.search(r"\b(pnl|profit|unrealized|realized)\b", text_lower) and re.search(
        r"\b(my|current|now|account|portfolio|positions?|am i|how much)\b",
        text_lower,
    ):
        return {"kind": "interaction", "action": "open_view", "target": "portfolio:view", "raw": raw}

    # Position-closing intents.
    if re.search(r"\b(close|exit)\b", text_lower):
        if re.search(r"\b(all|everything)\b", text_lower) and re.search(
            r"\b(position|positions|order|orders|trade|trades)\b",
            text_lower,
        ):
            return {"kind": "interaction", "action": "close_all", "raw": raw}

        product = _extract_product(text_lower, network=network, client=client)
        if product:
            return {
                "kind": "interaction",
                "action": "close_product",
                "product": product,
                "raw": raw,
            }

        if re.search(r"\b(position|positions|order|orders|trade|trades)\b", text_lower):
            return {"kind": "interaction", "action": "close_menu", "raw": raw}

    # Home/navigation intents (only for short command-like text to avoid hijacking Q&A).
    if is_short_command and not _looks_like_question(text_lower):
        if re.search(r"\b(portfolio|portofolio|pnl|profit)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "portfolio:view", "raw": raw}
        if re.search(r"\b(strategy|strategies)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "nav:strategy_hub", "raw": raw}
        if re.search(r"\b(settings?|config)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "settings:view", "raw": raw}
        if re.search(r"\b(alert|alerts)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "alert:menu", "raw": raw}
        if re.search(r"\b(wallet|balance|funds?)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "wallet:view", "raw": raw}
        if re.search(r"\b(position|positions|portfolio)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "pos:view", "raw": raw}
        if re.search(r"\b(points?|rewards?|radar)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "points:view", "raw": raw}
        if re.search(r"\b(mode|network|testnet|mainnet)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "nav:mode", "raw": raw}
        if re.search(r"\b(trade|long|short|buy|sell)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "nav:trade", "raw": raw}

    return None


def _looks_like_bracket_command(text_lower: str) -> bool:
    """NL TP/SL: avoid hijacking generic questions like \"what is tp?\"."""
    if re.search(r"\b(?:set|place|put|add|create)\b", text_lower):
        return True
    if re.search(r"\b(?:for|on)\s+(?:my|the|an)\b", text_lower):
        return True
    if re.search(r"\border\b", text_lower):
        return True
    if re.match(r"^\s*(?:tp|sl|take\s*profit|stop\s*loss)\s+\d", text_lower):
        return True
    if re.match(r"^\s*\w+\s+(?:tp|sl)\s+\d", text_lower):
        return True
    if re.search(r"\b(?:tp|take\s*-?profit|sl|stop\s*-?loss)\b", text_lower) and re.search(
        r"(?:at|@)\s*\d", text_lower
    ):
        return True
    return False


def _extract_tp_sl_prices(text_lower: str) -> tuple[Optional[float], Optional[float]]:
    tp = None
    sl = None
    if re.search(r"\b(?:tp|take\s*-?profit)\b", text_lower):
        m = re.search(r"\b(?:tp|take\s*-?profit)\b.*?(?:at|@)\s*(\d+(?:\.\d+)?)\b", text_lower)
        if m:
            tp = float(m.group(1))
        else:
            m2 = re.search(
                r"\b(?:tp|take\s*-?profit)\b\D{0,40}?(\d+(?:\.\d+)?)\b",
                text_lower,
            )
            if m2:
                tp = float(m2.group(1))
    if re.search(r"\b(?:sl|stop\s*-?loss)\b", text_lower):
        m = re.search(r"\b(?:sl|stop\s*-?loss)\b.*?(?:at|@)\s*(\d+(?:\.\d+)?)\b", text_lower)
        if m:
            sl = float(m.group(1))
        else:
            m2 = re.search(
                r"\b(?:sl|stop\s*-?loss)\b\D{0,40}?(\d+(?:\.\d+)?)\b",
                text_lower,
            )
            if m2:
                sl = float(m2.group(1))
    return tp, sl


def _parse_close_all_nl(text_lower: str) -> bool:
    if re.search(r"\b(close|exit|flatten|square|liquidate)\s+(all|everything)\b", text_lower):
        return True
    if re.search(r"\b(close|exit)\s+all\s+(positions|open\s+positions|my\s+positions)\b", text_lower):
        return True
    if re.search(r"\bmarket\s+close\s+(all|everything)\b", text_lower):
        return True
    if re.search(r"\b(close|exit)\s+everything\b", text_lower):
        return True
    if re.search(r"\b(all|everything)\s+(positions|open\s+positions)\b", text_lower) and re.search(
        r"\b(close|exit|flatten)\b", text_lower
    ):
        return True
    return False


def _parse_limit_close_nl(text_lower: str, network: str, client) -> Optional[dict]:
    has_close = bool(
        re.search(r"\b(close|exit|flatten|reduce)\b", text_lower)
        or re.search(r"\blimit\s+close\b", text_lower)
    )
    if not has_close:
        return None
    product = _extract_product(text_lower, network=network, client=client)
    if not product:
        return None
    price = None
    m = re.search(r"(?:at|@)\s*(\d+(?:\.\d+)?)\b", text_lower)
    if m:
        price = float(m.group(1))
    else:
        m2 = re.search(rf"\b{re.escape(product.lower())}\s+(\d+(?:\.\d+)?)\b", text_lower)
        if m2:
            price = float(m2.group(1))
    if price is None or price <= 0:
        return None
    # Partial: "close 0.5 btc at 3000"
    size = None
    m3 = re.search(
        rf"\b(?:close|exit|reduce)\s+(\d+(?:\.\d+)?)\s+{re.escape(product.lower())}\b",
        text_lower,
    )
    if m3:
        size = float(m3.group(1))
    return {
        "kind": "position",
        "action": "limit_close",
        "product": product,
        "limit_price": price,
        "size": size,
    }


def _parse_market_close_nl(text_lower: str, network: str, client) -> Optional[dict]:
    has_close = bool(
        re.search(r"\b(close|exit|flatten|square|reduce|liquidate)\b", text_lower)
        or re.search(r"\bmarket\s+close\b", text_lower)
    )
    if not has_close:
        return None
    product = _extract_product(text_lower, network=network, client=client)
    if not product:
        return None
    # Routed to limit close when an explicit exit price is present
    if re.search(r"(?:at|@)\s*(\d+(?:\.\d+)?)\b", text_lower):
        return None
    size = None
    m = re.search(
        rf"\b(?:close|exit|reduce)\s+(\d+(?:\.\d+)?)\s+{re.escape(product.lower())}\b",
        text_lower,
    )
    if m:
        size = float(m.group(1))
    return {
        "kind": "position",
        "action": "close_market",
        "product": product,
        "size": size,
    }


def parse_position_management_intent(text: str, network: str = "mainnet", client=None) -> Optional[dict]:
    """
    Natural-language position management: TP/SL, market close, limit close, close all.
    Does not run when parse_trade_intent is a complete open-order intent.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    text_lower = raw.lower()

    # Do not hijack Q&A (e.g. "how do I close all positions?")
    if raw.endswith("?") and _looks_like_question(text_lower):
        return None

    trade = parse_trade_intent(text, network=network, client=client)
    if trade and not trade.get("missing"):
        return None

    # TP / SL on open position
    has_tp = bool(re.search(r"\b(?:tp|take\s*-?profit)\b", text_lower))
    has_sl = bool(re.search(r"\b(?:sl|stop\s*-?loss)\b", text_lower))
    if (has_tp or has_sl) and _looks_like_bracket_command(text_lower):
        tp, sl = _extract_tp_sl_prices(text_lower)
        if tp is None and sl is None:
            return None
        product = _extract_product(text_lower, network=network, client=client)
        if not product:
            product = "BTC"
        return {
            "kind": "position",
            "action": "set_tp_sl",
            "product": product,
            "tp_price": tp,
            "sl_price": sl,
            "raw": raw,
        }

    if _parse_close_all_nl(text_lower):
        return {"kind": "position", "action": "close_all", "raw": raw}

    lim = _parse_limit_close_nl(text_lower, network, client)
    if lim:
        lim["raw"] = raw
        return lim

    mc = _parse_market_close_nl(text_lower, network, client)
    if mc:
        mc["raw"] = raw
        return mc

    return None
