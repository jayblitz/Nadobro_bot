import re
from typing import Optional

from src.nadobro.config import PRODUCTS

TRADE_KEYWORDS = ("buy", "sell", "long", "short", "market", "limit")


def _extract_product(text_lower: str) -> Optional[str]:
    for symbol, info in PRODUCTS.items():
        if info.get("type") != "perp":
            continue
        checks = (
            symbol.lower(),
            f"{symbol.lower()}-perp",
            info.get("symbol", "").lower(),
        )
        if any(token and re.search(rf"\b{re.escape(token)}\b", text_lower) for token in checks):
            return symbol
    return None


def _extract_direction(text_lower: str) -> Optional[str]:
    if re.search(r"\b(buy|long)\b", text_lower):
        return "long"
    if re.search(r"\b(sell|short)\b", text_lower):
        return "short"
    return None


def parse_trade_intent(text: str) -> Optional[dict]:
    raw = text.strip()
    if not raw:
        return None
    text_lower = raw.lower()
    if not any(word in text_lower for word in TRADE_KEYWORDS):
        return None

    direction = _extract_direction(text_lower)
    product = _extract_product(text_lower)
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


def parse_interaction_intent(text: str) -> Optional[dict]:
    raw = (text or "").strip()
    if not raw:
        return None

    text_lower = raw.lower()
    words = re.findall(r"[a-z0-9]+", text_lower)
    if not words:
        return None
    is_short_command = len(words) <= 5

    # Position-closing intents.
    if re.search(r"\b(close|exit)\b", text_lower):
        if re.search(r"\b(all|everything)\b", text_lower) and re.search(
            r"\b(position|positions|order|orders|trade|trades)\b",
            text_lower,
        ):
            return {"kind": "interaction", "action": "close_all", "raw": raw}

        product = _extract_product(text_lower)
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
        if re.search(r"\b(markets?|prices?|funding)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "mkt:menu", "raw": raw}
        if re.search(r"\b(mode|network|testnet|mainnet)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "nav:mode", "raw": raw}
        if re.search(r"\b(trade|long|short|buy|sell)\b", text_lower):
            return {"kind": "interaction", "action": "open_view", "target": "nav:trade", "raw": raw}

    return None
