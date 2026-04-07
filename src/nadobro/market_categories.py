"""Curated Nado-style market categories for UI tabs (All / Perps / Spot / Memes / DeFi / Chains / Commodities).

Nado does not expose these labels in the product catalog API; we map base symbols to a tab.
Unknown symbols default to PERPS. Extend _CATEGORY_BY_SYMBOL as new markets appear.
"""

from __future__ import annotations

# Tab keys (match Nado miniapp labels).
ALL = "all"
PERPS = "perps"
SPOT = "spot"
MEMES = "memes"
DEFI = "defi"
CHAINS = "chains"
COMMODITIES = "commodities"

_UI_TAB_ORDER: tuple[str, ...] = (ALL, PERPS, SPOT, MEMES, DEFI, CHAINS, COMMODITIES)

_CATEGORY_BY_SYMBOL: dict[str, str] = {
    # Commodities / metals / energy
    "WTI": COMMODITIES,
    "XAG": COMMODITIES,
    "XAU": COMMODITIES,
    "XAUT": COMMODITIES,
    "XPL": COMMODITIES,
    "XAU": COMMODITIES,
    # Chain / L2 narratives
    "SOL": CHAINS,
    "HYPE": CHAINS,
    # DeFi majors
    "LINK": DEFI,
    # Meme (extend as listings change)
    "PEPE": MEMES,
    "WIF": MEMES,
    "BONK": MEMES,
}


def get_market_category(symbol: str) -> str:
    """Return category tab key for a perp base symbol (e.g. BTC -> perps)."""
    key = (symbol or "").upper().strip()
    if not key:
        return PERPS
    return _CATEGORY_BY_SYMBOL.get(key, PERPS)


def ui_category_tabs() -> tuple[str, ...]:
    """Ordered tab ids for the miniapp (includes 'all')."""
    return _UI_TAB_ORDER
