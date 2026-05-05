"""Financial Modeling Prep stock news (headlines for major equities).

Used for stock-related headlines — not for live equity prices.
The morning brief snapshot only shows Nado-tradeable instruments;
equity headlines surface in the news drivers section.
"""

from __future__ import annotations

import logging
import os
import time

import requests

from src.nadobro.connectors.news import NewsItem

logger = logging.getLogger(__name__)

_BASE_URL = os.environ.get("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3")
_DEFAULT_TICKERS = "AAPL,MSFT,AMZN,NVDA,GOOGL,META,TSLA"
_TIMEOUT = 6.0


def fetch(limit: int = 10) -> list[NewsItem]:
    api_key = os.environ.get("FMP_API_KEY", "").strip()
    if not api_key:
        return []
    try:
        resp = requests.get(
            f"{_BASE_URL}/stock_news",
            params={"tickers": _DEFAULT_TICKERS, "limit": limit, "apikey": api_key},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug("FMP news fetch failed: %s", exc)
        return []

    if not isinstance(payload, list):
        return []

    out: list[NewsItem] = []
    for row in payload[:limit]:
        title = (row.get("title") or "").strip()
        url = (row.get("url") or "").strip()
        if not title or not url:
            continue
        published_at = None
        published_raw = row.get("publishedDate") or ""
        try:
            published_at = time.mktime(time.strptime(published_raw[:19], "%Y-%m-%d %H:%M:%S"))
        except Exception:
            published_at = None
        symbol = row.get("symbol") or ""
        out.append(
            NewsItem(
                title=title,
                url=url,
                source="FMP",
                category="stocks",
                summary=(row.get("text") or "")[:400],
                published_at=published_at,
                tickers=[symbol] if symbol else [],
            )
        )
    return out
