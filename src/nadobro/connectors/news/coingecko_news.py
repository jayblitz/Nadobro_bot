"""CoinGecko news endpoint (Demo plan supports the /news endpoint).

Endpoint shape varies by plan; on failure we return [] so the aggregator
gracefully falls back to other sources.
"""

from __future__ import annotations

import logging
import os

import requests

from src.nadobro.connectors.news import NewsItem

logger = logging.getLogger(__name__)

_BASE_URL = os.environ.get("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
_TIMEOUT = 6.0


def fetch(limit: int = 10) -> list[NewsItem]:
    headers = {"User-Agent": "NadoBro/1.0"}
    key = os.environ.get("COINGECKO_API_KEY", "").strip()
    if key:
        headers["x-cg-demo-api-key"] = key
    try:
        resp = requests.get(f"{_BASE_URL}/news", headers=headers, timeout=_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug("CoinGecko news fetch failed: %s", exc)
        return []

    rows = payload.get("data") or payload.get("news") or []
    out: list[NewsItem] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        title = (row.get("title") or row.get("name") or "").strip()
        url = (row.get("url") or row.get("link") or "").strip()
        if not title or not url:
            continue
        out.append(
            NewsItem(
                title=title,
                url=url,
                source="CoinGecko",
                category="crypto",
                summary=(row.get("description") or "")[:400],
            )
        )
    return out
