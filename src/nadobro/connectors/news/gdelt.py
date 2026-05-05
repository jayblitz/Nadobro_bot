"""GDELT global event tone for geopolitics signal."""

from __future__ import annotations

import logging

import requests

from src.nadobro.connectors.news import NewsItem

logger = logging.getLogger(__name__)

_API = "https://api.gdeltproject.org/api/v2/doc/doc"
_TIMEOUT = 6.0


def fetch(limit: int = 10) -> list[NewsItem]:
    try:
        resp = requests.get(
            _API,
            params={
                "query": "(geopolitics OR sanctions OR conflict OR \"central bank\")",
                "mode": "ArtList",
                "format": "json",
                "maxrecords": limit,
                "sort": "DateDesc",
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug("GDELT fetch failed: %s", exc)
        return []

    out: list[NewsItem] = []
    for art in (payload.get("articles") or [])[:limit]:
        title = (art.get("title") or "").strip()
        url = (art.get("url") or "").strip()
        if not title or not url:
            continue
        out.append(
            NewsItem(
                title=title,
                url=url,
                source=art.get("domain") or "GDELT",
                category="geopolitics",
                summary=(art.get("seendate") or ""),
            )
        )
    return out
