"""BBC World news RSS."""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._rss import fetch_rss_items

_FEED_URL = "https://feeds.bbci.co.uk/news/world/rss.xml"


def fetch(limit: int = 10) -> list[NewsItem]:
    return fetch_rss_items(_FEED_URL, source="BBC World", category="geopolitics", limit=limit)
