"""European Central Bank press releases RSS."""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._rss import fetch_rss_items

_FEED_URL = "https://www.ecb.europa.eu/rss/press.xml"


def fetch(limit: int = 10) -> list[NewsItem]:
    return fetch_rss_items(_FEED_URL, source="ECB", category="economics", limit=limit)
