"""Federal Reserve press releases RSS."""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._rss import fetch_rss_items

_FEED_URL = "https://www.federalreserve.gov/feeds/press_all.xml"


def fetch(limit: int = 10) -> list[NewsItem]:
    return fetch_rss_items(_FEED_URL, source="Federal Reserve", category="economics", limit=limit)
