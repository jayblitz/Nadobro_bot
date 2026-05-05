"""Google News RSS query helper — used for sources that don't expose a public RSS.

Google News supports query-based RSS at:
  https://news.google.com/rss/search?q=<query>&hl=en-US&gl=US&ceid=US:en
"""

from __future__ import annotations

from urllib.parse import quote_plus

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._rss import fetch_rss_items

_BASE = "https://news.google.com/rss/search"


def google_news_query(query: str, *, source: str, category: str, limit: int = 10) -> list[NewsItem]:
    url = f"{_BASE}?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    return fetch_rss_items(url, source=source, category=category, limit=limit)
