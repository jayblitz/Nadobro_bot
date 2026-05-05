"""Associated Press business via Google News (AP RSS endpoints have been unstable)."""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._googlenews import google_news_query


def fetch(limit: int = 10) -> list[NewsItem]:
    return google_news_query(
        "site:apnews.com (markets OR economy OR business) when:1d",
        source="AP",
        category="tradfi",
        limit=limit,
    )
