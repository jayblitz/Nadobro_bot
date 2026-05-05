"""Reuters business — Reuters dropped public RSS in 2020, so we use Google News."""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._googlenews import google_news_query


def fetch(limit: int = 10) -> list[NewsItem]:
    return google_news_query(
        "site:reuters.com when:1d",
        source="Reuters",
        category="tradfi",
        limit=limit,
    )
