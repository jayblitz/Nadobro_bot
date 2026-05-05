"""RWA / tokenization news via Google News (no canonical free RSS)."""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._googlenews import google_news_query


def fetch(limit: int = 10) -> list[NewsItem]:
    return google_news_query(
        "(\"real world assets\" OR tokenization OR Centrifuge OR Securitize OR Ondo) when:1d",
        source="RWA Wire",
        category="rwa",
        limit=limit,
    )
