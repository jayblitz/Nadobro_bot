"""Financial Times headlines via Google News (paywalled bodies — link only).

v1 ships free RSS / Google News only. When FT_API_KEY is later provisioned
we'll add a separate `ft_api.py` connector and prefer it.
"""

from __future__ import annotations

from src.nadobro.connectors.news import NewsItem
from src.nadobro.connectors.news._googlenews import google_news_query


def fetch(limit: int = 10) -> list[NewsItem]:
    return google_news_query(
        "site:ft.com when:1d",
        source="Financial Times",
        category="ft",
        limit=limit,
    )
