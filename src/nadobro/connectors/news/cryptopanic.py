"""CryptoPanic news connector — free tier API."""

from __future__ import annotations

import logging
import os

import requests

from src.nadobro.connectors.news import NewsItem

logger = logging.getLogger(__name__)

_BASE_URL = "https://cryptopanic.com/api/v1/posts/"
_TIMEOUT = 6.0


def fetch(limit: int = 10) -> list[NewsItem]:
    api_key = os.environ.get("CRYPTOPANIC_API_KEY", "").strip()
    if not api_key:
        return []
    try:
        resp = requests.get(
            _BASE_URL,
            params={"auth_token": api_key, "kind": "news", "public": "true"},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug("CryptoPanic fetch failed: %s", exc)
        return []

    out: list[NewsItem] = []
    for post in (payload.get("results") or [])[:limit]:
        title = (post.get("title") or "").strip()
        url = (post.get("url") or "").strip()
        if not title or not url:
            continue
        currencies = post.get("currencies") or []
        tickers = [c.get("code") for c in currencies if isinstance(c, dict) and c.get("code")]
        published_raw = post.get("published_at") or ""
        published_at = None
        try:
            from email.utils import parsedate_to_datetime

            published_at = parsedate_to_datetime(published_raw).timestamp() if published_raw else None
        except Exception:
            published_at = None
        out.append(
            NewsItem(
                title=title,
                url=url,
                source="CryptoPanic",
                category="crypto",
                published_at=published_at,
                tickers=tickers,
            )
        )
    return out
