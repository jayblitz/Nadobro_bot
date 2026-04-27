"""X (Twitter) API v2 client for fetching tweets directly.

Uses the X API v2 Recent Search endpoint to fetch tweets from specific
accounts or by keyword. Requires a Bearer Token from developer.x.com.

Falls back gracefully when X_API_BEARER_TOKEN is not set — the rest
of the system uses Grok's built-in X search instead.
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from src.nadobro.services.log_redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.x.com/2"

# ── Cache ────────────────────────────────────────────────────────────

_tweet_cache: dict = {}
TWEET_CACHE_TTL = 300  # 5 min
_TWEET_CACHE_MAX_ENTRIES = 128
_credits_depleted_until = 0.0
_credits_depleted_logged = False
_CREDITS_BACKOFF_SECONDS = int(os.environ.get("X_API_CREDITS_DEPLETED_BACKOFF_SECONDS", str(6 * 60 * 60)))


def _prune_tweet_cache(now: float | None = None) -> None:
    ts = now or time.time()
    stale = [k for k, v in _tweet_cache.items() if ts - float(v.get("ts") or 0) > TWEET_CACHE_TTL]
    for k in stale:
        _tweet_cache.pop(k, None)
    while len(_tweet_cache) > _TWEET_CACHE_MAX_ENTRIES:
        oldest = min(_tweet_cache, key=lambda k: float(_tweet_cache[k].get("ts") or 0))
        _tweet_cache.pop(oldest, None)


def _get_bearer_token() -> Optional[str]:
    return os.environ.get("X_API_BEARER_TOKEN")


def is_available() -> bool:
    """Check if X API is configured."""
    return bool(_get_bearer_token()) and time.time() >= _credits_depleted_until


def _credits_depleted_active() -> bool:
    return time.time() < _credits_depleted_until


def _mark_credits_depleted(body: str = "") -> None:
    global _credits_depleted_until, _credits_depleted_logged
    _credits_depleted_until = time.time() + max(300, _CREDITS_BACKOFF_SECONDS)
    if not _credits_depleted_logged:
        logger.warning(
            "X API credits depleted; disabling direct X polling for %.1f hours. Response: %s",
            max(300, _CREDITS_BACKOFF_SECONDS) / 3600,
            redact_sensitive_text((body or "")[:300]),
        )
        _credits_depleted_logged = True


def _record_x_source(detail: str, confidence: float = 0.85):
    try:
        from src.nadobro.services.source_registry import record_source

        record_source(
            "x",
            ttl_seconds=TWEET_CACHE_TTL,
            confidence=confidence,
            source_url="https://x.com",
            license_tier="api",
            allowed_use="sentiment",
            detail=detail,
        )
    except Exception:
        pass


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_bearer_token()}",
        "Content-Type": "application/json",
    }


# ── Tweet search ─────────────────────────────────────────────────────

def search_recent_tweets(
    query: str,
    max_results: int = 20,
    hours_back: int = 168,  # 7 days
) -> list[dict]:
    """Search recent tweets via X API v2.

    Args:
        query: X API search query (e.g., "from:nadoHQ" or "crypto alpha")
        max_results: Number of results (10-100)
        hours_back: How far back to search (max 168 hours / 7 days on free tier)

    Returns:
        List of tweet dicts: [{id, text, author_username, created_at, url, metrics}, ...]
    """
    token = _get_bearer_token()
    if not token:
        _record_x_source("X API not configured", confidence=0.0)
        return []
    if _credits_depleted_active():
        _record_x_source("X API credits depleted backoff", confidence=0.0)
        return []

    capped_hours_back = max(1, min(int(hours_back or 168), 168))

    # Check cache
    cache_key = f"{query}:{max_results}:{capped_hours_back}"
    _prune_tweet_cache()
    cached = _tweet_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < TWEET_CACHE_TTL:
        _record_x_source(f"X cached tweets: {query[:60]}")
        return cached["tweets"]

    start_time = (datetime.utcnow() - timedelta(hours=capped_hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "query": query,
        "max_results": min(max(max_results, 10), 100),
        "start_time": start_time,
        "tweet.fields": "created_at,public_metrics,author_id,conversation_id",
        "expansions": "author_id",
        "user.fields": "username,name,verified",
    }

    try:
        resp = requests.get(
            f"{_BASE_URL}/tweets/search/recent",
            headers=_headers(),
            params=params,
            timeout=10,
        )

        if resp.status_code == 429:
            logger.warning("X API rate limited — falling back to Grok search")
            _record_x_source("X API rate limited", confidence=0.0)
            return []

        if resp.status_code == 402 and "CreditsDepleted" in (resp.text or ""):
            _mark_credits_depleted(resp.text or "")
            _record_x_source("X API credits depleted", confidence=0.0)
            return []

        if resp.status_code != 200:
            logger.warning("X API error %d: %s", resp.status_code, redact_sensitive_text(resp.text[:300]))
            _record_x_source(f"X API error {resp.status_code}", confidence=0.0)
            return []

        data = resp.json()
        tweets_raw = data.get("data", [])
        users_raw = data.get("includes", {}).get("users", [])

        # Build author lookup
        author_map = {u["id"]: u for u in users_raw}

        tweets = []
        for t in tweets_raw:
            author = author_map.get(t.get("author_id"), {})
            username = author.get("username", "unknown")
            tweets.append({
                "id": t["id"],
                "conversation_id": t.get("conversation_id", ""),
                "text": t.get("text", ""),
                "author_username": username,
                "author_name": author.get("name", username),
                "created_at": t.get("created_at", ""),
                "url": f"https://x.com/{username}/status/{t['id']}",
                "metrics": t.get("public_metrics", {}),
            })

        # Cache results
        _tweet_cache[cache_key] = {"tweets": tweets, "ts": time.time()}
        _prune_tweet_cache()

        logger.info("X API returned %d tweets for query: %s", len(tweets), query[:80])
        _record_x_source(f"X recent search: {query[:60]}")
        return tweets

    except requests.Timeout:
        logger.warning("X API timeout for query: %s", query[:80])
        _record_x_source("X API timeout", confidence=0.0)
        return []
    except Exception:
        logger.warning("X API request failed", exc_info=True)
        _record_x_source("X API request failed", confidence=0.0)
        return []


def get_nado_tweets(max_results: int = 20, hours_back: int = 168) -> list[dict]:
    """Fetch recent tweets from @nadoHQ and @inkonchain."""
    return search_recent_tweets(
        query="from:nadoHQ OR from:inkonchain",
        max_results=max_results,
        hours_back=hours_back,
    )


def get_account_tweets(handles: list[str], max_results: int = 20, hours_back: int = 168) -> list[dict]:
    """Fetch recent tweets from specific X accounts."""
    query = " OR ".join(f"from:{h.lstrip('@')}" for h in handles)
    return search_recent_tweets(query=query, max_results=max_results, hours_back=hours_back)


def search_topic_tweets(topic: str, max_results: int = 20, hours_back: int = 168) -> list[dict]:
    """Search recent tweets by topic/keyword."""
    # Add -is:retweet to get original content only
    query = f"{topic} -is:retweet"
    return search_recent_tweets(query=query, max_results=max_results, hours_back=hours_back)


# ── Formatting helpers ───────────────────────────────────────────────

def format_tweets_for_context(tweets: list[dict], max_tweets: int = 10) -> str:
    """Format raw tweets into a context string for LLM consumption."""
    if not tweets:
        return ""

    lines = []
    for t in tweets[:max_tweets]:
        date_str = ""
        if t.get("created_at"):
            try:
                dt = datetime.fromisoformat(t["created_at"].replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                date_str = t["created_at"]

        metrics = t.get("metrics", {})
        engagement = ""
        likes = metrics.get("like_count", 0)
        retweets = metrics.get("retweet_count", 0)
        if likes or retweets:
            engagement = f" [❤️ {likes}, 🔁 {retweets}]"

        lines.append(
            f"@{t['author_username']} ({date_str}){engagement}:\n"
            f"{t['text']}\n"
            f"Link: {t['url']}\n"
        )

    return "\n".join(lines)


def format_tweets_for_edge_analysis(tweets: list[dict]) -> str:
    """Format tweets specifically for the edge scanner LLM analysis."""
    if not tweets:
        return "No recent tweets found."

    lines = []
    for t in tweets:
        lines.append(
            f"[@{t['author_username']}] {t['text']}\n"
            f"Date: {t.get('created_at', 'unknown')} | URL: {t['url']}"
        )

    return "\n\n".join(lines)
