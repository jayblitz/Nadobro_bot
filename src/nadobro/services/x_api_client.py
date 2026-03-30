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

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.x.com/2"

# ── Cache ────────────────────────────────────────────────────────────

_tweet_cache: dict = {}
TWEET_CACHE_TTL = 300  # 5 min


def _get_bearer_token() -> Optional[str]:
    return os.environ.get("X_API_BEARER_TOKEN")


def is_available() -> bool:
    """Check if X API is configured."""
    return bool(_get_bearer_token())


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
        return []

    # Check cache
    cache_key = f"{query}:{max_results}:{hours_back}"
    cached = _tweet_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < TWEET_CACHE_TTL:
        return cached["tweets"]

    start_time = (datetime.utcnow() - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

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
            return []

        if resp.status_code != 200:
            logger.warning("X API error %d: %s", resp.status_code, resp.text[:300])
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
                "text": t.get("text", ""),
                "author_username": username,
                "author_name": author.get("name", username),
                "created_at": t.get("created_at", ""),
                "url": f"https://x.com/{username}/status/{t['id']}",
                "metrics": t.get("public_metrics", {}),
            })

        # Cache results
        _tweet_cache[cache_key] = {"tweets": tweets, "ts": time.time()}

        logger.info("X API returned %d tweets for query: %s", len(tweets), query[:80])
        return tweets

    except requests.Timeout:
        logger.warning("X API timeout for query: %s", query[:80])
        return []
    except Exception:
        logger.warning("X API request failed", exc_info=True)
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
