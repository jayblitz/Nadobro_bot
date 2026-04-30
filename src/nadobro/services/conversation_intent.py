"""Lightweight conversation intent classification for chat routing.

The goal is to separate "teach/analyze/debug" messages from commands that
should place trades or start strategy loops. Keep this deterministic so it can
run before any LLM calls.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

ConversationIntentName = Literal[
    "execute",
    "learn",
    "debug",
    "market",
    "product_support",
    "casual",
    "unknown",
]


@dataclass(frozen=True)
class ConversationIntent:
    name: ConversationIntentName
    confidence: float
    reason: str = ""


_EXECUTE_VERBS = (
    "start",
    "run",
    "launch",
    "activate",
    "enable",
    "buy",
    "sell",
    "long",
    "short",
    "close",
    "stop",
    "cancel",
)
_STRATEGY_TERMS = (
    "grid",
    "rgrid",
    "r-grid",
    "reverse grid",
    "dynamic grid",
    "dgrid",
    "d-grid",
    "delta neutral",
    "volume bot",
    "vol bot",
    "bro mode",
    "alpha agent",
    "strategy",
)
_EDUCATIONAL_OPENERS = (
    "how can i",
    "how do i",
    "how would i",
    "how to",
    "what is",
    "what are",
    "explain",
    "teach me",
    "walk me through",
    "guide me",
    "help me understand",
    "can you explain",
    "i want to learn",
)
_BUILD_ANALYSIS_TERMS = (
    "build",
    "create",
    "design",
    "architecture",
    "implement",
    "code",
    "working",
    "compare",
    "pros and cons",
    "best practice",
    "framework",
)
_DEBUG_TERMS = (
    "debug",
    "root cause",
    "why did",
    "why doesn't",
    "why is",
    "failed",
    "error",
    "logs",
    "traceback",
    "not working",
    "didn't work",
)
_MARKET_TERMS = (
    "price",
    "market",
    "sentiment",
    "news",
    "latest",
    "trending",
    "ct saying",
    "twitter",
    "x saying",
    "fear and greed",
    "dominance",
    "gainers",
    "losers",
)
_PRODUCT_TERMS = (
    "nado",
    "nadobro",
    "ink",
    "points",
    "referral",
    "invite",
    "wallet",
    "deposit",
    "withdraw",
    "funding",
    "margin",
    "liquidation",
)
_CASUAL_TERMS = {
    "gm",
    "gn",
    "hi",
    "hey",
    "hello",
    "yo",
    "thanks",
    "thank you",
    "bro",
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _has_any(q: str, terms: tuple[str, ...]) -> bool:
    return any(term in q for term in terms)


def _starts_with_any(q: str, terms: tuple[str, ...]) -> bool:
    return any(q.startswith(term) for term in terms)


def is_educational_request(text: str) -> bool:
    """True for questions that mention trading verbs but are asking to learn."""
    q = _norm(text)
    if not q:
        return False
    if _starts_with_any(q, _EDUCATIONAL_OPENERS):
        return True
    if "?" in q and _has_any(q, _BUILD_ANALYSIS_TERMS):
        return True
    return _has_any(q, _BUILD_ANALYSIS_TERMS) and _has_any(q, ("how", "guide", "explain", "learn"))


def classify_conversation_intent(text: str) -> ConversationIntent:
    q = _norm(text)
    if not q:
        return ConversationIntent("unknown", 0.0, "empty")

    if q.rstrip("!?.,") in _CASUAL_TERMS or len(q) <= 3:
        return ConversationIntent("casual", 0.9, "short casual phrase")

    if _has_any(q, _DEBUG_TERMS):
        return ConversationIntent("debug", 0.82, "debugging/error language")

    if is_educational_request(q):
        if _has_any(q, _STRATEGY_TERMS):
            return ConversationIntent("learn", 0.92, "educational strategy wording")
        return ConversationIntent("learn", 0.82, "educational wording")

    if _has_any(q, _MARKET_TERMS):
        return ConversationIntent("market", 0.72, "market/current-data language")

    if _has_any(q, _PRODUCT_TERMS):
        return ConversationIntent("product_support", 0.68, "Nado/Nadobro product language")

    if _has_any(q, _EXECUTE_VERBS) and (
        _has_any(q, _STRATEGY_TERMS)
        or re.search(r"\b(btc|eth|sol|xrp|aapl|tsla|nvda|doge|bnb|link)\b", q)
    ):
        return ConversationIntent("execute", 0.78, "execution verb plus market/strategy target")

    return ConversationIntent("unknown", 0.35, "no strong deterministic signal")
