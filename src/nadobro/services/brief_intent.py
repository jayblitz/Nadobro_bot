"""Detect when a user is asking for the morning brief / today's market news.

Runs after the studio gate (so an active Studio session is not hijacked) and
before `classify_conversation_intent` so brief requests bypass the generic
Q&A path and go straight to the brief composer.
"""

from __future__ import annotations

import re

_BRIEF_PATTERNS = re.compile(
    r"\b("
    r"morning\s+brief|"
    r"today'?s?\s+brief|"
    r"the\s+brief\s+for\s+today|"
    r"give\s+me\s+(?:the|a)\s+brief|"
    r"today'?s?\s+(?:market\s+)?news|"
    r"market\s+news|"
    r"what'?s\s+today'?s\s+market(?:\s+news)?|"
    r"what'?s\s+(?:the\s+)?brief(?:\s+for\s+today)?\??$"
    r")",
    re.IGNORECASE,
)


def is_brief_request(text: str) -> bool:
    if not text:
        return False
    return bool(_BRIEF_PATTERNS.search(text))
