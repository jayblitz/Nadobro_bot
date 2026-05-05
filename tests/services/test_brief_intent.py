from src.nadobro.services.brief_intent import is_brief_request

POSITIVE = [
    "morning brief",
    "Morning Brief",
    "what's the brief for today?",
    "What's the brief?",
    "give me the brief",
    "give me a brief",
    "today's market news",
    "today's news",
    "market news",
    "what's today's market",
    "What's today's market news?",
]

NEGATIVE = [
    "brief me on Solana points",
    "tell me about the news on Solana",
    "what's BTC price",
    "long BTC 0.01 5x market",
    "hi",
    "thanks for the brief overview of how Nado works",
    "",
]


def test_brief_intent_positives():
    for text in POSITIVE:
        assert is_brief_request(text), f"expected positive: {text!r}"


def test_brief_intent_negatives():
    for text in NEGATIVE:
        assert not is_brief_request(text), f"expected negative: {text!r}"
