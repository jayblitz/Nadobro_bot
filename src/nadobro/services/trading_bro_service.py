"""Trading Bro answer orchestration.

This layer keeps broad educational/analysis chat separate from execution flows
while reusing the existing OpenAI/Grok knowledge pipeline underneath.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

from src.nadobro.services.conversation_intent import classify_conversation_intent
from src.nadobro.services.knowledge_service import answer_nado_question, stream_nado_answer


def answer_mode_for_text(text: str) -> str:
    intent = classify_conversation_intent(text)
    if intent.name == "learn":
        q = (text or "").lower()
        if any(term in q for term in ("build", "create", "design", "architecture", "implement", "code")):
            return "strategy_design"
        return "educational_guide"
    if intent.name == "debug":
        return "debugging"
    if intent.name == "market":
        return "market_analysis"
    if intent.name == "product_support":
        return "product_support"
    if intent.name == "casual":
        return "casual"
    return "general_trading_bro"


def build_trading_bro_question(text: str, *, mode: str | None = None) -> str:
    selected_mode = mode or answer_mode_for_text(text)
    return (
        "TRADING BRO ANSWER MODE\n"
        f"Mode: {selected_mode}\n\n"
        "Answer like a full trading companion, not a narrow product-support bot.\n"
        "- If the user is asking to learn/build/debug, do not execute trades or strategies.\n"
        "- Give a clear answer first, then structured sections with practical steps.\n"
        "- For strategy/build questions, cover architecture, logic, risk controls, execution, monitoring, and tests.\n"
        "- Keep Telegram readability high: short headings, concise bullets, useful examples, no filler.\n"
        "- Include sources only when live data, X/social context, quoted docs, or external facts were actually used.\n\n"
        f"User message:\n{text}"
    )


async def stream_trading_bro_answer(
    text: str,
    telegram_id: int | None = None,
    user_name: str | None = None,
    *,
    mode: str | None = None,
) -> AsyncIterator[str]:
    framed = build_trading_bro_question(text, mode=mode)
    async for chunk in stream_nado_answer(framed, telegram_id=telegram_id, user_name=user_name):
        yield chunk


async def answer_trading_bro_question(
    text: str,
    telegram_id: int | None = None,
    user_name: str | None = None,
    *,
    mode: str | None = None,
) -> str:
    framed = build_trading_bro_question(text, mode=mode)
    return await answer_nado_question(framed, telegram_id=telegram_id, user_name=user_name)
