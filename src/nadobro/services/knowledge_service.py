import os
import logging
from pathlib import Path
from openai import OpenAI

logger = logging.getLogger(__name__)

_knowledge_base = None
_xai_client = None

KNOWLEDGE_FILE = Path(__file__).parent.parent / "data" / "nado_knowledge.txt"


def _get_xai_client():
    global _xai_client
    if _xai_client is None:
        api_key = os.environ.get("XAI_API_KEY")
        if not api_key:
            return None
        _xai_client = OpenAI(base_url="https://api.x.ai/v1", api_key=api_key)
    return _xai_client


def _load_knowledge_base():
    global _knowledge_base
    if _knowledge_base is None:
        try:
            _knowledge_base = KNOWLEDGE_FILE.read_text(encoding="utf-8")
            logger.info(f"Loaded knowledge base: {len(_knowledge_base)} chars")
        except Exception as e:
            logger.error(f"Failed to load knowledge base: {e}")
            _knowledge_base = ""
    return _knowledge_base


KNOWLEDGE_SYSTEM_PROMPT = """You are Nadobro, the AI assistant for Nado DEX — a perpetual futures and spot trading platform on Ink blockchain (by the team behind Kraken).

You answer questions about Nado using the knowledge base provided below. Be helpful, accurate, and concise. Use specific details from the docs when available.

RULES:
- Answer based on the knowledge base. If the answer isn't in the docs, say so honestly.
- Keep answers concise but complete. Use bullet points for lists.
- For trading questions, mention relevant features (order types, margin modes, etc.)
- For technical questions, reference SDK details, API endpoints, chain IDs.
- Be friendly and conversational, not robotic.
- If asked about prices or live data, explain that you can check live prices using the bot's market features.
- Do NOT use MarkdownV2 formatting. Use plain text only. The caller will handle formatting.
- Maximum 800 characters in your response to keep it readable in Telegram.

KNOWLEDGE BASE:
{knowledge_base}
"""


async def answer_nado_question(question: str) -> str:
    client = _get_xai_client()
    if not client:
        return "AI service is not available right now. Please try again later."

    knowledge = _load_knowledge_base()
    if not knowledge:
        return "Knowledge base is not loaded. Please contact support."

    system = KNOWLEDGE_SYSTEM_PROMPT.format(knowledge_base=knowledge)

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: client.chat.completions.create(
                model="grok-3-mini-fast",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": question},
                ],
                max_tokens=600,
                temperature=0.3,
            )
        )
        content = response.choices[0].message.content
        if not content or not content.strip():
            return "I couldn't generate an answer. Please try rephrasing your question."
        return content.strip()
    except Exception as e:
        logger.error(f"Knowledge Q&A failed: {e}", exc_info=True)
        return "Something went wrong while answering your question. Please try again."
