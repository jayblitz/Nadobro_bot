import os
import re
import time
import logging
from pathlib import Path
from urllib.parse import quote_plus

import requests
from openai import OpenAI

logger = logging.getLogger(__name__)

_knowledge_base = None
_xai_client = None
_openai_client = None
_source_cache = {}

KNOWLEDGE_FILE = Path(__file__).parent.parent / "data" / "nado_knowledge.txt"
SOURCE_CACHE_TTL_SECONDS = 600
SOURCE_FETCH_TIMEOUT_SECONDS = 12

OFFICIAL_URLS = [
    "https://docs.nado.xyz/",
    "https://docs.nado.xyz/developer-resources/get-started",
    "https://docs.nado.xyz/developer-resources/api/gateway",
    "https://www.nado.xyz/",
    "https://x.com/nadoHQ",
]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_xai_client():
    global _xai_client
    if _xai_client is None:
        api_key = os.environ.get("XAI_API_KEY")
        if not api_key:
            return None
        _xai_client = OpenAI(base_url="https://api.x.ai/v1", api_key=api_key)
    return _xai_client


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            return None
        _openai_client = OpenAI(api_key=api_key)
    return _openai_client


def _pick_primary_provider(question: str) -> str:
    configured = os.environ.get("NADO_AI_PROVIDER", "auto").strip().lower()
    if configured in {"xai", "openai"}:
        return configured

    if configured != "auto":
        logger.warning("Unknown NADO_AI_PROVIDER=%s, defaulting to auto", configured)

    # Auto mode: xAI first for speed; escalate to OpenAI for harder questions.
    if _is_complex_question(question) and _env_bool("NADO_AI_ESCALATE_ON_COMPLEX", True):
        return "openai"
    return "xai"


def _is_complex_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if len(q) > 220:
        return True
    complexity_signals = [
        "compare",
        "difference",
        "architecture",
        "sdk",
        "api",
        "gateway",
        "signature",
        "auth",
        "websocket",
        "debug",
        "error",
        "best practice",
        "production",
        "integration",
        "explain step by step",
    ]
    return any(sig in q for sig in complexity_signals)


def _model_for(provider: str) -> str:
    if provider == "openai":
        return os.environ.get("OPENAI_SUPPORT_MODEL", "gpt-4.1-mini")
    return os.environ.get("XAI_SUPPORT_MODEL", "grok-3-mini-fast")


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


KNOWLEDGE_SYSTEM_PROMPT = """You are Nadobro Support AI for Nado.

Your role:
- Be the first-line support assistant for all Nado user questions.
- Use the retrieved context from Nado docs, developer docs, website, X, and live web search.
- Prefer official Nado sources when there is any conflict.

Rules:
- Be accurate and concise.
- If data is uncertain or missing, say it clearly and suggest the next best official source.
- For product/how-to questions, provide actionable steps.
- For developer questions, include concrete API/docs direction.
- Do NOT use MarkdownV2 syntax escapes; plain text only.
- Keep response under 1200 characters.
- End with a short "Sources:" line listing 2-5 source URLs used.

Static Knowledge:
{knowledge_base}

Retrieved Context:
{retrieved_context}
"""


def _extract_text(raw: str) -> str:
    text = raw or ""
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_url_text(url: str) -> str:
    cache_item = _source_cache.get(url)
    if cache_item and (time.time() - cache_item["ts"] < SOURCE_CACHE_TTL_SECONDS):
        return cache_item["text"]

    try:
        # Use jina AI reader proxy for cleaner text and JS-heavy pages like X.
        reader_url = f"https://r.jina.ai/http://{url.replace('https://', '').replace('http://', '')}"
        resp = requests.get(reader_url, timeout=SOURCE_FETCH_TIMEOUT_SECONDS)
        if resp.ok and resp.text:
            cleaned = _extract_text(resp.text)[:12000]
            _source_cache[url] = {"ts": time.time(), "text": cleaned}
            return cleaned
    except Exception:
        pass

    try:
        resp = requests.get(url, timeout=SOURCE_FETCH_TIMEOUT_SECONDS)
        if resp.ok and resp.text:
            cleaned = _extract_text(resp.text)[:12000]
            _source_cache[url] = {"ts": time.time(), "text": cleaned}
            return cleaned
    except Exception as e:
        logger.warning(f"Failed to fetch source {url}: {e}")

    return ""


def _search_live(question: str) -> list[tuple[str, str]]:
    results = []
    queries = [
        ("duckduckgo", f"https://duckduckgo.com/?q={quote_plus('nado ' + question)}"),
        ("x-search", f"https://x.com/search?q={quote_plus('from:nadoHQ ' + question)}&src=typed_query"),
    ]
    for _, url in queries:
        txt = _fetch_url_text(url)
        if txt:
            results.append((url, txt))
    return results


def _relevance_score(text: str, question: str) -> int:
    q_tokens = [t for t in re.split(r"[^a-zA-Z0-9]+", question.lower()) if len(t) > 2]
    if not q_tokens:
        return 0
    lower = text.lower()
    return sum(1 for tok in q_tokens if tok in lower)


def _build_retrieved_context(question: str) -> tuple[str, list[str]]:
    docs_payload = []
    used_sources = []

    source_pairs = []
    for url in OFFICIAL_URLS:
        txt = _fetch_url_text(url)
        if txt:
            source_pairs.append((url, txt))

    source_pairs.extend(_search_live(question))

    scored = []
    for url, txt in source_pairs:
        score = _relevance_score(txt, question)
        scored.append((score, url, txt))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:6] if scored else []
    for _, url, txt in top:
        snippet = txt[:1800]
        docs_payload.append(f"[SOURCE] {url}\n{snippet}")
        used_sources.append(url)

    if not docs_payload:
        return "No retrieved context available.", []
    return "\n\n".join(docs_payload), list(dict.fromkeys(used_sources))


def _call_support_llm(provider: str, system: str, question: str) -> str:
    if provider == "openai":
        client = _get_openai_client()
    else:
        client = _get_xai_client()

    if not client:
        raise RuntimeError(f"{provider.upper()} client not configured")

    response = client.chat.completions.create(
        model=_model_for(provider),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        max_tokens=700,
        temperature=0.2,
    )
    content = response.choices[0].message.content
    if not content or not content.strip():
        raise RuntimeError(f"{provider.upper()} returned empty response")
    return content.strip()


async def answer_nado_question(question: str) -> str:
    xai_client = _get_xai_client()
    openai_client = _get_openai_client()
    if not xai_client and not openai_client:
        return (
            "AI service is not configured. Add XAI_API_KEY and/or OPENAI_API_KEY "
            "then restart the bot."
        )

    knowledge = _load_knowledge_base()
    if not knowledge:
        return "Knowledge base is not loaded. Please contact support."

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        retrieved_context, used_sources = await loop.run_in_executor(None, _build_retrieved_context, question)
    except Exception:
        retrieved_context, used_sources = ("No retrieved context available.", [])

    system = KNOWLEDGE_SYSTEM_PROMPT.format(
        knowledge_base=knowledge[:12000],
        retrieved_context=retrieved_context[:18000],
    )

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        primary = _pick_primary_provider(question)
        secondary = "openai" if primary == "xai" else "xai"

        providers = [primary, secondary]
        # Remove provider with no configured client up front.
        providers = [
            p for p in providers
            if (p == "xai" and xai_client) or (p == "openai" and openai_client)
        ]

        last_error = None
        answer = None
        used_provider = None
        for provider in providers:
            try:
                answer = await loop.run_in_executor(
                    None,
                    lambda p=provider: _call_support_llm(p, system, question),
                )
                used_provider = provider
                break
            except Exception as provider_error:
                last_error = provider_error
                logger.warning("Support answer failed on provider=%s: %s", provider, provider_error)
                continue

        if not answer:
            if last_error:
                raise last_error
            return "I couldn't generate an answer. Please try again."

        logger.info("Support answer generated via provider=%s", used_provider)
        if "Sources:" not in answer:
            if used_sources:
                sources_line = "Sources: " + ", ".join(used_sources[:4])
            else:
                sources_line = "Sources: https://docs.nado.xyz/, https://x.com/nadoHQ"
            answer = f"{answer}\n\n{sources_line}"
        return answer
    except Exception as e:
        logger.error(f"Knowledge Q&A failed: {e}", exc_info=True)
        return "Something went wrong while answering your question. Please try again."
