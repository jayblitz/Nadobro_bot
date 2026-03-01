import os
import re
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import requests
from openai import OpenAI

logger = logging.getLogger(__name__)

_knowledge_base = None
_knowledge_sections = None
_xai_client = None
_openai_client = None
_source_cache = {}
_answer_cache = {}

KNOWLEDGE_FILE = Path(__file__).parent.parent / "data" / "nado_knowledge.txt"
SOURCE_CACHE_TTL_SECONDS = 600
SOURCE_FETCH_TIMEOUT_SECONDS = 12
ANSWER_CACHE_TTL_SECONDS = 300

OFFICIAL_URLS = [
    "https://docs.nado.xyz/",
    "https://docs.nado.xyz/developer-resources/get-started",
    "https://docs.nado.xyz/developer-resources/api/gateway",
    "https://www.nado.xyz/",
    "https://x.com/nadoHQ",
    "https://x.com/inkonchain",
]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_question(question: str) -> str:
    return re.sub(r"\s+", " ", (question or "").strip().lower())


def _should_use_live_retrieval(question: str) -> bool:
    if _env_bool("NADO_SUPPORT_LIVE_DEFAULT", False):
        return True
    q = _normalize_question(question)
    live_signals = [
        "latest", "recent", "today", "now", "update", "updates",
        "x.com", "twitter", "news", "what changed", "announced",
        "week", "points", "airdrop", "distributed", "launch",
    ]
    return any(sig in q for sig in live_signals)


def _is_x_twitter_question(question: str) -> bool:
    q = _normalize_question(question)
    signals = ["tweet", "tweets", "x.com", "twitter", "post on x", "posted on x", "nadohq", "inkonchain"]
    return any(sig in q for sig in signals)


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

    if _is_complex_question(question) and _env_bool("NADO_AI_ESCALATE_ON_COMPLEX", True):
        return "openai"
    return "xai"


def _is_complex_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if len(q) > 220:
        return True
    complexity_signals = [
        "compare", "difference", "architecture", "sdk", "api",
        "gateway", "signature", "auth", "websocket", "debug",
        "error", "best practice", "production", "integration",
        "explain step by step",
    ]
    return any(sig in q for sig in complexity_signals)


def _wants_detailed_answer(question: str) -> bool:
    q = (question or "").strip().lower()
    detail_signals = [
        "detailed", "step by step", "in depth", "deep dive",
        "full explanation", "comprehensive", "long answer",
    ]
    return any(sig in q for sig in detail_signals)


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


def _load_knowledge_sections() -> list[dict]:
    global _knowledge_sections
    if _knowledge_sections is not None:
        return _knowledge_sections

    kb = _load_knowledge_base()
    if not kb:
        _knowledge_sections = []
        return _knowledge_sections

    parts = re.split(r"\n(?=##+ )", kb)
    sections = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        first_line, _, rest = part.partition("\n")
        title = first_line.lstrip("#").strip()
        body = rest.strip()

        sub_headers = re.findall(r"^###+ .+", part, re.MULTILINE)
        if sub_headers and not first_line.startswith("## "):
            for sub_part in re.split(r"\n(?=### )", part):
                sub_part = sub_part.strip()
                if not sub_part:
                    continue
                sub_first, _, sub_rest = sub_part.partition("\n")
                sub_title = sub_first.lstrip("#").strip()
                sub_body = sub_rest.strip()
                kw = set(
                    t.lower() for t in re.split(r"[^a-zA-Z0-9]+", f"{sub_title} {sub_body}") if len(t) > 2
                )
                sections.append({"title": sub_title, "body": sub_body, "keywords": kw, "raw": sub_part})
            continue

        keywords = set(
            t.lower() for t in re.split(r"[^a-zA-Z0-9]+", f"{title} {body}") if len(t) > 2
        )
        sections.append({"title": title, "body": body, "keywords": keywords, "raw": part})

    if len(sections) <= 1:
        logger.warning("Knowledge section parsing produced %d sections — expected more. Falling back to full KB.", len(sections))

    _knowledge_sections = sections
    logger.info(f"Parsed {len(sections)} knowledge sections")
    return _knowledge_sections


_SYNONYMS = {
    "points": {"rewards", "incentives", "season", "earning"},
    "rewards": {"points", "incentives", "season", "earning"},
    "program": {"season", "system"},
    "season": {"points", "rewards", "program"},
    "airdrop": {"points", "rewards", "token", "distribution"},
    "referral": {"referrals", "invite", "refer"},
    "referrals": {"referral", "invite", "refer"},
    "fees": {"rebates", "taker", "maker", "trading"},
    "nft": {"templars", "storm"},
    "templars": {"nft", "storm"},
    "leverage": {"margin", "cross"},
    "liquidation": {"liquidations", "health"},
    "liquidations": {"liquidation", "health"},
}


def _expand_with_synonyms(tokens: set) -> set:
    expanded = set(tokens)
    for t in tokens:
        if t in _SYNONYMS:
            expanded.update(_SYNONYMS[t])
    return expanded


def _search_knowledge_sections(query: str, top_k: int = 4) -> str:
    sections = _load_knowledge_sections()
    if not sections:
        return ""

    q_tokens = set(
        t.lower() for t in re.split(r"[^a-zA-Z0-9]+", query) if len(t) > 2
    )
    if not q_tokens:
        return "\n\n".join(s["raw"] for s in sections[:top_k])

    expanded_tokens = _expand_with_synonyms(q_tokens)

    scored = []
    for s in sections:
        direct_overlap = len(q_tokens & s["keywords"])
        synonym_overlap = len((expanded_tokens - q_tokens) & s["keywords"])
        title_bonus = 3 * sum(1 for t in q_tokens if t in s["title"].lower())
        synonym_title_bonus = sum(1 for t in (expanded_tokens - q_tokens) if t in s["title"].lower())
        score = direct_overlap + (synonym_overlap * 0.5) + title_bonus + synonym_title_bonus
        scored.append((score, s))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = [s for score, s in scored[:top_k] if score > 0]

    if not top:
        top = [scored[0][1]] if scored else []

    return "\n\n".join(s["raw"] for s in top)


AGENT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_knowledge_base",
            "description": "Search the Nado knowledge base for information about a specific topic. Use this for questions about Nado features, trading, margin, fees, NFTs, developer resources, architecture, order types, liquidations, etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query to find relevant knowledge base sections"
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_web",
            "description": "Search the web for live/current information about Nado from official docs (docs.nado.xyz), website (nado.xyz), and general web. Use for recent events, news, updates, announcements, points distribution, airdrops, launches, or anything time-sensitive.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The web search query"
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_x_twitter",
            "description": "Search X (Twitter) for the latest posts from Nado's official account @nadoHQ and Ink blockchain's account @inkonchain. Use for questions about tweets, X posts, social media updates, announcements on X, or anything asking what Nado/Ink posted.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query for X/Twitter content"
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_market_sentiment",
            "description": "Get market sentiment and crypto market context. Combines general web search for crypto news and broader crypto Twitter sentiment. Use when the user asks about market conditions, price outlook, sentiment, what the market thinks, crypto trends, or general trading conditions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The market/sentiment query (e.g., 'ETH price outlook', 'crypto market sentiment today')"
                    }
                },
                "required": ["query"]
            }
        }
    },
]

XAI_X_SEARCH_MODEL = os.environ.get("XAI_X_SEARCH_MODEL", "grok-3")

X_NADO_SEARCH_PARAMS = {
    "mode": "on",
    "sources": [{"type": "x", "x_handles": ["nadoHQ", "inkonchain"]}],
}

X_CRYPTO_SEARCH_PARAMS = {
    "mode": "on",
    "sources": [{"type": "x"}],
}

ROUTER_SYSTEM_PROMPT = """You are a routing agent for Nadobro, the support AI for Nado DEX.

Today's date: {current_date}

Your job is to analyze the user's question and call the right tools to gather the information needed to answer it.

You have 4 tools available:
1. search_knowledge_base — Static Nado product knowledge (features, trading, margin, fees, NFTs, dev docs, architecture)
2. search_web — Live info from Nado docs (docs.nado.xyz), website (nado.xyz), and general web search
3. search_x_twitter — Latest posts from @nadoHQ and @inkonchain on X/Twitter
4. get_market_sentiment — Crypto market sentiment from general web + crypto Twitter

Rules:
- For product/feature questions: call search_knowledge_base
- For recent news/updates/announcements/points/airdrops: call search_web AND search_x_twitter
- For "what did Nado tweet" or X/Twitter questions: call search_x_twitter
- For market conditions/price/sentiment: call get_market_sentiment
- For questions needing both static knowledge AND current info: call multiple tools
- If you're not sure, call search_knowledge_base + search_web to be thorough
- You can call multiple tools in a single response

Do NOT answer the question yourself. Only call the tools."""

SYNTHESIZER_SYSTEM_PROMPT = """You are Nadobro Support AI for Nado DEX — a CLOB-based exchange on the Ink L2 blockchain offering perpetual futures and spot trading.

Today's date: {current_date}

You have been given context gathered from Nado's knowledge base and live web sources. Use ONLY this context to answer the user's question.

STRICT RULES:
- Answer ONLY based on the provided context. If the context does not contain the answer, say clearly: "I don't have confirmed information about this. Please check Nado's official channels for the latest."
- NEVER fabricate, guess, or invent information not present in the context.
- Be accurate and concise: 3-5 bullet points for standard questions, more detail if the user asks.
- Plain text only — no MarkdownV2 escapes.
- Keep response under 1200 characters unless user asks for detail.
- End with a "Sources:" line listing the sources used.
- For time-sensitive questions (points, airdrops, events), if you only have static/old info, say you don't have confirmed current data.

CONTEXT:
{context}"""

X_TWITTER_SYSTEM_PROMPT = """You are Nadobro Support AI for Nado, with real-time access to X (Twitter) content via live search.

Today's date: {current_date}

Official X accounts:
- @nadoHQ (https://x.com/nadoHQ) — Nado DEX official
- @inkonchain (https://x.com/inkonchain) — Ink L2 blockchain (Nado's chain)

Nado is a CLOB-based DEX on the Ink L2 blockchain offering perpetual futures and spot trading.

Your task:
- Search for and report the MOST RECENT posts from @nadoHQ and @inkonchain.
- "Latest" means the most recent tweets closest to today's date ({current_date}). Prioritize tweets from {current_year} over older ones.
- Report the actual tweet content verbatim, including the full text, date/time, and any links or media mentioned.
- If asked about a specific topic, search for relevant tweets about that topic from either account.

STRICT rules:
- ONLY return content from @nadoHQ and @inkonchain. Never include tweets from other accounts.
- If you cannot find relevant tweets, say so honestly. Do NOT fabricate or invent tweet content.
- Do NOT use MarkdownV2 syntax escapes; plain text only.
- Include the tweet date/time and which account posted it for each tweet.
- Keep response under 1200 characters.
- End with: Sources: https://x.com/nadoHQ, https://x.com/inkonchain

Relevant Nado Knowledge:
{knowledge_base}
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
    use_live = _should_use_live_retrieval(question)
    if use_live:
        for url in OFFICIAL_URLS:
            if "x.com" in url:
                continue
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


def _execute_x_search(query: str) -> tuple[str, list[str]]:
    client = _get_xai_client()
    if not client:
        return "[X SEARCH] xAI client not available — cannot search X/Twitter.", []

    now = datetime.utcnow()
    try:
        response = client.chat.completions.create(
            model=XAI_X_SEARCH_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        f"Today is {now.strftime('%Y-%m-%d')}. "
                        "Search X for the most recent posts from @nadoHQ and @inkonchain. "
                        "Return the actual tweet content verbatim with dates. "
                        f"Focus on tweets from {now.year}. Plain text only."
                    ),
                },
                {"role": "user", "content": query},
            ],
            max_tokens=600,
            temperature=0.1,
            extra_body={"search_parameters": X_NADO_SEARCH_PARAMS},
        )
        content = response.choices[0].message.content
        if content and content.strip():
            return (
                f"[X/TWITTER RESULTS — @nadoHQ & @inkonchain]\n{content.strip()}",
                ["https://x.com/nadoHQ", "https://x.com/inkonchain"],
            )
    except Exception as e:
        logger.warning(f"X search failed: {e}")

    return "[X SEARCH] No results found from @nadoHQ or @inkonchain.", []


def _execute_market_sentiment(query: str) -> tuple[str, list[str]]:
    parts = []
    sources = []

    web_results = _search_live(query)
    for url, txt in web_results[:3]:
        snippet = txt[:1500]
        parts.append(f"[WEB — {url}]\n{snippet}")
        sources.append(url)

    client = _get_xai_client()
    if client:
        now = datetime.utcnow()
        try:
            response = client.chat.completions.create(
                model=XAI_X_SEARCH_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            f"Today is {now.strftime('%Y-%m-%d')}. "
                            "Search crypto Twitter for the latest market sentiment, news, and opinions "
                            "related to the user's query. Include notable tweets from crypto traders, "
                            "analysts, and news accounts. Report tweet content with dates and handles. "
                            "Plain text only."
                        ),
                    },
                    {"role": "user", "content": query},
                ],
                max_tokens=600,
                temperature=0.1,
                extra_body={"search_parameters": X_CRYPTO_SEARCH_PARAMS},
            )
            content = response.choices[0].message.content
            if content and content.strip():
                parts.append(f"[CRYPTO TWITTER SENTIMENT]\n{content.strip()}")
                sources.append("https://x.com (crypto twitter)")
        except Exception as e:
            logger.warning(f"Crypto Twitter sentiment search failed: {e}")

    if not parts:
        return "[MARKET SENTIMENT] No market data available.", []

    return "\n\n".join(parts), list(dict.fromkeys(sources))


def _execute_agent_tool(tool_name: str, args: dict, question: str) -> tuple[str, list[str]]:
    if tool_name == "search_knowledge_base":
        query = args.get("query", question)
        sections = _search_knowledge_sections(query, top_k=4)
        if sections:
            return f"[KNOWLEDGE BASE RESULTS]\n{sections}", []
        return "[KNOWLEDGE BASE] No matching sections found.", []

    elif tool_name == "search_web":
        query = args.get("query", question)
        context, sources = _build_retrieved_context(query)
        if context and context != "No retrieved context available.":
            return f"[WEB SEARCH RESULTS]\n{context}", sources
        return "[WEB SEARCH] No relevant results found.", []

    elif tool_name == "search_x_twitter":
        query = args.get("query", question)
        return _execute_x_search(query)

    elif tool_name == "get_market_sentiment":
        query = args.get("query", question)
        return _execute_market_sentiment(query)

    return f"[ERROR] Unknown tool: {tool_name}", []


def _run_agent_pipeline(question: str, provider: str) -> tuple[str, list[str]]:
    client = _get_xai_client() if provider == "xai" else _get_openai_client()
    if not client:
        raise RuntimeError(f"{provider.upper()} client not configured")

    now = datetime.utcnow()
    current_date = now.strftime("%Y-%m-%d")

    router_system = ROUTER_SYSTEM_PROMPT.format(current_date=current_date)
    router_model = _model_for(provider)

    try:
        router_response = client.chat.completions.create(
            model=router_model,
            messages=[
                {"role": "system", "content": router_system},
                {"role": "user", "content": question},
            ],
            tools=AGENT_TOOLS,
            tool_choice="auto",
            max_tokens=200,
            temperature=0.0,
        )
    except Exception as e:
        logger.warning(f"Agent router call failed ({provider}): {e}")
        kb_context = _search_knowledge_sections(question, top_k=4)
        return kb_context or _load_knowledge_base()[:6000], []

    tool_calls = []
    if router_response.choices and router_response.choices[0].message.tool_calls:
        tool_calls = router_response.choices[0].message.tool_calls

    if not tool_calls:
        kb_context = _search_knowledge_sections(question, top_k=4)
        live_needed = _should_use_live_retrieval(question)
        if live_needed:
            web_ctx, web_sources = _build_retrieved_context(question)
            combined = f"[KNOWLEDGE BASE]\n{kb_context}\n\n[WEB SEARCH]\n{web_ctx}"
            return combined, web_sources
        return f"[KNOWLEDGE BASE]\n{kb_context}", []

    all_context_parts = []
    all_sources = []
    for tc in tool_calls:
        try:
            fn_name = tc.function.name
            fn_args = json.loads(tc.function.arguments) if tc.function.arguments else {}
        except (json.JSONDecodeError, AttributeError):
            continue

        ctx, sources = _execute_agent_tool(fn_name, fn_args, question)
        all_context_parts.append(ctx)
        all_sources.extend(sources)

    combined_context = "\n\n".join(all_context_parts) if all_context_parts else ""

    if not combined_context.strip():
        kb_context = _search_knowledge_sections(question, top_k=4)
        combined_context = f"[KNOWLEDGE BASE]\n{kb_context}"

    return combined_context, list(dict.fromkeys(all_sources))


def _stream_support_llm(provider: str, system: str, question: str, x_search: bool = False):
    if provider == "openai":
        client = _get_openai_client()
    else:
        client = _get_xai_client()

    if not client:
        raise RuntimeError(f"{provider.upper()} client not configured")

    max_tokens = 700 if (_wants_detailed_answer(question) or x_search) else 500

    kwargs = dict(
        model=XAI_X_SEARCH_MODEL if (x_search and provider == "xai") else _model_for(provider),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        max_tokens=max_tokens,
        temperature=0.2,
        stream=True,
    )
    if x_search and provider == "xai":
        kwargs["extra_body"] = {"search_parameters": X_NADO_SEARCH_PARAMS}

    stream = client.chat.completions.create(**kwargs)
    for chunk in stream:
        delta = chunk.choices[0].delta if chunk.choices else None
        if delta and delta.content:
            yield delta.content


async def stream_nado_answer(question: str):
    started_at = time.time()
    xai_client = _get_xai_client()
    openai_client = _get_openai_client()
    if not xai_client and not openai_client:
        yield "AI service is not configured. Add XAI_API_KEY and/or OPENAI_API_KEY then restart the bot."
        return

    if _is_x_twitter_question(question) and not xai_client:
        yield "X/Twitter search requires the xAI (Grok) service. Please ask a different question or contact support."
        return

    knowledge = _load_knowledge_base()
    if not knowledge:
        yield "Knowledge base is not loaded. Please contact support."
        return

    qkey = _normalize_question(question)
    cached = _answer_cache.get(qkey)
    if cached and (time.time() - cached["ts"] < ANSWER_CACHE_TTL_SECONDS):
        yield cached["answer"]
        return

    is_x_question = _is_x_twitter_question(question)
    use_x_prompt = is_x_question and xai_client is not None

    now = datetime.utcnow()
    current_date = now.strftime("%Y-%m-%d")

    if use_x_prompt:
        system = X_TWITTER_SYSTEM_PROMPT.format(
            knowledge_base=_search_knowledge_sections(question, top_k=2),
            current_date=current_date,
            current_year=str(now.year),
        )
        used_sources = ["https://x.com/nadoHQ", "https://x.com/inkonchain"]
    else:
        import asyncio
        loop = asyncio.get_event_loop()

        primary = _pick_primary_provider(question)
        try:
            gathered_context, used_sources = await loop.run_in_executor(
                None, _run_agent_pipeline, question, primary
            )
        except Exception as e:
            logger.warning(f"Agent pipeline failed: {e}")
            gathered_context = _search_knowledge_sections(question, top_k=4)
            used_sources = []

        system = SYNTHESIZER_SYSTEM_PROMPT.format(
            current_date=current_date,
            context=gathered_context[:12000],
        )

    primary = _pick_primary_provider(question)
    if use_x_prompt:
        primary = "xai"
    secondary = "openai" if primary == "xai" else "xai"
    providers = [primary, secondary]
    providers = [
        p for p in providers
        if (p == "xai" and xai_client) or (p == "openai" and openai_client)
    ]

    import asyncio, queue, threading
    loop = asyncio.get_event_loop()

    for provider in providers:
        try:
            chunk_queue = queue.Queue()

            def _run_stream(p=provider, xs=use_x_prompt):
                try:
                    for chunk_text in _stream_support_llm(p, system, question, x_search=xs):
                        chunk_queue.put(chunk_text)
                    chunk_queue.put(None)
                except Exception as e:
                    chunk_queue.put(e)

            thread = threading.Thread(target=_run_stream, daemon=True)
            thread.start()

            full_answer = ""
            while True:
                try:
                    item = await loop.run_in_executor(None, lambda: chunk_queue.get(timeout=30))
                except Exception:
                    break
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                full_answer += item
                yield item

            if not full_answer.strip():
                continue

            if "Sources:" not in full_answer:
                if use_x_prompt:
                    sources_line = "\n\nSources: https://x.com/nadoHQ, https://x.com/inkonchain"
                elif used_sources:
                    sources_line = "\n\nSources: " + ", ".join(used_sources[:4])
                else:
                    sources_line = "\n\nSources: https://docs.nado.xyz/, https://x.com/nadoHQ, https://x.com/inkonchain"
                yield sources_line
                full_answer += sources_line

            _answer_cache[qkey] = {"ts": time.time(), "answer": full_answer}
            logger.info("Streamed answer via %s in %.1fs", provider, time.time() - started_at)
            return
        except Exception as provider_error:
            logger.warning("Stream answer failed on provider=%s: %s", provider, provider_error)
            continue

    yield "I couldn't generate an answer. Please try again."


async def answer_nado_question(question: str) -> str:
    started_at = time.time()
    xai_client = _get_xai_client()
    openai_client = _get_openai_client()
    if not xai_client and not openai_client:
        return (
            "AI service is not configured. Add XAI_API_KEY and/or OPENAI_API_KEY "
            "then restart the bot."
        )

    if _is_x_twitter_question(question) and not xai_client:
        return "X/Twitter search requires the xAI (Grok) service. Please ask a different question or contact support."

    knowledge = _load_knowledge_base()
    if not knowledge:
        return "Knowledge base is not loaded. Please contact support."

    qkey = _normalize_question(question)
    cached = _answer_cache.get(qkey)
    if cached and (time.time() - cached["ts"] < ANSWER_CACHE_TTL_SECONDS):
        return cached["answer"]

    is_x_question = _is_x_twitter_question(question)
    use_x_prompt = is_x_question and xai_client is not None

    now = datetime.utcnow()
    current_date = now.strftime("%Y-%m-%d")

    if use_x_prompt:
        system = X_TWITTER_SYSTEM_PROMPT.format(
            knowledge_base=_search_knowledge_sections(question, top_k=2),
            current_date=current_date,
            current_year=str(now.year),
        )
        used_sources = ["https://x.com/nadoHQ", "https://x.com/inkonchain"]
    else:
        import asyncio
        loop = asyncio.get_event_loop()

        primary = _pick_primary_provider(question)
        try:
            gathered_context, used_sources = await loop.run_in_executor(
                None, _run_agent_pipeline, question, primary
            )
        except Exception as e:
            logger.warning(f"Agent pipeline failed: {e}")
            gathered_context = _search_knowledge_sections(question, top_k=4)
            used_sources = []

        system = SYNTHESIZER_SYSTEM_PROMPT.format(
            current_date=current_date,
            context=gathered_context[:12000],
        )

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        primary = _pick_primary_provider(question)
        if use_x_prompt:
            primary = "xai"
        secondary = "openai" if primary == "xai" else "xai"

        providers = [primary, secondary]
        providers = [
            p for p in providers
            if (p == "xai" and xai_client) or (p == "openai" and openai_client)
        ]

        last_error = None
        answer = None
        used_provider = None
        for provider in providers:
            try:
                def _call(p=provider):
                    client = _get_xai_client() if p == "xai" else _get_openai_client()
                    max_tokens = 700 if (_wants_detailed_answer(question) or use_x_prompt) else 500
                    kwargs = dict(
                        model=XAI_X_SEARCH_MODEL if (use_x_prompt and p == "xai") else _model_for(p),
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": question},
                        ],
                        max_tokens=max_tokens,
                        temperature=0.2,
                    )
                    if use_x_prompt and p == "xai":
                        kwargs["extra_body"] = {"search_parameters": X_NADO_SEARCH_PARAMS}
                    resp = client.chat.completions.create(**kwargs)
                    content = resp.choices[0].message.content
                    if not content or not content.strip():
                        raise RuntimeError(f"{p.upper()} returned empty response")
                    return content.strip()

                answer = await loop.run_in_executor(None, _call)
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

        logger.info("Support answer generated via provider=%s in %.1fs", used_provider, time.time() - started_at)
        if "Sources:" not in answer:
            if use_x_prompt:
                sources_line = "Sources: https://x.com/nadoHQ, https://x.com/inkonchain"
            elif used_sources:
                sources_line = "Sources: " + ", ".join(used_sources[:4])
            else:
                sources_line = "Sources: https://docs.nado.xyz/, https://x.com/nadoHQ, https://x.com/inkonchain"
            answer = f"{answer}\n\n{sources_line}"
        _answer_cache[qkey] = {"ts": time.time(), "answer": answer}
        return answer
    except Exception as e:
        logger.error(f"Knowledge Q&A failed: {e}", exc_info=True)
        return "Something went wrong while answering your question. Please try again."
