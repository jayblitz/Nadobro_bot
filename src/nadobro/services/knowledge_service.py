import os
import re
import json
import time
import logging
import requests as _requests
from datetime import datetime
from pathlib import Path
from collections import defaultdict

from openai import OpenAI

logger = logging.getLogger(__name__)

_knowledge_base = None
_knowledge_sections = None
_xai_client = None
_openai_client = None
_answer_cache = {}

_chat_history: dict[int, list[dict]] = defaultdict(list)
CHAT_HISTORY_MAX_MESSAGES = 8
CHAT_HISTORY_TTL_SECONDS = 900

_fng_cache: dict = {}
FNG_CACHE_TTL_SECONDS = 300

KNOWLEDGE_FILE = Path(__file__).parent.parent / "data" / "nado_knowledge.txt"
ANSWER_CACHE_TTL_SECONDS = 300

OFFICIAL_SOURCES = {
    "docs": "https://docs.nado.xyz/",
    "website": "https://www.nado.xyz/",
    "x_nado": "https://x.com/nadoHQ",
    "x_ink": "https://x.com/inkonchain",
    "points": "https://docs.nado.xyz/points/referrals",
    "api": "https://docs.nado.xyz/developer-resources/api/gateway",
    "get_started": "https://docs.nado.xyz/developer-resources/get-started",
}


def _normalize_question(question: str) -> str:
    return re.sub(r"\s+", " ", (question or "").strip().lower())


def _get_chat_history(telegram_id: int) -> list[dict]:
    now = time.time()
    history = _chat_history.get(telegram_id, [])
    history = [m for m in history if now - m["ts"] < CHAT_HISTORY_TTL_SECONDS]
    _chat_history[telegram_id] = history
    return history


def _add_to_chat_history(telegram_id: int, role: str, content: str):
    now = time.time()
    history = _chat_history[telegram_id]
    history = [m for m in history if now - m["ts"] < CHAT_HISTORY_TTL_SECONDS]
    history.append({"role": role, "content": content, "ts": now})
    if len(history) > CHAT_HISTORY_MAX_MESSAGES:
        history = history[-CHAT_HISTORY_MAX_MESSAGES:]
    _chat_history[telegram_id] = history


def _build_history_messages(telegram_id: int) -> list[dict]:
    history = _get_chat_history(telegram_id)
    return [{"role": m["role"], "content": m["content"]} for m in history]


def _is_casual_message(text: str) -> bool:
    t = (text or "").strip().lower().rstrip("!.?")
    casual_patterns = {
        "gm", "gn", "good morning", "good night", "good evening", "good afternoon",
        "hi", "hey", "hello", "hola", "sup", "yo", "whats up", "what's up",
        "thanks", "thank you", "thx", "ty", "cheers",
        "bye", "goodbye", "cya", "see ya", "later", "peace",
        "how are you", "how r u", "hows it going", "how's it going",
        "wassup", "wsg", "wagmi", "gm fam", "lfg",
    }
    return t in casual_patterns or len(t) <= 3


def _fetch_fear_greed_index() -> str:
    now = time.time()
    cached = _fng_cache.get("data")
    if cached and now - _fng_cache.get("ts", 0) < FNG_CACHE_TTL_SECONDS:
        return cached
    try:
        resp = _requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        data = resp.json().get("data", [{}])[0]
        value = data.get("value", "N/A")
        classification = data.get("value_classification", "N/A")
        result = f"Fear & Greed Index: {value}/100 ({classification})"
        _fng_cache["data"] = result
        _fng_cache["ts"] = now
        return result
    except Exception as e:
        logger.warning(f"Fear & Greed Index fetch failed: {e}")
        return "Fear & Greed Index: unavailable"


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
    if _is_complex_question(question):
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
    "deposit": {"collateral", "usdt0", "funding"},
    "withdraw": {"withdrawal", "remove"},
    "wallet": {"connect", "deposit", "address"},
    "nlp": {"vault", "liquidity", "provider", "yield"},
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
            "description": (
                "Search Nado's official knowledge base. PRIMARY source for all Nado DEX questions: "
                "features, trading, margin, fees, points, rewards, NFTs, NLP vault, developer docs, "
                "architecture, order types, liquidations, getting started, supported markets. "
                "ALWAYS call this first for any Nado-related question."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for the knowledge base"
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_live_price",
            "description": (
                "Get LIVE current price for a crypto asset from Nado DEX. "
                "Use when user asks about current price, how much something costs, or price of any asset. "
                "Supports: BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "product": {
                        "type": "string",
                        "description": "Asset symbol (BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX)"
                    }
                },
                "required": ["product"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_market_sentiment",
            "description": (
                "Get current crypto market sentiment including Fear & Greed Index, trending news, "
                "and opinions from crypto traders and analysts. Use when user asks about market conditions, "
                "sentiment, fear/greed, whether market is bullish/bearish, or general crypto outlook."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The market/sentiment query"
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
            "description": (
                "Search X (Twitter) for latest posts from @nadoHQ and @inkonchain. "
                "ONLY for questions about Nado tweets, social media posts, or recent announcements."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for X/Twitter"
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

ROUTER_SYSTEM_PROMPT = """You are a routing agent for Nadobro, a crypto trading AI on Nado DEX.

Today's date: {current_date}

Analyze the user's message and call the right tool(s) to gather information.

Tools (4):
1. search_knowledge_base — Nado product knowledge (features, fees, margin, points, NFTs, NLP, dev docs, getting started, how things work). PRIMARY source for all Nado questions.
2. get_live_price — LIVE current price from Nado DEX. Use for "what's BTC price?", "how much is ETH?", "price of SOL" etc.
3. get_market_sentiment — Crypto market sentiment, Fear & Greed Index, trending news. Use for "is the market bullish?", "what's the sentiment?", market outlook.
4. search_x_twitter — Latest tweets from @nadoHQ and @inkonchain. ONLY for Nado social media/announcements.

Routing rules:
- "What's BTC price?" / "price of ETH" / "how much is SOL" → get_live_price
- "What are Nado fees?" / "how does margin work?" / "how to get started" → search_knowledge_base
- "Is the market bullish?" / "market sentiment" / "fear and greed" → get_market_sentiment
- "What did Nado tweet?" / "latest announcements" → search_x_twitter
- "Should I buy BTC?" → get_live_price AND get_market_sentiment
- Casual greetings (gm, hi, hello, thanks, bye) → do NOT call any tools (handled separately)
- When in doubt → search_knowledge_base

You can call multiple tools. Do NOT answer the question yourself — only call tools."""

CASUAL_SYSTEM_PROMPT = """You are Nadobro, a friendly and knowledgeable crypto trading companion on Telegram for Nado DEX.

Today's date: {current_date}
User's name: {user_name}

You are chatting with {user_name}. Be warm, natural, and engaging — like a knowledgeable crypto friend.

PERSONALITY:
- Respond to greetings enthusiastically but naturally (GM {user_name}! / Hey! / What's good!)
- Use the user's name occasionally but not every message
- Be encouraging about trading but never give financial advice
- Match the user's energy — casual if they're casual, detailed if they ask specifics
- You can use crypto slang naturally (WAGMI, LFG, degen, alpha, etc.)
- Keep casual responses SHORT (1-3 sentences max)
- If thanked, be gracious and offer to help more
- If they say bye/gn, wish them well and remind them you're always here

You are a trading bot for Nado DEX. Users can:
- Ask about prices, market sentiment, Nado features
- Execute trades by typing: "long BTC 0.01 5x market" or "short ETH 0.05 10x limit 2500"
- Supported assets: BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX (perpetuals up to 20x leverage)

If the conversation naturally leads to trading, you can mention: "Just type something like 'long BTC 0.01 5x market' and I'll set it up for you!"

Plain text only. No markdown. No source links needed for casual chat."""

SYNTHESIZER_SYSTEM_PROMPT = """You are Nadobro, an expert crypto trading companion for Nado DEX — a high-performance CLOB exchange on Ink L2 (backed by Kraken) with perpetual futures and spot trading.

Today's date: {current_date}
User's name: {user_name}

You have context from Nado's knowledge base and/or live data. Use it to answer accurately and conversationally.

PERSONALITY:
- You're a knowledgeable crypto friend, not a stiff support bot
- Address the user by name occasionally
- Be direct — answer first, then elaborate
- If discussing trading ideas, you can suggest trades: "You could try typing: long BTC 0.01 5x market"
- Supported trade format: "[long/short] [asset] [size] [leverage]x [market/limit] [price]"
- Supported assets: BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX (perps, up to 20x)

RULES:
1. Answer the question DIRECTLY first, then provide supporting details
2. Use ONLY the provided context. If not in context, say: "I don't have that info right now. Check Nado's docs or ask in the community!"
3. NEVER fabricate or guess information
4. Plain text only — no markdown formatting
5. Keep responses focused: 2-6 sentences for simple questions, more for complex ones
6. For price data, always mention it's from Nado DEX and include bid/ask when available
7. For market sentiment, present it conversationally with the Fear & Greed reading

SOURCE RULES:
- ONLY cite: docs.nado.xyz, nado.xyz, x.com/nadoHQ, x.com/inkonchain
- NEVER include search engine links
- End with "Sources:" followed by 1-3 relevant official URLs (skip for price/sentiment queries)

CONTEXT:
{context}"""

X_TWITTER_SYSTEM_PROMPT = """You are Nadobro, the expert AI assistant for Nado DEX, with real-time access to X (Twitter).

Today's date: {current_date}

Official X accounts:
- @nadoHQ (https://x.com/nadoHQ) — Nado DEX official
- @inkonchain (https://x.com/inkonchain) — Ink L2 blockchain (Nado's chain)

Nado is a CLOB-based DEX on the Ink L2 blockchain (backed by Kraken) offering perpetual futures and spot trading with unified margin.

Your task:
- Search for and report the MOST RECENT posts from @nadoHQ and @inkonchain
- "Latest" means the most recent tweets closest to today ({current_date}). Prioritize tweets from {current_year}.
- Report actual tweet content with dates and which account posted
- If asked about a specific topic, find relevant tweets about that topic

RULES:
- ONLY return content from @nadoHQ and @inkonchain
- If you cannot find relevant tweets, say so honestly
- Plain text only
- Include tweet dates and account handles
- Keep response under 1500 characters
- End with: Sources: https://x.com/nadoHQ, https://x.com/inkonchain
- NEVER include DuckDuckGo, Google, or search engine links

Relevant Nado Knowledge:
{knowledge_base}
"""


def _pick_sources_for_question(question: str) -> list[str]:
    q = _normalize_question(question)
    sources = []

    if any(w in q for w in ["api", "sdk", "developer", "code", "integrate", "websocket", "gateway"]):
        sources.append(OFFICIAL_SOURCES["api"])
        sources.append(OFFICIAL_SOURCES["get_started"])
    elif any(w in q for w in ["point", "reward", "referral", "invite", "season", "earn"]):
        sources.append(OFFICIAL_SOURCES["points"])
        sources.append(OFFICIAL_SOURCES["docs"])
    elif any(w in q for w in ["tweet", "twitter", "x.com", "announced", "news"]):
        sources.append(OFFICIAL_SOURCES["x_nado"])
        sources.append(OFFICIAL_SOURCES["x_ink"])
    else:
        sources.append(OFFICIAL_SOURCES["docs"])
        sources.append(OFFICIAL_SOURCES["website"])

    return sources


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
                [OFFICIAL_SOURCES["x_nado"], OFFICIAL_SOURCES["x_ink"]],
            )
    except Exception as e:
        logger.warning(f"X search failed: {e}")

    return "[X SEARCH] No results found from @nadoHQ or @inkonchain.", []


def _execute_live_price(product: str) -> tuple[str, list[str]]:
    from src.nadobro.config import PRODUCTS, get_product_id
    from src.nadobro.services.nado_client import NadoClient

    symbol = product.strip().upper().replace("-PERP", "")
    product_id = get_product_id(symbol)

    if product_id is None:
        if symbol == "ALL":
            try:
                client = NadoClient.from_address("0x0000000000000000000000000000000000000000", "mainnet")
                prices = client.get_all_market_prices()
                lines = ["[LIVE PRICES FROM NADO DEX]"]
                for name, p in sorted(prices.items()):
                    if p.get("mid", 0) > 0:
                        lines.append(f"{name}-PERP: ${p['mid']:,.2f} (Bid: ${p['bid']:,.2f} / Ask: ${p['ask']:,.2f})")
                return "\n".join(lines), [OFFICIAL_SOURCES["website"]]
            except Exception as e:
                logger.warning(f"All prices fetch failed: {e}")
                return "[LIVE PRICE] Could not fetch prices right now.", []

        supported = [n for n, i in PRODUCTS.items() if i["type"] == "perp"]
        return f"[LIVE PRICE] Unknown asset '{product}'. Supported: {', '.join(supported)}", []

    try:
        client = NadoClient.from_address("0x0000000000000000000000000000000000000000", "mainnet")
        price_data = client.get_market_price(product_id)
        mid = price_data.get("mid", 0)
        bid = price_data.get("bid", 0)
        ask = price_data.get("ask", 0)
        spread = ask - bid if ask and bid else 0
        spread_bps = (spread / mid * 10000) if mid else 0

        result = (
            f"[LIVE PRICE FROM NADO DEX]\n"
            f"{symbol}-PERP:\n"
            f"  Mid Price: ${mid:,.2f}\n"
            f"  Bid: ${bid:,.2f}\n"
            f"  Ask: ${ask:,.2f}\n"
            f"  Spread: ${spread:,.2f} ({spread_bps:.1f} bps)"
        )
        return result, [OFFICIAL_SOURCES["website"]]
    except Exception as e:
        logger.warning(f"Live price fetch failed for {symbol}: {e}")
        return f"[LIVE PRICE] Could not fetch price for {symbol} right now.", []


def _execute_market_sentiment(query: str) -> tuple[str, list[str]]:
    fng = _fetch_fear_greed_index()

    client = _get_xai_client()
    if not client:
        return f"[MARKET SENTIMENT]\n{fng}\n\nxAI client not available for detailed sentiment.", []

    now = datetime.utcnow()
    try:
        response = client.chat.completions.create(
            model=XAI_X_SEARCH_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        f"Today is {now.strftime('%Y-%m-%d')}. "
                        "Search crypto Twitter for the latest market sentiment, breaking news, and analysis. "
                        "Focus on posts from major crypto news accounts like @WatcherGuru, @CoinDesk, "
                        "@TheBlock__, @Cointelegraph, @whale_alert, and prominent crypto analysts. "
                        "Report key developments, price movements, regulatory news, and market-moving events. "
                        "Include tweet content with dates and handles. Plain text only."
                    ),
                },
                {"role": "user", "content": query},
            ],
            max_tokens=700,
            temperature=0.1,
            extra_body={"search_parameters": X_CRYPTO_SEARCH_PARAMS},
        )
        content = response.choices[0].message.content
        if content and content.strip():
            return (
                f"[MARKET SENTIMENT]\n{fng}\n\n{content.strip()}",
                [OFFICIAL_SOURCES["x_nado"]],
            )
    except Exception as e:
        logger.warning(f"Crypto Twitter sentiment search failed: {e}")

    return f"[MARKET SENTIMENT]\n{fng}\n\nNo additional sentiment data available.", []


def _execute_agent_tool(tool_name: str, args: dict, question: str) -> tuple[str, list[str]]:
    if tool_name == "search_knowledge_base":
        query = args.get("query", question)
        sections = _search_knowledge_sections(query, top_k=5)
        if sections:
            return f"[KNOWLEDGE BASE RESULTS]\n{sections}", _pick_sources_for_question(query)
        return "[KNOWLEDGE BASE] No matching sections found.", [OFFICIAL_SOURCES["docs"]]

    elif tool_name == "get_live_price":
        product = args.get("product", "BTC")
        return _execute_live_price(product)

    elif tool_name == "search_x_twitter":
        query = args.get("query", question)
        return _execute_x_search(query)

    elif tool_name == "get_market_sentiment":
        query = args.get("query", question)
        return _execute_market_sentiment(query)

    elif tool_name == "search_web":
        query = args.get("query", question)
        sections = _search_knowledge_sections(query, top_k=5)
        if sections:
            return f"[KNOWLEDGE BASE RESULTS]\n{sections}", _pick_sources_for_question(query)
        return "[KNOWLEDGE BASE] No matching sections found.", [OFFICIAL_SOURCES["docs"]]

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
        kb_context = _search_knowledge_sections(question, top_k=5)
        return kb_context or _load_knowledge_base()[:6000], _pick_sources_for_question(question)

    tool_calls = []
    if router_response.choices and router_response.choices[0].message.tool_calls:
        tool_calls = router_response.choices[0].message.tool_calls

    if not tool_calls:
        kb_context = _search_knowledge_sections(question, top_k=5)
        return f"[KNOWLEDGE BASE]\n{kb_context}", _pick_sources_for_question(question)

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
        kb_context = _search_knowledge_sections(question, top_k=5)
        combined_context = f"[KNOWLEDGE BASE]\n{kb_context}"

    if not all_sources:
        all_sources = _pick_sources_for_question(question)

    return combined_context, list(dict.fromkeys(all_sources))


def _filter_official_sources(sources: list[str]) -> list[str]:
    allowed = set(OFFICIAL_SOURCES.values())
    filtered = [s for s in sources if s in allowed]
    if not filtered:
        filtered = [OFFICIAL_SOURCES["docs"], OFFICIAL_SOURCES["website"]]
    return filtered[:4]


def _stream_support_llm(provider: str, system: str, question: str, x_search: bool = False, history: list[dict] = None):
    if provider == "openai":
        client = _get_openai_client()
    else:
        client = _get_xai_client()

    if not client:
        raise RuntimeError(f"{provider.upper()} client not configured")

    max_tokens = 800 if (_wants_detailed_answer(question) or x_search) else 600

    messages = [{"role": "system", "content": system}]
    if history:
        messages.extend(history[-6:])
    messages.append({"role": "user", "content": question})

    kwargs = dict(
        model=XAI_X_SEARCH_MODEL if (x_search and provider == "xai") else _model_for(provider),
        messages=messages,
        max_tokens=max_tokens,
        temperature=0.3,
        stream=True,
    )
    if x_search and provider == "xai":
        kwargs["extra_body"] = {"search_parameters": X_NADO_SEARCH_PARAMS}

    stream = client.chat.completions.create(**kwargs)
    for chunk in stream:
        delta = chunk.choices[0].delta if chunk.choices else None
        if delta and delta.content:
            yield delta.content


def _is_x_twitter_question(question: str) -> bool:
    q = _normalize_question(question)
    signals = ["tweet", "tweets", "x.com", "twitter", "post on x", "posted on x", "nadohq", "inkonchain"]
    return any(sig in q for sig in signals)


def _is_price_question(question: str) -> bool:
    q = _normalize_question(question)
    price_signals = ["price of", "price for", "how much is", "what's btc", "what's eth",
                     "btc price", "eth price", "sol price", "current price", "live price",
                     "market price", "what is btc at", "what is eth at"]
    return any(sig in q for sig in price_signals)


async def stream_nado_answer(question: str, telegram_id: int = None, user_name: str = None):
    started_at = time.time()
    xai_client = _get_xai_client()
    openai_client = _get_openai_client()
    if not xai_client and not openai_client:
        yield "AI service is not configured. Add XAI_API_KEY and/or OPENAI_API_KEY then restart the bot."
        return

    display_name = user_name or "trader"
    history_msgs = _build_history_messages(telegram_id) if telegram_id else []

    if telegram_id:
        _add_to_chat_history(telegram_id, "user", question)

    now = datetime.utcnow()
    current_date = now.strftime("%Y-%m-%d")

    if _is_casual_message(question):
        system = CASUAL_SYSTEM_PROMPT.format(
            current_date=current_date,
            user_name=display_name,
        )
        primary = _pick_primary_provider(question)
        try:
            full_answer = ""
            import asyncio, queue, threading
            loop = asyncio.get_event_loop()
            chunk_queue = queue.Queue()

            def _run_casual(p=primary):
                try:
                    for chunk_text in _stream_support_llm(p, system, question, history=history_msgs):
                        chunk_queue.put(chunk_text)
                    chunk_queue.put(None)
                except Exception as e:
                    chunk_queue.put(e)

            thread = threading.Thread(target=_run_casual, daemon=True)
            thread.start()

            while True:
                try:
                    item = await loop.run_in_executor(None, lambda: chunk_queue.get(timeout=15))
                except Exception:
                    break
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                full_answer += item
                yield item

            if full_answer.strip() and telegram_id:
                _add_to_chat_history(telegram_id, "assistant", full_answer.strip())
            if full_answer.strip():
                logger.info("Casual response via %s in %.1fs", primary, time.time() - started_at)
                return
        except Exception as e:
            logger.warning(f"Casual response failed: {e}")

        yield f"GM {display_name}! How can I help you today?"
        if telegram_id:
            _add_to_chat_history(telegram_id, "assistant", f"GM {display_name}! How can I help you today?")
        return

    if _is_x_twitter_question(question) and not xai_client:
        yield "X/Twitter search requires the xAI (Grok) service. Please ask a different question or contact support."
        return

    knowledge = _load_knowledge_base()
    if not knowledge:
        yield "Knowledge base is not loaded. Please contact support."
        return

    is_x_question = _is_x_twitter_question(question)
    use_x_prompt = is_x_question and xai_client is not None

    if use_x_prompt:
        system = X_TWITTER_SYSTEM_PROMPT.format(
            knowledge_base=_search_knowledge_sections(question, top_k=2),
            current_date=current_date,
            current_year=str(now.year),
        )
        used_sources = [OFFICIAL_SOURCES["x_nado"], OFFICIAL_SOURCES["x_ink"]]
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
            gathered_context = _search_knowledge_sections(question, top_k=5)
            used_sources = _pick_sources_for_question(question)

        system = SYNTHESIZER_SYSTEM_PROMPT.format(
            current_date=current_date,
            user_name=display_name,
            context=gathered_context[:12000],
        )

    used_sources = _filter_official_sources(used_sources)
    skip_sources = _is_price_question(question) or _is_casual_message(question)

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
                    for chunk_text in _stream_support_llm(p, system, question, x_search=xs, history=history_msgs):
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

            if not skip_sources and "Sources:" not in full_answer:
                sources_line = "\n\nSources:\n" + "\n".join(f"- {s}" for s in used_sources)
                yield sources_line
                full_answer += sources_line

            if telegram_id:
                _add_to_chat_history(telegram_id, "assistant", full_answer.strip())

            _answer_cache[_normalize_question(question)] = {"ts": time.time(), "answer": full_answer}
            logger.info("Streamed answer via %s in %.1fs", provider, time.time() - started_at)
            return
        except Exception as provider_error:
            logger.warning("Stream answer failed on provider=%s: %s", provider, provider_error)
            continue

    yield "I couldn't generate an answer. Please try again."


async def answer_nado_question(question: str, telegram_id: int = None, user_name: str = None) -> str:
    started_at = time.time()
    xai_client = _get_xai_client()
    openai_client = _get_openai_client()
    if not xai_client and not openai_client:
        return (
            "AI service is not configured. Add XAI_API_KEY and/or OPENAI_API_KEY "
            "then restart the bot."
        )

    display_name = user_name or "trader"
    history_msgs = _build_history_messages(telegram_id) if telegram_id else []

    if telegram_id:
        _add_to_chat_history(telegram_id, "user", question)

    now = datetime.utcnow()
    current_date = now.strftime("%Y-%m-%d")

    if _is_casual_message(question):
        system = CASUAL_SYSTEM_PROMPT.format(current_date=current_date, user_name=display_name)
        primary = _pick_primary_provider(question)
        try:
            import asyncio
            loop = asyncio.get_event_loop()

            def _call_casual(p=primary):
                client = _get_xai_client() if p == "xai" else _get_openai_client()
                messages = [{"role": "system", "content": system}]
                if history_msgs:
                    messages.extend(history_msgs[-6:])
                messages.append({"role": "user", "content": question})
                resp = client.chat.completions.create(
                    model=_model_for(p), messages=messages, max_tokens=300, temperature=0.4,
                )
                return resp.choices[0].message.content.strip()

            answer = await loop.run_in_executor(None, _call_casual)
            if answer and telegram_id:
                _add_to_chat_history(telegram_id, "assistant", answer)
            return answer
        except Exception as e:
            logger.warning(f"Casual answer failed: {e}")
            fallback = f"GM {display_name}! How can I help you today?"
            if telegram_id:
                _add_to_chat_history(telegram_id, "assistant", fallback)
            return fallback

    if _is_x_twitter_question(question) and not xai_client:
        return "X/Twitter search requires the xAI (Grok) service. Please ask a different question or contact support."

    knowledge = _load_knowledge_base()
    if not knowledge:
        return "Knowledge base is not loaded. Please contact support."

    is_x_question = _is_x_twitter_question(question)
    use_x_prompt = is_x_question and xai_client is not None

    if use_x_prompt:
        system = X_TWITTER_SYSTEM_PROMPT.format(
            knowledge_base=_search_knowledge_sections(question, top_k=2),
            current_date=current_date,
            current_year=str(now.year),
        )
        used_sources = [OFFICIAL_SOURCES["x_nado"], OFFICIAL_SOURCES["x_ink"]]
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
            gathered_context = _search_knowledge_sections(question, top_k=5)
            used_sources = _pick_sources_for_question(question)

        system = SYNTHESIZER_SYSTEM_PROMPT.format(
            current_date=current_date,
            user_name=display_name,
            context=gathered_context[:12000],
        )

    used_sources = _filter_official_sources(used_sources)
    skip_sources = _is_price_question(question)

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
                    max_tokens = 800 if (_wants_detailed_answer(question) or use_x_prompt) else 600
                    messages = [{"role": "system", "content": system}]
                    if history_msgs:
                        messages.extend(history_msgs[-6:])
                    messages.append({"role": "user", "content": question})
                    kwargs = dict(
                        model=XAI_X_SEARCH_MODEL if (use_x_prompt and p == "xai") else _model_for(p),
                        messages=messages,
                        max_tokens=max_tokens,
                        temperature=0.3,
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
        if not skip_sources and "Sources:" not in answer:
            sources_line = "Sources:\n" + "\n".join(f"- {s}" for s in used_sources)
            answer = f"{answer}\n\n{sources_line}"

        if telegram_id:
            _add_to_chat_history(telegram_id, "assistant", answer)

        _answer_cache[_normalize_question(question)] = {"ts": time.time(), "answer": answer}
        return answer
    except Exception as e:
        logger.error(f"Knowledge Q&A failed: {e}", exc_info=True)
        return "Something went wrong while answering your question. Please try again."
