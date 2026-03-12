import os
import json
import logging
import time
from datetime import datetime
from typing import Optional

from openai import OpenAI

logger = logging.getLogger(__name__)

_xai_client: Optional[OpenAI] = None

BRO_DECISION_MODEL = os.environ.get("BRO_DECISION_MODEL", "grok-3")
BRO_SCAN_MODEL = os.environ.get("BRO_SCAN_MODEL", "grok-3-mini-fast")

_decision_cache: dict = {}
DECISION_CACHE_TTL = 240


def _get_client() -> Optional[OpenAI]:
    global _xai_client
    if _xai_client:
        return _xai_client
    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        return None
    _xai_client = OpenAI(api_key=api_key, base_url="https://api.x.ai/v1")
    return _xai_client


SYSTEM_PROMPT = """You are Bro, an autonomous quant trading agent for Nado DEX (perpetuals on Ink L2).

You analyze market data and make trading decisions. You are methodical, data-driven, and disciplined.

AVAILABLE ASSETS: {products}
RISK PROFILE: {risk_level}
BUDGET: ${budget:.0f} | CURRENT EXPOSURE: ${exposure:.0f} | REMAINING: ${remaining:.0f}
MAX LEVERAGE: {max_leverage}x | MAX POSITIONS: {max_positions}
OPEN POSITIONS: {positions_text}

DECISION RULES:
1. Only trade when you have HIGH CONFIDENCE (>={min_confidence:.0f}%) based on multiple confirming signals
2. Look for confluence: RSI + EMA alignment + MACD + momentum + sentiment should mostly agree
3. Never chase — if price moved significantly already, wait for pullback
4. Respect the risk profile: {risk_level} means {risk_description}
5. Always set TP and SL levels. TP should be realistic (1-3%), SL tight (0.5-1.5%)
6. Consider funding rates — positive funding = longs pay shorts, negative = shorts pay longs
7. If no good setup exists, respond with action "hold" — it's better to wait than force a bad trade
8. Consider existing positions — don't double up on correlated bets
9. For closing decisions: close if TP/SL hit, if thesis is invalidated, or if better opportunity exists

RESPOND WITH VALID JSON ONLY (no markdown, no code blocks):
{{
  "action": "open_long" | "open_short" | "close" | "hold" | "adjust",
  "product": "BTC" | "ETH" | "SOL" | etc,
  "confidence": 0.0 to 1.0,
  "leverage": 1 to {max_leverage},
  "size_pct": 0.1 to 1.0 (fraction of remaining budget),
  "tp_pct": take profit percentage from entry,
  "sl_pct": stop loss percentage from entry,
  "reasoning": "1-2 sentence explanation",
  "signals": ["list", "of", "key", "signals"],
  "close_product": "only if action is close — which product to close"
}}

For "hold" action, only provide: {{"action": "hold", "reasoning": "why", "confidence": 0.0}}
"""

RISK_DESCRIPTIONS = {
    "conservative": "small positions, low leverage, wait for strong setups only",
    "balanced": "moderate positions, medium leverage, trade good setups",
    "aggressive": "larger positions, higher leverage, trade more frequently on decent setups",
}


def _format_positions(positions: list[dict]) -> str:
    if not positions:
        return "None"
    parts = []
    for p in positions:
        product = p.get("product", "?")
        side = p.get("side", "?")
        notional = p.get("notional_usd", 0)
        pnl = p.get("unrealized_pnl", 0)
        entry = p.get("entry_price", 0)
        parts.append(f"{product} {side.upper()} ${notional:.0f} entry=${entry:,.2f} PnL=${pnl:+.2f}")
    return " | ".join(parts)


def make_decision(
    market_snapshot_text: str,
    products: list[str],
    risk_level: str,
    budget: float,
    exposure: float,
    remaining: float,
    max_leverage: int,
    max_positions: int,
    positions: list[dict],
    min_confidence: float,
) -> dict:
    client = _get_client()
    if not client:
        return {"action": "hold", "reasoning": "LLM client not available", "confidence": 0.0}

    system = SYSTEM_PROMPT.format(
        products=", ".join(products),
        risk_level=risk_level,
        budget=budget,
        exposure=exposure,
        remaining=remaining,
        max_leverage=max_leverage,
        max_positions=max_positions,
        positions_text=_format_positions(positions),
        min_confidence=min_confidence * 100,
        risk_description=RISK_DESCRIPTIONS.get(risk_level, RISK_DESCRIPTIONS["balanced"]),
    )

    try:
        response = client.chat.completions.create(
            model=BRO_DECISION_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": market_snapshot_text},
            ],
            max_tokens=500,
            temperature=0.3,
        )

        raw = response.choices[0].message.content or ""
        return _parse_decision(raw, min_confidence)
    except Exception as e:
        logger.error("Bro LLM decision failed: %s", e)
        return {"action": "hold", "reasoning": f"LLM error: {str(e)[:100]}", "confidence": 0.0}


def _parse_decision(raw: str, min_confidence: float) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        text = text.strip()
    if text.startswith("json"):
        text = text[4:].strip()

    try:
        decision = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                decision = json.loads(text[start:end])
            except json.JSONDecodeError:
                logger.warning("Failed to parse LLM decision: %s", text[:200])
                return {"action": "hold", "reasoning": "Failed to parse LLM output", "confidence": 0.0}
        else:
            return {"action": "hold", "reasoning": "No JSON in LLM output", "confidence": 0.0}

    action = decision.get("action", "hold")
    if action not in ("open_long", "open_short", "close", "hold", "adjust"):
        decision["action"] = "hold"
        decision["reasoning"] = f"Invalid action '{action}' — defaulting to hold"

    confidence = float(decision.get("confidence", 0))
    if confidence > 1.0:
        confidence = confidence / 100.0
    decision["confidence"] = max(0.0, min(1.0, confidence))

    if action in ("open_long", "open_short") and confidence < min_confidence:
        decision["action"] = "hold"
        decision["reasoning"] = (
            f"Confidence {confidence:.0%} below minimum {min_confidence:.0%}. "
            f"Original: {decision.get('reasoning', '')}"
        )

    if action in ("open_long", "open_short"):
        decision["leverage"] = max(1, min(int(decision.get("leverage", 3)), 40))
        decision["size_pct"] = max(0.1, min(1.0, float(decision.get("size_pct", 0.3))))
        decision["tp_pct"] = max(0.3, min(10.0, float(decision.get("tp_pct", 2.0))))
        decision["sl_pct"] = max(0.3, min(5.0, float(decision.get("sl_pct", 1.0))))

    return decision


def analyze_for_howl(
    trade_history: list[dict],
    current_settings: dict,
    performance_metrics: dict,
) -> Optional[dict]:
    client = _get_client()
    if not client:
        return None

    system = """You are HOWL, the nightly optimization engine for Bro Mode autonomous trading.

Analyze the past 24 hours of trading performance and suggest parameter adjustments.

Current settings:
{settings}

Performance metrics:
{metrics}

Recent trades:
{trades}

Suggest specific parameter changes with clear rationale. Focus on:
1. Risk level adjustment (conservative/balanced/aggressive)
2. Confidence threshold tuning
3. TP/SL optimization based on actual win rate and avg P&L
4. Product selection (which assets to focus on)
5. Leverage adjustments
6. Cycle timing

RESPOND WITH VALID JSON:
{{
  "suggestions": [
    {{
      "parameter": "parameter_name",
      "current_value": "current",
      "suggested_value": "new",
      "rationale": "why this change will improve performance",
      "expected_impact": "what improvement to expect"
    }}
  ],
  "overall_assessment": "1-2 sentence summary of performance",
  "confidence": 0.0 to 1.0
}}"""

    trades_text = ""
    for t in trade_history[-20:]:
        product = t.get("product_name", "?")
        side = t.get("side", "?")
        pnl = t.get("pnl", 0)
        trades_text += f"  {product} {side} PnL={pnl:+.2f}\n"

    settings_text = json.dumps(current_settings, indent=2)
    metrics_text = json.dumps(performance_metrics, indent=2)

    prompt = system.format(
        settings=settings_text,
        metrics=metrics_text,
        trades=trades_text or "  No trades in period",
    )

    try:
        response = client.chat.completions.create(
            model=BRO_DECISION_MODEL,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": "Run nightly HOWL analysis and suggest optimizations."},
            ],
            max_tokens=800,
            temperature=0.3,
        )

        raw = response.choices[0].message.content or ""
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
        return None
    except Exception as e:
        logger.error("HOWL analysis failed: %s", e)
        return None
