import os
import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

xai_client = None

def get_xai_client():
    global xai_client
    if xai_client is None:
        api_key = os.environ.get("XAI_API_KEY")
        if not api_key:
            return None
        xai_client = OpenAI(base_url="https://api.x.ai/v1", api_key=api_key)
    return xai_client


SYSTEM_PROMPT = """You are Nadobro, an AI trading assistant for Nado DEX (a perpetual futures and spot exchange on Ink blockchain).

Your job is to parse user messages and determine their trading intent. Respond ONLY with valid JSON.

Available products: BTC-PERP, ETH-PERP, SOL-PERP, ARB-PERP, OP-PERP, DOGE-PERP, LINK-PERP, AVAX-PERP
Available commands: long, short, limit_long, limit_short, close, close_all, positions, balance, price, funding, alerts, history, help, mode, wallet, cancel

Response format:
{
  "intent": "trade|query|command|chat",
  "action": "<command name or null>",
  "product": "<product symbol like BTC, ETH, etc or null>",
  "size": <number or null>,
  "price": <number or null>,
  "leverage": <number or null>,
  "tp_price": <number or null>,
  "sl_price": <number or null>,
  "alert_condition": "<above|below or null>",
  "alert_value": <number or null>,
  "message": "<friendly response to show the user>",
  "confidence": <0.0 to 1.0>
}

Rules:
- For "long BTC 0.01" -> intent=trade, action=long, product=BTC, size=0.01
- For "short ETH 0.05 at 10x" -> intent=trade, action=short, product=ETH, size=0.05, leverage=10
- For "limit buy BTC 0.01 at 95000" -> intent=trade, action=limit_long, product=BTC, size=0.01, price=95000
- For "what's the price of BTC" -> intent=query, action=price, product=BTC
- For "show my positions" -> intent=query, action=positions
- For "close my BTC position" -> intent=trade, action=close, product=BTC
- For "set alert when BTC goes above 100k" -> intent=command, action=alerts, product=BTC, alert_condition=above, alert_value=100000
- For "what's funding on ETH" -> intent=query, action=funding, product=ETH
- For casual chat/greetings -> intent=chat, action=null, message=friendly response
- If confidence < 0.5, suggest the user try specific commands
- Always include a user-friendly message explaining what you understood
- TP = take profit, SL = stop loss
"""


def parse_user_message(text: str) -> dict:
    client = get_xai_client()
    if not client:
        return _fallback_parse(text)

    try:
        response = client.chat.completions.create(
            model="grok-2-1212",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            response_format={"type": "json_object"},
            max_tokens=500,
            temperature=0.1,
        )
        result = json.loads(response.choices[0].message.content)
        if "intent" not in result:
            result["intent"] = "chat"
        if "confidence" not in result:
            result["confidence"] = 0.5
        if "message" not in result:
            result["message"] = ""
        return result
    except Exception as e:
        logger.error(f"AI parse failed: {e}")
        return _fallback_parse(text)


def _fallback_parse(text: str) -> dict:
    text_lower = text.lower().strip()

    products = ["btc", "eth", "sol", "arb", "op", "doge", "link", "avax"]
    detected_product = None
    for p in products:
        if p in text_lower:
            detected_product = p.upper()
            break

    numbers = []
    for word in text_lower.split():
        try:
            numbers.append(float(word))
        except ValueError:
            pass

    if any(w in text_lower for w in ["long", "buy"]):
        return {
            "intent": "trade", "action": "long",
            "product": detected_product, "size": numbers[0] if numbers else None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": f"Opening long on {detected_product or 'BTC'}" if detected_product else "Which product do you want to go long on?",
            "confidence": 0.7 if detected_product and numbers else 0.4,
        }

    if any(w in text_lower for w in ["short", "sell"]):
        return {
            "intent": "trade", "action": "short",
            "product": detected_product, "size": numbers[0] if numbers else None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": f"Opening short on {detected_product or 'BTC'}" if detected_product else "Which product do you want to short?",
            "confidence": 0.7 if detected_product and numbers else 0.4,
        }

    if any(w in text_lower for w in ["close", "exit"]):
        return {
            "intent": "trade", "action": "close" if detected_product else "close_all",
            "product": detected_product, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": f"Closing {detected_product or 'all'} positions",
            "confidence": 0.7,
        }

    if any(w in text_lower for w in ["position", "positions", "pnl"]):
        return {
            "intent": "query", "action": "positions",
            "product": detected_product, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": "Fetching your positions...",
            "confidence": 0.9,
        }

    if any(w in text_lower for w in ["balance", "margin", "account"]):
        return {
            "intent": "query", "action": "balance",
            "product": None, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": "Fetching your balance...",
            "confidence": 0.9,
        }

    if any(w in text_lower for w in ["price", "market", "what's", "whats", "how much"]):
        return {
            "intent": "query", "action": "price",
            "product": detected_product, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": f"Checking {detected_product or 'market'} price...",
            "confidence": 0.8,
        }

    if any(w in text_lower for w in ["funding", "rate"]):
        return {
            "intent": "query", "action": "funding",
            "product": detected_product, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": f"Fetching {detected_product or 'all'} funding rates...",
            "confidence": 0.8,
        }

    if any(w in text_lower for w in ["alert", "notify", "watch"]):
        return {
            "intent": "command", "action": "alerts",
            "product": detected_product, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": "above" if "above" in text_lower else ("below" if "below" in text_lower else None),
            "alert_value": numbers[0] if numbers else None,
            "message": "Setting up alert...",
            "confidence": 0.6,
        }

    if any(w in text_lower for w in ["history", "trades", "past"]):
        return {
            "intent": "query", "action": "history",
            "product": None, "size": None,
            "price": None, "leverage": None, "tp_price": None, "sl_price": None,
            "alert_condition": None, "alert_value": None,
            "message": "Fetching trade history...",
            "confidence": 0.8,
        }

    return {
        "intent": "chat", "action": None,
        "product": None, "size": None,
        "price": None, "leverage": None, "tp_price": None, "sl_price": None,
        "alert_condition": None, "alert_value": None,
        "message": "I'm not sure what you're looking for. Try commands like /positions, /balance, /long BTC 0.01, or /price BTC. For help, DM @nadobro on X.",
        "confidence": 0.2,
    }
