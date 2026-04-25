import logging
import re
from typing import TypedDict

from src.nadobro.config import get_perp_products
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.bot_runtime import start_user_bot, stop_all_user_bots
from src.nadobro.services.knowledge_service import answer_nado_question
from src.nadobro.services.managed_agent_prompt import (
    build_managed_agent_system_prompt,
    build_style_instruction,
)
from src.nadobro.services.user_service import ensure_active_wallet_ready, get_user

logger = logging.getLogger(__name__)

DEFAULT_PRODUCT = "BTC"


class ManagedAgentResult(TypedDict):
    handled: bool
    response: str
    show_menu: bool
    route: str


def _normalize_strategy_id(text: str) -> str | None:
    t = (text or "").lower()
    if any(x in t for x in ("alpha agent", "bro mode", "bro strategy", "alpha mode")):
        return "bro"
    if any(x in t for x in ("delta neutral", "delta-neutral", " dn ")):
        return "dn"
    if any(x in t for x in ("volume bot", "vol bot", " vol ")):
        return "vol"
    if any(x in t for x in ("r-grid", "rgrid", "reverse grid", "reverse-grid")):
        return "rgrid"
    if any(x in t for x in ("dynamic grid", "dgrid", "d-grid")):
        return "dgrid"
    if "grid" in t:
        return "grid"
    return None


def _extract_product(text: str, telegram_id: int) -> str:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    products = tuple(get_perp_products(network=network) or ("BTC", "ETH", "SOL"))
    upper_text = (text or "").upper()
    for product in products:
        if re.search(rf"\b{re.escape(product.upper())}\b", upper_text):
            return product.upper()
    return products[0].upper() if products else DEFAULT_PRODUCT


def _extract_leverage(text: str) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)\s*x\b", (text or "").lower())
    if not match:
        return 3.0
    try:
        return float(match.group(1))
    except ValueError:
        return 3.0


def _extract_direction(text: str) -> str:
    t = (text or "").lower()
    if " short" in f" {t}":
        return "short"
    return "long"


def _is_strategy_start_request(text: str) -> bool:
    t = (text or "").lower()
    trigger_words = ("start", "run", "launch", "activate", "enable")
    strategy_words = ("grid", "rgrid", "r-grid", "reverse grid", "dynamic grid", "dgrid", "d-grid", "delta neutral", "volume", "vol", "bro", "alpha")
    return any(w in t for w in trigger_words) and any(w in t for w in strategy_words)


def _is_strategy_stop_request(text: str) -> bool:
    t = (text or "").lower()
    return (
        "stop all" in t
        or "stop strategies" in t
        or "stop bots" in t
        or "disable strategies" in t
    )


def _is_casual_ping(text: str) -> bool:
    simple = (text or "").strip().lower().rstrip("!.?")
    return simple in {"gm", "gn", "hi", "hey", "hello", "yo", "thanks", "thank you", "bro"}


async def handle_managed_agent_turn(
    telegram_id: int,
    text: str,
    username: str | None = None,
) -> ManagedAgentResult:
    _system_prompt = build_managed_agent_system_prompt(username)
    style_instruction = build_style_instruction()
    prompt_text = (text or "").strip()
    if not prompt_text:
        return {
            "handled": True,
            "response": "Hey boss, say the word and we lock in.",
            "show_menu": False,
            "route": "empty",
        }

    if _is_casual_ping(prompt_text):
        return {
            "handled": True,
            "response": "Locked in legend. Printing soon - what move do you want next?",
            "show_menu": False,
            "route": "casual",
        }

    if _is_strategy_stop_request(prompt_text):
        ok, msg = stop_all_user_bots(telegram_id, cancel_orders=False)
        prefix = "Done boss." if ok else "Quick heads-up."
        return {
            "handled": True,
            "response": f"{prefix} {msg}",
            "show_menu": True,
            "route": "strategy_stop",
        }

    if _is_strategy_start_request(prompt_text):
        if is_trading_paused():
            return {
                "handled": True,
                "response": "Trading is paused right now by admin controls. I can keep scanning until it's back.",
                "show_menu": True,
                "route": "strategy_start_blocked_paused",
            }
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            return {
                "handled": True,
                "response": f"Need your wallet setup first, boss: {wallet_msg}",
                "show_menu": True,
                "route": "strategy_start_blocked_wallet",
            }
        strategy = _normalize_strategy_id(prompt_text)
        if not strategy:
            return {
                "handled": True,
                "response": (
                    "I couldn't map that strategy request yet. "
 "Try: start grid BTC 3x, run dgrid BTC, run r-grid ETH 4x, start delta neutral BTC, "
                    "start volume bot SOL, or activate alpha agent."
                ),
                "show_menu": False,
                "route": "strategy_start_unmapped",
            }

        product = "MULTI" if strategy == "bro" else _extract_product(prompt_text, telegram_id)
        leverage = _extract_leverage(prompt_text)
        direction = _extract_direction(prompt_text)

        ok, msg = start_user_bot(
            telegram_id,
            strategy=strategy,
            product=product,
            leverage=leverage,
            slippage_pct=1.0,
            direction=direction,
        )
        if ok:
            return {
                "handled": True,
                "response": f"Hey boss, strategy is live. {msg}",
                "show_menu": True,
                "route": "strategy_start_ok",
            }
        return {
            "handled": True,
            "response": f"Couldn't launch that yet: {msg}",
            "show_menu": True,
            "route": "strategy_start_failed",
        }

    try:
        framed_question = (
            f"{_system_prompt}\n\n"
            f"{style_instruction}\n\n"
            f"User message:\n{prompt_text}"
        )
        answer = await answer_nado_question(
            framed_question,
            telegram_id=telegram_id,
            user_name=username,
        )
        if answer and answer.strip():
            return {
                "handled": True,
                "response": answer.strip(),
                "show_menu": False,
                "route": "analysis",
            }
    except Exception as exc:
        logger.warning("Managed agent fallback failed for user %s: %s", telegram_id, exc)

    return {
        "handled": True,
        "response": "I hit a snag on that request, boss. Try again and I will reroute it clean.",
        "show_menu": False,
        "route": "error",
    }
