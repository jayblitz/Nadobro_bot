"""Map Strategy Studio intents to existing Nadobro execution services."""

from __future__ import annotations

import logging

from src.nadobro.config import get_product_id
from src.nadobro.models.database import insert_conditional_order, insert_strategy_session
from src.nadobro.services.trade_service import execute_limit_order, execute_market_order
from src.nadobro.services.user_service import get_user_nado_client
from src.nadobro.studio.intent import TradingIntent

logger = logging.getLogger(__name__)


def _quantity_to_contracts(intent: TradingIntent, telegram_id: int, network: str) -> float:
    if not intent.quantity:
        raise ValueError("Quantity is required before execution")
    q = intent.quantity
    if q.type == "contracts":
        return float(q.value)
    client = get_user_nado_client(telegram_id, network=network)
    product_id = get_product_id(intent.symbol or "", network=network, client=client)
    price = 0.0
    if client and product_id is not None:
        price = float((client.get_market_price(int(product_id)) or {}).get("mid") or 0)
    if price <= 0:
        raise ValueError("Could not resolve market price for quantity conversion")
    if q.type == "usdt":
        return float(q.value) / price
    # In v1 percent_balance is interpreted as percent of available quote balance when available.
    balance = 0.0
    try:
        account = client.get_account_info() if client else {}
        balance = float(account.get("available_balance") or account.get("quote_balance") or 0)
    except Exception:
        balance = 0.0
    if balance <= 0:
        raise ValueError("Could not resolve account balance for percent sizing")
    return (balance * (float(q.value) / 100.0)) / price


def _ensure_strategy_session(telegram_id: int, network: str, intent: TradingIntent) -> int | None:
    return insert_strategy_session(
        {
            "user_id": telegram_id,
            "strategy": "studio",
            "product_name": intent.symbol,
            "network": network,
            "status": "running",
            "config_snapshot": intent.dict(),
        }
    )


def execute_intent(telegram_id: int, intent: TradingIntent, studio_session_id: int | None = None) -> dict:
    network = intent.network or "mainnet"
    session_id = _ensure_strategy_session(telegram_id, network, intent)
    try:
        is_long = intent.action == "buy"
        if intent.order_type == "conditional" or intent.conditions:
            cid = insert_conditional_order(
                {
                    "telegram_id": telegram_id,
                    "network": network,
                    "studio_session_id": studio_session_id,
                    "strategy_session_id": session_id,
                    "symbol": intent.symbol,
                    "action": intent.action,
                    "order_type": intent.order_type or "conditional",
                    "intent_json": intent.dict(),
                    "conditions_json": [c.dict() for c in intent.conditions],
                    "time_limit": intent.time_limit,
                    "time_limit_source": "studio" if intent.time_limit else None,
                }
            )
            return {"success": True, "conditional_order_id": cid, "strategy_session_id": session_id, "status": "armed"}
        size = _quantity_to_contracts(intent, telegram_id, network)
        if intent.order_type == "limit":
            result = execute_limit_order(
                telegram_id,
                intent.symbol or "",
                size,
                float(intent.entry_price or 0),
                is_long=is_long,
                leverage=float(intent.leverage or 1),
                enforce_rate_limit=True,
                tp_price=_absolute_level(intent.take_profit, intent.entry_price, intent.action, "take_profit"),
                sl_price=_absolute_level(intent.stop_loss, intent.entry_price, intent.action, "stop_loss"),
                source="studio",
                strategy_session_id=session_id,
            )
        else:
            result = execute_market_order(
                telegram_id,
                intent.symbol or "",
                size,
                is_long=is_long,
                leverage=float(intent.leverage or 1),
                slippage_pct=1.0,
                enforce_rate_limit=True,
                tp_price=_absolute_level(intent.take_profit, None, intent.action, "take_profit"),
                sl_price=_absolute_level(intent.stop_loss, None, intent.action, "stop_loss"),
                source="studio",
                strategy_session_id=session_id,
            )
        result["strategy_session_id"] = session_id
        return result
    except Exception as e:
        logger.error("Studio execution failed: %s", e, extra={"feature": "studio"})
        return {"success": False, "error": str(e), "strategy_session_id": session_id}


def _absolute_level(level, entry_price, action: str = "buy", kind: str = "take_profit"):
    if not level:
        return None
    if level.type == "absolute_price":
        return float(level.value)
    if entry_price:
        pct = abs(float(level.value)) / 100.0
        is_short = action == "sell"
        is_stop = kind == "stop_loss"
        direction = 1.0
        if is_short:
            direction = 1.0 if is_stop else -1.0
        else:
            direction = -1.0 if is_stop else 1.0
        return float(entry_price) * (1.0 + direction * pct)
    return None
