"""Time-limit watcher for auto-closing positions and cancelling pending orders."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from src.nadobro.models.database import fetch_due_time_limits, update_conditional_order
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.execution_queue import enqueue_strategy
from src.nadobro.services.feature_flags import time_limit_enabled
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.user_service import get_user_nado_client

logger = logging.getLogger(__name__)
_bot_app = None


def set_bot_app(app) -> None:
    global _bot_app
    _bot_app = app


async def time_limit_tick(client_factory=None) -> None:
    if not time_limit_enabled():
        return
    now = datetime.now(timezone.utc)
    for network in ("testnet", "mainnet"):
        due = await run_blocking(fetch_due_time_limits, now, network)
        for row in due.get("positions", []):
            await enqueue_strategy(
                {"kind": "time_limit_position", "network": network, "row": row, "strategy": "time_limit"},
                dedupe_key=f"time-limit-position:{network}:{row.get('id')}",
            )
        for row in due.get("orders", []):
            await enqueue_strategy(
                {"kind": "time_limit_order", "network": network, "row": row, "strategy": "time_limit"},
                dedupe_key=f"time-limit-order:{network}:{row.get('id')}",
            )


async def handle_time_limit_job(payload: dict) -> None:
    kind = payload.get("kind")
    row = payload.get("row") or {}
    network = payload.get("network") or row.get("network") or "mainnet"
    if kind == "time_limit_position":
        await _close_position(row, network)
    elif kind == "time_limit_order":
        await _cancel_order(row, network)


async def _close_position(row: dict, network: str) -> None:
    user_id = int(row.get("user_id"))
    symbol = str(row.get("pair") or row.get("product_name") or "").replace("-PERP", "")
    size = abs(float(row.get("size") or 0))
    side = str(row.get("side") or "long").lower()
    if not symbol or size <= 0:
        return
    result = await run_blocking(
        execute_market_order,
        user_id,
        symbol,
        size,
        side == "short",  # closing a short requires buy/long direction
        float(row.get("leverage") or 1),
        1.0,
        True,
        None,
        None,
        "time_limit",
        None,
        True,
    )
    await _notify(user_id, f"Time-limit auto-close fired for {symbol}-PERP ({network}).\nReceipt: {result}")


async def _cancel_order(row: dict, network: str) -> None:
    user_id = int(row.get("user_id"))
    digest = row.get("order_digest")
    symbol = str(row.get("pair") or "").replace("-PERP", "")
    if not digest:
        return
    client = await run_blocking(get_user_nado_client, user_id, network)
    if not client:
        return
    from src.nadobro.config import get_product_id

    product_id = get_product_id(symbol, network=network, client=client)
    if product_id is None:
        return
    result = await run_blocking(client.cancel_order, int(product_id), str(digest))
    await _notify(user_id, f"Time-limit cancelled pending {symbol}-PERP order ({network}).\nReceipt: {result}")


async def _notify(telegram_id: int, text: str) -> None:
    if not _bot_app:
        return
    try:
        await _bot_app.bot.send_message(chat_id=telegram_id, text=text)
    except Exception as e:
        logger.warning("time-limit notification failed user=%s: %s", telegram_id, e, extra={"feature": "time_limit"})
