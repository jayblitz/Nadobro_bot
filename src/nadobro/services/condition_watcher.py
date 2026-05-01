"""Condition watcher for armed Strategy Studio orders."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from src.nadobro.config import get_product_id
from src.nadobro.models.database import (
    claim_conditional_order,
    get_armed_conditional_orders,
    update_conditional_order,
)
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.execution_queue import enqueue_strategy
from src.nadobro.services.feature_flags import studio_enabled
from src.nadobro.services.user_service import get_user_nado_client
from src.nadobro.studio.candles import fetch_candles
from src.nadobro.studio.conditions import describe_current, evaluate
from src.nadobro.studio.execution_bridge import execute_intent
from src.nadobro.studio.intent import Condition, intent_from_json

logger = logging.getLogger(__name__)


async def condition_tick() -> None:
    if not studio_enabled():
        return
    for network in ("testnet", "mainnet"):
        rows = await run_blocking(get_armed_conditional_orders, network)
        for row in rows:
            await enqueue_strategy(
                {"kind": "condition_order", "strategy": "studio", "network": network, "row": row},
                dedupe_key=f"condition:{network}:{row.get('id')}",
            )


async def handle_condition_job(payload: dict) -> None:
    row = payload.get("row") or {}
    network = payload.get("network") or row.get("network") or "mainnet"
    order_id = int(row.get("id"))
    if not studio_enabled():
        return
    intent_payload = _decode(row.get("intent_json"), {})
    conditions_payload = _decode(row.get("conditions_json"), [])
    conditions = [Condition.parse_obj(c) for c in conditions_payload]
    intent = intent_from_json(intent_payload, raw_fallback=intent_payload.get("raw_input", ""))
    user_id = int(row.get("telegram_id"))
    client = await run_blocking(get_user_nado_client, user_id, network)
    if not client:
        await run_blocking(update_conditional_order, order_id, {"last_evaluated_at": datetime.now(timezone.utc), "last_evaluation": "client unavailable"})
        return
    ctx = await _ctx(client, intent.symbol or row.get("symbol"), network)
    evaluations = []
    all_true = True
    for condition in conditions:
        candles = await run_blocking(fetch_candles, client, intent.symbol or row.get("symbol"), condition.timeframe, 200, network)
        ok = evaluate(condition, candles, ctx)
        all_true = all_true and ok
        evaluations.append(describe_current(condition, candles, ctx))
    await run_blocking(
        update_conditional_order,
        order_id,
        {"last_evaluated_at": datetime.now(timezone.utc), "last_evaluation": "; ".join(evaluations)[:500]},
    )
    if not all_true:
        return
    claimed = await run_blocking(claim_conditional_order, order_id)
    if not claimed:
        return
    fire_data = intent.dict()
    fire_data.update({"conditions": [], "order_type": "limit" if intent.entry_price is not None else "market"})
    fire_intent = intent_from_json(fire_data, raw_fallback=intent.raw_input)
    result = await run_blocking(execute_intent, user_id, fire_intent, row.get("studio_session_id"))
    await run_blocking(
        update_conditional_order,
        order_id,
        {
            "status": "fired" if result.get("success") else "error",
            "error_message": None if result.get("success") else str(result.get("error") or "execution failed")[:500],
        },
    )


async def _ctx(client, symbol: str, network: str) -> dict:
    product_id = get_product_id(symbol, network=network, client=client)
    ctx = {}
    if product_id is not None:
        try:
            funding = client.get_funding_rate(int(product_id)) or {}
            ctx["funding_rate"] = funding.get("funding_rate", 0)
        except Exception:
            ctx["funding_rate"] = 0
    return ctx


def _decode(value, default):
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default
