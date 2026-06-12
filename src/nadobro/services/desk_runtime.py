"""Desk runner — drives DeskController sessions from the scheduler.

Every tick (a few seconds): read which users have active desk plans (the DB
is the single source of truth — that's what makes a 24h TWAP survive a
deploy), make sure each has a running engine session, tick it, relay its
events to Telegram, and tear down sessions whose users have no active plans
left.

Runs in the scheduler process only (like alerts/HOWL), so multiprocess
strategy workers never race it. All DB and SDK calls go through
``run_blocking`` — nothing here may block the event loop.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from src.nadobro.services import desk_store
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

_bot_app = None
_RUNNING: set[tuple[int, str]] = set()
_NETWORKS = ("mainnet", "testnet")

# spot market-hours cache: product -> (expires_ts, is_open)
_SPOT_OPEN_TTL_SECONDS = 60.0
_spot_open_cache: Dict[tuple[str, str], tuple[float, bool]] = {}


def set_bot_app(app) -> None:
    global _bot_app
    _bot_app = app


def desk_enabled() -> bool:
    import os

    return (os.environ.get("NADO_DESK_ENABLE", "true").strip().lower()
            in ("1", "true", "yes", "on"))


# ---------------------------------------------------------------------------
# spot market hours (tokenized stocks)
# ---------------------------------------------------------------------------

def _spot_market_open_sync(product: str, network: str) -> bool:
    """True when the spot product is live AND inside market hours. Perp-less
    feed errors fail OPEN — a broken hours feed must not halt crypto spot."""
    key = (str(product or "").upper(), network)
    now = time.time()
    cached = _spot_open_cache.get(key)
    if cached and cached[0] > now:
        return cached[1]
    is_open = True
    try:
        from src.nadobro.services import product_catalog as pc

        spots = (pc.get_spot_catalog(network=network) or {}).get("spots") or {}
        row = spots.get(key[0]) if isinstance(spots, dict) else None
        if isinstance(row, dict):
            status = str(row.get("trading_status") or "live").lower()
            if status and status not in ("live", "active", "trading", "open"):
                is_open = False
            hours = row.get("market_hours")
            if is_open and isinstance(hours, dict) and hours:
                is_open = bool(hours.get("is_open", True))
    except Exception:  # noqa: BLE001 - fail open by design
        logger.warning("desk: spot hours lookup failed for %s", product, exc_info=True)
        is_open = True
    _spot_open_cache[key] = (now + _SPOT_OPEN_TTL_SECONDS, is_open)
    return is_open


# ---------------------------------------------------------------------------
# controller wiring
# ---------------------------------------------------------------------------

def _desk_configs(telegram_id: int, network: str) -> Dict[str, Any]:
    uid = int(telegram_id)

    async def plans_provider():
        return await run_blocking(desk_store.list_active_plans, uid, network)

    async def claim_trigger(plan_id: str) -> bool:
        return bool(await run_blocking(desk_store.claim_trigger_fire, plan_id, network))

    async def finish(plan_id: str, status: str, error: Optional[str]) -> None:
        if status == "failed":
            await run_blocking(desk_store.fail_plan, plan_id, network, error or "failed")
        else:
            await run_blocking(desk_store.finish_plan, plan_id, network, status)

    async def checkpoint(plan_id: str, state: dict) -> None:
        await run_blocking(desk_store.update_state, plan_id, network, state)

    async def spot_open(product: str) -> bool:
        return await run_blocking(_spot_market_open_sync, product, network)

    return {
        "trading_pair": "DESK",
        "plans_provider": plans_provider,
        "claim_trigger": claim_trigger,
        "finish": finish,
        "checkpoint": checkpoint,
        "spot_open": spot_open,
    }


async def _ensure_session(telegram_id: int, network: str) -> bool:
    from src.nadobro.services.engine_persistence import DbInventoryRepository
    from src.nadobro.services.engine_runtime import (
        RUNTIME,
        build_adapter,
        build_product_meta_from_catalog,
    )
    from src.nadobro.services.user_service import get_user_nado_client

    key = (int(telegram_id), network)
    if key in _RUNNING and RUNTIME._controllers.get((key[0], network, "desk")) is not None:  # noqa: SLF001
        return True
    client = await run_blocking(get_user_nado_client, int(telegram_id), network)
    if client is None:
        logger.warning("desk: no trading client for user=%s network=%s — plans stay queued",
                       telegram_id, network)
        return False
    meta = await run_blocking(build_product_meta_from_catalog, client)
    adapter = build_adapter(client, meta)
    await RUNTIME.start(
        int(telegram_id), network, "desk", _desk_configs(telegram_id, network),
        adapter, DbInventoryRepository(),
    )
    _RUNNING.add(key)
    logger.info("desk: session started user=%s network=%s", telegram_id, network)
    return True


async def _stop_session(telegram_id: int, network: str) -> None:
    from src.nadobro.services.engine_runtime import RUNTIME

    try:
        await RUNTIME.stop(int(telegram_id), network, "desk")
    except Exception:  # noqa: BLE001
        logger.warning("desk: session stop failed user=%s network=%s",
                       telegram_id, network, exc_info=True)
    _RUNNING.discard((int(telegram_id), network))


# ---------------------------------------------------------------------------
# event -> user notification
# ---------------------------------------------------------------------------

_EVENT_TEXT = {
    "trigger_fired": "🎯 Desk: trigger hit — starting {summary}",
    "entry_progress": "⏳ Desk: {product} entry {pct}% filled",
    "entry_filled": "✅ Desk: {product} entry filled — {filled_base} at ~{vwap}. Exit watch armed.",
    "entry_suspended": "⏸ Desk: {product} market is closed — TWAP paused, fills kept. Resumes at open.",
    "entry_resumed": "▶️ Desk: {product} market reopened — TWAP resuming the remainder.",
    "exit_triggered": "🎯 Desk: {reason} hit on {product} — closing now.",
    "exit_partial": "⚠️ Desk: partial close on {product} ({closed_base}) — retrying the remainder.",
    "plan_completed": "✅ Desk plan complete: {summary}\nFilled {filled_base} at avg {vwap} (fees {fees}).",
    "plan_failed": "❌ Desk plan failed: {summary}\nReason: {error}",
    "plan_cancelled": "🛑 Desk plan cancelled — resting orders pulled, fills kept.",
    "plan_recovered": "♻️ Desk: plan resumed after a restart ({detail}).",
    "plan_ambiguous": (
        "⚠️ Desk: a restart interrupted an order on {product} and the outcome is unknown. "
        "The plan was stopped for safety — check Portfolio before re-running it."
    ),
}


async def _notify_event(telegram_id: int, evt: Dict[str, Any]) -> None:
    if _bot_app is None:
        return
    template = _EVENT_TEXT.get(str(evt.get("type")))
    if not template:
        return
    try:
        from src.nadobro.i18n import (
            get_active_language,
            get_user_language,
            language_context,
            localize_text,
        )

        defaults = {k: "" for k in ("summary", "product", "pct", "filled_base",
                                    "vwap", "fees", "error", "reason",
                                    "closed_base", "detail")}
        defaults.update({k: v for k, v in evt.items() if v is not None})
        with language_context(get_user_language(telegram_id)):
            text = localize_text(template, get_active_language()).format(**defaults)
        await _bot_app.bot.send_message(chat_id=int(telegram_id), text=text)
    except Exception:  # noqa: BLE001 - notification failure must not stall the runner
        logger.warning("desk: notify failed user=%s type=%s",
                       telegram_id, evt.get("type"), exc_info=True)


# ---------------------------------------------------------------------------
# the scheduler job
# ---------------------------------------------------------------------------

async def tick_desk_runner() -> None:
    if not desk_enabled() or _bot_app is None:
        return
    from src.nadobro.services.engine_runtime import RUNTIME

    active: set[tuple[int, str]] = set()
    for network in _NETWORKS:
        try:
            users = await run_blocking(desk_store.list_users_with_active_plans, network)
        except Exception:  # noqa: BLE001 - DB blip: try again next tick
            logger.warning("desk: active-plan scan failed for %s", network, exc_info=True)
            continue
        for uid in users:
            active.add((int(uid), network))

    # Tear down sessions whose users have nothing active anymore.
    for key in list(_RUNNING):
        if key not in active:
            await _stop_session(*key)

    for uid, network in active:
        try:
            if not await _ensure_session(uid, network):
                continue
            await RUNTIME.tick(uid, network, "desk")
            controller = RUNTIME._controllers.get((uid, network, "desk"))  # noqa: SLF001
            events = controller.consume_desk_events() if controller is not None else []
            for evt in events:
                await _notify_event(uid, evt)
        except Exception:  # noqa: BLE001 - one user's session must not stall the rest
            logger.exception("desk: tick failed user=%s network=%s", uid, network)
