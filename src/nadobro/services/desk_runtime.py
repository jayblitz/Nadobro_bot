"""Desk runner — drives DeskController sessions from the scheduler.

Every tick (a few seconds): read which users have active desk plans (the DB
is the single source of truth), make sure each has a running engine session,
tick it, relay its events to Telegram, and tear down sessions whose users
have no active plans left.

REDEPLOY CONTRACT (user rule, 2026-07-05): trades and strategies are strictly
user-initiated — a redeploy must NEVER resume, re-fire, or re-arm anything on
its own. On the first tick after boot, every still-active desk plan is stood
down (guarded transition to ``cancelled``; fills kept) and its owner is told
exactly what was NOT resumed, so they can re-initiate deliberately. This
matches the engine strategies, which already skip auto-restore on startup
(``restore_running_bots(enabled=False)``). Operators can restore the legacy
resume-across-deploys behavior with ``NADO_DESK_RESUME_ON_RESTART=1``.

Runs in the scheduler process only (like alerts/HOWL), so multiprocess
strategy workers never race it. All DB and SDK calls go through
``run_blocking`` — nothing here may block the event loop.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from src.nadobro.services import desk_store
from src.nadobro.core.async_utils import run_blocking
from src.nadobro.utils.env import env_bool

logger = logging.getLogger(__name__)

_bot_app = None
_RUNNING: set[tuple[int, str]] = set()
_NETWORKS = ("mainnet", "testnet")

# Boot stand-down: flips to True once the first post-boot tick has parked (or
# been told to keep) every plan left active by the previous process.
_boot_standdown_done = False

_STANDDOWN_NOTE = (
    "stood down on redeploy — strategies are strictly user-initiated and are "
    "never auto-resumed"
)

# spot market-hours cache: product -> (expires_ts, is_open)
_SPOT_OPEN_TTL_SECONDS = 60.0
_spot_open_cache: Dict[tuple[str, str], tuple[float, bool]] = {}


def set_bot_app(app) -> None:
    global _bot_app
    _bot_app = app


def desk_enabled() -> bool:
    return env_bool("NADO_DESK_ENABLE", True)


def desk_resume_on_restart() -> bool:
    """Legacy escape hatch: resume active plans across a redeploy. Default OFF
    — the redeploy contract is that nothing trades without the user starting it."""
    return env_bool("NADO_DESK_RESUME_ON_RESTART")


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
        from src.nadobro.venue import product_catalog as pc

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
    existing = RUNTIME._controllers.get((key[0], network, "desk"))  # noqa: SLF001
    # A FAILED controller is not None but will never tick again — fall through
    # to RUNTIME.start (which tears it down first) instead of ticking a dead
    # session forever. Mirrors the grid-path needs_recovery guard.
    if key in _RUNNING and existing is not None and existing.is_active:
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
    # Redeploy stand-down (strictly user-initiated trading): worded per prior
    # status so the user knows exactly what was NOT resumed.
    "plan_parked_waiting": (
        "🛑 Desk: a redeploy stopped the waiting trigger for {summary}. "
        "Nothing was re-armed — strategies only start when you start them. "
        "Re-run it from the Desk if you still want it."
    ),
    "plan_parked_running": (
        "⚠️ Desk: a redeploy stopped {summary} — it was NOT resumed and its "
        "exit watch is OFF (strategies only run when you start them). "
        "Check Portfolio for any open position and close or re-arm it yourself."
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
# redeploy stand-down
# ---------------------------------------------------------------------------

async def _stand_down_on_boot() -> Optional[int]:
    """Park every plan the previous process left active and tell each owner.

    Returns the number of plans parked, or ``None`` when a DB error prevented
    a COMPLETE stand-down — the caller must then retry next tick and must NOT
    start ticking sessions, or an unparked plan would auto-resume after all.
    Idempotent: ``finish_plan`` is a guarded transition, and already-parked
    plans drop out of the active set."""
    from src.nadobro.services.desk_plans import ST_RUNNING

    parked = 0
    for network in _NETWORKS:
        try:
            users = await run_blocking(desk_store.list_users_with_active_plans, network)
        except Exception:  # noqa: BLE001 - DB blip: retry the whole pass next tick
            logger.warning("desk: boot stand-down scan failed for %s — sessions stay "
                           "parked until the stand-down completes", network, exc_info=True)
            return None
        for uid in users:
            try:
                records = await run_blocking(
                    desk_store.list_active_plans, int(uid), network
                ) or []
            except Exception:  # noqa: BLE001
                logger.warning("desk: boot stand-down plan read failed user=%s %s",
                               uid, network, exc_info=True)
                return None
            for rec in records:
                plan = rec.get("plan")
                plan_id = getattr(plan, "plan_id", None) or rec.get("plan_id")
                prior_status = str(rec.get("status") or "")
                if not plan_id:
                    continue
                try:
                    won = await run_blocking(
                        desk_store.finish_plan, str(plan_id), network, "cancelled",
                        error=_STANDDOWN_NOTE,
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("desk: boot stand-down failed for plan %s user=%s %s",
                                   plan_id, uid, network, exc_info=True)
                    return None
                if not won:
                    continue  # someone else already finished it
                parked += 1
                logger.info(
                    "desk: plan %s (user=%s network=%s status=%s) stood down on "
                    "redeploy — not resumed, exit watch not re-armed",
                    plan_id, uid, network, prior_status,
                )
                etype = ("plan_parked_running" if prior_status == ST_RUNNING
                         else "plan_parked_waiting")
                summary = ""
                try:
                    summary = plan.describe() if plan is not None else ""
                except Exception:  # noqa: BLE001 - description is cosmetic
                    summary = str(plan_id)
                await _notify_event(int(uid), {"type": etype, "summary": summary})
    return parked


# ---------------------------------------------------------------------------
# the scheduler job
# ---------------------------------------------------------------------------

async def tick_desk_runner() -> None:
    global _boot_standdown_done
    if not desk_enabled() or _bot_app is None:
        return
    # Redeploy contract: before ANY session is ensured or ticked, park what the
    # previous process left active. A plan must never trade, re-arm an exit
    # watch, or re-fire a trigger without the user starting it in THIS life of
    # the bot.
    if not _boot_standdown_done:
        if desk_resume_on_restart():
            logger.warning(
                "desk: NADO_DESK_RESUME_ON_RESTART=1 — resuming active plans "
                "across the redeploy (legacy behavior)"
            )
            _boot_standdown_done = True
        else:
            parked = await _stand_down_on_boot()
            if parked is None:
                return  # incomplete stand-down: retry next tick, tick nothing yet
            _boot_standdown_done = True
            if parked:
                # Start sessions on the NEXT tick, from a provably clean slate.
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
