"""Opt-in scheduler watcher for NLP vault deposit capacity openings."""

from __future__ import annotations

import logging
import os

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.utils.env import env_float
from src.nadobro.models.database import (
    get_enabled_vault_deposit_watches,
    set_vault_deposit_watch,
    update_vault_watch_last_mintable,
)
from src.nadobro.core.async_utils import run_blocking, run_blocking_db, run_blocking_sdk
from src.nadobro.vault.nlp_vault_service import PRIVATE_ALPHA_CAP_USDT0, get_user_vault_snapshot
from src.nadobro.users.user_service import get_user, get_user_nado_client
from src.nadobro.vault.vault_metrics_service import get_pool_metrics

logger = logging.getLogger(__name__)

_bot_app = None

CLOSED_EPSILON_USDT0 = env_float("VAULT_DEPOSIT_CLOSED_EPSILON_USDT0", 1.0)
OPEN_MIN_USDT0 = env_float("VAULT_DEPOSIT_OPEN_MIN_USDT0", 100.0)


def set_vault_watch_bot_app(app) -> None:
    global _bot_app
    _bot_app = app


def is_deposit_capacity_open(mintable_usdt0: float) -> bool:
    return float(mintable_usdt0 or 0.0) >= OPEN_MIN_USDT0


def is_deposit_capacity_closed(mintable_usdt0: float) -> bool:
    return float(mintable_usdt0 or 0.0) <= CLOSED_EPSILON_USDT0


def should_notify_deposit_opening(last_seen: float, current: float) -> bool:
    return is_deposit_capacity_closed(last_seen) and is_deposit_capacity_open(current)


def user_eligible_for_deposit_watch(lp_value_usdt0: float) -> bool:
    return float(lp_value_usdt0 or 0.0) < PRIVATE_ALPHA_CAP_USDT0 - 1e-9


async def _notify(telegram_id: int, text: str) -> None:
    if not _bot_app:
        return
    try:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬇️ Deposit now", callback_data="vault:deposit")],
            [InlineKeyboardButton("💰 Vault", callback_data="vault:home")],
        ])
        await _bot_app.bot.send_message(
            chat_id=telegram_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=kb,
        )
    except Exception as e:
        logger.warning("vault deposit watch notify failed user=%s err=%s", telegram_id, e)


def enable_deposit_watch(telegram_id: int, network: str) -> tuple[bool, str]:
    snap = get_user_vault_snapshot(telegram_id)
    if snap.get("error"):
        return False, snap["error"]
    if not user_eligible_for_deposit_watch(float(snap.get("lp_value_usdt0") or 0.0)):
        return False, (
            f"You are at the Private Alpha cap (${PRIVATE_ALPHA_CAP_USDT0:,.0f}). "
            "Deposit watch is only for accounts below the cap."
        )
    # Seed last_seen at 0 (closed) so the very next tick detects a transition
    # and notifies the user immediately if capacity is currently open.
    set_vault_deposit_watch(
        telegram_id,
        enabled=True,
        network=network,
        last_seen_mintable_usdt0=0.0,
    )
    mintable = float(snap.get("max_mintable_usdt0") or 0.0)
    if is_deposit_capacity_open(mintable):
        return True, (
            f"Watching now. Capacity is currently open (${mintable:,.2f}); "
            "you'll get a ping shortly."
        )
    return True, "You'll be notified when vault deposit capacity opens."


def disable_deposit_watch(telegram_id: int, network: str) -> tuple[bool, str]:
    set_vault_deposit_watch(telegram_id, enabled=False, network=network)
    return True, "Deposit opening alerts turned off."


def _check_watch_sync(row: dict, network: str) -> tuple[int, float, float] | None:
    """Blocking per-user check (venue reads + eligibility DB write).

    Runs on the SDK worker pool — never on the event loop. Returns
    ``(telegram_id, mintable, lp_value)`` when the user is still watchable,
    ``None`` when the row should be skipped this tick.
    """
    telegram_id = int(row["user_id"])
    user = get_user(telegram_id)
    if not user:
        return None
    client = get_user_nado_client(telegram_id, network=network)
    if not client:
        return None
    if not client._initialized:
        client.initialize()
    if not client._initialized:
        return None

    pos = client.get_nlp_position() or {}
    lp_value = float(pos.get("lp_value_usdt0") or 0.0)
    if not user_eligible_for_deposit_watch(lp_value):
        set_vault_deposit_watch(telegram_id, enabled=False, network=network)
        return None

    # product_id is REQUIRED by the gateway — without it the query returns 0
    # and the watch would never fire (the same bug that showed "deposits
    # closed" in the UI).
    nlp_pid = pos.get("nlp_product_id") or client.resolve_nlp_product_id()
    mintable = float(
        (client.get_max_nlp_mintable(spot_leverage=False, product_id=nlp_pid) or {}).get(
            "max_mintable_usdt0"
        )
        or 0.0
    )
    return telegram_id, mintable, lp_value


async def tick_vault_deposit_watch(network: str = "mainnet") -> None:
    """Poll opted-in users; notify on closed → open mintable transitions.

    All venue/DB work is dispatched to worker pools; only the Telegram send
    happens on the event loop (sync SDK init + HTTP here used to run directly
    in this coroutine and starved the scheduler).
    """
    watches = await run_blocking_db(get_enabled_vault_deposit_watches, network=network)
    if not watches:
        return

    pool = await run_blocking(get_pool_metrics, network)
    tvl = float(pool.get("tvl_usdt0") or 0.0)
    apr = pool.get("apr_pct")
    apr_str = f"{apr:.2f}%" if apr is not None else "—"

    for row in watches:
        last_seen = float(row.get("last_seen_mintable_usdt0") or 0.0)
        checked = await run_blocking_sdk(_check_watch_sync, row, network)
        if checked is None:
            continue
        telegram_id, mintable, lp_value = checked
        if should_notify_deposit_opening(last_seen, mintable):
            msg = (
                "🔔 *Nado Vault deposit capacity opened*\n\n"
                f"You can deposit up to *${mintable:,.2f}* USDT0 now "
                f"(your vault position: ${lp_value:,.2f}).\n"
                f"APR: *{apr_str}* · TVL: *${tvl:,.0f}*"
            )
            await _notify(telegram_id, msg)
        await run_blocking_db(
            update_vault_watch_last_mintable, telegram_id, mintable, network=network
        )
