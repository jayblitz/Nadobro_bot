"""Nado NLP Vault — deposit (mint) and withdraw (burn) orchestration."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from src.nadobro.utils.env import env_int
from src.nadobro.models.database import (
    get_bot_state,
    get_vault_deposit_watch,
    get_vault_lp_events,
    insert_vault_lp_event,
    set_bot_state,
)
from src.nadobro.services.nado_archive import query_nlp_lp_events
from src.nadobro.services.user_service import get_user, get_user_nado_client
from src.nadobro.services.vault_metrics_service import (
    build_lp_ledger_from_archive,
    compute_pnl_from_ledger,
    deposit_room_usdt0,
    get_pool_metrics,
)

logger = logging.getLogger(__name__)

PRIVATE_ALPHA_CAP_USDT0 = 20_000.0
LOCKUP_SECONDS = 4 * 24 * 60 * 60
SEQUENCER_FEE_USDT0 = 1.0
WITHDRAW_FEE_BPS = 10
WITHDRAW_FEE_FLOOR_USDT0 = 1.0
USDT0_PRODUCT_ID = 0

# Re-sync archive mint/burn events at most every N seconds per user.
_LP_EVENTS_SYNC_TTL_SECONDS = max(60, env_int("NADO_VAULT_LP_EVENTS_SYNC_TTL", 300))


def lockup_remaining_seconds(last_mint_ts_ms: Optional[int]) -> int:
    if not last_mint_ts_ms:
        return 0
    try:
        elapsed = time.time() - (int(last_mint_ts_ms) / 1000.0)
    except (TypeError, ValueError):
        return 0
    remaining = LOCKUP_SECONDS - int(elapsed)
    return max(0, remaining)


def estimate_withdraw_fee_usdt0(usdt0_out: float) -> float:
    if usdt0_out <= 0:
        return SEQUENCER_FEE_USDT0 + WITHDRAW_FEE_FLOOR_USDT0
    bps_fee = max(WITHDRAW_FEE_FLOOR_USDT0, usdt0_out * (WITHDRAW_FEE_BPS / 10_000.0))
    return SEQUENCER_FEE_USDT0 + bps_fee


def _user_network(user) -> str:
    return str(getattr(getattr(user, "network_mode", None), "value", None) or "mainnet")


def _ledger_rows_for_pnl(telegram_id: int, network: str) -> list[dict]:
    """Archive-sourced rows only (rows with submission_idx) — bot-side rows
    are stored for audit but excluded so PnL never double-counts after the
    archive backfill catches the same tx."""
    rows = get_vault_lp_events(telegram_id, network=network)
    ledger = []
    for row in rows:
        if not row.get("submission_idx"):
            continue
        ledger.append({
            "event_type": row.get("event_type"),
            "quote_usdt0": float(row.get("quote_usdt0") or 0.0),
            "nlp_amount": float(row.get("nlp_amount") or 0.0),
            "timestamp": int(row["event_ts"].timestamp()) if row.get("event_ts") else 0,
            "submission_idx": row.get("submission_idx"),
        })
    return ledger


def _lp_events_sync_marker_key(telegram_id: int, network: str) -> str:
    return f"vault_lp_events_sync:{network}:{telegram_id}"


def _sync_lp_events_with_archive(
    telegram_id: int,
    network: str,
    subaccount_hex: str,
    nlp_product_id: int,
    *,
    force: bool = False,
) -> None:
    """Pull archive mint/burn events into vault_lp_events_{network}, deduped
    via the unique constraint. TTL-gated to avoid hammering the archive."""
    marker_key = _lp_events_sync_marker_key(telegram_id, network)
    if not force:
        marker = get_bot_state(marker_key) or {}
        last_ts = float(marker.get("ts") or 0)
        if (time.time() - last_ts) < _LP_EVENTS_SYNC_TTL_SECONDS:
            return

    try:
        payload = query_nlp_lp_events(network, subaccount_hex, limit=500)
    except Exception as e:
        logger.debug("vault lp events archive query failed user=%s err=%s", telegram_id, e)
        return

    ledger = build_lp_ledger_from_archive(payload, nlp_product_id)
    for row in ledger:
        ts = row.get("timestamp")
        event_ts = datetime.fromtimestamp(int(ts), tz=timezone.utc) if ts else None
        insert_vault_lp_event(
            telegram_id,
            event_type=str(row.get("event_type") or "mint"),
            quote_usdt0=float(row.get("quote_usdt0") or 0.0),
            nlp_amount=float(row.get("nlp_amount") or 0.0),
            submission_idx=row.get("submission_idx"),
            event_ts=event_ts,
            network=network,
        )
    try:
        set_bot_state(marker_key, {"ts": time.time(), "count": len(ledger)})
    except Exception as e:
        logger.debug("vault lp events sync marker write failed user=%s err=%s", telegram_id, e)


def get_user_vault_snapshot(telegram_id: int) -> dict:
    """Return a UI-ready snapshot with Nado-parity vault metrics."""
    snapshot: dict = {
        "ready": False,
        "usdt0_balance": 0.0,
        "lp_balance": 0.0,
        "lp_value_usdt0": 0.0,
        "position_usdt0": 0.0,
        "all_time_earned_usdt0": 0.0,
        "unrealized_pnl_usdt0": 0.0,
        "max_mintable_usdt0": 0.0,
        "deposit_room_usdt0": 0.0,
        "lockup_seconds_remaining": 0,
        "last_mint_ts_ms": None,
        "pool": {"tvl_usdt0": 0.0, "apr_pct": None, "apr_source": "unavailable"},
        "private_alpha_cap_usdt0": PRIVATE_ALPHA_CAP_USDT0,
        "deposit_watch_enabled": False,
        "deposit_capacity_open": False,
        "error": None,
    }
    user = get_user(telegram_id)
    if not user:
        snapshot["error"] = "Wallet not initialized. Use /start first."
        return snapshot
    network = _user_network(user)
    client = get_user_nado_client(telegram_id)
    if not client:
        snapshot["error"] = "Wallet not linked. Open 💼 Wallet Vault from the home menu to link a signer."
        return snapshot
    if not client._initialized:
        client.initialize()
    if not client._initialized:
        snapshot["error"] = "Nado SDK unavailable. Try again shortly."
        return snapshot

    try:
        balance = client.get_balance() or {}
        balances = balance.get("balances") or {}
        raw_usdt0 = balances.get(USDT0_PRODUCT_ID, balances.get(str(USDT0_PRODUCT_ID), 0)) or 0
        snapshot["usdt0_balance"] = float(raw_usdt0)
    except Exception as e:
        logger.debug("vault snapshot: usdt0 balance failed user=%s err=%s", telegram_id, e)

    pos = client.get_nlp_position() or {}
    nlp_product_id = int(pos.get("nlp_product_id") or client.resolve_nlp_product_id() or 11)
    snapshot["lp_balance"] = float(pos.get("lp_balance") or 0.0)
    snapshot["lp_value_usdt0"] = float(pos.get("lp_value_usdt0") or 0.0)
    snapshot["position_usdt0"] = snapshot["lp_value_usdt0"]
    snapshot["last_mint_ts_ms"] = pos.get("last_mint_ts_ms")
    snapshot["lockup_seconds_remaining"] = lockup_remaining_seconds(snapshot["last_mint_ts_ms"])

    mintable = client.get_max_nlp_mintable(spot_leverage=False, product_id=nlp_product_id) or {}
    max_mintable = float(mintable.get("max_mintable_usdt0") or 0.0)
    snapshot["max_mintable_usdt0"] = max_mintable
    snapshot["deposit_room_usdt0"] = deposit_room_usdt0(
        snapshot["lp_value_usdt0"], max_mintable, PRIVATE_ALPHA_CAP_USDT0,
    )
    snapshot["deposit_capacity_open"] = max_mintable >= 100.0
    # The bot mints with spot_leverage=false (a vault deposit must never
    # borrow against the trading account), so its gate is STRICTER than the
    # Nado UI default (spot_leverage=true). A trader whose USDT0 is backing
    # open positions gets no-borrow max ≈ 0 and used to see a blanket
    # "Deposits closed" even while the vault itself was open. Distinguish
    # the two so the card tells the truth.
    snapshot["deposit_blocked_reason"] = None
    if max_mintable <= 1.0:
        try:
            lev = client.get_max_nlp_mintable(
                spot_leverage=True, product_id=nlp_product_id
            ) or {}
            lev_mintable = float(lev.get("max_mintable_usdt0") or 0.0)
        except Exception:  # policy: degrade-ok(diagnostic only; gate stays strict)
            lev_mintable = 0.0
        snapshot["deposit_blocked_reason"] = (
            "margin_locked" if lev_mintable > 1.0 else "vault_full"
        )

    pool = get_pool_metrics(network, client=client)
    snapshot["pool"] = pool

    try:
        _sync_lp_events_with_archive(
            telegram_id, network, client.subaccount_hex, nlp_product_id,
        )
        ledger = _ledger_rows_for_pnl(telegram_id, network)
        pnl = compute_pnl_from_ledger(ledger, snapshot["lp_value_usdt0"])
        snapshot["all_time_earned_usdt0"] = float(pnl.get("all_time_earned_usdt0") or 0.0)
        snapshot["unrealized_pnl_usdt0"] = float(pnl.get("unrealized_pnl_usdt0") or 0.0)
    except Exception as e:
        logger.debug("vault pnl metrics failed user=%s err=%s", telegram_id, e)

    watch = get_vault_deposit_watch(telegram_id, network=network)
    snapshot["deposit_watch_enabled"] = bool(watch and watch.get("enabled"))
    snapshot["ready"] = True
    return snapshot


def _log_vault_event(
    telegram_id: int,
    network: str,
    *,
    event_type: str,
    quote_usdt0: float | None = None,
    nlp_amount: float | None = None,
    digest: str | None = None,
) -> None:
    """Audit log for bot-initiated mint/burns. Stored with NULL submission_idx
    so they are excluded from PnL accounting; archive sync supplies the
    canonical PnL rows once the indexer catches up."""
    insert_vault_lp_event(
        telegram_id,
        event_type=event_type,
        quote_usdt0=quote_usdt0,
        nlp_amount=nlp_amount,
        tx_digest=str(digest) if digest else None,
        event_ts=datetime.now(timezone.utc),
        network=network,
    )
    # Invalidate the archive sync marker so the next vault refresh refetches
    # quickly and (eventually) replaces this row with the indexed event.
    try:
        set_bot_state(_lp_events_sync_marker_key(telegram_id, network), {"ts": 0})
    except Exception:
        pass


def deposit_to_vault(telegram_id: int, usdt0_amount: float) -> dict:
    usdt0_amount = float(usdt0_amount or 0)
    if usdt0_amount <= 0:
        return {"success": False, "error": "Enter a positive USDT0 amount."}
    user = get_user(telegram_id)
    network = _user_network(user) if user else "mainnet"
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not linked. Open 💼 Wallet Vault from the home menu first."}
    if not client._initialized:
        client.initialize()
    if not client._initialized:
        return {"success": False, "error": "Nado SDK unavailable. Try again shortly."}

    snap = get_user_vault_snapshot(telegram_id)
    if snap.get("usdt0_balance", 0.0) + 1e-9 < usdt0_amount:
        return {
            "success": False,
            "error": (
                f"Insufficient USDT0 balance "
                f"(have ${snap.get('usdt0_balance', 0.0):,.2f}). "
                "Deposit USDT0 into your Nado account first."
            ),
        }
    if usdt0_amount > float(snap.get("deposit_room_usdt0") or 0.0) + 1e-9:
        room = float(snap.get("deposit_room_usdt0") or 0.0)
        return {
            "success": False,
            "error": (
                f"Deposit room is ${room:,.2f} USDT0 "
                f"(Private Alpha cap ${PRIVATE_ALPHA_CAP_USDT0:,.0f} and/or vault capacity)."
            ),
        }
    projected = float(snap.get("lp_value_usdt0", 0.0)) + usdt0_amount
    if projected > PRIVATE_ALPHA_CAP_USDT0 + 1e-9:
        room = max(0.0, PRIVATE_ALPHA_CAP_USDT0 - float(snap.get("lp_value_usdt0", 0.0)))
        return {
            "success": False,
            "error": (
                f"Private Alpha cap is ${PRIVATE_ALPHA_CAP_USDT0:,.0f} USDT0 per account. "
                f"You can add at most ${room:,.2f} more."
            ),
        }
    result = client.mint_nlp(usdt0_amount, spot_leverage=False)
    if result.get("success"):
        _log_vault_event(
            telegram_id, network,
            event_type="mint",
            quote_usdt0=usdt0_amount,
            digest=result.get("digest"),
        )
        logger.info(
            "NLP deposit user=%s amount=%.2f USDT0 digest=%s",
            telegram_id, usdt0_amount, result.get("digest"),
        )
    return result


def withdraw_from_vault(telegram_id: int, nlp_amount: float) -> dict:
    nlp_amount = float(nlp_amount or 0)
    if nlp_amount <= 0:
        return {"success": False, "error": "Enter a positive NLP amount."}
    user = get_user(telegram_id)
    network = _user_network(user) if user else "mainnet"
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not linked. Open 💼 Wallet Vault from the home menu first."}
    if not client._initialized:
        client.initialize()
    if not client._initialized:
        return {"success": False, "error": "Nado SDK unavailable. Try again shortly."}
    snap = get_user_vault_snapshot(telegram_id)
    if nlp_amount > float(snap.get("lp_balance", 0.0)) + 1e-9:
        return {
            "success": False,
            "error": (
                f"You only have {snap.get('lp_balance', 0.0):.6f} NLP. "
                "Reduce the amount or pick a percentage preset."
            ),
        }
    lockup = int(snap.get("lockup_seconds_remaining") or 0)
    if lockup > 0:
        hours = lockup / 3600.0
        return {
            "success": False,
            "error": (
                f"Lockup active — burns are blocked for ~{hours:.1f}h more "
                "(4-day post-mint lock from docs.nado.xyz/nlp)."
            ),
        }
    lp_value = float(snap.get("lp_value_usdt0") or 0.0)
    lp_balance = float(snap.get("lp_balance") or 0.0)
    est_usdt0 = lp_value * (nlp_amount / lp_balance) if lp_balance > 0 else 0.0
    result = client.burn_nlp(nlp_amount)
    if result.get("success"):
        _log_vault_event(
            telegram_id, network,
            event_type="burn",
            nlp_amount=nlp_amount,
            quote_usdt0=est_usdt0,
            digest=result.get("digest"),
        )
        logger.info(
            "NLP withdraw user=%s amount=%.6f NLP digest=%s",
            telegram_id, nlp_amount, result.get("digest"),
        )
    return result
