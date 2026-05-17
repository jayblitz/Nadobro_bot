"""Nado NLP Vault — deposit (mint) and withdraw (burn) orchestration.

This service is a thin policy layer over `NadoClient.mint_nlp` / `burn_nlp`.
It enforces:

* USDT0 spot balance sanity checks before signing a mint.
* The 4-day post-mint lockup before allowing a burn (docs.nado.xyz/nlp).
* The Private Alpha cap of 20,000 USDT0 per trading account.
* Friendly error mapping for the most common gateway rejections.

The NLP gateway accepts USDT0 amounts as integer multiples of 1e18 (the
internal x18 representation), and NLP token amounts likewise as x18 integers.
All conversion happens inside `NadoClient`; this service deals in human
USDT0 / NLP floats.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from src.nadobro.services.user_service import get_user, get_user_nado_client

logger = logging.getLogger(__name__)

# Private Alpha cap per docs.nado.xyz/nlp.
PRIVATE_ALPHA_CAP_USDT0 = 20_000.0

# 4-day post-mint lockup before a burn is allowed.
LOCKUP_SECONDS = 4 * 24 * 60 * 60

# Withdrawal fee components (docs.nado.xyz/nlp).
SEQUENCER_FEE_USDT0 = 1.0
WITHDRAW_FEE_BPS = 10  # 10 bps of the withdrawn amount; floored at 1 USDT0.
WITHDRAW_FEE_FLOOR_USDT0 = 1.0

# Product id for USDT0 (the quote token) is 0 per Nado dev docs.
USDT0_PRODUCT_ID = 0


def lockup_remaining_seconds(last_mint_ts_ms: Optional[int]) -> int:
    """Return seconds left until the user can burn, or 0 if unlocked."""
    if not last_mint_ts_ms:
        return 0
    try:
        elapsed = time.time() - (int(last_mint_ts_ms) / 1000.0)
    except (TypeError, ValueError):
        return 0
    remaining = LOCKUP_SECONDS - int(elapsed)
    return max(0, remaining)


def estimate_withdraw_fee_usdt0(usdt0_out: float) -> float:
    """Quote the total fee the gateway will deduct on burn."""
    if usdt0_out <= 0:
        return SEQUENCER_FEE_USDT0 + WITHDRAW_FEE_FLOOR_USDT0
    bps_fee = max(WITHDRAW_FEE_FLOOR_USDT0, usdt0_out * (WITHDRAW_FEE_BPS / 10_000.0))
    return SEQUENCER_FEE_USDT0 + bps_fee


def get_user_vault_snapshot(telegram_id: int) -> dict:
    """Return a UI-ready snapshot: USDT0 balance, NLP position, pool info."""
    snapshot: dict = {
        "ready": False,
        "usdt0_balance": 0.0,
        "lp_balance": 0.0,
        "lp_value_usdt0": 0.0,
        "lockup_seconds_remaining": 0,
        "last_mint_ts_ms": None,
        "pool": {},
        "private_alpha_cap_usdt0": PRIVATE_ALPHA_CAP_USDT0,
        "error": None,
    }
    user = get_user(telegram_id)
    if not user:
        snapshot["error"] = "Wallet not initialized. Use /start first."
        return snapshot
    client = get_user_nado_client(telegram_id)
    if not client:
        snapshot["error"] = "Wallet not linked. Use /wallet to link a signer."
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
    snapshot["lp_balance"] = float(pos.get("lp_balance") or 0.0)
    snapshot["lp_value_usdt0"] = float(pos.get("lp_value_usdt0") or 0.0)
    snapshot["last_mint_ts_ms"] = pos.get("last_mint_ts_ms")
    snapshot["lockup_seconds_remaining"] = lockup_remaining_seconds(snapshot["last_mint_ts_ms"])

    snapshot["pool"] = client.get_nlp_pool_info().get("raw") or {}
    snapshot["ready"] = True
    return snapshot


def deposit_to_vault(telegram_id: int, usdt0_amount: float) -> dict:
    """Validate then call `NadoClient.mint_nlp`."""
    usdt0_amount = float(usdt0_amount or 0)
    if usdt0_amount <= 0:
        return {"success": False, "error": "Enter a positive USDT0 amount."}
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not linked. Use /wallet first."}
    if not client._initialized:
        client.initialize()
    if not client._initialized:
        return {"success": False, "error": "Nado SDK unavailable. Try again shortly."}
    # Sanity: USDT0 balance must cover the deposit.
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
    # Private Alpha cap: existing LP value + new mint must stay under 20k.
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
        logger.info(
            "NLP deposit user=%s amount=%.2f USDT0 digest=%s",
            telegram_id, usdt0_amount, result.get("digest"),
        )
    return result


def withdraw_from_vault(telegram_id: int, nlp_amount: float) -> dict:
    """Validate then call `NadoClient.burn_nlp`."""
    nlp_amount = float(nlp_amount or 0)
    if nlp_amount <= 0:
        return {"success": False, "error": "Enter a positive NLP amount."}
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not linked. Use /wallet first."}
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
    result = client.burn_nlp(nlp_amount)
    if result.get("success"):
        logger.info(
            "NLP withdraw user=%s amount=%.6f NLP digest=%s",
            telegram_id, nlp_amount, result.get("digest"),
        )
    return result
