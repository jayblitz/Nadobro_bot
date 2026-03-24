"""
Volume Bot cycle:
1) Place a LIMIT order at current mid (alternating long/short each round)
2) Wait until strategy interval elapses
3) Cancel leftover open orders and close the opened position immediately
"""
import logging
import time

from src.nadobro.config import EST_FEE_RATE, get_product_id
from src.nadobro.services.trade_service import execute_market_order, execute_limit_order
from src.nadobro.services.user_service import get_user_readonly_client, get_user_nado_client

logger = logging.getLogger(__name__)

DEFAULT_FLIP_SIZE_USD = 200.0
MIN_FLIP_SIZE_USD = 10.0


def get_fee_pnl_preview(telegram_id: int, product: str, target_volume_usd: float) -> dict:
    """Return estimated fees and PnL range for *target_volume_usd* of flips."""
    estimated_fees = target_volume_usd * EST_FEE_RATE
    num_flips = max(1, int(target_volume_usd / DEFAULT_FLIP_SIZE_USD))
    estimated_slippage = target_volume_usd * 0.0002

    return {
        "target_volume_usd": target_volume_usd,
        "flip_size_usd": DEFAULT_FLIP_SIZE_USD,
        "num_flips": num_flips,
        "fee_rate": EST_FEE_RATE,
        "estimated_fees": round(estimated_fees, 4),
        "estimated_slippage": round(estimated_slippage, 4),
        "estimated_total_cost": round(estimated_fees + estimated_slippage, 4),
        "product": product,
    }


def run_cycle(telegram_id: int, network: str, state: dict, **kwargs) -> dict:
    """
    One round:
      - open with LIMIT at mid
      - on next eligible cycle (after interval), close immediately

    Expected state keys (managed by bot_runtime):
      - product            : e.g. "BTC"
      - target_volume_usd  : total volume goal
      - volume_done_usd    : cumulative volume executed so far
      - fees_paid          : cumulative fees estimate
      - last_side          : "long" | "short" — last direction executed
      - leverage           : leverage multiplier
      - slippage_pct       : slippage tolerance %
      - flip_size_usd      : per-flip notional (default 200)

    Returns dict with cycle results.
    """
    # Verify network matches to prevent cross-network trading
    client = kwargs.get("client")
    if client and hasattr(client, 'network') and client.network != network:
        return {"success": False, "error": f"Network mismatch: expected {network}, got {client.network}"}

    product = state.get("product", "BTC")
    target_volume = float(state.get("target_volume_usd") or 0)
    volume_done = float(state.get("volume_done_usd") or 0)
    fees_paid = float(state.get("fees_paid") or 0)
    last_side = state.get("last_side", "short")
    leverage = float(state.get("leverage") or 1.0)
    slippage_pct = float(state.get("slippage_pct") or 1.0)
    interval_seconds = int(state.get("interval_seconds") or 30)
    flip_size_usd = float(
        state.get("flip_size_usd")
        or state.get("notional_usd")
        or DEFAULT_FLIP_SIZE_USD
    )

    if flip_size_usd < MIN_FLIP_SIZE_USD:
        flip_size_usd = MIN_FLIP_SIZE_USD

    if target_volume > 0 and volume_done >= target_volume:
        state["running"] = False
        return {
            "success": True,
            "done": True,
            "volume_done_usd": volume_done,
            "fees_paid": fees_paid,
            "message": "Target volume reached — bot auto-stopped.",
        }

    remaining = target_volume - volume_done if target_volume > 0 else flip_size_usd
    this_flip_usd = min(flip_size_usd, remaining) if target_volume > 0 else flip_size_usd

    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'"}

    client = get_user_readonly_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet client unavailable"}

    mp = client.get_market_price(product_id)
    mid = float(mp.get("mid") or 0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price"}

    phase = state.get("vol_phase", "idle")
    now_ts = time.time()

    if phase == "open_wait":
        opened_at = float(state.get("vol_opened_at") or 0.0)
        if opened_at > 0 and (now_ts - opened_at) < interval_seconds:
            return {"success": True, "done": False, "action": "waiting"}

        # Interval exhausted -> close immediately and clean any stale open orders.
        signer_client = get_user_nado_client(telegram_id)
        if signer_client:
            try:
                for order in (client.get_open_orders(product_id) or []):
                    digest = order.get("digest")
                    if digest:
                        signer_client.cancel_order(product_id, digest)
            except Exception as e:
                logger.warning("Volume bot order cancel sweep failed for user %s: %s", telegram_id, e)

        pos = None
        for p in (client.get_all_positions() or []):
            if int(p.get("product_id", -1)) == product_id:
                pos = p
                break

        if pos:
            pos_size = abs(float(pos.get("amount", 0) or 0))
            if pos_size > 0:
                side = str(pos.get("side", "") or "").upper()
                close_is_long = side == "SHORT"
                close_result = execute_market_order(
                    telegram_id,
                    product,
                    pos_size,
                    is_long=close_is_long,
                    leverage=leverage,
                    slippage_pct=slippage_pct,
                    enforce_rate_limit=False,
                    source="vol",
                    strategy_session_id=state.get("strategy_session_id"),
                )
                if not close_result.get("success"):
                    return {
                        "success": False,
                        "error": close_result.get("error", "Close after interval failed"),
                        "volume_done_usd": round(volume_done, 4),
                        "fees_paid": round(fees_paid, 6),
                    }
                close_notional = pos_size * mid
                close_fee = close_notional * EST_FEE_RATE
                volume_done += close_notional
                fees_paid += close_fee
                state["volume_done_usd"] = round(volume_done, 4)
                state["fees_paid"] = round(fees_paid, 6)

        if not pos and signer_client:
            try:
                for order in (client.get_open_orders(product_id) or []):
                    digest = order.get("digest")
                    if digest:
                        signer_client.cancel_order(product_id, digest)
            except Exception as e:
                logger.warning("Volume bot stale order cleanup failed for user %s: %s", telegram_id, e)

        state["vol_phase"] = "idle"
        state["vol_opened_at"] = 0.0
        state["vol_position_size"] = 0.0
        state["vol_position_side"] = None

        if target_volume > 0 and volume_done >= target_volume:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "volume_done_usd": round(volume_done, 4),
                "fees_paid": round(fees_paid, 6),
                "message": "Target volume reached — bot auto-stopped.",
            }

        return {
            "success": True,
            "done": False,
            "action": "closed_after_interval",
            "volume_done_usd": round(volume_done, 4),
            "fees_paid": round(fees_paid, 6),
        }

    size = max(this_flip_usd / mid, 0.0001)
    is_long = last_side != "long"
    open_result = execute_limit_order(
        telegram_id,
        product,
        size,
        mid,
        is_long=is_long,
        leverage=leverage,
        enforce_rate_limit=False,
        source="vol",
        strategy_session_id=state.get("strategy_session_id"),
    )

    if open_result.get("success"):
        notional = size * mid
        fee = notional * EST_FEE_RATE

        volume_done += notional
        fees_paid += fee
        state["volume_done_usd"] = round(volume_done, 4)
        state["fees_paid"] = round(fees_paid, 6)
        state["last_side"] = "long" if is_long else "short"
        state["vol_phase"] = "open_wait"
        state["vol_opened_at"] = now_ts
        state["vol_position_size"] = size
        state["vol_position_side"] = "LONG" if is_long else "SHORT"

        return {
            "success": True,
            "done": False,
            "volume_done_usd": round(volume_done, 4),
            "target_volume_usd": target_volume,
            "fees_paid": round(fees_paid, 6),
            "this_open_notional": round(notional, 4),
            "side": "LONG" if is_long else "SHORT",
            "entry_price": round(mid, 8),
            "action": "opened_limit_mid",
            "remaining_usd": round(max(target_volume - volume_done, 0), 4) if target_volume > 0 else None,
        }

    return {
        "success": False,
        "error": open_result.get("error", "Limit order failed"),
        "volume_done_usd": round(volume_done, 4),
        "fees_paid": round(fees_paid, 6),
    }
