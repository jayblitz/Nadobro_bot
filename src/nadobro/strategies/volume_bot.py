"""
Volume Bot — alternating small market orders (long/short flips) until target
volume is reached.  Shows fee/PnL preview before start.  Auto-stops on
completion.  Uses linked signer via trade_service.execute_market_order.
"""
import logging
import time

from src.nadobro.config import EST_FEE_RATE, get_product_id
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.user_service import get_user_nado_client

logger = logging.getLogger(__name__)

DEFAULT_FLIP_SIZE_USD = 100.0
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


def run_cycle(telegram_id: int, network: str, state: dict) -> dict:
    """
    One cycle: place a single small market flip (alternating long/short)
    toward the cumulative target volume.

    Expected state keys (managed by bot_runtime):
      - product            : e.g. "BTC"
      - target_volume_usd  : total volume goal
      - volume_done_usd    : cumulative volume executed so far
      - fees_paid          : cumulative fees estimate
      - last_side          : "long" | "short" — last direction executed
      - leverage           : leverage multiplier
      - slippage_pct       : slippage tolerance %
      - flip_size_usd      : per-flip notional (default 100)

    Returns dict with cycle results.
    """
    product = state.get("product", "BTC")
    target_volume = float(state.get("target_volume_usd") or 0)
    volume_done = float(state.get("volume_done_usd") or 0)
    fees_paid = float(state.get("fees_paid") or 0)
    last_side = state.get("last_side", "short")
    leverage = float(state.get("leverage") or 3.0)
    slippage_pct = float(state.get("slippage_pct") or 1.0)
    flip_size_usd = float(state.get("flip_size_usd") or DEFAULT_FLIP_SIZE_USD)

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

    client = get_user_nado_client(telegram_id, passphrase=None)
    if not client:
        return {"success": False, "error": "Wallet client unavailable"}

    mp = client.get_market_price(product_id)
    mid = float(mp.get("mid") or 0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price"}

    size = max(this_flip_usd / mid, 0.0001)

    is_long = last_side != "long"

    result = execute_market_order(
        telegram_id,
        product,
        size,
        is_long=is_long,
        leverage=leverage,
        slippage_pct=slippage_pct,
        enforce_rate_limit=False,
    )

    if result.get("success"):
        exec_price = float(result.get("price") or mid)
        notional = size * exec_price
        fee = notional * EST_FEE_RATE

        volume_done += notional
        fees_paid += fee
        state["volume_done_usd"] = round(volume_done, 4)
        state["fees_paid"] = round(fees_paid, 6)
        state["last_side"] = "long" if is_long else "short"

        if target_volume > 0 and volume_done >= target_volume:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "volume_done_usd": round(volume_done, 4),
                "fees_paid": round(fees_paid, 6),
                "this_flip_notional": round(notional, 4),
                "side": "LONG" if is_long else "SHORT",
                "message": "Target volume reached — bot auto-stopped.",
            }

        return {
            "success": True,
            "done": False,
            "volume_done_usd": round(volume_done, 4),
            "target_volume_usd": target_volume,
            "fees_paid": round(fees_paid, 6),
            "this_flip_notional": round(notional, 4),
            "side": "LONG" if is_long else "SHORT",
            "remaining_usd": round(max(target_volume - volume_done, 0), 4) if target_volume > 0 else None,
        }

    return {
        "success": False,
        "error": result.get("error", "Market order failed"),
        "volume_done_usd": round(volume_done, 4),
        "fees_paid": round(fees_paid, 6),
    }
