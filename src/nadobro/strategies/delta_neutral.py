"""
Delta Neutral — funding rate farming strategy.

Holistic DN flow on Nado:
1) Buy spot (BTC/ETH) on Nado Spot.
2) Short the same asset on perp with 1x-5x leverage.
3) Keep both legs size-matched and collect favorable funding.
4) Exit both legs after persistent unfavorable funding.
"""
import logging

logger = logging.getLogger(__name__)

MIN_FAVORABLE_FUNDING = 0.000001
UNFAVORABLE_EXIT_CYCLES = 5
POSITION_SIZE_TOLERANCE = 0.10


def run_cycle(telegram_id: int, network: str, state: dict, **kwargs) -> dict:
    """
    One cycle of the delta-neutral funding farm strategy.

    Expects kwargs:
        client: NadoClient instance
        mid: float current mid price
        product_id: int
        product: str product name (e.g. "BTC")
        open_orders: list of current open orders

    State fields used/set:
        notional_usd: target notional in USD
        leverage: leverage multiplier
        dn_unfavorable_count: consecutive unfavorable funding cycles
        dn_total_funding_earned: cumulative estimated funding earned
        dn_position_side: current position side ("SHORT" or None)
        dn_entry_price: price at which position was entered
        dn_last_funding_rate: last observed funding rate
    """
    client = kwargs.get("client")
    mid = kwargs.get("mid", 0.0)
    product_id = kwargs.get("product_id")
    product = kwargs.get("product", state.get("product", "BTC"))

    # DN strategy only supports assets with spot pairs (BTC, ETH)
    if product not in ("BTC", "ETH"):
        return {"success": False, "error": f"{product} is not supported for Delta Neutral. Only BTC and ETH have spot pairs on Nado."}

    if not client or mid <= 0 or product_id is None:
        return {"success": False, "error": "Missing client, price, or product_id"}

    from src.nadobro.config import get_spot_product_id
    from src.nadobro.services.trade_service import execute_market_order, execute_spot_market_order

    notional = float(state.get("notional_usd") or 100.0)
    leverage = float(state.get("leverage") or 3.0)
    leverage = max(1.0, min(leverage, 5.0))
    state["leverage"] = leverage
    if notional <= 0:
        return {"success": False, "error": "Invalid notional_usd; must be > 0"}
    target_size = notional / mid
    spot_product_id = get_spot_product_id(product)
    if spot_product_id is None:
        return {"success": False, "error": f"{product} spot is not supported for Delta Neutral."}

    fr_data = client.get_funding_rate(product_id) or {}
    funding_rate = float(fr_data.get("funding_rate", 0) or 0)
    state["dn_last_funding_rate"] = funding_rate

    unfavorable_count = int(state.get("dn_unfavorable_count") or 0)
    total_funding = float(state.get("dn_total_funding_earned") or 0.0)
    position_side = state.get("dn_position_side")
    entry_price = float(state.get("dn_entry_price") or 0.0)

    positions = client.get_all_positions() or []
    current_position = None
    for p in positions:
        if int(p.get("product_id", -1)) == product_id:
            current_position = p
            break

    current_size = abs(float(current_position.get("amount", 0) or 0)) if current_position else 0.0
    current_side = current_position.get("side") if current_position else None
    balance = client.get_balance() or {}
    balances = balance.get("balances", {}) or {}
    spot_size = float(balances.get(spot_product_id, balances.get(str(spot_product_id), 0)) or 0.0)
    state["dn_spot_size"] = spot_size

    result = {
        "success": True,
        "action": "hold",
        "funding_rate": funding_rate,
        "position_size": current_size,
        "position_side": current_side,
        "spot_size": spot_size,
        "total_funding_earned": total_funding,
        "unfavorable_cycles": unfavorable_count,
    }

    funding_favorable = funding_rate > MIN_FAVORABLE_FUNDING

    if not funding_favorable:
        unfavorable_count += 1
        state["dn_unfavorable_count"] = unfavorable_count

        if (current_position or spot_size > 0) and unfavorable_count >= UNFAVORABLE_EXIT_CYCLES:
            logger.info(
                "DN user %s: funding unfavorable for %d cycles, unwinding DN legs",
                telegram_id, unfavorable_count,
            )
            close_result = {"success": True}
            if current_position and current_size > 0:
                close_side = current_side != "LONG"
                close_result = execute_market_order(
                    telegram_id,
                    product,
                    current_size,
                    is_long=close_side,
                    leverage=leverage,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                    enforce_rate_limit=False,
                )
            spot_close_result = {"success": True}
            if spot_size > 0:
                spot_close_result = execute_spot_market_order(
                    telegram_id,
                    product,
                    spot_size,
                    is_buy=False,
                    enforce_rate_limit=False,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                )
            result["action"] = "exit"
            result["exit_reason"] = f"Funding unfavorable for {unfavorable_count} cycles"
            result["close_result"] = close_result.get("success", False)
            result["spot_close_result"] = spot_close_result.get("success", False)
            if close_result.get("success") and spot_close_result.get("success"):
                state["dn_position_side"] = None
                state["dn_entry_price"] = 0.0
                state["dn_unfavorable_count"] = 0
                state["dn_spot_size"] = 0.0
            else:
                result["success"] = False
                result["order_error"] = (
                    close_result.get("error")
                    or spot_close_result.get("error")
                    or "Failed to unwind delta-neutral legs"
                )
            return result

        result["action"] = "wait_unfavorable"
        return result

    state["dn_unfavorable_count"] = 0

    # Keep spot leg aligned to target size.
    spot_size_diff = abs(spot_size - target_size)
    if target_size > 0 and (spot_size_diff / target_size > POSITION_SIZE_TOLERANCE):
        if spot_size < target_size:
            buy_size = target_size - spot_size
            logger.info("DN user %s: increasing spot by %.6f %s", telegram_id, buy_size, product)
            spot_adjust = execute_spot_market_order(
                telegram_id,
                product,
                buy_size,
                is_buy=True,
                enforce_rate_limit=False,
                slippage_pct=float(state.get("slippage_pct") or 1.0),
            )
            result["action"] = "spot_buy"
            result["spot_adjust_result"] = spot_adjust.get("success", False)
            if not spot_adjust.get("success"):
                result["success"] = False
                result["order_error"] = spot_adjust.get("error", "Spot buy adjust failed")
                return result
        else:
            sell_size = spot_size - target_size
            logger.info("DN user %s: reducing spot by %.6f %s", telegram_id, sell_size, product)
            spot_adjust = execute_spot_market_order(
                telegram_id,
                product,
                sell_size,
                is_buy=False,
                enforce_rate_limit=False,
                slippage_pct=float(state.get("slippage_pct") or 1.0),
            )
            result["action"] = "spot_sell"
            result["spot_adjust_result"] = spot_adjust.get("success", False)
            if not spot_adjust.get("success"):
                result["success"] = False
                result["order_error"] = spot_adjust.get("error", "Spot sell adjust failed")
                return result

    if current_position and current_side == "SHORT":
        est_funding_this_cycle = abs(funding_rate) * current_size * mid
        total_funding += est_funding_this_cycle
        state["dn_total_funding_earned"] = total_funding
        result["funding_earned_this_cycle"] = est_funding_this_cycle
        result["total_funding_earned"] = total_funding

        size_diff = abs(current_size - target_size)
        if target_size > 0 and (size_diff / target_size > POSITION_SIZE_TOLERANCE):
            if current_size < target_size:
                add_size = target_size - current_size
                logger.info("DN user %s: increasing short by %.6f", telegram_id, add_size)
                adj_result = execute_market_order(
                    telegram_id,
                    product,
                    add_size,
                    is_long=False,
                    leverage=leverage,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                    enforce_rate_limit=False,
                )
                result["action"] = "adjust_increase"
                result["adjust_result"] = adj_result.get("success", False)
                if not adj_result.get("success"):
                    result["success"] = False
                    result["order_error"] = adj_result.get("error", "Adjust increase failed")
            else:
                reduce_size = current_size - target_size
                logger.info("DN user %s: reducing short by %.6f", telegram_id, reduce_size)
                adj_result = execute_market_order(
                    telegram_id,
                    product,
                    reduce_size,
                    is_long=True,
                    leverage=leverage,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                    enforce_rate_limit=False,
                )
                result["action"] = "adjust_decrease"
                result["adjust_result"] = adj_result.get("success", False)
                if not adj_result.get("success"):
                    result["success"] = False
                    result["order_error"] = adj_result.get("error", "Adjust decrease failed")
        else:
            result["action"] = "hold"

        if result["action"] == "hold":
            result["detail"] = "Spot+perp hedge balanced; collecting funding."
        return result

    if current_position and current_side == "LONG":
        logger.info("DN user %s: closing unexpected LONG position before opening SHORT", telegram_id)
        close_wrong_side = execute_market_order(
            telegram_id,
            product,
            current_size,
            is_long=False,
            leverage=leverage,
            slippage_pct=float(state.get("slippage_pct") or 1.0),
            enforce_rate_limit=False,
        )
        if close_wrong_side.get("success"):
            result["action"] = "close_wrong_side"
        else:
            result["action"] = "close_wrong_side_failed"
            result["success"] = False
            result["order_error"] = close_wrong_side.get("error", "Failed to close wrong-side position")
        return result

    logger.info(
        "DN user %s: opening short position, size=%.6f, funding_rate=%.8f",
        telegram_id, target_size, funding_rate,
    )
    order_result = execute_market_order(
        telegram_id,
        product,
        target_size,
        is_long=False,
        leverage=leverage,
        slippage_pct=float(state.get("slippage_pct") or 1.0),
        enforce_rate_limit=False,
    )

    if order_result.get("success"):
        state["dn_position_side"] = "SHORT"
        state["dn_entry_price"] = mid
        result["action"] = "enter_short"
        result["entry_price"] = mid
        result["order_success"] = True
        result["detail"] = "Perp short opened against spot long."
    else:
        result["action"] = "entry_failed"
        result["order_error"] = order_result.get("error", "Unknown")
        result["success"] = False

    return result
