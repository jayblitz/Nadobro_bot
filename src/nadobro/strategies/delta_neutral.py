"""
Delta Neutral — funding rate farming strategy.

Opens a short perp position to earn funding when the funding rate is positive
(shorts get paid). Monitors funding rate and auto-exits if it flips unfavorable
for an extended period. Adjusts position size toward target notional.

Note: Nado DEX is perps-only, so true delta-neutral would require external
spot hedging. This strategy focuses on funding rate farming with protective
TP/SL managed by bot_runtime.
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

    if not client or mid <= 0 or product_id is None:
        return {"success": False, "error": "Missing client, price, or product_id"}

    from src.nadobro.services.trade_service import execute_market_order

    notional = float(state.get("notional_usd") or 100.0)
    leverage = float(state.get("leverage") or 3.0)
    target_size = notional / mid

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

    current_size = float(current_position.get("amount", 0)) if current_position else 0.0
    current_side = current_position.get("side") if current_position else None

    result = {
        "success": True,
        "action": "hold",
        "funding_rate": funding_rate,
        "position_size": current_size,
        "position_side": current_side,
        "total_funding_earned": total_funding,
        "unfavorable_cycles": unfavorable_count,
    }

    funding_favorable = funding_rate > MIN_FAVORABLE_FUNDING

    if not funding_favorable:
        unfavorable_count += 1
        state["dn_unfavorable_count"] = unfavorable_count

        if current_position and unfavorable_count >= UNFAVORABLE_EXIT_CYCLES:
            logger.info(
                "DN user %s: funding unfavorable for %d cycles, exiting position",
                telegram_id, unfavorable_count,
            )
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
            state["dn_position_side"] = None
            state["dn_entry_price"] = 0.0
            state["dn_unfavorable_count"] = 0
            result["action"] = "exit"
            result["exit_reason"] = f"Funding unfavorable for {unfavorable_count} cycles"
            result["close_result"] = close_result.get("success", False)
            return result

        result["action"] = "wait_unfavorable"
        return result

    state["dn_unfavorable_count"] = 0

    if current_position and current_side == "SHORT":
        est_funding_this_cycle = abs(funding_rate) * current_size * mid
        total_funding += est_funding_this_cycle
        state["dn_total_funding_earned"] = total_funding
        result["funding_earned_this_cycle"] = est_funding_this_cycle
        result["total_funding_earned"] = total_funding

        size_diff = abs(current_size - target_size)
        if size_diff / target_size > POSITION_SIZE_TOLERANCE and target_size > 0:
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
        else:
            result["action"] = "hold"

        return result

    if current_position and current_side == "LONG":
        logger.info("DN user %s: closing unexpected LONG position before opening SHORT", telegram_id)
        execute_market_order(
            telegram_id,
            product,
            current_size,
            is_long=False,
            leverage=leverage,
            slippage_pct=float(state.get("slippage_pct") or 1.0),
            enforce_rate_limit=False,
        )
        result["action"] = "close_wrong_side"
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
    else:
        result["action"] = "entry_failed"
        result["order_error"] = order_result.get("error", "Unknown")
        result["success"] = False

    return result
