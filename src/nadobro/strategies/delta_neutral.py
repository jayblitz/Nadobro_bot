"""
Delta Neutral — funding rate farming strategy.

Holistic DN flow on Nado:
1) Buy spot on Nado Spot.
2) Short the same asset on perp with 1x-5x leverage.
3) Keep both legs size-matched and collect favorable funding.
4) Exit both legs after persistent unfavorable funding.
"""
import logging

logger = logging.getLogger(__name__)

MIN_FAVORABLE_FUNDING = 0.000001
UNFAVORABLE_EXIT_CYCLES = 5
POSITION_SIZE_TOLERANCE = 0.10


def _xstock_exchange_rate(pair: dict) -> float | None:
    """Wrapped Backed spot uses exchange_rate_x18 (underlying per 1 wrapped token). See Nado xStocks docs."""
    raw = (pair or {}).get("exchange_rate_x18")
    if raw is None:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    r = val / 1e18
    if r <= 0:
        return None
    return r


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

    from src.nadobro.config import get_dn_pair
    from src.nadobro.services.trade_service import (
        close_delta_neutral_legs,
        execute_market_order,
        execute_spot_market_order,
    )
    pair = get_dn_pair(product, network=network, client=client)
    if not pair:
        return {"success": False, "error": f"{product} is not supported for Delta Neutral."}
    product = str(pair.get("product") or product).upper()
    product_id = int(pair.get("perp_product_id") or product_id)
    spot_product_id = pair.get("spot_product_id")
    if spot_product_id is None:
        return {"success": False, "error": f"{product} spot is not supported for Delta Neutral."}
    spot_product_id = int(spot_product_id)
    spot_symbol = str(pair.get("spot_symbol") or product).upper()
    can_add_exposure = bool(pair.get("entry_allowed", True))
    entry_block_reason = str(pair.get("entry_block_reason") or "DN entry is currently blocked for this market.")

    notional = float(state.get("notional_usd") or 100.0)
    leverage = float(state.get("leverage") or 3.0)
    leverage = max(1.0, min(leverage, 5.0))
    state["leverage"] = leverage
    if notional <= 0:
        return {"success": False, "error": "Invalid notional_usd; must be > 0"}
    # Perp leg size in contract/share units; spot xStocks leg is wrapped-token units when exchange_rate applies.
    target_perp_size = notional / mid
    xrate = _xstock_exchange_rate(pair)
    target_spot_wrapped = target_perp_size / xrate if xrate else target_perp_size

    fr_data = client.get_funding_rate(product_id) or {}
    funding_rate = float(fr_data.get("funding_rate", 0) or 0)
    state["dn_last_funding_rate"] = funding_rate
    funding_entry_mode = str(state.get("funding_entry_mode") or "enter_anyway").strip().lower()
    if funding_entry_mode not in ("wait", "enter_anyway"):
        funding_entry_mode = "enter_anyway"
    state["dn_mode"] = funding_entry_mode

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
        "product": product,
        "spot_symbol": spot_symbol,
        "funding_rate": funding_rate,
        "funding_entry_mode": funding_entry_mode,
        "position_size": current_size,
        "position_side": current_side,
        "spot_size": spot_size,
        "target_perp_size": target_perp_size,
        "target_spot_wrapped": target_spot_wrapped,
        "xstock_exchange_rate": xrate,
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
            close_bundle = close_delta_neutral_legs(
                telegram_id,
                product,
                network=network,
                slippage_pct=float(state.get("slippage_pct") or 1.0),
                source="dn",
                strategy_session_id=state.get("strategy_session_id"),
                pair=pair,
            )
            close_result = close_bundle.get("perp") or {"success": True}
            spot_close_result = close_bundle.get("spot") or {"success": True}
            result["action"] = "exit"
            result["exit_reason"] = f"Funding unfavorable for {unfavorable_count} cycles"
            result["close_result"] = close_result.get("success", False)
            result["spot_close_result"] = spot_close_result.get("success", False)
            if close_bundle.get("success"):
                state["dn_position_side"] = None
                state["dn_entry_price"] = 0.0
                state["dn_unfavorable_count"] = 0
                state["dn_spot_size"] = 0.0
            else:
                result["success"] = False
                result["order_error"] = (
                    close_bundle.get("error")
                    or close_result.get("error")
                    or spot_close_result.get("error")
                    or "Failed to unwind delta-neutral legs"
                )
            return result

        if funding_entry_mode == "wait":
            result["action"] = "wait_unfavorable"
            result["detail"] = (
                f"Funding unfavorable ({funding_rate:.6f}); waiting for favorable rate "
                f"(cycle {unfavorable_count})."
            )
            return result

        result["detail"] = (
            f"Funding unfavorable ({funding_rate:.6f}); enter-anyway mode keeps hedge active."
        )

    if funding_favorable:
        state["dn_unfavorable_count"] = 0

    if not can_add_exposure and current_size <= 0 and spot_size <= 0:
        result["action"] = "wait_market_hours"
        result["detail"] = entry_block_reason
        return result

    # Keep spot leg aligned to target (wrapped token amount for xStocks).
    spot_size_diff = abs(spot_size - target_spot_wrapped)
    if target_spot_wrapped > 0 and (spot_size_diff / target_spot_wrapped > POSITION_SIZE_TOLERANCE):
        if spot_size < target_spot_wrapped:
            if can_add_exposure:
                buy_size = target_spot_wrapped - spot_size
                logger.info("DN user %s: increasing spot by %.6f %s", telegram_id, buy_size, spot_symbol)
                spot_adjust = execute_spot_market_order(
                    telegram_id,
                    product,
                    buy_size,
                    is_buy=True,
                    enforce_rate_limit=False,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                    source="dn",
                    strategy_session_id=state.get("strategy_session_id"),
                    network=network,
                    spot_product_id=spot_product_id,
                    spot_symbol=spot_symbol,
                    asset_label=spot_symbol,
                )
                result["action"] = "spot_buy"
                result["spot_adjust_result"] = spot_adjust.get("success", False)
                if not spot_adjust.get("success"):
                    result["success"] = False
                    result["order_error"] = spot_adjust.get("error", "Spot buy adjust failed")
                    return result
            else:
                result["action"] = "wait_market_hours"
                result["detail"] = entry_block_reason
                return result
        else:
            sell_size = spot_size - target_spot_wrapped
            logger.info("DN user %s: reducing spot by %.6f %s", telegram_id, sell_size, spot_symbol)
            spot_adjust = execute_spot_market_order(
                telegram_id,
                product,
                sell_size,
                is_buy=False,
                enforce_rate_limit=False,
                slippage_pct=float(state.get("slippage_pct") or 1.0),
                source="dn",
                strategy_session_id=state.get("strategy_session_id"),
                network=network,
                spot_product_id=spot_product_id,
                spot_symbol=spot_symbol,
                asset_label=spot_symbol,
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

        size_diff = abs(current_size - target_perp_size)
        if target_perp_size > 0 and (size_diff / target_perp_size > POSITION_SIZE_TOLERANCE):
            if current_size < target_perp_size:
                if can_add_exposure:
                    add_size = target_perp_size - current_size
                    logger.info("DN user %s: increasing short by %.6f", telegram_id, add_size)
                    adj_result = execute_market_order(
                        telegram_id,
                        product,
                        add_size,
                        is_long=False,
                        leverage=leverage,
                        slippage_pct=float(state.get("slippage_pct") or 1.0),
                        enforce_rate_limit=False,
                        source="dn",
                        strategy_session_id=state.get("strategy_session_id"),
                    )
                    result["action"] = "adjust_increase"
                    result["adjust_result"] = adj_result.get("success", False)
                    if not adj_result.get("success"):
                        result["success"] = False
                        result["order_error"] = adj_result.get("error", "Adjust increase failed")
                else:
                    result["action"] = "hold"
                    result["detail"] = entry_block_reason
            else:
                reduce_size = current_size - target_perp_size
                logger.info("DN user %s: reducing short by %.6f", telegram_id, reduce_size)
                adj_result = execute_market_order(
                    telegram_id,
                    product,
                    reduce_size,
                    is_long=True,
                    leverage=leverage,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                    enforce_rate_limit=False,
                    source="dn",
                    strategy_session_id=state.get("strategy_session_id"),
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
            source="dn",
            strategy_session_id=state.get("strategy_session_id"),
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
        telegram_id, target_perp_size, funding_rate,
    )
    if not can_add_exposure:
        result["action"] = "wait_market_hours"
        result["detail"] = entry_block_reason
        return result
    order_result = execute_market_order(
        telegram_id,
        product,
        target_perp_size,
        is_long=False,
        leverage=leverage,
        slippage_pct=float(state.get("slippage_pct") or 1.0),
        enforce_rate_limit=False,
        source="dn",
        strategy_session_id=state.get("strategy_session_id"),
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
