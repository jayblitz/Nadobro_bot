"""
Delta Neutral strategy.

Rules enforced:
- Spot and perp legs use equal base size (BTC/ETH only)
- Perp leverage is capped at 5x
- Runtime window is fixed at 2 hours per cycle
- Limit orders only (same reference limit price per attempt)
- Auto-requote on timeout: cancel stale limits and retry
"""
import logging
import os
import time

logger = logging.getLogger(__name__)

MIN_FAVORABLE_FUNDING = 0.000001
UNFAVORABLE_EXIT_CYCLES = 5
POSITION_SIZE_TOLERANCE = 0.001
SIZE_EPSILON = 1e-8
DN_MAX_PERP_LEVERAGE = 5.0
DN_DEFAULT_CYCLE_SECONDS = 2 * 60 * 60
DN_ALLOWED_PRODUCTS = {"BTC", "ETH"}
REQUOTE_MAX_ATTEMPTS = 3
REQUOTE_WAIT_SECONDS = 1.0
# Fallback spot product ids used when exchange metadata omits symbol/base fields.
DN_FALLBACK_SPOT_PRODUCT_IDS = {
    "BTC": 1,
    "ETH": 3,
}


def _resolve_spot_product_id(client, state: dict, asset: str) -> int | None:
    asset = str(asset or "").upper().strip()
    if asset not in DN_ALLOWED_PRODUCTS:
        return None

    cached = state.get("dn_spot_product_id")
    cached_asset = str(state.get("dn_spot_product_asset") or "").upper().strip()
    if cached is not None and cached_asset == asset:
        try:
            return int(cached)
        except (TypeError, ValueError):
            pass

    env_spot_id = os.environ.get(f"NADO_DN_{asset}_SPOT_PRODUCT_ID") or os.environ.get(f"NADO_{asset}_SPOT_PRODUCT_ID")
    if env_spot_id:
        try:
            sid = int(env_spot_id)
            state["dn_spot_product_id"] = sid
            state["dn_spot_product_asset"] = asset
            return sid
        except (TypeError, ValueError):
            logger.warning("Invalid %s spot override: %s", asset, env_spot_id)

    try:
        products = client.get_all_products_info() or {}
        for spot in products.get("spot", []) or []:
            sid = int(spot.get("id", -1))
            if sid < 0:
                continue
            symbol = str(spot.get("symbol", "") or "").upper()
            base_asset = str(spot.get("base_asset", "") or "").upper()
            if asset in symbol or base_asset == asset:
                state["dn_spot_product_id"] = sid
                state["dn_spot_product_asset"] = asset
                return sid
    except Exception as e:
        logger.warning("Spot product resolution failed for %s: %s", asset, e)

    fallback_sid = DN_FALLBACK_SPOT_PRODUCT_IDS.get(asset)
    if fallback_sid is not None:
        state["dn_spot_product_id"] = fallback_sid
        state["dn_spot_product_asset"] = asset
        logger.info(
            "Using fallback %s spot product id=%s for DN (metadata lookup unavailable).",
            asset,
            fallback_sid,
        )
        return fallback_sid
    return None


def _get_spot_balance_size(client, spot_product_id: int) -> float:
    bal = client.get_balance() or {}
    balances = bal.get("balances", {}) or {}
    return float(balances.get(spot_product_id, balances.get(str(spot_product_id), 0)) or 0.0)


def _get_signed_perp_size(client, perp_product_id: int) -> float:
    positions = client.get_positions_only() or []
    for p in positions:
        if int(p.get("product_id", -1)) != int(perp_product_id):
            continue
        size = abs(float(p.get("amount", 0) or 0.0))
        side = str(p.get("side") or "").upper()
        return size if side == "LONG" else -size
    return 0.0


def _place_spot_limit(client, spot_product_id: int, size: float, is_buy: bool, limit_price: float) -> dict:
    if size <= SIZE_EPSILON:
        return {"success": True}
    try:
        return client.place_limit_order(spot_product_id, size, limit_price, is_buy=is_buy)
    except Exception as e:
        return {"success": False, "error": str(e)}


def _place_perp_limit(
    telegram_id: int,
    product: str,
    size: float,
    is_long: bool,
    leverage: float,
    passphrase: str | None,
    limit_price: float,
) -> dict:
    from src.nadobro.services.trade_service import execute_limit_order

    if size <= SIZE_EPSILON:
        return {"success": True}
    return execute_limit_order(
        telegram_id,
        product,
        size,
        price=limit_price,
        is_long=is_long,
        leverage=leverage,
        enforce_rate_limit=False,
        passphrase=passphrase,
    )


def _cancel_stale_limits(client, perp_product_id: int, spot_product_id: int):
    try:
        client.cancel_all_orders(perp_product_id)
    except Exception:
        pass
    try:
        client.cancel_all_orders(spot_product_id)
    except Exception:
        pass


def _drive_to_target_sizes(
    telegram_id: int,
    read_client,
    exec_client,
    state: dict,
    product: str,
    perp_product_id: int,
    spot_product_id: int,
    target_size: float,
    leverage: float,
    passphrase: str | None,
) -> tuple[bool, str | None]:
    # Desired: perp short of target size; spot long of target size.
    for attempt in range(1, REQUOTE_MAX_ATTEMPTS + 1):
        current_signed_perp = _get_signed_perp_size(exec_client, perp_product_id)
        current_spot = _get_spot_balance_size(exec_client, spot_product_id)
        desired_signed_perp = -target_size
        perp_delta_signed = desired_signed_perp - current_signed_perp
        spot_delta = target_size - current_spot

        perp_ok = abs(perp_delta_signed) <= max(SIZE_EPSILON, target_size * POSITION_SIZE_TOLERANCE)
        spot_ok = abs(spot_delta) <= max(SIZE_EPSILON, target_size * POSITION_SIZE_TOLERANCE)
        if perp_ok and spot_ok:
            return True, None

        mp = read_client.get_market_price(perp_product_id) or {}
        limit_price = float(mp.get("mid") or 0.0)
        if limit_price <= 0:
            return False, "Could not fetch market price for requote."

        if abs(perp_delta_signed) > max(SIZE_EPSILON, target_size * POSITION_SIZE_TOLERANCE):
            perp_size = abs(perp_delta_signed)
            perp_is_long = perp_delta_signed > 0
            perp_r = _place_perp_limit(
                telegram_id,
                product,
                perp_size,
                perp_is_long,
                leverage,
                passphrase,
                limit_price,
            )
            if not perp_r.get("success"):
                return False, perp_r.get("error", "Perp limit order failed")

        if abs(spot_delta) > max(SIZE_EPSILON, target_size * POSITION_SIZE_TOLERANCE):
            spot_size = abs(spot_delta)
            spot_is_buy = spot_delta > 0
            spot_r = _place_spot_limit(exec_client, spot_product_id, spot_size, spot_is_buy, limit_price)
            if not spot_r.get("success"):
                return False, spot_r.get("error", "Spot limit order failed")

        time.sleep(REQUOTE_WAIT_SECONDS)

        # If not done yet, cancel stale resting limits before next re-quote.
        if attempt < REQUOTE_MAX_ATTEMPTS:
            _cancel_stale_limits(exec_client, perp_product_id, spot_product_id)

    return False, "Requote timeout: limit orders not filled after retries."


def close_open_hedge_legs(telegram_id: int, state: dict, passphrase: str | None) -> dict:
    from src.nadobro.config import get_product_id
    from src.nadobro.services.user_service import get_user_nado_client

    exec_client = get_user_nado_client(telegram_id, passphrase=passphrase)
    if not exec_client:
        return {"success": False, "error": "Wallet client unavailable for DN cleanup."}

    product = str(state.get("product") or "BTC").upper()
    perp_pid = get_product_id(product)
    leverage = min(float(state.get("dn_perp_leverage") or state.get("leverage") or 3.0), DN_MAX_PERP_LEVERAGE)
    if perp_pid is None:
        return {"success": False, "error": f"Invalid DN product '{product}'."}

    spot_pid = _resolve_spot_product_id(exec_client, state, product)
    if spot_pid is None:
        return {"success": False, "error": f"{product} spot product not found for DN cleanup."}

    # Closing both legs means driving target base sizes to zero.
    ok, err = _drive_to_target_sizes(
        telegram_id=telegram_id,
        read_client=exec_client,
        exec_client=exec_client,
        state=state,
        product=product,
        perp_product_id=perp_pid,
        spot_product_id=spot_pid,
        target_size=0.0,
        leverage=leverage,
        passphrase=passphrase,
    )
    if not ok:
        return {"success": False, "error": err or "DN cleanup failed"}
    return {"success": True}


def run_cycle(telegram_id: int, network: str, state: dict, **kwargs) -> dict:
    from src.nadobro.services.user_service import get_user_nado_client

    read_client = kwargs.get("client")
    mid = float(kwargs.get("mid", 0.0) or 0.0)
    perp_product_id = kwargs.get("product_id")
    product = str(kwargs.get("product", state.get("product", "BTC"))).upper()
    passphrase = kwargs.get("passphrase")
    if not read_client or mid <= 0 or perp_product_id is None:
        return {"success": False, "error": "Missing client, price, or product_id"}
    if product not in DN_ALLOWED_PRODUCTS:
        return {"success": False, "error": "Delta Neutral supports BTC and ETH only (spot + perp short)."}

    exec_client = get_user_nado_client(telegram_id, passphrase=passphrase)
    if not exec_client:
        return {"success": False, "error": "DN runtime requires an active passphrase session."}

    spot_notional = float(state.get("notional_usd") or 100.0)
    leverage = min(float(state.get("dn_perp_leverage") or state.get("leverage") or 3.0), DN_MAX_PERP_LEVERAGE)
    if spot_notional <= 0:
        return {"success": False, "error": "Invalid notional_usd; must be > 0"}

    state["dn_perp_leverage"] = leverage
    state["leverage"] = leverage
    # Example: $100 spot @ 5x => $20 perp collateral.
    state["dn_perp_margin_usd"] = spot_notional / leverage if leverage > 0 else spot_notional
    target_size = spot_notional / mid

    spot_pid = _resolve_spot_product_id(exec_client, state, product)
    if spot_pid is None:
        return {"success": False, "error": f"{product} spot product not found. Set NADO_DN_{product}_SPOT_PRODUCT_ID."}

    fr_data = read_client.get_funding_rate(perp_product_id) or {}
    funding_rate = float(fr_data.get("funding_rate", 0) or 0)
    state["dn_last_funding_rate"] = funding_rate

    signed_perp = _get_signed_perp_size(exec_client, perp_product_id)
    perp_size = abs(signed_perp)
    perp_side = "LONG" if signed_perp > 0 else ("SHORT" if signed_perp < 0 else None)
    spot_size = _get_spot_balance_size(exec_client, spot_pid)
    state["dn_last_spot_size"] = spot_size
    state["dn_last_perp_size"] = perp_size
    state["dn_last_hedge_diff_size"] = abs(spot_size - perp_size)

    now = time.time()
    cycle_started = float(state.get("dn_cycle_started_at") or 0.0)
    if cycle_started > 0 and (now - cycle_started) >= DN_DEFAULT_CYCLE_SECONDS:
        close = close_open_hedge_legs(telegram_id, state, passphrase)
        if close.get("success"):
            state["dn_cycle_started_at"] = 0.0
            state["dn_unfavorable_count"] = 0
            state["dn_position_side"] = None
            state["dn_entry_price"] = 0.0
            return {"success": True, "action": "rollover_close", "rolled": True}
        return {"success": False, "action": "rollover_close_failed", "error": close.get("error", "DN rollover close failed")}

    unfavorable = int(state.get("dn_unfavorable_count") or 0)
    if funding_rate <= MIN_FAVORABLE_FUNDING:
        unfavorable += 1
        state["dn_unfavorable_count"] = unfavorable
        if perp_side == "SHORT" and unfavorable >= UNFAVORABLE_EXIT_CYCLES:
            close = close_open_hedge_legs(telegram_id, state, passphrase)
            if close.get("success"):
                state["dn_unfavorable_count"] = 0
                state["dn_cycle_started_at"] = 0.0
                return {"success": True, "action": "exit", "exit_reason": f"Funding unfavorable for {unfavorable} cycles"}
            return {"success": False, "action": "exit_failed", "error": close.get("error", "DN exit close failed")}
        return {"success": True, "action": "wait_unfavorable", "funding_rate": funding_rate}
    state["dn_unfavorable_count"] = 0

    if perp_side == "LONG":
        ok, err = _drive_to_target_sizes(
            telegram_id, read_client, exec_client, state, product, perp_product_id, spot_pid, 0.0, leverage, passphrase
        )
        if ok:
            return {"success": True, "action": "close_wrong_side"}
        return {"success": False, "action": "close_wrong_side_failed", "error": err or "Failed to close wrong-side position"}

    # Live funding estimate
    if perp_side == "SHORT":
        est = funding_rate * perp_size * mid
        recv = float(state.get("dn_total_funding_received") or 0.0)
        paid = float(state.get("dn_total_funding_paid") or 0.0)
        if est >= 0:
            recv += est
        else:
            paid += abs(est)
        state["dn_total_funding_received"] = recv
        state["dn_total_funding_paid"] = paid
        state["dn_total_funding_net"] = recv - paid
        state["dn_total_funding_earned"] = recv
        state["dn_last_funding_cycle"] = est

    # Enforce equal spot/perp base size at target using requote loop.
    ok, err = _drive_to_target_sizes(
        telegram_id=telegram_id,
        read_client=read_client,
        exec_client=exec_client,
        state=state,
        product=product,
        perp_product_id=perp_product_id,
        spot_product_id=spot_pid,
        target_size=target_size,
        leverage=leverage,
        passphrase=passphrase,
    )
    if not ok:
        return {"success": False, "action": "requote_failed", "error": err or "DN requote failed"}

    if not cycle_started:
        state["dn_position_side"] = "SHORT"
        state["dn_entry_price"] = mid
        state["dn_cycle_started_at"] = now
        return {"success": True, "action": "enter_short", "entry_price": mid}
    return {"success": True, "action": "hold", "funding_rate": funding_rate}
