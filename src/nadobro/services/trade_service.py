import logging
from datetime import datetime, timedelta
from src.nadobro.models.database import (
    TradeStatus, OrderSide, OrderTypeEnum, NetworkMode,
    get_last_trade_for_rate_limit, insert_trade, update_trade, get_trades_by_user,
    find_open_trade,
)
from src.nadobro.config import (
    get_product_id,
    get_spot_product_id,
    get_product_name,
    get_product_max_leverage,
    get_perp_products,
    is_product_isolated_only,
    RATE_LIMIT_SECONDS,
    MIN_TRADE_SIZE_USD,
)
from src.nadobro.services.user_service import get_user, get_user_nado_client, get_user_readonly_client, update_trade_stats, ensure_active_wallet_ready

logger = logging.getLogger(__name__)


def _cancel_open_orders_for_product(client, product_id: int) -> tuple[int, list[str]]:
    cancelled = 0
    errors: list[str] = []
    try:
        open_orders = client.get_open_orders(product_id) or []
    except Exception as e:
        return 0, [f"{get_product_name(product_id)}: open-orders lookup failed ({e})"]
    for order in open_orders:
        digest = order.get("digest")
        if not digest:
            continue
        try:
            r = client.cancel_order(product_id, digest)
            if r.get("success"):
                cancelled += 1
            else:
                errors.append(f"{get_product_name(product_id)}: cancel failed ({r.get('error', 'unknown')})")
        except Exception as e:
            errors.append(f"{get_product_name(product_id)}: cancel exception ({e})")
    return cancelled, errors


def check_rate_limit(telegram_id: int, network: str = "mainnet") -> tuple[bool, str]:
    last_trade = get_last_trade_for_rate_limit(telegram_id, network=network)
    if last_trade and last_trade.get("created_at"):
        elapsed = None
        try:
            raw = last_trade["created_at"]
            if isinstance(raw, str):
                created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            else:
                created = raw
            if created.tzinfo:
                now = datetime.now(created.tzinfo)
            else:
                now = datetime.utcnow()
            elapsed = (now - created).total_seconds()
        except Exception as e:
            logger.warning("Rate-limit timestamp parse failed for user %s: %s", telegram_id, e)
            return True, ""
        if elapsed < 0:
            elapsed = 0
        if elapsed < RATE_LIMIT_SECONDS:
            remaining = int(RATE_LIMIT_SECONDS - elapsed)
            return False, f"Rate limit: wait {remaining}s before next trade."
    return True, ""


def validate_trade(
    telegram_id: int,
    product: str,
    size: float,
    leverage: float = 1.0,
    enforce_rate_limit: bool = True,
) -> tuple[bool, str]:
    user_obj = get_user(telegram_id)
    network = user_obj.network_mode.value if user_obj else "mainnet"
    client = get_user_readonly_client(telegram_id)
    product_id = get_product_id(product, network=network, client=client)
    if product_id is None:
        available = ", ".join(get_perp_products(network=network, client=client))
        return False, f"Unknown product '{product}'. Available: {available}"

    if size <= 0:
        return False, "Trade size must be positive."

    max_leverage = get_product_max_leverage(product, network=network, client=client)
    if leverage > max_leverage:
        return False, f"Max leverage for {product.upper()} is {max_leverage}x."

    if leverage < 1:
        return False, "Leverage must be at least 1x."

    wallet_ok, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ok:
        return False, wallet_msg

    if not client:
        return False, "Could not initialize client. Please try again."

    balance = client.get_balance()
    if not balance.get("exists"):
        return False, "Subaccount not found. Please deposit funds first on Nado."

    balances = balance.get("balances", {}) or {}
    usdt_balance = balances.get(0, balances.get("0", 0))

    mp = client.get_market_price(product_id)
    if mp["mid"] == 0:
        return False, f"Could not fetch {product} price. Market may be unavailable."

    notional = size * mp["mid"]
    required_margin = notional / leverage if leverage > 1 else notional

    if required_margin > usdt_balance * 0.95:
        return False, (
            f"Insufficient margin.\n"
            f"Required: ~${required_margin:,.2f}\n"
            f"Available: ${usdt_balance:,.2f}\n"
            f"(Using 95% safety buffer)"
        )

    if notional < MIN_TRADE_SIZE_USD:
        return False, f"Minimum trade size is ${MIN_TRADE_SIZE_USD}."

    if enforce_rate_limit:
        allowed, msg = check_rate_limit(telegram_id, network=network)
        if not allowed:
            return False, msg

    return True, ""


def execute_market_order(
    telegram_id: int,
    product: str,
    size: float,
    is_long: bool,
    leverage: float = 1.0,
    slippage_pct: float = 1.0,
    enforce_rate_limit: bool = True,
    tp_price: float = None,
    sl_price: float = None,
    **kwargs,
) -> dict:
    valid, msg = validate_trade(
        telegram_id,
        product,
        size,
        leverage,
        enforce_rate_limit=enforce_rate_limit,
    )
    if not valid:
        return {"success": False, "error": msg}

    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    product_id = get_product_id(product, network=network)
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required. Check Wallet settings."}

    trade_id = insert_trade({
        "user_id": telegram_id,
        "product_id": product_id,
        "product_name": get_product_name(product_id, network=network),
        "order_type": OrderTypeEnum.MARKET.value,
        "side": OrderSide.LONG.value if is_long else OrderSide.SHORT.value,
        "size": size,
        "leverage": leverage,
        "status": TradeStatus.PENDING.value,
        "created_at": datetime.utcnow().isoformat(),
    }, network=network)
    if not trade_id:
        return {"success": False, "error": "Failed to record trade."}

    isolated_only = is_product_isolated_only(product, network=network, client=client)
    isolated_margin = None
    if isolated_only:
        try:
            mp = client.get_market_price(product_id)
            mid = float(mp.get("mid", 0) or 0)
            if mid > 0 and float(leverage or 0) > 0:
                isolated_margin = (float(size) * mid) / max(1.0, float(leverage))
        except Exception:
            isolated_margin = None
    result = client.place_market_order(
        product_id,
        size,
        is_buy=is_long,
        slippage_pct=slippage_pct,
        isolated_only=isolated_only,
        isolated_margin=isolated_margin,
    )

    if result["success"]:
        update_trade(trade_id, {
            "status": TradeStatus.FILLED.value,
            "order_digest": result.get("digest"),
            "price": _get_post_fill_price(client, product_id) or result.get("price", 0),
            "filled_at": datetime.utcnow().isoformat(),
        }, network=network)
    else:
        update_trade(trade_id, {
            "status": TradeStatus.FAILED.value,
            "error_message": result.get("error", "Unknown error"),
        }, network=network)

    if result["success"]:
        mp = client.get_market_price(product_id)
        update_trade_stats(telegram_id, size * mp["mid"])
        payload = {
            "success": True,
            "side": "LONG" if is_long else "SHORT",
            "size": size,
            "product": get_product_name(product_id, network=network),
            "price": mp["mid"],
            "digest": result.get("digest"),
            "network": user.network_mode.value,
        }
        tp_result = _place_take_profit_order(
            client=client,
            product_id=product_id,
            size=size,
            is_long=is_long,
            tp_price=tp_price,
        )
        if tp_result:
            payload.update(tp_result)
        sl_result = _arm_stop_loss_rule(
            telegram_id=telegram_id,
            network=network,
            product=get_product_name(product_id, network=network),
            is_long=is_long,
            stop_price=sl_price,
            size=size,
        )
        if sl_result:
            payload.update(sl_result)
        return payload

    return result


def execute_spot_market_order(
    telegram_id: int,
    asset: str,
    size: float,
    is_buy: bool,
    enforce_rate_limit: bool = False,
    slippage_pct: float = 1.0,
) -> dict:
    asset = (asset or "").upper().strip()
    spot_product_id = get_spot_product_id(asset)
    if spot_product_id is None:
        return {"success": False, "error": f"{asset} spot is not supported for this strategy."}
    if size <= 0:
        return {"success": False, "error": "Spot size must be positive."}

    wallet_ok, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ok:
        return {"success": False, "error": wallet_msg}

    readonly = get_user_readonly_client(telegram_id)
    if not readonly:
        return {"success": False, "error": "Could not initialize client. Please try again."}

    spot_price = readonly.get_market_price(spot_product_id)
    mid = float(spot_price.get("mid") or 0.0)
    if mid <= 0:
        return {"success": False, "error": f"Could not fetch {asset} spot price."}
    notional = size * mid
    if notional < MIN_TRADE_SIZE_USD:
        return {"success": False, "error": f"Minimum trade size is ${MIN_TRADE_SIZE_USD}."}

    balance = readonly.get_balance() or {}
    balances = balance.get("balances", {}) or {}
    usdt_balance = float(balances.get(0, balances.get("0", 0)) or 0.0)
    spot_balance = float(balances.get(spot_product_id, balances.get(str(spot_product_id), 0)) or 0.0)
    if is_buy and notional > usdt_balance * 0.98:
        return {
            "success": False,
            "error": (
                f"Insufficient USDT0 for {asset} spot buy.\n"
                f"Required: ~${notional:,.2f}\n"
                f"Available: ${usdt_balance:,.2f}"
            ),
        }
    if (not is_buy) and size > spot_balance * 0.999:
        return {
            "success": False,
            "error": (
                f"Insufficient {asset} spot balance to sell.\n"
                f"Required: {size:,.6f}\n"
                f"Available: {spot_balance:,.6f}"
            ),
        }

    if enforce_rate_limit:
        user_obj = get_user(telegram_id)
        net = user_obj.network_mode.value if user_obj else "mainnet"
        allowed, msg = check_rate_limit(telegram_id, network=net)
        if not allowed:
            return {"success": False, "error": msg}

    client = get_user_nado_client(telegram_id)
    user = get_user(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required. Check Wallet settings."}
    network = user.network_mode.value

    trade_id = insert_trade({
        "user_id": telegram_id,
        "product_id": spot_product_id,
        "product_name": f"{asset}-SPOT",
        "order_type": OrderTypeEnum.MARKET.value,
        "side": OrderSide.LONG.value if is_buy else OrderSide.SHORT.value,
        "size": size,
        "leverage": 1.0,
        "status": TradeStatus.PENDING.value,
        "created_at": datetime.utcnow().isoformat(),
    }, network=network)
    if not trade_id:
        return {"success": False, "error": "Failed to record spot trade."}

    result = client.place_market_order(spot_product_id, size, is_buy=is_buy, slippage_pct=slippage_pct)
    if result.get("success"):
        post_px = _get_post_fill_price(client, spot_product_id) or mid
        update_trade(trade_id, {
            "status": TradeStatus.FILLED.value,
            "order_digest": result.get("digest"),
            "price": post_px,
            "filled_at": datetime.utcnow().isoformat(),
        }, network=network)
        update_trade_stats(telegram_id, abs(size * post_px))
        return {
            "success": True,
            "side": "BUY" if is_buy else "SELL",
            "asset": asset,
            "size": size,
            "price": post_px,
            "product_id": spot_product_id,
            "digest": result.get("digest"),
            "network": network,
        }

    update_trade(trade_id, {
        "status": TradeStatus.FAILED.value,
        "error_message": result.get("error", "Unknown error"),
    }, network=network)
    return result


def execute_limit_order(
    telegram_id: int,
    product: str,
    size: float,
    price: float,
    is_long: bool,
    leverage: float = 1.0,
    enforce_rate_limit: bool = True,
    tp_price: float = None,
    sl_price: float = None,
    **kwargs,
) -> dict:
    valid, msg = validate_trade(
        telegram_id,
        product,
        size,
        leverage,
        enforce_rate_limit=enforce_rate_limit,
    )
    if not valid:
        return {"success": False, "error": msg}

    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    product_id = get_product_id(product, network=network)
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required. Check Wallet settings."}

    trade_id = insert_trade({
        "user_id": telegram_id,
        "product_id": product_id,
        "product_name": get_product_name(product_id, network=network),
        "order_type": OrderTypeEnum.LIMIT.value,
        "side": OrderSide.LONG.value if is_long else OrderSide.SHORT.value,
        "size": size,
        "price": price,
        "leverage": leverage,
        "status": TradeStatus.PENDING.value,
        "created_at": datetime.utcnow().isoformat(),
    }, network=network)
    if not trade_id:
        return {"success": False, "error": "Failed to record trade."}

    isolated_only = is_product_isolated_only(product, network=network, client=client)
    isolated_margin = None
    if isolated_only and float(leverage or 0) > 0:
        isolated_margin = (float(size) * float(price)) / max(1.0, float(leverage))
    result = client.place_limit_order(
        product_id,
        size,
        price,
        is_buy=is_long,
        isolated_only=isolated_only,
        isolated_margin=isolated_margin,
    )

    if result["success"]:
        update_trade(
            trade_id,
            {
                "status": TradeStatus.PENDING.value,
                "order_digest": result.get("digest"),
            },
            network=network,
        )
    else:
        update_trade(trade_id, {"status": TradeStatus.FAILED.value, "error_message": result.get("error", "Unknown error")}, network=network)

    if result["success"]:
        return {
            "success": True,
            "side": "LONG" if is_long else "SHORT",
            "size": size,
            "product": get_product_name(product_id, network=network),
            "price": price,
            "digest": result.get("digest"),
            "network": network,
            "type": "LIMIT",
            "status": TradeStatus.PENDING.value,
            "message": "Limit order accepted and recorded as pending until execution.",
        }

    return result


def _place_take_profit_order(client, product_id: int, size: float, is_long: bool, tp_price: float | None) -> dict:
    if tp_price is None:
        return {}
    try:
        tp = float(tp_price)
    except (TypeError, ValueError):
        return {"tp_requested": True, "tp_set": False, "tp_error": "Invalid TP price."}
    if tp <= 0:
        return {"tp_requested": True, "tp_set": False, "tp_error": "TP price must be greater than 0."}

    # TP for a long is a sell limit, for a short is a buy limit.
    tp_result = client.place_limit_order(product_id, float(size), tp, is_buy=(not is_long))
    if tp_result.get("success"):
        return {
            "tp_requested": True,
            "tp_set": True,
            "tp_price": tp,
            "tp_digest": tp_result.get("digest"),
        }
    return {
        "tp_requested": True,
        "tp_set": False,
        "tp_price": tp,
        "tp_error": tp_result.get("error", "Failed to place TP order."),
    }


def _arm_stop_loss_rule(
    telegram_id: int,
    network: str,
    product: str,
    is_long: bool,
    stop_price: float | None,
    size: float,
) -> dict:
    if stop_price is None:
        return {}
    from src.nadobro.services.stop_loss_service import register_stop_loss_rule

    side = "LONG" if is_long else "SHORT"
    result = register_stop_loss_rule(
        telegram_id=telegram_id,
        network=network,
        product=product,
        side=side,
        stop_price=stop_price,
        size=size,
    )
    if result.get("success"):
        return {
            "sl_requested": True,
            "sl_armed": True,
            "sl_price": float(result.get("stop_price") or 0),
            "sl_rule_id": result.get("rule_id"),
        }
    return {
        "sl_requested": True,
        "sl_armed": False,
        "sl_price": float(stop_price or 0),
        "sl_error": result.get("error", "Failed to arm stop-loss."),
    }


def apply_tp_sl_to_open_position(
    telegram_id: int,
    product: str,
    tp_price: float | None = None,
    sl_price: float | None = None,
) -> dict:
    """
    Place a reduce-only take-profit limit and/or arm the bot stop-loss rule for an existing PERP position.
    Used by natural-language commands so replies reflect real execution, not LLM text.
    """
    tp_price = None if tp_price is None else float(tp_price)
    sl_price = None if sl_price is None else float(sl_price)
    if (tp_price is None or tp_price <= 0) and (sl_price is None or sl_price <= 0):
        return {"success": False, "error": "Provide a valid TP and/or SL price greater than zero."}

    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    product_id = get_product_id(product, network=network)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'."}

    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required."}

    from src.nadobro.services.settings_service import get_user_settings

    _, settings = get_user_settings(telegram_id)
    leverage = float(settings.get("default_leverage", 3) or 3)
    leverage = max(1.0, leverage)

    net_positions = _normalize_net_positions(client.get_all_positions() or [])
    product_pos = net_positions.get(product_id)
    if not product_pos:
        return {"success": False, "error": f"No open position on {get_product_name(product_id, network=network)}."}

    signed_amount = float(product_pos.get("signed_amount", 0) or 0)
    pos_size = abs(signed_amount)
    if pos_size <= 0:
        return {"success": False, "error": f"No open position on {get_product_name(product_id, network=network)}."}

    is_long = signed_amount > 0
    product_name = get_product_name(product_id, network=network)

    isolated_only = is_product_isolated_only(product, network=network, client=client)
    isolated_margin = None
    if isolated_only:
        mp = client.get_market_price(product_id)
        mid = float(mp.get("mid", 0) or 0)
        if mid > 0:
            isolated_margin = (float(pos_size) * mid) / leverage

    out: dict = {
        "product": product_name,
        "network": network,
        "position_size": pos_size,
        "side": "LONG" if is_long else "SHORT",
    }
    any_ok = False

    if tp_price is not None and tp_price > 0:
        out["tp_requested"] = True
        tp_res = client.place_limit_order(
            product_id,
            float(pos_size),
            float(tp_price),
            is_buy=(not is_long),
            isolated_only=isolated_only,
            isolated_margin=isolated_margin,
            reduce_only=True,
        )
        if tp_res.get("success"):
            any_ok = True
            out["tp_set"] = True
            out["tp_price"] = float(tp_price)
            out["tp_digest"] = tp_res.get("digest")
        else:
            out["tp_set"] = False
            out["tp_price"] = float(tp_price)
            out["tp_error"] = tp_res.get("error", "Failed to place TP order.")

    if sl_price is not None and sl_price > 0:
        out["sl_requested"] = True
        sl_part = _arm_stop_loss_rule(
            telegram_id=telegram_id,
            network=network,
            product=product_name,
            is_long=is_long,
            stop_price=float(sl_price),
            size=float(pos_size),
        )
        out.update(sl_part)
        if sl_part.get("sl_armed"):
            any_ok = True

    if out.get("tp_requested") and out.get("sl_requested"):
        out["success"] = bool(any_ok)
    elif out.get("tp_requested"):
        out["success"] = bool(out.get("tp_set"))
    elif out.get("sl_requested"):
        out["success"] = bool(out.get("sl_armed"))
    else:
        out["success"] = False

    if not out.get("success"):
        errs = []
        if out.get("tp_error"):
            errs.append(str(out["tp_error"]))
        if out.get("sl_error"):
            errs.append(str(out["sl_error"]))
        out["error"] = errs[0] if errs else "TP/SL placement failed."

    return out


def limit_close_position(
    telegram_id: int,
    product: str,
    limit_price: float,
    size: float | None = None,
) -> dict:
    """
    Place a reduce-only limit order to close (fully or partially) an existing PERP position.
    """
    try:
        lp = float(limit_price)
    except (TypeError, ValueError):
        return {"success": False, "error": "Invalid limit price."}
    if lp <= 0:
        return {"success": False, "error": "Limit price must be greater than zero."}

    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    product_id = get_product_id(product, network=network)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'."}

    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required."}

    from src.nadobro.services.settings_service import get_user_settings

    _, settings = get_user_settings(telegram_id)
    leverage = float(settings.get("default_leverage", 3) or 3)
    leverage = max(1.0, leverage)

    net_positions = _normalize_net_positions(client.get_all_positions() or [])
    product_pos = net_positions.get(product_id)
    if not product_pos:
        return {"success": False, "error": f"No open position on {get_product_name(product_id, network=network)}."}

    signed_amount = float(product_pos.get("signed_amount", 0) or 0)
    pos_size = abs(signed_amount)
    if pos_size <= 0:
        return {"success": False, "error": f"No open position on {get_product_name(product_id, network=network)}."}

    is_long = signed_amount > 0
    close_sz = float(size) if size is not None else pos_size
    close_sz = min(close_sz, pos_size)
    if close_sz <= 0:
        return {"success": False, "error": "Invalid close size."}

    product_name = get_product_name(product_id, network=network)
    isolated_only = is_product_isolated_only(product, network=network, client=client)
    isolated_margin = None
    if isolated_only:
        mp = client.get_market_price(product_id)
        mid = float(mp.get("mid", 0) or 0)
        if mid > 0:
            isolated_margin = (float(close_sz) * mid) / leverage

    r = client.place_limit_order(
        product_id,
        float(close_sz),
        float(lp),
        is_buy=(not is_long),
        isolated_only=isolated_only,
        isolated_margin=isolated_margin,
        reduce_only=True,
    )
    if not r.get("success"):
        return {"success": False, "error": r.get("error", "Limit close failed.")}

    return {
        "success": True,
        "kind": "LIMIT_CLOSE",
        "product": product_name,
        "network": network,
        "size": close_sz,
        "limit_price": float(lp),
        "digest": r.get("digest"),
        "side": "LONG" if is_long else "SHORT",
    }


def _normalize_net_positions(positions: list) -> dict[int, dict]:
    """
    Build net position by product with de-dup protection.

    Some client payloads can include duplicated entries for the same product from
    different source lists. We fingerprint rows and collapse to a net signed size
    so close actions execute exactly once per product.
    """
    seen_rows = set()
    net_by_product: dict[int, dict] = {}

    for p in positions or []:
        try:
            pid = int(p.get("product_id", -1))
            if pid < 0:
                continue
            side = str(p.get("side", "") or "").upper()
            amount = float(p.get("amount", 0) or 0)
            if amount <= 0:
                continue
            # Fingerprint rounded values to suppress near-identical duplicates.
            fp = (pid, side, round(amount, 12), round(float(p.get("price", 0) or 0), 8))
            if fp in seen_rows:
                continue
            seen_rows.add(fp)

            signed = amount if side == "LONG" else -amount
            current = net_by_product.get(
                pid,
                {"product_name": p.get("product_name", get_product_name(pid)), "signed_amount": 0.0},
            )
            current["signed_amount"] = float(current.get("signed_amount", 0.0)) + signed
            net_by_product[pid] = current
        except Exception:
            continue

    return net_by_product


def _get_post_fill_price(client, product_id: int) -> float | None:
    try:
        price_data = client.get_market_price(product_id)
        mid = float(price_data.get("mid", 0) or 0)
        return mid if mid > 0 else None
    except Exception:
        return None

# NOTE: The Nado SDK place_order response only returns a digest (order hash),
# not the exchange-confirmed fill price. There is no fills/trade-history endpoint
# in the current SDK to retrieve actual execution prices post-trade. Using the
# real-time market mid immediately after a successful IOC order is the best
# available approximation — IOC orders fill instantly at market, so mid at that
# moment closely tracks the actual fill level. This is strictly better than the
# old approach of using the slippage-inflated submission price.
# If the SDK adds a fills endpoint in the future, _get_post_fill_price should
# be updated to query actual execution data by digest.


def _record_close_in_db(
    telegram_id: int,
    product_id: int,
    close_size: float,
    pos_size: float,
    side: str,
    client,
    fill_price: float = None,
    network: str | None = None,
):
    try:
        user = get_user(telegram_id)
        selected_network = str(network or (user.network_mode.value if user else "mainnet"))

        close_price = fill_price or 0.0
        if not close_price:
            try:
                price_data = client.get_market_price(product_id)
                close_price = float(price_data.get("mid", 0) or 0)
            except Exception:
                pass

        open_trade = find_open_trade(telegram_id, product_id, network=selected_network)
        is_full_close = close_size >= pos_size

        if open_trade:
            open_price = float(open_trade.get("price") or 0)
            open_side = open_trade.get("side", "")
            pnl = 0.0
            if open_price > 0 and close_price > 0:
                if open_side == "long":
                    pnl = (close_price - open_price) * close_size
                elif open_side == "short":
                    pnl = (open_price - close_price) * close_size

            if is_full_close:
                update_trade(open_trade["id"], {
                    "status": TradeStatus.CLOSED.value,
                    "close_price": close_price,
                    "closed_at": datetime.utcnow().isoformat(),
                    "pnl": round(pnl, 4),
                }, network=selected_network)
                logger.info(
                    "Trade #%d fully closed: %s %s size=%.4f open=%.2f close=%.2f pnl=%.4f",
                    open_trade["id"], open_side, get_product_name(product_id),
                    close_size, open_price, close_price, pnl,
                )
            else:
                update_trade(open_trade["id"], {
                    "pnl": round(pnl, 4),
                    "close_price": close_price,
                }, network=selected_network)
                logger.info(
                    "Trade #%d partially closed: %s %s closed=%.4f/%.4f open=%.2f close=%.2f pnl=%.4f",
                    open_trade["id"], open_side, get_product_name(product_id),
                    close_size, pos_size, open_price, close_price, pnl,
                )
        else:
            insert_trade({
                "user_id": telegram_id,
                "product_id": product_id,
                "product_name": get_product_name(product_id),
                "order_type": "market",
                "side": side,
                "size": close_size,
                "price": close_price,
                "leverage": 1.0,
                "status": TradeStatus.CLOSED.value,
                "close_price": close_price,
                "closed_at": datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
                "filled_at": datetime.utcnow().isoformat(),
            }, network=selected_network)
            logger.info(
                "Close trade recorded (no matching open): %s %s size=%.4f price=%.2f",
                side, get_product_name(product_id), close_size, close_price,
            )
    except Exception as e:
        logger.warning("Failed to record close in DB: %s", e)


def close_position(
    telegram_id: int,
    product: str,
    size: float = None,
    network: str | None = None,
    **kwargs,
) -> dict:
    selected_network = str(network or "")
    product_id = get_product_id(product, network=selected_network or "mainnet")
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'."}

    client = get_user_nado_client(telegram_id, network=selected_network or None)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required."}
    cancelled_orders, order_errors = _cancel_open_orders_for_product(client, product_id)

    net_positions = _normalize_net_positions(client.get_all_positions() or [])
    product_pos = net_positions.get(product_id)
    if not product_pos:
        if cancelled_orders > 0:
            return {
                "success": True,
                "cancelled": 0.0,
                "product": get_product_name(product_id, network=selected_network or "mainnet"),
                "cancelled_orders": cancelled_orders,
                "order_errors": order_errors if order_errors else None,
            }
        return {"success": False, "error": f"No open positions on {product}."}

    signed_amount = float(product_pos.get("signed_amount", 0) or 0)
    pos_size = abs(signed_amount)
    if pos_size <= 0:
        return {"success": False, "error": f"No open positions on {product}."}

    close_size = min(pos_size, float(size)) if size else pos_size
    full_close_requested = size is None or close_size >= pos_size
    close_side = "short" if signed_amount > 0 else "long"

    remaining_size = close_size
    attempts = 0
    while remaining_size > 0 and attempts < 3:
        attempts += 1
        # Refresh side before each close attempt in case position flips between retries.
        latest_positions = _normalize_net_positions(client.get_all_positions() or [])
        latest_pos = latest_positions.get(product_id)
        if not latest_pos:
            remaining_size = 0.0
            break
        latest_signed = float(latest_pos.get("signed_amount", 0) or 0)
        latest_abs = abs(latest_signed)
        if latest_abs <= 0:
            remaining_size = 0.0
            break
        is_buy = latest_signed < 0
        this_close_size = min(remaining_size, latest_abs)
        r = client.place_market_order(
            product_id,
            this_close_size,
            is_buy=is_buy,
            slippage_pct=1.0,
            reduce_only=True,
        )
        if not r.get("success"):
            return {"success": False, "error": f"Failed to close position: {r.get('error', 'unknown')}"}
        fill_price = _get_post_fill_price(client, product_id)
        _record_close_in_db(
            telegram_id,
            product_id,
            this_close_size,
            pos_size,
            close_side,
            client,
            fill_price=fill_price,
            network=selected_network or None,
        )
        remaining_size -= this_close_size

    post_positions = _normalize_net_positions(client.get_all_positions() or [])
    post_pos = post_positions.get(product_id)
    post_open_orders = client.get_open_orders(product_id) or []
    if full_close_requested:
        still_open = bool(post_pos and abs(float(post_pos.get("signed_amount", 0) or 0)) > 0)
        if still_open or post_open_orders:
            detail = []
            if still_open:
                detail.append("position still open")
            if post_open_orders:
                detail.append(f"{len(post_open_orders)} open orders remain")
            return {
                "success": False,
                "error": f"Close verification failed for {product}: {', '.join(detail)}.",
            }

    payload = {
        "success": True,
        "cancelled": close_size,
        "product": get_product_name(product_id, network=selected_network or "mainnet"),
    }
    if cancelled_orders:
        payload["cancelled_orders"] = cancelled_orders
    if order_errors:
        payload["order_errors"] = order_errors
    return payload


def close_all_positions(telegram_id: int, network: str | None = None, **kwargs) -> dict:
    selected_network = str(network or "")
    client = get_user_nado_client(telegram_id, network=selected_network or None)
    if not client:
        return {"success": False, "error": "Wallet not initialized or key migration required."}

    cancelled_orders = 0
    order_errors = []
    # Always cancel stale open orders first so strategy stop leaves no resting orders.
    for product_name in get_perp_products(network=selected_network or "mainnet", client=client):
        pid = get_product_id(product_name, network=selected_network or "mainnet", client=client)
        if pid is None:
            continue
        c_count, c_errors = _cancel_open_orders_for_product(client, pid)
        cancelled_orders += c_count
        if c_errors:
            order_errors.extend(c_errors)

    net_positions = _normalize_net_positions(client.get_all_positions() or [])
    if not net_positions:
        if cancelled_orders > 0 and not order_errors:
            return {
                "success": True,
                "cancelled": 0.0,
                "products": [],
                "cancelled_orders": cancelled_orders,
            }
        if cancelled_orders > 0:
            return {
                "success": True,
                "cancelled": 0.0,
                "products": [],
                "cancelled_orders": cancelled_orders,
                "order_errors": order_errors,
            }
        return {
            "success": True,
            "cancelled": 0.0,
            "products": [],
        }

    cancelled = 0.0
    errors = []
    products_closed = set()
    for pid, p in net_positions.items():
        try:
            signed_amount = float(p.get("signed_amount", 0) or 0)
            pos_size = abs(signed_amount)
            if pos_size <= 0:
                continue
            is_buy = signed_amount < 0
            r = client.place_market_order(
                pid,
                pos_size,
                is_buy=is_buy,
                slippage_pct=1.0,
                reduce_only=True,
            )
            if r["success"]:
                cancelled += pos_size
                product_name = p.get("product_name", get_product_name(pid, network=selected_network or "mainnet"))
                products_closed.add(product_name)
                close_side = "short" if signed_amount > 0 else "long"
                fill_price = _get_post_fill_price(client, pid)
                _record_close_in_db(
                    telegram_id,
                    pid,
                    pos_size,
                    pos_size,
                    close_side,
                    client,
                    fill_price=fill_price,
                    network=selected_network or None,
                )
            else:
                errors.append(
                    f"{p.get('product_name', get_product_name(pid, network=selected_network or 'mainnet'))}: "
                    f"{r.get('error', 'unknown')}"
                )
        except Exception as e:
            errors.append(f"{p.get('product_name', 'unknown')}: {str(e)}")

    if cancelled == 0 and errors:
        all_errors = list(errors)
        if order_errors:
            all_errors.extend(order_errors)
        return {"success": False, "error": f"Failed to close positions: {'; '.join(all_errors)}"}

    result = {
        "success": True,
        "cancelled": cancelled,
        "products": list(products_closed),
    }
    if cancelled_orders:
        result["cancelled_orders"] = cancelled_orders
    if order_errors:
        result["order_errors"] = order_errors
    # Verify flatten succeeded before reporting success to callers that notify users.
    post_positions = _normalize_net_positions(client.get_all_positions() or [])
    if post_positions:
        result["success"] = False
        result["error"] = (
            "Close-all verification failed: open positions remain on "
            + ", ".join(get_product_name(pid, network=selected_network or "mainnet") for pid in post_positions.keys())
        )
    remaining_orders = 0
    for product_name in get_perp_products(network=selected_network or "mainnet", client=client):
        pid = get_product_id(product_name, network=selected_network or "mainnet", client=client)
        if pid is None:
            continue
        try:
            remaining_orders += len(client.get_open_orders(pid) or [])
        except Exception:
            continue
    if remaining_orders > 0:
        result["success"] = False
        existing_error = result.get("error", "")
        suffix = f"{remaining_orders} open orders remain."
        result["error"] = f"{existing_error} {suffix}".strip()
    return result


def get_trade_history(telegram_id: int, limit: int = 20) -> list:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    trades = get_trades_by_user(telegram_id, limit=limit, network=network)
    result = []
    for t in trades:
        entry = {
            "id": t.get("id"),
            "product": t.get("product_name"),
            "type": t.get("order_type"),
            "side": t.get("side"),
            "size": t.get("size"),
            "price": t.get("price"),
            "status": t.get("status"),
            "pnl": t.get("pnl"),
            "close_price": t.get("close_price"),
            "network": network,
            "created_at": t.get("created_at", "")[:19] if t.get("created_at") else "",
            "closed_at": t.get("closed_at", "")[:19] if t.get("closed_at") else "",
        }
        result.append(entry)
    return result


def get_trade_analytics(telegram_id: int) -> dict:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    trades = get_trades_by_user(telegram_id, limit=500, network=network)
    if not trades:
        return {"total_trades": 0}
    total = len(trades)
    filled = [t for t in trades if t.get("status") == TradeStatus.FILLED.value]
    closed = [t for t in trades if t.get("status") == TradeStatus.CLOSED.value]
    failed = [t for t in trades if t.get("status") == TradeStatus.FAILED.value]
    completed = filled + closed
    pnl_trades = [t for t in completed if t.get("pnl") is not None]
    total_pnl = sum(float(t["pnl"]) for t in pnl_trades) if pnl_trades else 0
    wins = len([t for t in pnl_trades if float(t["pnl"]) > 0])
    losses = len([t for t in pnl_trades if float(t["pnl"]) <= 0])
    win_rate = (wins / len(pnl_trades) * 100) if pnl_trades else 0
    total_volume = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in completed)

    by_product = {}
    for t in pnl_trades:
        product = (t.get("product_name") or "Unknown").replace("-PERP", "")
        if product not in by_product:
            by_product[product] = {"pnl": 0.0, "count": 0}
        by_product[product]["pnl"] += float(t["pnl"])
        by_product[product]["count"] += 1
    by_product = dict(sorted(by_product.items(), key=lambda x: abs(x[1]["pnl"]), reverse=True))

    return {
        "total_trades": total,
        "filled": len(filled),
        "closed": len(closed),
        "failed": len(failed),
        "total_pnl": total_pnl,
        "win_rate": win_rate,
        "wins": wins,
        "losses": losses,
        "total_volume": total_volume,
        "by_product": by_product,
    }
