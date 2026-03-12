import logging
import json
import os
import time
from datetime import datetime, timedelta
from src.nadobro.models.database import (
    TradeStatus, OrderSide, OrderTypeEnum, NetworkMode,
    get_last_trade_for_rate_limit, insert_trade, update_trade, get_trades_by_user,
    find_open_trade, get_pending_limit_trades,
)
from src.nadobro.config import (
    get_product_id,
    get_product_name,
    get_product_max_leverage,
    RATE_LIMIT_SECONDS,
    MIN_TRADE_SIZE_USD,
    PRODUCTS,
    EST_FEE_RATE,
)
from src.nadobro.services.user_service import get_user, get_user_nado_client, get_user_readonly_client, update_trade_stats, ensure_active_wallet_ready

logger = logging.getLogger(__name__)


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict):
    """Optional debug logging when NADO_DEBUG_LOG_PATH is set. No-op in production."""
    path = os.environ.get("NADO_DEBUG_LOG_PATH")
    if path:
        try:
            payload = {
                "runId": run_id,
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data,
                "timestamp": int(time.time() * 1000),
            }
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=True) + "\n")
        except Exception:
            pass


def _from_x18(raw) -> float:
    try:
        return float(raw) / 1e18
    except Exception:
        return 0.0


def _get_position_entry_price(client, product_id: int) -> float:
    """Fetch the position's entry (avg fill) price for product from the exchange. Returns 0 if unavailable."""
    try:
        positions = client.get_positions_only() or []
        for p in positions:
            if int(p.get("product_id", -1)) != int(product_id):
                continue
            price = float(p.get("price", 0) or 0)
            if price > 0:
                return price
    except Exception:
        pass
    return 0.0


def _fetch_close_execution_report(client, product_id: int, order_digest: str | None) -> dict:
    if not order_digest:
        return {}
    if not getattr(client, "subaccount_hex", None):
        return {}
    if not hasattr(client, "query_archive"):
        return {}

    digest = str(order_digest).lower()
    max_time = int(datetime.utcnow().timestamp())
    report = {"digest": digest, "filled_size": 0.0, "fill_price": 0.0, "realized_pnl": None}

    # Archive propagation can lag briefly after execution; probe a couple times.
    for wait_s in (0.0, 0.25):
        if wait_s > 0:
            time.sleep(wait_s)
        # Matches give actual executed base/quote so we can compute weighted fill price.
        try:
            match_payload = {
                "matches": {
                    "subaccounts": [client.subaccount_hex],
                    "max_time": max_time,
                    "limit": 300,
                    "isolated": False,
                }
            }
            m_resp = client.query_archive(match_payload) or {}
            matches = m_resp.get("matches") or []
            rel_matches = []
            for m in matches:
                try:
                    if int(m.get("product_id")) != int(product_id):
                        continue
                except Exception:
                    continue
                m_digest = str(m.get("digest", "")).lower()
                if m_digest == digest:
                    rel_matches.append(m)
            base_total = sum(abs(_from_x18(m.get("base_filled", 0))) for m in rel_matches)
            quote_total = sum(abs(_from_x18(m.get("quote_filled", 0))) for m in rel_matches)
            if base_total > 0:
                report["filled_size"] = float(base_total)
                report["fill_price"] = float(quote_total / base_total)
        except Exception:
            pass

        # Orders can expose exchange-realized pnl directly.
        try:
            order_payload = {
                "orders": {
                    "subaccounts": [client.subaccount_hex],
                    "max_time": max_time,
                    "limit": 300,
                    "isolated": False,
                }
            }
            o_resp = client.query_archive(order_payload) or {}
            orders = o_resp.get("orders") or []
            rel_orders = []
            for o in orders:
                try:
                    if int(o.get("product_id")) != int(product_id):
                        continue
                except Exception:
                    continue
                o_digest = str(o.get("digest", "")).lower()
                if o_digest == digest:
                    rel_orders.append(o)
            if rel_orders:
                realized = sum(_from_x18(o.get("realized_pnl", 0)) for o in rel_orders)
                report["realized_pnl"] = float(realized)
        except Exception:
            pass

        if report.get("fill_price", 0) > 0 or report.get("filled_size", 0) > 0:
            break

    return report


def get_last_fill_price(client, product_id: int) -> float:
    """Fetch the most recent fill price for product from archive matches. Returns 0 if unavailable."""
    if not getattr(client, "subaccount_hex", None) or not hasattr(client, "query_archive"):
        return 0.0
    try:
        payload = {
            "matches": {
                "subaccounts": [client.subaccount_hex],
                "max_time": int(datetime.utcnow().timestamp()),
                "limit": 50,
                "isolated": False,
            }
        }
        resp = client.query_archive(payload) or {}
        matches = resp.get("matches") or []
        for m in matches:
            try:
                if int(m.get("product_id")) != int(product_id):
                    continue
            except Exception:
                continue
            base = abs(_from_x18(m.get("base_filled", 0)))
            quote = abs(_from_x18(m.get("quote_filled", 0)))
            if base > 0:
                return float(quote / base)
        return 0.0
    except Exception:
        return 0.0


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


def reconcile_pending_limit_trades(telegram_id: int, client) -> None:
    """
    Sync DB pending limit trades with exchange state. Orders no longer on exchange
    are checked for fills via archive; if filled update to filled, else to cancelled.
    """
    if not client or not getattr(client, "subaccount_hex", None):
        return
    pending = get_pending_limit_trades(telegram_id)
    if not pending:
        return
    product_ids = {int(t["product_id"]) for t in pending}
    for product_id in product_ids:
        try:
            open_orders = client.get_open_orders(product_id) or []
        except Exception as e:
            logger.warning("reconcile_pending_limit_trades: get_open_orders failed product_id=%s: %s", product_id, e)
            continue
        open_digests = {str(o.get("digest", "")).strip().lower() for o in open_orders if o.get("digest")}
        for t in pending:
            if int(t.get("product_id", -1)) != product_id:
                continue
            digest = str(t.get("order_digest") or "").strip().lower()
            if not digest or digest in open_digests:
                continue
            report = _fetch_close_execution_report(client, product_id, digest)
            filled_size = float(report.get("filled_size") or 0.0)
            fill_price = float(report.get("fill_price") or 0.0)
            trade_id = t.get("id")
            if not trade_id:
                continue
            if filled_size > 0:
                exec_price = fill_price if fill_price > 0 else float(t.get("price") or 0)
                update_trade(trade_id, {
                    "status": TradeStatus.FILLED.value,
                    "price": exec_price,
                    "filled_at": datetime.utcnow().isoformat(),
                })
                logger.info("Reconciled trade #%d: filled (size=%.4f)", trade_id, filled_size)
                update_trade_stats(telegram_id, filled_size * exec_price)
            else:
                update_trade(trade_id, {"status": TradeStatus.CANCELLED.value})
                logger.info("Reconciled trade #%d: cancelled (expired or cancelled on exchange)", trade_id)


def check_rate_limit(telegram_id: int) -> tuple[bool, str]:
    last_trade = get_last_trade_for_rate_limit(telegram_id)
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
    product_id = get_product_id(product)
    if product_id is None:
        from src.nadobro.config import PRODUCTS as _P
        available = ", ".join([n for n, i in _P.items() if i["type"] == "perp"])
        return False, f"Unknown product '{product}'. Available: {available}"

    if size <= 0:
        return False, "Trade size must be positive."

    max_leverage = get_product_max_leverage(product)
    if leverage > max_leverage:
        return False, f"Max leverage for {product.upper()} is {max_leverage}x."

    if leverage < 1:
        return False, "Leverage must be at least 1x."

    wallet_ok, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ok:
        return False, wallet_msg

    client = get_user_readonly_client(telegram_id)
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
        allowed, msg = check_rate_limit(telegram_id)
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
    passphrase: str = None,
    tp_price: float = None,
    sl_price: float = None,
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

    product_id = get_product_id(product)
    client = get_user_nado_client(telegram_id, passphrase=passphrase)
    user = get_user(telegram_id)
    if not client:
        return {"success": False, "error": "Invalid passphrase or wallet not initialized. Please try again."}

    trade_id = insert_trade({
        "user_id": telegram_id,
        "product_id": product_id,
        "product_name": get_product_name(product_id),
        "order_type": OrderTypeEnum.MARKET.value,
        "side": OrderSide.LONG.value if is_long else OrderSide.SHORT.value,
        "size": size,
        "leverage": leverage,
        "status": TradeStatus.PENDING.value,
        "network": user.network_mode.value,
        "created_at": datetime.utcnow().isoformat(),
    })
    if not trade_id:
        return {"success": False, "error": "Failed to record trade."}

    result = client.place_market_order(product_id, size, is_buy=is_long, slippage_pct=slippage_pct)
    execution_report = {}
    executed_size = float(size)
    executed_price = 0.0

    if result["success"]:
        execution_report = _fetch_close_execution_report(client, product_id, result.get("digest"))
        filled_size = float(execution_report.get("filled_size") or 0.0)
        fill_price = float(execution_report.get("fill_price") or 0.0)
        executed_size = filled_size if filled_size > 0 else float(size)
        # Prefer archive fill_price; fallback to position entry (exchange source of truth), then IOC limit/mid
        if fill_price <= 0:
            time.sleep(0.3)  # Let exchange update position before we fetch entry price
            position_entry = _get_position_entry_price(client, product_id)
        else:
            position_entry = 0.0
        executed_price = (
            fill_price
            if fill_price > 0
            else (position_entry if position_entry > 0 else float(result.get("price") or client.get_market_price(product_id)["mid"] or 0.0))
        )
        update_trade(trade_id, {
            "status": TradeStatus.FILLED.value,
            "order_digest": result.get("digest"),
            "price": executed_price,
            "filled_at": datetime.utcnow().isoformat(),
        })
    else:
        update_trade(trade_id, {
            "status": TradeStatus.FAILED.value,
            "error_message": result.get("error", "Unknown error"),
        })

    if result["success"]:
        traded_notional = executed_size * executed_price if executed_price > 0 else 0.0
        if traded_notional > 0:
            update_trade_stats(telegram_id, traded_notional)
        payload = {
            "success": True,
            "side": "LONG" if is_long else "SHORT",
            "size": executed_size,
            "product": get_product_name(product_id),
            "price": executed_price,
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
            network=user.network_mode.value,
            product=get_product_name(product_id),
            is_long=is_long,
            stop_price=sl_price,
            size=size,
            client=client,
        )
        if sl_result:
            payload.update(sl_result)
        return payload

    return result


def execute_limit_order(
    telegram_id: int,
    product: str,
    size: float,
    price: float,
    is_long: bool,
    leverage: float = 1.0,
    enforce_rate_limit: bool = True,
    passphrase: str = None,
    tp_price: float = None,
    sl_price: float = None,
    reduce_only: bool = False,
) -> dict:
    product_id = get_product_id(product)
    if product_id is None:
        from src.nadobro.config import PRODUCTS as _P
        available = ", ".join([n for n, i in _P.items() if i["type"] == "perp"])
        return {"success": False, "error": f"Unknown product '{product}'. Available: {available}"}
    if float(size or 0) <= 0:
        return {"success": False, "error": "Trade size must be positive."}

    if not reduce_only:
        valid, msg = validate_trade(
            telegram_id,
            product,
            size,
            leverage,
            enforce_rate_limit=enforce_rate_limit,
        )
        if not valid:
            return {"success": False, "error": msg}
    else:
        wallet_ok, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ok:
            return {"success": False, "error": wallet_msg}

    client = get_user_nado_client(telegram_id, passphrase=passphrase)
    user = get_user(telegram_id)
    if not client:
        return {"success": False, "error": "Invalid passphrase or wallet not initialized. Please try again."}

    trade_id = insert_trade({
        "user_id": telegram_id,
        "product_id": product_id,
        "product_name": get_product_name(product_id),
        "order_type": OrderTypeEnum.LIMIT.value,
        "side": OrderSide.LONG.value if is_long else OrderSide.SHORT.value,
        "size": size,
        "price": price,
        "leverage": leverage,
        "status": TradeStatus.PENDING.value,
        "network": user.network_mode.value,
        "created_at": datetime.utcnow().isoformat(),
    })
    if not trade_id:
        return {"success": False, "error": "Failed to record trade."}

    result = client.place_limit_order(
        product_id,
        size,
        price,
        is_buy=is_long,
        reduce_only=reduce_only,
    )

    if result["success"]:
        update_trade(
            trade_id,
            {
                "status": TradeStatus.PENDING.value,
                "order_digest": result.get("digest"),
            },
        )
    else:
        update_trade(trade_id, {"status": TradeStatus.FAILED.value, "error_message": result.get("error", "Unknown error")})

    if result["success"]:
        payload = {
            "success": True,
            "side": "LONG" if is_long else "SHORT",
            "size": size,
            "product": get_product_name(product_id),
            "price": price,
            "digest": result.get("digest"),
            "network": user.network_mode.value,
            "type": "LIMIT",
            "filled": False,
            "reduce_only": bool(reduce_only),
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
            network=user.network_mode.value,
            product=get_product_name(product_id),
            is_long=is_long,
            stop_price=sl_price,
            size=size,
            client=client,
        )
        if sl_result:
            payload.update(sl_result)
        return payload

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

    # Prefer native trigger TP (server-side), fallback to plain reduce-only limit order.
    trigger_when = "oracle_price_above" if is_long else "oracle_price_below"
    trigger_result = client.place_price_trigger_order(
        product_id=product_id,
        size=float(size),
        trigger_price=tp,
        order_price=tp,
        is_buy=(not is_long),
        trigger_when=trigger_when,
        reduce_only=True,
        order_type="default",
    )
    if trigger_result.get("success"):
        return {
            "tp_requested": True,
            "tp_set": True,
            "tp_price": tp,
            "tp_digest": trigger_result.get("digest"),
            "tp_mode": "trigger",
        }

    tp_result = client.place_limit_order(
        product_id,
        float(size),
        tp,
        is_buy=(not is_long),
        reduce_only=True,
    )
    if tp_result.get("success"):
        return {
            "tp_requested": True,
            "tp_set": True,
            "tp_price": tp,
            "tp_digest": tp_result.get("digest"),
            "tp_mode": "limit_fallback",
        }
    return {
        "tp_requested": True,
        "tp_set": False,
        "tp_price": tp,
        "tp_error": trigger_result.get("error") or tp_result.get("error", "Failed to place TP order."),
    }


def _arm_stop_loss_rule(
    telegram_id: int,
    network: str,
    product: str,
    is_long: bool,
    stop_price: float | None,
    size: float,
    client=None,
) -> dict:
    if stop_price is None:
        return {}
    from src.nadobro.services.stop_loss_service import register_stop_loss_rule
    from src.nadobro.config import get_product_id

    stop_price_f = float(stop_price)
    product_base = str(product or "").replace("-PERP", "").upper()
    product_id = get_product_id(product_base)
    if client and product_id is not None and size > 0 and stop_price_f > 0:
        trigger_when = "oracle_price_below" if is_long else "oracle_price_above"
        # Use IOC with conservative stop-to-limit conversion for higher execution certainty.
        order_price = stop_price_f * (0.995 if is_long else 1.005)
        trigger_result = client.place_price_trigger_order(
            product_id=product_id,
            size=float(size),
            trigger_price=stop_price_f,
            order_price=order_price,
            is_buy=(not is_long),
            trigger_when=trigger_when,
            reduce_only=True,
            order_type="ioc",
        )
        if trigger_result.get("success"):
            return {
                "sl_requested": True,
                "sl_armed": True,
                "sl_price": stop_price_f,
                "sl_rule_id": trigger_result.get("digest") or trigger_result.get("request_id"),
                "sl_mode": "trigger",
            }

    side = "LONG" if is_long else "SHORT"
    result = register_stop_loss_rule(
        telegram_id=telegram_id,
        network=network,
        product=product,
        side=side,
        stop_price=stop_price_f,
        size=size,
    )
    if result.get("success"):
        return {
            "sl_requested": True,
            "sl_armed": True,
            "sl_price": float(result.get("stop_price") or 0),
            "sl_rule_id": result.get("rule_id"),
            "sl_mode": "bot_fallback",
        }
    return {
        "sl_requested": True,
        "sl_armed": False,
        "sl_price": stop_price_f,
        "sl_error": result.get("error", "Failed to arm stop-loss."),
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
            if p.get("is_limit_order"):
                continue  # Resting limit orders are not positions
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


def _record_close_in_db(
    telegram_id: int,
    product_id: int,
    close_size: float,
    pos_size: float,
    side: str,
    client,
    fill_price: float = None,
    realized_pnl_override: float | None = None,
):
    try:
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"

        close_price = fill_price or 0.0
        close_price_source = "fill_price" if close_price else "market_mid_fallback"
        if not close_price:
            try:
                price_data = client.get_market_price(product_id)
                close_price = float(price_data.get("mid", 0) or 0)
            except Exception:
                pass

        open_trade = find_open_trade(telegram_id, product_id, network=network)
        is_full_close = close_size >= pos_size
        # region agent log
        _debug_log(
            run_id="pre-fix-1",
            hypothesis_id="H2",
            location="trade_service.py:_record_close_in_db:pre_pnl",
            message="Close price selection and open trade chosen for DB close update",
            data={
                "product_id": product_id,
                "network": network,
                "close_size": float(close_size or 0),
                "position_size": float(pos_size or 0),
                "close_price_source": close_price_source,
                "input_fill_price": float(fill_price or 0),
                "selected_close_price": float(close_price or 0),
                "open_trade_id": open_trade.get("id") if open_trade else None,
                "open_trade_side": open_trade.get("side") if open_trade else None,
                "open_trade_price": float((open_trade or {}).get("price") or 0),
                "open_trade_status": (open_trade or {}).get("status"),
                "is_full_close": bool(is_full_close),
            },
        )
        # endregion

        if open_trade:
            open_price = float(open_trade.get("price") or 0)
            open_size = float(open_trade.get("size") or 0)
            open_side = open_trade.get("side", "")
            pnl = 0.0
            if realized_pnl_override is not None:
                pnl = float(realized_pnl_override)
            elif open_price > 0 and close_price > 0:
                if open_side == "long":
                    pnl = (close_price - open_price) * close_size
                elif open_side == "short":
                    pnl = (open_price - close_price) * close_size
            open_notional = open_size * open_price if open_size and open_price else 0
            close_notional = close_size * close_price if close_size and close_price else 0
            fees = round((open_notional + close_notional) * EST_FEE_RATE, 6)
            # region agent log
            _debug_log(
                run_id="pre-fix-1",
                hypothesis_id="H3",
                location="trade_service.py:_record_close_in_db:pnl_calc",
                message="Computed close PnL from open trade side and selected close price",
                data={
                    "product_id": product_id,
                    "open_trade_id": open_trade.get("id"),
                    "open_side": open_side,
                    "open_price": float(open_price or 0),
                    "close_price": float(close_price or 0),
                    "close_size": float(close_size or 0),
                    "computed_pnl": float(pnl or 0),
                },
            )
            # endregion

            if is_full_close:
                update_trade(open_trade["id"], {
                    "status": TradeStatus.CLOSED.value,
                    "close_price": close_price,
                    "closed_at": datetime.utcnow().isoformat(),
                    "pnl": round(pnl, 4),
                    "fees": fees,
                })
                logger.info(
                    "Trade #%d fully closed: %s %s size=%.4f open=%.2f close=%.2f pnl=%.4f",
                    open_trade["id"], open_side, get_product_name(product_id),
                    close_size, open_price, close_price, pnl,
                )
            else:
                update_trade(open_trade["id"], {
                    "pnl": round(pnl, 4),
                    "fees": fees,
                    "close_price": close_price,
                })
                logger.info(
                    "Trade #%d partially closed: %s %s closed=%.4f/%.4f open=%.2f close=%.2f pnl=%.4f",
                    open_trade["id"], open_side, get_product_name(product_id),
                    close_size, pos_size, open_price, close_price, pnl,
                )
        else:
            close_notional = close_size * close_price if close_size and close_price else 0
            fees = round(close_notional * EST_FEE_RATE, 6)
            insert_data = {
                "user_id": telegram_id,
                "product_id": product_id,
                "product_name": get_product_name(product_id),
                "order_type": "market",
                "side": side,
                "size": close_size,
                "price": close_price,
                "leverage": 1.0,
                "status": TradeStatus.CLOSED.value,
                "network": network,
                "close_price": close_price,
                "closed_at": datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
                "filled_at": datetime.utcnow().isoformat(),
                "fees": fees,
            }
            if realized_pnl_override is not None:
                insert_data["pnl"] = round(float(realized_pnl_override), 4)
            insert_trade(insert_data)
            logger.info(
                "Close trade recorded (no matching open): %s %s size=%.4f price=%.2f",
                side, get_product_name(product_id), close_size, close_price,
            )
    except Exception as e:
        logger.warning("Failed to record close in DB: %s", e)


def close_position(telegram_id: int, product: str, size: float = None, passphrase: str = None) -> dict:
    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'."}

    client = get_user_nado_client(telegram_id, passphrase=passphrase)
    if not client:
        return {"success": False, "error": "Invalid passphrase or wallet not initialized."}
    cancelled_orders, order_errors = _cancel_open_orders_for_product(client, product_id)

    net_positions = _normalize_net_positions(client.get_all_positions() or [])
    product_pos = net_positions.get(product_id)
    if not product_pos:
        if cancelled_orders > 0:
            return {
                "success": True,
                "cancelled": 0.0,
                "product": get_product_name(product_id),
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
    # region agent log
    _debug_log(
        run_id="pre-fix-1",
        hypothesis_id="H4",
        location="trade_service.py:close_position:pre_close_loop",
        message="Close request normalized from current net position",
        data={
            "product_id": product_id,
            "requested_size": float(size or 0) if size is not None else None,
            "net_signed_amount": float(signed_amount or 0),
            "position_size": float(pos_size or 0),
            "close_size": float(close_size or 0),
            "close_side": close_side,
            "full_close_requested": bool(full_close_requested),
        },
    )
    # endregion

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
        r = client.place_market_order(product_id, this_close_size, is_buy=is_buy, slippage_pct=1.0)
        if not r.get("success"):
            return {"success": False, "error": f"Failed to close position: {r.get('error', 'unknown')}"}
        fill_price = float(r.get("price") or 0) if r.get("price") else None
        execution_report = _fetch_close_execution_report(client, product_id, r.get("digest"))
        latest_after_positions = _normalize_net_positions(client.get_all_positions() or [])
        latest_after_pos = latest_after_positions.get(product_id)
        latest_after_abs = abs(float((latest_after_pos or {}).get("signed_amount", 0) or 0))
        actual_reduction = max(0.0, latest_abs - latest_after_abs)
        reported_filled = float(execution_report.get("filled_size") or 0.0)
        executed_size = reported_filled if reported_filled > 0 else actual_reduction
        effective_fill_price = (
            float(execution_report.get("fill_price") or 0.0)
            if float(execution_report.get("fill_price") or 0.0) > 0
            else fill_price
        )
        realized_pnl = execution_report.get("realized_pnl")
        # region agent log
        _debug_log(
            run_id="pre-fix-1",
            hypothesis_id="H1",
            location="trade_service.py:close_position:post_place_market_order",
            message="Close loop received market order response before DB update",
            data={
                "product_id": product_id,
                "attempt": attempts,
                "is_buy": bool(is_buy),
                "this_close_size": float(this_close_size or 0),
                "remaining_size_before_decrement": float(remaining_size or 0),
                "result_success": bool(r.get("success")),
                "result_price": float(r.get("price", 0) or 0),
                "result_digest": str(r.get("digest", ""))[:24],
                "latest_abs_before": float(latest_abs or 0),
                "latest_abs_after": float(latest_after_abs or 0),
                "actual_position_reduction": float(actual_reduction or 0),
                "archive_reported_filled": float(reported_filled or 0),
                "executed_size_used": float(executed_size or 0),
                "archive_fill_price": float(execution_report.get("fill_price") or 0),
                "effective_fill_price_used": float(effective_fill_price or 0),
                "archive_realized_pnl": float(realized_pnl or 0) if realized_pnl is not None else None,
            },
        )
        # endregion
        # region agent log
        _debug_log(
            run_id="pre-fix-1",
            hypothesis_id="H6",
            location="trade_service.py:close_position:fill_vs_requested",
            message="Compared requested close size versus actual position delta after exchange response",
            data={
                "product_id": product_id,
                "requested_close_size": float(this_close_size or 0),
                "actual_position_reduction": float(actual_reduction or 0),
                "archive_reported_filled": float(reported_filled or 0),
                "executed_size_used": float(executed_size or 0),
                "latest_abs_before": float(latest_abs or 0),
                "latest_abs_after": float(latest_after_abs or 0),
                "partial_fill_detected": bool((executed_size + 1e-12) < float(this_close_size or 0)),
            },
        )
        # endregion
        if executed_size <= 0:
            return {"success": False, "error": f"Close verification failed for {product}: no executed size detected."}
        _record_close_in_db(
            telegram_id,
            product_id,
            executed_size,
            latest_abs,
            close_side,
            client,
            fill_price=effective_fill_price,
            realized_pnl_override=realized_pnl,
        )
        remaining_size -= executed_size

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
        "product": get_product_name(product_id),
    }
    if cancelled_orders:
        payload["cancelled_orders"] = cancelled_orders
    if order_errors:
        payload["order_errors"] = order_errors
    return payload


def close_all_positions(telegram_id: int, passphrase: str = None) -> dict:
    client = get_user_nado_client(telegram_id, passphrase=passphrase)
    if not client:
        return {"success": False, "error": "Invalid passphrase or wallet not initialized."}

    cancelled_orders = 0
    order_errors = []
    # Always cancel stale open orders first so strategy stop leaves no resting orders.
    for _, info in PRODUCTS.items():
        if info.get("type") != "perp":
            continue
        pid = info.get("id")
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
        return {"success": False, "error": "No open positions found."}

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
            r = client.place_market_order(pid, pos_size, is_buy=is_buy, slippage_pct=1.0)
            if r["success"]:
                execution_report = _fetch_close_execution_report(client, pid, r.get("digest"))
                reported_size = float(execution_report.get("filled_size") or 0.0)
                executed_size = reported_size if reported_size > 0 else pos_size
                cancelled += executed_size
                product_name = p.get("product_name", get_product_name(pid))
                products_closed.add(product_name)
                close_side = "short" if signed_amount > 0 else "long"
                execution_fill = float(execution_report.get("fill_price") or 0.0)
                fallback_fill = float(r.get("price") or 0) if r.get("price") else 0.0
                fill_price = execution_fill if execution_fill > 0 else (fallback_fill if fallback_fill > 0 else None)
                realized_pnl = execution_report.get("realized_pnl")
                _record_close_in_db(
                    telegram_id, pid, executed_size, pos_size, close_side, client,
                    fill_price=fill_price,
                    realized_pnl_override=float(realized_pnl) if realized_pnl is not None else None,
                )
            else:
                errors.append(f"{p.get('product_name', get_product_name(pid))}: {r.get('error', 'unknown')}")
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
            + ", ".join(get_product_name(pid) for pid in post_positions.keys())
        )
    remaining_orders = 0
    for _, info in PRODUCTS.items():
        if info.get("type") != "perp":
            continue
        try:
            remaining_orders += len(client.get_open_orders(info.get("id")) or [])
        except Exception:
            continue
    if remaining_orders > 0:
        result["success"] = False
        existing_error = result.get("error", "")
        suffix = f"{remaining_orders} open orders remain."
        result["error"] = f"{existing_error} {suffix}".strip()
    return result


def get_trade_history(telegram_id: int, limit: int = 20) -> list:
    trades = get_trades_by_user(telegram_id, limit=limit)
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
            "fees": t.get("fees"),
            "close_price": t.get("close_price"),
            "network": t.get("network"),
            "created_at": t.get("created_at", "")[:19] if t.get("created_at") else "",
            "closed_at": t.get("closed_at", "")[:19] if t.get("closed_at") else "",
        }
        result.append(entry)
    return result


def get_trade_analytics(telegram_id: int) -> dict:
    trades = get_trades_by_user(telegram_id, limit=500)
    if not trades:
        return {"total_trades": 0}
    total = len(trades)
    filled = [t for t in trades if t.get("status") == TradeStatus.FILLED.value]
    closed = [t for t in trades if t.get("status") == TradeStatus.CLOSED.value]
    failed = [t for t in trades if t.get("status") == TradeStatus.FAILED.value]
    completed = filled + closed
    pnl_trades = [t for t in completed if t.get("pnl") is not None]
    total_pnl = sum(float(t["pnl"]) for t in pnl_trades) if pnl_trades else 0
    total_fees = sum(float(t.get("fees") or 0) for t in completed)
    wins = len([t for t in pnl_trades if float(t["pnl"]) > 0])
    losses = len([t for t in pnl_trades if float(t["pnl"]) <= 0])
    win_rate = (wins / len(pnl_trades) * 100) if pnl_trades else 0
    total_volume = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in completed)
    return {
        "total_trades": total,
        "filled": len(filled),
        "closed": len(closed),
        "failed": len(failed),
        "total_pnl": total_pnl,
        "total_fees": total_fees,
        "win_rate": win_rate,
        "wins": wins,
        "losses": losses,
        "total_volume": total_volume,
    }
