import logging
from datetime import datetime, timedelta
from src.nadobro.models.database import (
    Trade, TradeStatus, OrderSide, OrderTypeEnum, NetworkMode,
    get_session,
)
from src.nadobro.config import get_product_id, get_product_name, RATE_LIMIT_SECONDS, MAX_LEVERAGE, MIN_TRADE_SIZE_USD
from src.nadobro.services.user_service import get_user, get_user_nado_client, update_trade_stats

logger = logging.getLogger(__name__)


def check_rate_limit(telegram_id: int) -> tuple[bool, str]:
    with get_session() as session:
        last_trade = (
            session.query(Trade)
            .filter_by(user_id=telegram_id)
            .filter(Trade.status.in_([TradeStatus.PENDING, TradeStatus.FILLED]))
            .order_by(Trade.created_at.desc())
            .first()
        )
        if last_trade:
            elapsed = (datetime.utcnow() - last_trade.created_at).total_seconds()
            if elapsed < RATE_LIMIT_SECONDS:
                remaining = int(RATE_LIMIT_SECONDS - elapsed)
                return False, f"Rate limit: wait {remaining}s before next trade."
    return True, ""


def validate_trade(telegram_id: int, product: str, size: float, leverage: float = 1.0) -> tuple[bool, str]:
    product_id = get_product_id(product)
    if product_id is None:
        available = ", ".join(["BTC", "ETH", "SOL", "ARB", "OP", "DOGE", "LINK", "AVAX"])
        return False, f"Unknown product '{product}'. Available: {available}"

    if size <= 0:
        return False, "Trade size must be positive."

    if leverage > MAX_LEVERAGE:
        return False, f"Max leverage is {MAX_LEVERAGE}x."

    if leverage < 1:
        return False, "Leverage must be at least 1x."

    client = get_user_nado_client(telegram_id)
    if not client:
        return False, "Wallet not initialized. Use /start first."

    balance = client.get_balance()
    if not balance.get("exists"):
        return False, "Subaccount not found. Please deposit funds first on Nado."

    usdt_balance = balance.get("balances", {}).get(0, 0)

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

    allowed, msg = check_rate_limit(telegram_id)
    if not allowed:
        return False, msg

    return True, ""


def execute_market_order(telegram_id: int, product: str, size: float, is_long: bool, leverage: float = 1.0) -> dict:
    valid, msg = validate_trade(telegram_id, product, size, leverage)
    if not valid:
        return {"success": False, "error": msg}

    product_id = get_product_id(product)
    client = get_user_nado_client(telegram_id)
    user = get_user(telegram_id)

    with get_session() as session:
        trade = Trade(
            user_id=telegram_id,
            product_id=product_id,
            product_name=get_product_name(product_id),
            order_type=OrderTypeEnum.MARKET,
            side=OrderSide.LONG if is_long else OrderSide.SHORT,
            size=size,
            leverage=leverage,
            status=TradeStatus.PENDING,
            network=user.network_mode,
        )
        session.add(trade)
        session.commit()
        trade_id = trade.id

    result = client.place_market_order(product_id, size, is_buy=is_long)

    with get_session() as session:
        trade = session.query(Trade).get(trade_id)
        if result["success"]:
            trade.status = TradeStatus.FILLED
            trade.order_digest = result.get("digest")
            trade.price = result.get("price") or client.get_market_price(product_id)["mid"]
            trade.filled_at = datetime.utcnow()
        else:
            trade.status = TradeStatus.FAILED
            trade.error_message = result.get("error", "Unknown error")
        session.commit()

    if result["success"]:
        mp = client.get_market_price(product_id)
        update_trade_stats(telegram_id, size * mp["mid"])
        return {
            "success": True,
            "side": "LONG" if is_long else "SHORT",
            "size": size,
            "product": get_product_name(product_id),
            "price": mp["mid"],
            "digest": result.get("digest"),
            "network": user.network_mode.value,
        }

    return result


def execute_limit_order(telegram_id: int, product: str, size: float, price: float, is_long: bool, leverage: float = 1.0) -> dict:
    valid, msg = validate_trade(telegram_id, product, size, leverage)
    if not valid:
        return {"success": False, "error": msg}

    product_id = get_product_id(product)
    client = get_user_nado_client(telegram_id)
    user = get_user(telegram_id)

    with get_session() as session:
        trade = Trade(
            user_id=telegram_id,
            product_id=product_id,
            product_name=get_product_name(product_id),
            order_type=OrderTypeEnum.LIMIT,
            side=OrderSide.LONG if is_long else OrderSide.SHORT,
            size=size,
            price=price,
            leverage=leverage,
            status=TradeStatus.PENDING,
            network=user.network_mode,
        )
        session.add(trade)
        session.commit()
        trade_id = trade.id

    result = client.place_limit_order(product_id, size, price, is_buy=is_long)

    with get_session() as session:
        trade = session.query(Trade).get(trade_id)
        if result["success"]:
            trade.status = TradeStatus.FILLED
            trade.order_digest = result.get("digest")
            trade.filled_at = datetime.utcnow()
        else:
            trade.status = TradeStatus.FAILED
            trade.error_message = result.get("error", "Unknown error")
        session.commit()

    if result["success"]:
        update_trade_stats(telegram_id, size * price)
        return {
            "success": True,
            "side": "LONG" if is_long else "SHORT",
            "size": size,
            "product": get_product_name(product_id),
            "price": price,
            "digest": result.get("digest"),
            "network": user.network_mode.value,
            "type": "LIMIT",
        }

    return result


def close_position(telegram_id: int, product: str, size: float = None) -> dict:
    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'."}

    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized."}

    orders = client.get_open_orders(product_id)
    if not orders:
        return {"success": False, "error": f"No open positions on {product}."}

    cancelled = 0
    for o in orders:
        r = client.cancel_order(product_id, o["digest"])
        if r["success"]:
            cancelled += 1

    return {
        "success": True,
        "cancelled": cancelled,
        "product": get_product_name(product_id),
    }


def close_all_positions(telegram_id: int) -> dict:
    client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet not initialized."}

    positions = client.get_all_positions()
    if not positions:
        return {"success": False, "error": "No open positions found."}

    cancelled = 0
    errors = []
    products_closed = set()
    for p in positions:
        try:
            r = client.cancel_order(p["product_id"], p["digest"])
            if r["success"]:
                cancelled += 1
                products_closed.add(p["product_name"])
            else:
                errors.append(f"{p['product_name']}: {r.get('error', 'unknown')}")
        except Exception as e:
            errors.append(f"{p.get('product_name', 'unknown')}: {str(e)}")

    if cancelled == 0 and errors:
        return {"success": False, "error": f"Failed to close positions: {'; '.join(errors)}"}

    return {
        "success": True,
        "cancelled": cancelled,
        "products": list(products_closed),
    }


def get_trade_history(telegram_id: int, limit: int = 20) -> list:
    with get_session() as session:
        trades = (
            session.query(Trade)
            .filter_by(user_id=telegram_id)
            .order_by(Trade.created_at.desc())
            .limit(limit)
            .all()
        )
        result = []
        for t in trades:
            result.append({
                "id": t.id,
                "product": t.product_name,
                "type": t.order_type.value,
                "side": t.side.value,
                "size": t.size,
                "price": t.price,
                "status": t.status.value,
                "pnl": t.pnl,
                "network": t.network.value,
                "created_at": t.created_at.isoformat(),
            })
        return result


def get_trade_analytics(telegram_id: int) -> dict:
    with get_session() as session:
        trades = session.query(Trade).filter_by(user_id=telegram_id).all()
        if not trades:
            return {"total_trades": 0}

        total = len(trades)
        filled = [t for t in trades if t.status == TradeStatus.FILLED]
        failed = [t for t in trades if t.status == TradeStatus.FAILED]

        pnl_trades = [t for t in filled if t.pnl is not None]
        total_pnl = sum(t.pnl for t in pnl_trades) if pnl_trades else 0
        wins = len([t for t in pnl_trades if t.pnl > 0])
        losses = len([t for t in pnl_trades if t.pnl <= 0])
        win_rate = (wins / len(pnl_trades) * 100) if pnl_trades else 0

        total_volume = sum(t.size * (t.price or 0) for t in filled)

        return {
            "total_trades": total,
            "filled": len(filled),
            "failed": len(failed),
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "wins": wins,
            "losses": losses,
            "total_volume": total_volume,
        }
