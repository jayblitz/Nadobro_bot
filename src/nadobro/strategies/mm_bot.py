"""
MM Bot (Grid + MM) — places symmetric limit orders around mid price.
MM mode: tight spread, 1-2 levels each side.
Grid mode: wider spacing, 3-5 levels each side.
Cancels stale orders that drifted beyond threshold from current mid.
"""
import logging
from src.nadobro.config import get_product_id
from src.nadobro.services.trade_service import execute_limit_order

logger = logging.getLogger(__name__)

STALE_DRIFT_MULTIPLIER = 3.0
MM_DEFAULT_LEVELS = 2
GRID_DEFAULT_LEVELS = 4
MM_MIN_SPREAD_BP = 2.0
GRID_MIN_SPREAD_BP = 8.0


def _compute_grid_prices(
    mid: float,
    spread_bp: float,
    levels: int,
    strategy: str,
    min_range_pct: float = 1.0,
    max_range_pct: float = 1.0,
) -> list[dict]:
    half_spread = spread_bp / 10000.0
    orders = []
    if strategy == "grid":
        min_off = max(0.0001, min_range_pct / 100.0)
        max_off = max(min_off, max_range_pct / 100.0)
        step = (max_off - min_off) / max(levels - 1, 1)
        for i in range(levels):
            offset = min_off + (step * i)
            buy_price = mid * (1.0 - offset)
            sell_price = mid * (1.0 + offset)
            lvl = i + 1
            orders.append({"price": buy_price, "is_long": True, "level": lvl})
            orders.append({"price": sell_price, "is_long": False, "level": lvl})
        return orders
    for i in range(1, levels + 1):
        offset = half_spread * i
        buy_price = mid * (1.0 - offset)
        sell_price = mid * (1.0 + offset)
        orders.append({"price": buy_price, "is_long": True, "level": i})
        orders.append({"price": sell_price, "is_long": False, "level": i})
    return orders


def _cancel_stale_orders(client, product_id: int, mid: float, spread_bp: float, levels: int, open_orders: list, close_offset_bp: float = 0.0) -> int:
    if not open_orders:
        return 0
    max_offset = (spread_bp / 10000.0) * levels * STALE_DRIFT_MULTIPLIER
    if close_offset_bp > 0:
        max_offset = max(max_offset, close_offset_bp / 10000.0)
    cancelled = 0
    for order in open_orders:
        order_price = float(order.get("price", 0))
        if order_price <= 0:
            continue
        drift = abs(order_price - mid) / mid
        if drift > max_offset:
            digest = order.get("digest")
            if digest:
                result = client.cancel_order(product_id, digest)
                if result.get("success"):
                    cancelled += 1
                    logger.info("Cancelled stale order %s (drift %.4f%% > threshold %.4f%%)",
                                digest[:12], drift * 100, max_offset * 100)
    return cancelled


def run_cycle(
    telegram_id: int,
    network: str,
    state: dict,
    client=None,
    mid: float = 0.0,
    open_orders: list = None,
    passphrase: str = None,
) -> dict:
    product = state.get("product", "BTC")
    strategy = state.get("strategy", "mm")
    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Invalid product '{product}'", "orders_placed": 0}

    if client is None:
        from src.nadobro.services.user_service import get_user_nado_client
        client = get_user_nado_client(telegram_id, passphrase=None)
    if not client:
        return {"success": False, "error": "Wallet client unavailable", "orders_placed": 0}

    if mid <= 0:
        mp = client.get_market_price(product_id)
        mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price", "orders_placed": 0}

    if open_orders is None:
        open_orders = client.get_open_orders(product_id)

    spread_bp = float(state.get("spread_bp") or 5.0)
    notional = float(state.get("notional_usd") or 100.0)
    leverage = float(state.get("leverage") or 3.0)
    max_orders = int(state.get("max_open_orders", 6))
    min_range_pct = float(state.get("min_range_pct") or 1.0)
    max_range_pct = float(state.get("max_range_pct") or 1.0)
    threshold_bp = float(state.get("threshold_bp") or 0.0)
    close_offset_bp = float(state.get("close_offset_bp") or 0.0)

    if strategy == "grid":
        levels = int(state.get("levels", GRID_DEFAULT_LEVELS))
        spread_bp = max(spread_bp, GRID_MIN_SPREAD_BP)
    else:
        levels = int(state.get("levels", MM_DEFAULT_LEVELS))
        spread_bp = max(spread_bp, MM_MIN_SPREAD_BP)

    if threshold_bp > 0 and strategy == "mm":
        reference = float(state.get("reference_price") or mid)
        moved_bp = abs(mid - reference) / max(reference, 1e-9) * 10000.0
        if moved_bp < threshold_bp:
            return {"success": True, "orders_placed": 0, "orders_cancelled": 0, "reason": "below threshold"}

    orders_cancelled = _cancel_stale_orders(client, product_id, mid, spread_bp, levels, open_orders, close_offset_bp=close_offset_bp)

    if orders_cancelled > 0:
        open_orders = client.get_open_orders(product_id)

    available_slots = max(0, max_orders - len(open_orders))
    if available_slots == 0:
        return {
            "success": True,
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "max open orders reached",
        }

    existing_prices = set()
    for o in open_orders:
        p = float(o.get("price", 0))
        if p > 0:
            existing_prices.add(round(p, 8))

    grid_orders = _compute_grid_prices(
        mid, spread_bp, levels, strategy=strategy, min_range_pct=min_range_pct, max_range_pct=max_range_pct
    )

    size_per_level = max(notional / mid / levels, 0.0001)

    orders_placed = 0
    errors = []

    for order_spec in grid_orders:
        if orders_placed >= available_slots:
            break
        order_price = order_spec["price"]
        rounded_price = round(order_price, 8)
        if rounded_price in existing_prices:
            continue

        result = execute_limit_order(
            telegram_id,
            product,
            size_per_level,
            order_price,
            is_long=order_spec["is_long"],
            leverage=leverage,
            enforce_rate_limit=False,
            passphrase=passphrase,
        )

        if result.get("success"):
            orders_placed += 1
            existing_prices.add(rounded_price)
            logger.info("Placed %s order at %.6f (level %d) for user %s",
                        "BUY" if order_spec["is_long"] else "SELL",
                        order_price, order_spec["level"], telegram_id)
        else:
            errors.append(f"L{order_spec['level']} {'BUY' if order_spec['is_long'] else 'SELL'}: {result.get('error', 'unknown')}")

    success = not (orders_placed == 0 and errors and len(open_orders) == 0)
    return {
        "success": success,
        "error": errors[0] if not success else None,
        "orders_placed": orders_placed,
        "orders_cancelled": orders_cancelled,
        "levels": levels,
        "spread_bp": spread_bp,
        "mid_price": mid,
        "errors": errors if errors else None,
    }
