"""
Simple DCA cycle for perps:
- Opens base position when none exists.
- Adds DCA legs when price deviates against position.
- Uses TP/SL from runtime guardrails handled in bot_runtime.
"""
import logging

from src.nadobro.services.trade_service import execute_market_order

logger = logging.getLogger(__name__)


def run_cycle(telegram_id: int, network: str, state: dict, **kwargs) -> dict:
    client = kwargs.get("client")
    mid = float(kwargs.get("mid", 0.0) or 0.0)
    product = kwargs.get("product", state.get("product", "BTC"))
    product_id = kwargs.get("product_id")
    passphrase = kwargs.get("passphrase")
    if not client or not product_id or mid <= 0:
        return {"success": False, "error": "Missing client/market inputs"}

    leverage = float(state.get("leverage") or 3.0)
    slippage_pct = float(state.get("slippage_pct") or 1.0)
    max_dca_orders = int(float(state.get("max_dca_orders") or 3))
    base_order_usd = float(state.get("base_order_usd") or 25.0)
    dca_order_usd = float(state.get("dca_order_usd") or base_order_usd)
    deviation_pct = float(state.get("deviation_pct") or 1.0)
    size_multiplier = float(state.get("size_multiplier") or 1.5)
    is_long_bias = float(state.get("is_long_bias") or 1.0) >= 0.5

    positions = client.get_all_positions() or []
    current = None
    for p in positions:
        if int(p.get("product_id", -1)) == int(product_id):
            current = p
            break

    dca_count = int(state.get("dca_count") or 0)
    avg_entry = float(state.get("dca_avg_entry") or 0.0)
    if current and avg_entry <= 0:
        avg_entry = float(current.get("price", 0) or 0)
        state["dca_avg_entry"] = avg_entry

    def _execute(notional_usd: float, is_long: bool, action: str) -> dict:
        size = max(0.0001, notional_usd / max(mid, 1e-9))
        res = execute_market_order(
            telegram_id,
            product,
            size=size,
            is_long=is_long,
            leverage=leverage,
            slippage_pct=slippage_pct,
            enforce_rate_limit=False,
            passphrase=passphrase,
        )
        if res.get("success"):
            state["dca_notional_done"] = float(state.get("dca_notional_done") or 0.0) + notional_usd
            state["dca_last_fill"] = mid
        return {"success": bool(res.get("success")), "action": action, "order": res}

    if not current:
        state["dca_count"] = 0
        state["dca_avg_entry"] = mid
        return _execute(base_order_usd, is_long_bias, "enter_base")

    side = str(current.get("side", "LONG")).upper()
    move_against = 0.0
    if avg_entry > 0:
        if side == "LONG":
            move_against = max(0.0, (avg_entry - mid) / avg_entry * 100.0)
        else:
            move_against = max(0.0, (mid - avg_entry) / avg_entry * 100.0)

    if dca_count >= max_dca_orders:
        return {"success": True, "action": "hold_max_dca", "dca_count": dca_count, "move_against_pct": move_against}

    threshold = deviation_pct * (dca_count + 1)
    if move_against < threshold:
        return {"success": True, "action": "hold", "dca_count": dca_count, "move_against_pct": move_against}

    this_notional = dca_order_usd * (size_multiplier ** dca_count)
    add_long = side == "LONG"
    res = _execute(this_notional, add_long, "add_dca")
    if res["success"]:
        state["dca_count"] = dca_count + 1
        # lightweight running-entry approximation for trigger decisions
        state["dca_avg_entry"] = ((avg_entry * (dca_count + 1)) + mid) / (dca_count + 2) if avg_entry > 0 else mid
    return res
