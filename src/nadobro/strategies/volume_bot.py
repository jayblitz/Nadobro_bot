"""Simple volume strategy: fixed-side, fixed-margin, timed close loop."""
import logging
import time

from src.nadobro.config import EST_FEE_RATE, get_product_id
from src.nadobro.services.nado_archive import query_order_by_digest
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.user_service import get_user_readonly_client

logger = logging.getLogger(__name__)

FIXED_MARGIN_USD = 100.0
FIXED_LEVERAGE = 1.0
CLOSE_AFTER_SECONDS = 60.0
MIN_SIZE = 0.0001


def _normalize_direction(raw: str) -> str:
    val = str(raw or "").strip().lower()
    return "short" if val == "short" else "long"


def _load_position(client, product_id: int) -> dict | None:
    for p in (client.get_all_positions() or []):
        if int(p.get("product_id", -1)) == product_id:
            amount = abs(float(p.get("amount", 0) or 0))
            if amount > 0:
                return p
    return None


def _entry_fill_data(network: str, digest: str) -> dict | None:
    if not digest:
        return None
    return query_order_by_digest(network, digest, max_wait_seconds=0.05, poll_interval=0.05)


def _close_realized_pnl(network: str, digest: str) -> tuple[float | None, float]:
    if not digest:
        return None, 0.0
    parsed = query_order_by_digest(network, digest, max_wait_seconds=0.35, poll_interval=0.1)
    if not parsed:
        return None, 0.0
    return float(parsed.get("realized_pnl", 0.0) or 0.0), float(parsed.get("fee", 0.0) or 0.0)


def get_fee_pnl_preview(telegram_id: int, product: str, target_volume_usd: float) -> dict:
    estimated_fees = target_volume_usd * EST_FEE_RATE
    return {
        "target_volume_usd": target_volume_usd,
        "flip_size_usd": FIXED_MARGIN_USD,
        "num_flips": max(1, int(target_volume_usd / FIXED_MARGIN_USD)),
        "fee_rate": EST_FEE_RATE,
        "estimated_fees": round(estimated_fees, 4),
        "estimated_slippage": round(target_volume_usd * 0.0002, 4),
        "estimated_total_cost": round(estimated_fees + (target_volume_usd * 0.0002), 4),
        "product": product,
    }


def run_cycle(telegram_id: int, network: str, state: dict, **kwargs) -> dict:
    client = kwargs.get("client") or get_user_readonly_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet client unavailable"}
    if hasattr(client, "network") and client.network != network:
        return {"success": False, "error": f"Network mismatch: expected {network}, got {client.network}"}

    product = str(state.get("product") or "BTC").upper()
    direction = _normalize_direction(state.get("vol_direction") or state.get("direction") or "long")
    state["vol_direction"] = direction
    state["direction"] = direction

    fixed_margin = float(state.get("fixed_margin_usd") or FIXED_MARGIN_USD)
    state["fixed_margin_usd"] = FIXED_MARGIN_USD
    state["leverage"] = FIXED_LEVERAGE
    slippage_pct = float(state.get("slippage_pct") or 1.0)

    product_id = get_product_id(product, network=network)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'"}

    mp = client.get_market_price(product_id)
    mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price"}

    phase = str(state.get("vol_phase") or "idle")
    now_ts = time.time()
    session_pnl = float(state.get("session_realized_pnl_usd") or 0.0)
    volume_done = float(state.get("volume_done_usd") or 0.0)

    tp_pct = float(state.get("tp_pct") or 0.0)
    sl_pct = float(state.get("sl_pct") or 0.0)
    tp_usd = (fixed_margin * tp_pct / 100.0) if tp_pct > 0 else 0.0
    sl_usd = (fixed_margin * sl_pct / 100.0) if sl_pct > 0 else 0.0
    if tp_usd > 0 and session_pnl >= tp_usd:
        state["running"] = False
        return {
            "success": True,
            "done": True,
            "stop_reason": "tp_hit",
            "action": "session_tp_hit",
            "session_realized_pnl_usd": round(session_pnl, 6),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
        }
    if sl_usd > 0 and session_pnl <= -sl_usd:
        state["running"] = False
        return {
            "success": True,
            "done": True,
            "stop_reason": "sl_hit",
            "action": "session_sl_hit",
            "session_realized_pnl_usd": round(session_pnl, 6),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
        }

    if phase == "pending_fill":
        entry_digest = str(state.get("vol_entry_digest") or "")
        if not entry_digest:
            state["vol_phase"] = "idle"
            return {"success": True, "done": False, "action": "entry_digest_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}

        open_digests = {str(o.get("digest") or "") for o in (client.get_open_orders(product_id) or [])}
        if entry_digest in open_digests:
            return {"success": True, "done": False, "action": "waiting_entry_fill", "orders_placed": 0, "placed_notional_usd": 0.0}

        fill_data = _entry_fill_data(network, entry_digest)
        pos = _load_position(client, product_id)
        if not fill_data and not pos:
            # If the order disappeared and no position exists (manual cancel, etc.), reset and repost.
            state["vol_phase"] = "idle"
            state["vol_entry_digest"] = None
            return {"success": True, "done": False, "action": "entry_not_filled_repost", "orders_placed": 0, "placed_notional_usd": 0.0}

        entry_price = float((fill_data or {}).get("fill_price") or mid)
        entry_size = float((fill_data or {}).get("fill_size") or 0.0)
        if entry_size <= 0 and pos:
            entry_size = abs(float(pos.get("amount") or 0.0))
        entry_size = max(entry_size, MIN_SIZE)

        state["vol_phase"] = "filled_wait_close"
        state["vol_entry_fill_ts"] = now_ts
        state["vol_entry_fill_price"] = entry_price
        state["vol_entry_size"] = entry_size
        return {
            "success": True,
            "done": False,
            "action": "entry_filled_wait_close",
            "orders_placed": 0,
            "placed_notional_usd": round(entry_size * entry_price, 4),
        }

    if phase == "filled_wait_close":
        entry_ts = float(state.get("vol_entry_fill_ts") or 0.0)
        if entry_ts <= 0:
            state["vol_phase"] = "idle"
            return {"success": True, "done": False, "action": "entry_ts_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}
        if now_ts < (entry_ts + CLOSE_AFTER_SECONDS):
            return {"success": True, "done": False, "action": "waiting_close_timer", "orders_placed": 0, "placed_notional_usd": 0.0}

        pos = _load_position(client, product_id)
        if not pos:
            state["vol_phase"] = "idle"
            state["vol_entry_digest"] = None
            return {"success": True, "done": False, "action": "position_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}

        pos_size = abs(float(pos.get("amount", 0.0) or 0.0))
        pos_side = str(pos.get("side", "") or "").upper()
        close_is_long = pos_side == "SHORT"
        close_result = execute_market_order(
            telegram_id,
            product,
            pos_size,
            is_long=close_is_long,
            leverage=FIXED_LEVERAGE,
            slippage_pct=slippage_pct,
            enforce_rate_limit=False,
            source="vol",
            strategy_session_id=state.get("strategy_session_id"),
        )
        if not close_result.get("success"):
            return {"success": False, "error": close_result.get("error", "Market close failed"), "orders_placed": 0, "placed_notional_usd": 0.0}

        close_price = float(close_result.get("price") or mid)
        close_digest = str(close_result.get("digest") or "")
        close_pnl, close_fee = _close_realized_pnl(network, close_digest)
        entry_price = float(state.get("vol_entry_fill_price") or mid)
        entry_size = float(state.get("vol_entry_size") or pos_size or MIN_SIZE)
        side_sign = 1.0 if direction == "long" else -1.0
        approx_pnl = side_sign * (close_price - entry_price) * min(entry_size, pos_size)
        if close_pnl is None:
            cycle_pnl = approx_pnl - ((entry_size * entry_price + pos_size * close_price) * EST_FEE_RATE)
        else:
            cycle_pnl = close_pnl - close_fee

        traded_notional = (entry_size * entry_price) + (pos_size * close_price)
        volume_done += traded_notional
        session_pnl += cycle_pnl

        state["volume_done_usd"] = round(volume_done, 4)
        state["session_realized_pnl_usd"] = round(session_pnl, 6)
        state["vol_phase"] = "idle"
        state["vol_entry_digest"] = None
        state["vol_entry_fill_ts"] = 0.0
        state["vol_entry_fill_price"] = 0.0
        state["vol_entry_size"] = 0.0

        if tp_usd > 0 and session_pnl >= tp_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "tp_hit",
                "action": "closed_and_session_tp_hit",
                "orders_placed": 1,
                "placed_notional_usd": round(traded_notional, 4),
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
            }
        if sl_usd > 0 and session_pnl <= -sl_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "sl_hit",
                "action": "closed_and_session_sl_hit",
                "orders_placed": 1,
                "placed_notional_usd": round(traded_notional, 4),
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
            }

        return {
            "success": True,
            "done": False,
            "action": "closed_reloop",
            "orders_placed": 1,
            "placed_notional_usd": round(traded_notional, 4),
            "session_realized_pnl_usd": round(session_pnl, 6),
            "cycle_realized_pnl_usd": round(cycle_pnl, 6),
            "volume_done_usd": round(volume_done, 4),
        }

    # phase == idle
    size = max(fixed_margin / mid, MIN_SIZE)
    is_long = direction == "long"
    open_result = execute_market_order(
        telegram_id,
        product,
        size,
        is_long=is_long,
        leverage=FIXED_LEVERAGE,
        slippage_pct=slippage_pct,
        enforce_rate_limit=False,
        source="vol",
        strategy_session_id=state.get("strategy_session_id"),
    )
    if not open_result.get("success"):
        return {"success": False, "error": open_result.get("error", "Market entry failed"), "orders_placed": 0, "placed_notional_usd": 0.0}

    entry_price = float(open_result.get("price") or mid)
    entry_size = max(float(open_result.get("size") or size or 0.0), MIN_SIZE)
    state["vol_phase"] = "filled_wait_close"
    state["vol_entry_digest"] = open_result.get("digest")
    state["vol_entry_size"] = entry_size
    state["vol_entry_fill_price"] = entry_price
    state["vol_entry_fill_ts"] = now_ts
    return {
        "success": True,
        "done": False,
        "action": "opened_market_wait_close",
        "orders_placed": 1,
        "placed_notional_usd": round(entry_size * entry_price, 4),
        "entry_digest": open_result.get("digest"),
        "direction": direction.upper(),
        "entry_price": entry_price,
        "session_realized_pnl_usd": round(session_pnl, 6),
        "volume_done_usd": round(volume_done, 4),
    }
