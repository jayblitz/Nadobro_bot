"""Simple volume strategy: fixed-side, fixed-margin, timed close loop."""
import logging
import time

from src.nadobro.config import EST_FEE_RATE, get_product_id
from src.nadobro.services.nado_archive import query_order_by_digest
from src.nadobro.services.trade_service import execute_limit_order
from src.nadobro.services.user_service import get_user_readonly_client

logger = logging.getLogger(__name__)

FIXED_MARGIN_USD = 100.0
FIXED_LEVERAGE = 1.0
CLOSE_AFTER_SECONDS = 60.0
MIN_SIZE = 0.0001
MIN_EFFECTIVE_MARGIN_USD = 10.0


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


def _available_quote_balance(client) -> float:
    try:
        bal = client.get_balance() or {}
        balances = bal.get("balances", {}) or {}
        return float(balances.get(0, balances.get("0", 0.0)) or 0.0)
    except Exception:
        return 0.0


def _aggressive_limit_price(mid: float, is_buy: bool, slippage_pct: float) -> float:
    slip = max(0.1, float(slippage_pct or 0.0)) / 100.0
    if is_buy:
        return mid * (1.0 + slip)
    return mid * (1.0 - slip)


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
    available_quote = _available_quote_balance(client)
    effective_margin = min(fixed_margin, max(0.0, available_quote * 0.90)) if available_quote > 0 else fixed_margin
    if effective_margin > 0:
        state["vol_effective_margin_usd"] = round(effective_margin, 4)

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
    tp_usd = (max(effective_margin, MIN_EFFECTIVE_MARGIN_USD) * tp_pct / 100.0) if tp_pct > 0 else 0.0
    sl_usd = (max(effective_margin, MIN_EFFECTIVE_MARGIN_USD) * sl_pct / 100.0) if sl_pct > 0 else 0.0
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

    if phase == "pending_close_fill":
        close_digest = str(state.get("vol_close_digest") or "")
        if not close_digest:
            state["vol_phase"] = "filled_wait_close"
            return {"success": True, "done": False, "action": "close_digest_missing_retry", "orders_placed": 0, "placed_notional_usd": 0.0}
        open_digests = {str(o.get("digest") or "") for o in (client.get_open_orders(product_id) or [])}
        if close_digest in open_digests:
            return {"success": True, "done": False, "action": "waiting_limit_close_fill", "orders_placed": 0, "placed_notional_usd": 0.0}

        pos = _load_position(client, product_id)
        if pos:
            state["vol_phase"] = "filled_wait_close"
            state["vol_close_digest"] = None
            return {"success": True, "done": False, "action": "close_not_filled_retry", "orders_placed": 0, "placed_notional_usd": 0.0}

        close_fill = _entry_fill_data(network, close_digest) or {}
        close_pnl, close_fee = _close_realized_pnl(network, close_digest)
        close_price = float(close_fill.get("fill_price") or mid)
        close_size = float(state.get("vol_close_size") or state.get("vol_entry_size") or MIN_SIZE)
        entry_price = float(state.get("vol_entry_fill_price") or mid)
        entry_size = float(state.get("vol_entry_size") or close_size or MIN_SIZE)
        side_sign = 1.0 if direction == "long" else -1.0
        approx_pnl = side_sign * (close_price - entry_price) * min(entry_size, close_size)
        if close_pnl is None:
            cycle_pnl = approx_pnl - ((entry_size * entry_price + close_size * close_price) * EST_FEE_RATE)
        else:
            cycle_pnl = close_pnl - close_fee
        traded_notional = (entry_size * entry_price) + (close_size * close_price)
        volume_done += traded_notional
        session_pnl += cycle_pnl
        state["volume_done_usd"] = round(volume_done, 4)
        state["session_realized_pnl_usd"] = round(session_pnl, 6)
        state["vol_phase"] = "idle"
        state["vol_entry_digest"] = None
        state["vol_entry_fill_ts"] = 0.0
        state["vol_entry_fill_price"] = 0.0
        state["vol_entry_size"] = 0.0
        state["vol_close_digest"] = None
        state["vol_close_size"] = 0.0

        if tp_usd > 0 and session_pnl >= tp_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "tp_hit",
                "action": "closed_limit_and_session_tp_hit",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
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
                "action": "closed_limit_and_session_sl_hit",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
            }
        return {
            "success": True,
            "done": False,
            "action": "limit_close_filled_reloop",
            "orders_placed": 0,
            "placed_notional_usd": round(traded_notional, 4),
            "vol_order_attempts": 0,
            "vol_order_failures": 0,
            "session_realized_pnl_usd": round(session_pnl, 6),
            "cycle_realized_pnl_usd": round(cycle_pnl, 6),
            "volume_done_usd": round(volume_done, 4),
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
        logger.info(
            "VOL close order attempt user=%s network=%s product=%s phase=%s side=%s size=%.8f",
            telegram_id,
            network,
            product,
            phase,
            pos_side or "?",
            pos_size,
        )
        close_limit = _aggressive_limit_price(mid, is_buy=close_is_long, slippage_pct=slippage_pct)
        close_result = execute_limit_order(
            telegram_id,
            product,
            pos_size,
            close_limit,
            is_long=close_is_long,
            leverage=FIXED_LEVERAGE,
            reduce_only=True,
            enforce_rate_limit=False,
            source="vol",
            strategy_session_id=state.get("strategy_session_id"),
        )
        if not close_result.get("success"):
            close_error = str(close_result.get("error") or "Market close failed")
            logger.warning(
                "VOL close order failed user=%s network=%s product=%s phase=%s error=%s",
                telegram_id,
                network,
                product,
                phase,
                close_error[:200],
            )
            return {
                "success": False,
                "error": close_error,
                "orders_placed": 0,
                "placed_notional_usd": 0.0,
                "vol_order_attempts": 1,
                "vol_order_failures": 1,
                "last_order_error": close_error,
            }
        logger.info(
            "VOL close order result user=%s network=%s product=%s success=%s digest=%s",
            telegram_id,
            network,
            product,
            bool(close_result.get("success")),
            str(close_result.get("digest") or "")[:32],
        )

        state["vol_phase"] = "pending_close_fill"
        state["vol_close_digest"] = close_result.get("digest")
        state["vol_close_size"] = float(pos_size or 0.0)
        logger.info(
            "VOL state transition user=%s network=%s product=%s from=filled_wait_close to=pending_close_fill",
            telegram_id,
            network,
            product,
        )
        return {
            "success": True,
            "done": False,
            "action": "placed_limit_close_wait_fill",
            "orders_placed": 1,
            "placed_notional_usd": round(pos_size * close_limit, 4),
            "vol_order_attempts": 1,
            "vol_order_failures": 0,
            "close_digest": close_result.get("digest"),
        }

    # phase == idle
    if effective_margin < MIN_EFFECTIVE_MARGIN_USD:
        return {
            "success": False,
            "error": (
                f"Insufficient margin for Vol cycle. Need >= ${MIN_EFFECTIVE_MARGIN_USD:.2f} "
                f"effective margin, available ${available_quote:.2f}."
            ),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 0,
            "vol_order_failures": 1,
            "last_order_error": "insufficient_effective_margin",
        }
    size = max(effective_margin / mid, MIN_SIZE)
    is_long = direction == "long"
    entry_limit = _aggressive_limit_price(mid, is_buy=is_long, slippage_pct=slippage_pct)
    logger.info(
        "VOL entry order attempt user=%s network=%s product=%s phase=%s direction=%s size=%.8f limit=%.8f effective_margin=%.2f",
        telegram_id,
        network,
        product,
        phase,
        direction,
        size,
        entry_limit,
        effective_margin,
    )
    open_result = execute_limit_order(
        telegram_id,
        product,
        size,
        entry_limit,
        is_long=is_long,
        leverage=FIXED_LEVERAGE,
        enforce_rate_limit=False,
        source="vol",
        strategy_session_id=state.get("strategy_session_id"),
    )
    if not open_result.get("success"):
        open_error = str(open_result.get("error") or "Market entry failed")
        logger.warning(
            "VOL entry order failed user=%s network=%s product=%s phase=%s error=%s",
            telegram_id,
            network,
            product,
            phase,
            open_error[:200],
        )
        return {
            "success": False,
            "error": open_error,
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 1,
            "vol_order_failures": 1,
            "last_order_error": open_error,
        }
    logger.info(
        "VOL entry order result user=%s network=%s product=%s success=%s digest=%s",
        telegram_id,
        network,
        product,
        bool(open_result.get("success")),
        str(open_result.get("digest") or "")[:32],
    )

    entry_price = float(open_result.get("price") or entry_limit or mid)
    entry_size = max(float(open_result.get("size") or size or 0.0), MIN_SIZE)
    state["vol_phase"] = "pending_fill"
    state["vol_entry_digest"] = open_result.get("digest")
    state["vol_entry_size"] = entry_size
    state["vol_entry_fill_price"] = entry_price
    state["vol_entry_fill_ts"] = 0.0
    logger.info(
        "VOL state transition user=%s network=%s product=%s from=idle to=pending_fill",
        telegram_id,
        network,
        product,
    )
    return {
        "success": True,
        "done": False,
        "action": "opened_limit_wait_fill",
        "orders_placed": 1,
        "placed_notional_usd": round(entry_size * entry_price, 4),
        "vol_order_attempts": 1,
        "vol_order_failures": 0,
        "entry_digest": open_result.get("digest"),
        "direction": direction.upper(),
        "entry_price": entry_price,
        "session_realized_pnl_usd": round(session_pnl, 6),
        "volume_done_usd": round(volume_done, 4),
    }
