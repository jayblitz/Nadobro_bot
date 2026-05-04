"""Simple volume strategy: fixed-notional, max-leverage, timed close loop.

CEO directive (2026-05): Volume strategy now sizes by NOTIONAL ($100 per trade by
default) and uses per-asset MAX leverage on perp so margin shrinks proportionally
and post-only refresh quotes can sit alongside closing legs. Spot stays at 1x.
SL/TP percentages are applied to the position NOTIONAL (not margin), so a 5% SL
at $100 notional triggers at -$5 PnL regardless of leverage.
"""
import logging
import time

from src.nadobro.config import (
    EST_FEE_RATE,
    get_product_id,
    get_product_max_leverage,
    get_spot_metadata,
    get_spot_product_id,
    list_volume_spot_product_names,
    normalize_volume_spot_symbol,
)
from src.nadobro.services.nado_archive import query_order_by_digest
from src.nadobro.services.trade_service import (
    execute_limit_order,
    execute_market_order,
    execute_spot_limit_order,
    execute_spot_market_order,
)
from src.nadobro.services.user_service import get_user_readonly_client

logger = logging.getLogger(__name__)

# Position notional per Volume trade (the "position value" in CEO terms).
# User-overridable via state["target_notional_usd"] (legacy: state["fixed_margin_usd"]).
TARGET_NOTIONAL_USD = 100.0
# Backward-compat alias — external callers (preview cards, copy trading) may still
# import FIXED_MARGIN_USD; we keep the name pointing at the same value so they don't break.
FIXED_MARGIN_USD = TARGET_NOTIONAL_USD
CLOSE_AFTER_SECONDS = 60.0
MIN_SIZE = 0.0001
# Notional floor — abort the cycle if user/state misconfigures below this.
MIN_NOTIONAL_USD = 10.0
# Legacy floor kept for any external import; semantically replaced by MIN_NOTIONAL_USD.
MIN_EFFECTIVE_MARGIN_USD = 10.0
DEFAULT_TARGET_VOLUME_USD = 10000.0
DEFAULT_VOL_EMA_LEN = 50
DEFAULT_VOL_RSI_LEN = 14
DEFAULT_VOL_RSI_LONG_MAX = 50.0
DEFAULT_VOL_RSI_SHORT_MIN = 50.0
DEFAULT_VOL_TRADE_TP_PCT = 0.40
DEFAULT_VOL_TRADE_SL_PCT = 0.20
DEFAULT_VOL_HOLD_MIN_SECONDS = 60.0
DEFAULT_VOL_HOLD_MAX_SECONDS = 540.0
DEFAULT_VOL_MAX_SPREAD_BP = 12.0
DEFAULT_VOL_MIN_EDGE_BP = 4.0

# Max seconds a post-only close is allowed to rest before we cancel-and-retry
# with a wider limit. After CLOSE_ESCALATE_AFTER_SECONDS we'll force-close
# via a reduce-only IOC order so margin is actually released and the next
# cycle can re-enter (the "compiles and uses up the margin" bug).
CLOSE_REPOST_AFTER_SECONDS = 45.0
CLOSE_ESCALATE_AFTER_SECONDS = 180.0
SPOT_BALANCE_RACE_GRACE_SECONDS = 15.0
FORCE_CLOSE_RETRY_COOLDOWN_SECONDS = 20.0


def _resolve_max_leverage(product: str, network: str, client, *, vol_market: str) -> float:
    """Per-asset max leverage for Volume. Spot is pinned to 1x (spot has no leverage)."""
    if (vol_market or "perp").lower() == "spot":
        return 1.0
    try:
        return float(get_product_max_leverage(product, network=network, client=client))
    except Exception:
        # Defensive: if catalog is unavailable, fall back to the legacy 1x so we
        # never accidentally over-leverage when the resolver fails.
        return 1.0


def _resolve_target_notional(state: dict) -> float:
    """User-overridable position notional with $100 default.

    Reads state["target_notional_usd"] first (new key); falls back to legacy
    state["fixed_margin_usd"] for backward compatibility with existing user configs.
    Floored at MIN_NOTIONAL_USD; defaults to TARGET_NOTIONAL_USD ($100).
    """
    raw = state.get("target_notional_usd")
    if raw is None:
        raw = state.get("fixed_margin_usd")
    try:
        val = float(raw) if raw is not None else TARGET_NOTIONAL_USD
    except (TypeError, ValueError):
        val = TARGET_NOTIONAL_USD
    return max(MIN_NOTIONAL_USD, val)


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


def _maker_limit_price(book: dict, is_buy: bool) -> float:
    bid = float(book.get("bid") or 0.0)
    ask = float(book.get("ask") or 0.0)
    mid = float(book.get("mid") or 0.0)
    if is_buy:
        return bid if bid > 0 else mid
    return ask if ask > 0 else mid


def _pseudo_random_hold_seconds(seed: int, lo: float, hi: float) -> float:
    lo_i = max(1, int(lo))
    hi_i = max(lo_i, int(hi))
    x = ((int(seed) * 9301 + 49297) % 233280)
    r = x / 233280.0
    return float(lo_i + int(r * (hi_i - lo_i + 1)))


def _quote_quality(mp: dict, mid: float, max_spread_bp: float) -> tuple[bool, float, str]:
    bid = float(mp.get("bid") or 0.0)
    ask = float(mp.get("ask") or 0.0)
    if mid <= 0 or bid <= 0 or ask <= 0 or ask < bid:
        return False, 0.0, "invalid_book"
    spread_bp = (ask - bid) / max(mid, 1e-9) * 10000.0
    if max_spread_bp > 0 and spread_bp > max_spread_bp:
        return False, spread_bp, "spread_too_wide"
    return True, spread_bp, ""


def _volume_signal(product: str, mid: float, state: dict) -> dict:
    """Return a conservative entry signal from local price history.

    The volume bot should not manufacture trades just to hit volume. It only
    enters when short-term price action agrees with trend and pullback filters.
    """
    try:
        from src.nadobro.services.price_tracker import compute_ema, compute_rsi, get_history
    except Exception:
        return {"ok": False, "reason": "signal_unavailable"}

    ema_len = int(state.get("vol_ema_len") or DEFAULT_VOL_EMA_LEN)
    rsi_len = int(state.get("vol_rsi_len") or DEFAULT_VOL_RSI_LEN)
    rsi_long_max = float(state.get("vol_rsi_long_max") or DEFAULT_VOL_RSI_LONG_MAX)
    rsi_short_min = float(state.get("vol_rsi_short_min") or DEFAULT_VOL_RSI_SHORT_MIN)
    min_edge_bp = float(state.get("vol_min_edge_bp") or DEFAULT_VOL_MIN_EDGE_BP)

    history = get_history(product, limit=max(ema_len, rsi_len) + 2)
    if len(history) < max(ema_len, rsi_len) + 1:
        return {"ok": False, "reason": "warming_signal_history", "history_points": len(history)}
    prev_mid = float((history[-2] or {}).get("mid") or 0.0)
    ema = compute_ema(product, ema_len)
    rsi = compute_rsi(product, rsi_len)
    if not ema or rsi is None or prev_mid <= 0 or mid <= 0:
        return {"ok": False, "reason": "signal_incomplete", "history_points": len(history)}

    edge_bp = abs(mid - ema) / max(mid, 1e-9) * 10000.0
    long_ok = mid > ema and mid > prev_mid and rsi < rsi_long_max and edge_bp >= min_edge_bp
    short_ok = mid < ema and mid < prev_mid and rsi > rsi_short_min and edge_bp >= min_edge_bp

    direction_mode = str(state.get("vol_direction_mode") or "signal").strip().lower()
    configured = _normalize_direction(state.get("vol_direction") or state.get("direction") or "long")
    next_dir = _normalize_direction(state.get("vol_next_direction") or configured)
    if direction_mode == "fixed":
        direction = configured
        ok = long_ok if direction == "long" else short_ok
    elif direction_mode in ("pingpong", "signal_pingpong"):
        direction = next_dir
        ok = long_ok if direction == "long" else short_ok
    else:
        if long_ok and not short_ok:
            direction = "long"
            ok = True
        elif short_ok and not long_ok:
            direction = "short"
            ok = True
        else:
            direction = configured
            ok = False

    reason = "signal_ok" if ok else "waiting_signal"
    return {
        "ok": bool(ok),
        "direction": direction,
        "reason": reason,
        "ema": float(ema),
        "rsi": float(rsi),
        "prev_mid": prev_mid,
        "edge_bp": edge_bp,
        "long_ok": bool(long_ok),
        "short_ok": bool(short_ok),
        "direction_mode": direction_mode,
    }


def _volume_exit_reason(state: dict, mid: float, now_ts: float, direction: str) -> tuple[bool, str]:
    entry_price = float(state.get("vol_entry_fill_price") or 0.0)
    entry_ts = float(state.get("vol_entry_fill_ts") or 0.0)
    if entry_price <= 0 or entry_ts <= 0 or mid <= 0:
        return False, "missing_entry"

    trade_tp_pct = float(state.get("vol_trade_tp_pct") or DEFAULT_VOL_TRADE_TP_PCT)
    trade_sl_pct = float(state.get("vol_trade_sl_pct") or DEFAULT_VOL_TRADE_SL_PCT)
    hold_min = float(state.get("vol_hold_min_seconds") or DEFAULT_VOL_HOLD_MIN_SECONDS)
    hold_max = float(state.get("vol_hold_seconds") or state.get("vol_hold_max_seconds") or DEFAULT_VOL_HOLD_MAX_SECONDS)
    held = max(0.0, now_ts - entry_ts)
    side = 1.0 if direction == "long" else -1.0
    pnl_pct = side * (mid - entry_price) / max(entry_price, 1e-9) * 100.0

    if trade_tp_pct > 0 and pnl_pct >= trade_tp_pct:
        return True, "trade_tp_hit"
    if trade_sl_pct > 0 and pnl_pct <= -trade_sl_pct:
        return True, "trade_sl_hit"
    if held >= max(hold_min, hold_max):
        return True, "hold_expired"
    return False, "min_hold" if held < hold_min else "waiting_edge"


def _record_volume_cycle_metrics(state: dict, cycle_pnl: float) -> None:
    closed = int(state.get("vol_closed_cycles") or 0) + 1
    wins = int(state.get("vol_winning_cycles") or 0)
    losses = int(state.get("vol_losing_cycles") or 0)
    if cycle_pnl > 1e-9:
        wins += 1
    elif cycle_pnl < -1e-9:
        losses += 1
    total_pnl = float(state.get("vol_total_cycle_pnl_usd") or 0.0) + float(cycle_pnl or 0.0)
    state["vol_closed_cycles"] = closed
    state["vol_winning_cycles"] = wins
    state["vol_losing_cycles"] = losses
    state["vol_total_cycle_pnl_usd"] = round(total_pnl, 6)
    state["vol_avg_cycle_pnl_usd"] = round(total_pnl / max(1, closed), 6)
    state["vol_win_rate"] = round(wins / max(1, wins + losses), 6) if (wins + losses) > 0 else 0.0


def _spot_base_balance(client, spot_product_id: int) -> float:
    try:
        bal = client.get_balance() or {}
        b = bal.get("balances", {}) or {}
        return float(b.get(spot_product_id, b.get(str(spot_product_id), 0)) or 0.0)
    except Exception:
        return 0.0


def _compute_close_ttl_windows(state: dict, mp: dict, now_mid: float) -> tuple[float, float]:
    """Compute repost/escalate windows with optional adaptive widening.

    Goal: reduce fee drag from unnecessary force-close IOC/market escalation during
    temporarily wide spreads or fast moves. Windows can be overridden directly via
    state keys `close_repost_after_seconds` / `close_escalate_after_seconds`.
    """
    configured_repost = float(state.get("close_repost_after_seconds") or 0.0)
    configured_escalate = float(state.get("close_escalate_after_seconds") or 0.0)
    base_repost = configured_repost if configured_repost > 0 else CLOSE_REPOST_AFTER_SECONDS
    base_escalate = configured_escalate if configured_escalate > 0 else CLOSE_ESCALATE_AFTER_SECONDS

    adaptive_enabled = bool(state.get("adaptive_close_ttl", True))
    if not adaptive_enabled or now_mid <= 0:
        return base_repost, base_escalate

    bid = float(mp.get("bid") or 0.0)
    ask = float(mp.get("ask") or 0.0)
    spread_bp = 0.0
    if bid > 0 and ask > 0 and ask >= bid:
        spread_bp = (ask - bid) / max(now_mid, 1e-9) * 10000.0

    prev_mid = float(state.get("vol_prev_mid") or now_mid)
    move_bp = abs(now_mid - prev_mid) / max(prev_mid, 1e-9) * 10000.0 if prev_mid > 0 else 0.0

    # Mild adaptive widening: at most +50% (keeps the close loop responsive but
    # avoids jumping to taker close too aggressively when maker fill conditions are poor).
    spread_component = min(0.35, max(0.0, (spread_bp - 2.0) / 20.0))
    move_component = min(0.15, move_bp / 50.0)
    widen_mult = 1.0 + spread_component + move_component

    repost_after = min(120.0, max(15.0, base_repost * widen_mult))
    escalate_after = min(360.0, max(90.0, base_escalate * widen_mult))
    return repost_after, escalate_after


def _run_volume_spot_cycle(
    telegram_id: int,
    network: str,
    state: dict,
    client,
    product: str,
    spot_product_id: int,
    spot_symbol: str,
    mp: dict,
    mid: float,
    fixed_margin: float,
    effective_margin: float,
    target_notional: float,
    target_volume: float,
    tp_usd: float,
    sl_usd: float,
    session_pnl: float,
    volume_done: float,
    phase: str,
    now_ts: float,
) -> dict:
    """Volume spot: post-only buy → wait fill → 60s → post-only sell (same phase keys as perp).

    Sizing is by ``target_notional`` (USD); ``effective_margin`` is retained for the
    wallet sanity floor and legacy preview fields. SL/TP USD thresholds (``tp_usd``,
    ``sl_usd``) are pre-computed by the caller against the position notional.
    """
    direction = "long"

    if phase == "pending_fill":
        entry_digest = str(state.get("vol_entry_digest") or "")
        if not entry_digest:
            state["vol_phase"] = "idle"
            return {"success": True, "done": False, "action": "entry_digest_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}

        open_digests = {str(o.get("digest") or "") for o in (client.get_open_orders(spot_product_id) or [])}
        if entry_digest in open_digests:
            return {"success": True, "done": False, "action": "waiting_entry_fill", "orders_placed": 0, "placed_notional_usd": 0.0}

        fill_data = _entry_fill_data(network, entry_digest)
        posted = max(float(state.get("vol_entry_size") or 0.0), MIN_SIZE)
        base = _spot_base_balance(client, spot_product_id)
        filled_by_balance = base >= posted * 0.98
        if not fill_data and not filled_by_balance:
            state["vol_phase"] = "idle"
            state["vol_entry_digest"] = None
            return {"success": True, "done": False, "action": "entry_not_filled_repost", "orders_placed": 0, "placed_notional_usd": 0.0}

        entry_price = float((fill_data or {}).get("fill_price") or mid)
        entry_size = float((fill_data or {}).get("fill_size") or 0.0)
        if entry_size <= 0 and filled_by_balance:
            entry_size = max(min(base, posted), MIN_SIZE)
        entry_size = max(entry_size, MIN_SIZE)

        state["vol_phase"] = "filled_wait_close"
        state["vol_entry_fill_ts"] = now_ts
        state["vol_entry_fill_price"] = entry_price
        state["vol_entry_size"] = entry_size
        hold_seed = int(now_ts) + int(telegram_id) + int(spot_product_id)
        state["vol_hold_seconds"] = _pseudo_random_hold_seconds(
            hold_seed,
            float(state.get("vol_hold_min_seconds") or DEFAULT_VOL_HOLD_MIN_SECONDS),
            float(state.get("vol_hold_max_seconds") or DEFAULT_VOL_HOLD_MAX_SECONDS),
        )
        state["vol_last_order_digest"] = entry_digest
        state["vol_last_order_kind"] = "entry_filled"
        return {
            "success": True,
            "done": False,
            "action": "entry_filled_wait_close",
            "detail": f"Spot entry filled #{entry_digest[:10]} at ${entry_price:,.2f}",
            "orders_placed": 0,
            "placed_notional_usd": round(entry_size * entry_price, 4),
        }

    if phase == "pending_close_fill":
        close_digest = str(state.get("vol_close_digest") or "")
        if not close_digest:
            state["vol_phase"] = "filled_wait_close"
            return {"success": True, "done": False, "action": "close_digest_missing_retry", "orders_placed": 0, "placed_notional_usd": 0.0}
        open_digests = {str(o.get("digest") or "") for o in (client.get_open_orders(spot_product_id) or [])}
        if close_digest in open_digests:
            # Same TTL / escalation ladder as perp — a post-only spot sell can get
            # marooned above a dropping market and hold the base balance forever.
            close_posted_ts = float(state.get("vol_close_posted_ts") or 0.0)
            entry_ts = float(state.get("vol_entry_fill_ts") or 0.0)
            stuck_in_close = (now_ts - close_posted_ts) if close_posted_ts > 0 else 0.0
            stuck_since_entry = (now_ts - entry_ts) if entry_ts > 0 else stuck_in_close
            repost_after_s, escalate_after_s = _compute_close_ttl_windows(state, mp, mid)

            if stuck_since_entry >= escalate_after_s:
                last_force_ts = float(state.get("vol_last_force_close_attempt_ts") or 0.0)
                if last_force_ts > 0 and (now_ts - last_force_ts) < FORCE_CLOSE_RETRY_COOLDOWN_SECONDS:
                    return {
                        "success": True,
                        "done": False,
                        "action": "waiting_force_close_cooldown",
                        "orders_placed": 0,
                        "placed_notional_usd": 0.0,
                    }
                try:
                    client.cancel_order(spot_product_id, close_digest)
                except Exception as cancel_err:
                    logger.warning(
                        "VOL spot close cancel-before-escalate failed user=%s product=%s digest=%s err=%s",
                        telegram_id, product, close_digest[:16], cancel_err,
                    )
                escalate_size = _spot_base_balance(client, spot_product_id)
                state["vol_last_force_close_attempt_ts"] = now_ts
                if escalate_size >= MIN_SIZE * 2:
                    logger.warning(
                        "VOL spot force-close (market sell) user=%s product=%s stuck_seconds=%.1f size=%.8f",
                        telegram_id, product, stuck_since_entry, escalate_size,
                    )
                    force_res = execute_spot_market_order(
                        telegram_id,
                        product,
                        escalate_size,
                        is_buy=False,
                        enforce_rate_limit=False,
                        source="vol",
                        strategy_session_id=state.get("strategy_session_id"),
                        network=network,
                        spot_product_id=spot_product_id,
                        spot_symbol=spot_symbol,
                        asset_label=spot_symbol,
                    )
                    if force_res.get("success"):
                        state["vol_close_digest"] = force_res.get("digest")
                        state["vol_close_posted_ts"] = now_ts
                        state["vol_last_order_digest"] = str(force_res.get("digest") or "")
                        state["vol_last_order_kind"] = "close_escalated_market"
                        return {
                            "success": True,
                            "done": False,
                            "action": "close_escalated_force_close",
                            "detail": f"Spot force-sold stuck inventory (stuck {stuck_since_entry:.0f}s)",
                            "orders_placed": 1,
                            "placed_notional_usd": round(escalate_size * mid, 4),
                            "vol_order_attempts": 1,
                            "vol_order_failures": 0,
                        }
                    logger.warning(
                        "VOL spot force-close failed user=%s product=%s err=%s",
                        telegram_id, product, str(force_res.get("error"))[:200],
                    )
                state["vol_phase"] = "filled_wait_close"
                state["vol_close_digest"] = None
                state["vol_close_posted_ts"] = 0.0
                return {
                    "success": True, "done": False,
                    "action": "close_escalate_fallback_retry",
                    "orders_placed": 0, "placed_notional_usd": 0.0,
                }

            if stuck_in_close >= repost_after_s:
                try:
                    client.cancel_order(spot_product_id, close_digest)
                except Exception as cancel_err:
                    logger.warning(
                        "VOL spot close cancel-for-repost failed user=%s product=%s digest=%s err=%s",
                        telegram_id, product, close_digest[:16], cancel_err,
                    )
                logger.info(
                    "VOL spot close stale; cancel-and-repost user=%s product=%s stuck=%.1fs",
                    telegram_id, product, stuck_in_close,
                )
                state["vol_phase"] = "filled_wait_close"
                state["vol_close_digest"] = None
                state["vol_close_posted_ts"] = 0.0
                return {
                    "success": True, "done": False,
                    "action": "close_stale_cancel_for_repost",
                    "orders_placed": 0, "placed_notional_usd": 0.0,
                }

            return {"success": True, "done": False, "action": "waiting_limit_close_fill", "orders_placed": 0, "placed_notional_usd": 0.0}

        entry_sz = float(state.get("vol_entry_size") or MIN_SIZE)
        base_after = _spot_base_balance(client, spot_product_id)
        if base_after > max(MIN_SIZE, entry_sz * 0.05):
            state["vol_phase"] = "filled_wait_close"
            state["vol_close_digest"] = None
            state["vol_close_posted_ts"] = 0.0
            return {"success": True, "done": False, "action": "close_not_filled_retry", "orders_placed": 0, "placed_notional_usd": 0.0}

        close_fill = _entry_fill_data(network, close_digest) or {}
        close_pnl, close_fee = _close_realized_pnl(network, close_digest)
        close_price = float(close_fill.get("fill_price") or mid)
        close_size = float(state.get("vol_close_size") or state.get("vol_entry_size") or MIN_SIZE)
        entry_price = float(state.get("vol_entry_fill_price") or mid)
        entry_size = float(state.get("vol_entry_size") or close_size or MIN_SIZE)
        approx_pnl = (close_price - entry_price) * min(entry_size, close_size)
        if close_pnl is None:
            cycle_pnl = approx_pnl - ((entry_size * entry_price + close_size * close_price) * EST_FEE_RATE)
        else:
            cycle_pnl = close_pnl - close_fee
        traded_notional = (entry_size * entry_price) + (close_size * close_price)
        volume_done += traded_notional
        session_pnl += cycle_pnl
        _record_volume_cycle_metrics(state, cycle_pnl)
        state["volume_done_usd"] = round(volume_done, 4)
        state["volume_remaining_usd"] = round(max(0.0, target_volume - volume_done), 4)
        state["session_realized_pnl_usd"] = round(session_pnl, 6)
        state["vol_phase"] = "idle"
        state["vol_entry_digest"] = None
        state["vol_entry_fill_ts"] = 0.0
        state["vol_entry_fill_price"] = 0.0
        state["vol_entry_size"] = 0.0
        state["vol_close_digest"] = None
        state["vol_close_size"] = 0.0
        state["vol_close_posted_ts"] = 0.0
        state["vol_last_force_close_attempt_ts"] = 0.0
        state["vol_last_order_digest"] = close_digest
        state["vol_last_order_kind"] = "close_filled"

        if sl_usd > 0 and session_pnl <= -sl_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "sl_hit",
                "action": "closed_limit_and_session_sl_hit",
                "detail": f"SL hit after close #{close_digest[:10]}",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
                "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
            }
        if tp_usd > 0 and session_pnl >= tp_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "tp_hit",
                "action": "closed_limit_and_session_tp_hit",
                "detail": f"TP hit after close #{close_digest[:10]}",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
                "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
            }
        if target_volume > 0 and volume_done >= target_volume:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "target_volume_hit",
                "action": "target_volume_hit",
                "detail": f"Target volume reached with order #{close_digest[:10]}",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
                "volume_remaining_usd": 0.0,
            }
        return {
            "success": True,
            "done": False,
            "action": "limit_close_filled_reloop",
            "detail": f"Spot close filled #{close_digest[:10]} · cycle PnL ${cycle_pnl:,.2f}",
            "orders_placed": 0,
            "placed_notional_usd": round(traded_notional, 4),
            "vol_order_attempts": 0,
            "vol_order_failures": 0,
            "session_realized_pnl_usd": round(session_pnl, 6),
            "cycle_realized_pnl_usd": round(cycle_pnl, 6),
            "volume_done_usd": round(volume_done, 4),
            "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
        }

    if phase == "filled_wait_close":
        entry_ts = float(state.get("vol_entry_fill_ts") or 0.0)
        if entry_ts <= 0:
            state["vol_phase"] = "idle"
            return {"success": True, "done": False, "action": "entry_ts_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}
        if bool(state.get("vol_trade_exits_enabled", False)):
            should_close, exit_reason = _volume_exit_reason(state, mid, now_ts, direction)
            if not should_close:
                return {"success": True, "done": False, "action": exit_reason, "orders_placed": 0, "placed_notional_usd": 0.0}
        else:
            exit_reason = "close_timer"
            if now_ts < (entry_ts + CLOSE_AFTER_SECONDS):
                return {"success": True, "done": False, "action": "waiting_close_timer", "orders_placed": 0, "placed_notional_usd": 0.0}

        base = _spot_base_balance(client, spot_product_id)
        if base < MIN_SIZE * 2:
            # Guard against a balance-lookup race. If the entry was very recent and
            # the balance endpoint simply hasn't indexed the fill yet, swallow this
            # cycle and wait another tick rather than resetting to idle (which would
            # cause a second entry while the first is still settling — the "margin
            # stacks" symptom on spot).
            if (now_ts - entry_ts) < SPOT_BALANCE_RACE_GRACE_SECONDS:
                return {
                    "success": True,
                    "done": False,
                    "action": "waiting_balance_settle",
                    "orders_placed": 0,
                    "placed_notional_usd": 0.0,
                }
            state["vol_phase"] = "idle"
            state["vol_entry_digest"] = None
            state["vol_close_digest"] = None
            state["vol_close_posted_ts"] = 0.0
            state["vol_last_force_close_attempt_ts"] = 0.0
            return {"success": True, "done": False, "action": "position_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}

        # Defensive: clear any stale close order before posting a fresh one.
        stale_close_digest = str(state.get("vol_close_digest") or "")
        if stale_close_digest:
            try:
                client.cancel_order(spot_product_id, stale_close_digest)
            except Exception as cancel_err:
                logger.debug(
                    "VOL spot stale close cancel (pre-post) failed user=%s digest=%s err=%s",
                    telegram_id, stale_close_digest[:16], cancel_err,
                )
            state["vol_close_digest"] = None
            state["vol_close_posted_ts"] = 0.0

        pos_size = min(float(state.get("vol_entry_size") or base), base)
        pos_size = max(pos_size, MIN_SIZE)
        logger.info(
            "VOL spot close attempt user=%s network=%s product=%s phase=%s size=%.8f",
            telegram_id,
            network,
            product,
            phase,
            pos_size,
        )
        close_limit = _maker_limit_price(mp, is_buy=False)
        close_result = execute_spot_limit_order(
            telegram_id,
            product,
            pos_size,
            close_limit,
            is_buy=False,
            enforce_rate_limit=False,
            post_only=True,
            reduce_only=True,
            source="vol",
            strategy_session_id=state.get("strategy_session_id"),
            network=network,
            spot_product_id=spot_product_id,
            spot_symbol=spot_symbol,
            asset_label=spot_symbol,
        )
        if not close_result.get("success"):
            close_error = str(close_result.get("error") or "Spot close failed")
            logger.warning(
                "VOL spot close failed user=%s network=%s product=%s error=%s",
                telegram_id,
                network,
                product,
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

        state["vol_phase"] = "pending_close_fill"
        state["vol_close_digest"] = close_result.get("digest")
        state["vol_close_size"] = float(pos_size or 0.0)
        state["vol_close_posted_ts"] = now_ts
        state["vol_last_order_digest"] = str(close_result.get("digest") or "")
        state["vol_last_order_kind"] = "close_posted"
        return {
            "success": True,
            "done": False,
            "action": "placed_limit_close_wait_fill",
            "detail": f"Spot maker sell posted #{str(close_result.get('digest') or '')[:10]} at ${close_limit:,.2f} ({exit_reason})",
            "orders_placed": 1,
            "placed_notional_usd": round(pos_size * close_limit, 4),
            "vol_order_attempts": 1,
            "vol_order_failures": 0,
            "close_digest": close_result.get("digest"),
        }

    # idle
    if target_notional < MIN_NOTIONAL_USD:
        return {
            "success": False,
            "error": (
                f"Volume requires a notional >= ${MIN_NOTIONAL_USD:.2f} per trade "
                f"(configured target ${target_notional:.2f})."
            ),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 0,
            "vol_order_failures": 1,
            "last_order_error": "insufficient_notional",
        }
    if effective_margin <= 0:
        return {
            "success": False,
            "error": (
                f"Wallet quote balance too low for Vol max-leverage margin "
                f"(need ~${target_notional:.2f}/leverage, available balance is empty)."
            ),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 0,
            "vol_order_failures": 1,
            "last_order_error": "insufficient_margin_for_max_lev",
        }
    if bool(state.get("vol_signal_filter_enabled", False)):
        max_spread_bp = float(state.get("vol_max_spread_bp") or DEFAULT_VOL_MAX_SPREAD_BP)
        quality_ok, spread_bp, quality_reason = _quote_quality(mp, mid, max_spread_bp)
        state["vol_spread_bp"] = round(spread_bp, 4)
        if not quality_ok:
            return {
                "success": True,
                "done": False,
                "action": quality_reason,
                "detail": f"VOL spot skipped entry: {quality_reason} (spread {spread_bp:.2f} bp)",
                "orders_placed": 0,
                "placed_notional_usd": 0.0,
                "vol_signal_reason": quality_reason,
                "vol_spread_bp": round(spread_bp, 4),
            }
        signal_state = dict(state)
        signal_state["vol_direction_mode"] = "fixed"
        signal_state["vol_direction"] = "long"
        signal = _volume_signal(product, mid, signal_state)
        state["vol_signal"] = {
            "ok": bool(signal.get("ok")),
            "reason": signal.get("reason"),
            "direction": signal.get("direction"),
            "ema": round(float(signal.get("ema") or 0.0), 6),
            "rsi": round(float(signal.get("rsi") or 0.0), 4),
            "edge_bp": round(float(signal.get("edge_bp") or 0.0), 4),
            "direction_mode": "spot_long",
        }
        if not signal.get("ok"):
            return {
                "success": True,
                "done": False,
                "action": str(signal.get("reason") or "waiting_signal"),
                "detail": "VOL spot skipped entry: long signal filter did not confirm setup.",
                "orders_placed": 0,
                "placed_notional_usd": 0.0,
                "vol_signal": state["vol_signal"],
            }
    # Size by target NOTIONAL (USD) — spot leverage is always 1x so notional == margin here.
    size = max(target_notional / mid, MIN_SIZE)
    entry_limit = _maker_limit_price(mp, is_buy=True)
    logger.info(
        "VOL spot entry user=%s network=%s product=%s size=%.8f limit=%.8f target_notional=%.2f",
        telegram_id,
        network,
        product,
        size,
        entry_limit,
        target_notional,
    )
    open_result = execute_spot_limit_order(
        telegram_id,
        product,
        size,
        entry_limit,
        is_buy=True,
        enforce_rate_limit=False,
        post_only=True,
        reduce_only=False,
        source="vol",
        strategy_session_id=state.get("strategy_session_id"),
        network=network,
        spot_product_id=spot_product_id,
        spot_symbol=spot_symbol,
        asset_label=spot_symbol,
    )
    if not open_result.get("success"):
        open_error = str(open_result.get("error") or "Spot entry failed")
        return {
            "success": False,
            "error": open_error,
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 1,
            "vol_order_failures": 1,
            "last_order_error": open_error,
        }

    entry_price = float(open_result.get("price") or entry_limit or mid)
    entry_size = max(float(open_result.get("size") or size or 0.0), MIN_SIZE)
    state["vol_phase"] = "pending_fill"
    state["vol_entry_digest"] = open_result.get("digest")
    state["vol_entry_size"] = entry_size
    state["vol_entry_fill_price"] = entry_price
    state["vol_entry_fill_ts"] = 0.0
    state["vol_last_order_digest"] = str(open_result.get("digest") or "")
    state["vol_last_order_kind"] = "entry_posted"
    state["volume_remaining_usd"] = round(max(0.0, target_volume - volume_done), 4)
    return {
        "success": True,
        "done": False,
        "action": "opened_limit_wait_fill",
        "detail": f"Spot maker buy posted #{str(open_result.get('digest') or '')[:10]} at ${entry_limit:,.2f}",
        "orders_placed": 1,
        "placed_notional_usd": round(entry_size * entry_price, 4),
        "vol_order_attempts": 1,
        "vol_order_failures": 0,
        "entry_digest": open_result.get("digest"),
        "direction": direction.upper(),
        "entry_price": entry_price,
        "session_realized_pnl_usd": round(session_pnl, 6),
        "volume_done_usd": round(volume_done, 4),
        "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
    }


# === AUDIT COMPLETE: Volume Execution Bot ===
# Status: Improved & Ready
# Key Changes: Session SL/TP now takes priority over target-volume completion, and closed-cycle win-rate/PnL metrics are tracked.


def get_fee_pnl_preview(telegram_id: int, product: str, target_volume_usd: float) -> dict:
    estimated_fees = target_volume_usd * EST_FEE_RATE
    return {
        "target_volume_usd": target_volume_usd,
        "flip_size_usd": TARGET_NOTIONAL_USD,
        "num_flips": max(1, int(target_volume_usd / TARGET_NOTIONAL_USD)),
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
    vol_market = str(state.get("vol_market") or "perp").strip().lower()
    if vol_market not in ("perp", "spot"):
        vol_market = "perp"
    state["vol_market"] = vol_market

    if vol_market == "spot":
        product = normalize_volume_spot_symbol(product)
        state["product"] = product
        direction = "long"
        state["vol_direction"] = "long"
        state["direction"] = "long"
    else:
        direction = _normalize_direction(state.get("vol_direction") or state.get("direction") or "long")
        state["vol_direction"] = direction
        state["direction"] = direction

    # CEO directive: Volume Perp now sizes by NOTIONAL ($100 default) at per-asset
    # MAX leverage; margin shrinks proportionally so post-only refresh quotes can
    # coexist with closing legs. Spot stays at 1x via _resolve_max_leverage.
    target_notional = _resolve_target_notional(state)
    state["target_notional_usd"] = round(target_notional, 4)
    # Legacy mirror — preview cards and copy-trade still read fixed_margin_usd.
    state["fixed_margin_usd"] = round(target_notional, 4)
    fixed_margin = target_notional  # alias retained for downstream call signatures
    max_lev = _resolve_max_leverage(product, network, client, vol_market=vol_market)
    state["leverage"] = max_lev
    state["leverage_mode"] = "MAX"
    required_margin = target_notional / max(1.0, max_lev)
    available_quote = _available_quote_balance(client)
    effective_margin = (
        min(required_margin, max(0.0, available_quote * 0.90))
        if available_quote > 0
        else required_margin
    )
    if effective_margin > 0:
        state["vol_effective_margin_usd"] = round(effective_margin, 4)
    state["vol_required_margin_usd"] = round(required_margin, 4)
    target_volume = float(state.get("target_volume_usd") or DEFAULT_TARGET_VOLUME_USD)
    state["target_volume_usd"] = round(target_volume, 4)

    if vol_market == "spot":
        allowed = list_volume_spot_product_names(network=network, client=client) or []
        # Forgive common aliases: users typing "BTC" on mainnet should resolve to
        # the spot-equivalent symbol (KBTC on current Nado catalog) without forcing
        # them to know the exact listing name.
        if product not in allowed:
            aliases = {"BTC": "KBTC", "ETH": "WETH"}
            aliased = aliases.get(product)
            if aliased and aliased in allowed:
                product = aliased
                state["product"] = product
            elif not allowed:
                return {
                    "success": False,
                    "error": f"Volume spot has no resolvable spot pairs on {network}.",
                }
            else:
                return {
                    "success": False,
                    "error": (
                        f"Volume spot supports only: {', '.join(allowed)} on {network}."
                    ),
                }
        raw_spot_pid = get_spot_product_id(product, network=network, client=client)
        if raw_spot_pid is None:
            return {
                "success": False,
                "error": (
                    f"Could not resolve spot product id for '{product}' on {network}. "
                    f"Available: {', '.join(allowed) or 'none'}."
                ),
            }
        spot_product_id = int(raw_spot_pid)
        meta = get_spot_metadata(product, network=network) or {}
        spot_symbol = str(meta.get("symbol") or product).upper()
        mp = client.get_market_price(spot_product_id)
        mid = float(mp.get("mid") or 0.0)
        if mid <= 0:
            return {"success": False, "error": "Could not fetch spot market price"}
        state["vol_prev_mid"] = mid
        phase = str(state.get("vol_phase") or "idle")
        now_ts = time.time()
        session_pnl = float(state.get("session_realized_pnl_usd") or 0.0)
        volume_done = float(state.get("volume_done_usd") or 0.0)
        tp_pct = float(state.get("tp_pct") or 0.0)
        sl_pct = float(state.get("sl_pct") or 0.0)
        # Session SL/TP applied to NOTIONAL (CEO directive). Otherwise at high leverage
        # the SL would trigger on cents of margin PnL.
        tp_usd = (max(target_notional, MIN_NOTIONAL_USD) * tp_pct / 100.0) if tp_pct > 0 else 0.0
        sl_usd = (max(target_notional, MIN_NOTIONAL_USD) * sl_pct / 100.0) if sl_pct > 0 else 0.0
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
        return _run_volume_spot_cycle(
            telegram_id,
            network,
            state,
            client,
            product,
            spot_product_id,
            spot_symbol,
            mp,
            mid,
            fixed_margin,
            effective_margin,
            target_notional,
            target_volume,
            tp_usd,
            sl_usd,
            session_pnl,
            volume_done,
            phase,
            now_ts,
        )

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
    state["vol_prev_mid"] = mid

    tp_pct = float(state.get("tp_pct") or 0.0)
    sl_pct = float(state.get("sl_pct") or 0.0)
    # Session SL/TP applied to NOTIONAL (CEO directive). Otherwise at high leverage
    # the SL would trigger on cents of margin PnL.
    tp_usd = (max(target_notional, MIN_NOTIONAL_USD) * tp_pct / 100.0) if tp_pct > 0 else 0.0
    sl_usd = (max(target_notional, MIN_NOTIONAL_USD) * sl_pct / 100.0) if sl_pct > 0 else 0.0
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
        hold_seed = int(now_ts) + int(telegram_id) + int(product_id)
        state["vol_hold_seconds"] = _pseudo_random_hold_seconds(
            hold_seed,
            float(state.get("vol_hold_min_seconds") or DEFAULT_VOL_HOLD_MIN_SECONDS),
            float(state.get("vol_hold_max_seconds") or DEFAULT_VOL_HOLD_MAX_SECONDS),
        )
        state["vol_last_order_digest"] = entry_digest
        state["vol_last_order_kind"] = "entry_filled"
        return {
            "success": True,
            "done": False,
            "action": "entry_filled_wait_close",
            "detail": f"Entry filled #{entry_digest[:10]} at ${entry_price:,.2f}",
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
            # Core fix for "compiles and uses up the margin": a post-only close can
            # rest on the book forever in a trending market with the position still
            # open and margin locked. Tier the stuck-close handling:
            #   1) REPOST: after CLOSE_REPOST_AFTER_SECONDS, cancel and re-quote at
            #      a fresh maker price.
            #   2) ESCALATE: after CLOSE_ESCALATE_AFTER_SECONDS since entry, cancel
            #      and force-close via a reduce-only market order so margin is
            #      actually released and the bot can re-enter the next cycle.
            close_posted_ts = float(state.get("vol_close_posted_ts") or 0.0)
            entry_ts = float(state.get("vol_entry_fill_ts") or 0.0)
            stuck_in_close = (now_ts - close_posted_ts) if close_posted_ts > 0 else 0.0
            stuck_since_entry = (now_ts - entry_ts) if entry_ts > 0 else stuck_in_close
            repost_after_s, escalate_after_s = _compute_close_ttl_windows(state, mp, mid)

            if stuck_since_entry >= escalate_after_s:
                last_force_ts = float(state.get("vol_last_force_close_attempt_ts") or 0.0)
                if last_force_ts > 0 and (now_ts - last_force_ts) < FORCE_CLOSE_RETRY_COOLDOWN_SECONDS:
                    return {
                        "success": True,
                        "done": False,
                        "action": "waiting_force_close_cooldown",
                        "orders_placed": 0,
                        "placed_notional_usd": 0.0,
                    }
                try:
                    client.cancel_order(product_id, close_digest)
                except Exception as cancel_err:
                    logger.warning(
                        "VOL close cancel-before-escalate failed user=%s product=%s digest=%s err=%s",
                        telegram_id, product, close_digest[:16], cancel_err,
                    )
                pos_for_escalate = _load_position(client, product_id)
                if pos_for_escalate:
                    escalate_size = abs(float(pos_for_escalate.get("amount", 0.0) or 0.0))
                    escalate_side = str(pos_for_escalate.get("side", "") or "").upper()
                    escalate_is_long = escalate_side == "SHORT"
                    state["vol_last_force_close_attempt_ts"] = now_ts
                    if escalate_size < MIN_SIZE:
                        state["vol_phase"] = "filled_wait_close"
                        state["vol_close_digest"] = None
                        state["vol_close_posted_ts"] = 0.0
                        return {
                            "success": True,
                            "done": False,
                            "action": "close_escalate_size_too_small_retry",
                            "orders_placed": 0,
                            "placed_notional_usd": 0.0,
                        }
                    logger.warning(
                        "VOL force-close (IOC reduce_only) user=%s product=%s stuck_seconds=%.1f size=%.8f",
                        telegram_id, product, stuck_since_entry, escalate_size,
                    )
                    force_res = execute_market_order(
                        telegram_id,
                        product,
                        escalate_size,
                        is_long=escalate_is_long,
                        leverage=float(state.get("leverage") or 1.0),
                        reduce_only=True,
                        enforce_rate_limit=False,
                        source="vol",
                        strategy_session_id=state.get("strategy_session_id"),
                    )
                    if force_res.get("success"):
                        state["vol_close_digest"] = force_res.get("digest")
                        state["vol_close_posted_ts"] = now_ts
                        state["vol_last_order_digest"] = str(force_res.get("digest") or "")
                        state["vol_last_order_kind"] = "close_escalated_ioc"
                        return {
                            "success": True,
                            "done": False,
                            "action": "close_escalated_force_close",
                            "detail": f"Force-closed stuck position with IOC reduce-only (stuck {stuck_since_entry:.0f}s)",
                            "orders_placed": 1,
                            "placed_notional_usd": round(escalate_size * mid, 4),
                            "vol_order_attempts": 1,
                            "vol_order_failures": 0,
                        }
                    # Force-close failed — fall through to repost path below
                    logger.warning(
                        "VOL force-close IOC failed user=%s product=%s err=%s",
                        telegram_id, product, str(force_res.get("error"))[:200],
                    )
                state["vol_phase"] = "filled_wait_close"
                state["vol_close_digest"] = None
                state["vol_close_posted_ts"] = 0.0
                return {
                    "success": True, "done": False,
                    "action": "close_escalate_fallback_retry",
                    "orders_placed": 0, "placed_notional_usd": 0.0,
                }

            if stuck_in_close >= repost_after_s:
                try:
                    client.cancel_order(product_id, close_digest)
                except Exception as cancel_err:
                    logger.warning(
                        "VOL close cancel-for-repost failed user=%s product=%s digest=%s err=%s",
                        telegram_id, product, close_digest[:16], cancel_err,
                    )
                logger.info(
                    "VOL close stale; cancel-and-repost user=%s product=%s stuck=%.1fs",
                    telegram_id, product, stuck_in_close,
                )
                state["vol_phase"] = "filled_wait_close"
                state["vol_close_digest"] = None
                state["vol_close_posted_ts"] = 0.0
                return {
                    "success": True, "done": False,
                    "action": "close_stale_cancel_for_repost",
                    "orders_placed": 0, "placed_notional_usd": 0.0,
                }

            return {"success": True, "done": False, "action": "waiting_limit_close_fill", "orders_placed": 0, "placed_notional_usd": 0.0}

        pos = _load_position(client, product_id)
        if pos:
            state["vol_phase"] = "filled_wait_close"
            state["vol_close_digest"] = None
            state["vol_close_posted_ts"] = 0.0
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
        _record_volume_cycle_metrics(state, cycle_pnl)
        state["volume_done_usd"] = round(volume_done, 4)
        state["volume_remaining_usd"] = round(max(0.0, target_volume - volume_done), 4)
        state["session_realized_pnl_usd"] = round(session_pnl, 6)
        state["vol_phase"] = "idle"
        state["vol_entry_digest"] = None
        state["vol_entry_fill_ts"] = 0.0
        state["vol_entry_fill_price"] = 0.0
        state["vol_entry_size"] = 0.0
        state["vol_close_digest"] = None
        state["vol_close_size"] = 0.0
        state["vol_close_posted_ts"] = 0.0
        state["vol_last_force_close_attempt_ts"] = 0.0
        state["vol_last_order_digest"] = close_digest
        state["vol_last_order_kind"] = "close_filled"

        if sl_usd > 0 and session_pnl <= -sl_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "sl_hit",
                "action": "closed_limit_and_session_sl_hit",
                "detail": f"SL hit after close #{close_digest[:10]}",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
                "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
            }
        if tp_usd > 0 and session_pnl >= tp_usd:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "tp_hit",
                "action": "closed_limit_and_session_tp_hit",
                "detail": f"TP hit after close #{close_digest[:10]}",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
                "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
            }
        if target_volume > 0 and volume_done >= target_volume:
            state["running"] = False
            return {
                "success": True,
                "done": True,
                "stop_reason": "target_volume_hit",
                "action": "target_volume_hit",
                "detail": f"Target volume reached with order #{close_digest[:10]}",
                "orders_placed": 0,
                "placed_notional_usd": round(traded_notional, 4),
                "vol_order_attempts": 0,
                "vol_order_failures": 0,
                "session_realized_pnl_usd": round(session_pnl, 6),
                "cycle_realized_pnl_usd": round(cycle_pnl, 6),
                "volume_done_usd": round(volume_done, 4),
                "volume_remaining_usd": 0.0,
            }
        return {
            "success": True,
            "done": False,
            "action": "limit_close_filled_reloop",
            "detail": f"Close filled #{close_digest[:10]} · cycle PnL ${cycle_pnl:,.2f}",
            "orders_placed": 0,
            "placed_notional_usd": round(traded_notional, 4),
            "vol_order_attempts": 0,
            "vol_order_failures": 0,
            "session_realized_pnl_usd": round(session_pnl, 6),
            "cycle_realized_pnl_usd": round(cycle_pnl, 6),
            "volume_done_usd": round(volume_done, 4),
            "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
        }

    if phase == "filled_wait_close":
        entry_ts = float(state.get("vol_entry_fill_ts") or 0.0)
        if entry_ts <= 0:
            state["vol_phase"] = "idle"
            return {"success": True, "done": False, "action": "entry_ts_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}
        if bool(state.get("vol_trade_exits_enabled", False)):
            should_close, exit_reason = _volume_exit_reason(state, mid, now_ts, direction)
            if not should_close:
                return {"success": True, "done": False, "action": exit_reason, "orders_placed": 0, "placed_notional_usd": 0.0}
        else:
            exit_reason = "close_timer"
            if now_ts < (entry_ts + CLOSE_AFTER_SECONDS):
                return {"success": True, "done": False, "action": "waiting_close_timer", "orders_placed": 0, "placed_notional_usd": 0.0}

        pos = _load_position(client, product_id)
        if not pos:
            state["vol_phase"] = "idle"
            state["vol_entry_digest"] = None
            state["vol_close_digest"] = None
            state["vol_close_posted_ts"] = 0.0
            state["vol_last_force_close_attempt_ts"] = 0.0
            return {"success": True, "done": False, "action": "position_missing_reset", "orders_placed": 0, "placed_notional_usd": 0.0}

        # Defensive: if a stale close_digest lingered (e.g., from a prior cycle that
        # transitioned back after a cancel-and-repost), make sure it's off the book
        # so we don't leave two reduce-only quotes resting simultaneously.
        stale_close_digest = str(state.get("vol_close_digest") or "")
        if stale_close_digest:
            try:
                client.cancel_order(product_id, stale_close_digest)
            except Exception as cancel_err:
                logger.debug(
                    "VOL stale close cancel (pre-post) failed user=%s product=%s digest=%s err=%s",
                    telegram_id, product, stale_close_digest[:16], cancel_err,
                )
            state["vol_close_digest"] = None
            state["vol_close_posted_ts"] = 0.0

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
        close_limit = _maker_limit_price(mp, is_buy=close_is_long)
        close_result = execute_limit_order(
            telegram_id,
            product,
            pos_size,
            close_limit,
            is_long=close_is_long,
            leverage=float(state.get("leverage") or 1.0),
            reduce_only=True,
            enforce_rate_limit=False,
            post_only=True,
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
        state["vol_close_posted_ts"] = now_ts
        state["vol_last_order_digest"] = str(close_result.get("digest") or "")
        state["vol_last_order_kind"] = "close_posted"
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
            "detail": f"Maker close posted #{str(close_result.get('digest') or '')[:10]} at ${close_limit:,.2f} ({exit_reason})",
            "orders_placed": 1,
            "placed_notional_usd": round(pos_size * close_limit, 4),
            "vol_order_attempts": 1,
            "vol_order_failures": 0,
            "close_digest": close_result.get("digest"),
        }

    # phase == idle
    if target_notional < MIN_NOTIONAL_USD:
        return {
            "success": False,
            "error": (
                f"Volume requires a notional >= ${MIN_NOTIONAL_USD:.2f} per trade "
                f"(configured target ${target_notional:.2f})."
            ),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 0,
            "vol_order_failures": 1,
            "last_order_error": "insufficient_notional",
        }
    if effective_margin <= 0:
        return {
            "success": False,
            "error": (
                f"Wallet quote balance too low for Vol max-leverage margin "
                f"(need ~${(target_notional / max(1.0, float(state.get('leverage') or 1.0))):.2f}, available ${available_quote:.2f})."
            ),
            "orders_placed": 0,
            "placed_notional_usd": 0.0,
            "vol_order_attempts": 0,
            "vol_order_failures": 1,
            "last_order_error": "insufficient_margin_for_max_lev",
        }
    if bool(state.get("vol_signal_filter_enabled", False)):
        max_spread_bp = float(state.get("vol_max_spread_bp") or DEFAULT_VOL_MAX_SPREAD_BP)
        quality_ok, spread_bp, quality_reason = _quote_quality(mp, mid, max_spread_bp)
        state["vol_spread_bp"] = round(spread_bp, 4)
        if not quality_ok:
            return {
                "success": True,
                "done": False,
                "action": quality_reason,
                "detail": f"VOL skipped entry: {quality_reason} (spread {spread_bp:.2f} bp)",
                "orders_placed": 0,
                "placed_notional_usd": 0.0,
                "vol_signal_reason": quality_reason,
                "vol_spread_bp": round(spread_bp, 4),
            }

        signal = _volume_signal(product, mid, state)
        state["vol_signal"] = {
            "ok": bool(signal.get("ok")),
            "reason": signal.get("reason"),
            "direction": signal.get("direction"),
            "ema": round(float(signal.get("ema") or 0.0), 6),
            "rsi": round(float(signal.get("rsi") or 0.0), 4),
            "edge_bp": round(float(signal.get("edge_bp") or 0.0), 4),
            "direction_mode": signal.get("direction_mode"),
        }
        if not signal.get("ok"):
            return {
                "success": True,
                "done": False,
                "action": str(signal.get("reason") or "waiting_signal"),
                "detail": "VOL skipped entry: signal filter did not confirm a positive-expectancy setup.",
                "orders_placed": 0,
                "placed_notional_usd": 0.0,
                "vol_signal": state["vol_signal"],
            }
        direction = _normalize_direction(signal.get("direction") or direction)
        state["vol_direction"] = direction
        state["direction"] = direction
    # Size by target NOTIONAL (USD) at max leverage. e.g. $100 notional / 50x = $2 margin.
    size = max(target_notional / mid, MIN_SIZE)
    is_long = direction == "long"
    entry_limit = _maker_limit_price(mp, is_buy=is_long)
    logger.info(
        "VOL entry order attempt user=%s network=%s product=%s phase=%s direction=%s "
        "size=%.8f limit=%.8f target_notional=%.2f leverage=%.1fx margin=%.2f",
        telegram_id,
        network,
        product,
        phase,
        direction,
        size,
        entry_limit,
        target_notional,
        float(state.get("leverage") or 1.0),
        effective_margin,
    )
    open_result = execute_limit_order(
        telegram_id,
        product,
        size,
        entry_limit,
        is_long=is_long,
        leverage=float(state.get("leverage") or 1.0),
        enforce_rate_limit=False,
        post_only=True,
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
    state["vol_last_order_digest"] = str(open_result.get("digest") or "")
    state["vol_last_order_kind"] = "entry_posted"
    state["volume_remaining_usd"] = round(max(0.0, target_volume - volume_done), 4)
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
        "detail": f"Maker entry posted #{str(open_result.get('digest') or '')[:10]} at ${entry_limit:,.2f}",
        "orders_placed": 1,
        "placed_notional_usd": round(entry_size * entry_price, 4),
        "vol_order_attempts": 1,
        "vol_order_failures": 0,
        "entry_digest": open_result.get("digest"),
        "direction": direction.upper(),
        "entry_price": entry_price,
        "session_realized_pnl_usd": round(session_pnl, 6),
        "volume_done_usd": round(volume_done, 4),
        "volume_remaining_usd": round(max(0.0, target_volume - volume_done), 4),
    }
