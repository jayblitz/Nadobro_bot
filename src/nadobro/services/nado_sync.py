from __future__ import annotations

import asyncio
import logging
import time
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.nadobro.db import execute, query_all, query_one
from src.nadobro.services.feature_flags import portfolio_ws_enabled
from src.nadobro.services.portfolio_calculator import aggregate_trading_stats, positions_from_account_summary
from src.nadobro.services.user_service import get_user, get_user_nado_client
from src.nadobro.utils.x18 import from_x18

logger = logging.getLogger(__name__)

_snapshot_cache: dict[tuple[int, str], dict[str, Any]] = {}
_inflight: dict[tuple[int, str], asyncio.Lock] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _cache_key(user_id: int, network: str) -> tuple[int, str]:
    return int(user_id), _normalize_network(network)


def clear_cache(user_id: int | None = None, network: str | None = None) -> None:
    if user_id is None:
        _snapshot_cache.clear()
        return
    normalized_network = _normalize_network(network) if network is not None else None
    for key in list(_snapshot_cache.keys()):
        if key[0] == int(user_id) and (normalized_network is None or key[1] == normalized_network):
            _snapshot_cache.pop(key, None)


def get_cached_snapshot(user_id: int, network: str | None = None) -> dict[str, Any] | None:
    if network is None:
        user = get_user(user_id)
        network = user.network_mode.value if user else "mainnet"
    snapshot = _snapshot_cache.get(_cache_key(user_id, network))
    return deepcopy(snapshot) if snapshot else None


def set_cached_snapshot(user_id: int, network: str, snapshot: dict[str, Any]) -> None:
    cached = dict(snapshot)
    cached.setdefault("user_id", int(user_id))
    cached.setdefault("network", network)
    _snapshot_cache[_cache_key(user_id, network)] = deepcopy(cached)


def mark_user_active(user_id: int) -> None:
    try:
        execute("UPDATE users SET last_active = now() WHERE telegram_id = %s", (int(user_id),))
    except Exception:
        logger.debug("mark_user_active failed user=%s", user_id, exc_info=True)


def active_users() -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT DISTINCT u.telegram_id, u.network_mode AS network
        FROM users u
        WHERE u.main_address IS NOT NULL
          AND (
            u.last_active >= now() - interval '60 minutes'
            OR EXISTS (
              SELECT 1 FROM positions p
              WHERE p.user_id = u.telegram_id
                AND p.network = u.network_mode
                AND p.closed_at IS NULL
            )
            OR EXISTS (
              SELECT 1 FROM open_orders o
              WHERE o.user_id = u.telegram_id
                AND o.network = u.network_mode
                AND o.status IN ('open', 'pending', 'armed')
            )
            OR EXISTS (
              SELECT 1 FROM strategy_sessions s
              WHERE s.user_id = u.telegram_id
                AND s.network = u.network_mode
                AND s.status = 'running'
            )
          )
        ORDER BY u.last_active DESC NULLS LAST
        LIMIT 200
        """
    )


async def sync_active_users(reason: str = "poll") -> None:
    try:
        rows = await asyncio.to_thread(active_users)
    except Exception as exc:
        logger.warning("portfolio active user query failed: %s", exc)
        return
    for row in rows:
        user_id = int(row.get("telegram_id"))
        network = str(row.get("network") or "mainnet")
        await sync_user(user_id, network=network, reason=reason)


async def sync_user(user_id: int, *, network: str | None = None, reason: str = "manual", force: bool = False) -> dict[str, Any]:
    user = await asyncio.to_thread(get_user, int(user_id))
    network = _normalize_network(network or (user.network_mode.value if user else "mainnet"))
    key = _cache_key(user_id, network)
    if key not in _inflight and len(_inflight) > 1000:
        _inflight.clear()
    lock = _inflight.setdefault(key, asyncio.Lock())
    async with lock:
        if not force:
            cached = _snapshot_cache.get(key)
            if cached and time.time() - float(cached.get("monotonic_ts", 0)) < 2:
                return deepcopy(cached)

        started = time.perf_counter()
        try:
            client = await asyncio.to_thread(get_user_nado_client, int(user_id), network)
            if not client:
                raise RuntimeError("Nado client unavailable")
            if portfolio_ws_enabled() and not str(reason).startswith("ws"):
                try:
                    from src.nadobro.services.nado_ws import PortfolioWsSubscription, portfolio_ws

                    subaccount = getattr(client, "subaccount_hex", None)
                    if subaccount:
                        portfolio_ws.subscribe(PortfolioWsSubscription(int(user_id), network, str(subaccount)))
                except Exception:
                    logger.debug("portfolio ws subscribe failed user=%s network=%s", user_id, network, exc_info=True)

            summary, orders, trigger_orders, matches, funding = await asyncio.gather(
                client.calculate_account_summary(ts=int(time.time())),
                asyncio.to_thread(client.get_all_open_orders, True),
                client.get_trigger_orders(limit=200),
                client.get_matches(limit=200),
                client.get_interest_and_funding_payments(limit=200),
            )
            plain_orders = _normalize_order_rows(orders)
            trigger_rows = [_mark_trigger_order(o) for o in _normalize_order_rows(trigger_orders)]
            all_orders = plain_orders + trigger_rows
            positions = [p.to_dict() for p in positions_from_account_summary(summary or {})]
            stats = aggregate_trading_stats(matches or [], funding or [])
            snapshot = {
                "user_id": int(user_id),
                "network": network,
                "summary": summary or {},
                "positions": positions,
                "open_orders": all_orders,
                "matches": matches or [],
                "funding_payments": funding or [],
                "stats": stats,
                "last_sync": _now(),
                "monotonic_ts": time.time(),
                "stale": False,
                "reason": reason,
            }
            await asyncio.to_thread(_write_snapshot, snapshot, int((time.perf_counter() - started) * 1000))
            _snapshot_cache[key] = deepcopy(snapshot)
            return deepcopy(snapshot)
        except Exception as exc:
            logger.warning("portfolio sync failed user=%s network=%s reason=%s: %s", user_id, network, reason, exc)
            stale = deepcopy(_snapshot_cache.get(key) or {"user_id": int(user_id), "network": network})
            stale.update({"stale": True, "error": str(exc), "last_error_at": _now(), "monotonic_ts": time.time()})
            _snapshot_cache[key] = deepcopy(stale)
            await asyncio.to_thread(_write_sync_log_error, int(user_id), network, int((time.perf_counter() - started) * 1000), str(exc))
            return stale


def _write_snapshot(snapshot: dict[str, Any], duration_ms: int) -> None:
    user_id = int(snapshot["user_id"])
    network = _normalize_network(snapshot["network"])
    positions = list(snapshot.get("positions") or [])
    orders = list(snapshot.get("open_orders") or [])
    matches = list(snapshot.get("matches") or [])
    funding = list(snapshot.get("funding_payments") or [])

    _write_positions(user_id, network, positions)
    _write_open_orders(user_id, network, orders)
    fills_inserted = _write_matches(user_id, network, matches)
    funding_inserted = _write_funding(user_id, network, funding)
    execute(
        """
        INSERT INTO sync_log (
          user_id, network, positions_seen, positions_closed, orders_seen,
          orders_cleared, fills_inserted, funding_inserted, duration_ms
        )
        VALUES (%s, %s, %s, 0, %s, 0, %s, %s, %s)
        """,
        (user_id, network, len(positions), len(orders), fills_inserted, funding_inserted, duration_ms),
    )


def _write_positions(user_id: int, network: str, positions: list[dict[str, Any]]) -> None:
    live_keys = [(int(p.get("product_id") or 0), bool(p.get("isolated"))) for p in positions]
    if live_keys:
        clauses = " OR ".join(["(product_id = %s AND isolated = %s)" for _ in live_keys])
        params: list[Any] = [user_id, network]
        for product_id, isolated in live_keys:
            params.extend([product_id, isolated])
        execute(
            f"""
            UPDATE positions
            SET closed_at = COALESCE(closed_at, now()), synced_at = now(), status = 'closed'
            WHERE user_id = %s
              AND network = %s
              AND closed_at IS NULL
              AND NOT ({clauses})
            """,
            tuple(params),
        )
    else:
        execute(
            """
            UPDATE positions
            SET closed_at = COALESCE(closed_at, now()), synced_at = now(), status = 'closed'
            WHERE user_id = %s AND network = %s AND closed_at IS NULL
            """,
            (user_id, network),
        )

    for pos in positions:
        product_id = int(pos.get("product_id") or 0)
        isolated = bool(pos.get("isolated"))
        pair = str(pos.get("symbol") or pos.get("product_name") or f"ID:{product_id}")
        side = "long" if bool(pos.get("is_long", True)) else "short"
        amount = abs(Decimal(str(pos.get("amount") or 0)))
        execute(
            """
            UPDATE positions
            SET pair = %s, side = %s, size = %s, notional_value = %s,
                avg_entry_price = %s, est_liq_price = %s, est_pnl = %s,
                margin_used = %s, leverage = %s, synced_at = now(), status = 'open'
            WHERE user_id = %s AND network = %s AND product_id = %s
              AND isolated = %s AND closed_at IS NULL
            """,
            (
                pair,
                side,
                str(amount),
                _decimal_or_none(pos.get("notional_value")),
                _decimal_or_none(pos.get("avg_entry_price")),
                _decimal_or_none(pos.get("est_liq_price")),
                _decimal_or_none(pos.get("est_pnl")),
                _decimal_or_none(pos.get("margin_used")),
                _decimal_or_none(pos.get("leverage")),
                user_id,
                network,
                product_id,
                isolated,
            ),
        )
        execute(
            """
            INSERT INTO positions (
              user_id, network, product_id, isolated, pair, side, size,
              notional_value, avg_entry_price, est_liq_price, est_pnl,
              margin_used, leverage, status, opened_at, synced_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'open', now(), now()
            WHERE NOT EXISTS (
              SELECT 1 FROM positions
              WHERE user_id = %s AND network = %s AND product_id = %s
                AND isolated = %s AND closed_at IS NULL
            )
            """,
            (
                user_id,
                network,
                product_id,
                isolated,
                pair,
                side,
                str(amount),
                _decimal_or_none(pos.get("notional_value")),
                _decimal_or_none(pos.get("avg_entry_price")),
                _decimal_or_none(pos.get("est_liq_price")),
                _decimal_or_none(pos.get("est_pnl")),
                _decimal_or_none(pos.get("margin_used")),
                _decimal_or_none(pos.get("leverage")),
                user_id,
                network,
                product_id,
                isolated,
            ),
        )


def _write_open_orders(user_id: int, network: str, orders: list[dict[str, Any]]) -> None:
    digests = [str(o.get("digest") or o.get("order_digest") or "") for o in orders if o.get("digest") or o.get("order_digest")]
    if digests:
        placeholders = ", ".join(["%s"] * len(digests))
        execute(
            f"""
            UPDATE open_orders
            SET status = 'cancelled_or_filled', synced_at = now()
            WHERE user_id = %s AND network = %s AND status IN ('open', 'pending', 'armed')
              AND order_digest NOT IN ({placeholders})
            """,
            (user_id, network, *digests),
        )
    elif not orders:
        execute(
            """
            UPDATE open_orders
            SET status = 'cancelled_or_filled', synced_at = now()
            WHERE user_id = %s AND network = %s AND status IN ('open', 'pending', 'armed')
            """,
            (user_id, network),
        )
    else:
        logger.warning(
            "portfolio sync saw %s orders without stable digests; skipping stale order sweep user=%s network=%s",
            len(orders),
            user_id,
            network,
        )
    for order in orders:
        digest = str(order.get("digest") or order.get("order_digest") or "")
        if not digest:
            continue
        product_id = order.get("product_id")
        pair = str(order.get("product_name") or order.get("product") or (f"ID:{product_id}" if product_id else ""))
        execute(
            """
            INSERT INTO open_orders (
              user_id, network, product_id, pair, side, order_type, size, price,
              order_digest, status, placed_at, updated_at, synced_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'open', now(), now(), now())
            ON CONFLICT (user_id, network, order_digest)
            DO UPDATE SET product_id = EXCLUDED.product_id, pair = EXCLUDED.pair,
              side = EXCLUDED.side, order_type = EXCLUDED.order_type, size = EXCLUDED.size,
              price = EXCLUDED.price, status = 'open', updated_at = now(), synced_at = now()
            """,
            (
                user_id,
                network,
                product_id,
                pair,
                str(order.get("side") or "").lower() or "unknown",
                str(order.get("type") or order.get("order_type") or "limit").lower(),
                str(abs(Decimal(str(order.get("amount") or order.get("size") or 0)))),
                _decimal_or_none(order.get("price") or order.get("limit_price")),
                digest,
            ),
        )


def _write_matches(user_id: int, network: str, matches: list[dict[str, Any]]) -> int:
    inserted = 0
    table = f"trades_{_network_table_suffix(network)}"
    for match in matches:
        submission_idx = match.get("submission_idx")
        if submission_idx is None:
            continue
        order = match.get("order") or {}
        product_id = match.get("product_id") or order.get("product_id") or 0
        row = query_one(f"SELECT id FROM {table} WHERE submission_idx = %s LIMIT 1", (submission_idx,))
        if row:
            continue
        base_x18 = _x18_field(match, "base_filled", "base_filled_x18")
        quote_x18 = _x18_field(match, "quote_filled", "quote_filled_x18")
        fee_x18 = _x18_field(match, "fee", "fee_x18")
        pnl_x18 = _x18_field(match, "realized_pnl", "realized_pnl_x18")
        base_amount = from_x18(base_x18)
        execute(
            f"""
            INSERT INTO {table} (
              user_id, product_id, product_name, order_type, side, size, status,
              submission_idx, isolated, realized_pnl_x18, fee_x18, base_filled_x18, quote_filled_x18,
              filled_at, created_at
            )
            VALUES (%s, %s, %s, 'match', %s, %s, 'filled', %s, %s, %s, %s, %s, %s, %s, now())
            """,
            (
                user_id,
                int(product_id or 0),
                str(match.get("product_name") or f"ID:{product_id}"),
                "long" if base_amount >= 0 else "short",
                str(abs(base_amount)),
                submission_idx,
                bool(match.get("isolated")),
                pnl_x18,
                fee_x18,
                base_x18,
                quote_x18,
                _timestamp_or_now(match.get("timestamp")),
            ),
        )
        inserted += 1
    return inserted


def _write_funding(user_id: int, network: str, payments: list[dict[str, Any]]) -> int:
    inserted = 0
    table = f"funding_payments_{_network_table_suffix(network)}"
    for payment in payments:
        if str(payment.get("type") or "funding") != "funding":
            continue
        product_id = int(payment.get("product_id") or 0)
        paid_at = _timestamp_or_now(payment.get("timestamp"))
        amount_x18 = _x18_field(payment, "amount", "amount_x18")
        row = query_one(
            f"SELECT id FROM {table} WHERE user_id = %s AND product_id = %s AND paid_at = %s AND amount_x18 = %s LIMIT 1",
            (user_id, product_id, paid_at, amount_x18),
        )
        if row:
            continue
        execute(
            f"""
            INSERT INTO {table} (
              user_id, product_id, amount_x18, balance_amount_x18,
              rate_x18, oracle_price_x18, paid_at, synced_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            """,
            (
                user_id,
                product_id,
                amount_x18,
                payment.get("balance_amount") or payment.get("balance_amount_x18"),
                payment.get("rate_x18"),
                payment.get("oracle_price_x18"),
                paid_at,
            ),
        )
        inserted += 1
    return inserted


def _write_sync_log_error(user_id: int, network: str, duration_ms: int, error: str) -> None:
    try:
        execute(
            "INSERT INTO sync_log (user_id, network, duration_ms, error) VALUES (%s, %s, %s, %s)",
            (user_id, network, duration_ms, error[:1000]),
        )
    except Exception:
        logger.debug("failed to write portfolio sync error log", exc_info=True)


def _decimal_or_none(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(Decimal(str(value)))


def _timestamp_or_now(value: Any) -> datetime:
    if value is None:
        return _now()
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = str(value)
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return _now()


def _normalize_network(network: Any) -> str:
    text = str(network or "mainnet").lower()
    return "testnet" if text == "testnet" else "mainnet"


def _network_table_suffix(network: Any) -> str:
    normalized = _normalize_network(network)
    if normalized not in {"mainnet", "testnet"}:
        raise ValueError(f"unsupported network: {network}")
    return normalized


def _normalize_order_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(row) for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        rows = value.get("orders") or value.get("data") or []
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
    return []


def _x18_field(row: dict[str, Any], human_key: str, x18_key: str) -> str:
    if row.get(x18_key) is not None:
        return str(row.get(x18_key))
    value = row.get(human_key)
    if value is None or value == "":
        return "0"
    text = str(value)
    if any(ch in text for ch in ".eE"):
        return str(int(Decimal(text) * Decimal("1000000000000000000")))
    return text


def _mark_trigger_order(order: dict[str, Any]) -> dict[str, Any]:
    row = dict(order or {})
    row["type"] = str(row.get("type") or row.get("order_type") or "trigger").upper()
    row["is_trigger"] = True
    return row
