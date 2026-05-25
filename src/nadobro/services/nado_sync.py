from __future__ import annotations

import asyncio
import logging
import time
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.nadobro.db import execute, query_all, query_one
from src.nadobro.config import NADO_MAINNET_REST, NADO_TESTNET_REST
from src.nadobro.services.feature_flags import (
    portfolio_heavy_sync_seconds,
    portfolio_poll_cache_seconds,
    portfolio_sync_interval_seconds,
    portfolio_sync_users_per_tick,
    portfolio_ws_enabled,
)
from src.nadobro.services.portfolio_calculator import (
    aggregate_trading_stats,
    compute_total_equity,
    positions_from_account_summary,
)
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


def _user_has_isolated_artifacts(snapshot: dict[str, Any] | None) -> bool:
    """True if the prior snapshot shows any isolated-margin position or order.
    Used by the poller to decide whether to pay for the isolated-subaccount
    fan-out on this tick."""
    if not snapshot:
        return False
    for position in snapshot.get("positions") or []:
        if isinstance(position, dict) and (position.get("isolated") or position.get("subaccount")):
            return True
    for order in snapshot.get("open_orders") or []:
        if isinstance(order, dict) and (order.get("isolated") or order.get("subaccount")):
            return True
    return False


def _gateway_circuit_open(network: str) -> bool:
    try:
        from src.nadobro.services.http_session import is_circuit_open

        gateway = NADO_MAINNET_REST if _normalize_network(network) == "mainnet" else NADO_TESTNET_REST
        return bool(is_circuit_open(gateway))
    except Exception:
        return False


def mark_user_active(user_id: int) -> None:
    try:
        execute("UPDATE users SET last_active = now() WHERE telegram_id = %s", (int(user_id),))
    except Exception:
        logger.debug("mark_user_active failed user=%s", user_id, exc_info=True)


_ACTIVE_USERS_PAGE_SIZE = portfolio_sync_users_per_tick()
# Module-level cursor for active_users pagination. Lives in the process so the
# scheduler walks all active users across ticks without re-syncing the same
# top-N every iteration. Reset to 0 when we reach the end (handled below).
_active_users_cursor: int = 0


def active_users(limit: int | None = None, after_user_id: int | None = None) -> list[dict[str, Any]]:
    """Return active users on this network, paginated.

    SCALE: previously hard-capped at LIMIT 200, which silently dropped every
    other user once we crossed that count. Now we paginate by ``telegram_id``
    (stable across calls) and the scheduler walks the full set across ticks.
    """
    page = int(limit if limit is not None else _ACTIVE_USERS_PAGE_SIZE)
    cursor = int(after_user_id or 0)
    return query_all(
        """
        SELECT u.telegram_id, u.network_mode AS network, u.last_active
        FROM users u
        WHERE u.main_address IS NOT NULL
          AND u.telegram_id > %s
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
        ORDER BY u.telegram_id ASC
        LIMIT %s
        """,
        (cursor, page),
    )


async def sync_active_users(reason: str = "poll") -> None:
    """Walk active users in stable cursor order, one page per tick.

    Each scheduler invocation advances ``_active_users_cursor`` so we never
    re-sync the same top-N users every tick. Page size and poll interval are
    tunable via env (defaults: 8 users / 30s) to stay under Cloudflare limits.
    """
    global _active_users_cursor

    if reason == "poll":
        try:
            from src.nadobro.services.http_session import is_circuit_open

            if is_circuit_open(NADO_MAINNET_REST) and is_circuit_open(NADO_TESTNET_REST):
                logger.debug("portfolio sync skipped: Cloudflare circuit open on both gateways")
                return
        except Exception:
            pass

    try:
        rows = await asyncio.to_thread(active_users, _ACTIVE_USERS_PAGE_SIZE, _active_users_cursor)
    except Exception as exc:
        logger.warning("portfolio active user query failed: %s", exc)
        return
    if not rows:
        _active_users_cursor = 0
        return

    poll_cache_ms = portfolio_poll_cache_seconds() * 1000 if reason == "poll" else None
    tick_budget = max(5.0, portfolio_sync_interval_seconds() * 0.85)
    deadline = time.monotonic() + tick_budget
    synced = 0

    for row in rows:
        if time.monotonic() >= deadline:
            logger.debug(
                "portfolio sync tick budget exhausted after %d users (budget=%.0fs)",
                synced,
                tick_budget,
            )
            break
        user_id = int(row.get("telegram_id"))
        network = str(row.get("network") or "mainnet")
        await sync_user(
            user_id,
            network=network,
            reason=reason,
            max_age_ms=poll_cache_ms,
        )
        _active_users_cursor = max(_active_users_cursor, user_id)
        synced += 1


async def sync_user(
    user_id: int,
    *,
    network: str | None = None,
    reason: str = "manual",
    force: bool = False,
    max_age_ms: int | None = None,
) -> dict[str, Any]:
    user = await asyncio.to_thread(get_user, int(user_id))
    network = _normalize_network(network or (user.network_mode.value if user else "mainnet"))
    key = _cache_key(user_id, network)
    if key not in _inflight and len(_inflight) > 1000:
        _inflight.clear()
    lock = _inflight.setdefault(key, asyncio.Lock())
    async with lock:
        if max_age_ms is None:
            if reason == "poll":
                max_age_ms = portfolio_poll_cache_seconds() * 1000
            elif reason in ("cold_render", "refresh"):
                max_age_ms = 5000
            else:
                max_age_ms = 2000

        cache_ttl = max(0.0, float(max_age_ms) / 1000.0) if max_age_ms is not None else 0.0
        if not force and cache_ttl > 0:
            cached = _snapshot_cache.get(key)
            if cached and time.time() - float(cached.get("monotonic_ts", 0)) < cache_ttl:
                return deepcopy(cached)

        if _gateway_circuit_open(network):
            cached = _snapshot_cache.get(key)
            if cached:
                stale = deepcopy(cached)
                stale.update({"stale": True, "reason": reason})
                return stale
            logger.debug(
                "portfolio sync skipped user=%s network=%s reason=%s: gateway circuit open",
                user_id,
                network,
                reason,
            )
            return {
                "user_id": int(user_id),
                "network": network,
                "stale": True,
                "reason": reason,
                "monotonic_ts": time.time(),
            }

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

            prior = _snapshot_cache.get(key) or {}
            heavy_interval = float(portfolio_heavy_sync_seconds())
            last_heavy = float(prior.get("last_heavy_monotonic", 0))
            need_heavy = force or str(reason) not in ("poll",) or (
                time.time() - last_heavy >= heavy_interval
            )

            # The DB write below treats missing live orders as closed, so every
            # authoritative sync must include isolated subaccounts too.
            include_isolated = True

            summary, orders, trigger_orders, balance = await asyncio.gather(
                client.calculate_account_summary(ts=int(time.time())),
                asyncio.to_thread(client.get_all_open_orders, True, include_isolated=include_isolated, strict=True),
                client.get_trigger_orders(limit=200, strict=True),
                asyncio.to_thread(client.get_balance),
            )

            if need_heavy:
                matches, funding = await asyncio.gather(
                    client.get_matches(limit=200),
                    client.get_interest_and_funding_payments(limit=200),
                )
                last_heavy_monotonic = time.time()
            else:
                matches = list(prior.get("matches") or [])
                funding = list(prior.get("funding_payments") or [])
                last_heavy_monotonic = last_heavy
            plain_orders = _normalize_order_rows(orders)
            trigger_rows = [_mark_trigger_order(o) for o in _normalize_order_rows(trigger_orders)]
            all_orders = _dedupe_orders_by_digest(plain_orders + trigger_rows)
            positions = [p.to_dict() for p in positions_from_account_summary(summary or {})]
            stats = aggregate_trading_stats(matches or [], funding or [])
            spot_balances = ((balance or {}).get("balances") or {}) if isinstance(balance, dict) else {}
            equity = compute_total_equity(summary or {}, spot_balances)
            snapshot = {
                "user_id": int(user_id),
                "network": network,
                "summary": summary or {},
                "positions": positions,
                "open_orders": all_orders,
                "matches": matches or [],
                "funding_payments": funding or [],
                "stats": stats,
                "equity": equity,
                "spot_balances": spot_balances,
                "last_sync": _now(),
                "monotonic_ts": time.time(),
                "last_heavy_monotonic": last_heavy_monotonic,
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
        leverage = _resolve_leverage(pos)
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
                leverage,
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
                leverage,
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
        digest = str(
            match.get("digest")
            or match.get("order_digest")
            or order.get("digest")
            or order.get("order_digest")
            or ""
        ).strip()
        session_id, source = _back_link_intent(digest, network)
        execute(
            f"""
            INSERT INTO {table} (
              user_id, product_id, product_name, order_type, side, size, status,
              submission_idx, isolated, realized_pnl_x18, fee_x18, base_filled_x18, quote_filled_x18,
              order_digest, strategy_session_id, source,
              filled_at, created_at
            )
            VALUES (%s, %s, %s, 'match', %s, %s, 'filled', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
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
                digest or None,
                session_id,
                source,
                _timestamp_or_now(match.get("timestamp")),
            ),
        )
        inserted += 1
        _maybe_increment_session_win_loss(session_id, pnl_x18)
    return inserted


def _maybe_increment_session_win_loss(session_id: int | None, pnl_x18: str) -> None:
    """Bump ``strategy_sessions.win_count`` / ``loss_count`` on decisive closes.

    The performance cards read these counters directly. We increment here
    when a venue-sync fill arrives with a back-linked ``strategy_session_id``
    and a non-zero realized PnL — belt-and-suspenders alongside the
    session-end rollup in ``bot_runtime._finalize_session``.
    """
    if not session_id:
        return
    try:
        pnl = float(from_x18(pnl_x18))
    except Exception:
        return
    if abs(pnl) <= 1e-9:
        return
    column = "win_count" if pnl > 0 else "loss_count"
    try:
        execute(
            f"UPDATE strategy_sessions SET {column} = {column} + 1 WHERE id = %s",
            (int(session_id),),
        )
    except Exception:
        logger.debug("session win/loss increment failed session=%s", session_id, exc_info=True)


def _back_link_intent(digest: str, network: str) -> tuple[int | None, str]:
    """Resolve ``(strategy_session_id, source)`` from order_intents.

    Strategies and engine adapters write to ``order_intents`` with ``value``
    JSONB carrying ``strategy_session_id`` and ``source``. Venue-sync fills
    only arrive with an ``order_digest``; this lookup re-attaches the tags
    so per-session rollups and History (source=manual) filtering work.
    Returns ``(None, "manual")`` when no intent is found.

    Execution-mode safety: the resolved ``strategy_session_id`` is dropped
    if it doesn't belong to the same ``network`` as the incoming fill, so a
    testnet fill never links to a mainnet session (or vice-versa).
    """
    if not digest:
        return None, "manual"
    try:
        intent = query_one(
            "SELECT value FROM order_intents WHERE order_digest = %s ORDER BY updated_at DESC NULLS LAST LIMIT 1",
            (digest,),
        )
    except Exception:
        logger.debug("intent back-link query failed digest=%s", digest, exc_info=True)
        return None, "manual"
    if not intent:
        return None, "manual"
    value = intent.get("value") or {}
    if isinstance(value, str):
        try:
            import json

            value = json.loads(value)
        except Exception:
            value = {}
    if not isinstance(value, dict):
        return None, "manual"
    raw_session = value.get("strategy_session_id")
    session_id: int | None
    try:
        session_id = int(raw_session) if raw_session is not None else None
    except (TypeError, ValueError):
        session_id = None
    source = str(value.get("source") or "manual") or "manual"
    if session_id is not None:
        try:
            session_row = query_one(
                "SELECT network FROM strategy_sessions WHERE id = %s",
                (int(session_id),),
            )
        except Exception:
            session_row = None
        if not session_row or _normalize_network(session_row.get("network")) != _normalize_network(network):
            logger.debug(
                "back-link dropped: session=%s network=%s != fill network=%s",
                session_id,
                session_row.get("network") if session_row else None,
                network,
            )
            return None, source
    return session_id, source


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


def _resolve_leverage(pos: dict[str, Any]) -> str:
    """Return a non-NULL leverage string.

    Nado's ``calculate_account_summary`` often omits ``leverage`` on cross/
    isolated rows; the ``positions`` table has a legacy ``NOT NULL DEFAULT 1``
    column from the original DDL. Derive leverage from
    ``notional / max(margin_used, eps)`` when available, otherwise fall back
    to ``1`` so the row insert never fails.
    """
    raw = pos.get("leverage")
    if raw is not None and str(raw) != "":
        try:
            value = Decimal(str(raw))
            if value > 0:
                return str(value)
        except Exception:
            pass
    try:
        notional = Decimal(str(pos.get("notional_value") or 0))
        margin = Decimal(str(pos.get("margin_used") or 0))
        if margin > 0 and notional > 0:
            return str(notional / margin)
    except Exception:
        pass
    return "1"


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


def _dedupe_orders_by_digest(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate open orders when trigger + plain fetches overlap."""
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for order in orders:
        digest = str(order.get("digest") or order.get("order_digest") or "")
        if digest:
            if digest in seen:
                continue
            seen.add(digest)
        deduped.append(order)
    return deduped


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
