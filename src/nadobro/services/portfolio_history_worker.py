"""Portfolio history sampler + retention worker (Engine v2 Phase 2).

- Sampler: every 60s, snapshot each active user's portfolio into
  ``engine_portfolio_history``.
- Retention: daily, downsample to 1m/7d, 1h/30d, 1d/1y (older rows dropped).

Pure helpers (``run_sampler_once`` / ``run_retention_once``) are unit-tested
with in-memory repos; the loop wiring + DB-backed repos are integration glue
(validated against a live Postgres). The account data is sourced from the
existing portfolio snapshot so the live /portfolio view does not regress.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Dict, List, Optional

from src.nadobro.engine.portfolio import (
    Accounts,
    AccountProvider,
    ExecutorsRepository,
    Portfolio,
    PortfolioHistoryRepository,
    PortfolioHistoryRow,
)
from src.nadobro.engine.types import _dec

logger = logging.getLogger(__name__)

SAMPLER_INTERVAL_SECONDS = 60
RETENTION_INTERVAL_SECONDS = 24 * 3600


# --------------------------------------------------------------------------
# Pure, testable helpers
# --------------------------------------------------------------------------
async def run_sampler_once(
    portfolio: Portfolio, user_ids: List[int], now: Optional[datetime] = None
) -> int:
    """Snapshot each user once. Returns the number of rows written."""
    from src.nadobro.services.portfolio_service import PortfolioSnapshotUnavailable

    written = 0
    for uid in user_ids:
        try:
            await portfolio.sample(uid, now)
            written += 1
        except PortfolioSnapshotUnavailable as e:
            # Expected for users who haven't linked / funded a wallet yet.
            # Logging a full traceback every 60s for each such user is what
            # turns the logs into noise — keep it to a single debug line.
            logger.debug("portfolio sample skipped for user %s: %s", uid, e)
        except Exception:  # noqa: BLE001 - one bad user shouldn't kill the loop
            logger.warning("portfolio sample failed for user %s", uid, exc_info=True)
    return written


async def run_retention_once(
    history: PortfolioHistoryRepository, now: Optional[datetime] = None
) -> int:
    """Prune history per the retention policy. Returns rows removed."""
    return history.prune(now)


# --------------------------------------------------------------------------
# Loop orchestration (integration glue)
# --------------------------------------------------------------------------
async def sampler_loop(
    portfolio_factory: Callable[[], Portfolio],
    list_users: Callable[[], List[int]],
    *,
    interval: int = SAMPLER_INTERVAL_SECONDS,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    while stop_event is None or not stop_event.is_set():
        try:
            await run_sampler_once(portfolio_factory(), list_users())
        except Exception:  # noqa: BLE001
            logger.warning("portfolio sampler tick failed", exc_info=True)
        await asyncio.sleep(interval)


async def retention_loop(
    history_factory: Callable[[], PortfolioHistoryRepository],
    *,
    interval: int = RETENTION_INTERVAL_SECONDS,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    while stop_event is None or not stop_event.is_set():
        try:
            await run_retention_once(history_factory())
        except Exception:  # noqa: BLE001
            logger.warning("portfolio retention tick failed", exc_info=True)
        await asyncio.sleep(interval)


def start_portfolio_history_worker() -> List["asyncio.Task[None]"]:
    """Spawn the sampler + retention loops on the running event loop."""
    tasks = [
        asyncio.create_task(sampler_loop(build_db_portfolio, list_active_user_ids)),
        asyncio.create_task(retention_loop(lambda: DbPortfolioHistoryRepository())),
    ]
    logger.info("portfolio history worker started")
    return tasks


# --------------------------------------------------------------------------
# DB-backed collaborators
# --------------------------------------------------------------------------
def _json(obj: Dict[str, Decimal]) -> str:
    return json.dumps({k: str(v) for k, v in obj.items()})


class DbPortfolioHistoryRepository(PortfolioHistoryRepository):
    def record(self, row: PortfolioHistoryRow) -> None:
        from src.nadobro.db import execute

        execute(
            """
            INSERT INTO engine_portfolio_history
                (user_id, network, ts, total_value_quote, by_account_json, by_asset_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, network, ts) DO NOTHING
            """,
            (
                row.user_id, row.network, row.ts, str(row.total_value_quote),
                _json(row.by_account), _json(row.by_asset),
            ),
        )

    def fetch(
        self, user_id: int, since: Optional[datetime] = None, until: Optional[datetime] = None,
        network: Optional[str] = None,
    ) -> List[PortfolioHistoryRow]:
        from src.nadobro.db import query_all

        clauses = ["user_id = %s"]
        params: List[object] = [user_id]
        if network is not None:
            clauses.append("network = %s")
            params.append(network)
        if since is not None:
            clauses.append("ts >= %s")
            params.append(since)
        if until is not None:
            clauses.append("ts <= %s")
            params.append(until)
        rows = query_all(
            f"SELECT user_id, network, ts, total_value_quote, by_account_json, by_asset_json "
            f"FROM engine_portfolio_history WHERE {' AND '.join(clauses)} ORDER BY ts",
            tuple(params),
        )
        out: List[PortfolioHistoryRow] = []
        for r in rows or []:
            out.append(
                PortfolioHistoryRow(
                    user_id=r["user_id"],
                    ts=r["ts"],
                    total_value_quote=_dec(r["total_value_quote"]),
                    by_account={k: _dec(v) for k, v in _loads(r["by_account_json"]).items()},
                    by_asset={k: _dec(v) for k, v in _loads(r["by_asset_json"]).items()},
                    network=str(r.get("network") or "mainnet"),
                )
            )
        return out

    def prune(self, now: Optional[datetime] = None) -> int:
        """Atomically run the three retention DELETEs in a single transaction
        (BUG-PHW-2) and return the total row count removed (BUG-PHW-3).
        Uses a CTE-driven SELECT to identify rows to delete; this is O(N)
        per band instead of the previous O(N²) correlated-subquery form
        (BUG-PHW-4).
        """
        from src.nadobro.db import get_db, put_db

        now = now or datetime.now(timezone.utc)
        d7 = now - timedelta(days=7)
        d30 = now - timedelta(days=30)
        d365 = now - timedelta(days=365)
        conn = get_db()
        removed = 0
        try:
            with conn.cursor() as cur:
                # Drop rows older than 1y.
                cur.execute(
                    "DELETE FROM engine_portfolio_history WHERE ts < %s",
                    (d365,),
                )
                removed += cur.rowcount or 0
                # 30d..1y: keep one per (user, network, day). Pick the row
                # with max(ts) per bucket via DISTINCT ON (CTE pattern, O(N)).
                # Bucketing by network keeps each mode's series intact — one
                # survivor per network per day, never one across both.
                cur.execute(
                    """
                    WITH keep AS (
                        SELECT DISTINCT ON (user_id, network, date_trunc('day', ts))
                               user_id, network, ts
                        FROM engine_portfolio_history
                        WHERE ts >= %s AND ts < %s
                        ORDER BY user_id, network, date_trunc('day', ts), ts DESC
                    )
                    DELETE FROM engine_portfolio_history h
                    WHERE h.ts >= %s AND h.ts < %s
                      AND NOT EXISTS (
                        SELECT 1 FROM keep k
                        WHERE k.user_id = h.user_id AND k.network = h.network AND k.ts = h.ts
                      )
                    """,
                    (d365, d30, d365, d30),
                )
                removed += cur.rowcount or 0
                # 7d..30d: keep one per (user, network, hour).
                cur.execute(
                    """
                    WITH keep AS (
                        SELECT DISTINCT ON (user_id, network, date_trunc('hour', ts))
                               user_id, network, ts
                        FROM engine_portfolio_history
                        WHERE ts >= %s AND ts < %s
                        ORDER BY user_id, network, date_trunc('hour', ts), ts DESC
                    )
                    DELETE FROM engine_portfolio_history h
                    WHERE h.ts >= %s AND h.ts < %s
                      AND NOT EXISTS (
                        SELECT 1 FROM keep k
                        WHERE k.user_id = h.user_id AND k.network = h.network AND k.ts = h.ts
                      )
                    """,
                    (d30, d7, d30, d7),
                )
                removed += cur.rowcount or 0
            conn.commit()
            return removed
        except Exception:
            conn.rollback()
            raise
        finally:
            put_db(conn)


class DbExecutorsRepository(ExecutorsRepository):
    def open_count(self, user_id: int, controller_id: str) -> int:
        from src.nadobro.db import query_count

        return query_count(
            "SELECT 1 FROM engine_executors "
            "WHERE user_id = %s AND controller_id = %s AND state <> 'TERMINATED'",
            (user_id, controller_id),
        )


class SnapshotAccountProvider(AccountProvider):
    """Sources normalized accounts + marks from the existing portfolio
    snapshot, so portfolio v2 reuses the live Nado data path."""

    def __init__(self, network: str | None = None) -> None:
        # None = follow each user's ACTIVE network (matches the perp snapshot
        # path, which samples via get_portfolio_snapshot on the user's current
        # mode). Pinning "mainnet" here while the perp leg followed the user's
        # mode summed testnet perps + mainnet spot into one equity number.
        # Stored under ``_network``: a plain ``self.network`` attribute would
        # shadow the ``network()`` accessor inherited from AccountProvider.
        self._network = network

    async def accounts(self, user_id: int) -> Accounts:
        from src.nadobro.core.async_utils import run_blocking

        snapshot = await run_blocking(self._snapshot, user_id)
        accounts: Accounts = {"nado_perps": {}, "nado_spot": {}, "nado_vault": {}}
        for pos in getattr(snapshot, "positions", None) or []:
            pair = str(pos.get("product_name") or pos.get("pair") or "")
            if not pair:
                continue
            units = _dec(pos.get("signed_amount", pos.get("amount", 0)))
            mark = _dec(pos.get("mark_price", pos.get("price", 0)))
            value = _dec(pos.get("notional_value")) if pos.get("notional_value") is not None else Decimal(0)
            if value <= 0 and units != 0 and mark > 0:
                value = abs(units) * mark
            accounts["nado_perps"][pair] = {
                "units": units,
                "mark": mark,
                "value": value,
            }

        # BUG-PHW-1 fix: also include spot balances so the portfolio history
        # total reflects USDC/quote sitting in the trading account, not just
        # the open perp positions. Best-effort; failure here must not break
        # the perp snapshot.
        try:
            spot = await run_blocking(self._spot_balances, user_id)
            for product_id, amount in (spot or {}).items():
                key = f"spot:{product_id}"
                value = _dec(amount)
                # Spot product_id=0 is the USDC quote leg; its value IS the
                # amount. For other spot legs, look up a mark price from the
                # snapshot prices dict.
                mark = self._mark_for_spot(snapshot, product_id)
                if mark > 0 and product_id != 0:
                    value = value * mark
                accounts["nado_spot"][key] = {
                    "units": _dec(amount),
                    "mark": mark,
                    "value": value,
                }
        except Exception:  # noqa: BLE001
            logger.warning(
                "spot balance fetch failed for user %s; snapshot will only include perps",
                user_id, exc_info=True,
            )

        return accounts

    async def mark_prices(self, user_id: int) -> Dict[str, Decimal]:
        from src.nadobro.core.async_utils import run_blocking

        snapshot = await run_blocking(self._snapshot, user_id)
        marks: Dict[str, Decimal] = {}
        for pos in getattr(snapshot, "positions", None) or []:
            pair = str(pos.get("product_name") or pos.get("pair") or "")
            if pair:
                marks[pair] = _dec(pos.get("mark_price", pos.get("price", 0)))
        return marks

    async def network(self, user_id: int) -> str:
        """Network the sample is taken on: an explicitly pinned constructor
        network wins; otherwise the user's ACTIVE network as resolved by the
        portfolio snapshot (the same snapshot ``accounts()`` just read — the
        2s snapshot cache keeps the two calls coherent)."""
        if self._network is not None:
            return str(self._network)
        from src.nadobro.core.async_utils import run_blocking

        snapshot = await run_blocking(self._snapshot, user_id)
        return str(getattr(snapshot, "network", None) or "mainnet")

    def _snapshot(self, user_id: int) -> object:
        from src.nadobro.services.portfolio_service import get_portfolio_snapshot

        return get_portfolio_snapshot(user_id)

    def _spot_balances(self, user_id: int) -> Dict[object, Decimal]:
        from src.nadobro.services.user_service import get_user_readonly_client

        # network=None resolves the user's active network — same network the
        # perp snapshot above was taken on, so one sample never mixes modes.
        client = get_user_readonly_client(user_id, network=self._network)
        if client is None:
            return {}
        try:
            payload = client.get_balance() or {}
        except Exception:  # noqa: BLE001
            return {}
        balances = (payload.get("balances") or {}) if isinstance(payload, dict) else {}
        return {pid: _dec(amount) for pid, amount in balances.items() if amount}

    @staticmethod
    def _mark_for_spot(snapshot: object, product_id: object) -> Decimal:
        prices = getattr(snapshot, "prices", None) or {}
        if not isinstance(prices, dict):
            return Decimal(0)
        # Try common keyings (int / str / dict-keyed-by-pair).
        for k in (product_id, str(product_id), f"spot:{product_id}"):
            v = prices.get(k)
            if v is not None:
                try:
                    return _dec(v)
                except Exception:  # noqa: BLE001
                    continue
        return Decimal(0)


def _loads(value: object) -> Dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:  # noqa: BLE001
            return {}
    return {}


def build_db_portfolio() -> Portfolio:
    from src.nadobro.services.engine_persistence import DbInventoryRepository

    return Portfolio(
        inventory=DbInventoryRepository(),
        accounts_provider=SnapshotAccountProvider(),
        executors=DbExecutorsRepository(),
        history=DbPortfolioHistoryRepository(),
    )


def list_active_user_ids(days: int = 1) -> List[int]:
    from src.nadobro.db import query_all

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    # Only sample users who actually have a linked wallet. Sampling wallet-less
    # users just raised PortfolioSnapshotUnavailable every 60s (one burned
    # snapshot + one log line per user); filtering at the source removes both
    # the wasted SDK-pool work and the log noise.
    rows = query_all(
        "SELECT telegram_id FROM users "
        "WHERE last_active >= %s AND main_address IS NOT NULL AND main_address <> ''",
        (cutoff,),
    )
    return [r["telegram_id"] for r in rows or []]
