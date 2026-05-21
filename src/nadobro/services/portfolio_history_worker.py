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
    written = 0
    for uid in user_ids:
        try:
            await portfolio.sample(uid, now)
            written += 1
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
                (user_id, ts, total_value_quote, by_account_json, by_asset_json)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, ts) DO NOTHING
            """,
            (row.user_id, row.ts, str(row.total_value_quote), _json(row.by_account), _json(row.by_asset)),
        )

    def fetch(
        self, user_id: int, since: Optional[datetime] = None, until: Optional[datetime] = None
    ) -> List[PortfolioHistoryRow]:
        from src.nadobro.db import query_all

        clauses = ["user_id = %s"]
        params: List[object] = [user_id]
        if since is not None:
            clauses.append("ts >= %s")
            params.append(since)
        if until is not None:
            clauses.append("ts <= %s")
            params.append(until)
        rows = query_all(
            f"SELECT user_id, ts, total_value_quote, by_account_json, by_asset_json "
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
                )
            )
        return out

    def prune(self, now: Optional[datetime] = None) -> int:
        from src.nadobro.db import execute

        now = now or datetime.now(timezone.utc)
        d7 = now - timedelta(days=7)
        d30 = now - timedelta(days=30)
        d365 = now - timedelta(days=365)
        # Drop rows older than 1y.
        execute("DELETE FROM engine_portfolio_history WHERE ts < %s", (d365,))
        # 30d..1y: keep one per day.
        execute(
            """
            DELETE FROM engine_portfolio_history h
            WHERE h.ts >= %s AND h.ts < %s
              AND h.ts <> (
                SELECT max(h2.ts) FROM engine_portfolio_history h2
                WHERE h2.user_id = h.user_id
                  AND date_trunc('day', h2.ts) = date_trunc('day', h.ts))
            """,
            (d365, d30),
        )
        # 7d..30d: keep one per hour.
        execute(
            """
            DELETE FROM engine_portfolio_history h
            WHERE h.ts >= %s AND h.ts < %s
              AND h.ts <> (
                SELECT max(h2.ts) FROM engine_portfolio_history h2
                WHERE h2.user_id = h.user_id
                  AND date_trunc('hour', h2.ts) = date_trunc('hour', h.ts))
            """,
            (d30, d7),
        )
        return 0  # row counts not tracked for the DB path


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

    def __init__(self, network: str = "mainnet") -> None:
        self.network = network

    async def accounts(self, user_id: int) -> Accounts:
        snapshot = self._snapshot(user_id)
        if snapshot is None:
            return {}
        accounts: Accounts = {"nado_perps": {}, "nado_spot": {}, "nado_vault": {}}
        for pos in getattr(snapshot, "positions", None) or []:
            pair = str(pos.get("product_name") or pos.get("pair") or "")
            if not pair:
                continue
            accounts["nado_perps"][pair] = {
                "units": _dec(pos.get("signed_amount", pos.get("amount", 0))),
                "mark": _dec(pos.get("mark_price", pos.get("price", 0))),
                "value": _dec(pos.get("notional_value", 0)),
            }
        return accounts

    async def mark_prices(self, user_id: int) -> Dict[str, Decimal]:
        snapshot = self._snapshot(user_id)
        marks: Dict[str, Decimal] = {}
        for pos in getattr(snapshot, "positions", None) or []:
            pair = str(pos.get("product_name") or pos.get("pair") or "")
            if pair:
                marks[pair] = _dec(pos.get("mark_price", pos.get("price", 0)))
        return marks

    def _snapshot(self, user_id: int) -> object:
        try:
            from src.nadobro.services.portfolio_service import get_portfolio_snapshot

            return get_portfolio_snapshot(user_id)
        except Exception:  # noqa: BLE001
            logger.warning("account snapshot unavailable for user %s", user_id, exc_info=True)
            return None


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
    from src.nadobro.engine.inventory import InventoryRepository

    # NOTE: a DB-backed holds source over engine_position_hold is wired in
    # Phase 4 when executors begin writing holds; until then the sampler only
    # needs account data, and per-controller PnL reads an empty hold set.
    return Portfolio(
        inventory=InventoryRepository(),
        accounts_provider=SnapshotAccountProvider(),
        executors=DbExecutorsRepository(),
        history=DbPortfolioHistoryRepository(),
    )


def list_active_user_ids(days: int = 1) -> List[int]:
    from src.nadobro.db import query_all

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = query_all(
        "SELECT telegram_id FROM users WHERE last_active >= %s", (cutoff,)
    )
    return [r["telegram_id"] for r in rows or []]
