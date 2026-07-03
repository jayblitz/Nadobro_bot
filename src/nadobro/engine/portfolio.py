"""Portfolio v2 — normalized account state + per-controller PnL, history
time-series, and distribution.

Built on injectable collaborators so the logic is unit-testable without a live
Postgres / Nado connection (mirrors the Phase 1 in-memory-repo seam):

- ``InventoryRepository`` (engine/inventory) -> per-controller realized /
  unrealized / fees from position holds.
- ``AccountProvider`` -> normalized accounts (spot / perps / vault) + mark
  prices. The DB/Nado-backed default reuses the existing portfolio snapshot
  (non-regressive); tests inject an in-memory fake.
- ``ExecutorsRepository`` -> open-executor counts per controller.
- ``PortfolioHistoryRepository`` -> sampler writes + retention pruning.

Implemented in Phase 2.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Protocol

from src.nadobro.engine.inventory import PositionHold
from src.nadobro.engine.types import _dec


class HoldsSource(Protocol):
    """Anything that can list a user's position holds (the in-memory
    ``InventoryRepository`` and the DB-backed repo both satisfy this)."""

    def list_for_user(self, user_id: int) -> List[PositionHold]:
        ...

# accounts: account_name -> token -> {"units"|"price"|"value"|...: Decimal}
Accounts = Dict[str, Dict[str, Dict[str, Decimal]]]


@dataclass
class ControllerPnL:
    realized: Decimal = Decimal(0)
    unrealized: Decimal = Decimal(0)
    fees: Decimal = Decimal(0)
    net: Decimal = Decimal(0)
    open_executors: int = 0

    def as_dict(self) -> Dict[str, object]:
        return {
            "realized": self.realized,
            "unrealized": self.unrealized,
            "fees": self.fees,
            "net": self.net,
            "open_executors": self.open_executors,
        }


@dataclass
class PortfolioState:
    accounts: Accounts = field(default_factory=dict)
    total_value_quote: Decimal = Decimal(0)
    per_controller: Dict[str, ControllerPnL] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, object]:
        return {
            "accounts": self.accounts,
            "total_value_quote": self.total_value_quote,
            "per_controller": {k: v.as_dict() for k, v in self.per_controller.items()},
        }


@dataclass
class PortfolioHistoryRow:
    user_id: int
    ts: datetime
    total_value_quote: Decimal
    by_account: Dict[str, Decimal]
    by_asset: Dict[str, Decimal]
    # Network the sample was taken on (the sampler follows the user's ACTIVE
    # network, so testnet and mainnet rows form separate series). Trailing
    # default keeps existing positional constructions valid.
    network: str = "mainnet"


# --------------------------------------------------------------------------
# Collaborator interfaces + in-memory implementations (DB-backed live in
# services/* and wrap db.py; the engine stays venue/DB-agnostic here).
# --------------------------------------------------------------------------
class AccountProvider(abc.ABC):
    @abc.abstractmethod
    async def accounts(self, user_id: int) -> Accounts:
        ...

    @abc.abstractmethod
    async def mark_prices(self, user_id: int) -> Dict[str, Decimal]:
        ...

    async def network(self, user_id: int) -> str:
        """Network the account data is sourced from. Non-abstract with a
        mainnet default so existing providers keep working; the DB-backed
        provider overrides this with the user's active network."""
        return "mainnet"


class ExecutorsRepository(abc.ABC):
    @abc.abstractmethod
    def open_count(self, user_id: int, controller_id: str) -> int:
        ...


class PortfolioHistoryRepository(abc.ABC):
    @abc.abstractmethod
    def record(self, row: PortfolioHistoryRow) -> None:
        ...

    @abc.abstractmethod
    def fetch(
        self, user_id: int, since: Optional[datetime] = None, until: Optional[datetime] = None,
        network: Optional[str] = None,
    ) -> List[PortfolioHistoryRow]:
        """``network=None`` returns all rows (back-compat); pass a network to
        read one mode's series without the other interleaved."""
        ...

    @abc.abstractmethod
    def prune(self, now: Optional[datetime] = None) -> int:
        ...


class InMemoryAccountProvider(AccountProvider):
    def __init__(self) -> None:
        self._accounts: Dict[int, Accounts] = {}
        self._marks: Dict[int, Dict[str, Decimal]] = {}
        self._networks: Dict[int, str] = {}

    def set(
        self, user_id: int, accounts: Accounts, marks: Optional[Dict[str, Decimal]] = None,
        network: Optional[str] = None,
    ) -> None:
        self._accounts[user_id] = accounts
        self._marks[user_id] = marks or {}
        if network is not None:
            self._networks[user_id] = network

    async def accounts(self, user_id: int) -> Accounts:
        return self._accounts.get(user_id, {})

    async def mark_prices(self, user_id: int) -> Dict[str, Decimal]:
        return self._marks.get(user_id, {})

    async def network(self, user_id: int) -> str:
        return self._networks.get(user_id, "mainnet")


class InMemoryExecutorsRepository(ExecutorsRepository):
    def __init__(self) -> None:
        self._counts: Dict[tuple, int] = {}

    def set_open_count(self, user_id: int, controller_id: str, n: int) -> None:
        self._counts[(user_id, controller_id)] = n

    def open_count(self, user_id: int, controller_id: str) -> int:
        return self._counts.get((user_id, controller_id), 0)


# Retention policy: 1m granularity for 7d, 1h for 30d, 1d for 1y (else drop).
RETENTION_FINE_DAYS = 7
RETENTION_HOURLY_DAYS = 30
RETENTION_DAILY_DAYS = 365


class InMemoryPortfolioHistoryRepository(PortfolioHistoryRepository):
    def __init__(self) -> None:
        self._rows: List[PortfolioHistoryRow] = []

    def record(self, row: PortfolioHistoryRow) -> None:
        self._rows.append(row)

    def fetch(
        self, user_id: int, since: Optional[datetime] = None, until: Optional[datetime] = None,
        network: Optional[str] = None,
    ) -> List[PortfolioHistoryRow]:
        out = [r for r in self._rows if r.user_id == user_id]
        if network is not None:
            out = [r for r in out if r.network == network]
        if since is not None:
            out = [r for r in out if r.ts >= since]
        if until is not None:
            out = [r for r in out if r.ts <= until]
        return sorted(out, key=lambda r: r.ts)

    def prune(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.now(timezone.utc)
        kept: Dict[tuple, PortfolioHistoryRow] = {}
        for row in self._rows:
            bucket = _retention_bucket(row.ts, now)
            if bucket is None:
                continue  # older than 1y -> drop
            # Bucket per network so downsampling never collapses a testnet and
            # a mainnet sample into one surviving row.
            key = (row.user_id, row.network, bucket)
            current = kept.get(key)
            if current is None or row.ts > current.ts:
                kept[key] = row
        before = len(self._rows)
        self._rows = sorted(kept.values(), key=lambda r: (r.user_id, r.ts))
        return before - len(self._rows)


def _retention_bucket(ts: datetime, now: datetime) -> Optional[tuple]:
    """Return a bucket key for ``ts`` per the retention policy, or ``None`` if
    the row is older than the 1-year horizon."""
    age = now - ts
    if age <= timedelta(days=RETENTION_FINE_DAYS):
        return ("m", ts)  # keep every sample
    if age <= timedelta(days=RETENTION_HOURLY_DAYS):
        return ("h", ts.year, ts.month, ts.day, ts.hour)
    if age <= timedelta(days=RETENTION_DAILY_DAYS):
        return ("d", ts.year, ts.month, ts.day)
    return None


# --------------------------------------------------------------------------
# Portfolio façade
# --------------------------------------------------------------------------
class Portfolio:
    def __init__(
        self,
        inventory: HoldsSource,
        accounts_provider: AccountProvider,
        executors: ExecutorsRepository,
        history: PortfolioHistoryRepository,
    ) -> None:
        self.inventory = inventory
        self.accounts_provider = accounts_provider
        self.executors = executors
        self.history_repo = history

    async def state(self, user_id: int) -> PortfolioState:
        accounts = await self.accounts_provider.accounts(user_id)
        marks = await self.accounts_provider.mark_prices(user_id)
        total_value = _total_value(accounts)

        per_controller: Dict[str, ControllerPnL] = {}
        for hold in self.inventory.list_for_user(user_id):
            cid = hold.controller_id
            agg = per_controller.setdefault(cid, ControllerPnL())
            mark = marks.get(hold.trading_pair)
            agg.realized += hold.realized_pnl
            agg.fees += hold.cum_fees_quote
            if mark is not None:
                agg.unrealized += hold.unrealized_pnl(mark)
        for cid, agg in per_controller.items():
            agg.net = agg.realized - agg.fees + agg.unrealized
            agg.open_executors = self.executors.open_count(user_id, cid)

        return PortfolioState(
            accounts=accounts, total_value_quote=total_value, per_controller=per_controller
        )

    async def distribution(self, user_id: int) -> Dict[str, Decimal]:
        """Asset -> weight percent (sums to 100 when total > 0)."""
        accounts = await self.accounts_provider.accounts(user_id)
        by_asset: Dict[str, Decimal] = {}
        for tokens in accounts.values():
            for token, fields_ in tokens.items():
                by_asset[token] = by_asset.get(token, Decimal(0)) + _value_of(fields_)
        return _to_pct(by_asset)

    async def accounts_distribution(self, user_id: int) -> Dict[str, Decimal]:
        """Account -> weight percent (sums to 100 when total > 0)."""
        accounts = await self.accounts_provider.accounts(user_id)
        by_account = {acct: _account_value(tokens) for acct, tokens in accounts.items()}
        return _to_pct(by_account)

    async def history(
        self, user_id: int, interval: str = "24h", start: Optional[datetime] = None,
        end: Optional[datetime] = None, network: Optional[str] = None,
    ) -> List[PortfolioHistoryRow]:
        now = end or datetime.now(timezone.utc)
        if interval == "custom":
            since = start
        else:
            span = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}
            since = now - span.get(interval, timedelta(hours=24))
        return self.history_repo.fetch(user_id, since=since, until=now, network=network)

    async def sample(self, user_id: int, now: Optional[datetime] = None) -> PortfolioHistoryRow:
        """Capture one history row from the current state (used by the sampler)."""
        accounts = await self.accounts_provider.accounts(user_id)
        by_account = {acct: _account_value(tokens) for acct, tokens in accounts.items()}
        by_asset: Dict[str, Decimal] = {}
        for tokens in accounts.values():
            for token, fields_ in tokens.items():
                by_asset[token] = by_asset.get(token, Decimal(0)) + _value_of(fields_)
        row = PortfolioHistoryRow(
            user_id=user_id,
            ts=now or datetime.now(timezone.utc),
            total_value_quote=_total_value(accounts),
            by_account=by_account,
            by_asset=by_asset,
            network=await self.accounts_provider.network(user_id),
        )
        self.history_repo.record(row)
        return row


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _value_of(fields_: Dict[str, Decimal]) -> Decimal:
    if "value" in fields_:
        return _dec(fields_["value"])
    units = _dec(fields_.get("units", 0))
    price = _dec(fields_.get("price", fields_.get("mark", 0)))
    return units * price


def _account_value(tokens: Dict[str, Dict[str, Decimal]]) -> Decimal:
    return sum((_value_of(f) for f in tokens.values()), Decimal(0))


def _total_value(accounts: Accounts) -> Decimal:
    return sum((_account_value(tokens) for tokens in accounts.values()), Decimal(0))


def _to_pct(values: Dict[str, Decimal]) -> Dict[str, Decimal]:
    total = sum(values.values(), Decimal(0))
    if total <= 0:
        return {k: Decimal(0) for k in values}
    return {k: (v / total) * Decimal(100) for k, v in values.items()}
