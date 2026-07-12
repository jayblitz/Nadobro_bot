"""Portfolio v2 tests: state shape + per-controller PnL, distribution sums to
100%, sampler writes rows, retention prunes per policy."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.portfolio import (
    InMemoryAccountProvider,
    InMemoryExecutorsRepository,
    InMemoryPortfolioHistoryRepository,
    Portfolio,
    PortfolioHistoryRow,
)
from src.nadobro.engine.types import TradeType
from src.nadobro.portfolio.portfolio_history_worker import (
    SnapshotAccountProvider,
    run_retention_once,
    run_sampler_once,
)


def _portfolio(inv=None, acct=None, execs=None, hist=None) -> Portfolio:
    return Portfolio(
        inv or InventoryRepository(),
        acct or InMemoryAccountProvider(),
        execs or InMemoryExecutorsRepository(),
        hist or InMemoryPortfolioHistoryRepository(),
    )


def test_state_per_controller_pnl_accuracy():
    async def body():
        inv = InventoryRepository()
        inv.apply_fill(1, "SOL-USDC", "c1", TradeType.BUY, Decimal(10), Decimal(1000))
        inv.apply_fill(1, "SOL-USDC", "c1", TradeType.SELL, Decimal(4), Decimal(440))
        acct = InMemoryAccountProvider()
        acct.set(
            1,
            {"nado_spot": {"USDC": {"units": Decimal(500), "price": Decimal(1), "value": Decimal(500)}}},
            marks={"SOL-USDC": Decimal(120)},
        )
        execs = InMemoryExecutorsRepository()
        execs.set_open_count(1, "c1", 2)
        state = await _portfolio(inv, acct, execs).state(1)
        c = state.per_controller["c1"]
        assert c.realized == Decimal(40)          # (110-100)*min(10,4)
        assert c.unrealized == Decimal(120)        # 6 * (120-100)
        assert c.fees == Decimal(0)
        assert c.net == Decimal(160)               # 40 - 0 + 120
        assert c.open_executors == 2
        assert state.total_value_quote == Decimal(500)

    asyncio.run(body())


def test_state_isolates_controllers_on_same_pair():
    async def body():
        inv = InventoryRepository()
        inv.apply_fill(1, "SOL-USDC", "c1", TradeType.BUY, Decimal(1), Decimal(100))
        inv.apply_fill(1, "SOL-USDC", "c2", TradeType.SELL, Decimal(1), Decimal(110))
        acct = InMemoryAccountProvider()
        acct.set(1, {}, marks={"SOL-USDC": Decimal(105)})
        state = await _portfolio(inv, acct).state(1)
        assert set(state.per_controller) == {"c1", "c2"}

    asyncio.run(body())


def test_distribution_sums_to_100():
    async def body():
        acct = InMemoryAccountProvider()
        acct.set(
            1,
            {
                "nado_spot": {"USDC": {"value": Decimal(300)}, "SOL": {"value": Decimal(100)}},
                "nado_perps": {"BTC-USDC": {"value": Decimal(100)}},
            },
        )
        p = _portfolio(acct=acct)
        dist = await p.distribution(1)
        assert sum(dist.values()) == Decimal(100)
        assert dist["USDC"] == Decimal(60)
        adist = await p.accounts_distribution(1)
        assert sum(adist.values()) == Decimal(100)
        assert adist["nado_spot"] == Decimal(80)

    asyncio.run(body())


def test_distribution_empty_when_no_value():
    async def body():
        acct = InMemoryAccountProvider()
        acct.set(1, {})
        assert await _portfolio(acct=acct).distribution(1) == {}

    asyncio.run(body())


def test_sampler_writes_one_row_per_user():
    async def body():
        acct = InMemoryAccountProvider()
        acct.set(1, {"nado_spot": {"USDC": {"value": Decimal(500)}}})
        acct.set(2, {"nado_spot": {"USDC": {"value": Decimal(200)}}})
        hist = InMemoryPortfolioHistoryRepository()
        p = _portfolio(acct=acct, hist=hist)
        n = await run_sampler_once(p, [1, 2])
        assert n == 2
        rows = hist.fetch(1)
        assert len(rows) == 1
        assert rows[0].total_value_quote == Decimal(500)
        assert rows[0].by_asset["USDC"] == Decimal(500)

    asyncio.run(body())


def test_snapshot_account_provider_derives_missing_notional_value():
    class FakeSnapshotProvider(SnapshotAccountProvider):
        def _snapshot(self, user_id: int) -> object:
            return SimpleNamespace(
                positions=[
                    {
                        "product_name": "BTC-USDC",
                        "signed_amount": "-2",
                        "price": "100",
                    }
                ]
            )

    async def body():
        provider = FakeSnapshotProvider()
        accounts = await provider.accounts(1)
        assert accounts["nado_perps"]["BTC-USDC"]["value"] == Decimal("200")

        hist = InMemoryPortfolioHistoryRepository()
        row = await _portfolio(acct=provider, hist=hist).sample(1)
        assert row.total_value_quote == Decimal("200")
        assert hist.fetch(1)[0].by_asset["BTC-USDC"] == Decimal("200")

    asyncio.run(body())


def test_sampler_skips_snapshot_failures_without_zero_row():
    class FailingSnapshotProvider(SnapshotAccountProvider):
        def _snapshot(self, user_id: int) -> object:
            raise RuntimeError("snapshot unavailable")

    async def body():
        hist = InMemoryPortfolioHistoryRepository()
        n = await run_sampler_once(_portfolio(acct=FailingSnapshotProvider(), hist=hist), [1])
        assert n == 0
        assert hist.fetch(1) == []

    asyncio.run(body())


def test_retention_downsamples_per_policy():
    async def body():
        hist = InMemoryPortfolioHistoryRepository()
        now = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)

        def add(dt):
            hist.record(PortfolioHistoryRow(1, dt, Decimal(1), {}, {}))

        # < 7d: keep every sample (3)
        add(now - timedelta(minutes=1))
        add(now - timedelta(minutes=2))
        add(now - timedelta(minutes=3))
        # 7d..30d: 3 rows in the same hour (~10d ago) -> keep 1
        base10 = now - timedelta(days=10)
        add(base10)
        add(base10 + timedelta(minutes=20))
        add(base10 + timedelta(minutes=40))
        # 30d..1y: 2 rows on the same day (~100d ago) -> keep 1
        base100 = now - timedelta(days=100)
        add(base100)
        add(base100 + timedelta(hours=3))
        # > 1y: dropped
        add(now - timedelta(days=400))

        removed = await run_retention_once(hist, now)
        kept = hist.fetch(1)
        assert len(kept) == 5      # 3 fine + 1 hourly + 1 daily
        assert removed == 4        # 9 total - 5 kept

    asyncio.run(body())


def test_history_window_by_interval():
    async def body():
        hist = InMemoryPortfolioHistoryRepository()
        now = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
        hist.record(PortfolioHistoryRow(1, now - timedelta(hours=1), Decimal(10), {}, {}))
        hist.record(PortfolioHistoryRow(1, now - timedelta(days=10), Decimal(5), {}, {}))
        p = _portfolio(hist=hist)
        assert len(await p.history(1, "24h", end=now)) == 1
        assert len(await p.history(1, "7d", end=now)) == 1
        assert len(await p.history(1, "custom", start=now - timedelta(days=30), end=now)) == 2

    asyncio.run(body())


def test_sample_stamps_provider_network():
    async def body():
        acct = InMemoryAccountProvider()
        acct.set(1, {"nado_spot": {"USDC": {"value": Decimal(100)}}}, network="testnet")
        acct.set(2, {"nado_spot": {"USDC": {"value": Decimal(200)}}})  # no network -> mainnet
        hist = InMemoryPortfolioHistoryRepository()
        p = _portfolio(acct=acct, hist=hist)
        row1 = await p.sample(1)
        row2 = await p.sample(2)
        assert row1.network == "testnet"
        assert row2.network == "mainnet"

    asyncio.run(body())


def test_fetch_and_history_filter_by_network():
    async def body():
        hist = InMemoryPortfolioHistoryRepository()
        now = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
        hist.record(PortfolioHistoryRow(1, now - timedelta(hours=1), Decimal(10), {}, {}, network="testnet"))
        hist.record(PortfolioHistoryRow(1, now - timedelta(hours=2), Decimal(20), {}, {}, network="mainnet"))
        # No filter -> both series (back-compat).
        assert len(hist.fetch(1)) == 2
        # Filtered -> one mode's series only.
        t_rows = hist.fetch(1, network="testnet")
        assert [r.total_value_quote for r in t_rows] == [Decimal(10)]
        m_rows = hist.fetch(1, network="mainnet")
        assert [r.total_value_quote for r in m_rows] == [Decimal(20)]
        p = _portfolio(hist=hist)
        assert len(await p.history(1, "24h", end=now, network="testnet")) == 1
        assert len(await p.history(1, "24h", end=now)) == 2

    asyncio.run(body())


def test_retention_buckets_per_network():
    async def body():
        hist = InMemoryPortfolioHistoryRepository()
        now = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
        # Two samples in the SAME hourly bucket (~10d ago) on DIFFERENT
        # networks: downsampling must keep one per network, not one total.
        base10 = now - timedelta(days=10)
        hist.record(PortfolioHistoryRow(1, base10, Decimal(1), {}, {}, network="testnet"))
        hist.record(PortfolioHistoryRow(1, base10 + timedelta(minutes=5), Decimal(2), {}, {}, network="mainnet"))
        removed = await run_retention_once(hist, now)
        assert removed == 0
        kept = hist.fetch(1)
        assert {r.network for r in kept} == {"testnet", "mainnet"}

    asyncio.run(body())


def test_snapshot_provider_network_resolution():
    class TestnetSnapshotProvider(SnapshotAccountProvider):
        def _snapshot(self, user_id: int) -> object:
            return SimpleNamespace(positions=[], network="testnet")

    class NetworklessSnapshotProvider(SnapshotAccountProvider):
        def _snapshot(self, user_id: int) -> object:
            return SimpleNamespace(positions=[])

    async def body():
        # Follows the snapshot's (i.e. the user's active) network.
        assert await TestnetSnapshotProvider().network(1) == "testnet"
        # A pinned constructor network wins over the snapshot.
        assert await TestnetSnapshotProvider(network="mainnet").network(1) == "mainnet"
        # Snapshot without a network attribute degrades to mainnet.
        assert await NetworklessSnapshotProvider().network(1) == "mainnet"
        # End-to-end: the sampled row carries the resolved network.
        hist = InMemoryPortfolioHistoryRepository()
        row = await _portfolio(acct=TestnetSnapshotProvider(), hist=hist).sample(1)
        assert row.network == "testnet"
        assert hist.fetch(1, network="testnet")[0].ts == row.ts

    asyncio.run(body())


def test_state_and_controller_as_dict():
    async def body():
        inv = InventoryRepository()
        inv.apply_fill(1, "SOL-USDC", "c1", TradeType.BUY, Decimal(1), Decimal(100))
        acct = InMemoryAccountProvider()
        acct.set(1, {"nado_spot": {"USDC": {"value": Decimal(100)}}}, marks={"SOL-USDC": Decimal(110)})
        state = await _portfolio(inv, acct).state(1)
        d = state.as_dict()
        assert set(d) == {"accounts", "total_value_quote", "per_controller"}
        assert set(d["per_controller"]["c1"]) == {
            "realized", "unrealized", "fees", "net", "open_executors",
        }

    asyncio.run(body())
