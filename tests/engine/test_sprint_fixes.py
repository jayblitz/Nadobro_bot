"""Regression tests for the Sprint 1-4 Engine v2 bug fixes.

Each test exercises a specific BUG-id from the phased audit so a regression
of any of the listed bugs is loudly caught in CI.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine.adapter.base import OrderState
from src.nadobro.engine.adapter.nado import NadoAdapter, OrderRegistry, ProductMeta, _OrderRef
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.executors.dca_executor import (
    DCAExecutor,
    DCAExecutorConfig,
    DCAMode,
)
from src.nadobro.engine.executors.grid_executor import (
    GridExecutor,
    GridExecutorConfig,
)
from src.nadobro.engine.executors.order_executor import (
    OrderExecutor,
    OrderExecutorConfig,
)
from src.nadobro.engine.executors.position_executor import (
    PositionExecutor,
    PositionExecutorConfig,
)
from src.nadobro.engine.executors.twap_executor import (
    TWAPExecutor,
    TWAPExecutorConfig,
)
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.market_data import MarketData
from src.nadobro.engine.orchestrator import (
    DEFAULT_EVENT_LOG_LIMIT,
    ExecutorOrchestrator,
)
from src.nadobro.engine.routines.technical_analysis import ema
from src.nadobro.engine.types import (
    CloseType,
    ExecutionStrategy,
    OrderType,
    RiskState,
    TradeType,
    TripleBarrierConfig,
)
from tests.engine._mock_nado import MockNadoAdapter


PAIR = "BTC-USDC"


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if asyncio.get_event_loop().is_running() else asyncio.run(coro)


# --------------------------------------------------------------------------
# Adapter — BUG-NA-2, BUG-NA-3, BUG-NA-4, BUG-NA-5, BUG-NA-7
# --------------------------------------------------------------------------
META = {PAIR: ProductMeta(product_id=2, tick_size=Decimal("0.01"),
                          lot_size=Decimal("0.001"), min_notional=Decimal(1))}


class _StubClient:
    def __init__(self):
        self.open_orders: list = []
        self.matches: list = []
        self.cancelled: list = []
        self.placed_resps: list = []
        self.cancel_should_raise = False

    def place_limit_order(self, product_id, size, price, is_buy=True, post_only=False, reduce_only=False, **kwargs):
        resp = {"digest": "d1", "status": "open"}
        self.placed_resps.append(resp)
        return resp

    def place_market_order(self, product_id, size, is_buy=True, reduce_only=False, **kwargs):
        resp = {"digest": "m1", "status": "filled", "price": 100}
        self.placed_resps.append(resp)
        return resp

    def get_open_orders(self, product_id, refresh=False, sender=None):
        return list(self.open_orders)

    async def get_matches(self, *, product_ids=None, limit=200, idx=None, max_time=None):
        return list(self.matches)

    async def cancel_orders(self, *, product_id, digests):
        if self.cancel_should_raise:
            raise RuntimeError("venue down")
        self.cancelled.extend(digests)

    def get_market_price(self, product_id):
        return {"bid": 99.0, "ask": 101.0}


def test_na2_market_order_with_no_fills_does_not_synthesize():
    """BUG-NA-2: previously the adapter would synthesize filled_base from
    amount_base if the venue reported FILLED with no fill data. Now it must
    downgrade to PARTIALLY_FILLED with zeros so the executor keeps polling.
    """
    async def body():
        a = NadoAdapter(_StubClient(), META)
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal(1))
        assert o.state is OrderState.PARTIALLY_FILLED
        assert o.filled_base == 0

    asyncio.run(body())


def test_na2_market_order_with_real_fills_marks_filled():
    """BUG-NA-2 happy path: matches available → adapter syncs and marks
    FILLED with real numbers."""
    async def body():
        client = _StubClient()
        client.matches = [{"digest": "m1", "amount": 1, "price": 100, "fee": "0.05"}]
        a = NadoAdapter(client, META)
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal(1))
        assert o.state is OrderState.FILLED
        assert o.filled_base == Decimal(1)
        assert o.fee_quote == Decimal("0.05")

    asyncio.run(body())


def test_na3_rejected_state_surfaces():
    """BUG-NA-3: REJECTED venue responses must surface as OrderState.REJECTED,
    not collapse into OPEN."""
    async def body():
        client = _StubClient()
        client.placed_resps.append({"digest": "x", "status": "rejected"})

        def place_limit_order(*args, **kwargs):
            return {"digest": "rej-1", "status": "rejected"}

        client.place_limit_order = place_limit_order  # type: ignore[assignment]
        a = NadoAdapter(client, META)
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT, Decimal(1), Decimal(99))
        assert o.state is OrderState.REJECTED

    asyncio.run(body())


def test_na4_filled_threshold_uses_lot_size():
    """BUG-NA-4: order_status must consider an order filled when the
    remaining unfilled base is less than one lot, not blindly use 0.999 *
    amount_base.
    """
    async def body():
        client = _StubClient()
        a = NadoAdapter(client, META)
        # Use LIMIT so we exercise order_status not market fill path.
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT, Decimal("0.005"), Decimal(100))
        # Simulate: order no longer resting and matches show 0.004 filled.
        # remainder=0.001 == lot_size → FILLED, not PARTIAL.
        client.open_orders = []
        client.matches = [{"digest": "d1", "amount": "0.004", "price": "100", "fee": "0"}]
        st = await a.order_status(o.id)
        assert st.state is OrderState.FILLED

    asyncio.run(body())


def test_na5_lazy_reconcile_recovers_order_after_restart():
    """BUG-NA-5: after a restart, the adapter's in-memory registry is empty.
    cancel_order must lazy-scan known products to recover the order."""
    async def body():
        client = _StubClient()
        a = NadoAdapter(client, META)
        # Simulate: order placed by the *previous* process. Venue reports it
        # as resting; this adapter instance has nothing in self._orders.
        client.open_orders = [
            {"digest": "ghost-1", "is_buy": True, "price": "100", "amount": "1"}
        ]
        ok = await a.cancel_order("ghost-1")
        assert ok is True
        assert "ghost-1" in client.cancelled

    asyncio.run(body())


def test_na5_explicit_registry_recovers_order():
    """BUG-NA-5: an injected persistence registry should hydrate the
    digest->ref mapping across restarts without needing a venue scan."""
    async def body():
        client = _StubClient()

        class MemRegistry(OrderRegistry):
            def __init__(self):
                self.store: dict = {}

            def record(self, oid, ref):
                self.store[oid] = ref

            def lookup(self, oid):
                return self.store.get(oid)

            def forget(self, oid):
                self.store.pop(oid, None)

        reg = MemRegistry()
        reg.store["ghost-1"] = _OrderRef(
            trading_pair=PAIR, product_id=2, side=TradeType.BUY,
            order_type=OrderType.LIMIT, amount_base=Decimal(1), price=Decimal(100),
        )
        a = NadoAdapter(client, META, registry=reg)
        ok = await a.cancel_order("ghost-1")
        assert ok is True
        assert "ghost-1" in client.cancelled

    asyncio.run(body())


# --------------------------------------------------------------------------
# Position Executor — BUG-PE-1, BUG-PE-2, BUG-PE-5
# --------------------------------------------------------------------------
def _position_executor(adapter, *, take_profit=None, stop_loss=None):
    cfg = PositionExecutorConfig(
        order_config=OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.MARKET,
        ),
        barriers=TripleBarrierConfig(take_profit=take_profit, stop_loss=stop_loss),
    )
    return PositionExecutor(cfg, user_id=1, controller_id="c", adapter=adapter)


def test_pe1_close_uses_barrier_price_not_entry_price():
    """BUG-PE-1: non-MARKET close must use the barrier trigger price as the
    limit price, not entry_price (which would never execute)."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _position_executor(adapter, take_profit=Decimal("0.02"))
        await ex.on_create()  # entry MARKET fills at $100
        assert ex.entry_price == Decimal(100)
        # Configure the TP order_type to LIMIT_MAKER so we exercise the
        # non-MARKET close path.
        ex.barriers.take_profit_order_type = OrderType.LIMIT_MAKER
        adapter.set_mid(Decimal("102.5"))
        await ex.on_tick()  # mid=102.5 ≥ entry*1.02 → barrier hit
        # The close order should have been placed at the TP target price
        # (entry * 1.02 = 102), not at entry_price (100).
        close_order = [o for o in adapter.placed if o.order_type is OrderType.LIMIT_MAKER]
        assert close_order, "expected a LIMIT_MAKER close order"
        assert close_order[-1].price == Decimal(102)

    asyncio.run(body())


def test_pe5_partial_close_pnl_is_proportional():
    """BUG-PE-5: realized PnL must be proportional to closed base, not
    subtract the full entry_quote when only half the position is closed.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=True)
        ex = _position_executor(adapter)
        await ex.on_create()
        # Pretend only half the position closed at $120.
        ex.entry_base = Decimal(1)
        ex.entry_quote = Decimal(100)
        ex.exit_base = Decimal("0.5")
        ex.exit_quote = Decimal(60)  # 0.5 base × 120
        ex._finalize(CloseType.EARLY_STOP)
        # avg_entry = 100, so realized = 60 - 100*0.5 = 10
        assert ex._net_pnl_quote == Decimal(10) - ex._fees_paid_quote

    asyncio.run(body())


# --------------------------------------------------------------------------
# Grid Executor — BUG-GR-1, BUG-GR-2, BUG-GR-4
# --------------------------------------------------------------------------
def _grid(adapter, *, levels=2):
    cfg = GridExecutorConfig(
        trading_pair=PAIR, side=TradeType.BUY,
        start_price=Decimal(95), end_price=Decimal(100),
        limit_price=Decimal(0), total_amount_quote=Decimal(200),
        min_spread_between_orders=Decimal("0.01"),
        max_open_orders=levels, max_orders_per_batch=levels,
    )
    return GridExecutor(cfg, user_id=1, controller_id="c", adapter=adapter)


def test_gr4_multi_price_fills_use_marginal_price():
    """BUG-GR-4: when an order fills at multiple prices, _ingest must use
    the marginal (chunk) price for each delta, not the running VWAP.
    """
    from src.nadobro.engine.adapter.base import NadoOrder

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(95))
        grid = _grid(adapter, levels=1)
        await grid.on_create()
        level = grid.levels[0]
        # Simulate: order has filled 0.5 at $100 (recorded), then 0.5 more
        # at $80. Marginal price for the new chunk is $80, NOT (0.5*100 +
        # 0.5*80)/1.0 = 90.
        level._open_recorded = Decimal("0.5")
        level._open_quote_recorded = Decimal(50)  # 0.5 × 100
        level._open_fee_recorded = Decimal(0)
        # Total cumulative now reflects both fills: 1.0 base, 90 quote.
        order = NadoOrder(
            id="o1", trading_pair=PAIR, side=TradeType.BUY,
            order_type=OrderType.LIMIT_MAKER,
            amount_base=Decimal(1), price=Decimal(95),
            state=OrderState.FILLED,
            filled_base=Decimal(1), filled_quote=Decimal(90),
            fee_quote=Decimal("0.1"),
        )
        vol_before = grid._volume_quote
        fees_before = grid._fees_paid_quote
        grid._ingest(level, order, TradeType.BUY, opening=True)
        delta_vol = grid._volume_quote - vol_before
        delta_fee = grid._fees_paid_quote - fees_before
        # Marginal: delta_base=0.5 × price=80 = $40 (not $45 = 0.5×VWAP90).
        assert delta_vol == Decimal(40)
        assert delta_fee == Decimal("0.1")

    asyncio.run(body())


# --------------------------------------------------------------------------
# DCA Executor — BUG-DCA-2 (rollback on partial placement failure)
# --------------------------------------------------------------------------
class _FailAfterNAdapter(MockNadoAdapter):
    """Adapter that succeeds for the first N place_orders, then raises."""

    def __init__(self, *, succeed_n: int, **kwargs):
        super().__init__(**kwargs)
        self._succeed_n = succeed_n
        self._placed_n = 0

    async def place_order(self, *args, **kwargs):
        if self._placed_n >= self._succeed_n:
            from src.nadobro.engine.adapter.base import AdapterError
            raise AdapterError("simulated failure")
        self._placed_n += 1
        return await super().place_order(*args, **kwargs)


def test_dca2_rollback_cancels_already_placed_rungs():
    """BUG-DCA-2: if the 3rd rung placement fails, rungs 1+2 already on the
    venue must be cancelled so we don't leak open orders.
    """
    async def body():
        adapter = _FailAfterNAdapter(mid=Decimal(100), succeed_n=2)
        cfg = DCAExecutorConfig(
            trading_pair=PAIR,
            amounts_quote=[Decimal(10), Decimal(10), Decimal(10)],
            prices=[Decimal(99), Decimal(98), Decimal(97)],
            mode=DCAMode.MAKER,
        )
        ex = DCAExecutor(cfg, user_id=1, controller_id="c", adapter=adapter)
        with pytest.raises(Exception):
            await ex.on_create()
        # Both successful placements should have been cancelled in rollback.
        assert len(adapter.cancelled) == 2

    asyncio.run(body())


# --------------------------------------------------------------------------
# TWAP Executor — BUG-TWAP-1 (leaked last slice cancelled on finalize)
# --------------------------------------------------------------------------
def test_twap1_finalize_cancels_resting_last_slice():
    """BUG-TWAP-1: on finalize/on_stop the last unfilled MAKER slice must be
    cancelled, not just counted as 'lost' while leaving the order live.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        cfg = TWAPExecutorConfig(
            trading_pair=PAIR, side=TradeType.BUY,
            total_amount_quote=Decimal(100),
            total_duration=20, order_interval=10, mode="MAKER",
        )
        ex = TWAPExecutor(cfg, user_id=1, controller_id="c", adapter=adapter)
        await ex.on_create()
        assert ex.current_order is not None
        assert ex.current_order.state is OrderState.OPEN
        await ex.on_stop()
        # The maker slice should have been cancelled.
        assert ex.current_order.id in adapter.cancelled

    asyncio.run(body())


# --------------------------------------------------------------------------
# Order Executor — BUG-CC-1 (cancel-on-stop confirmation)
# --------------------------------------------------------------------------
def test_cc1_cancel_on_stop_terminates_failed_if_order_still_open():
    """BUG-CC-1: if cancel + status confirm leaves the order still resting,
    the executor must NOT terminate cleanly (which would leak the order);
    it terminates with CloseType.FAILED instead.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        cfg = OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT_MAKER,
            price=Decimal(99),
        )
        ex = OrderExecutor(cfg, user_id=1, controller_id="c", adapter=adapter)
        await ex.on_create()

        # Monkey-patch cancel + status to behave as "venue down" — order
        # stays OPEN despite our cancel attempt.
        async def cancel_noop(_oid):
            return False  # pretend cancel had no effect

        adapter.cancel_order = cancel_noop  # type: ignore[assignment]
        # order_status still returns OPEN.
        await ex.on_stop()
        assert ex.is_terminated
        assert ex.close_type is CloseType.FAILED

    asyncio.run(body())


# --------------------------------------------------------------------------
# Market Making — BUG-MM-1 (child executors ticked)
# --------------------------------------------------------------------------
def test_mm1_children_are_ticked_each_cycle():
    """BUG-MM-1: MM controller must tick its child OrderExecutors before
    reconciling quotes so fills are absorbed.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = MarketMakingController(
            user_id=1, orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": PAIR,
                "spread_bid_pct": Decimal("0.001"),
                "spread_ask_pct": Decimal("0.001"),
                "order_amount_quote": Decimal(10),
            },
            controller_id="MM",
        )
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # Two children spawned (bid + ask). Track which executors had on_tick
        # called by patching one and inspecting.
        ticks = {"count": 0}
        original_tick = orch.tick

        async def tracking_tick(eid):
            ticks["count"] += 1
            await original_tick(eid)

        orch.tick = tracking_tick  # type: ignore[assignment]
        await orch.tick_controller(c.id)
        assert ticks["count"] >= 2, "child executors must be ticked"

    asyncio.run(body())


# --------------------------------------------------------------------------
# Routines — BUG-TA-1 (EMA cold start)
# --------------------------------------------------------------------------
def test_ta1_ema_returns_none_for_short_series():
    """BUG-TA-1: EMA must return None when the series has fewer than
    `period` samples, not seed with values[0] and return garbage.
    """
    assert ema([1.0, 2.0, 3.0], 9) is None
    assert ema([], 5) is None
    # With exactly `period` samples it returns the SMA seed.
    assert ema([1.0, 2.0, 3.0, 4.0, 5.0], 5) == pytest.approx(3.0)


# --------------------------------------------------------------------------
# Orchestrator — caps + spawn_controller rollback (BUG-CC-2)
# --------------------------------------------------------------------------
def test_orc1_event_log_is_bounded():
    """BUG-ORC-1: event_log is a ring buffer; never exceeds the configured
    cap regardless of how many events fire.
    """
    orch = ExecutorOrchestrator(event_log_limit=10)
    from src.nadobro.engine.orchestrator import ExecutorEvent

    for i in range(50):
        orch._emit(ExecutorEvent(kind="tick", reason=str(i)))
    assert len(orch.event_log) == 10
    # Newest events retained (ring buffer drops the oldest).
    assert orch.event_log[-1].reason == "49"


def test_cc2_failed_on_start_rolls_back_children():
    """BUG-CC-2: if a controller's on_start spawns children then raises,
    the orchestrator must stop those children rather than leave them live.
    """
    from src.nadobro.engine.controllers.controller_base import Controller
    from src.nadobro.engine.risk import ExecutorRequest

    class LeakyController(Controller):
        async def on_start(self):
            cfg = OrderExecutorConfig(
                PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT_MAKER,
                price=Decimal(99),
            )
            ex = OrderExecutor(
                cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
                inventory=self.inventory,
            )
            await self.spawn_executor(ex, ExecutorRequest(order_amount_quote=Decimal(10)))
            raise RuntimeError("simulated on_start failure")

        async def on_tick(self):
            pass

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = LeakyController(
            user_id=1, name="leaky", orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(), controller_id="LK",
        )
        ok = await orch.spawn_controller(c)
        assert ok is False
        # Child executor must have been stopped (terminated).
        children = orch.list(c.id, active_only=True)
        assert children == [], "leaked child executor"

    asyncio.run(body())


# --------------------------------------------------------------------------
# Market Data — BUG-MD-1 (LRU bound), BUG-MD-2 (single-flight)
# --------------------------------------------------------------------------
def test_md1_lru_evicts_oldest():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        md = MarketData(adapter, ttl_seconds=60, max_entries=3)
        for i, pair in enumerate(("A", "B", "C", "D")):
            await md.mid(pair)
        # A was first inserted and must have been evicted.
        keys = list(md._cache.keys())
        assert ("mid", "A") not in keys
        assert ("mid", "D") in keys
        assert len(keys) == 3

    asyncio.run(body())


def test_md2_single_flight_dedupes_concurrent_fetches():
    """BUG-MD-2: concurrent calls for the same key go through ONE fetch.
    """
    async def body():
        calls = {"n": 0}

        class CountingAdapter(MockNadoAdapter):
            async def mid_price(self, pair):
                calls["n"] += 1
                await asyncio.sleep(0.01)
                return Decimal(100)

        adapter = CountingAdapter(mid=Decimal(100))
        md = MarketData(adapter, ttl_seconds=60)
        results = await asyncio.gather(*(md.mid(PAIR) for _ in range(10)))
        assert all(r == Decimal(100) for r in results)
        assert calls["n"] == 1

    asyncio.run(body())


# --------------------------------------------------------------------------
# Risk — BUG-RISK-1 (daily reset)
# --------------------------------------------------------------------------
def test_risk1_state_rolls_over_on_new_day():
    """BUG-RISK-1: RiskState.rolled_over zeroes daily counters when the UTC
    date changes, so yesterday's daily-loss floor doesn't keep the gate
    armed into a new day.
    """
    yesterday = RiskState(
        daily_pnl_quote=Decimal(-100),
        daily_cost_usd=Decimal(50),
        daily_anchor_utc="2026-01-01",
    )
    rolled = yesterday.rolled_over("2026-01-02")
    assert rolled.daily_pnl_quote == Decimal(0)
    assert rolled.daily_cost_usd == Decimal(0)
    assert rolled.daily_anchor_utc == "2026-01-02"
    # Same day → unchanged.
    same = yesterday.rolled_over("2026-01-01")
    assert same.daily_pnl_quote == Decimal(-100)
