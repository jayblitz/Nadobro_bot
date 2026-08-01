"""Volume Bot v4.2 execution algos — maker TWAP (default) and chase.

The measured prize (KBTC spot, 2026-07-31): a taker round trip costs 14.3 bp
(5.7 bp spread + 2x4.3 bp fee); resting costs 3.6 bp. That is ~4x the volume
per unit of the session loss budget. Price impact at $100-$5,000 measures
0.00 bp, so this is a FEE decision, not an impact one — which is why these
tests assert order TYPE and crossing discipline, not price improvement.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import OrderType

PAIR = "KBTC"


class SpreadAdapter(MockNadoAdapter):
    """Mock book with a real spread — the shared mock quotes bid == ask == mid,
    which cannot express maker-vs-taker pricing at all."""

    def __init__(self, *a, spread_bp=Decimal("20"), **kw):
        super().__init__(*a, **kw)
        self.spread_bp = Decimal(str(spread_bp))

    async def order_book(self, trading_pair):
        from src.nadobro.engine.adapter.base import OrderBookLevel, OrderBookSnapshot
        import time as _t

        mid = self._current_mid()
        half = mid * self.spread_bp / Decimal(20000)
        return OrderBookSnapshot(
            trading_pair=trading_pair,
            bids=[OrderBookLevel(mid - half, Decimal(10))],
            asks=[OrderBookLevel(mid + half, Decimal(10))],
            timestamp=_t.time(),
        )


def _vb(adapter, orch, **cfg):
    configs = {
        "trading_pair": PAIR,
        "total_amount_quote": "100",
        "target_volume_usd": "1000000",
        "spot_taker_fee_rate": "0.00033",
        "vol_builder_fee_rate": "0.0001",
    }
    configs.update(cfg)
    return VolumeBotController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(), configs=configs, controller_id="VB",
    )


# --------------------------------------------------------------------------
# Default: maker TWAP
# --------------------------------------------------------------------------
def test_maker_twap_is_the_shipped_default():
    adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
    c = _vb(adapter, ExecutorOrchestrator())
    assert c.execution_algo == "twap"
    assert c.taker_mode is False and c.maker_mode is True


def test_twap_rests_post_only_instead_of_crossing():
    """The whole point: pay 1.8 bp resting, not 4.3 bp crossing."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch)
        await orch.spawn_controller(c)

        placed = adapter.placed[0]
        assert placed.order_type is OrderType.LIMIT_MAKER, "must be post-only"
        book = await adapter.order_book(PAIR)
        # A maker buy may IMPROVE the bid by a tick (it becomes the new best
        # bid — price-time priority). The invariant is that it does not cross
        # the ASK, which is what would make it a taker fill.
        assert placed.price < book.best_ask, (
            f"a maker buy must not cross the ask: {placed.price} vs {book.best_ask}"
        )
        assert placed.price >= book.best_bid, "and should be at/improving the touch"

    asyncio.run(body())


def test_twap_sell_leg_also_rests():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=buy.order.price)
        await orch.tick_controller(c.id)

        sell = adapter.placed[-1]
        assert sell.order_type is OrderType.LIMIT_MAKER
        book = await adapter.order_book(PAIR)
        assert sell.price > book.best_bid, (
            f"a maker sell must not cross the bid: {sell.price} vs {book.best_bid}"
        )
        assert sell.price <= book.best_ask, "and should be at/improving the touch"

    asyncio.run(body())


def test_twap_does_not_cross_while_on_schedule():
    """Patience is the product: an unfilled maker order early in the horizon
    must keep resting, not escalate."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_twap_horizon_seconds=600.0,
                vol_chase_interval_seconds=0)     # isolate crossing from chasing
        await orch.spawn_controller(c)
        n = len(adapter.placed)
        for _ in range(5):
            await orch.tick_controller(c.id)
        assert len(adapter.placed) == n, "must keep resting, not re-place or cross"
        assert c.crosses == 0

    asyncio.run(body())


def test_twap_crosses_once_materially_behind_schedule():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_twap_horizon_seconds=60.0,
                vol_chase_interval_seconds=0)
        await orch.spawn_controller(c)
        assert c.crosses == 0
        c.leg_started_ts -= 600.0          # blow through the horizon, nothing filled
        await orch.tick_controller(c.id)
        assert c.crosses == 1, "a leg far behind schedule must escalate"
        assert adapter.placed[-1].order_type is OrderType.LIMIT, (
            "escalation crosses with a BOUNDED limit, never a naked market order"
        )

    asyncio.run(body())


def test_taker_budget_stops_further_crossing():
    """Past the budget the leg deliberately runs late rather than reverting to
    the 14.3 bp cost curve."""
    from src.nadobro.quant.twap_schedule import should_cross

    spent = dict(filled_quote=0, elapsed_seconds=95, horizon_seconds=100)
    assert should_cross(100, crossed_quote=0, max_taker_frac="0.25", **spent)
    assert not should_cross(100, crossed_quote=30, max_taker_frac="0.25", **spent)


# --------------------------------------------------------------------------
# Chase
# --------------------------------------------------------------------------
def test_chase_is_impatient_by_construction():
    adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
    twap = _vb(adapter, ExecutorOrchestrator())
    chase = _vb(adapter, ExecutorOrchestrator(), vol_execution_algo="chase")
    assert chase.chase_interval_seconds < twap.chase_interval_seconds
    assert chase.twap_horizon_seconds < twap.twap_horizon_seconds
    assert chase.maker_mode is True, "chase is still a MAKER algo"


def test_chase_reposts_at_the_fresh_touch_when_the_book_moves():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_execution_algo="chase",
                vol_chase_interval_seconds=5.0, vol_twap_horizon_seconds=600.0)
        await orch.spawn_controller(c)
        first = adapter.placed[0]

        adapter.set_mid(Decimal(101))       # book walks away from our bid
        c.leg_quoted_ts -= 60.0             # chase interval elapses
        await orch.tick_controller(c.id)

        assert len(adapter.placed) == 2, "must cancel and re-post"
        repriced = adapter.placed[-1]
        assert repriced.order_type is OrderType.LIMIT_MAKER, "still maker"
        assert repriced.price > first.price, "must follow the book up"
        book = await adapter.order_book(PAIR)
        assert repriced.price < book.best_ask, "and still not cross the ask"
        assert c.chases == 1

    asyncio.run(body())


# --------------------------------------------------------------------------
# Reporting — the numbers that prove the prize is being realised
# --------------------------------------------------------------------------
def test_execution_metrics_are_reported():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=buy.order.price)
        await orch.tick_controller(c.id)

        m = c.volume_metrics()
        assert m["vol_execution_algo"] == "twap"
        assert m["vol_maker_fills"] >= 1
        assert m["vol_maker_fill_ratio"] == 1.0, "a rested fill is a maker fill"
        assert m["vol_taker_fills"] == 0
        assert "vol_chases" in m and "vol_crossed_quote" in m

    asyncio.run(body())


def test_taker_algo_is_still_reachable_as_the_kill_switch():
    adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
    c = _vb(adapter, ExecutorOrchestrator(), vol_execution_algo="taker")
    assert c.taker_mode is True and c.execution_algo == "taker"
    legacy = _vb(adapter, ExecutorOrchestrator(), vol_taker_mode=1)
    assert legacy.taker_mode is True, "the legacy kill-switch still forces taker"


# --------------------------------------------------------------------------
# MAKER-LEG-STALL (self-review 2026-07-31). Schedule debt alone cannot rescue
# a PARTIALLY filled leg: at 60% done the remaining 40% of debt sits under the
# 50% tolerance, so it never escalates. Once the chase budget is also spent the
# leg rests forever and the cycle never completes — the exact v3 stall this
# whole line of work exists to remove. Patience needs a hard end.
# --------------------------------------------------------------------------
def test_partially_filled_maker_leg_cannot_rest_forever():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_twap_horizon_seconds=60.0,
                vol_chase_interval_seconds=5.0)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        # 60% filled -> debt 40 < tolerance 50, so debt can never escalate it.
        adapter.fill_order(buy.order.id, amount=buy.order.amount_base * Decimal("0.6"),
                           price=buy.order.price, partial=True)
        c.requotes = c._MAX_REQUOTES_PER_CYCLE      # chase budget spent
        c.leg_started_ts -= 10_000                  # far past the hard deadline

        await orch.tick_controller(c.id)
        assert c.crosses == 1, "patience must have a hard end"

    asyncio.run(body())


def test_hard_deadline_rescues_even_with_chasing_disabled():
    """With vol_chase_interval_seconds=0 the chase budget never depletes, so
    the deadline is the ONLY thing that can finish a stuck maker leg."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_twap_horizon_seconds=60.0,
                vol_chase_interval_seconds=0, vol_leg_hard_deadline_mult=3.0)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, amount=buy.order.amount_base * Decimal("0.6"),
                           price=buy.order.price, partial=True)
        assert c.requotes == 0, "precondition: chasing is off, budget untouched"

        c.leg_started_ts -= 100.0        # inside 3x horizon (180s) -> still patient
        await orch.tick_controller(c.id)
        assert c.crosses == 0

        c.leg_started_ts -= 200.0        # now past 3x horizon
        await orch.tick_controller(c.id)
        assert c.crosses == 1, "the hard deadline must finish it"

    asyncio.run(body())


def test_safety_valve_overrides_the_taker_budget():
    """The taker budget bounds ECONOMIC crossing. It must not be able to cause
    a permanent stall — an un-completable leg is worse than the fee, the same
    reasoning that lets the v4.1 emergency exit price through the loss floor."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_twap_horizon_seconds=60.0,
                vol_chase_interval_seconds=5.0)
        await orch.spawn_controller(c)
        c.crossed_quote = Decimal("999")            # budget fully spent
        c.requotes = c._MAX_REQUOTES_PER_CYCLE
        c.leg_started_ts -= 10_000
        await orch.tick_controller(c.id)
        assert c.crosses == 1, "the valve must bypass the taker budget"

    asyncio.run(body())


# --------------------------------------------------------------------------
# The v4.1 safety work was all pinned in TAKER mode, but maker TWAP is now the
# shipped default. These re-assert the money-critical invariants on the path
# users actually get.
# --------------------------------------------------------------------------
def test_session_loss_stop_fires_on_the_maker_default_path():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("20"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_session_loss_limit_usd="5",
                vol_chase_interval_seconds=0, vol_twap_horizon_seconds=600.0)
        assert c.execution_algo == "twap", "precondition: the shipped default"

        await orch.spawn_controller(c)
        for _ in range(400):
            if c.completed:
                break
            for attr in ("buy_id", "sell_id"):
                oid = getattr(c, attr)
                ex = orch.get(oid) if oid else None
                if ex is None or ex.is_terminated:
                    continue
                px = (ex.order.price if attr == "buy_id"
                      else ex.order.price * Decimal("0.995"))   # a real loss
                adapter.fill_order(ex.order.id, price=px)
                await orch.tick_controller(c.id)

        assert c.stop_reason == "session_loss_limit"
        assert c.session_realized_pnl_usd <= Decimal("-5")
        assert c.entry_base - c.sold_base == 0, "must stop FLAT"

    asyncio.run(body())


def test_maker_default_stops_flat_so_the_sweep_takes_nothing():
    """A flat bot must authorise no sweep — otherwise min(wallet, cap) reaches
    the user's own pre-existing spot."""
    from src.nadobro.strategy.strategy_lifecycle import _volume_spot_managed_size

    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="0",
                vol_chase_interval_seconds=0, vol_twap_horizon_seconds=600.0)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=buy.order.price)
        await orch.tick_controller(c.id)
        sell = orch.get(c.sell_id)
        adapter.fill_order(sell.order.id, price=sell.order.price)
        await orch.tick_controller(c.id)

        assert c.entry_base - c.sold_base == 0
        assert _volume_spot_managed_size(dict(c.volume_metrics())) == 0.0

    asyncio.run(body())
