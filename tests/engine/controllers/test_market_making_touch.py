"""Touch-join quote mode for MarketMakingController (Turbo Volume).

quote_mode="touch" glues quotes to the live best bid/ask (improving by one
tick when the spread leaves at least two ticks of room) instead of pricing
mid ± spread. Post-only placement is unchanged, so a touch quote can never
cross. A dead/one-sided book falls back to the mid ± spread targets, and the
default mode ("mid") must behave exactly as before.
"""
import asyncio
import time
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.adapter.base import OrderBookLevel, OrderBookSnapshot
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import TradeType


class BookAdapter(MockNadoAdapter):
    """MockNadoAdapter with a controllable two-sided touch."""

    def __init__(self, *, bid, ask, **kw):
        super().__init__(**kw)
        self.set_book(bid, ask)

    def set_book(self, bid, ask) -> None:
        self._book_bid = Decimal(str(bid))
        self._book_ask = Decimal(str(ask))

    async def order_book(self, trading_pair: str) -> OrderBookSnapshot:
        self._maybe_fail("order_book")
        return OrderBookSnapshot(
            trading_pair=trading_pair,
            bids=[OrderBookLevel(self._book_bid, Decimal(1))],
            asks=[OrderBookLevel(self._book_ask, Decimal(1))],
            timestamp=time.time(),
        )


def _mm(adapter, configs):
    orch = ExecutorOrchestrator()
    c = MarketMakingController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(), configs=configs,
    )
    return orch, c


TOUCH_CFG = {
    "trading_pair": "P",
    "spread_bid_pct": "0.01",
    "spread_ask_pct": "0.01",
    "order_amount_quote": "10",
    "price_distance_tolerance": "0.001",
    "max_base_quote": "1000",
    "quote_mode": "touch",
}


def test_touch_improves_by_one_tick_on_a_wide_book():
    async def body():
        adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        prices = sorted(o.price for o in adapter.placed)
        assert prices == [Decimal("100.01"), Decimal("100.09")]

    asyncio.run(body())


def test_touch_joins_without_improving_on_a_one_tick_book():
    async def body():
        adapter = BookAdapter(bid="100.00", ask="100.01", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        prices = sorted(o.price for o in adapter.placed)
        assert prices == [Decimal("100.00"), Decimal("100.01")]

    asyncio.run(body())


def test_touch_falls_back_to_mid_spread_on_dead_book():
    async def body():
        adapter = BookAdapter(
            bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01",
            fail_on=["order_book"], fail_times=99,
        )
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # mid ± 1% spread — the classic targets.
        prices = sorted(o.price for o in adapter.placed)
        assert prices == [Decimal(99), Decimal(101)]

    asyncio.run(body())


def test_touch_requotes_when_the_touch_moves_a_tick():
    async def body():
        adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert sorted(o.price for o in adapter.placed) == [Decimal("100.01"), Decimal("100.09")]
        # Touch moves up two ticks: the stale bid/ask must be replaced.
        adapter.set_book("100.02", "100.12")
        await orch.tick_controller(c.id)
        fresh = sorted(o.price for o in adapter.placed[-2:])
        assert fresh == [Decimal("100.03"), Decimal("100.11")]
        assert len(adapter.cancelled) == 2

    asyncio.run(body())


def test_touch_leaves_quotes_alone_when_touch_is_unchanged():
    async def body():
        adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        placed_after_first = len(adapter.placed)
        await orch.tick_controller(c.id)
        assert len(adapter.placed) == placed_after_first
        assert adapter.cancelled == []

    asyncio.run(body())


def test_default_mode_is_mid_and_unchanged():
    async def body():
        cfg = dict(TOUCH_CFG)
        cfg.pop("quote_mode")
        adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, cfg)
        assert c.quote_mode == "mid"
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        prices = sorted(o.price for o in adapter.placed)
        assert prices == [Decimal(99), Decimal(101)]

    asyncio.run(body())


def test_touch_does_not_walk_inward_on_its_own_reflection():
    """AUDIT-MM-2026-07-14 #6: once our improved quote IS the venue BBO, the
    improve rule computes target = our_price + 1 tick forever. The inclusive
    one-tick tolerance must PARK the quote instead of cancel-improving on our
    own reflection until bid and ask meet mid-spread."""
    async def body():
        adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert sorted(o.price for o in adapter.placed) == [Decimal("100.01"), Decimal("100.09")]
        # Our own quotes are now the BBO — the venue book reflects them.
        adapter.set_book("100.01", "100.09")
        placed_before = len(adapter.placed)
        for _ in range(3):
            await orch.tick_controller(c.id)
        assert len(adapter.placed) == placed_before   # parked, no walk
        assert adapter.cancelled == []

    asyncio.run(body())


def test_degraded_feed_bid_equals_ask_falls_back_to_mid_pricing():
    """AUDIT-MM-2026-07-14 #7: a mid-only feed substitutes bid = ask = mid;
    that is not a touch to join — fall back to mid ± spread."""
    async def body():
        adapter = BookAdapter(bid="100.00", ask="100.00", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, dict(TOUCH_CFG))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        prices = sorted(o.price for o in adapter.placed)
        assert prices == [Decimal(99), Decimal(101)]  # classic mid ± 1%

    asyncio.run(body())


def test_turbo_projected_exposure_blocks_a_second_full_size_bid():
    """A slight adverse mark leaves the first fill just below the current-value
    cap. The next full quote must be projected before placement or inventory can
    jump from ~1x to ~2x the configured Turbo exposure allowance."""

    async def body():
        cfg = dict(TOUCH_CFG)
        cfg.update({
            "order_amount_quote": "1000",
            "max_base_quote": "1000",
            "margin_quote": "1000",
            "max_net_exposure_pct": 100,
        })
        adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
        orch, c = _mm(adapter, cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)

        # Flat inventory can place the first full-size quote exactly at cap.
        assert len([o for o in adapter.placed if o.side is TradeType.BUY]) == 1
        bid = orch.get(c._bid_id)
        adapter.fill_order(bid.order.id)

        # Mark the fill slightly lower: current exposure is $999.40 (< $1,000),
        # but another $1,000 buy would project to ~$1,999.40 and must be blocked.
        adapter.set_book("99.90", "100.00")
        await orch.tick_controller(c.id)

        assert c._base_value(Decimal("99.95")) < Decimal("1000")
        assert len([o for o in adapter.placed if o.side is TradeType.BUY]) == 1
        assert c._bid_id is None
        assert c._ask_id is not None  # reducing side remains live

    asyncio.run(body())


def test_projected_cap_keeps_reducing_side_open_above_cap():
    cfg = dict(TOUCH_CFG)
    cfg.update({
        "order_amount_quote": "1000",
        "margin_quote": "1000",
        "max_net_exposure_pct": 100,
    })
    adapter = BookAdapter(bid="100.00", ask="100.10", mid=Decimal(100), tick="0.01")
    _, c = _mm(adapter, cfg)
    c.inventory.apply_fill(
        c.user_id, c.trading_pair, c.id,
        TradeType.BUY, Decimal("25"), Decimal("2500"),
    )

    assert not c._projected_order_within_exposure(TradeType.BUY, Decimal("100"))
    # Selling $1,000 reduces $2,500 long exposure to $1,500. It remains above
    # cap, but must stay allowed so the controller can work inventory down.
    assert c._projected_order_within_exposure(TradeType.SELL, Decimal("100"))
