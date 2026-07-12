import asyncio
import logging
from decimal import Decimal

import pytest
from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator


def _vb_kwargs(configs, *, adapter=None):
    return dict(
        user_id=1,
        orchestrator=ExecutorOrchestrator(),
        adapter=adapter or MockNadoAdapter(mid=Decimal(100)),
        inventory=InventoryRepository(),
        configs=configs,
        controller_id="VB",
    )


def test_rejects_perp_leverage_quote_and_empty_pair():
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC-USDC", "market": "perp"}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC-USDC", "leverage": 5}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "BTC-PERP"}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": ""}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "USDC"}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "USDT0"}))


def test_accepts_catalog_listed_pairs_per_network():
    for pair in ("KBTC", "WETH", "QQQX", "SPYX", "BTC", "ETH", "kBTC-USDC0", "QQQX-USDT0"):
        controller = VolumeBotController(**_vb_kwargs({"trading_pair": pair}))
        assert controller.trading_pair == pair.upper()


def test_partial_buy_waits_until_fully_filled_before_sell():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        buy_order = buy.order
        half = buy_order.amount_base / Decimal(2)
        adapter.fill_order(buy_order.id, amount=half, price=buy_order.price, partial=True)

        await orch.tick_controller(c.id)

        assert c.phase == "pending_fill"
        assert c.sell_id is None
        assert c.entry_base == half
        assert c.entry_fill_ts > 0

        adapter.fill_order(buy_order.id, price=buy_order.price)
        await orch.tick_controller(c.id)

        sell = orch.get(c.sell_id)
        assert c.phase == "pending_close_fill"
        assert sell is not None
        assert sell.order.amount_base == buy_order.amount_base

    asyncio.run(body())


def test_sell_joins_the_ask_instead_of_forcing_profit():
    """v3: the sell quotes AT the market (join the ask), not at breakeven+edge.
    v2's px >= max(breakeven+fees, entry+edge, ask) made a cycle impossible to
    complete unless price rose — the structural cause of near-zero prod volume."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "KBTC",
                "total_amount_quote": "100",
                "spot_maker_fee_rate": "0.001",
                "vol_max_cycle_loss_bp": 20,
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        sell = orch.get(c.sell_id)
        assert sell is not None
        # ask == 100 and the 20bp loss budget covers both 10bp fees, so the
        # sell joins the ask exactly instead of resting above breakeven.
        assert sell.order.price == Decimal("100.00")
        assert sell.order.amount_base == buy.order.amount_base

    asyncio.run(body())


def test_sell_floor_bounds_the_cycle_loss():
    """The loss budget is a FLOOR, not gone: with fees far above the budget the
    sell rests at breakeven-minus-budget rather than selling at any price."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "KBTC",
                "total_amount_quote": "100",
                "spot_maker_fee_rate": "0.01",
                "vol_max_cycle_loss_bp": 20,
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        sell = orch.get(c.sell_id)
        assert sell is not None
        # breakeven ~= 100*(1.01)/(0.99) ~= 102.02; floor = be*(1-20bp) ~= 101.82
        assert sell.order.price >= Decimal("101.8")

    asyncio.run(body())


def test_sell_waits_for_live_book_and_resumes(caplog):
    """A dead book (RWA market closed) parks the sell leg in market_closed
    instead of quoting blind off the entry floor (v2 rested a sell above a
    closed WNVDAX book for 8.5h in prod). It resumes when the book is back."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))

        adapter.fail_on.add("order_book")
        adapter.fail_remaining = 1
        caplog.set_level(logging.WARNING, logger="src.nadobro.engine.controllers.volume_bot")

        await orch.tick_controller(c.id)
        assert c.phase == "market_closed"
        assert c.sell_id is None
        assert "no live book" in caplog.text

        await orch.tick_controller(c.id)   # book recovered
        assert c.phase == "pending_close_fill"
        assert orch.get(c.sell_id) is not None

    asyncio.run(body())


async def _fill_one_active(adapter, orch, c):
    active = list(orch.list(c.id, active_only=True))
    assert len(active) == 1
    ex = active[0]
    order = ex.order
    adapter.fill_order(order.id, price=order.price)
    await orch.tick_controller(c.id)


def test_single_round_trip_completes_when_no_target():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "40"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        await _fill_one_active(adapter, orch, c)
        await _fill_one_active(adapter, orch, c)

        assert c.completed is True
        assert c.stop_reason == "round_trip_complete"
        assert c.cycles_completed == 1
        assert c.close_base_remaining == 0
        assert not c.is_active

    asyncio.run(body())


def test_loops_until_target_volume_then_completes():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "KBTC",
                "total_amount_quote": "40",
                "target_volume_usd": "200",
                "max_cycles": 50,
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        for _ in range(20):
            if c.completed:
                break
            await _fill_one_active(adapter, orch, c)

        assert c.completed is True
        assert c.stop_reason == "target_volume_hit"
        assert c.cycles_completed >= 2
        assert c.session_volume_usd >= Decimal("200")

    asyncio.run(body())


def test_buy_requotes_to_fresh_touch_on_timer():
    """v3 requoting is TIMER-driven: unfilled past vol_requote_seconds, the buy
    is cancelled and re-placed at the fresh touch. v2 only chased after a ~25bp
    adverse run — a 56-minute fill for $101 on KBTC in prod."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        first_buy = orch.get(c.buy_id)
        assert first_buy.order.price == Decimal("100.00")   # joins the touch

        adapter.set_mid(Decimal(101))
        c.leg_quoted_ts -= 25                                # older than the 20s default
        await orch.tick_controller(c.id)

        assert c.requotes == 1
        assert first_buy.order.id in adapter.cancelled
        second_buy = orch.get(c.buy_id)
        assert second_buy.id != first_buy.id
        assert second_buy.order.price == Decimal("101.00")   # fresh touch
        assert c.phase == "pending_fill"

    asyncio.run(body())


def test_buy_does_not_requote_before_timer():
    """Price movement alone must not churn the quote — the requote timer is
    the only trigger, so queue priority is kept while the order is young."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)

        adapter.set_mid(Decimal(150))
        await orch.tick_controller(c.id)

        assert c.requotes == 0
        assert orch.get(c.buy_id).id == buy.id     # untouched

    asyncio.run(body())


def test_partial_buy_then_external_cancel_sells_the_partial():
    """A buy that fills partially and then terminates must round-trip the
    filled portion — completing without selling strands the bought base."""
    from src.nadobro.engine.adapter.base import OrderState

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        half = buy.order.amount_base / Decimal(2)
        adapter.fill_order(buy.order.id, amount=half, price=buy.order.price, partial=True)
        adapter._orders[buy.order.id].state = OrderState.CANCELLED  # venue cancel

        await orch.tick_controller(c.id)

        assert c.phase == "pending_close_fill"
        sell = orch.get(c.sell_id)
        assert sell is not None
        assert sell.order.amount_base == half      # sells exactly what filled

    asyncio.run(body())


def test_partial_sell_replaces_remainder_and_books_cycle_once():
    from src.nadobro.engine.adapter.base import OrderState

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=buy.order.price)
        await orch.tick_controller(c.id)

        first_sell = orch.get(c.sell_id)
        half = first_sell.order.amount_base / Decimal(2)
        adapter.fill_order(first_sell.order.id, amount=half,
                           price=first_sell.order.price, partial=True)
        adapter._orders[first_sell.order.id].state = OrderState.CANCELLED
        await orch.tick_controller(c.id)

        # Remainder re-placed instead of stranding half the inventory.
        second_sell = orch.get(c.sell_id)
        assert second_sell.id != first_sell.id
        assert second_sell.order.amount_base == half
        assert c.completed is False

        adapter.fill_order(second_sell.order.id, price=second_sell.order.price)
        await orch.tick_controller(c.id)

        assert c.completed is True
        assert c.stop_reason == "round_trip_complete"
        assert c.cycles_completed == 1
        # Volume/PnL book BOTH sell legs exactly once.
        assert c.sold_base == c.entry_base
        assert c.session_volume_usd == c.entry_quote + c.sold_quote
        assert c.session_realized_pnl_usd == c.sold_quote - c.entry_quote

    asyncio.run(body())


def test_zero_fill_buy_requotes_then_completes_no_fill():
    from src.nadobro.engine.adapter.base import OrderState

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        for expected_retries in (1, 2, 3, 3):
            buy = orch.get(c.buy_id)
            adapter._orders[buy.order.id].state = OrderState.CANCELLED
            await orch.tick_controller(c.id)
            assert c.buy_retries == expected_retries

        assert c.completed is True
        assert c.stop_reason == "no_fill"

    asyncio.run(body())


def test_sell_spawn_raise_recovers_instead_of_stranding():
    """If the sell spawn raises mid-transition, the controller must not park in
    ``filled_wait_close`` forever — the next tick retries the sell."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=buy.order.price)

        real_start_sell = c._start_sell_cycle
        boom = {"armed": True}

        async def flaky(amount_base=None):
            if boom.pop("armed", False):
                # A transient-classified error keeps the controller ACTIVE
                # (backoff), which is exactly the case the recovery branch
                # serves; an unknown error would mark it FAILED and take the
                # engine's rebuild path instead.
                raise RuntimeError("venue rate limited")
            return await real_start_sell(amount_base)

        c._start_sell_cycle = flaky
        await orch.tick_controller(c.id)           # raise absorbed by orchestrator
        assert c.phase == "filled_wait_close"      # held base, no branch lost
        assert c.is_active                         # transient => stays ACTIVE

        orch._controller_backoff_until.pop(c.id, None)   # backoff elapsed
        await orch.tick_controller(c.id)           # recovery branch retries
        assert c.phase == "pending_close_fill"
        assert orch.get(c.sell_id) is not None

    asyncio.run(body())


def test_max_cycles_caps_runaway_loop():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "KBTC",
                "total_amount_quote": "40",
                "target_volume_usd": "1000000",
                "max_cycles": 2,
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        for _ in range(10):
            if c.completed:
                break
            await _fill_one_active(adapter, orch, c)

        assert c.completed is True
        assert c.stop_reason == "max_cycles"
        assert c.cycles_completed == 2

    asyncio.run(body())

def test_buy_crosses_with_marketable_limit_after_deadline():
    """Maker-first, cross-on-deadline (the 8bf08d0 feature lost in the
    d10e6f1 merge, restored in v3): a buy unfilled past vol_cross_after_seconds
    is finished with a marketable LIMIT priced through the touch — still a
    price-bounded limit order, fills as taker."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        first_buy = orch.get(c.buy_id)

        c.leg_started_ts -= 80          # older than the 75s default deadline
        await orch.tick_controller(c.id)

        assert c.crosses == 1
        assert c.leg_crossed is True
        assert first_buy.order.id in adapter.cancelled
        cross_buy = orch.get(c.buy_id)
        assert cross_buy.id != first_buy.id
        # ask(100) * (1 + 15bp) = 100.15 — through the touch, price-bounded.
        assert cross_buy.order.price == Decimal("100.15")

        adapter.fill_order(cross_buy.order.id, price=Decimal("100.10"))
        await orch.tick_controller(c.id)
        assert c.phase == "pending_close_fill"      # leg completed, sell placed
        assert c.entry_base > 0

    asyncio.run(body())


def test_sell_cross_respects_loss_floor():
    """A cross that would blow the per-cycle loss budget is skipped: the
    remainder re-rests as a maker sell at max(ask, floor) instead."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "KBTC",
                "total_amount_quote": "100",
                "spot_maker_fee_rate": "0",
                "vol_max_cycle_loss_bp": 5,       # floor 99.95 > bid*(1-15bp)=99.85
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)
        first_sell = orch.get(c.sell_id)
        assert first_sell is not None

        c.leg_started_ts -= 80
        await orch.tick_controller(c.id)

        assert c.crosses == 1                       # attempt counted...
        replacement = orch.get(c.sell_id)
        assert replacement.id != first_sell.id
        assert replacement.order.price == Decimal("100.00")   # ...but re-rested at the ask, not crossed

    asyncio.run(body())


def test_sell_cross_executes_within_loss_budget():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "KBTC",
                "total_amount_quote": "100",
                "spot_maker_fee_rate": "0",
                "vol_max_cycle_loss_bp": 50,      # floor 99.50 < bid*(1-15bp)=99.85
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        c.leg_started_ts -= 80
        await orch.tick_controller(c.id)

        assert c.crosses == 1
        cross_sell = orch.get(c.sell_id)
        # bid(100) * (1 - 15bp) = 99.85 — marketable, still above the floor.
        assert cross_sell.order.price == Decimal("99.85")

        adapter.fill_order(cross_sell.order.id, price=Decimal("99.90"))
        await orch.tick_controller(c.id)
        assert c.cycles_completed == 1              # cycle books despite the small loss

    asyncio.run(body())


def test_market_closed_on_start_waits_and_resumes():
    """No live book at start (RWA market closed) must WAIT, not fail the
    session — prod session 98 died buy_spawn_failed at 01:30 UTC on WGOOGLX."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), fail_on=["order_book"], fail_times=2)
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1,
            orchestrator=orch,
            adapter=adapter,
            inventory=InventoryRepository(),
            configs={"trading_pair": "WNVDAX", "total_amount_quote": "100"},
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        assert c.phase == "market_closed"
        assert c.completed is False
        assert c.buy_id is None

        await orch.tick_controller(c.id)            # still closed (2nd failure)
        assert c.phase == "market_closed"

        await orch.tick_controller(c.id)            # book is back
        assert c.phase == "pending_fill"
        assert orch.get(c.buy_id) is not None

    asyncio.run(body())


def test_vol_risk_limits_track_session_margin():
    """map_risk_limits must size vol caps off session_margin_usd with lot-
    rounding headroom. The old grid fallback gave every vol session $100 caps:
    prod session 104's lot-rounded $101.01 close was rejected 1.4s after the
    buy fill (max_single_order_quote), stranding the bought spot."""
    from src.nadobro.strategy.engine_runtime import map_risk_limits

    lim = map_risk_limits({"session_margin_usd": 100.0}, "vol")
    assert float(lim.max_single_order_quote) == 150.0     # covers $101.01
    assert float(lim.max_position_size_quote) == 150.0

    lim500 = map_risk_limits({"session_margin_usd": 500.0}, "vol")
    assert float(lim500.max_single_order_quote) == 650.0  # $500 margin spawns fine
