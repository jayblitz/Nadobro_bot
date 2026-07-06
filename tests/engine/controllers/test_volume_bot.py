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


def test_sell_price_covers_positive_maker_fee_and_edge():
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
                "vol_maker_offset_bp": 0,
                "vol_min_edge_bp": 0,
                "spot_maker_fee_rate": "0.001",
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        sell = orch.get(c.sell_id)
        assert sell is not None
        assert sell.order.price > Decimal("100.19")
        assert sell.order.amount_base == buy.order.amount_base

    asyncio.run(body())


def test_sell_price_logs_when_order_book_guard_unavailable(caplog):
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

        assert c.phase == "pending_close_fill"
        assert "sell price using fee/entry floor without live book guard" in caplog.text
        assert "post-only sell may reject or rest away from book" in caplog.text

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


def test_buy_chases_price_up():
    """The resting maker buy must FOLLOW the price: when mid runs away above
    the initial gap + reprice threshold, cancel and requote near the new mid."""
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
        first_price = first_buy.order.price
        assert first_price < Decimal(100)          # rests below mid

        adapter.set_mid(Decimal(101))              # > 5bp offset + 20bp threshold
        await orch.tick_controller(c.id)

        assert c.buy_reprices == 1
        assert first_buy.order.id in adapter.cancelled
        second_buy = orch.get(c.buy_id)
        assert second_buy.id != first_buy.id
        assert second_buy.order.price > first_price   # requoted near the new mid
        assert c.phase == "pending_fill"

    asyncio.run(body())


def test_buy_does_not_chase_within_threshold():
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

        adapter.set_mid(Decimal("100.1"))          # 10bp: inside 5bp gap + 20bp band
        await orch.tick_controller(c.id)

        assert c.buy_reprices == 0
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
