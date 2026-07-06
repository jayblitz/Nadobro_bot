import asyncio
import time
from decimal import Decimal

import pytest
from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.adapter.base import OrderState
from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import OrderType, TradeType


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


def test_cross_on_timeout_replaces_unfilled_maker_buy_with_market():
    # Option 5: a post-only buy that stays unfilled past the timeout is cancelled
    # and re-placed as a taker MARKET order for the full size.
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
                "vol_cross_after_seconds": 1,
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        maker = orch.get(c.buy_id)
        maker_id = maker.order.id
        target = c.buy_target_base
        assert maker.order.order_type is OrderType.LIMIT_MAKER
        # Simulate the maker resting unfilled past the timeout.
        c.buy_placed_ts = time.time() - 100

        await orch.tick_controller(c.id)

        assert c.buy_crossed is True
        assert maker_id in adapter.cancelled              # maker cancelled
        crossed = orch.get(c.buy_id)
        assert crossed.id != maker.id                     # a NEW executor
        assert crossed.order.order_type is OrderType.MARKET
        assert crossed.order.amount_base == target        # full size re-placed
        # The market buy auto-fills; the next tick advances to the sell leg.
        await orch.tick_controller(c.id)
        assert c.phase == "pending_close_fill"
        assert c.entry_base == target

    asyncio.run(body())


def test_cross_does_not_double_place_when_maker_has_a_fill():
    # Safety guard: if a fill lands at/before the cancel, _cross_to_taker must
    # return None (no market order) so the position is never doubled.
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
        maker = orch.get(c.buy_id)
        # A partial fill exists on the resting maker.
        adapter.fill_order(maker.order.id, amount=maker.order.amount_base / Decimal(2),
                           price=maker.order.price, partial=True)
        placed_before = len(adapter.placed)

        result = await c._cross_to_taker(
            maker, TradeType.BUY, c.buy_target_base, maker.config.position_action
        )

        assert result is None                             # no cross
        assert len(adapter.placed) == placed_before       # no new order placed
        assert maker.order.id in adapter.cancelled        # maker still cancelled

    asyncio.run(body())


def test_cross_disabled_when_timeout_zero():
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
                "vol_cross_after_seconds": 0,
            },
            controller_id="VB",
        )
        await orch.spawn_controller(c)
        maker_id = orch.get(c.buy_id).order.id
        c.buy_placed_ts = time.time() - 100  # well past any timeout

        await orch.tick_controller(c.id)

        # Disabled -> still resting, never crossed/cancelled.
        assert c.buy_crossed is False
        assert maker_id not in adapter.cancelled
        assert c.phase == "pending_fill"

    asyncio.run(body())


def test_effectively_full_tolerates_one_lot_rounding():
    # A taker MARKET cross is lot-rounded DOWN by the venue, so an exact
    # filled>=target test would strand the just-bought/sold base. The gate now
    # treats a fill within one lot (mock lot_size = 0.001) as complete.
    c = VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC"}))
    assert c._effectively_full(Decimal("1.33333"), Decimal("1.33333")) is True   # exact
    assert c._effectively_full(Decimal("1.34"), Decimal("1.33333")) is True       # over
    assert c._effectively_full(Decimal("1.333"), Decimal("1.33333")) is True      # within one lot
    assert c._effectively_full(Decimal("1.30"), Decimal("1.33333")) is False      # >1 lot short
    assert c._effectively_full(Decimal("0"), Decimal("1.0")) is False             # nothing filled


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
