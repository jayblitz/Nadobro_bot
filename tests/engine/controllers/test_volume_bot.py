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
