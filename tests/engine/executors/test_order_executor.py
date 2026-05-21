"""OrderExecutor tests: each ExecutionStrategy, chaser refresh + cap, retry."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine.executor_base import ExecutorFailed
from src.nadobro.engine.executors.order_executor import (
    OrderExecutor,
    OrderExecutorConfig,
)
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import (
    CloseType,
    ExecutionStrategy,
    LimitChaserConfig,
    OrderType,
    TradeType,
)
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def _ex(cfg, adapter, inv=None):
    return OrderExecutor(cfg, user_id=1, controller_id="c", adapter=adapter, inventory=inv)


def test_market_fills_and_routes_to_inventory():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        cfg = OrderExecutorConfig(PAIR, TradeType.BUY, Decimal(2), ExecutionStrategy.MARKET)
        ex = _ex(cfg, adapter, inv)
        await ex.on_create()
        assert ex.is_terminated and ex.close_type is CloseType.COMPLETED
        hold = inv.get(1, PAIR, "c")
        assert hold.buy_amount_base == Decimal(2)
        assert hold.buy_amount_quote == Decimal(200)

    asyncio.run(body())


def test_limit_waits_then_fills_on_tick():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT, price=Decimal(99)
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.is_active and not ex.is_terminated
        adapter.fill_order(ex.order.id, price=Decimal(99))
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.COMPLETED

    asyncio.run(body())


def test_limit_maker_posts_with_maker_type():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = OrderExecutorConfig(
            PAIR, TradeType.SELL, Decimal(1), ExecutionStrategy.LIMIT_MAKER, price=Decimal(101)
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert adapter.placed[0].order_type is OrderType.LIMIT_MAKER
        assert not ex.is_terminated

    asyncio.run(body())


def test_partial_then_full_fill_delta_accounting():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        cfg = OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(10), ExecutionStrategy.LIMIT, price=Decimal(100)
        )
        ex = _ex(cfg, adapter, inv)
        await ex.on_create()
        adapter.fill_order(ex.order.id, amount=Decimal(4), price=Decimal(100), partial=True)
        await ex.on_tick()
        assert not ex.is_terminated
        assert inv.get(1, PAIR, "c").buy_amount_base == Decimal(4)
        adapter.fill_order(ex.order.id, amount=Decimal(6), price=Decimal(100))
        await ex.on_tick()
        assert ex.is_terminated
        assert inv.get(1, PAIR, "c").buy_amount_base == Decimal(10)

    asyncio.run(body())


def test_chaser_refreshes_when_mid_moves_past_threshold():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        chaser = LimitChaserConfig(distance=Decimal("0.001"), refresh_threshold=Decimal("0.01"))
        cfg = OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT_CHASER, chaser_config=chaser
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.refreshes == 0 and ex.orders_placed == 1
        adapter.set_mid(Decimal(102))  # +2% > 1% threshold
        await ex.on_tick()
        assert ex.refreshes == 1 and ex.orders_placed == 2
        adapter.set_mid(Decimal("102.05"))  # +0.05% < threshold
        await ex.on_tick()
        assert ex.refreshes == 1

    asyncio.run(body())


def test_chaser_caps_at_50_refreshes():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        chaser = LimitChaserConfig(distance=Decimal("0.001"), refresh_threshold=Decimal("0.001"))
        cfg = OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT_CHASER, chaser_config=chaser
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        price = Decimal(100)
        for _ in range(60):
            price = price * Decimal("1.01")
            adapter.set_mid(price)
            await ex.on_tick()
        assert ex.refreshes == 50
        assert ex.orders_placed == 51  # initial + 50

    asyncio.run(body())


def test_retry_then_success():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), fail_on=["place_order"], fail_times=2)
        cfg = OrderExecutorConfig(PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.MARKET)
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.retries == 2
        assert ex.is_terminated and ex.close_type is CloseType.COMPLETED

    asyncio.run(body())


def test_retry_exhausted_marks_failed():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), fail_on=["place_order"], fail_times=10)
        cfg = OrderExecutorConfig(PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.MARKET)
        ex = _ex(cfg, adapter)
        with pytest.raises(ExecutorFailed):
            await ex.on_create()
        assert ex.is_terminated and ex.close_type is CloseType.FAILED
        assert ex.retries == 3

    asyncio.run(body())


def test_config_requires_price_for_limit():
    with pytest.raises(ValueError):
        OrderExecutorConfig(PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT)


def test_config_requires_chaser_config():
    with pytest.raises(ValueError):
        OrderExecutorConfig(PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT_CHASER)


def test_on_stop_cancels_open_order():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = OrderExecutorConfig(
            PAIR, TradeType.BUY, Decimal(1), ExecutionStrategy.LIMIT, price=Decimal(99)
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        order_id = ex.order.id
        await ex.on_stop()
        assert ex.is_terminated and ex.close_type is CloseType.EARLY_STOP
        assert order_id in adapter.cancelled

    asyncio.run(body())


def test_sell_chaser_posts_above_mid():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        chaser = LimitChaserConfig(distance=Decimal("0.001"), refresh_threshold=Decimal("0.01"))
        cfg = OrderExecutorConfig(
            PAIR, TradeType.SELL, Decimal(1), ExecutionStrategy.LIMIT_CHASER, chaser_config=chaser
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        # sell chaser posts at mid * (1 + distance) = 100.1
        assert adapter.placed[0].price == Decimal("100.1")

    asyncio.run(body())
