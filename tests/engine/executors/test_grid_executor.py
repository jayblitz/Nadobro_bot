"""GridExecutor tests: level generation, activation_bounds, limit_price stop,
per-level fill -> close, keep_position, adversarial."""
from __future__ import annotations

import asyncio
from decimal import Decimal

from src.nadobro.engine.executors.grid_executor import (
    GridExecutor,
    GridExecutorConfig,
    GridLevelState,
    generate_grid_levels,
)
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import CloseType, OrderType, TradeType
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def _cfg(**kw):
    base = dict(
        trading_pair=PAIR, side=TradeType.BUY, start_price=Decimal(100), end_price=Decimal(110),
        limit_price=Decimal(95), total_amount_quote=Decimal(1000),
        min_spread_between_orders=Decimal("0.02"),
    )
    base.update(kw)
    return GridExecutorConfig(**base)


def _ex(cfg, adapter, inv=None):
    return GridExecutor(cfg, user_id=1, controller_id="c", adapter=adapter, inventory=inv)


def test_level_generation_count_and_prices():
    levels = generate_grid_levels(_cfg())
    assert len(levels) == 5
    assert [lv.open_price for lv in levels] == [
        Decimal(100), Decimal("102.5"), Decimal(105), Decimal("107.5"), Decimal(110)
    ]
    assert levels[0].close_price == Decimal(100) * Decimal("1.02")
    # per-level capital 1000/5 = 200
    assert levels[0].amount_base == Decimal(200) / Decimal(100)


def test_on_create_places_all_in_bounds_open_orders():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        ex = _ex(_cfg(), adapter)
        await ex.on_create()
        assert ex.is_active
        assert len(adapter.placed) == 5
        assert all(o.order_type is OrderType.LIMIT_MAKER for o in adapter.placed)

    asyncio.run(body())


def test_level_fill_places_close_then_completes():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        inv = InventoryRepository()
        ex = _ex(_cfg(), adapter, inv)
        await ex.on_create()
        lvl = ex.levels[0]
        adapter.fill_order(lvl.open_order_id, price=lvl.open_price)
        await ex.on_tick()
        assert lvl.state is GridLevelState.CLOSE_ORDER_PLACED
        assert lvl.close_order_id is not None
        assert inv.get(1, PAIR, "c").buy_amount_base > 0
        adapter.fill_order(lvl.close_order_id, price=lvl.close_price)
        await ex.on_tick()
        assert lvl.state is GridLevelState.COMPLETE

    asyncio.run(body())


def test_activation_bounds_skips_far_levels():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _ex(_cfg(activation_bounds=Decimal("0.03")), adapter)
        await ex.on_create()
        # within 3% of 100 -> only 100 and 102.5
        assert len(adapter.placed) == 2

    asyncio.run(body())


def test_activation_bounds_cancels_when_mid_moves_away():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _ex(_cfg(activation_bounds=Decimal("0.03")), adapter)
        await ex.on_create()
        lvl0 = ex.levels[0]
        oid = lvl0.open_order_id
        adapter.set_mid(Decimal(110))  # 100 now ~9% from mid -> out of bounds
        await ex.on_tick()
        assert lvl0.state is GridLevelState.NOT_ACTIVE
        assert oid in adapter.cancelled

    asyncio.run(body())


def test_limit_price_breach_triggers_stop_loss():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        ex = _ex(_cfg(limit_price=Decimal(98)), adapter)
        await ex.on_create()
        adapter.set_mid(Decimal(97))  # below hard stop
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.STOP_LOSS

    asyncio.run(body())


def test_keep_position_false_flattens_on_stop():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        inv = InventoryRepository()
        ex = _ex(_cfg(keep_position=False), adapter, inv)
        await ex.on_create()
        lvl = ex.levels[0]
        adapter.fill_order(lvl.open_order_id, price=lvl.open_price)
        await ex.on_tick()
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated
        assert inv.get(1, PAIR, "c").net_amount_base == Decimal(0)

    asyncio.run(body())


def test_keep_position_true_retains_on_stop():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        inv = InventoryRepository()
        ex = _ex(_cfg(keep_position=True), adapter, inv)
        await ex.on_create()
        lvl = ex.levels[0]
        adapter.fill_order(lvl.open_order_id, price=lvl.open_price)
        await ex.on_tick()
        bought = inv.get(1, PAIR, "c").buy_amount_base
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated
        assert inv.get(1, PAIR, "c").net_amount_base == bought

    asyncio.run(body())


def test_adversarial_transient_errors_on_open_placement():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105), fail_on=["place_order"], fail_times=2)
        ex = _ex(_cfg(), adapter)
        await ex.on_create()
        # first level retried twice then placed; all levels eventually placed
        assert ex.retries == 2
        assert len(adapter.placed) == 5

    asyncio.run(body())
