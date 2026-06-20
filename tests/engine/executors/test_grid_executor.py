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


def test_take_profit_breach_triggers_take_profit():
    """GRID-TP-DEAD fix: a favorable move of take_profit from the average entry
    flattens the position with CloseType.TAKE_PROFIT. Previously take_profit was
    passed into the barrier but never read, so this never fired."""
    from src.nadobro.engine.types import TripleBarrierConfig

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        # BUY grid; 5% take-profit, no limit_price/stop so only TP can fire.
        cfg = _cfg(
            start_price=Decimal(100), end_price=Decimal(100), limit_price=Decimal(0),
            triple_barrier_config=TripleBarrierConfig(take_profit=Decimal("0.05"), stop_loss=None),
        )
        ex = _ex(cfg, adapter, InventoryRepository())
        await ex.on_create()
        # Fill a level so there's an average entry (~100).
        adapter.fill_order(ex.levels[0].open_order_id, price=Decimal(100))
        await ex.on_tick()
        assert not ex.is_terminated            # +0% move, TP not hit yet
        adapter.set_mid(Decimal(106))          # +6% > 5% TP from avg entry
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.TAKE_PROFIT

    asyncio.run(body())


def test_no_take_profit_without_a_position():
    """TP is inert until there's a real average entry — no position, nothing to
    take, even if mid runs far above the band."""
    from src.nadobro.engine.types import TripleBarrierConfig

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = _cfg(limit_price=Decimal(0),
                   triple_barrier_config=TripleBarrierConfig(take_profit=Decimal("0.01"), stop_loss=None))
        ex = _ex(cfg, adapter, InventoryRepository())
        await ex.on_create()
        adapter.set_mid(Decimal(130))  # way up, but nothing filled
        await ex.on_tick()
        assert not ex.is_terminated

    asyncio.run(body())


def test_reduce_position_books_through_executor_and_advances_accounting():
    """DGRID-BOOK-RACE fix: reduce_position fires ONE reduce-only MARKET, records
    it in the shared inventory (net drops), and advances per-level close
    accounting — fully booked levels complete and their resting close legs are
    cancelled, so the executor and the venue can't drift apart."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        inv = InventoryRepository()
        ex = _ex(_cfg(), adapter, inv)
        await ex.on_create()
        # Fill two open levels → held long inventory + resting close legs.
        held_levels = ex.levels[:2]
        for lv in held_levels:
            adapter.fill_order(lv.open_order_id, price=lv.open_price)
        await ex.on_tick()
        net_before = inv.get(1, PAIR, "c").net_amount_base
        assert net_before > 0
        one_level_base = held_levels[0].filled_base
        cancelled_before = len(adapter.cancelled)

        booked = await ex.reduce_position(one_level_base)

        assert booked > 0
        net_after = inv.get(1, PAIR, "c").net_amount_base
        # Inventory net dropped by (about) the booked amount — views stay in sync.
        assert net_after < net_before
        assert abs(float(net_before - net_after - booked)) < 1e-9
        # The fully-booked level completed and its resting close leg was cancelled.
        assert any(lv.state is GridLevelState.COMPLETE for lv in ex.levels)
        assert len(adapter.cancelled) > cancelled_before

    asyncio.run(body())


def test_reduce_position_caps_at_held_inventory():
    """Asking to reduce more than is held only books what's actually held."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        inv = InventoryRepository()
        ex = _ex(_cfg(), adapter, inv)
        await ex.on_create()
        lv = ex.levels[0]
        adapter.fill_order(lv.open_order_id, price=lv.open_price)
        await ex.on_tick()
        held = lv.filled_base
        booked = await ex.reduce_position(held * Decimal(10))  # ask for way more
        assert abs(float(booked - held)) < 1e-9

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
