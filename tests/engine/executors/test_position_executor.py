"""PositionExecutor (Triple Barrier) tests: each close_type, trailing math,
barrier priority, keep_position, retry -> FAILED."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine.executor_base import ExecutorFailed
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.executors.position_executor import (
    PositionExecState,
    PositionExecutor,
    PositionExecutorConfig,
)
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import (
    CloseType,
    ExecutionStrategy,
    OrderType,
    TradeType,
    TripleBarrierConfig,
    TrailingStop,
)
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def _pos(adapter, barriers, *, side=TradeType.BUY, keep_position=False, inv=None):
    oc = OrderExecutorConfig(PAIR, side, Decimal(1), ExecutionStrategy.MARKET)
    cfg = PositionExecutorConfig(order_config=oc, barriers=barriers)
    return PositionExecutor(
        cfg, user_id=1, controller_id="c", adapter=adapter, inventory=inv,
        keep_position=keep_position,
    )


def test_entry_fills_and_activates():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05")))
        await ex.on_create()
        assert ex.position_state is PositionExecState.ACTIVE_POSITION
        assert ex.entry_price == Decimal(100)

    asyncio.run(body())


def test_take_profit_close():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05"), stop_loss=Decimal("0.05")))
        await ex.on_create()
        adapter.set_mid(Decimal(106))
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.TAKE_PROFIT
        assert ex.metrics()["net_pnl_quote"] == Decimal(6)

    asyncio.run(body())


def test_stop_loss_close():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05"), stop_loss=Decimal("0.05")))
        await ex.on_create()
        adapter.set_mid(Decimal(94))
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.STOP_LOSS

    asyncio.run(body())


def test_time_limit_close():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(time_limit=5))
        await ex.on_create()
        ex.opened_at -= 100  # pretend 100s elapsed
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.TIME_LIMIT

    asyncio.run(body())


def test_trailing_stop_arms_and_triggers():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(
            adapter,
            TripleBarrierConfig(
                trailing_stop=TrailingStop(activation_price=Decimal("0.02"), trailing_delta=Decimal("0.01"))
            ),
        )
        await ex.on_create()
        adapter.set_mid(Decimal(103))  # +3% -> arms; stop = 103*0.99 = 101.97
        await ex.on_tick()
        assert ex.trailing_armed and not ex.is_terminated
        adapter.set_mid(Decimal(105))  # new high; stop = 103.95
        await ex.on_tick()
        assert not ex.is_terminated
        adapter.set_mid(Decimal("103.9"))  # <= 103.95 -> trail trigger
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.TRAILING_STOP

    asyncio.run(body())


def test_barrier_priority_time_limit_beats_take_profit():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05"), time_limit=5))
        await ex.on_create()
        ex.opened_at -= 100
        adapter.set_mid(Decimal(110))  # would be TP, but time elapsed
        await ex.on_tick()
        assert ex.close_type is CloseType.TIME_LIMIT

    asyncio.run(body())


def test_barrier_priority_stop_loss_beats_trailing():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(
            adapter,
            TripleBarrierConfig(
                stop_loss=Decimal("0.05"),
                trailing_stop=TrailingStop(Decimal("0.02"), Decimal("0.01")),
            ),
        )
        await ex.on_create()
        adapter.set_mid(Decimal(103))  # arm trailing
        await ex.on_tick()
        assert ex.trailing_armed and not ex.is_terminated
        adapter.set_mid(Decimal(94))  # both SL and trailing would fire; SL wins
        await ex.on_tick()
        assert ex.close_type is CloseType.STOP_LOSS

    asyncio.run(body())


def test_early_stop_keep_position_retains_inventory():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        ex = _pos(adapter, TripleBarrierConfig(stop_loss=Decimal("0.1")), keep_position=True, inv=inv)
        await ex.on_create()
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated and ex.close_type is CloseType.EARLY_STOP
        assert inv.get(1, PAIR, "c").net_amount_base == Decimal(1)  # position retained

    asyncio.run(body())


def test_early_stop_flatten_clears_inventory():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        ex = _pos(adapter, TripleBarrierConfig(stop_loss=Decimal("0.1")), keep_position=False, inv=inv)
        await ex.on_create()
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated and ex.close_type is CloseType.EARLY_STOP
        assert inv.get(1, PAIR, "c").net_amount_base == Decimal(0)  # flattened

    asyncio.run(body())


def test_completed_via_manual_close():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05")), keep_position=False)
        await ex.on_create()
        await ex.on_stop(CloseType.COMPLETED)
        assert ex.is_terminated and ex.close_type is CloseType.COMPLETED

    asyncio.run(body())


def test_failed_when_entry_placement_exhausts_retries():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), fail_on=["place_order"], fail_times=10)
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05")))
        with pytest.raises(ExecutorFailed):
            await ex.on_create()
        assert ex.is_terminated and ex.close_type is CloseType.FAILED

    asyncio.run(body())


def test_short_take_profit_and_pnl():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(take_profit=Decimal("0.05")), side=TradeType.SELL)
        await ex.on_create()
        adapter.set_mid(Decimal(94))  # -6% -> short TP
        await ex.on_tick()
        assert ex.close_type is CloseType.TAKE_PROFIT
        assert ex.metrics()["net_pnl_quote"] == Decimal(6)

    asyncio.run(body())


def _pos_limit_entry(adapter, barriers, *, side=TradeType.BUY, inv=None):
    oc = OrderExecutorConfig(PAIR, side, Decimal(1), ExecutionStrategy.LIMIT, price=Decimal(100))
    cfg = PositionExecutorConfig(order_config=oc, barriers=barriers)
    return PositionExecutor(cfg, user_id=1, controller_id="c", adapter=adapter, inventory=inv)


def test_limit_entry_opens_then_fills_on_tick():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos_limit_entry(adapter, TripleBarrierConfig(take_profit=Decimal("0.05")))
        await ex.on_create()
        assert ex.position_state is PositionExecState.OPENING
        await ex.on_tick()  # entry still resting
        assert ex.position_state is PositionExecState.OPENING
        adapter.fill_order(ex.entry_order.id, price=Decimal(100))
        await ex.on_tick()  # entry fills -> active
        assert ex.position_state is PositionExecState.ACTIVE_POSITION
        assert ex.entry_price == Decimal(100)

    asyncio.run(body())


def test_limit_close_polls_until_filled():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        b = TripleBarrierConfig(
            take_profit=Decimal("0.05"), take_profit_order_type=OrderType.LIMIT
        )
        ex = _pos(adapter, b)
        await ex.on_create()  # market entry @100 -> active
        adapter.set_mid(Decimal(106))
        await ex.on_tick()  # TP hit -> LIMIT close placed (resting)
        assert ex.position_state is PositionExecState.CLOSING and not ex.is_terminated
        adapter.fill_order(ex.close_order.id, price=Decimal(106))
        await ex.on_tick()  # poll close -> filled -> finalize
        assert ex.is_terminated and ex.close_type is CloseType.TAKE_PROFIT

    asyncio.run(body())


def test_on_stop_during_closing_terminates():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        b = TripleBarrierConfig(
            take_profit=Decimal("0.05"), take_profit_order_type=OrderType.LIMIT
        )
        ex = _pos(adapter, b)
        await ex.on_create()
        adapter.set_mid(Decimal(106))
        await ex.on_tick()
        assert ex.position_state is PositionExecState.CLOSING
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated

    asyncio.run(body())


def test_short_stop_loss():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(adapter, TripleBarrierConfig(stop_loss=Decimal("0.05")), side=TradeType.SELL)
        await ex.on_create()
        adapter.set_mid(Decimal(106))  # +6% against a short -> SL
        await ex.on_tick()
        assert ex.close_type is CloseType.STOP_LOSS

    asyncio.run(body())


def test_short_trailing_stop():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        ex = _pos(
            adapter,
            TripleBarrierConfig(trailing_stop=TrailingStop(Decimal("0.02"), Decimal("0.01"))),
            side=TradeType.SELL,
        )
        await ex.on_create()
        adapter.set_mid(Decimal(97))  # -3% in our favor -> arms; stop = 97*1.01 = 97.97
        await ex.on_tick()
        assert ex.trailing_armed and not ex.is_terminated
        adapter.set_mid(Decimal(95))  # new low; stop = 95.95
        await ex.on_tick()
        assert not ex.is_terminated
        adapter.set_mid(Decimal("96.1"))  # >= 95.95 -> trail trigger
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.TRAILING_STOP

    asyncio.run(body())
