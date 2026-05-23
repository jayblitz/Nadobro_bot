"""DCAExecutor tests: weighted-average entry, TP/SL vs WAE, MAKER/TAKER,
trailing, adversarial partial fill."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine.executors.dca_executor import (
    DCAExecutor,
    DCAExecutorConfig,
    DCAMode,
)
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import CloseType, TradeType, TrailingStop
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def _ex(cfg, adapter, inv=None):
    return DCAExecutor(cfg, user_id=1, controller_id="c", adapter=adapter, inventory=inv)


def test_config_rejects_mismatched_lengths():
    with pytest.raises(ValueError):
        DCAExecutorConfig(PAIR, amounts_quote=[Decimal(1)], prices=[Decimal(1), Decimal(2)])


def test_maker_weighted_average_and_take_profit():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000), Decimal(900)], prices=[Decimal(100), Decimal(90)],
            take_profit=Decimal("0.05"),
        )
        ex = _ex(cfg, adapter, inv)
        await ex.on_create()
        assert len(adapter.placed) == 2
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))  # 10 base
        adapter.fill_order(ex.rungs[1].order_id, price=Decimal(90))   # 10 base
        await ex.on_tick()
        assert ex.wae == Decimal(95)  # 1900 / 20
        assert ex.is_terminated and ex.close_type is CloseType.TAKE_PROFIT
        assert ex.metrics()["net_pnl_quote"] == Decimal(100)  # (2000-1900)

    asyncio.run(body())


def test_stop_loss_vs_wae():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)], stop_loss=Decimal("0.05")
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))
        adapter.set_mid(Decimal(94))  # WAE 100, SL 95
        await ex.on_tick()
        assert ex.close_type is CloseType.STOP_LOSS

    asyncio.run(body())


def test_taker_fires_when_mid_reaches_rung():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(95)],
            mode=DCAMode.TAKER, take_profit=Decimal("0.10"),
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert len(adapter.placed) == 0  # nothing posted up front
        await ex.on_tick()  # mid 100 hasn't reached buy rung 95
        assert ex.filled_base == 0
        adapter.set_mid(Decimal(94))
        await ex.on_tick()  # mid <= 95 -> market buy
        assert ex.filled_base > 0

    asyncio.run(body())


def test_trailing_stop_interaction():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)],
            trailing_stop=TrailingStop(Decimal("0.02"), Decimal("0.01")),
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))
        adapter.set_mid(Decimal(103))
        await ex.on_tick()
        assert ex.trailing_armed and not ex.is_terminated
        adapter.set_mid(Decimal("101.5"))
        await ex.on_tick()
        assert ex.close_type is CloseType.TRAILING_STOP

    asyncio.run(body())


def test_adversarial_partial_then_full_fill():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)], take_profit=Decimal("0.50")
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, amount=Decimal(4), price=Decimal(100), partial=True)
        await ex.on_tick()
        assert ex.filled_base == Decimal(4) and not ex.is_terminated
        adapter.fill_order(ex.rungs[0].order_id, amount=Decimal(6), price=Decimal(100))
        await ex.on_tick()
        assert ex.filled_base == Decimal(10)

    asyncio.run(body())


def test_short_dca_take_profit():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)],
            side=TradeType.SELL, take_profit=Decimal("0.05"),
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))  # sell entry
        adapter.set_mid(Decimal(94))  # short TP at -5%
        await ex.on_tick()
        assert ex.close_type is CloseType.TAKE_PROFIT

    asyncio.run(body())


def test_short_dca_trailing_stop():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)],
            side=TradeType.SELL, trailing_stop=TrailingStop(Decimal("0.02"), Decimal("0.01")),
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))
        adapter.set_mid(Decimal(95))  # -5% favorable -> arms; min=95, stop=95.95
        await ex.on_tick()
        assert ex.trailing_armed and not ex.is_terminated
        adapter.set_mid(Decimal(96))  # bounces back >= 95.95 -> trail
        await ex.on_tick()
        assert ex.close_type is CloseType.TRAILING_STOP

    asyncio.run(body())


def test_close_polls_until_filled():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)], take_profit=Decimal("0.05")
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))
        adapter.set_mid(Decimal(106))
        await ex.on_tick()  # TP -> market close placed but not auto-filled
        assert ex.dca_state.name == "CLOSING" and not ex.is_terminated
        adapter.fill_order(ex.close_order.id, price=Decimal(106))
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.TAKE_PROFIT

    asyncio.run(body())


def test_on_stop_flattens_and_cancels_unfilled_rungs():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000), Decimal(900)], prices=[Decimal(100), Decimal(90)]
        )
        ex = _ex(cfg, adapter, inv)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))
        await ex.on_tick()
        rung1_id = ex.rungs[1].order_id
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated
        assert rung1_id in adapter.cancelled
        assert inv.get(1, PAIR, "c").net_amount_base == Decimal(0)

    asyncio.run(body())


def test_on_stop_keep_position_leaves_inventory():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        cfg = DCAExecutorConfig(
            PAIR, amounts_quote=[Decimal(1000)], prices=[Decimal(100)], keep_position=True
        )
        ex = _ex(cfg, adapter, inv)
        await ex.on_create()
        adapter.fill_order(ex.rungs[0].order_id, price=Decimal(100))
        await ex.on_tick()
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated and ex.close_type is CloseType.EARLY_STOP
        assert inv.get(1, PAIR, "c").net_amount_base == Decimal(10)

    asyncio.run(body())
