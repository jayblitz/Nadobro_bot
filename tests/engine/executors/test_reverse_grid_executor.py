"""ReverseGridExecutor (short grid) mirror tests."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine.executors.grid_executor import GridExecutorConfig, GridLevelState
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import CloseType, TradeType
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def _cfg(**kw):
    base = dict(
        trading_pair=PAIR, side=TradeType.SELL, start_price=Decimal(100), end_price=Decimal(110),
        limit_price=Decimal(115), total_amount_quote=Decimal(1000),
        min_spread_between_orders=Decimal("0.02"),
    )
    base.update(kw)
    return GridExecutorConfig(**base)


def _ex(cfg, adapter, inv=None):
    return ReverseGridExecutor(cfg, user_id=1, controller_id="c", adapter=adapter, inventory=inv)


def test_requires_sell_side():
    with pytest.raises(ValueError):
        ReverseGridExecutor(
            _cfg(side=TradeType.BUY), user_id=1, controller_id="c", adapter=MockNadoAdapter()
        )


def test_short_grid_close_price_is_below_open():
    ex = _ex(_cfg(), MockNadoAdapter(mid=Decimal(105)))
    lvl = ex.levels[0]
    # short grid: take-profit (BUY) one step *below* the sell price
    assert lvl.close_price == lvl.open_price * Decimal("0.98")


def test_short_grid_sell_fill_places_buy_close():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        inv = InventoryRepository()
        ex = _ex(_cfg(), adapter, inv)
        await ex.on_create()
        lvl = ex.levels[0]
        adapter.fill_order(lvl.open_order_id, price=lvl.open_price)
        await ex.on_tick()
        assert lvl.state is GridLevelState.CLOSE_ORDER_PLACED
        assert inv.get(1, PAIR, "c").sell_amount_base > 0

    asyncio.run(body())


def test_short_grid_limit_price_above_triggers_stop():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(105))
        ex = _ex(_cfg(limit_price=Decimal(108)), adapter)
        await ex.on_create()
        adapter.set_mid(Decimal(109))  # above hard stop for a short
        await ex.on_tick()
        assert ex.is_terminated and ex.close_type is CloseType.STOP_LOSS

    asyncio.run(body())
