"""TWAPExecutor tests: slice math, TAKER execution, MAKER missed-slice drop,
VWAP + net_pnl, adversarial."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine.executors.twap_executor import TWAPExecutor, TWAPExecutorConfig
from src.nadobro.engine.types import CloseType, TradeType
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def _ex(cfg, adapter):
    return TWAPExecutor(cfg, user_id=1, controller_id="c", adapter=adapter)


def test_slice_math():
    cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(1000), total_duration=100, order_interval=20)
    assert cfg.n_orders == 5
    assert cfg.amount_per_order_quote == Decimal(200)


def test_taker_executes_all_slices_and_completes():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(1000), 100, 20, mode="TAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.current_index == 0 and ex.filled_base > 0
        ex.start_ts -= 100  # make every slice due
        await ex.on_tick()
        assert ex.current_index == 4
        assert ex.is_terminated and ex.close_type is CloseType.COMPLETED
        assert len(adapter.placed) == 5
        assert ex.filled_base == Decimal(10)  # 5 * (200/100)

    asyncio.run(body())


def test_taker_metrics_vwap():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(1000), 100, 20, mode="TAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()
        ex.start_ts -= 100
        await ex.on_tick()
        m = ex.metrics()
        assert m["average_executed_price"] == Decimal(100)
        assert m["filled_amount_base"] == Decimal(10)
        assert m["lost_slices"] == 0

    asyncio.run(body())


def test_maker_missed_slice_dropped_not_retried():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(1000), 100, 20, mode="MAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.current_index == 0 and ex.filled_base == 0
        slice0_id = ex.current_order.id
        ex.start_ts -= 20  # slice 1 becomes due; slice 0 still unfilled
        await ex.on_tick()
        assert ex.current_index == 1
        assert ex.lost_slices == 1
        assert slice0_id in adapter.cancelled
        adapter.fill_order(ex.current_order.id, price=Decimal(100))
        await ex.on_tick()
        assert ex.filled_base > 0

    asyncio.run(body())


def test_maker_vwap_and_net_pnl():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(200), 40, 20, mode="MAKER")  # n=2
        ex = _ex(cfg, adapter)
        await ex.on_create()
        adapter.fill_order(ex.current_order.id, price=Decimal(100))  # slice 0 fills (1 base)
        ex.start_ts -= 20
        await ex.on_tick()  # slice 0 filled -> place slice 1
        adapter.fill_order(ex.current_order.id, price=Decimal(100))  # slice 1 fills (1 base)
        ex.start_ts -= 40
        adapter.set_mid(Decimal(110))  # mark moves up before final tick
        await ex.on_tick()
        m = ex.metrics()
        assert m["average_executed_price"] == Decimal(100)
        assert m["filled_amount_base"] == Decimal(2)
        assert m["net_pnl_quote"] == Decimal(20)  # (110-100)*2

    asyncio.run(body())


def test_adversarial_transient_error_on_first_slice():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), fail_on=["place_order"], fail_times=2)
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(1000), 100, 20, mode="TAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.retries == 2
        assert ex.filled_base > 0

    asyncio.run(body())


def test_config_validation():
    with pytest.raises(ValueError):
        TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(100), 100, 0)  # interval <= 0
    with pytest.raises(ValueError):
        TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(100), 10, 20)  # duration < interval


def test_maker_finalizes_with_lost_last_slice_after_duration():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(200), 40, 20, mode="MAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()           # slice 0 posted, unfilled
        ex.start_ts -= 20
        await ex.on_tick()             # slice 0 lost, slice 1 posted (unfilled)
        ex.start_ts -= 40              # past total_duration
        await ex.on_tick()             # last slice unfilled + duration elapsed -> finalize
        assert ex.is_terminated and ex.close_type is CloseType.COMPLETED
        assert ex.lost_slices == 2
        assert ex.filled_base == Decimal(0)

    asyncio.run(body())


def test_on_stop_cancels_outstanding_maker_order():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(200), 40, 20, mode="MAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()
        oid = ex.current_order.id
        await ex.on_stop(CloseType.EARLY_STOP)
        assert ex.is_terminated and ex.close_type is CloseType.EARLY_STOP
        assert oid in adapter.cancelled

    asyncio.run(body())


def test_maker_offset_rests_buy_below_and_sell_above_mid():
    """Volume-bot pricing: with maker_offset_bp set, a BUY slice rests BELOW
    mid and a SELL slice ABOVE it (buy low / sell high, post-only can't cross);
    the slice size is derived from the offset price, not raw mid."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(
            PAIR, TradeType.BUY, Decimal(200), 40, 20, mode="MAKER", maker_offset_bp=10.0
        )
        ex = _ex(cfg, adapter)
        await ex.on_create()
        buy_px = ex.current_order.price
        assert buy_px == Decimal("100") * Decimal("0.999")  # 10 bp below mid
        assert ex.current_order.amount_base == Decimal(100) / buy_px

        adapter2 = MockNadoAdapter(mid=Decimal(100))
        cfg2 = TWAPExecutorConfig(
            PAIR, TradeType.SELL, Decimal(200), 40, 20, mode="MAKER", maker_offset_bp=10.0
        )
        ex2 = _ex(cfg2, adapter2)
        await ex2.on_create()
        assert ex2.current_order.price == Decimal("100") * Decimal("1.001")  # 10 bp above

    asyncio.run(body())


def test_maker_offset_zero_keeps_legacy_at_mid_pricing():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        cfg = TWAPExecutorConfig(PAIR, TradeType.BUY, Decimal(200), 40, 20, mode="MAKER")
        ex = _ex(cfg, adapter)
        await ex.on_create()
        assert ex.current_order.price == Decimal(100)

    asyncio.run(body())
