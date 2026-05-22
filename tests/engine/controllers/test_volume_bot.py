import asyncio
from decimal import Decimal

import pytest
from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator


def _vb_kwargs(configs):
    return dict(user_id=1, orchestrator=ExecutorOrchestrator(),
                adapter=MockNadoAdapter(mid=Decimal(100)),
                inventory=InventoryRepository(), configs=configs, controller_id="VB")


def test_rejects_perp_leverage_and_unsupported_pair():
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC-USDC", "market": "perp"}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC-USDC", "leverage": 5}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "BTC-PERP"}))


def test_buy_then_sell_cleanup():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        assert c.phase == "buying"
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.current_order.id, price=Decimal(100))
        buy.start_ts -= 20
        await orch.tick_controller(c.id)        # ingest slice0, post slice1
        adapter.fill_order(buy.current_order.id, price=Decimal(100))
        buy.start_ts -= 60
        await orch.tick_controller(c.id)        # finalize buy -> spawn sell
        assert buy.is_terminated
        assert c.phase == "selling" and c.sell_id is not None

    asyncio.run(body())


def test_buy_with_no_fills_completes_to_done():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "WETH-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        buy.start_ts -= 200  # all slices due + past duration, nothing filled
        await orch.tick_controller(c.id)
        assert buy.is_terminated
        assert c.phase == "done" and c.sell_id is None

    asyncio.run(body())
