import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
from src.nadobro.engine.executors.grid_executor import GridExecutor
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator

CFG = {"trading_pair": "P", "start_price": "98", "end_price": "102", "limit_price": "110",
       "total_amount_quote": "100", "min_spread_between_orders": "0.01", "max_open_orders": 4}


def _candles(closes):
    return [{"high": c + 1, "low": c - 1, "close": c} for c in closes]


def test_trending_down_selects_reverse_grid_no_midflight_swap():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        down = _candles([float(i) for i in range(60, 1, -1)])
        cfg = dict(CFG, candle_provider=lambda p: down)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.last_regime == "TRENDING_DOWN"
        active = orch.list(c.id, active_only=True)
        assert active and isinstance(active[0], ReverseGridExecutor)
        n = len(orch.list(c.id))
        await orch.tick_controller(c.id)  # active exists -> no swap
        assert len(orch.list(c.id)) == n

    asyncio.run(body())


def test_ranging_selects_long_grid():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        flat = _candles([100.0, 100.5, 100.0, 99.5] * 8)
        cfg = dict(CFG, candle_provider=lambda p: flat)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.last_regime == "RANGING"
        active = orch.list(c.id, active_only=True)
        assert active and isinstance(active[0], GridExecutor)

    asyncio.run(body())
