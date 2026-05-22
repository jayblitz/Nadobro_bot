import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.grid_trading import GridController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator

CFG = {"trading_pair": "P", "start_price": "98", "end_price": "102", "limit_price": "95",
       "total_amount_quote": "100", "min_spread_between_orders": "0.01", "max_open_orders": 4}


def test_grid_controller_lifecycle():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = GridController(user_id=1, orchestrator=orch, adapter=adapter,
                           inventory=InventoryRepository(), configs=dict(CFG))
        await orch.spawn_controller(c)
        assert len(orch.list(c.id)) == 1
        await orch.tick_controller(c.id)
        assert orch.get_controller_status(c.id)["state"] == "ACTIVE"
        await orch.stop_controller(c.id)
        assert orch.get_controller_status(c.id)["state"] == "STOPPED"
        assert orch.list(c.id, active_only=True) == []

    asyncio.run(body())
