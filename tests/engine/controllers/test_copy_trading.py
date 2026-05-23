import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.copy_trading import CopyController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator


def test_signal_spawns_position_and_slippage_gate_and_dedupe():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        signals = [
            {"id": "s1", "pair": "P", "side": "BUY", "amount_quote": "10",
             "price": "100", "take_profit": "0.05", "stop_loss": "0.03"},
            {"id": "s2", "pair": "P", "side": "BUY", "amount_quote": "10", "price": "50"},
        ]
        c = CopyController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"max_slippage_pct": "0.01", "signal_source": lambda: signals},
            controller_id="CP",
        )
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert len(orch.list(c.id)) == 1      # s1 entered; s2 slippage-skipped
        assert "s2" in c.skipped
        await orch.tick_controller(c.id)      # dedupe -> no new executors
        assert len(orch.list(c.id)) == 1

    asyncio.run(body())
