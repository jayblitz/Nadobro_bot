import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.delta_neutral import DeltaNeutralController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator


def _dn(adapter, inv, configs):
    orch = ExecutorOrchestrator()
    c = DeltaNeutralController(user_id=1, orchestrator=orch, adapter=adapter,
                               inventory=inv, configs=configs, controller_id="DN")
    return orch, c


def test_two_legs_balanced_no_drift():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05"})
        await orch.spawn_controller(c)
        assert len(orch.list(c.id)) == 2
        await orch.tick_controller(c.id)
        assert not c.hedge_broken

    asyncio.run(body())


def test_hedge_drift_closes_both():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "2", "leg_amount_quote": "50", "max_drift_pct": "0.05"})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.hedge_broken
        assert orch.list(c.id, active_only=True) == []

    asyncio.run(body())
