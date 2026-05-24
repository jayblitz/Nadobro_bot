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


def test_hedge_ratio_2_balanced_when_short_is_2x_long():
    """BUG-DN-1: hedge_ratio is short_notional/long_notional. With ratio=2
    on_start spawns short=2*long, and on_tick should consider that balanced.
    Previously this self-destructed on the first tick.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "2", "leg_amount_quote": "50", "max_drift_pct": "0.05"})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert not c.hedge_broken
        assert len(orch.list(c.id, active_only=True)) == 2

    asyncio.run(body())


def test_actual_drift_breaks_hedge():
    """When actual short-to-long ratio diverges past max_drift_pct, the
    controller stops both legs."""
    from src.nadobro.engine.types import TradeType

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv,
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05"})
        await orch.spawn_controller(c)
        # Inject a synthetic 50% drift on the long leg's inventory. We have
        # to use apply_fill (the public mutator) instead of mutating the
        # PositionHold returned by inv.get(): post AUDIT-FIX-INV-1, get()
        # returns a snapshot copy so external mutations can't corrupt the
        # repository's live state.
        existing = inv.get(c.user_id, "L", c.id)
        bump_base = existing.buy_amount_base * Decimal("0.5")
        bump_quote = existing.buy_amount_quote * Decimal("0.5")
        inv.apply_fill(
            c.user_id, "L", c.id, TradeType.BUY,
            bump_base, bump_quote, Decimal(0),
        )
        await orch.tick_controller(c.id)
        assert c.hedge_broken
        assert orch.list(c.id, active_only=True) == []

    asyncio.run(body())
