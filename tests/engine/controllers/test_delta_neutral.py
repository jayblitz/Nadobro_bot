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


def test_hold_timer_closes_both_legs_together():
    """With the hold elapsed, the controller fires a synchronized close on both
    legs (same tick) and they terminate together — one side is never left
    exposed. close_type is TIME_LIMIT (planned exit, not a drift break)."""
    from src.nadobro.engine.types import CloseType
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "hold_seconds": 0, "cycles": 1})
        await orch.spawn_controller(c)
        assert len(orch.list(c.id, active_only=True)) == 2
        # Tick 1: hold (0s) already elapsed → close both legs now.
        await orch.tick_controller(c.id)
        assert orch.list(c.id, active_only=True) == []
        assert c.last_close_type is CloseType.TIME_LIMIT
        assert not c.hedge_broken
        # Tick 2: closing legs already terminal → cycle completes, run is DONE.
        await orch.tick_controller(c.id)
        assert c.cycles_completed == 1
        assert c.phase is DNPhase.DONE

    asyncio.run(body())


def test_auto_loops_multiple_cycles():
    """cycles=2 → the controller repeats open→hold→close twice, producing spot
    + perp volume on each cycle, then finishes."""
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "hold_seconds": 0,
                       "cycles": 2, "cycle_gap_seconds": 0})
        await orch.spawn_controller(c)             # cycle 1 opens
        for _ in range(6):                          # drive the state machine
            await orch.tick_controller(c.id)
        assert c.cycles_completed == 2
        assert c.phase is DNPhase.DONE
        # 2 cycles × (long open + short open + long close + short close) = 8.
        assert len(adapter.placed) == 8

    asyncio.run(body())


def test_funding_accumulates_at_cycle_close():
    """The controller polls the adapter funding feed and accumulates net funding
    received over the run, surfaced via funding_quote for the PnL card."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_quote = Decimal("1.25")   # simulate accrued funding
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "hold_seconds": 0, "cycles": 1})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)          # hold elapsed → close
        await orch.tick_controller(c.id)          # cycle completes → settle funding
        assert c.cumulative_funding == Decimal("1.25")
        assert c.funding_quote == Decimal("1.25")

    asyncio.run(body())


def test_position_executor_reduce_position():
    """reduce_position partially closes an open leg (reduce-only MARKET) and
    keeps it ACTIVE — the primitive the DN controller uses to trim a leg."""
    from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
    from src.nadobro.engine.executors.position_executor import (
        PositionExecutor, PositionExecutorConfig, PositionExecState,
    )
    from src.nadobro.engine.types import (
        ExecutionStrategy, OrderType, TradeType, TripleBarrierConfig,
    )

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        oc = OrderExecutorConfig("L", TradeType.BUY, Decimal("1.0"), ExecutionStrategy.MARKET)
        ex = PositionExecutor(
            PositionExecutorConfig(order_config=oc, barriers=TripleBarrierConfig()),
            user_id=1, controller_id="DN", adapter=adapter, inventory=InventoryRepository(),
        )
        await ex.on_create()
        assert ex.entry_base == Decimal("1.0")
        reduced = await ex.reduce_position(Decimal("0.3"))
        assert reduced == Decimal("0.3")
        assert ex.exit_base == Decimal("0.3")
        assert ex.position_state is PositionExecState.ACTIVE_POSITION
        sells = [o for o in adapter.placed
                 if o.side is TradeType.SELL and o.order_type is OrderType.MARKET]
        assert len(sells) == 1

    asyncio.run(body())


def test_short_underfill_trims_long():
    """When the short underfills, _rebalance_after_open trims the over-hedged
    long so the legs match in base terms instead of leaning on the drift gate."""
    from src.nadobro.engine.types import TradeType

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "hold_seconds": 3600})
        await orch.spawn_controller(c)            # both legs fill 0.5 each
        placed_before = len(adapter.placed)
        # Simulate the short having only half-filled, then re-run the rebalance.
        real_entry = c._entry_base
        c._entry_base = lambda eid: (Decimal("0.25") if eid == c.short_id else real_entry(eid))
        await c._rebalance_after_open()
        trims = [o for o in adapter.placed[placed_before:]
                 if o.trading_pair == "L" and o.side is TradeType.SELL]
        assert trims, "expected a trim sell on the over-hedged long leg"

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


def test_restore_cycles_completed_resumes_count_on_rebuild():
    """DN-CYCLES fix: a rebuilt controller restores cycles_completed from
    persisted progress (injected as restore_cycles_completed) so it RESUMES the
    configured cycle count instead of restarting from 0. With 2 of 3 done, it
    opens the final cycle, holds (0s), closes, and finishes at 3 — not 4+."""
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "hold_seconds": 0, "cycles": 3,
                       "restore_cycles_completed": 2, "restore_funding_usd": "1.50"})
        assert c.cycles_completed == 2
        assert c.cumulative_funding == Decimal("1.50")
        await orch.spawn_controller(c)
        # 2 < 3 → the final cycle opens (both legs).
        assert len(orch.list(c.id, active_only=True)) == 2
        await orch.tick_controller(c.id)   # hold(0s) elapsed → close both
        await orch.tick_controller(c.id)   # cycle completes → DONE at 3
        assert c.cycles_completed == 3
        assert c.phase is DNPhase.DONE

    asyncio.run(body())


def test_restore_at_or_above_total_opens_no_new_cycle():
    """DN-CYCLES fix: if the restored count already meets the configured total,
    on_start must NOT open another cycle (the 'ignores the cycle count on
    restart' bug) — it goes straight to DONE with zero legs."""
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "hold_seconds": 3600, "cycles": 3,
                       "restore_cycles_completed": 3})
        await orch.spawn_controller(c)
        assert orch.list(c.id, active_only=True) == []
        assert c.phase is DNPhase.DONE

    asyncio.run(body())
