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
                       "max_drift_pct": "0.05", "funding_exit_enabled": False, "hold_seconds": 0, "cycles": 1})
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
                       "max_drift_pct": "0.05", "funding_exit_enabled": False, "hold_seconds": 0,
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
                       "max_drift_pct": "0.05", "funding_exit_enabled": False, "hold_seconds": 0, "cycles": 1})
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
                       "max_drift_pct": "0.05", "funding_exit_enabled": False, "hold_seconds": 0, "cycles": 3,
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


def test_completed_and_stop_reason_lifecycle():
    """``completed`` is False while the run is live (so cycles auto-restart) and
    True only at DONE, so the runtime finalizes the session instead of idling
    'running' forever. stop_reason reflects how it ended."""
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50",
                       "max_drift_pct": "0.05", "funding_exit_enabled": False,
                       "hold_seconds": 0, "cycles": 2, "cycle_gap_seconds": 0})
        await orch.spawn_controller(c)
        assert c.completed is False                  # HOLDING cycle 1
        for _ in range(6):
            await orch.tick_controller(c.id)
        assert c.phase is DNPhase.DONE
        assert c.completed is True
        assert c.stop_reason == "cycles_complete"

    asyncio.run(body())


def test_drift_break_reports_hedge_broken_stop_reason():
    from src.nadobro.engine.types import TradeType

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv,
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05"})
        await orch.spawn_controller(c)
        existing = inv.get(c.user_id, "L", c.id)
        inv.apply_fill(c.user_id, "L", c.id, TradeType.BUY,
                       existing.buy_amount_base * Decimal("0.5"),
                       existing.buy_amount_quote * Decimal("0.5"), Decimal(0))
        from src.nadobro.engine.controllers.delta_neutral import DNPhase
        for _ in range(8):                           # drift → close → residual sweep → DONE
            await orch.tick_controller(c.id)
            if c.phase is DNPhase.DONE:
                break
        assert c.hedge_broken is True
        assert c.completed is True
        assert c.stop_reason == "hedge_broken"

    asyncio.run(body())


def test_funding_flip_closes_early_and_aborts_cycles():
    """A confirmed funding flip closes BOTH legs EARLY — before the planned hold
    (TIME_LIMIT exit) — and aborts further cycles so we don't churn back into
    unfavorable funding (cycles=2 stops at 1). hold_seconds is large so the
    PLANNED close can't fire; only the funding flip can."""
    from src.nadobro.engine.types import CloseType
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_rate_value = Decimal("-0.0002")   # unfavorable for the short
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05",
                       "hold_seconds": 100000, "funding_exit_enabled": True,
                       "funding_poll_seconds": 0, "funding_flip_confirmations": 2,
                       "cycles": 2, "cycle_gap_seconds": 0})
        await orch.spawn_controller(c)
        assert len(orch.list(c.id, active_only=True)) == 2
        await orch.tick_controller(c.id)                  # poll 1 → unfavorable (count 1)
        assert len(orch.list(c.id, active_only=True)) == 2
        assert c.funding_unfavorable_count == 1
        await orch.tick_controller(c.id)                  # poll 2 → confirmed flip → close
        assert orch.list(c.id, active_only=True) == []
        assert c.last_close_type is CloseType.TIME_LIMIT
        assert c._abort_cycles is True
        assert "funding_flip" in [e["kind"] for e in c.consume_dn_events()]
        await orch.tick_controller(c.id)                  # cycle completes → DONE, no re-loop
        assert c.cycles_completed == 1
        assert c.phase is DNPhase.DONE
        assert c.completed is True
        assert c.stop_reason == "stopped_early"

    asyncio.run(body())


def test_favorable_funding_does_not_exit_early():
    """While funding stays favorable (short earns), the funding-flip early exit
    does NOT fire — the hedge keeps holding until its PLANNED close. (hold is
    large here so we observe holding before the planned close.)"""
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_rate_value = Decimal("0.0003")    # favorable: short earns
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05",
                       "hold_seconds": 100000, "funding_exit_enabled": True,
                       "funding_poll_seconds": 0, "funding_flip_confirmations": 2,
                       "max_hold_seconds": 0})
        await orch.spawn_controller(c)
        for _ in range(5):
            await orch.tick_controller(c.id)
        assert len(orch.list(c.id, active_only=True)) == 2
        assert c.phase is DNPhase.HOLDING
        assert c.funding_unfavorable_count == 0
        assert c.last_funding_rate == Decimal("0.0003")

    asyncio.run(body())


def test_funding_flip_debounce_resets_on_favorable():
    """A single unfavorable read does not close; the debounce resets when
    funding returns favorable, so only consecutive flips trigger the exit."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_rate_value = Decimal("-0.0001")
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05",
                       "hold_seconds": 100000, "funding_exit_enabled": True,
                       "funding_poll_seconds": 0, "funding_flip_confirmations": 2,
                       "max_hold_seconds": 0})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)                  # unfavorable → count 1
        assert c.funding_unfavorable_count == 1
        adapter.funding_rate_value = Decimal("0.0005")    # favorable again
        await orch.tick_controller(c.id)                  # resets debounce
        assert c.funding_unfavorable_count == 0
        assert len(orch.list(c.id, active_only=True)) == 2
        adapter.funding_rate_value = Decimal("-0.0001")
        await orch.tick_controller(c.id)                  # count 1
        await orch.tick_controller(c.id)                  # count 2 → close
        assert orch.list(c.id, active_only=True) == []

    asyncio.run(body())


def test_funding_flip_exits_early_before_hold():
    """A confirmed unfavorable flip closes the hedge EARLY, before the planned
    hold elapses (the safety early-exit). With confirmations=1 a single
    unfavorable poll triggers it."""
    from src.nadobro.engine.types import CloseType

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_rate_value = Decimal("-0.5")      # very unfavorable
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05",
                       "hold_seconds": 3600, "funding_exit_enabled": True,
                       "funding_poll_seconds": 0, "funding_flip_confirmations": 1})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)                  # elapsed << hold, but flip confirmed → close early
        assert orch.list(c.id, active_only=True) == []
        assert c.last_close_type is CloseType.TIME_LIMIT
        assert c._abort_cycles is True
        assert "funding_flip" in [e["kind"] for e in c.consume_dn_events()]

    asyncio.run(body())


def test_hold_is_hard_close_even_when_funding_favorable():
    """The user's hold is the PLANNED close time: once it elapses the hedge
    closes even with favorable funding. (Regression for the reported bug where a
    6h hold ran on toward the hidden 24h cap because favorable funding extended
    it.)"""
    import time as _time
    from src.nadobro.engine.types import CloseType
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_rate_value = Decimal("0.0009")    # favorable — would have held under the old logic
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05",
                       "hold_seconds": 3600, "funding_exit_enabled": True,
                       "funding_poll_seconds": 0, "funding_flip_confirmations": 2,
                       "max_hold_seconds": 0, "cycles": 1})
        await orch.spawn_controller(c)
        c.opened_at = _time.time() - 3601                 # the 3600s hold just elapsed
        await orch.tick_controller(c.id)                  # planned close fires despite favorable funding
        assert orch.list(c.id, active_only=True) == []
        assert c.last_close_type is CloseType.TIME_LIMIT
        assert c._abort_cycles is False                   # planned close, not an abort
        await orch.tick_controller(c.id)
        assert c.cycles_completed == 1
        assert c.phase is DNPhase.DONE

    asyncio.run(body())


def test_max_hold_caps_total_hold():
    """A favorable funding rate would hold indefinitely, but max_hold_seconds is
    a hard safety cap — a planned TIME_LIMIT close that does NOT abort cycles."""
    import time as _time
    from src.nadobro.engine.types import CloseType
    from src.nadobro.engine.controllers.delta_neutral import DNPhase

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        adapter.funding_rate_value = Decimal("0.0009")    # favorable — would otherwise hold
        orch, c = _dn(adapter, InventoryRepository(),
                      {"trading_pair_long": "L", "trading_pair_short": "S",
                       "hedge_ratio": "1", "leg_amount_quote": "50", "max_drift_pct": "0.05",
                       "hold_seconds": 0, "funding_exit_enabled": True,
                       "funding_poll_seconds": 0, "funding_flip_confirmations": 2,
                       "max_hold_seconds": 5})
        await orch.spawn_controller(c)
        c.opened_at = _time.time() - 10                   # simulate 10s elapsed (> 5s cap)
        await orch.tick_controller(c.id)                  # max-hold cap → planned close
        assert orch.list(c.id, active_only=True) == []
        assert c.last_close_type is CloseType.TIME_LIMIT
        assert c._abort_cycles is False
        await orch.tick_controller(c.id)
        assert c.cycles_completed == 1
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
