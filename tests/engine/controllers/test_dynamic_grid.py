import asyncio
import math
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
from src.nadobro.engine.executors.grid_executor import GridExecutor
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator

CFG = {"trading_pair": "P", "start_price": "98", "end_price": "102", "limit_price": "0",
       "total_amount_quote": "100", "min_spread_between_orders": "0.002", "max_open_orders": 4,
       # step/levels make _rebuild_bounds_for_side produce side-correct bounds
       # around the live mid (as the real engine config does), so a flipped
       # SELL grid places sells ABOVE mid rather than reusing long-grid bounds.
       "step_pct": "0.002", "levels_count": 3}


def _candles(closes):
    return [{"high": c + 1, "low": c - 1, "close": c} for c in closes]


def _down(n=60):
    return _candles([float(i) for i in range(n, 1, -1)])


def _range(n=60, base=100.0, amp=1.0, period=7.0):
    return _candles([base + amp * math.sin(2 * math.pi * i / period) for i in range(n)])


def test_trending_down_selects_reverse_grid():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        cfg = dict(CFG, candle_provider=lambda p: _down())
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.current_phase == "rgrid"
        assert c.variance_ratio >= 1.25
        active = orch.list(c.id, active_only=True)
        assert active and isinstance(active[0], ReverseGridExecutor)

    asyncio.run(body())


def test_ranging_selects_long_grid():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        cfg = dict(CFG, candle_provider=lambda p: _range())
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.current_phase == "grid"
        active = orch.list(c.id, active_only=True)
        assert active and isinstance(active[0], GridExecutor)

    asyncio.run(body())


def test_same_regime_does_not_flip():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        cfg = dict(CFG, candle_provider=lambda p: _down())
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        first = orch.list(c.id, active_only=True)[0]
        await orch.tick_controller(c.id)  # still a downtrend -> no flip
        active = orch.list(c.id, active_only=True)
        assert len(active) == 1 and active[0] is first

    asyncio.run(body())


def test_midflight_flip_grid_to_rgrid_on_confirmed_regime_change():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        box = {"data": _range()}
        cfg = dict(CFG, candle_provider=lambda p: box["data"], dgrid_flip_confirm_ticks=2)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        active = orch.list(c.id, active_only=True)
        assert isinstance(active[0], GridExecutor) and c.current_phase == "grid"

        # Regime turns down. Flip is debounced by confirm_ticks=2.
        box["data"] = _down()
        await orch.tick_controller(c.id)  # streak 1 -> hold
        active = orch.list(c.id, active_only=True)
        assert isinstance(active[0], GridExecutor), "must not flip on a single tick"
        await orch.tick_controller(c.id)  # streak 2 -> flip
        active = orch.list(c.id, active_only=True)
        assert len(active) == 1 and isinstance(active[0], ReverseGridExecutor)
        assert c.current_phase == "rgrid"
        # Flip event surfaced exactly once.
        event = c.consume_dgrid_event()
        assert event and event["from"] == "grid" and event["to"] == "rgrid"
        assert c.consume_dgrid_event() is None

    asyncio.run(body())


def test_metrics_exposed_for_dashboard():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        # 25bp requested, but it must be floored above the grid band (step 20bp
        # x (3-1) = 40bp band -> 120bp floor) so reset can't fire inside the band.
        cfg = dict(CFG, candle_provider=lambda p: _down(), dgrid_reset_threshold_bp=25.0)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        m = c.dgrid_metrics()
        assert m["dgrid_phase"] == "rgrid"
        assert m["dgrid_variance_ratio"] >= 1.25
        assert m["dgrid_reset_threshold_bp"] >= 50.0  # floored, not the raw 25


def test_reset_off_by_default_no_churn_on_moving_mid():
    # Finding 1 regression: with reset OFF (default), a moving mid must NOT
    # flatten + respawn the grid every tick. The same executor must persist.
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        cfg = dict(CFG, candle_provider=lambda p: _range())  # ranging -> grid, no flip
        assert "dgrid_reset_threshold_bp" not in cfg  # default OFF
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        first = orch.list(c.id, active_only=True)[0]
        # Walk the mid well beyond any 0.2% band, several ticks.
        for px in (101, 99, 103, 97, 104):
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
        active = orch.list(c.id, active_only=True)
        assert len(active) == 1 and active[0] is first, "reset must be OFF by default (no churn)"
        assert c.reset_threshold_bp == 0.0

    asyncio.run(body())


def test_reset_recenters_in_place_on_large_move():
    # When enabled, reset re-quotes the SAME executor's ladder in place (no
    # flatten, no new executor) only after a move past the floored threshold.
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        cfg = dict(CFG, candle_provider=lambda p: _range(), dgrid_reset_threshold_bp=200.0)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        first = orch.list(c.id, active_only=True)[0]
        opens0 = [lv.open_price for lv in first.levels]
        # Small move (50bp) — below threshold, no re-center.
        adapter.set_mid(Decimal("100.5"))
        await orch.tick_controller(c.id)
        active = orch.list(c.id, active_only=True)
        assert active[0] is first and [lv.open_price for lv in first.levels] == opens0, \
            "small move must not re-center"
        # Large move (3%) — past the floored 200bp threshold: re-center in place.
        adapter.set_mid(Decimal("103"))
        await orch.tick_controller(c.id)
        active = orch.list(c.id, active_only=True)
        assert len(active) == 1 and active[0] is first, "re-center must reuse the same executor"
        assert max(lv.open_price for lv in first.levels) > Decimal("102"), "ladder must move up"
        assert c.current_phase == "grid"  # same regime, not a flip
        assert c.consume_dgrid_event() is None  # re-center is not a flip notification

    asyncio.run(body())


def test_flip_deferred_while_gate_paused():
    # Finding 2 regression: a confirmed flip during a breakout/expansion PAUSE
    # must close the old position but NOT arm a fresh grid into the chaos.
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        box = {"data": _range()}  # gate disabled in cfg; we drive the verdict
        cfg = dict(CFG, candle_provider=lambda p: box["data"], dgrid_flip_confirm_ticks=1)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert isinstance(orch.list(c.id, active_only=True)[0], GridExecutor)

        # Regime turns down AND the gate is paused (breakout). Gate is disabled
        # in cfg, so set the verdict directly; on_tick won't overwrite it.
        box["data"] = _down()
        c.gate_verdict, c.gate_reason = "PAUSE", "breakout"
        await orch.tick_controller(c.id)
        assert orch.list(c.id, active_only=True) == [], "must close + sit out, not arm into breakout"
        assert c.consume_dgrid_event() is None, "no flip notification when nothing armed"

        # Range/acceptance returns: now it arms the short side.
        c.gate_verdict, c.gate_reason = "QUOTE", ""
        await orch.tick_controller(c.id)
        active = orch.list(c.id, active_only=True)
        assert len(active) == 1 and isinstance(active[0], ReverseGridExecutor)

    asyncio.run(body())


def test_tick_records_diagnostics_for_services_log():
    # The per-tick diagnostics (candle count + mid) must be captured so the
    # services-stream engine_diag line can pinpoint a no-orders run.
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        cfg = dict(CFG, candle_provider=lambda p: _down())
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=InventoryRepository(), configs=cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c._last_candle_count == len(_down())
        assert c._last_mid == Decimal(100)
        assert c.current_phase in ("grid", "rgrid")

    asyncio.run(body())


def test_profit_booking_scales_out_on_tier_cross():
    """Tiered profit-booking: as the run's uPnL crosses rising tiers (% of
    margin), a fraction of the position is closed reduce-only; each tier books
    once."""
    from src.nadobro.engine.types import OrderType, TradeType

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(102))
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        cfg = dict(CFG, candle_provider=lambda p: _range(), margin_quote="100",
                   dgrid_tp_tiers_pct=[2.0, 4.0, 6.0], dgrid_tp_fraction=0.33)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=inv, configs=cfg)
        # Seed a 1.0-base long @ 100 (margin 100): uPnL = net*(mark-entry).
        inv.apply_fill(1, "P", c.id, TradeType.BUY, Decimal("1.0"), Decimal("100"))

        # mid 102 -> uPnL +2 = +2% of margin -> only tier 1 (2%) books.
        await c._maybe_book_profit(Decimal("102"))
        sells = [o for o in adapter.placed if o.side == TradeType.SELL]
        assert len(sells) == 1
        assert sells[0].order_type == OrderType.MARKET
        assert abs(float(sells[0].amount_base) - 0.33) < 1e-9   # 33% of 1.0
        assert c._booked_tiers == {0}

        # Same tier again -> nothing new (booked once).
        await c._maybe_book_profit(Decimal("102"))
        assert len([o for o in adapter.placed if o.side == TradeType.SELL]) == 1

        # Jump to +6 (mid 106) -> tiers 4% and 6% both book this tick.
        await c._maybe_book_profit(Decimal("106"))
        sells = [o for o in adapter.placed if o.side == TradeType.SELL]
        assert len(sells) == 2
        assert c._booked_tiers == {0, 1, 2}

    asyncio.run(body())


def test_profit_booking_skips_when_below_tier():
    from src.nadobro.engine.types import TradeType

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(101))
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        cfg = dict(CFG, candle_provider=lambda p: _range(), margin_quote="100",
                   dgrid_tp_tiers_pct=[2.0, 4.0, 6.0], dgrid_tp_fraction=0.33)
        c = DynamicGridController(user_id=1, orchestrator=orch, adapter=adapter,
                                  inventory=inv, configs=cfg)
        inv.apply_fill(1, "P", c.id, TradeType.BUY, Decimal("1.0"), Decimal("100"))
        # mid 101 -> uPnL +1 = +1% < first tier (2%): no booking.
        await c._maybe_book_profit(Decimal("101"))
        assert [o for o in adapter.placed if o.side == TradeType.SELL] == []
        assert c._booked_tiers == set()

    asyncio.run(body())
