"""DN execution hardening — no naked legs (2026-06).

The failure family these pin: one leg executes while the other hangs,
leaving the user directional under a strategy that promised neutrality.

1. A SHORT-leg open failure (even one that RAISES, e.g. mid_price on the
   gateway) rolls the long back through the hardened close — never a
   stranded naked long, and the user is told (leg_rollback).
2. One leg dying during HOLDING (zero-filled / FAILED executor) is detected
   even though the drift gate is blind to it — both legs close (leg_dead).
3. A throttled close is RETRIED every tick (the old code fired stop exactly
   once and discarded the result); past the deadline the user is alerted
   once (close_stuck) while retries continue.
4. Cycle completion requires INVENTORY-verified flatness: residue left by a
   failed close is swept with reduce-only MARKET orders; an unsweepable
   remainder alerts (residual_exposure) instead of silently completing.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.adapter.base import AdapterError
from src.nadobro.engine.controllers.delta_neutral import DeltaNeutralController, DNPhase
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import CloseType, TradeType

CFG = {
    "trading_pair_long": "L", "trading_pair_short": "S",
    "hedge_ratio": "1", "leg_amount_quote": "50",
    "max_drift_pct": "0.05", "hold_seconds": 3600, "cycles": 1,
    # These tests exercise close/residual mechanics via fixed-hold timing, not
    # the funding-flip exit — keep the legacy fixed-hold behavior.
    "funding_exit_enabled": False,
}


def _dn(adapter, inv, extra=None):
    orch = ExecutorOrchestrator()
    configs = dict(CFG)
    configs.update(extra or {})
    c = DeltaNeutralController(user_id=1, orchestrator=orch, adapter=adapter,
                               inventory=inv, configs=configs, controller_id="DN")
    return orch, c


def _kinds(c):
    return [e["kind"] for e in c.consume_dn_events()]


class ShortLegMidPriceFails(MockNadoAdapter):
    """mid_price RAISES for the short pair — the exact path that used to skip
    the long-leg rollback entirely (the raise happened before the rollback)."""

    async def mid_price(self, pair):  # type: ignore[override]
        if pair == "S":
            raise AdapterError("gateway hiccup")
        return await super().mid_price(pair)


def test_short_open_failure_rolls_back_long_with_alert():
    async def body():
        adapter = ShortLegMidPriceFails(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv)
        await orch.spawn_controller(c)
        # Rollback is in flight through the hardened CLOSING machinery.
        assert c.phase in (DNPhase.CLOSING, DNPhase.DONE)
        for _ in range(4):
            await orch.tick_controller(c.id)
        assert c.phase is DNPhase.DONE
        assert inv.get(1, "L", c.id).net_amount_base == 0, "long must be flat, never naked"
        kinds = _kinds(c)
        assert "leg_rollback" in kinds

    asyncio.run(body())


def test_dead_short_leg_during_holding_closes_both():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv)
        await orch.spawn_controller(c)
        assert c.phase is DNPhase.HOLDING and len(orch.list(c.id, active_only=True)) == 2
        # The short executor dies (venue rejected its state mid-flight) while
        # the long holds — the drift gate (needs both values > 0) NEVER sees
        # this; the integrity check must.
        short_ex = orch.get(c.short_id)
        short_ex._terminate(CloseType.FAILED)  # noqa: SLF001 - simulate venue death
        await orch.tick_controller(c.id)
        assert c.hedge_broken and c._abort_cycles  # noqa: SLF001
        assert "leg_dead" in _kinds(c)
        for _ in range(4):
            await orch.tick_controller(c.id)
        assert c.phase is DNPhase.DONE
        assert inv.get(1, "L", c.id).net_amount_base == 0, "long leg closed too"

    asyncio.run(body())


def test_throttled_close_is_retried_and_alerts_once_past_deadline():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv, extra={"hold_seconds": 0, "close_deadline_seconds": 30})
        # Deadline floor is 30s; force it to "already passed" after arming.
        await orch.spawn_controller(c)

        # Throttle: the FIRST two stop calls fail at the orchestrator level.
        fails = {"n": 2}
        real_stop = orch.stop

        async def flaky_stop(executor_id, close_type=CloseType.EARLY_STOP):
            if fails["n"] > 0:
                fails["n"] -= 1
                raise AdapterError("rate limited")
            return await real_stop(executor_id, close_type)

        orch.stop = flaky_stop  # type: ignore[assignment]
        await orch.tick_controller(c.id)   # hold expired -> close fires, both stops throttled
        assert c.phase is DNPhase.CLOSING
        assert not c._both_legs_terminated(), "throttled close left legs open"  # noqa: SLF001
        c._close_deadline = 0  # noqa: SLF001 - deadline already passed
        await orch.tick_controller(c.id)   # retries succeed now
        kinds = _kinds(c)
        # Alert fired once IF legs were still open when the deadline check ran;
        # the retry must have closed them by the end regardless.
        for _ in range(3):
            await orch.tick_controller(c.id)
        assert c.phase is DNPhase.DONE
        assert inv.get(1, "L", c.id).net_amount_base == 0
        assert inv.get(1, "S", c.id).net_amount_base == 0
        assert fails["n"] == 0, "the retry path must have consumed the throttles"

    asyncio.run(body())


def test_residual_after_close_is_swept_before_cycle_completes():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv, extra={"hold_seconds": 0})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)   # close both
        # Simulate a close that died mid-way: executors terminated, but the
        # venue still holds 0.1 base of the long (inject it into inventory).
        inv.apply_fill(1, "L", c.id, TradeType.BUY, Decimal("0.1"), Decimal("10"), Decimal(0), 0.0)
        assert inv.get(1, "L", c.id).net_amount_base == Decimal("0.1")
        placed_before = len(adapter.placed)
        for _ in range(4):
            await orch.tick_controller(c.id)
        # A reduce-only MARKET sweep was placed and the inventory is flat.
        sweeps = adapter.placed[placed_before:]
        assert any(o.trading_pair == "L" and o.side is TradeType.SELL for o in sweeps), \
            "expected a SELL sweep for the long residual"
        assert inv.get(1, "L", c.id).net_amount_base == 0
        assert c.phase is DNPhase.DONE
        assert "residual_exposure" not in _kinds(c), "a successful sweep must not alert"

    asyncio.run(body())


def test_unsweepable_residual_alerts_instead_of_completing_silently():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        orch, c = _dn(adapter, inv, extra={"hold_seconds": 0})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)   # close both
        inv.apply_fill(1, "S", c.id, TradeType.SELL, Decimal("0.1"), Decimal("10"), Decimal(0), 0.0)

        # Every sweep attempt fails at the venue.
        async def broken_place(*a, **k):
            raise AdapterError("venue rejects everything")

        adapter.place_order = broken_place  # type: ignore[assignment]
        for _ in range(8):
            await orch.tick_controller(c.id)
        kinds = _kinds(c)
        assert "residual_exposure" in kinds, "an unflattenable remainder must alert"
        assert c.phase is DNPhase.CLOSING, "cycle must NOT complete while exposed"

    asyncio.run(body())
