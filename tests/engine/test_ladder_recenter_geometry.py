"""RGRID-STALE-LADDER — the re-center threshold must be bounded by the ladder.

Reported 2026-07-28: "the bot just places orders and does not quote again until
the market comes and picks the order". Investigated on production (Fly, app
nadobro-bot) against RGRID session 165 (user 5776741680, BTC-PERP, mainnet,
06:28 -> 13:32 UTC, stopped by session SL):

    total_cycles            2080        <- one tick every ~12s for 7 hours
    total_orders_placed       26        <- one order every ~16 minutes
    zero_order_cycles       2063        <- 99.2% of cycles placed nothing
    levels                     3
    spread_bp               10.0        -> ladder band = 10 x (3-1) = 20bp
    dgrid_reset_threshold_bp  80.0      <- 4x the band it is supposed to track
    dgrid_realized_move_bp   17.83      <- never got close to firing

The fills prove the ladder froze: the SAME maker price
(63352.4017854148, to ten decimal places) was re-issued from 09:58 to 13:27 —
three and a half hours — with gaps of 83, 39 and 126 minutes between fills.
BTC's ENTIRE range over the session was 63189 -> 63502 = 49bp, so
``realized_move_bp >= 80`` was arithmetically unreachable: the grid could not
re-center once, by construction.

Root cause: ``rgrid_reset_threshold_pct`` is a percent of PRICE (registry
default 1.0%; UI presets 0.8% / 1.5%) mapped straight to
``dgrid_reset_threshold_bp``, and the controller then applied it as
``max(user, 12bp, band/2)`` — a one-sided FLOOR. Nothing related the threshold
to the width of the ladder it steers, and ``_recycle_completed_levels`` re-arms
a round-tripped level at its ORIGINAL price, so the re-center is the ONLY thing
that ever moves a ladder toward price.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.controller_base import (
    LADDER_RECENTER_FLOOR_BP,
    ladder_recenter_threshold_bp,
)
from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
from src.nadobro.engine.controllers.grid_trading import GridController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator


# ── the pure geometry rule ──────────────────────────────────────

def test_production_session_165_threshold_is_capped_to_the_band():
    """80bp asked of a 20bp ladder must come back as 20bp."""
    bp, clamped = ladder_recenter_threshold_bp(step_bp=10.0, levels=3, user_bp=80.0)
    assert clamped is True
    assert bp == 20.0


@pytest.mark.parametrize("user_pct", [0.8, 1.0, 1.5])
def test_every_shipped_rgrid_reset_preset_is_capped(user_pct):
    """The registry default (1.0%) and both UI presets all exceed the band.

    This is why the bug was not one user's misconfiguration: on a 10bp x 3
    ladder every value the product offers is 4x-7.5x too wide.
    """
    bp, clamped = ladder_recenter_threshold_bp(
        step_bp=10.0, levels=3, user_bp=user_pct * 100.0)
    assert clamped is True and bp == 20.0


def test_unset_threshold_keeps_the_band_width_auto_follow():
    assert ladder_recenter_threshold_bp(step_bp=10.0, levels=3, user_bp=0.0) == (20.0, False)


def test_a_reasonable_user_value_inside_the_band_is_honored():
    """The cap must not steamroll a value that is already sane."""
    bp, clamped = ladder_recenter_threshold_bp(step_bp=20.0, levels=5, user_bp=45.0)
    assert clamped is False and bp == 45.0   # band = 80bp, so 45 stands


def test_threshold_never_churns_inside_one_level_step():
    """Below one step a re-center cancels and re-places at the same rung."""
    bp, clamped = ladder_recenter_threshold_bp(step_bp=30.0, levels=3, user_bp=1.0)
    assert clamped is False and bp == 30.0


def test_tiny_ladders_keep_the_absolute_floor():
    bp, _ = ladder_recenter_threshold_bp(step_bp=1.0, levels=2, user_bp=0.5)
    assert bp == LADDER_RECENTER_FLOOR_BP


def test_degenerate_geometry_does_not_explode():
    assert ladder_recenter_threshold_bp(0.0, 0, 0.0)[0] == LADDER_RECENTER_FLOOR_BP
    assert ladder_recenter_threshold_bp(0.0, 1, -5.0)[0] == LADDER_RECENTER_FLOOR_BP


# ── the behaviour that was broken in production ─────────────────

_PROD_CFG = {
    # Session 165's real geometry: BTC, 10bp step, 3 levels, 0.8% reset.
    "trading_pair": "BTC-PERP",
    "start_price": "63200", "end_price": "63400", "limit_price": "0",
    "total_amount_quote": "1000", "min_spread_between_orders": "0.001",
    "max_open_orders": 3, "step_pct": "0.001", "levels_count": 3,
    "dgrid_reset_threshold_bp": 80.0,
    "regime_gate_enabled": 0.0,
}


def _range_candles():
    return [{"close": 63300 + (i % 2) * 20} for i in range(200)]


def test_rgrid_ladder_follows_a_150_dollar_btc_move():
    """The reported symptom: BTC moved >$150 and the orders did not move.

    $150 on BTC at 63373 is 23.7bp — under the old 80bp threshold, over the
    20bp band. The ladder must now re-quote toward the new mid.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63373.5"))
        orch = ExecutorOrchestrator()
        c = DynamicGridController(
            user_id=1, orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(),
            configs=dict(_PROD_CFG, candle_provider=lambda p: _range_candles()),
        )
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        ex = orch.list(c.id, active_only=True)[0]
        before = [lv.open_price for lv in ex.levels]
        assert before, "the ladder must arm before we can test following"
        assert c.reset_threshold_bp == 20.0, "80bp must be capped to the 20bp band"

        # BTC +$150.
        adapter.set_mid(Decimal("63523.5"))
        await orch.tick_controller(c.id)

        after = [lv.open_price for lv in orch.list(c.id, active_only=True)[0].levels]
        assert after != before, (
            "the ladder froze: a 23.7bp move left every quote at its old price — "
            "this is the production bug (2063/2080 cycles placed nothing)"
        )
        assert max(after) > max(before), "quotes must move toward the new mid"

    asyncio.run(body())


def test_recenter_is_not_starved_by_an_unconfirmed_phase_flip():
    """Re-center used to live in the `else` of `if flip_needed:`.

    dgrid needs 2 consecutive ticks to confirm a regime change, so on every tick
    where the classifier disagreed with the live phase the ladder ALSO skipped
    its re-center — it froze during exactly the moves big enough to shift the
    regime read.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63373.5"))
        orch = ExecutorOrchestrator()
        trending = [{"close": 63300 + i * 3} for i in range(200)]
        c = DynamicGridController(
            user_id=1, orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(),
            configs=dict(_PROD_CFG, candle_provider=lambda p: _range_candles(),
                         dgrid_flip_confirm_ticks=2),
        )
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        ex = orch.list(c.id, active_only=True)[0]
        before = [lv.open_price for lv in ex.levels]

        # Force a pending-but-unconfirmed flip on the same tick as a big move.
        c.configs["candle_provider"] = lambda p: trending
        adapter.set_mid(Decimal("63523.5"))
        await orch.tick_controller(c.id)

        live = orch.list(c.id, active_only=True)
        if live and live[0] is ex:
            # Still the same executor => no flip fired, so the re-center owed us
            # a re-quote on this very tick.
            assert [lv.open_price for lv in ex.levels] != before, (
                "an unconfirmed flip must not suppress the re-center"
            )

    asyncio.run(body())


def test_classic_grid_shares_the_same_cap():
    """GridController carried the identical one-sided floor."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("100"))
        orch = ExecutorOrchestrator()
        c = GridController(
            user_id=1, orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(),
            configs={
                "trading_pair": "BTC-PERP", "start_price": Decimal("99"),
                "end_price": Decimal("100"), "limit_price": Decimal(0),
                "total_amount_quote": Decimal(1000),
                "min_spread_between_orders": Decimal("0.001"),
                "max_open_orders": 3, "step_pct": Decimal("0.001"),
                "levels_count": 3, "regime_gate_enabled": 0.0,
                # 1.0% — the registry default, 5x this ladder's 20bp band.
                "reset_threshold_bp": 100.0,
            },
            controller_id="G",
        )
        await orch.spawn_controller(c)
        assert c.reset_threshold_bp == 20.0

    asyncio.run(body())
