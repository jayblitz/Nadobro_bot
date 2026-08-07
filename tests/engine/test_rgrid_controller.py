"""Reverse Grid (R-Grid) — its own controller and its own taker executor.

Production report 2026-08-06: an R-Grid BTC session posted "Reverse GRID switched
RGRID → GRID … downtrend detected (variance ratio 0.83) … now quoting the LONG
ladder" and showed a "DGRID phase" line on /status, because R-Grid was wired to
the D-Grid phase switcher. R-Grid has no phases.

What defines it (user spec):
1. anchor = AVERAGE of the buy and sell exposure prices, each a rolling VWAP over
   the most recent portion of that leg's filled volume.
2. buy = anchor x (1 + spread) acts as price RISES above it;
   sell = anchor x (1 - spread) acts as price FALLS below it.
3. Fills are TAKER on both legs — passive-only is OFF, structurally.
4. Soft reset: once price has drifted favourably by reset_threshold_pct AND the
   position is in profit, the opposite (exit) leg FOLLOWS the trend.
5. SL/TP are %-of-margin session rails, never also a price barrier.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.rgrid import RGridController
from src.nadobro.engine.executors.rgrid_taker_executor import (
    INTENT_ENTRY,
    INTENT_TRAIL_EXIT,
    RGridTakerExecutor,
)
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import ExecutionStrategy, OrderType, PositionAction, TradeType

PAIR = "BTC-PERP"
SPREAD = Decimal("0.001")


def _controller(adapter, extra=None):
    configs = {
        "trading_pair": PAIR,
        "spread_bid_pct": SPREAD,
        "spread_ask_pct": SPREAD,
        "order_amount_quote": Decimal(10),
        "price_distance_tolerance": Decimal("0.0001"),
    }
    configs.update(extra or {})
    orch = ExecutorOrchestrator()
    c = RGridController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(), configs=configs, controller_id="RG",
    )
    return orch, c


def _takers(adapter, side=None):
    out = [o for o in adapter.placed if o.order_type is OrderType.MARKET]
    return [o for o in out if side is None or o.side is side]


# ==========================================================================
# 1. The anchor
# ==========================================================================
def test_anchor_is_the_average_of_the_two_leg_exposure_prices():
    """A single blended VWAP over both sides cannot express this: it weights
    whichever leg traded more volume."""
    adapter = MockNadoAdapter(mid=Decimal(100))
    _, c = _controller(adapter)
    for px, base in ((Decimal(100), Decimal(2)), (Decimal(100), Decimal(1))):
        c._leg_fills["buy"].append((px, base))
    c._leg_fills["sell"].append((Decimal(110), Decimal(1)))

    assert c.leg_exposure_price("buy") == Decimal(100)
    assert c.leg_exposure_price("sell") == Decimal(110)
    # Midpoint between the legs — NOT the volume-blended 102.5.
    assert c.exposure_anchor() == Decimal(105)


def test_one_sided_book_anchors_on_the_leg_that_traded():
    adapter = MockNadoAdapter(mid=Decimal(100))
    _, c = _controller(adapter)
    for px in (Decimal(100), Decimal(104)):
        c._leg_fills["buy"].append((px, Decimal(1)))
    assert c.exposure_anchor() == Decimal(102)
    assert c.leg_exposure_price("sell") is None


def test_discretion_windows_each_leg_independently():
    """vwap_volume_fraction (2 x the user's discretion) VWAPs only the most recent
    slice of EACH leg's volume — a tighter, more reactive exposure price."""
    adapter = MockNadoAdapter(mid=Decimal(100))
    _, c = _controller(adapter, extra={"vwap_volume_fraction": Decimal("0.5")})
    for px in (100, 100, 110, 120):
        c._leg_fills["buy"].append((Decimal(px), Decimal(1)))
    # Last 50% of the buy leg's volume (2 of 4 units): (120 + 110) / 2 = 115.
    assert c.leg_exposure_price("buy") == Decimal("115")
    c.vwap_volume_fraction = Decimal(0)
    assert c.leg_exposure_price("buy") == Decimal("107.5")


def test_no_fills_anchors_on_the_seeded_mid_so_the_first_move_is_a_real_break():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)      # mid == anchor → no break
        assert adapter.placed == []
        assert c.exposure_anchor() == Decimal(100)

    asyncio.run(body())


def test_seed_from_session_history_scopes_the_anchor_to_the_run():
    """A rebuild (worker handoff / restart) must not start blank: the runtime seeds
    this session's OWN recorded fills, newest-first."""
    adapter = MockNadoAdapter(mid=Decimal(100))
    seed = [
        {"price": 120, "size": 1, "side": "long"},
        {"price": 100, "size": 1, "side": "long"},
        {"price": 110, "size": 1, "side": "short"},
    ]
    _, c = _controller(adapter, extra={"seed_fills": seed})
    assert c.leg_exposure_price("buy") == Decimal(110)     # (100 + 120) / 2
    assert c.leg_exposure_price("sell") == Decimal(110)
    assert c.exposure_anchor() == Decimal(110)
    # A fresh session starts blank.
    _, c2 = _controller(adapter)
    assert c2.exposure_anchor() is None


def test_exposure_window_is_isolated_per_controller_and_user():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        base = {
            "trading_pair": PAIR, "spread_bid_pct": SPREAD, "spread_ask_pct": SPREAD,
            "order_amount_quote": Decimal(10), "price_distance_tolerance": Decimal("0.0001"),
        }
        a = RGridController(user_id=1, orchestrator=orch, adapter=adapter,
                            inventory=InventoryRepository(), configs=dict(base),
                            controller_id="rgrid:1:mainnet")
        b = RGridController(user_id=2, orchestrator=orch, adapter=adapter,
                            inventory=InventoryRepository(), configs=dict(base),
                            controller_id="rgrid:2:mainnet")
        await orch.spawn_controller(a)
        await orch.spawn_controller(b)
        await orch.tick_controller(a.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(a.id)      # A takes the break
        await orch.tick_controller(a.id)      # A absorbs its own fill
        await orch.tick_controller(b.id)
        assert a._has_fills()
        assert not b._has_fills(), "another user's fills bled into this anchor"

    asyncio.run(body())


# ==========================================================================
# 2. The triggers
# ==========================================================================
def test_triggers_straddle_the_exposure_average_by_one_spread():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        # Legs at 98 and 102 → anchor 100.
        c._leg_fills["buy"].append((Decimal(98), Decimal(1)))
        c._leg_fills["sell"].append((Decimal(102), Decimal(1)))
        for px in ("100.05", "99.95"):        # inside the band both ways
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
        assert _takers(adapter) == [], "acted inside the exposure band"
        adapter.set_mid(Decimal("100.2"))     # above 100 * 1.001
        await orch.tick_controller(c.id)
        assert len(_takers(adapter, TradeType.BUY)) == 1
        assert _takers(adapter, TradeType.SELL) == []

    asyncio.run(body())


def test_buys_the_break_up_and_sells_the_break_down():
    async def body():
        for px, side, other in (("101", TradeType.BUY, TradeType.SELL),
                                ("99", TradeType.SELL, TradeType.BUY)):
            adapter = MockNadoAdapter(mid=Decimal(100))
            orch, c = _controller(adapter)
            await orch.spawn_controller(c)
            await orch.tick_controller(c.id)
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
            assert len(_takers(adapter, side)) == 1, px
            assert _takers(adapter, other) == [], px

    asyncio.run(body())


def test_adds_into_a_trend_and_never_fights_it():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        for px in ("101", "104", "108", "113"):
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)      # may fire a taker
            await orch.tick_controller(c.id)      # absorb it into the anchor
        assert len(_takers(adapter, TradeType.BUY)) >= 2, "adds longs as price pumps"
        assert _takers(adapter, TradeType.SELL) == [], "never shorts into a pump"

    asyncio.run(body())


def test_only_one_taker_is_in_flight_at_a_time():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("110"))
        await orch.tick_controller(c.id)
        assert len(_takers(adapter)) == 1
        for px in ("112", "115", "118"):          # price keeps running
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
        assert len(_takers(adapter)) == 1, "stacked takers while one was unsettled"

    asyncio.run(body())


# ==========================================================================
# 3. Taker-only, structurally
# ==========================================================================
@pytest.mark.parametrize("strategy", [
    ExecutionStrategy.LIMIT_MAKER, ExecutionStrategy.LIMIT,
])
def test_the_executor_refuses_to_be_a_maker(strategy):
    """Passive-only is OFF by design: a buy trigger sits ABOVE the market, so a
    post-only order there is rejected, not filled. Enforced in the constructor so
    a later edit cannot quietly turn R-Grid into a book that never fills."""
    adapter = MockNadoAdapter(mid=Decimal(100))
    cfg = OrderExecutorConfig(
        PAIR, TradeType.BUY, Decimal(1), strategy, price=Decimal(100),
    )
    with pytest.raises(ValueError, match="taker-only"):
        RGridTakerExecutor(cfg, user_id=1, controller_id="RG", adapter=adapter)


def test_every_order_rgrid_places_is_a_market_order():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        for px in ("101", "104", "103"):
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
            await orch.tick_controller(c.id)
        assert adapter.placed, "expected R-Grid to have traded"
        assert all(o.order_type is OrderType.MARKET for o in adapter.placed), \
            "R-Grid placed a non-taker order"

    asyncio.run(body())


def test_entry_takers_open_and_exit_takers_are_reduce_only():
    adapter = MockNadoAdapter(mid=Decimal(100))
    from src.nadobro.engine.executors.rgrid_taker_executor import (
        build_entry_taker, build_exit_taker,
    )
    entry = RGridTakerExecutor(
        build_entry_taker(PAIR, TradeType.BUY, Decimal(1)),
        user_id=1, controller_id="RG", adapter=adapter, intent=INTENT_ENTRY,
    )
    exit_ = RGridTakerExecutor(
        build_exit_taker(PAIR, TradeType.SELL, Decimal(1)),
        user_id=1, controller_id="RG", adapter=adapter, intent=INTENT_TRAIL_EXIT,
    )
    assert entry.config.position_action is PositionAction.OPEN and not entry.is_exit
    assert exit_.config.position_action is PositionAction.CLOSE and exit_.is_exit


# ==========================================================================
# 4. The soft reset
# ==========================================================================
def test_soft_reset_lets_a_winning_trend_run_then_banks_it_on_the_stall():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))                  # break up → long
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        assert len(_takers(adapter, TradeType.BUY)) == 1

        adapter.set_mid(Decimal("103"))                  # +2% > 1% arm threshold
        await orch.tick_controller(c.id)
        assert c.anchor_state()["trail_armed"] is True
        assert _takers(adapter, TradeType.SELL) == [], "banked instead of following"

        adapter.set_mid(Decimal("108"))                  # trend continues
        await orch.tick_controller(c.id)
        assert _takers(adapter, TradeType.SELL) == []
        assert c.anchor_state()["trail_peak"] == Decimal("108")

        adapter.set_mid(Decimal("107.5"))                # below 108 * 0.999
        await orch.tick_controller(c.id)
        assert _takers(adapter, TradeType.SELL), "the trailing exit never fired"

    asyncio.run(body())


def test_soft_reset_trails_a_winning_short_the_other_way():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("99"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        assert len(_takers(adapter, TradeType.SELL)) == 1
        adapter.set_mid(Decimal("97"))
        await orch.tick_controller(c.id)
        assert c.anchor_state()["trail_armed"] is True
        adapter.set_mid(Decimal("92"))
        await orch.tick_controller(c.id)
        assert c.anchor_state()["trail_peak"] == Decimal("92")
        assert _takers(adapter, TradeType.BUY) == []
        adapter.set_mid(Decimal("92.5"))                 # above 92 * 1.001
        await orch.tick_controller(c.id)
        assert _takers(adapter, TradeType.BUY), "the trailing exit never fired for a short"

    asyncio.run(body())


def test_soft_reset_never_arms_on_a_losing_position():
    """"...and you're in profit". A trail that armed underwater would become a stop
    that front-runs the session SL rail."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
        c._position_entry_price = lambda: Decimal(100)        # type: ignore[method-assign]
        for px in ("99", "97", "95"):
            assert await c._maybe_soft_reset(Decimal(px)) == "none"
        assert c._trail_armed is False
        assert adapter.placed == [], "the trail closed a loser — that is the SL rail's job"
        assert await c._maybe_soft_reset(Decimal("102")) == "none"
        assert c._trail_armed is True

    asyncio.run(body())


def test_a_threshold_inside_the_band_widens_the_arm_it_does_not_disable_the_exit():
    """An exit narrower than the entry band would arm before the break that opened
    the trade is established. The old code REFUSED in that case — which silently
    removed R-Grid's only non-rail exit, because the overlay scales the spread live
    while the threshold is not scaled, and the shipped defaults sit exactly on the
    2 x band boundary. Widen the arm instead. (Audit 2026-08-06.)"""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.0015"),   # < 2 x 0.001 band
            "trail_enabled": True,
        })
        c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
        c._position_entry_price = lambda: Decimal(100)        # type: ignore[method-assign]
        # Inside the widened arm (2 x band = 0.2%): still not armed.
        assert await c._maybe_soft_reset(Decimal("100.15")) == "none"
        assert c._trail_armed is False
        # Past it: armed — the mechanism is alive, just wider.
        assert await c._maybe_soft_reset(Decimal("100.5")) == "none"
        assert c._trail_armed is True

    asyncio.run(body())


def test_the_overlay_widening_the_spread_cannot_disarm_the_soft_reset():
    """The exact production path: overlay spread_mult > 1 rewrites spread_ask_pct
    live, and reset_threshold_pct is not scaled with it."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.002"),   # shipped default, == 2 x band
            "trail_enabled": True,
        })
        c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
        c._position_entry_price = lambda: Decimal(100)        # type: ignore[method-assign]
        c.spread_ask_pct = Decimal("0.0012")                  # overlay widened it 1.2x
        assert await c._maybe_soft_reset(Decimal("101")) == "none"
        assert c._trail_armed is True, "a widened spread disarmed the only exit"

    asyncio.run(body())


def test_entry_basis_is_this_positions_own_leg_not_the_session_lifetime_average():
    """inventory.breakeven is the session-LIFETIME avg buy/avg sell and every leg
    accumulates into it — including trail exits. A long closed by a SELL leaves
    that exit inside avg_sell_price, so the NEXT short read a large favourable
    excursion the moment it opened, armed instantly, and exited on the first
    band-width move against it. (Audit 2026-08-06.)"""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        # Session history: long @100 entered, exited @102, then a fresh short @99.
        c.inventory.apply_fill(c.user_id, PAIR, c.id, TradeType.BUY, Decimal(1), Decimal(100), Decimal(0))
        c.inventory.apply_fill(c.user_id, PAIR, c.id, TradeType.SELL, Decimal(1), Decimal(102), Decimal(0))
        c.inventory.apply_fill(c.user_id, PAIR, c.id, TradeType.SELL, Decimal(1), Decimal(99), Decimal(0))
        # The exposure window holds ONLY the open short's entry (exits excluded,
        # cleared when flat).
        c._leg_fills["sell"].append((Decimal(99), Decimal(1)))

        assert c.inventory.get(c.user_id, PAIR, c.id).breakeven == Decimal("100.5")
        assert c._position_entry_price() == Decimal(99), "used the lifetime average"
        # And so a fresh at-breakeven short does NOT arm the trail.
        assert await c._maybe_soft_reset(Decimal(99)) == "none"
        assert c._trail_armed is False

    asyncio.run(body())


def test_a_refused_exit_does_not_fall_through_into_the_add_branch():
    """Piling risk onto a position we just failed to reduce is the wrong default
    for a safety action."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("103"))
        await orch.tick_controller(c.id)                 # arms the trail
        placed_before = len(adapter.placed)

        async def _refuse(*_a, **_k):
            return False
        c.spawn_executor = _refuse                       # type: ignore[method-assign]
        adapter.set_mid(Decimal("102"))                  # stall → exit wanted, refused
        await orch.tick_controller(c.id)
        assert len(adapter.placed) == placed_before, "added risk after a refused exit"

    asyncio.run(body())


# ==========================================================================
# 5. Exit fills never pollute the anchor
# ==========================================================================
def test_exit_fills_are_excluded_from_the_exposure_window():
    """The window is where the book took risk ON. An exit prints at whatever the
    market offered; folding it in drags the anchor to the exit price, which
    re-triggers an entry a tick after the close."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        assert c._leg_fills["buy"], "the entry belongs in the buy leg window"
        adapter.set_mid(Decimal("103"))
        await orch.tick_controller(c.id)                 # arm
        adapter.set_mid(Decimal("102.5"))
        await orch.tick_controller(c.id)                 # trailing exit fires
        await orch.tick_controller(c.id)                 # settles
        assert all(px != Decimal("102.5") for px, _ in c._leg_fills["sell"]), \
            "the exit price entered the exposure window"

    asyncio.run(body())


def test_flat_after_banking_re_arms_instead_of_re_entering_at_the_exit():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("103"))
        await orch.tick_controller(c.id)                 # arm
        adapter.set_mid(Decimal("102.5"))
        await orch.tick_controller(c.id)                 # bank
        await orch.tick_controller(c.id)                 # settle → flat
        assert c._net_base() == 0
        buys_before = len(_takers(adapter, TradeType.BUY))
        await orch.tick_controller(c.id)                 # the re-arm tick
        assert len(_takers(adapter, TradeType.BUY)) == buys_before, \
            "re-entered immediately at the price it just sold"
        assert not c._has_fills() and c._trail_armed is False
        adapter.set_mid(Decimal("106"))                  # a genuine fresh break
        await orch.tick_controller(c.id)
        assert len(_takers(adapter, TradeType.BUY)) > buys_before

    asyncio.run(body())


# ==========================================================================
# 6. Wiring
# ==========================================================================
def test_rgrid_is_always_its_own_controller_never_the_phase_switcher():
    from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
    from src.nadobro.strategy.engine_runtime import (
        CONTROLLER_REGISTRY, build_controller, map_strategy_config,
    )

    assert CONTROLLER_REGISTRY["rgrid"] is RGridController
    assert CONTROLLER_REGISTRY["dgrid"] is DynamicGridController
    assert not hasattr(RGridController, "consume_dgrid_event")

    for extra in ({}, {"fill_anchored": 0}, {"fill_anchored": 1}):
        cfg = map_strategy_config(
            "rgrid", {"notional_usd": 100.0, "levels": 2, **extra}, Decimal(100), product=PAIR,
        )
        assert cfg["anchor_mode"] == "rgrid"
        assert cfg["momentum"] is True and cfg["trail_enabled"] is True
        assert cfg["ladder_levels"] == 1, "R-Grid rests no ladder"
        for dead in ("start_price", "end_price", "dgrid_trend_on_vr",
                     "dgrid_flip_confirm_ticks", "triple_barrier_config"):
            assert dead not in cfg, dead
        built = build_controller(
            "rgrid", user_id=1, configs=cfg,
            orchestrator=ExecutorOrchestrator(), adapter=MockNadoAdapter(mid=Decimal(100)),
            inventory=InventoryRepository(),
        )
        assert isinstance(built, RGridController), extra


def test_discretion_maps_to_twice_the_knob_and_clamps():
    from src.nadobro.strategy.engine_runtime import map_strategy_config

    # Spec: "last 12% of quantity when discretion is 0.06" — per side, so 2x.
    cfg = map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "levels": 2, "rgrid_discretion": 0.06},
        Decimal(100), product=PAIR,
    )
    assert cfg["vwap_volume_fraction"] == 0.12
    wide = map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "levels": 2, "rgrid_discretion": 0.8},
        Decimal(100), product=PAIR,
    )
    assert wide["vwap_volume_fraction"] == 1.0


def test_session_rails_are_percent_of_margin_and_not_also_a_price_barrier():
    from src.nadobro.strategy.engine_runtime import map_strategy_config
    from src.nadobro.strategy.strategy_registry import effective_sl_tp_pct

    cfg = map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "levels": 4, "sl_pct": 0.8,
                  "rgrid_stop_loss_pct": 2.0, "rgrid_take_profit_pct": 5.0},
        Decimal(100), product=PAIR,
    )
    assert "triple_barrier_config" not in cfg, "the same number must not be both"
    assert effective_sl_tp_pct(
        "rgrid", {"rgrid_stop_loss_pct": 2.0, "rgrid_take_profit_pct": 5.0}
    ) == (2.0, 5.0)


def test_metrics_expose_the_anchor_triggers_and_trail():
    adapter = MockNadoAdapter(mid=Decimal(100))
    _, c = _controller(adapter, extra={"reset_threshold_pct": Decimal("0.01")})
    c._leg_fills["buy"].append((Decimal(100), Decimal(1)))
    c._last_anchor = c.exposure_anchor()
    m = c.grid_metrics()
    assert m["grid_mode"] == "rgrid"
    assert m["grid_anchor_price"] == 100.0
    assert m["rgrid_buy_trigger"] == pytest.approx(100.1)
    assert m["rgrid_sell_trigger"] == pytest.approx(99.9)
    assert m["rgrid_trail_armed"] is False


# ==========================================================================
# 7. Telemetry contract (audit 2026-08-06)
# ==========================================================================
# The /status + order-monitor cards read fixed key names out of bot_state, which
# the runtime copies from the controller's grid_metrics(). A controller that
# emits FEWER keys than the card reads does not fail — it renders 0.000% / NONE /
# n/a, or worse keeps the STALE value from whatever controller ran before, since
# the runtime only overwrites a key it is actually handed. That is how an R-Grid
# card could keep showing a D-Grid side after the controller split.
def test_grid_metrics_emits_every_key_the_runtime_persists_and_the_card_reads():
    import inspect

    from src.nadobro.strategy import bot_runtime

    adapter = MockNadoAdapter(mid=Decimal(100))
    _, c = _controller(adapter, extra={"reset_threshold_pct": Decimal("0.01")})
    c._leg_fills["buy"].append((Decimal(100), Decimal(1)))
    c._last_anchor = c.exposure_anchor()
    c._last_mid = Decimal("101")
    metrics = c.grid_metrics()

    # Every grid_* key the cycle persists must be produced by this controller.
    source = inspect.getsource(bot_runtime._run_cycle)
    persisted = {
        k for k in source.split('for _k in ("dgrid_phase"')[1].split("):")[0].replace('"', "").replace("\n", "").split(",")
        if k.strip().startswith("grid_")
    }
    missing = {k.strip() for k in persisted} - set(metrics)
    assert not missing, f"grid_metrics() omits persisted telemetry: {sorted(missing)}"


def test_drift_and_reset_side_reflect_r_grid_reality():
    adapter = MockNadoAdapter(mid=Decimal(100))
    _, c = _controller(adapter, extra={"reset_threshold_pct": Decimal("0.01")})
    c._leg_fills["buy"].append((Decimal(100), Decimal(1)))
    c._last_anchor = c.exposure_anchor()

    # Flat / unarmed: no side to protect, no drift without a mid.
    assert c.grid_metrics()["grid_reset_side"] == "NONE"

    c._last_mid = Decimal("102")
    assert c.grid_metrics()["grid_drift_from_anchor_pct"] == pytest.approx(2.0)

    # Armed on a long ⇒ the soft reset protects the SELL exit.
    c._trail_armed = True
    c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
    c.inventory.apply_fill(c.user_id, PAIR, c.id, TradeType.BUY, Decimal(1), Decimal(100), Decimal(0))
    m = c.grid_metrics()
    assert m["grid_reset_side"] == "SELL" and m["grid_soft_reset_engaged"] is True
    # And the two legs the anchor averages are published for the card.
    assert m["grid_buy_exposure_price"] == 100.0
    assert m["grid_sell_exposure_price"] == 0.0           # leg never traded → n/a


def test_a_partially_filled_exit_keeps_reducing_instead_of_stranding_the_remainder():
    """A market close can fill partially. Disarming the trail on the REQUEST would
    leave the remainder needing a fresh favourable excursion before the exit
    re-armed — unprotected, even though the trend has already turned."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        # Armed long, trend has turned: the exit fires.
        c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
        c._position_entry_price = lambda: Decimal(100)        # type: ignore[method-assign]
        assert await c._maybe_soft_reset(Decimal("103")) == "none"   # arms
        assert c._trail_armed is True
        assert await c._maybe_soft_reset(Decimal("102")) == "fired"  # peak 103 → trail
        # Still armed, so a residual position keeps being reduced next tick.
        assert c._trail_armed is True and c._trail_peak is not None

        # Once genuinely flat, the trail disarms and the window resets.
        c._net_base = lambda: Decimal(0)                      # type: ignore[method-assign]
        assert await c._maybe_soft_reset(Decimal("102")) == "none"
        assert c._trail_armed is False and c._trail_peak is None

    asyncio.run(body())


# ==========================================================================
# 7. Regime switches: recalibrate, never pause (audit 2026-08-06)
# ==========================================================================
# R-Grid's whole essence is following a move. Standing it down is backwards, and
# standing it down MID-POSITION is worse: nobody is left managing the exit. It
# handles a switch by getting flat and re-anchoring instead.
def test_a_break_against_the_book_flattens_it_in_one_order():
    """Nibbling one step per tick leaves most of the position on the wrong side of
    a running move, paying a taker fee per nibble."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # Build a multi-step long.
        for px in ("101", "104", "108"):
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
            await orch.tick_controller(c.id)
        net_before = c._net_base()
        assert net_before > 0, "precondition: a long book"
        entries = len(_takers(adapter, TradeType.BUY))

        # Price breaks the SELL trigger: the thesis is invalidated.
        adapter.set_mid(Decimal("100"))
        await orch.tick_controller(c.id)
        sells = _takers(adapter, TradeType.SELL)
        assert sells, "the reversal never fired"
        # ONE order for the WHOLE position, not one step.
        assert sells[-1].amount_base == abs(net_before)
        assert len(_takers(adapter, TradeType.BUY)) == entries, "added while reversing"

    asyncio.run(body())


def test_the_reversal_is_reduce_only_and_re_anchors_for_the_next_break():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("104"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        assert c._net_base() > 0

        adapter.set_mid(Decimal("100"))
        await orch.tick_controller(c.id)          # reversal flatten
        closes = [
            e for e in c.my_executors(active_only=False)
            if getattr(getattr(e, "config", None), "position_action", None)
            is PositionAction.CLOSE
        ]
        assert closes, "the reversal must be reduce-only"
        await orch.tick_controller(c.id)          # settles -> flat
        assert c._net_base() == 0
        await orch.tick_controller(c.id)          # re-anchor tick
        assert not c._has_fills(), "the closed position's entries still anchor us"

    asyncio.run(body())


def test_rgrid_is_never_choked_or_gated_by_the_overlay():
    """SUPPRESS-CAP-ZERO. The overlay must not hand R-Grid a suppression posture:
    it keeps its configured cap, is not put reduce-only, and is not gated."""
    from src.nadobro.llm.signal_engine import Signal
    from src.nadobro.strategy.overlay_actuator import (
        apply_overrides_to_configs, compute_overrides,
    )

    overrides = compute_overrides("rgrid", Signal(regime="chop", entry_ok=False, confidence=0.6))
    cfg = {"max_net_exposure_pct": 30.0, "order_amount_quote": Decimal(100)}
    apply_overrides_to_configs("rgrid", cfg, overrides)
    assert float(cfg["max_net_exposure_pct"]) == 30.0
    assert "suppress_new_entries" not in cfg
    assert "regime_gate_enabled" not in cfg


def test_the_overlay_arms_the_exit_early_instead_of_pausing():
    """The protection that REPLACES suppression: a position the overlay reads
    against gets its trailing exit armed as soon as it is in profit, rather than
    waiting for the full threshold. It still refuses to arm underwater."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.05"),     # far away: normally no arm
            "trail_enabled": True,
            "signal_regime": "trend_down", "signal_confidence": 0.9,
        })
        c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
        c._position_entry_price = lambda: Decimal(100)        # type: ignore[method-assign]
        # Underwater with a hostile signal: still no arm.
        assert await c._maybe_soft_reset(Decimal("99")) == "none"
        assert c._trail_armed is False
        # Barely in profit, well under the 5% threshold: the hostile read arms it.
        assert await c._maybe_soft_reset(Decimal("100.5")) == "none"
        assert c._trail_armed is True

    asyncio.run(body())


def test_a_supportive_overlay_does_not_arm_early():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        _, c = _controller(adapter, extra={
            "reset_threshold_pct": Decimal("0.05"), "trail_enabled": True,
            "signal_regime": "trend_up", "signal_confidence": 0.9,
        })
        c._net_base = lambda: Decimal(1)                      # type: ignore[method-assign]
        c._position_entry_price = lambda: Decimal(100)        # type: ignore[method-assign]
        assert await c._maybe_soft_reset(Decimal("100.5")) == "none"
        assert c._trail_armed is False, "a supportive read must not cut the run short"

    asyncio.run(body())


# ==========================================================================
# 8. Quantization — the venue must never grow us past the approved size
# ==========================================================================
def test_an_entry_is_rounded_down_to_the_lot_never_up():
    adapter = MockNadoAdapter(mid=Decimal(100), lot=Decimal("0.5"), min_notional=Decimal(1))
    _, c = _controller(adapter)
    # 0.37 units -> 0 lots of 0.5 would be a no-trade; 1.2 -> 1.0.
    assert c._quantize_entry(Decimal("1.2"), Decimal(100)) == Decimal("1.0")
    assert c._quantize_entry(Decimal("0.37"), Decimal(100)) is None


def test_a_sub_minimum_entry_is_refused_not_shipped():
    """NadoClient.place_order GROWS a non-reducing order that lands under the
    venue minimum. For an ENTRY that means trading more than the risk engine, the
    step cap and the stop budget were all sized against. Decline instead."""
    adapter = MockNadoAdapter(mid=Decimal(100), lot=Decimal("0.001"),
                              min_notional=Decimal(50))
    _, c = _controller(adapter)
    assert c._quantize_entry(Decimal("0.1"), Decimal(100)) is None       # $10 < $50
    assert c._quantize_entry(Decimal("1.0"), Decimal(100)) == Decimal("1.0")   # $100 ok


def test_risk_approval_describes_the_quantized_size():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), lot=Decimal("0.5"),
                                  min_notional=Decimal(1))
        orch, c = _controller(adapter, extra={"order_amount_quote": Decimal(120)})
        seen = {}

        async def _capture(ex, request):
            seen["quote"] = request.order_amount_quote
            return False
        c.spawn_executor = _capture                          # type: ignore[method-assign]
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("110"))
        await orch.tick_controller(c.id)
        # 120/110 = 1.09 units -> 1.0 lot -> $110, not the requested $120.
        assert seen["quote"] == Decimal("110.0")

    asyncio.run(body())
