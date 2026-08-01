"""Volume Bot v4.1 — taker mode (docs/volume_bot_taker_v4.md).

Both legs are taker orders (marketable limits, price-bounded) fired back to
back with NO profit gate — PnL is not the objective, volume is. The bot is
FLAT between cycles, and the session loss limit (5% of margin, net of fees)
is what bounds a run.

The v3 maker path is pinned by tests/engine/controllers/test_volume_bot.py.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import OrderType

PAIR = "KBTC"


def _vb(adapter, orch, **cfg):
    configs = {
        "trading_pair": PAIR,
        "total_amount_quote": "100",
        # 10 bp/leg all-in keeps the arithmetic legible in assertions.
        "spot_taker_fee_rate": "0.0009",
        "vol_builder_fee_rate": "0.0001",
        "vol_min_profit_bp": 1.0,
        "vol_taker_max_hold_seconds": 300.0,
        "vol_max_cycle_loss_bp": 20.0,
    }
    configs.update(cfg)
    return VolumeBotController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(), configs=configs, controller_id="VB",
    )


def test_taker_is_the_default_and_buys_at_market():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch)
        assert c.taker_mode is True, "v4 ships taker-by-default"
        await orch.spawn_controller(c)
        assert adapter.placed, "a taker buy must be submitted on start"
        # Marketable LIMIT, not naked MARKET: it crosses the book (immediate
        # taker fill) but carries a hard price bound. See F1.
        assert adapter.placed[0].order_type is OrderType.LIMIT
        assert adapter.placed[0].price is not None

    asyncio.run(body())


def test_taker_round_trip_books_both_legs_into_volume():
    """Fee estimates are quoted against target volume, so both legs MUST count
    — otherwise the estimate is off by 2x (see vol_fee_estimator)."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="0")
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)
        sell = orch.get(c.sell_id)
        adapter.fill_order(sell.order.id, price=Decimal("101"))
        await orch.tick_controller(c.id)

        assert c.cycles_completed == 1
        # ~100 bought + ~101 sold, both counted.
        assert c.session_volume_usd > Decimal("195")

    asyncio.run(body())


def test_maker_mode_still_reachable_as_a_kill_switch():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_taker_mode=0)
        assert c.taker_mode is False
        await orch.spawn_controller(c)
        assert adapter.placed[0].order_type is OrderType.LIMIT_MAKER
        assert adapter.placed[0].price is not None

    asyncio.run(body())


def test_session_loss_limit_stops_the_bot_flat():
    """User directive: with a $100 margin and a 5% stop, cumulative loss NET OF
    FEES reaching -$5 stops the bot. Enforced on the controller's own book —
    the live-session rail is structurally blind to spot."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("20"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_session_loss_limit_usd="5",
                target_volume_usd="1000000")
        await orch.spawn_controller(c)

        # Drive losing round trips until the limit trips.
        for _ in range(40):
            if c.completed:
                break
            buy = orch.get(c.buy_id)
            if buy is not None and not buy.is_terminated:
                adapter.fill_order(buy.order.id, price=Decimal("100"))
                await orch.tick_controller(c.id)
            sell = orch.get(c.sell_id) if c.sell_id else None
            if sell is not None and not sell.is_terminated:
                # Sell 0.5% below the buy: a real, deterministic per-cycle loss.
                adapter.fill_order(sell.order.id, price=Decimal("99.5"))
                await orch.tick_controller(c.id)

        assert c.completed is True
        assert c.stop_reason == "session_loss_limit"
        assert c.session_realized_pnl_usd <= Decimal("-5")
        # Stopped FLAT — the cycle closed before the check, so nothing is left.
        assert c.close_base_remaining == 0
        assert c.entry_base - c.sold_base <= 0

    asyncio.run(body())


def test_session_loss_limit_disabled_when_zero():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_session_loss_limit_usd="0")
        assert c._session_loss_breached() is False
        c.session_realized_pnl_usd = Decimal("-9999")
        assert c._session_loss_breached() is False, "0 disables the limit"

    asyncio.run(body())


def test_round_trip_fee_cost_is_reported():
    """With no profit gate every cycle costs both legs' fees by construction —
    surface it so the approaching stop is explicable."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_session_loss_limit_usd="5")
        m = c.volume_metrics()
        # 9bp venue + 1bp builder per leg = 20bp per round trip.
        assert abs(m["vol_round_trip_fee_bp"] - 20.0) < 1e-6
        assert m["vol_session_loss_limit_usd"] == 5.0

    asyncio.run(body())


def test_partial_buy_is_sold_immediately_not_stranded():
    """A taker buy that only partially fills before the venue closes it must
    round-trip what it actually holds — completing here would strand bought
    base in the user's wallet."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        half = buy.order.amount_base / Decimal(2)
        adapter.fill_order(buy.order.id, amount=half, price=Decimal("100"), partial=True)
        await adapter.cancel_order(buy.order.id)
        await orch.tick_controller(c.id)

        assert c.entry_base == half
        # v4.1: no hold — the partial is sold on the spot.
        assert c.phase == "pending_close_fill"
        sell = orch.get(c.sell_id)
        assert sell is not None
        assert sell.order.amount_base == half, "must sell exactly what is held"

        adapter.fill_order(sell.order.id, price=Decimal("101"))
        await orch.tick_controller(c.id)
        assert c.cycles_completed == 1
        assert c.close_base_remaining == 0, "no inventory may be left stranded"

    asyncio.run(body())


# --------------------------------------------------------------------------
# AUDIT-VOL-2026-07-31 regressions. The shared MockNadoAdapter quotes
# bid == ask == mid (a ZERO-spread book), which is exactly why none of the
# tests above could observe F1/F2 — both are spread-dependent. This adapter
# gives the book a real spread.
# --------------------------------------------------------------------------
class SpreadAdapter(MockNadoAdapter):
    """Mock book with a configurable spread around the mid."""

    def __init__(self, *a, spread_bp=Decimal("30"), **kw):
        super().__init__(*a, **kw)
        self.spread_bp = Decimal(str(spread_bp))

    async def order_book(self, trading_pair):
        from src.nadobro.engine.adapter.base import OrderBookLevel, OrderBookSnapshot
        import time as _t

        mid = self._current_mid()
        half = mid * self.spread_bp / Decimal(20000)
        return OrderBookSnapshot(
            trading_pair=trading_pair,
            bids=[OrderBookLevel(mid - half, Decimal(1))],
            asks=[OrderBookLevel(mid + half, Decimal(1))],
            timestamp=_t.time(),
        )


def test_F1_taker_orders_are_price_bounded_not_naked_market():
    """place_market_order is an IOC at 1% slippage the adapter never overrides,
    so a bare MARKET could fill ~99bp through the loss floor. Both legs must
    therefore be marketable LIMITs carrying a hard price bound."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch)
        await orch.spawn_controller(c)
        buy = adapter.placed[0]
        assert buy.order_type is not OrderType.MARKET, "naked MARKET is unbounded"
        assert buy.price is not None and buy.price > 0, "taker buy needs a price bound"

        adapter.fill_order(orch.get(c.buy_id).order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("102"))
        await orch.tick_controller(c.id)
        sell = adapter.placed[-1]
        assert sell.order_type is not OrderType.MARKET
        assert sell.price is not None and sell.price > 0

    asyncio.run(body())


def test_F4_unsold_inventory_stops_instead_of_buying_on_top():
    """Exhausting sell attempts on a REAL position must end the session, not
    zero entry_base and buy again — that accumulates orphaned spot the
    controller can no longer see."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="1000000")
        await orch.spawn_controller(c)
        adapter.fill_order(orch.get(c.buy_id).order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("102"))
        await orch.tick_controller(c.id)

        held = c.entry_base
        c.sell_attempts = c._MAX_SELL_ATTEMPTS      # attempts exhausted
        await adapter.cancel_order(orch.get(c.sell_id).order.id)
        await orch.tick_controller(c.id)

        assert c.completed is True
        assert c.stop_reason == "unsold_inventory"
        assert c.entry_base == held, "the bag must stay visible, not be zeroed"

    asyncio.run(body())


# --------------------------------------------------------------------------
# "Make sure no user position is left exposed" (user directive 2026-07-31).
# Every terminal path must leave the controller flat, or leave the held size
# VISIBLE so the stop-path spot sweep can size against it.
# --------------------------------------------------------------------------
def test_no_stop_path_leaves_inventory_invisible_to_the_sweep():
    """The stop sweep sizes from state via _volume_spot_managed_size. If the
    controller stops holding base, its metrics must expose that size —
    otherwise the sweep sells nothing and the user keeps the bag."""
    from src.nadobro.strategy.strategy_lifecycle import _volume_spot_managed_size

    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="1000000")
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        held = c.entry_base - c.sold_base
        assert held > 0, "precondition: the bot is holding base"

        # Whatever the phase, the metrics the runtime persists must let the
        # sweep size the close.
        state = dict(c.volume_metrics())
        managed = _volume_spot_managed_size(state)
        assert managed > 0, f"sweep would sell nothing while holding {held}"
        assert managed >= float(held) * 0.99

    asyncio.run(body())


def test_flat_between_cycles_so_nothing_is_exposed_at_the_stop():
    """v4.1 sells immediately, so the bot is FLAT between cycles. That is what
    makes stopping on the loss limit safe: the check runs after a completed
    round trip, with nothing left to close."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="1000000")
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)
        # The sell is already live in the same tick — no idle holding window.
        assert c.sell_id is not None
        assert c.phase == "pending_close_fill"

        sell = orch.get(c.sell_id)
        adapter.fill_order(sell.order.id, price=Decimal("99.9"))
        await orch.tick_controller(c.id)
        assert c.entry_base - c.sold_base <= 0, "flat after the round trip"
        assert c.close_base_remaining == 0

    asyncio.run(body())


# --------------------------------------------------------------------------
# TAKER-REST-STALL (found in self-review, 2026-07-31). A marketable LIMIT is
# NOT an IOC: if the book cannot fill it within the slippage bound the
# remainder RESTS. Taker mode disables the maker requote/cross rescues, so
# without a taker-specific timeout the leg sits forever holding a partial
# position — a stall AND an exposed position.
# --------------------------------------------------------------------------
def test_partially_swept_taker_buy_does_not_rest_forever():
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_taker_fill_timeout_seconds=5.0)
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        half = buy.order.amount_base / Decimal(2)
        # Thin book: only half sweeps, the rest RESTS (order stays OPEN).
        adapter.fill_order(buy.order.id, amount=half, price=Decimal("100"), partial=True)

        # Before the timeout the leg is legitimately still working.
        await orch.tick_controller(c.id)
        assert c.phase == "pending_fill"
        assert c.sell_id is None

        c.leg_quoted_ts -= 60.0          # timeout elapses
        await orch.tick_controller(c.id)

        assert c.sell_id is not None, "must stop resting and sell what it holds"
        assert c.phase == "pending_close_fill"
        sell = orch.get(c.sell_id)
        assert sell.order.amount_base == half

    asyncio.run(body())


def test_unfilled_taker_buy_reprices_with_taker_geometry():
    """A taker requote must re-place MARKETABLE, never post a passive maker
    bid — otherwise the rescue silently converts the strategy to maker."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("20"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_taker_fill_timeout_seconds=5.0)
        await orch.spawn_controller(c)
        first = adapter.placed[0]
        c.leg_quoted_ts -= 60.0          # nothing filled; timeout elapses
        await orch.tick_controller(c.id)

        assert len(adapter.placed) == 2, "must cancel and re-place"
        repriced = adapter.placed[-1]
        assert repriced.order_type is OrderType.LIMIT
        book = await adapter.order_book(PAIR)
        assert repriced.price >= book.best_ask, (
            "a taker re-place must still be marketable (at/through the ask), "
            f"got {repriced.price} vs ask {book.best_ask}"
        )
        assert c.entry_base == 0

    asyncio.run(body())


def test_resting_taker_sell_is_repriced_not_left_hanging():
    """An exit that cannot fully sweep must be re-placed at the fresh bid —
    leaving it resting leaves the user holding the unsold remainder."""
    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, vol_taker_fill_timeout_seconds=5.0,
                target_volume_usd="1000000")
        await orch.spawn_controller(c)
        adapter.fill_order(orch.get(c.buy_id).order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)
        assert c.phase == "pending_close_fill"
        first_sell_id = c.sell_id
        placed_before = len(adapter.placed)

        c.leg_quoted_ts -= 60.0          # the sell rests, timeout elapses
        await orch.tick_controller(c.id)

        assert len(adapter.placed) > placed_before, "the exit must be re-placed"
        assert c.sell_id != first_sell_id
        assert c.entry_base - c.sold_base > 0, "still holding, still working the exit"
        assert not c.completed

    asyncio.run(body())


# --------------------------------------------------------------------------
# VOL-STOP-RESET (self-review 2026-07-31). The 5%-of-margin stop is enforced
# against the CONTROLLER's in-memory cumulative PnL. A rebuild (recovery /
# worker handoff) reset it to 0 and restarted the whole loss budget — and the
# max_cycles cap with it — so a run could bleed several multiples of the
# user's stop. Same class of bug DN's restore_* keys already fixed.
# --------------------------------------------------------------------------
def test_rebuilt_controller_resumes_the_loss_budget():
    adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
    c = _vb(
        adapter, ExecutorOrchestrator(),
        vol_session_loss_limit_usd="5",
        restore_session_realized_pnl_usd=-4.80,
        restore_cycles_completed=37,
        restore_session_volume_usd=7400.0,
    )
    assert c.session_realized_pnl_usd == Decimal("-4.80"), "must resume, not restart"
    assert c.cycles_completed == 37
    assert c.session_volume_usd == Decimal("7400.0")
    # Still inside the budget...
    assert c._session_loss_breached() is False
    # ...and one more losing cycle trips it, instead of getting a fresh $5.
    c.session_realized_pnl_usd = Decimal("-5.01")
    assert c._session_loss_breached() is True


def test_fresh_controller_starts_from_zero():
    """A new run must never inherit a prior run's counters (the runs>0 guard in
    run_engine_cycle is what enforces this upstream)."""
    adapter = SpreadAdapter(mid=Decimal(100), auto_fill_market=False)
    c = _vb(adapter, ExecutorOrchestrator(), vol_session_loss_limit_usd="5")
    assert c.session_realized_pnl_usd == Decimal(0)
    assert c.cycles_completed == 0
    assert c.session_volume_usd == Decimal(0)


# --------------------------------------------------------------------------
# VOL-SWEEP-OVERREACH (self-review 2026-07-31). The stop sweep is sized from
# state. It used to fall back to vol_entry_size — the last cycle's BUY size,
# which _finish_cycle never resets — so a FLAT bot authorised a sweep capped
# at that size, and stop_volume_spot_cleanup's min(wallet, cap) would sell the
# user's OWN pre-existing spot. Selling a user's unrelated assets is
# irreversible; this is the most dangerous direction to get wrong.
# --------------------------------------------------------------------------
def test_flat_bot_authorises_no_sweep_at_all():
    from src.nadobro.strategy.strategy_lifecycle import _volume_spot_managed_size

    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="0")
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)
        sell = orch.get(c.sell_id)
        adapter.fill_order(sell.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        assert c.entry_base - c.sold_base == 0, "precondition: the bot is flat"
        state = dict(c.volume_metrics())
        assert state["vol_entry_size"] > 0, "the stale key is still populated"
        assert _volume_spot_managed_size(state) == 0.0, (
            "a flat bot must authorise NO sweep — otherwise min(wallet, cap) "
            "sells the user's own pre-existing spot balance"
        )

    asyncio.run(body())


def test_holding_bot_authorises_exactly_what_it_holds():
    from src.nadobro.strategy.strategy_lifecycle import _volume_spot_managed_size

    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="1000000")
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        held = float(c.entry_base - c.sold_base)
        assert held > 0
        assert _volume_spot_managed_size(dict(c.volume_metrics())) == held

    asyncio.run(body())


def test_unsold_inventory_stop_still_reports_what_is_held():
    """_complete("unsold_inventory") means base IS still held — the sweep must
    be able to size it, or the completion path strands the position."""
    from src.nadobro.strategy.strategy_lifecycle import _volume_spot_managed_size

    async def body():
        adapter = SpreadAdapter(mid=Decimal(100), spread_bp=Decimal("5"),
                                auto_fill_market=False)
        orch = ExecutorOrchestrator()
        c = _vb(adapter, orch, target_volume_usd="1000000")
        await orch.spawn_controller(c)
        adapter.fill_order(orch.get(c.buy_id).order.id, price=Decimal("100"))
        await orch.tick_controller(c.id)

        held = float(c.entry_base - c.sold_base)
        c.sell_attempts = c._MAX_SELL_ATTEMPTS
        await adapter.cancel_order(orch.get(c.sell_id).order.id)
        await orch.tick_controller(c.id)

        assert c.completed and c.stop_reason == "unsold_inventory"
        assert _volume_spot_managed_size(dict(c.volume_metrics())) == held, (
            "the completion sweep must know exactly what is still held"
        )

    asyncio.run(body())
