"""DN-DOUBLE-OPEN — a hedge must never be opened twice, and never left naked.

Reported 2026-07-28: "a user started a session, within a few minutes orders were
placed and closed but the spot position remains, leaving the user exposed."
Root-caused on production (Fly, app nadobro-bot) against DN session 167 —
user 5776741680, BTC, mainnet, 20:39:47 -> 20:43:05 UTC, stop_reason
``hedge_broken``, 3m18s against a ``hold_seconds`` of 3600.

The venue fills (trades_mainnet, session 167):

    20:39:49  KBTC      long  0.00155     <- spot buy #1   (eager kickoff)
    20:39:50  KBTC      long  0.00155     <- spot buy #2   (scheduled cycle)
    20:39:51  BTC-PERP  short 0.00155
    20:39:52  BTC-PERP  short 0.00155
    20:39:55  KBTC      short 0.00155     <- ONE spot sell
    20:39:57  BTC-PERP  long  0.00155
    20:41:30  BTC-PERP  long  0.00155     <- residual sweep

    net spot  +0.00155  (~$99 UNHEDGED)      net perp  0  (flat)

Four executors existed for one session, including a spot BUY created at 20:39:47
and EARLY_STOPped two seconds later with ``volume_quote`` 99.03 — one buy, never
sold. ``engine_started ... active_executors=2`` was logged TWICE, 1.65s apart.

Two independent defects:

1. THE RACE. ``_should_build_controller`` is a read-then-act check with no mutual
   exclusion, and bot_runtime's eager kickoff calls ``run_engine_cycle``
   directly — outside the ``_job_locks`` that serialize scheduled cycles. Both
   callers saw ``is_running`` False and both ran ``on_start``.

2. THE SWEEP WAS BLIND. ``_residuals_flat`` sources truth from
   ``_net_base`` = controller-scoped engine inventory. ``engine_position_hold``
   recorded BTC-USDT0 as 0.00155 bought / 0.00155 sold — FLAT — because the
   orphaned executor's fill never reached the shared hold. So the sweep saw the
   perp residual (0.0031/0.0031 was recorded correctly there) and swept it, and
   never saw the spot residual at all.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.delta_neutral import DeltaNeutralController, DNPhase
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import TradeType


_CFG = {
    # Session 167's shape: BTC spot long + BTC perp short, $99 a leg, 1h hold.
    "trading_pair_long": "BTC-USDT0",
    "trading_pair_short": "BTC-PERP",
    "trading_pair": "BTC-USDT0",
    "hedge_ratio": Decimal(1),
    "leg_amount_quote": Decimal(99),
    "hold_seconds": 3600,
    "cycles": 1,
    "leverage": 1,
}


def _controller(adapter, orch, inventory=None, **over):
    return DeltaNeutralController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=inventory if inventory is not None else InventoryRepository(),
        configs=dict(_CFG, **over), controller_id="dn:1:mainnet",
    )


def test_a_second_open_never_stacks_a_second_hedge():
    """The reported trigger: two on_start calls, one session.

    The second must NOT spawn a fresh pair — that is what orphaned the first
    spot leg and left the user holding unhedged BTC.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True)
        orch = ExecutorOrchestrator()
        c = _controller(adapter, orch)
        await orch.spawn_controller(c)          # start #1 -> opens the pair
        assert len(adapter.placed) >= 2, "the first open must place both legs"

        def _opens(side, pair):
            # Count against the ADAPTER (venue truth), not the engine inventory —
            # inventory is the thing that was wrong in production, so asserting
            # on it would hide exactly this bug.
            return [o for o in adapter.placed
                    if o.side is side and o.trading_pair == pair]

        spot_buys_before = len(_opens(TradeType.BUY, "BTC-USDT0"))
        perp_sells_before = len(_opens(TradeType.SELL, "BTC-PERP"))

        await c._open_cycle()                   # start #2, racing

        assert len(_opens(TradeType.BUY, "BTC-USDT0")) == spot_buys_before, (
            "a SECOND spot long was opened on top of the live hedge — this is "
            "production session 167: two spot buys, one sell, ~$99 left naked"
        )
        assert len(_opens(TradeType.SELL, "BTC-PERP")) == perp_sells_before, (
            "a second perp short was opened on top of the live hedge"
        )

    asyncio.run(body())


def test_open_onto_a_non_flat_book_flattens_instead_of_stacking():
    """Residue from any source (crash, worker handoff, restart) must be cleared,
    never built on top of."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True)
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        # Pre-seed an orphaned spot long, exactly like b12f54e1 left behind.
        inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.BUY,
                       Decimal("0.00155"), Decimal("63890"), Decimal(0))
        c = _controller(adapter, orch, inventory=inv)
        assert c._net_base("BTC-USDT0") != 0

        placed_before = len(adapter.placed)
        await c._open_cycle()

        events = [e["kind"] for e in c.consume_dn_events()]
        assert "duplicate_open_blocked" in events, (
            "opening onto a non-flat book must be refused and surfaced"
        )
        # It must not have opened a fresh long on top of the orphan.
        new_buys = [o for o in adapter.placed[placed_before:]
                    if o.side is TradeType.BUY and o.trading_pair == "BTC-USDT0"]
        assert not new_buys, "stacked a second spot long on top of live exposure"

    asyncio.run(body())


def test_the_orphaned_spot_leg_gets_swept_not_left_naked():
    """The user-facing damage: perp flat, spot still long.

    Once the residue is visible to the controller the sweep must flatten BOTH
    legs — a naked spot remainder is the failure the strategy exists to avoid.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True)
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.BUY,
                       Decimal("0.00155"), Decimal("63890"), Decimal(0))
        c = _controller(adapter, orch, inventory=inv)

        flat = await c._residuals_flat()
        assert flat is False, "a $99 spot remainder must not read as flat"
        sweeps = [o for o in adapter.placed
                  if o.trading_pair == "BTC-USDT0" and o.side is TradeType.SELL]
        assert sweeps, (
            "no sell was issued for the orphaned spot leg — this is exactly the "
            "reported exposure (perp swept, spot left long)"
        )

    asyncio.run(body())


def test_a_perp_only_residual_still_sweeps_perp():
    """Guard the path that DID work in production, so the fix doesn't regress it."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True)
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        inv.apply_fill(1, "BTC-PERP", "dn:1:mainnet", TradeType.SELL,
                       Decimal("0.00155"), Decimal("63890"), Decimal(0))
        c = _controller(adapter, orch, inventory=inv)

        assert await c._residuals_flat() is False
        sweeps = [o for o in adapter.placed
                  if o.trading_pair == "BTC-PERP" and o.side is TradeType.BUY]
        assert sweeps, "perp residual sweep regressed"

    asyncio.run(body())


def test_dust_is_still_ignored():
    """The cap must not turn rounding dust into an order storm."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True)
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        # ~$0.06 — well under the max($1, 0.2% of leg) tolerance.
        inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.BUY,
                       Decimal("0.000001"), Decimal("63890"), Decimal(0))
        c = _controller(adapter, orch, inventory=inv)
        assert await c._residuals_flat() is True
        assert not adapter.placed, "dust must not be swept"

    asyncio.run(body())


def test_a_clean_start_still_opens_both_legs():
    """The guard must not block the normal first open."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True)
        orch = ExecutorOrchestrator()
        c = _controller(adapter, orch)
        await orch.spawn_controller(c)

        pairs = {o.trading_pair for o in adapter.placed}
        assert "BTC-USDT0" in pairs and "BTC-PERP" in pairs, (
            "a clean DN start must open both legs"
        )
        assert c.phase is DNPhase.HOLDING
        assert c.hedge_broken is False

    asyncio.run(body())


# ── SPOT-RECONCILE: the venue is the authority on exposure ──────
# Session 167's engine hold recorded BTC-USDT0 as 0.00155 bought / 0.00155 sold
# — FLAT — while the venue had filled 0.0031 and the user held ~$99 of unhedged
# spot. `_residuals_flat` trusted that hold, so it swept the perp (whose hold
# happened to be right) and never saw the spot leg at all.


def _session_167_inventory():
    """The REAL engine_position_hold row from prod session 167:

        BTC-USDT0  buy_amount_base 0.00155   sell_amount_base 0.00155

    Net reads FLAT (which is why the sweep was blind) but the gross BUY is
    recorded — so the venue-vs-book correction is allowed to act on it without
    ever reaching coins this controller did not buy.
    """
    inv = InventoryRepository()
    inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.BUY,
                   Decimal("0.00155"), Decimal("63890"), Decimal(0))
    inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.SELL,
                   Decimal("0.00155"), Decimal("63890"), Decimal(0))
    return inv


def test_venue_exposure_is_swept_even_when_inventory_says_flat():
    """THE reported damage: inventory flat, venue long, nothing swept."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True,
                                  venue_held={"BTC-USDT0": Decimal("0.00155"),
                                              "BTC-PERP": Decimal(0)})
        orch = ExecutorOrchestrator()
        c = _controller(adapter, orch, inventory=_session_167_inventory())
        assert c._net_base("BTC-USDT0") == 0, "precondition: the book NETS to flat"

        assert await c._residuals_flat() is False, (
            "the venue holds 0.00155 spot — this must not read as flat"
        )
        sells = [o for o in adapter.placed
                 if o.trading_pair == "BTC-USDT0" and o.side is TradeType.SELL]
        assert sells, "the stranded spot leg was not swept"

    asyncio.run(body())


def test_an_unreadable_venue_falls_back_to_the_book_not_to_flat():
    """A failed read must never be interpreted as 'no exposure'."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True,
                                  venue_held={})       # None => unknown
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.BUY,
                       Decimal("0.00155"), Decimal("63890"), Decimal(0))
        c = _controller(adapter, orch, inventory=inv)

        assert await c._venue_net_base("BTC-USDT0") == Decimal("0.00155"), (
            "an unknown venue read must fall back to the book, not to zero"
        )

    asyncio.run(body())


def test_the_larger_magnitude_wins_when_venue_and_book_disagree():
    """Under-reporting exposure is what leaves a user naked."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"),
                                  venue_held={"BTC-USDT0": Decimal("0.0031")})
        orch = ExecutorOrchestrator()
        inv = InventoryRepository()
        inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.BUY,
                       Decimal("0.0031"), Decimal("63890"), Decimal(0))
        inv.apply_fill(1, "BTC-USDT0", "dn:1:mainnet", TradeType.SELL,
                       Decimal("0.00155"), Decimal("63890"), Decimal(0))
        c = _controller(adapter, orch, inventory=inv)
        # book nets 0.00155, venue says 0.0031, gross buys 0.0031 -> venue wins.
        assert await c._venue_net_base("BTC-USDT0") == Decimal("0.0031")

    asyncio.run(body())


def test_a_venue_stranded_leg_blocks_a_fresh_open():
    """Residue from a previous run must be cleared before a new hedge, even when
    this controller's book knows nothing about it."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True,
                                  venue_held={"BTC-USDT0": Decimal("0.00155")})
        orch = ExecutorOrchestrator()
        c = _controller(adapter, orch, inventory=_session_167_inventory())
        await c._open_cycle()
        assert "duplicate_open_blocked" in [e["kind"] for e in c.consume_dn_events()]

    asyncio.run(body())


def test_a_spot_balance_this_controller_never_bought_is_NEVER_sold():
    """AUDIT 2026-07-29 — the worst possible outcome of a venue-truth sweep.

    held_base() returns the subaccount's ENTIRE spot balance; there is no
    per-controller attribution on a spot balance. If the user holds kBTC bought
    manually or by another strategy, DN must not read it as its own residue and
    MARKET-SELL it. The venue figure is capped at this controller's gross buys.
    """
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True,
                                  venue_held={"BTC-USDT0": Decimal("0.5")})
        orch = ExecutorOrchestrator()
        c = _controller(adapter, orch)          # this controller bought NOTHING
        assert await c._venue_net_base("BTC-USDT0") == 0, (
            "claimed 0.5 BTC of someone else's spot as its own residue"
        )
        assert await c._residuals_flat() is True
        assert not [o for o in adapter.placed if o.side is TradeType.SELL], (
            "market-sold a balance this controller never bought"
        )

    asyncio.run(body())


def test_the_cap_limits_a_partial_orphan_to_our_own_buys():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("63890"), auto_fill_market=True,
                                  venue_held={"BTC-USDT0": Decimal("0.5")})
        orch = ExecutorOrchestrator()
        c = _controller(adapter, orch, inventory=_session_167_inventory())
        # We bought 0.00155 gross; the venue shows 0.5 (mostly the user's own).
        assert await c._venue_net_base("BTC-USDT0") == Decimal("0.00155")

    asyncio.run(body())
