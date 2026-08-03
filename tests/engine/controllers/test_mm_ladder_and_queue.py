"""Phase 0 (queue preservation) + Phase 2 (position scaling ladder).

Phase 0: on a one-tick book a quote cannot be improved, only re-queued at the
back — so every avoidable cancel is a direct cut to the fill rate. These tests
pin WHEN a resting quote is kept and, just as importantly, when it is not.

Phase 2: Mid and fill-anchored Grid both used to rest ONE order per side and
then wait in position. The ladder splits the side's deployment across levels;
the per-side TOTAL is unchanged, which is what keeps the geometric curve from
being a martingale.
"""
import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.fill_anchored import FillAnchoredQuotingController
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import TradeType


class BookAdapter(MockNadoAdapter):
    """Mock with a real two-sided book. Like the venue, the BBO INCLUDES our own
    resting orders — that reflection is what the queue rules must survive."""

    def __init__(self, bid, ask, *, tick="0.01", min_notional="0", **kw):
        super().__init__(mid=(Decimal(bid) + Decimal(ask)) / Decimal(2), **kw)
        self._bid, self._ask = Decimal(bid), Decimal(ask)
        self._tick = Decimal(tick)
        self._min_notional = Decimal(min_notional)

    def set_book(self, bid, ask):
        self._bid, self._ask = Decimal(bid), Decimal(ask)

    async def order_book(self, trading_pair):
        from src.nadobro.engine.adapter.base import OrderBookLevel, OrderBookSnapshot
        import time as _t
        return OrderBookSnapshot(
            trading_pair=trading_pair,
            bids=[OrderBookLevel(self._bid, Decimal(10))],
            asks=[OrderBookLevel(self._ask, Decimal(10))],
            timestamp=_t.time(),
        )

    async def mid_price(self, trading_pair):
        return (self._bid + self._ask) / Decimal(2)


TOUCH = {
    "trading_pair": "P", "quote_mode": "touch",
    "spread_bid_pct": "0.001", "spread_ask_pct": "0.001",
    "order_amount_quote": "1000", "price_distance_tolerance": "0.0005",
    "max_base_quote": "100000", "max_net_exposure_pct": None,
}


def _mm(adapter, cfg, inv=None, cls=MarketMakingController):
    orch = ExecutorOrchestrator()
    return orch, cls(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=inv or InventoryRepository(), configs=cfg,
    )


# ==========================================================================
# Phase 0 — queue preservation
# ==========================================================================
def test_quote_is_kept_when_it_is_the_touch_and_target_is_no_better():
    """The core win: our quote IS the best price and re-placing would only cost
    queue position. A cancel here is a pure loss of fill rate."""
    async def body():
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, dict(TOUCH))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # Our improved quotes are now the BBO (as the venue would report).
        adapter.set_book("100.01", "100.09")
        placed = len(adapter.placed)
        for _ in range(5):
            await orch.tick_controller(c.id)
        assert len(adapter.placed) == placed, "re-quoted despite holding the touch"
        assert adapter.cancelled == []

    asyncio.run(body())


def test_quote_is_replaced_once_we_are_behind_the_touch():
    """Queue position is worthless behind the touch — that quote will not fill."""
    async def body():
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, dict(TOUCH))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)          # bid 100.01
        adapter.set_book("100.05", "100.20")      # outbid by 4 ticks
        await orch.tick_controller(c.id)
        bids = [o for o in adapter.placed if o.side is TradeType.BUY]
        assert bids[-1].price > Decimal("100.01"), "stale bid behind the touch was kept"

    asyncio.run(body())


def test_quote_is_replaced_when_the_market_moves_in_our_favour():
    """Holding must not mean leaving money on the table: if the bid can back off
    and buy cheaper, the queue slot is worth less than the better price."""
    async def body():
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, dict(TOUCH))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)          # bid 100.01
        adapter.set_book("99.50", "99.60")        # market fell away
        await orch.tick_controller(c.id)
        bids = [o for o in adapter.placed if o.side is TradeType.BUY]
        assert bids[-1].price < Decimal("100.01"), "did not back off to a cheaper bid"

    asyncio.run(body())


def test_min_lifetime_holds_a_young_quote_but_never_a_disallowed_one():
    """The lifetime rule bounds churn — but it must NEVER delay a safety cancel
    (inventory ceiling, exposure cap, regime gate). Holding a stale quote costs
    a missed fill; holding a disallowed one is unmanaged exposure."""
    async def body():
        cfg = dict(TOUCH)
        cfg["min_quote_lifetime_s"] = "60"
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # Market moves in our favour — normally a re-quote; the young quote holds.
        adapter.set_book("99.50", "99.60")
        placed = len(adapter.placed)
        await orch.tick_controller(c.id)
        assert len(adapter.placed) == placed, "young quote was churned"

        # Same young quote, now disallowed: it must be cancelled immediately.
        c.max_base_quote = Decimal(0)             # inventory ceiling hit
        c.min_base_quote = Decimal("1e18")
        await orch.tick_controller(c.id)
        assert c._bid_id is None and c._ask_id is None, "safety cancel was delayed"

    asyncio.run(body())


def test_lifetime_expiry_releases_the_quote():
    async def body():
        cfg = dict(TOUCH)
        cfg["min_quote_lifetime_s"] = "60"
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, cfg)
        clock = [1000.0]
        c._now = lambda: clock[0]                  # type: ignore[method-assign]
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_book("99.50", "99.60")
        placed = len(adapter.placed)
        await orch.tick_controller(c.id)
        assert len(adapter.placed) == placed       # still inside the lifetime
        clock[0] += 61                             # lifetime elapsed
        await orch.tick_controller(c.id)
        assert len(adapter.placed) > placed, "quote never released after its lifetime"

    asyncio.run(body())


def test_mid_mode_is_unaffected_by_the_touch_rules():
    """Mid-mode ticks fetch no book, so the queue rules must not fire there —
    behaviour stays exactly as shipped."""
    async def body():
        cfg = dict(TOUCH)
        cfg["quote_mode"] = "mid"
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c._touch_bid is None and c._touch_ask is None
        assert not c._holds_queue_position(True, Decimal(99), Decimal(100))

    asyncio.run(body())


# ==========================================================================
# Phase 2 — the ladder
# ==========================================================================
def test_one_level_is_byte_identical_to_the_single_quote_behaviour():
    async def body():
        adapter = BookAdapter("100.00", "100.10")
        orch, c = _mm(adapter, dict(TOUCH, ladder_levels=1))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert len(orch.list(c.id, active_only=True)) == 2      # one bid, one ask
        assert sorted(o.price for o in adapter.placed) == [Decimal("100.01"), Decimal("100.09")]

    asyncio.run(body())


def test_ladder_rests_multiple_levels_per_side():
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        orch, c = _mm(adapter, dict(TOUCH, ladder_levels=4, ladder_step_bp="10"))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert len(orch.list(c.id, active_only=True)) == 8      # 4 per side
        bids = sorted(o.price for o in adapter.placed if o.side is TradeType.BUY)
        assert len(bids) == 4 and len(set(bids)) == 4, "levels collapsed onto one price"
        assert bids[0] < bids[-1] <= Decimal("100.01"), "bid levels must step DOWN"
        asks = sorted(o.price for o in adapter.placed if o.side is TradeType.SELL)
        assert asks[0] >= Decimal("100.09"), "ask levels must step UP"

    asyncio.run(body())


def test_ladder_total_equals_the_single_quote_total():
    """The property that makes this safe to turn on: laddering redistributes the
    side's deployment, it never adds to it."""
    async def body():
        totals = {}
        for levels in (1, 4):
            ad = BookAdapter("100.00", "100.10", min_notional="1")
            orch, c = _mm(ad, dict(TOUCH, ladder_levels=levels, ladder_step_bp="10"))
            await orch.spawn_controller(c)
            await orch.tick_controller(c.id)
            totals[levels] = sum(
                o.amount_base * o.price for o in ad.placed if o.side is TradeType.BUY
            )
        assert abs(totals[1] - totals[4]) < Decimal("0.01"), totals
        assert abs(totals[1] - Decimal(1000)) < Decimal("0.01")

    asyncio.run(body())


def test_levels_are_clamped_by_the_venue_minimum():
    """A level under the venue floor is rejected on arrival — the clamp is why
    the ``levels`` input silently died the first time it was tried."""
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="400")
        orch, c = _mm(adapter, dict(TOUCH, ladder_levels=8, ladder_step_bp="10"))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        bids = [o for o in adapter.placed if o.side is TradeType.BUY]
        assert len(bids) == 2, "$1000 / $400 floor allows exactly 2 levels"
        assert all(o.amount_base * o.price >= Decimal(400) for o in bids)

    asyncio.run(body())


def test_shrinking_the_level_count_retires_the_orphans():
    """A live edit that lowers the level count must cancel the levels it drops —
    otherwise they rest forever as orphan orders nothing tracks."""
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        orch, c = _mm(adapter, dict(TOUCH, ladder_levels=4, ladder_step_bp="10"))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert len(orch.list(c.id, active_only=True)) == 8
        c.ladder_levels = 1
        await orch.tick_controller(c.id)
        assert len(orch.list(c.id, active_only=True)) == 2
        assert c.ladder_metrics()["ladder_live_bids"] == 1

    asyncio.run(body())


def test_stop_all_quotes_clears_every_level_not_just_level_zero():
    """The live re-size path walks this; missing deep levels would strand them."""
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        orch, c = _mm(adapter, dict(TOUCH, ladder_levels=4, ladder_step_bp="10"))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert len(c.live_quote_ids()) == 8
        await c.stop_all_quotes()
        assert c.live_quote_ids() == []
        assert len(orch.list(c.id, active_only=True)) == 0

    asyncio.run(body())


def test_exposure_cap_counts_resting_levels_not_just_filled_inventory():
    """N levels must not each be waved through on the strength of the same
    headroom — but the level being REPLACED must not block itself either."""
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        cfg = dict(TOUCH, ladder_levels=4, ladder_step_bp="10",
                   margin_quote="1000", max_net_exposure_pct="50")
        orch, c = _mm(adapter, cfg)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # Cap floors at one full side deployment ($1000), so the whole ladder
        # fits — exactly the notional a single quote would have placed.
        bids = [o for o in adapter.placed if o.side is TradeType.BUY]
        assert len(bids) == 4
        # Re-reconciling a resting level must not count that level against itself.
        assert c._projected_order_within_exposure(
            TradeType.BUY, Decimal(100), Decimal(250), 0
        ), "a resting level blocked its own replacement (deadlock)"

    asyncio.run(body())


def test_size_curve_backloads_depth():
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        orch, c = _mm(adapter, dict(TOUCH, ladder_levels=4, ladder_step_bp="10",
                                    ladder_curve="geometric"))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        bids = sorted(
            (o for o in adapter.placed if o.side is TradeType.BUY),
            key=lambda o: o.price, reverse=True,      # nearest the touch first
        )
        sizes = [o.amount_base * o.price for o in bids]
        assert sizes[-1] > sizes[0] * 4, f"deep level not back-loaded: {sizes}"

    asyncio.run(body())


# ==========================================================================
# Phase 2 — fill-anchored Grid gets the same ladder
# ==========================================================================
def test_grid_ladders_around_the_fill_anchor():
    """Grid's complaint was the same as Mid's: one order, then wait in position.
    It inherits the ladder, anchored on the last fill instead of the touch."""
    async def body():
        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        cfg = {
            "trading_pair": "P", "anchor_mode": "grid",
            "spread_bid_pct": "0.001", "spread_ask_pct": "0.001",
            "order_amount_quote": "1000", "price_distance_tolerance": "0.0005",
            "ladder_levels": 3, "ladder_step_bp": "10",
        }
        orch, c = _mm(adapter, cfg, cls=FillAnchoredQuotingController)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        bids = sorted(o.price for o in adapter.placed if o.side is TradeType.BUY)
        asks = sorted(o.price for o in adapter.placed if o.side is TradeType.SELL)
        assert len(bids) == 3 and len(asks) == 3
        assert len(set(bids)) == 3, "grid levels collapsed onto one price"
        assert max(bids) < min(asks), "grid ladder crossed itself"
        total = sum(o.amount_base * o.price for o in adapter.placed if o.side is TradeType.BUY)
        assert abs(total - Decimal(1000)) < Decimal("0.01")

    asyncio.run(body())


# ==========================================================================
# Audit findings, 2026-08-03 (strategy-auditor on 4897dd7/410c20a/d0fe3df)
# ==========================================================================
def test_executor_budget_admits_the_whole_ladder_both_sides():
    """AUDIT-F1 (critical): max_open_executors was `levels + 2`, sized for the
    CLASSIC grid where ONE executor holds every level. The laddered quoters
    spawn one per level PER SIDE. Bids reconcile first, so from levels=3 up the
    deep ask rungs were rejected every tick with risk:max_open_executors — a
    permanent 2:1 long-skewed book with half the depth to unwind it."""
    from src.nadobro.strategy import engine_runtime as er

    for levels in (1, 2, 3, 4, 8):
        lim = er.map_risk_limits({"notional_usd": 100.0, "levels": levels}, "mid", leverage=1)
        assert lim.max_open_executors >= 2 * levels, (
            f"mid levels={levels}: budget {lim.max_open_executors} starves a {2*levels}-order ladder"
        )
        fa = er.map_risk_limits(
            {"notional_usd": 100.0, "levels": levels, "fill_anchored": 1}, "grid", leverage=1
        )
        assert fa.max_open_executors >= 2 * levels, f"fill-anchored grid levels={levels}"
    # The CLASSIC grid holds every level in one executor — it must NOT inflate.
    classic = er.map_risk_limits({"notional_usd": 100.0, "levels": 4}, "grid", leverage=1)
    assert classic.max_open_executors == 6


def test_both_sides_actually_get_their_full_ladder():
    """The end-to-end consequence of the budget fix."""
    async def body():
        from src.nadobro.engine.risk import RiskEngine
        from src.nadobro.strategy import engine_runtime as er

        adapter = BookAdapter("100.00", "100.10", min_notional="1")
        orch = ExecutorOrchestrator()
        orch.risk = RiskEngine(er.map_risk_limits(
            {"notional_usd": 1000.0, "levels": 4}, "mid", leverage=1))
        c = MarketMakingController(
            user_id=1, orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(),
            configs=dict(TOUCH, ladder_levels=4, ladder_step_bp="10"),
        )
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        m = c.ladder_metrics()
        assert m["ladder_live_bids"] == 4 and m["ladder_live_asks"] == 4, m

    asyncio.run(body())


def test_curve_never_puts_a_rung_under_the_venue_minimum():
    """AUDIT-F4 (money): max_levels answers 'how many EQUAL slices clear the
    floor', but a curve makes near rungs far smaller than that average. Rungs
    landed under the venue minimum and the venue CLIENT grows a sub-minimum
    non-reducing order before signing — so the side deployed MORE than the
    user's budget (measured +38% on geometric/8)."""
    from src.nadobro.quant.ladder import GEOMETRIC, LINEAR, ladder_notional, plan_ladder

    for curve in (LINEAR, GEOMETRIC):
        for want in (2, 4, 8, 12):
            plan = plan_ladder(100, levels=want, step_bp=1, curve=curve, min_notional=10)
            assert plan, (curve, want)
            assert min(lv.size_quote for lv in plan) >= Decimal(10), (
                f"{curve}/{want}: rung under the venue floor -> client bumps it -> over-deployment"
            )
            assert ladder_notional(plan) == Decimal(100), "sizes must still sum exactly"
