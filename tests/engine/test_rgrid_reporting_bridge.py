"""R-Grid → legacy reporting bridge (trades_<network> / strategy_sessions).

CLAUDE.md: "Engine fills are bridged into the legacy trades_<network> /
strategy_sessions tables so /status, fills, and portfolio views show real numbers
— keep that bridge in sync when touching fill handling."

R-Grid moved to its OWN controller and its OWN executor class
(RGridTakerExecutor), so this pins that the bridge is class-agnostic and that
every kind of R-Grid fill — the break entry, the trailing exit and the reversal
flatten — still reaches the recorder with a digest, a side and a size.

The venue reports NO per-fill realized PnL (realized_pnl_x18 is always 0), so all
attribution comes from these rows. A fill that misses the bridge is invisible
volume and corrupt PnL.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.rgrid import RGridController
from src.nadobro.engine.executors.rgrid_taker_executor import RGridTakerExecutor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import OrderType, TradeType

PAIR = "BTC-PERP"
SPREAD = Decimal("0.001")
CID = "rgrid:4242:mainnet"


class _SpyRecorder:
    """Stands in for DbTradeRecorder. Same call shape, no DB."""

    def __init__(self):
        self.rows: list[dict] = []
        self.placements: list[tuple[str, str]] = []

    def record(self, controller_id, trading_pair, side, amount_base, price,
               fee_quote, order_id=None, timestamp=None, *,
               realized_pnl=None, is_taker=False):
        self.rows.append({
            "controller_id": controller_id, "trading_pair": trading_pair,
            "side": side, "amount_base": amount_base, "price": price,
            "fee_quote": fee_quote, "order_digest": order_id,
            "is_taker": is_taker,
        })

    def link_placement(self, controller_id, order_id):
        self.placements.append((controller_id, order_id))


def _controller(adapter, recorder, extra=None):
    configs = {
        "trading_pair": PAIR,
        "spread_bid_pct": SPREAD,
        "spread_ask_pct": SPREAD,
        "order_amount_quote": Decimal(10),
        "price_distance_tolerance": Decimal("0.0001"),
    }
    configs.update(extra or {})
    orch = ExecutorOrchestrator(trade_recorder=recorder)
    c = RGridController(
        user_id=4242, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(), configs=configs, controller_id=CID,
    )
    return orch, c


def test_a_break_entry_reaches_the_reporting_bridge():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        rec = _SpyRecorder()
        orch, c = _controller(adapter, rec)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)          # break up -> taker BUY
        await orch.tick_controller(c.id)          # settle

        assert rec.rows, "the entry fill never reached the bridge"
        row = rec.rows[0]
        assert row["controller_id"] == CID, "session resolution keys off this"
        assert row["trading_pair"] == PAIR
        assert row["side"] is TradeType.BUY
        assert Decimal(str(row["amount_base"])) > 0
        assert row["order_digest"], "no digest — the venue match cannot reconcile"

    asyncio.run(body())


def test_the_trailing_exit_reaches_the_bridge_too():
    """An unrecorded close is the phantom-History / missing-close-volume defect:
    the open is counted and the close is not, so the session shows a position it
    no longer holds and undercounts volume."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        rec = _SpyRecorder()
        orch, c = _controller(adapter, rec, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        entries = len(rec.rows)
        assert entries >= 1

        adapter.set_mid(Decimal("108"))
        await orch.tick_controller(c.id)          # arms the trail
        adapter.set_mid(Decimal("107"))
        await orch.tick_controller(c.id)          # trailing exit fires
        await orch.tick_controller(c.id)          # settle

        assert len(rec.rows) > entries, "the trailing exit never reached the bridge"
        assert rec.rows[-1]["side"] is TradeType.SELL
        assert rec.rows[-1]["order_digest"]

    asyncio.run(body())


def test_the_reversal_flatten_reaches_the_bridge():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        rec = _SpyRecorder()
        orch, c = _controller(adapter, rec)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("104"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        entries = len(rec.rows)

        adapter.set_mid(Decimal("100"))           # break against the long
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        assert len(rec.rows) > entries, "the reversal flatten never reached the bridge"
        assert rec.rows[-1]["side"] is TradeType.SELL

    asyncio.run(body())


def test_every_rgrid_fill_is_recorded_as_a_taker():
    """R-Grid crosses on BOTH legs. The bridge defaulted is_taker to False and
    nothing downstream corrected it, so a taker-only strategy reported 100% maker
    fills — wrong in every fee/maker-taker surface."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        rec = _SpyRecorder()
        orch, c = _controller(adapter, rec, extra={
            "reset_threshold_pct": Decimal("0.01"), "trail_enabled": True,
        })
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        for px in ("101", "108", "107"):
            adapter.set_mid(Decimal(px))
            await orch.tick_controller(c.id)
            await orch.tick_controller(c.id)

        assert rec.rows, "expected fills"
        assert all(r["is_taker"] is True for r in rec.rows), (
            "a taker-only strategy reported maker fills"
        )
        # And the orders really were market orders.
        assert all(o.order_type is OrderType.MARKET for o in adapter.placed)

    asyncio.run(body())


def test_the_recorder_is_injected_into_the_new_executor_class():
    """The orchestrator injects the recorder at spawn. RGridTakerExecutor is a new
    class; if injection were class-gated, R-Grid would go dark on every surface."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        rec = _SpyRecorder()
        orch, c = _controller(adapter, rec)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)

        takers = [e for e in c.my_executors(active_only=False)
                  if isinstance(e, RGridTakerExecutor)]
        assert takers, "expected an RGridTakerExecutor"
        assert all(e.trade_recorder is rec for e in takers), (
            "the recorder was not injected into R-Grid's executor"
        )

    asyncio.run(body())


def test_placement_linking_is_wired_for_rgrids_controller_id():
    """The on_place hook writes the digest->session intent BEFORE any fill, so
    nado_sync attributes the venue match to the session even if the executor's own
    fill detection misses it. It must parse R-Grid's controller id."""
    from src.nadobro.trading.engine_persistence import _parse_controller_id
    from src.nadobro.strategy.engine_runtime import deterministic_controller_id

    cid = deterministic_controller_id("rgrid", 4242, "mainnet")
    assert cid == CID
    assert _parse_controller_id(cid) == ("rgrid", 4242, "mainnet")


def test_the_bridge_survives_a_recorder_that_raises():
    """A reporting failure must never lose a fill or break execution."""
    async def body():
        class _Broken(_SpyRecorder):
            def record(self, *a, **k):
                raise RuntimeError("db down")

        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _controller(adapter, _Broken())
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        adapter.set_mid(Decimal("101"))
        await orch.tick_controller(c.id)
        await orch.tick_controller(c.id)
        # The fill still landed in inventory even though reporting blew up.
        assert c._net_base() > 0
        assert c._has_fills()

    asyncio.run(body())
