"""Fill-anchored quoting controller (Phase 4 — TreadFi Grid/RGrid semantics).

Pins the four behaviors that define the mode:
1. The reference price re-anchors to the LAST FILL (grid mode).
2. The no-cross invariant: never buy above the last sell / sell below the
   last buy — every round trip must capture spread.
3. Soft reset: when mid drifts past reset_threshold_pct from the reference,
   quotes re-anchor to mid (no flatten, no restart).
4. RGrid mode references the exposure VWAP of recent fills.
Plus the engine_runtime opt-in mapping (controller_override + TreadFi
default reset thresholds).
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.fill_anchored import FillAnchoredQuotingController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import TradeType

PAIR = "BTC-PERP"
SPREAD = Decimal("0.001")


def _controller(adapter, mode="grid", extra=None):
    configs = {
        "trading_pair": PAIR,
        "anchor_mode": mode,
        "spread_bid_pct": SPREAD,
        "spread_ask_pct": SPREAD,
        "order_amount_quote": Decimal(10),
        "price_distance_tolerance": Decimal("0.0001"),
    }
    configs.update(extra or {})
    orch = ExecutorOrchestrator()
    c = FillAnchoredQuotingController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(), configs=configs, controller_id="FA",
    )
    return orch, c


def _quotes(adapter):
    """Latest live (bid, ask) prices from the adapter's order book."""
    bids = [o for o in adapter.placed if o.side is TradeType.BUY]
    asks = [o for o in adapter.placed if o.side is TradeType.SELL]
    return (bids[-1].price if bids else None, asks[-1].price if asks else None)


def test_reference_reanchors_to_last_fill():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        bid, ask = _quotes(adapter)
        assert bid == Decimal("99.9") and ask == Decimal("100.1"), "first pair quotes around mid"

        # The bid fills at 99.9 -> reference must move there.
        bid_order = [o for o in adapter.placed if o.side is TradeType.BUY][-1]
        adapter.fill_order(bid_order.id)
        await orch.tick_controller(c.id)
        state = c.anchor_state()
        assert state["reference"] == Decimal("99.9")
        assert state["last_buy_px"] == Decimal("99.9")
        _, new_ask = _quotes(adapter)
        # Ask re-anchored to the fill: 99.9 * 1.001 (also satisfies no-cross).
        assert new_ask == Decimal("99.9") * (1 + SPREAD)

    asyncio.run(body())


def test_no_cross_invariant_clamps_quotes():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _controller(adapter)
        await orch.spawn_controller(c)
        # Book history: we last SOLD at 100 and last BOUGHT at 100.
        c._last_sell_px = Decimal(100)
        c._last_buy_px = Decimal(100)
        # Reference drifts up (e.g. a later fill at 100.2 within threshold).
        c._reference = Decimal("100.2")
        await orch.tick_controller(c.id)
        bid, ask = _quotes(adapter)
        # Bid must NOT exceed last_sell * (1 - spread): never buy above where
        # we sold. Ref-anchored bid would be 100.2*0.999 = 100.0998 — clamped.
        assert bid == Decimal(100) * (1 - SPREAD)
        # Ask >= last_buy * (1 + spread): never sell below where we bought.
        assert ask >= Decimal(100) * (1 + SPREAD)

    asyncio.run(body())


def test_soft_reset_reanchors_to_mid_beyond_threshold():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _controller(adapter, extra={"reset_threshold_pct": Decimal("0.0025")})
        await orch.spawn_controller(c)
        c._reference = Decimal(100)
        # Price runs 0.5% away — beyond the 0.25% threshold.
        adapter.set_mid(Decimal("100.5"))
        await orch.tick_controller(c.id)
        bid, _ = _quotes(adapter)
        # Quotes re-anchored to MID (100.5), not the stranded reference (100).
        assert bid == Decimal("100.5") * (1 - SPREAD)
        # Within the threshold the reference stays in charge.
        adapter.set_mid(Decimal("100.1"))
        c._reference = Decimal("100.05")
        await orch.tick_controller(c.id)
        bid2, _ = _quotes(adapter)
        assert bid2 == Decimal("100.05") * (1 - SPREAD)

    asyncio.run(body())


def test_rgrid_mode_uses_exposure_vwap():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(101), auto_fill_market=False)
        orch, c = _controller(adapter, mode="rgrid")
        await orch.spawn_controller(c)
        c._fills.append((Decimal(100), Decimal(1)))
        c._fills.append((Decimal(102), Decimal(1)))
        assert c.anchor_state()["exposure_vwap"] == Decimal(101)
        await orch.tick_controller(c.id)
        bid, ask = _quotes(adapter)
        assert bid == Decimal(101) * (1 - SPREAD)
        assert ask == Decimal(101) * (1 + SPREAD)
        # rgrid follows the trend: NO no-cross clamping. In grid mode a
        # last_buy at 150 would force the ask up to >=150.15 (a requote);
        # rgrid leaves the vwap-anchored ask resting untouched.
        c._last_buy_px = Decimal(150)
        await orch.tick_controller(c.id)
        _, ask2 = _quotes(adapter)
        assert ask2 == Decimal(101) * (1 + SPREAD)
        assert all(
            o.price < Decimal(150) for o in adapter.placed if o.side is TradeType.SELL
        ), "no clamped requote may appear in rgrid mode"

    asyncio.run(body())


def test_runtime_opt_in_maps_override_and_treadfi_defaults():
    from src.nadobro.services.engine_runtime import map_strategy_config

    grid_cfg = map_strategy_config(
        "grid", {"fill_anchored": 1, "notional_usd": 100.0, "levels": 2},
        Decimal(100), product=PAIR,
    )
    assert grid_cfg["controller_override"] == "fill_anchored"
    assert grid_cfg["anchor_mode"] == "grid"
    assert grid_cfg["reset_threshold_pct"] == Decimal("0.0025")   # TreadFi 0.25%

    rgrid_cfg = map_strategy_config(
        "rgrid", {"fill_anchored": 1, "notional_usd": 100.0, "levels": 2},
        Decimal(100), product=PAIR,
    )
    assert rgrid_cfg["anchor_mode"] == "rgrid"
    assert rgrid_cfg["reset_threshold_pct"] == Decimal("0.00125")  # TreadFi 0.125%

    # Without the opt-in, the classic ladder mapping is unchanged.
    ladder_cfg = map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 2}, Decimal(100), product=PAIR,
    )
    assert "controller_override" not in ladder_cfg
    assert "start_price" in ladder_cfg
