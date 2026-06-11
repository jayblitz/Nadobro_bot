"""Regime gate + inventory cap + ATR auto-spread (2026-06 grid upgrade).

Verifies the layered defense from the nine-run grid post-mortem:
1. regime_gate routine: QUOTE in compressed ranges; PAUSE on trend /
   breakout / expansion; fail-open on insufficient history.
2. Controllers: MM quotes reduce-only while paused; grid defers arming
   into a trend (and never re-arms while paused); dgrid sits out.
3. Pause NEVER touches exits: close legs keep placing while paused.
4. Inventory cap: worsening side suppressed at 30% of margin, hysteresis
   resume at 70% of the cap.
5. ATR auto-spread: tracks k x ATR/2 within [floor, cap].
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.dynamic_grid import DynamicGridController
from src.nadobro.engine.controllers.grid_trading import GridController
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.executors.grid_executor import (
    GridExecutor,
    GridExecutorConfig,
    GridLevelState,
)
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.routines import regime_gate
from src.nadobro.engine.types import TradeType

PAIR = "BTC-PERP"


# --------------------------------------------------------------------------
# Candle builders
# --------------------------------------------------------------------------
def ranging_candles(n: int = 80, base: float = 100.0) -> list[dict]:
    """Tight oscillation around ``base`` with volume concentrated there."""
    out = []
    for i in range(n):
        wiggle = 0.05 if i % 2 == 0 else -0.05
        close = base + wiggle
        out.append({
            "open": base, "high": close + 0.05, "low": close - 0.05,
            "close": close, "volume": 1000.0,
        })
    return out


def trending_candles(n: int = 80, base: float = 100.0, step: float = 0.4) -> list[dict]:
    out = []
    price = base
    for _ in range(n):
        nxt = price + step
        out.append({
            "open": price, "high": nxt + 0.05, "low": price - 0.05,
            "close": nxt, "volume": 1000.0,
        })
        price = nxt
    return out


def breakout_candles(n: int = 80, base: float = 100.0) -> list[dict]:
    """Ranging EMAs, but the LAST close escapes the value area."""
    out = ranging_candles(n - 1, base)
    out.append({
        "open": base, "high": base + 3.0, "low": base,
        "close": base + 3.0, "volume": 50.0,  # thin volume out of acceptance
    })
    return out


# --------------------------------------------------------------------------
# 1. The routine
# --------------------------------------------------------------------------
def test_gate_quotes_in_compressed_range():
    result = asyncio.run(regime_gate.run(PAIR, ranging_candles()))
    assert result["verdict"] == regime_gate.QUOTE
    assert result["gate_active"] is True


def test_gate_pauses_on_trend_up_and_down():
    up = asyncio.run(regime_gate.run(PAIR, trending_candles(step=0.4)))
    assert up["verdict"] == regime_gate.PAUSE and up["reason"] == "trending_up"
    down = asyncio.run(regime_gate.run(PAIR, trending_candles(step=-0.4)))
    assert down["verdict"] == regime_gate.PAUSE and down["reason"] == "trending_down"


def test_gate_pauses_on_breakout_from_value_area():
    result = asyncio.run(regime_gate.run(PAIR, breakout_candles()))
    assert result["verdict"] == regime_gate.PAUSE
    assert result["reason"] in ("breakout", "expansion", "trending_up")


def test_gate_fails_open_on_insufficient_history():
    result = asyncio.run(regime_gate.run(PAIR, ranging_candles(10)))
    assert result["verdict"] == regime_gate.QUOTE
    assert result["gate_active"] is False
    assert result["reason"] == "insufficient_history"


def test_volume_profile_compression_vs_smear():
    compressed = regime_gate.volume_profile(ranging_candles())
    smeared = regime_gate.volume_profile(
        [{"open": 100 + i, "high": 101 + i, "low": 99 + i,
          "close": 100 + i, "volume": 1000.0} for i in range(40)]
    )
    assert compressed["va_range_frac"] < smeared["va_range_frac"]


# --------------------------------------------------------------------------
# 2 + 3. MM controller behavior under the gate
# --------------------------------------------------------------------------
def _mm(adapter, candles, inventory=None, extra=None):
    configs = {
        "trading_pair": PAIR,
        "spread_bid_pct": Decimal("0.001"),
        "spread_ask_pct": Decimal("0.001"),
        "order_amount_quote": Decimal(10),
        "regime_gate_enabled": True,
        "candle_provider": lambda _pair: candles,
    }
    configs.update(extra or {})
    orch = ExecutorOrchestrator()
    c = MarketMakingController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=inventory or InventoryRepository(),
        configs=configs, controller_id="MM",
    )
    return orch, c


def test_mm_paused_flat_places_no_quotes_and_emits_event():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _mm(adapter, trending_candles())
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert adapter.placed == [], "paused + flat book must quote nothing"
        event = c.consume_gate_event()
        assert event == {"state": "PAUSE", "reason": "trending_up"}
        assert c.consume_gate_event() is None, "event must be consumed once"

    asyncio.run(body())


def test_mm_paused_with_long_inventory_quotes_only_the_exit_side():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        inv = InventoryRepository()
        # Seed a LONG: pause must keep the SELL (reducing) quote only.
        inv.apply_fill(1, PAIR, "MM", TradeType.BUY, Decimal("1"), Decimal("100"), Decimal(0), 0.0)
        orch, c = _mm(adapter, trending_candles(), inventory=inv)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        sides = {o.side for o in adapter.placed}
        assert sides == {TradeType.SELL}, f"pause is stop-digging, not flatten: {sides}"

    asyncio.run(body())


def test_mm_quotes_both_sides_in_ranging_market():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _mm(adapter, ranging_candles())
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert {o.side for o in adapter.placed} == {TradeType.BUY, TradeType.SELL}

    asyncio.run(body())


# --------------------------------------------------------------------------
# 4. Inventory cap with hysteresis
# --------------------------------------------------------------------------
def test_inventory_cap_suppresses_worsening_side_with_hysteresis():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        inv = InventoryRepository()
        orch, c = _mm(
            adapter, ranging_candles(), inventory=inv,
            extra={"max_net_exposure_pct": 30, "margin_quote": Decimal(100)},
        )
        await orch.spawn_controller(c)
        # cap = 30% of $100 = $30 net. Long $40 -> buys suppressed.
        inv.apply_fill(1, PAIR, "MM", TradeType.BUY, Decimal("0.4"), Decimal("40"), Decimal(0), 0.0)
        allowed = c.exposure_allowed_sides(PAIR, Decimal(100))
        assert allowed == {"buy": False, "sell": True}
        # Trim to $25 net (between resume $21 and cap $30): STILL suppressed.
        inv.apply_fill(1, PAIR, "MM", TradeType.SELL, Decimal("0.15"), Decimal("15"), Decimal(0), 0.0)
        allowed = c.exposure_allowed_sides(PAIR, Decimal(100))
        assert allowed == {"buy": False, "sell": True}, "hysteresis must hold below cap"
        # Trim to $20 net (under resume): released.
        inv.apply_fill(1, PAIR, "MM", TradeType.SELL, Decimal("0.05"), Decimal("5"), Decimal(0), 0.0)
        allowed = c.exposure_allowed_sides(PAIR, Decimal(100))
        assert allowed == {"buy": True, "sell": True}

    asyncio.run(body())


# --------------------------------------------------------------------------
# 5. ATR auto-spread
# --------------------------------------------------------------------------
def test_mm_auto_spread_tracks_atr_within_floor_and_cap():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _mm(
            adapter, ranging_candles(),
            extra={"auto_spread": True, "auto_spread_k": Decimal("1.5")},
        )
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        # Spread followed the formula: clamp(ATR x k / 2, floor, cap) — and
        # moved off the static 0.001 the config started with.
        assert c.gate_atr_pct > 0
        expected = max(
            c.spread_floor_half_pct,
            min(Decimal(str(c.gate_atr_pct)) * c.auto_spread_k / Decimal(2),
                c.spread_cap_half_pct),
        )
        assert c.spread_bid_pct == c.spread_ask_pct == expected
        assert c.spread_floor_half_pct <= c.spread_bid_pct <= c.spread_cap_half_pct

    asyncio.run(body())


def test_mm_auto_spread_never_quotes_below_fee_floor():
    async def body():
        # Near-zero volatility: clamp must hold the spread AT the fee floor —
        # quoting tighter than fees pays to trade.
        flat = [{"open": 100.0, "high": 100.001, "low": 99.999,
                 "close": 100.0, "volume": 1000.0} for _ in range(80)]
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _mm(adapter, flat, extra={"auto_spread": True})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.spread_bid_pct == c.spread_ask_pct == c.spread_floor_half_pct

    asyncio.run(body())


# --------------------------------------------------------------------------
# Grid family: defer arming, suppress entries, dgrid sit-out
# --------------------------------------------------------------------------
def _grid_configs(candles):
    return {
        "trading_pair": PAIR,
        "start_price": Decimal("99"),
        "end_price": Decimal("100"),
        "limit_price": Decimal("95"),
        "total_amount_quote": Decimal(100),
        "min_spread_between_orders": Decimal("0.002"),
        "max_open_orders": 3,
        "regime_gate_enabled": True,
        "candle_provider": lambda _pair: candles,
    }


def test_grid_defers_arming_into_a_trend_then_arms_on_range():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("99.5"), auto_fill_market=False)
        candles_box = {"data": trending_candles()}
        orch = ExecutorOrchestrator()
        c = GridController(
            user_id=1, orchestrator=orch, adapter=adapter,
            inventory=InventoryRepository(),
            configs=_grid_configs(lambda: None) | {"candle_provider": lambda _p: candles_box["data"]},
            controller_id="G",
        )
        await orch.spawn_controller(c)
        assert c.my_executors() == [], "must not arm a grid into a trend"
        await orch.tick_controller(c.id)
        assert c.my_executors() == [], "must not re-arm while trending"
        # Range returns -> resume needs gate_resume_confirm_ticks (2)
        # consecutive QUOTE verdicts (anti-flap hysteresis), then arms.
        candles_box["data"] = ranging_candles()
        await orch.tick_controller(c.id)
        assert c.my_executors() == [], "one QUOTE verdict must not re-arm yet"
        await orch.tick_controller(c.id)
        assert len(c.my_executors()) == 1

    asyncio.run(body())


def test_paused_grid_suppresses_new_entries_but_close_legs_continue():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal("99.5"), auto_fill_market=False)
        cfg = GridExecutorConfig(
            trading_pair=PAIR, side=TradeType.BUY,
            start_price=Decimal("99"), end_price=Decimal("100"),
            limit_price=Decimal("95"), total_amount_quote=Decimal(100),
            min_spread_between_orders=Decimal("0.002"), max_open_orders=2,
        )
        ex = GridExecutor(cfg, user_id=1, controller_id="G", adapter=adapter,
                          inventory=InventoryRepository())
        orch = ExecutorOrchestrator()
        await orch.spawn(ex)
        await orch.tick(ex.id)
        opens = [lv for lv in ex.levels if lv.state is GridLevelState.OPEN_ORDER_PLACED]
        assert opens, "grid placed entry orders while ungated"
        # Fill one entry, then suppress (gate pause): the CLOSE leg must
        # still be placed — pause never strands an open position.
        adapter.fill_order(opens[0].open_order_id)
        ex.suppress_new_entries = True
        before = len([lv for lv in ex.levels if lv.state is GridLevelState.OPEN_ORDER_PLACED])
        await orch.tick(ex.id)
        assert opens[0].state is GridLevelState.CLOSE_ORDER_PLACED, "exit leg must run while paused"
        after = len([lv for lv in ex.levels if lv.state is GridLevelState.OPEN_ORDER_PLACED])
        assert after <= before, "no NEW entries while suppressed"

    asyncio.run(body())


def _dgrid(adapter, candle_box):
    orch = ExecutorOrchestrator()
    c = DynamicGridController(
        user_id=1, orchestrator=orch, adapter=adapter,
        inventory=InventoryRepository(),
        configs={
            "trading_pair": PAIR,
            "total_amount_quote": Decimal(100),
            "min_spread_between_orders": Decimal("0.002"),
            "start_price": Decimal("99"), "end_price": Decimal("100"),
            "limit_price": Decimal(0),
            "step_pct": Decimal("0.002"), "levels_count": 3,
            "regime_gate_enabled": True,
            "candle_provider": lambda _p: candle_box["data"],
        },
        controller_id="DG",
    )
    return orch, c


def test_dgrid_trades_a_trend_with_reverse_grid():
    # The gate must NOT pre-empt dgrid's defining behavior: TRENDING_DOWN
    # spawns a ReverseGrid (pause_on_trend=False for dgrid).
    from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor

    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        orch, c = _dgrid(adapter, {"data": trending_candles(step=-0.4)})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        executors = c.my_executors()
        assert len(executors) == 1, "dgrid must trade a downtrend"
        assert isinstance(executors[0], ReverseGridExecutor)

    asyncio.run(body())


def expansion_candles(n: int = 80, base: float = 100.0) -> list[dict]:
    """Flat EMAs (alternating closes) but volume smeared across a WIDE range:
    no acceptance anywhere — the chaos dgrid must sit out, NOT a trend."""
    out = []
    for i in range(n):
        px = base * (1.02 if i % 2 == 0 else 0.98)
        out.append({
            "open": base, "high": px + 0.05, "low": px - 0.05,
            "close": px, "volume": 1000.0,
        })
    return out


def test_dgrid_sits_out_expansion_chaos():
    # Expansion (price accepted nowhere) is NOT a tradeable trend: the third
    # state — sit out, no re-arm, retry next tick. (A clean trend, by
    # contrast, spawns a ReverseGrid — see the test above.)
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        box = {"data": expansion_candles()}
        orch, c = _dgrid(adapter, box)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.my_executors() == [], (
            f"expected sit-out, gate={c.gate_verdict}/{c.gate_reason}"
        )
        assert c.gate_reason == "expansion"

    asyncio.run(body())


def test_gate_resume_requires_consecutive_quote_verdicts():
    # PAUSE commits immediately (protection first); resuming needs
    # gate_resume_confirm_ticks consecutive QUOTE verdicts so a regime
    # flickering at the threshold can't churn quotes or spam notifications.
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=False)
        box = {"data": trending_candles()}
        orch, c = _mm(adapter, None, extra={"candle_provider": lambda _p: box["data"]})
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c.gate_paused and c.consume_gate_event() == {"state": "PAUSE", "reason": "trending_up"}
        # One ranging evaluation: NOT enough to resume.
        box["data"] = ranging_candles()
        await orch.tick_controller(c.id)
        assert c.gate_paused, "single QUOTE verdict must not resume"
        assert c.consume_gate_event() is None
        # Second consecutive ranging evaluation: resume + one event.
        await orch.tick_controller(c.id)
        assert not c.gate_paused
        assert c.consume_gate_event() == {"state": "QUOTE", "reason": ""}

    asyncio.run(body())
