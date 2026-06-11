"""Gated vs ungated MM over a trending and a ranging week (simulation).

The evidence the nine-run grid post-mortem asks for before sizing the gate
up on mainnet: the SAME market-making configuration run over the SAME price
paths, once with the regime gate and once without.

Deterministic event loop: each step moves mid along the path, ticks the
controller, then fills every resting maker quote the move crossed (buy
fills when price trades at/below the bid, sell when at/above the ask).
PnL = inventory realized + unrealized at the final mark.

Expected (and asserted):
- Trending week: the UNGATED bot keeps buying into the fall and finishes
  deeply negative; the GATED bot pauses and finishes near flat — the gate
  must recover the bulk of the trend loss.
- Ranging week: the gate must NOT cost meaningful PnL (it quotes through
  the chop just like the ungated bot).
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.adapter.base import OrderState
from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import TradeType

PAIR = "BTC-PERP"
WINDOW = 80  # candles visible to the gate (>= ema_slow 50)


def trending_down_path(steps: int = 240, start: float = 100.0, step_pct: float = -0.001) -> list[float]:
    out, px = [], start
    for _ in range(steps):
        px *= 1 + step_pct
        out.append(px)
    return out


def ranging_path(steps: int = 240, base: float = 100.0, amp_pct: float = 0.004) -> list[float]:
    # Deterministic triangle wave around base — classic grid-friendly chop.
    out = []
    for i in range(steps):
        phase = i % 8
        frac = (phase / 4 - 1) if phase >= 4 else (1 - phase / 4)
        out.append(base * (1 + amp_pct * frac))
    return out


def _candles_from(path: list[float], upto: int) -> list[dict]:
    window = path[max(0, upto - WINDOW): upto + 1]
    out = []
    for i, px in enumerate(window):
        prev = window[i - 1] if i else px
        hi, lo = max(prev, px), min(prev, px)
        out.append({"open": prev, "high": hi * 1.0001, "low": lo * 0.9999,
                    "close": px, "volume": 1000.0})
    return out


def _fill_crossed_quotes(adapter: MockNadoAdapter, px: Decimal) -> None:
    for order in list(adapter._orders.values()):  # noqa: SLF001 - sim harness
        if order.state not in (OrderState.OPEN, OrderState.PARTIALLY_FILLED):
            continue
        if order.price is None:
            continue
        if order.side is TradeType.BUY and px <= order.price:
            adapter.fill_order(order.id)
        elif order.side is TradeType.SELL and px >= order.price:
            adapter.fill_order(order.id)


async def _simulate(path: list[float], *, gated: bool) -> Decimal:
    adapter = MockNadoAdapter(mid=Decimal(str(path[0])), auto_fill_market=False)
    inv = InventoryRepository()
    orch = ExecutorOrchestrator()
    step_box = {"i": 0}
    configs: dict[str, object] = {
        "trading_pair": PAIR,
        "spread_bid_pct": Decimal("0.0007"),   # the article's 7 bps
        "spread_ask_pct": Decimal("0.0007"),
        "order_amount_quote": Decimal(100),
        "price_distance_tolerance": Decimal("0.0002"),
    }
    if gated:
        configs["regime_gate_enabled"] = True
        configs["candle_provider"] = lambda _p: _candles_from(path, step_box["i"])
    c = MarketMakingController(
        user_id=1, orchestrator=orch, adapter=adapter, inventory=inv,
        configs=configs, controller_id="SIM",
    )
    await orch.spawn_controller(c)
    for i, px_f in enumerate(path):
        step_box["i"] = i
        px = Decimal(str(px_f))
        adapter.set_mid(px)
        # The move trades against RESTING quotes before the maker can react
        # (that ordering IS adverse selection — fill first, then requote).
        _fill_crossed_quotes(adapter, px)
        await orch.tick_controller(c.id)
    # One last tick so the controller absorbs the final fills.
    await orch.tick_controller(c.id)
    hold = inv.get(1, PAIR, "SIM")
    return hold.realized_pnl + hold.unrealized_pnl(Decimal(str(path[-1])))


def test_gate_recovers_the_trend_loss():
    # The article's scenario: the session starts in chop (gate warmed with
    # full candle history) and the trend arrives MID-SESSION. The gate is
    # fail-open for its first ~50 candles, so a cold start into a trend is
    # partially unprotected by design — that window belongs to the
    # inventory cap + session stop, not the gate.
    warmup = ranging_path(80)
    path = warmup + trending_down_path(240, start=warmup[-1])
    ungated = asyncio.run(_simulate(path, gated=False))
    gated = asyncio.run(_simulate(path, gated=True))
    print(f"\ntrending week: ungated net {ungated:.2f} | gated net {gated:.2f}")
    assert ungated < 0, "the ungated grid must bleed in a one-way market"
    assert gated > ungated, "the gate must beat quoting through the trend"
    # The gate should recover the bulk (>=70%) of the trend damage.
    assert abs(gated) <= abs(ungated) * Decimal("0.3"), (
        f"gate recovered too little: gated {gated:.2f} vs ungated {ungated:.2f}"
    )


def test_gate_does_not_cost_pnl_in_chop():
    path = ranging_path()
    ungated = asyncio.run(_simulate(path, gated=False))
    gated = asyncio.run(_simulate(path, gated=True))
    print(f"\nranging week: ungated net {ungated:.2f} | gated net {gated:.2f}")
    # The chop must actually trade (10bp steps cross the 7bp spread): both
    # bots capture spread, and the gate may not cost meaningful PnL.
    assert ungated > 0, "chop harness must produce round trips"
    assert gated > 0, "the gate must quote through grid-friendly chop"
    assert gated >= ungated - abs(ungated) * Decimal("0.2") - Decimal("1")
